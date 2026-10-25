#!/usr/bin/env bash
# Saves a FlinkDeployment and its JobManager REST responses as test fixtures.
# Usage: OUT=<dir> KUBE_CONTEXT=<ctx> hack/capture-fixtures.sh <name>
set -euo pipefail
name=${1:?name}
: "${OUT:?}" "${KUBE_CONTEXT:?}"
k() { kubectl --context "$KUBE_CONTEXT" "$@"; }
# Cleared first: files of an earlier capture would otherwise mix in, e.g. vertex metrics of a job that now restarts.
rm -rf "$OUT/rest"
mkdir -p "$OUT/rest"

k get flinkdeployment "$name" -o json --show-managed-fields > "$OUT/flinkdeployment.json"
k get pods -l "app=$name" -o json > "$OUT/pods.json"
# All namespace events: pod and deployment events carry other involvedObject names than the CR.
k get events -o json > "$OUT/events.json"

# Port 0 lets kubectl pick a free local port, which it prints; a fixed port could hit a stale forward.
pflog=$(mktemp)
kubectl --context "$KUBE_CONTEXT" port-forward "svc/$name-rest" :8081 >"$pflog" 2>&1 &
pf=$!
trap 'kill $pf 2>/dev/null || true; rm -f "$pflog"' EXIT
port=
for _ in $(seq 30); do
  port=$(sed -nE 's/^Forwarding from 127\.0\.0\.1:([0-9]+) .*/\1/p' "$pflog" | head -1)
  [[ -n $port ]] && curl -sf "localhost:$port/config" >/dev/null && break
  kill -0 "$pf" 2>/dev/null || { cat "$pflog" >&2; exit 1; }
  port=
  sleep 1
done
[[ -n $port ]] || { echo "JobManager REST not reachable through port-forward after 30 s" >&2; cat "$pflog" >&2; exit 1; }

# Some endpoints, such as backpressure, fail until the JobManager has sampled the job once; retry for 60 s.
# get <path> <name> [jq condition]: with a condition, also retry until the response satisfies it, for data that
# arrives later, such as metrics after the first report or the exception history after a failure.
get() {
  local cond=${3:-true}
  for _ in $(seq 30); do
    curl -sf "localhost:$port$1" > "$OUT/rest/$2.json" && jq -e "$cond" "$OUT/rest/$2.json" >/dev/null && return
    sleep 2
  done
  echo "GET $1 did not satisfy '$cond' for 60 s: $(curl -s "localhost:$port$1" | head -c 300)" >&2
  exit 1
}
# optional <path> <name>: one request; an endpoint a job cannot serve (one without checkpointing has no
# checkpoint config) only skips the file, with the answer on stderr.
optional() {
  local code
  code=$(curl -s -o "$OUT/rest/$2.json" -w '%{http_code}' "localhost:$port$1") || code=000
  [[ $code == 200 ]] || { echo "skipped $1: HTTP $code $(head -c 200 "$OUT/rest/$2.json" 2>/dev/null)" >&2; rm -f "$OUT/rest/$2.json"; }
}
get /config config
get /jobmanager/config jobmanager-config
get /taskmanagers taskmanagers
get /jobs/overview jobs-overview
jid=$(jq -er '.jobs[0].jid' "$OUT/rest/jobs-overview.json") || { echo "no job in /jobs/overview" >&2; exit 1; }
get "/jobs/$jid" job
# REQUIRE_EXCEPTIONS=1 waits for the failure a scenario induced to reach the exception history.
if [[ ${REQUIRE_EXCEPTIONS:-} == 1 ]]; then
  get "/jobs/$jid/exceptions?maxExceptions=20" exceptions '.exceptionHistory.entries | length > 0'
else
  get "/jobs/$jid/exceptions?maxExceptions=20" exceptions
fi
get "/jobs/$jid/checkpoints" checkpoints
optional "/jobs/$jid/checkpoints/config" checkpoints-config
get "/jobs/$jid/config" job-config
# Metrics only of a running job. The names list shows which optional metrics, such as a source's
# <operator>.pendingRecords, a vertex has.
if [[ $(jq -r .state "$OUT/rest/job.json") == RUNNING ]]; then
  for vid in $(jq -r '.vertices[].id' "$OUT/rest/job.json"); do
    optional "/jobs/$jid/vertices/$vid/backpressure" "backpressure-$vid"
    get "/jobs/$jid/vertices/$vid/subtasks/metrics" "metric-names-$vid" 'length > 0'
    get "/jobs/$jid/vertices/$vid/subtasks/metrics?get=busyTimeMsPerSecond,backPressuredTimeMsPerSecond,idleTimeMsPerSecond,numRecordsInPerSecond,numRecordsOutPerSecond&agg=sum,max,avg,min" "metrics-$vid" 'length > 0'
  done
fi
echo "fixtures in $OUT"
