#!/usr/bin/env bash
# Saves a FlinkDeployment and its JobManager REST responses as test fixtures.
# Usage: OUT=<dir> KUBE_CONTEXT=<ctx> hack/capture-fixtures.sh <name>
set -euo pipefail
name=${1:?name}
: "${OUT:?}" "${KUBE_CONTEXT:?}"
k() { kubectl --context "$KUBE_CONTEXT" "$@"; }
mkdir -p "$OUT/rest"

k get flinkdeployment "$name" -o json > "$OUT/flinkdeployment.json"
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

get() { curl -sf "localhost:$port$1" > "$OUT/rest/$2.json" || { echo "GET $1 failed" >&2; exit 1; }; }
get /config config
get /jobmanager/config jobmanager-config
get /taskmanagers taskmanagers
get /jobs/overview jobs-overview
jid=$(jq -er '.jobs[0].jid' "$OUT/rest/jobs-overview.json") || { echo "no job in /jobs/overview" >&2; exit 1; }
get "/jobs/$jid" job
get "/jobs/$jid/exceptions?maxExceptions=20" exceptions
get "/jobs/$jid/checkpoints" checkpoints
get "/jobs/$jid/checkpoints/config" checkpoints-config
get "/jobs/$jid/config" job-config
for vid in $(jq -r '.vertices[].id' "$OUT/rest/job.json"); do
  get "/jobs/$jid/vertices/$vid/backpressure" "backpressure-$vid"
  get "/jobs/$jid/vertices/$vid/subtasks/metrics?get=busyTimeMsPerSecond,backPressuredTimeMsPerSecond,idleTimeMsPerSecond,numRecordsInPerSecond,numRecordsOutPerSecond,pendingRecords&agg=sum,max,avg" "metrics-$vid"
done
echo "fixtures in $OUT"
