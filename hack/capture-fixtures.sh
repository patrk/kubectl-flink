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
k get events --field-selector "involvedObject.name=$name" -o json > "$OUT/events.json"

port=18081
k port-forward "svc/$name-rest" "$port:8081" >/dev/null &
pf=$!
trap 'kill $pf' EXIT
for _ in $(seq 30); do curl -sf "localhost:$port/config" >/dev/null && break; sleep 1; done

get() { curl -sf "localhost:$port$1" > "$OUT/rest/$2.json"; }
get /config config
get /jobmanager/config jobmanager-config
get /taskmanagers taskmanagers
get /jobs/overview jobs-overview
jid=$(jq -r '.jobs[0].jid' "$OUT/rest/jobs-overview.json")
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
