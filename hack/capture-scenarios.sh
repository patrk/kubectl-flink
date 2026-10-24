#!/usr/bin/env bash
# Captures FlinkDeployments in failure states as test fixtures for health and triage.
# Usage: OUT=<dir> KUBE_CONTEXT=<ctx> FLINK_VERSION=<x.y> hack/capture-scenarios.sh
set -euo pipefail
: "${OUT:?}" "${KUBE_CONTEXT:?}" "${FLINK_VERSION:?}"
k() { kubectl --context "$KUBE_CONTEXT" "$@"; }
minor=$(cut -d. -f1-2 <<<"$FLINK_VERSION")
export FLINK_VERSION FLINK_VERSION_ENUM=${minor/./_}
mkdir -p "$OUT"

# deploy <name> <yq-style sed edits...>: renders the basic template under a new name with edits applied.
deploy() {
  local name=$1; shift
  envsubst < test/e2e/basic.yaml.tmpl | sed "s/name: basic/name: $name/" | sed "$@" | k apply -f -
}
# wait_for <name> <jsonpath> <regex> <seconds>
wait_for() {
  local deadline=$((SECONDS + $4))
  until [[ "$(k get flinkdeployment "$1" -o jsonpath="$2")" =~ $3 ]]; do
    (( SECONDS > deadline )) && { echo "$1: timeout waiting for $2 =~ $3" >&2; return 1; }
    sleep 5
  done
}
# wait_pod <name> <regex>: waits until the JobManager container's waiting reason matches, so a fixture shows the
# real failure rather than a startup state.
wait_pod() {
  local deadline=$((SECONDS + 300))
  until [[ "$(k get pods -l "app=$1,component=jobmanager" -o jsonpath='{.items[*].status.containerStatuses[*].state.waiting.reason}')" =~ $2 ]]; do
    (( SECONDS > deadline )) && { echo "$1: timeout waiting for pod reason =~ $2" >&2; return 1; }
    sleep 5
  done
}
# wait_pod_event <name> <reason>: waits until an event with that reason names one of the job's pods.
wait_pod_event() {
  local deadline=$((SECONDS + 300))
  until k get events --field-selector "reason=$2" -o jsonpath='{.items[*].involvedObject.name}' | grep -q "$1"; do
    (( SECONDS > deadline )) && { echo "$1: timeout waiting for event $2" >&2; return 1; }
    sleep 5
  done
}
# check_why <name> <rule id>: when the binary is built, why must name the induced cause first.
check_why() {
  [[ -x bin/kubectl-flink ]] || return 0
  local first
  first=$(bin/kubectl-flink --context "$KUBE_CONTEXT" why "default/$1" -o json | jq -r '.verdict.findings[0].id // "none"')
  [[ $first == "$2" ]] || { echo "why $1: first finding $first, want $2" >&2; bin/kubectl-flink --context "$KUBE_CONTEXT" why "default/$1" >&2; exit 1; }
  echo "why $1: $first"
}
save() {
  mkdir -p "$OUT/$1"
  k get flinkdeployment "$1" -o json --show-managed-fields > "$OUT/$1/flinkdeployment.json"
  k get pods -l "app=$1" -o json > "$OUT/$1/pods.json"
  # Events of this resource and its pods only, by uid: events of an earlier resource with the same name linger.
  local uids
  uids=$(jq -r '.metadata.uid' "$OUT/$1/flinkdeployment.json"; jq -r '.items[].metadata.uid' "$OUT/$1/pods.json")
  k get events -o json | jq --arg uids "$uids" '($uids | split("\n")) as $u | .items |= map(select(.involvedObject.uid as $x | $u | index($x)))' > "$OUT/$1/events.json"
  # JobManager logs, current and of the previous container after a crash; a pod that never started has none.
  mkdir -p "$OUT/$1/logs"
  for pod in $(k get pods -l "app=$1,component=jobmanager" -o name); do
    k logs "$pod" -c flink-main-container --tail 300 > "$OUT/$1/logs/${pod#pod/}.log" 2>/dev/null || true
    k logs "$pod" -c flink-main-container --tail 300 --previous > "$OUT/$1/logs/${pod#pod/}.previous.log" 2>/dev/null || true
  done
  find "$OUT/$1/logs" -empty -delete
  rm -f "$OUT/$1/events-cr.json"
}
SCENARIOS='bad-image missing-jar suspended bad-memory missing-secret unschedulable tm-lost'
trap 'k delete flinkdeployment $SCENARIOS --ignore-not-found --wait=false >/dev/null' EXIT

deploy bad-image -e "s|image: flink:.*|image: flink:does-not-exist|"
deploy missing-jar -e "s|jarURI: .*|jarURI: local:///opt/flink/does-not-exist.jar|"
deploy suspended -e ""
# The operator rejects a TaskManager too small for Flink's fixed memory pools before creating anything.
deploy bad-memory -e '/taskManager:/,/resource:/ s|memory: 1024m|memory: 400m|'
# A pod template volume from a Secret that does not exist: the JobManager pod waits in ContainerCreating.
deploy missing-secret -e 's|  jobManager:|  podTemplate:\n    spec:\n      containers:\n        - name: flink-main-container\n          volumeMounts: [{name: creds, mountPath: /opt/creds}]\n      volumes: [{name: creds, secret: {secretName: does-not-exist}}]\n  jobManager:|'
# More CPU than any node has: the JobManager pod stays Pending.
deploy unschedulable -e '/jobManager:/,/resource:/ s|cpu: 0.5|cpu: 100|'

# Suspended the way users do it: a running job whose spec.job.state is flipped.
wait_for suspended '{.status.lifecycleState}/{.status.jobStatus.state}' '^STABLE/RUNNING$' 300
k patch flinkdeployment suspended --type merge -p '{"spec":{"job":{"state":"suspended"}}}'
wait_for suspended '{.status.lifecycleState}' '^SUSPENDED$' 300
save suspended
wait_pod bad-image 'ImagePullBackOff'
wait_for bad-image '{.status.jobManagerDeploymentStatus}' '^ERROR$' 300
save bad-image
check_why bad-image image-pull
wait_pod missing-jar 'CrashLoopBackOff'
wait_for missing-jar '{.status.jobManagerDeploymentStatus}' '^ERROR$' 300
save missing-jar
check_why missing-jar jar-missing
wait_for bad-memory '{.status.error}' 'IllegalConfigurationException' 180
save bad-memory
check_why bad-memory memory-config
wait_pod_event missing-secret 'FailedMount'
save missing-secret
check_why missing-secret mount-failed
wait_pod_event unschedulable 'FailedScheduling'
save unschedulable
check_why unschedulable unschedulable
# A lost TaskManager: the job restarts, and the exception history and checkpoints show it. It runs alone, after
# the others are gone, because each cluster requests 2 GiB and a kind node has little more than 8.
k delete flinkdeployment bad-image missing-jar suspended bad-memory missing-secret unschedulable --wait=true --timeout=120s
# Checkpoints every 10 s, so that the restart after the lost TaskManager restores from one.
deploy tm-lost -e 's|    taskmanager.numberOfTaskSlots: "1"|    taskmanager.numberOfTaskSlots: "1"\n    execution.checkpointing.interval: 10s|'
wait_for tm-lost '{.status.lifecycleState}/{.status.jobStatus.state}' '^STABLE/RUNNING$' 300
sleep 30 # a few checkpoints first
k delete pod -l app=tm-lost,component=taskmanager --wait=false
wait_for tm-lost '{.status.jobStatus.state}' '^(RESTARTING|INITIALIZING|CREATED)$' 120 || true
wait_for tm-lost '{.status.jobStatus.state}' '^RUNNING$' 300
REQUIRE_EXCEPTIONS=1 KUBE_CONTEXT=$KUBE_CONTEXT OUT=$OUT/tm-lost hack/capture-fixtures.sh tm-lost
save tm-lost
check_why tm-lost taskmanager-lost
echo "scenarios in $OUT"
