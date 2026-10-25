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
# Scenario pods request little CPU so that a 4-CPU CI runner fits a batch next to basic and kube-system (about
# 2 CPUs); the limit factor lets each still use about 0.9 CPU, since Flink sets the limit to the request by default.
# Scenario edits run first, so unschedulable keeps its 100 CPUs.
small='s|cpu: 0.5 }|cpu: 0.15 }|g; s|^    taskmanager.numberOfTaskSlots: "1"$|    taskmanager.numberOfTaskSlots: "1"\n    kubernetes.jobmanager.cpu.limit-factor: "6"\n    kubernetes.taskmanager.cpu.limit-factor: "6"|'
deploy() {
  local name=$1; shift
  local manifest
  manifest=$(envsubst < test/e2e/basic.yaml.tmpl | sed "s/name: basic/name: $name/" | sed "$@" | sed "$small")
  # Without the limit factor the pods would be throttled at 0.15 CPU; fail here rather than time out later.
  grep -q 'kubernetes.taskmanager.cpu.limit-factor: "6"' <<<"$manifest" ||
    { echo "$name: CPU limit factor not set; does a scenario edit change numberOfTaskSlots?" >&2; exit 1; }
  k apply -f - <<<"$manifest"
}
# diagnose_scheduling: what a timeout needs to be understood in CI, where the cluster is gone afterwards.
diagnose_scheduling() {
  echo "--- pods" >&2
  k get pods -o wide >&2 || true
  echo "--- node resources" >&2
  k describe node | sed -n '/Allocated resources/,/Events/p' >&2 || true
  echo "--- warning events" >&2
  k get events --field-selector type=Warning --sort-by=.lastTimestamp 2>/dev/null | tail -20 >&2 || true
}
# wait_for <name> <jsonpath> <regex> <seconds>
wait_for() {
  local deadline=$((SECONDS + $4))
  until [[ "$(k get flinkdeployment "$1" -o jsonpath="$2")" =~ $3 ]]; do
    (( SECONDS > deadline )) && { echo "$1: timeout waiting for $2 =~ $3" >&2; diagnose_scheduling; return 1; }
    sleep 5
  done
}
# wait_pod <name> <regex>: waits until the JobManager container's waiting reason matches, so a fixture shows the
# real failure rather than a startup state.
wait_pod() {
  local deadline=$((SECONDS + 300))
  until [[ "$(k get pods -l "app=$1,component=jobmanager" -o jsonpath='{.items[*].status.containerStatuses[*].state.waiting.reason}')" =~ $2 ]]; do
    (( SECONDS > deadline )) && { echo "$1: timeout waiting for pod reason =~ $2" >&2; diagnose_scheduling; return 1; }
    sleep 5
  done
}
# wait_pod_event <name> <reason>: waits until an event with that reason names one of the job's pods.
wait_pod_event() {
  local deadline=$((SECONDS + 300))
  until k get events --field-selector "reason=$2" -o jsonpath='{.items[*].involvedObject.name}' | grep -q "$1"; do
    (( SECONDS > deadline )) && { echo "$1: timeout waiting for event $2" >&2; diagnose_scheduling; return 1; }
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
# save <name> [dir]: the resource <name> and what belongs to it, into $OUT/<dir>, by default $OUT/<name>.
save() {
  local out="$OUT/${2:-$1}"
  mkdir -p "$out"
  # The time of the capture, for ages in tests; kind shares the host's clock.
  date -u +%Y-%m-%dT%H:%M:%SZ > "$out/captured-at.txt"
  k get flinkdeployment "$1" -o json --show-managed-fields > "$out/flinkdeployment.json"
  k get pods -l "app=$1" -o json > "$out/pods.json"
  # Events of this resource and its pods only, by uid: events of an earlier resource with the same name linger.
  local uids
  uids=$(jq -r '.metadata.uid' "$out/flinkdeployment.json"; jq -r '.items[].metadata.uid' "$out/pods.json")
  k get events -o json | jq --arg uids "$uids" '($uids | split("\n")) as $u | .items |= map(select(.involvedObject.uid as $x | $u | index($x)))' > "$out/events.json"
  # JobManager logs, current and of the previous container after a crash; a pod that never started has none.
  # Saved as .txt: global gitignore files commonly exclude *.log, which would drop them from commits silently.
  rm -rf "$out/logs"
  mkdir -p "$out/logs"
  for pod in $(k get pods -l "app=$1,component=jobmanager" -o name); do
    k logs "$pod" -c flink-main-container --tail 300 > "$out/logs/${pod#pod/}.txt" 2>/dev/null || true
    k logs "$pod" -c flink-main-container --tail 300 --previous > "$out/logs/${pod#pod/}.previous.txt" 2>/dev/null || true
  done
  find "$out/logs" -empty -delete
  # ConfigMaps of the cluster (HA leader, job plans, checkpoint pointers) by key; values are blobs and dropped.
  k get configmaps -l "app=$1" -o json | jq '.items |= map(.data |= (if . then with_entries(.value = "") else . end) | del(.binaryData))' > "$out/configmaps.json"
  rm -f "$out/events-cr.json"
  # FlinkStateSnapshots of the job, by spec: an operator that does not process them leaves them without labels.
  k get flinkstatesnapshots -o json | jq --arg n "$1" '.items |= map(select(.spec.jobReference.name == $n))' > "$out/snapshots.json"
}
# operator_snapshots on|off: whether the operator processes FlinkStateSnapshots. It decides only when it starts,
# so a change restarts it; "on" is the chart's default and does nothing when nothing was changed.
operator_snapshots() {
  local key=kubernetes.operator.snapshot.resource.enabled conf
  # Charts up to 1.15 also ship the legacy flink-conf.yaml, which Flink reads first when it exists; both get the
  # setting.
  local files='def files: .key == "config.yaml" or .key == "flink-conf.yaml";'
  conf=$(k get configmap flink-operator-config -o json)
  if [[ $1 == on ]]; then
    jq -e --arg key "$key" '.data | to_entries | any(.value | contains($key))' <<<"$conf" >/dev/null || return 0
    conf=$(jq --arg key "$key" "$files"'
      .data |= with_entries(if files then .value |= (split("\n") | map(select(startswith($key) | not)) | join("\n") | sub("\n+$"; "\n")) else . end)' <<<"$conf")
  else
    conf=$(jq --arg key "$key" "$files"' .data |= with_entries(if files then .value += "\n\($key): false\n" else . end)' <<<"$conf")
  fi
  k replace -f - <<<"$conf" >/dev/null
  k rollout restart deploy/flink-kubernetes-operator >/dev/null
  k rollout status deploy/flink-kubernetes-operator --timeout=180s >/dev/null
}
SCENARIOS='bad-image missing-jar suspended bad-memory missing-secret unschedulable checkpoint-storage tm-lost unreachable restart-loop restart-bot ha snap'
# Leftovers of an earlier run are deleted and waited for: applying a name that is still being deleted
# creates nothing. A snapshot whose job is gone keeps the operator's finalizer, so its finalizer is dropped.
for s in $(k get flinkstatesnapshots -o name); do
  k patch "$s" --type merge -p '{"metadata":{"finalizers":null}}' >/dev/null
done
k delete flinkstatesnapshots --all --ignore-not-found --wait=true --timeout=60s >/dev/null
k delete flinkdeployment $SCENARIOS --ignore-not-found --wait=true --timeout=180s >/dev/null
operator_snapshots on # left off by an interrupted run
trap 'k delete flinkstatesnapshots --all --ignore-not-found --wait=false >/dev/null; k delete flinkdeployment $SCENARIOS --ignore-not-found --wait=false >/dev/null; operator_snapshots on' EXIT

deploy bad-image -e "s|image: flink:.*|image: flink:does-not-exist|"
deploy missing-jar -e "s|jarURI: .*|jarURI: local:///opt/flink/does-not-exist.jar|"
deploy suspended -e ""
# The operator rejects a TaskManager too small for Flink's fixed memory pools before creating anything.
deploy bad-memory -e '/taskManager:/,/resource:/ s|memory: 1024m|memory: 400m|'
# A pod template volume from a Secret that does not exist: the JobManager pod waits in ContainerCreating.
deploy missing-secret -e 's|  jobManager:|  podTemplate:\n    spec:\n      containers:\n        - name: flink-main-container\n          volumeMounts: [{name: creds, mountPath: /opt/creds}]\n      volumes: [{name: creds, secret: {secretName: does-not-exist}}]\n  jobManager:|'
# More CPU than any node has: the JobManager pod stays Pending.
deploy unschedulable -e '/jobManager:/,/resource:/ s|cpu: 0.5|cpu: 100|'
# data_dir <path>: a sed edit mounting a node directory at /flink-data. Each scenario has its own, so that one
# that makes it writable does not change another's on the same node.
data_dir() {
  printf '%s\n' 's|  jobManager:|  podTemplate:\n    spec:\n      containers:\n        - name: flink-main-container\n          volumeMounts: [{name: flink-data, mountPath: /flink-data}]\n      volumes: [{name: flink-data, hostPath: {path: '"$1"', type: DirectoryOrCreate}}]\n  jobManager:|'
}
# Checkpoints to a directory Flink cannot write: a hostPath that kubelet creates owned by root.
fs_checkpoints='s|^    taskmanager.numberOfTaskSlots: "1"$|    taskmanager.numberOfTaskSlots: "1"\n    execution.checkpointing.interval: 10s\n    execution.checkpointing.dir: file:///flink-data/checkpoints\n    execution.checkpointing.savepoint-dir: file:///flink-data/savepoints|'
deploy checkpoint-storage -e "$fs_checkpoints" -e "$(data_dir /tmp/flink-unwritable-$RANDOM)"

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
wait_pod checkpoint-storage 'CrashLoopBackOff'
save checkpoint-storage
check_why checkpoint-storage checkpoint-storage-unwritable
# The first batch is deleted before the second starts, which runs five jobs to RUNNING at 2 GiB each.
k delete flinkdeployment bad-image missing-jar suspended bad-memory missing-secret unschedulable checkpoint-storage --wait=true --timeout=120s
checkpointing='s|    taskmanager.numberOfTaskSlots: "1"|    taskmanager.numberOfTaskSlots: "1"\n    execution.checkpointing.interval: 10s|'
unreachable_job='s|jarURI: .*|jarURI: local:///opt/flink/examples/streaming/SocketWindowWordCount.jar\n    args: ["--hostname", "nowhere.invalid", "--port", "9999"]|'
# Checkpoints every 10 s, so that the restart after the lost TaskManager restores from one.
deploy tm-lost -e "$checkpointing"
# A source that cannot reach its host, without a restart strategy: the first failure is final.
deploy unreachable -e "$unreachable_job"
# The same with a restart strategy that never gives up: the job keeps restarting.
deploy restart-loop -e "$unreachable_job" -e 's|    taskmanager.numberOfTaskSlots: "1"|    taskmanager.numberOfTaskSlots: "1"\n    restart-strategy.type: fixed-delay\n    restart-strategy.fixed-delay.attempts: "2147483647"\n    restart-strategy.fixed-delay.delay: 5 s|'
# Restarted twice through spec.restartNonce by another field manager, as an external controller would.
deploy restart-bot -e ""
# Kubernetes HA: the HA ConfigMaps hold the job plan and checkpoint pointers.
deploy ha -e 's|    taskmanager.numberOfTaskSlots: "1"|    taskmanager.numberOfTaskSlots: "1"\n    high-availability.type: kubernetes\n    high-availability.storageDir: file:///tmp/flink-ha|' -e "$checkpointing"

wait_for unreachable '{.status.jobStatus.state}' '^FAILED$' 300
KUBE_CONTEXT=$KUBE_CONTEXT OUT=$OUT/unreachable hack/capture-fixtures.sh unreachable
save unreachable
check_why unreachable dependency-unreachable

wait_for restart-loop '{.status.jobStatus.state}' '^(RESTARTING|RUNNING|CREATED)$' 300
sleep 30 # several restarts
REQUIRE_EXCEPTIONS=1 KUBE_CONTEXT=$KUBE_CONTEXT OUT=$OUT/restart-loop hack/capture-fixtures.sh restart-loop
save restart-loop
check_why restart-loop dependency-unreachable

wait_for restart-bot '{.status.lifecycleState}/{.status.jobStatus.state}' '^STABLE/RUNNING$' 300
for nonce in 1 2; do
  k patch flinkdeployment restart-bot --type merge --field-manager restart-bot -p "{\"spec\":{\"restartNonce\":$nonce}}"
  wait_for restart-bot '{.status.observedGeneration}' "^$((nonce + 1))$" 300
  wait_for restart-bot '{.status.lifecycleState}/{.status.jobStatus.state}' '^STABLE/RUNNING$' 300
done
save restart-bot
check_why restart-bot external-restart

wait_for ha '{.status.lifecycleState}/{.status.jobStatus.state}' '^STABLE/RUNNING$' 300
sleep 20 # a checkpoint pointer in the HA ConfigMap
KUBE_CONTEXT=$KUBE_CONTEXT OUT=$OUT/ha hack/capture-fixtures.sh ha
save ha

# A lost TaskManager: the job restarts, and the exception history and checkpoints show it.
wait_for tm-lost '{.status.lifecycleState}/{.status.jobStatus.state}' '^STABLE/RUNNING$' 300
sleep 30 # a few checkpoints first
k delete pod -l app=tm-lost,component=taskmanager --wait=false
wait_for tm-lost '{.status.jobStatus.state}' '^(RESTARTING|INITIALIZING|CREATED)$' 120 || true
wait_for tm-lost '{.status.jobStatus.state}' '^RUNNING$' 300
REQUIRE_EXCEPTIONS=1 KUBE_CONTEXT=$KUBE_CONTEXT OUT=$OUT/tm-lost hack/capture-fixtures.sh tm-lost
save tm-lost
check_why tm-lost taskmanager-lost

# FlinkStateSnapshots, last: switching the operator's snapshot handling restarts it. The job checkpoints and saves to a
# directory both its pods share, made writable first; basic has no savepoint directory, so its savepoint fails.
k delete flinkdeployment tm-lost unreachable restart-loop restart-bot ha --wait=true --timeout=120s
writable='s|      volumes: \[{name: flink-data|      initContainers:\n        - {name: data-dir, image: "flink:'"$FLINK_VERSION"'", command: [sh, -c, "chmod 0777 /flink-data"], securityContext: {runAsUser: 0}, volumeMounts: [{name: flink-data, mountPath: /flink-data}]}\n      volumes: [{name: flink-data|'
deploy snap -e "$fs_checkpoints" -e "$(data_dir /tmp/flink-snap)" -e 's|StateMachineExample.jar|TopSpeedWindowing.jar|' -e "$writable"
wait_for snap '{.status.lifecycleState}/{.status.jobStatus.state}' '^STABLE/RUNNING$' 300
sleep 30 # a few checkpoints
# First with snapshot handling off, as when the operator could not list FlinkStateSnapshots when it started: the
# snapshots stay without a status.
operator_snapshots off
k apply -f - <<'YAML'
apiVersion: flink.apache.org/v1beta1
kind: FlinkStateSnapshot
metadata: {name: snap-savepoint}
spec: {jobReference: {kind: FlinkDeployment, name: snap}, savepoint: {formatType: CANONICAL}}
---
apiVersion: flink.apache.org/v1beta1
kind: FlinkStateSnapshot
metadata: {name: snap-checkpoint}
spec: {jobReference: {kind: FlinkDeployment, name: snap}, checkpoint: {}}
---
apiVersion: flink.apache.org/v1beta1
kind: FlinkStateSnapshot
metadata: {name: basic-savepoint}
spec: {jobReference: {kind: FlinkDeployment, name: basic}, savepoint: {}}
YAML
sleep 130 # why reports snapshots without a status after two minutes
save snap snapshot-ignored
check_why snap snapshots-ignored
operator_snapshots on
deadline=$((SECONDS + 300))
until [[ $(k get flinkstatesnapshot snap-savepoint snap-checkpoint -o jsonpath='{.items[*].status.state}') == "COMPLETED COMPLETED" &&
         $(k get flinkstatesnapshot basic-savepoint -o jsonpath='{.status.failures}') -ge 1 ]]; do
  (( SECONDS > deadline )) && { echo "snapshots: timeout" >&2; k get flinkstatesnapshots -o yaml >&2; diagnose_scheduling; exit 1; }
  sleep 5
done
KUBE_CONTEXT=$KUBE_CONTEXT OUT=$OUT/snapshots hack/capture-fixtures.sh snap
save snap snapshots
save basic savepoint-failing
check_why basic savepoint-dir-missing
echo "scenarios in $OUT"
