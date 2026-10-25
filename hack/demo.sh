#!/usr/bin/env bash
# Deploys long-lived example jobs in three namespaces and many states, to try kubectl flink by hand and to record
# the README demos. Every job carries a team label and a use-case ID, as real fleets name their jobs.
# Usage: KUBE_CONTEXT=<ctx> FLINK_VERSION=<x.y> hack/demo.sh [clean]
set -euo pipefail
: "${KUBE_CONTEXT:?}" "${FLINK_VERSION:?}"
k() { kubectl --context "$KUBE_CONTEXT" "$@"; }
minor=$(cut -d. -f1-2 <<<"$FLINK_VERSION")
export FLINK_VERSION FLINK_VERSION_ENUM=${minor/./_}
namespaces=(payments orders analytics)

if [[ ${1:-} == clean ]]; then
  for ns in "${namespaces[@]}"; do
    k delete flinkstatesnapshots --all -n "$ns" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  done
  k delete namespace "${namespaces[@]}" --ignore-not-found
  exit 0
fi

# Each namespace gets the service account the chart creates for jobs in the operator's namespace.
for ns in "${namespaces[@]}"; do
  k create namespace "$ns" --dry-run=client -o yaml | k apply -f - >/dev/null
  for obj in serviceaccount/flink role/flink rolebinding/flink-role-binding; do
    k get "$obj" -n default -o json |
      jq --arg ns "$ns" 'del(.metadata.namespace, .metadata.uid, .metadata.resourceVersion, .metadata.creationTimestamp,
        .metadata.managedFields, .metadata.annotations, .metadata.ownerReferences, .secrets)
        | if .subjects then .subjects[].namespace = $ns else . end' |
      k apply -n "$ns" -f - >/dev/null
  done
done

# Small pods so that the jobs fit a 16 GiB kind node: a little CPU each, allowed to burst, and a smaller JobManager.
small='/jobManager:/,/resource:/ s|memory: 1024m|memory: 768m|; s|cpu: 0.5 }|cpu: 0.15 }|g; s|^    taskmanager.numberOfTaskSlots: "1"$|    taskmanager.numberOfTaskSlots: "1"\n    kubernetes.jobmanager.cpu.limit-factor: "6"\n    kubernetes.taskmanager.cpu.limit-factor: "6"|'

# job <namespace> <name> <team> <use-case ID> [sed edits...]: the e2e template under a new name, with edits.
job() {
  local ns=$1 name=$2 team=$3 id=$4; shift 4
  local edits=(-e "s|^  name: basic$|  name: $name\n  namespace: $ns\n  labels: {team: $team, use-case: \"$id\"}|")
  for e in "$@"; do edits+=(-e "$e"); done
  envsubst < test/e2e/basic.yaml.tmpl | sed "${edits[@]}" | sed "$small" | k apply -f -
}
jar() { printf 's|jarURI: .*|jarURI: local:///opt/flink/examples/streaming/%s|' "$1"; }
conf() { printf 's|^    taskmanager.numberOfTaskSlots: "1"$|    taskmanager.numberOfTaskSlots: "1"\\n    %s|' "$1"; }

# payments: a healthy job, and settlement, whose generator runs flat out on a TaskManager held to 0.15 CPU (its two
# slots opt it out of the CPU burst): a busy source in its job graph. Flink's examples have no slow operator, so
# none shows back pressure.
job payments fraud-scoring payments 4711
job payments settlement payments 4713 \
  's|^    upgradeMode: stateless$|    upgradeMode: stateless\n    args: ["--sleep", "0"]|' \
  's|parallelism: 1|parallelism: 2|' \
  's|taskmanager.numberOfTaskSlots: "1"|taskmanager.numberOfTaskSlots: "2"|'

# orders: a job that cannot reach its source and restarts, one whose jar is missing, and one suspended the way
# users do it: once it runs. The operator leaves a job created suspended without any status.
job orders order-enrich orders 5101 \
  's|jarURI: .*|jarURI: local:///opt/flink/examples/streaming/SocketWindowWordCount.jar\n    args: ["--hostname", "orders-db.invalid", "--port", "9999"]|' \
  "$(conf 'restart-strategy.type: fixed-delay\n    restart-strategy.fixed-delay.attempts: "2147483647"\n    restart-strategy.fixed-delay.delay: 10 s')"
job orders order-export orders 5102 "$(jar does-not-exist.jar)"
job orders order-archive orders 5103

# analytics: an image that does not exist, more CPU than the node has, and a savepoint with nowhere to go.
job analytics clickstream analytics 6101 's|image: flink:.*|image: flink:does-not-exist|'
job analytics sessionizer analytics 6102 '/jobManager:/,/resource:/ s|cpu: 0.5|cpu: 100|'
job analytics daily-report analytics 6103 "$(jar TopSpeedWindowing.jar)"
k apply -f - <<'YAML'
apiVersion: flink.apache.org/v1beta1
kind: FlinkStateSnapshot
metadata: {name: daily-report-savepoint, namespace: analytics}
spec: {jobReference: {kind: FlinkDeployment, name: daily-report}, savepoint: {}}
YAML

echo "waiting for order-archive to run before suspending it…"
for _ in $(seq 60); do
  [[ $(k get flinkdeployment order-archive -n orders -o jsonpath='{.status.jobStatus.state}') == RUNNING ]] && break
  sleep 5
done
k patch flinkdeployment order-archive -n orders --type merge -p '{"spec":{"job":{"state":"suspended"}}}'

cat <<EOF

Deployed. Jobs settle in two to three minutes; then try:
  bin/kubectl-flink --context $KUBE_CONTEXT top -A
  bin/kubectl-flink --context $KUBE_CONTEXT ls -A -o wide
  bin/kubectl-flink --context $KUBE_CONTEXT why 4711
  bin/kubectl-flink --context $KUBE_CONTEXT graph payments/settlement
Remove them with: make demo-clean
EOF
