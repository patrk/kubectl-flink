#!/usr/bin/env bash
# Deploys one FlinkDeployment and waits until the operator reports it STABLE and RUNNING.
set -euo pipefail
: "${FLINK_VERSION:?}" "${KUBE_CONTEXT:?}"
k() { kubectl --context "$KUBE_CONTEXT" "$@"; }

minor=$(cut -d. -f1-2 <<<"$FLINK_VERSION")
FLINK_VERSION_ENUM=${minor/./_} envsubst < test/e2e/basic.yaml.tmpl | k apply -f -

diagnose() {
  k get flinkdeployment basic -o yaml || true
  k get pods,events || true
  k logs deploy/flink-kubernetes-operator --tail=200 || true
  exit 1
}

# A JobManager ERROR right after submit is normal: the operator creates the config ConfigMap just after the
# Deployment, so the first pod can fail to mount it. Only an ERROR that lasts counts as a failure.
deadline=$((SECONDS + 600))
error_since=
while :; do
  state=$(k get flinkdeployment basic -o jsonpath='{.status.lifecycleState}/{.status.jobStatus.state}/{.status.jobManagerDeploymentStatus}')
  case "$state" in
    STABLE/RUNNING/*) break ;;
    FAILED/* | */FAILED/*) echo "basic failed: $state" >&2; diagnose ;;
    */*/ERROR)
      error_since=${error_since:-$SECONDS}
      (( SECONDS - error_since > 90 )) && { echo "basic in JobManager ERROR for 90 s: $state" >&2; diagnose; } ;;
    *) error_since= ;;
  esac
  (( SECONDS > deadline )) && { echo "basic not running after 600 s: $state" >&2; diagnose; }
  sleep 5
done
echo "basic is STABLE/RUNNING"
bin/kubectl-flink version

# ls against the real operator: basic must be OK with its TaskManager ready.
bin/kubectl-flink --context "$KUBE_CONTEXT" ls -A -o wide
item=$(bin/kubectl-flink --context "$KUBE_CONTEXT" ls -A -o json | jq -c '.items[] | select(.name == "basic")')
jq -e '.health == "OK" and .taskManagers.ready == 1 and .taskManagers.desired == 1' <<<"$item" >/dev/null ||
  { echo "ls: basic is not OK with 1/1 TaskManagers: $item" >&2; diagnose; }
bin/kubectl-flink --context "$KUBE_CONTEXT" ls -A --exit-code --no-headers >/dev/null ||
  { echo "ls --exit-code reports unhealthy jobs" >&2; diagnose; }
echo "ls reports basic OK"

# report: a bundle with a summary and the evidence, and history of the same job.
out=$(mktemp -d)
bin/kubectl-flink --context "$KUBE_CONTEXT" report default/basic --dir "$out" -q >/dev/null
for f in summary.md why.txt history.txt resource.json pods.json events.json flink-config.json rest/job.json; do
  [[ -s $out/$f ]] || { echo "report: $f missing or empty" >&2; ls -R "$out" >&2; diagnose; }
done
grep -q '^## Matched patterns' "$out/summary.md" || { echo "report: summary has no verdict" >&2; diagnose; }
# Output first: with pipefail, grep -q closing the pipe early would fail the writer.
hist=$(bin/kubectl-flink --context "$KUBE_CONTEXT" history default/basic)
grep -q 'created, generation 1' <<<"$hist" || { echo "history: no creation entry" >&2; echo "$hist" >&2; diagnose; }
echo "report and history work for basic"
