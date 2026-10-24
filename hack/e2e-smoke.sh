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

deadline=$((SECONDS + 600))
while :; do
  state=$(k get flinkdeployment basic -o jsonpath='{.status.lifecycleState}/{.status.jobStatus.state}/{.status.jobManagerDeploymentStatus}')
  case "$state" in
    STABLE/RUNNING/*) break ;;
    FAILED/* | */FAILED/* | */*/ERROR) echo "basic failed: $state" >&2; diagnose ;;
  esac
  (( SECONDS > deadline )) && { echo "basic not running after 600 s: $state" >&2; diagnose; }
  sleep 5
done
echo "basic is STABLE/RUNNING"
bin/kubectl-flink version
