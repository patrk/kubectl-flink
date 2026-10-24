#!/usr/bin/env bash
# Deploys one FlinkDeployment and waits until the operator reports it STABLE and RUNNING.
set -euo pipefail
: "${FLINK_VERSION:?}" "${KUBE_CONTEXT:?}"
k() { kubectl --context "$KUBE_CONTEXT" "$@"; }

FLINK_VERSION_ENUM=${FLINK_VERSION/./_} envsubst < test/e2e/basic.yaml.tmpl | k apply -f -

deadline=$((SECONDS + 600))
until [[ "$(k get flinkdeployment basic -o jsonpath='{.status.lifecycleState}/{.status.jobStatus.state}')" == STABLE/RUNNING ]]; do
  if (( SECONDS > deadline )); then
    k get flinkdeployment basic -o yaml
    k get pods,events
    exit 1
  fi
  sleep 5
done
echo "basic is STABLE/RUNNING"
bin/kubectl-flink version
