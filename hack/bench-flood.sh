#!/usr/bin/env bash
# Times kubectl flink against plain kubectl for the same questions with hyperfine, against the current cluster:
# make demo flood gives 2,009 FlinkDeployments. Writes docs/demo/out/bench.md. Both read the same API server, so
# the numbers compare the tools, not the cluster; a remote cluster adds its round trips to both.
# Usage: KUBE_CONTEXT=<ctx> hack/bench-flood.sh, from the repository root after make build.
set -euo pipefail
: "${KUBE_CONTEXT:?}"
command -v hyperfine >/dev/null || { echo "needs hyperfine: brew install hyperfine" >&2; exit 1; }
mkdir -p docs/demo/out
k="kubectl --context $KUBE_CONTEXT" f="bin/kubectl-flink --context $KUBE_CONTEXT"
n=$($k get flinkdeployments -A --no-headers | wc -l | tr -d ' ')
# What kubectl alone can say about an unhealthy job: a lifecycle other than STABLE or SUSPENDED, or an error.
unhealthy='.items[] | select((.status.lifecycleState | IN("STABLE", "SUSPENDED") | not) or .status.error != null) | .metadata.name'
{
  echo "$n FlinkDeployments, $(date -u +%Y-%m-%d), hyperfine $(hyperfine --version | cut -d' ' -f2); mean ± σ of 20 runs after 3 warmups."
  echo
} > docs/demo/out/bench.md
run() {
  local table
  table=$(mktemp)
  hyperfine --shell=default --warmup 3 --runs 20 --style basic --export-markdown "$table" "$@" >&2
  cat "$table" >> docs/demo/out/bench.md
  echo >> docs/demo/out/bench.md
  rm -f "$table"
}
run -n "kubectl get -A" "$k get flinkdeployments -A" -n "kubectl flink ls -A" "$f ls -A"
run -n "kubectl get -A -o json | jq (unhealthy)" "$k get flinkdeployments -A -o json | jq -r '$unhealthy'" \
  -n "kubectl flink ls -A --unhealthy" "$f ls -A --unhealthy"
run -n "kubectl get -A -l use-case=1234" "$k get flinkdeployments -A -l use-case=1234" -n "kubectl flink ls -A 1234" "$f ls -A 1234"
cat docs/demo/out/bench.md
