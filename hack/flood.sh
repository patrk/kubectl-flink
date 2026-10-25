#!/usr/bin/env bash
# Adds a fleet of FlinkDeployments that no Flink cluster runs behind, to try kubectl flink at the scale of a real
# platform: FLOOD_JOBS jobs (default 2000) in FLOOD_NAMESPACES namespaces (default 40). Their specs and statuses are
# copied from jobs the operator really ran (testdata/fixtures), with timestamps moved to now, so each resource has a
# real size and a real status; the operator is told to ignore them by a label selector, so it neither deploys them
# nor overwrites the statuses. About 93 % are healthy; the rest are suspended, restarting, failed, stuck or
# deploying, the way a large fleet looks on an ordinary day. The jobs carry Argo CD, Flux and Helm markers and
# field managers in the proportions of a mixed platform. The jobs of make demo keep running beside them.
# Deploying and stuck statuses age: after about 10 minutes run it again, which only rewrites the statuses.
# Usage: KUBE_CONTEXT=<ctx> hack/flood.sh [clean]
set -euo pipefail
: "${KUBE_CONTEXT:?}"
jobs=${FLOOD_JOBS:-2000} nss=${FLOOD_NAMESPACES:-40}
k() { kubectl --context "$KUBE_CONTEXT" "$@"; }
label=kubectl-flink.demo/flood
fixtures=testdata/fixtures/scenarios-operator-1.16.1-flink-2.2

# operator_ignore on|off: whether the operator skips resources with the flood label. Its label selector is read
# only when it starts, so a change restarts it. Charts up to 1.15 also ship the legacy flink-conf.yaml, which Flink
# reads first when it exists; both get the setting.
operator_ignore() {
  local key=kubernetes.operator.label.selector conf
  local files='def files: .key == "config.yaml" or .key == "flink-conf.yaml";'
  conf=$(k get configmap flink-operator-config -n default -o json)
  local has
  # The chart ships the key commented out; only a line that starts with it counts.
  has=$(jq --arg key "$key" '[.data[] | split("\n")[] | select(startswith($key))] | length > 0' <<<"$conf")
  if [[ $1 == off ]]; then
    [[ $has == true ]] || return 0
    conf=$(jq --arg key "$key" "$files"'
      .data |= with_entries(if files then .value |= (split("\n") | map(select(startswith($key) | not)) | join("\n") | sub("\n+$"; "\n")) else . end)' <<<"$conf")
  else
    [[ $has == false ]] || return 0
    conf=$(jq --arg key "$key" --arg sel "!$label" "$files"' .data |= with_entries(if files then .value += "\n\($key): \"\($sel)\"\n" else . end)' <<<"$conf")
  fi
  k replace -f - <<<"$conf" >/dev/null
  k rollout restart deploy/flink-kubernetes-operator -n default >/dev/null
  k rollout status deploy/flink-kubernetes-operator -n default --timeout=180s >/dev/null
}

if [[ ${1:-} == clean ]]; then
  # The jobs must be gone before the operator looks at them again, or it adds its finalizer to each and the
  # namespaces wait on it. A finalizer left by an earlier operator is dropped first; a timeout stops here.
  # shellcheck disable=SC2016 # expanded by the inner sh
  k get flinkdeployments -A -l "$label" -o json | jq -r '.items[] | select(.metadata.finalizers) | .metadata | "\(.namespace) \(.name)"' |
    xargs -P 16 -n 2 sh -c 'kubectl --context "$0" patch flinkdeployment -n "$1" "$2" --type merge -p "{\"metadata\":{\"finalizers\":null}}" >/dev/null' "$KUBE_CONTEXT"
  k delete namespace -l "$label" --ignore-not-found --wait=true --timeout=300s
  operator_ignore off
  exit 0
fi

operator_ignore on
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

# One template per kind of day a job can have: the fixture's spec and status, and where its timestamps go.
jq -n \
  --slurpfile ok testdata/fixtures/operator-1.16.1-flink-2.2/flinkdeployment.json \
  --slurpfile suspended "$fixtures/suspended/flinkdeployment.json" \
  --slurpfile unreachable "$fixtures/unreachable/flinkdeployment.json" \
  --slurpfile missingjar "$fixtures/missing-jar/flinkdeployment.json" \
  --slurpfile badimage "$fixtures/bad-image/flinkdeployment.json" \
  --slurpfile upgrade "$fixtures/bad-memory/flinkdeployment.json" \
  --slurpfile deploying "$fixtures/unschedulable/flinkdeployment.json" \
  --argjson jobs "$jobs" --argjson nss "$nss" --arg label "$label" '
  def domains: ["billing","catalog","checkout","delivery","fraud","inventory","ledger","loyalty","marketing","media",
    "notify","pricing","recommend","search","shipping","support","telemetry","travel","warehouse","weather"];
  def stages: ["ingest","enrich","dedupe","aggregate","score","join","route","archive","alert","sessionize"];
  def ms: now * 1000 | floor;
  # A day out of 400: 10 suspended, 6 restarting, 8 failed, 3 stuck deploying, 3 deploying, the rest healthy.
  def bucket($i): ($i % 400) as $b
    | if $b < 10 then "suspended" elif $b < 16 then "restarting" elif $b < 18 then "unreachable"
      elif $b < 20 then "missingjar" elif $b < 22 then "badimage" elif $b < 24 then "upgrade"
      elif $b < 27 then "stuck" elif $b < 30 then "deploying" else "ok" end;
  def template($b): {ok: $ok, suspended: $suspended, unreachable: $unreachable, missingjar: $missingjar,
    badimage: $badimage, upgrade: $upgrade, stuck: $deploying, deploying: $deploying, restarting: $ok}[$b][0];
  # How long ago the job reconciled and its state began, in minutes, for each kind of day.
  def ago($b; $i): {ok: [60 + ($i * 37 % 4000), 600 + ($i * 53 % 20000)], suspended: [300 + $i % 3000, 300 + $i % 3000],
    restarting: [20 + $i % 40, 20 + $i % 40], unreachable: [90 + $i % 600, 90 + $i % 600],
    missingjar: [45 + $i % 300, 45 + $i % 300], badimage: [30 + $i % 200, 30 + $i % 200],
    upgrade: [15 + $i % 60, 15 + $i % 60], stuck: [25 + $i % 90, 25 + $i % 90], deploying: [1, 1]}[$b];
  [range($jobs) as $i
    | ($i % $nss) as $n
    | (domains[$n % 20] + "-" + (if $n < 20 then "eu" else "us" end)) as $ns
    | domains[$n % 20] as $team
    | bucket($i) as $b | template($b) as $t | ago($b; $i) as $ago
    | (stages[$i % 10] + "-" + (["orders","payments","events","users","items","sessions","devices","invoices"][($i / 10 | floor) % 8])
        + "-" + ($i / $nss | floor | tostring)) as $name
    # Who deploys the namespace: half Argo CD, three in ten Flux, one in ten Helm, one in ten by hand.
    | ($n % 10 | if . < 5 then "argocd" elif . < 8 then "flux" elif . < 9 then "helm" else "kubectl" end) as $owner
    | {apiVersion: "flink.apache.org/v1beta1", kind: "FlinkDeployment",
       metadata: {name: $name, namespace: $ns,
         labels: ({($label): "true", team: $team, "use-case": (1000 + $i | tostring)}
           + if $owner == "flux" then {"kustomize.toolkit.fluxcd.io/name": $team, "kustomize.toolkit.fluxcd.io/namespace": "flux-system"}
             elif $owner == "helm" then {"app.kubernetes.io/managed-by": "Helm", "app.kubernetes.io/instance": $name}
             else {} end),
         annotations: (if $owner == "argocd" then {"argocd.argoproj.io/tracking-id": "\($ns)-jobs:flink.apache.org/FlinkDeployment:\($ns)/\($name)"}
           elif $owner == "helm" then {"meta.helm.sh/release-name": $name, "meta.helm.sh/release-namespace": $ns}
           else {} end)},
       spec: $t.spec,
       status: ($t.status
         | .observedGeneration = 1
         | .reconciliationStatus.reconciliationTimestamp = (ms - $ago[0] * 60000)
         | .jobStatus.updateTime = (ms - $ago[1] * 60000 | tostring)
         | .jobStatus.startTime = (ms - ($ago[1] + 30) * 60000 | tostring)
         | if $b == "restarting" then .jobStatus.state = "RESTARTING" else . end
         # Without TaskManager pods behind them, a desired count would turn every running job DEGRADED.
         | del(.taskManager)
         # A field the operator left null or empty is not valid when applied back.
         | del(.. | select(. == null or . == {}))),
       owner: $owner}]
  | group_by(.owner)[] | {owner: .[0].owner, items: map(del(.owner))}' |
  jq -c '.' > "$tmp/groups.jsonl"

# Flood jobs of an earlier run that this one does not have, such as from other FLOOD_JOBS, are deleted.
jq -r '.items[].metadata | "\(.namespace)/\(.name)"' "$tmp/groups.jsonl" | sort > "$tmp/want"
k get flinkdeployments -A -l "$label" -o jsonpath='{range .items[*]}{.metadata.namespace}/{.metadata.name}{"\n"}{end}' | sort |
  comm -23 - "$tmp/want" | while IFS=/ read -r ns name; do k delete flinkdeployment -n "$ns" "$name" --wait=false >/dev/null; done

# Namespaces first, then each owner's jobs with its own field manager, then the statuses.
jq -r '.items[].metadata.namespace' "$tmp/groups.jsonl" | sort -u |
  jq -R --arg label "$label" '{apiVersion: "v1", kind: "Namespace", metadata: {name: ., labels: {($label): "true"}}}' |
  jq -s '{apiVersion: "v1", kind: "List", items: .}' | k apply --server-side -f - >/dev/null
declare -A manager=([argocd]=argocd-controller [flux]=kustomize-controller [helm]=helm [kubectl]=kubectl)
while read -r group; do
  owner=$(jq -r .owner <<<"$group")
  jq '{apiVersion: "v1", kind: "List", items: [.items[] | del(.status)]}' <<<"$group" > "$tmp/$owner.json"
  jq '{apiVersion: "v1", kind: "List", items: [.items[] | {apiVersion, kind, metadata: {name: .metadata.name, namespace: .metadata.namespace}, status}]}' \
    <<<"$group" > "$tmp/$owner-status.json"
  k apply --server-side --force-conflicts --field-manager "${manager[$owner]}" -f "$tmp/$owner.json" >/dev/null
  k apply --server-side --force-conflicts --field-manager flink-kubernetes-operator --subresource=status -f "$tmp/$owner-status.json" >/dev/null
  echo "$owner: $(jq '.items | length' "$tmp/$owner.json") jobs"
done < "$tmp/groups.jsonl"
