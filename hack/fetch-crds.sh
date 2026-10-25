#!/usr/bin/env bash
# Stores the FlinkDeployment and FlinkSessionJob CRDs of each operator version, gzipped, for the schema test
# that checks every field the tool reads still exists upstream.
set -euo pipefail

VERSIONS=${VERSIONS:-"1.15.0 1.16.1"}
OUT=${OUT:-testdata/crds}
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

for v in $VERSIONS; do
  helm pull --repo "https://archive.apache.org/dist/flink/flink-kubernetes-operator-$v/" \
    flink-kubernetes-operator --version "$v" --untar -d "$tmp/$v" >/dev/null
  mkdir -p "$OUT/operator-$v"
  for crd in flinkdeployments flinksessionjobs flinkstatesnapshots; do
    gzip -9 -n -c "$tmp/$v/flink-kubernetes-operator/crds/$crd.flink.apache.org-v1.yml" >"$OUT/operator-$v/$crd.yml.gz"
  done
  echo "operator $v: $(ls "$OUT/operator-$v" | tr '\n' ' ')"
done
