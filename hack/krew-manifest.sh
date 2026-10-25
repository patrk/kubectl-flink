#!/usr/bin/env bash
# Renders .krew.yaml for a published release with the krew-release-bot image, which downloads each archive for its
# sha256: the manifest for the first, manual krew-index submission. Needs docker.
# Usage: hack/krew-manifest.sh v0.1.0 > flink.yaml
set -euo pipefail
tag=${1:?usage: hack/krew-manifest.sh <tag>}
docker run --rm -v "$PWD/.krew.yaml:/tmp/template-file.yaml:ro" ghcr.io/rajatjindal/krew-release-bot:v0.0.51 \
  krew-release-bot template --tag "$tag" --template-file /tmp/template-file.yaml
