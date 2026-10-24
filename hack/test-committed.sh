#!/usr/bin/env bash
# Runs the tests on exactly the files git would commit: tracked and untracked files, minus everything ignored,
# global ignore files included. A fixture that exists only locally fails here instead of in CI.
set -euo pipefail
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
git ls-files -z --cached --others --exclude-standard | while IFS= read -r -d '' f; do
  [[ -e $f ]] || continue # deleted but not yet staged
  mkdir -p "$tmp/$(dirname "$f")"
  cp -p "$f" "$tmp/$f"
done
missing=$(comm -23 <(find . -path ./bin -prune -o -path '*/testdata/*' -type f -print | sort) \
  <(cd "$tmp" && find . -path '*/testdata/*' -type f -print | sort))
if [[ -n $missing ]]; then
  echo "testdata files git would not commit (check .gitignore and your global ignore file):" >&2
  echo "$missing" >&2
  exit 1
fi
cd "$tmp" && go test ./...
