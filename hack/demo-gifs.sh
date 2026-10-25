#!/usr/bin/env bash
# Records the README's tapes twice, on GitHub's dark and light page colours, so that the README can show the one
# that matches the reader's theme: docs/demo/<name>.gif (dark) and docs/demo/<name>-light.gif. VHS cannot make the
# margin transparent, and GIF transparency would fray the window's rounded corners.
# Usage: hack/demo-gifs.sh [tape...], from the repository root after make build demo.
set -euo pipefail
tapes=("$@")
[[ ${#tapes[@]} -gt 0 ]] || tapes=(ls why top ids classic crashloop graph)
# VHS takes only relative paths after Source, so the variants are written next to the tapes.
work=docs/demo/.work
mkdir -p "$work"
trap 'rm -rf "$work"' EXIT
for name in "${tapes[@]}"; do
  for theme in dark light; do
    look=docs/demo/look.tape out=docs/demo/$name.gif
    if [[ $theme == light ]]; then
      look=$work/look-light.tape out=docs/demo/$name-light.gif
      sed 's|^Set MarginFill .*|Set MarginFill "#ffffff"|' docs/demo/look.tape > "$look"
    fi
    sed -e "s|^Output .*|Output $out|" -e "s|^Source docs/demo/look.tape$|Source $look|" "docs/demo/$name.tape" > "$work/$name.tape"
    echo "recording $out"
    vhs "$work/$name.tape" >/dev/null
  done
done
