#!/usr/bin/env bash
# Records the demo tapes twice, on GitHub's dark and light page colours, into docs/demo/out: <name>.gif (dark) and
# <name>-light.gif, plus highlights.mp4. The recordings are not kept in git; they are uploaded to the "demo" release,
# which the README and docs/demos.md link to. VHS cannot make the margin transparent, and GIF transparency would
# fray the window's rounded corners, hence two variants.
# Usage: hack/demo-gifs.sh [tape...], from the repository root after make build demo.
set -euo pipefail
tapes=("$@")
[[ ${#tapes[@]} -gt 0 ]] || tapes=(highlights ls why top ids classic crashloop graph)
out=docs/demo/out
# VHS takes only relative paths after Source, so the variants are written next to the tapes.
work=docs/demo/.work
mkdir -p "$work" "$out"
trap 'rm -rf "$work"' EXIT
for name in "${tapes[@]}"; do
  for theme in dark light; do
    look=docs/demo/look.tape suffix= fill='#0d1117'
    if [[ $theme == light ]]; then
      look=$work/look-light.tape suffix=-light fill='#ffffff'
      sed "s|^Set MarginFill .*|Set MarginFill \"$fill\"|" docs/demo/look.tape > "$look"
    fi
    # The light pass writes <name>-light.gif and skips the MP4.
    sed -E -e "s#^Output docs/demo/out/([a-z]+)\.(gif|mp4)\$#Output $out/\1$suffix.\2#" \
      -e "s|^Set MarginFill .*|Set MarginFill \"$fill\"|" \
      -e "s#^Source docs/demo/look.tape\$#Source $look#" "docs/demo/$name.tape" > "$work/$name.tape"
    [[ $theme == dark ]] || sed -i.bak '/\.mp4$/d' "$work/$name.tape"
    echo "recording $name ($theme)"
    vhs "$work/$name.tape" >/dev/null
  done
done
echo "upload with: gh release upload demo $out/* --clobber"
