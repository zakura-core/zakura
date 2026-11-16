#!/usr/bin/env bash
# Audit every dependency group for the platforms used by CI and developers.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

uv lock --check --no-build

status=0
for target in \
  x86_64-unknown-linux-gnu \
  aarch64-unknown-linux-gnu \
  x86_64-apple-darwin \
  aarch64-apple-darwin; do
  printf '\nAuditing Python 3.12 on %s (all dependency groups)\n' "$target"
  if uv audit --locked --no-build --python-version 3.12 --python-platform "$target"; then
    :
  else
    status=1
  fi
done
exit "$status"
