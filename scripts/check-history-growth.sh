#!/usr/bin/env bash

# Enforce a limit on how much a change grows the repository's git history.
#
# Sums the packed on-disk size of every object reachable from <head-rev> but
# not from <base-rev>. The repository is repacked first so new objects are
# delta-compressed against existing history, approximating the real growth
# once the change is merged and the server repacks.
#
# Added after a 68 MB embedded artifact had to be removed from history with a
# force push: large artifacts belong in release assets or external storage
# with a pinned hash, never in git history.
#
# Usage:
#   ./scripts/check-history-growth.sh <base-rev> [head-rev]
#
# Environment:
#   HISTORY_GROWTH_LIMIT_BYTES   Growth limit in bytes (default: 2097152, 2 MiB).
#   HISTORY_GROWTH_SKIP_REPACK   Set to 1 to skip the repack. Faster, but new
#                                objects may not be delta-compressed, which
#                                overestimates growth.

set -euo pipefail

BASE_REV="${1:?usage: check-history-growth.sh <base-rev> [head-rev]}"
HEAD_REV="${2:-HEAD}"
LIMIT_BYTES="${HISTORY_GROWTH_LIMIT_BYTES:-2097152}"

git rev-parse --verify --quiet "${BASE_REV}^{commit}" >/dev/null \
  || { echo "ERROR: base revision '${BASE_REV}' not found" >&2; exit 2; }
git rev-parse --verify --quiet "${HEAD_REV}^{commit}" >/dev/null \
  || { echo "ERROR: head revision '${HEAD_REV}' not found" >&2; exit 2; }

if [ "${HISTORY_GROWTH_SKIP_REPACK:-0}" != "1" ]; then
  git repack -adq
fi

# One line per new object: "<sha>" for commits, "<sha> <path>" for trees and blobs.
objects="$(git rev-list --objects "${HEAD_REV}" --not "${BASE_REV}")"

human() {
  numfmt --to=iec-i --suffix=B "$1" 2>/dev/null || echo "$1 B"
}

if [ -z "${objects}" ]; then
  echo "OK: no new objects; history growth is 0 B (limit $(human "${LIMIT_BYTES}"))."
  exit 0
fi

# "<sha> <type> <size>" per new object.
sizes="$(cut -d' ' -f1 <<<"${objects}" \
  | git cat-file --batch-check='%(objectname) %(objecttype) %(objectsize:disk)')"

total="$(awk '{ sum += $3 } END { print sum }' <<<"${sizes}")"
count="$(wc -l <<<"${sizes}")"

echo "New objects: ${count}; history growth: $(human "${total}") (limit $(human "${LIMIT_BYTES}"))."

if [ "${total}" -le "${LIMIT_BYTES}" ]; then
  exit 0
fi

echo
echo "ERROR: this change grows the git history by $(human "${total}"), over the $(human "${LIMIT_BYTES}") limit."
echo
echo "Largest new blobs (packed size in bytes):"
awk 'NR == FNR { type[$1] = $2; size[$1] = $3; next }
     type[$1] == "blob" {
       path = $0
       sub(/^[0-9a-f]+ ?/, "", path)
       printf "%12d  %s\n", size[$1], (path == "" ? $1 : path)
     }' <(printf '%s\n' "${sizes}") <(printf '%s\n' "${objects}") \
  | sort -rn | head -10
echo
echo "Large files must not be committed to git history: distribute them as"
echo "release assets or external downloads pinned by hash, or shrink the data."
echo "If the growth is intentional, get maintainer sign-off and adjust"
echo "HISTORY_GROWTH_LIMIT_BYTES for this check."
exit 1
