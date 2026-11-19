#!/usr/bin/env bash

# Enforce a limit on how much a change grows the repository's git history.
#
# Measures a thin pack of every object reachable from <head-rev> but not from
# <base-rev>: the bytes a push of the change sends. A thin pack stores each
# new object as a delta against <base-rev>'s objects where it can, which is
# what the change adds once the server repacks. An object with no base, such
# as a new artifact, counts at its whole compressed size.
#
# The sum of the new objects' on-disk sizes after a repack is not a stable
# measure: a repack may store a new version of a large file whole and its old
# version as a delta, which charges the change for the whole file.
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

set -euo pipefail

BASE_REV="${1:?usage: check-history-growth.sh <base-rev> [head-rev]}"
HEAD_REV="${2:-HEAD}"
LIMIT_BYTES="${HISTORY_GROWTH_LIMIT_BYTES:-2097152}"

git rev-parse --verify --quiet "${BASE_REV}^{commit}" >/dev/null \
  || { echo "ERROR: base revision '${BASE_REV}' not found" >&2; exit 2; }
git rev-parse --verify --quiet "${HEAD_REV}^{commit}" >/dev/null \
  || { echo "ERROR: head revision '${HEAD_REV}' not found" >&2; exit 2; }

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

total="$(printf '%s\n^%s\n' "${HEAD_REV}" "${BASE_REV}" \
  | git pack-objects --revs --thin --stdout -q | wc -c)"
count="$(wc -l <<<"${sizes}")"

echo "New objects: ${count}; history growth: $(human "${total}") (limit $(human "${LIMIT_BYTES}"))."

if [ "${total}" -le "${LIMIT_BYTES}" ]; then
  exit 0
fi

echo
echo "ERROR: this change grows the git history by $(human "${total}"), over the $(human "${LIMIT_BYTES}") limit."
echo
echo "Largest new blobs (compressed size in bytes, before deltas):"
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
