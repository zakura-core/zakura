# shellcheck shell=bash
# Sparse crates.io index queries for the release scripts.
#
# The sparse index (https://index.crates.io) serves one JSON line per
# published version, including yanked versions and each version's dependency
# requirements. It is the authority on what a published crate's manifest
# actually pins — the local manifest of an already-published crate can
# diverge (for example when cargo-release rewrites a requirement without a
# version bump).
#
# All functions cache responses for the lifetime of the sourcing process.
# Transport failures (anything but HTTP 200/404) return status 2 so callers
# can distinguish "not published" from "could not check".

CRATES_INDEX_URL="${CRATES_INDEX_URL:-https://index.crates.io}"

_crates_index_cache_dir=""

# crates_index_path NAME — echo the index path segment for a crate name.
crates_index_path() {
  local name="${1,,}"
  case "${#name}" in
    1) printf '1/%s' "$name" ;;
    2) printf '2/%s' "$name" ;;
    3) printf '3/%s/%s' "${name:0:1}" "$name" ;;
    *) printf '%s/%s/%s' "${name:0:2}" "${name:2:2}" "$name" ;;
  esac
}

# crates_index_fetch NAME — echo the path of a cached file holding the
# crate's raw index lines. The file is empty when the crate has never been
# published (HTTP 404). Returns 2 on transport failure.
crates_index_fetch() {
  local name="$1"
  if [ -z "$_crates_index_cache_dir" ]; then
    _crates_index_cache_dir="$(mktemp -d "${TMPDIR:-/tmp}/crates-index.XXXXXX")"
  fi
  local cache="${_crates_index_cache_dir}/${name}"
  if [ ! -e "$cache" ]; then
    local url code
    url="${CRATES_INDEX_URL}/$(crates_index_path "$name")"
    code="$(curl -sS --retry 4 --retry-connrefused -o "${cache}.tmp" \
      -w '%{http_code}' "$url")" || {
      echo "crates_index_fetch: could not reach ${url}" >&2
      rm -f "${cache}.tmp"
      return 2
    }
    case "$code" in
      200) mv "${cache}.tmp" "$cache" ;;
      404) : >"$cache"; rm -f "${cache}.tmp" ;;
      *)
        echo "crates_index_fetch: ${url} returned HTTP ${code}" >&2
        rm -f "${cache}.tmp"
        return 2
        ;;
    esac
  fi
  printf '%s\n' "$cache"
}

# crates_index_forget NAME — drop NAME's cached response so the next query
# refetches it. Only polling callers need this: the index is the moving part
# they are waiting on, while every other caller wants one stable answer for
# the lifetime of the process.
crates_index_forget() {
  [ -n "$_crates_index_cache_dir" ] || return 0
  rm -f "${_crates_index_cache_dir}/${1}"
}

# crates_index_versions NAME — echo every published version of NAME, one per
# line (yanked included: the version number stays taken), or nothing when
# the crate has never been published. Returns 2 on transport failure.
crates_index_versions() {
  local cache
  cache="$(crates_index_fetch "$1")" || return 2
  jq -r '.vers' <"$cache"
}

# crates_index_has_version NAME VERSION — 0 when NAME@VERSION exists on the
# index, 1 when it does not, 2 on transport failure.
crates_index_has_version() {
  local versions
  versions="$(crates_index_versions "$1")" || return 2
  grep -Fxq -- "$2" <<<"$versions"
}

# crates_index_deps NAME VERSION — echo the published dependency
# requirements of NAME@VERSION as "name<TAB>req<TAB>kind" lines (kind is
# normal, build, or dev; renamed dependencies report their real package
# name). Nothing when the version is not published. Returns 2 on transport
# failure.
crates_index_deps() {
  local cache
  cache="$(crates_index_fetch "$1")" || return 2
  jq -r --arg v "$2" '
    select(.vers == $v)
    | .deps[]
    | [(.package // .name), .req, (.kind // "normal")]
    | @tsv
  ' <"$cache"
}
