# shellcheck shell=bash
# Semver requirement matching for workspace-internal dependency requirements.
#
# Supports exactly the requirement grammar this workspace writes: a bare or
# caret-prefixed full version, "X.Y.Z" or "X.Y.Z-PRE" (cargo's default caret
# semantics). Anything else is reported as unsupported so callers fail closed
# instead of guessing.
#
# The rules that matter for release preparation, and that this file encodes:
#   - a requirement without a pre-release tag never matches a pre-release
#     version ("^3.0.0" cannot select 3.1.0-rc0);
#   - a requirement with a pre-release tag matches pre-releases only on the
#     same X.Y.Z ("^3.1.0-rc0" matches 3.1.0-rc1 but not 3.2.0-rc0);
#   - otherwise caret compatibility applies (same leftmost non-zero
#     component, at or above the lower bound).
#
# Sourced by prepare-release.sh; unit tests are in
# scripts/tests/test_semver_req.py.

# semver_parse VERSION X Y Z PRE
# Parse VERSION into the named variables (PRE may be empty). Returns 1 when
# VERSION is not a full X.Y.Z[-PRE] semver version.
semver_parse() {
  local _v="$1"
  if [[ ! "$_v" =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)(-([0-9A-Za-z.-]+))?$ ]]; then
    return 1
  fi
  printf -v "$2" '%s' "${BASH_REMATCH[1]}"
  printf -v "$3" '%s' "${BASH_REMATCH[2]}"
  printf -v "$4" '%s' "${BASH_REMATCH[3]}"
  printf -v "$5" '%s' "${BASH_REMATCH[5]}"
}

# semver_pre_cmp A B
# Compare two non-empty pre-release strings by semver precedence and echo
# -1, 0, or 1. Dot-separated identifiers: numeric identifiers compare
# numerically and order below alphanumeric ones; alphanumeric identifiers
# compare as ASCII strings; a strict identifier prefix orders first.
# (ASCII order means "rc10" < "rc2" — semver's documented behavior for
# alphanumeric identifiers. Use "rc.10"-style numeric identifiers if a line
# ever needs more than ten release candidates.)
semver_pre_cmp() {
  local LC_ALL=C
  local -a a b
  IFS=. read -ra a <<<"$1"
  IFS=. read -ra b <<<"$2"
  local i ai bi
  for ((i = 0; i < ${#a[@]} && i < ${#b[@]}; i++)); do
    ai="${a[i]}" bi="${b[i]}"
    if [[ "$ai" =~ ^[0-9]+$ && "$bi" =~ ^[0-9]+$ ]]; then
      if ((10#$ai < 10#$bi)); then echo -1; return; fi
      if ((10#$ai > 10#$bi)); then echo 1; return; fi
    elif [[ "$ai" =~ ^[0-9]+$ ]]; then
      echo -1; return
    elif [[ "$bi" =~ ^[0-9]+$ ]]; then
      echo 1; return
    else
      if [[ "$ai" < "$bi" ]]; then echo -1; return; fi
      if [[ "$ai" > "$bi" ]]; then echo 1; return; fi
    fi
  done
  if ((${#a[@]} < ${#b[@]})); then echo -1; return; fi
  if ((${#a[@]} > ${#b[@]})); then echo 1; return; fi
  echo 0
}

# semver_req_matches REQ VERSION
# Whether VERSION satisfies REQ under cargo's default caret semantics.
# Returns 0 (matches), 1 (does not match), or 2 (unsupported grammar,
# with a message on stderr).
semver_req_matches() {
  local req="${1#^}" ver="$2"
  local rx ry rz rpre vx vy vz vpre
  if ! semver_parse "$req" rx ry rz rpre; then
    echo "semver_req_matches: unsupported requirement '$1' (expected [^]X.Y.Z[-PRE])" >&2
    return 2
  fi
  if ! semver_parse "$ver" vx vy vz vpre; then
    echo "semver_req_matches: unsupported version '$2' (expected X.Y.Z[-PRE])" >&2
    return 2
  fi

  if [ -n "$vpre" ]; then
    # Pre-release versions match only a requirement carrying a pre-release
    # on the same X.Y.Z, at or above the requirement's pre-release.
    [ -n "$rpre" ] || return 1
    [ "$vx.$vy.$vz" = "$rx.$ry.$rz" ] || return 1
    [ "$(semver_pre_cmp "$vpre" "$rpre")" -ge 0 ] && return 0 || return 1
  fi

  # Stable version below the requirement's core: never matches. On an equal
  # core, a stable version satisfies any pre-release lower bound.
  if ((vx < rx)) \
    || ((vx == rx && vy < ry)) \
    || ((vx == rx && vy == ry && vz < rz)); then
    return 1
  fi

  # Caret upper bound: the leftmost non-zero component must stay equal.
  if ((rx > 0)); then
    ((vx == rx)) && return 0 || return 1
  elif ((ry > 0)); then
    ((vx == 0 && vy == ry)) && return 0 || return 1
  else
    ((vx == 0 && vy == 0 && vz == rz)) && return 0 || return 1
  fi
}
