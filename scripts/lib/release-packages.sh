#!/usr/bin/env bash

# Print package name, version, and manifest path for every package at a git
# revision. Package identity is independent of repository layout.
list_release_packages_at() {
  local revision="$1"
  local manifest_rel package

  while IFS= read -r manifest_rel; do
    case "$manifest_rel" in
      Cargo.toml|*/Cargo.toml) ;;
      *) continue ;;
    esac

    package="$(
      git show "${revision}:${manifest_rel}" \
        | awk -F ' *= *' '
            $0 == "[package]" { in_package = 1; next }
            in_package && /^\[/ { exit }
            in_package && $1 == "name" {
              gsub(/"/, "", $2)
              name = $2
            }
            in_package && $1 == "version" {
              gsub(/"/, "", $2)
              version = $2
            }
            END {
              if (name != "" && version != "") {
                print name "\t" version
              }
            }
          '
    )"
    [ -n "$package" ] || continue
    printf '%s\t%s\n' "$package" "$manifest_rel"
  done < <(git ls-tree -r --name-only "$revision")
}
