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

# Rewrite every requirement for a workspace dependency to an explicit
# prerelease target. Stable requirements never select prerelease versions, even
# when they select the corresponding stable package version, so any requirement
# left behind fails the release run at the next `cargo metadata`.
#
# Covers the spellings the workspace uses: an inline table keyed by the crate
# name, an inline table that renames the crate through `package = "<crate>"`,
# and a bare version string. Renaming is impossible in the bare-string form, so
# that case matches on the key alone.
rewrite_prerelease_dependency_requirements() {
  local manifest="$1" crate="$2" target="$3"

  CRATE="$crate" TARGET="$target" perl -0777 -pi -e '
    my ($crate, $target) = ($ENV{CRATE}, $ENV{TARGET});

    # Inline tables. The body stops at the first closing brace, so it cannot
    # run past its own entry, and it may span lines (a wrapped feature list).
    s{^([ \t]*(?:"\Q$crate\E"|[A-Za-z0-9_.-]+))(\s*=\s*\{)([^\}]*)(\})}{
      my ($key, $sep, $body, $close) = ($1, $2, $3, $4);
      my $name = $key;
      $name =~ s/^[ \t]*//;
      $name =~ s/^"|"$//g;
      if ($name eq $crate || $body =~ /package\s*=\s*"\Q$crate\E"/) {
        $body =~ s/(version\s*=\s*")[^"]*(")/$1 . $target . $2/e;
      }
      $key . $sep . $body . $close;
    }gme;

    # Bare version string.
    s/^([ \t]*\Q$crate\E\s*=\s*")[^"]*(")/$1 . $target . $2/gme;
  ' "$manifest"
}
