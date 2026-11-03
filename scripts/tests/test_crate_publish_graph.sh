#!/usr/bin/env bash
# Exercise the merge gate without cargo builds or requests to crates.io.
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
fixture="$(mktemp -d)"
trap 'rm -rf "$fixture"' EXIT
export fixture
mkdir -p "$fixture/bin" "$fixture/repo"
touch "$fixture/repo/Cargo.toml"

cat >"$fixture/metadata.json" <<'EOF'
{"packages":[
  {"name":"new-crate","version":"0.2.0","publish":null,"dependencies":[]},
  {"name":"private-tool","version":"0.1.0","publish":[],"dependencies":[]}
]}
EOF

cat >"$fixture/bin/cargo" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
case "$1" in
  metadata) cat "$fixture/metadata.json" ;;
  publish)
    touch "$fixture/publish-called"
    mkdir -p "$CARGO_TARGET_DIR/package" "$fixture/new-crate-0.2.0"
    printf '[[package]]\nname = "new-crate"\nversion = "0.2.0"\n' \
      >"$fixture/new-crate-0.2.0/Cargo.lock"
    tar -czf "$CARGO_TARGET_DIR/package/new-crate-0.2.0.crate" \
      -C "$fixture" new-crate-0.2.0
    ;;
  *) exit 2 ;;
esac
EOF

cat >"$fixture/bin/curl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
url="${*: -1}"
if [[ "$url" != */ne/w-/new-crate ]]; then
  echo "unexpected index query: $url" >&2
  exit 2
fi
while [ "$#" -gt 0 ]; do
  if [ "$1" = -o ]; then
    cp "$fixture/index.json" "$2"
    shift 2
  else
    shift
  fi
done
printf '%s' "$index_status"
EOF
chmod +x "$fixture/bin/cargo" "$fixture/bin/curl"

export PATH="$fixture/bin:$PATH"
export ZAKURA_REPO_ROOT="$fixture/repo"
export CRATES_INDEX_URL="https://fixture.invalid"
export index_status
# Isolate the cases from overrides in the calling shell.
export ZAKURA_ALLOW_UNPUBLISHABLE_CRATE_GRAPH=0
export ZAKURA_ALLOW_OVERSIZED_VERSION_BUMP=0

run_check() {
  rm -f "$fixture/publish-called"
  result=0
  bash "$repo_root/scripts/check-crate-publish-graph.sh" \
    >"$fixture/output" 2>&1 || result=$?
}

assert_result() {
  if [ "$result" -ne "$1" ]; then
    cat "$fixture/output" >&2
    echo "expected exit $1, got $result" >&2
    exit 1
  fi
}

# An absent crate name fails before the dry-run, even for a GitHub-only release.
index_status=404
: >"$fixture/index.json"
for override in 0 1; do
  export ZAKURA_ALLOW_UNPUBLISHABLE_CRATE_GRAPH="$override"
  run_check
  assert_result 1
  grep -Fq 'ERROR: new-crate has never been published' "$fixture/output"
  [ ! -e "$fixture/publish-called" ]
done
export ZAKURA_ALLOW_UNPUBLISHABLE_CRATE_GRAPH=0

# A registry outage must not turn into a pass or an absent-name diagnosis.
index_status=503
run_check
assert_result 1
grep -Fq 'could not query the crates.io index' "$fixture/output"
[ ! -e "$fixture/publish-called" ]

# An existing crate with a pending version bump still runs the graph check.
index_status=200
printf '{"vers":"0.1.0","deps":[]}\n' >"$fixture/index.json"
run_check
assert_result 0
[ -e "$fixture/publish-called" ]
grep -Fq 'Publish graph resolves at workspace versions: new-crate@0.2.0' "$fixture/output"

# Already-published versions pass without publishing. The private package never
# reaches the index mock, which rejects any request except for new-crate.
printf '{"vers":"0.2.0","deps":[]}\n' >"$fixture/index.json"
run_check
assert_result 0
[ ! -e "$fixture/publish-called" ]
grep -Fq 'Every publishable crate is already on the index' "$fixture/output"

echo 'crate publication gate tests passed'
