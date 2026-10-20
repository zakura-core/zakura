#!/usr/bin/env bash
# Tests scripts/resolve-zcashd-compat-manifest.sh and the compat-zcashd-prepare
# make target against fixture manifests. A fake curl serves fixture files, so
# no network access is needed.
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
resolver="${repo_root}/scripts/resolve-zcashd-compat-manifest.sh"

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

# Fake curl: records its arguments and copies $FAKE_CURL_ROOT/<URL basename>
# to the --output path.
mkdir -p "$work/fake-bin" "$work/served"
cat > "$work/fake-bin/curl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "$FAKE_CURL_LOG"
output=""
url=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --output | -o) output="$2"; shift 2 ;;
    --proto | --proto-redir | --max-redirs | --max-filesize | --max-time) shift 2 ;;
    -*) shift ;;
    *) url="$1"; shift ;;
  esac
done
source="$FAKE_CURL_ROOT/${url##*/}"
if [[ ! -f "$source" ]]; then
  echo "curl: (22) The requested URL returned error: 404" >&2
  exit 22
fi
cp "$source" "$output"
EOF
chmod +x "$work/fake-bin/curl"
export PATH="$work/fake-bin:$PATH"
export FAKE_CURL_ROOT="$work/served"
export FAKE_CURL_LOG="$work/curl.log"
: > "$FAKE_CURL_LOG"

printf '#!/bin/sh\necho "Zcash Daemon version v0.0.0-fixture"\n' > "$work/served/zcashd-fixture"
printf '#!/bin/sh\necho "Zcash Daemon version v0.0.0-override"\n' > "$work/served/zcashd-override"
fixture_sha256="$(sha256sum "$work/served/zcashd-fixture" | awk '{print $1}')"
override_sha256="$(sha256sum "$work/served/zcashd-override" | awk '{print $1}')"

write_manifest() {
  local path="$1"
  local artifacts="$2"
  local schema_version="${3:-2}"
  printf '{"schema_version": %s, "release_tag": "v0.0.0-fixture", "artifacts": [%s]}\n' \
    "$schema_version" "$artifacts" > "$path"
}

artifact() {
  local url="$1"
  local sha256="$2"
  printf '{"target_triple": "x86_64-pc-linux-gnu", "runtime_binary_url": "%s", "runtime_binary_sha256": "%s"}' \
    "$url" "$sha256"
}

fixture_manifest="$work/manifest.json"
write_manifest "$fixture_manifest" "$(artifact https://fixtures.invalid/zcashd-fixture "$fixture_sha256")"

expect_rejected() {
  local description="$1"
  local expected_error="$2"
  shift 2
  local stderr="$work/stderr"
  if "$resolver" "$@" 2> "$stderr" > /dev/null; then
    fail "$description was accepted"
  fi
  grep -q -- "$expected_error" "$stderr" || fail "$description: unexpected error: $(cat "$stderr")"
}

# The committed manifest is valid, and GitHub outputs keep their names.
GITHUB_OUTPUT="$work/github-output" "$resolver" --write-github-output --require-targets x86_64-pc-linux-gnu
committed="${repo_root}/crates/zakurad/zcashd-compat-manifest.json"
expected_outputs="$(printf 'release_tag=%s\nmanifest_path=%s\nurl_amd64=%s\nsha256_amd64=%s' \
  "$(jq -r .release_tag "$committed")" \
  "$committed" \
  "$(jq -r '.artifacts[0].runtime_binary_url' "$committed")" \
  "$(jq -r '.artifacts[0].runtime_binary_sha256' "$committed")")"
[[ "$(cat "$work/github-output")" == "$expected_outputs" ]] \
  || fail "unexpected GitHub outputs: $(cat "$work/github-output")"

# Malformed manifests are rejected.
cat > "$work/schema1.json" <<'EOF'
{
  "schema_version": 1,
  "release_tag": "v1.1.0",
  "artifacts": [{
    "target_triple": "x86_64-pc-linux-gnu",
    "runtime_archive_url": "https://example.com/zcashd.tar.gz",
    "runtime_archive_sha256": "b131e901fb05782e047b9faa593a9da897092915eaa8f7a6cf2438c7634d2f06",
    "runtime_archive_member_binary_path": "./bin/zcashd"
  }]
}
EOF
expect_rejected "schema 1 manifest" "unsupported zcashd compat manifest schema_version" \
  --manifest-path "$work/schema1.json" --write-github-output

write_manifest "$work/archive-fields.json" \
  "$(artifact https://fixtures.invalid/zcashd-fixture "$fixture_sha256" | sed 's/}$/, "runtime_archive_member_binary_path": ".\/bin\/zcashd"}/')"
expect_rejected "schema 2 manifest with archive fields" "malformed zcashd compat manifest" \
  --manifest-path "$work/archive-fields.json" --write-github-output

write_manifest "$work/http.json" "$(artifact http://fixtures.invalid/zcashd-fixture "$fixture_sha256")"
expect_rejected "http manifest URL" "malformed zcashd compat manifest" \
  --manifest-path "$work/http.json" --write-github-output

write_manifest "$work/uppercase.json" \
  "$(artifact https://fixtures.invalid/zcashd-fixture "$(tr a-f A-F <<< "$fixture_sha256")")"
expect_rejected "uppercase SHA-256" "malformed zcashd compat manifest" \
  --manifest-path "$work/uppercase.json" --write-github-output

write_manifest "$work/short.json" "$(artifact https://fixtures.invalid/zcashd-fixture "${fixture_sha256:1}")"
expect_rejected "short SHA-256" "malformed zcashd compat manifest" \
  --manifest-path "$work/short.json" --write-github-output

entry="$(artifact https://fixtures.invalid/zcashd-fixture "$fixture_sha256")"
write_manifest "$work/duplicate.json" "$entry, $entry"
expect_rejected "duplicate targets" "duplicate target triple" \
  --manifest-path "$work/duplicate.json" --write-github-output

expect_rejected "missing required target" "missing required zcashd compat artifact" \
  --manifest-path "$fixture_manifest" --require-targets aarch64-unknown-linux-gnu --write-github-output

# A prepared context holds the verified executable at bin/zcashd and replaces
# the previous context.
context="$work/context"
mkdir -p "$context/bin"
echo stale > "$context/stale-file"
"$resolver" --manifest-path "$fixture_manifest" --platform linux/amd64 --prepare-build-context "$context" > /dev/null
cmp "$context/bin/zcashd" "$work/served/zcashd-fixture" || fail "prepared context has the wrong executable"
[[ -x "$context/bin/zcashd" ]] || fail "prepared executable is not executable"
[[ -n "$(find "$context/bin/zcashd" -perm 0755)" ]] || fail "prepared executable mode is not 0755"
[[ ! -e "$context/stale-file" ]] || fail "previous context contents were kept"
grep -q -- "--proto =https --proto-redir =https --max-redirs 5" "$FAKE_CURL_LOG" \
  || fail "curl was not limited to HTTPS: $(cat "$FAKE_CURL_LOG")"
grep -q -- "--max-filesize" "$FAKE_CURL_LOG" || fail "curl download size was not bounded"

# A checksum mismatch keeps the previous context and leaves no staging files.
write_manifest "$work/mismatch.json" "$(artifact https://fixtures.invalid/zcashd-fixture "$override_sha256")"
expect_rejected "checksum mismatch" "SHA-256 mismatch" \
  --manifest-path "$work/mismatch.json" --target-triple x86_64-pc-linux-gnu --prepare-build-context "$context"
cmp "$context/bin/zcashd" "$work/served/zcashd-fixture" || fail "checksum mismatch replaced the previous executable"
# A failed download also keeps the previous context.
write_manifest "$work/missing.json" "$(artifact https://fixtures.invalid/zcashd-missing "$fixture_sha256")"
expect_rejected "failed download" "404" \
  --manifest-path "$work/missing.json" --target-triple x86_64-pc-linux-gnu --prepare-build-context "$context"
cmp "$context/bin/zcashd" "$work/served/zcashd-fixture" || fail "failed download replaced the previous executable"
if compgen -G "$work/.zcashd-compat-context.*" > /dev/null; then
  fail "failed preparation left staging directories behind"
fi

# URL and checksum overrides replace the manifest values, and must stay HTTPS.
"$resolver" --manifest-path "$fixture_manifest" --target-triple x86_64-pc-linux-gnu \
  --binary-url https://fixtures.invalid/zcashd-override --binary-sha256 "$override_sha256" \
  --prepare-build-context "$context" > /dev/null
cmp "$context/bin/zcashd" "$work/served/zcashd-override" || fail "URL override was not used"
: > "$FAKE_CURL_LOG"
expect_rejected "http override URL" "must use https" \
  --manifest-path "$fixture_manifest" --target-triple x86_64-pc-linux-gnu \
  --binary-url http://fixtures.invalid/zcashd-override --binary-sha256 "$override_sha256" \
  --prepare-build-context "$context"
[[ ! -s "$FAKE_CURL_LOG" ]] || fail "http override URL was downloaded"
cmp "$context/bin/zcashd" "$work/served/zcashd-override" || fail "rejected override replaced the previous executable"

# compat-zcashd-prepare uses the manifest, its URL and checksum overrides, or a
# caller-provided build context.
run_make() {
  make --no-print-directory -s -C "$repo_root" -f scripts/make/zcashd-compat.mk \
    ZCASHD_COMPAT_MANIFEST="$fixture_manifest" "$@" compat-zcashd-prepare > /dev/null
}

run_make ZCASHD_COMPAT_CONTEXT_DIR="$work/make-context"
cmp "$work/make-context/bin/zcashd" "$work/served/zcashd-fixture" || fail "make did not prepare the manifest executable"

run_make ZCASHD_COMPAT_CONTEXT_DIR="$work/make-context" \
  ZCASHD_COMPAT_URL=https://fixtures.invalid/zcashd-override ZCASHD_COMPAT_SHA256="$override_sha256"
cmp "$work/make-context/bin/zcashd" "$work/served/zcashd-override" || fail "make did not use the URL override"

if run_make ZCASHD_COMPAT_CONTEXT_DIR="$work/make-context" ZCASHD_COMPAT_SHA256="$fixture_sha256" \
  ZCASHD_COMPAT_URL=https://fixtures.invalid/zcashd-override 2> /dev/null; then
  fail "make accepted a checksum mismatch"
fi
cmp "$work/make-context/bin/zcashd" "$work/served/zcashd-override" || fail "make checksum mismatch replaced the executable"

: > "$FAKE_CURL_LOG"
run_make ZCASHD_COMPAT_BUILD_CONTEXT="$context"
[[ ! -s "$FAKE_CURL_LOG" ]] || fail "make downloaded despite ZCASHD_COMPAT_BUILD_CONTEXT"
if run_make ZCASHD_COMPAT_BUILD_CONTEXT="$work/empty-context" 2> /dev/null; then
  fail "make accepted a build context without bin/zcashd"
fi

echo "resolve-zcashd-compat-manifest tests passed"
