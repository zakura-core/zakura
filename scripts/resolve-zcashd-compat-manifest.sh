#!/usr/bin/env bash
# Resolve the hash-pinned zcashd compat executable from crates/zakurad/zcashd-compat-manifest.json.
set -euo pipefail

DEFAULT_MANIFEST="crates/zakurad/zcashd-compat-manifest.json"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MANIFEST_SCHEMA_VERSION=2
# Download limits: the pinned executable is about 70 MiB.
MAX_DOWNLOAD_BYTES=$((512 * 1024 * 1024))
MAX_DOWNLOAD_SECONDS=600
MAX_REDIRECTS=5

MANIFEST_PATH=""
TARGET_TRIPLE=""
DOCKER_PLATFORM=""
CONTEXT_DIR=""
WRITE_GITHUB_OUTPUT=0
REQUIRE_TARGETS=""
BINARY_URL=""
BINARY_SHA256=""

usage() {
  cat <<'EOF'
Usage: resolve-zcashd-compat-manifest.sh [options]

Reads the committed zcashd compat manifest and either exports GitHub Actions
outputs or prepares a Docker build context containing bin/zcashd.

Options:
  --manifest-path PATH       Manifest JSON path (default: crates/zakurad/zcashd-compat-manifest.json)
  --target-triple TRIPLE     Rust-style target triple
  --platform PLATFORM        Docker platform (linux/amd64)
  --require-targets LIST     Comma-separated target triples that must be present
  --write-github-output      Write release_tag/manifest_path/url_amd64/sha256_amd64
  --prepare-build-context DIR
                             Download and verify the standalone zcashd executable,
                             then replace DIR with a context holding it at bin/zcashd
  --binary-url URL           Override the manifest executable URL (HTTPS only)
  --binary-sha256 SHA256     Override the manifest executable SHA-256
  -h, --help                 Show this help
EOF
}

abs_manifest_path() {
  if [[ -n "$MANIFEST_PATH" ]]; then
    if [[ "$MANIFEST_PATH" = /* ]]; then
      echo "$MANIFEST_PATH"
    else
      echo "$REPO_ROOT/$MANIFEST_PATH"
    fi
  else
    echo "$REPO_ROOT/$DEFAULT_MANIFEST"
  fi
}

platform_to_target_triple() {
  case "$1" in
    linux/amd64) echo "x86_64-pc-linux-gnu" ;;
    *)
      echo "unsupported Docker platform for zcashd compat artifacts: $1" >&2
      exit 1
      ;;
  esac
}

target_to_github_prefix() {
  case "$1" in
    x86_64-pc-linux-gnu) echo "amd64" ;;
    *)
      echo "unsupported target triple for GitHub Actions outputs: $1" >&2
      exit 1
      ;;
  esac
}

validate_manifest() {
  local manifest="$1"

  if [[ ! -f "$manifest" ]]; then
    echo "zcashd compat manifest not found: $manifest" >&2
    exit 1
  fi

  if ! jq -e --argjson version "$MANIFEST_SCHEMA_VERSION" '.schema_version == $version' "$manifest" >/dev/null; then
    echo "unsupported zcashd compat manifest schema_version in $manifest (expected $MANIFEST_SCHEMA_VERSION)" >&2
    exit 1
  fi

  if ! jq -e '
    (keys == ["artifacts", "release_tag", "schema_version"])
    and (.release_tag | type == "string" and length > 0)
    and (.artifacts | type == "array" and length > 0)
    and all(.artifacts[];
      type == "object"
      and (keys == ["runtime_binary_sha256", "runtime_binary_url", "target_triple"])
      and (.target_triple | type == "string" and length > 0)
      and (.runtime_binary_url | type == "string" and startswith("https://"))
      and (.runtime_binary_sha256 | type == "string" and test("^[0-9a-f]{64}$")))
  ' "$manifest" >/dev/null; then
    echo "malformed zcashd compat manifest: $manifest" >&2
    echo "each artifact needs only target_triple, an https runtime_binary_url and a lowercase hex runtime_binary_sha256" >&2
    exit 1
  fi

  local unique_targets
  unique_targets="$(jq -r '.artifacts[].target_triple' "$manifest" | sort -u | wc -l | tr -d ' ')"
  local total_targets
  total_targets="$(jq -r '.artifacts | length' "$manifest")"
  if [[ "$unique_targets" != "$total_targets" ]]; then
    echo "duplicate target triple found in zcashd compat manifest: $manifest" >&2
    exit 1
  fi
}

artifact_field() {
  local manifest="$1"
  local target_triple="$2"
  local field="$3"

  jq -er --arg target "$target_triple" --arg field "$field" '
    .artifacts[]
    | select(.target_triple == $target)
    | .[$field]
  ' "$manifest"
}

require_targets_present() {
  local manifest="$1"
  local missing=0
  local triple

  IFS=',' read -ra required <<< "$REQUIRE_TARGETS"
  for triple in "${required[@]}"; do
    triple="${triple// /}"
    if [[ -z "$triple" ]]; then
      continue
    fi

    if ! jq -e --arg target "$triple" '
      [.artifacts[] | select(.target_triple == $target)] | length == 1
    ' "$manifest" >/dev/null; then
      echo "missing required zcashd compat artifact for target triple: $triple" >&2
      missing=1
    fi
  done

  if [[ "$missing" -ne 0 ]]; then
    exit 1
  fi
}

write_github_output() {
  local manifest="$1"
  local output_file="${GITHUB_OUTPUT:-}"

  if [[ -z "$output_file" ]]; then
    echo "GITHUB_OUTPUT is required for --write-github-output" >&2
    exit 1
  fi

  {
    echo "release_tag=$(jq -r '.release_tag' "$manifest")"
    echo "manifest_path=$manifest"
  } >> "$output_file"

  local triple="x86_64-pc-linux-gnu" prefix url sha256
  if jq -e --arg target "$triple" '
    [.artifacts[] | select(.target_triple == $target)] | length == 1
  ' "$manifest" >/dev/null; then
    prefix="$(target_to_github_prefix "$triple")"
    url="$(artifact_field "$manifest" "$triple" "runtime_binary_url")"
    sha256="$(artifact_field "$manifest" "$triple" "runtime_binary_sha256")"
    {
      echo "url_${prefix}=$url"
      echo "sha256_${prefix}=$sha256"
    } >> "$output_file"
  fi
}

# Downloads URL to DEST over HTTPS only, including redirects, and checks its
# SHA-256 before making it executable.
download_verified_binary() {
  local url="$1"
  local sha256="$2"
  local dest="$3"
  local actual

  if [[ "$url" != https://* ]]; then
    echo "zcashd compat executable URL must use https: $url" >&2
    exit 1
  fi
  if [[ ! "$sha256" =~ ^[0-9a-f]{64}$ ]]; then
    echo "zcashd compat executable SHA-256 must be 64 lowercase hex characters: $sha256" >&2
    exit 1
  fi

  curl --fail --silent --show-error --location \
    --proto '=https' --proto-redir '=https' --max-redirs "$MAX_REDIRECTS" \
    --max-filesize "$MAX_DOWNLOAD_BYTES" --max-time "$MAX_DOWNLOAD_SECONDS" \
    --output "$dest" "$url"

  actual="$(sha256sum "$dest" | awk '{print $1}')"
  if [[ "$actual" != "$sha256" ]]; then
    echo "zcashd compat executable SHA-256 mismatch for $url: expected $sha256, got $actual" >&2
    exit 1
  fi
  chmod 0755 "$dest"
}

prepare_build_context() {
  local manifest="$1"
  local target_triple="$2"
  local context_dir="$3"
  local url sha256 parent_dir staged_context

  url="${BINARY_URL:-$(artifact_field "$manifest" "$target_triple" "runtime_binary_url")}"
  sha256="${BINARY_SHA256:-$(artifact_field "$manifest" "$target_triple" "runtime_binary_sha256")}"

  # Stage the new context next to DIR, so a failed download or check leaves an
  # existing DIR untouched and the final rename stays on one filesystem.
  context_dir="${context_dir%/}"
  parent_dir="$(dirname "$context_dir")"
  mkdir -p "$parent_dir"
  STAGING_DIR="$(mktemp -d "$parent_dir/.zcashd-compat-context.XXXXXX")"
  trap 'rm -rf "$STAGING_DIR"' EXIT
  staged_context="$STAGING_DIR/context"
  mkdir -p "$staged_context/bin"

  download_verified_binary "$url" "$sha256" "$staged_context/bin/zcashd"

  # The executable only runs on its own platform; other hosts may still prepare
  # a Docker context for it.
  if [[ "$target_triple" == "x86_64-pc-linux-gnu" && "$(uname -sm)" == "Linux x86_64" ]]; then
    "$staged_context/bin/zcashd" --version
  fi

  rm -rf "$context_dir"
  mv "$staged_context" "$context_dir"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --manifest-path)
      MANIFEST_PATH="$2"
      shift 2
      ;;
    --target-triple)
      TARGET_TRIPLE="$2"
      shift 2
      ;;
    --platform)
      DOCKER_PLATFORM="$2"
      shift 2
      ;;
    --require-targets)
      REQUIRE_TARGETS="$2"
      shift 2
      ;;
    --write-github-output)
      WRITE_GITHUB_OUTPUT=1
      shift
      ;;
    --prepare-build-context)
      CONTEXT_DIR="$2"
      shift 2
      ;;
    --binary-url)
      BINARY_URL="$2"
      shift 2
      ;;
    --binary-sha256)
      BINARY_SHA256="$2"
      shift 2
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

manifest="$(abs_manifest_path)"
validate_manifest "$manifest"

if [[ -n "$REQUIRE_TARGETS" ]]; then
  require_targets_present "$manifest"
fi

if [[ -n "$DOCKER_PLATFORM" ]]; then
  if [[ -n "$TARGET_TRIPLE" && "$TARGET_TRIPLE" != "$(platform_to_target_triple "$DOCKER_PLATFORM")" ]]; then
    echo "conflicting --platform and --target-triple arguments" >&2
    exit 1
  fi
  TARGET_TRIPLE="$(platform_to_target_triple "$DOCKER_PLATFORM")"
fi

if [[ "$WRITE_GITHUB_OUTPUT" -eq 1 ]]; then
  write_github_output "$manifest"
fi

if [[ -n "$CONTEXT_DIR" ]]; then
  if [[ -z "$TARGET_TRIPLE" ]]; then
    echo "--prepare-build-context requires --target-triple or --platform" >&2
    exit 1
  fi
  prepare_build_context "$manifest" "$TARGET_TRIPLE" "$CONTEXT_DIR"
fi

if [[ "$WRITE_GITHUB_OUTPUT" -eq 0 && -z "$CONTEXT_DIR" ]]; then
  echo "no action requested; pass --write-github-output and/or --prepare-build-context" >&2
  exit 1
fi
