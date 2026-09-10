#!/usr/bin/env bash
# Register us-east-0 as the GitHub Actions runner used by
# .github/workflows/zakura-mainnet-deploy.yml.
#
# Run from this repository on an operator machine with SSH access to the node.
# CI credentials are loaded from ~/agents-env by default; secret values are never
# printed.

set -euo pipefail

RUNNER_HOST="${RUNNER_HOST:-159.65.183.89}"
RUNNER_SSH="${RUNNER_SSH:-root@${RUNNER_HOST}}"
RUNNER_LABELS="${RUNNER_LABELS:-zakura-mainnet-deployer,zakura-mainnet,linux-x64}"
RUNNER_NAME="${RUNNER_NAME:-zakura-mainnet-1}"
RUNNER_DIR="${RUNNER_DIR:-/opt/actions-runner/zakura-mainnet-deployer}"
ENV_FILE="${ENV_FILE:-$HOME/agents-env}"
FORCE_REGISTER="${FORCE_REGISTER:-0}"

if [ ! -f "$ENV_FILE" ] && [ -f "$HOME/agent-global.env" ]; then
    ENV_FILE="$HOME/agent-global.env"
fi

if [ -f "$ENV_FILE" ] && bash -n "$ENV_FILE" >/dev/null 2>&1; then
    set -a
    # shellcheck disable=SC1090
    . "$ENV_FILE"
    set +a
elif [ -f "$ENV_FILE" ]; then
    echo "warning: env file is not shell-parseable, relying on existing gh auth/env: $ENV_FILE" >&2
else
    echo "warning: env file not found: $ENV_FILE" >&2
fi

repo_slug="${GITHUB_REPOSITORY:-}"
if [ -z "$repo_slug" ]; then
    remote_url="$(git config --get remote.origin.url)"
    repo_slug="$(python3 - "$remote_url" <<'PY'
import re
import sys

url = sys.argv[1]
patterns = [
    r"github\.com[:/](?P<slug>[^/]+/[^/.]+)(?:\.git)?$",
    r"https://github\.com/(?P<slug>[^/]+/[^/.]+)(?:\.git)?$",
]
for pattern in patterns:
    match = re.search(pattern, url)
    if match:
        print(match.group("slug"))
        raise SystemExit
raise SystemExit(f"could not infer GitHub repository from remote.origin.url: {url}")
PY
)"
fi

repo_url="https://github.com/${repo_slug}"

github_api() {
    if command -v gh >/dev/null 2>&1; then
        GH_TOKEN="${GH_TOKEN:-${GITHUB_TOKEN:-}}" gh api "$@"
        return
    fi

    token="${GH_TOKEN:-${GITHUB_TOKEN:-}}"
    if [ -z "$token" ]; then
        echo "GH_TOKEN or GITHUB_TOKEN is required when gh is unavailable" >&2
        exit 1
    fi

    method="GET"
    path=""
    while [ "$#" -gt 0 ]; do
        case "$1" in
            -X)
                method="$2"
                shift 2
                ;;
            *)
                path="$1"
                shift
                ;;
        esac
    done

    curl -fsSL \
        -X "$method" \
        -H "Accept: application/vnd.github+json" \
        -H "Authorization: Bearer ${token}" \
        -H "X-GitHub-Api-Version: 2022-11-28" \
        "https://api.github.com/${path#/}"
}

registration_token="$(
    github_api -X POST "repos/${repo_slug}/actions/runners/registration-token" \
        | python3 -c 'import json, sys; print(json.load(sys.stdin)["token"])'
)"

remove_token="$(
    github_api -X POST "repos/${repo_slug}/actions/runners/remove-token" \
        | python3 -c 'import json, sys; print(json.load(sys.stdin)["token"])'
)"

runner_version="$(
    github_api "repos/actions/runner/releases/latest" \
        | python3 -c 'import json, sys; print(json.load(sys.stdin)["tag_name"].lstrip("v"))'
)"

quote() {
    printf "%q" "$1"
}

echo "Bootstrapping ${RUNNER_NAME} at ${RUNNER_SSH} for ${repo_slug}"

ssh \
    -o BatchMode=yes \
    -o StrictHostKeyChecking=accept-new \
    "$RUNNER_SSH" \
    "RUNNER_TOKEN=$(quote "$registration_token") REMOVE_TOKEN=$(quote "$remove_token") REPO_URL=$(quote "$repo_url") RUNNER_NAME=$(quote "$RUNNER_NAME") RUNNER_LABELS=$(quote "$RUNNER_LABELS") RUNNER_DIR=$(quote "$RUNNER_DIR") RUNNER_VERSION=$(quote "$runner_version") FORCE_REGISTER=$(quote "$FORCE_REGISTER") bash -s" <<'REMOTE'
set -euo pipefail

export RUNNER_ALLOW_RUNASROOT=1

apt-get -qq update
apt-get -qq install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    clang \
    cmake \
    curl \
    git \
    jq \
    libclang-dev \
    libicu-dev \
    pkg-config \
    protobuf-compiler \
    python3 \
    tar

if ! command -v cargo >/dev/null 2>&1; then
    curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain stable
fi

if [ -f "$HOME/.cargo/env" ]; then
    # shellcheck disable=SC1091
    . "$HOME/.cargo/env"
fi

mkdir -p "$RUNNER_DIR"
cd "$RUNNER_DIR"

if [ ! -x ./config.sh ]; then
    archive="actions-runner-linux-x64-${RUNNER_VERSION}.tar.gz"
    curl -fsSLO "https://github.com/actions/runner/releases/download/v${RUNNER_VERSION}/${archive}"
    tar xzf "$archive"
    rm -f "$archive"
fi

if systemctl list-unit-files | grep -q "actions.runner.*${RUNNER_NAME}.*service"; then
    if [ "$FORCE_REGISTER" != "1" ]; then
        echo "runner service already installed; use FORCE_REGISTER=1 to re-register"
        systemctl --no-pager --full status "actions.runner.*${RUNNER_NAME}*.service" || true
        exit 0
    fi
    ./svc.sh stop || true
    ./svc.sh uninstall || true
fi

if [ -f .runner ]; then
    if [ "$FORCE_REGISTER" = "1" ]; then
        ./config.sh remove --unattended --token "$REMOVE_TOKEN" || true
    else
        echo "runner is already configured; use FORCE_REGISTER=1 to re-register"
        exit 0
    fi
fi

./config.sh \
    --unattended \
    --url "$REPO_URL" \
    --token "$RUNNER_TOKEN" \
    --name "$RUNNER_NAME" \
    --labels "$RUNNER_LABELS" \
    --work _work \
    --replace

./svc.sh install root
./svc.sh start
./svc.sh status
REMOTE

echo "Runner bootstrap complete."
