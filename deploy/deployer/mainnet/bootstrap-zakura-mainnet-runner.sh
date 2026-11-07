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

# shellcheck source=deploy/deployer/lib/bootstrap-runner.sh
. "$(dirname "${BASH_SOURCE[0]}")/../lib/bootstrap-runner.sh"
