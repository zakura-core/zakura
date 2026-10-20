#!/usr/bin/env bash
# Register zakura-testnet-1 as the GitHub Actions runner used by
# .github/workflows/zakura-testnet-deploy.yml.
#
# Run from this repository on an operator machine with SSH access to the node.
# CI credentials are loaded from ~/agents-env by default; secret values are never
# printed.

set -euo pipefail

RUNNER_HOST="${RUNNER_HOST:-167.99.103.111}"
RUNNER_SSH="${RUNNER_SSH:-root@${RUNNER_HOST}}"
RUNNER_LABELS="${RUNNER_LABELS:-zakura-testnet-deployer,zakura-testnet,linux-x64}"
RUNNER_NAME="${RUNNER_NAME:-zakura-testnet-1}"
RUNNER_DIR="${RUNNER_DIR:-/opt/actions-runner/zakura-testnet-deployer}"
ENV_FILE="${ENV_FILE:-$HOME/agents-env}"
FORCE_REGISTER="${FORCE_REGISTER:-0}"

# shellcheck source=deploy/deployer/lib/bootstrap-runner.sh
. "$(dirname "${BASH_SOURCE[0]}")/../lib/bootstrap-runner.sh"
