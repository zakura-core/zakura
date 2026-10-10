#!/usr/bin/env bash
# Build both database formats and exercise the forward upgrade between them.
set -euo pipefail
cd "$(dirname "$0")/.."
fixture=$(mktemp -d "${TMPDIR:-$HOME/.tmp}/zakura-db-upgrade.XXXXXX")
trap 'rm -rf "$fixture"' EXIT
export ZAKURA_DB_UPGRADE_FIXTURE="$fixture"
RUSTFLAGS='' cargo test --locked -p zakura-state --lib cross_build_database_upgrade -- --ignored --nocapture
RUSTFLAGS='--cfg zcash_unstable="nutachyon"' cargo test --locked -p zakura-state --lib cross_build_database_upgrade -- --ignored --nocapture
