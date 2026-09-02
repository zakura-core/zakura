#!/usr/bin/env bash

set -euo pipefail

usage() {
    cat <<'EOF'
Run the local GetBlocks wire-contract evidence.

Usage:
  scripts/test-p2p-message-contracts.sh [--deep | --mutants]

Options:
  --deep      Use 10,000 generated attempts per property by default.
  --mutants   Check whether focused production mutations are caught.
  -h, --help  Show this help.

Set PROPTEST_CASES to override the generated-attempt count in any mode.
Mutation mode requires cargo-mutants. Its report is written below target/.
EOF
}

mode=standard

while (($# > 0)); do
    case "$1" in
        --deep)
            if [[ $mode != standard ]]; then
                echo "error: --deep and --mutants are separate modes" >&2
                exit 2
            fi
            mode=deep
            shift
            ;;
        --mutants)
            if [[ $mode != standard ]]; then
                echo "error: --deep and --mutants are separate modes" >&2
                exit 2
            fi
            mode=mutants
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "error: unknown argument: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

test_filter='message_contracts::get_blocks'

if [[ $mode == standard ]]; then
    cargo test -p zakura-network "$test_filter" -- --nocapture --test-threads=1
    exit
fi

if [[ $mode == deep ]]; then
    export PROPTEST_CASES=${PROPTEST_CASES:-10000}
    echo "Running $PROPTEST_CASES generated attempts per property"
    cargo test -p zakura-network "$test_filter" -- --nocapture --test-threads=1
    exit
fi

if command -v cargo-mutants >/dev/null 2>&1; then
    mutants=(cargo-mutants mutants)
elif [[ -x target/property-tools/bin/cargo-mutants ]]; then
    mutants=(target/property-tools/bin/cargo-mutants mutants)
else
    echo "error: mutation mode requires cargo-mutants" >&2
    echo "install it with: cargo install --locked cargo-mutants" >&2
    exit 2
fi

export PROPTEST_CASES=${PROPTEST_CASES:-256}

"${mutants[@]}" \
    --no-config \
    --package zakura-network \
    --file crates/zakura-network/src/zakura/block_sync/wire.rs \
    --file crates/zakura-network/src/zakura/block_sync/service.rs \
    --re 'BlockSyncMessage::(message_type|encode|decode|encode_frame|decode_frame) ->|validate_block_count|read_height|reject_trailing|try_send_get_blocks ->' \
    --output target/p2p-message-mutants-get-blocks \
    -- "$test_filter" -- --test-threads=1
