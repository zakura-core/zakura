#!/usr/bin/env bash

set -euo pipefail

usage() {
    cat <<'EOF'
Run the local GetBlocks wire-contract evidence.

Usage:
  scripts/test-p2p-message-contracts.sh [--deep | --passing-serving | --mutants]

Options:
  --deep      Use 10,000 wire and 1,000 stateful attempts per property by default.
  --passing-serving
              Run only the serving properties already satisfied by the baseline.
  --mutants   Check whether focused production mutations are caught.
  -h, --help  Show this help.

Set PROPTEST_CASES to override wire attempts. Set
ZAKURA_SERVING_PROPTEST_CASES to override stateful serving attempts.
Set ZAKURA_SERVING_PROPTEST_SEED to replay or vary the serving sequence.
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
        --passing-serving)
            if [[ $mode != standard ]]; then
                echo "error: choose only one mode" >&2
                exit 2
            fi
            mode=passing-serving
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
export ZAKURA_SERVING_PROPTEST_SEED=${ZAKURA_SERVING_PROPTEST_SEED:-8650902}

if [[ $mode == standard ]]; then
    echo "Serving seed: $ZAKURA_SERVING_PROPTEST_SEED"
    cargo test -p zakura-network "$test_filter" -- --nocapture --test-threads=1
    exit
fi

if [[ $mode == passing-serving ]]; then
    cargo test -p zakura-network property_saturated_peer_does_not_block_honest_peer
    cargo test -p zakura-network property_response_is_largest_contiguous_prefix_within_byte_cap
    cargo test -p zakura-network response_accepts_a_prefix_ending_exactly_at_the_byte_cap
    exit
fi

if [[ $mode == deep ]]; then
    export PROPTEST_CASES=${PROPTEST_CASES:-10000}
    export ZAKURA_SERVING_PROPTEST_CASES=${ZAKURA_SERVING_PROPTEST_CASES:-1000}
    echo "Running $PROPTEST_CASES wire attempts per property"
    echo "Running $ZAKURA_SERVING_PROPTEST_CASES stateful attempts per property"
    echo "Serving seed: $ZAKURA_SERVING_PROPTEST_SEED"
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
