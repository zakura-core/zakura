#!/usr/bin/env bash

set -euo pipefail

usage() {
    cat <<'EOF'
Run the local property-contract evidence for the two P2P pilot messages.

Usage:
  scripts/test-p2p-message-contracts.sh [--message all|get-blocks|status] [--deep]
  scripts/test-p2p-message-contracts.sh [--message all|get-blocks|status] --mutants

Options:
  --message NAME  Select both pilots (default), GetBlocks, or Status.
  --deep          Use 10,000 generated attempts per property by default.
  --mutants       Check whether focused production mutations are caught.
  -h, --help      Show this help.

Set PROPTEST_CASES to override the generated-attempt count in any mode.
Mutation mode requires cargo-mutants. Its reports are written below target/.
EOF
}

message=all
mode=standard

while (($# > 0)); do
    case "$1" in
        --message)
            if (($# < 2)); then
                echo "error: --message requires a value" >&2
                usage >&2
                exit 2
            fi
            message=$2
            shift 2
            ;;
        --deep)
            if [[ $mode == mutants ]]; then
                echo "error: --deep and --mutants are separate modes" >&2
                exit 2
            fi
            mode=deep
            shift
            ;;
        --mutants)
            if [[ $mode == deep ]]; then
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

case "$message" in
    all)
        test_filter='message_contracts'
        mutation_filter='BlockSyncMessage::(message_type|encode|decode|encode_frame|decode_frame) ->|BlockSyncStatus::(encode_to|decode_from) ->|validate_block_count|read_height|reject_trailing|try_send_(status|get_blocks) ->'
        mutation_files=(
            'crates/zakura-network/src/zakura/block_sync/wire.rs'
            'crates/zakura-network/src/zakura/block_sync/config.rs'
            'crates/zakura-network/src/zakura/block_sync/service.rs'
        )
        ;;
    get-blocks)
        test_filter='message_contracts::get_blocks'
        mutation_filter='BlockSyncMessage::(message_type|encode|decode|encode_frame|decode_frame) ->|validate_block_count|read_height|reject_trailing|try_send_get_blocks ->'
        mutation_files=(
            'crates/zakura-network/src/zakura/block_sync/wire.rs'
            'crates/zakura-network/src/zakura/block_sync/service.rs'
        )
        ;;
    status)
        test_filter='message_contracts::status'
        mutation_filter='BlockSyncMessage::(message_type|encode|decode|encode_frame|decode_frame) ->|BlockSyncStatus::(encode_to|decode_from) ->|read_height|reject_trailing|try_send_status ->'
        mutation_files=(
            'crates/zakura-network/src/zakura/block_sync/wire.rs'
            'crates/zakura-network/src/zakura/block_sync/config.rs'
            'crates/zakura-network/src/zakura/block_sync/service.rs'
        )
        ;;
    *)
        echo "error: unsupported message: $message" >&2
        usage >&2
        exit 2
        ;;
esac

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$repo_root"

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
mutation_file_args=()
for mutation_file in "${mutation_files[@]}"; do
    mutation_file_args+=(--file "$mutation_file")
done

"${mutants[@]}" \
    --no-config \
    --package zakura-network \
    "${mutation_file_args[@]}" \
    --re "$mutation_filter" \
    --output "target/p2p-message-mutants-$message" \
    -- "$test_filter" -- --test-threads=1
