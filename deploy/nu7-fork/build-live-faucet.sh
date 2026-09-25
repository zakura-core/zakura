#!/usr/bin/env bash
# Rebuild the faucet sender for the existing public fork's 77190ad9 branch ID.
set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "usage: $0 <fresh-build-directory>" >&2
    exit 2
fi

source_root=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
build_root=$(mkdir -p "$1" && cd "$1" && pwd)
if [[ -e "$build_root/zakura" || -e "$build_root/common" ]]; then
    echo "build directory must not contain zakura or common" >&2
    exit 2
fi

git -C "$source_root" fetch origin nu7-testnet-live-branch-id-ad9
git -C "$source_root" worktree add --detach "$build_root/zakura" FETCH_HEAD
git clone --quiet https://github.com/zakura-core/common.git "$build_root/common"
git -C "$build_root/common" checkout --quiet c4c255c4be5cacaad77b918a481cbf6d35b1f8fd

cp "$source_root/deploy/nu7-fork/txload/Cargo.toml" \
    "$build_root/zakura/deploy/nu7-fork/txload/Cargo.toml"
cp "$source_root/deploy/nu7-fork/txload/src/main.rs" \
    "$build_root/zakura/deploy/nu7-fork/txload/src/main.rs"

python3 - "$build_root" <<'PY'
import json
import sys
from pathlib import Path

root = Path(sys.argv[1])
protocol = root / "common/crates/zcash_protocol/src/consensus.rs"
source = protocol.read_text()
assert source.count("0x7719_0ad8") == 4, "unexpected NU7 branch ID source"
protocol.write_text(source.replace("0x7719_0ad8", "0x7719_0ad9"))
(root / "patch.toml").write_text(
    '[patch."https://github.com/zakura-core/common.git"]\n'
    f'zakura-protocol = {{ path = {json.dumps(str(protocol.parent.parent))} }}\n'
)
PY

CARGO_TARGET_DIR="$build_root/target" cargo \
    --config "$build_root/patch.toml" \
    build --release -p zakura-fork-txload \
    --manifest-path "$build_root/zakura/Cargo.toml"

echo "$build_root/target/release/zakura-fork-txload"
