#!/usr/bin/env bash
# Runs ON an ephemeral checkpoint-sync-bench droplet
# (checkpoint-sync-bench.yml). Mounts the cloned Mainnet sandblast volume,
# validates DB format against the benched tree when building, then delegates to
# scripts/checkpoint-sync-bench.sh with STATE_MASTER set so the historical
# archive download is skipped.
#
# The volume clone is disposable and deleted with the droplet. Per-run state
# forks stay on the volume via hard-links; builds reuse the baked
# /root/zakura + /root/cargo-target cache.
#
# Config via /root/run.env (sourced by the caller before exec). Helper scripts
# scp'd next to this one by the workflow:
#   /root/checkpoint-sync-bench.sh
#   /root/zakura-metrics-dashboard.py
set -euo pipefail

OUT_DIR=/root/out
BENCH_SH=/root/checkpoint-sync-bench.sh
DASHBOARD_PY=/root/zakura-metrics-dashboard.py
BENCH_HOME=/root/zakura-bench
mkdir -p "$OUT_DIR" "$BENCH_HOME"

log() { printf '[ckpt-bench-run %(%H:%M:%S)T] %s\n' -1 "$*" >&2; }
die() { log "FATAL: $*"; exit 1; }

[[ -f "$BENCH_SH" ]] || die "missing $BENCH_SH (workflow must scp it)"
[[ -n "${VOLUME_NAME:-}" ]] || die "VOLUME_NAME is required"

cloud-init status --wait >/dev/null 2>&1 || true

# ---------------------------------------------------------------------------- #
# State: mount the baked sandblast tree from the cloned volume snapshot
# ---------------------------------------------------------------------------- #

DEV="/dev/disk/by-id/scsi-0DO_Volume_${VOLUME_NAME}"
for _ in $(seq 1 30); do [ -e "$DEV" ] && break; sleep 2; done
[ -e "$DEV" ] || die "state volume device not found: $DEV"
mkdir -p /mnt/snapshots
mount "$DEV" /mnt/snapshots
STATE_MASTER="/mnt/snapshots/sandblast"
[ -d "$STATE_MASTER/state" ] || die "no sandblast/state on the volume"
df -h /mnt/snapshots >&2

# Forks must share a filesystem with STATE_MASTER for cp -al.
FORKS_DIR="/mnt/snapshots/forks"
mkdir -p "$FORKS_DIR"

# ---------------------------------------------------------------------------- #
# Source: prepare the baked clone / release download tooling
# ---------------------------------------------------------------------------- #

# shellcheck source=/dev/null
[[ -f "$HOME/.cargo/env" ]] && . "$HOME/.cargo/env"
export PATH="${HOME}/.cargo/bin:${PATH}"
export CARGO_TARGET_DIR=/root/cargo-target

PRIMARY_SHA=""
if [[ -n "${BUILD_REF:-}" ]]; then
  [[ -d /root/zakura/.git ]] || die "baked /root/zakura clone missing"
  cd /root/zakura
  # Fetch every ref we may build so BUILD_REF / BASELINE_REF resolve offline later.
  git fetch --no-tags origin "${BUILD_REF}"
  PRIMARY_SHA=$(git rev-parse --verify 'FETCH_HEAD^{commit}')
  if [[ -n "${BASELINE_REF:-}" && "${SKIP_BASELINE:-0}" != "1" ]]; then
    git fetch --no-tags origin "${BASELINE_REF}"
  fi
  git checkout --detach "${PRIMARY_SHA}"

  CODE_VER=$(grep -oE 'DATABASE_FORMAT_VERSION: .* [0-9]+' \
    crates/zakura-state/src/constants.rs | grep -oE '[0-9]+' | tail -n1)
  DIR_VER=$(find "$STATE_MASTER/state" -mindepth 1 -maxdepth 1 -type d -name 'v*' 2>/dev/null | \
    sed 's#.*/v##' | sort -n | tail -1)
  if [ -z "$DIR_VER" ]; then
    die "no state/v* directory under $STATE_MASTER"
  elif [ "$DIR_VER" != "$CODE_VER" ] && [ "$DIR_VER" != "$((CODE_VER - 1))" ]; then
    die "DB format mismatch: volume is v${DIR_VER}, benched tree is v${CODE_VER}; re-bake the state snapshot"
  fi
  log "state DB v${DIR_VER} compatible with code v${CODE_VER}; primary=${PRIMARY_SHA}"
elif [[ -z "${RELEASE_TAG:-}" ]]; then
  die "set BUILD_REF or RELEASE_TAG"
fi

rm -f /root/run.env

# ---------------------------------------------------------------------------- #
# Run metadata for the artifact (image / snapshot / droplet identity)
# ---------------------------------------------------------------------------- #

export PRIMARY_SHA
python3 - <<'PY'
import json, os
from pathlib import Path
meta = {
    "droplet_id": os.environ.get("DROPLET_ID", ""),
    "droplet_size": os.environ.get("DROPLET_SIZE", ""),
    "image_id": os.environ.get("IMAGE_ID", ""),
    "state_snapshot_id": os.environ.get("STATE_SNAPSHOT_ID", ""),
    "volume_name": os.environ.get("VOLUME_NAME", ""),
    "start_height": os.environ.get("START_HEIGHT", ""),
    "stop_height": os.environ.get("STOP_HEIGHT", ""),
    "verify_mode": os.environ.get("VERIFY_MODE", ""),
    "build_ref": os.environ.get("BUILD_REF", ""),
    "baseline_ref": os.environ.get("BASELINE_REF", ""),
    "release_tag": os.environ.get("RELEASE_TAG", ""),
    "primary_sha": os.environ.get("PRIMARY_SHA", ""),
    "github_run_url": os.environ.get("GITHUB_RUN_URL", ""),
    "github_run_id": os.environ.get("GITHUB_RUN_ID", ""),
}
Path("/root/out/run-meta.json").write_text(json.dumps(meta, indent=2) + "\n")
PY

# ---------------------------------------------------------------------------- #
# Delegate to the shared bench script
# ---------------------------------------------------------------------------- #

export OUT_DIR BENCH_HOME STATE_MASTER FORKS_DIR DASHBOARD_PY
export BUILD_SRC=/root/zakura
export BUILD_TARGET=/root/cargo-target
export BUILD_CARGO_HOME="${CARGO_HOME:-$HOME/.cargo}"
export DASHBOARD_ARCHIVE="${OUT_DIR}/dashboard-runs"
export GITHUB_STEP_SUMMARY="${OUT_DIR}/summary.md"
export PRIMARY_SHA

# Prefer the already-resolved SHA so the bench builds the same commit we validated.
if [[ -n "${PRIMARY_SHA}" && -n "${BUILD_REF:-}" ]]; then
  export BUILD_REF="${PRIMARY_SHA}"
fi

bash "$BENCH_SH"
status=$?
exit "$status"
