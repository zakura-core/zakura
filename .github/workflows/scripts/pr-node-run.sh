#!/usr/bin/env bash
# Runs on an ephemeral PR-node droplet. The standard profile mounts one Golden
# Zakura state clone. The zcashd-compat profile also mounts a Golden zcashd
# datadir, exercises a cold managed install and a clean stop/warm restart, then
# monitors both nodes.
set -euo pipefail

OUT_DIR=/root/out
NOTES="$OUT_DIR/notes.md"
mkdir -p "$OUT_DIR"

note() {
  echo "$1"
  echo "- $1" >> "$NOTES"
}

mount_snapshot() {
  local volume_name="$1"
  local mount_dir="$2"
  local device="/dev/disk/by-id/scsi-0DO_Volume_${volume_name}"
  for _ in $(seq 1 30); do
    [ -e "$device" ] && break
    sleep 2
  done
  [ -e "$device" ] || { echo "volume device not found: $device" >&2; exit 1; }
  mkdir -p "$mount_dir"
  mount "$device" "$mount_dir"
  mountpoint -q "$mount_dir" || { echo "volume was not mounted at $mount_dir" >&2; exit 1; }
}

copy_fixture_manifest() {
  local output="$1"
  shift
  local candidate
  for candidate in "$@"; do
    if [ -f "$candidate" ]; then
      cp "$candidate" "$output"
      note "Fixture manifest: \`$candidate\`."
      return 0
    fi
  done
  note "**WARNING:** fixture manifest was not present in the Golden state clone."
  return 0
}

validate_compat_fixtures() {
  python3 - "$1" "$2" <<'PY'
import json
import re
import sys
from pathlib import Path

zakura_path, zcashd_path = map(Path, sys.argv[1:])
zakura = json.loads(zakura_path.read_text())
zcashd = json.loads(zcashd_path.read_text())

def require(condition, message):
    if not condition:
        raise SystemExit(f"incompatible Golden fixture pair: {message}")

def uint(value):
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0

def block_hash(value):
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None

require(zakura.get("schema_version") == 1, "Zakura schema_version must be 1")
require(zakura.get("fixture") == "zakura-state", "Zakura fixture kind is invalid")
require(zakura.get("network") == "mainnet", "Zakura fixture is not mainnet")
require(zakura.get("mode") == "tip", "Zakura fixture is not tip mode")
require(zcashd.get("schema_version") == 1, "zcashd schema_version must be 1")
require(zcashd.get("fixture") == "zcashd-mainnet", "zcashd fixture kind is invalid")
require(zcashd.get("network") == "mainnet", "zcashd fixture is not mainnet")
require(zcashd.get("status") in {"candidate", "verified"}, "zcashd status is invalid")

zakura_tip = zakura.get("tip") or {}
zcashd_tip = zcashd.get("tip") or {}
refresh = zcashd.get("refresh") or {}
refresh_zakura_tip = refresh.get("zakura_tip") or {}
require(uint(zakura_tip.get("height")), "Zakura tip height is invalid")
require(block_hash(zakura_tip.get("hash")), "Zakura tip hash is invalid")
require(uint(zcashd_tip.get("height")), "zcashd tip height is invalid")
require(block_hash(zcashd_tip.get("hash")), "zcashd tip hash is invalid")
require(refresh.get("clean_shutdown") is True, "zcashd refresh was not cleanly shut down")
require(refresh_zakura_tip == zakura_tip, "zcashd refresh Zakura tip does not match the Zakura fixture")

lag = refresh.get("lag_blocks")
retention = refresh.get("tx_retention")
require(uint(lag), "zcashd refresh lag is invalid")
require(isinstance(retention, int) and not isinstance(retention, bool) and retention > 0,
        "zcashd refresh retention is invalid")
require(lag < retention, "zcashd lag is outside the recorded pruning retention")
require(zakura.get("tx_retention") == retention, "fixture retention values do not match")
require(zakura_tip["height"] - zcashd_tip["height"] == lag,
        "recorded lag does not match the paired fixture tips")
print(f"paired Golden fixtures validated: Zakura {zakura_tip['height']}, "
      f"zcashd {zcashd_tip['height']}, lag {lag}/{retention}, status {zcashd['status']}")
PY
}

rpc_result() {
  local url="$1"
  local method="$2"
  local cookie_file="${3:-}"
  local auth=()
  if [ -n "$cookie_file" ]; then
    [ -s "$cookie_file" ] || return 1
    auth=(--user "$(<"$cookie_file")")
  fi
  curl -fsS "${auth[@]}" \
    -H 'Content-Type: application/json' \
    --data "{\"jsonrpc\":\"1.0\",\"id\":\"pr-node-run\",\"method\":\"${method}\",\"params\":[]}" \
    "$url" | python3 -c 'import json,sys; response=json.load(sys.stdin); assert response.get("error") is None, response.get("error"); print(response["result"])'
}

wait_for_compat() {
  local zakura_height=""
  local zcashd_height=""
  local main_pid=""
  local child_pid=""
  local children=()
  for _ in $(seq 1 180); do
    main_pid=$(systemctl show zakurad --property MainPID --value)
    mapfile -t children < <(pgrep --parent "${main_pid:-0}" --exact zcashd 2>/dev/null || true)
    child_pid=""
    if [ "${#children[@]}" -eq 1 ]; then
      child_pid=${children[0]}
    fi
    zakura_height=$(rpc_result http://127.0.0.1:18232 getblockcount 2>/dev/null || true)
    zcashd_height=$(rpc_result http://127.0.0.1:8232 getblockcount /mnt/zcashd/.cookie 2>/dev/null || true)
    if [[ "$zakura_height" =~ ^[0-9]+$ && "$zcashd_height" =~ ^[0-9]+$ && "$child_pid" =~ ^[0-9]+$ ]]; then
      printf '%s %s %s\n' "$zakura_height" "$zcashd_height" "$child_pid"
      return 0
    fi
    sleep 5
  done
  echo "managed zcashd and Zakura did not become RPC-ready in 15 minutes" >&2
  return 1
}

wait_for_compat_progress() {
  local start_zakura="$1"
  local start_zcashd="$2"
  local status=""
  local zakura_height=""
  local zcashd_height=""
  local child_pid=""
  for _ in $(seq 1 180); do
    status=$(wait_for_compat) || return 1
    read -r zakura_height zcashd_height child_pid <<< "$status"
    if [ "$zakura_height" -gt "$start_zakura" ] && [ "$zcashd_height" -gt "$start_zcashd" ]; then
      printf '%s %s %s\n' "$zakura_height" "$zcashd_height" "$child_pid"
      return 0
    fi
    sleep 5
  done
  echo "Zakura and managed zcashd did not both advance in 15 minutes" >&2
  return 1
}

cloud-init status --wait >/dev/null 2>&1 || true
TEST_PROFILE=${TEST_PROFILE:-zakura}

# ---------------------------------------------------------------------------- #
# Golden state volumes
# ---------------------------------------------------------------------------- #

if [ "$MODE" = "genesis" ]; then
  STATE_CACHE_DIR=/var/lib/zakura
  STORAGE_MODE=archive
  mkdir -p "$STATE_CACHE_DIR"
else
  mount_snapshot "$ZAKURA_VOLUME_NAME" /mnt/snapshots
  STATE_CACHE_DIR="/mnt/snapshots/${MODE}"
  case "$MODE" in
    tip)       STORAGE_MODE=pruned ;;
    sandblast) STORAGE_MODE=archive ;;
    *)         echo "unknown snapshot mode: $MODE" >&2; exit 1 ;;
  esac
  [ -d "$STATE_CACHE_DIR" ] || { echo "no ${MODE}/ state on the Zakura volume" >&2; exit 1; }
  if [ "$TEST_PROFILE" = "zcashd-compat" ]; then
    [ -f "$STATE_CACHE_DIR/fixture-manifest.json" ] || {
      echo "compat requires the canonical Zakura fixture-manifest.json" >&2
      exit 1
    }
    cp "$STATE_CACHE_DIR/fixture-manifest.json" "$OUT_DIR/zakura-fixture-manifest.json"
    note "Fixture manifest: \`$STATE_CACHE_DIR/fixture-manifest.json\`."
  else
    copy_fixture_manifest "$OUT_DIR/zakura-fixture-manifest.json" \
      "$STATE_CACHE_DIR/fixture-manifest.json" \
      "$STATE_CACHE_DIR/zakura-fixture.json" \
      /mnt/snapshots/fixture-manifest.json \
      /mnt/snapshots/zakura-fixture.json
  fi
fi

if [ "$TEST_PROFILE" = "zcashd-compat" ]; then
  [ "$MODE" = "tip" ] && [ "$NETWORK" = "mainnet" ] || {
    echo "zcashd-compat requires snapshot_mode=tip and network=mainnet" >&2
    exit 1
  }
  [ -n "${ZCASHD_VOLUME_NAME:-}" ] || { echo "zcashd Golden volume is missing" >&2; exit 1; }
  mount_snapshot "$ZCASHD_VOLUME_NAME" /mnt/zcashd
  for required in blocks chainstate unity; do
    [ -d "/mnt/zcashd/$required" ] || {
      echo "zcashd Golden fixture is missing /mnt/zcashd/$required" >&2
      exit 1
    }
  done
  [ -f /mnt/zcashd/fixture-manifest.json ] || {
    echo "compat requires the canonical zcashd fixture-manifest.json" >&2
    exit 1
  }
  cp /mnt/zcashd/fixture-manifest.json "$OUT_DIR/zcashd-fixture-manifest.json"
  note "Fixture manifest: \`/mnt/zcashd/fixture-manifest.json\`."
  validate_compat_fixtures \
    "$OUT_DIR/zakura-fixture-manifest.json" \
    "$OUT_DIR/zcashd-fixture-manifest.json"
fi

df -h / /mnt/snapshots /mnt/zcashd 2>/dev/null || true

# ---------------------------------------------------------------------------- #
# Exact source ref
# ---------------------------------------------------------------------------- #

cd /root/zakura
GIT_AUTH=$(printf 'x-access-token:%s' "${GH_CLONE_TOKEN}" | base64 -w0)
git -c http.extraheader="AUTHORIZATION: basic ${GIT_AUTH}" fetch --no-tags origin "${REFSPEC}"
git checkout --detach "${SHA}"
# Deployment is test harness, not code under test. Use the workflow revision's
# renderer so old target refs understand the Golden zcashd profile while Cargo
# still builds exactly SHA from this checkout.
mkdir -p deploy/deployer/templates
install -m 755 /root/pr-node-deploy.py deploy/deployer/deploy.py
install -m 644 /root/pr-node-zakura.toml deploy/deployer/templates/zakura.toml
install -m 644 /root/pr-node-zakurad.service deploy/deployer/templates/zakurad.service
[ "$(git rev-parse HEAD)" = "$SHA" ] || {
  echo "target checkout moved away from requested SHA $SHA" >&2
  exit 1
}
rm -f /root/run.env
unset GH_CLONE_TOKEN GIT_AUTH

CODE_VER=$(grep -oE 'DATABASE_FORMAT_VERSION: .* [0-9]+' zakura-state/src/constants.rs | grep -oE '[0-9]+' | tail -n1)
if [ "$MODE" != "genesis" ]; then
  DIR_VER=$(find "$STATE_CACHE_DIR/state" -mindepth 1 -maxdepth 1 -type d -name 'v*' 2>/dev/null | \
    sed 's#.*/v##' | sort -n | tail -1)
  if [ -z "$DIR_VER" ]; then
    note "**WARNING:** no state/v* directory found under \`$STATE_CACHE_DIR\`."
    [ "$TEST_PROFILE" != "zcashd-compat" ] || {
      echo "zcashd-compat requires a compatible Golden Zakura state DB" >&2
      exit 1
    }
  elif [ "$DIR_VER" = "$CODE_VER" ]; then
    note "State DB format v${DIR_VER} matches the PR tree."
  elif [ "$DIR_VER" = "$((CODE_VER - 1))" ]; then
    note "State snapshot is v${DIR_VER}, PR tree is v${CODE_VER}. The in-place format upgrade is part of this run."
  else
    note "**WARNING: DB format mismatch.** Snapshot is v${DIR_VER}, PR tree is v${CODE_VER}."
    [ "$TEST_PROFILE" != "zcashd-compat" ] || {
      echo "zcashd-compat refuses an incompatible Golden Zakura state DB" >&2
      exit 1
    }
  fi
fi

# ---------------------------------------------------------------------------- #
# Build and deploy through the normal deployer
# ---------------------------------------------------------------------------- #

cat > /root/.ssh/config <<'CFG'
Host localhost
    IdentityFile /root/.ssh/pr_node_loopback
    StrictHostKeyChecking no
    UserKnownHostsFile /dev/null
CFG
chmod 600 /root/.ssh/config
rm -f /root/.ssh/known_hosts
ssh -o BatchMode=yes root@localhost true

case "$NETWORK" in
  mainnet) NET_TOML=Mainnet ;;
  testnet) NET_TOML=Testnet ;;
  *) echo "unknown network: $NETWORK" >&2; exit 1 ;;
esac

if [ "$TEST_PROFILE" = "zcashd-compat" ]; then
  # The wrapper cache belongs to this ephemeral clone. Removing only its binary
  # cache makes the first start exercise the pinned embedded install path; the
  # second start must reuse it unchanged.
  WRAPPER_CACHE="$STATE_CACHE_DIR/zcashd-compat/bin"
  case "$WRAPPER_CACHE" in
    /mnt/snapshots/tip/zcashd-compat/bin) ;;
    *) echo "refusing to clear unexpected wrapper cache path: $WRAPPER_CACHE" >&2; exit 1 ;;
  esac
  rm -rf -- "$WRAPPER_CACHE"

  cat > /root/fleet.toml <<TOML
[defaults]
service_kill_mode = "mixed"
service_timeout_stop_sec = "6m"

[defaults.zcashd_compat]
enabled = true
manage_zcashd = true
zcashd_source = "embedded"
zcashd_datadir = "/mnt/zcashd"
zcashd_extra_args = ["-server=1", "-rpcbind=127.0.0.1", "-rpcallowip=127.0.0.1"]
shutdown_grace_period = "5m"

[[nodes]]
name = "pr-node"
ssh_string = "root@localhost"
commit = "${SHA}"
network = "${NET_TOML}"
state_cache_dir = "${STATE_CACHE_DIR}"
storage_mode = "${STORAGE_MODE}"
p2p_stack = "legacy"
rpc_listen_addr = "127.0.0.1:18232"
rpc_enable_cookie_auth = false
metrics_endpoint = "127.0.0.1:9999"
TOML
  ZAKURA_RPC_URL=http://127.0.0.1:18232
else
  cat > /root/fleet.toml <<TOML
[[nodes]]
name = "pr-node"
ssh_string = "root@localhost"
commit = "${SHA}"
network = "${NET_TOML}"
state_cache_dir = "${STATE_CACHE_DIR}"
storage_mode = "${STORAGE_MODE}"
p2p_stack = "default"
rpc_listen_addr = "127.0.0.1:8232"
rpc_enable_cookie_auth = false
metrics_endpoint = "127.0.0.1:9999"
TOML
  ZAKURA_RPC_URL=http://127.0.0.1:8232
fi

export CARGO_TARGET_DIR=/root/cargo-target
BUILD_START=$(date +%s)
python3 deploy/deployer/deploy.py build --config /root/fleet.toml
[ -x "deploy/deployer/.build-cache/zakurad-${SHA}" ] || {
  echo "deployer did not produce the SHA-keyed binary for $SHA" >&2
  exit 1
}
note "Incremental build took $(( $(date +%s) - BUILD_START ))s using the baked Cargo cache."
note "Built exact target commit \`$SHA\`; only the workflow's deployer harness was overlaid."

mkdir -p /var/log/zakura
: > /var/log/zakura/zakura.log
python3 deploy/deployer/deploy.py deploy --config /root/fleet.toml
python3 deploy/deployer/deploy.py status --config /root/fleet.toml || true

# ---------------------------------------------------------------------------- #
# Managed zcashd cold start, clean stop, and warm restart
# ---------------------------------------------------------------------------- #

if [ "$TEST_PROFILE" = "zcashd-compat" ]; then
  read -r COLD_READY_ZAKURA_HEIGHT COLD_READY_ZCASHD_HEIGHT COLD_READY_CHILD_PID < <(wait_for_compat)
  read -r COLD_ZAKURA_HEIGHT COLD_ZCASHD_HEIGHT COLD_CHILD_PID < <(
    wait_for_compat_progress "$COLD_READY_ZAKURA_HEIGHT" "$COLD_READY_ZCASHD_HEIGHT"
  )
  [ "$COLD_READY_CHILD_PID" = "$COLD_CHILD_PID" ] || {
    echo "managed zcashd restarted during the cold progress check" >&2
    exit 1
  }
  CACHED_ZCASHD=$(find "$WRAPPER_CACHE" -type f -name zcashd -perm -u+x -print -quit)
  [ -n "$CACHED_ZCASHD" ] || { echo "managed zcashd cache was not populated" >&2; exit 1; }
  case "$CACHED_ZCASHD" in
    "$WRAPPER_CACHE"/*/x86_64-pc-linux-gnu/zcashd) ;;
    *) echo "managed zcashd resolved outside its source-defined cache layout: $CACHED_ZCASHD" >&2; exit 1 ;;
  esac
  COLD_CACHE_SHA=$(sha256sum "$CACHED_ZCASHD" | awk '{print $1}')
  COLD_CACHE_MTIME=$(stat -c %Y "$CACHED_ZCASHD")
  note "Cold start installed managed zcashd. Both nodes advanced from Zakura ${COLD_READY_ZAKURA_HEIGHT}, zcashd ${COLD_READY_ZCASHD_HEIGHT} to Zakura ${COLD_ZAKURA_HEIGHT}, zcashd ${COLD_ZCASHD_HEIGHT}."

  STOP_START_LINE=$(( $(wc -l < /var/log/zakura/zakura.log) + 1 ))
  systemctl stop zakurad
  for _ in $(seq 1 30); do
    if ! pgrep --exact zcashd >/dev/null 2>&1; then break; fi
    sleep 2
  done
  ! pgrep --exact zcashd >/dev/null 2>&1 || { echo "managed zcashd survived Zakura stop" >&2; exit 1; }
  STOP_LOG="$OUT_DIR/cold-stop.log"
  tail -n "+${STOP_START_LINE}" /var/log/zakura/zakura.log > "$STOP_LOG"
  for evidence in \
    "Shutdown: main: done" \
    "zcashd-compat zcashd exited cleanly after SIGTERM" \
    "zcashd-compat zcashd child stopped on shutdown" \
    "stopping zakurad"; do
    grep -Fq "$evidence" "$STOP_LOG" || {
      echo "clean-shutdown evidence missing: $evidence" >&2
      exit 1
    }
  done

  systemctl start zakurad
  read -r WARM_ZAKURA_HEIGHT WARM_ZCASHD_HEIGHT WARM_CHILD_PID < <(wait_for_compat)
  WARM_CACHE_SHA=$(sha256sum "$CACHED_ZCASHD" | awk '{print $1}')
  WARM_CACHE_MTIME=$(stat -c %Y "$CACHED_ZCASHD")
  [ "$COLD_CACHE_SHA" = "$WARM_CACHE_SHA" ] && [ "$COLD_CACHE_MTIME" = "$WARM_CACHE_MTIME" ] || {
    echo "warm restart replaced the managed zcashd cache" >&2
    exit 1
  }
  [ "$COLD_CHILD_PID" != "$WARM_CHILD_PID" ] || {
    echo "warm restart did not launch a new managed zcashd child" >&2
    exit 1
  }
  python3 - "$OUT_DIR/compat-lifecycle.json" \
    "$COLD_CHILD_PID" "$WARM_CHILD_PID" "$COLD_CACHE_SHA" \
    "$COLD_READY_ZAKURA_HEIGHT" "$COLD_READY_ZCASHD_HEIGHT" \
    "$COLD_ZAKURA_HEIGHT" "$COLD_ZCASHD_HEIGHT" \
    "$WARM_ZAKURA_HEIGHT" "$WARM_ZCASHD_HEIGHT" <<'PY'
import json
import sys
from pathlib import Path

(
    out,
    cold_pid,
    warm_pid,
    cache_sha,
    cold_ready_z,
    cold_ready_d,
    cold_z,
    cold_d,
    warm_z,
    warm_d,
) = sys.argv[1:]
Path(out).write_text(json.dumps({
    "passed": True,
    "cold_child_pid": int(cold_pid),
    "warm_child_pid": int(warm_pid),
    "managed_binary_sha256": cache_sha,
    "cache_reused": True,
    "clean_shutdown_evidence": [
        "Shutdown: main: done",
        "zcashd-compat zcashd exited cleanly after SIGTERM",
        "zcashd-compat zcashd child stopped on shutdown",
        "stopping zakurad",
    ],
    "cold_ready_heights": {"zakura": int(cold_ready_z), "zcashd": int(cold_ready_d)},
    "cold_progress_heights": {"zakura": int(cold_z), "zcashd": int(cold_d)},
    "warm_heights": {"zakura": int(warm_z), "zcashd": int(warm_d)},
}, indent=2) + "\n")
PY
  note "Managed zcashd stopped cleanly and warm restarted from the unchanged cache at heights Zakura ${WARM_ZAKURA_HEIGHT}, zcashd ${WARM_ZCASHD_HEIGHT}."
fi

# ---------------------------------------------------------------------------- #
# Monitor and package evidence
# ---------------------------------------------------------------------------- #

META="mode=${MODE},network=${NETWORK},sha=${SHA},image_id=${IMAGE_ID},image_name=${IMAGE_NAME},image_created=${IMAGE_CREATED},zakura_snapshot_id=${ZAKURA_SNAPSHOT_ID},zakura_snapshot_name=${ZAKURA_SNAPSHOT_NAME},zakura_snapshot_created=${ZAKURA_SNAPSHOT_CREATED},zcashd_snapshot_id=${ZCASHD_SNAPSHOT_ID:-},zcashd_snapshot_name=${ZCASHD_SNAPSHOT_NAME:-},zcashd_snapshot_created=${ZCASHD_SNAPSHOT_CREATED:-}"
MONITOR_ARGS=(
  --duration-minutes "${DURATION_MINUTES}"
  --interval 30
  --test-profile "$TEST_PROFILE"
  --rpc-url "$ZAKURA_RPC_URL"
  --service zakurad
  --log-file /var/log/zakura/zakura.log
  --notes "$NOTES"
  --meta "$META"
  --out "$OUT_DIR"
)
if [ -f "$OUT_DIR/zakura-fixture-manifest.json" ]; then
  MONITOR_ARGS+=(--zakura-fixture-manifest "$OUT_DIR/zakura-fixture-manifest.json")
fi
if [ "$TEST_PROFILE" = "zcashd-compat" ]; then
  MONITOR_ARGS+=(
    --zcashd-rpc-url http://127.0.0.1:8232
    --zcashd-cookie-file /mnt/zcashd/.cookie
    --lifecycle-file "$OUT_DIR/compat-lifecycle.json"
  )
  if [ -f "$OUT_DIR/zcashd-fixture-manifest.json" ]; then
    MONITOR_ARGS+=(--zcashd-fixture-manifest "$OUT_DIR/zcashd-fixture-manifest.json")
  fi
fi

MONITOR_RC=0
python3 /root/pr-node-monitor.py "${MONITOR_ARGS[@]}" || MONITOR_RC=$?
tail -n 2000 /var/log/zakura/zakura.log > "$OUT_DIR/zakura-tail.log" 2>/dev/null || true
zstd -T0 -q -f /var/log/zakura/zakura.log -o "$OUT_DIR/zakura-full.log.zst" 2>/dev/null || true

exit "$MONITOR_RC"
