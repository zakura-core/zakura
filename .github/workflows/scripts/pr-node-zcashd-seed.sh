#!/usr/bin/env bash
# Copies a clean, stopped mainnet zcashd datadir from an explicitly approved
# seed Droplet onto a newly attached PR-node fixture volume.
set -euo pipefail

: "${ZCASHD_VOLUME_NAME:?missing ZCASHD_VOLUME_NAME}"
: "${ZCASHD_SEED_DATADIR:?missing ZCASHD_SEED_DATADIR}"
: "${ZCASHD_SEED_HEIGHT:?missing ZCASHD_SEED_HEIGHT}"
: "${ZCASHD_SEED_HASH:?missing ZCASHD_SEED_HASH}"
: "${ZCASHD_SEED_DROPLET_ID:?missing ZCASHD_SEED_DROPLET_ID}"
: "${ZCASHD_SEED_CHECKSUM_VERIFIED:?missing ZCASHD_SEED_CHECKSUM_VERIFIED}"

[[ "$ZCASHD_SEED_HEIGHT" =~ ^[0-9]+$ ]] || {
  echo "zcashd seed height must be a non-negative integer" >&2
  exit 1
}
[[ "$ZCASHD_SEED_HASH" =~ ^[0-9a-fA-F]{64}$ ]] || {
  echo "zcashd seed hash must be 64 hexadecimal characters" >&2
  exit 1
}
case "$ZCASHD_SEED_CHECKSUM_VERIFIED" in
  true|false) ;;
  *) echo "zcashd seed checksum verification must be true or false" >&2; exit 1 ;;
esac
if [[ "$ZCASHD_SEED_CHECKSUM_VERIFIED" == true ]]; then
  [[ "${ZCASHD_SEED_ARCHIVE_SHA256:-}" =~ ^[0-9a-fA-F]{64}$ ]] || {
    echo "a verified seed requires its archive SHA256" >&2
    exit 1
  }
  [[ -n "${ZCASHD_SEED_ARCHIVE_URL:-}" ]] || {
    echo "a verified seed requires its archive URL" >&2
    exit 1
  }
fi

SOURCE=$(realpath -e "$ZCASHD_SEED_DATADIR")
for directory in blocks chainstate unity; do
  [[ -d "$SOURCE/$directory" ]] || {
    echo "seed datadir is missing $SOURCE/$directory" >&2
    exit 1
  }
done

# Copying a live LevelDB datadir can yield a corrupt fixture even when the
# resulting filesystem snapshot itself is crash consistent.
for executable in /proc/[0-9]*/exe; do
  executable_name=$(basename "$(readlink -f "$executable" 2>/dev/null || true)")
  case "$executable_name" in
    zakurad*|zcashd*)
      echo "refusing to copy while $executable_name is running ($executable)" >&2
      exit 1
      ;;
  esac
done
for proc_path in /proc/[0-9]*/fd/* /proc/[0-9]*/cwd /proc/[0-9]*/root; do
  target=$(readlink -f "$proc_path" 2>/dev/null || true)
  case "$target" in
    "$SOURCE"|"$SOURCE"/*)
      echo "refusing to copy while a process has the datadir open: $proc_path -> $target" >&2
      exit 1
      ;;
  esac
done

DEVICE="/dev/disk/by-id/scsi-0DO_Volume_${ZCASHD_VOLUME_NAME}"
for _ in $(seq 1 60); do [[ -e "$DEVICE" ]] && break; sleep 2; done
[[ -e "$DEVICE" ]] || { echo "zcashd fixture volume device not found: $DEVICE" >&2; exit 1; }

MOUNT=/mnt/zakura-pr-zcashd-seed
mkdir -p "$MOUNT"
if ! blkid "$DEVICE" >/dev/null 2>&1; then
  mkfs.ext4 -q -L zakura-zcashd-fixture "$DEVICE"
fi
existing_mount=$(findmnt -rn -S "$DEVICE" -o TARGET | head -n 1 || true)
if [[ -n "$existing_mount" && "$existing_mount" != "$MOUNT" ]]; then
  umount "$existing_mount"
fi
mountpoint -q "$MOUNT" || mount "$DEVICE" "$MOUNT"

cleanup() {
  sync
  mountpoint -q "$MOUNT" && umount "$MOUNT"
}
trap cleanup EXIT

shopt -s nullglob dotglob
existing=("$MOUNT"/*)
shopt -u nullglob dotglob
(( ${#existing[@]} == 1 )) && [[ "${existing[0]}" == "$MOUNT/lost+found" ]] && existing=()
(( ${#existing[@]} == 0 )) || {
  echo "refusing to seed a non-empty fixture volume" >&2
  printf 'existing path: %s\n' "${existing[@]}" >&2
  exit 1
}

for directory in blocks chainstate unity; do
  echo "Copying $SOURCE/$directory"
  cp -a "$SOURCE/$directory" "$MOUNT/$directory"
done

STATUS=verified
[[ "$ZCASHD_SEED_CHECKSUM_VERIFIED" == true ]] || STATUS=candidate
export STATUS
python3 - "$MOUNT/fixture-manifest.json.tmp" <<'PY'
import json
import os
import sys
from datetime import datetime, timezone

manifest = {
    "schema_version": 1,
    "fixture": "zcashd-mainnet",
    "network": "mainnet",
    "status": os.environ["STATUS"],
    "captured_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    "origin": {
        "kind": "existing_droplet",
        "droplet_id": os.environ["ZCASHD_SEED_DROPLET_ID"],
        "datadir": os.environ["ZCASHD_SEED_DATADIR"],
        "archive_url": os.environ.get("ZCASHD_SEED_ARCHIVE_URL") or None,
        "archive_sha256": os.environ.get("ZCASHD_SEED_ARCHIVE_SHA256") or None,
        "archive_checksum_verified": os.environ["ZCASHD_SEED_CHECKSUM_VERIFIED"] == "true",
    },
    "tip": {
        "height": int(os.environ["ZCASHD_SEED_HEIGHT"]),
        "hash": os.environ["ZCASHD_SEED_HASH"].lower(),
    },
    "contents": ["blocks", "chainstate", "unity", "fixture-manifest.json"],
}
with open(sys.argv[1], "w", encoding="utf-8") as output:
    json.dump(manifest, output, indent=2, sort_keys=True)
    output.write("\n")
PY
mv "$MOUNT/fixture-manifest.json.tmp" "$MOUNT/fixture-manifest.json"
sync
echo "Seed fixture copied with status $STATUS."
