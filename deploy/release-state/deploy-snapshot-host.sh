#!/usr/bin/env bash
# Build the pinned offline exporter and install the snapshot-host hook.
# This script does not restart either Zakura container or start a snapshot job.
set -euo pipefail

TARGET=${1:-root@45.55.96.29}
EXPECTED_TARGET=root@45.55.96.29
EXPECTED_HOST=zakura-snapshot
EXPORTER_REVISION=d1fed3e6e0e420571ecacb9e1984dea6353cc7a3
REPOSITORY=https://github.com/zakura-core/zakura.git
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
WORK=$(mktemp -d "${TMPDIR:-/tmp}/zakura-release-state-deploy.XXXXXX")
BUILD_TARGET=${ZAKURA_RELEASE_STATE_BUILD_TARGET:-"$HOME/.cache/zakura-release-state-target"}
REMOTE_STAGE="/tmp/zakura-release-state-deploy-$$"
trap 'rm -rf "$WORK"; ssh -o BatchMode=yes "$TARGET" "rm -rf \"$REMOTE_STAGE\"" >/dev/null 2>&1 || true' EXIT

die() {
    echo "release-state deployment: $*" >&2
    exit 1
}

[ "$TARGET" = "$EXPECTED_TARGET" ] \
    || die "refusing unexpected target $TARGET (expected $EXPECTED_TARGET)"
for command in cargo git install scp ssh; do
    command -v "$command" >/dev/null 2>&1 || die "missing local command: $command"
done

ssh -o BatchMode=yes "$TARGET" "
    set -e
    [ \"\$(hostname -s)\" = \"$EXPECTED_HOST\" ]
    ! systemctl is-active --quiet zakura-snapshot-pruned.service
    ! systemctl is-active --quiet zakura-snapshot.service
    [ \"\$(docker inspect --format '{{.State.Running}}' zakura-pruned)\" = true ]
    [ \"\$(docker inspect --format '{{.State.Running}}' zakura)\" = true ]
" || die "host identity or inactive-publisher preflight failed"

echo "Building zakura-checkpoints at pinned main revision $EXPORTER_REVISION"
git clone --quiet --filter=blob:none "$REPOSITORY" "$WORK/source"
git -C "$WORK/source" fetch --quiet origin main
git -C "$WORK/source" merge-base --is-ancestor "$EXPORTER_REVISION" origin/main \
    || die "pinned exporter revision is no longer verifiable on main"
git -C "$WORK/source" checkout --quiet --detach "$EXPORTER_REVISION"
CARGO_TARGET_DIR="$BUILD_TARGET" cargo build --locked --release \
    --manifest-path "$WORK/source/Cargo.toml" \
    -p zakura-utils --features zakura-checkpoints-offline --bin zakura-checkpoints

install -d "$WORK/stage/bin" "$WORK/stage/systemd"
install -m 0755 "$BUILD_TARGET/release/zakura-checkpoints" \
    "$WORK/stage/bin/zakura-checkpoints"
install -m 0755 "$SCRIPT_DIR/publish-release-state.sh" \
    "$WORK/stage/bin/publish-release-state.sh"
install -m 0755 "$SCRIPT_DIR/publish-from-snapshot-host.sh" \
    "$WORK/stage/bin/publish-from-snapshot-host.sh"
install -m 0644 "$SCRIPT_DIR/zakura-snapshot-pruned-release-state.conf" \
    "$WORK/stage/systemd/release-state.conf"
printf '%s\n' "$EXPORTER_REVISION" > "$WORK/stage/EXPORTER_REVISION"

ssh -o BatchMode=yes "$TARGET" "install -d -m 0700 '$REMOTE_STAGE'"
scp -q -r "$WORK/stage/." "$TARGET:$REMOTE_STAGE/"

ssh -o BatchMode=yes "$TARGET" "
    set -euo pipefail
    [ \"\$(hostname -s)\" = \"$EXPECTED_HOST\" ]
    ! systemctl is-active --quiet zakura-snapshot-pruned.service
    ! systemctl is-active --quiet zakura-snapshot.service
    [ \"\$(docker inspect --format '{{.State.Running}}' zakura-pruned)\" = true ]
    [ \"\$(docker inspect --format '{{.State.Running}}' zakura)\" = true ]

    if ! command -v rclone >/dev/null 2>&1; then
        apt-get update -qq
        DEBIAN_FRONTEND=noninteractive apt-get install -y -qq rclone
    fi

    install -d -m 0755 /opt/zakura-release-state/bin
    install -m 0755 '$REMOTE_STAGE/bin/zakura-checkpoints' \
        /opt/zakura-release-state/bin/zakura-checkpoints
    install -m 0755 '$REMOTE_STAGE/bin/publish-release-state.sh' \
        /opt/zakura-release-state/bin/publish-release-state.sh
    install -m 0755 '$REMOTE_STAGE/bin/publish-from-snapshot-host.sh' \
        /opt/zakura-release-state/bin/publish-from-snapshot-host.sh
    install -m 0644 '$REMOTE_STAGE/EXPORTER_REVISION' \
        /opt/zakura-release-state/EXPORTER_REVISION

    install -d -m 0755 \
        /etc/systemd/system/zakura-snapshot-pruned.service.d
    install -m 0644 '$REMOTE_STAGE/systemd/release-state.conf' \
        /etc/systemd/system/zakura-snapshot-pruned.service.d/release-state.conf
    systemctl daemon-reload
    systemd-analyze verify zakura-snapshot-pruned.service

    /opt/zakura-release-state/bin/zakura-checkpoints --help >/dev/null
    bash -n /opt/zakura-release-state/bin/publish-release-state.sh
    bash -n /opt/zakura-release-state/bin/publish-from-snapshot-host.sh
"

echo "Installed release-state hook on $TARGET without starting either publisher."
