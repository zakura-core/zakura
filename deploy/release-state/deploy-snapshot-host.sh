#!/usr/bin/env bash
# Build the offline exporter from this checkout's pinned commit and install the release-state
# publisher timer. This script does not restart either Zakura container, start a snapshot job,
# or enable the timer.
set -euo pipefail

TARGET=${1:-root@45.55.96.29}
EXPECTED_TARGET=root@45.55.96.29
EXPECTED_HOST=zakura-snapshot
REPOSITORY=https://github.com/zakura-core/zakura.git
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SOURCE_ROOT=$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)
EXPORTER_REVISION=$(git -C "$SOURCE_ROOT" rev-parse HEAD)
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
    ! systemctl is-active --quiet zakura-release-state.service
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
install -m 0755 "$WORK/source/deploy/release-state/publish-release-state.sh" \
    "$WORK/stage/bin/publish-release-state.sh"
install -m 0755 "$WORK/source/deploy/release-state/publish-from-archive-host.sh" \
    "$WORK/stage/bin/publish-from-archive-host.sh"
install -m 0644 "$WORK/source/deploy/release-state/zakura-release-state.service" \
    "$WORK/stage/systemd/zakura-release-state.service"
install -m 0644 "$WORK/source/deploy/release-state/zakura-release-state.timer" \
    "$WORK/stage/systemd/zakura-release-state.timer"
printf '%s\n' "$EXPORTER_REVISION" > "$WORK/stage/EXPORTER_REVISION"

ssh -o BatchMode=yes "$TARGET" "install -d -m 0700 '$REMOTE_STAGE'"
scp -q -r "$WORK/stage/." "$TARGET:$REMOTE_STAGE/"

ssh -o BatchMode=yes "$TARGET" "
    set -euo pipefail
    [ \"\$(hostname -s)\" = \"$EXPECTED_HOST\" ]
    ! systemctl is-active --quiet zakura-snapshot-pruned.service
    ! systemctl is-active --quiet zakura-snapshot.service
    ! systemctl is-active --quiet zakura-release-state.service
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
    install -m 0755 '$REMOTE_STAGE/bin/publish-from-archive-host.sh' \
        /opt/zakura-release-state/bin/publish-from-archive-host.sh
    install -m 0644 '$REMOTE_STAGE/EXPORTER_REVISION' \
        /opt/zakura-release-state/EXPORTER_REVISION

    # The publisher used to run as a stopped-node hook on the pruned snapshot
    # service. Remove that drop-in so a host mid-cutover cannot run both.
    rm -f /etc/systemd/system/zakura-snapshot-pruned.service.d/release-state.conf
    rmdir --ignore-fail-on-non-empty \
        /etc/systemd/system/zakura-snapshot-pruned.service.d 2>/dev/null || true
    install -m 0644 '$REMOTE_STAGE/systemd/zakura-release-state.service' \
        /etc/systemd/system/zakura-release-state.service
    install -m 0644 '$REMOTE_STAGE/systemd/zakura-release-state.timer' \
        /etc/systemd/system/zakura-release-state.timer
    systemctl daemon-reload
    systemd-analyze verify zakura-snapshot-pruned.service
    systemd-analyze verify zakura-release-state.timer

    # The unit reads the archive cache path from here. Installing without it
    # leaves the timer inert rather than exporting from the wrong database.
    if [ ! -e /etc/zakura-release-state.env ]; then
        echo 'note: create /etc/zakura-release-state.env with RELEASE_STATE_ARCHIVE_CACHE=... before enabling the timer' >&2
    fi

    /opt/zakura-release-state/bin/zakura-checkpoints --help >/dev/null
    bash -n /opt/zakura-release-state/bin/publish-release-state.sh
    bash -n /opt/zakura-release-state/bin/publish-from-archive-host.sh
"

echo "Installed the release-state publisher on $TARGET without enabling its timer."
