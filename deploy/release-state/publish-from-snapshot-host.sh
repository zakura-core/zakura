#!/usr/bin/env bash
# Stopped-node hook for zakura-snapshot-pruned.service.
set -euo pipefail

EXPECTED_HOST=zakura-snapshot
EXPECTED_CACHE=/mnt/zec_snapshot/zakura-cache-pruned
CONTAINER=zakura-pruned
INSTALL_ROOT=/opt/zakura-release-state
R2_ENDPOINT=https://152e2a8834283136c2f0575782b1b7aa.r2.cloudflarestorage.com
R2_BUCKET=zakura-release-state
PUBLIC_BASE=https://zakura-release.valargroup.dev/release-state

die() {
    echo "release-state snapshot hook: $*" >&2
    exit 1
}

main() {
    local state_dir running
    state_dir=${1:?usage: publish-from-snapshot-host.sh <stopped-node-zakura-cache-dir>}

    [ "$(hostname -s)" = "$EXPECTED_HOST" ] \
        || die "must run on $EXPECTED_HOST"
    [ "$state_dir" = "$EXPECTED_CACHE" ] \
        || die "expected cache $EXPECTED_CACHE, got $state_dir"
    [ -d "$state_dir" ] \
        || die "cache directory does not exist: $state_dir"

    running=$(docker inspect --format '{{.State.Running}}' "$CONTAINER" 2>/dev/null) \
        || die "cannot inspect container $CONTAINER"
    [ "$running" = false ] \
        || die "container $CONTAINER must be stopped before export"

    : "${R2_ACCESS_KEY_ID:?R2 access key was not injected}"
    : "${R2_SECRET_ACCESS_KEY:?R2 secret key was not injected}"

    # rclone reads this named remote entirely from the process environment. No
    # credentials are written to disk or included in command-line arguments.
    export RCLONE_CONFIG_RELEASE_TYPE=s3
    export RCLONE_CONFIG_RELEASE_PROVIDER=Cloudflare
    export RCLONE_CONFIG_RELEASE_ACCESS_KEY_ID="$R2_ACCESS_KEY_ID"
    export RCLONE_CONFIG_RELEASE_SECRET_ACCESS_KEY="$R2_SECRET_ACCESS_KEY"
    export RCLONE_CONFIG_RELEASE_ENDPOINT="$R2_ENDPOINT"
    export RCLONE_CONFIG_RELEASE_NO_CHECK_BUCKET=true
    unset R2_ACCESS_KEY_ID R2_SECRET_ACCESS_KEY

    export RELEASE_STATE_R2_REMOTE="release:${R2_BUCKET}"
    export RELEASE_STATE_PUBLIC_BASE="$PUBLIC_BASE"
    export RELEASE_STATE_LOCK_FILE=/run/zakura-release-state-publish.lock
    export ZAKURA_CHECKPOINTS_BIN="$INSTALL_ROOT/bin/zakura-checkpoints"

    exec "$INSTALL_ROOT/bin/publish-release-state.sh" "$state_dir"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    main "$@"
fi
