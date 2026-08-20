#!/usr/bin/env bash
# Host wrapper for the release-state publisher timer.
#
# Exports from the archive node's live cache directory: the exporter opens it as
# a read-only RocksDB secondary, so the container keeps running throughout. The
# pruned cache is not a valid source — the frontier grid covers heights a pruned
# database no longer holds.
set -euo pipefail

EXPECTED_HOST=zakura-snapshot
CONTAINER=zakura
INSTALL_ROOT=/opt/zakura-release-state
R2_ENDPOINT=https://152e2a8834283136c2f0575782b1b7aa.r2.cloudflarestorage.com
R2_BUCKET=zakura-release-state
PUBLIC_BASE=https://zakura-release.valargroup.dev/release-state

die() {
    echo "release-state publisher: $*" >&2
    exit 1
}

main() {
    local state_dir running

    # No default: the archive cache path is host configuration, and silently
    # falling back to the wrong directory would publish a bundle from a database
    # that cannot produce a complete one.
    state_dir=${1:-${RELEASE_STATE_ARCHIVE_CACHE:-}}
    [ -n "$state_dir" ] \
        || die "set RELEASE_STATE_ARCHIVE_CACHE to the archive node's cache directory"

    [ "$(hostname -s)" = "$EXPECTED_HOST" ] \
        || die "must run on $EXPECTED_HOST"
    [ -d "$state_dir" ] \
        || die "cache directory does not exist: $state_dir"

    running=$(docker inspect --format '{{.State.Running}}' "$CONTAINER" 2>/dev/null) \
        || die "cannot inspect container $CONTAINER"
    [ "$running" = true ] \
        || die "container $CONTAINER must be running so its cache is a live archive node"

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
