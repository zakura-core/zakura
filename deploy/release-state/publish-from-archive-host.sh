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
SNAPSHOT_UNIT=zakura-snapshot.service
INSTALL_ROOT=/opt/zakura-release-state
R2_ENDPOINT=https://152e2a8834283136c2f0575782b1b7aa.r2.cloudflarestorage.com
R2_BUCKET=zakura-release-state
PUBLIC_BASE=https://zakura-release.valargroup.dev/release-state

die() {
    echo "release-state publisher: $*" >&2
    exit 1
}

note() {
    echo "release-state publisher: $*" >&2
}

# Deliberately the same predicate check-and-publish.sh uses, so both sides agree on what "a
# snapshot is running" means. Reading the unit rather than the lock matters: the snapshot side
# decides from the unit too, so a lock this script held would not defer a snapshot, it would
# start one that then died on the lock.
snapshot_publish_active() {
    local state
    state=$(systemctl show -p ActiveState --value "$SNAPSHOT_UNIT" 2>/dev/null) || return 1
    [ "$state" = active ] || [ "$state" = activating ]
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

    # The archive snapshot job stops this container to tar a quarter-terabyte of state, and it
    # triggers on snapshot age rather than a fixed hour, so its window drifts across this timer's
    # every few days. Skip rather than fail: the import side is weekly, so a missed daily export
    # costs nothing but a slightly older bundle, whereas a failed unit costs an alert.
    if snapshot_publish_active; then
        note "$SNAPSHOT_UNIT is running; skipping this export"
        exit 0
    fi

    # A stopped container is not fatal. The exporter reads the cache as a read-only RocksDB
    # secondary, which works just as well against a quiesced database — that is what the
    # pre-secondary design did on purpose. This check exists to catch a cache path that belongs
    # to no container at all, so it warns rather than refusing.
    running=$(docker inspect --format '{{.State.Running}}' "$CONTAINER" 2>/dev/null) \
        || die "cannot inspect container $CONTAINER"
    [ "$running" = true ] \
        || note "warning: $CONTAINER is not running; exporting from its cache as a quiesced database"

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
