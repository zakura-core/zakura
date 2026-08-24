#!/usr/bin/env bash
# Host wrapper for the release-state publisher timer.
#
# Exports from the archive node's live cache directory: the exporter opens it as
# a read-only RocksDB secondary, so the container keeps running throughout. The
# pruned cache is not a valid source — the frontier grid covers heights a pruned
# database no longer holds.
set -euo pipefail

INSTALL_ROOT=/opt/zakura-release-state
# Written by deploy-snapshot-host.sh from the profile it installed, so the identity
# and liveness checks below describe this host rather than a hardcoded one. Absent
# only on an installation predating profiles, where the defaults reproduce the
# original snapshot-host behaviour.
PROFILE_ENV="$INSTALL_ROOT/profile.env"
# shellcheck source=/dev/null
[ -r "$PROFILE_ENV" ] && . "$PROFILE_ENV"
EXPECTED_HOST=${RELEASE_STATE_EXPECTED_HOST:-zakura-snapshot}
# Unset and empty differ: empty means this profile has no container to check.
CONTAINER=${RELEASE_STATE_NODE_CONTAINER-zakura}
NODE_UNIT=${RELEASE_STATE_NODE_UNIT-}
SNAPSHOT_UNIT=${RELEASE_STATE_SNAPSHOT_UNIT-zakura-snapshot.service}
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

# A host that keeps R2 credentials in a secrets manager sets
# RELEASE_STATE_SECRETS_WRAPPER to a command that injects them and execs its
# argument. A host that keeps them in the EnvironmentFile leaves it unset. The
# guard variable stops the wrapper from re-entering this script forever.
maybe_reexec_with_secrets() {
    [ -n "${RELEASE_STATE_SECRETS_WRAPPER:-}" ] || return 0
    [ -z "${RELEASE_STATE_SECRETS_REEXEC:-}" ] || return 0
    [ -x "$RELEASE_STATE_SECRETS_WRAPPER" ] \
        || die "RELEASE_STATE_SECRETS_WRAPPER is not executable: $RELEASE_STATE_SECRETS_WRAPPER"
    export RELEASE_STATE_SECRETS_REEXEC=1
    exec "$RELEASE_STATE_SECRETS_WRAPPER" "$0" "$@"
}

# Deliberately the same predicate check-and-publish.sh uses, so both sides agree on
# what "a snapshot is running" means. Reading the unit rather than taking the lock
# matters: the snapshot side decides from the unit too, so a lock held here would
# not defer a snapshot, it would start one that then died on the lock. Reports
# false on a host with no such unit, which is the archive profile.
snapshot_publish_active() {
    [ -n "$SNAPSHOT_UNIT" ] || return 1
    local state
    state=$(systemctl show -p ActiveState --value "$SNAPSHOT_UNIT" 2>/dev/null) || return 1
    [ "$state" = active ] || [ "$state" = activating ]
}

# True when the node owning this cache looks alive: docker on the snapshot
# profile, systemd on the archive profile, nothing to check when neither is
# configured. Advisory either way — the exporter reads a read-only RocksDB
# secondary, which works against a quiesced database too.
node_looks_live() {
    if [ -n "$CONTAINER" ] && command -v docker >/dev/null 2>&1; then
        [ "$(docker inspect --format '{{.State.Running}}' "$CONTAINER" 2>/dev/null)" = true ]
        return
    fi
    if [ -n "$NODE_UNIT" ]; then
        systemctl is-active --quiet "$NODE_UNIT"
        return
    fi
    return 0
}

main() {
    local state_dir

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

    # Only the snapshot profile shares a host with a job that stops the node to tar a
    # quarter-terabyte of state, and that job triggers on snapshot age rather than a
    # fixed hour, so its window drifts across this timer's. Skip rather than fail: the
    # import side is weekly, so a missed daily export costs a slightly older bundle
    # while a failed unit costs an alert.
    if snapshot_publish_active; then
        note "$SNAPSHOT_UNIT is running; skipping this export"
        exit 0
    fi

    # A stopped node is not fatal: the exporter reads a read-only RocksDB secondary,
    # which works against a quiesced database too — that is what the pre-secondary
    # design did on purpose. So this warns rather than refusing.
    node_looks_live \
        || note "warning: the node owning this cache is not running; exporting from a quiesced database"

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
    maybe_reexec_with_secrets "$@"
    main "$@"
fi
