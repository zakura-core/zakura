#!/usr/bin/env bash
# Called by deploy.py with binaries built at one resolved commit. The existing
# publisher installation and credentials remain host-managed.
set -euo pipefail

STAGE=${1:?staged binaries required}
BIN_PATH=${2:?node binary path required}
NODE_SERVICE=${3:?node service required}
REVISION=${4:?revision required}
INSTALL_ROOT=/opt/zakura-release-state
PUBLISHER=zakura-release-state.service
TIMER=zakura-release-state.timer

[ "$(hostname -s)" = roman-zakura-archive-vct-off ]
[ "$BIN_PATH" = /usr/local/bin/zakurad ]
[ "$NODE_SERVICE" = zakurad ]
[[ "$REVISION" =~ ^[0-9a-f]{40}$ ]]
[ -x "$INSTALL_ROOT/bin/publish-from-archive-host.sh" ]
[ -f /etc/zakura-release-state.env ]
# Require the existing archive profile, which cannot skip for a snapshot job.
# shellcheck source=/dev/null
. "$INSTALL_ROOT/profile.env"
[ "${RELEASE_STATE_EXPECTED_HOST:-}" = roman-zakura-archive-vct-off ]
[ "${RELEASE_STATE_NODE_UNIT:-}" = zakurad.service ]
[ -z "${RELEASE_STATE_SNAPSHOT_UNIT:-}" ]
version=$("$STAGE/zakurad" --version)
[[ "$version" =~ \+([0-9]+\.)?g${REVISION:0:9} ]]
# The publisher scripts remain host-managed. A main ancestor can still predate
# their CLI contract. Include optional resume/cost arguments so later timer runs
# cannot discover an incompatibility after the binaries have been replaced.
require_options() {
    local tool=$1 help=$2 option
    shift 2
    for option in "$@"; do
        if ! grep -Eq -- "(^|[[:space:],])${option}([=[:space:]]|$)" <<< "$help"; then
            echo "$tool $REVISION lacks publisher option $option; refusing deployment." >&2
            exit 1
        fi
    done
}
require_options Exporter "$("$STAGE/zakura-checkpoints" --help)" \
    --state-cache-dir --full-list --mainnet-frontier-output \
    --mainnet-subtree-output --mainnet-frontier-grid-output \
    --mainnet-frontier-grid-input --frontier-grid-target-cost-ms
require_options "Spentness helper replay" "$("$STAGE/zakura-spentness" replay --help)" \
    --source --destination --height --block-hash
require_options "Spentness helper generate" "$("$STAGE/zakura-spentness" generate --help)" \
    --state --height --block-hash --output --commitment
require_options "Spentness helper verify" "$("$STAGE/zakura-spentness" verify --help)" \
    --state --artifact --commitment --report

# Keep concurrent deployments out until publication starts. A later deployment
# then waits on the publisher lock and fails before changing any binary.
exec 8>/run/zakura-release-state-deploy.lock
flock -w 600 8
RESUME_MARKER="$INSTALL_ROOT/deploy.resume-timer"
PAUSE_MARKER="$INSTALL_ROOT/deploy.paused"
TIMER_WAS_ACTIVE=false
# An inherited marker means an earlier attempt may have left a partial pair.
# Even an early failure of this retry must keep publication paused.
RESTORE_TIMER=true
if [ -f "$PAUSE_MARKER" ] || [ -f "$RESUME_MARKER" ]; then
    RESTORE_TIMER=false
fi
if systemctl is-active --quiet "$TIMER"; then
    touch "$RESUME_MARKER"
fi
if [ -f "$RESUME_MARKER" ]; then
    TIMER_WAS_ACTIVE=true
fi
cleanup() {
    status=$?
    trap - EXIT
    if $RESTORE_TIMER && $TIMER_WAS_ACTIVE; then
        if systemctl start "$TIMER"; then
            rm -f "$RESUME_MARKER"
        else
            status=1
        fi
    elif ! $RESTORE_TIMER; then
        echo "Publisher timer left stopped: repair the node/exporter pair before resuming publication." >&2
    fi
    exit "$status"
}
trap cleanup EXIT
systemctl stop "$TIMER"

# Do not stop a running export. It owns this same lock until upload and retention
# finish. A busy publisher fails this deployment before changing either binary.
exec 9>/run/zakura-release-state-publish.lock
flock -w 600 9
# The service may still be exiting after releasing its lock. Wait boundedly so
# the later start cannot accidentally join the preceding invocation.
for ((attempt = 0; attempt < 30; attempt++)); do
    state=$(systemctl show -p ActiveState --value "$PUBLISHER")
    case "$state" in
        inactive|failed) break ;;
    esac
    sleep 1
done
case "$state" in inactive|failed) ;; *) exit 1 ;; esac

# Stage on the destination filesystems before stopping the node. Rename avoids
# overwriting an executing binary. Once restart begins, do not automatically
# roll back: startup may already have migrated the database to a newer format.
install -m 755 "$STAGE/zakurad" "${BIN_PATH}.new"
install -m 755 "$STAGE/zakura-checkpoints" "$INSTALL_ROOT/bin/zakura-checkpoints.new"
install -m 755 "$STAGE/zakura-spentness" "$INSTALL_ROOT/bin/zakura-spentness.new"
printf '%s\n' "$REVISION" > "$INSTALL_ROOT/EXPORTER_REVISION.new"
# A persistent unit condition also blocks timer activation after a reboot.
# Install the guard before recording the pause, and record the pause before
# replacing either binary. It applies even when the timer was initially stopped.
install -d /etc/systemd/system/zakura-release-state.service.d
printf '[Unit]\nConditionPathExists=!%s\n' "$PAUSE_MARKER" \
    > /etc/systemd/system/zakura-release-state.service.d/deployment-pause.conf
systemctl daemon-reload
touch "$PAUSE_MARKER"
RESTORE_TIMER=false
systemctl stop "$NODE_SERVICE"
mv -f "$INSTALL_ROOT/bin/zakura-checkpoints.new" "$INSTALL_ROOT/bin/zakura-checkpoints"
mv -f "$INSTALL_ROOT/bin/zakura-spentness.new" "$INSTALL_ROOT/bin/zakura-spentness"
mv -f "$INSTALL_ROOT/EXPORTER_REVISION.new" "$INSTALL_ROOT/EXPORTER_REVISION"
mv -f "${BIN_PATH}.new" "$BIN_PATH"
# Clear historical automatic restarts before measuring this deployment. Keep
# requiring zero so even a restart before the first readiness poll is rejected.
systemctl reset-failed "$NODE_SERVICE"
systemctl start "$NODE_SERVICE"

# The archive node has unauthenticated loopback RPC. Wait for database migration,
# then observe a full 90-second settle window before exporting from that database.
rpc_ready() {
    curl -fsS --max-time 10 -H 'Content-Type: application/json' \
        --data '{"jsonrpc":"2.0","id":1,"method":"getblockchaininfo","params":[]}' \
        http://127.0.0.1:8232 | python3 -c '
import json, sys
value = json.load(sys.stdin)
assert value.get("error") is None
assert isinstance(value.get("result", {}).get("blocks"), int)
'
}
deadline=$((SECONDS + 600))
until rpc_ready >/dev/null 2>&1; do
    systemctl is-active --quiet "$NODE_SERVICE"
    [ "$SECONDS" -lt "$deadline" ]
    sleep 5
done
deadline=$((SECONDS + 90))
while [ "$SECONDS" -lt "$deadline" ]; do
    systemctl is-active --quiet "$NODE_SERVICE"
    sleep 5
done
[ "$(systemctl show -p NRestarts --value "$NODE_SERVICE")" = 0 ]
rpc_ready
pid=$(systemctl show -p MainPID --value "$NODE_SERVICE")
[[ "$pid" =~ ^[1-9][0-9]*$ ]]
[ "$(readlink "/proc/$pid/exe")" = "$BIN_PATH" ]
rm -f "$PAUSE_MARKER"
RESTORE_TIMER=true
cursor=$(journalctl -n 1 --show-cursor -o cat --no-pager | sed -n 's/^-- cursor: //p')
[ -n "$cursor" ]
flock -u 9
exec 9>&-

# A publication can run for days, so start it without waiting. A new invocation
# that is still running counts as started. A publication failure leaves the
# compatible pair installed and restores the timer for retries.
previous_invocation=$(systemctl show -p InvocationID --value "$PUBLISHER")
systemctl start --no-block "$PUBLISHER"
invocation=$previous_invocation
for ((attempt = 0; attempt < 60; attempt++)); do
    invocation=$(systemctl show -p InvocationID --value "$PUBLISHER")
    [ -n "$invocation" ] && [ "$invocation" != "$previous_invocation" ] && break
    sleep 1
done
if [ -z "$invocation" ] || [ "$invocation" = "$previous_invocation" ]; then
    echo "Release-state publication did not start." >&2
    exit 1
fi
state=$(systemctl show -p ActiveState --value "$PUBLISHER")
case "$state" in
    activating|active) ;;
    inactive)
        # A finished run must have moved the pointer after this deploy released
        # the lock. A skipped publication cannot pass.
        journalctl -u "$PUBLISHER" --after-cursor="$cursor" -o cat --no-pager | \
            grep -Eq '^pointer now at height [0-9]+$' || {
            echo "Release-state publication exited without publishing." >&2
            exit 1
        }
        ;;
    *)
        echo "Release-state publication failed to start (state $state)." >&2
        exit 1
        ;;
esac
echo "Installed node and publisher helpers $REVISION and started release-state publication."
