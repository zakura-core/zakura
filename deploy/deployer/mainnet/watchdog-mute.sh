#!/usr/bin/env bash
# Manage the mainnet deployment mute files read by the two watchdog checks.
set -euo pipefail

fleet_marker=/run/zakura-fleet-watchdog/deploy-suppressed-until
compat_marker=/run/zakura-watchdog/deployment-suppressed-until

compat_ssh() {
    local deadline=$1
    shift
    timeout --signal=TERM --kill-after=5s "$deadline" ssh \
        -o BatchMode=yes -o ConnectTimeout=20 -o StrictHostKeyChecking=yes \
        -o ServerAliveInterval=10 -o ServerAliveCountMax=3 \
        -- root@159.203.113.196 "$@"
}

preserve_fleet_runtime() {
    local unit=zakura-fleet-watchdog.service
    local drop_in="/etc/systemd/system/${unit}.d/60-deploy-mute.conf"
    printf '[Service]\nRuntimeDirectoryPreserve=restart\n' |
        sudo -n install -D -m 644 /dev/stdin "$drop_in"
    sudo -n systemctl daemon-reload

    # On a fresh runner the first watchdog install will load the drop-in.
    local load_state
    load_state=$(systemctl show --property=LoadState --value "$unit")
    if [[ "$load_state" == not-found ]]; then
        return
    fi
    if [[ $(systemctl show --property=RuntimeDirectoryPreserve --value "$unit") != restart ]]; then
        echo '::error::The fleet watchdog would remove its mute file on restart.' >&2
        return 1
    fi
}

arm_marker() (
    local marker=$1 expires_at temporary
    install -d -m 755 "$(dirname "$marker")"
    temporary=$(mktemp "${marker}.tmp.XXXXXX")
    trap 'rm -f -- "$temporary"' EXIT
    # Calculate the deadline on the host that reads the marker.
    expires_at=$(( $(date +%s) + 1200 ))
    printf '%s\n' "$expires_at" > "$temporary"
    chmod 644 "$temporary"
    mv -f -- "$temporary" "$marker"
    printf '%s\n' "$expires_at"
)

clear_marker() {
    local marker=$1 expires_at=$2 current
    [[ "$expires_at" =~ ^[0-9]+$ ]]
    if [[ -f "$marker" ]]; then
        current=$(cat -- "$marker")
        if [[ "$current" == "$expires_at" ]]; then
            rm -f -- "$marker"
        fi
    fi
}

case "${1:-}" in
    arm-fleet)
        preserve_fleet_runtime
        sudo -n bash "$0" arm-marker "$fleet_marker"
        ;;
    arm-compat)
        compat_ssh 60s bash -s -- arm-marker "$compat_marker" < "$0"
        ;;
    clear-fleet)
        sudo -n bash "$0" clear-marker "$fleet_marker" "$2"
        ;;
    clear-compat)
        [[ "$2" =~ ^[0-9]+$ ]]
        compat_ssh 60s bash -s -- clear-marker "$compat_marker" "$2" < "$0"
        ;;
    arm-marker) arm_marker "$2" ;;
    clear-marker) clear_marker "$2" "$3" ;;
    *) echo "Unknown deployment mute operation." >&2; exit 2 ;;
esac
