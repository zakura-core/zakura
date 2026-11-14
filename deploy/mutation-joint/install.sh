#!/usr/bin/env bash
# Installs the joint-run timer on the remote host (mutation plan, mut-4.6).
#
# Usage, as root on the host, from any checkout:
#
#   deploy/mutation-joint/install.sh [--checkout <dir>] [--state <dir>]
#
# It clones the repository into --checkout (default
# /root/mutation-joint/zakura) if it isn't there, writes
# /etc/zakura-mutation-joint.env from the example if it's missing, installs
# the service and timer, and enables the timer. It never starts a run; start
# one with `systemctl start zakura-mutation-joint.service`.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
checkout=/root/mutation-joint/zakura
state=/root/mutation-joint/state
while [ $# -gt 0 ]; do
    case "$1" in
        --checkout) checkout="${2:?--checkout needs a directory}"; shift 2 ;;
        --state) state="${2:?--state needs a directory}"; shift 2 ;;
        *) sed -n '/^# Usage/,/^# It clones/p' "$0" | sed '$d; s/^# \{0,1\}//' >&2; exit 2 ;;
    esac
done

if [ ! -d "${checkout}/.git" ]; then
    git clone --quiet "$(git -C "${here}" remote get-url origin)" "${checkout}"
fi
mkdir -p "${state}"
env=/etc/zakura-mutation-joint.env
if [ ! -e "${env}" ]; then
    sed -e "s|^ZAKURA_MUTATION_CHECKOUT=.*|ZAKURA_MUTATION_CHECKOUT=${checkout}|" \
        -e "s|^ZAKURA_MUTATION_STATE=.*|ZAKURA_MUTATION_STATE=${state}|" \
        "${here}/zakura-mutation-joint.env.example" > "${env}"
fi
install -m 0644 "${here}/zakura-mutation-joint.service" "${here}/zakura-mutation-joint.timer" \
    /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now zakura-mutation-joint.timer
systemctl list-timers zakura-mutation-joint.timer --no-pager
