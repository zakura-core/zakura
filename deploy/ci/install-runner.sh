#!/usr/bin/env bash
# Install on a prepared, dedicated host. Read the short-lived registration token
# from stdin; keep registered-runner credentials outside all job-writable paths.
set -euo pipefail
[[ $EUID == 0 ]] || { echo 'Run as root on the dedicated host.' >&2; exit 1; }
readonly repo=zakura-core/zakura
readonly version=2.338.2
readonly checksum=baebf204307bac6193480852732f0aba2ae97b6c5597c244f4d5558390147c40
readonly root=/opt/zakura-ci
readonly registration="$root/registration"
readonly name=zakura-ci-temporary-01
read -r token
[[ -n "$token" ]] || { echo 'A registration token is required on stdin.' >&2; exit 1; }
[[ ! -e "$registration" ]] || { echo 'Runner already registered; inspect it before changing registration.' >&2; exit 1; }
id zakura-ci >/dev/null
install -d -m 755 /var/lib/zakura-ci
install -d -o zakura-ci -g zakura-ci -m 700 "$registration"
archive=$(mktemp)
trap 'rm -f "$archive"' EXIT
curl --max-time 120 -fsSL -o "$archive" "https://github.com/actions/runner/releases/download/v$version/actions-runner-linux-x64-$version.tar.gz"
printf '%s  %s\n' "$checksum" "$archive" | sha256sum --check --status
tar -xzf "$archive" -C "$registration"
chown -R zakura-ci:zakura-ci "$registration"
# config.sh only accepts the single-use registration token as an argument.
(cd "$registration" && runuser -u zakura-ci -- env HOME=/var/lib/zakura-ci/home \
  ./config.sh --unattended --url "https://github.com/$repo" \
  --token "$token" --name "$name" --labels zakura-ci-temporary --disableupdate)
unset token
rm -rf /var/lib/zakura-ci/home
chown -R root:root "$registration"
chmod 700 "$registration"
install -m 755 "$(dirname "$0")/run-isolated-job.sh" "$root/run-isolated-job.sh"
install -m 644 "$(dirname "$0")/zakura-ci-runner.service" /etc/systemd/system/zakura-ci-runner.service
# Jobs need public downloads and loopback tests, but no cloud metadata, private
# networks, host SSH, or access to another service on the runner's public IP.
for network in 100.64.0.0/10 10.0.0.0/8 172.16.0.0/12 192.168.0.0/16 169.254.0.0/16; do
  iptables -C OUTPUT -m owner --uid-owner zakura-ci -d "$network" -j REJECT 2>/dev/null || \
    iptables -I OUTPUT -m owner --uid-owner zakura-ci -d "$network" -j REJECT
done
iptables -C OUTPUT -m owner --uid-owner zakura-ci -p tcp --dport 22 -j REJECT 2>/dev/null || \
  iptables -I OUTPUT -m owner --uid-owner zakura-ci -p tcp --dport 22 -j REJECT
for network in fc00::/7 fe80::/10; do
  ip6tables -C OUTPUT -m owner --uid-owner zakura-ci -d "$network" -j REJECT 2>/dev/null || \
    ip6tables -I OUTPUT -m owner --uid-owner zakura-ci -d "$network" -j REJECT
done
# The host's system resolver uses a public upstream and remains available to jobs.
# A reboot requires reapplying these rules before starting the runner.
systemctl daemon-reload
systemctl start zakura-ci-runner.service
systemctl is-active zakura-ci-runner.service
