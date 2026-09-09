#!/usr/bin/env bash
# Runs on the Actions runner; credentials never move between the two droplets.
set -euo pipefail
umask 077
SSH_OPTS=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ServerAliveInterval=30 -o ServerAliveCountMax=20 -i /tmp/do_ssh)
scp_to() { local ip=$1; shift; scp "${SSH_OPTS[@]}" "$@" "root@${ip}:/root/"; }
# Arguments are intentional runner-generated remote commands, never user input.
# shellcheck disable=SC2029
remote() { local ip=$1; shift; ssh "${SSH_OPTS[@]}" "root@${ip}" "$@"; }
cleanup() { rm -f /tmp/profile-baseline.env /tmp/profile-primary.env; }
trap cleanup EXIT
write_env() {
  local leg=$1 sha=$2 volume=$3
  {
    printf 'GH_REPO=%q\n' "$GITHUB_REPOSITORY"
    printf 'GH_CLONE_TOKEN=%q\n' "$GH_CLONE_TOKEN"
    printf 'LEG=%q\nSHA=%q\nVOLUME_NAME=%q\n' "$leg" "$sha" "$volume"
    printf 'GITHUB_RUN_URL=%q\nGITHUB_RUN_ID=%q\n' "$GITHUB_RUN_URL" "$GITHUB_RUN_ID"
    cat <<'ENV'
WORKLOAD=historical_sync
VERIFY_MODE=checkpoint
PROFILE=diagnostic
STOP_HEIGHT=1750000
WALL_CAP=1800
START_HEIGHT=1707210
HEAD_PROFILE_MINUTES=60
PEERSET_SIZE=75
P2P_STACK=zakura
VCT_FAST_SYNC=auto
ENABLE_TRACES=true
COMPARISON=refs
HISTORICAL_PRUNED=true
NEEDRESTART_MODE=l
ENV
  } > "/tmp/profile-${leg}.env"
}
write_env baseline 1b77bb18a7f4f915d760971e97083896221f70bf "$BASELINE_VOLUME"
write_env primary c8cde61726d2822a758ae08815efce3dcdd16bc3 "$PRIMARY_VOLUME"
for leg in baseline primary; do
  if [[ "$leg" == baseline ]]; then ip=$BASELINE_IP; volume=$BASELINE_VOLUME; else ip=$PRIMARY_IP; volume=$PRIMARY_VOLUME; fi
  scp_to "$ip" .github/workflows/scripts/perf-bench-run.sh .github/workflows/scripts/profile-replay-setup.sh scripts/zakura-bench-digest.py scripts/zakura-sync-profile-sample.py scripts/zakura-metrics-dashboard.py
  printf -v setup 'timeout 20m bash /root/profile-replay-setup.sh %q' "$volume"
  remote "$ip" "$setup"
done
# Prepare once on the original binary, then stream that exact state to the other host.
scp "${SSH_OPTS[@]}" /tmp/profile-baseline.env "root@${BASELINE_IP}:/root/run.env"
remote "$BASELINE_IP" 'set -euo pipefail; set -a; . /root/run.env; set +a; PROFILE=off PREPARE_ONLY=true FRESH_STATE_COPY=true OUT_DIR=/root/out/preparation timeout 45m bash /root/perf-bench-run.sh' > /tmp/profile-preparation.log 2>&1
remote "$PRIMARY_IP" 'set -euo pipefail; test ! -e /mnt/snapshots/perf-prepared-common; mkdir /mnt/snapshots/perf-prepared-common; mkdir -p /root/out/preparation'
remote "$BASELINE_IP" 'timeout 20m tar -C /mnt/snapshots/perf-prepared-common -cf - .' | remote "$PRIMARY_IP" 'timeout 20m tar -C /mnt/snapshots/perf-prepared-common -xf -'
remote "$BASELINE_IP" 'tar -C /root/out/preparation -cf - .' | remote "$PRIMARY_IP" 'tar -C /root/out/preparation -xf -'
# Candidate source is fetched only after common preparation. Both helpers verify
# independent copies against the same manifest before starting their measured nodes.
run_leg() {
  local ip=$1 leg=$2
  scp "${SSH_OPTS[@]}" "/tmp/profile-${leg}.env" "root@${ip}:/root/run.env"
  remote "$ip" 'set -euo pipefail; set -a; . /root/run.env; set +a; FRESH_STATE_COPY=true PREPARED_STATE_DIR=/mnt/snapshots/perf-prepared-common COMMON_PREPARATION_DIR=/root/out/preparation timeout 90m bash /root/perf-bench-run.sh; dpkg-query -W > /root/out/packages-after.txt'
}
run_leg "$BASELINE_IP" baseline > /tmp/profile-baseline.log 2>&1 &
baseline_pid=$!
run_leg "$PRIMARY_IP" primary > /tmp/profile-primary.log 2>&1 &
primary_pid=$!
baseline_status=0; primary_status=0
wait "$baseline_pid" || baseline_status=$?
wait "$primary_pid" || primary_status=$?
printf 'Replay statuses: baseline=%s primary=%s\n' "$baseline_status" "$primary_status"
[[ "$baseline_status" == 0 && "$primary_status" == 0 ]]
