#!/usr/bin/env bash
# perf-lab frozen cohort: two seeded-then-frozen serving droplets that serve
# ONLY the bench over a private dev_network tag (B-15, approved 2026-07-22).
#   cohort.sh seed NAME     provision + seed state to COHORT_SEED_STOP (keeps the fork)
#   cohort.sh freeze NAME   write serve.toml (cohort tag, no public peers) + start zakurad
#   cohort.sh peers         capture node_id@ip:8234 from both serve logs -> config.env
#   cohort.sh status NAME | stop NAME
# Names must be perf-lab-serve-a / perf-lab-serve-b (droplet.sh guards apply).
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$DIR/config.env"
SSH="${SSH_BIN:-ssh}"
SSH_OPTS=(-i "$SSH_KEY_FILE" -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10)
die() { echo "cohort.sh: $*" >&2; exit 1; }
check_serve_name() { case "$1" in perf-lab-serve-a|perf-lab-serve-b) ;; *) die "serve name must be perf-lab-serve-a|b: $1";; esac; }
ip_of() { bash "$DIR/droplet.sh" ip "$1"; }

cmd_seed() {
  local name="${1:?usage: cohort.sh seed NAME}"; check_serve_name "$name"
  bash "$DIR/droplet.sh" provision "${name#perf-lab-}"
  local ip; ip="$(ip_of "$name")"; [ -n "$ip" ] || die "no ip for $name"
  # Seed = one harness run whose fork we keep. SKIP_BASELINE=1 → single leg;
  # public peers; generous wall cap for the longer range.
  # shellcheck disable=SC2087  # client-side expansion intended
  $SSH "${SSH_OPTS[@]}" "root@$ip" bash -s <<REMOTE
set -euo pipefail
rm -rf ${BENCH_OUT_REMOTE}/seed ${BENCH_OUT_REMOTE}/seed.log ${BENCH_OUT_REMOTE}/seed.pid ${BENCH_OUT_REMOTE}/seed.exit
rm -rf ${BENCH_HOME_REMOTE}/forks/*
mkdir -p ${BENCH_OUT_REMOTE}/seed
cd ${CTL_CLONE_REMOTE}
( nohup env \
    BUILD_REF='main' SKIP_BASELINE=1 \
    TARGET_P2P_STACK=zakura \
    BENCH_HOME='${BENCH_HOME_REMOTE}' \
    STOP_HEIGHT='${COHORT_SEED_STOP}' WALL_CAP=3600 \
    OUT_DIR='${BENCH_OUT_REMOTE}/seed' DASHBOARD=0 \
    bash scripts/checkpoint-sync-bench.sh
  echo \$? > ${BENCH_OUT_REMOTE}/seed.exit
) > ${BENCH_OUT_REMOTE}/seed.log 2>&1 < /dev/null &
echo \$! > ${BENCH_OUT_REMOTE}/seed.pid
disown
REMOTE
  echo "seeding started on $name ($ip) to height ${COHORT_SEED_STOP} — poll with: cohort.sh status $name"
}

cmd_freeze() {
  local name="${1:?usage: cohort.sh freeze NAME}"; check_serve_name "$name"
  local ip; ip="$(ip_of "$name")"; [ -n "$ip" ] || die "no ip for $name"
  # shellcheck disable=SC2087
  $SSH "${SSH_OPTS[@]}" "root@$ip" bash -s <<REMOTE
set -euo pipefail
[ -f ${BENCH_OUT_REMOTE}/seed.exit ] || { echo "seed not finished (no seed.exit)"; exit 1; }
[ "\$(cat ${BENCH_OUT_REMOTE}/seed.exit)" = "0" ] || { echo "seed failed: exit \$(cat ${BENCH_OUT_REMOTE}/seed.exit) — see seed.log"; exit 1; }
fork=\$(ls -d ${BENCH_HOME_REMOTE}/forks/primary-* 2>/dev/null | head -1)
[ -n "\$fork" ] || { echo "no primary fork found to serve from"; exit 1; }
rm -rf ${BENCH_HOME_REMOTE}/serve-state
mv "\$fork" ${BENCH_HOME_REMOTE}/serve-state
cat > /root/serve.toml <<TOML
[network]
network = "Mainnet"
cache_dir = "${BENCH_HOME_REMOTE}/serve-state"
listen_addr = "127.0.0.1:18233"
p2p_stack = "zakura"

[network.zakura]
listen_addr = "0.0.0.0:8234"
dev_network = "${COHORT_TAG}"
bootstrap_peers = []

[state]
cache_dir = "${BENCH_HOME_REMOTE}/serve-state"
TOML
bin=\$(ls ${BENCH_HOME_REMOTE}/bins/*/zakurad | head -1)
[ -n "\$bin" ] || { echo "no cached zakurad binary"; exit 1; }
pkill -F /root/serve.pid 2>/dev/null || true
( nohup "\$bin" -c /root/serve.toml start > /root/serve.log 2>&1 < /dev/null & echo \$! > /root/serve.pid )
disown 2>/dev/null || true
sleep 5
kill -0 "\$(cat /root/serve.pid)" || { echo "serve node died on start:"; tail -5 /root/serve.log; exit 1; }
echo "frozen: serving cohort '${COHORT_TAG}' from \$(basename "\$fork") state"
REMOTE
}

cmd_peers() {
  local out="" name ip id
  for name in perf-lab-serve-a perf-lab-serve-b; do
    ip="$(ip_of "$name")"; [ -n "$ip" ] || die "no ip for $name (both serve nodes must exist)"
    id="$($SSH "${SSH_OPTS[@]}" "root@$ip" "grep -oE 'node_id=[0-9a-f]+' /root/serve.log | tail -1 | cut -d= -f2" 2>/dev/null)"
    [ -n "$id" ] || die "no node_id in $name's serve.log (is the serve node up?)"
    out="$out $id@$ip:8234"
  done
  out="${out# }"
  python3 - "$out" <<'PYEOF'
import sys
peers = sys.argv[1]
import re
path = "perf-lab/config.env"
s = open(path).read()
s = re.sub(r'COHORT_PEERS="[^"]*"', f'COHORT_PEERS="{peers}"', s)
open(path, "w").write(s)
print(f"COHORT_PEERS={peers}")
PYEOF
}

cmd_status() {
  local name="${1:?}"; check_serve_name "$name"
  local ip; ip="$(ip_of "$name")"; [ -n "$ip" ] || die "no ip for $name"
  # shellcheck disable=SC2087  # client-side expansion intended
  $SSH "${SSH_OPTS[@]}" "root@$ip" bash -s <<REMOTE
if [ -f /root/serve.pid ] && kill -0 "\$(cat /root/serve.pid)" 2>/dev/null; then
  echo "SERVING (pid \$(cat /root/serve.pid))"; tail -1 /root/serve.log
elif [ -f ${BENCH_OUT_REMOTE}/seed.exit ]; then
  echo "SEED DONE:\$(cat ${BENCH_OUT_REMOTE}/seed.exit)"
elif [ -f ${BENCH_OUT_REMOTE}/seed.pid ] && kill -0 "\$(cat ${BENCH_OUT_REMOTE}/seed.pid)" 2>/dev/null; then
  echo "SEEDING"; tail -1 ${BENCH_OUT_REMOTE}/seed.log
else
  echo "IDLE/UNKNOWN"
fi
REMOTE
}

cmd_stop() {
  local name="${1:?}"; check_serve_name "$name"
  local ip; ip="$(ip_of "$name")"; [ -n "$ip" ] || die "no ip for $name"
  $SSH "${SSH_OPTS[@]}" "root@$ip" 'pkill -F /root/serve.pid 2>/dev/null && echo stopped || echo "not running"'
}

case "${1:-}" in
  seed)   shift; cmd_seed "$@";;
  freeze) shift; cmd_freeze "$@";;
  peers)  cmd_peers;;
  status) shift; cmd_status "$@";;
  stop)   shift; cmd_stop "$@";;
  *) die "usage: cohort.sh seed|freeze|peers|status|stop ...";;
esac
