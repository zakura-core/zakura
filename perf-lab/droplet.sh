#!/usr/bin/env bash
# perf-lab droplet lifecycle. Subcommands:
#   provision [suffix]   create+prepare a bench droplet (golden image if found)
#   ip NAME | ssh NAME [cmd...] | destroy NAME | reap | list
# Safety: destroy/reap only act on droplets named ${NAME_PREFIX}-* AND tagged
# ${PERF_TAG}. DRYRUN=1 prints mutating commands instead of running them.
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null
source "$DIR/config.env"
DOCTL="${DOCTL_BIN:-doctl}"
SSH="${SSH_BIN:-ssh}"
SSH_OPTS=(-i "$SSH_KEY_FILE" -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile=/dev/null -o ConnectTimeout=10)

die() { echo "droplet.sh: $*" >&2; exit 1; }
run() { if [ -n "${DRYRUN:-}" ]; then echo "DRYRUN: $*"; else "$@"; fi; }

ensure_key() {
  if [ ! -f "$SSH_KEY_FILE" ]; then
    run ssh-keygen -t ed25519 -N "" -C "$SSH_KEY_NAME" -f "$SSH_KEY_FILE"
  fi
  # awk field-match: doctl's columnar output can pad lines, defeating grep -qx
  if ! $DOCTL compute ssh-key list --format Name --no-header \
      | awk -v n="$SSH_KEY_NAME" '$1==n{found=1} END{exit !found}'; then
    run $DOCTL compute ssh-key import "$SSH_KEY_NAME" --public-key-file "$SSH_KEY_FILE.pub"
  fi
  # In real mode the run-wrapped keygen above guarantees the pubkey exists; in
  # DRYRUN it may not, so substitute a placeholder instead of failing.
  if [ -f "$SSH_KEY_FILE.pub" ]; then
    FP=$(ssh-keygen -lf "$SSH_KEY_FILE.pub" -E md5 | awk '{print $2}' | sed 's/^MD5://')
  elif [ -n "${DRYRUN:-}" ]; then
    FP="dryrun-fp-placeholder"
  else
    die "ssh public key missing after keygen: $SSH_KEY_FILE.pub"
  fi
}

golden_image() {  # newest zakura-pr-node-* image id, empty if none (pr-node recipe)
  $DOCTL compute image list-user --format ID,Name --no-header \
    | awk -v p="$GOLDEN_IMAGE_PREFIX" '$2 ~ "^"p {print $2, $1}' | sort | tail -1 | awk '{print $2}'
}

droplet_json() { $DOCTL compute droplet list --tag-name "$PERF_TAG" --output json; }
droplet_ip()  {
  droplet_json | python3 -c '
import json,sys
name=sys.argv[1]
for d in json.load(sys.stdin) or []:
    if d["name"]==name:
        print(next((n["ip_address"] for n in d["networks"]["v4"] if n["type"]=="public"), "")); break
' "$1"
}

cmd_provision() {
  local name="${NAME_PREFIX}-${1:-$(date +%m%d%H%M%S)}"
  # names must stay shell- and doctl-safe (the cleanup trap re-uses them)
  case "$name" in *[!A-Za-z0-9._-]*) die "bad droplet name: $name";; esac
  # hard rule (design §5): concurrent perf-lab droplets <= MAX_DROPLETS.
  # Checked before ensure_key so a refused provision has zero side effects.
  local count; count="$(droplet_json | python3 -c 'import json,sys; print(len(json.load(sys.stdin) or []))')"
  [ "$count" -lt "$MAX_DROPLETS" ] || die "refusing: $count perf-lab droplet(s) exist (MAX_DROPLETS=$MAX_DROPLETS)"
  ensure_key
  local image; image="$(golden_image)"
  if [ -n "$image" ]; then echo "using golden image $image"
  else image="$DO_FALLBACK_IMAGE"; echo "WARN: no ${GOLDEN_IMAGE_PREFIX}* image; falling back to $image (slow bootstrap)"; fi
  run $DOCTL compute droplet create "$name" \
    --region "$DO_REGION" --size "$DO_SIZE" --image "$image" \
    --ssh-keys "$FP" --tag-name "$PERF_TAG" \
    --wait --format ID,PublicIPv4 --no-header
  [ -n "${DRYRUN:-}" ] && return 0
  # Self-clean: from here on a failure would orphan a paid droplet that also
  # counts against MAX_DROPLETS — destroy it on the way out. EXIT (not ERR):
  # `|| die` failure paths bypass ERR traps. ORPHAN is global so the
  # single-quoted trap resolves it safely at fire time; the subshell contains
  # cmd_destroy's `exit`, so `|| true` really does absorb a guard refusal.
  ORPHAN="$name"
  trap 'echo "provision failed; destroying orphan $ORPHAN" >&2; ( cmd_destroy "$ORPHAN" ) || true' EXIT
  local ip; ip="$(droplet_ip "$name")"; [ -n "$ip" ] || die "no ip for $name"
  echo "waiting for ssh on $ip ..."
  for _ in $(seq 1 30); do
    $SSH "${SSH_OPTS[@]}" "root@$ip" true 2>/dev/null && break; sleep 10
  done
  $SSH "${SSH_OPTS[@]}" "root@$ip" true || die "ssh never came up on $ip"
  prepare_remote "$ip"
  trap - EXIT
  echo "$name ready at $ip"
}

prepare_remote() {  # idempotent post-boot prep (golden image or fallback)
  local ip="$1"
  $SSH "${SSH_OPTS[@]}" "root@$ip" bash -s <<REMOTE
set -euo pipefail
# a fresh droplet runs apt at boot; wait for the lock (pr-node-bake gotcha)
for _ in \$(seq 1 120); do pgrep -x apt-get >/dev/null || break; sleep 5; done
if ! command -v cargo >/dev/null 2>&1; then   # fallback-image path only
  apt-get -o DPkg::Lock::Timeout=600 update -qq
  apt-get -o DPkg::Lock::Timeout=600 install -y -qq \
    build-essential clang pkg-config libssl-dev protobuf-compiler \
    git curl zstd jq python3
  curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain stable
  ln -sf /root/.cargo/bin/cargo /root/.cargo/bin/rustc /root/.cargo/bin/rustup /usr/local/bin/
fi
# bench cache root + warm-cache symlinks onto the golden clone/target
mkdir -p ${BENCH_HOME_REMOTE}
[ -d ${GOLDEN_CLONE_REMOTE} ]  && ln -sfn ${GOLDEN_CLONE_REMOTE}  ${BENCH_HOME_REMOTE}/src
[ -d ${GOLDEN_TARGET_REMOTE} ] && ln -sfn ${GOLDEN_TARGET_REMOTE} ${BENCH_HOME_REMOTE}/build-target
[ -d /root/.cargo ]            && ln -sfn /root/.cargo            ${BENCH_HOME_REMOTE}/cargo-home
# pinned control clone the bench script RUNS from (BUILD_SRC gets checked out
# per-ref mid-run, so the script must not execute from BUILD_SRC itself)
if [ ! -d ${CTL_CLONE_REMOTE} ]; then
  git clone --depth 1 https://github.com/zakura-core/zakura.git ${CTL_CLONE_REMOTE}
else
  git -C ${CTL_CLONE_REMOTE} fetch --depth 1 origin main && git -C ${CTL_CLONE_REMOTE} checkout -f origin/main
fi
# B-14 local patch: each leg's fork grows ~116 GB and the harness keeps the
# baseline fork alive through the primary leg, which filled the disk to 0
# twice; free it as soon as its summary row is written (banner-safe, proven
# live). Idempotent; reapplied after every checkout. Upstream PR pending.
if ! grep -q "perf-lab B-14" ${CTL_CLONE_REMOTE}/scripts/checkpoint-sync-bench.sh; then
  sed -i '/summary_row "\$BASELINE_SPEC (baseline)"/s/$/; rm -rf "\$CUR_FORK"  # perf-lab B-14: free baseline fork before primary leg/' \
    ${CTL_CLONE_REMOTE}/scripts/checkpoint-sync-bench.sh
  grep -q "perf-lab B-14" ${CTL_CLONE_REMOTE}/scripts/checkpoint-sync-bench.sh \
    || echo "WARN: perf-lab B-14 patch no longer applies upstream — disk-fill risk is back" >&2
fi
# B-15 cohort patches: env-overridable bootstrap peers + dev_network tag.
# python (not sed) — the replacement text carries shell syntax that would need
# a third escaping layer through sed (the CUR_FORK lesson).
if ! grep -q "perf-lab cohort" ${CTL_CLONE_REMOTE}/scripts/checkpoint-sync-bench.sh; then
  python3 - ${CTL_CLONE_REMOTE}/scripts/checkpoint-sync-bench.sh <<'PYPATCH'
# Escape-proof by construction: every sensitive character comes from chr(), so
# no heredoc/python layer can mangle it (the trace_dir-anchor lesson).
import sys
path = sys.argv[1]
lines = open(path).read().split(chr(10))
q, d, bs = chr(34), chr(36), chr(92)
out, done_peers, done_dev = [], False, False
for i, line in enumerate(lines):
    out.append(line)
    if (not done_peers and line == ")"
            and any("ZAKURA_BOOTSTRAP_PEERS=(" in l for l in lines[max(0, i-10):i])):
        out += [
            "# perf-lab cohort: PERF_COHORT_PEERS (space-separated id@ip:port)",
            "# replaces the public bootstrap list when set",
            "if [ -n " + q + d + "{PERF_COHORT_PEERS:-}" + q + " ]; then",
            "  read -r -a ZAKURA_BOOTSTRAP_PEERS <<< " + q + d + "PERF_COHORT_PEERS" + q,
            "fi",
        ]
        done_peers = True
    if not done_dev and "echo" in line and "trace_dir = " in line:
        ind = line[:len(line) - len(line.lstrip())]
        out.append(ind + "[ -n " + q + d + "{PERF_DEV_NETWORK:-}" + q + " ] && echo "
                   + q + "dev_network = " + bs + q + d + "{PERF_DEV_NETWORK}" + bs + q + q
                   + "  # perf-lab cohort")
        done_dev = True
assert done_peers and done_dev, (done_peers, done_dev)
open(path, "w").write(chr(10).join(out))
print("cohort patches applied")
PYPATCH
  bash -n ${CTL_CLONE_REMOTE}/scripts/checkpoint-sync-bench.sh \
    || echo "WARN: cohort patch broke harness syntax — cohort mode unusable" >&2
fi
# keepfork: the harness's EXIT-trap cleanup deletes CUR_FORK on every exit,
# which would destroy a cohort seed's served state; guard it behind
# KEEP_CUR_FORK (set only by cohort.sh seed). B-14's summary_row rm is
# untouched (that line does not start with [[ and never runs in seed mode).
if ! grep -q "perf-lab keepfork" ${CTL_CLONE_REMOTE}/scripts/checkpoint-sync-bench.sh; then
  python3 - ${CTL_CLONE_REMOTE}/scripts/checkpoint-sync-bench.sh <<'PYKEEP'
import sys
path = sys.argv[1]
lines = open(path).read().split(chr(10))
q, d = chr(34), chr(36)
frag = "rm -rf " + q + d + "CUR_FORK" + q
cond = "[[ -n " + q + d + "CUR_FORK" + q + " ]]"
newcond = ("[[ -n " + q + d + "CUR_FORK" + q + " && " + q + d
           + "{KEEP_CUR_FORK:-0}" + q + " != " + q + "1" + q + " ]]")
done = False
for i, line in enumerate(lines):
    if not done and frag in line and line.lstrip().startswith("[[") and cond in line:
        lines[i] = line.replace(cond, newcond) + "  # perf-lab keepfork"
        done = True
assert done, "keepfork anchor"
open(path, "w").write(chr(10).join(lines))
print("keepfork patch applied")
PYKEEP
  bash -n ${CTL_CLONE_REMOTE}/scripts/checkpoint-sync-bench.sh \
    || echo "WARN: keepfork patch broke harness syntax" >&2
fi
# bsknob: PERF_BS_MAX_BLOCKS_PER_RESPONSE emits a [network.zakura.block_sync]
# section in the generated bench config — request-shape experiments without
# touching Zakura code defaults (supply-bound attribution, 2026-07-22).
if ! grep -q "perf-lab bsknob" ${CTL_CLONE_REMOTE}/scripts/checkpoint-sync-bench.sh; then
  python3 - ${CTL_CLONE_REMOTE}/scripts/checkpoint-sync-bench.sh <<'PYBSK'
import sys
path = sys.argv[1]
lines = open(path).read().split(chr(10))
q, d = chr(34), chr(36)
done = False
for i, line in enumerate(lines):
    if (not done and line.strip() == "echo " + chr(39) + "]" + chr(39)
            and any("bootstrap_peers = [" in l for l in lines[max(0, i-12):i])):
        ind = line[:len(line) - len(line.lstrip())]
        lines[i] = line + chr(10) + chr(10).join([
            ind + "if [ -n " + q + d + "{PERF_BS_MAX_BLOCKS_PER_RESPONSE:-}" + q + " ]; then  # perf-lab bsknob",
            ind + "  echo " + chr(39) + chr(39),
            ind + "  echo " + chr(39) + "[network.zakura.block_sync]" + chr(39),
            ind + "  echo " + q + "max_blocks_per_response = " + d + "{PERF_BS_MAX_BLOCKS_PER_RESPONSE}" + q,
            ind + "fi",
        ])
        done = True
assert done, "bsknob anchor"
open(path, "w").write(chr(10).join(lines))
print("bsknob patch applied")
PYBSK
  bash -n ${CTL_CLONE_REMOTE}/scripts/checkpoint-sync-bench.sh \
    || echo "WARN: bsknob patch broke harness syntax" >&2
fi
mkdir -p ${BENCH_OUT_REMOTE}
echo "remote prep done"
REMOTE
}

assert_ours() {  # refuse to touch anything not name-prefixed AND tagged
  local name="$1"
  case "$name" in "${NAME_PREFIX}"-*) ;; *) die "refusing: '$name' lacks prefix ${NAME_PREFIX}-";; esac
  droplet_json | python3 -c '
import json,sys
name=sys.argv[1]
ok=any(d["name"]==name for d in json.load(sys.stdin) or [])
sys.exit(0 if ok else 1)
' "$name" || die "refusing: '$name' is not tagged $PERF_TAG"
}

cmd_destroy() {
  local name="${1:?usage: droplet.sh destroy NAME}"
  assert_ours "$name"
  run $DOCTL compute droplet delete "$name" -f
  echo "destroyed $name"
}

cmd_reap() {  # delete tagged droplets older than REAP_MAX_AGE_HOURS (reaper recipe)
  local max=$((REAP_MAX_AGE_HOURS * 3600))
  droplet_json | python3 -c '
import json,sys,datetime
max_age=int(sys.argv[1]); now=datetime.datetime.now(datetime.timezone.utc)
for d in json.load(sys.stdin) or []:
    created=datetime.datetime.fromisoformat(d["created_at"].replace("Z","+00:00"))
    if (now-created).total_seconds() > max_age: print(d["name"])
' "$max" | while read -r name; do
    # long-lived frozen cohort servers are exempt (B-15; Adam-approved)
    case "$name" in "${NAME_PREFIX}"-serve-*) echo "keeping cohort server $name"; continue;; esac
    echo "reaping stale droplet $name"
    # subshell contains die's `exit`, so the loop survives a guard refusal
    ( cmd_destroy "$name" ) || echo "reap skipped (guard refused): $name" >&2
  done
}

cmd_list() { droplet_json | python3 -c '
import json,sys
for d in json.load(sys.stdin) or []:
    ip=next((n["ip_address"] for n in d["networks"]["v4"] if n["type"]=="public"),"?")
    print(d["name"], ip, d["created_at"])'; }

cmd_ssh() { local name="${1:?}"; shift; local ip; ip="$(droplet_ip "$name")"
  [ -n "$ip" ] || die "no perf-lab droplet named $name"
  exec $SSH "${SSH_OPTS[@]}" "root@$ip" "$@"; }

case "${1:-}" in
  provision) shift; cmd_provision "$@";;
  ip)        shift; droplet_ip "${1:?}";;
  ssh)       shift; cmd_ssh "$@";;
  destroy)   shift; cmd_destroy "$@";;
  reap)      cmd_reap;;
  list)      cmd_list;;
  *) die "usage: droplet.sh provision|ip|ssh|destroy|reap|list";;
esac
