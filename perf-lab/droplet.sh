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
  if ! $DOCTL compute ssh-key list --format Name --no-header | grep -qx "$SSH_KEY_NAME"; then
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
