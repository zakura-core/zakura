#!/usr/bin/env bash
# Runs ON the ephemeral mempool-load droplet (zakura-mempool-load.yml): checks
# out the target commit in the baked repo clone, builds zakurad and the Kresko
# load generator, brings up an isolated local-genesis testnet, blasts funded
# Orchard transactions at it, and grades the result.
#
# Config via /root/run.env (sourced by the caller before exec):
#   GH_REPO / GH_CLONE_TOKEN  repo slug + per-run token for the ref fetch
#   SHA / REFSPEC             commit to test + refspec that reaches it
#   KRESKO_REPO / KRESKO_REF  load generator source, pinned per run
#   NODE_COUNT                zakurad nodes on the loopback testnet
#   TX_RATE                   target transactions per second
#   DURATION_SECS             how long to sustain the blast
#   BASELINE_REF              optional second commit to A/B against ("" to skip)
set -euo pipefail

# Paths are overridable so this script can be exercised outside a droplet.
# Every bug found on the droplet path so far has been in this file, precisely
# because the local rehearsal drove the Python directly and never ran it.
#
# Deliberately not named ZAKURA_*: zakurad maps ZAKURA_<FIELD> environment
# variables onto config fields, so an exported ZAKURA_DIR is read as a
# top-level `dir` key and the node refuses to start with
# "unknown field `dir`". Nothing here may share that prefix.
NODE_SRC_DIR=${NODE_SRC_DIR:-/root/zakura}
LOADGEN_DIR=${LOADGEN_DIR:-/root/kresko}
OUT_DIR=${OUT_DIR:-/root/out}
LAB_DIR=${LAB_DIR:-/root/mempool-lab}
SCRIPTS=${SCRIPTS:-/root}
CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-/root/cargo-target}
# Set when the caller has already prepared the source tree and kresko binary
# (the local rehearsal), so the fetch/build phases are skipped.
SKIP_SOURCE_FETCH=${SKIP_SOURCE_FETCH:-0}
SKIP_KRESKO_BUILD=${SKIP_KRESKO_BUILD:-0}
# How long the blast may spend proving its initial Orchard lane inventory
# before the measured window opens. Observed at ~25s for 24 lanes on a small
# box; this leaves headroom without stalling a failed bootstrap forever.
BOOTSTRAP_BUDGET_SECS=300
# Initial Orchard lane inventory. Kresko's own default is 384, which is far
# more than a bounded run can use: every lane is proved up front, so a 2-minute
# workload spends its whole window bootstrapping and submits nothing. Lanes are
# recycled as transactions confirm, so a few dozen sustain a run of any length.
ORCHARD_LANES=${ORCHARD_LANES:-32}
mkdir -p "$OUT_DIR"

cloud-init status --wait >/dev/null 2>&1 || true

# ---------------------------------------------------------------------------- #
# Source: fetch the target ref into the baked clone
# ---------------------------------------------------------------------------- #

cd "$NODE_SRC_DIR"
# git-over-HTTPS wants basic auth (the bearer form is API-only); this is the
# same header actions/checkout configures.
if [ "$SKIP_SOURCE_FETCH" != "1" ]; then
GIT_AUTH=$(printf 'x-access-token:%s' "${GH_CLONE_TOKEN}" | base64 -w0)
git -c http.extraheader="AUTHORIZATION: basic ${GIT_AUTH}" \
  fetch --no-tags origin "${REFSPEC}"
if [ -n "${BASELINE_REF}" ]; then
  git -c http.extraheader="AUTHORIZATION: basic ${GIT_AUTH}" \
    fetch --no-tags origin "${BASELINE_REF}"
fi
rm -f "${SCRIPTS}/run.env"
unset GH_CLONE_TOKEN GIT_AUTH
fi

export CARGO_TARGET_DIR

# ---------------------------------------------------------------------------- #
# Load generator: Kresko, built against this repo's crates
# ---------------------------------------------------------------------------- #
# Kresko depends on the zakura crates at its own pinned tag, so it is built
# once and reused across both A/B legs -- it is independent of which zakura
# commit is under test. That pin is Kresko's, not this ref's: it only has to
# generate a chain the zakurad under test accepts.

# The baked image ships a prebuilt kresko (see pr-node-bake.sh). Reuse it only
# when it is the ref this run asked for -- otherwise a stale binary would
# silently generate the chain, which is exactly the kind of thing that makes an
# A/B result untrustworthy.
WANT_SHA=""
if [ -d "$LOADGEN_DIR" ]; then
  git -C "$LOADGEN_DIR" fetch --no-tags origin "${KRESKO_REF}" 2>/dev/null || true
  WANT_SHA=$(git -C "$LOADGEN_DIR" rev-parse FETCH_HEAD 2>/dev/null || true)
fi
BAKED_SHA=$(cat "${LOADGEN_DIR}/.baked-ref" 2>/dev/null || true)

if [ "$SKIP_KRESKO_BUILD" = "1" ] && [ -x "${LOADGEN_DIR}/target/release/kresko" ]; then
  echo "reusing the caller-provided kresko binary"
elif [ -x "${LOADGEN_DIR}/target/release/kresko" ] && [ -n "$WANT_SHA" ] && [ "$BAKED_SHA" = "$WANT_SHA" ]; then
  echo "reusing baked kresko at ${BAKED_SHA}"
else
  echo "building kresko at ${KRESKO_REF} (baked: ${BAKED_SHA:-none}, want: ${WANT_SHA:-unknown})"
  git clone "${KRESKO_REPO}" "$LOADGEN_DIR" 2>/dev/null || true
  git -C "$LOADGEN_DIR" fetch --no-tags origin "${KRESKO_REF}"
  # Force the checkout: the baked image's clone carries a `.baked-ref` marker
  # (written by pr-node-bake.sh, and tracked in the kresko repo), so a plain
  # checkout to a ref other than the baked one aborts with "would be
  # overwritten". This rebuild path runs precisely when the wanted ref differs
  # from the baked one -- e.g. any kresko main commit after the weekly bake --
  # so it must not choke on that marker. It is rewritten below anyway.
  git -C "$LOADGEN_DIR" checkout -f --detach FETCH_HEAD
  # No patch step: Kresko builds against the zakura crates upstream.
  # Own target dir: CARGO_TARGET_DIR is exported for the zakura build, and
  # inheriting it puts the binary somewhere KRESKO_BIN does not point --
  # and makes kresko and zakurad thrash one cache built from different
  # versions of the same crates.
  ( cd "$LOADGEN_DIR" && CARGO_TARGET_DIR="${LOADGEN_DIR}/target" cargo build --release )
  git -C "$LOADGEN_DIR" rev-parse HEAD > "${LOADGEN_DIR}/.baked-ref"
fi

KRESKO_BIN="${LOADGEN_DIR}/target/release/kresko"
# Fail here rather than at first use: a build that lands the binary somewhere
# else still "succeeds", and the next error is an opaque FileNotFoundError from
# inside the Python driver several steps later.
if [ ! -x "$KRESKO_BIN" ]; then
  echo "kresko build produced no binary at ${KRESKO_BIN}" >&2
  exit 1
fi
KRESKO_SHA=$(git -C "$LOADGEN_DIR" rev-parse HEAD)
echo "kresko: ${KRESKO_SHA}"

# ---------------------------------------------------------------------------- #
# One measured leg: build the commit, run the workload, grade it
# ---------------------------------------------------------------------------- #

run_leg() {
  # Separate `local` statements on purpose: bash declares every name in a
  # single `local` before assigning any of them, so referring to $label in the
  # same statement that defines it is an unbound-variable error under `set -u`.
  local label="$1"
  local commit="$2"
  local leg_out="$OUT_DIR/$label"
  mkdir -p "$leg_out"

  echo "=== leg ${label}: building ${commit} ==="
  # Explicit `|| return` rather than relying on `set -e`: bash disables errexit
  # for the whole body of a function called in a `||` list, which both call
  # sites below are. Without these guards a failed checkout or build would fall
  # through and the leg would silently measure whatever binary was already on
  # disk -- reporting a green result for code that was never built.
  git -C "$NODE_SRC_DIR" checkout --detach "${commit}" || {
    echo "leg ${label}: checkout of ${commit} failed" >&2
    return 90
  }
  # The metrics endpoint carries the mempool backpressure counters, and the
  # internal miner produces the blocks that let the workload drain.
  ( cd "$NODE_SRC_DIR" && cargo build --release \
      --features internal-miner,prometheus --package zakura --bin zakurad ) || {
    echo "leg ${label}: build of ${commit} failed" >&2
    return 91
  }
  local zakurad_bin="${CARGO_TARGET_DIR}/release/zakurad"
  [ -x "$zakurad_bin" ] || {
    echo "leg ${label}: no zakurad binary at ${zakurad_bin}" >&2
    return 92
  }

  # A leg must start from a clean chain, or the previous leg's state and
  # mempool would carry into the measurement.
  rm -rf "$LAB_DIR"

  echo "=== leg ${label}: generating local-genesis chain ==="
  python3 "$SCRIPTS/mempool-load-lab.py" \
    --lab-dir "$LAB_DIR" --zakurad-binary "$zakurad_bin" \
    --kresko-binary "$KRESKO_BIN" --node-count "$NODE_COUNT" \
    genesis --chain-id "mempool-load-${label}" \
    --orchard-lanes-per-miner "${ORCHARD_LANES}" || {
    echo "leg ${label}: genesis failed" >&2
    return 93
  }

  echo "=== leg ${label}: starting ${NODE_COUNT} nodes ==="
  python3 "$SCRIPTS/mempool-load-lab.py" \
    --lab-dir "$LAB_DIR" --zakurad-binary "$zakurad_bin" \
    --kresko-binary "$KRESKO_BIN" --node-count "$NODE_COUNT" up || {
    echo "leg ${label}: nodes failed to start" >&2
    python3 "$SCRIPTS/mempool-load-lab.py" --lab-dir "$LAB_DIR" down || true
    return 94
  }

  echo "=== leg ${label}: blasting at ${TX_RATE} tx/s for ${DURATION_SECS}s ==="
  # The blast must outlive the measured window: it needs a bootstrap phase
  # (Orchard proving for the initial lane inventory) before it submits
  # anything, and the monitor should not be sampling during that.
  python3 "$SCRIPTS/mempool-load-lab.py" \
    --lab-dir "$LAB_DIR" --zakurad-binary "$zakurad_bin" \
    --kresko-binary "$KRESKO_BIN" --node-count "$NODE_COUNT" \
    blast --tx-rate "$TX_RATE" \
    --duration-secs "$(( DURATION_SECS + BOOTSTRAP_BUDGET_SECS ))" &
  local blast_pid=$!

  # Wait for steady state so the measured window is steady-state throughput,
  # not proving latency -- otherwise short runs grade a bootstrapping blast as
  # "zero submissions", and A/B legs compare different phases.
  local waited=0
  while [ "$waited" -lt "$BOOTSTRAP_BUDGET_SECS" ]; do
    if grep -q 'steady state' "$LAB_DIR/txblast.log" 2>/dev/null; then
      echo "=== leg ${label}: blast reached steady state after ${waited}s ==="
      break
    fi
    if ! kill -0 "$blast_pid" 2>/dev/null; then
      echo "=== leg ${label}: blast exited during bootstrap ===" >&2
      break
    fi
    sleep 5
    waited=$(( waited + 5 ))
  done

  python3 "$SCRIPTS/mempool-load-monitor.py" \
    --lab-dir "$LAB_DIR" --node-count "$NODE_COUNT" \
    --duration-secs "$DURATION_SECS" --out "$leg_out" \
    --meta "sha=${commit},tx_rate=${TX_RATE},leg=${label}" &
  local monitor_pid=$!

  local leg_rc=0
  wait "$monitor_pid" || leg_rc=$?
  kill -INT "$blast_pid" 2>/dev/null || true
  wait "$blast_pid" 2>/dev/null || true

  python3 "$SCRIPTS/mempool-load-lab.py" --lab-dir "$LAB_DIR" down || true
  python3 "$SCRIPTS/mempool-load-lab.py" --lab-dir "$LAB_DIR" \
    collect --out "$leg_out" || true

  echo "=== leg ${label}: verdict rc=${leg_rc} ==="
  return "$leg_rc"
}

# ---------------------------------------------------------------------------- #
# Run the legs and package the result
# ---------------------------------------------------------------------------- #

RC=0
if [ -n "${BASELINE_REF}" ]; then
  # Baseline first, so the target ref's numbers are the ones left in a warm
  # page cache -- and record both even if the baseline itself fails.
  run_leg baseline "${BASELINE_REF}" || true
  run_leg target "${SHA}" || RC=$?
  python3 "$SCRIPTS/mempool-load-compare.py" \
    --baseline "$OUT_DIR/baseline/summary.json" \
    --target "$OUT_DIR/target/summary.json" \
    --out "$OUT_DIR/summary.md" || true
else
  run_leg target "${SHA}" || RC=$?
  cp "$OUT_DIR/target/summary.md" "$OUT_DIR/summary.md" 2>/dev/null || true
fi

echo "kresko_sha=${KRESKO_SHA}" >> "$OUT_DIR/run-meta.txt"
echo "zakura_sha=${SHA}" >> "$OUT_DIR/run-meta.txt"

exit "$RC"
