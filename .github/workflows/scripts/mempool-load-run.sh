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

OUT_DIR=/root/out
LAB_DIR=/root/mempool-lab
SCRIPTS=/root
# How long the blast may spend proving its initial Orchard lane inventory
# before the measured window opens. Observed at ~25s for 24 lanes on a small
# box; this leaves headroom without stalling a failed bootstrap forever.
BOOTSTRAP_BUDGET_SECS=300
mkdir -p "$OUT_DIR"

cloud-init status --wait >/dev/null 2>&1 || true

# ---------------------------------------------------------------------------- #
# Source: fetch the target ref into the baked clone
# ---------------------------------------------------------------------------- #

cd /root/zakura
# git-over-HTTPS wants basic auth (the bearer form is API-only); this is the
# same header actions/checkout configures.
GIT_AUTH=$(printf 'x-access-token:%s' "${GH_CLONE_TOKEN}" | base64 -w0)
git -c http.extraheader="AUTHORIZATION: basic ${GIT_AUTH}" \
  fetch --no-tags origin "${REFSPEC}"
if [ -n "${BASELINE_REF}" ]; then
  git -c http.extraheader="AUTHORIZATION: basic ${GIT_AUTH}" \
    fetch --no-tags origin "${BASELINE_REF}"
fi
rm -f /root/run.env
unset GH_CLONE_TOKEN GIT_AUTH

export CARGO_TARGET_DIR=/root/cargo-target

# ---------------------------------------------------------------------------- #
# Load generator: Kresko, built against this repo's crates
# ---------------------------------------------------------------------------- #
# Kresko upstream pins the zebra-* crates; the zakura-compat dep set renames
# them to this checkout. Built once and reused across both A/B legs, since it
# is independent of which zakura commit is under test.

if [ ! -x /root/kresko/target/release/kresko ]; then
  git clone "${KRESKO_REPO}" /root/kresko 2>/dev/null || true
  git -C /root/kresko fetch --no-tags origin "${KRESKO_REF}"
  git -C /root/kresko checkout --detach FETCH_HEAD
  # Repoint Kresko's zebra-* deps at this checkout's zakura-* crates and drop
  # its hardcoded NU7 activation. Remove once the upstream PR lands; until
  # then `git apply` failing is a real signal that Kresko moved under us.
  sed 's|ZAKURA_CHECKOUT|/root/zakura|g' \
    /root/kresko-zakura-compat.patch > /root/kresko/zakura-compat.patch
  git -C /root/kresko apply --verbose zakura-compat.patch
  ( cd /root/kresko && cargo build --release )
fi
KRESKO_BIN=/root/kresko/target/release/kresko
KRESKO_SHA=$(git -C /root/kresko rev-parse HEAD)
echo "kresko: ${KRESKO_SHA}"

# ---------------------------------------------------------------------------- #
# One measured leg: build the commit, run the workload, grade it
# ---------------------------------------------------------------------------- #

run_leg() {
  local label="$1" commit="$2" leg_out="$OUT_DIR/$label"
  mkdir -p "$leg_out"

  echo "=== leg ${label}: building ${commit} ==="
  # Explicit `|| return` rather than relying on `set -e`: bash disables errexit
  # for the whole body of a function called in a `||` list, which both call
  # sites below are. Without these guards a failed checkout or build would fall
  # through and the leg would silently measure whatever binary was already on
  # disk -- reporting a green result for code that was never built.
  git -C /root/zakura checkout --detach "${commit}" || {
    echo "leg ${label}: checkout of ${commit} failed" >&2
    return 90
  }
  # The metrics endpoint carries the mempool backpressure counters, and the
  # internal miner produces the blocks that let the workload drain.
  ( cd /root/zakura && cargo build --release \
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
    genesis --chain-id "mempool-load-${label}" || {
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
