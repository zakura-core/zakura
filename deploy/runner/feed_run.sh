#!/usr/bin/env bash
# Bottleneck-instrumentation bench run.
#
# Hard-link-forks a snapshot, syncs forward to STOP, and samples the five-category
# bottleneck metrics every 5s into a CSV. All host-specific paths come from
# cohort.env (the BENCH_* vars) or the environment — nothing is hard-coded here.
#
#   network   : zcash.net.{in,out}.bytes.total, peers.connected
#   download  : sync.downloads.in_flight, sync.downloaded.block.count
#   verifier  : zebra.feed.equihash_pow / merkle_root (checkpoint verifier CPU)
#   commit cpu: update_trees (note tree) + commitment_check + batch_prep
#   commit db : prep_reads (UTXO/addr reads) + rocksdb.batch_commit (write I/O)
#   committer : commit busy + input_queue_depth + poll_ready/poll_empty
#
# The clone breaks hardlinks on RocksDB mutable metadata (CURRENT/MANIFEST/LOG/
# *.log/OPTIONS/LOCK/IDENTITY) and the format `version` file, so per-run RocksDB
# open churn and any NoMigration version bump can never write through to the
# pristine source snapshot.
#
# Usage: feed_run.sh LABEL BIN [stop] [met] [maxsec]
set -uo pipefail

RUNNER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Pull host-specific paths/defaults from the top config.
# shellcheck source=/dev/null
[ -f "$RUNNER_DIR/cohort.env" ] && source "$RUNNER_DIR/cohort.env"

LABEL="${1:?usage: feed_run.sh LABEL BIN [stop] [met] [maxsec]}"
BIN="${2:?need binary path}"
STOP="${3:-${BENCH_STOP_DEFAULT:-1830000}}"
MET="${4:-${BENCH_MET_DEFAULT:-19998}}"
MAXSEC="${5:-${BENCH_MAXSEC_DEFAULT:-3600}}"

# Host-specific paths (configurable via cohort.env / env).
WORK="${BENCH_WORK_DIR:?set BENCH_WORK_DIR in cohort.env}"
MASTER_WARM="${BENCH_MASTER:?set BENCH_MASTER in cohort.env}"
MASTER_COLD="${BENCH_MASTER_COLD:-$MASTER_WARM}"
DBREL="${BENCH_DB_REL:-state/v27/mainnet}"
FORK_DIR="${BENCH_FORK_DIR:-$WORK}"
LOG_DIR="${BENCH_LOG_DIR:-$WORK}"
CONFIG_SRC="${CONFIG_SRC:-${BENCH_CONFIG_SRC:-$RUNNER_DIR/zakura-bench-config.toml}}"

# Prefer the warm master (already upgraded with a repaired history tree) so a run
# starts in seconds; fall back to the cold snapshot (rebuild on open) if absent.
MASTER=${MASTER:-$([ -d "$MASTER_WARM/$DBREL" ] && echo "$MASTER_WARM" || echo "$MASTER_COLD")}
if [ "$MASTER" = "$MASTER_COLD" ] && [ "$MASTER_COLD" != "$MASTER_WARM" ]; then
  echo "[$LABEL] WARNING: using COLD snapshot; expect a one-time history-tree rebuild on open."
fi
FORK="$FORK_DIR/feedrun-fork-$LABEL"
CFG="$WORK/cfg-feedrun-$LABEL.toml"
LOG="$LOG_DIR/feedrun-$LABEL.log"
CSV="$WORK/feedrun-$LABEL.csv"
# Structured Zakura JSONL trace tables (BLOCK_SYNC_STATE, commit-state, etc.) go
# here via the config's `[network.zakura] trace_dir = "@@TRACEDIR@@"`. Lives in
# LOG_DIR (preserved) — NOT in the fork, which is rm -rf'd on cleanup. Cleared per run.
TRACEDIR="$LOG_DIR/feedrun-$LABEL-traces"
mkdir -p "$WORK" "$FORK_DIR" "$LOG_DIR"
rm -rf "$TRACEDIR"; mkdir -p "$TRACEDIR"

if [ ! -d "$MASTER/$DBREL" ]; then echo "FATAL: master missing at $MASTER/$DBREL"; exit 1; fi
if [ ! -x "$BIN" ]; then echo "FATAL: binary not executable: $BIN"; exit 1; fi

# Stop any prior run with THIS label that is still alive. Re-forking and starting
# a second node on the same DB path/ports makes RocksDB detect "multiple active
# instances" and force-shut-down the new node instantly (looks like a stall).
# Match on the per-label config path so we only ever touch our own label's node.
OLD=$(pgrep -f "cfg-feedrun-$LABEL\.toml" || true)
if [ -n "$OLD" ]; then
  echo "[$LABEL] stopping prior run still holding the fork/ports: pid(s) $OLD"
  # shellcheck disable=SC2086
  kill $OLD 2>/dev/null || true
  for _ in $(seq 1 30); do pgrep -f "cfg-feedrun-$LABEL\.toml" >/dev/null || break; sleep 1; done
  # shellcheck disable=SC2086
  kill -9 $OLD 2>/dev/null || true
  sleep 1
fi

# Also free the metrics port regardless of label: bench runs share the default MET
# port, so a straggler from ANOTHER label makes the new node panic on bind
# (metrics.rs "Trying to open metrics endpoint" -> address in use). Only bench nodes
# use this port, so killing its holder is safe here.
PORTPID=$(ss -ltnp 2>/dev/null | awk -v p=":$MET" '$4 ~ p {print $0}' | grep -oE 'pid=[0-9]+' | head -1 | cut -d= -f2)
if [ -n "$PORTPID" ]; then
  echo "[$LABEL] freeing metrics port $MET held by pid $PORTPID (a different-label run)"
  kill "$PORTPID" 2>/dev/null || true
  for _ in $(seq 1 30); do ss -ltn 2>/dev/null | grep -q ":$MET " || break; sleep 1; done
  kill -9 "$PORTPID" 2>/dev/null || true
  sleep 1
fi

if [ -n "${BENCH_FROM_GENESIS:-}" ]; then
  # Genesis mode: start from an EMPTY state (no snapshot clone), so the node
  # syncs from block 0. The cohort serving nodes hold genesis..~1.85M, so this
  # runs genesis->cohort-tip deterministically; for genesis->network-tip use a
  # public-peer config (see genesis-sync.toml) instead of the cohort.
  echo "[$LABEL] GENESIS mode: empty state at $FORK (no snapshot clone)"
  rm -rf "$FORK"
  mkdir -p "$FORK/$DBREL"
else
  echo "[$LABEL] safe-cloning $MASTER -> $FORK"
  rm -rf "$FORK"
  mkdir -p "$FORK/$(dirname "$DBREL")"
  cp -al "$MASTER/$DBREL" "$FORK/$DBREL"
  # Break links on every file the clone may rewrite, so writes can't reach the source.
  ( cd "$FORK/$DBREL"
    for f in CURRENT IDENTITY LOG LOCK OPTIONS-* MANIFEST-* *.log version; do
      [ -e "$f" ] || continue
      cp -p "$f" "$f.unlink" && mv -f "$f.unlink" "$f"
    done )
fi

# Build the per-run config from the canonical source-of-truth template by
# substituting the per-run tokens. Override the template with CONFIG_SRC=...
# This is the ONLY config the node loads.
LISTEN=$((8200 + MET % 100))
if [ ! -f "$CONFIG_SRC" ]; then echo "FATAL: config template not found: $CONFIG_SRC"; exit 1; fi
sed -e "s#@@FORK@@#$FORK#g" \
    -e "s#@@LISTEN@@#$LISTEN#g" \
    -e "s#@@MET@@#$MET#g" \
    -e "s#@@STOP@@#$STOP#g" \
    -e "s#@@TRACEDIR@@#$TRACEDIR#g" \
    "$CONFIG_SRC" > "$CFG"
# Fail loudly if any token went unsubstituted (typo / missing var).
if grep -q '@@' "$CFG"; then echo "FATAL: unsubstituted @@TOKEN@@ in $CFG:"; grep -n '@@' "$CFG"; exit 1; fi
echo "[$LABEL] config: $CONFIG_SRC -> $CFG (cache_dir=$FORK, met=$MET, listen=$LISTEN, stop=$STOP)"

: > "$LOG"
"$BIN" -c "$CFG" start >>"$LOG" 2>&1 &
PID=$!; sleep 3
if ! kill -0 "$PID" 2>/dev/null; then echo "[$LABEL] died on startup"; tail -15 "$LOG"; exit 1; fi
echo "[$LABEL] started pid=$PID stop=$STOP met=$MET fork=$FORK trace_dir=$TRACEDIR"

HZ=$(getconf CLK_TCK)
pstat(){ awk '{s=$0;sub(/^[0-9]+ \([^)]*\) /,"",s);split(s,a," ");print a[12]+a[13]}' "/proc/$PID/stat" 2>/dev/null; }
# Bare-name scrape (metric with no labels): first matching `name value`.
mv(){ awk -v n="$1" '$1==n{print $2; exit}' <<<"$2"; }
# Label-summing scrape: sum $2 over `name value` and every `name{...} value` line.
msum(){ awk -v n="$1" '$1==n || index($1,n"{")==1 {s+=$2} END{printf "%.6f", s+0}' <<<"$2"; }
# Floor-gap state-tick counter for one `state="X"` label (cumulative; 0 if absent).
fg(){ awk -v s="state=\"$1\"" '$1 ~ /^sync_block_floor_gap_state_ticks\{/ && index($0,s){print $2; exit}' <<<"$2"; }

# Columns are the metrics actually emitted by a Zakura-v2 + commit-metrics build:
# the note-commitment commit pipeline (CPU + DB phases) + Zakura overlay health.
# Legacy TCP sync / verifier / committer-task metrics are not emitted on this
# path, so they are intentionally dropped.
echo "epoch,elapsed,height,blk_s,cpu_cores,\
zk_peers,zk_qdepth,zk_bs_streams,\
btx_sum,btx_cnt,\
cc_sum,cc_cnt,ut_sum,ut_cnt,hp_sum,hp_cnt,ckc_sum,ckc_cnt,\
sur_sum,sur_cnt,ar_sum,ar_cnt,bp_sum,bp_cnt,bc_sum,bc_cnt,bb_sum,bb_cnt,\
vct_fast,vct_legacy,\
dl_floor,ver_tip,commit_gap,commit_stall_s,missing_bodies,outstanding,floor_claim,\
fg_outstanding,fg_queued,fg_needsched,fg_inflight,fg_absent,fg_none,\
applying,reorder,unsub_applying,backlog_at_cap,body_lead,\
avc_sum,avc_cnt,afq_sum,afq_cnt,\
vr_sum,vr_cnt,sc_sum,sc_cnt,\
cft_sum,cft_cnt,trd_sum,trd_cnt,\
wi_sum,wi_cnt,wpark,wpvs,\
ck_intake,ck_release,ck_queued,apply_inflight,\
aw_sum,aw_cnt,c2c_sum,c2c_cnt,\
sa_sum,sa_cnt,scl_sum,scl_cnt,cinf,bsz_sum,bsz_cnt" > "$CSV"

read pcpu < <(pstat); prevh=0; START=$(date +%s)
while kill -0 "$PID" 2>/dev/null; do
  el=$(( $(date +%s)-START )); [ "$el" -ge "$MAXSEC" ] && { echo "[$LABEL] wall cap"; break; }
  sleep 5
  m=$(curl -s --max-time 4 "http://127.0.0.1:$MET/metrics" 2>/dev/null)
  read ncpu < <(pstat)
  h=$(mv state_finalized_block_height "$m"); h=${h:-0}
  # Zakura overlay health (block data flows over Zakura, not the legacy net/sync counters).
  zkp=$(mv zakura_p2p_conn_active "$m"); zkq=$(mv zakura_p2p_queue_depth "$m")
  zkbs=$(awk '$1 ~ /^zakura_p2p_stream_accepted\{.*block_sync/ {print $2; exit}' <<<"$m")
  # commit CPU: note-commitment compute phases (behind the commit-metrics build feature).
  btx_s=$(mv zakura_state_write_block_tx_count_sum "$m");                      btx_c=$(mv zakura_state_write_block_tx_count_count "$m")
  cc_s=$(mv zakura_state_write_commitment_check_duration_seconds_sum "$m");    cc_c=$(mv zakura_state_write_commitment_check_duration_seconds_count "$m")
  ut_s=$(mv zakura_state_write_update_trees_duration_seconds_sum "$m");        ut_c=$(mv zakura_state_write_update_trees_duration_seconds_count "$m")
  hp_s=$(mv zakura_state_commit_history_push_duration_seconds_sum "$m");       hp_c=$(mv zakura_state_commit_history_push_duration_seconds_count "$m")
  ckc_s=$(mv zakura_state_write_checkpoint_compute_duration_seconds_sum "$m"); ckc_c=$(mv zakura_state_write_checkpoint_compute_duration_seconds_count "$m")
  # commit DB: spent-UTXO reads + address-balance reads + batch build + rocksdb write.
  sur_s=$(mv zakura_state_write_spent_utxo_reads_duration_seconds_sum "$m");   sur_c=$(mv zakura_state_write_spent_utxo_reads_duration_seconds_count "$m")
  ar_s=$(mv zakura_state_write_address_reads_duration_seconds_sum "$m");       ar_c=$(mv zakura_state_write_address_reads_duration_seconds_count "$m")
  bp_s=$(mv zakura_state_write_batch_prep_duration_seconds_sum "$m");          bp_c=$(mv zakura_state_write_batch_prep_duration_seconds_count "$m")
  bc_s=$(mv zakura_state_rocksdb_batch_commit_duration_seconds_sum "$m");      bc_c=$(mv zakura_state_rocksdb_batch_commit_duration_seconds_count "$m")
  bb_s=$(mv zakura_state_write_batch_bytes_sum "$m");                          bb_c=$(mv zakura_state_write_batch_bytes_count "$m")
  vf=$(mv state_vct_fast_block_count "$m"); vl=$(mv state_vct_legacy_block_count "$m")
  # Floor-gap attribution: frontiers + the per-reason state-tick counters (why the
  # next-to-commit height is not advancing). commit_gap = download_floor-verified_tip.
  dlf=$(mv sync_block_download_floor_height "$m");   vtip=$(mv sync_block_verified_tip_height "$m")
  # Body lead/backlog: contiguous downloaded/queued bodies ahead of finalized state
  # = body floor (download_floor) - finalized tip (state_finalized_block_height).
  # Large => bodies are downloaded but not yet finalized (downstream/glue-bound);
  # ~0 => finalized keeps up with the body floor (download/supply is the limiter).
  blead=$(awk -v a="${dlf:-0}" -v b="${h:-0}" 'BEGIN{print (a>0 ? a-b : 0)}')
  cgap=$(mv sync_block_commit_gap_height "$m");      cstall=$(mv sync_block_commit_frontier_stall_seconds "$m")
  miss=$(mv sync_block_missing_bodies "$m");         outs=$(mv sync_block_outstanding "$m")
  fclaim=$(mv sync_block_floor_claim_count "$m")
  fg_out=$(fg outstanding "$m");                     fg_q=$(fg queued "$m")
  fg_ns=$(fg needed_unscheduled "$m");               fg_if=$(fg in_flight_without_outstanding "$m")
  fg_ab=$(fg absent "$m");                           fg_no=$(fg none "$m")
  # Apply-queue depth (Evan's HOL-vs-glue test): contiguous applying queue is
  # capped at 400 (MAX_CHECKPOINT_HEIGHT_GAP). applying~400 => glue/commit-bound;
  # applying<400 with high reorder => download HOL; both low => supply starvation.
  applying=$(mv sync_block_applying "$m");            reorder=$(mv sync_block_reorder "$m")
  unsub=$(mv sync_block_unsubmitted_applying "$m");   atcap=$(mv sync_block_backlog_at_cap "$m")
  # Per-apply latency decomposition (commit-metrics): verify+commit roundtrip and the
  # post-commit frontier re-read, summed across apply-class labels. Pinpoints where the
  # serial apply-drain wall goes (verifier+handoff vs read_state) while CPU sits idle.
  avc_s=$(msum zebra_zakura_apply_verify_commit_duration_seconds_sum "$m");  avc_c=$(msum zebra_zakura_apply_verify_commit_duration_seconds_count "$m")
  afq_s=$(msum zebra_zakura_apply_frontier_query_duration_seconds_sum "$m"); afq_c=$(msum zebra_zakura_apply_frontier_query_duration_seconds_count "$m")
  # LEVER 0 split (commit-metrics): checkpoint VERIFY (process_checkpoint_range CPU) vs
  # the single-writer STATE COMMIT await. Tells us which serial stage caps throughput.
  vr_s=$(mv zakura_consensus_checkpoint_verify_range_duration_seconds_sum "$m");  vr_c=$(mv zakura_consensus_checkpoint_verify_range_duration_seconds_count "$m")
  sc_s=$(mv zakura_consensus_checkpoint_state_commit_duration_seconds_sum "$m");  sc_c=$(mv zakura_consensus_checkpoint_state_commit_duration_seconds_count "$m")
  # PROBE 2: total single-writer service per block (cft) vs the per-block tree-read/clone
  # setup (trd). cft-write_block localizes the ~15ms un-instrumented serial cost.
  cft_s=$(mv zakura_state_commit_commit_finalized_total_duration_seconds_sum "$m"); cft_c=$(mv zakura_state_commit_commit_finalized_total_duration_seconds_count "$m")
  trd_s=$(mv zakura_state_commit_tree_read_duration_seconds_sum "$m");              trd_c=$(mv zakura_state_commit_tree_read_duration_seconds_count "$m")
  # PROBE 3: writer starvation — idle gap between commits (wi) + 10ms empty-channel
  # polls (wpark). Δwi_sum/Δt = fraction of wall the single writer sat idle.
  wi_s=$(mv zakura_state_write_writer_idle_duration_seconds_sum "$m");  wi_c=$(mv zakura_state_write_writer_idle_duration_seconds_count "$m")
  wpark=$(awk '$1=="zakura_state_write_writer_park_total" || $1=="zakura_state_write_writer_park"{print $2; exit}' <<<"$m")
  # PROBE 3b: the VCT-successor-deferral park (the OTHER park site) — commits paced
  # one-behind by successor arrival. If wpvs ~= writer_idle, this is the bottleneck.
  wpvs=$(awk '$1=="zakura_state_write_writer_park_vct_successor_total" || $1=="zakura_state_write_writer_park_vct_successor"{print $2; exit}' <<<"$m")
  # PROBE 4: verifier->writer delivery side. intake/release rates + queue depth + apply
  # in-flight concurrency, to see why ~4000 buffered blocks only reach the writer at ~34/s.
  ckin=$(awk '$1=="checkpoint_commit_intake_count_total"||$1=="checkpoint_commit_intake_count"{print $2;exit}' <<<"$m")
  ckrel=$(awk '$1=="checkpoint_commit_release_count_total"||$1=="checkpoint_commit_release_count"{print $2;exit}' <<<"$m")
  ckq=$(mv checkpoint_queued_slots "$m");   apif=$(mv zebra_zakura_apply_inflight "$m")
  # PROBE 4b: split the apply roundtrip — admit_wait (poll_ready, before the verifier
  # accepts) vs call_to_commit (accepted->committed). Localizes the ~34s outside the verifier.
  aw_s=$(mv zebra_zakura_apply_admit_wait_duration_seconds_sum "$m");      aw_c=$(mv zebra_zakura_apply_admit_wait_duration_seconds_count "$m")
  c2c_s=$(mv zebra_zakura_apply_call_to_commit_duration_seconds_sum "$m"); c2c_c=$(mv zebra_zakura_apply_call_to_commit_duration_seconds_count "$m")
  # PROBE 5: the NEXT door — state-service admission (poll_ready behind the state Buffer)
  # vs the actual commit call, + commits in flight at the writer.
  sa_s=$(mv zakura_state_commit_state_admit_wait_duration_seconds_sum "$m");  sa_c=$(mv zakura_state_commit_state_admit_wait_duration_seconds_count "$m")
  scl_s=$(mv zakura_state_commit_state_call_duration_seconds_sum "$m");       scl_c=$(mv zakura_state_commit_state_call_duration_seconds_count "$m")
  cinf=$(mv zakura_state_commit_inflight "$m")
  # Batched body commit: blocks per DiskWriteBatch (>1 means batching is active).
  bsz_s=$(mv zakura_state_commit_batch_size_sum "$m");  bsz_c=$(mv zakura_state_commit_batch_size_count "$m")

  cores=$(awk -v d=$((ncpu-pcpu)) -v hz=$HZ 'BEGIN{printf "%.2f",d/hz/5}')
  bps=$(awk -v dh=$((h-prevh)) 'BEGIN{printf "%.1f",dh/5}')
  echo "$(date +%s),$el,$h,$bps,$cores,\
${zkp:-0},${zkq:-0},${zkbs:-0},\
${btx_s:-0},${btx_c:-0},\
${cc_s:-0},${cc_c:-0},${ut_s:-0},${ut_c:-0},${hp_s:-0},${hp_c:-0},${ckc_s:-0},${ckc_c:-0},\
${sur_s:-0},${sur_c:-0},${ar_s:-0},${ar_c:-0},${bp_s:-0},${bp_c:-0},${bc_s:-0},${bc_c:-0},${bb_s:-0},${bb_c:-0},\
${vf:-0},${vl:-0},\
${dlf:-0},${vtip:-0},${cgap:-0},${cstall:-0},${miss:-0},${outs:-0},${fclaim:-0},\
${fg_out:-0},${fg_q:-0},${fg_ns:-0},${fg_if:-0},${fg_ab:-0},${fg_no:-0},\
${applying:-0},${reorder:-0},${unsub:-0},${atcap:-0},${blead:-0},\
${avc_s:-0},${avc_c:-0},${afq_s:-0},${afq_c:-0},\
${vr_s:-0},${vr_c:-0},${sc_s:-0},${sc_c:-0},\
${cft_s:-0},${cft_c:-0},${trd_s:-0},${trd_c:-0},\
${wi_s:-0},${wi_c:-0},${wpark:-0},${wpvs:-0},\
${ckin:-0},${ckrel:-0},${ckq:-0},${apif:-0},\
${aw_s:-0},${aw_c:-0},${c2c_s:-0},${c2c_c:-0},\
${sa_s:-0},${sa_c:-0},${scl_s:-0},${scl_c:-0},${cinf:-0},${bsz_s:-0},${bsz_c:-0}" >> "$CSV"
  pcpu=$ncpu; prevh=$h
done
fin=$(mv state_finalized_block_height "$(curl -s http://127.0.0.1:$MET/metrics 2>/dev/null)")
echo "[$LABEL] DONE height=${fin:-?}  csv=$CSV  log=$LOG  traces=$TRACEDIR"
echo "[$LABEL] cleanup: rm -rf $FORK   (keeps source $MASTER pristine)"
