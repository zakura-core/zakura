#!/usr/bin/env python3
"""Steady-state commit-pipeline attribution for feed_run.sh CSVs.

Reports the note-commitment commit pipeline that gates the single-writer:
the CPU compute phases (checkpoint_compute = commitment_check ∥ update_trees,
then history_push) and the DB phases (spent-UTXO reads, address reads, batch
build, rocksdb write). Per-block times come from the histogram sum/count deltas
(1000*Δsum/Δcount = ms/block). Throughput is the height rate; supply health is
the Zakura overlay (peers / queue depth / block-sync streams).

Usage: feed_analyze.py CSV [h_lo h_hi]
  No window -> steady-state middle 60% of rows (skips warm-up + final flush).
"""
import csv, sys

def load(path):
    with open(path) as f:
        return [r for r in csv.DictReader(f)]

def fnum(r, k):
    try: return float(r.get(k, 0) or 0)
    except (ValueError, TypeError): return 0.0

def window_indices(rows, h_lo, h_hi):
    hs = [fnum(r, "height") for r in rows]
    if h_lo is None:
        return int(len(rows)*0.2), int(len(rows)*0.8)
    lo = next((i for i,h in enumerate(hs) if h >= h_lo), 0)
    hi = next((i for i,h in enumerate(hs) if h >= h_hi), len(rows)-1)
    return lo, hi

def per_block(a, b, key):
    """ms per block from a monotonic histogram's sum/count columns."""
    ds = fnum(b, key+"_sum") - fnum(a, key+"_sum")
    dc = fnum(b, key+"_cnt") - fnum(a, key+"_cnt")
    return (1000.0*ds/dc) if dc > 0 else 0.0

def avg(rows, key):
    return sum(fnum(r, key) for r in rows)/len(rows) if rows else 0.0

def main():
    if len(sys.argv) < 2:
        print(__doc__); sys.exit(1)
    rows = load(sys.argv[1])
    if len(rows) < 3:
        print("not enough samples yet"); sys.exit(0)
    h_lo = float(sys.argv[2]) if len(sys.argv) > 2 else None
    h_hi = float(sys.argv[3]) if len(sys.argv) > 3 else None
    lo, hi = window_indices(rows, h_lo, h_hi)
    a, b = rows[lo], rows[hi]
    win = rows[lo:hi+1]

    dh = fnum(b,"height") - fnum(a,"height")
    dt = fnum(b,"elapsed") - fnum(a,"elapsed")
    blk_s = dh/dt if dt > 0 else 0
    ms_per_block = 1000.0/blk_s if blk_s > 0 else 0

    # commit CPU phases (commitment_check ∥ update_trees run in parallel inside
    # checkpoint_compute; history_push joins them — so ckc is the compute wall time).
    cc  = per_block(a,b,"cc")    # commitment_check
    ut  = per_block(a,b,"ut")    # update_trees (note-commitment trees)
    hp  = per_block(a,b,"hp")    # history MMR push
    ckc = per_block(a,b,"ckc")   # checkpoint_compute (compute wall: cc∥ut + hp)
    # commit DB phases (sequential on the writer's critical path).
    sur = per_block(a,b,"sur")   # spent-UTXO reads (∥ raw-tx serialize)
    ar  = per_block(a,b,"ar")    # address-balance reads
    bp  = per_block(a,b,"bp")    # batch build (prepare_block_batch)
    # rocksdb write is recorded once per DiskWriteBatch, so its histogram count is
    # BATCHES (not blocks) when batch_commit_max>1. Normalize by committed blocks
    # (dh) so commit_wall/util stay per-block instead of inflating ~K×. For K=1 one
    # batch == one block, so this equals the old per_block(...,"bc").
    bc_ds = fnum(b,"bc_sum") - fnum(a,"bc_sum")
    bc  = (1000.0*bc_ds/dh) if dh > 0 else 0.0   # rocksdb write, ms per committed block
    btx = per_block(a,b,"btx")   # mean tx / block (block_tx_count histogram)

    commit_wall = ckc + sur + ar + bp + bc          # writer per-block busy time
    util = commit_wall/ms_per_block if ms_per_block > 0 else 0
    db_total  = sur + ar + bp + bc
    cpu_total = ckc

    # committed batch size -> write throughput (separates block size from disk speed).
    bb_ds = fnum(b,"bb_sum") - fnum(a,"bb_sum"); bb_dc = fnum(b,"bb_cnt") - fnum(a,"bb_cnt")
    mb_per_block = (bb_ds/bb_dc/1e6) if bb_dc > 0 else 0.0
    write_mbps   = (bb_ds/dt/1e6)    if dt > 0 else 0.0
    ms_per_mb    = (bc/mb_per_block)  if mb_per_block > 0 else 0.0

    vf = fnum(b,"vct_fast")   - fnum(a,"vct_fast")
    vl = fnum(b,"vct_legacy") - fnum(a,"vct_legacy")
    peers = avg(win,"zk_peers"); qd = avg(win,"zk_qdepth"); bss = avg(win,"zk_bs_streams")
    cores = avg(win,"cpu_cores")

    bps = [fnum(r,"blk_s") for r in win]
    gap_frac = (sum(1 for x in bps if x < 5)/len(bps)) if bps else 0
    burst = (sum(x for x in bps if x >= 5)/max(1,sum(1 for x in bps if x >= 5)))

    print(f"window: height {fnum(a,'height'):.0f} -> {fnum(b,'height'):.0f}  "
          f"({dh:.0f} blocks, {dt:.0f}s, {len(win)} samples)")
    print(f"throughput: {blk_s:.1f} blk/s  ({ms_per_block:.2f} ms/block wall)   "
          f"VCT fast/legacy: {vf:.0f}/{vl:.0f}   block size ~{btx:.0f} tx\n")

    print(f"COMMIT pipeline  busy={commit_wall:.2f} ms/blk  util={util*100:.0f}% of wall  cpu~{cores:.1f} cores")
    print(f"  CPU {cpu_total:.2f}   checkpoint_compute={ckc:.2f}  "
          f"(commitment_check={cc:.2f} ∥ note_tree={ut:.2f}, history_push={hp:.2f})")
    print(f"  DB  {db_total:.2f}   spent_utxo_reads={sur:.2f}  address_reads={ar:.2f}  "
          f"batch_prep={bp:.2f}  rocksdb_write={bc:.2f}")
    print(f"  write  {mb_per_block:.3f} MB/block  {write_mbps:.1f} MB/s  "
          f"(rocksdb_write = {ms_per_mb:.1f} ms/MB)")

    # Per-apply latency (block_sync_driver): the serial apply-drain wall. verify_commit
    # is the verifier roundtrip through the single-writer state; commit-DB (cc..bc) is a
    # subset of it, so (verify_commit - db_total) ≈ verify + async handoff/await.
    avc = per_block(a, b, "avc")   # verify+commit per apply
    afq = per_block(a, b, "afq")   # post-commit frontier re-read per apply
    handoff = avc - db_total
    print(f"  apply  verify_commit={avc:.2f}  frontier_query={afq:.2f} ms/apply   "
          f"(commit-DB {db_total:.2f} of verify_commit; verify+handoff ≈ {handoff:.2f})")

    # LEVER 0: which serial stage caps throughput — checkpoint verify or state commit?
    # verify amortized per committed block (process_checkpoint_range is per-range);
    # state_commit is per-block (the single-writer await). commit-DB (db_total) is the
    # RocksDB write inside state_commit; (state_commit - db_total) = poll_ready/queue wait.
    dvr = fnum(b,"vr_sum") - fnum(a,"vr_sum")
    verify_pb = (1000.0*dvr/dh) if dh > 0 else 0.0
    sc = per_block(a, b, "sc")
    if verify_pb or sc:
        sink = "VERIFY-bound" if verify_pb > sc else "COMMIT-bound"
        print(f"  LEVER0 verify={verify_pb:.2f}  state_commit={sc:.2f} ms/blk   "
              f"(commit-DB {db_total:.2f}, writer-queue ≈ {sc-db_total:.2f})  -> serial stage: {sink}")

    # PROBE 2: localize the ~15ms gap inside the single writer. commit_finalized_total =
    # the writer's per-block service time; tree_read = history-tree DB read + tree clones.
    # gap = total - db_total - tree_read (VCT fold + write_block internals + bookkeeping).
    cft = per_block(a, b, "cft")
    trd = per_block(a, b, "trd")
    if cft:
        gap = cft - db_total - trd
        print(f"  PROBE2 writer_service={cft:.2f} ms/blk = tree_read {trd:.2f} + commit-DB "
              f"{db_total:.2f} + other {gap:.2f}   (other = VCT fold / write_block-internals / bookkeeping)")

    # PROBE 3: is the single writer STARVED or SATURATED? Δwi_sum/Δt = fraction of wall
    # the writer sat idle between commits; writer_park counts 10ms empty-channel polls.
    dwi = fnum(b,"wi_sum") - fnum(a,"wi_sum")
    idle_frac = (dwi/dt) if dt > 0 else 0.0
    wi_pb = per_block(a, b, "wi")
    park_rate = (fnum(b,"wpark") - fnum(a,"wpark"))/dt if dt > 0 else 0.0
    if dwi or park_rate:
        vcts_rate = (fnum(b,"wpvs") - fnum(a,"wpvs"))/dt if dt > 0 else 0.0
        # Attribute the idle: empty-channel park vs VCT-successor-deferral park.
        if vcts_rate > park_rate * 2:
            state = "WAITING ON SUCCESSOR (VCT one-behind commit dependency)"
        elif idle_frac > 0.5:
            state = "STARVED on input channel (cadence-bound)"
        else:
            state = "SATURATED (commit-bound)"
        print(f"  PROBE3 writer_idle={idle_frac*100:.0f}% of wall  ({wi_pb:.1f} ms/blk idle vs {cft:.2f} commit)  "
              f"park_empty={park_rate:.0f}/s  park_successor={vcts_rate:.0f}/s (~{vcts_rate*10:.0f} ms/s)  -> {state}")

    # PROBE 4: verifier->writer delivery side. release rate = blocks/s leaving for the
    # writer; queued residency = queue depth / release rate; apply_inflight = achieved
    # concurrency vs the ~400 checkpoint cap.
    intake_rate  = (fnum(b,"ck_intake")  - fnum(a,"ck_intake"))  / dt if dt > 0 else 0.0
    release_rate = (fnum(b,"ck_release") - fnum(a,"ck_release")) / dt if dt > 0 else 0.0
    ck_queued = avg(win, "ck_queued")
    ap_if = avg(win, "apply_inflight")
    resid = (ck_queued / release_rate) if release_rate > 0 else 0.0
    if intake_rate or release_rate:
        cap = "CONCURRENCY-bound (~400 cap)" if ap_if >= 360 else "SUBMISSION-fed below cap"
        print(f"  PROBE4 verifier intake={intake_rate:.0f}/s  release={release_rate:.0f}/s  "
              f"queued~{ck_queued:.0f} (resid ~{resid:.1f}s)  apply_inflight~{ap_if:.0f}  -> {cap}")

    # PROBE 4b: split the apply roundtrip. admit_wait = poll_ready (before the verifier
    # accepts); call_to_commit = accepted -> committed (~0.5s verifier residency + the
    # release->commit roundtrip). Whichever dominates says which side of the verifier the
    # ~34s lives on -> upstream backpressure vs the commit roundtrip (batch-commit lever).
    aw  = per_block(a, b, "aw")    # admit_wait s/apply (note: seconds, large)
    c2c = per_block(a, b, "c2c")   # call_to_commit s/apply
    if aw or c2c:
        side = ("ADMIT-bound (upstream of commit; batching won't help)" if aw > c2c
                else "COMMIT-side (release->commit roundtrip; batch-commit is the lever)")
        print(f"  PROBE4b admit_wait={aw/1000:.1f}s/apply  call_to_commit={c2c/1000:.1f}s/apply  -> {side}")

    # PROBE 5: the NEXT door. state_admit = poll_ready behind the state Buffer;
    # state_call = the actual commit. If raising VERIFIER_BUFFER_BOUND just moves the
    # throttle here, state_admit balloons and cinf (commits in flight) piles up.
    sa  = per_block(a, b, "sa")    # state admit_wait s/apply
    scl = per_block(a, b, "scl")   # state call (commit) s/apply
    cinf = avg(win, "cinf")
    if sa or scl:
        nxt = ("STATE-ADMIT-bound (throttle moved to the state Buffer)" if sa > scl
               else "state commit itself (writer) is the cost")
        print(f"  PROBE5 state_admit={sa:.1f}ms  state_call={scl:.1f}ms/commit  commits_inflight~{cinf:.0f}  -> {nxt}")

    print(f"\nZAKURA supply: peers~{peers:.0f}  queue_depth~{qd:.0f}  block_sync_streams~{bss:.0f}")
    print(f"           burst rate={burst:.0f} blk/s during active samples; "
          f"idle (blk_s<5) fraction={gap_frac*100:.0f}% of wall")

    # verdict
    phases = {"checkpoint_compute(note_tree+history)":ckc, "spent_utxo_reads":sur,
              "address_reads":ar, "batch_prep":bp, "rocksdb_write":bc}
    top, topv = max(phases.items(), key=lambda kv: kv[1])
    if util >= 0.70:
        v = (f"COMMIT-BOUND ({util*100:.0f}% writer util) — dominant phase: {top} "
             f"({topv:.2f} ms/blk). CPU {cpu_total:.2f} vs DB {db_total:.2f} ms/blk.")
    elif gap_frac > 0.30:
        v = (f"SUPPLY-BOUND — the writer is only {util*100:.0f}% busy and {gap_frac*100:.0f}% "
             f"of wall has ~no progress: the Zakura cohort isn't delivering blocks fast "
             f"enough to keep the committer fed (queue_depth~{qd:.0f}).")
    else:
        v = (f"BALANCED — writer {util*100:.0f}% busy, no large idle gaps; "
             f"heaviest commit phase is {top} ({topv:.2f} ms/blk).")
    print(f"\nVERDICT: {v}")

if __name__ == "__main__":
    main()
