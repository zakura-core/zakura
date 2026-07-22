# perf-lab report

Last regenerated: 2026-07-22, SESSION 2 close.

1. **WIN (pending confirmation): block-request batching.** Raising
   `max_blocks_per_response` from 1 to 8 gains +5-9% sync throughput; 32
   overshoots into head-of-line losses (inverted-U curve, ledger EXP-002).
   Proposal: default 1→8 once PRs 166/217 release the file, after a
   live-peer sanity run. Config-level evidence, no code changed.
2. **No mainline regression.** 63b8d4dc vs 30b0c63d raced head-to-head:
   +9% for the newer main. The scare was the rig, not the code.
3. **Cohort cache regime decoded.** Fresh-seeded servers serve RAM-warm
   (~190 pc blk/s, unreproducible); steady disk regime is ~105 and
   deterministic to ~0.1% when measured back-to-back. Protocol: measure in
   cadence; settling run after >30 min idle; two-run confirmation for wins.
4. **Standing infrastructure**: frozen serve pair (reaper-exempt, ~$1/h,
   Adam-approved) + all tooling. Next session: exp002-k8-c confirmation,
   then B-04/B-06/B-07 under the steady protocol.

Spend: ~$9 today; ~$22 program-to-date. Batches: 3 participated (6 of 8 in
batch 3 at close).
