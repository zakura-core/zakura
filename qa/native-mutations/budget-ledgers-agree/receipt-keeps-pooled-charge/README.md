# A receipt leaves its charge pooled

`WorkQueue::begin_receipt` (`work_queue.rs`) splits only the retired
bytes out of the queue's pooled funding, not the received height's own
charge. The work queue's ledger drops the height's charge at receipt, but
the pooled funding keeps it, so the byte budget never releases it. At the
next rest the budget counts more than the work queue's tally.

The existing planted case `handoff-keeps-charge` forgets the routine's
receipt funding after the handoff. This patch never moves the charge out
of the queue's pool.

## Guards on the rule's path

1. `split_receipt_funding` moves the receipt's charge into the handoff's
   reservation. The patch splits zero bytes for it.
2. `ReceiptSettlement`'s drop releases that reservation. It still runs,
   but holds nothing to release.

## Witness

`the_handoff_input_holds_budget_ledgers_agree` in
`crates/zakura-sim/tests/block_resource_properties.rs` runs the committed
input `block-sync-handoff.bin` through `node_run` before its witness
judgement. The planted case `handoff-keeps-charge` can't witness this patch.
Its trigger history needs a handoff that carries a charge, and the patch
moves no charge into the handoff.
One mirror serves eight bodies, and each receipt leaves its estimate in the budget.

## Reproduce

The loop applies the patch in a worktree of its own and runs each case's
witness on the patched build:

```sh
cargo xtask planted loop --cases <dir> --only block-sync/budget-ledgers-agree --out <report dir>
```

Required failure: the run ends at `block-sync/budget-ledgers-agree`.
