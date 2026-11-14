# A returned height keeps its funding

`WorkQueueState::return_owned` (`work_queue/state.rs`) settles a returned
height's charge from the ledger but leaves it out of the transition's
released bytes. The queue then settles nothing from its pooled funding, so
the byte budget keeps every returned estimate while the work queue's tally
and the routines' estimates drop it.

Returns happen on a timeout, a retry ending and a routine's drop. The
work-list witness, `handoff-keeps-charge`, returns nothing, so this case
uses the teardown case's input, whose routines end owing requests.

## Guards on the rule's path

1. `return_owned` reports the released bytes. The patch omits the
   returned heights' charges from them.
2. `release_reserved_and_return_items_detailed_for_owner` settles exactly
   the reported bytes from the pooled funding.

## Witness

`qa/planted/block-sync/teardown-returns-baseline/skip-return-on-drop.toml`: the committed input `block-sync-peers-leave.bin`.
Two mirrors withhold every height from 9 and then leave, so each routine
returns its outstanding requests when it ends. At the next rest the budget
counts the returned estimates, and the tally and the routines count none.
Once teardown settles, the leaked budget also fails
`block-sync/teardown-returns-baseline` in the same step, so the case lists
it in `also_violates`.

## Reproduce

The loop applies the patch in a worktree of its own and runs each case's
witness on the patched build:

```sh
cargo xtask planted loop --cases <dir> --only block-sync/budget-ledgers-agree --out <report dir>
```

Required failure: the run ends at `block-sync/budget-ledgers-agree`.
