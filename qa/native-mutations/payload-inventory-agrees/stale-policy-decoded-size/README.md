# The policy keeps a raw body's size after decode

`Sequencer::prepare_submit` (`sequencer.rs`) no longer records the decoded
size of a body that the submission decoded from its raw bytes. The policy
entry keeps the size it had before the decode, zero for a raw body, while
the payload table holds the decoded block. The `applying` totals still add
the new bytes.

The work-list witness, `lost-payload-release`, hands every body off
decoded and in height order, so no submission decodes one. This case uses
the reset-mid-flight input, whose out-of-order bodies the policy demotes to
their raw bytes.

## Guards on the rule's path

1. `prepare_submit` is the only place the policy learns a materialized
   body's decoded size.

## Witness

`qa/planted/block-sync/submit-only-requested/height-only.toml`: the
committed input `block-sync-reset-mid-flight.bin`. Two mirrors serve
sixteen bodies out of height order. The sequencer keeps each body above the
next height as raw bytes, and decodes it when it submits it. The snapshot
before the `SubmitBlock` send shows the decoded payload beside a policy
size of zero.

The input-decoded case's input, `block-sync-decoded-input-stall.bin`,
can't witness this patch. Storage never answers a submission there, so
the submission window never frees, and no raw body is ever submitted.

## Reproduce

The loop applies the patch in a worktree of its own and runs each case's
witness on the patched build:

```sh
cargo xtask planted loop --cases <dir> --only block-sync/payload-inventory-agrees --out <report dir>
```

Required failure: the run ends at `block-sync/payload-inventory-agrees`.
