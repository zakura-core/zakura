# IO ignores the policy's demotion

`SequencerPayloads::apply` (`sequencer_task.rs`) ignores
`PayloadChange::KeepRaw`. The policy demotes a body to its raw bytes and
records a decoded size of zero, but the payload table keeps the decoded
block. At the next sequencer boundary the payload's measured decoded size
differs from its policy entry.

The existing planted case `lost-payload-release` ignores
`PayloadChange::Release`. This patch ignores the other payload change.

## Guards on the rule's path

1. `SequencerPayloads::apply` is the only place IO applies a policy
   demotion. No later step drops the decoded copy.

## Witness

`qa/planted/block-sync/submit-only-requested/height-only.toml`: the
committed input `block-sync-reset-mid-flight.bin`. Two mirrors serve
sixteen bodies, so bodies reach the sequencer out of height order. Each
body arrives decoded beside its raw bytes. When the sequencer accepts a
body above the next height, the policy demotes it to its raw bytes, and
the body waits in the reorder buffer.

The work-list witness, `lost-payload-release`, runs
`block-sync-handoff.bin`. Its one mirror serves eight bodies in height
order, and every verdict commits, so the policy never demotes a body.

## Reproduce

The loop applies the patch in a worktree of its own and runs each case's
witness on the patched build:

```sh
cargo xtask planted loop --cases <dir> --only block-sync/payload-inventory-agrees --out <report dir>
```

Required failure: the run ends at `block-sync/payload-inventory-agrees`.
