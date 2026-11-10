# The decoded handoff charge releases twice

`DecodedHandoffCharge::hand_over` (`sequencer_task.rs`) no longer clears
the charge before it drops, so the charge returns its decoded bytes to the
input queue's decoded counter at the handoff. The queued body's own
accounting still owns those bytes and releases them again when the body
leaves the queue. The counter saturates at zero, so it undercounts every
queued decoded body, and each routine's room check passes while the queue's
real decoded bytes are far past `BS_MAX_DECODED_SUBMISSION_WINDOW_BYTES`.

The existing planted case `no-room-check` removes the room check. This
patch keeps the check and breaks the counter it reads.

## Guards on the rule's path

1. `charge_decoded_handoff`'s room check, which reads the input queue's
   decoded counter. The patch makes that counter undercount.

No second guard reads the queue's decoded bytes. Admission's resident
estimate reads the same counter.

## Witness

`qa/planted/block-sync/input-decoded-bound/no-room-check.toml`: the committed input `block-sync-decoded-input-stall.bin`.
The sequencer stalls, and the mirror keeps delivering 2 MB bodies after the
queue's decoded bytes reach the cap. The undercounting counter lets every
body keep its decoded block.

## Reproduce

The loop applies the patch in a worktree of its own and runs each case's
witness on the patched build:

```sh
cargo xtask planted loop --cases <dir> --only block-sync/input-decoded-bound --out <report dir>
```

Required failure: the run ends at `block-sync/input-decoded-bound`.
