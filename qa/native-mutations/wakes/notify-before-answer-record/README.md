# The answer notify runs before its record

`SharedPeerState::record_answer` notifies the waiting source before it
records the answer. A source that the final notify wakes can check the
exchange before the record lands. It finds the exchange incomplete, waits
again, and nothing wakes it.

The planted patch `lost-wakeup` breaks the source's order. This patch
breaks the sink's order, so the race moves to the window between the final
notify and the final record. The planted input holds the source until the
sink has run the whole final mark, so it never checks inside that window.

## Witness

`discovery_wait_complete_check_inside_final_mark` in
`crates/zakura-sim/tests/witness.rs`. It runs the planted input up to the
sink's first step of the final mark, then runs the source for four picks.
On `main`, the source's check lands between the final record and the final
notify, and passes. With the patch, the check lands between the final
notify and the final record, and fails. The source then creates its next
`Notified` after the last notify and parks for good.

## Reproduce

The loop applies the patch in its own worktree. Run it over this
property's cases:

```sh
cargo xtask planted loop --only discovery/wait-complete-wakes --out <report dir>
```

Required failure: the run ends at `discovery/wait-complete-wakes`.
