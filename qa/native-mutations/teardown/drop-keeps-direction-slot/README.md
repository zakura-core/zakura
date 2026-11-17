# The last owner keeps the direction slot

`Drop for SessionReservation` (`regulation/session_capacity.rs`) forgets
the session's direction permit instead of dropping it. Once a session's
last owner drops, its inbound or outbound slot never returns to the
service.

The existing planted case `kept-session-sender` keeps the reservation
alive through a stray sender. This patch drops the reservation, and the
reservation's own drop leaks the slot.

## Guards on the rule's path

1. `Drop for SessionReservation` returns the direction slot. The patch
   breaks it. No other owner returns a direction slot.

## Witness

`qa/planted/block-sync/teardown-returns-baseline/kept-session-sender.toml`: its typed scenario reserves the node's session
slots for each link. Every link closes, and each reservation's last owner
drops, so every link's direction slot stays held once teardown settles.

## Reproduce

The loop applies the patch in a worktree of its own and runs each case's
witness on the patched build:

```sh
cargo xtask planted loop --cases <dir> --only block-sync/teardown-returns-baseline --out <report dir>
```

Required failure: the run ends at `block-sync/teardown-returns-baseline`.
