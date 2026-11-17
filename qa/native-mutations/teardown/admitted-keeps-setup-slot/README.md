# Admission keeps the setup slots

`SessionReservation::admitted` (`regulation/session_capacity.rs`) takes the
session's setup slots out of the reservation and forgets them, so they
never return to the service. Each admitted session holds one setup slot,
and an inbound one an inbound setup slot too, after its last owner drops.

The existing planted case `kept-session-sender` keeps a whole reservation
alive through a stray sender. This patch drops the reservation but leaks
the setup slots at admission.

## Guards on the rule's path

1. `admitted` returns the setup slots once every member is ready.
2. `Drop for SessionReservation` returns any setup slots `admitted` left.
   The patch takes them before the drop, so the drop finds none.

## Witness

`qa/planted/block-sync/teardown-returns-baseline/kept-session-sender.toml`: its typed scenario reserves the node's session
slots for each link, as the transport does. Every link closes once the node
has asked storage to admit eight bodies, and the routines end. The service
then holds a setup slot for each link it admitted.

## Reproduce

The loop applies the patch in a worktree of its own and runs each case's
witness on the patched build:

```sh
cargo xtask planted loop --cases <dir> --only block-sync/teardown-returns-baseline --out <report dir>
```

Required failure: the run ends at `block-sync/teardown-returns-baseline`.
