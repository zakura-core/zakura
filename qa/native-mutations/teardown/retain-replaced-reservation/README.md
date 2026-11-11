# Retained reservation after session replacement

The patch retains a clone of the replaced session after the table returns it.
The witness installs real session reservations in the production `SessionTable`.
It retains an old snapshot and a worker while replacing the session, then drops
both owners. The retired reservation must release its slot at that boundary.

Run `cargo test -p zakura-network --lib zakura::regulation::session_table::tests::replacement_keeps_capacity_until_retired_snapshot_owners_drop -- --exact`.
Require one executed test in the unchanged, patched, and restored runs.
The patched run must fail with `retired reservation remains after its last snapshot owner drops`.

This native assertion supplements the wire capacity property. It does not claim
a simulator property kill or generator coverage.

The droplet verified commit `989a9034d2`: the unchanged and restored builds each
passed one exact test. The patched build failed that one test at the declared
assertion. The report and logs are under `/root/handshake-replacement-989/`.
