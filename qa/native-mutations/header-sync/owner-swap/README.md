# Owner swap between staging and dispatch

`owner-swap.patch` rebinds an ordinary round's work owner to the next request
ID in `begin_prepare`, after its pages were staged and before its prepare is
dispatched. So the prepare names a request other than the one whose pages it
carries.

The witness is `two_forks_stage_and_prepare_their_own_pages` in
`crates/zakura-sim/tests/header_node_reservations.rs`. Two mirrors on
different forks at the same height each feed the node a round, and every
prepare must hold its own round's pages for the owner they were staged for.
The reactor tap's ownership ledger fails the run with `WrongOwner: call 3
holds round 1 for ... request_id: 2 ..., but its pages were staged for ...
request_id: 1 ...`.

Run it from a clean checkout:

```sh
export RUSTFLAGS="--cfg zakura_sim"
cargo test --locked --profile planted -p zakura-sim --test header_node_reservations \
  two_forks_stage_and_prepare_their_own_pages
git apply qa/native-mutations/header-sync/owner-swap/owner-swap.patch
cargo test --locked --profile planted -p zakura-sim --test header_node_reservations \
  two_forks_stage_and_prepare_their_own_pages
git apply -R qa/native-mutations/header-sync/owner-swap/owner-swap.patch
```

Restore the patch even if the mutant fails unexpectedly. The baseline and
restored runs must pass. Only the `WrongOwner` harness failure counts as
detection. A build error, a timeout, a panic or another property's failure
doesn't.
