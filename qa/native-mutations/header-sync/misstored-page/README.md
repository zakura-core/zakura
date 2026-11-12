# Misstored page

Retired as a duplicate of the planted case
`qa/planted/header-sync/commit-only-authorized/stored-order`, which reverses
the page one call down, in `WorkPolicy::stage_page`, so the round stores the
same headers. On that case's forks-sharing-a-header input both first violate
`header-sync/commit-only-authorized` at step 709 with the same `UnstagedPage`
finding. The witness below is the retired mutation's own.

`misstored-page.patch` has `PeerWorkQueue::stage_page` add a page's headers to
its round in reverse order. The round holds as many headers as it staged, so
only what it stored differs, and the check fires at staging, before any
prepare.

The witness is `two_forks_stage_and_prepare_their_own_pages` in
`crates/zakura-sim/tests/header_node_reservations.rs`. Each mirror's round
stages pages. The reactor tap's ownership ledger fails the run with
`UnstagedPage: a page of 5 headers with 5 hashes left round 1 holding 5 of the
5 it staged, or holding other headers`.

Run it from a clean checkout:

```sh
export RUSTFLAGS="--cfg zakura_sim"
cargo test --locked --profile planted -p zakura-sim --test header_node_reservations \
  two_forks_stage_and_prepare_their_own_pages
git apply qa/native-mutations/header-sync/misstored-page/misstored-page.patch
cargo test --locked --profile planted -p zakura-sim --test header_node_reservations \
  two_forks_stage_and_prepare_their_own_pages
git apply -R qa/native-mutations/header-sync/misstored-page/misstored-page.patch
```

Restore the patch even if the mutant fails unexpectedly. The baseline and
restored runs must pass. Only the `UnstagedPage` harness failure counts as
detection. A build error, a timeout, a panic or another property's failure
doesn't.
