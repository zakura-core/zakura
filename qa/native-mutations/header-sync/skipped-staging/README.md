# Skipped staging

`skipped-staging.patch` has `PeerWorkQueue::stage_page` take a page's capacity
from its round's reservation without adding the page to the round. The staging
report comes from after the insertion, with the headers the round then holds,
so the round shows fewer than it staged. The patch also drops the queue's
debug assertion, which would catch the same mismatch, as a build without debug
assertions does.

The witness is `two_forks_stage_and_prepare_their_own_pages` in
`crates/zakura-sim/tests/header_node_reservations.rs`. Each mirror's round
stages pages. The reactor tap's ownership ledger fails the run with
`UnstagedPage: a page of 5 headers with 5 hashes left round 1 holding 0 of the
5 it staged`.

Run it from a clean checkout:

```sh
export RUSTFLAGS="--cfg zakura_sim"
cargo test --locked --profile planted -p zakura-sim --test header_node_reservations \
  two_forks_stage_and_prepare_their_own_pages
git apply qa/native-mutations/header-sync/skipped-staging/skipped-staging.patch
cargo test --locked --profile planted -p zakura-sim --test header_node_reservations \
  two_forks_stage_and_prepare_their_own_pages
git apply -R qa/native-mutations/header-sync/skipped-staging/skipped-staging.patch
```

Restore the patch even if the mutant fails unexpectedly. The baseline and
restored runs must pass. Only the `UnstagedPage` harness failure counts as
detection. A build error, a timeout, a panic or another property's failure
doesn't.
