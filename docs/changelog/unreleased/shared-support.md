<!-- changelog: none -->

This PR adds simulator and fuzz test support: machine-trace records and
shrinking, a shared machine adapter, a campaign lock probe fix, a boundary-read
guard, and an empty `qa/fuzz/p2p` package. `zakura-rt` changes only its simulator
build, and `zakura-network` gains a feature no production build enables. Neither
changes `zakurad`.

The PR number is not assigned yet. `scripts/changelog.py` requires a numeric
name, so rename this file to `<PR-number>.md` when the PR opens.
