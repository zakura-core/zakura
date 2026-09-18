# Common compatibility

This check exercises the pending common cleanup before it is published. `dependencies.patch` pins the complete 17-crate family to
`a036226543f54dbc83c8d27713c7a24864b26301` and includes the matching lockfile. The snapshot retains workspace version
1.2.0, but its git source distinguishes it from the published 1.2.0 crates.

The Common compatibility workflow applies the patch only in its disposable
checkout. Normal builds retain the published dependencies from the root
manifest and lockfile. Their semver, supply-chain, and publish checks continue
to run against that normal graph. Passing this integration job does not certify
the pending snapshot for publication or deployment.

The job verifies one copy of each common crate at the pinned revision, compares
native and primitives hashes and V6 encoding, and constructs transparent and
shielded coinbases with and without the node's `nu7` Cargo feature. The ignored
shielded test generates real Sapling, Orchard, and Ironwood proofs. Common's own
CI separately checks its `zcash_unstable="nu7"` compiler configuration.

For a local run, create a disposable worktree, apply `dependencies.patch` there,
and run the commands from `.github/workflows/common-compatibility.yml`. Reuse a
build cache with `CARGO_TARGET_DIR`. Do not apply the patch in a checkout used for
packaging or release checks.

Refresh the patch and its lockfile together when the candidate revision or base
dependency graph changes. After a reviewed common 1.x release containing the cleanup exists, update the
normal dependencies through the regular dependency and API compatibility checks
and remove this temporary integration patch and job.
