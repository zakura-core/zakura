# Common note-encryption dependency migration

Depends on [Common #540](https://github.com/zakura-core/common/pull/540).
The migration baseline is node main `0a7ecd432` and its resolved
`zcash_encoding 0.4.0` / `zcash_note_encryption 0.4.2` implementations.

The chain's direct note-encryption edge now explicitly selects the Common
`zakura-note-encryption` package with `alloc`. The library target remains
`zcash_note_encryption`; all Sapling, Orchard, Ironwood, and primitives
production types must come from the same Common release. Protocol supplies
encoding internally, so the node no longer compiles a standalone encoding
package after its Common dependency family is migrated.

## Release and API review

At the user's request, release numbers are deferred. The existing `=2.2.0`
requirements are staging values, not permission to republish 2.2.0 or to mix
old published Common crates with the new note-encryption package. Select the
breaking coordinated Common number and rewrite every Common requirement
before merging. Do not automatically bump the node binary's major version.

There are no new public or `pub(crate)` items, enum variants, constructors,
features, aliases, or handwritten signatures in this node change.
`primitives::zcash_note_encryption::decrypts_successfully` retains its public
signature. The direct fork trait calls are private. Existing Common-facing
interfaces (for example Sapling output commitments, tree nodes/frontiers,
conversion trait implementations, and `Transaction::to_librustzcash`'s
crate-visible adapter) need a separate node library SemVer assessment once
the final Common version is selected. A library major requirement does not
force a binary major release.

## Validation

Using temporary local overrides for every published Common package:

- `cargo test -p zakura-chain primitives::zcash_note_encryption --locked`:
  both Orchard/Ironwood domain and V6 Ironwood coinbase routing tests passed.
- `cargo test -p zakura-consensus coinbase_outputs --locked`: three tests
  passed, including real shielded-coinbase and zero-key Orchard vectors.
- `cargo test -p zakura-consensus
  shielded_outputs_are_not_decryptable_for_fake_v5_blocks --locked`: the
  nonzero-key rejection vector test passed.
- `cargo clippy -p zakura-chain --all-targets --no-deps -- -D warnings` and
  `cargo fmt --all -- --check`: passed.
- `cargo tree -p zakura --edges normal,build --prefix none --locked`: no
  `zcash_encoding` or upstream `zcash_note_encryption`; one local
  `zakura-note-encryption`, shared by chain, Orchard, Sapling, and primitives.

The local overrides were `[patch.crates-io]` entries in `.cargo/config.toml`
pointing every published Common package to the isolated migration worktree.
They and their resulting local lockfile are excluded from this draft. The
original registry lockfile is retained until the chosen Common release is
published; do not treat that old lockfile as migration validation. After
Common publishes, regenerate the registry lockfile and repeat locked graph
and recovery checks without overrides before merging or releasing.

Folding encoding removes one compilation unit. Note-encryption ownership
alone does not reduce compilation work. No benchmarks were run.
