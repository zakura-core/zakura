# Common note-encryption dependency migration

Depends on [Common #540](https://github.com/zakura-core/common/pull/540).
Coordinated wallet draft: [#84](https://github.com/zakura-core/wallet-libraries/pull/84).
The migration baseline is node main `0a7ecd432` and its resolved
`zcash_encoding 0.4.0` / `zcash_note_encryption 0.4.2` implementations.

The chain's direct note-encryption edge now explicitly selects the Common
`zakura-note-encryption` package with `alloc`. The library target remains
`zcash_note_encryption`; all Sapling, Orchard, Ironwood, and primitives
production types must come from the same Common release. Protocol supplies
encoding internally, so the node no longer compiles a standalone encoding
package after its Common dependency family is migrated.

## Release and API review

Common 3.0.0 is staged across every Common dependency in this draft.
The new package shares one note-encryption type family with Sapling,
Orchard, Ironwood, and primitives. The node binary remains 1.6.1.

There are no new public or `pub(crate)` items, enum variants, constructors,
features, aliases, or handwritten signatures in this node change.
`primitives::zcash_note_encryption::decrypts_successfully` retains its public
signature. The direct fork trait calls are private. Existing Common-facing
interfaces (for example Sapling output commitments, tree nodes/frontiers,
conversion trait implementations, and `Transaction::to_librustzcash`'s
crate-visible adapter) were reviewed against the current sparse registry index. A library major requirement does not
force a binary major release.

The current sparse index was queried before staging versions. Existing
major bumps cover chain 9.0.0 → 10.0.0, network 9.0.0 → 10.0.0,
node-services 4.0.0 → 5.0.0, state 10.0.0 → 11.0.0, and RPC
12.0.0 → 13.0.0. Consensus exposes the prover and verification type
family, while script/header-chain expose chain types in constructors,
requests, results, and errors. Their pending patch bumps were raised in
place: consensus 10.0.0 → 11.0.0, script 4.0.0 → 5.0.0, and
header-chain 4.0.0 → 5.0.0. `cargo release version` rewrote dependent
requirements. No second major was stacked on an existing pending major.
Utils has an empty public library and keeps its pending 2.2.7 patch over
published 2.2.6. The node binary remains 1.6.1.

Existing public fields, methods, aliases, conversion impls, trait impls,
and feature-gated interfaces exposing these dependencies change identity
transitively; no additional visibility or signature edits were made.
Crate-visible Sapling verifier adapters (`Item::new`, `verify_single`
inputs) and Halo2 `Item::new_with_wtx_id`/verification-cache types carry
that same identity change. See Common's complete API inventory for the
underlying byte-wrapper and trait changes.

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
original registry lockfile is retained until Common 3.0.0 is
published; do not treat that old lockfile as migration validation. After
Common publishes, regenerate the registry lockfile and repeat locked graph
and recovery checks without overrides before merging or releasing.

Folding encoding removes one compilation unit. Note-encryption ownership
alone does not reduce compilation work. No benchmarks were run.
