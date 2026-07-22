# Kresko zakura-compat patch

`zakura-compat.patch` makes the [Kresko](https://github.com/valargroup/kresko)
load generator build and run against this repository. It is applied to a fresh
Kresko checkout by `.github/workflows/scripts/mempool-load-run.sh`, and by hand
for local runs.

It is a stopgap. The intent is to upstream these changes to Kresko; once that
lands, this patch and the `git apply` step go away. Until then, `git apply`
failing is a useful signal that Kresko has moved and the harness needs a look.

`ZAKURA_CHECKOUT` in the patch is a placeholder that the runner substitutes with
the path to this checkout.

## What it changes, and why

**1. Crate renames (`Cargo.toml`).** Kresko pins `zebra-chain` / `zebrad` /
`zebra-jsonl-trace` to `valargroup/zebra`. This repo renamed those to
`zakura-chain` / `zakura` / `zakura-jsonl-trace`. Cargo's `package = "..."`
rename keeps the extern names, so every `zebra_chain::…` path in Kresko's source
compiles unchanged. `zakura-chain/src/local_genesis.rs` already exposes exactly
the `generate_local_testnet_with_funded_keys` API Kresko's genesis calls — its
default network name is literally `KreskoLocalGenesis`.

**2. `ZebradConfig` → `ZakuradConfig` (`src/zebra_config.rs`).** Three call
sites; the type was renamed with the crate.

**3. `Solution::check` arity (`src/pow_tuning.rs`).** Zakura's takes a
`&Network` argument that upstream Zebra's does not.

**4. Network upgrade selection (`src/commands/genesis.rs`, `src/config.rs`,
`src/zebra_config.rs`).** Kresko hardcoded NU7 in three separate places: the
seed-chain generator, its `LocalGenesisActivationHeights` struct, and the
activation table written into the deployed node config.

A release-profile zakurad has **no NU7 consensus branch id at all** — it is
gated behind `#[cfg(any(test, feature = "zakura-test"))]` pending librustzcash
(see `zakura-chain/src/parameters/network_upgrade.rs`). A chain that activates
NU7 therefore cannot mine a single block: every attempt is rejected with
`WrongTransactionConsensusBranchId`. The patch makes the upgrade configurable
via `KRESKO_LATEST_NETWORK_UPGRADE` and defaults it to Nu6_3, and makes the
`nu7` activation height optional so it is only declared when actually requested.

**5. Transaction-builder activation heights (`src/txblast/mod.rs`).** This is
the subtle one. `TxblastNetworkParams::LocalGenesis` returned `None` for
`Nu6_1` and `Some(1)` for everything else. But `Nu6_1` *is* the newest upgrade
the build knows about (NU7 and ZFuture are feature-gated out of
`zcash_protocol`), and Kresko's own config writer activates `NU6.1` on the
chain. So the builder signed transactions at Nu6's branch id while the chain
ran NU6.1, and every transaction was rejected with "incorrect consensus branch
id" — after paying for Orchard proving.

The builder's activation table and the chain's must agree exactly. The patch
activates everything the build knows about from height 1, matching the config
writer.

## Verifying a Kresko bump

The failure modes above are all silent until something is rejected, so after
changing `kresko_ref`, run the local rehearsal in
`.github/workflows/scripts/README-mempool-load.md` and confirm you see:

- blocks mined past the seed tip (catches 4)
- `[shielded] steady state ready_lanes=N` followed by `submitted=N, errors=0`
  (catches 5)
- a non-zero mempool on every node
