# NU7 / NSM release confidence

The release gate must run against a separately built production artifact. Rust
unit tests enable a test-only NU7 branch ID and can pass even when the deployed
binary cannot validate NU7 transactions. Never use a test-built node as evidence
that production NU7 support works.

## Coverage

| Layer | Added checks |
| --- | --- |
| Chain accounting | Generated six-pool distributions, stale NSM counters, idempotent activation seeding, wrong-height no-op, oversupply rejection |
| Contextual state | Mature transparent spends with independently calculated fee recycling, reward rounding and balances; activation and delayed reissuance; derived and overridden seeds; fixed edge cases and generated histories |
| State persistence | Finalization, reopen, finalized rollback and checkpoint replay agree with the independently calculated history |
| Forks | Different fee histories produce different bonuses; invalidation, reconsideration and winning-branch finalization agree with fresh replay |
| Rejection atomicity | Duplicate/value-creating spends and aggregate-cap violations preserve the entire populated fork; rejected checkpoint commits survive reopen without persisting UTXO, nullifier or tree changes; valid siblings still commit |
| PoW | Header and block checks accept the independently fixed correct threshold and reject the wrong averaging window before, at and after activation |
| Production compatibility | Non-test `xtask nu7-readiness` checks real branch/dependency compatibility, V5/V6 serialization and digest round trips, wrong signature context and unknown branch decoding |
| Production transparent transactions | Signed mature P2PK spends, corrupt signatures, wrong branch IDs, exact reward and ±1 payout rejection, proposals and independent second-node submission |
| Production shielded outputs | Real shielded coinbases, corrupt proof rejection with unchanged tip, valid sibling acceptance across activation and reissuance |
| RPC/mempool/forks | Queued V4 transaction revalidation, restart, long-poll refresh, invalidate/reconsider, competing branches and P2P convergence |
| Upgrade/recovery | Previous released binary creates a database containing finalized blocks; new binary reopens it and matches fresh replay, then crosses both boundaries and rehearses graceful and abrupt restart |

The contextual shielded-state fixture deliberately bypasses cryptographic
verification. Its synthetic Ironwood bundle proves state atomicity, not proof
soundness. The production shielded cases verify coinbase output proofs; they do
not provide a valid shielded-spend history. Such a history remains required for
full end-to-end shielded-spend release evidence.

## Commands

Run the deterministic and generated contextual tests:

```sh
cargo test --locked -p zakura-chain --lib nsm_release
cargo test --locked -p zakura-state --lib nsm_release
cargo test --locked -p zakura-state --lib max_money
cargo test --locked -p zakura-state --lib block_daa_enforces
NSM_RELEASE_CASES=256 cargo test --locked -p zakura-state --lib nsm_release_generated
```

Run the production prerequisite separately from test compilation. Do not add
`zakura-test` or `proptest-impl` features to make it pass:

```sh
cargo run --locked --release -p xtask -- nu7-readiness
cargo build --locked --release -p zakura --bin zakurad
mkdir -p target/nu7-production-artifact
cp target/release/zakurad target/nu7-production-artifact/zakurad
NSM_RELEASE_ACCEPTANCE=1 \
NSM_RELEASE_NODE="$PWD/target/nu7-production-artifact/zakurad" \
  cargo test --locked --release -p zakura --test acceptance nsm_release_ -- --nocapture
```

The opt-in acceptance cases return early unless `NSM_RELEASE_ACCEPTANCE=1`.
An ordinary test-suite pass therefore is **not** production readiness evidence.
Copying the artifact before compiling tests prevents Cargo feature unification
from replacing it with a binary containing test-only consensus support.

The upgrade harness requires explicit binaries and an empty artifact directory:

```sh
python3 scripts/test-nsm-release-upgrade.py \
  --old-binary /path/to/v1.4.0/zakurad \
  --new-binary "$PWD/target/nu7-production-artifact/zakurad" \
  --artifacts /path/to/empty/nu7-upgrade-evidence
```

It preserves configurations, node logs and raw blocks. The GitHub workflow
`nu7-release-tests.yml` downloads the Linux v1.4.0 artifact, verifies its published
checksum, runs the gate and uploads upgrade evidence. Scheduled/manual runs also
extend the generated histories to 256 cases. The workflow does not configure
GitHub branch protection; maintainers must select it as a required check if desired.

## Evidence and remaining release blockers

Against main `171ef38475cfc042b1cf4af01c8dd4dd589e67d8`:

- The focused chain/state/consensus/RPC suite passed all 100 selected tests.
- All 256 extended generated histories passed. Changed Rust crates passed
  all-target Clippy with warnings denied; workflow lint, formatting and Python
  compilation also passed.
- Three deliberate mutations were detected: counting the NSM counter as issued
  supply, removing the aggregate cap, and changing the fee-recycling fraction.
  The production files were restored after each experiment.
- A normal production-feature debug node accepted signed transparent spends
  through height 103, rejected a corrupt signature and revalidated the pending
  mempool at the boundary. Preparing NU7 height 104 failed with
  `invalid consensus branch id`.
- A current-binary/current-binary harness smoke run created 1,103 blocks, reopened
  the persistent state and matched fresh replay. Node-generated NU7 height 1,104
  failed with `WrongTransactionConsensusBranchId`.
- Real Sapling and Ironwood coinbase proofs were rejected after corruption and their valid
  counterparts accepted at heights 1–3. At NU7 height 4 the valid proposal failed
  with `WrongTransactionConsensusBranchId`.
- The non-test prerequisite fails explicitly because NU7 has no production
  consensus branch ID. The pinned protocol dependency must also recognize the
  authoritative ID; inserting a test placeholder is not a fix.

These failures block the release. Post-NU7 production assertions are implemented
but are not validated by passing contextual tests. The actual previous-release
upgrade must run on Linux; the local macOS rehearsal used the current binary in
both roles and is only evidence for the harness up to activation. A protocol
maintainer must supply the authoritative branch/dependency integration, after
which the complete production gates and valid shielded-spend histories must pass.
Consensus changes also require an independent second reviewer under the shared
reviewer rule.

## Dependency integration identified during testing

[Valargroup librustzcash PR #76](https://github.com/valargroup/librustzcash/pull/76),
head `4c88a0bc791eeb7ffacd65ad01bbc33d0d0d3072`, exposes NU7 without an unstable
build flag and maps it to `0x77190AD8`, matching the
[NU7 deployment draft](https://github.com/zcash/zips/blob/e753a6a301912cf77202db8f0d840f4796f5cca1/zips/draft-arya-deploy-nu7.md).
The readiness command now requires this specific ID.

Testing that exact protocol revision as an isolated dependency override failed
with four `E0004` errors in `zakura-primitives 1.3.0-alpha.1`: its exhaustive
branch matches omit `BranchId::Nu7` without the unstable build flag. That trial
restored the original lockfile; no temporary dependency override is part of this
branch. The current lockfile resolves `zcash_protocol` to 0.10.5.

The required follow-up is to port the relevant unconditional NU7 support into
`zakura-core/common`'s `zakura-primitives`, test and publish a compatible dependency
set, then update Zakura's branch-ID table and dependency versions together.
[Common PR #471](https://github.com/zakura-core/common/pull/471) imports a standalone
protocol fork but explicitly does not migrate the public-type dependency closure,
so it does not complete this integration. Merely changing Zakura's branch ID or
updating its protocol dependency alone does not unblock the release.
