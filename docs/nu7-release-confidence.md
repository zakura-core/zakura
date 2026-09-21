# NU7 / NSM release confidence

The release gate must run against a separately built production artifact. The
pre-#1093 dependency stack exposed a test-only NU7 branch ID, allowing unit
tests to pass while the deployed binary could not validate NU7 transactions.
Never use a test-built node as evidence that production NU7 support works.

## Coverage

- **Chain accounting**: Generated six-pool distributions, stale NSM counters,
  idempotent activation seeding, wrong-height no-op, oversupply rejection
- **Contextual state**: Mature transparent spends with independently calculated
  fee recycling, reward rounding and balances; activation and delayed
  reissuance; derived and overridden seeds; fixed edge cases and generated
  histories
- **State persistence**: Finalization, reopen, finalized rollback and checkpoint
  replay agree with the independently calculated history
- **Forks**: Different fee histories produce different bonuses; invalidation,
  reconsideration and winning-branch finalization agree with fresh replay
- **Rejection atomicity**: Duplicate/value-creating spends and aggregate-cap
  violations preserve the entire populated fork; rejected checkpoint commits
  survive reopen without persisting UTXO, nullifier or tree changes; valid
  siblings still commit
- **PoW**: Header and block checks accept the independently fixed correct
  threshold and reject the wrong averaging window before, at and after
  activation
- **Production compatibility**: Non-test `xtask nu7-readiness` checks real
  branch/dependency compatibility, V5/V6 serialization and digest round trips,
  wrong signature context and unknown branch decoding
- **Production transparent transactions**: Signed mature P2PK spends, corrupt
  signatures, wrong branch IDs, exact reward and ±1 payout rejection, proposals
  and independent second-node submission
- **Production shielded outputs**: Real shielded coinbases, corrupt proof
  rejection with unchanged tip, valid sibling acceptance across NU7 activation
- **RPC/mempool/forks**: Queued V4 transaction revalidation, restart, long-poll
  refresh, invalidate/reconsider, competing branches and P2P convergence
- **Upgrade/recovery**: Previous released binary creates a database containing
  finalized blocks; new binary reopens it and matches fresh replay, then crosses
  NU7 and rehearses graceful and abrupt restart

Production derives the reissuance height from the subsidy reference crossover.
The short-chain `test_nsm_reissuance_height` override is compiled only into test
builds; the live-node configurations deliberately do not include it. Reissuance
boundary coverage is contextual. These short live chains cover NU7 and do not
claim to reach the production reissuance crossover.

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

## Evidence on PR #1093

The test branch is based on PR #1093 head
`b497ca29b126c8445dcc444e3d663b861e413b25`, using Common revision
`c4c255c4be5cacaad77b918a481cbf6d35b1f8fd` and branch ID `0x77190AD8`.

- The focused chain/state/consensus/RPC suite passed all 110 selected tests.
- All 256 extended generated spending histories passed.
- All-target Clippy with warnings denied, formatting, workflow lint, Markdown
  lint, Python compilation and changelog validation passed.
- The standalone production readiness command passes for V5 and V6.
- All three production-artifact acceptance cases pass: transparent spends and
  competing forks, Sapling coinbase proofs, and Ironwood coinbase proofs.
- Transparent acceptance includes exact rewards, malformed signature/branch and
  ±1 reward rejection, restart, long-poll refresh, mempool revalidation,
  invalidate/reconsider and P2P convergence after a competing branch wins.
- The current-version restart/replay rehearsal passes through height 1,110,
  including graceful stop and abrupt process termination with missing-block replay.
- The actual v1.4.0 source commit
  `1e36d1bb6a8a9778a1bd316704b9c8cb75182de6` was built on macOS and used to
  create the 1,103-block prefix. The new binary recovered the tip from that cache,
  matched independent fresh replay, crossed NU7 and passed both restart cases.
  The old/new state formats are 28.1.5 and 29.0.0; this checks startup recovery
  across that change, not an assertion that RocksDB is migrated in place.
- Three deliberate accounting mutations were detected before the rebase:
  counting the NSM counter as issued supply, removing the aggregate cap and
  changing the fee-recycling fraction. Production sources were restored afterward.

The local binaries use production features with the debug profile. The workflow
uses release-profile binaries and the published Linux v1.4.0 archive; that exact
Linux artifact combination still needs CI execution. The local source build was
pinned to the GitHub release commit because the checkout's local `v1.4.0` tag
pointed to an unrelated historical upstream Zebra release.

## Remaining release conditions

PR #1093 resolves the old production branch-ID/dependency incompatibility. Its
Common Git dependencies are intentionally unpublished, so the crates.io release
still depends on merging and publishing that compatible Common stack.

The full production reissuance crossover and valid shielded-spend histories
remain outside these short-chain acceptance cases. Contextual reissuance tests
and shielded coinbase proof tests must not be presented as that end-to-end evidence.
Consensus changes also require an independent second reviewer under the shared
reviewer rule.
