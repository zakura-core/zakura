# Reissuance accounting

Fee recycling activates at NU7 and reissuance at the ZIP 234 start height.
Both activate by height alone in every build.
Production activation still requires policy guidance and a production NU7 branch ID.

## The seed

The balance holds `INITIAL_NSM_VALUE_BALANCE` on the last block below NU7: the block
subsidy and fees that earlier coinbase transactions never claimed. The 2026-09-15
ZIP Editor call settled this, and zips#1354 defines the constant.

`Network::initial_nsm_value_balance` in
`crates/zakura-chain/src/parameters/network/subsidy.rs` holds the value. Mainnet and
Testnet carry the measured constants. Every other network carries zero, because a chain
with no history before NU7 has nothing to seed, and
`ParametersBuilder::with_initial_nsm_value_balance` overrides it.

Let `N` denote NU7 activation, `S(h)` cumulative scheduled issuance with zero genesis
issuance, `I(h)` the sum of the six monetary pools, and `C` the seed. The stored balance
is:

- `D(h) = 0` for `h < N - 1`, or when the network has no NU7 activation.
- `D(N - 1) = C`.
- `D(h) = C + (S(h) - S(N-1)) - (I(h) - I(N-1))` for `h >= N`.
- For NU7 at genesis there is no seeded block, so `D(h) = S(h) - I(h)`.

`Block::nsm_value_balance_change` in `crates/zakura-chain/src/block.rs` applies the rule
as the chain grows, and the migration in
`crates/zakura-state/src/service/finalized_state/disk_format/upgrade/nsm_value_balance_pool.rs`
applies it to an existing database. Change both together.

## Fee recycling

The NU7 deployment draft's [NSM reserve rules] contribute
`floor(6 * TransactionFees(h) / 10)` from aggregate block fees to NSM.
Contributions start at NU7 activation in every build, including the interval before
reissuance starts. Before NU7, the miner receives all fees.

For example, with 1,000 zatoshi in fees, the miner receives 400 and NSM receives 600.
The calculation rounds down the contribution once per block, so the remainder favors
miners. Two transactions paying one zatoshi each contribute one zatoshi together.
Rounding each transaction separately would incorrectly contribute zero.

`subsidy::miner_fee_share` in `zakura-chain` supplies the same calculation to coinbase
validation and block templates. The coinbase must claim the subsidy plus the miner's
share, subject to the existing funding stream and deferred pool rules. Claiming one
zatoshi more or less than the permitted amount is rejected.

In `getblocktemplate`, non-coinbase `fee` fields still report the full transaction fee.
The coinbase `fee` is the negative amount of fees it collects, excluding the NSM
contribution. For the 1,000-zatoshi example, it is `-400`. Miners that change the
transaction list must recompute the split from the new aggregate fees.

[NSM reserve rules]: https://github.com/zcash/zips/blob/32f447759aba83acfb20aab0757b68147643de22/zips/draft-valargroup-deploy-nu7.md#L120-L149

## Running total

The balance follows this recurrence from NU7 onward:

```text
NSMValueBalance(h) = NSMValueBalance(h - 1) - AdditionalBlockSubsidy(h) + removed(h)
```

Here, `removed(h)` is the fee contribution. The block's change across the six monetary
pools is `BlockSubsidy(h) - removed(h)`. Subtracting that change from the halving
subsidy yields `-AdditionalBlockSubsidy(h) + removed(h)`. The contribution is already
included through reduced issuance and must not be credited a second time. The same
accounting applies during replay and the format 29 backfill.

Transfers between monetary pools leave the balance unchanged. Reductions in issued value
increase it. The balance itself holds no spendable value and does not contribute to
monetary pool totals.

At the reissuance start height, the bonus becomes
`ceil(D(parent) * BLOCK_SUBSIDY_FRACTION)`. Each fork uses its own parent balance.
Contextual validation rejects negative balances from NU7 onward. The stored type remains
signed because a chain can run ahead of its schedule before NU7.

The reissuance start height still differs from both drafts, which take it from the
deployment ZIP. Configured networks can override it.

## Migration and recovery

Format 29.0.0 reuses the v28 database by moving it to the v29 cache path.
Older binaries do not select that path. Do not move the upgraded database back
to v28: older decoders cannot read the expanded BlockInfo layout. Downgrade
requires a pre-upgrade backup or a separate sync.

The migration reads legacy pool records and offsets them so the balance starts at the
seed on the last block below NU7. The offset is zero when the constant matches the
chain's own history, as the measured Mainnet and Testnet constants do. The migration
performs cumulative schedule arithmetic without clamping either operand to MAX_MONEY. It
checks the eligible balance after the offset.

The migration writes batches of 10,000 BlockInfo records. It preserves monetary
pools and block sizes. It updates the separately stored tip balance last.
Cancellation or a failed write leaves the version marker unchanged. Restarting
the migration recomputes every balance, including already rewritten records.

A missing baseline, malformed record, invalid pool total, or arithmetic error
stops migration. Do not replace such data with zero. Restore a verified database
backup or repair the identified corruption before retrying startup.

## Validation and activation requirements

The tests cover aggregate fee rounding, fee activation before reissuance, coinbase
claims and template fees, the resulting NSM contribution, schedule sums, the seed and
its rollback, the fraction and halving interval per target spacing era, the half-life over one
interval, termination from a small balance, transfers through every monetary
pool, reductions in issued value, contextual rejection from NU7, independent
non-finalized forks, finalized rollback, replay, alternate branches, restart,
fresh replay equivalence, legacy records, migration batch boundaries, failed
writes, cancellation, corruption, and startup retry.

Checkpoint fixtures isolate accounting. Some intentionally underclaim coinbases
and do not represent semantically valid post-NU6 blocks. The semantic subsidy
test checks exact claims and one-zatoshi overclaims, but mocks transaction
verification because NU7 has no production branch ID. Accounting reductions do
not establish support for a ZIP 233 transaction format.

Before production activation:

- Resolve the reissuance start height against the deployment ZIP.
- Assign the production NU7 branch ID and activation heights.
- Run real transaction verification across activation on a private network.
- Mine bonus-paying blocks, create forks, restart nodes, migrate a database,
  and verify convergence on that network.
- Verify historical pool anchors against an independent node.
- Measure migration time and disk usage on a representative database copy.
- Obtain independent review of the accounting contract, migration, and oracle.

Passing the bounded properties does not establish that every possible state or
failure has been tested.
