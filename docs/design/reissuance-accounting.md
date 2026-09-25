# Reissuance accounting

Fee recycling activates at NU7 and reissuance at the ZIP 234 start height.
Both activate by height alone in every build.
Production activation still requires policy guidance and production NU7 activation heights.

## The seed

The balance holds `INITIAL_NSM_VALUE_BALANCE` on the last block below NU7: the block
subsidy and fees that earlier coinbase transactions never claimed. The 2026-09-15
ZIP Editor call settled this, and zips#1354 defines the constant.

State derives the seed at `NU7 - 1` from `scheduled_issuance_zatoshis` minus the
monetary pool total at that height. Both inputs are local: the calculation uses no
RPC, block scan, or archive node. The monetary total excludes the NSM balance.

Mainnet and public Testnet check the derived seed against the measured constants
in `Network::initial_nsm_value_balance`. A mismatch fails the state update or
migration. The draft still leaves the exact values as a TODO; these constants
retain #1040's measurements, which still need an independent zcashd cross-check.

Configured networks derive their own seed by default. An explicit
`initial_nsm_value_balance`, including zero, overrides the derivation for synthetic
histories. Omitting the setting and specifying zero have different meanings.
A configured network can omit the seed only when cumulative scheduled issuance
through `NU7 - 1` is at most `MAX_MONEY`, which guarantees that every possible
derived seed fits the stored amount. Networks with larger schedules must supply
an explicit bounded seed or change their schedule.
A configured network that already activated NU7 with the old implicit zero seed
must set an explicit zero to preserve its rules, or resync under the new rules.
The existing format-29 validation detects an inconsistent stored balance.

Let `N` denote NU7 activation, `S(h)` cumulative scheduled issuance with zero genesis
issuance, `I(h)` the sum of the six monetary pools, and `C = S(N-1) - I(N-1)`
(unless a configured network overrides the seed). The stored balance
is:

- `D(h) = 0` for `h < N - 1`, or when the network has no NU7 activation.
- `D(N - 1) = C`.
- `D(h) = C + (S(h) - S(N-1)) - (I(h) - I(N-1))` for `h >= N`.
- For NU7 at genesis there is no seeded block, so `D(h) = S(h) - I(h)`.

Both commit paths call `ValueBalance::seed_nsm_value_balance` after applying the
last pre-NU7 block's monetary changes. Later blocks use `Block::nsm_value_balance_change`.
The migration in
`crates/zakura-state/src/service/finalized_state/disk_format/upgrade/nsm_value_balance_pool.rs`
uses the same seed derivation. Non-finalized rollback clears the seed when it
removes the last pre-NU7 block. Finalized rollback restores the target BlockInfo
pools. Replay derives the seed again.

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
`ceil(D(parent) * 1375 / 10_000_000_000)`. The fraction is fixed on every network,
including configured networks with different halving intervals. Each fork uses
its own parent balance.
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
seed on the last block below NU7. Derived seeds have zero offset. Only an explicit
configured-network override can introduce an offset. The migration
performs cumulative schedule arithmetic without clamping either operand to MAX_MONEY. It
checks the eligible balance after the offset.

The migration skips records before NU7, except for the preceding block when it
must receive a nonzero seed. Earlier legacy records already decode with a zero
balance. If NU7 is unscheduled or the tip precedes the first affected block, the
migration performs no data writes, including to the separately stored tip pools.

The migration reads no history before the pre-NU7 baseline and does not audit
or repair the absolute historical Deferred balance. A nonzero NSM seed is written
at that baseline, but Deferred changes are validated only from NU7 onward.
A historical monetary-pool error changes the derived seed; the public-network
constant check rejects a changed total. An explicit configured seed still cancels
a constant historical offset. Each post-activation Deferred
change must match funding minus disbursements before the corresponding NSM balance
is written. This rejects mixed replay/commit histories whose changing Deferred
undercount would otherwise distort the NSM balance.

Malformed monetary records, invalid totals, missing required rows, and disagreement
between tip records remain errors. Detecting or repairing other historical
corruption is a separate database-integrity task. Cancellation and failures preserve
the version marker so startup can retry. Older binaries require a backup or a
separate sync to downgrade.

The migration writes batches of 10,000 affected BlockInfo records. It preserves monetary
pools and block sizes. It updates the separately stored tip balance last.
Cancellation or a failed write leaves the version marker unchanged. Restarting
the migration recomputes every balance, including already rewritten records. It
always rewrites and validates the `NU7 - 1` seed row, including for an explicit
zero, so a v27 attempt interrupted under different seed configuration cannot
leave a mixed history.

The migration refuses a database whose finalized tip is at or above the ZIP 234
start height. Older versions committed those blocks without reissuance, so their
Deferred balances differ from a fresh sync. Startup fails until the operator
deletes the database. The error says to delete it and sync again. The refusal
happens before format 29 writes any record. An operator can move a format 28
database back to the v28 path to restore it for the older binary.

A missing baseline, malformed record, invalid pool total, or arithmetic error
stops migration. Do not replace such data with zero. Restore a verified database
backup or repair the identified corruption before retrying startup.

## Validation and activation requirements

The tests cover aggregate fee rounding, fee activation before reissuance, coinbase
claims and template fees, the resulting NSM contribution, schedule sums, the seed and
its rollback, the fixed NU7 fraction, its half-life over 5,040,000 blocks,
termination from a small balance, transfers through every monetary
pool, reductions in issued value, contextual rejection from NU7, independent
non-finalized forks, finalized rollback, replay, alternate branches, restart,
fresh replay equivalence, legacy records, migration batch boundaries, failed
writes, cancellation, corruption, and startup retry.

Checkpoint fixtures isolate accounting. Some intentionally underclaim coinbases
and do not represent semantically valid post-NU6 blocks. The semantic subsidy
test checks exact claims and one-zatoshi overclaims, but mocks transaction
verification to isolate the accounting checks. Accounting reductions do not
establish support for a ZIP 233 transaction format.

Before production activation:

- Resolve the reissuance start height against the deployment ZIP.
- Assign the production NU7 activation heights.
- Run real transaction verification across activation on a private network.
- Mine bonus-paying blocks, create forks, restart nodes, migrate a database,
  and verify convergence on that network.
- Verify historical pool anchors against an independent node.
- Measure migration time and disk usage on a representative database copy.
- Obtain independent review of the accounting contract, migration, and oracle.

Passing the bounded properties does not establish that every possible state or
failure has been tested.
