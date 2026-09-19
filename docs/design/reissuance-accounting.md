# Reissuance accounting

Reissuance activates by height alone, from the ZIP 234 start height, in every build.
Production activation still requires policy guidance and a production NU7 branch ID.

## Provisional historical-funds policy

The balance starts at zero immediately before NU7. It excludes all pre-NU7
unclaimed subsidy and fees. We need guidance on whether those funds should seed
reissuance. The PR deliberately does not seed them while that decision remains open.

`Block::issuance_deficit_change` in `crates/zakura-chain/src/block.rs` selects
this baseline. The migration in
`crates/zakura-state/src/service/finalized_state/disk_format/upgrade/issuance_deficit_pool.rs`
subtracts the same excluded balance. Change both implementations and their tests
if the policy changes.

Let `N` denote NU7 activation, `S(h)` cumulative scheduled issuance with zero
genesis issuance, and `I(h)` the sum of the six monetary pools. The stored balance is:

- `D(h) = 0` for `h < N`, or when the network has no NU7 activation.
- `D(h) = S(h) - I(h) - (S(N-1) - I(N-1))` for `h >= N`.
- For NU7 at genesis, the excluded baseline is zero.

Runtime updates add scheduled block issuance minus the block's monetary pool
change from NU7 onward. Transfers between monetary pools leave the balance
unchanged. Reductions in issued value increase it. The deficit itself holds no
spendable value and does not contribute to monetary pool totals.

At the reissuance start height, the bonus becomes
`ceil(D(parent) * 4126 / 10^10)`. Each fork uses its own parent balance.
Contextual validation rejects negative balances from that start height.
The stored type remains signed because the rejection rule does not apply earlier.

This baseline differs from zips#1354's genesis-based deficit. The later rejection
height also differs from the draft's NU7 rule. The fraction remains fixed per
block across ZIP 218 spacing changes. Configured networks can override the
reissuance start height. These choices require confirmation before activation.

## Migration and recovery

Format 29.0.0 reuses the v28 database by moving it to the v29 cache path.
Older binaries do not select that path. Do not move the upgraded database back
to v28: older decoders cannot read the expanded BlockInfo layout. Downgrade
requires a pre-upgrade backup or a separate sync.

The migration reads legacy pool records and subtracts the pre-NU7 baseline.
It performs cumulative schedule arithmetic without clamping either operand to
MAX_MONEY. It checks the eligible balance after subtraction.

The migration writes batches of 10,000 BlockInfo records. It preserves monetary
pools and block sizes. It updates the separately stored tip balance last.
Cancellation or a failed write leaves the version marker unchanged. Restarting
the migration recomputes every balance, including already rewritten records.

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

The tests cover integer rounding, schedule sums, historical exclusion, transfers
through every monetary pool, reductions in issued value, contextual rejection,
independent non-finalized forks, finalized rollback, replay, alternate branches,
restart, fresh replay equivalence, legacy records, migration batch boundaries,
failed writes, cancellation, corruption, and startup retry.

Checkpoint fixtures isolate accounting. Some intentionally underclaim coinbases
and do not represent semantically valid post-NU6 blocks. The semantic subsidy
test checks exact claims and one-zatoshi overclaims, but mocks transaction
verification because NU7 has no production branch ID. Accounting reductions do
not establish support for a ZIP 233 transaction format.

Before production activation:

- Resolve the historical seed, rejection height, and spacing policy.
- Assign the production NU7 branch ID and activation heights.
- Run real transaction verification across activation on a private network.
- Mine bonus-paying blocks, create forks, restart nodes, migrate a database,
  and verify convergence on that network.
- Verify historical pool anchors against an independent node.
- Measure migration time and disk usage on a representative database copy.
- Obtain independent review of the accounting contract, migration, and oracle.

Passing the bounded properties does not establish that every possible state or
failure has been tested.
