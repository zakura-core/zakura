# Issuance deficit accounting

The counter starts at zero immediately before NU7. It excludes pre-NU7 unclaimed
subsidy and fees. Historical seeding remains a policy decision.

Each block from NU7 adds its scheduled halving subsidy minus its monetary pool
change. Genesis has zero scheduled issuance. Transfers between monetary pools
leave the counter unchanged. The signed counter holds no spendable value and
never contributes to monetary totals.

Runtime accounting and migration use the same excluded pre-NU7 baseline.
Changing that policy requires changing both paths and their tests together.

Format 29 appends the counter to pool records and BlockInfo. The upgrade moves
an existing v28 database to the v29 path and backfills records from NU7 activation
through the finalized tip, using the preceding block as the excluded baseline.
Earlier records remain unchanged: legacy records decode with a zero deficit.
If NU7 is unscheduled or the tip precedes activation, the migration performs no
data writes, including to the separately stored tip pools. Otherwise it preserves
monetary pools and block sizes and writes the separately stored tip pools last.
Both paths validate the tip before advancing the format version.

The migration reads no history before the pre-NU7 baseline and does not audit
or repair the absolute historical Deferred balance. It derives the counter from
changes since that baseline, so a constant historical monetary-pool offset cancels.
Each post-activation Deferred change must match funding minus disbursements before
the corresponding deficit is written. This rejects mixed replay/commit histories
whose Deferred undercount changes after the baseline and would otherwise distort
the NSM counter. The format marker remains unchanged on failure.
Existing monetary records are inputs to the migration: malformed records, invalid
totals, missing required rows, and disagreement between the tip records remain
errors. These checks do not establish that every other monetary-pool change is correct;
detecting or repairing historical corruption is a separate database-integrity task.

Failures and cancellation preserve the version marker so startup can retry.
Older binaries cannot read the expanded records; downgrade requires a backup
or a separate sync.

This change does not activate bonus issuance or reject negative deficits.
The next consensus change defines those rules separately.

Accounting tests cover transfers, historical exclusion, signed balances, forks,
finalization, rollback, alternate replay, reopen, migration failure, and retry.
Checkpoint fixtures isolate accounting and may bypass semantic claim rules.
They do not establish production NU7 transaction support.
