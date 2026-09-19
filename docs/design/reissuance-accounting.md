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
an existing v28 database to the v29 path and backfills every record. It preserves
monetary pools and block sizes. It writes the separately stored tip pools last.
Failures and cancellation preserve the version marker so startup can retry.
Older binaries cannot read the expanded records; downgrade requires a backup
or a separate sync.

This change does not activate bonus issuance or reject negative deficits.
The next consensus change defines those rules separately.

Accounting tests cover transfers, historical exclusion, signed balances, forks,
finalization, rollback, alternate replay, reopen, migration failure, and retry.
Checkpoint fixtures isolate accounting and may bypass semantic claim rules.
They do not establish production NU7 transaction support.
