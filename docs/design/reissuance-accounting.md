# NSM balance accounting

The signed NSM balance is seeded on the last block before NU7 using the
network's `initial_nsm_value_balance`. Mainnet and public Testnet use their
measured historical unclaimed subsidy and fees. Configured networks start
with zero unless a seed is supplied. NU7 at genesis has no preceding block
to seed.

Each block from NU7 adds its scheduled halving subsidy minus its monetary pool
change. Genesis has zero scheduled issuance. Transfers between monetary pools
leave the balance unchanged. The balance holds no spendable value and never
contributes to monetary totals. Both commit paths reject a negative balance
from NU7 onward.

Format 29 appends the balance to pool records and BlockInfo. Migration uses the
same seed and subsequent accounting as live commits. It starts at the last
pre-NU7 block when the seed is nonzero, or at NU7 for a zero seed. Earlier
legacy records decode with a zero balance and remain unchanged. Monetary pools
and block sizes are preserved, and separate tip pools are written last.

The migration reads no history before the pre-NU7 baseline and does not audit
or repair the absolute historical Deferred balance. A nonzero NSM seed is written
at that baseline, but Deferred changes are validated only from NU7 onward.
A constant historical monetary-pool offset cancels. Each post-activation Deferred
change must match funding minus disbursements before the corresponding NSM balance
is written. This rejects mixed replay/commit histories whose changing Deferred
undercount would otherwise distort the NSM balance.

Malformed monetary records, invalid totals, missing required rows, and disagreement
between tip records remain errors. Detecting or repairing other historical
corruption is a separate database-integrity task. Cancellation and failures preserve
the version marker so startup can retry. Older binaries require a backup or a
separate sync to downgrade.

Tests cover configured seeds, both commit paths, migration, forks, finalization,
rollback across the seed height, replay, and startup retry. Checkpoint fixtures
isolate accounting and may bypass semantic claim rules. They do not establish
production NU7 transaction support.

Reissuance reward arithmetic, payout activation, and parent-aware verification
follow in the remaining replacement stack for #1053.
