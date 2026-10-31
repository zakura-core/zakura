# P2P machine fuzz targets

Run these targets locally or in the nightly/manual campaign.

## Discovery state targets

| Target | Local property ID | Applicability witness |
| --- | --- | --- |
| `discovery_book` | `discovery/record-book` | `discovery_book_witness_exercises_provenance_quota_and_expiry` |
| `discovery_state` | `discovery/session-installation` | `discovery_state_witness_exercises_stale_verification_and_expiry` |

Both targets reuse the existing independent models. Decoder version 1 reads at
most 512 bytes in four-byte operations. Empty inputs report zero applicability.

The book witness imports relayed records, rejects a relay-quota overflow, updates
dial metadata, changes provenance, advances time and rejects an expired record.
Two planted failures add unearned confirmation or an extra dial failure.
The session witness replaces a session before verification completes, rejects
the stale result, installs the current result and expires a live service summary.
Two planted failures remove the current admission or install an unearned hint.
Each planted failure must report the intended property ID.

Decoder version 1 excludes peer sampling, and its corpus keeps that meaning.
The existing proptest checks sampling eligibility and disclosure limits. The
`zakura-sim` target `discovery/record-book` uses decoder version 2 to replay
signed samples under a fixed disclosure key, a seeded shuffle and a fixture clock.
Record metadata and session checks compare normalized state and do not depend on
hash iteration order. These targets check synchronous installation rules;
`discovery/verification-capacity` checks the real handle's task cancellation and
verification waits.

## Discovery session and redial targets

| Target | Local property ID | Applicability witness |
| --- | --- | --- |
| `discovery_session` | `discovery/session-policy` | `discovery_session_witness_reaches_close_after_recheck` |
| `discovery_redial` | `discovery/redial-policy` | `redial_witness_exercises_limits_resets_and_lagging_registration` |

Both decoder-v1 targets read at most 512 bytes and reuse their existing
independent models for proptest and cargo-fuzz. Empty inputs report zero
applicability.

`discovery_session` decodes hello, admitted answers and operation outcomes.
It checks exchange completion, single settlement, and ownership checks before
close. Its witness reaches a close after the answer wait and ownership recheck.
Two planted failures skip the recheck or discard the success charge.

`discovery_redial` uses three header bytes for policy and backoff bounds.
Each remaining triple selects an attempt and registration lifetime. Its witness
checks local limits, backoff growth, registration upgrades, resets and stopping.
Two planted failures reset a failed attempt or charge a local limit.

These targets check pure policies. They do not check worker cancellation or
connection ownership across scheduled IO.
