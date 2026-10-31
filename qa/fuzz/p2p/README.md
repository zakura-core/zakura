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
