# September 30, 2026 main compatibility preflight

PR #1097 was rebased onto main
`af944f5194ef2e9921bc96af017629450375013c` without conflicts.
Main includes Common v2.1.0 (#1206), the template timestamp fix (#1203),
Mainnet FPF rotation (#1201), and the Testnet NU7 minimum-difficulty change
(#1209). The infrastructure branch adds no node consensus overrides.

## Verdict: existing NU7 fork cannot be upgraded in place

PR #1209 changes the minimum-difficulty exception from a gap greater than
150 seconds to a gap greater than 450 seconds at the configured NU7 activation
height. It applies to `Nu7StagingV2`, whose activation is 4,398,756.

Read-only RPC queries on `zakura-nu7-fork-1` returned:

| Field | Value |
| --- | --- |
| Activation height | 4,398,756 |
| Parent timestamp | 1,790,450,729 |
| Activation timestamp | 1,790,450,880 |
| Gap | 151 seconds |
| Actual compact target | `2007ffff` (minimum difficulty) |
| Recomputed main compact target | `1f1363ec` |

Activation hash:
`035144c0ff852897608979e863e0210809ee08c3fe9bf35cfab4a9c09649e08d`.
Parent hash:
`005133aa322b2fae551ddd689afb439d4e6fa733d461f5dba2528f5fbf468f98`.

The main target was independently recomputed in Python from all 113 parent
headers, using the source's 102-block arithmetic mean, 11-header median spans,
25-second target spacing, damping factor 4, 16%/32% adjustment bounds, and
compact-target encoding. The actual median timespan was 831 seconds and the
bounded timespan was 2,142 seconds. This is a source-guided arithmetic check,
not an end-to-end replay using a newly built node.

The first NU7 block already has the wrong target under current main. Later
examples at heights 4,398,757 and 4,398,758 also use minimum difficulty after
151-second gaps. Upgrading every node together does not repair that history.
A startup over previously trusted state is not proof that a fresh participant
can validate the chain.

## Live provenance

The public manifest advertises revision
`738d175061e23d1ad65ec99b2d2a6b5d004bb10f`, while both the primary running
executable (`/proc/619069/exe`) and installed observer binary report
`1.5.1-rc0+gb3d597eea5b0`. The primary service started September 29 at
08:08:29 UTC, before #1209 merged. The manifest is stale relative to the
installed binary. No publication update was performed during this preflight.

Both local node services and the primary miner were active. The public status
reported all three regional miners healthy and agreeing with the primary.
No services were stopped, no state was deleted, and no deployment was dispatched.

## Required next step

To run stock main, preserve the current fork for rollback and launch a separately
identified fork from a pre-NU7 public-Testnet seed. Keep the name, magic, all
validator/miner revisions, faucet state, manifest, participant config, and
bootstrap snapshot consistent for that new chain. A caught-up seed avoids
old-timestamp mining limits. Replacing the public fork needs operator approval
because it replaces the chain and invalidates existing fork balances.

Alternatively, retaining the current history requires a separately designed and
reviewed consensus migration that preserves historical rules and switches at a
future height. Stock main does not provide that migration.

The sidecar miner now reads the consensus helper instead of hardcoding six
spacings; its activation-boundary test covers the parent immediately before
NU7 and both neighboring heights. Documentation now states the 450-second
post-NU7 gap.
