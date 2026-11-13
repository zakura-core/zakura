# Handshake coverage audit

This audit maps the handshake-1 and handshake-2 plan to retained evidence.
The two PRs are #2337 and #2340.
The final campaigns finished on 2026-11-12 at `dde6140aca`.
Every retained native and planted case failed its named rule.
The generator lanes and their limits are listed under [Campaign results](#campaign-results).

## Plan requirements

| Requirement | Implementation and evidence | Limits |
| --- | --- | --- |
| Shared hostile peer and address model | `zakura-sim/src/peer/handshake.rs`, `peer/mod.rs`, and `harness/handshake_wire.rs`; handshake integration tests cover malformed control fields, preludes, aliases, pacing, retry, and concurrent connections | None beyond the generator reach below |
| Table-generated consequences | `zakura-sim/src/table/handshake.rs`; hello, ack, and prelude witnesses check production judgements and exact labels; every retained guard mutant failed its named rule | Nine rare guard rows were not drawn in 250 generated runs |
| Production admission machine | `zakura-network/src/zakura/transport/handshake_counts.rs`; `handshake/counts` drives production decisions and compares independent owners; its generator killed all four admission rules by name | Eight modeled hosts; see the ledger note below |
| Overall control deadline | `slow_control_steps_share_one_deadline`; the retained deadline bypass and the wire generator both failed `handshake/finishes-in-time` | None |
| Capability generation | `capability_flip_covers_inflight_handshake` covers both registration orders and two flips; the stale-snapshot bypass failed its named rule, retained and generated | None |
| Cadence and conformant dialers | Both harnesses carry cadence properties; production tests cover refill, refunds, aliases, ledger capacity, and the sender's redial policy; all three cadence bypasses failed their named rules, retained and generated | Ledger exhaustion is a unit test only |
| Session installation | `installed_services_follow_each_peers_negotiation` covers inbound and outbound negotiation, custom layouts, and unnegotiated services; the installed-services bypass failed its named rule | None |
| Session capacity and setup | Partial and complete session witnesses compare permit counts with reservation owners; stream churn reaches the open-rate limit; all capacity, setup, and stream-open patches failed their named rules | None |
| Retired session owners | Production `SessionTable` replacement test retains snapshots and workers; droplet baseline and restored runs pass, and the retained-owner mutant fails its declared assertion | No simulator generator claim for this native test |
| QUIC admission, timeout, and closing state | Retained wire witnesses cover address capacity, Retry, the TLS timeout, and closing-state release behind a nine-second return path; every QUIC mutant failed its named rule | The generator did not reach a nine-second return path |
| TLS authorization | Shared peer knobs exercise unsupported ALPN and a key different from the dialed identity; both mutants failed their named rules, retained and generated | Not a substitute for TLS-library tests |
| Generator and fuzz registration | Both harnesses have generator entries and fuzz targets; the curated seeds, including 42 wire seeds and one counts seed added for this audit, apply every own property of both targets | None |
| Existing saved scenarios | Both branches pass the saved-plan, saved-end, and fresh-process replay checks | Compatibility exception below |
| Failed-handshake CPU cost | Three droplet samples each complete 256 failed handshakes; measured process CPU is 3.52–3.59 ms per attempt | Measurement includes both peers and test overhead |

Paths in the table are relative to `crates/` unless they name a test function.
The integration witnesses live in `crates/zakura-sim/tests/handshake.rs`.

## Campaign results

The joint campaign ran 55 native cases with 250 generated runs at seed 11.
All 55 retained witnesses failed their named rules.
The generator lanes gave these results:

| Result | Lanes | Cases |
| --- | --- | --- |
| Killed by the named rule | 38 | Every admission, QUIC, TLS, conformant-acceptance, release, and most guard cases |
| Killed by a sibling rule | 7 | Five shared hello/ack limit patches, `retain-quic-slot`, and `remove-inbound-reserve`; each patch also has a named generated kill on its other rule |
| Not applicable | 9 | Hello bad magic, bad protocol, wrong chain, missing capability, nonzero initiator nonce, and bad frame limit; ack unoffered capability and bad frame limit; prelude ineligible opener |
| Held | 1 | `no-closing-deadline` |

A not-applicable lane means the 250 runs never drew that guard's row.
Each guard row is one of up to 19 rows inside one of 15 families, so 250 runs draw some rows rarely or never.
The retained witness and a curated fuzz seed apply each of these rows.
The generator baseline needs about ten seconds per wire run, so 1,000 runs exceed `cargo xtask planted loop`'s one-hour baseline deadline.

The closing-state mutant needs a slow return path, such as the witness's nine seconds.
The generated runs applied `handshake/hello/refusal-releases-slot` 53 times without such a path.
The retained witness `slow_link_refusal_releases_the_quic_slot` fails the named rule.

The planted campaign ran 11 retained patches with the same generator budget.
All 11 failed their named rules, retained and generated.
The cadence admission bypass now applies; the `da37` campaign could not apply it.

The wire generator found one production bug.
The `da37` wire baselines failed `handshake/hello/refusal-releases-slot` at seed 11, run 178.
quinn-proto kept a refused connection's closing state for three probe timeouts, which a slow peer controls.
This PR caps the drain at `SHUTDOWN_DRAIN`.
The `dde6` wire baselines pass, so `dde6` is the first campaign with generated wire evidence.

## Scope and coverage limits

The counts generator models eight admission hosts.
It cannot exhaust the production address ledger's default 124 entries.
`handshake_cadence_retains_debt_when_its_address_ledger_is_full` exercises the
production machine with a one-entry ledger, including IPv6 aliases, refusal of
a new address, debt retention, and reclamation after refill.
That unit test does not establish generated ledger-exhaustion coverage.

The native required-capability mask is zero under the production network policy.
Deleting its mask check cannot change a native handshake under that policy.
The required-channel guard remains reachable and has a missing-capability witness.
The handshake-table README records this distinction.

The session matrix uses actual reservation owners and permit counts.
The replacement test supplements the wire property with retired snapshot and
worker owners in the production table.
Its mutation result is a native assertion failure, not a named simulator failure.

Production enables header sync after bootstrap and reconnects registered peers.
The simulator disables header sync before its controlled handshake to exercise
that transition, then tests a second generation while registration is paused.
These witnesses do not claim a production live-service disable operation.

The TLS identity mutation removes both expected-key checks because either check
alone enforces the same policy.
These tests exercise ALPN selection and expected raw-key identity.
They do not replace cryptographic-library signature or certificate-format tests.

The admission refactor, the handshake observations, and the closing-state cap
change simulator scheduling.
The stack refreshed saved end records and regenerated the discovery late-response
schedule while preserving its scenario hash.
Each branch pins its own `sync_wire` end records; their outcomes are unchanged.
The PRs therefore do not claim byte-for-byte preservation of every saved plan.

## Campaign pins

| Campaign | Source commit | Scope |
| --- | --- | --- |
| `handshake-final-joint-dde6` | `dde6140acafe7f4974cda57458d22c599457d118` | 55 native handshake and capacity cases; 250 generated runs with seed 11 |
| `handshake-final-planted-dde6` | `dde6140acafe7f4974cda57458d22c599457d118` | 11 planted deadline, cadence, capability, installed-services, capacity, setup, and stream-open cases; 250 generated runs with seed 11 |
| `handshake2-joint-da37` | `da37af9d7c8285059f0e335479e70550828a5c58` | 52 native cases; retained kills only, since every wire baseline failed on the closing-state bug |
| `handshake2-planted-da37` | `da37af9d7c8285059f0e335479e70550828a5c58` | 11 planted cases; ten retained kills, and the cadence bypass did not apply |
| `handshake1-tls-b8d` | `b8d05560ca87bedd57d3b2549ae510758a6866a8` | Two TLS authorization cases; 100 wire runs with seed 11 |
| `handshake-replacement-989` | `989a9034d2851defabe463de1bc32f79b21b7c0b` | Native replacement baseline, mutant, and restored test |

The droplet stores each report under `/root/<campaign>/report/report.json`.
The `dde6` campaigns used wire generator 10.
Generator 11 fixed the retry family's selector after them, which changes some aliased inputs' scenarios.
