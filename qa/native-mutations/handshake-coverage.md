# Handshake coverage audit

This audit maps the handshake-1 and handshake-2 plan to retained evidence.
The two PRs are #2337 and #2340.
The mutation campaigns listed below are still running as of 2026-11-11.
Their successful baseline runs do not establish mutation coverage.

## Plan requirements

| Requirement | Implementation and evidence | Remaining evidence |
| --- | --- | --- |
| Shared hostile peer and address model | `zakura-sim/src/peer/handshake.rs`, `peer/mod.rs`, and `harness/handshake_wire.rs`; handshake integration tests cover malformed control fields, preludes, aliases, pacing, retry, and concurrent connections | Final generated classifications |
| Table-generated consequences | `zakura-sim/src/table/handshake.rs`; hello, ack, and prelude witnesses check production judgements and exact labels | Final native campaign and survivor triage |
| Production admission machine | `zakura-network/src/zakura/transport/handshake_counts.rs`; `handshake/counts` drives production decisions and compares independent owners | Final shared counts/wire classifications |
| Overall control deadline | `slow_control_steps_share_one_deadline`; retained deadline bypass previously failed its named rule | Final planted campaign |
| Capability generation | `capability_flip_covers_inflight_handshake` covers both registration orders and two flips; retained stale-snapshot bypass previously failed its named rule | Final planted campaign |
| Cadence and conformant dialers | Both handshake harnesses contain cadence properties; production tests cover refill, refunds, aliases, ledger capacity, and the sender's redial policy | Final planted campaign; ledger limitation below |
| Session installation | `installed_services_follow_each_peers_negotiation` covers inbound and outbound negotiation, custom layouts, and unnegotiated services | Final installed-services bypass classification |
| Session capacity and setup | Partial and complete session witnesses compare permit counts with reservation owners; stream churn reaches the open-rate limit | Final capacity, setup, and stream-open classifications |
| Retired session owners | Production `SessionTable` replacement test retains snapshots and workers; droplet baseline and restored runs pass, and the retained-owner mutant fails its declared assertion | No simulator generator claim for this native test |
| QUIC admission and timeout | Retained wire witnesses cover address capacity, Retry, and TLS timeout | Final native campaign |
| TLS authorization | Shared peer knobs exercise unsupported ALPN and a key different from the dialed identity; both wire witnesses pass | Separate TLS mutation campaign |
| Generator and fuzz registration | Both harnesses have generator entries and fuzz targets; seed metadata tests pass | Final campaign reports |
| Existing saved scenarios | Both branches pass discovery and header saved-plan replay checks after the TLS observations | Compatibility exception below |
| Failed-handshake CPU cost | Three droplet samples each complete 256 failed handshakes; measured process CPU is 3.52–3.59 ms per attempt | Measurement includes both peers and test overhead |

Paths in the table are relative to `crates/` unless they name a test function.
The integration witnesses live in `crates/zakura-sim/tests/handshake.rs`.

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

The admission refactor and capability observations changed simulator scheduling.
The stack refreshed saved end records and regenerated the discovery late-response
schedule while preserving its scenario hash.
Both branches pass the resulting semantic and pinned-record checks.
The PRs therefore do not claim byte-for-byte preservation of every saved plan.

## Campaign pins

| Campaign | Source commit | Scope |
| --- | --- | --- |
| `handshake2-joint-da37` | `da37af9d7c8285059f0e335479e70550828a5c58` | Native handshake and capacity cases; 1,000 generated runs with seed 11; both harnesses where registered |
| `handshake2-planted-da37` | `da37af9d7c8285059f0e335479e70550828a5c58` | Planted handshake, epoch, installed-services, capacity, setup, and stream-open cases; 1,000 generated runs with seed 11 |
| `handshake1-tls-b8d` | `b8d05560ca87bedd57d3b2549ae510758a6866a8` | Two TLS authorization cases; 100 wire runs with seed 11 |
| `handshake-replacement-989` | `989a9034d2851defabe463de1bc32f79b21b7c0b` | Completed native replacement baseline, mutant, and restored test |

The droplet stores each report under `/root/<campaign>/`.
The two `da37` campaigns predate the TLS authorization properties.
The separate TLS campaign supplies their evidence.
Review each final report's named-rule, other-rule, survivor, applicability,
confirmation, and shrinking results before treating this audit as complete.
