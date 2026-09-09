# Zakura P2P Iroh dependency

## Decision

Use Iroh 1.1.0 with default features disabled and the explicit `tls-ring`
backend. Root `[patch.crates-io]` entries pin `iroh`, `iroh-base`, `iroh-relay`
and `iroh-dns` to one revision of `https://github.com/zakura-core/iroh`.
`Cargo.lock` records that revision and the upstream noq 1.2.0 transport family.
The workspace Rust requirement remains 1.97.

The compatibility source starts from upstream Iroh 1.1.0 and changes three
manifest requirements. Iroh and Iroh-base retain published ed25519-dalek 2.2;
Iroh-base retains curve25519-dalek 4.1.3. The fork changes no production Rust
source. BIP32, the Zcash cryptographic packages, and their digest requirements
remain unchanged. The Iroh fork's `FORK.md` records its consumption and
maintenance policy.

Unmodified Iroh 1.1 requires stable SHA-2 through its newer Dalek dependencies.
That cannot resolve with BIP32 0.6.0-pre.1's exact prerelease digest requirements.
The compatibility source avoids a BIP32 fork and a broader Zcash dependency
migration. It still requires review of authentication compatibility and ongoing
monitoring of upstream Iroh and Dalek security changes.

## Endpoint behavior

`zakura_network::zakura::direct_endpoint_builder` uses the `Minimal` preset,
explicitly disables relays and address lookup, and clears default IP transports.
Callers must add explicit bind addresses. Production uses the configured native
listen address; an unset address binds only IPv4 and IPv6 loopback sockets.
A configured port already in use now fails startup instead of silently choosing
an ephemeral port. Parallel test endpoints explicitly request loopback port zero.

No relay or external discovery service is installed. Direct connectivity still
requires reachable addresses supplied by Zakura's discovery or legacy upgrade
hints. Network defaults remain legacy on Mainnet and dual on other networks.

The transport's current selected connection path supplies the peer IP for
admission limits. Advertised addresses are not used for that attribution. A
record with a decoy address before the reachable address must still be charged
to the address that actually carries the connection.

Existing stream counts, receive/send windows, idle deadlines, keepalive interval
and disabled QUIC datagrams are preserved through `QuicTransportConfig`.

## Identity and API changes

The durable identity location and 32-byte secret encoding are unchanged. New
production identities still draw their bytes from the operating system RNG.
`network.zakura_node_secret_key` remains redacted in debug and serialized output.
Without an override, identities persist under `network.identity_dir`, outside
state/cache snapshots, at `~/.zakura/<network>.zakura-iroh-secret-key` by default.

Iroh now exposes `EndpointId`, `EndpointAddr`, `Endpoint::id`, `Endpoint::addr`,
`Connection::remote_id` and `QuicTransportConfig`. Dialers pass the complete
`EndpointAddr` directly. The obsolete `ZakuraEndpoint::add_node_addr` and unused
test-factory `wire` helpers are removed rather than installing an unbounded
address-lookup cache. `ZakuraTestNodeBuilder::transport` now accepts a complete
`QuicTransportConfig` instead of a closure mutating the old transport type.
These exposed Rust API changes require normal downstream compatibility review.

## Rolling compatibility

The native transport now requires Zakura protocol 2 and ALPN `p2p-v2/2`.
Both the legacy upgrade prelude and native discovery advertise only protocol 2.
A process probe against Iroh 0.92 completed in one mixed dialing direction but
timed out in the other; same-version pairs succeeded. This is insufficient for
a reliable rolling native upgrade, so protocol 1 is not offered by this cohort.
The existing prelude and control encoding versions remain unchanged.

Old and upgraded dual-stack nodes reject the native upgrade before handing off
TCP and retain legacy connectivity. Independently built old/new process tests
verify two Ping/Pong exchanges in both dialing directions with no native session
registered. Full-node propagation and fresh-client catch-up also pass in both
directions at a 66-block gap. Smaller gaps use the existing ten-minute fallback
window; see the [measured recovery evidence](iroh-1.1-validation.md).
Native-only nodes in different cohorts cannot communicate. They
need reachable same-cohort seeds and a coordinated upgrade; a protocol bump does
not supply them with a legacy fallback.

## Validation and release gate

The native process probe in `testkit/interop.rs` runs in independently built
network test binaries. `scripts/test_iroh_interop.py --expect-mixed-rejection`
checks successful old-old and new-new payload delivery and bounded failure in
both mixed native directions. Each successful pair uses the production native
control handshake and checks 1 MiB across bounded gossip frames. With `--mode legacy`, the runner instead executes the ignored legacy handshake probe in both
mixed directions and requires successful Ping/Pong completion. These tests are
ignored because they require an externally coordinated peer process. Both
binaries must include the same probes, adapting only the renamed Iroh APIs on
the old revision and assigning ephemeral native ports to test endpoints.

The large bidirectional transfer regression sends 64 MiB each way with production
transport limits, exceeding the send and receive windows. The exact noq 1.2.0
`tail_loss_respect_max_datagrams` regression passes on macOS and Linux. Local
workspace tests also cover identity persistence, per-IP admission, decoy
addresses, legacy handoff/recovery, cancellation, framing and block sync.
These checks do not replace independently pinned full-node sync/propagation
tests, Linux impairment tests or an authorized mixed-cohort Testnet soak.

Root Cargo patches are not inherited by library consumers and are not a
registry distribution strategy. The existing crate publish-graph and release
checks must remain intact. This branch is for source/binary integration only:
no packages are created or published to crates.io. A registry-compatible
upstream release or separately approved publication strategy is required before
release readiness. Retire the fork when the upstream dependency graph resolves
and passes the same interoperability checks.

The Git source has explicit cargo-vet policy and reviewed compatibility deltas,
and the upstream baseline and dependency graph are covered by audit records and
documented exemptions. One hold gates deployment: the Iroh 1.1 delta retains
mapped-address entries for every authenticated identity for the life of the
endpoint, a remotely driven memory-exhaustion vector that must be fixed in the
fork or upstream before native transport is enabled on public nodes. Existing
release and supply-chain checks remain enabled; passing cargo-deny does not
satisfy cargo-vet or constitute a cryptographic audit.

The follow-up [preparation evidence](iroh-1.1-validation.md) records the proposed
package family, archive and consumer checks, authentication review, full-node
recovery timing, and remaining cargo-vet coverage.
