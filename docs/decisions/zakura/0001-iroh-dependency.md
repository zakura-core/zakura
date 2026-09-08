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

## Validation and release gate

The native process probe in `testkit/interop.rs` runs in two independently built
network test binaries. `scripts/test_iroh_interop.py` checks old-old, new-new and
both mixed dialing directions, using the production native control handshake
and delivery of 1 MiB across bounded gossip frames. It is an ignored test because
it requires an externally coordinated peer process. Both binaries must include
the same probe, adapting only Iroh's renamed address/identity APIs on the old
revision. The runner requires successful payload verification, not just a
completed QUIC handshake.

This probe complements the local network tests for identity persistence,
per-IP admission, decoy addresses, legacy handoff/recovery, cancellation,
framing and block sync. It does not replace independently pinned full-node
sync/propagation tests, Linux impairment tests or an authorized mixed-cohort
Testnet soak. No protocol version change should be inferred solely from the
Iroh version number; use the actual compatibility results.

Root Cargo patches are not inherited by library consumers and are not a
registry distribution strategy. The existing crate publish-graph and release
checks must remain intact. This branch is for source/binary integration only:
no packages are created or published to crates.io. A registry-compatible
upstream release or separately approved publication strategy is required before
release readiness. Retire the fork when the upstream dependency graph resolves
and passes the same interoperability checks.
