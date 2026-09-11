# Zakura P2P iroh Dependency

Plan: `/home/evan/src/valar/art/inbox/zakura_p2p/00.0_iroh_dependency.md`

> **Status:** This decision contributes to the experimental Zakura P2P v2
> stack.

## Decision

Zakura pins `iroh = { package = "zakura-iroh", version = "=1.1.0-rc.1" }`
in the workspace with `default-features = false` and the explicit `tls-ring`
backend. The four registry packages (`zakura-iroh`, `zakura-iroh-base`,
`zakura-iroh-dns`, and `zakura-iroh-relay`) retain the original Rust library
names and pin their sibling dependencies to the same release candidate.
`Cargo.lock` records registry checksums and upstream noq 1.2.0. The dependency
is wired into `zakura-network`'s native protocol, endpoint service, handshake,
and discovery. Consumers do not need workspace source patches.

The compatibility source starts from upstream Iroh 1.1.0 and retains three
compatibility manifest requirements. Iroh and Iroh-base retain published
ed25519-dalek 2.2; Iroh-base retains curve25519-dalek 4.1.3. Rc1 adds an explicit
QUIC NAT traversal opt-out to the transport builder. Its relay manifest pins
LRU to 0.18.3 so fresh consumers cannot select
0.18.4's faulty `retain` implementation. BIP32, the Zcash cryptographic packages,
and their digest requirements remain unchanged. The Iroh fork's `FORK.md` records its consumption and
maintenance policy.

Unmodified Iroh 1.1 requires stable SHA-2 through its newer Dalek dependencies.
That cannot resolve with BIP32 0.6.0-pre.1's exact prerelease digest requirements.
The compatibility source avoids a BIP32 fork and a broader Zcash dependency
migration. It still requires review of authentication compatibility and ongoing
monitoring of upstream Iroh and Dalek security changes.

The latest stable iroh checked during the initial implementation was `0.98.2`,
but it could not resolve with Zebra's librustzcash dependency set at the time. `iroh-base 0.98`
pulls the `sha2 0.11.0-rc` line, while the current Zcash stack pulls
`sha2 0.11.0-pre` through `bip32 0.6.0-pre.1`. `iroh 0.95.1` had the same
conflict through `ed25519-dalek 3.0.0-pre`. `iroh 0.92.0` was the newest iroh
line selected during the initial review fixes that resolved with that tree,
used the patched `rustls-webpki 0.103.13` line, and still exposed the protocol router and
endpoint APIs needed by later Zakura plans.

`iroh 0.92.0` declares `rust-version = "1.85"`, but its transitive dependencies
(`time`, and the `tonic`/`darling`/`serde_with` chain) have since moved to
`rust-version = "1.88"` to pick up upstream fixes — including the
RUSTSEC-2026-0009 stack-exhaustion fix shipped in `time 0.3.47`. The workspace
MSRV was therefore unified at 1.91 (matching the `zebrad` binary) rather than
held at the former 1.85.1 library floor. The current workspace requirement is
1.97 and is unchanged by this upgrade.

## Privacy Posture

Network defaults remain legacy on Mainnet and dual on other networks.
`zakura_network::zakura::direct_endpoint_builder` constructs an iroh endpoint
builder with:

- the `Minimal` preset;
- `RelayMode::Disabled`;
- `clear_address_lookup()`;
- `clear_ip_transports()`;
- a caller-provided durable `SecretKey`.

The iroh dependency is built with `default-features = false` and only the
`tls-ring` feature enabled. No relay or external discovery service is installed.
The local smoke test binds only to `127.0.0.1:0`, reads back the endpoint
`EndpointAddr`, confirms there is at least one direct address, and confirms both
relay URL and address lookup are absent.

Callers must add explicit bind addresses. Production uses the configured native
listen address; an unset address binds only IPv4 and IPv6 loopback sockets.
A configured port already in use now fails startup instead of silently choosing
an ephemeral port. Only the configured address family is bound, including for
wildcard addresses. An IPv6-only bind cannot dial IPv4 bootstrap peers. Parallel
test endpoints explicitly request loopback port zero.

Automatic UPnP, NAT-PMP and PCP port mapping is not compiled in. Operators behind
a router must forward the native UDP port manually and advertise a reachable
address. Public-IP nodes do not need this mapping.

QUIC NAT traversal is explicitly disabled with
`max_remote_nat_traversal_addresses(0)`. The rc1 fork allows zero to disable the
extension, preventing its interface-address exchange and peer-directed UDP
probes even when the other endpoint enables it. Relay configuration alone does
not disable this extension. Direct connections remain available.

With relays and external discovery disabled, connectivity is limited to
directly reachable peers, local networks, forwarded ports, and addresses
supplied by Zakura's discovery or legacy-upgrade address hints. Hard-NAT peers
remain out of scope until a future relay/discovery decision.

The connection's selected path supplies the peer IP for admission limits. Iroh
can select a path on a different concurrent connection to the same identity. If
this connection has no selected path, its sole open IP path supplies the IP.
Connections without an attributable IP are refused admission. Advertised
addresses never supply the admission IP.

Existing stream counts, receive/send windows, idle deadlines
and disabled QUIC datagrams are preserved through `QuicTransportConfig`. Iroh also enables a five-second
per-path heartbeat independently of the connection keepalive interval.

## API Names Confirmed

Against `iroh 1.1.0`, these names compile:

- `iroh::protocol::{Router, ProtocolHandler}`;
- `ProtocolHandler::accept(&self, iroh::endpoint::Connection) ->
  impl Future<Output = Result<(), iroh::protocol::AcceptError>> + Send`;
- `iroh::{Endpoint, RelayMode, SecretKey}`;
- `Endpoint::builder(endpoint::presets::Minimal)`, `Endpoint::addr()`,
  `Endpoint::id()`, and `Endpoint::address_lookup()`;
- `iroh::{EndpointAddr, EndpointId}`;
- `Connection::remote_id()`;
- `iroh::endpoint::QuicTransportConfig`.

Dialers pass the complete `EndpointAddr` directly. The obsolete
`ZakuraEndpoint::add_node_addr` and unused test-factory `wire` helpers are removed
rather than installing an unbounded address-lookup cache.
`ZakuraTestNodeBuilder::transport` now accepts a complete `QuicTransportConfig`
instead of a closure mutating the old transport type. These exposed Rust API
changes require normal downstream compatibility review.

Later Zakura plans should target these names unless the iroh pin is changed.

## Dependency Reconciliation

The selected pin keeps the main TLS backend unified with Zakura's existing
reqwest stack:

- `rustls v0.23.41` is shared by reqwest and iroh;
- `ring v0.17.14` is shared by rustls and iroh;
- `rustls-webpki v0.103.13` is shared by rustls and iroh, with no older
  `rustls-webpki 0.102.x` copy left in `Cargo.lock`;
- iroh brings `noq v1.2.0`, a quinn-derived transport stack, rather than the
  upstream `quinn` package currently pulled by reqwest HTTP/3 paths;
- smaller iroh subtree duplicates from the iroh-side network/interface helper
  crates are recorded narrowly in `deny.toml` for duplicate detection until the
  upstream crates converge.

`cargo deny check bans licenses sources` passes locally. Dependency review
evidence and exemptions are recorded in `qa/supply-chain/audits.toml` and
`qa/supply-chain/config.toml`.

## Reserved Identity Surface

`network.zakura_node_secret_key` remains an optional explicit iroh
secret-key override. It is deserialized into a redacted config newtype so the
value does not appear in startup `Debug` logs or generated serialized config.
If it is unset, Zakura endpoint construction generates an ed25519 iroh
`SecretKey` on first use and persists it under `network.identity_dir`, which
defaults outside Zakura's cache and state directories at:

```text
~/.zakura/<network>.zakura-iroh-secret-key
```

This location is intentionally independent from `network.cache_dir`, so state
or cache snapshots do not clone a node's long-term iroh identity. Operators can
override it with `network.identity_dir`, but should keep it outside snapshot
paths.

`network.identity_dir` is the canonical config surface for this reserved
storage path. The durable identity location and 32-byte secret encoding are
unchanged. New production identities still draw their bytes from the operating
system RNG.

## Rolling Compatibility

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
directions at a 66-block gap, recovering in about 31 seconds. The faster legacy
probe requires a gap of at least 64 blocks; smaller gaps use the existing
ten-minute fallback window. Six-block restart tests took about 603 seconds in
both directions, despite working TCP. Ping/Pong success does not imply fast
restart recovery.
During checkpoint sync, a dual-stack node with current commitment-tree frontiers
can verify TCP blocks by recomputing the trees when native VCT roots or successor
metadata are unavailable. Once a VCT fast commit freezes those frontiers, the node
still needs authenticated metadata from a compatible native supplier until the
checkpoint handoff restores the full trees.
Native-only nodes in different cohorts cannot communicate. They
need reachable same-cohort seeds and a coordinated upgrade; a protocol bump does
not supply them with a legacy fallback.

## Validation and Release Gate

The authentication review confirmed TLS 1.3 raw-public-key identity binding and
Dalek `verify_strict` for handshake signatures. The server key must match the
requested endpoint identity; the retained Dalek version rejects small-order
keys and signature points and noncanonical scalars. `legacy_compatibility` is
not enabled. Known-answer and malformed-signature tests passed with both the
fork and upstream Iroh 1.1, supporting the narrow dependency change without
constituting a full cryptographic audit.

The independently built process probes verified 1 MiB payload delivery for
old-old and new-new native pairs, bounded rejection in both mixed native
directions, and successful Ping/Pong exchanges in both mixed TCP directions.
The probes and their runner are deferred to a separate testing PR.

The large bidirectional transfer regression sends 64 MiB each way with production
transport limits, exceeding the send and receive windows. The exact noq 1.2.0
`tail_loss_respect_max_datagrams` regression passes on macOS and Linux. Local
workspace tests also cover identity persistence, per-IP admission, decoy
addresses, legacy handoff/recovery, cancellation, framing and block sync.
Ten local Linux full-node cases also passed across both versions, covering
baseline, loss/delay, reordering, MTU 1280 and outage recovery without TCP.
The emulated runs used different build profiles, so they are not a performance
comparison or proof of long-term memory stability under hostile load. An
authorized long-duration deployment soak remains a separate release gate; no
deployment has been performed.

The compatibility family is distributed as registry packages at `1.1.0-rc.1`,
prepared from the committed fork using `scripts/prepare-zakura-packages.py`.
The preparation manifest records the source revision and hashes every Rust
file; package verification checks the renamed manifests and registry contents.
The existing crate packaging, publish-graph, semver and supply-chain gates
remain enabled. No Git patch or semver-specific Iroh source override is needed.
The LRU pin is part of the published relay manifest because consumers do not
inherit Zakura's lockfile or deny policy.

Audit records cover the renamed base and DNS packages using their reviewed
source and compatibility deltas. Exact-version exemptions preserve the
recorded Iroh and relay defects and their existing disposition; renaming the
packages does not fix those defects. Retire the fork when upstream's dependency
graph resolves and passes the same interoperability checks.

Zakura accepts Iroh 1.1's mapped-address retention of
roughly 100–200 bytes per distinct authenticated identity until endpoint shutdown,
with no public eviction API. Track this growth under sustained identity churn
and adopt the upstream fix when released. No patch for mapped-address retention is planned.

The path-open retry issue requires two or more outgoing connections to one peer.
Zakura deduplicates pending dials and evicts duplicate connections, keeping one
connection per identity, so no change is planned for this integration.

Existing release and supply-chain checks remain enabled; passing cargo-deny
does not satisfy cargo-vet or constitute a cryptographic audit.

Pre-existing review concerns around ban admission, peer-directed internal-address
probes, advertised addresses, dial/backoff behavior and handoff metadata binding
still need separate fixes or explicit security dispositions before broader native
deployment. Their presence before this upgrade does not establish safety;
exploitability and the reported severity labels have not been independently
confirmed.
