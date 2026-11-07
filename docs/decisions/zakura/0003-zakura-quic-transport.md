---
status: proposed
date: 2026-10-10
builds-on: [Zakura P2P iroh Dependency](0001-iroh-dependency.md)
---

# Replace Iroh with a Zakura-owned QUIC transport on upstream quinn-proto

## Context and Problem Statement

Zakura's native P2P stack ran on a fork of Iroh 1.1 (decision 0001). Iroh added
77 packages to `zakurad`, 59 of them wrapper code (relay, DNS, netwatch,
netlink, n0 helpers) that Zakura's direct-only configuration never runs.
Iroh also hid the QUIC controls Zakura needs:

- it dropped transmits when the UDP socket returned `Pending`, which the
  congestion controller read as loss;
- netwatch fixed `SO_RCVBUF`/`SO_SNDBUF` at 7 MiB with no API to change them;
- it forced a 5 s path keepalive and a 15 s path idle timeout;
- no endpoint-level `max_incoming`, incoming-buffer or handshake limits could
  be set, and admission ran only after the TLS handshake.

The first draft of this decision (2026-10-01) built `zakura-quic` on noq,
Iroh's QUIC engine, to keep Iroh 1.x wire compatibility. noq 1.2.0 through
`main` lacks the quinn fixes for GHSA-4w2j/qfwj, GHSA-hmxj, GHSA-wppq and
GHSA-6pp4, and RustSec never flags noq. That draft therefore needed a Zakura
fork of noq, `zakura-core/iroh-quinn`, and still added 20 packages.

On 2026-10-07 Zakura dropped Iroh wire compatibility. Iroh compatibility was
the only reason to use noq. On 2026-10-10 removing dependencies became a hard
requirement.

## Decision Outcome

Zakura uses a new crate, `zakura-quic`, built on upstream `quinn-proto` from
crates.io with a Zakura-owned async driver. Zakura maintains no fork. The crate:

- copies Iroh's reviewed TLS 1.3 raw-public-key profile and key handling, so
  node IDs, key files and bootstrap entries don't change;
- verifies Ed25519 with `ed25519-zebra`, which discovery already uses, after
  `curve25519-dalek` point checks on the key and each signature's R;
- runs one endpoint task per socket and one connection task per connection,
  which exchange ordered messages and share no `quinn-proto` object;
- binds its UDP sockets itself, sizes and reads back their buffers, keeps a
  blocked transmit instead of dropping it, and uses its own Linux GSO, GRO and
  packet-info path;
- asks the application to accept, refuse, retry or ignore each connection
  attempt before any handshake work;
- makes every transport setting a `[network.zakura.quic]` key whose default
  reproduces the Iroh backend's behavior.

`quinn-proto` 0.11.19 ships the GHSA-4w2j/qfwj and GHSA-hmxj fixes, and its
RustSec and GitHub advisories cover Zakura directly. Zakura writes the
`getifaddrs` call and the Linux UDP fast path as small owned `unsafe` code
instead of taking `nix` and `quinn-udp`. The `quinn` crate isn't used: the
owned driver reads every connection stat in place and sees `quinn-proto`'s
drained event, which the `quinn` crate hides.

The four `zakura-iroh*` packages leave the dependency graph in the same change.
No release compiles both backends. Rollback is the previous release binary.

[The zakura-quic spec](../../specs/zakura-quic.md) is authoritative for the
behavior. This record supersedes 0001 for the transport; 0001's privacy posture
(direct-only, no relay, no external address lookup, no port mapping) still
holds.

### Wire compatibility

The direct backend uses ALPN `p2p-v2/3`. The move from noq to `quinn-proto`
removes multipath, so the wire profile differs from Iroh 1.1's `p2p-v2/2`.
The profile keeps TLS 1.3 with raw public keys both ways, datagrams off and
`grease_quic_bit` off. The server allows migration and closes a connection
that migrates to a banned IP. It sends no NEW_TOKEN frames.

A node on this release and a node on an older release reach each other only
over the legacy protocol. Dual-stack nodes bridge the two cohorts.

### NAT traversal

`[network.zakura] nat_traversal = true` now fails at startup. With relays off,
Iroh's hole punching ran only inside an existing direct connection, so it never
gave a node new inbound reachability. Router port mapping (UPnP-IGD, NAT-PMP,
PCP) is the local alternative for home nodes and is a separate follow-up.

### Consequences

- Good: Iroh's 77 packages leave `zakurad`, and `zakura-quic` adds three:
  `quinn-proto`, `lru-slab` and `rand_pcg`. The noq draft added 20.
- Good: no fork to maintain; advisories reach Zakura through `quinn-proto`'s
  own RustSec entries.
- Good: no self-inflicted loss on a busy socket; operators control socket
  buffers, congestion control, timers and admission limits.
- Good: connection attempts are refused before handshake work.
- Good: the driver exposes congestion, flow-control and socket stats that
  neither Iroh nor the `quinn` crate report.
- Bad: Zakura owns the driver, the socket layer and two small `unsafe` items.
  Changes to `tls/`, key parsing, admission, migration handling or the
  `unsafe` items need both transport owners' review.
- Bad: the `p2p-v2/3` bump splits the native cohort until nodes upgrade.
- Bad: `bbr3`, `max_send_rate_bytes_per_second` and the path keepalive and
  idle keys go away. `quinn-proto` has no BBRv3 or send-rate cap, and no
  multipath.
- Neutral: when a node's address changes (a laptop joins another Wi-Fi
  network, say), the connections it dialed move to the new address, as under
  Iroh. Connections that peers dialed to the old address still end and get
  redialed. Iroh learned of the change from OS events; zakura-quic polls the
  interface list every 5 s, so recovery can start up to 5 s later (SOCK-12).
