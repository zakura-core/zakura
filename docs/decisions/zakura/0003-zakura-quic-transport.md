---
status: proposed
date: 2026-10-01
builds-on: [Zakura P2P iroh Dependency](0001-iroh-dependency.md)
---

# Replace Iroh with a Zakura-owned QUIC transport on noq

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

## Decision Outcome

Zakura uses a new crate, `zakura-quic`, built directly on noq. The crate:

- copies Iroh's reviewed TLS 1.3 raw-public-key profile and key handling, so
  node IDs, key files and bootstrap entries don't change;
- binds noq-udp sockets itself, sizes and reads back their buffers, and keeps
  noq's backpressure instead of dropping packets;
- asks the application to accept, refuse, retry or ignore each connection
  attempt before any handshake work;
- makes every transport setting a `[network.zakura.quic]` key whose default
  reproduces the Iroh backend's behavior.

The four `zakura-iroh*` packages leave the dependency graph in the same change.
No release compiles both backends. Rollback is the previous release binary.

[The zakura-quic spec](../../specs/zakura-quic.md) is authoritative for the
behavior. This record supersedes 0001 for the transport; 0001's privacy posture
(direct-only, no relay, no external address lookup, no port mapping) still
holds.

### Wire compatibility

The direct backend keeps ALPN `p2p-v2/2` and the Iroh 1.1 wire profile:
TLS 1.3 with raw public keys both ways, multipath negotiated with 8 paths, no
NAT-traversal parameter, datagrams off and `grease_quic_bit` off. Peer-opened
paths are accepted, and paths from banned IPs close. The direct backend never
opens extra paths itself.

`tools/iroh-interop/` runs old-release and upstream-Iroh peers against the
direct backend. If a cell fails and the fix isn't simple, the ALPN moves to
`p2p-v2/3`. Dual-stack nodes then reach the other cohort over the legacy stack.

On 2026-10-02 every cell passed in three runs, plus the netem pass, against
both the Zakura Iroh 1.1 backend and upstream Iroh 1.3. The transport
parameters match the Iroh backend in every value except connection IDs and the
stateless reset token, so the ALPN stays `p2p-v2/2`.

### NAT traversal

`[network.zakura] nat_traversal = true` now fails at startup. With relays off,
Iroh's hole punching ran only inside an existing direct connection, so it never
gave a node new inbound reachability. Router port mapping (UPnP-IGD, NAT-PMP,
PCP) is the local alternative for home nodes and is a separate follow-up.

### noq fork

noq 1.2.0 through `main` lacks the quinn fixes for GHSA-4w2j/qfwj, GHSA-hmxj,
GHSA-wppq and GHSA-6pp4, and RustSec never flags noq. Zakura pins noq to its
fork, `zakura-core/iroh-quinn`, which carries those fixes plus two dependency
trims: the Retry integrity tag on ring (removing aes-gcm, ctr, ghash, polyval)
and a hand-written frame-type table (removing enum-assoc and syn 3).

### Consequences

- Good: `zakurad`'s normal dependency graph drops from 518 to 452 packages,
  before Zakura pins the fork and its two trims. The swap prunes 18
  supply-chain exemptions and adds 5: four for the rustls 0.23.45 security
  update and the `nix` `net` feature, and one for the optional qlog crate.
- Good: no self-inflicted loss on a busy socket; operators control socket
  buffers, congestion control, timers and admission limits.
- Good: connection attempts are refused before handshake work.
- Bad: Zakura owns about 3,000 lines of transport code and the noq pin. Changes
  to `tls/`, key parsing, admission or path handling need both transport owners'
  review.
- Neutral: when a node's address changes (a laptop joins another Wi-Fi
  network, say), the connections it dialed move to the new address, as under
  Iroh. Connections that peers dialed to the old address still end and get
  redialed. Iroh learned of the change from OS events; zakura-quic polls the
  interface list every 5 s, so recovery can start up to 5 s later (SOCK-12).
