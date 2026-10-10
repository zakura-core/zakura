# Spec: `zakura-quic` transport

Status: draft for review, 2026-10-10. Version 0.6.
This document is authoritative for the `zakura-quic` crate, its configuration,
its wire behavior and its `quinn-proto` dependency policy.
[Decision 0003](../decisions/zakura/0003-zakura-quic-transport.md) records the
decision. "PLAN" and "DESIGN" below name the planning notes that hold the
evidence and step order; they live outside this repository.

Version 0.2 matches the first implementation (zakura draft PR). It changes
DEP-6, SOCK-1, WIRE-9, ADM-1, ADM-3, PATH-3, OBS-3, API-2, API-3, API-6 and
API-7; §17a lists each change. Version 0.3 answers the V12 audit of that
implementation. It changes SOCK-11, ADM-3, PATH-2, DIAL-4, DIAL-5, CTRL-17,
CTRL-22 and API-7; §17b lists each change. Version 0.4 adds SOCK-12; §17c
explains it. Version 0.6 moves the crate from the noq fork to upstream
`quinn-proto` with a Zakura-owned driver and drops Iroh compatibility; §17d
lists each change. Version 0.5 was the draft in #1296. This version doesn't
include it.

## 0. Conventions

- **MUST**, **MUST NOT**, **SHOULD** and **MAY** follow RFC 2119.
- Each requirement has an ID such as `SOCK-3`. Tests, PRs and review comments
  cite the ID.
- A change to this spec needs a PR that bumps the version above and names the
  changed IDs.

Terms:

- **Endpoint**: one `QuicEndpoint`. It runs one endpoint task per bound socket.
- **Endpoint task**: the task that owns one socket's `quinn_proto::Endpoint`
  (§6a).
- **Connection task**: the task that owns one `quinn_proto::Connection` (§6a).
- **Handle**: a `QuicEndpoint`, `Conn`, `SendStream` or `RecvStream`. Handles
  reach the tasks only through messages.
- **Iroh backend**: the transport before `zakura-quic`, Zakura's fork of Iroh 1.1.
- **Direct backend**: `zakura-quic`.
- **Admitted IP**: the source IP of the `Incoming` that `admit` accepted, or the
  IP a dial connected to. It never changes for the life of the connection.

## 1. Goals

| ID | Goal | Met by |
| --- | --- | --- |
| G1 | Remove the Iroh wrapper packages and the noq fork, and add at most three new crates | §2 |
| G2 | Give Zakura control of every QUIC setting that affects throughput, latency or memory | §6, §9 |
| G3 | Refuse or limit an IP before the handshake allocates state | §7 |
| G4 | Report transport health per endpoint and per connection | §11 |
| G5 | Dropped in version 0.6: wire compatibility with Iroh 1.x | §13 |
| G6 | Keep node identity, key files and bootstrap entries unchanged | §3 |
| G7 | Never lose application data in the transport, and never drop a packet on a busy socket | §6, §6a |

## 2. Dependencies

- **DEP-1.** The `zakurad` production graph MUST NOT contain `zakura-iroh`,
  `zakura-iroh-base`, `zakura-iroh-dns`, `zakura-iroh-relay`, `netwatch`,
  `netdev`, any `netlink-*`, any `n0-*`, any `hickory-*`, `papaya`, `seize`,
  `iroh-metrics`, `tokio-websockets`, any `noq*`, `quinn`, `quinn-udp`,
  `ed25519-dalek`, `nix` or `memoffset`. CI enforces the list with
  `cargo tree -e normal -i <pkg>` and fails on any hit.
- **DEP-2.** `zakura-quic` MAY depend directly on these packages and no others:
  `bytes`, `curve25519-dalek`, `ed25519-zebra`, `futures`, `hex`, `libc`,
  `metrics`, `quinn-proto`, `ring`, `rustls`, `rustls-pki-types`, `serde`,
  `socket2`, `thiserror`, `tokio`, `tracing` and `zeroize`.
  - `data-encoding` MAY be added only if step 3 of PLAN §6 finds a deployed base32 key.
  - Any other dependency needs a spec change.
- **DEP-3.** `zakura-quic` MUST enable the `quinn-proto` feature `rustls-ring`
  and no others, with default features off. The `qlog` feature MAY be enabled
  behind a `zakura-quic` cargo feature that release builds leave off.
- **DEP-4.** The workspace MUST take `quinn-proto` from crates.io at version
  0.11.19 or later, the first release with the GHSA-4w2j/qfwj and GHSA-hmxj
  fixes. A git source or a `[patch]` entry for `quinn-proto` MUST NOT appear.
- **DEP-6.** When Iroh is removed (PLAN step 10):
  - `cargo vet prune` MUST run;
  - `deny.toml` MUST lose every Iroh-only skip, ban and ignore;
  - `qa/supply-chain/iroh-1.1-audit-backlog.json` MUST be deleted.
- **DEP-7.** Zakura writes small, well-defined OS calls itself instead of
  taking a dependency. Only these items MAY contain `unsafe` code:

  | Item | Replaces | Budget |
  | --- | --- | --- |
  | `sys.rs` `unix_interface_ips` (`getifaddrs`, SOCK-10) | `nix`, `memoffset` | ~40 lines, 3 `unsafe` sites |
  | `socket.rs` `mod linux` (SOCK-13) | `quinn-udp` | ~400 lines, ~12 `unsafe` sites |

  Each item MUST carry `#[allow(unsafe_code)]` on the item alone, a
  `// SAFETY:` comment at each `unsafe` site and a test that runs in CI. An item
  that exceeds its budget needs a spec change.

## 3. Identity

- **ID-1.** `NodeId` is a 32-byte compressed Ed25519 public key. Construction
  MUST reject bytes that:
  - don't decompress to a curve point;
  - aren't the canonical encoding of that point (the point must recompress to
    the same bytes);
  - encode a small-order point.

  An honest key (A = a·B) always passes, so no existing `NodeId` changes.
  Construction accepts a mixed-order key (a point with a torsion component),
  because the torsion check costs a full scalar multiplication and Zakura
  converts peer IDs to node IDs on hot paths. ID-6 refuses every signature
  under such a key, so it can never complete a handshake.
- **ID-2.** `NodeId` text form is 64 lowercase hex characters. Parsing MUST
  accept upper- and lowercase hex. This matches Iroh's `PublicKey` Display, so
  existing `id@addr` bootstrap entries keep parsing.
- **ID-3.** `NodeSecretKey` holds a 32-byte Ed25519 seed. It MUST zeroize on
  drop. Its `Debug`, `Display` and `Serialize` output MUST NOT contain the seed.
- **ID-4.** The public key MUST derive from the seed per RFC 8032, so every
  existing key file yields the same `NodeId` as on the Iroh backend.
- **ID-5.** The identity file stays `<network>.zakura-iroh-secret-key` under
  `network.identity_dir`, holding 64 hex characters. `zakura-quic` MUST read
  existing files. It MUST create missing files with mode `0600`.
- **ID-6.** Signature verification for the TLS handshake MUST:
  1. refuse a key `A` or a signature `R` that fails any ID-1 point check or
     has a torsion component, using `curve25519-dalek`;
  2. refuse a non-canonical `S`;
  3. verify the equation with `ed25519-zebra`.

  With A and R in the prime-order subgroup, ZIP 215's cofactored equation and
  the cofactorless one agree. The accepted set is therefore a subset of
  `ed25519-dalek`'s `verify_strict`, which also accepts mixed-order keys.
- **ID-7.** Zakura's own wire formats keep encoding `NodeId` as 32 raw bytes.

## 4. TLS profile

The profile reproduces Iroh 1.1 exactly. The code comes from
`iroh/src/tls{,/verifier,/resolver,/name,/misc}.rs` and `iroh-base/src/key.rs`
at fork tag `zakura-iroh-v1.1.0-rc.1`, and it keeps their MIT/Apache-2.0 notices.

- **TLS-1.** TLS 1.3 only. The client MUST NOT offer, and the server MUST NOT
  accept, TLS 1.2 or lower.
- **TLS-2.** Both sides MUST use RFC 7250 raw public keys. They MUST set
  `server_certificate_type` and `client_certificate_type` to `RawPublicKey`, and
  they MUST refuse X.509 certificates.
- **TLS-3.** The SubjectPublicKeyInfo MUST be the Ed25519 OID prefix followed by
  the 32-byte key, byte for byte as Iroh encodes it.
- **TLS-4.** The only signature scheme is `ED25519`.
- **TLS-5.** Client authentication is mandatory. The server MUST abort a
  handshake without a client key.
- **TLS-6.** The client MUST NOT send SNI. It passes the expected `NodeId` to
  its verifier in memory.
- **TLS-7.** The client verifier MUST abort unless the server's key equals the
  dialed `NodeId`.
- **TLS-8.** The server MUST take the client's `NodeId` from the client's key.
  `Conn::remote_id()` MUST return only a key proven this way.
- **TLS-9.** The crypto provider is rustls' `ring` provider with its default
  TLS 1.3 cipher suites.
- **TLS-10.** The server MUST refuse 0-RTT (`max_early_data_size = 0`). The
  client MUST NOT send 0-RTT data. Session tickets MAY be issued; they don't
  affect 1-RTT compatibility.
- **TLS-11.** All TLS code lives in `zakura-quic/src/tls/`. Nothing outside that
  module may build a rustls config. A later Noise backend plugs in at
  `quinn-proto`'s crypto traits under its own QUIC version (§13).

## 5. QUIC wire profile

- **WIRE-1.** QUIC version 1 (`0x00000001`), using `quinn-proto`'s version list.
- **WIRE-2.** Production ALPN is `p2p-v2/3`. The server MUST offer ALPNs in the
  order the acceptor lists them, not sorted. A handshake that negotiates no
  shared ALPN MUST fail with TLS alert `no_application_protocol`.
- **WIRE-5.** `grease_quic_bit` MUST be `false`.
- **WIRE-6.** Datagrams MUST be off: `datagram_receive_buffer_size(None)` and
  `datagram_send_buffer_size(0)`. No `max_datagram_frame_size` parameter is sent.
- **WIRE-7.** Unidirectional streams MUST be off: `max_concurrent_uni_streams(0)`.
- **WIRE-8.** `max_concurrent_bidi_streams` equals the handshake config's
  `max_open_streams` (1,024 by default).
- **WIRE-9.** The `min_ack_delay` transport parameter is always sent with
  value 1,000 µs, as `quinn-proto` does. `ack_frequency = true` (CTRL-9) only
  lets this endpoint send ACK_FREQUENCY frames; it doesn't change the transport
  parameters.
- **WIRE-10.** Observed-address reports are neither sent nor requested.
- **WIRE-11.** The server MUST allow connection migration
  (`ServerConfig::migration(true)`). PATH-3 re-checks every new address.
- **WIRE-12.** The client MUST NOT enable `server_handshake_migration`. Zakura
  races separate handshakes instead (DIAL-3).
- **WIRE-13.** Any change to WIRE-1, WIRE-2, WIRE-5, WIRE-6, WIRE-7 or WIRE-14
  is a wire break. A wire break MUST bump the ALPN (COMPAT-4).
- **WIRE-14.** The server MUST NOT send NEW_TOKEN frames
  (`ValidationTokenConfig::sent(0)`). Without its `bloom` feature,
  `quinn-proto` can't detect token reuse, so it ignores received tokens anyway.
  Retry (ADM-2) still validates addresses.

## 6. Endpoint and sockets

- **SOCK-1.** Bind addresses follow `[network.zakura] listen_addr`:
  - a set address binds exactly that address;
  - an unset address binds `127.0.0.1:0` and `[::1]:0`.
  Each bound socket gets its own endpoint task. A connection lives on the
  socket that created it for its whole life. Only an unset `listen_addr` binds
  more than one socket.
- **SOCK-2.** IPv6 sockets MUST set `IPV6_V6ONLY`.
- **SOCK-3.** At bind, the endpoint MUST request `recv_buffer_bytes` and
  `send_buffer_bytes` through `socket2`'s `set_recv_buffer_size` and
  `set_send_buffer_size`. It MUST read both sizes back.
- **SOCK-4.** If a read-back size is below the request, the endpoint MUST log
  one `warn` line that names the requested size, the effective size and the
  sysctl to raise (`net.core.rmem_max` or `wmem_max`). Linux reports double the
  granted size; the comparison MUST account for that.
- **SOCK-5.** The effective sizes MUST be exported as gauges (OBS-1).
- **SOCK-6.** The send path MUST NOT drop a connection's packet because the
  socket returned `WouldBlock`. The connection task keeps the blocked transmit,
  waits until the socket is writable, and sends it before it polls
  `quinn-proto` for more. A stateless response from the endpoint task (Retry,
  `CONNECTION_REFUSED`, version negotiation or stateless reset) MAY be dropped;
  the peer retries.
- **SOCK-7.** On Linux, the endpoint MUST read the per-socket `drops` column from
  `/proc/net/udp` or `/proc/net/udp6`, matching the socket's inode. It reads
  every `kernel_drop_poll_secs` and exports the value as a counter (OBS-1). On
  other platforms the metric is absent.
- **SOCK-8.** GSO and GRO follow `gso` (CTRL-12) and SOCK-13. The endpoint sends
  no ECN marks and reads none, so `quinn-proto` reacts to loss only.
- **SOCK-9.** On a fatal socket error (not `WouldBlock` or `Interrupted`), the
  endpoint task MUST replace the socket once with a new socket on the same
  address. It closes the old socket and waits up to 1 s for in-flight sends to
  release it, so the new bind doesn't fail with `EADDRINUSE`. Connection tasks
  drop what they would send while the rebind runs; `quinn-proto` counts it as
  loss and retransmits. If the rebind fails or the error repeats within 60 s,
  the endpoint MUST log the error and stop that socket's endpoint task.
- **SOCK-10.** `local_addrs()` returns the bound addresses. For an unspecified
  bind, the advertised-address logic in `zakura-network` lists interfaces itself
  through `libc::getifaddrs` (DEP-7), replacing netwatch.
- **SOCK-11.** Every `SocketAddr` that crosses the `zakura-quic` boundary MUST be
  canonical: an `::ffff:a.b.c.d` address becomes `a.b.c.d`. Other IPv6
  addresses keep their scope ID and flow info, because a link-local address
  needs its scope ID to be dialable.
- **SOCK-12.** When a socket is bound to an unspecified address, the endpoint
  MUST list the host's interface addresses every 5 s. When the list changes, it
  MUST tell every connection, which calls
  `quinn_proto::Connection::local_address_changed`. Each connection then
  forgets the local address it sends from, and the kernel picks a new one. The
  endpoint MUST NOT rebind a socket for a network change.
- **SOCK-13.** On Linux, the socket layer MUST use its own fast path (DEP-7):
  - `sendmsg` with a `UDP_SEGMENT` control message for GSO, probed once per
    process, at most 10 segments per send;
  - an `EIO` on a segmented send disables GSO on that socket for good;
  - `recvmsg` with `UDP_GRO`, splitting each batch by the reported segment size;
  - `IP_PKTINFO` and `IPV6_RECVPKTINFO` only on unspecified binds, so a reply
    leaves from the address the peer used;
  - `IP_MTU_DISCOVER` and `IPV6_MTU_DISCOVER` set to `PMTUDISC_PROBE`. MTU
    discovery (CTRL-14) runs only when that option succeeds;
  - fixed-size, aligned control buffers. The parser MUST bounds-check every
    control-message length, and a receive with `MSG_TRUNC` or `MSG_CTRUNC`
    MUST drop the datagram.

  Other platforms use plain tokio `send_to` and `recv_from` without offloads.
  A receive loop MUST NOT spin on a persistent error (GHSA-6pp4).

## 6a. Driver

The driver follows the P2P stack's routine and reactor pattern. No lock guards a
`quinn-proto` object.

- **DRV-1.** One endpoint task per socket owns the `quinn_proto::Endpoint`, the
  socket's receive side, the connection table and admission (§7).
- **DRV-2.** One connection task per connection owns the
  `quinn_proto::Connection`, its timer and every stream's transport state. It
  loops `handle_event`, `handle_timeout`, `poll_transmit` and
  `poll_endpoint_events`, and sends its own datagrams. Pacing comes from
  `quinn-proto` through `poll_timeout`.
- **DRV-3.** The endpoint task routes each datagram to its connection over a
  queue bounded at 1,024 datagrams. When the queue is full, the endpoint task
  MUST drop the datagram and count it (`zakura.quic.endpoint.queue_drops`). The
  peer retransmits. Control events from `quinn-proto` use a separate queue that
  never drops.
- **DRV-4.** Handles reach a connection task over one ordered command channel
  per connection. Each read or write is a request with a one-shot reply.
  - A `RecvStream` has at most one read in flight.
  - A `SendStream` pipelines writes. A write returns once at most 64 KiB
    (`WRITE_AHEAD_BYTES`) of that stream's data waits at the connection task
    for `quinn-proto`. An earlier write's failure surfaces on a later write.
    Without write-ahead, each write cost a round trip to the
    connection task and its own packet.
- **DRV-5.** Cancelling a read or write future MUST NOT lose or reorder stream
  data. The handle keeps the cancelled request's reply, and its next call
  waits for that reply first.
- **DRV-6.** The connection task reads a stream from `quinn-proto` only when
  the application asks for data. Flow-control credit therefore follows what
  the application consumes, and Zakura's stream queue limits reach the peer as
  flow control.
- **DRV-7.** Dropping the last `Conn` handle closes the connection with code 0.
  Stream handles hold the connection open. Dropping the last `QuicEndpoint`
  handle closes the endpoint (API-7).

## 7. Admission

Admission runs in two stages. Stage 1 runs on each `Incoming` before any crypto
work. Stage 2 runs after TLS proves the `NodeId` and is today's Zakura logic.

- **ADM-1.** For every `Incoming`, the endpoint task MUST call
  `Acceptor::admit(&IncomingInfo)` before it calls `accept`. `IncomingInfo` holds:
  - `remote`, the canonical source address (SOCK-11);
  - `validated`, which is `Incoming::remote_address_validated()`;
  - `pending_total` and `pending_from_ip`, the handshakes in progress on the
    endpoint and from `remote`'s IP (ADM-7).
- **ADM-2.** `admit` returns one of four outcomes, each mapped to a
  `quinn_proto::Endpoint` call:
  - `Accept` → `accept`;
  - `Refuse` → `refuse`, which sends `CONNECTION_REFUSED`;
  - `Retry` → `retry`, which sends a stateless Retry. If the source is already
    validated, `retry` fails and the endpoint refuses instead;
  - `Ignore` → `ignore`, which sends nothing.
- **ADM-3.** Zakura's acceptor MUST return:
  1. `Ignore` for a banned IP;
  2. `Refuse` when the IP's established plus pending connections reach
     `max_connections_per_ip`;
  3. `Refuse` when `max_pending_per_ip` is set and the IP has that many
     handshakes in progress;
  4. `Refuse` when the endpoint has `max_pending_handshakes` handshakes in progress;
  5. `Retry` when `retry_threshold` is set, `validated` is false, and pending
     handshakes reach `retry_threshold`;
  6. `Accept` otherwise.
  Rules apply in that order. Zakura's acceptor applies rules 1, 2 and 4: it
  counts `pending_from_ip` with the IP's control handshakes and established
  connections, and counts
  `pending_total` with the control handshakes holding `max_pending_handshakes`
  permits. The endpoint applies rules 3 and 5 after the acceptor returns
  `Accept`. Rules 3 and 4 both refuse, so their order doesn't change the result.
  Zakura's acceptor also refuses when the `max_connections` cap is full, and
  ignores attempts once shutdown starts. Zakura has no IP ban list yet, so
  rule 1 never fires.

  A control handshake is an inbound connection past TLS that hasn't
  registered yet. The transport stops counting a connection when TLS
  finishes, so Zakura counts it against its IP from then until registration.
- **ADM-4.** `admit` MUST NOT block. It reads in-memory state only.
- **ADM-5.** The endpoint MUST set:
  - `ServerConfig::max_incoming` to `max_incoming`;
  - `incoming_buffer_size` to `incoming_buffer_bytes`;
  - `incoming_buffer_size_total` to `incoming_buffer_total_bytes`.
- **ADM-6.** When `handshake_timeout_secs` is set, every accepted handshake MUST
  finish within it. On expiry, the endpoint MUST drop the connecting attempt,
  which closes the connection, and count `zakura.quic.handshake.timed_out`.
- **ADM-7.** A pending handshake counts against its IP and against the endpoint
  from `Accept` until the handshake completes or fails.
- **ADM-8.** An established connection counts against its admitted IP until
  `Conn::closed()` resolves. The slot MUST NOT be released earlier.
- **ADM-9.** After the handshake, `Acceptor::handle` receives a `Conn` whose
  `remote_id()` is proven. Stage 2 (control hello, per-identity dedup, cohort
  check) stays in `zakura-network` and doesn't change.
- **ADM-10.** Retry tokens and stateless-reset tokens MUST use `quinn-proto`'s
  ring-backed defaults: a random `ring::hkdf::Prk` for tokens and a random
  `ring::hmac::Key` for resets. Each key is random per startup and never
  persists. Iroh's BLAKE3 keys go away.
- **ADM-11.** The endpoint task MUST remove a connection from its table only
  when `quinn-proto` reports the connection drained, and MUST NOT pass
  `quinn-proto` an event for a connection it already removed. A connection
  task that exits without a drained event reports one when it drops.

## 8. Dialing

- **DIAL-1.** `connect(NodeAddr, alpn)` MUST refuse a `NodeId` equal to the
  local one, before sending any packet.
- **DIAL-2.** `connect` MUST canonicalize, deduplicate and filter the addresses.
  It keeps only addresses whose family has a bound socket. If none remain, it
  fails with `ConnectFailed(NoUsableAddress)`.
- **DIAL-3.** With several addresses, `connect` MUST start one handshake per
  address in the given order, `dial_stagger_ms` apart. It MUST keep the first
  handshake that completes and close the others with application code 0. It
  fails only when all attempts fail.
- **DIAL-4.** The handshake deadline (ADM-6) also applies to each dial attempt.
  A caller's timeout around the whole dial MUST cover the last attempt:
  `dial_stagger` × (addresses − 1) plus the handshake deadline, or the idle
  timeout when the deadline is unset.
- **DIAL-5.** An ALPN mismatch MUST surface as `ConnectFailed(AlpnMismatch)`.
  `zakura-network` MUST back off from that peer for at least 10 minutes, so the
  node doesn't redial another cohort in a loop. The backoff applies to the
  node ID, whatever addresses its later records advertise. Each address keeps
  the ordinary dial backoff, because the dial error doesn't say which address
  mismatched.
- **DIAL-6.** The dialer's admitted IP is the address of the winning attempt.
- **DIAL-7.** A connection uses one path. `quinn-proto` has no multipath.

## 9. Transport control

All keys live in `[network.zakura.quic]`. `zakurad` reads them at startup.
Changing a key needs a restart.

Rules for every key:

- **CTRL-0.** Defaults MUST reproduce the Iroh backend's behavior, except where
  a row says otherwise. Startup MUST fail with a clear error when a value is
  out of range. Only `zakura-quic/src/config.rs` and the driver's
  per-connection transport builder may call `quinn-proto` config setters.
  The first release keeps the Iroh backend's admission and timer values. A
  later release may tighten them only with the SEC-4 measurement.

### 9.1 Configuration keys

| ID | Key | Type | Default | Range | quinn-proto mapping | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| CTRL-1 | `recv_buffer_bytes` | u32 | 7,340,032 | 65,536 – 1 GiB | `socket2` `set_recv_buffer_size` | The Iroh backend's netwatch value |
| CTRL-2 | `send_buffer_bytes` | u32 | 7,340,032 | 65,536 – 1 GiB | `socket2` `set_send_buffer_size` | The Iroh backend's netwatch value |
| CTRL-3 | `congestion_controller` | enum `cubic` \| `new_reno` | `cubic` | — | `congestion_controller_factory` | MUST be set explicitly, even for the default. Changing the default needs the PLAN step 12 fleet A/B and a spec change. The driver wraps the factory per connection to observe the controller (OBS-12). |
| CTRL-4 | `initial_window_bytes` | optional u64 | unset (`quinn-proto` default for the controller) | 14,720 – 16 MiB | `{Cubic,NewReno}Config::initial_window` | |
| CTRL-5 | `stream_receive_window_bytes` | u32 | 16 MiB | 64 KiB – 256 MiB | `stream_receive_window` | The Iroh backend's constant |
| CTRL-6 | `receive_window_bytes` | u32 | 32 MiB | ≥ CTRL-5, ≤ 1 GiB | `receive_window` | The Iroh backend's constant |
| CTRL-7 | `send_window_bytes` | u64 | 32 MiB | 64 KiB – 1 GiB | `send_window` | The Iroh backend's constant |
| CTRL-9 | `ack_frequency` | bool | `false` | — | `ack_frequency_config` | Sends ACK_FREQUENCY frames when `true` (WIRE-9) |
| CTRL-10 | `idle_timeout_secs` | u32 | 150 | 30 – 600 | `max_idle_timeout` | The Iroh backend's constant. MUST exceed `keep_alive_interval_secs` × 3. |
| CTRL-11 | `keep_alive_interval_secs` | u32 | 10 | 1 – 60 | `keep_alive_interval` | The Iroh backend's value |
| CTRL-12 | `gso` | bool | `true` | — | `enable_segmentation_offload`, and SOCK-13's GSO and GRO | Off also when the kernel lacks GSO |
| CTRL-13 | `initial_mtu` | u16 | 1,200 | 1,200 – 1,500 | `initial_mtu` | |
| CTRL-14 | `mtu_discovery` | bool | `true` | — | `mtu_discovery_config` (`None` when off) | Runs only when the socket accepts `PMTUDISC_PROBE` (SOCK-13) |
| CTRL-17 | `handshake_timeout_secs` | optional u32 | 10 | 2 – 60 | tokio timeout around the connecting attempt | **New**. Iroh had no deadline: only the 150 s idle timeout bounded a handshake. ADM-3 rule 4 makes a stalled handshake hold an admission slot, so a 10 s default bounds it. |
| CTRL-18 | `max_incoming` | u32 | 65,536 | 16 – 65,536 | `ServerConfig::max_incoming` | `quinn-proto`'s default; Iroh had no endpoint-level setter |
| CTRL-19 | `incoming_buffer_bytes` | u64 | 10 MiB | 4,096 – 10 MiB | `incoming_buffer_size` | `quinn-proto`'s default. 0-RTT is refused, so only Initial packets wait here. |
| CTRL-20 | `incoming_buffer_total_bytes` | u64 | 100 MiB | ≥ CTRL-19, ≤ 100 MiB | `incoming_buffer_size_total` | `quinn-proto`'s default |
| CTRL-21 | `max_pending_per_ip` | optional u32 | unset | 1 – 64 | endpoint (ADM-3) | **New**, off by default |
| CTRL-22 | `retry_threshold` | optional u32 | 8 | 0 – CTRL-18 | endpoint (ADM-3); 0 = always Retry unvalidated sources; CTRL-18 = never | **New**. Iroh never sent Retry. With 8, spoofed sources hold at most 8 of the `max_pending_handshakes` slots, and honest peers pay one extra round trip only while 8 handshakes are pending. |
| CTRL-23 | `dial_stagger_ms` | u32 | 250 | 0 – 5,000 | dialer (DIAL-3) | **New** |
| CTRL-24 | `kernel_drop_poll_secs` | u32 | 10 | 1 – 300 | SOCK-7 | **New**, Linux only |
| CTRL-25 | `qlog_dir` | optional path | unset | — | `qlog_stream`, one file per connection | Only with the `qlog` cargo feature; startup MUST fail if set without it |

These keys stay in `[network.zakura]` and keep their meaning:
`listen_addr`, `max_connections` (256), `max_connections_per_ip` (16),
`max_pending_handshakes` (32), `stream_open_rate_per_second`,
`message_rate_per_second` and the handshake's `max_open_streams`.

No release compiles both backends. There is no `transport` switch: the release
that adds the direct backend deletes the Iroh backend (PLAN step 10).

`[network.zakura] nat_traversal = true` MUST fail at startup. The error MUST say that hole punching was removed and name this spec.

Version 0.6 removed `bbr3`, `max_send_rate_bytes_per_second`,
`path_keep_alive_interval_secs` and `path_idle_timeout_secs`. A config that
sets a removed key MUST fail at startup with an unknown-key error.

### 9.2 Fixed settings

These are not configurable. Changing one needs a spec change.

- **CTRL-31.** Loss detection (`packet_threshold`, `time_threshold`,
  `initial_rtt`, `persistent_congestion_threshold`) and `max_ack_delay` stay at
  `quinn-proto` defaults.
- **CTRL-32.** `send_fairness` stays at `quinn-proto`'s default (on).

### 9.3 Stream priority

- **CTRL-40.** `zakura-network` MAY set a stream's priority once `SendStream`
  exposes `quinn-proto`'s `set_priority`. Block announcements and control
  messages SHOULD run at a higher priority than bulk block-sync bodies on the
  same connection. The P1 gate is block-propagation p99 under concurrent sync.

## 10. Migration

- **PATH-3.** A connection has one path. When the peer migrates to a new
  address (WIRE-11), the endpoint MUST check the new IP against
  `Acceptor::is_banned`. `quinn-proto` emits no migration event, so the
  connection task compares `remote_address()` with the last seen value at each
  10 s sample (OBS-3). If the new IP is banned, it MUST close the connection
  with application code 0 and reason `banned path`, and count
  `zakura.quic.paths.closed_banned`. Only a peer that holds the connection's
  keys can migrate, so PATH-3 guards against ban evasion, not against
  unauthenticated traffic.
- **PATH-4.** Migrations MUST NOT change the admitted IP. Per-IP accounting
  (ADM-7, ADM-8) uses only the admitted IP.

## 11. Observability

### 11.1 Metrics

- **OBS-1.** The endpoint MUST export these metrics through the `metrics` crate:

| Name | Kind | Meaning |
| --- | --- | --- |
| `zakura.quic.socket.recv_buffer_bytes` | gauge | Effective `SO_RCVBUF` (SOCK-3) |
| `zakura.quic.socket.send_buffer_bytes` | gauge | Effective `SO_SNDBUF` |
| `zakura.quic.socket.kernel_drops` | counter | `/proc/net/udp` drops for this socket (SOCK-7) |
| `zakura.quic.socket.rebinds` | counter | SOCK-9 rebinds |
| `zakura.quic.socket.recv_calls` / `.datagrams_received` | counter | Receive system calls and the datagrams they returned; the ratio is the GRO batch size (OBS-12) |
| `zakura.quic.socket.transmits` / `.datagrams_sent` | counter | Send system calls and the datagrams they carried; the ratio is the GSO batch size (OBS-12) |
| `zakura.quic.socket.recv_truncated` | counter | Datagrams dropped for `MSG_TRUNC` or `MSG_CTRUNC` (SOCK-13) |
| `zakura.quic.socket.send_dropped` / `.send_errors` | counter | Sends dropped for a droppable error (an oversized MTU probe, a vanished source address, GSO `EIO`), and other send errors |
| `zakura.quic.endpoint.queue_drops` | counter | Datagrams dropped at a full connection queue (DRV-3) |
| `zakura.quic.network_changes` | counter | SOCK-12 interface changes passed to connections |
| `zakura.quic.incoming.accepted` / `.refused` / `.retried` / `.ignored` | counter | ADM-2 outcomes |
| `zakura.quic.handshake.completed` / `.failed` / `.timed_out` | counter | Handshake results |
| `zakura.quic.handshake.duration_seconds` | histogram | Accept or dial to handshake complete |
| `zakura.quic.connections` | gauge | Open connections |
| `zakura.quic.dial.attempts` / `.alpn_mismatch` | counter | DIAL-3, DIAL-5 |
| `zakura.quic.paths.closed_banned` | counter | PATH-3 closes |
| `zakura.quic.packets.lost` | counter | Sum of `PathStats::lost_packets` deltas |
| `zakura.quic.congestion_events` | counter | Sum of `PathStats::congestion_events` deltas |
| `zakura.quic.bytes.sent` / `.received` | counter | UDP payload bytes |
| `zakura.quic.path.rtt_seconds` | histogram | Sampled every 10 s per connection |
| `zakura.quic.path.cwnd_bytes` | histogram | Sampled every 10 s per connection |

- **OBS-2.** Metrics MUST NOT carry per-peer labels. Per-connection detail goes
  to OBS-3.
- **OBS-3.** When `[network.zakura] trace_dir` is set, Zakura MUST write one
  `quic_conn` JSONL row per connection every 10 s and on close. The endpoint
  delivers each sample to a `ConnObserver` callback, because `zakura-quic`
  doesn't depend on Zakura's trace crate. Each row holds `event` (`sample` or
  `closed`), `peer`, `admitted_ip`, `rtt_micros`, `cwnd`, `lost_packets`,
  `congestion_events`, `bytes_sent`, `bytes_received`, `current_mtu`,
  `bytes_in_flight`, `send_blocked_micros` and `close_reason`. `peer` is the
  hashed peer label the other tables use, so rows join with `conn.jsonl`.
  `admitted_ip` follows `expose_peer_addresses`: it is `v4redacted` or
  `v6redacted` unless that is set.
- **OBS-4.** `Conn::stats()` MUST return a `ConnStats` with three parts:
  - `connection`: `quinn-proto`'s `ConnectionStats`, with UDP, frame and path
    stats;
  - `congestion`: the OBS-12 controller stats;
  - `driver`: the OBS-12 driver counters.
- **OBS-12.** The endpoint MUST expose these low-level stats without a fork:
  - `quinn-proto`'s `ConnectionStats`: RTT, congestion window, congestion
    events, lost packets and bytes, MTU and black holes; UDP datagrams, bytes
    and I/O calls in each direction; and per-frame-type counts, including
    `DATA_BLOCKED` and `STREAM_DATA_BLOCKED`.
  - An instrumented congestion controller that wraps Cubic or NewReno through
    `congestion_controller_factory`. It reports the window, the slow-start
    threshold, the pacing rate, bytes in flight and the app-limited flag after
    the last ACK batch, and acknowledged bytes.
  - Driver counters per connection: bytes Zakura handed over that
    `quinn-proto` hasn't accepted yet, the time writes waited on flow control
    or the send window, send system calls, datagrams sent and datagrams routed
    in.
  - Socket counters per socket (`SocketStats`): receive calls, datagrams
    received, queue drops (DRV-3) and whether GSO is on.

  Per-stream unacknowledged bytes and the retransmit queue size need a fork,
  so the endpoint doesn't report them.

### 11.2 Logs

- **OBS-10.** Logs use the target `zakura_quic`. Startup MUST log once at
  `info`: bound addresses, effective buffers, GSO state, congestion controller
  and ALPNs.
- **OBS-11.** Per-packet and per-`Incoming` events log at `trace` only.

## 12. API

The public surface of `zakura-quic`. Changing it is a semver change of the crate.

- **API-1.** Identity: `NodeId`, `NodeSecretKey` and `NodeAddr { id, direct: Vec<SocketAddr> }`.
- **API-2.** `QuicEndpoint`:
  - `bind(secret, &QuicBindConfig, &QuicConfig) -> Result<QuicEndpoint, BindError>`;
  - `local_id()`, `local_addrs()`, `socket_stats()`, `network_changes()` and `config()`;
  - `set_conn_observer(ConnObserver)` for OBS-3;
  - `connect(NodeAddr, alpn: &[u8]) -> Result<Conn, ConnectError>`;
  - `serve(impl Acceptor) -> Result<(), BindError>`, which starts admission on
    every endpoint task;
  - `shutdown(&self)`. `QuicEndpoint` is a cheap clone.
- **API-3.** `trait Acceptor: Send + Sync + 'static`:
  - `fn admit(&self, incoming: &IncomingInfo) -> Admit`;
  - `fn alpns(&self) -> Vec<Vec<u8>>`, in preference order;
  - `fn handle(&self, conn: Conn) -> BoxFuture<'static, ()>`; `Conn::alpn()`
    carries the negotiated ALPN;
  - `fn is_banned(&self, ip: IpAddr) -> bool`, default `false`, for PATH-3.
- **API-4.** `Conn`:
  - `remote_id()`, `admitted_ip()`, `admitted_addr()`, `alpn()` and `stable_id()`;
  - `open_bi()`, `accept_bi()`, `close(code, reason)`, `closed()` and
    `close_reason()`;
  - `async stats() -> Option<ConnStats>`, `None` once the connection task has
    exited.
  Dropping the last `Conn` handle closes the connection (DRV-7).
- **API-5.** Streams are `zakura-quic`'s own `SendStream` and `RecvStream`
  (DRV-4, DRV-5). A `SendStream` write may return before `quinn-proto` accepts
  its data, so a write error can belong to an earlier write (DRV-4):
  - `SendStream`: `write_all`, `write_chunk` (no copy), `finish` and `reset`;
  - `RecvStream`: `read`, `read_chunk`, `read_exact`, `read_to_end` and `stop`.
  The crate defines `WriteError`, `ReadError`, `ReadExactError`,
  `ReadToEndError` and `ClosedStream`. It re-exports `quinn-proto`'s `VarInt`,
  `ConnectionError`, `ConnectionStats`, `PathStats`, `UdpStats` and
  `FrameStats`.
- **API-6.** Errors: `ConnectError` has the variants `SelfDial`,
  `NoUsableAddress`, `AlpnMismatch`, `HandshakeTimeout`, `Refused`, `WrongIdentity`,
  `Endpoint(quinn_proto::ConnectError)`, `Tls` and
  `Transport(quinn_proto::ConnectionError)`.
  `Endpoint` covers local endpoint failures such as a stopping endpoint; `Tls`
  covers other TLS alerts. `BindError` and `ConfigError` name the failing key
  or address.
- **API-7.** `shutdown` MUST:
  1. stop accepting;
  2. close every connection with application code 0;
  3. wait for every connection to drain, for at most 3 s;
  4. drop the sockets.

  Dropping the last `QuicEndpoint` handle without `shutdown` MUST still stop
  accepting, close every connection with code 0 and free the sockets.
  Spawned tasks MUST NOT hold a handle that keeps the endpoint alive.
  That includes a task waiting on a peer's handshake. After `shutdown`, other
  handles stay valid but can no longer dial or serve.
- **API-8.** The testkit's `LocalEndpointFactory` MUST build `zakura-quic`
  endpoints bound to `127.0.0.1:0` with production settings, except where a test
  overrides a `QuicConfig` field.

## 13. Compatibility

Version 0.6 drops Iroh compatibility (decided 2026-10-07). Moving from the noq
fork to `quinn-proto` changes the wire profile (§17d), so the ALPN moved from
`p2p-v2/2` to `p2p-v2/3` under COMPAT-4. A node on `p2p-v2/3` and a node on an
older release reach each other only over the legacy protocol, and
`p2p_stack = "dual"` nodes bridge the two (COMPAT-7).

- **COMPAT-4.** A wire break (a WIRE-13 change, a moved `quinn-proto`
  codepoint, or a `quinn-proto` release that changes the transport
  parameters) MUST bump the ALPN to the next `p2p-v2/N`. The endpoint MUST NOT
  advertise an ALPN whose wire profile it doesn't match.
- **COMPAT-5.** QUIC version numbers other than v1 are reserved for a future
  Noise handshake. That backend MUST NOT change the v1 TLS profile.
- **COMPAT-7.** After an ALPN bump, a `p2p_stack = "dual"` node MUST fall back
  to legacy for a peer on the other ALPN without a stall: the native dial
  fails within the handshake bound with an ALPN error, and DIAL-5 backs off.
  `p2p_stack = "zakura"` nodes lose peers on the other ALPN until they upgrade.

## 14. Security

- **SEC-1.** The TLS and key code MUST pass these known-answer tests:
  - RFC 8032 vectors;
  - small-order and non-canonical public keys refused (ID-1);
  - no signature verifying under a mixed-order key (ID-6);
  - small-order `R` points, `R` points with a torsion component, and
    non-canonical `S` scalars refused (ID-6);
  - honest keys and signatures accepted.
- **SEC-2.** These handshakes MUST fail:
  - a dial that reaches a server with the wrong `NodeId`;
  - a client without a key;
  - TLS 1.2;
  - an X.509 certificate instead of a raw public key;
  - a signature scheme other than Ed25519.
- **SEC-3.** For a refused or ignored IP, no `Connecting` may exist. A test
  asserts that `quinn-proto` created no connection state.
- **SEC-4.** A flood of Initials that never complete MUST stay bounded by
  `max_incoming`, `incoming_buffer_total_bytes` and `handshake_timeout_secs`.
  The test MUST report RSS growth at 10,000 Initials per second, both at
  today's values and with every limit in §9.1 set. That measurement decides
  whether a later release tightens the defaults (§9.1).
- **SEC-6.** No early data reaches `quinn-proto` buffers (TLS-10).
- **SEC-8.** A change to `tls/`, `key.rs`, a DEP-7 `unsafe` item, `ADM-*` or
  `PATH-*` needs approval from both transport owners (PLAN §9). An AI-assisted
  review counts as one reviewer at most.

## 14a. Platforms

- **PLAT-1.** `zakura-quic` MUST build and pass its tests on Linux x86_64,
  Linux aarch64 and macOS aarch64 in CI. macOS is a supported platform for
  native nodes.
- **PLAT-2.** Linux-only features (the SOCK-7 kernel drop counter and the
  SOCK-13 fast path) MUST compile out on other platforms. Their metrics are
  absent there.

## 15. quinn-proto dependency policy

- **QP-1.** The transport owners MUST watch RustSec and Quinn's GitHub security
  advisories for `quinn-proto`.
- **QP-2.** When an advisory's fix ships, the workspace MUST move to the fixed
  patch release within a week.
- **QP-3.** Each `quinn-proto` bump MUST add a cargo-vet delta audit from the
  previous audited version.
- **QP-4.** Zakura MUST NOT fork `quinn-proto` (DEP-4). A fix Zakura needs goes
  upstream, or Zakura works around it in `zakura-quic`.

## 16. Conformance tests

| Test | Covers |
| --- | --- |
| `cargo tree` denylist in CI | DEP-1 |
| `unsafe` appears only in the DEP-7 items | DEP-7 |
| Identity known-answer and key-file round trip | ID-1–ID-7, SEC-1 |
| TLS refusal matrix | TLS-1–TLS-8, SEC-2 |
| Transport-parameter snapshot: decode the client and server parameters and compare each value with the WIRE profile, ignoring connection IDs, the stateless reset token and the parameter order | WIRE-5–WIRE-10, WIRE-14 |
| Buffer read-back and clamp warning: bind under a lowered `rmem_max` in a user namespace | SOCK-3–SOCK-5 |
| Busy-socket loopback run: zero `lost_packets` caused by the sender, bidirectional, 64 MiB | SOCK-6, G7 |
| `/proc/net/udp` drop counter with a deliberately slow receiver | SOCK-7 |
| A changed interface list notifies connections once, and an open connection keeps working | SOCK-12 |
| Control-message round trip, and every truncated or lying control message parses to nothing | SOCK-13 |
| Loopback transfer through the SOCK-13 fast path beats the plain path on the same host | SOCK-13 |
| Cancelled reads lose no data; cancelled writes keep their order; concurrent streams keep their data apart; a transfer larger than the receive window completes | DRV-4–DRV-6 |
| Dropping the last `Conn` closes the connection | DRV-7 |
| Admission ordering, refuse, ignore, retry, timeout | ADM-1–ADM-7, SEC-3, SEC-4 |
| Happy-eyeballs dial with mixed v4/v6 addresses and one black-holed address | DIAL-1–DIAL-6 |
| A relay that rebinds like a NAT: migration to a banned IP closes the connection, and migration to another IP keeps it with the admitted IP unchanged | PATH-3, PATH-4 |
| Config round trip, defaults, range errors, removed keys, `nat_traversal` error | CTRL-0–CTRL-25 |
| Congestion controller is Cubic when unset | CTRL-3 |
| Stats report the controller, driver and socket counters | OBS-4, OBS-12 |
| Metrics presence and no per-peer labels | OBS-1, OBS-2 |
| Shutdown order and the 3 s bound; drop without `shutdown` frees the port | API-7 |
| The focused `zakura-network` tests and the full workspace suite with Iroh deleted | No regression |
| Fleet throughput: one 1 GiB transfer and 64 concurrent streams match or beat the noq build in throughput, at no more CPU per GB | G7, PLAN §8 |
| Netem matrix (PLAN §7.3): the swap branch matches or beats the current release in every cell beyond noise | PLAN §7.4 |
| Dual-stack node falls back to legacy against the other ALPN | COMPAT-7 |

## 17. Feature priority

Every requirement above is P0 except these:

- **P1** (after the default flips, each gated by a measurement): changing the
  CTRL-3 default, CTRL-9, CTRL-40 and the CTRL-25 qlog feature. The keys and
  their mappings ship in P0; only their use waits for the measurement.
- **P2** (on demand only): several `SO_REUSEPORT` sockets, rebinding a socket
  on a network change (SOCK-12 only notifies connections), multipath, BBR,
  ECN, hole punching and Noise (COMPAT-5).

## 17a. Changes in version 0.2

The first implementation changed these requirements:

- **DEP-6.** The audit backlog file lives under `qa/supply-chain/`.
- **SOCK-1.** States that a peer-opened path to a second socket never
  validates. The interop probe found this with a node bound to both loopback
  families.
- **WIRE-9.** noq always sends `min_ack_delay`, and so does Iroh 1.1; version
  0.1 said the parameter was absent by default.
- **ADM-1.** `admit` takes an `IncomingInfo` that also carries the endpoint's
  pending handshake counts, so the acceptor can apply ADM-3 without its own table.
- **ADM-3.** The endpoint applies rules 3 and 5 and the acceptor applies rules
  1, 2 and 4. Zakura's acceptor also refuses at the `max_connections` cap.
- **PATH-3.** The path 0 check runs at each 10 s sample only.
- **OBS-3.** Rows come from a `ConnObserver` callback and use Zakura's hashed
  peer label and redaction.
- **API-2.** `serve` returns `Result<(), BindError>` and spawns its loops;
  `shutdown` takes `&self`; `config()` and `set_conn_observer()` are added.
- **API-3.** `admit` takes `&IncomingInfo`; `handle` returns a boxed future and
  takes only the `Conn`; `is_banned` is added.
- **API-6.** `ConnectError` adds `Endpoint` and `Tls`.
- **API-7.** Dropping the last handle also closes the endpoint. An embedded
  node that drops its future without calling `shutdown` got its port back
  under Iroh and must still get it back.

## 17b. Changes in version 0.3

The V12 audit of the first implementation (run 8760) changed these
requirements:

- **SOCK-11.** Canonicalization keeps IPv6 scope IDs (F-305595).
- **ADM-3.** Rule 2 counts control handshakes against their IP (F-305585), and
  rule 5 fires when pending handshakes reach the threshold, as implemented.
- **PATH-2.** Lagged path events trigger a rebuild of the open path set
  (F-305586).
- **DIAL-4.** A whole-dial timeout covers the last staggered attempt (F-305593).
- **DIAL-5.** The 10-minute backoff applies to the node ID, not to each address
  (F-305587, F-305594).
- **CTRL-17, CTRL-22.** The handshake deadline defaults to 10 s and the Retry
  threshold to 8 (F-305590). Without them, spoofed Initials held every
  admission slot for 150 s and Zakura refused every honest inbound peer.
- **API-7.** Handshake tasks hold the endpoint weakly (F-305588), and
  `shutdown` drops its sockets while other handles live (F-305596).

## 17c. Changes in version 0.4

- **SOCK-12.** Added. Version 0.3 dropped Iroh's network-change handling on the
  premise that servers bind explicit addresses. The default `listen_addr` is
  `0.0.0.0:8234`, an unspecified address, and noq pins each path to the local
  address that first received its packets. Without SOCK-12, a node whose
  address changed kept sending from the vanished address until its
  connections idled out and it redialed. Iroh learned of changes from OS
  events; polling every 5 s needs no new dependency, because SOCK-10 already
  lists interfaces.

## 17d. Changes in version 0.6

Zakura dropped Iroh compatibility on 2026-10-07. Iroh compatibility was the
only reason to build on noq. noq lacks the GHSA-4w2j/qfwj and GHSA-hmxj fixes
that `quinn-proto` 0.11.19 ships, and depending on it meant maintaining a fork.
Version 0.6 builds on upstream `quinn-proto` with a Zakura-owned driver, and
adds three crates to `zakurad` instead of twenty. Version 0.5 (#1296) isn't
part of this version; its policies return later on the driver.

Deleted:

- **DEP-5.** `tools/iroh-interop/` is gone.
- **WIRE-3, WIRE-4.** `quinn-proto` has no multipath and no n0 NAT-traversal
  parameter.
- **CTRL-8.** `max_send_rate_bytes_per_second`: `quinn-proto` has no send-rate
  cap.
- **CTRL-15, CTRL-16, CTRL-30.** The path keepalive, the path idle timeout and
  the multipath path budget went with multipath.
- **PATH-1, PATH-2, PATH-5, PATH-6.** Path events and peer-opened paths went
  with multipath. PATH-3 keeps the migration ban check.
- **COMPAT-1, COMPAT-2, COMPAT-3, COMPAT-6.** Iroh interop and the n0 frames
  are gone.
- **SEC-5, SEC-7.** The peer-opened path test and the fork regression tests are
  gone. The PATH-3 relay test replaces SEC-5.
- **NOQ-1 to NOQ-5.** §15 now holds the `quinn-proto` policy (QP-1 to QP-4).

Added:

- **DEP-7.** The owned-`unsafe` budget for `getifaddrs` and the Linux UDP fast
  path, which replace `nix`, `memoffset` and `quinn-udp`.
- **WIRE-14.** No NEW_TOKEN frames.
- **SOCK-13.** The Linux fast path: GSO, GRO, packet info and the
  don't-fragment probe option.
- **§6a, DRV-1 to DRV-7.** The driver: an endpoint task per socket, a
  connection task per connection, a bounded datagram queue, ordered
  request-and-reply stream operations with 64 KiB of write-ahead per stream,
  cancellation safety, read-driven flow control and drop-closes.
- **ADM-11.** The connection table frees an entry only on `quinn-proto`'s
  drained event.
- **OBS-12.** Controller, driver and socket stats.
- **QP-1 to QP-4.** The `quinn-proto` dependency policy.

Changed:

- **DEP-1.** Also forbids noq, `quinn`, `quinn-udp`, `ed25519-dalek`, `nix` and
  `memoffset`.
- **DEP-2, DEP-3, DEP-4.** List `quinn-proto` with `rustls-ring`, require
  crates.io 0.11.19 or later, and forbid git and `[patch]` pins.
- **ID-1, ID-6.** Ed25519 runs on `ed25519-zebra` with `curve25519-dalek` point
  checks on A and R, replacing `ed25519-dalek`'s `verify_strict`. `NodeId`
  construction skips the torsion check; signature verification runs it on A
  and R.
- **WIRE-1, WIRE-9, TLS-11.** Name `quinn-proto`.
- **WIRE-2.** The ALPN moves to `p2p-v2/3`, a wire break under COMPAT-4.
- **WIRE-13.** Lists the surviving wire rules.
- **SOCK-1, SOCK-3, SOCK-6, SOCK-8, SOCK-9, SOCK-10, SOCK-12.** Describe the
  owned socket layer and driver. SOCK-12 calls `local_address_changed` on each
  connection instead of noq's network-change hint.
- **ADM-1, ADM-2, ADM-5, ADM-6, ADM-8, ADM-10.** Map onto `quinn_proto::Endpoint`.
- **DIAL-7.** One path per connection.
- **CTRL-0, CTRL-1–CTRL-4, CTRL-11, CTRL-12, CTRL-14, CTRL-18–CTRL-20,
  CTRL-25, CTRL-31, CTRL-32, CTRL-40.** Name `quinn-proto` mappings. CTRL-3
  drops `bbr3`, because `quinn-proto`'s BBR is an older, experimental BBRv1.
- **PATH-3, PATH-4.** Rewritten for a single path.
- **OBS-1.** Drops `zakura.quic.paths.peer_opened` and adds the socket, GSO,
  GRO and queue counters.
- **OBS-3.** Replaces `paths_open` with `bytes_in_flight` and
  `send_blocked_micros`.
- **OBS-4.** `ConnStats` holds connection, congestion and driver stats.
- **API-2, API-4, API-5, API-6, API-7.** `network_changes()`; an async
  `stats()`; the crate's own stream and error types; `quinn-proto` error types;
  shutdown waits for drain instead of `wait_idle`.
- **COMPAT-4.** Names `quinn-proto` codepoints.
- **SEC-1, SEC-3, SEC-6, SEC-8, PLAT-2.** Follow the identity, driver and
  socket changes.
