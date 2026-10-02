# Spec: `zakura-quic` transport

Status: draft for review, 2026-10-02. Version 0.2.
This document is authoritative for the `zakura-quic` crate, its configuration,
its wire behavior and the Zakura noq fork.
[Decision 0003](../decisions/zakura/0003-zakura-quic-transport.md) records the
decision. "PLAN" and "DESIGN" below name the planning notes that hold the
evidence and step order; they live outside this repository.

Version 0.2 matches the first implementation (zakura draft PR). It changes
DEP-6, SOCK-1, WIRE-9, ADM-1, ADM-3, PATH-3, OBS-3, API-2, API-3 and API-6;
§17a lists each change.

## 0. Conventions

- **MUST**, **MUST NOT**, **SHOULD** and **MAY** follow RFC 2119.
- Each requirement has an ID such as `SOCK-3`. Tests, PRs and review comments
  cite the ID.
- A change to this spec needs a PR that bumps the version above and names the
  changed IDs.

Terms:

- **Endpoint**: one `QuicEndpoint`. It owns one noq endpoint per bound socket.
- **Iroh backend**: today's transport, Zakura's fork of Iroh 1.1.
- **Direct backend**: `zakura-quic`.
- **Admitted IP**: the source IP of the `Incoming` that `admit` accepted, or the
  IP a dial connected to. It never changes for the life of the connection.
- **Peer-opened path**: a multipath path that the remote opens after the handshake.
- **Fork**: Zakura's noq fork, `zakura-core/iroh-quinn`.

## 1. Goals

| ID | Goal | Met by |
| --- | --- | --- |
| G1 | Remove the Iroh wrapper packages, and trim the QUIC engine's packages where the fork allows it | §2 |
| G2 | Give Zakura control of every QUIC setting that affects throughput, latency or memory | §6, §9 |
| G3 | Refuse or limit an IP before the handshake allocates state | §7 |
| G4 | Report transport health per endpoint and per connection | §11 |
| G5 | Stay wire-compatible with Iroh 1.x on direct paths when that is simple (a goal, not a requirement) | §4, §5, §13 |
| G6 | Keep node identity, key files and bootstrap entries unchanged | §3 |
| G7 | Never lose application data in the transport, and never drop a packet on a busy socket | §6 |

## 2. Dependencies

- **DEP-1.** The `zakurad` production graph MUST NOT contain `zakura-iroh`,
  `zakura-iroh-base`, `zakura-iroh-dns`, `zakura-iroh-relay`, `netwatch`,
  `netdev`, any `netlink-*`, any `n0-*`, any `hickory-*`, `papaya`, `seize`,
  `iroh-metrics` or `tokio-websockets`. CI enforces the list with
  `cargo tree -e normal -i <pkg>` and fails on any hit.
- **DEP-2.** `zakura-quic` MAY depend directly on these packages and no others:
  `noq`, `noq-proto` and `noq-udp` (from the fork), `rustls`, `rustls-pki-types`,
  `ring`, `socket2`, `ed25519-dalek`, `curve25519-dalek`, `zeroize`, `tokio`, `futures`,
  `bytes`, `tracing`, `metrics`, `thiserror`, `serde` and `hex`.
  - `nix` or `libc` MAY be added for interface listing and `/proc/net/udp` parsing.
  - `data-encoding` MAY be added only if step 3 of PLAN §6 finds a deployed base32 key.
  - Any other dependency needs a spec change.
- **DEP-3.** `zakura-quic` MUST enable these noq features and no others:
  `rustls`, `ring` and `runtime-tokio`. The `qlog` feature MAY be enabled behind
  a `zakura-quic` cargo feature that release builds leave off.
- **DEP-4.** The workspace MUST pin noq, noq-proto and noq-udp to an exact fork
  release (`=x.y.z`).
- **DEP-5.** Iroh crates MAY appear only in `tools/iroh-interop/`. That
  directory MUST be its own Cargo workspace with its own `Cargo.lock`, so
  `cargo deny`, `cargo vet` and the `zakurad` build never see them.
- **DEP-6.** When Iroh is removed (PLAN step 10):
  - `cargo vet prune` MUST run;
  - `deny.toml` MUST lose every Iroh-only skip, ban and ignore;
  - `qa/supply-chain/iroh-1.1-audit-backlog.json` MUST be deleted.

## 3. Identity

- **ID-1.** `NodeId` is a 32-byte compressed Ed25519 public key. Construction
  MUST reject bytes that don't decompress to a curve point.
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
- **ID-6.** Signature verification for the TLS handshake MUST use
  `ed25519_dalek::VerifyingKey::verify_strict`.
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
  module may build a rustls config. A later Noise backend plugs in at noq's
  crypto traits under its own QUIC version (§13).

## 5. QUIC wire profile

- **WIRE-1.** QUIC version 1 (`0x00000001`), using noq's version list.
- **WIRE-2.** Production ALPN is `p2p-v2/2`. The server MUST offer ALPNs in the
  order the acceptor lists them, not sorted. A handshake that negotiates no
  shared ALPN MUST fail with TLS alert `no_application_protocol`.
- **WIRE-3.** Multipath MUST be negotiated with
  `max_concurrent_multipath_paths(8)` (transport parameter `initial_max_path_id`,
  `0x3e`, paths 0–7). This matches Iroh 1.1.
- **WIRE-4.** The `N0NatTraversal` transport parameter (`0x3d7f91120401`) MUST
  be absent (`max_remote_nat_traversal_addresses` unset).
- **WIRE-5.** `grease_quic_bit` MUST be `false`.
- **WIRE-6.** Datagrams MUST be off: `datagram_receive_buffer_size(None)` and
  `datagram_send_buffer_size(0)`. No `max_datagram_frame_size` parameter is sent.
- **WIRE-7.** Unidirectional streams MUST be off: `max_concurrent_uni_streams(0)`.
- **WIRE-8.** `max_concurrent_bidi_streams` equals the handshake config's
  `max_open_streams` (1,024 by default).
- **WIRE-9.** The `min_ack_delay` transport parameter is always sent with
  value 1,000 µs, as noq and Iroh 1.1 both do. `ack_frequency = true` (CTRL-9)
  only lets this endpoint send ACK_FREQUENCY frames; it doesn't change the
  transport parameters.
- **WIRE-10.** Observed-address reports are neither sent nor requested.
- **WIRE-11.** The server MUST allow connection migration
  (`ServerConfig::migration(true)`). PATH-3 re-checks every new address.
- **WIRE-12.** The client MUST NOT enable `server_handshake_migration`. Zakura
  races separate handshakes instead (DIAL-3).
- **WIRE-13.** Any change to WIRE-1 through WIRE-7 is a wire break. A wire
  break MUST bump the ALPN (COMPAT-4).

## 6. Endpoint and sockets

- **SOCK-1.** Bind addresses follow `[network.zakura] listen_addr`:
  - a set address binds exactly that address;
  - an unset address binds `127.0.0.1:0` and `[::1]:0`.
  Each bound socket gets its own noq endpoint. A path that a peer opens to
  another of this node's sockets therefore never validates: that socket's
  endpoint doesn't know the connection, and the peer abandons the path. The
  connection stays on its first path. Only an unset `listen_addr` binds more
  than one socket.
- **SOCK-2.** IPv6 sockets MUST set `IPV6_V6ONLY`.
- **SOCK-3.** At bind, the endpoint MUST request `recv_buffer_bytes` and
  `send_buffer_bytes` through noq-udp's `set_recv_buffer_size` and
  `set_send_buffer_size`. It MUST read both sizes back.
- **SOCK-4.** If a read-back size is below the request, the endpoint MUST log
  one `warn` line that names the requested size, the effective size and the
  sysctl to raise (`net.core.rmem_max` or `wmem_max`). Linux reports double the
  granted size; the comparison MUST account for that.
- **SOCK-5.** The effective sizes MUST be exported as gauges (OBS-1).
- **SOCK-6.** The send path MUST NOT drop a packet because the socket returned
  `WouldBlock` or `Pending`. The endpoint MUST use noq's own UDP driver, which
  keeps the pending transmit and retries when the socket is writable.
- **SOCK-7.** On Linux, the endpoint MUST read the per-socket `drops` column from
  `/proc/net/udp` or `/proc/net/udp6`, matching the socket's inode. It reads
  every `kernel_drop_poll_secs` and exports the value as a counter (OBS-1). On
  other platforms the metric is absent.
- **SOCK-8.** GSO and GRO follow `gso` (CTRL-12). ECN and packet-info handling
  stay at noq-udp's defaults.
- **SOCK-9.** On a fatal socket error (not `WouldBlock` or `Interrupted`), the
  endpoint MUST call `noq::Endpoint::rebind` once with a new socket on the same
  address. If that fails or the error repeats within 60 s, the endpoint MUST
  surface the error and stop.
- **SOCK-10.** `local_addrs()` returns the bound addresses. For an unspecified
  bind, the advertised-address logic in `zakura-network` lists interfaces itself
  (`getifaddrs`), replacing netwatch.
- **SOCK-11.** Every `SocketAddr` that crosses the `zakura-quic` boundary MUST be
  canonical: an `::ffff:a.b.c.d` address becomes `a.b.c.d`.

## 7. Admission

Admission runs in two stages. Stage 1 runs on each `Incoming` before any crypto
work. Stage 2 runs after TLS proves the `NodeId` and is today's Zakura logic.

- **ADM-1.** For every `Incoming`, the accept loop MUST call
  `Acceptor::admit(&IncomingInfo)` before it calls `accept`. `IncomingInfo` holds:
  - `remote`, the canonical source address (SOCK-11);
  - `validated`, which is `Incoming::remote_address_validated()`;
  - `pending_total` and `pending_from_ip`, the handshakes in progress on the
    endpoint and from `remote`'s IP (ADM-7).
- **ADM-2.** `admit` returns one of four outcomes, each mapped to a noq call:
  - `Accept` → `Incoming::accept`;
  - `Refuse` → `Incoming::refuse`, which sends `CONNECTION_REFUSED`;
  - `Retry` → `Incoming::retry`, which sends a stateless Retry;
  - `Ignore` → `Incoming::ignore`, which sends nothing.
- **ADM-3.** Zakura's acceptor MUST return:
  1. `Ignore` for a banned IP;
  2. `Refuse` when the IP's established plus pending connections reach
     `max_connections_per_ip`;
  3. `Refuse` when `max_pending_per_ip` is set and the IP has that many
     handshakes in progress;
  4. `Refuse` when the endpoint has `max_pending_handshakes` handshakes in progress;
  5. `Retry` when `retry_threshold` is set, `validated` is false, and pending
     handshakes exceed `retry_threshold`;
  6. `Accept` otherwise.
  Rules apply in that order. Zakura's acceptor applies rules 1, 2 and 4: it
  counts `pending_from_ip` with the IP's established connections, and counts
  `pending_total` with the control handshakes holding `max_pending_handshakes`
  permits. The endpoint applies rules 3 and 5 after the acceptor returns
  `Accept`. Rules 3 and 4 both refuse, so their order doesn't change the result.
  Zakura's acceptor also refuses when the `max_connections` cap is full, and
  ignores attempts once shutdown starts. Zakura has no IP ban list yet, so
  rule 1 never fires.
- **ADM-4.** `admit` MUST NOT block. It reads in-memory state only.
- **ADM-5.** The endpoint MUST set:
  - `ServerConfig::max_incoming` to `max_incoming`;
  - `incoming_buffer_size` to `incoming_buffer_bytes`;
  - `incoming_buffer_size_total` to `incoming_buffer_total_bytes`.
- **ADM-6.** When `handshake_timeout_secs` is set, every accepted handshake MUST
  finish within it. On expiry, the endpoint MUST drop the connecting future, which closes the
  connection, and count `zakura.quic.handshake.timed_out`.
- **ADM-7.** A pending handshake counts against its IP and against the endpoint
  from `Accept` until the handshake completes or fails.
- **ADM-8.** An established connection counts against its admitted IP until
  `Conn::closed()` resolves. The slot MUST NOT be released earlier. (Q5: if the
  noq connection-lifetime hook from #1179 lands, release follows noq's state
  release instead.)
- **ADM-9.** After the handshake, `Acceptor::handle` receives a `Conn` whose
  `remote_id()` is proven. Stage 2 (control hello, per-identity dedup, cohort
  check) stays in `zakura-network` and doesn't change.
- **ADM-10.** Retry tokens and stateless-reset tokens MUST use noq's ring-backed
  defaults: `RetryTokenKey` for tokens and `ring::hmac::Key` for resets
  (`noq-proto/src/crypto/ring_like.rs`). Each key is random per startup and
  never persists. Iroh's BLAKE3 keys go away.

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
- **DIAL-5.** An ALPN mismatch MUST surface as `ConnectFailed(AlpnMismatch)`.
  `zakura-network` MUST back off from that peer for at least 10 minutes, so the
  node doesn't redial another cohort in a loop.
- **DIAL-6.** The dialer's admitted IP is the address of the winning attempt.
- **DIAL-7.** The dialer MUST NOT open extra multipath paths. It MAY do so in a
  later spec version.

## 9. Transport control

All keys live in `[network.zakura.quic]`. `zakurad` reads them at startup.
Changing a key needs a restart.

Rules for every key:

- **CTRL-0.** Defaults MUST reproduce today's Iroh-backend behavior, except where
  a row says otherwise. Startup MUST fail with a clear error when a value is
  out of range. Only `zakura-quic/src/config.rs` may call noq config setters.
  The first release keeps today's admission and path-timer values. A later
  release may tighten them only with the SEC-4 measurement.

### 9.1 Configuration keys

| ID | Key | Type | Default | Range | noq mapping | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| CTRL-1 | `recv_buffer_bytes` | u32 | 7,340,032 | 65,536 – 1 GiB | `set_recv_buffer_size` | Today's netwatch value |
| CTRL-2 | `send_buffer_bytes` | u32 | 7,340,032 | 65,536 – 1 GiB | `set_send_buffer_size` | Today's netwatch value |
| CTRL-3 | `congestion_controller` | enum `cubic` \| `new_reno` \| `bbr3` | `cubic` | — | `congestion_controller_factory` | MUST be set explicitly, even for the default. Changing the default needs the PLAN step 12 fleet A/B and a spec change. |
| CTRL-4 | `initial_window_bytes` | optional u64 | unset (noq default for the controller) | 14,720 – 16 MiB | `{Cubic,NewReno,Bbr3}Config::initial_window` | |
| CTRL-5 | `stream_receive_window_bytes` | u32 | 16 MiB | 64 KiB – 256 MiB | `stream_receive_window` | Today's constant |
| CTRL-6 | `receive_window_bytes` | u32 | 32 MiB | ≥ CTRL-5, ≤ 1 GiB | `receive_window` | Today's constant |
| CTRL-7 | `send_window_bytes` | u64 | 32 MiB | 64 KiB – 1 GiB | `send_window` | Today's constant |
| CTRL-8 | `max_send_rate_bytes_per_second` | optional u64 | unset (no cap) | ≥ 125,000 | `max_outgoing_bytes_per_second` | Per connection; for metered links |
| CTRL-9 | `ack_frequency` | bool | `false` | — | `ack_frequency_config` | Sends ACK_FREQUENCY frames when `true` (WIRE-9) |
| CTRL-10 | `idle_timeout_secs` | u32 | 150 | 30 – 600 | `max_idle_timeout` | Today's constant. MUST exceed `keep_alive_interval_secs` × 3. |
| CTRL-11 | `keep_alive_interval_secs` | u32 | 10 | 1 – 60 | `keep_alive_interval` | Today's value. The 5 s path keepalive (CTRL-15) still overrides it on multipath connections. |
| CTRL-12 | `gso` | bool | `true` | — | `enable_segmentation_offload` | |
| CTRL-13 | `initial_mtu` | u16 | 1,200 | 1,200 – 1,500 | `initial_mtu` | |
| CTRL-14 | `mtu_discovery` | bool | `true` | — | `mtu_discovery_config` (`None` when off) | |
| CTRL-15 | `path_keep_alive_interval_secs` | u32 | 5 | 1 – 60 | `default_path_keep_alive_interval` | Today's value (Iroh forces 5 s) |
| CTRL-16 | `path_idle_timeout_secs` | u32 | 15 | 5 – CTRL-10 | `default_path_max_idle_timeout` | Today's value (Iroh forces 15 s). noq never idles out the last path; CTRL-10 governs it. |
| CTRL-17 | `handshake_timeout_secs` | optional u32 | unset | 2 – 60 | tokio timeout around `Connecting` | **New**, off by default. Unset matches today: only the 150 s idle timeout bounds a handshake. |
| CTRL-18 | `max_incoming` | u32 | 65,536 | 16 – 65,536 | `ServerConfig::max_incoming` | Today's value (noq default; Iroh has no endpoint-level setter) |
| CTRL-19 | `incoming_buffer_bytes` | u64 | 10 MiB | 4,096 – 10 MiB | `incoming_buffer_size` | Today's value (noq default). 0-RTT is refused, so only Initial packets wait here. |
| CTRL-20 | `incoming_buffer_total_bytes` | u64 | 100 MiB | ≥ CTRL-19, ≤ 100 MiB | `incoming_buffer_size_total` | Today's value (noq default) |
| CTRL-21 | `max_pending_per_ip` | optional u32 | unset | 1 – 64 | acceptor (ADM-3) | **New**, off by default |
| CTRL-22 | `retry_threshold` | optional u32 | unset | 0 – CTRL-18 | acceptor (ADM-3); 0 = always Retry unvalidated sources | **New**, off by default: the acceptor never sends Retry |
| CTRL-23 | `dial_stagger_ms` | u32 | 250 | 0 – 5,000 | dialer (DIAL-3) | **New** |
| CTRL-24 | `kernel_drop_poll_secs` | u32 | 10 | 1 – 300 | SOCK-7 | **New**, Linux only |
| CTRL-25 | `qlog_dir` | optional path | unset | — | `qlog_from_path` | Only with the `qlog` cargo feature; startup MUST fail if set without it |

These keys stay in `[network.zakura]` and keep their meaning:
`listen_addr`, `max_connections` (256), `max_connections_per_ip` (16),
`max_pending_handshakes` (32), `stream_open_rate_per_second`,
`message_rate_per_second` and the handshake's `max_open_streams`.

No release compiles both backends. There is no `transport` switch: the release
that adds the direct backend deletes the Iroh backend (PLAN step 10).

`[network.zakura] nat_traversal = true` MUST fail at startup. The error MUST say that hole punching was removed and name this spec.

### 9.2 Fixed settings

These are not configurable. Changing one needs a spec change.

- **CTRL-30.** Multipath path budget: 8 (WIRE-3).
- **CTRL-31.** Loss detection (`packet_threshold`, `time_threshold`,
  `initial_rtt`, `persistent_congestion_threshold`) and `max_ack_delay` stay at
  noq defaults.
- **CTRL-32.** `send_fairness` stays at noq's default (on).

### 9.3 Stream priority

- **CTRL-40.** `zakura-network` MAY call `SendStream::set_priority`. Block
  announcements and control messages SHOULD run at a higher priority than bulk
  block-sync bodies on the same connection. The P1 gate is block-propagation
  p99 under concurrent sync.

## 10. Paths and migration

- **PATH-1.** The endpoint MUST subscribe to `Connection::path_events()` for
  every connection.
- **PATH-2.** On `PathEvent::Established` for a path other than path 0, the
  endpoint MUST read `Path::remote_address()`. If the IP is banned, it MUST call
  `Path::close()`.
  - If `close` returns `LastOpenPath`, the endpoint MUST close the connection.
  - Peer-opened paths from IPs that aren't banned stay open.
- **PATH-3.** A migration of path 0 to a new address MUST get the same ban check
  as PATH-2. noq 1.2 emits no event when path 0 migrates. The endpoint therefore
  MUST compare path 0's `remote_address()` with the last seen value at each
  10 s stats sample (OBS-3). If the fork adds a migration event, the endpoint
  SHOULD use it. (Version 0.1 also required the check before each stream
  hand-off. Version 0.2 drops it: Zakura has no ban list for the check to
  consult, and the 10 s sample bounds the window once one exists.)
- **PATH-4.** Paths and migrations MUST NOT change the admitted IP. Per-IP
  accounting (ADM-7, ADM-8) uses only the admitted IP.
- **PATH-5.** The endpoint MUST count opened and closed peer paths (OBS-1).
- **PATH-6.** Only a peer that completed the handshake can open a path, because
  path packets use the connection's keys. PATH-2 therefore guards against ban
  evasion, not against unauthenticated traffic.

## 11. Observability

### 11.1 Metrics

- **OBS-1.** The endpoint MUST export these metrics through the `metrics` crate:

| Name | Kind | Meaning |
| --- | --- | --- |
| `zakura.quic.socket.recv_buffer_bytes` | gauge | Effective `SO_RCVBUF` (SOCK-3) |
| `zakura.quic.socket.send_buffer_bytes` | gauge | Effective `SO_SNDBUF` |
| `zakura.quic.socket.kernel_drops` | counter | `/proc/net/udp` drops for this socket (SOCK-7) |
| `zakura.quic.socket.rebinds` | counter | SOCK-9 rebinds |
| `zakura.quic.incoming.accepted` / `.refused` / `.retried` / `.ignored` | counter | ADM-2 outcomes |
| `zakura.quic.handshake.completed` / `.failed` / `.timed_out` | counter | Handshake results |
| `zakura.quic.handshake.duration_seconds` | histogram | Accept or dial to handshake complete |
| `zakura.quic.connections` | gauge | Open connections |
| `zakura.quic.dial.attempts` / `.alpn_mismatch` | counter | DIAL-3, DIAL-5 |
| `zakura.quic.paths.peer_opened` / `.closed_banned` | counter | PATH-5 |
| `zakura.quic.packets.lost` | counter | Sum of `PathStats::lost_packets` deltas |
| `zakura.quic.congestion_events` | counter | Sum of `PathStats::congestion_events` deltas |
| `zakura.quic.bytes.sent` / `.received` | counter | UDP payload bytes |
| `zakura.quic.path.rtt_seconds` | histogram | Sampled every 10 s per open path |
| `zakura.quic.path.cwnd_bytes` | histogram | Sampled every 10 s per open path |

- **OBS-2.** Metrics MUST NOT carry per-peer labels. Per-connection detail goes
  to OBS-3.
- **OBS-3.** When `[network.zakura] trace_dir` is set, Zakura MUST write one
  `quic_conn` JSONL row per connection every 10 s and on close. The endpoint
  delivers each sample to a `ConnObserver` callback, because `zakura-quic`
  doesn't depend on Zakura's trace crate. Each row holds `event` (`sample` or
  `closed`), `peer`, `admitted_ip`, `rtt_micros`, `cwnd`, `lost_packets`,
  `congestion_events`, `bytes_sent`, `bytes_received`, `current_mtu`,
  `paths_open` and `close_reason`. `peer` is the hashed peer label the other
  tables use, so rows join with `conn.jsonl`. `admitted_ip` follows
  `expose_peer_addresses`: it is `v4redacted` or `v6redacted` unless that is set.
- **OBS-4.** `Conn::stats()` MUST return noq's `ConnectionStats` plus
  per-path `PathStats`.

### 11.2 Logs

- **OBS-10.** Logs use the target `zakura_quic`. Startup MUST log once at
  `info`: bound addresses, effective buffers, congestion controller and ALPNs.
- **OBS-11.** Per-packet and per-`Incoming` events log at `trace` only.

## 12. API

The public surface of `zakura-quic`. Changing it is a semver change of the crate.

- **API-1.** Identity: `NodeId`, `NodeSecretKey` and `NodeAddr { id, direct: Vec<SocketAddr> }`.
- **API-2.** `QuicEndpoint`:
  - `bind(secret, &QuicBindConfig, &QuicConfig) -> Result<QuicEndpoint, BindError>`;
  - `local_id()`, `local_addrs()`, `socket_stats()` and `config()`;
  - `set_conn_observer(ConnObserver)` for OBS-3;
  - `connect(NodeAddr, alpn: &[u8]) -> Result<Conn, ConnectError>`;
  - `serve(impl Acceptor) -> Result<(), BindError>`, which spawns the accept loops;
  - `shutdown(&self)`. `QuicEndpoint` is a cheap clone.
- **API-3.** `trait Acceptor: Send + Sync + 'static`:
  - `fn admit(&self, incoming: &IncomingInfo) -> Admit`;
  - `fn alpns(&self) -> Vec<Vec<u8>>`, in preference order;
  - `fn handle(&self, conn: Conn) -> BoxFuture<'static, ()>`; `Conn::alpn()`
    carries the negotiated ALPN;
  - `fn is_banned(&self, ip: IpAddr) -> bool`, default `false`, for PATH-2 and
    PATH-3.
- **API-4.** `Conn`:
  - `remote_id()`, `admitted_ip()` and `alpn()`;
  - `open_bi()`, `accept_bi()`, `close(code, reason)`, `closed()` and `stats()`.
  Dropping the last `Conn` handle closes the connection, as today.
- **API-5.** Streams are `noq::SendStream` and `noq::RecvStream`, re-exported
  with `VarInt`, `ReadError`, `WriteError` and `ClosedStream`.
- **API-6.** Errors: `ConnectError` has the variants `SelfDial`,
  `NoUsableAddress`, `AlpnMismatch`, `HandshakeTimeout`, `Refused`, `WrongIdentity`,
  `Endpoint(noq::ConnectError)`, `Tls` and `Transport(noq::ConnectionError)`.
  `Endpoint` covers local endpoint failures such as a stopping endpoint; `Tls`
  covers other TLS alerts. `BindError` and `ConfigError` name the failing key
  or address.
- **API-7.** `shutdown` MUST:
  1. stop accepting;
  2. close every connection with application code 0;
  3. wait on `Endpoint::wait_idle` for at most 3 s;
  4. drop the sockets.
- **API-8.** The testkit's `LocalEndpointFactory` MUST build `zakura-quic`
  endpoints bound to `127.0.0.1:0` with production settings, except where a test
  overrides a `QuicConfig` field.

## 13. Compatibility

Iroh compatibility is a goal, not a requirement. COMPAT-1 to COMPAT-3 decide the
ALPN. When they pass, the direct backend stays on `p2p-v2/2`. When one fails and
the fix isn't simple, the direct backend moves to `p2p-v2/3` under COMPAT-4,
and the legacy stack bridges old and new `p2p_stack = "dual"` nodes (COMPAT-7).

- **COMPAT-1.** A direct-backend node SHOULD interoperate with an Iroh-backend
  Zakura node (the previous release) on `p2p-v2/2`:
  - in both dial directions;
  - for 1 MiB and 64 MiB bidirectional transfers;
  - across a `kill -9` and restart of either side;
  - with the Iroh side opening extra paths.
- **COMPAT-2.** The direct backend SHOULD also interoperate at the transport level
  with the newest released Iroh 1.x (relay off, direct address given) on a test
  ALPN. The test covers: handshake in both directions, `remote_id` on both
  sides, a 64 MiB echo, and an Iroh-opened second path.
- **COMPAT-3.** `tools/iroh-interop/` runs COMPAT-1 and COMPAT-2 on every noq
  bump, on every `zakura-quic` PR that touches `tls/`, `config.rs` or the wire
  profile, and weekly. While the direct backend is on `p2p-v2/2`, a failure
  blocks the change.
- **COMPAT-4.** A wire break (a failed COMPAT-1, WIRE-13, a moved noq codepoint,
  or an incompatible Iroh release) MUST bump the ALPN to the next `p2p-v2/N`.
  The endpoint MUST NOT advertise an ALPN whose wire profile it doesn't match.
- **COMPAT-5.** QUIC version numbers other than v1 are reserved for a future
  Noise handshake. That backend MUST NOT change the v1 TLS profile.
- **COMPAT-6.** noq's n0 NAT-traversal frames and transport parameter MUST stay
  compiled in. A later hole-punching module will use them.

- **COMPAT-7.** After an ALPN bump, a `p2p_stack = "dual"` node MUST fall back
  to legacy for a peer on the other ALPN without a stall: the native dial
  fails within the handshake bound with an ALPN error, and DIAL-5 backs off.
  `p2p_stack = "zakura"` nodes lose peers on the other ALPN until they upgrade.

## 14. Security

- **SEC-1.** The TLS and key code MUST pass these known-answer tests:
  - RFC 8032 vectors;
  - small-order public keys and small-order `R` points refused;
  - non-canonical `S` scalars refused;
  - the Dalek compatibility vectors from `iroh-base/tests/dalek_compat.rs`.
- **SEC-2.** These handshakes MUST fail:
  - a dial that reaches a server with the wrong `NodeId`;
  - a client without a key;
  - TLS 1.2;
  - an X.509 certificate instead of a raw public key;
  - a signature scheme other than Ed25519.
- **SEC-3.** For a refused or ignored IP, no `Connecting` may exist. A test
  asserts that noq created no connection state.
- **SEC-4.** A flood of Initials that never complete MUST stay bounded by
  `max_incoming`, `incoming_buffer_total_bytes` and `handshake_timeout_secs`.
  The test MUST report RSS growth at 10,000 Initials per second, both at
  today's values and with every limit in §9.1 set. That measurement decides
  whether a later release tightens the defaults (§9.1).
- **SEC-5.** A test MUST show that a peer-opened path from a banned IP closes
  (PATH-2), and that the admitted IP stays the same.
- **SEC-6.** No early data reaches noq buffers (TLS-10).
- **SEC-7.** The fork's regression tests for NOQ-1 MUST pass on every bump.
- **SEC-8.** A change to `tls/`, `key/`, `ADM-*` or `PATH-*` needs approval from
  both transport owners (PLAN §9). An AI-assisted review counts as one reviewer at most.

## 14a. Platforms

- **PLAT-1.** `zakura-quic` MUST build and pass its tests on Linux x86_64,
  Linux aarch64 and macOS aarch64 in CI. macOS is a supported platform for
  native nodes.
- **PLAT-2.** Linux-only features (SOCK-7 kernel drop counter) MUST compile out
  on other platforms. Their metrics are absent there.

## 15. noq fork

- **NOQ-1.** Before the direct backend ships, the fork MUST carry:
  - the quinn assembler bound for gapped STREAM and CRYPTO data (GHSA-4w2j, with the
    qfwj bypass fix);
  - the `retire_cids` cap (GHSA-hmxj);
  - the oversized Retry token check (wppq).
  - the `recv` spin fix for macOS and BSD (GHSA-6pp4).
- **NOQ-2.** The fork MUST carry trims 1 and 2, each in its own PR. It MAY
  carry trim 3:
  1. the Retry integrity tag on ring's AES-128-GCM, removing aes-gcm, ctr,
     ghash and polyval;
  2. a hand-written `FrameType::{to_u64, from_u64}` with a round-trip test over
     all 44 frame types, removing enum-assoc and syn 3;
  3. derive_more replaced by hand-written impls, only after an upstream PR is
     open.
- **NOQ-3.** Security patches (NOQ-1) MUST stay under 500 lines of fork delta in
  total. Each patch MUST also go upstream to `n0-computer/noq`. Each patch MUST
  be dropped when upstream merges an equivalent.
- **NOQ-4.** The fork MUST NOT carry Zakura features. Features belong in
  `zakura-quic`.
- **NOQ-5.** A fork release MUST NOT change any wire codepoint relative to its
  upstream base, except by a separate decision under COMPAT-4.

## 16. Conformance tests

| Test | Covers |
| --- | --- |
| `cargo tree` denylist in CI | DEP-1, DEP-5 |
| Identity known-answer and key-file round trip | ID-1–ID-7, SEC-1 |
| TLS refusal matrix | TLS-1–TLS-8, SEC-2 |
| Transport-parameter snapshot: decode the client and server parameters and compare each value with the Iroh backend's, ignoring connection IDs, the stateless reset token, noq's random grease parameter and the shuffled parameter order | WIRE-3–WIRE-10 |
| Buffer read-back and clamp warning: bind under a lowered `rmem_max` in a user namespace | SOCK-3–SOCK-5 |
| Busy-socket loopback run: zero `lost_packets` caused by the sender, bidirectional, 64 MiB | SOCK-6, G7 |
| `/proc/net/udp` drop counter with a deliberately slow receiver | SOCK-7 |
| Admission ordering, refuse, ignore, retry, timeout | ADM-1–ADM-7, SEC-3, SEC-4 |
| Happy-eyeballs dial with mixed v4/v6 addresses and one black-holed address | DIAL-1–DIAL-6 |
| Path ban and admitted-IP invariance | PATH-1–PATH-5, SEC-5 |
| Config round trip, defaults, range errors, `nat_traversal` error | CTRL-0–CTRL-25 |
| Congestion controller is Cubic when unset, checked through `Connection::congestion_state` | CTRL-3 |
| Metrics presence and no per-peer labels | OBS-1, OBS-2 |
| Shutdown order and the 3 s bound | API-7 |
| `tools/iroh-interop` matrix | COMPAT-1–COMPAT-3 |
| Fork regression tests (gapped frames, retired-CID flood, oversized token) | NOQ-1, SEC-7 |
| The 239 focused `zakura-network` tests and the full workspace suite with Iroh deleted | No regression |
| Netem matrix (PLAN §7.3): the swap branch matches or beats the current release in every cell beyond noise | PLAN §7.4 |
| Dual-stack node falls back to legacy against the other ALPN (only after a bump) | COMPAT-7 |

## 17. Feature priority

Every requirement above is P0 except these:

- **P1** (after the default flips, each gated by a measurement): changing the
  CTRL-3 default, CTRL-8, CTRL-9, CTRL-40 and the CTRL-25 qlog feature. The
  keys and their mappings ship in P0; only their use waits for the measurement.
- **P2** (on demand only): several `SO_REUSEPORT` sockets, rebinding on network
  change beyond SOCK-9, extra paths opened by the dialer (DIAL-7), hole
  punching (COMPAT-6) and Noise (COMPAT-5).

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
