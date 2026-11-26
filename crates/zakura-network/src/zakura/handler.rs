//! Zakura P2P v2 endpoint, protocol handler, and bounded connection serving.

mod service_session;
mod trace;
use service_session::{
    may_open_ordered_stream, spawn_service_session, trace_session_step, ConnectionRoles,
    DemandChange, DemandWait, OpenedStream, PendingSessions, PreparedStream, RemoteCompletion,
    RemoteDemand, ServiceSessionState, SessionExit, SetupIo,
};
#[cfg(any(test, feature = "p2p-fuzz"))]
pub use service_session::{replay_service_session_bytes, ServiceSessionReplaySummary};

use std::{
    collections::BTreeMap,
    future,
    io::{Cursor, Read},
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use bytes::Bytes;
use futures::{future::BoxFuture, stream::FuturesUnordered, FutureExt as _, StreamExt as _};
use rand_10::Rng;
use thiserror::Error;
use zakura_chain::{
    block::{self, Block, CountedHeader},
    parameters::{Network, NetworkKind},
    serialization::{
        CompactSizeMessage, ZcashDeserialize, MAX_HEADERS_PER_MESSAGE, MAX_PROTOCOL_MESSAGE_LEN,
    },
    transaction::Transaction,
};
use zakura_node_services::task::{erased, spawn_erased_named};
use zakura_quic::{
    Acceptor, Admit, Conn as Connection, IncomingInfo, NodeAddr, NodeId, NodeSecretKey,
    QuicBindConfig, QuicConfig, QuicEndpoint, RecvStream, SendStream, VarInt,
};
use zakura_rt::{
    collections::{HashMap, HashSet},
    stream::StreamMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        broadcast, mpsc, oneshot, watch, AsyncMutex, Barrier, CancellationToken, Mutex as StdMutex,
        MutexGuard, OwnedSemaphorePermit, PoisonError, Semaphore,
    },
    task::{AbortOnDropHandle, JoinHandle, JoinSet, JoinSetExt},
    time::{timeout, Instant},
};

use self::trace::ZakuraConnTrace;
use super::discovery::{
    self, native_dial_supervised, spawn_native_bootstrap_dialer, RedialEnd, RedialPolicy,
};
use super::legacy_gossip::LegacyResponseItem;
use super::peer_memory::{
    budgeted_quic_bytes, ConnectionMemory, ZAKURA_CLOSING_CONNECTIONS_PER_LIVE,
};
use super::regulation::{parks_on_exhaustion, CadenceBuckets, CadenceCharge, PrecheckSlot};
use super::trace::{reject_reason_label, ZakuraTrace};
use super::transport::{
    check_reactor_shares, frame_wire_bytes, sim_arg, sim_chain, transport_observe,
    worker_framed_channel, AdmittedFrame, CloseOutcome, CloseRow, CompatResponseGrant,
    ConnectionPlaces, ConnectionResources, FrameGate, FrameGrant, FramedWorkerRecv,
    HandshakeCounts, HeldPlaces, IpAdmission, MovedNodeIds, NeutralCause, NodeResources,
    OrderedStreamFailure, OrderedStreamFailureCause, OwnerBound, OwnerGuard, OwnerPhase,
    ProvenNode, ReactorBudget, ReactorShares, ReceiveBudget, RegisteredPeers, ResourceEnvelope,
    SessionLayout, SlotClass, SlotRefusal, StreamResources, TransportLimits,
    COMPAT_EXCHANGE_BUDGET, COMPAT_RESPONSE_MAX_AGGREGATE_BYTES,
};
#[cfg(any(test, feature = "zakura-testkit"))]
use crate::zakura::drive_header_sync_actions;
#[cfg(any(test, feature = "zakura-testkit"))]
use crate::zakura::HeaderSyncAction;
use crate::{
    constants::MAX_FIND_BLOCKS_RESPONSE_HASHES, peer_registry::PeerRegistry, BoxError, Config,
    MAX_TX_INV_IN_SENT_MESSAGE,
};
use crate::{
    protocol::external::InventoryHash,
    zakura::{
        admission_host, canonical_ip, frame_cap_carries_payload, spawn_block_sync_reactor,
        spawn_header_sync_reactor, supported_p2p_v2_alpns, zakura_protocol_for_alpn,
        AuthenticatedPeerRegistration, BlockSyncAction, BlockSyncFrontiers, BlockSyncHandle,
        BlockSyncService, BlockSyncStartup, BoxRunFuture, Clock, CloseCause, Frame, FrameFilter,
        FrameRejection, FramedRecv, FramedSend, FullStateFrontiers, HeaderSyncPassthroughService,
        HeaderSyncService, HeaderSyncStartup, InboundReader, MessageRule, Peer, RealClock, Service,
        ServicePeerDirection, ServiceRegistry, ServiceStream, SessionDemand, SessionStep,
        SinkReject, Stream, StreamMode, StreamPrelude, StreamQueueDepths, StreamWritePolicy,
        ZakuraAcceptedLimits, ZakuraBlockSyncConfig, ZakuraConnId, ZakuraControlAck,
        ZakuraControlHello, ZakuraControlRole, ZakuraControlValidation, ZakuraHandshakeConfig,
        ZakuraHandshakePath, ZakuraHeaderSyncConfig, ZakuraInitialLimits, ZakuraLimits,
        ZakuraPeerId, ZakuraPeerSupervisor, ZakuraProtocolError, ZakuraRejectReason,
        ZakuraServiceId, ZakuraUpgradeDialStart, ZakuraValidationError, CONTROL_ACK_MAGIC,
        CONTROL_HELLO_MAGIC, CONTROL_VERSION, FRAME_HEADER_BYTES, MAX_CONTROL_PAYLOAD_BYTES,
        STREAM_PRELUDE_MAGIC, TRANSCRIPT_HASH_BYTES, ZAKURA_CAP_HEADER_SYNC,
        ZAKURA_HEADER_SYNC_STREAM_VERSION, ZAKURA_REGULATED_BLOCK_SYNC_STREAM_VERSION,
        ZAKURA_STREAM_BLOCK_SYNC, ZAKURA_STREAM_HEADER_SYNC,
    },
};

// A legacy upgrade hand-off dials at most `MAX_IROH_DIRECT_ADDRESSES`, so the
// QUIC dial reserve that fits one discovery record fits one hand-off too (see
// `ZakuraLocalLimits::quic_bind_config`).
const _: () = assert!(super::MAX_IROH_DIRECT_ADDRESSES <= discovery::MAX_DIRECT_ADDRS_PER_RECORD);

/// Default total Zakura connections when P2P v2 is enabled: 92, the count
/// the previous 8 GiB budget admitted at the default windows. The default peer
/// memory budget funds this many at every reactor's share (see `peer_memory`).
pub const DEFAULT_ZAKURA_MAX_CONNECTIONS: usize = 92;
/// Default per-IP Zakura connection cap.
pub const DEFAULT_ZAKURA_MAX_CONNS_PER_IP: usize = 16;
/// Default simultaneous Zakura control handshakes.
pub const DEFAULT_ZAKURA_MAX_PENDING_HANDSHAKES: usize = 32;
/// Default stream-open churn per connection.
pub const DEFAULT_ZAKURA_STREAM_OPEN_RATE_PER_SECOND: u32 = 32;
/// Per-kind inbound message rate per connection.
///
/// This is a generous universal cap: block-sync legitimately delivers
/// hundreds of solicited bodies per second in bursts, so a low limit
/// starves sync. Exceeding it is a transport-level hard failure rather than a
/// peer-scoring decision: we never silently drop a solicited frame because a
/// dropped block body is a permanent gap on a reliable stream. Longer term
/// this should be split per message type (some unbounded, some near-one-shot)
/// rather than a single universal value.
pub const DEFAULT_ZAKURA_MESSAGE_RATE_PER_SECOND: u32 = 2048;
/// Default native Zakura QUIC listen address.
pub const DEFAULT_ZAKURA_LISTEN_ADDR: SocketAddr =
    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 8234));
/// Default native Zakura bootstrap peers for Mainnet.
///
/// Entries name each seed by its protocol 5 node ID, which derives from keys
/// only the seed holds. Take a new ID from the line the seed logs at startup;
/// a stale ID fails the dial's identity check.
pub const DEFAULT_MAINNET_ZAKURA_BOOTSTRAP_PEERS: &[&str] = &[
    "078a60720f0d474743a51674b152f49f575704a77905c16475683cff9d18f246@165.22.54.66:8234",
    "1bb8f2f70bdbf9d44ef2ccc3de1a5cbbba56cea2cf371e227d501ce19a8bdeb9@104.131.184.123:8234",
    "a98d73e0114f9f8ae3d5a0b4ec0b8cef20b51d715de6898a39fc3f23948196b7@159.65.183.89:8234",
    "02ef9b05d81ad2f6176a68fd047bda5e4e277d69ac51b3b2536d43ea8f38de1c@143.244.184.176:8234",
    "0018a0f4fc1f3cc3b4a84e8ebcb77008343c9e54fb80f77fd03271c7a33063b7@159.203.38.10:8234",
    "4607c5353e2c07ecd10d73c6c19d0b0de092939328f33e8f04d5ced382fadf7b@64.227.44.93:8234",
    "8ac35544d02141e7a3cc3c8b8661a3e726985a266673b60cf45f5b33c3f36e73@161.35.156.226:8234",
    "42623a00c2632e0cf99d1c10cec024b0217d12d5fecc91947a3a040f142df06b@139.59.64.115:8234",
    "ba06d75332e3a430d4e838b5838a2356338124cf2930bb0f8e2962e01223ed95@168.144.173.250:8234",
];
/// Default native Zakura bootstrap peers for Testnet.
pub const DEFAULT_TESTNET_ZAKURA_BOOTSTRAP_PEERS: &[&str] = &[
    "884eb8c4e0d6005bd3a6e086df064efc12b866632dababa4249b2ceb42d43982@167.99.103.111:8234",
    "1d0dd618bb699637322dcbf20a0830fe9078733ba13c3ee01768bfb518653064@164.92.209.78:8234",
    "b5410aedf93d00bbc9f93405a4c054b30289da9b76269432a5e6e0e132f0480c@206.189.148.0:8234",
];
/// Default native Zakura bootstrap peers.
pub const DEFAULT_ZAKURA_BOOTSTRAP_PEERS: &[&str] = DEFAULT_MAINNET_ZAKURA_BOOTSTRAP_PEERS;
/// Default maximum bytes read before the peer's stream prelude is decoded.
pub const DEFAULT_ZAKURA_PRELUDE_TIMEOUT: Duration = Duration::from_secs(3);
/// Default timeout for one control-handshake read or write.
pub const DEFAULT_ZAKURA_CONTROL_TIMEOUT: Duration = Duration::from_secs(10);
/// QUIC idle timeout used by Zakura endpoints.
///
/// This also bounds the application-level idle reaper that closes a connection
/// after a quiet period. A gossip connection is legitimately quiet between
/// blocks (minutes apart on mainnet), so a short timeout would tear down healthy
/// peers and force constant re-dials; the keepalive interval keeps the transport
/// live well within this window.
pub const DEFAULT_ZAKURA_QUIC_IDLE_TIMEOUT: Duration = Duration::from_secs(150);
/// QUIC keepalive interval used by Zakura endpoints.
pub const DEFAULT_ZAKURA_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(10);
/// Minimum age of an incumbent Zakura connection before a duplicate connection
/// for the same identity is allowed to evict it.
///
/// Duplicates can be ordinary redial races while the incumbent is healthy and
/// actively serving block sync. Evicting those incumbents drops in-flight body
/// ranges and makes a fresh sync repeatedly re-download the same windows. Keep
/// the incumbent through the transport idle timeout; genuinely dead connections
/// are reaped by QUIC idle cleanup, and duplicate redials after that point can
/// reclaim the slot without disturbing active transfers.
pub const ZAKURA_DUPLICATE_EVICT_MIN_AGE: Duration = Duration::from_secs(300);
/// Minimum incumbent age before a *same-IP* duplicate may evict the incumbent.
///
/// A duplicate arriving from the same remote IP as the incumbent is almost
/// always a restarted or resyncing peer redialing from its stable address,
/// whose previous connection lingers as a dead incumbent until QUIC idle
/// cleanup (~150s) reaps it. Waiting out [`ZAKURA_DUPLICATE_EVICT_MIN_AGE`]
/// locks that peer out of block sync for that whole window. Use a much smaller
/// gate for same-IP redials so a restarted peer recovers in seconds, while
/// still keeping it comfortably above worst-case simultaneous-open race
/// resolution (milliseconds) so a genuine race keeps the transcript-tiebreak
/// winner instead of flapping.
pub const ZAKURA_SAME_IP_DUPLICATE_EVICT_MIN_AGE: Duration = Duration::from_secs(5);
/// Minimum QUIC receive or send window allowed by `QuicConfig::validate` (64 KiB).
pub const MIN_QUIC_WINDOW_BYTES: u64 = 64 * 1024;

/// Control stream rules for the handshake (`ControlHello` + `ControlAck`),
/// each bounded by [`MAX_CONTROL_PAYLOAD_BYTES`] plus its 4-byte length prefix.
const CONTROL_FRAME_CAP_BYTES: usize = MAX_CONTROL_PAYLOAD_BYTES + CONTROL_LENGTH_BYTES;
const CONTROL_RULES: [super::MessageRule; 2] = [
    super::MessageRule {
        name: "control-hello",
        message_type: 1,
        payload: super::PayloadLen::between(1, CONTROL_FRAME_CAP_BYTES),
        role: super::MessageRole::Request {
            max_in_flight: 1,
            cadence: None,
        },
    },
    super::MessageRule {
        name: "control-ack",
        message_type: 2,
        payload: super::PayloadLen::between(1, CONTROL_FRAME_CAP_BYTES),
        role: super::MessageRole::Response {
            request: 1,
            ends_exchange: true,
        },
    },
];

/// Control stream layout used to derive [`CONTROL_RECEIVE_BYTES`] through
/// [`Stream::receive_bound`].
const CONTROL_STREAM_LAYOUT: [Stream; 1] = [Stream {
    kind: 0,
    version: 1,
    frame_cap: (CONTROL_FRAME_CAP_BYTES + FRAME_HEADER_BYTES) as u32,
    capability: 0,
    messages: Some(&CONTROL_RULES),
    ..Stream::PERSISTENT
}];

const _: () = Stream::validate_layout(&CONTROL_STREAM_LAYOUT);

/// Control stream receive bound derived directly from [`CONTROL_STREAM_LAYOUT`]
/// via [`Stream::receive_bound`] (`32,776 B`).
pub const DERIVED_CONTROL_RECEIVE_BYTES: u64 =
    match Stream::receive_bound(&CONTROL_STREAM_LAYOUT, super::LocalRole::Both) {
        Ok(bytes) => bytes,
        Err(error) => panic!("{}", error.rule()),
    };

/// Control share of a connection's receive window: [`DERIVED_CONTROL_RECEIVE_BYTES`]
/// floored to QUIC's [`MIN_QUIC_WINDOW_BYTES`] (`64 KiB`), replacing the 32 MiB
/// `DEFAULT_ZAKURA_RECEIVE_WINDOW` for Zakura connections.
pub const CONTROL_RECEIVE_BYTES: u64 = if DERIVED_CONTROL_RECEIVE_BYTES > MIN_QUIC_WINDOW_BYTES {
    DERIVED_CONTROL_RECEIVE_BYTES
} else {
    MIN_QUIC_WINDOW_BYTES
};

/// Send window held by an unauthenticated pending handshake (`64 KiB`).
///
/// Before authentication finishes, only `ControlHello` / `ControlAck` (capped at
/// 16 KiB) are sent; `serve_connection` raises the send window once the peer is
/// admitted.
pub const HANDSHAKE_SEND_WINDOW_BYTES: u64 = MIN_QUIC_WINDOW_BYTES;

/// Base send window for an admitted connection: the largest native frame
/// (`MAX_BS_FRAME_BYTES`), which also holds every part of one largest BPv1
/// block ([`super::bpv1::block_push_bytes`]), so propagation never needs to
/// lease window growth to send one block.
pub const BASE_SEND_WINDOW_BYTES: u64 = super::MAX_BS_FRAME_BYTES as u64;

/// A paused stream may consume at most half the connection receive window,
/// leaving credit for another service stream.
pub const DEFAULT_ZAKURA_STREAM_RECEIVE_WINDOW: u32 = 16 * 1024 * 1024;
/// Default `QuicConfig` connection receive window (32 MiB). Zakura connections
/// replace this with [`CONTROL_RECEIVE_BYTES`] unless explicitly overridden.
pub const DEFAULT_ZAKURA_RECEIVE_WINDOW: u32 = 32 * 1024 * 1024;
/// QUIC send window used by Zakura endpoints after admission.
pub const DEFAULT_ZAKURA_SEND_WINDOW: u64 = 32 * 1024 * 1024;
/// Minimum wait before redialing a peer that answered with another ALPN, so a
/// node doesn't redial another protocol cohort in a loop (zakura-quic DIAL-5).
pub const ZAKURA_ALPN_MISMATCH_BACKOFF: Duration = Duration::from_secs(10 * 60);
/// Startup error for `[network.zakura] nat_traversal = true`.
pub const ZAKURA_NAT_TRAVERSAL_REMOVED: &str =
    "zakura.nat_traversal = true is no longer supported: the zakura-quic transport removed \
     QUIC hole punching (see docs/specs/zakura-quic.md, section 9.1); set it to false or remove it";
/// Initial backoff before re-dialing a configured Zakura bootstrap peer.
pub const DEFAULT_ZAKURA_REDIAL_INITIAL_BACKOFF: Duration = Duration::from_secs(1);
/// Maximum backoff between re-dials of a configured Zakura bootstrap peer.
pub const DEFAULT_ZAKURA_REDIAL_MAX_BACKOFF: Duration = Duration::from_secs(30);
/// Consecutive failed dials after which a legacy->Zakura upgrade dial gives up.
///
/// The upgrade dial targets a peer-supplied address, so it must not redial
/// forever. Six attempts back off 1 + 2 + 4 + 8 + 16 = 31 s between dials, and
/// each dial can wait up to [`DEFAULT_ZAKURA_CONTROL_TIMEOUT`] to connect, so
/// the dial ends within about 90 s after the peer stops answering. An ALPN
/// mismatch ends it at once. The legacy crawler then re-upgrades the peer if it
/// is still reachable.
const ZAKURA_UPGRADE_REDIAL_MAX_ATTEMPTS: usize = 6;
const CONTROL_LENGTH_BYTES: usize = 4;
const STREAM_PRELUDE_FIXED_BYTES: usize = 4 + 2 + 2 + 1;
const STREAM_PRELUDE_REQUEST_ID_FLAG_OFFSET: usize = STREAM_PRELUDE_FIXED_BYTES - 1;
const STREAM_PRELUDE_REQUEST_ID_BYTES: usize = 8;
const STREAM_PRELUDE_CAP_BYTES: usize = 4;
const STREAM_WORKER_DRAIN_TIMEOUT: Duration = Duration::from_secs(1);

/// How often the endpoint reports each stream kind's held and gated bytes.
#[cfg(not(zakura_sim))]
const TRANSPORT_USAGE_REPORT_INTERVAL: Duration = Duration::from_secs(10);
const OUTBOUND_STREAM_WRITE_TIMEOUT: Duration = Duration::from_secs(10);
const OUTBOUND_REQUEST_RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);
// Mirrors the legacy gossip compatibility protocol. Compile-time assertions
// below keep this transport-side budget validator pinned to the codec constants.
const LEGACY_GOSSIP_STREAM_KIND: u16 = 2;
const LEGACY_REQUEST_STREAM_KIND: u16 = 3;
const DISCOVERY_STREAM_KIND: u16 = 4;
const HEADER_SYNC_STREAM_KIND: u16 = ZAKURA_STREAM_HEADER_SYNC;
const LEGACY_REQUEST_BLOCKS_BY_HASH: u16 = 3;
const LEGACY_REQUEST_TRANSACTIONS_BY_ID: u16 = 4;
const LEGACY_RESPONSE_BLOCK: u16 = 5;
const LEGACY_RESPONSE_TRANSACTION: u16 = 6;
const LEGACY_RESPONSE_MISSING_BLOCKS: u16 = 7;
const LEGACY_RESPONSE_MISSING_TRANSACTIONS: u16 = 8;
const LEGACY_REQUEST_FIND_BLOCKS: u16 = 9;
const LEGACY_REQUEST_FIND_HEADERS: u16 = 10;
const LEGACY_REQUEST_MEMPOOL_TRANSACTION_IDS: u16 = 11;
const LEGACY_REQUEST_PING: u16 = 12;
const LEGACY_REQUEST_PUSH_TRANSACTION: u16 = 13;
const LEGACY_RESPONSE_BLOCK_HASHES: u16 = 14;
const LEGACY_RESPONSE_BLOCK_HEADERS: u16 = 15;
const LEGACY_RESPONSE_TRANSACTION_IDS: u16 = 16;
const LEGACY_RESPONSE_PONG: u16 = 17;
const LEGACY_RESPONSE_NIL: u16 = 18;
const LEGACY_RESPONSE_REQUEST_ID_BYTES: usize = 8;
const LEGACY_RESPONSE_CHUNK_HEADER_BYTES: usize = LEGACY_RESPONSE_REQUEST_ID_BYTES + 1;
const LEGACY_COMPACT_SIZE_PREFIX_BYTES: usize = 9;
const LEGACY_BLOCK_HASH_BYTES: usize = 32;
const LEGACY_INVENTORY_HASH_BYTES: usize = 36;
/// A witnessed transaction's inventory entry: a type code and two 32-byte hashes.
const LEGACY_WTX_INVENTORY_BYTES: usize = 68;
const LEGACY_RESPONSE_MAX_FRAMES_PER_ITEM: usize = 8;
const _: () = assert!(LEGACY_GOSSIP_STREAM_KIND == super::legacy_gossip::ZAKURA_STREAM_GOSSIP);
const _: () =
    assert!(LEGACY_REQUEST_STREAM_KIND == super::legacy_gossip::ZAKURA_STREAM_LEGACY_REQUESTS);
const _: () = assert!(DISCOVERY_STREAM_KIND == super::discovery::ZAKURA_STREAM_DISCOVERY);
const _: () = assert!(HEADER_SYNC_STREAM_KIND == super::header_sync::ZAKURA_STREAM_HEADER_SYNC);
const _: () = assert!(ZAKURA_HEADER_SYNC_STREAM_VERSION == 9);
const _: () =
    assert!(LEGACY_REQUEST_BLOCKS_BY_HASH == super::legacy_gossip::MSG_REQUEST_BLOCKS_BY_HASH);
const _: () = assert!(
    LEGACY_REQUEST_TRANSACTIONS_BY_ID == super::legacy_gossip::MSG_REQUEST_TRANSACTIONS_BY_ID
);
const _: () = assert!(LEGACY_RESPONSE_BLOCK == super::legacy_gossip::MSG_RESPONSE_BLOCK);
const _: () =
    assert!(LEGACY_RESPONSE_TRANSACTION == super::legacy_gossip::MSG_RESPONSE_TRANSACTION);
const _: () =
    assert!(LEGACY_RESPONSE_MISSING_BLOCKS == super::legacy_gossip::MSG_RESPONSE_MISSING_BLOCKS);
const _: () = assert!(
    LEGACY_RESPONSE_MISSING_TRANSACTIONS == super::legacy_gossip::MSG_RESPONSE_MISSING_TRANSACTIONS
);
const _: () = assert!(LEGACY_REQUEST_FIND_BLOCKS == super::legacy_gossip::MSG_REQUEST_FIND_BLOCKS);
const _: () =
    assert!(LEGACY_REQUEST_FIND_HEADERS == super::legacy_gossip::MSG_REQUEST_FIND_HEADERS);
const _: () = assert!(
    LEGACY_REQUEST_MEMPOOL_TRANSACTION_IDS
        == super::legacy_gossip::MSG_REQUEST_MEMPOOL_TRANSACTION_IDS
);
const _: () = assert!(LEGACY_REQUEST_PING == super::legacy_gossip::MSG_REQUEST_PING);
const _: () =
    assert!(LEGACY_REQUEST_PUSH_TRANSACTION == super::legacy_gossip::MSG_REQUEST_PUSH_TRANSACTION);
const _: () =
    assert!(LEGACY_RESPONSE_BLOCK_HASHES == super::legacy_gossip::MSG_RESPONSE_BLOCK_HASHES);
const _: () =
    assert!(LEGACY_RESPONSE_BLOCK_HEADERS == super::legacy_gossip::MSG_RESPONSE_BLOCK_HEADERS);
const _: () =
    assert!(LEGACY_RESPONSE_TRANSACTION_IDS == super::legacy_gossip::MSG_RESPONSE_TRANSACTION_IDS);
const _: () = assert!(LEGACY_RESPONSE_PONG == super::legacy_gossip::MSG_RESPONSE_PONG);
const _: () = assert!(LEGACY_RESPONSE_NIL == super::legacy_gossip::MSG_RESPONSE_NIL);
const ZAKURA_CLOSE_NEUTRAL: u32 = 0;
const ZAKURA_CLOSE_RESOURCE: u32 = 1;
const ZAKURA_CLOSE_BAD_PRELUDE: u32 = 2;
const ZAKURA_CLOSE_RATE_LIMIT: u32 = 3;
const ZAKURA_CLOSE_OVERSIZE: u32 = 4;
/// Stream reset code for a prelude naming an unknown stream kind or an
/// unsupported version of a known kind. Bounded, peer-visible, and distinct
/// from the malformed-prelude code so peers can tell a parse failure from an
/// unsupported-but-well-formed stream.
const ZAKURA_CLOSE_UNKNOWN_STREAM: u32 = 5;

/// Native Zakura endpoint and handler configuration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ZakuraConfig {
    /// Native Zakura bootstrap peers as `node_id@direct_addr`.
    ///
    /// Native bootstrap uses direct addresses only because relays and discovery
    /// are disabled for Zakura v1. Each entry must contain the peer's 32-byte
    /// node id and a directly reachable socket address.
    ///
    /// An entry can still name a default seed by its p2p-v2/3 node ID, which
    /// [`replace_retired_seed_node_ids`](Self::replace_retired_seed_node_ids)
    /// switches to the seed's current ID.
    pub bootstrap_peers: Vec<String>,
    /// Address the native Zakura QUIC endpoint binds to.
    ///
    /// Defaults to `0.0.0.0:8234`, giving the node a stable, advertisable
    /// Zakura endpoint so other nodes can list it in their
    /// [`bootstrap_peers`](Self::bootstrap_peers). If code constructs this as
    /// `None`, the endpoint binds an OS-assigned ephemeral port on loopback
    /// only (`127.0.0.1` and `::1`), which is fine for a node that only dials
    /// out and keeps the experimental native P2P_V2_ALPN surface off all
    /// non-loopback interfaces.
    pub listen_addr: Option<SocketAddr>,
    /// Removed: QUIC NAT traversal (hole punching).
    ///
    /// Only `false` is accepted. `true` fails at startup, because the
    /// `zakura-quic` transport has no hole punching (see
    /// `docs/specs/zakura-quic.md`). The key stays so existing configs parse.
    pub nat_traversal: bool,
    /// Native QUIC transport settings (`[network.zakura.quic]`).
    ///
    /// The defaults reproduce the previous transport's behavior. See
    /// `docs/specs/zakura-quic.md` section 9 for every key and its range.
    pub quic: QuicConfig,
    /// Optional cap on ordinary concurrent Zakura connections, inbound plus
    /// outbound (`N_cap`, `02 §8`). Up to ten preferred gateways may connect
    /// in addition, when `Consumer::Sessions` can fund them.
    ///
    /// When unset (`None`), defaults to `DEFAULT_SERVICE_MAX_PEERS` (256).
    /// Operators may raise or lower it; `[memory] budget` funds live sessions
    /// dynamically through `Consumer::Sessions`.
    /// Inbound peers leave a quarter of it, rounded down but at least one
    /// slot once it is 2 or more, free for this node's dials (see
    /// [`ZakuraLocalLimits::max_inbound_connections`]).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_connections: Option<usize>,
    /// Pre-resolved `[memory]` budget passed in from `zakurad` (`02 §2`,
    /// decision-2). Never serialized under `[network.zakura]`.
    #[serde(skip)]
    pub memory_budget: super::memory::ResolvedMemoryBudget,
    /// Deprecated and ignored: the QUIC-only peer memory budget that
    /// `[memory] budget` replaces. Kept so older configs still load, and
    /// `zakurad start` warns when it is set.
    #[serde(rename = "peer_memory_budget_bytes", skip_serializing)]
    #[doc(hidden)]
    pub deprecated_peer_memory_budget_bytes: Option<u64>,
    /// Maximum established Zakura connections admitted from one source IP.
    ///
    /// A global unicast IPv6 source counts by its /64, since one host can hold
    /// every address in it. Other sources count by their exact address, after
    /// IPv4 transition encodings fold to the IPv4 address.
    ///
    /// Zakura allows a small number of same-IP peers by default so NATed nodes
    /// and co-hosted fleets can connect without hitting the legacy TCP crawler's
    /// stricter per-IP default. The outbound share of
    /// [`max_connections`](Self::max_connections) remains the primary
    /// eclipse-resistance control.
    pub max_connections_per_ip: usize,
    /// Connections concurrently running the control handshake.
    ///
    /// Inbound handshakes leave the same reserve free as
    /// [`max_connections`](Self::max_connections).
    ///
    /// Also the QUIC connections that `[memory] budget` funds
    /// beyond the admitted ones, for handshakes and extra dial attempts.
    pub max_pending_handshakes: usize,
    /// New streams per second admitted per connection after a valid prelude.
    pub stream_open_rate_per_second: u32,
    /// Messages per second admitted per stream kind on a connection.
    pub message_rate_per_second: u32,
    /// Optional directory for structured Zakura JSONL trace tables.
    ///
    /// When unset, Zakura trace emission is disabled. When set, the native Zakura endpoint writes
    /// the production trace schema and the legacy sync pipeline writes `legacy_sync.jsonl` into
    /// this directory. Legacy peer fields in `legacy_sync.jsonl` follow
    /// [`Config::expose_peer_addresses`](crate::config::Config::expose_peer_addresses).
    /// The state, mempool, and `getblocktemplate` write `chain_tip.jsonl`, `mempool.jsonl`, and
    /// `mining_template.jsonl`, which trace the path from a new tip to a mining template.
    /// Legacy peer connections write `legacy_gossip.jsonl`: each block announcement sent to
    /// a peer, the peer's request for that block and the block being sent, and a bounded
    /// number of the announcements each peer sends. Its rows never hold a peer's address;
    /// [`trace_peer_names`](Self::trace_peer_names) names peers an operator wants to tell apart.
    pub trace_dir: Option<PathBuf>,
    /// Operator-chosen peer names in gossip traces, keyed by IP address.
    ///
    /// Only list nodes you operate. Unlisted and proxied peers remain unnamed.
    /// This has no effect when [`trace_dir`](Self::trace_dir) is unset.
    pub trace_peer_names: BTreeMap<IpAddr, String>,
    /// Native header-sync wire settings.
    pub header_sync: ZakuraHeaderSyncConfig,
    /// Native stream-6 block-sync wire, scheduling, serving, and rollout settings.
    pub block_sync: ZakuraBlockSyncConfig,
    /// `tx-relay` settings (`[network.zakura.tx_relay]`).
    pub tx_relay: super::ZakuraTxRelayConfig,
    /// BPv1 block propagation settings (`[network.zakura.bpv1]`, opt-in).
    pub bpv1: super::ZakuraBpv1Config,
    /// Optional private dev-network cohort tag.
    ///
    /// When set, this node only forms Zakura (v2) connections with peers
    /// advertising the same tag: its handshakes and signed discovery records
    /// carry a cohort-derived chain id, so public mainnet nodes and other
    /// cohorts reject (and are rejected by) this node at the Zakura layer.
    ///
    /// This does NOT change consensus rules. Genesis, network magic, and
    /// activation heights stay as configured for the network; the tag only
    /// scopes the Zakura v2 overlay so a team can test breaking changes in
    /// isolation while still validating the same chain. Has no effect unless
    /// [`v2_p2p`](crate::config::Config::v2_p2p) is enabled.
    pub dev_network: Option<String>,
}

impl Default for ZakuraConfig {
    fn default() -> Self {
        Self {
            bootstrap_peers: DEFAULT_ZAKURA_BOOTSTRAP_PEERS
                .iter()
                .map(ToString::to_string)
                .collect(),
            listen_addr: Some(DEFAULT_ZAKURA_LISTEN_ADDR),
            nat_traversal: false,
            quic: QuicConfig::default(),
            max_connections: None,
            memory_budget: super::memory::ResolvedMemoryBudget::default(),
            deprecated_peer_memory_budget_bytes: None,
            max_connections_per_ip: DEFAULT_ZAKURA_MAX_CONNS_PER_IP,
            max_pending_handshakes: DEFAULT_ZAKURA_MAX_PENDING_HANDSHAKES,
            stream_open_rate_per_second: DEFAULT_ZAKURA_STREAM_OPEN_RATE_PER_SECOND,
            message_rate_per_second: DEFAULT_ZAKURA_MESSAGE_RATE_PER_SECOND,
            trace_dir: None,
            trace_peer_names: BTreeMap::new(),
            header_sync: ZakuraHeaderSyncConfig::default(),
            block_sync: ZakuraBlockSyncConfig::default(),
            tx_relay: super::ZakuraTxRelayConfig::default(),
            bpv1: super::ZakuraBpv1Config::default(),
            dev_network: None,
        }
    }
}

impl ZakuraConfig {
    /// Refuses a stream receive window that can't hold the largest native
    /// frame, a block-sync frame of `MAX_BS_FRAME_BYTES`.
    ///
    /// The five native reactors take their receive shares from their own
    /// budgets ([`Self::reactor_shares`]). Any other stream kind receives one
    /// stream window ([`ReactorShares::from_quic`]). This applies endpoint
    /// startup's share check, at one stream window, to block sync's stream.
    /// Startup checks every registered kind again.
    pub fn validate_stream_receive_window(&self) -> Result<(), zakura_quic::ConfigError> {
        check_reactor_shares(
            &ReactorShares::from_quic(&self.quic),
            super::block_sync::BLOCK_SYNC_SERVICE_STREAMS,
            super::LOCAL_MAX_MESSAGE_BYTES,
        )
        .map_err(|error| zakura_quic::ConfigError {
            key: "stream_receive_window_bytes",
            reason: format!("{error}; a stream window must hold the largest native frame"),
        })
    }

    /// Effective connection policy ceiling `N_cap` (`DEFAULT_SERVICE_MAX_PEERS` = 256
    /// when unset, `02 §8`).
    pub fn effective_max_connections(&self) -> usize {
        self.max_connections
            .unwrap_or(super::DEFAULT_SERVICE_MAX_PEERS)
            .max(1)
    }

    /// Return the effective per-IP admission cap, preserving a non-zero limit.
    pub fn max_connections_per_ip(&self) -> usize {
        if self.max_connections_per_ip == 0 {
            DEFAULT_ZAKURA_MAX_CONNS_PER_IP
        } else {
            self.max_connections_per_ip
        }
    }

    /// Each reactor's receive and output share on one connection on
    /// `network`, keyed by its stream kind.
    ///
    /// The handshake share is the `[network.zakura.quic]` receive window if
    /// set, and [`CONTROL_RECEIVE_BYTES`] if not. The
    /// kinds are legacy gossip (2), legacy requests (3), discovery (4), header
    /// sync (5) and block sync (6), and `tx-relay`'s data (9) and state (10)
    /// when it is enabled. The compatibility exchanges and TCP ingress
    /// have their own envelope terms, so they are not kinds. Other kinds keep
    /// [`ReactorShares::from_quic`]'s share.
    pub fn reactor_shares(&self, network: &Network) -> ReactorShares {
        // A cwnd cap so large that its share overflows can't be funded, so the
        // peer memory budget refuses it.
        let block_sync = super::block_sync_reactor_budget(
            self.block_sync.bbr_max_cwnd_bytes,
            self.block_sync.floor_bypass_slots,
        )
        .unwrap_or(ReactorBudget {
            receive: ReceiveBudget {
                unsolicited: u64::MAX,
                per_request: 0,
            },
            output: u64::MAX,
        });
        let mut kinds = vec![
            (LEGACY_GOSSIP_STREAM_KIND, super::LEGACY_GOSSIP_BUDGET),
            (LEGACY_REQUEST_STREAM_KIND, super::LEGACY_REQUESTS_BUDGET),
            (DISCOVERY_STREAM_KIND, super::DISCOVERY_BUDGET),
            (
                HEADER_SYNC_STREAM_KIND,
                super::header_sync::v9::budget(network),
            ),
            (ZAKURA_STREAM_BLOCK_SYNC, block_sync),
        ];
        // `tx-relay`'s kinds are listed only when it runs, so a node with it
        // off budgets exactly as before.
        if self.tx_relay.enabled {
            use super::tx_relay::wire::{
                TX_RELAY_DATA_BUDGET, TX_RELAY_DATA_KIND, TX_RELAY_STATE_BUDGET,
                TX_RELAY_STATE_KIND,
            };
            kinds.push((TX_RELAY_DATA_KIND, TX_RELAY_DATA_BUDGET));
            kinds.push((TX_RELAY_STATE_KIND, TX_RELAY_STATE_BUDGET));
        }
        // BPv1's kinds are listed only when enabled, so disabled nodes budget
        // identically to before (`dual-6`).
        if self.bpv1.enabled {
            use super::bpv1::wire::{
                BPV1_CONTROL_BUDGET, BPV1_CONTROL_KIND, BPV1_DATA_BUDGET, BPV1_DATA_KIND,
            };
            kinds.push((BPV1_CONTROL_KIND, BPV1_CONTROL_BUDGET));
            kinds.push((BPV1_DATA_KIND, BPV1_DATA_BUDGET));
        }
        // An unset window gets the derived control share; a set one is the
        // operator's handshake window, even if it equals the default.
        let control_receive_bytes = self
            .quic
            .receive_window_bytes
            .map_or(CONTROL_RECEIVE_BYTES, u64::from);
        ReactorShares {
            control_receive_bytes,
            kinds,
            ..ReactorShares::from_quic(&self.quic)
        }
    }

    /// The QUIC send window held while a connection is still in its handshake.
    pub fn handshake_send_window_bytes(&self) -> u64 {
        HANDSHAKE_SEND_WINDOW_BYTES.min(self.quic.send_window_bytes)
    }

    /// What one connection on `network` can make QUIC hold, when it
    /// negotiates every reactor in [`Self::reactor_shares`] and every stream
    /// kind in `custom_kinds`.
    ///
    /// A custom kind receives [`ReactorShares::other`], as it does on a live
    /// connection. The service registry refuses a custom kind that repeats a
    /// native one.
    pub fn connection_memory(&self, network: &Network, custom_kinds: &[u16]) -> ConnectionMemory {
        let shares = self.reactor_shares(network);
        let kinds = shares
            .kinds
            .iter()
            .map(|(_, share)| share.receive_total())
            .chain(
                custom_kinds
                    .iter()
                    .map(|kind| shares.share(*kind).receive_total()),
            )
            .fold(0_u64, u64::saturating_add);
        ConnectionMemory::with_handshake_send(
            shares.control_receive_bytes,
            self.handshake_send_window_bytes(),
            kinds,
            self.quic.send_window_bytes,
        )
    }

    /// The QUIC memory the node can hold on `network` with `max_connections`
    /// admitted: every admitted connection, the handshake headroom, and the
    /// bound sockets' queues. Each connection negotiates the native kinds and
    /// `custom_kinds`.
    pub fn quic_memory_bytes(
        &self,
        network: &Network,
        max_connections: usize,
        custom_kinds: &[u16],
    ) -> u128 {
        budgeted_quic_bytes(
            self.bound_socket_count(),
            self.connection_memory(network, custom_kinds),
            max_connections,
            self.max_pending_handshakes.max(1),
        )
    }

    /// Returns how many sockets the native endpoint binds for this config (see
    /// `native_bind_addrs`), each with its own datagram queue.
    pub(crate) fn bound_socket_count(&self) -> u64 {
        u64::try_from(native_bind_addrs(self.listen_addr).len()).unwrap_or(u64::MAX)
    }

    /// Return the default bootstrap peers for `network`.
    pub fn default_bootstrap_peers_for_network(network: &Network) -> Vec<String> {
        match network.kind() {
            NetworkKind::Mainnet => {
                bootstrap_peers_to_strings(DEFAULT_MAINNET_ZAKURA_BOOTSTRAP_PEERS)
            }
            NetworkKind::Testnet => {
                bootstrap_peers_to_strings(DEFAULT_TESTNET_ZAKURA_BOOTSTRAP_PEERS)
            }
            NetworkKind::Regtest => Vec::new(),
        }
    }

    /// Switch default bootstrap peers to the selected network.
    ///
    /// This preserves explicit user overrides, including an empty list, and only
    /// rewrites the implicit default populated by serde before the network is known.
    /// A generated config spells the Mainnet defaults out, possibly by the seeds'
    /// retired node IDs, so on other networks a list that matches once those are
    /// replaced counts too. On Mainnet the list is left alone, so the startup
    /// replacement of retired IDs sees and reports them.
    pub fn apply_network_defaults(&mut self, network: &Network) {
        if network.kind() == NetworkKind::Mainnet {
            return;
        }
        let entries: Vec<&str> = self.bootstrap_peers.iter().map(String::as_str).collect();
        let is_mainnet_default = super::retired_seed_replacements(&entries)
            .into_iter()
            .zip(&entries)
            .map(|(replacement, entry)| replacement.unwrap_or_else(|| entry.to_string()))
            .eq(DEFAULT_MAINNET_ZAKURA_BOOTSTRAP_PEERS
                .iter()
                .map(ToString::to_string));
        if !is_mainnet_default {
            return;
        }

        self.bootstrap_peers = Self::default_bootstrap_peers_for_network(network);
    }
}

fn bootstrap_peers_to_strings(peers: &[&str]) -> Vec<String> {
    peers.iter().map(ToString::to_string).collect()
}

/// Hard local ceilings enforced by the Zakura endpoint and handler.
#[derive(Clone, Debug)]
pub struct ZakuraLocalLimits {
    /// Native QUIC transport settings.
    pub quic: QuicConfig,
    /// Memory-funded cap for ordinary connections.
    pub max_connections: usize,
    /// Additional connections the peer memory budget funds for preferred gateways.
    pub max_preferred_connections: usize,
    /// Concurrent control handshakes cap.
    pub max_pending_handshakes: usize,
    /// Per-connection QUIC/app idle timeout.
    pub quic_idle_timeout: Duration,
    /// QUIC keepalive interval.
    pub keep_alive_interval: Duration,
    /// Stream prelude read timeout.
    pub prelude_timeout: Duration,
    /// Control handshake deadline: it bounds each read and write, and the
    /// whole exchange in both directions. A responder measures it from the
    /// QUIC admission, so TLS and the control exchange share it.
    pub control_timeout: Duration,
    /// Per-connection stream-open rate.
    pub stream_open_rate_per_second: u32,
    /// Per-stream-kind message rate.
    pub message_rate_per_second: u32,
    /// Maximum frame bytes accepted locally.
    pub max_frame_bytes: u32,
    /// Maximum reassembled message bytes accepted locally.
    pub max_message_bytes: u32,
    /// Maximum concurrent admitted streams per connection.
    pub max_open_streams: u16,
    /// Maximum inbound queue depth per stream kind.
    pub max_inbound_queue_depth: u16,
    /// Each stream kind's receive and output share on one connection. Unset,
    /// [`ReactorShares::from_quic`] derives them from `quic`'s windows.
    pub reactor_shares: Option<ReactorShares>,
    /// Connection-level QUIC send window applied once a connection finishes its
    /// control handshake and is admitted. While handshaking, `quic.send_window_bytes`
    /// stays small ([`HANDSHAKE_SEND_WINDOW_BYTES`]).
    pub admitted_send_window_bytes: u64,
}

impl ZakuraLocalLimits {
    /// Build local handler limits from network configuration and handshake
    /// policy, for a node that serves only the native stream kinds.
    ///
    /// The connection cap is the configured `N_cap`. Endpoint startup takes
    /// these limits from `MemoryPlan::local_limits`, and `Consumer::Sessions`
    /// decides how many connections memory funds below that cap.
    pub fn from_config(config: &Config) -> Self {
        let handshake = ZakuraHandshakeConfig::for_network(&config.network);
        let admitted_send_window_bytes = config.zakura.quic.send_window_bytes;
        Self {
            quic: config.zakura.quic.clone(),
            max_connections: config.zakura.effective_max_connections(),
            max_preferred_connections: super::MAX_PREFERRED_GATEWAYS,
            max_pending_handshakes: config.zakura.max_pending_handshakes.max(1),
            quic_idle_timeout: config.zakura.quic.idle_timeout(),
            keep_alive_interval: config.zakura.quic.keep_alive_interval(),
            prelude_timeout: DEFAULT_ZAKURA_PRELUDE_TIMEOUT,
            control_timeout: DEFAULT_ZAKURA_CONTROL_TIMEOUT,
            stream_open_rate_per_second: config.zakura.stream_open_rate_per_second.max(1),
            message_rate_per_second: config.zakura.message_rate_per_second.max(1),
            max_frame_bytes: handshake.max_message_bytes,
            max_message_bytes: handshake.max_message_bytes,
            max_open_streams: handshake.max_open_streams,
            max_inbound_queue_depth: handshake.max_inbound_queue_depth,
            reactor_shares: None,
            admitted_send_window_bytes,
        }
        .with_reactor_shares(config.zakura.reactor_shares(&config.network))
    }

    /// These limits with `shares`, and the QUIC windows that carry them: the
    /// connection receive and send windows start at the small handshake shares,
    /// and the stream receive window holds the largest receive share.
    fn with_reactor_shares(mut self, shares: ReactorShares) -> Self {
        self.quic.receive_window_bytes =
            Some(u32::try_from(shares.control_receive_bytes).unwrap_or(u32::MAX));
        self.quic.stream_receive_window_bytes =
            u32::try_from(shares.stream_receive_window()).unwrap_or(u32::MAX);
        self.quic.send_window_bytes =
            HANDSHAKE_SEND_WINDOW_BYTES.min(self.admitted_send_window_bytes);
        self.reactor_shares = Some(shares);
        self
    }

    /// Each stream kind's receive and output share on one connection.
    pub fn reactor_shares(&self) -> ReactorShares {
        self.reactor_shares
            .clone()
            .unwrap_or_else(|| ReactorShares::from_quic(&self.quic))
    }

    /// Clamp peer-negotiated limits to local hard ceilings.
    pub fn clamp(&self, negotiated: &ZakuraAcceptedLimits) -> ZakuraConnectionLimits {
        let max_open_streams = negotiated
            .max_open_streams
            .min(self.max_open_streams)
            .max(1);
        let idle_timeout = Duration::from_millis(
            u64::from(negotiated.idle_timeout_millis)
                .min(self.quic_idle_timeout.as_millis().saturating_sub(1) as u64)
                .max(1),
        );

        ZakuraConnectionLimits {
            max_frame_bytes: negotiated.max_frame_bytes.min(self.max_frame_bytes).max(1),
            max_message_bytes: negotiated
                .max_message_bytes
                .min(self.max_message_bytes)
                .max(1),
            max_open_streams,
            max_inbound_queue_depth: negotiated
                .max_inbound_queue_depth
                .min(self.max_inbound_queue_depth)
                .max(1),
            idle_timeout,
            prelude_timeout: self.prelude_timeout,
            control_timeout: self.control_timeout,
            stream_open_rate_per_second: self.stream_open_rate_per_second,
            message_rate_per_second: self.message_rate_per_second,
        }
    }

    /// Returns the initial limits advertised in a native control hello.
    pub fn initial_limits(&self) -> ZakuraInitialLimits {
        ZakuraLimits {
            max_frame_bytes: self.max_frame_bytes,
            max_message_bytes: self.max_message_bytes,
            max_open_streams: self.max_open_streams,
            max_inbound_queue_depth: self.max_inbound_queue_depth,
            idle_timeout_millis: self.quic_idle_timeout.as_millis().saturating_sub(1) as u32,
        }
    }

    /// Returns the transport's byte bounds under these limits, for a node
    /// that serves `registry`'s stream kinds.
    pub(crate) fn resource_envelope(&self, registry: &ServiceRegistry) -> ResourceEnvelope {
        ResourceEnvelope::new(self.transport_limits(registry))
    }

    /// The limits that the transport's bounds derive from, for a node that
    /// serves `registry`'s stream kinds.
    pub(crate) fn transport_limits(&self, registry: &ServiceRegistry) -> TransportLimits {
        let kinds = registry
            .services()
            .iter()
            .flat_map(|service| service.streams().iter().map(|stream| stream.kind));
        self.transport_limits_for_kinds(kinds)
    }

    /// The limits that the transport's bounds derive from, for a node that
    /// serves streams of `kinds`.
    pub(crate) fn transport_limits_for_kinds(
        &self,
        kinds: impl IntoIterator<Item = u16>,
    ) -> TransportLimits {
        let shares = self.reactor_shares();
        let mut kinds: Vec<u16> = kinds.into_iter().collect();
        kinds.sort_unstable();
        kinds.dedup();
        let connection_shares = kinds.iter().fold(ReactorBudget::ZERO, |sum, kind| {
            let share = shares.share(*kind);
            // Guarded `Serve` and `Push` frames carry their own `FrameGuard` and
            // skip `FrameGate.output`; count only unguarded kinds here so serving
            // output is accounted once in `serve_output_bytes`.
            let unguarded_output = if Self::is_guarded_output_kind(*kind) {
                0
            } else {
                share.output
            };
            ReactorBudget {
                receive: sum.receive.saturating_add(share.receive),
                output: sum.output.saturating_add(unguarded_output),
            }
        });
        let compat_exchange_bytes = if kinds.contains(&LEGACY_REQUEST_STREAM_KIND) {
            // Widening usize to u64 is lossless on supported targets.
            COMPAT_EXCHANGE_BUDGET
                .receive_total()
                .saturating_mul(super::legacy_gossip::LEGACY_CLIENT_REQUEST_SLOTS as u64)
        } else {
            0
        };
        TransportLimits {
            max_open_streams: self.max_open_streams,
            control_receive_bytes: shares.control_receive_bytes,
            connection_shares,
            compat_exchange_bytes,
            send_window: self.admitted_send_window_bytes,
            handshake_send_window: self.quic.send_window_bytes,
            max_connections: self.total_connections(),
            max_pending_handshakes: self.max_pending_handshakes,
        }
    }

    /// Returns `true` for stream kinds whose outbound frames are guarded by `Serve`
    /// or `Push` (or cadence/request guards) and therefore bypass `FrameGate.output`.
    pub(crate) const fn is_guarded_output_kind(kind: u16) -> bool {
        matches!(
            kind,
            DISCOVERY_STREAM_KIND | HEADER_SYNC_STREAM_KIND | ZAKURA_STREAM_BLOCK_SYNC
        )
    }

    /// Total admitted connections for sizing transport and serving resources.
    pub fn total_connections(&self) -> usize {
        self.max_connections
            .saturating_add(self.max_preferred_connections)
    }

    /// The most owners that these limits allow at once.
    pub(crate) fn owner_bound(&self) -> OwnerBound {
        OwnerBound::new(
            self.max_pending_handshakes,
            self.total_connections(),
            self.max_open_streams,
        )
        .expect("the node envelope refused these limits at startup if they overflow")
    }

    /// Returns the QUIC bind settings for `addrs` under these limits.
    ///
    /// The transport holds the admitted connections plus
    /// `max_pending_handshakes` more live ones, and two closing connections
    /// per live slot (see `peer_memory`). Inbound attempts may hold
    /// [`Self::max_inbound_connections`] plus [`Self::max_inbound_handshakes`]
    /// plus the funded gateway places
    /// of the live slots (103 at the defaults), counted until each connection
    /// closes, so a peer that this node closes right after its TLS handshake
    /// still counts until the close finishes.
    ///
    /// The dial reserve fits one maintained dial of a full address record
    /// ([`discovery::MAX_DIRECT_ADDRS_PER_RECORD`]), within the slots inbound can't
    /// hold: 8 at the defaults. Discovery stops at the reserve, so with
    /// inbound full it keeps the slots between the two. That's the whole
    /// outbound share (23 at the defaults) whenever the handshake reserve is
    /// at least the address cap, which holds from 32 pending handshakes up.
    pub fn quic_bind_config(&self, addrs: Vec<SocketAddr>) -> QuicBindConfig {
        let max_connections = self
            .total_connections()
            .saturating_add(self.max_pending_handshakes);
        let max_inbound_connections = self
            .max_inbound_connections()
            .saturating_add(self.max_preferred_connections)
            .saturating_add(self.max_inbound_handshakes());
        let outbound_slots = max_connections.saturating_sub(max_inbound_connections);
        QuicBindConfig {
            addrs,
            max_bidi_streams: u32::from(self.max_open_streams),
            max_connections,
            dial_reserve: outbound_slots.clamp(1, discovery::MAX_DIRECT_ADDRS_PER_RECORD),
            max_inbound_connections: Some(max_inbound_connections),
            max_closing_connections: max_connections
                .saturating_mul(ZAKURA_CLOSING_CONNECTIONS_PER_LIVE),
        }
    }

    /// Connections inbound peers may hold at once: `max_connections` less the
    /// outbound reserve (69 of the default 92). See
    /// [`without_outbound_reserve`] for the reserve's size.
    ///
    /// Peers that dial this node can't take the slots its own dials need, so
    /// they can't crowd out the peers it chose. Dials may still use any free
    /// slot, inbound's share included.
    pub fn max_inbound_connections(&self) -> usize {
        without_outbound_reserve(self.max_connections)
    }

    /// Control handshakes inbound peers may run at once:
    /// `max_pending_handshakes` less the same outbound reserve (24 of the
    /// default 32), as [`Self::max_inbound_connections`] explains.
    pub fn max_inbound_handshakes(&self) -> usize {
        without_outbound_reserve(self.max_pending_handshakes)
    }
}

/// One cap's share left to inbound peers after reserving a quarter of it,
/// rounded down, for this node's dials. Any cap of 2 or more reserves at least
/// one slot, so small caps still leave dials room. A cap of 1 stays inbound's,
/// since a node that accepts nothing can't be reached.
fn without_outbound_reserve(cap: usize) -> usize {
    super::transport::inbound_share(cap)
}

/// Per-connection limits after negotiation and clamping.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct ZakuraConnectionLimits {
    /// Maximum encoded frame bytes.
    pub max_frame_bytes: u32,
    /// Maximum reassembled message bytes.
    pub max_message_bytes: u32,
    /// Maximum concurrent streams.
    pub max_open_streams: u16,
    /// Bounded inbound queue depth.
    pub max_inbound_queue_depth: u16,
    /// Application idle timeout.
    pub idle_timeout: Duration,
    /// Stream prelude read timeout.
    pub prelude_timeout: Duration,
    /// Control handshake timeout.
    pub control_timeout: Duration,
    /// Per-connection stream-open rate.
    pub stream_open_rate_per_second: u32,
    /// Per-stream-kind message rate.
    pub message_rate_per_second: u32,
}

/// A service supplied by the application embedding Zakura.
#[derive(Clone, Debug)]
pub struct CustomService {
    /// Custom p2p service.
    pub service: Arc<dyn Service>,

    /// Service ids to advertise to the network.
    ///
    /// The node's record carries at most
    /// [`MAX_SERVICES_PER_RECORD`](discovery::MAX_SERVICES_PER_RECORD)
    /// services, the six built-in services included. Startup fails if the
    /// distinct services exceed that limit.
    pub provides: Vec<ZakuraServiceId>,

    /// Remote service ids to prefer when selecting outbound discovery peers.
    pub seeks: Vec<ZakuraServiceId>,

    /// `tx-relay`'s live-session record, set only by
    /// [`tx_relay_service`](super::tx_relay_service). With it, the legacy
    /// adapter leaves each peer with a live session to `tx-relay`.
    pub tx_relay_sessions: Option<super::TxRelaySessions>,
}

impl From<CustomService> for Arc<dyn Service> {
    fn from(service: CustomService) -> Self {
        service.service
    }
}

/// Running Zakura endpoint owned by `zakura-network`/`zakurad` startup.
#[derive(Debug, Clone)]
pub struct ZakuraEndpoint {
    quic: QuicEndpoint,
    supervisor: ZakuraSupervisorHandle,
    handler: ZakuraProtocolHandler,
    header_sync: Option<super::HeaderSyncHandle>,
    block_sync: Option<BlockSyncHandle>,
    header_sync_tasks: Option<Arc<HeaderSyncBackgroundTasks>>,
    header_sync_fatal_events:
        Option<Arc<AsyncMutex<Option<mpsc::UnboundedReceiver<super::HeaderSyncFatalEvent>>>>>,
    #[cfg(any(test, feature = "zakura-testkit"))]
    header_sync_actions: Option<Arc<AsyncMutex<Option<mpsc::Receiver<HeaderSyncAction>>>>>,
    block_sync_actions: Option<Arc<AsyncMutex<Option<mpsc::Receiver<BlockSyncAction>>>>>,
    /// BPv1's handle, when the endpoint started BPv1.
    bpv1: Option<super::Bpv1Handle>,
    /// Maintained native dials started by the legacy->Zakura upgrade hand-off,
    /// keyed by the advertised peer id. See [`Self::start_upgrade_native_dial`]
    /// and [`Self::cancel_upgrade_native_dial`].
    upgrade_dials: Arc<StdMutex<HashMap<ZakuraPeerId, UpgradeDialOwnership>>>,
    /// When each upgrade hint's ALPN-mismatch floor ends, for upgrade dials
    /// that ended on a mismatch. No upgrade dial starts for a hint inside its
    /// floor (zakura-quic DIAL-5). See [`upgrade_hint`].
    upgrade_alpn_floors: Arc<StdMutex<HashMap<NodeAddr, Instant>>>,
}

#[derive(Debug)]
struct UpgradeDialOwnership {
    /// Cancelled when this endpoint stops owning the dial. The dial then
    /// starts no new attempt and ends once its in-flight one does.
    lifetime: CancellationToken,
}

#[derive(Debug)]
struct HeaderSyncBackgroundTasks {
    shutdown: CancellationToken,
    tasks: AsyncMutex<Vec<JoinHandle<()>>>,
}

/// Durable state facts required before attaching the production header-sync driver.
#[derive(Clone, Debug)]
pub struct ZakuraHeaderSyncDriverStartup {
    /// Backend block-count allowance for native decoded ownership.
    pub decoded_backend_blocks: u64,
    /// State-backed source for bounded native GetBlocks serving.
    pub block_range_source: Arc<dyn super::BlockRangeSource>,
    /// Durable state frontiers loaded at node startup.
    pub frontiers: FullStateFrontiers,
    /// Durable best header tip loaded from state.
    pub best_header_tip: Option<(block::Height, block::Hash)>,
    /// Hash of `frontiers.verified_block_tip`.
    pub verified_block_tip_hash: block::Hash,
    /// Durable header snapshots.
    /// The watch value remains absent until semantic handoff succeeds.
    pub committed_snapshots: watch::Receiver<Option<zakura_header_chain::EngineSnapshot>>,
    /// Atomic committed snapshots and body-work epochs.
    pub committed_views: watch::Receiver<Option<zakura_header_chain::CommittedHeaderChainView>>,
    /// Durable retained-body floor, published before each corresponding committed view.
    pub retained_block_height: watch::Receiver<block::Height>,
    /// Coordinator-owned capability and ordered-service demand epochs.
    pub service_demand: watch::Receiver<zakura_node_services::sync_lifecycle::SyncServiceDemand>,
    /// VCT metadata repair needs published by the finalized writer.
    pub vct_root_repairs: Option<watch::Receiver<zakura_header_chain::VctRootRepairStatus>>,
    /// Typed header-chain operations used directly by the reactor.
    pub header_chain_port: Arc<dyn zakura_node_services::header_chain::Port>,
    /// Optional shared body claims coordinating native block sync with BPv1.
    pub body_claims: Option<zakura_node_services::body_claims::SharedBodyClaims>,
}

impl ZakuraEndpoint {
    /// Applies the production initial capability gate for a simulator witness.
    #[cfg(zakura_sim)]
    pub fn disable_header_sync_for_sim(&self) {
        self.handler.set_header_sync_enabled(false);
    }

    /// Runs production capability enablement and its registered-peer reconnect pass.
    #[cfg(zakura_sim)]
    pub async fn enable_header_sync_for_sim(&self, epoch: u64) -> usize {
        enable_header_sync_and_renegotiate(
            &self.handler,
            &self.supervisor,
            zakura_node_services::sync_lifecycle::LifecycleEpoch::new(epoch),
        )
        .await
    }
    /// Returns the local node identity used to authenticate native connections.
    pub fn local_node_id(&self) -> NodeId {
        self.quic.local_id()
    }

    /// Returns the connector injected into the legacy handshake path.
    pub fn connector(&self) -> super::ZakuraHandshakeConnector {
        super::ZakuraHandshakeConnector::new_with_endpoint(self.clone())
    }

    /// Returns our local Zakura dial hints (node id, and direct addresses
    /// each encoded as a `SocketAddr` string) for the legacy upgrade prelude.
    ///
    /// Direct addresses are capped at [`MAX_IROH_DIRECT_ADDRESSES`](super::MAX_IROH_DIRECT_ADDRESSES)
    /// so the encoded prelude stays within its bounded hint limits.
    pub(crate) async fn local_upgrade_hints(&self) -> (Vec<u8>, Vec<Vec<u8>>) {
        let node_id = self.quic.local_id().as_bytes().to_vec();
        let direct_addresses = local_direct_addrs(&self.quic)
            .into_iter()
            .take(super::MAX_IROH_DIRECT_ADDRESSES)
            .map(|addr| addr.to_string().into_bytes())
            .collect();
        (node_id, direct_addresses)
    }

    /// Returns the active supervisor handle.
    pub fn supervisor(&self) -> ZakuraSupervisorHandle {
        self.supervisor.clone()
    }

    /// Returns the endpoint trace emitter.
    pub fn trace(&self) -> ZakuraTrace {
        self.handler.trace.clone()
    }

    /// Returns the startup memory sizing plan enforced by this endpoint, if started
    /// through the production startup path.
    pub fn memory_plan(&self) -> Option<super::memory::MemoryPlan> {
        self.handler.memory_plan.clone()
    }

    /// Returns the node-wide memory pool owned by this endpoint.
    pub fn memory_pool(&self) -> Option<super::memory::MemoryPool> {
        self.handler.memory_pool.clone()
    }

    /// Returns BPv1's handle when this endpoint started BPv1.
    pub fn bpv1(&self) -> Option<super::Bpv1Handle> {
        self.bpv1.clone()
    }

    /// Returns the header-sync handle when native header sync is active.
    pub fn header_sync(&self) -> Option<super::HeaderSyncHandle> {
        self.header_sync.clone()
    }

    /// Returns the block-sync handle when native block sync is active.
    pub fn block_sync(&self) -> Option<BlockSyncHandle> {
        self.block_sync.clone()
    }

    /// Take the header-sync action receiver when this endpoint was started in external-driver mode.
    #[cfg(any(test, feature = "zakura-testkit"))]
    pub async fn take_header_sync_actions(&self) -> Option<mpsc::Receiver<HeaderSyncAction>> {
        let actions = self.header_sync_actions.as_ref()?;
        actions.lock().await.take()
    }

    /// Take the block-sync action receiver when this endpoint was started in external-driver mode.
    pub async fn take_block_sync_actions(&self) -> Option<mpsc::Receiver<BlockSyncAction>> {
        let actions = self.block_sync_actions.as_ref()?;
        actions.lock().await.take()
    }

    /// Returns the endpoint-owned header-sync shutdown token.
    pub fn header_sync_shutdown(&self) -> Option<CancellationToken> {
        self.header_sync_tasks
            .as_ref()
            .map(|tasks| tasks.shutdown.clone())
    }

    /// Take the private header-sync fatal event receiver for the node root.
    pub async fn take_header_sync_fatal_events(
        &self,
    ) -> Option<mpsc::UnboundedReceiver<super::HeaderSyncFatalEvent>> {
        let events = self.header_sync_fatal_events.as_ref()?;
        events.lock().await.take()
    }

    /// The endpoint-wide background-task shutdown token, cancelled by
    /// [`ZakuraEndpoint::shutdown`].
    ///
    /// Detached dial/discovery loops observe this so they stop promptly at
    /// teardown instead of running on against a torn-down router. They each hold
    /// an endpoint clone, which keeps the supervisor registration watch open, so
    /// watch closure is *not* a reliable exit signal for them; this token is.
    /// Falls back to an un-cancelled token for endpoints built without a
    /// background-task owner (recorder-only test nodes).
    pub(crate) fn background_shutdown_token(&self) -> CancellationToken {
        self.header_sync_tasks
            .as_ref()
            .map(|tasks| tasks.shutdown.clone())
            .unwrap_or_default()
    }

    /// Track a header-sync integration task under the endpoint shutdown owner.
    pub async fn push_header_sync_task(&self, task: JoinHandle<()>) {
        if let Some(tasks) = self.header_sync_tasks.as_ref() {
            tasks.tasks.lock().await.push(task);
        }
    }

    /// Track a block-sync integration task under the endpoint shutdown owner.
    pub async fn push_block_sync_task(&self, task: JoinHandle<()>) {
        self.push_header_sync_task(task).await;
    }

    /// Returns the endpoint's current direct node address.
    pub async fn node_addr(&self) -> NodeAddr {
        NodeAddr::with_addrs(self.quic.local_id(), local_direct_addrs(&self.quic))
    }

    /// Start a native Zakura dial in the background, maintaining it with
    /// bounded backoff.
    ///
    /// Used by the legacy->Zakura upgrade hand-off: the legacy handshake just
    /// proved the peer is live, so a transient QUIC dial miss (e.g. the peer's
    /// endpoint is momentarily not ready) is worth retrying promptly instead of
    /// waiting for the legacy crawler to re-dial and re-run the whole upgrade.
    /// Once the legacy TCP connection is dropped, this task is the prompt
    /// recovery path for short Zakura disconnects; the address-book liveness
    /// keeper prevents the slower legacy crawler from churning while this dial
    /// owns the peer.
    pub fn spawn_native_dial(&self, node_addr: NodeAddr) -> JoinHandle<()> {
        let endpoint = self.clone();
        let limits = self.handler.limits.clone();
        let policy = RedialPolicy::maintain(
            DEFAULT_ZAKURA_REDIAL_INITIAL_BACKOFF,
            DEFAULT_ZAKURA_REDIAL_MAX_BACKOFF,
        );
        spawn_erased_named("native_dial", async move {
            native_dial_supervised(
                endpoint,
                node_addr,
                limits,
                policy,
                CancellationToken::new(),
            )
            .await;
        })
    }

    /// Start one maintained native dial for a legacy handoff.
    ///
    /// The legacy crawler can retry the same peer while a short-lived upgraded
    /// connection is still settling. Deduplicate those retries so repeated
    /// legacy upgrades do not create a swarm of independent maintained QUIC
    /// dial loops to the same peer. The dial gives up after
    /// [`ZAKURA_UPGRADE_REDIAL_MAX_ATTEMPTS`] consecutive failures because the
    /// peer chose the address. A dial that ends on an ALPN mismatch starts the
    /// hint's [`ZAKURA_ALPN_MISMATCH_BACKOFF`] floor, and no dial starts inside
    /// it. See [`upgrade_hint`] for why the floor keys the hint, not the node ID.
    pub(crate) fn start_upgrade_native_dial(&self, node_addr: NodeAddr) -> ZakuraUpgradeDialStart {
        let Ok(peer_id) = ZakuraPeerId::new(node_addr.id.as_bytes().to_vec()) else {
            return ZakuraUpgradeDialStart::InvalidPeerId;
        };

        // Hold the registry lock across the spawn so the dedup check and the
        // ownership insert are atomic against a concurrent upgrade to the
        // same peer. `tokio::spawn` does not await, and the spawned task only
        // re-locks after its (non-instant) dial returns, so this cannot
        // deadlock or `.await` under the lock.
        let mut upgrade_dials = self
            .upgrade_dials
            .lock()
            .expect("Zakura upgrade dial registry mutex is never poisoned");
        if upgrade_dials.contains_key(&peer_id) {
            return ZakuraUpgradeDialStart::AlreadyRunning;
        }
        let hint = upgrade_hint(&node_addr);
        if self.in_upgrade_alpn_floor(&hint) {
            return ZakuraUpgradeDialStart::AlpnBackoff;
        }

        let endpoint = self.clone();
        let limits = self.handler.limits.clone();
        let policy = RedialPolicy::maintain_bounded(
            DEFAULT_ZAKURA_REDIAL_INITIAL_BACKOFF,
            DEFAULT_ZAKURA_REDIAL_MAX_BACKOFF,
            ZAKURA_UPGRADE_REDIAL_MAX_ATTEMPTS,
        );
        // A cancelled hand-off lets the in-flight attempt run to the bound of a
        // whole dial (zakura-quic DIAL-4), so an ALPN mismatch that a slower
        // address delays still starts the floor.
        let drain = native_dial_timeout(
            self.quic.config(),
            node_addr.direct.len(),
            limits.control_timeout,
        );
        let lifetime = CancellationToken::new();
        let task_lifetime = lifetime.clone();
        let task_peer_id = peer_id.clone();
        spawn_erased_named("upgrade_dial", async move {
            let stop = task_lifetime.clone();
            let _lifetime_guard = task_lifetime.drop_guard();
            let supervised =
                native_dial_supervised(endpoint.clone(), node_addr, limits, policy, stop.clone());
            let end = zakura_rt::select! {
                end = supervised => end,
                () = async {
                    stop.cancelled().await;
                    zakura_rt::time::sleep(drain).await;
                } => RedialEnd::Ended,
            };
            if end == RedialEnd::AlpnMismatch {
                endpoint.hold_upgrade_alpn_floor(hint);
            }
            endpoint
                .supervisor
                .forget_retained_native_metadata(&task_peer_id);
            endpoint
                .upgrade_dials
                .lock()
                .expect("Zakura upgrade dial registry mutex is never poisoned")
                .remove(&task_peer_id);
        });
        upgrade_dials.insert(peer_id, UpgradeDialOwnership { lifetime });
        ZakuraUpgradeDialStart::Started
    }

    /// Starts `hint`'s ALPN-mismatch floor, and drops the floors that ended.
    fn hold_upgrade_alpn_floor(&self, hint: NodeAddr) {
        let now = Instant::now();
        let mut floors = self
            .upgrade_alpn_floors
            .lock()
            .expect("Zakura upgrade ALPN floor mutex is never poisoned");
        floors.retain(|_, until| *until > now);
        floors.insert(hint, now + ZAKURA_ALPN_MISMATCH_BACKOFF);
    }

    /// Whether `hint` is inside the ALPN-mismatch floor of an earlier upgrade
    /// dial.
    fn in_upgrade_alpn_floor(&self, hint: &NodeAddr) -> bool {
        let mut floors = self
            .upgrade_alpn_floors
            .lock()
            .expect("Zakura upgrade ALPN floor mutex is never poisoned");
        match floors.get(hint) {
            Some(until) if *until > Instant::now() => true,
            Some(_) => {
                floors.remove(hint);
                false
            }
            None => false,
        }
    }

    /// Returns how many maintained upgrade dials this endpoint owns.
    #[cfg(test)]
    pub(crate) fn upgrade_dial_count(&self) -> usize {
        self.upgrade_dials
            .lock()
            .expect("Zakura upgrade dial registry mutex is never poisoned")
            .len()
    }

    /// Return a token that is canceled when this endpoint stops owning the
    /// maintained native dial for `peer_id`.
    pub(crate) fn upgrade_dial_lifetime(
        &self,
        peer_id: &ZakuraPeerId,
    ) -> Option<CancellationToken> {
        self.upgrade_dials
            .lock()
            .expect("Zakura upgrade dial registry mutex is never poisoned")
            .get(peer_id)
            .map(|ownership| ownership.lifetime.clone())
    }

    /// Stop the maintained native dial started by the legacy upgrade hand-off
    /// for `peer_id`, if this node still owns one.
    ///
    /// Called when the upgrade hand-off ends without the peer registering,
    /// including when the outer handshake timeout drops the hand-off. The
    /// maintained dial would otherwise keep redialing a peer-supplied, possibly
    /// unreachable address until its attempt limit, and repeating the failed
    /// upgrade with distinct node ids would grow dial tasks and outbound QUIC
    /// traffic. The dial starts no new attempt, and its in-flight attempt ends
    /// within the bound of one whole dial. The dial then forgets the peer. A
    /// no-op if the dial already ended.
    pub(crate) fn cancel_upgrade_native_dial(&self, peer_id: &ZakuraPeerId) {
        if let Some(ownership) = self
            .upgrade_dials
            .lock()
            .expect("Zakura upgrade dial registry mutex is never poisoned")
            .get(peer_id)
        {
            ownership.lifetime.cancel();
        }
    }

    /// How many address attempts discovery can start now while preserving
    /// inbound capacity (zakura-quic ADM-12): none without a free connection
    /// slot, otherwise the transport's free discovery slots. A dial claims
    /// a slot for every address it tries, or fails with none claimed.
    pub(crate) fn discovery_admission_capacity(&self) -> usize {
        if self.handler.free_connection_slots() == 0 {
            return 0;
        }
        self.quic.discovery_capacity()
    }

    /// Transport room for an ordinary replacement after one established connection leaves.
    pub(crate) fn discovery_replacement_address_capacity(&self) -> usize {
        self.quic.discovery_replacement_capacity()
    }

    /// Select preferred gateways within this endpoint's funded extra capacity.
    pub fn publish_gateway_ranking(&self, ranking: impl IntoIterator<Item = super::RankedGateway>) {
        self.handler.publish_gateway_ranking(ranking);
    }

    /// Applies discovery's ordinary slot budget when yielding a moved gateway's place.
    pub(super) fn publish_discovery_gateway_ranking(
        &self,
        ranking: impl IntoIterator<Item = super::RankedGateway>,
        ordinary_cap: usize,
    ) {
        self.handler
            .publish_discovery_gateway_ranking(ranking, ordinary_cap);
    }

    /// The places the connection-slot checks hold for preferred gateways
    /// now, and the gateways that fill them. Discovery keeps its ordinary
    /// dials out of them.
    pub(crate) fn held_places(&self) -> HeldPlaces {
        self.handler
            .supervisor
            .ip_slots
            .counts()
            .preferred_gateways()
            .held_places()
    }

    /// The places held for preferred mining gateways, how the connection
    /// slots are used, and the v2 peers registered now.
    ///
    /// The peers are read just before the slot counts, so a connection that
    /// registers or closes in between can show in one and not the other.
    pub fn connection_places(&self) -> ConnectionPlaces {
        let registered: HashSet<NodeId> = self
            .supervisor
            .subscribe()
            .borrow()
            .iter()
            .filter_map(ZakuraPeerId::node_id)
            .collect();
        ConnectionPlaces::read(
            || self.handler.supervisor.ip_slots.counts(),
            self.handler.limits.total_connections(),
            &registered,
        )
    }

    /// How many dials discovery can start now: the handler's free ordinary
    /// connection slots, which leave the places held for preferred gateways.
    /// Each dial holds one slot from its start, or fails as a local limit
    /// without one.
    pub(crate) fn discovery_dial_permits(&self, ordinary_cap: usize) -> usize {
        self.supervisor
            .ip_slots
            .counts()
            .free_ordinary_connections(ordinary_cap.min(self.handler.limits.max_connections))
    }

    /// How many dials to preferred gateways can start now: every free
    /// connection slot, since a member that holds no slot takes a held
    /// place.
    pub(crate) fn preferred_dial_permits(&self) -> usize {
        self.handler
            .limits
            .total_connections()
            .saturating_sub(self.handler.supervisor.ip_slots.counts().connections())
    }

    /// How many address attempts dials to preferred gateways can start now:
    /// none without a free connection slot, otherwise the room under
    /// discovery's transport ceiling, which leaves the dial reserve to
    /// bootstrap and maintained dials.
    ///
    /// With `max_pending_handshakes` below its default of 32, that ceiling
    /// can leave fewer free attempts than places to fill while the inbound
    /// share is not full, so some held places may stay empty until inbound
    /// handshakes finish. The defaults leave room for all ten.
    pub(crate) fn preferred_dial_capacity(&self) -> usize {
        if self.preferred_dial_permits() == 0 {
            return 0;
        }
        self.quic.discovery_capacity()
    }

    /// Holds an inbound slot for the authenticated peer named by a dial-round test.
    #[cfg(test)]
    pub(crate) fn hold_inbound_gateway(&self, node: NodeId) -> ConnectionAdmissionGuard {
        self.handler
            .claim_connection(node, true)
            .expect("the test has a free inbound slot")
    }

    /// Holds `count` of the handler's connection slots, as connections with
    /// unrelated nodes, inbound or dialed, until the returned guards drop.
    #[cfg(test)]
    pub(crate) fn hold_connection_slots(
        &self,
        count: usize,
        inbound: bool,
    ) -> Vec<ConnectionAdmissionGuard> {
        (0..count)
            .map(|_| {
                self.handler
                    .claim_connection(NodeSecretKey::generate().public(), inbound)
                    .expect("the test holds free connection slots")
            })
            .collect()
    }

    /// Stop accepting, close every native connection and drain background tasks.
    pub async fn shutdown(&self) {
        if let Some(tasks) = &self.header_sync_tasks {
            tasks.shutdown.cancel();
            let mut tasks = tasks.tasks.lock().await;
            for mut task in tasks.drain(..) {
                if timeout(Duration::from_secs(1), &mut task).await.is_err() {
                    task.abort();
                    let _ = task.await;
                }
            }
        }
        self.supervisor.shutdown();
        self.handler.shutdown.cancel();
        self.quic.shutdown().await;
    }

    /// An endpoint over a bound `quic` endpoint and the `handler` that serves
    /// its connections, with no sync services attached.
    ///
    /// It uses the handler's supervisor, so registration and the endpoint's
    /// peer view can't diverge. It owns a background shutdown token, so
    /// [`Self::shutdown`] also stops dials started with
    /// [`Self::spawn_native_dial`].
    ///
    /// It doesn't serve inbound connections itself. Pass `handler` to
    /// `quic.serve` for those, and dial with [`Self::dial`].
    pub fn new(quic: QuicEndpoint, handler: ZakuraProtocolHandler) -> Self {
        Self {
            quic,
            supervisor: handler.supervisor.clone(),
            handler,
            header_sync: None,
            block_sync: None,
            header_sync_tasks: Some(Arc::new(HeaderSyncBackgroundTasks {
                shutdown: CancellationToken::new(),
                tasks: AsyncMutex::new(Vec::new()),
            })),
            header_sync_fatal_events: None,
            #[cfg(any(test, feature = "zakura-testkit"))]
            header_sync_actions: None,
            block_sync_actions: None,
            bpv1: None,
            upgrade_dials: Arc::new(StdMutex::new(HashMap::new())),
            upgrade_alpn_floors: Arc::new(StdMutex::new(HashMap::new())),
        }
    }

    /// Dials `node_addr` as a maintained dial and serves the connection under
    /// `limits` until it ends.
    ///
    /// Returns the dial, handshake, or serving error unchanged, such as a
    /// [`ZakuraHandlerError::QuicConnect`] that holds the transport's
    /// handshake timeout.
    pub async fn dial(
        &self,
        node_addr: NodeAddr,
        limits: &ZakuraLocalLimits,
    ) -> Result<(), ZakuraHandlerError> {
        serve_native_dial_connection(self, node_addr, limits).await
    }

    #[cfg(any(test, feature = "zakura-testkit"))]
    pub(crate) fn from_parts(
        quic: QuicEndpoint,
        supervisor: ZakuraSupervisorHandle,
        handler: ZakuraProtocolHandler,
    ) -> Self {
        // Test nodes keep their own supervisor and no background-task owner.
        Self {
            supervisor,
            header_sync_tasks: None,
            ..Self::new(quic, handler)
        }
    }

    #[cfg(any(test, feature = "zakura-testkit"))]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_parts_with_sync_services(
        quic: QuicEndpoint,
        supervisor: ZakuraSupervisorHandle,
        handler: ZakuraProtocolHandler,
        header_sync: super::HeaderSyncHandle,
        block_sync: BlockSyncHandle,
        shutdown: CancellationToken,
        tasks: Vec<JoinHandle<()>>,
        header_sync_actions: Option<mpsc::Receiver<HeaderSyncAction>>,
        block_sync_actions: Option<mpsc::Receiver<BlockSyncAction>>,
    ) -> Self {
        Self {
            quic,
            supervisor,
            handler,
            header_sync: Some(header_sync),
            block_sync: Some(block_sync),
            header_sync_tasks: Some(Arc::new(HeaderSyncBackgroundTasks {
                shutdown,
                tasks: AsyncMutex::new(tasks),
            })),
            header_sync_fatal_events: None,
            header_sync_actions: header_sync_actions
                .map(|actions| Arc::new(AsyncMutex::new(Some(actions)))),
            block_sync_actions: block_sync_actions
                .map(|actions| Arc::new(AsyncMutex::new(Some(actions)))),
            bpv1: None,
            upgrade_dials: Arc::new(StdMutex::new(HashMap::new())),
            upgrade_alpn_floors: Arc::new(StdMutex::new(HashMap::new())),
        }
    }
}

/// Shared supervisor handle for Zakura peer registration and outbound work.
#[derive(Clone, Debug)]
pub struct ZakuraSupervisorHandle {
    id: u64,
    inner: Arc<AsyncMutex<ZakuraSupervisorState>>,
    shutdown: CancellationToken,
    peer_set_tx: watch::Sender<Vec<ZakuraPeerId>>,
    /// The earlier IDs of nodes that this node's dials found under a new ID.
    /// A dial writes it before its connection registers, so a reader woken by
    /// `peer_set_tx` already sees the move behind a new registration.
    moved_tx: watch::Sender<MovedNodeIds>,
    registration_tx: broadcast::Sender<ZakuraConnectionRegistration>,
    pending_handoffs: Arc<StdMutex<HashMap<ZakuraPeerId, ZakuraHandoffId>>>,
    peer_registry: Option<PeerRegistry>,
    /// The same counts the supervisor state charges, read by the QUIC acceptor.
    ip_slots: IpSlots,
    max_connections_per_ip: usize,
    /// `tx-relay`'s live-session record, or `None` when `tx-relay` is not
    /// registered.
    tx_relay_sessions: Option<super::TxRelaySessions>,
    /// The next connection ID. Admission reserves one before it leases
    /// session memory, so the leases and the registration share it.
    next_registration_id: Arc<zakura_rt::sync::atomic::AtomicU64>,
}

/// Connections charged to each source IP: registered connections, and inbound
/// connections still in their control handshake (zakura-quic ADM-3 rule 2).
/// Each count is keyed by [`admission_host`], so an IPv6 /64 is one IP here.
///
/// A std mutex guards the counts, so the QUIC acceptor, which must not block,
/// reads them without the supervisor's async lock (zakura-quic ADM-4).
#[derive(Clone, Debug, Default)]
struct IpSlots(Arc<StdMutex<HandshakeCounts>>);

impl IpSlots {
    fn counts(&self) -> MutexGuard<'_, HandshakeCounts> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn registered(&self, ip: IpAddr) -> usize {
        self.counts().registered(ip)
    }

    #[cfg(test)]
    fn charged(&self, ip: IpAddr) -> usize {
        let counts = self.counts();
        counts.registered(ip).saturating_add(counts.handshaking(ip))
    }

    #[cfg(any(test, all(debug_assertions, not(zakura_sim))))]
    fn registered_total(&self) -> usize {
        self.counts().registered_total()
    }

    fn add_registered(&self, ip: IpAddr) {
        self.counts().add_registered(ip);
    }

    fn remove_registered(&self, ip: IpAddr) {
        self.counts().remove_registered(ip);
    }

    fn enter_handshake(&self, ip: IpAddr) -> ControlHandshakeGuard {
        self.counts().enter_handshake(ip);
        ControlHandshakeGuard {
            slots: self.clone(),
            ip,
            #[cfg(any(test, zakura_sim))]
            observation: None,
        }
    }
}

/// Charges one inbound control handshake to its IP until dropped.
#[derive(Debug)]
struct ControlHandshakeGuard {
    slots: IpSlots,
    ip: IpAddr,
    #[cfg(any(test, zakura_sim))]
    observation: Option<HandshakeObservation>,
}

#[cfg(any(test, zakura_sim))]
#[derive(Clone, Debug)]
struct HandshakeObservation {
    observer: Arc<dyn super::TransportObserver>,
    remote: SocketAddr,
    shared_id: [u8; 16],
    started: Instant,
}

#[cfg(any(test, zakura_sim))]
impl HandshakeObservation {
    fn released(&self, stage: &'static str, elapsed: Option<Duration>) {
        self.observer.observe(super::TransportEvent::Handshake {
            remote: self.remote,
            shared_id: self.shared_id,
            stage,
            validation: None,
            error: None,
            elapsed,
        });
    }
}

impl Drop for ControlHandshakeGuard {
    fn drop(&mut self) {
        self.slots.counts().leave_handshake(self.ip);
        #[cfg(any(test, zakura_sim))]
        if let Some(observation) = &self.observation {
            observation.released("released", Some(observation.started.elapsed()));
        }
    }
}

/// Releases the semaphore permits and their machine charge under the same lock.
struct PendingHandshakeGuard {
    slots: IpSlots,
    shared: Option<OwnedSemaphorePermit>,
    inbound: Option<OwnedSemaphorePermit>,
    #[cfg(any(test, zakura_sim))]
    observation: Option<HandshakeObservation>,
}

impl Drop for PendingHandshakeGuard {
    fn drop(&mut self) {
        let mut counts = self.slots.counts();
        let inbound = self.inbound.is_some();
        drop(self.shared.take());
        drop(self.inbound.take());
        counts.release_pending(inbound);
        #[cfg(any(test, zakura_sim))]
        if let Some(observation) = &self.observation {
            observation.released("pending-released", None);
        }
    }
}

/// Holds one connection slot of `node` in one direction, and releases
/// exactly that slot when it drops.
pub(crate) struct ConnectionAdmissionGuard {
    authenticated_dial: bool,
    slots: IpSlots,
    node: NodeId,
    inbound: bool,
    /// Ordinary slot limit for this caller, also enforced if TLS changes the slot class.
    ordinary_cap: usize,
    #[cfg(any(test, zakura_sim))]
    observation: Option<HandshakeObservation>,
}

impl ConnectionAdmissionGuard {
    /// The outbound QUIC connection has authenticated the node this guard's
    /// dial reached, `connected`. When its ID isn't the one the slot was
    /// claimed for, the guard holds its slot under that ID from here on.
    ///
    /// The limits apply to `connected` as they did to the claim. A refusal
    /// leaves the guard as it was, and the caller closes the connection.
    fn outbound_connected<'a>(
        &mut self,
        connected: impl Into<ProvenNode<'a>>,
    ) -> Result<(), SlotRefusal> {
        assert!(!self.inbound && !self.authenticated_dial);
        let connected = connected.into();
        self.slots
            .counts()
            .outbound_connected(self.node, connected, self.ordinary_cap)?;
        self.node = connected.id();
        self.authenticated_dial = true;
        Ok(())
    }
}

impl Drop for ConnectionAdmissionGuard {
    fn drop(&mut self) {
        if self.authenticated_dial {
            self.slots.counts().release_authenticated_dial(self.node);
        } else {
            self.slots
                .counts()
                .release_connection(self.node, self.inbound);
        }
        #[cfg(any(test, zakura_sim))]
        if let Some(observation) = &self.observation {
            observation.released("connection-released", None);
        }
    }
}

/// One exact authenticated connection generation published by the supervisor.
#[derive(Clone, Debug, Eq, PartialEq)]
struct ZakuraConnectionRegistration {
    peer_id: ZakuraPeerId,
    conn_id: ZakuraConnId,
}

/// Monotonic ownership token for one pending legacy-to-native handoff.
type ZakuraHandoffId = u64;

/// Registration events can burst during startup. A lagged handoff fails closed
/// instead of attaching metadata to an unobserved connection generation.
const ZAKURA_REGISTRATION_EVENT_CAPACITY: usize = 1024;

/// Waits for the first new authenticated generation of one peer.
pub(crate) struct ZakuraConnectionRegistrationWait {
    peer_id: ZakuraPeerId,
    handoff_id: ZakuraHandoffId,
    registrations: broadcast::Receiver<ZakuraConnectionRegistration>,
    pending_handoffs: Arc<StdMutex<HashMap<ZakuraPeerId, ZakuraHandoffId>>>,
}

/// Result of reserving a peer identity for one legacy-to-native handoff.
pub(crate) enum ZakuraConnectionRegistrationWaitStart {
    /// This handoff owns the next registration for the peer identity.
    Waiting(ZakuraConnectionRegistrationWait),
    /// An active connection or another handoff already owns the peer identity.
    Duplicate,
    /// The supervisor cannot allocate another handoff identifier.
    Unavailable,
}

impl ZakuraConnectionRegistrationWait {
    /// Returns the exact registered generation, or `None` if the wait cannot
    /// observe one within `registration_timeout`.
    pub(crate) async fn wait(mut self, registration_timeout: Duration) -> Option<ZakuraConnId> {
        timeout(registration_timeout, async {
            loop {
                match self.registrations.recv().await {
                    Ok(registration) if registration.peer_id == self.peer_id => {
                        return Some(registration.conn_id);
                    }
                    Ok(_) => {}
                    Err(broadcast::error::RecvError::Lagged(_))
                    | Err(broadcast::error::RecvError::Closed) => return None,
                }
            }
        })
        .await
        .unwrap_or(None)
    }
}

impl Drop for ZakuraConnectionRegistrationWait {
    fn drop(&mut self) {
        let peer_id = self.peer_id.clone();
        let handoff_id = self.handoff_id;
        let mut pending_handoffs = self
            .pending_handoffs
            .lock()
            .expect("Zakura pending handoff mutex is never poisoned");
        // A late drop cannot release a newer handoff reservation.
        if pending_handoffs.get(&peer_id) == Some(&handoff_id) {
            pending_handoffs.remove(&peer_id);
        }
    }
}

static NEXT_SUPERVISOR_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
struct ZakuraSupervisorState {
    supervisor: ZakuraPeerSupervisor,
    /// Each peer's current registration generation.
    active_by_peer: HashMap<ZakuraPeerId, ZakuraPeerConnectionEntry>,
    ip_slots: IpSlots,
    /// The [`admission_host`] each live registration charged in `ip_slots`.
    /// A replaced registration keeps its charge until its own cleanup
    /// deregisters it, because its connection still runs until then.
    ip_charges: HashMap<ZakuraConnId, IpAddr>,
    next_handoff_id: ZakuraHandoffId,
    max_connections_per_ip: usize,
}

#[derive(Debug)]
struct ZakuraPeerConnectionEntry {
    /// Monotonic supervisor registration generation used by services to ignore
    /// stale add/remove work from a superseded connection.
    conn_id: ZakuraConnId,
    outbound_handle: ZakuraPeerHandle,
    disconnect_token: CancellationToken,
    /// The connection's close outcome, which a tampered body records.
    close_cause: CloseCause,
    registered_at: Instant,
}

impl ZakuraSupervisorState {
    /// The [`admission_host`] that `conn_id` is charged to, if its IP is known.
    fn host(&self, conn_id: ZakuraConnId) -> Option<IpAddr> {
        self.ip_charges.get(&conn_id).copied()
    }

    /// Charges registration `conn_id` to `host` until [`Self::release_ip`].
    fn charge_ip(&mut self, conn_id: ZakuraConnId, host: Option<IpAddr>) {
        if let Some(host) = host {
            self.ip_charges.insert(conn_id, host);
            self.ip_slots.add_registered(host);
        }
    }

    /// Returns the per-IP charge of registration `conn_id`, whether or not it
    /// is still its peer's current generation. A second call releases nothing.
    fn release_ip(&mut self, conn_id: ZakuraConnId) {
        if let Some(host) = self.ip_charges.remove(&conn_id) {
            self.ip_slots.remove_registered(host);
        }
    }

    /// Checks, in debug builds, that the per-IP registered totals match the
    /// live registrations with known IPs.
    ///
    /// Simulator builds skip it. Its `IpSlots` lock is a scheduler point
    /// there, so a debug-only lock would give debug and release builds
    /// different schedules for the same seed.
    #[cfg(all(debug_assertions, not(zakura_sim)))]
    fn debug_assert_accounting(&self) {
        debug_assert_eq!(
            self.ip_slots.registered_total(),
            self.ip_charges.len(),
            "Zakura per-IP registered totals must match live registrations with known IPs",
        );
    }

    #[cfg(any(not(debug_assertions), zakura_sim))]
    fn debug_assert_accounting(&self) {}
}

/// Queue-backed outbound send handle for one authenticated Zakura peer.
#[derive(Clone, Debug)]
pub struct ZakuraPeerHandle {
    peer_id: ZakuraPeerId,
    sender: mpsc::Sender<ZakuraOutboundFrame>,
}

impl ZakuraPeerHandle {
    #[cfg(test)]
    pub(crate) fn new_for_tests(
        peer_id: ZakuraPeerId,
        sender: mpsc::Sender<ZakuraOutboundFrame>,
    ) -> Self {
        Self { peer_id, sender }
    }

    /// Authenticated peer id for this handle.
    pub fn peer_id(&self) -> &ZakuraPeerId {
        &self.peer_id
    }

    fn has_outbound_capacity(&self) -> bool {
        self.sender.capacity() > 0
    }

    /// Open a request stream, write one frame, then return its validated items.
    ///
    /// `started` is described on [`ZakuraOutboundFrame::Request`]. The returned
    /// value keeps its serialized-byte grant until it drops. The node's resource
    /// envelope counts the compatibility client's requests, so another caller
    /// bounds the values it keeps.
    pub async fn request(
        &self,
        stream_kind: u16,
        request_id: u64,
        message_type: u16,
        flags: u16,
        payload: Vec<u8>,
        started: Option<tokio::sync::oneshot::Sender<()>>,
    ) -> Result<ResponseFrames, BoxError> {
        self.request_frame(
            stream_kind,
            request_id,
            Frame {
                message_type,
                flags,
                payload,
            },
            started,
            None,
        )
        .await
    }

    /// Hand a request to the peer once its queue has space. Reserving first lets a
    /// competing download finish during backpressure without sending a stale request.
    pub(super) async fn request_frame(
        &self,
        stream_kind: u16,
        request_id: u64,
        frame: Frame,
        started: Option<tokio::sync::oneshot::Sender<()>>,
        attempt: Option<&crate::BlockHedge>,
    ) -> Result<ResponseFrames, BoxError> {
        let Frame {
            message_type,
            flags,
            payload,
        } = frame;
        let (completion, completed) = oneshot::channel();
        let frame = ZakuraOutboundFrame::Request {
            stream_kind,
            request_id,
            message_type,
            flags,
            payload,
            started,
            completion,
        };
        let slot = self
            .sender
            .reserve()
            .await
            .map_err(|_| -> BoxError { "Zakura outbound peer queue closed".into() })?;
        match attempt {
            Some(attempt) => attempt.dispatch_if_needed(None, None, || slot.send(frame))?,
            None => slot.send(frame),
        }
        completed
            .await
            .map_err(|_| -> BoxError { "Zakura outbound completion dropped".into() })?
    }
}

/// A compatibility response's validated items and their serialized-byte grant.
///
/// Completed inventory items replace their raw chunks. Decoded payloads, as in
/// the caller's pipeline, are outside the serialized-byte grant.
#[derive(Debug)]
pub struct ResponseFrames {
    items: Vec<LegacyResponseItem>,
    grant: CompatResponseGrant,
}

impl ResponseFrames {
    /// Split the response items from their serialized-byte grant.
    pub(super) fn into_parts(self) -> (Vec<LegacyResponseItem>, CompatResponseGrant) {
        (self.items, self.grant)
    }

    /// A scripted peer's frames under a real zero-byte grant.
    #[cfg(test)]
    pub(crate) async fn with_zero_grant_for_tests(frames: Vec<Frame>) -> Self {
        Self {
            items: frames.into_iter().map(Into::into).collect(),
            grant: ConnectionResources::unconstrained(&[])
                .admit_compat_response(0)
                .await,
        }
    }
}

/// Request/response outbound work owned by a connection-serving task.
#[derive(Debug)]
pub enum ZakuraOutboundFrame {
    /// Compatibility request frame expecting a response on the same stream.
    Request {
        /// Application stream kind to open.
        stream_kind: u16,
        /// Request id written into the stream prelude.
        request_id: u64,
        /// Application message type.
        message_type: u16,
        /// Message flags.
        flags: u16,
        /// Message payload bytes.
        payload: Vec<u8>,
        /// Fired when the connection takes this request off its queue and
        /// starts the exchange, since it serves one request at a time.
        ///
        /// Only legacy gossip sets it, and legacy code stays on tokio, so this
        /// is tokio's sender in both builds.
        started: Option<tokio::sync::oneshot::Sender<()>>,
        /// Completion sent with validated response items or an error.
        completion: oneshot::Sender<Result<ResponseFrames, BoxError>>,
    },
}

impl ZakuraSupervisorHandle {
    /// Register `handle` as a connected legacy-gossip peer.
    #[cfg(test)]
    pub(crate) async fn register_handle_for_tests(&self, handle: ZakuraPeerHandle) {
        let registration = self
            .register(
                1,
                handle.peer_id().clone(),
                &[],
                Some("192.0.2.6".parse().expect("test IP parses")),
                [6; TRANSCRIPT_HASH_BYTES],
                handle,
                CancellationToken::new(),
                CloseCause::new(),
                super::ZAKURA_CAP_LEGACY_GOSSIP,
            )
            .await;
        assert!(matches!(
            registration,
            ZakuraRegistration::Registered { .. }
        ));
    }

    /// Create an empty supervisor.
    pub fn new(max_connections_per_ip: usize) -> Self {
        Self::new_inner(max_connections_per_ip, None)
    }

    pub(crate) fn new_with_peer_registry(
        max_connections_per_ip: usize,
        peer_registry: PeerRegistry,
    ) -> Self {
        Self::new_inner(max_connections_per_ip, Some(peer_registry))
    }

    fn new_inner(max_connections_per_ip: usize, peer_registry: Option<PeerRegistry>) -> Self {
        let (registration_tx, _) = broadcast::channel(ZAKURA_REGISTRATION_EVENT_CAPACITY);
        let ip_slots = IpSlots::default();
        let max_connections_per_ip = max_connections_per_ip.max(1);
        Self {
            id: NEXT_SUPERVISOR_ID.fetch_add(1, Ordering::Relaxed),
            inner: Arc::new(AsyncMutex::new(ZakuraSupervisorState {
                supervisor: ZakuraPeerSupervisor::default(),
                active_by_peer: HashMap::new(),
                ip_slots: ip_slots.clone(),
                ip_charges: HashMap::new(),
                next_handoff_id: 1,
                max_connections_per_ip,
            })),
            shutdown: CancellationToken::new(),
            peer_set_tx: watch::channel(Vec::new()).0,
            moved_tx: watch::channel(MovedNodeIds::default()).0,
            registration_tx,
            pending_handoffs: Arc::new(StdMutex::new(HashMap::new())),
            peer_registry,
            ip_slots,
            max_connections_per_ip,
            tx_relay_sessions: None,
            next_registration_id: Arc::new(zakura_rt::sync::atomic::AtomicU64::new(1)),
        }
    }

    /// Reserve the next connection ID, or `None` once IDs run out.
    // Rust 1.99 deprecates std's `fetch_update` for `try_update`, which the
    // 1.97 MSRV lacks. The simulator's atomic keeps its own `fetch_update`.
    #[allow(deprecated)]
    fn reserve_conn_id(&self) -> Option<ZakuraConnId> {
        self.next_registration_id
            .fetch_update(
                zakura_rt::sync::atomic::Ordering::Relaxed,
                zakura_rt::sync::atomic::Ordering::Relaxed,
                |id| (id < u64::MAX).then(|| id + 1),
            )
            .ok()
    }

    /// This supervisor with `tx-relay`'s live-session record, which the
    /// legacy adapters built from it read. Set it before cloning the handle.
    pub(crate) fn with_tx_relay_sessions(
        mut self,
        sessions: Option<super::TxRelaySessions>,
    ) -> Self {
        self.tx_relay_sessions = sessions;
        self
    }

    /// `tx-relay`'s live-session record, or `None` when `tx-relay` is not
    /// registered.
    pub(crate) fn tx_relay_sessions(&self) -> Option<&super::TxRelaySessions> {
        self.tx_relay_sessions.as_ref()
    }

    /// Counts an inbound control handshake against `remote_ip` until the guard
    /// drops at registration (zakura-quic ADM-3 rule 2).
    fn enter_control_handshake(&self, remote_ip: IpAddr) -> ControlHandshakeGuard {
        self.ip_slots.enter_handshake(admission_host(remote_ip))
    }

    /// Returns the currently registered authenticated Zakura peer ids.
    pub async fn registered_ids(&self) -> Vec<ZakuraPeerId> {
        let state = self.inner.lock().await;
        state.active_by_peer.keys().cloned().collect()
    }

    pub(crate) fn id(&self) -> u64 {
        self.id
    }

    /// Returns queue-backed handles for peers currently able to accept outbound work.
    ///
    /// A peer whose disconnect has been requested stays in `active_by_peer`
    /// until its connection task deregisters it, so it is skipped here.
    pub async fn outbound_peer_handles(&self) -> Vec<ZakuraPeerHandle> {
        let state = self.inner.lock().await;
        state
            .active_by_peer
            .values()
            .filter(|entry| !entry.disconnect_token.is_cancelled())
            .map(|entry| &entry.outbound_handle)
            .filter(|handle| handle.has_outbound_capacity())
            .cloned()
            .collect()
    }

    /// Subscribe to peer-set changes for event-driven tests and diagnostics.
    pub fn subscribe(&self) -> watch::Receiver<Vec<ZakuraPeerId>> {
        self.peer_set_tx.subscribe()
    }

    /// The node IDs that this node's dials found their nodes have moved on
    /// from.
    pub(crate) fn moved_node_ids(&self) -> watch::Receiver<MovedNodeIds> {
        self.moved_tx.subscribe()
    }

    /// The registered peers, for a caller that asks whether a node it knows
    /// by ID is registered. Unlike [`Self::subscribe`]'s peer set, it finds a
    /// node that has moved on from that ID.
    pub(crate) fn registered_peers(&self) -> RegisteredPeers {
        RegisteredPeers::new(self.peer_set_tx.subscribe(), self.moved_tx.subscribe())
    }

    /// Records that a dial to `dialed` reached the node whose ID is now
    /// `connected`.
    pub(super) fn learn_move(&self, dialed: NodeId, connected: NodeId) {
        metrics::counter!("zakura.p2p.conn.dial.moved").increment(1);
        let news = self
            .moved_tx
            .send_if_modified(|moved| moved.learn(dialed, connected));
        if news {
            info!(
                %dialed,
                connected_as = %connected,
                "A Zakura peer answered a dial to its earlier node ID under a new one"
            );
        }
    }

    /// Whether a connection of `peer_id` is registered now.
    pub(crate) fn is_registered(&self, peer_id: &ZakuraPeerId) -> bool {
        self.peer_set_tx.borrow().contains(peer_id)
    }

    /// Start an atomic wait for the next connection generation of `peer_id`.
    ///
    /// The returned wait observes registrations that occur after this call.
    /// The supervisor lock makes the ownership checks and event subscription
    /// atomic with connection registration.
    pub(crate) async fn begin_connection_registration_wait(
        &self,
        peer_id: &ZakuraPeerId,
    ) -> ZakuraConnectionRegistrationWaitStart {
        let mut state = self.inner.lock().await;
        let mut pending_handoffs = self
            .pending_handoffs
            .lock()
            .expect("Zakura pending handoff mutex is never poisoned");
        if state.active_by_peer.contains_key(peer_id) || pending_handoffs.contains_key(peer_id) {
            return ZakuraConnectionRegistrationWaitStart::Duplicate;
        }
        if state.next_handoff_id == u64::MAX {
            return ZakuraConnectionRegistrationWaitStart::Unavailable;
        }
        let handoff_id = state.next_handoff_id;
        state.next_handoff_id += 1;
        pending_handoffs.insert(peer_id.clone(), handoff_id);
        let registrations = self.registration_tx.subscribe();
        ZakuraConnectionRegistrationWaitStart::Waiting(ZakuraConnectionRegistrationWait {
            peer_id: peer_id.clone(),
            handoff_id,
            registrations,
            pending_handoffs: self.pending_handoffs.clone(),
        })
    }

    /// Apply the connection policy for one block-sync violation.
    ///
    /// Violations are record-only, except a body that proves tampering (see
    /// [`crate::zakura::BlockSyncMisbehavior::proves_tampered_body`]), which disconnects
    /// the peer.
    /// Returns whether this call closed an active connection that wasn't
    /// already closing.
    pub async fn enforce_block_sync_misbehavior(
        &self,
        peer_id: &ZakuraPeerId,
        conn_id: ZakuraConnId,
        reason: &crate::zakura::BlockSyncMisbehavior,
    ) -> bool {
        if !reason.proves_tampered_body() {
            return false;
        }
        let disconnected = {
            let state = self.inner.lock().await;
            state
                .active_by_peer
                .get(peer_id)
                .filter(|entry| entry.conn_id == conn_id && !entry.disconnect_token.is_cancelled())
                .is_some_and(|entry| {
                    entry.close_cause.record(CloseOutcome::fault(
                        ZAKURA_STREAM_BLOCK_SYNC,
                        ZAKURA_REGULATED_BLOCK_SYNC_STREAM_VERSION,
                        CloseRow::BSYNC_CLOSE_18,
                    ));
                    entry.disconnect_token.cancel();
                    true
                })
        };
        if disconnected {
            metrics::counter!("sync.block.peer.tampered_body_disconnect").increment(1);
        }
        info!(
            ?peer_id,
            conn_id,
            ?reason,
            disconnected,
            "recorded tampered block body verdict for Zakura connection"
        );
        disconnected
    }

    /// Disconnect one active Zakura peer.
    pub async fn disconnect_peer(&self, peer_id: &ZakuraPeerId) -> bool {
        let token = {
            let state = self.inner.lock().await;
            state
                .active_by_peer
                .get(peer_id)
                .map(|entry| entry.disconnect_token.clone())
        };

        if let Some(token) = token {
            token.cancel();
            true
        } else {
            false
        }
    }

    /// Registers the authenticated connection of `peer_id` under a fresh
    /// connection ID. See [`Self::register_with_reserved_conn_id`].
    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    async fn register(
        &self,
        _trace_conn_id: ZakuraConnId,
        peer_id: ZakuraPeerId,
        proven_earlier: &[NodeId],
        remote_ip: Option<IpAddr>,
        transcript_hash: [u8; TRANSCRIPT_HASH_BYTES],
        outbound_handle: ZakuraPeerHandle,
        disconnect_token: CancellationToken,
        close_cause: CloseCause,
        accepted_capabilities: u64,
    ) -> ZakuraRegistration {
        self.register_with_reserved_conn_id(
            self.reserve_conn_id(),
            peer_id,
            proven_earlier,
            remote_ip,
            transcript_hash,
            outbound_handle,
            disconnect_token,
            close_cause,
            accepted_capabilities,
        )
        .await
    }

    /// Registers the authenticated connection of `peer_id` under `conn_id`,
    /// which [`Self::reserve_conn_id`] reserved. `None` means IDs ran out. The
    /// handshake also proved the node IDs `proven_earlier` that the peer had
    /// before it added keys (zakura-quic TLS-7).
    #[allow(clippy::too_many_arguments)]
    async fn register_with_reserved_conn_id(
        &self,
        conn_id: Option<ZakuraConnId>,
        peer_id: ZakuraPeerId,
        proven_earlier: &[NodeId],
        remote_ip: Option<IpAddr>,
        transcript_hash: [u8; TRANSCRIPT_HASH_BYTES],
        outbound_handle: ZakuraPeerHandle,
        disconnect_token: CancellationToken,
        close_cause: CloseCause,
        _accepted_capabilities: u64,
    ) -> ZakuraRegistration {
        // Discovery matches the registered IP against advertised addresses,
        // so it keeps the address itself. Only accounting uses the host.
        let remote_ip = remote_ip.map(canonical_ip);
        let host = remote_ip.map(admission_host);
        let mut state = self.inner.lock().await;
        // A re-registration for a peer id that is already active from the same
        // IP is a duplicate redial, not a new per-IP allocation: the incumbent
        // already holds that IP slot, and the duplicate branch below either
        // keeps the incumbent or evicts a stale incumbent through its normal
        // cleanup path. Exempting only same-IP duplicates lets a dead incumbent
        // be evicted in milliseconds rather than blocking the peer until the
        // QUIC idle timeout (~150s), while still enforcing the target IP bucket
        // for a winning replacement that moved to a different full IP.
        let same_ip_duplicate_redial = state
            .active_by_peer
            .get(&peer_id)
            .is_some_and(|entry| state.host(entry.conn_id) == host);
        if let Some(host) = host {
            if !state.ip_slots.counts().can_register(
                host,
                state.max_connections_per_ip,
                same_ip_duplicate_redial,
            ) {
                metrics::counter!("zakura.p2p.conn.rejected.admission").increment(1);
                return ZakuraRegistration::Rejected(ZakuraRejectReason::ResourceLimit);
            }
        }
        let Some(conn_id) = conn_id else {
            metrics::counter!("zakura.p2p.conn.rejected.admission").increment(1);
            return ZakuraRegistration::Rejected(ZakuraRejectReason::TemporaryUnavailable);
        };

        match state
            .supervisor
            .register_authenticated(peer_id.clone(), transcript_hash)
        {
            AuthenticatedPeerRegistration::Registered => {
                self.pending_handoffs
                    .lock()
                    .expect("Zakura pending handoff mutex is never poisoned")
                    .remove(&peer_id);
                let entry = ZakuraPeerConnectionEntry {
                    conn_id,
                    outbound_handle,
                    disconnect_token,
                    close_cause,
                    registered_at: Instant::now(),
                };
                // The replaced registration keeps its per-IP charge until its
                // own cleanup guard deregisters it, because its connection
                // still runs until then.
                if let Some(old_entry) = state.active_by_peer.insert(peer_id.clone(), entry) {
                    old_entry.disconnect_token.cancel();
                    metrics::counter!("zakura.p2p.conn.duplicate.evicted_upgraded").increment(1);
                }
                state.charge_ip(conn_id, host);
                state.debug_assert_accounting();
                // A node registered under an ID has not moved on from it,
                // whatever an earlier dial found.
                if let Some(node_id) = peer_id.node_id() {
                    self.moved_tx
                        .send_if_modified(|moved| moved.forget(&node_id));
                }
                let registered_ids: Vec<_> = state.active_by_peer.keys().cloned().collect();
                set_active_connection_gauge(registered_ids.len());
                self.peer_set_tx.send_replace(registered_ids);
                if let Some(peer_registry) = &self.peer_registry {
                    peer_registry.register_native_connection(
                        peer_id.clone(),
                        conn_id,
                        remote_ip,
                        proven_earlier,
                    );
                }
                let _ = self.registration_tx.send(ZakuraConnectionRegistration {
                    peer_id: peer_id.clone(),
                    conn_id,
                });
                let disconnect_token = state
                    .active_by_peer
                    .get(&peer_id)
                    .map(|entry| entry.disconnect_token.clone())
                    .expect("disconnect token exists because this peer was just registered");
                ZakuraRegistration::Registered {
                    conn_id,
                    peer_id,
                    remote_ip,
                    disconnect_token,
                }
            }
            AuthenticatedPeerRegistration::Duplicate => {
                // A duplicate for an identity that already has a connection is
                // almost always a restart or redial. If the incumbent has been
                // registered long enough to be a stale connection left behind by
                // a restarted peer, cancel it now so it tears down through its
                // normal cleanup path and frees the slot in milliseconds; the
                // peer's redial then takes the freed slot instead of waiting for
                // the dead connection's QUIC idle timeout (~150s). A young
                // incumbent is kept to avoid flapping on simultaneous-open races.
                // The newcomer is still closed (its redial reconnects cleanly
                // once the slot is free), which avoids racing the incumbent's
                // service-registration teardown.
                //
                // A same-IP redial is almost always a restarted peer reclaiming
                // its own slot, so use a much smaller eviction gate for it: this
                // is the fast path the per-IP admission exemption above is meant
                // to enable, so a restarted peer recovers in seconds instead of
                // being locked out until the incumbent's QUIC idle timeout.
                let evict_min_age = if same_ip_duplicate_redial {
                    ZAKURA_SAME_IP_DUPLICATE_EVICT_MIN_AGE
                } else {
                    ZAKURA_DUPLICATE_EVICT_MIN_AGE
                };
                if let Some(entry) = state.active_by_peer.get(&peer_id) {
                    if entry.registered_at.elapsed() >= evict_min_age
                        && !entry.disconnect_token.is_cancelled()
                    {
                        entry.disconnect_token.cancel();
                        metrics::counter!("zakura.p2p.conn.duplicate.evicted_stale").increment(1);
                    }
                }
                ZakuraRegistration::Duplicate { peer_id }
            }
        }
    }

    /// Releases registration `conn_id`'s per-IP charge, and removes it from
    /// `peer_id`'s current generation if it still is that generation.
    async fn deregister(&self, peer_id: &ZakuraPeerId, conn_id: ZakuraConnId) {
        let mut state = self.inner.lock().await;
        state.release_ip(conn_id);
        if state
            .active_by_peer
            .get(peer_id)
            .is_none_or(|entry| entry.conn_id != conn_id)
        {
            state.debug_assert_accounting();
            return;
        }
        state.active_by_peer.remove(peer_id);
        state.supervisor.deregister_authenticated(peer_id);
        state.debug_assert_accounting();
        let registered_ids: Vec<_> = state.active_by_peer.keys().cloned().collect();
        set_active_connection_gauge(registered_ids.len());
        self.peer_set_tx.send_replace(registered_ids);
        if let Some(peer_registry) = &self.peer_registry {
            peer_registry.deregister_native_connection(peer_id, conn_id);
        }
    }

    fn forget_retained_native_metadata(&self, peer_id: &ZakuraPeerId) {
        if let Some(peer_registry) = &self.peer_registry {
            peer_registry.forget_retained_native_metadata(peer_id);
        }
    }

    fn shutdown(&self) {
        self.shutdown.cancel();
    }

    /// Returns whether another connection from `remote_ip` would stay within the
    /// per-IP cap, counting `in_flight_count` dials this caller already has in
    /// flight to that IP. Used by the discovery dialer to reserve per-IP slots
    /// before launching a candidate dial.
    pub(crate) fn can_accept_remote_ip_with_in_flight(
        &self,
        remote_ip: IpAddr,
        in_flight_count: usize,
    ) -> bool {
        self.ip_slots
            .registered(admission_host(remote_ip))
            .saturating_add(in_flight_count)
            < self.max_connections_per_ip
    }
}

fn set_active_connection_gauge(active_connections: usize) {
    // Active Zakura connections are bounded by the configured connection limit,
    // far below f64's exact integer range.
    metrics::gauge!("zakura.p2p.conn.active").set(active_connections as f64);
}

#[derive(Debug)]
enum ZakuraRegistration {
    Registered {
        conn_id: ZakuraConnId,
        peer_id: ZakuraPeerId,
        remote_ip: Option<IpAddr>,
        disconnect_token: CancellationToken,
    },
    Duplicate {
        peer_id: ZakuraPeerId,
    },
    Rejected(ZakuraRejectReason),
}

/// Ends one registration when its connection ends: it cancels the connection,
/// removes its service admissions and deregisters it. Deregistration returns
/// the registration's per-IP charge even after a replacement took its peer's
/// current generation.
#[derive(Debug)]
struct RegisteredPeerCleanupGuard {
    supervisor: ZakuraSupervisorHandle,
    registry: Arc<ServiceRegistry>,
    peer_id: ZakuraPeerId,
    conn_id: ZakuraConnId,
    disconnect_token: CancellationToken,
    admitted_capabilities: u64,
    session_funding: Option<Arc<SessionFunding>>,
    armed: bool,
}

impl RegisteredPeerCleanupGuard {
    fn new(
        supervisor: ZakuraSupervisorHandle,
        registry: Arc<ServiceRegistry>,
        peer_id: ZakuraPeerId,
        conn_id: ZakuraConnId,
        disconnect_token: CancellationToken,
    ) -> Self {
        Self {
            supervisor,
            registry,
            peer_id,
            conn_id,
            disconnect_token,
            admitted_capabilities: 0,
            session_funding: None,
            armed: true,
        }
    }

    fn with_session_funding(mut self, session_funding: Option<Arc<SessionFunding>>) -> Self {
        self.session_funding = session_funding;
        self
    }

    /// Return the connection's session leases. Session workers may still hold
    /// the funding, so this releases its leases rather than dropping it.
    fn release_session_funding(&mut self) {
        if let Some(funding) = self.session_funding.take() {
            funding.release();
        }
    }

    fn add_admitted_capabilities(&mut self, capabilities: u64) {
        self.admitted_capabilities |= capabilities;
    }

    fn remove_admitted_services(&mut self) {
        if self.admitted_capabilities != 0 {
            self.registry
                .remove_peer(&self.peer_id, self.conn_id, self.admitted_capabilities);
            self.admitted_capabilities = 0;
        }
    }

    async fn cleanup_registered_peer(mut self) {
        if self.armed {
            self.disconnect_token.cancel();
            self.remove_admitted_services();
            self.supervisor
                .deregister(&self.peer_id, self.conn_id)
                .await;
            self.release_session_funding();
            self.armed = false;
        }
    }
}

impl Drop for RegisteredPeerCleanupGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }

        self.disconnect_token.cancel();
        self.remove_admitted_services();
        self.release_session_funding();

        let supervisor = self.supervisor.clone();
        let peer_id = self.peer_id.clone();
        let conn_id = self.conn_id;
        let _task = zakura_rt::task::try_spawn(erased(async move {
            supervisor.deregister(&peer_id, conn_id).await;
        }));
    }
}

/// Why [`SessionFunding`] refused a connection or a session.
#[derive(Debug, Error)]
enum SessionFundingRefusal {
    /// Connections in this direction hold all the sessions share they may
    /// lease: inbound ones stop short of the dial reserve (`02 §8` rule 1).
    #[error("connections in this direction hold their part of the sessions share")]
    DirectionFull,
    /// The pool refused the lease.
    #[error("the memory pool refused the lease: {0:?}")]
    Pool(super::memory::LeaseDenied),
    /// The connection released its funding before the session arrived.
    #[error("the connection released its session leases")]
    Released,
}

/// One connection's `Consumer::Sessions` lease (`02 §8`): the bases its
/// admission funded, plus each reactor's session part that a service
/// admitted later. Releasing it, or dropping the last handle, returns the
/// lease and the connection's charge in the direction ledger.
#[derive(Debug)]
struct SessionFunding {
    directions: Arc<zakura_rt::sync::Mutex<super::memory::SessionDirections>>,
    bases: super::memory::SessionBases,
    sessions_share: u64,
    side: super::memory::SessionSide,
    held: zakura_rt::sync::Mutex<HeldSessionLeases>,
}

#[derive(Debug)]
struct HeldSessionLeases {
    /// The connection's one lease. Each later session part grows it.
    lease: super::memory::PoolLease,
    /// The reactor capabilities whose session parts the lease holds.
    funded_sessions: u64,
    /// Bytes this connection charged to the direction ledger.
    charged: u64,
    released: bool,
}

impl SessionFunding {
    /// Fund `cost` bytes, which hold the session parts of `funded_sessions`,
    /// for connection `conn_id` on `side`: charge the direction ledger,
    /// then lease every base together from the pool, waiting up to
    /// `wait_timeout` for borrowed bytes to return.
    #[allow(clippy::too_many_arguments)]
    async fn acquire(
        pool: &super::memory::MemoryPool,
        plan: &super::memory::MemoryPlan,
        directions: &Arc<zakura_rt::sync::Mutex<super::memory::SessionDirections>>,
        conn_id: ZakuraConnId,
        side: super::memory::SessionSide,
        cost: u64,
        funded_sessions: u64,
        shutdown: &CancellationToken,
        wait_timeout: Duration,
    ) -> Result<Self, SessionFundingRefusal> {
        if !lock_session_directions(directions).try_charge(side, cost, plan.sessions_share) {
            return Err(SessionFundingRefusal::DirectionFull);
        }
        // From here, dropping `funding` returns the charge.
        let funding = Self {
            directions: directions.clone(),
            bases: plan.bases,
            sessions_share: plan.sessions_share,
            side,
            held: zakura_rt::sync::Mutex::new(HeldSessionLeases {
                lease: pool.empty_lease(super::memory::Consumer::Sessions, Some(conn_id)),
                funded_sessions,
                charged: cost,
                released: false,
            }),
        };
        let lease = pool
            .acquire_session_lease(conn_id, cost, shutdown, wait_timeout)
            .await
            .map_err(SessionFundingRefusal::Pool)?;
        funding.lock_held().lease = lease;
        Ok(funding)
    }

    /// Fund the session part of the reactor that `capability` names, after
    /// its service admitted a session that admission didn't fund. A
    /// connection funds each reactor once and keeps it until it closes.
    fn fund_session(&self, capability: u64) -> Result<(), SessionFundingRefusal> {
        let mut held = self.lock_held();
        if held.released {
            return Err(SessionFundingRefusal::Released);
        }
        let Some((reactor, bytes)) = self.bases.session_top_up(held.funded_sessions, capability)
        else {
            return Ok(());
        };
        if !lock_session_directions(&self.directions).try_charge(
            self.side,
            bytes,
            self.sessions_share,
        ) {
            return Err(SessionFundingRefusal::DirectionFull);
        }
        let grown = held.lease.bytes().saturating_add(bytes);
        match held.lease.try_grow_to(grown) {
            Ok(()) => {
                held.funded_sessions |= reactor;
                held.charged = held.charged.saturating_add(bytes);
                Ok(())
            }
            Err(denied) => {
                lock_session_directions(&self.directions).release(self.side, bytes);
                Err(SessionFundingRefusal::Pool(denied))
            }
        }
    }

    /// Return the lease and the direction charge. Later calls do nothing.
    fn release(&self) {
        let mut held = self.lock_held();
        held.released = true;
        held.lease.shrink_to(0);
        let charged = std::mem::take(&mut held.charged);
        drop(held);
        lock_session_directions(&self.directions).release(self.side, charged);
    }

    fn lock_held(&self) -> zakura_rt::sync::MutexGuard<'_, HeldSessionLeases> {
        self.held
            .lock()
            .unwrap_or_else(zakura_rt::sync::PoisonError::into_inner)
    }
}

impl Drop for SessionFunding {
    fn drop(&mut self) {
        self.release();
    }
}

fn lock_session_directions(
    directions: &zakura_rt::sync::Mutex<super::memory::SessionDirections>,
) -> zakura_rt::sync::MutexGuard<'_, super::memory::SessionDirections> {
    directions
        .lock()
        .unwrap_or_else(zakura_rt::sync::PoisonError::into_inner)
}

struct IncomingStreamSetup {
    io: SetupIo,
    permit: OwnedSemaphorePermit,
    /// Created with the permit, so this future's first suspend finds it
    /// counted. The stream's gates follow once its prelude names its kind.
    owner: OwnerGuard,
    stream_id: u64,
    stream: Stream,
    prelude: StreamPrelude,
    session: Option<(SessionLayout, u64)>,
}

struct StreamAdmission<'a> {
    is_initiator: bool,
    direction: ServicePeerDirection,
    conn: ZakuraConnTrace,
    peer_id: &'a ZakuraPeerId,
    stream_sem: &'a Arc<Semaphore>,
    resources: &'a ConnectionResources,
    open_limiter: &'a mut TokenBucket,
    message_buckets: &'a mut MessageRateBuckets,
    workers: &'a mut JoinSet<()>,
    limits: ZakuraConnectionLimits,
    accepted_capabilities: u64,
    connection_token: CancellationToken,
    close_cause: CloseCause,
    freshness_tx: watch::Sender<Instant>,
    /// The connection's session leases, when a memory pool funds them.
    session_funding: Option<&'a SessionFunding>,
}

struct ConnectionServeContext {
    limits: ZakuraConnectionLimits,
    accepted_capabilities: u64,
    /// See [`NativeHandshakeNegotiated::metered_epoch`].
    metered_epoch: Instant,
    capability_epoch: u64,
    role: &'static str,
    direction: ServicePeerDirection,
    transcript_hash: [u8; TRANSCRIPT_HASH_BYTES],
    /// Whether this node is the deterministic proactive opener for symmetric
    /// ordered streams on this connection. It also wins any same-kind collision
    /// caused by an older peer racing its own offer. Computed from the node ids
    /// so the two ends always agree: `local_node_id < remote_node_id`.
    i_open_collision_winner: bool,
    conn: ZakuraConnTrace,
    /// Counts an inbound connection against its IP until registration.
    control_handshake: Option<ControlHandshakeGuard>,
    /// Tells a reported dial how registration ended. See [`DialReport`].
    registration_report: Option<oneshot::Sender<DialRegistration>>,
}

struct RegisteredConnectionServeContext {
    limits: ZakuraConnectionLimits,
    /// See [`NativeHandshakeNegotiated::metered_epoch`].
    metered_epoch: Instant,
    conn: ZakuraConnTrace,
    conn_id: ZakuraConnId,
    connection_token: CancellationToken,
    close_cause: CloseCause,
    accepted_capabilities: u64,
    /// Whether this side dialed the connection. The dialer (initiator) opens
    /// ordinary ordered streams. The node-id winner opens block sync (the sole
    /// symmetric service), independently of which side dialed.
    is_initiator: bool,
    /// See [`ConnectionServeContext::i_open_collision_winner`].
    i_open_collision_winner: bool,
    direction: ServicePeerDirection,
}

struct StreamWorkerContext {
    conn: ZakuraConnTrace,
    peer_id: ZakuraPeerId,
    stream_id: u64,
    _permit: OwnedSemaphorePermit,
    limits: ZakuraConnectionLimits,
    inbound_frame_cap: u32,
    messages: Option<&'static [MessageRule]>,
    queue_depths: Option<StreamQueueDepths>,
    write_policy: StreamWritePolicy,
    session_resources: Option<Arc<dyn crate::zakura::SessionResources>>,
    outbound_frame_cap: u32,
    message_bucket: SharedMessageBucket,
    /// Cadence buckets for a stream with a table; `None` for legacy streams.
    cadence: Option<CadenceScope>,
    /// The response precheck the stream's service attaches, if any.
    precheck: PrecheckSlot,
    /// This stream's frame gates and its place in the node's owner count.
    resources: StreamResources,
    connection_token: CancellationToken,
    stream_token: CancellationToken,
    close_cause: CloseCause,
    freshness_tx: watch::Sender<Instant>,
    /// The stream kind and version a fault on this stream names.
    kind_version: (u16, u16),
}

impl StreamWorkerContext {
    /// The stream kind and version a fault on this stream names.
    fn stream(&self) -> (u16, u16) {
        self.kind_version
    }

    /// Credit a pause in which this reader waited for its own handler.
    fn credit_read_pause(&self, paused: Duration) {
        let (Some(cadence), Some(rules)) = (&self.cadence, self.messages) else {
            return;
        };
        // A reader that enforces cadence has a wall-clock dispatch budget.
        if cadence.parks || paused.is_zero() {
            return;
        }
        cadence
            .buckets
            .lock()
            .expect("Zakura cadence bucket mutex is never poisoned")
            .credit_pause(cadence.layout, rules, paused);
    }

    /// Wait until the cadence bucket of `frame`'s row holds a token, and take
    /// it. Returns `false` if the stream or its connection was cancelled
    /// first; the wait itself never closes anything.
    ///
    /// Call this only for a frame that [`admit_inbound_message`] parked. The
    /// frame keeps its receive grant while it waits, and pays exactly one
    /// token. The wait earns no pause credit, so it never refills the bucket
    /// it waits on.
    async fn park_for_cadence(&self, frame: &Frame) -> bool {
        let (Some(cadence), Some(rules)) = (&self.cadence, self.messages) else {
            return true;
        };
        let rule = MessageRule::find(rules, frame.message_type)
            .expect("the frame filter admits only rows of the stream's table");
        loop {
            let taken = cadence
                .buckets
                .lock()
                .expect("Zakura cadence bucket mutex is never poisoned")
                .take_parked(cadence.layout, rule);
            let Err(next_token) = taken else {
                return true;
            };
            zakura_rt::select! {
                biased;
                _ = self.connection_token.cancelled() => return false,
                _ = self.stream_token.cancelled() => return false,
                () = zakura_rt::time::sleep_until(next_token) => {}
            }
        }
    }
}

struct AdmittedSession {
    kind: u16,
    session_id: u64,
    cancel_token: CancellationToken,
    streams: Vec<ServiceStreamRole>,
    /// The primary stream's QUIC stream ID, if it has one.
    #[cfg(any(test, zakura_sim))]
    quic_stream: Option<u64>,
    /// Where the session came from, once [`ServiceSessionIO::with_origin`]
    /// says. Every member stream reports it.
    #[cfg(any(test, zakura_sim))]
    origin: Option<crate::zakura::SessionOrigin>,
}

struct ServiceStreamRole {
    kind: u16,
    version: u16,
    recv: FramedRecv,
    send: FramedSend,
}

impl AdmittedSession {
    fn into_service_streams(self) -> HashMap<u16, ServiceStream> {
        self.streams
            .into_iter()
            .map(|role| {
                let stream = ServiceStream::new(
                    self.session_id,
                    role.version,
                    role.recv,
                    role.send,
                    self.cancel_token.clone(),
                );
                #[cfg(any(test, zakura_sim))]
                let stream = ServiceStream {
                    origin: self.origin,
                    ..stream
                };
                (role.kind, stream)
            })
            .collect()
    }
}

type SessionWait = futures::stream::Once<BoxRunFuture<'static, ()>>;
type SessionWaits = StreamMap<u16, SessionWait>;

/// One connection's ordered services: each service's [`ServiceSessionState`],
/// its reopen wait, and the trace of its lifecycle.
///
/// Each `on_` method applies one connection-loop event to the state, records the
/// event's step, and changes the wait as the state directs, so one event has one
/// call site. A service holds a wait exactly while its state is waiting. Dropping
/// this value drops every pending wait.
struct ServiceSessionIO {
    sessions: HashMap<u16, ServiceSessionState>,
    waits: SessionWaits,
    conn: ZakuraConnTrace,
    close_cause: CloseCause,
}

impl ServiceSessionIO {
    fn new(
        sessions: HashMap<u16, ServiceSessionState>,
        conn: ZakuraConnTrace,
        close_cause: CloseCause,
    ) -> Self {
        Self {
            sessions,
            waits: SessionWaits::default(),
            conn,
            close_cause,
        }
    }

    /// `admitted`, reporting where its session came from: connection
    /// `conn_id`, its primary QUIC stream, and the generation this state
    /// installed for it, which is kept apart from the session ID the
    /// service is handed. A session with no QUIC stream or no installed
    /// generation reports none.
    #[cfg(any(test, zakura_sim))]
    fn with_origin(
        &self,
        conn_id: ZakuraConnId,
        mut admitted: AdmittedSession,
        opened_locally: bool,
    ) -> AdmittedSession {
        let generation = self
            .sessions
            .get(&admitted.kind)
            .and_then(|session| session.installed(opened_locally));
        admitted.origin = admitted
            .quic_stream
            .zip(generation)
            .map(|(quic_stream, generation)| crate::zakura::SessionOrigin {
                connection: conn_id,
                quic_stream: Some(quic_stream),
                generation,
            });
        admitted
    }

    fn session(&mut self, kind: u16) -> &mut ServiceSessionState {
        self.sessions
            .get_mut(&kind)
            .expect("the connection loop opens and adopts only negotiated services")
    }

    fn trace(&self, kind: u16, step: SessionStep) {
        trace_session_step(&self.conn, &self.close_cause, kind, step);
    }

    /// Whether `stream` is a service's primary stream that this endpoint opens.
    fn opens_locally(&self, stream: Stream) -> bool {
        self.sessions
            .get(&stream.kind)
            .is_some_and(|session| session.stream() == stream && session.opens_locally())
    }

    /// Whether `kind`'s service drops a peer's offer before reserving capacity.
    fn rejects_remote_offer(&self, kind: u16) -> bool {
        self.sessions
            .get(&kind)
            .is_some_and(ServiceSessionState::rejects_remote_offer)
    }

    /// Name a new local open of `kind`. Returns its attempt.
    fn begin_attempt(&mut self, kind: u16) -> u64 {
        let attempt = self.session(kind).begin_attempt();
        self.trace(kind, SessionStep::AttemptStarted { attempt });
        attempt
    }

    /// A local open succeeded: install its generation and drop any reopen wait.
    fn on_opened(&mut self, kind: u16, session_id: u64, attempt: u64) {
        self.session(kind).opened(session_id);
        self.trace(
            kind,
            SessionStep::GenerationInstalled {
                generation: session_id,
                attempt: Some(attempt),
            },
        );
        self.cancel(kind);
    }

    /// A local open failed. The caller decides whether the service retries.
    fn on_open_failed(&mut self, kind: u16, attempt: u64) {
        self.session(kind).open_failed();
        self.trace(kind, SessionStep::AttemptFailed { attempt });
    }

    /// Whether `kind`'s local open, whose streams are ready, may install its
    /// generation. A peer session that took the service meanwhile wins.
    fn accepts_local_open(&mut self, kind: u16, connection_cancelled: bool) -> bool {
        self.session(kind).accepts_local_open(connection_cancelled)
    }

    /// A session's workers finished. Returns the backoff that its service now
    /// waits before reopening, if any.
    fn on_exit(&mut self, exit: SessionExit, connection_cancelled: bool) -> Option<Duration> {
        let kind = exit.stream.kind;
        let session = self.sessions.get_mut(&kind)?;
        let outcome = session.exited(exit, connection_cancelled);
        self.trace(
            kind,
            SessionStep::GenerationExited {
                generation: exit.session_id,
                opened_locally: exit.opened_locally,
                current: outcome.current,
            },
        );
        if let Some(delay) = outcome.reopen_after {
            self.start_backoff(kind, delay);
        }
        outcome.reopen_after
    }

    /// `kind`'s reopen wait fired. Returns the stream to check demand for, if
    /// the service may reopen.
    fn on_wait_fired(&mut self, kind: u16, connection_cancelled: bool) -> Option<Stream> {
        let session = self.sessions.get_mut(&kind)?;
        session.clear_wait();
        let reopen = session
            .may_reopen(connection_cancelled)
            .then(|| session.stream());
        self.finish(kind);
        reopen
    }

    /// A peer's session of `kind` completed. Returns how it joins its service,
    /// or `None` if the connection never negotiated `kind`.
    fn on_remote_complete(&mut self, kind: u16, session_id: u64) -> Option<RemoteCompletion> {
        let completion = self.sessions.get_mut(&kind)?.remote_completed(session_id);
        if completion == RemoteCompletion::ParkCollision {
            self.trace(
                kind,
                SessionStep::GenerationParked {
                    generation: session_id,
                },
            );
        }
        Some(completion)
    }

    /// Apply `demand` to a completed peer's session. Returns whether the service
    /// adopted it rather than parking it.
    fn on_remote_demand(&mut self, kind: u16, session_id: u64, demand: SessionDemand) -> bool {
        let wait = DemandWait::from(&demand);
        match self.session(kind).remote_demand(session_id, wait) {
            RemoteDemand::Adopt => {
                self.cancel(kind);
                self.trace(
                    kind,
                    SessionStep::GenerationInstalled {
                        generation: session_id,
                        attempt: None,
                    },
                );
                true
            }
            RemoteDemand::Park(change) => {
                self.trace(
                    kind,
                    SessionStep::GenerationParked {
                        generation: session_id,
                    },
                );
                if let Some(change) = change {
                    self.apply_demand(kind, change, demand);
                }
                false
            }
        }
    }

    /// Apply `demand` after `kind`'s reopen wait fired. Returns whether the
    /// service opens now.
    fn on_reopen_demand(&mut self, kind: u16, demand: SessionDemand) -> bool {
        let wait = DemandWait::from(&demand);
        match self.session(kind).reopen_demand(wait) {
            None => true,
            Some(change) => {
                self.apply_demand(kind, change, demand);
                false
            }
        }
    }

    /// Apply `demand` to `kind`'s state and keep the demand's wait if the state
    /// starts one.
    fn schedule_demand(&mut self, kind: u16, demand: SessionDemand) {
        let change = self
            .session(kind)
            .schedule_demand(DemandWait::from(&demand));
        self.apply_demand(kind, change, demand);
    }

    /// Apply the state's `change` for `demand` to `kind`'s wait.
    fn apply_demand(&mut self, kind: u16, change: DemandChange, demand: SessionDemand) {
        match change {
            DemandChange::Keep => {}
            DemandChange::Cancel => self.cancel(kind),
            DemandChange::Start => self.start_demand(kind, demand),
        }
    }

    /// Start a transport backoff if `kind`'s state allows one. Returns its delay.
    fn schedule_transport_backoff(&mut self, kind: u16) -> Option<Duration> {
        let delay = self.session(kind).schedule_transport_backoff()?;
        self.start_backoff(kind, delay);
        Some(delay)
    }

    /// Replace any wait of `kind` with `demand`'s own wait, if it has one.
    fn start_demand(&mut self, kind: u16, demand: SessionDemand) {
        self.cancel(kind);
        if let Some(wait) = session_demand_wait(demand) {
            self.waits.insert(kind, futures::stream::once(wait));
        }
    }

    /// Wait `delay` before `kind` may reopen.
    fn start_backoff(&mut self, kind: u16, delay: Duration) {
        let replaced = self.waits.insert(
            kind,
            futures::stream::once(Box::pin(zakura_rt::time::sleep(delay))),
        );
        debug_assert!(
            replaced.is_none(),
            "idle ordered session cannot already have a reopen wait"
        );
    }

    /// Drop `kind`'s wait, if any.
    fn cancel(&mut self, kind: u16) {
        self.waits.remove(&kind);
    }

    /// Remove `kind`'s wait after it fired.
    fn finish(&mut self, kind: u16) {
        let removed = self.waits.remove(&kind);
        debug_assert!(
            removed.is_some(),
            "yielded ordered session wait remains keyed until it is handled"
        );
    }
}

/// The future that ends when `demand` next asks to check its service, or `None`
/// if it never does.
fn session_demand_wait(demand: SessionDemand) -> Option<BoxRunFuture<'static, ()>> {
    match demand {
        SessionDemand::OpenNow => Some(Box::pin(future::ready(()))),
        SessionDemand::RetryAt(at) => Some(Box::pin(async move {
            zakura_rt::time::sleep_until(at).await;
        })),
        SessionDemand::WaitForChange(changed) => Some(changed),
        SessionDemand::Retire => None,
    }
}

/// What every local open of a service session on one connection shares.
struct SessionOpenContext<'a> {
    connection: &'a Connection,
    stream_sem: &'a Arc<Semaphore>,
    resources: &'a ConnectionResources,
    limits: ZakuraConnectionLimits,
    per_stream_queue_depth: usize,
    connection_token: CancellationToken,
    close_cause: CloseCause,
    freshness_tx: watch::Sender<Instant>,
    conn: ZakuraConnTrace,
    peer_id: ZakuraPeerId,
    direction: ServicePeerDirection,
    session_exit_tx: mpsc::UnboundedSender<SessionExit>,
    /// The connection's session leases, when a memory pool funds them.
    session_funding: Option<&'a SessionFunding>,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum InboundMessageAdmission {
    Admit,
    Oversize,
    Throttled,
    /// A metered row overran its connection's byte bucket.
    ByteRateOverrun,
    /// The row's cadence bucket is empty, and the stream's reader waits for
    /// a token before it forwards the frame. See [`parks_on_exhaustion`].
    Park,
}

fn admit_inbound_message(
    frame: &Frame,
    context: &StreamWorkerContext,
    stream_kind: u16,
) -> InboundMessageAdmission {
    let payload_len = frame.payload.len();
    let stream_kind = stream_kind_label(stream_kind);
    let max_message_bytes = usize::try_from(context.limits.max_message_bytes)
        .expect("u32 message byte limit fits in usize");
    if payload_len > max_message_bytes {
        metrics::counter!(
            "zakura.p2p.ratelimit.message.oversize",
            "stream_kind" => stream_kind,
        )
        .increment(1);
        context.conn.trace_rate_limit(
            "message.oversize",
            context.stream_id,
            stream_kind,
            None,
            None,
            None,
        );
        return InboundMessageAdmission::Oversize;
    }

    // A stream with a table charges only the rows that declare a cadence.
    // Commitments bound requests and reservations bound responses.
    if let (Some(cadence), Some(rules)) = (&context.cadence, context.messages) {
        return admit_tabled_message(
            frame,
            rules,
            cadence.parks,
            |rule, payload_len| {
                let mut buckets = cadence
                    .buckets
                    .lock()
                    .expect("Zakura cadence bucket mutex is never poisoned");
                if cadence.parks {
                    buckets.charge_dispatch(cadence.layout, rule)
                } else {
                    buckets.charge_frame(cadence.layout, rule, payload_len)
                }
            },
            || {
                context.conn.trace_rate_limit(
                    "byte_rate.overrun",
                    context.stream_id,
                    stream_kind,
                    Some(u64::try_from(payload_len).unwrap_or(u64::MAX)),
                    None,
                    None,
                );
            },
            || {
                context.conn.trace_rate_limit(
                    "cadence.exhausted",
                    context.stream_id,
                    stream_kind,
                    None,
                    None,
                    None,
                );
            },
        );
    }

    let admitted = {
        let mut bucket = context
            .message_bucket
            .lock()
            .expect("Zakura message-rate bucket mutex is never poisoned");
        bucket.try_take()
    };
    if !admitted {
        metrics::counter!(
            "zakura.p2p.ratelimit.message.throttled",
            "stream_kind" => stream_kind,
        )
        .increment(1);
        context.conn.trace_rate_limit(
            "message.throttled",
            context.stream_id,
            stream_kind,
            None,
            None,
            None,
        );
        return InboundMessageAdmission::Throttled;
    }

    InboundMessageAdmission::Admit
}

/// Admits `frame` on a stream with the message table `rules`. It finds the
/// frame's row and charges the row's bucket through `charge`, with the
/// frame's payload length. If a metered row's frame overran its byte bucket,
/// it calls `overrun` and refuses the frame. If the bucket was empty, it
/// calls `exhausted`. An empty bucket never refuses the frame: it parks the
/// frame if the stream's reader `parks`, and admits it otherwise.
///
/// # Panics
///
/// If no row of `rules` has the frame's type. The frame filter admits only
/// rows of the stream's table.
pub(crate) fn admit_tabled_message(
    frame: &Frame,
    rules: &[MessageRule],
    parks: bool,
    charge: impl FnOnce(&MessageRule, usize) -> CadenceCharge,
    overrun: impl FnOnce(),
    exhausted: impl FnOnce(),
) -> InboundMessageAdmission {
    let rule = MessageRule::find(rules, frame.message_type)
        .expect("the frame filter admits only rows of the stream's table");
    let charge = charge(rule, frame.payload.len());
    // A metered row's bucket already holds every byte a conformant sender
    // can have buffered, so an overrun is a violation.
    if charge == CadenceCharge::Overrun {
        overrun();
        return InboundMessageAdmission::ByteRateOverrun;
    }
    // A sibling stream can keep the connection alive during an unbounded
    // transport stall. Exhaustion cannot prove a sender violation, so it
    // waits or passes, and never closes.
    if charge == CadenceCharge::Exhausted {
        exhausted();
        if parks {
            return InboundMessageAdmission::Park;
        }
    }
    InboundMessageAdmission::Admit
}

impl InboundMessageAdmission {
    /// The error an ordered stream's worker closes with for this result, or
    /// `None` if it admits the frame.
    pub(crate) fn ordered_error(self) -> Option<ZakuraHandlerError> {
        match self {
            InboundMessageAdmission::Admit | InboundMessageAdmission::Park => None,
            InboundMessageAdmission::Oversize => Some(ZakuraHandlerError::Oversize),
            InboundMessageAdmission::Throttled => Some(ZakuraHandlerError::RateLimited),
            InboundMessageAdmission::ByteRateOverrun => Some(ZakuraHandlerError::ByteRateOverrun),
        }
    }

    /// The close outcome the reader of the `(kind, version)` ordered stream
    /// records for this result, or `None` if it admits the frame.
    #[cfg(any(test, zakura_sim))]
    pub(crate) fn ordered_close_outcome(self, stream: (u16, u16)) -> Option<CloseOutcome> {
        self.ordered_error()
            .map(|error| read_close_outcome(ReadPath::Ordered, stream, &error))
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct NativeHandshakeNegotiated {
    pub(crate) limits: ZakuraAcceptedLimits,
    pub(crate) accepted_capabilities: u64,
    /// When this node wrote its half of the handshake: the initiator's hello
    /// or the responder's ack. The peer can pay for no metered frame before
    /// it reads that, so the connection's byte buckets earn from here. See
    /// [`CadenceBuckets::with_metered_epoch`].
    pub(crate) metered_epoch: Instant,
    /// The capability mask this node sent, as the transport observer reports
    /// it.
    #[cfg(any(test, zakura_sim))]
    pub(crate) local_capabilities: u64,
    /// The capability mask the peer sent, as the transport observer reports
    /// it.
    #[cfg(any(test, zakura_sim))]
    pub(crate) offered_capabilities: u64,
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn service_registry(
    _supervisor: &ZakuraSupervisorHandle,
    header_sync: Option<super::HeaderSyncHandle>,
    block_sync: Option<BlockSyncHandle>,
    block_sync_config: ZakuraBlockSyncConfig,
    block_sync_memory: super::BlockSyncMemory,
    legacy_service: Arc<dyn Service>,
    discovery_service: Arc<super::DiscoveryService>,
    service_demand: Option<
        watch::Receiver<zakura_node_services::sync_lifecycle::SyncServiceDemand>,
    >,
    custom_services: Vec<CustomService>,
) -> Result<Arc<ServiceRegistry>, BoxError> {
    let mut services = vec![legacy_service.clone()];
    let header_sync_service = if let Some(header_sync) = &header_sync {
        Arc::new(
            HeaderSyncService::new(header_sync.clone()).with_service_demand(service_demand.clone()),
        ) as Arc<dyn Service>
    } else {
        Arc::new(HeaderSyncPassthroughService::new(legacy_service.clone())) as Arc<dyn Service>
    };
    services.push(header_sync_service.clone());
    let block_sync = match block_sync {
        Some(block_sync) => BlockSyncService::new_with_handle(block_sync),
        None => match header_sync.as_ref() {
            Some(header_sync) => BlockSyncService::new_with_header_tip(
                block_sync_config,
                header_sync.subscribe_tip(),
                block_sync_memory,
            )?,
            None => BlockSyncService::new(block_sync_config, block_sync_memory)?,
        },
    };
    let block_sync = Arc::new(block_sync.with_service_demand(service_demand)) as Arc<dyn Service>;
    let custom_services = custom_services
        .into_iter()
        .map(Into::into)
        .collect::<Vec<_>>();
    let mut connection_owners = vec![legacy_service, header_sync_service, block_sync.clone()];
    connection_owners.extend(custom_services.iter().cloned());
    discovery_service.set_connection_owners(connection_owners);
    services.push(discovery_service as Arc<dyn Service>);
    services.push(block_sync);
    services.extend(custom_services);

    Ok(Arc::new(
        ServiceRegistry::new(services).map_err(|error| -> BoxError { Box::new(error) })?,
    ))
}

/// Every stream that `registry`'s services serve.
fn registry_streams(registry: &ServiceRegistry) -> impl Iterator<Item = Stream> + '_ {
    registry
        .services()
        .iter()
        .flat_map(|service| service.streams().iter().copied())
}

/// zakura-quic acceptor for the Zakura `p2p-v2/5` ALPN.
#[derive(Debug, Clone)]
pub struct ZakuraProtocolHandler {
    supervisor: ZakuraSupervisorHandle,
    handshake_config: ZakuraHandshakeConfig,
    supported_capabilities: Arc<AtomicU64>,
    capability_epoch: Arc<AtomicU64>,
    limits: ZakuraLocalLimits,
    registry: Arc<ServiceRegistry>,
    trace: ZakuraTrace,
    next_stream_id: Arc<AtomicU64>,
    /// Control handshakes in either direction.
    pending_handshakes: Arc<Semaphore>,
    /// Inbound control handshakes, which also hold a `pending_handshakes`
    /// permit.
    inbound_handshakes: Arc<Semaphore>,
    /// Node-wide owners of raw frames, unfunded output, and request workers.
    resources: NodeResources,
    shutdown: CancellationToken,
    // Bound endpoint identity, used for connection collision handling.
    local_node_id: Option<NodeId>,
    /// Node-wide memory pool shared by this handler and its services.
    memory_pool: Option<super::memory::MemoryPool>,
    /// Startup memory plan enforced for this handler.
    memory_plan: Option<super::memory::MemoryPlan>,
    /// `Consumer::Sessions` bytes leased by inbound and by outbound
    /// connections, which keeps the dial reserve (`02 §8` rule 1).
    session_directions: Arc<zakura_rt::sync::Mutex<super::memory::SessionDirections>>,
    /// Receives this handler's transport events.
    #[cfg(any(test, zakura_sim))]
    transport_observer: Option<Arc<dyn super::TransportObserver>>,
}

fn random_stream_session_seed() -> u64 {
    let mut rng = zakura_rt::rand::rng();
    loop {
        let seed = rng.next_u64();
        if seed != 0 {
            return seed;
        }
    }
}

fn native_connection_transcript_hash(
    direction: ServicePeerDirection,
    local_node_id: &NodeId,
    remote_node_id: &NodeId,
) -> [u8; TRANSCRIPT_HASH_BYTES] {
    let initiator = match direction {
        ServicePeerDirection::Inbound => remote_node_id,
        ServicePeerDirection::Outbound => local_node_id,
    };
    *initiator.as_bytes()
}

/// Whether the local node proactively opens symmetric ordered streams to `remote_node_id`.
/// The endpoint with the smaller node ID opens the stream.
/// The other endpoint accepts the stream.
/// Older peers also use this result to resolve simultaneous offers.
pub(crate) fn i_open_collision_winner(local_node_id: &NodeId, remote_node_id: &NodeId) -> bool {
    local_node_id.as_bytes() < remote_node_id.as_bytes()
}

impl ZakuraProtocolHandler {
    /// Create a handler sharing the given supervisor.
    pub fn new(
        supervisor: ZakuraSupervisorHandle,
        network: Network,
        handshake_config: ZakuraHandshakeConfig,
        limits: ZakuraLocalLimits,
    ) -> Self {
        Self::new_with_registry(
            supervisor,
            network,
            handshake_config,
            limits,
            Arc::new(ServiceRegistry::default()),
        )
    }

    /// Create a handler with an injected service registry.
    pub fn new_with_registry(
        supervisor: ZakuraSupervisorHandle,
        network: Network,
        handshake_config: ZakuraHandshakeConfig,
        limits: ZakuraLocalLimits,
        registry: Arc<ServiceRegistry>,
    ) -> Self {
        Self::new_with_registry_and_trace(
            supervisor,
            network,
            handshake_config,
            limits,
            registry,
            ZakuraTrace::noop(),
        )
    }

    /// Create a handler with an injected service registry and trace emitter.
    pub fn new_with_registry_and_trace(
        supervisor: ZakuraSupervisorHandle,
        _network: Network,
        handshake_config: ZakuraHandshakeConfig,
        limits: ZakuraLocalLimits,
        registry: Arc<ServiceRegistry>,
        trace: ZakuraTrace,
    ) -> Self {
        let mut handshake_config = handshake_config;
        handshake_config.supported_capabilities = registry.supported_capabilities();
        let supported_capabilities =
            Arc::new(AtomicU64::new(handshake_config.supported_capabilities));
        let envelope = limits.resource_envelope(&registry);
        let resources = NodeResources::new(
            limits.reactor_shares(),
            registry_streams(&registry),
            limits.max_frame_bytes,
        );
        debug!(
            ?envelope,
            transport_memory_bytes = %envelope.transport_memory_bytes(),
            "Zakura transport resource envelope"
        );
        Self {
            supervisor,
            handshake_config,
            supported_capabilities,
            capability_epoch: Arc::new(AtomicU64::new(0)),
            registry,
            trace,
            // Stream session IDs are local correlation generations, not wire
            // sequence numbers. A random process seed prevents preserved traces
            // and late completions from colliding after a node restart.
            next_stream_id: Arc::new(AtomicU64::new(random_stream_session_seed())),
            pending_handshakes: Arc::new(Semaphore::new(limits.max_pending_handshakes)),
            inbound_handshakes: Arc::new(Semaphore::new(limits.max_inbound_handshakes())),
            resources,
            shutdown: CancellationToken::new(),
            limits,
            local_node_id: None,
            memory_pool: None,
            memory_plan: None,
            session_directions: Arc::default(),
            #[cfg(any(test, zakura_sim))]
            transport_observer: None,
        }
    }

    /// Attach the startup [`MemoryPlan`](super::memory::MemoryPlan) and its
    /// live [`MemoryPool`](super::memory::MemoryPool), which serving reactors
    /// and per-connection send-window autotuning share (`02 §4–6`). The
    /// caller builds the pool once and passes the same one to every reactor.
    pub fn with_memory_pool(
        mut self,
        plan: super::memory::MemoryPlan,
        pool: super::memory::MemoryPool,
    ) -> Self {
        self.memory_plan = Some(plan);
        self.memory_pool = Some(pool);
        self
    }

    /// Report every connection's transport events to `observer`. Install it
    /// before serving or dialing any connection.
    #[cfg(any(test, zakura_sim))]
    pub fn with_transport_observer(mut self, observer: Arc<dyn super::TransportObserver>) -> Self {
        #[cfg(zakura_sim)]
        observer.handshakes(super::HandshakeProbe::new(
            &self.supervisor.ip_slots.0,
            &self.pending_handshakes,
            &self.inbound_handshakes,
            self.supervisor.max_connections_per_ip,
            &self.limits,
        ));
        self.transport_observer = Some(observer);
        self
    }

    /// Attach the observer to `connection`'s `close_cause`, keyed by its
    /// remote address until registration gives it a `conn_id`.
    #[cfg(any(test, zakura_sim))]
    fn observe_close(&self, close_cause: &CloseCause, connection: &Connection) {
        if let Some(observer) = &self.transport_observer {
            close_cause.observe_with(Arc::new(super::ConnTap::new(
                observer.clone(),
                connection.admitted_addr(),
            )));
        }
    }

    /// Report `connection`'s registration as `conn_id`, which its later
    /// events carry.
    #[cfg(any(test, zakura_sim))]
    fn observe_registered(
        &self,
        conn_id: ZakuraConnId,
        close_cause: &CloseCause,
        connection: &Connection,
    ) {
        if let Some(tap) = close_cause.tap() {
            tap.registered(conn_id);
        }
        transport_observe!(
            self.transport_observer,
            super::TransportEvent::Registered {
                conn_id,
                remote: connection.admitted_addr(),
                shared_id: connection.shared_id(),
            }
        );
    }

    #[cfg(any(test, zakura_sim))]
    fn observe_control_result<T>(
        &self,
        connection: &Connection,
        stage: &'static str,
        result: &Result<T, ZakuraHandlerError>,
    ) {
        transport_observe!(
            self.transport_observer,
            super::TransportEvent::Handshake {
                remote: connection.admitted_addr(),
                shared_id: connection.shared_id(),
                stage,
                validation: match result {
                    Err(ZakuraHandlerError::Validation(error)) => Some(*error),
                    _ => None,
                },
                error: result.as_ref().err().map(ToString::to_string),
                elapsed: None,
            }
        );
    }

    #[cfg(any(test, zakura_sim))]
    fn handshake_observation(&self, connection: &Connection) -> Option<HandshakeObservation> {
        self.transport_observer
            .as_ref()
            .map(|observer| HandshakeObservation {
                observer: observer.clone(),
                remote: connection.admitted_addr(),
                shared_id: connection.shared_id(),
                started: Instant::now(),
            })
    }

    #[cfg(zakura_sim)]
    fn observe_handshake_slot(&self, connection: &Connection) {
        if let Some(observer) = &self.transport_observer {
            observer.handshake_slot(
                connection.admitted_addr(),
                connection.shared_id(),
                connection.slot_probe(),
            );
        }
    }

    /// Report that `connection` was refused before registration, for
    /// `reason`: its handshake error or the refusal's trace label.
    #[cfg(any(test, zakura_sim))]
    fn handshake_refused(&self, connection: &Connection, reason: impl ToString) {
        transport_observe!(
            self.transport_observer,
            super::TransportEvent::HandshakeRefused {
                remote: connection.admitted_addr(),
                shared_id: connection.shared_id(),
                reason: reason.to_string(),
            }
        );
    }

    /// [`Self::handshake_refused`] for a connection the supervisor refused to
    /// register. The refusal is its close decision, so a later cancellation of
    /// its token reports none. If a cancellation during `register` already
    /// reported `CloseDecided`, that stays the only decision and this reports
    /// nothing.
    #[cfg(any(test, zakura_sim))]
    fn registration_refused(
        &self,
        close_cause: &CloseCause,
        connection: &Connection,
        reason: &'static str,
    ) {
        if close_cause.tap().is_none_or(|tap| tap.refused()) {
            self.handshake_refused(connection, reason);
        }
    }

    /// Restrict the locally advertised capability mask.
    ///
    /// Test nodes use this to exercise compatibility negotiation against older
    /// peers while retaining the production service registry.
    #[cfg(any(test, feature = "zakura-testkit"))]
    pub(crate) fn with_supported_capabilities(mut self, supported_capabilities: u64) -> Self {
        self.handshake_config.supported_capabilities &= supported_capabilities;
        self.supported_capabilities.store(
            self.handshake_config.supported_capabilities,
            Ordering::Relaxed,
        );
        self
    }

    /// Return both cadence charges for an admission cancelled before TLS.
    fn refund_cadence(&self, remote: SocketAddr, validated: bool) {
        let mut counts = self.supervisor.ip_slots.counts();
        counts.refund_cadence(remote.ip(), validated, Instant::now());
        #[cfg(any(test, zakura_sim))]
        let hosts = counts.cadence_hosts(validated);
        drop(counts);
        #[cfg(any(test, zakura_sim))]
        self.observe_handshake_cadence(remote, validated, "cancelled", hosts);
    }

    /// Reports a handshake cadence decision for `remote`. The caller reads
    /// `hosts` under the `ip_slots` guard it already holds, so a tap adds no
    /// lock and no scheduling point.
    #[cfg(any(test, zakura_sim))]
    fn observe_handshake_cadence(
        &self,
        remote: SocketAddr,
        validated: bool,
        action: &'static str,
        hosts: usize,
    ) {
        let cadence = super::handshake_cadence(self.supervisor.max_connections_per_ip);
        transport_observe!(
            self.transport_observer,
            super::TransportEvent::HandshakeCadence {
                remote,
                validated,
                action,
                capacity: cadence.capacity,
                refill_interval: cadence.refill_interval,
                hosts,
                host_limit: self
                    .limits
                    .total_connections()
                    .saturating_add(self.limits.max_pending_handshakes),
            }
        );
    }

    fn current_handshake_config(&self) -> ZakuraHandshakeConfig {
        let mut config = self.handshake_config;
        config.supported_capabilities = self.supported_capabilities.load(Ordering::SeqCst);
        config
    }

    fn handshake_snapshot(&self) -> (ZakuraHandshakeConfig, u64) {
        // Read the generation first: an overlapping change makes this snapshot stale.
        let epoch = self.capability_epoch.load(Ordering::SeqCst);
        (self.current_handshake_config(), epoch)
    }

    fn set_header_sync_enabled(&self, enabled: bool) {
        let previous = if enabled {
            self.supported_capabilities
                .fetch_or(ZAKURA_CAP_HEADER_SYNC, Ordering::SeqCst)
        } else {
            self.supported_capabilities
                .fetch_and(!ZAKURA_CAP_HEADER_SYNC, Ordering::SeqCst)
        };
        // A call that leaves the mask unchanged leaves in-flight negotiations
        // current, so it keeps their generation.
        if (previous & ZAKURA_CAP_HEADER_SYNC != 0) == enabled {
            return;
        }
        // Publish the generation before the caller snapshots registered peers.
        // Registrations before this change enter that snapshot; later ones reject
        // the old generation before installing services.
        self.capability_epoch.fetch_add(1, Ordering::SeqCst);
        transport_observe!(
            self.transport_observer,
            super::TransportEvent::CapabilitiesChanged {
                supported: previous ^ ZAKURA_CAP_HEADER_SYNC,
            }
        );
    }

    #[cfg(zakura_sim)]
    async fn pause_handshake(&self, phase: super::HandshakePhase) {
        if let Some(pause) = self
            .transport_observer
            .as_ref()
            .and_then(|observer| observer.handshake_pause())
        {
            pause.at(phase).await;
        }
    }

    /// Attach the bound endpoint's identity for connection collision handling.
    pub fn with_local_node_id(mut self, local_node_id: NodeId) -> Self {
        self.local_node_id = Some(local_node_id);
        self
    }

    /// Moves an inbound connection from the transport's pending count into
    /// this handler's own accounting (zakura-quic ADM-7).
    ///
    /// `Acceptor::handle` calls this synchronously, before the transport
    /// releases the connection's pending count, so no admission decision sees
    /// the IP uncounted.
    fn claim_inbound(
        &self,
        connection: &Connection,
        transfer: bool,
    ) -> Result<InboundClaim, InboundRefusal> {
        #[cfg(zakura_sim)]
        self.observe_handshake_slot(connection);
        transport_observe!(
            self.transport_observer,
            super::TransportEvent::Handshake {
                remote: connection.admitted_addr(),
                shared_id: connection.shared_id(),
                stage: "started",
                validation: None,
                error: None,
                elapsed: None,
            }
        );
        let earlier = connection.remote_earlier_ids();
        let admission = self
            .claim_slot(ProvenNode::new(connection.remote_id(), &earlier), true)
            .map_err(|refusal| match refusal {
                SlotRefusal::Hold => InboundRefusal::Hold,
                SlotRefusal::Full | SlotRefusal::InboundShare => InboundRefusal::Admission,
            })?;
        let pending = self
            .claim_pending_handshake_with_transfer(true, transfer)
            .ok_or(InboundRefusal::PendingHandshake)?;
        let control_handshake = self
            .supervisor
            .enter_control_handshake(connection.admitted_ip());
        #[cfg(any(test, zakura_sim))]
        let (admission, control_handshake) = {
            let mut admission = admission;
            let mut control_handshake = control_handshake;
            let observation = self.handshake_observation(connection);
            admission.observation = observation.clone();
            control_handshake.observation = observation;
            (admission, control_handshake)
        };
        Ok(InboundClaim {
            pending,
            _admission: admission,
            control_handshake,
        })
    }

    async fn serve_inbound(
        &self,
        connection: Connection,
        claim: Result<InboundClaim, InboundRefusal>,
    ) {
        let conn_id = trace_conn_id(&connection);
        let (pending, _admission, control_handshake) = match claim {
            Ok(InboundClaim {
                pending,
                _admission,
                control_handshake,
            }) => (pending, _admission, control_handshake),
            Err(refusal) => {
                self.refuse_inbound(&connection, conn_id, refusal);
                return;
            }
        };

        let remote_node_id = connection.remote_id();
        let remote_peer_id = ZakuraPeerId::from_node_id(remote_node_id);
        let conn = ZakuraConnTrace::new(&self.trace, conn_id, &remote_peer_id);

        let (negotiated, capability_epoch) = match self
            .run_native_responder_handshake_with_permit(
                &connection,
                &remote_peer_id,
                &conn,
                pending,
            )
            .await
        {
            Ok(negotiated) => negotiated,
            Err(error) => {
                debug!(?error, "Zakura control handshake failed");
                #[cfg(any(test, zakura_sim))]
                self.handshake_refused(&connection, &error);
                connection.close(VarInt::from_u32(ZAKURA_CLOSE_NEUTRAL), b"control handshake");
                return;
            }
        };
        transport_observe!(
            self.transport_observer,
            super::TransportEvent::Negotiated {
                remote: connection.admitted_addr(),
                local: negotiated.local_capabilities,
                offered: negotiated.offered_capabilities,
                negotiated: negotiated.accepted_capabilities,
            }
        );

        let conn_limits = self.limits.clamp(&negotiated.limits);
        // Charge inbound peers to the IP admitted before the handshake, which
        // later paths and migrations never change.
        let remote_ip = Some(connection.admitted_ip());
        let local_node_id = self.local_node_id.unwrap_or(remote_node_id);
        let direction = ServicePeerDirection::Inbound;
        let result = self
            .register_and_serve(
                connection,
                remote_peer_id,
                remote_ip,
                ConnectionServeContext {
                    limits: conn_limits,
                    accepted_capabilities: negotiated.accepted_capabilities,
                    metered_epoch: negotiated.metered_epoch,
                    capability_epoch,
                    role: "responder",
                    direction,
                    transcript_hash: native_connection_transcript_hash(
                        direction,
                        &local_node_id,
                        &remote_node_id,
                    ),
                    i_open_collision_winner: i_open_collision_winner(
                        &local_node_id,
                        &remote_node_id,
                    ),
                    conn,
                    control_handshake: Some(control_handshake),
                    registration_report: None,
                },
            )
            .await;
        if let Err(error) = result {
            debug!(?error, "inbound Zakura connection ended with an error");
        }
    }

    /// Closes an inbound connection whose claim found no room, and counts the
    /// refusal under the budget that was full.
    fn refuse_inbound(&self, connection: &Connection, conn_id: u64, refusal: InboundRefusal) {
        let (label, error) = match refusal {
            InboundRefusal::Admission => {
                metrics::counter!("zakura.p2p.conn.rejected.admission").increment(1);
                ("admission", ADMISSION_LIMIT)
            }
            InboundRefusal::Hold => {
                metrics::counter!("zakura.p2p.conn.rejected.hold").increment(1);
                ("hold", ADMISSION_LIMIT)
            }
            InboundRefusal::PendingHandshake => {
                metrics::counter!("zakura.p2p.conn.rejected.pending_handshake").increment(1);
                ("pending_handshake", PENDING_HANDSHAKE_LIMIT)
            }
        };
        #[cfg(any(test, zakura_sim))]
        match refusal {
            InboundRefusal::Admission | InboundRefusal::Hold => {
                self.handshake_refused(connection, label)
            }
            InboundRefusal::PendingHandshake => {
                self.handshake_refused(connection, ZakuraHandlerError::ResourceLimit(error))
            }
        }
        let conn = ZakuraConnTrace::without_peer_on(&self.trace, conn_id);
        conn.trace_connection("rejected.admission", None, Some("inbound"), Some(label));
        connection.close(VarInt::from_u32(ZAKURA_CLOSE_RESOURCE), error.as_bytes());
        transport_observe!(
            self.transport_observer,
            super::TransportEvent::Handshake {
                remote: connection.admitted_addr(),
                shared_id: connection.shared_id(),
                stage: "released",
                validation: None,
                error: None,
                elapsed: None,
            }
        );
    }

    fn publish_gateway_ranking(&self, ranking: impl IntoIterator<Item = super::RankedGateway>) {
        self.publish_discovery_gateway_ranking(ranking, self.limits.max_connections);
    }

    fn publish_discovery_gateway_ranking(
        &self,
        ranking: impl IntoIterator<Item = super::RankedGateway>,
        ordinary_cap: usize,
    ) {
        let ranking = ranking.into_iter().collect();
        let moved = self.supervisor.moved_tx.borrow();
        let mut counts = self.supervisor.ip_slots.counts();
        counts.publish_gateway_ranking(
            &ranking,
            self.limits.max_preferred_connections,
            self.limits.max_connections,
        );
        counts.defer_gateway_rollbacks(&moved, ordinary_cap, Instant::now());
        let hold = counts.preferred_gateways().hold();
        let split = counts.slot_split();
        drop(counts);
        if let Some(plan) = &self.memory_plan {
            lock_session_directions(&self.session_directions).gateway_reserve =
                plan.gateway_reserve(hold);
        }
        // These counts are bounded by the funded connection budget and fit exactly in f64.
        metrics::gauge!("zakura.p2p.conn.slots.hold").set(hold as f64);
        metrics::gauge!("zakura.p2p.conn.slots.preferred").set(split.preferred as f64);
        metrics::gauge!("zakura.p2p.conn.slots.ordinary").set(split.ordinary as f64);
    }

    /// Charges a dial to the last authenticated ID. When its ordinary slot is
    /// unavailable, periodically let the original preferred ID try its held place.
    /// TLS must still prove which ID can keep the slot.
    fn claim_dial_connection(
        &self,
        dialed: NodeId,
        ordinary_cap: usize,
    ) -> Result<ConnectionAdmissionGuard, SlotRefusal> {
        let moves = &self.supervisor.moved_tx;
        let expected = moves.borrow().current(dialed);
        let claim = self.claim_slot_with_ordinary_cap(expected.into(), false, ordinary_cap);
        if expected == dialed || !matches!(claim, Err(SlotRefusal::Hold)) {
            return claim;
        }
        let preferred = self
            .supervisor
            .ip_slots
            .counts()
            .preferred_gateways()
            .contains(&dialed);
        if preferred
            && moves.send_if_modified(|moves| moves.take_recheck(dialed, expected, Instant::now()))
        {
            return self.claim_slot_with_ordinary_cap(dialed.into(), false, ordinary_cap);
        }
        claim
    }

    /// Claims a connection slot for `node`, a dial's target or an inbound
    /// caller that proved no earlier ID, as one locked check and charge.
    #[cfg(test)]
    fn claim_connection(
        &self,
        node: NodeId,
        inbound: bool,
    ) -> Result<ConnectionAdmissionGuard, SlotRefusal> {
        self.claim_slot(node.into(), inbound)
    }

    /// Claims a connection slot for `node`, as one locked check and charge.
    /// `node` is a dial's target, which has proved nothing yet, or the
    /// remote an inbound connection proved, with its earlier IDs.
    fn claim_slot(
        &self,
        node: ProvenNode<'_>,
        inbound: bool,
    ) -> Result<ConnectionAdmissionGuard, SlotRefusal> {
        self.claim_slot_with_ordinary_cap(node, inbound, self.limits.max_connections)
    }

    /// Keeps discovery's ordinary dials below the ceiling that leaves bootstrap room.
    fn claim_slot_with_ordinary_cap(
        &self,
        node: ProvenNode<'_>,
        inbound: bool,
        ordinary_cap: usize,
    ) -> Result<ConnectionAdmissionGuard, SlotRefusal> {
        let mut counts = self.supervisor.ip_slots.counts();
        let preferred_dial = !inbound
            && counts.preferred_gateways().contains(&node.id())
            && counts.node_connections(&node.id()).total() == 0;
        if !preferred_dial
            && ordinary_cap < self.limits.max_connections
            && counts.free_ordinary_connections(ordinary_cap) == 0
        {
            return Err(SlotRefusal::Hold);
        }
        let claim = if inbound {
            counts.claim_inbound_connection(node, self.limits.max_connections)
        } else {
            counts.claim_connection(node.id(), false, self.limits.max_connections)
        };
        drop(counts);
        if inbound && matches!(claim, Ok(SlotClass::Preferred)) {
            metrics::counter!("zakura.p2p.conn.accepted.preferred").increment(1);
        }
        if preferred_dial && claim.is_err() {
            metrics::counter!("zakura.p2p.conn.dial.refused.preferred").increment(1);
        }
        claim?;
        Ok(ConnectionAdmissionGuard {
            authenticated_dial: false,
            #[cfg(any(test, zakura_sim))]
            observation: None,
            slots: self.supervisor.ip_slots.clone(),
            node: node.id(),
            inbound,
            ordinary_cap,
        })
    }

    /// Connection slots a dial to a node outside the preferred set may
    /// claim now.
    fn free_connection_slots(&self) -> usize {
        self.supervisor
            .ip_slots
            .counts()
            .free_ordinary_connections(self.limits.max_connections)
    }

    /// Charges the machine and semaphore together without an await.
    fn claim_pending_handshake(&self, inbound: bool) -> Option<PendingHandshakeGuard> {
        self.claim_pending_handshake_with_transfer(inbound, false)
    }

    fn claim_pending_handshake_with_transfer(
        &self,
        inbound: bool,
        transfer: bool,
    ) -> Option<PendingHandshakeGuard> {
        let mut counts = self.supervisor.ip_slots.counts();
        // Take the inbound share first, so a refused responder cannot hold a dial slot.
        let inbound_permit = if inbound {
            Some(self.inbound_handshakes.clone().try_acquire_owned().ok()?)
        } else {
            None
        };
        let shared = self.pending_handshakes.clone().try_acquire_owned().ok()?;
        let claimed = if transfer {
            counts.claim_transferred_pending();
            true
        } else {
            counts.claim_pending(inbound, self.limits.max_pending_handshakes)
        };
        if !claimed {
            return None;
        }
        Some(PendingHandshakeGuard {
            #[cfg(any(test, zakura_sim))]
            observation: None,
            slots: self.supervisor.ip_slots.clone(),
            shared: Some(shared),
            inbound: inbound_permit,
        })
    }

    async fn run_native_responder_handshake_with_permit(
        &self,
        connection: &Connection,
        remote_peer_id: &ZakuraPeerId,
        conn: &ZakuraConnTrace,
        _handshake: PendingHandshakeGuard,
    ) -> Result<(NativeHandshakeNegotiated, u64), ZakuraHandlerError> {
        #[cfg(any(test, zakura_sim))]
        let _handshake = {
            let mut guard = _handshake;
            guard.observation = self.handshake_observation(connection);
            guard
        };
        let _owner = self.resources.owners().enter(OwnerPhase::Handshake);

        // One deadline from admission bounds the whole handshake: the control
        // exchange gets what TLS left of `control_timeout`, so a peer that
        // paces each stage holds the slot for `control_timeout` at most.
        let remaining = self
            .limits
            .control_timeout
            .saturating_sub(connection.admitted_at().elapsed());
        let result = match timeout(
            remaining,
            self.run_native_responder_handshake(connection, remote_peer_id, conn),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => Err(ZakuraHandlerError::Timeout("control handshake")),
        };
        #[cfg(any(test, zakura_sim))]
        self.observe_control_result(connection, "hello", &result);
        result
    }

    async fn run_native_responder_handshake(
        &self,
        connection: &Connection,
        remote_peer_id: &ZakuraPeerId,
        conn: &ZakuraConnTrace,
    ) -> Result<(NativeHandshakeNegotiated, u64), ZakuraHandlerError> {
        let (handshake_config, capability_epoch) = self.handshake_snapshot();
        transport_observe!(
            self.transport_observer,
            super::TransportEvent::HandshakeCapabilities {
                remote: connection.admitted_addr(),
                shared_id: connection.shared_id(),
                supported: handshake_config.supported_capabilities,
            }
        );
        #[cfg(zakura_sim)]
        self.pause_handshake(super::HandshakePhase::Snapshot).await;
        conn.trace_handshake(
            "control.started",
            "responder",
            None,
            handshake_config.network_label(),
        );
        let (mut send, mut recv) = timeout(self.limits.control_timeout, connection.accept_bi())
            .await
            .map_err(|_| ZakuraHandlerError::Timeout("accept control stream"))??;

        let hello_bytes = read_control_payload(
            &mut recv,
            handshake_config.max_control_frame_bytes,
            self.limits.control_timeout,
        )
        .await?;
        let hello = ZakuraControlHello::decode(&hello_bytes)?;
        let expected = ZakuraControlValidation {
            local: &handshake_config,
            authenticated_remote_id: remote_peer_id.as_bytes(),
            selected_zakura_protocol: negotiated_zakura_protocol(connection)?,
            handshake_path: ZakuraHandshakePath::Native,
            remote_role: ZakuraControlRole::Initiator,
            initiator_upgrade_nonce: [0; 32],
            responder_upgrade_nonce: [0; 32],
            legacy_upgrade_transcript: [0; 32],
        };
        hello.validate(&expected)?;

        let mut local_nonce = [0; 32];
        zakura_rt::rand::rng().fill_bytes(&mut local_nonce);

        let accepted_limits = self.accepted_limits_for(&hello.initial_limits);
        let ack = ZakuraControlAck {
            magic: CONTROL_ACK_MAGIC,
            control_version: CONTROL_VERSION,
            selected_zakura_protocol: hello.selected_zakura_protocol,
            peer_nonce: local_nonce,
            remote_peer_nonce: hello.peer_nonce,
            accepted_capabilities: hello.capabilities & handshake_config.supported_capabilities,
            accepted_channels: hello.required_channels & handshake_config.supported_channels,
            accepted_limits,
        };
        let metered_epoch = Instant::now();
        write_control_payload(&mut send, &ack.encode()?, self.limits.control_timeout).await?;
        conn.trace_handshake(
            "control.succeeded",
            "responder",
            Some(ack.selected_zakura_protocol),
            handshake_config.network_label(),
        );
        Ok((
            NativeHandshakeNegotiated {
                limits: accepted_limits,
                accepted_capabilities: ack.accepted_capabilities,
                metered_epoch,
                #[cfg(any(test, zakura_sim))]
                local_capabilities: handshake_config.supported_capabilities,
                #[cfg(any(test, zakura_sim))]
                offered_capabilities: hello.capabilities,
            },
            capability_epoch,
        ))
    }

    fn accepted_limits_for(&self, remote_limits: &ZakuraInitialLimits) -> ZakuraAcceptedLimits {
        ZakuraLimits {
            max_frame_bytes: remote_limits
                .max_frame_bytes
                .min(self.limits.max_frame_bytes),
            max_message_bytes: remote_limits
                .max_message_bytes
                .min(self.limits.max_message_bytes),
            max_open_streams: remote_limits
                .max_open_streams
                .min(self.limits.max_open_streams),
            max_inbound_queue_depth: remote_limits
                .max_inbound_queue_depth
                .min(self.limits.max_inbound_queue_depth),
            idle_timeout_millis: remote_limits
                .idle_timeout_millis
                .min(self.limits.initial_limits().idle_timeout_millis),
        }
    }

    async fn serve_connection(
        &self,
        connection: Connection,
        peer_id: ZakuraPeerId,
        remote_ip: Option<IpAddr>,
        mut outbound_rx: mpsc::Receiver<ZakuraOutboundFrame>,
        context: RegisteredConnectionServeContext,
        mut cleanup_guard: RegisteredPeerCleanupGuard,
    ) -> Result<(), ZakuraHandlerError> {
        let limits = context.limits;
        let conn = context.conn;
        let conn_id = context.conn_id;
        let connection_token = context.connection_token;
        let close_cause = context.close_cause;
        let accepted_capabilities = context.accepted_capabilities;
        let session_funding = cleanup_guard.session_funding.clone();
        let stream_sem = Arc::new(Semaphore::new(usize::from(limits.max_open_streams)));
        let limits_owner_bound = self.limits.owner_bound();
        let connection_resources = self.resources.connection(accepted_capabilities);
        self.resources.report_usage(limits_owner_bound);
        // Services such as BPv1 read this connection's path load.
        let load = connection.subscribe_load();
        let serving_peer_id = peer_id.clone();
        let serving_peer_floor = super::block_sync::LARGEST_SERVED_BLOCK_RESPONSE_BYTES;
        let _serving_registration = self
            .memory_pool
            .as_ref()
            .map(|pool| pool.register_serving_connection(&serving_peer_id, serving_peer_floor));
        let conn_credit = self.memory_pool.as_ref().map(|_| {
            super::memory::ConnectionCredit::new_quic(
                conn_id,
                connection.clone(),
                connection_resources.base_receive_window(),
                BASE_SEND_WINDOW_BYTES.min(self.limits.admitted_send_window_bytes),
            )
        });
        let _close_credit = super::memory::CloseCreditOnDrop(conn_credit.clone());
        #[cfg(zakura_sim)]
        if let Some(observer) = &self.transport_observer {
            observer.connection(
                conn_id,
                connection_resources.probe(
                    connection.load_probe(),
                    conn_credit
                        .as_ref()
                        .map(super::memory::ConnectionCredit::live_receive_window_atomic),
                    &stream_sem,
                    limits,
                ),
            );
        }
        // When running without a MemoryPool (standalone transport handler tests),
        // raise the static receive and send windows once the peer is admitted
        // (zakura-quic API-9). Pool-backed connections start at their static base
        // receive window above and raise it per leased request (`02 §3`).
        if conn_credit.is_none() {
            let _ = connection.set_limits(
                zakura_quic::ConnLimits::default()
                    .with_receive_window(connection_resources.receive_window())
                    .with_send_window(self.limits.admitted_send_window_bytes),
            );
        }
        let mut workers = JoinSet::new();
        let (session_exit_tx, mut session_exit_rx) = mpsc::unbounded_channel();
        let mut pending_sessions = PendingSessions::default();
        let mut incoming_setup: Option<BoxFuture<'static, Option<IncomingStreamSetup>>> = None;
        let mut open_limiter = TokenBucket::new(limits.stream_open_rate_per_second);
        let mut message_buckets = MessageRateBuckets::for_connection(
            u64::from(self.limits.quic.stream_receive_window_bytes),
            context.metered_epoch,
        );
        let (freshness_tx, freshness_rx) = watch::channel(Instant::now());
        let negotiated_ordered_streams = self
            .registry
            .persistent_streams_for_negotiated(accepted_capabilities);
        let roles = ConnectionRoles {
            is_initiator: context.is_initiator,
            i_open_collision_winner: context.i_open_collision_winner,
        };
        let sessions: HashMap<u16, ServiceSessionState> = negotiated_ordered_streams
            .iter()
            .copied()
            .filter(|stream| {
                self.registry
                    .session_layout(*stream)
                    .is_some_and(|layout| layout.primary() == *stream)
            })
            .map(|stream| {
                let policy = self.registry.session_policy(stream.kind);
                (stream.kind, ServiceSessionState::new(stream, policy, roles))
            })
            .collect();
        let mut session_io = ServiceSessionIO::new(sessions, conn.clone(), close_cause.clone());
        // The dialer opens ordinary ordered streams. For symmetric block sync,
        // the mirror-stable node-id tiebreak designates one proactive opener,
        // avoiding crossed service generations while retaining one bidirectional
        // session regardless of who dialed. The accept path still resolves raced
        // offers from older peers with the same tiebreak.
        let mut ordered_streams = Vec::new();
        let mut deferred_ordered_streams = Vec::new();
        for stream in negotiated_ordered_streams.iter().copied() {
            // Only a layout's primary stream has session state; the other
            // members open with it.
            if !session_io.opens_locally(stream) {
                continue;
            }

            match self.registry.session_demand(
                stream.kind,
                conn_id,
                accepted_capabilities,
                &peer_id,
                context.direction,
            ) {
                SessionDemand::OpenNow => ordered_streams.push(stream),
                demand => deferred_ordered_streams.push((stream, demand)),
            }
        }
        let request_response_stream_count = self
            .registry
            .request_response_streams_for_negotiated(accepted_capabilities)
            .len();
        // Count every required stream in the sessions that local demand will open.
        let opening_stream_count: usize = ordered_streams
            .iter()
            .map(|stream| {
                self.registry
                    .session_layout(*stream)
                    .map_or(1, |layout| layout.streams().len())
            })
            .sum();
        if opening_stream_count > usize::from(limits.max_open_streams) {
            debug!(
                max_open_streams = limits.max_open_streams,
                ordered_stream_count = opening_stream_count,
                "closing Zakura peer because negotiated ordered streams exceed max-open-streams"
            );
            connection.close(VarInt::from_u32(ZAKURA_CLOSE_RESOURCE), b"ordered streams");
            close_cause.record(CloseOutcome::neutral(
                NeutralCause::IncompatibleConfiguration,
                "resource_ordered_streams",
            ));
            connection_token.cancel();
        } else if !negotiated_ordered_streams.is_empty()
            && usize::from(limits.max_inbound_queue_depth)
                < minimum_ordered_queue_capacity(&negotiated_ordered_streams)
        {
            debug!(
                max_inbound_queue_depth = limits.max_inbound_queue_depth,
                ordered_stream_count = negotiated_ordered_streams.len(),
                "closing Zakura peer because inbound queue depth cannot be split across ordered streams"
            );
            connection.close(VarInt::from_u32(ZAKURA_CLOSE_RESOURCE), b"queue split");
            close_cause.record(CloseOutcome::neutral(
                NeutralCause::IncompatibleConfiguration,
                "resource_queue_split",
            ));
            connection_token.cancel();
        }
        let queue_split_stream_count = negotiated_ordered_streams.len().max(ordered_streams.len());
        let per_stream_queue_depth = per_stream_inbound_queue_depth(
            limits.max_inbound_queue_depth,
            queue_split_stream_count,
        );
        let mut service_streams = HashMap::new();
        let run_freshness_reaper =
            should_run_freshness_reaper(queue_split_stream_count, request_response_stream_count);
        let open_context = SessionOpenContext {
            connection: &connection,
            stream_sem: &stream_sem,
            resources: &connection_resources,
            limits,
            per_stream_queue_depth,
            connection_token: connection_token.clone(),
            close_cause: close_cause.clone(),
            freshness_tx: freshness_tx.clone(),
            conn: conn.clone(),
            peer_id: peer_id.clone(),
            direction: context.direction,
            session_exit_tx: session_exit_tx.clone(),
            session_funding: session_funding.as_deref(),
        };

        if negotiated_ordered_streams.is_empty() {
            self.registry.add_peer(
                Peer::new_with_conn_id_and_direction_and_close_cause(
                    conn_id,
                    peer_id.clone(),
                    remote_ip,
                    accepted_capabilities,
                    context.direction,
                    HashMap::new(),
                    connection_token.clone(),
                    close_cause.clone(),
                )
                .with_load(load.clone()),
            );
            cleanup_guard.add_admitted_capabilities(accepted_capabilities);
        } else if !connection_token.is_cancelled() {
            let mut opened_capabilities = 0;
            for stream in ordered_streams {
                // The peer paces this open through stream credit. Local
                // shutdown or eviction cancels the connection meanwhile, and
                // dropping the open resets its streams and returns its permits.
                let opened = zakura_rt::select! {
                    biased;
                    () = connection_token.cancelled() => break,
                    opened = self.open_traced_session(
                        &open_context,
                        stream,
                        &mut session_io,
                        &mut workers,
                        &mut message_buckets,
                    ) => opened,
                };
                let admitted = match opened {
                    Ok(admitted) => admitted,
                    Err(ZakuraHandlerError::SessionFull) => {
                        // Demand is advisory: another connection can reserve the
                        // last slot before this open. Retry only this service.
                        session_io.schedule_transport_backoff(stream.kind);
                        continue;
                    }
                    Err(error) => {
                        debug!(
                            ?error,
                            stream_kind = stream.kind,
                            "closing Zakura peer after ordered stream setup failed"
                        );
                        connection.close(
                            VarInt::from_u32(ZAKURA_CLOSE_RESOURCE),
                            b"ordered stream setup",
                        );
                        close_cause.record(CloseOutcome::neutral(
                            NeutralCause::IoFailure,
                            "stream_setup_failed",
                        ));
                        connection_token.cancel();
                        break;
                    }
                };
                opened_capabilities |= stream.capability;
                #[cfg(any(test, zakura_sim))]
                let admitted = session_io.with_origin(conn_id, admitted, true);
                service_streams.extend(admitted.into_service_streams());
            }
            if !connection_token.is_cancelled() {
                // Escalation is already narrowed to opened ordered services.
                // Disconnect fanout still uses the registry's returned admitted
                // mask, not this peer context.
                let admitted_capabilities = self.registry.add_escalated_peer(
                    Peer::new_with_service_streams(
                        conn_id,
                        peer_id.clone(),
                        remote_ip,
                        opened_capabilities,
                        context.direction,
                        std::mem::take(&mut service_streams),
                        connection_token.clone(),
                        close_cause.clone(),
                    )
                    .with_load(load.clone())
                    .with_rtt_reader(Some(connection.rtt_reader()))
                    .with_connection_credit(conn_credit.clone()),
                );
                cleanup_guard.add_admitted_capabilities(admitted_capabilities);
            }
        }

        if !connection_token.is_cancelled() {
            for (stream, demand) in deferred_ordered_streams {
                debug!(
                    ?peer_id,
                    stream_kind = stream.kind,
                    ?demand,
                    "deferring ordered service session according to reactor demand"
                );
                session_io.schedule_demand(stream.kind, demand);
            }
        }

        // Local reopens whose transport IO is in flight, created on the first
        // reopen.
        let mut local_opens: Option<FuturesUnordered<BoxFuture<'_, _>>> = None;
        // The credit machine wakes this loop only while it has credit work.
        let mut credit_waiter = conn_credit
            .as_ref()
            .map(super::memory::ConnectionCredit::waiter);
        loop {
            let session_deadline = pending_sessions.deadline();
            zakura_rt::select! {
                biased;
                _ = connection_token.cancelled() => break,
                _ = async {
                    match credit_waiter.as_mut() {
                        Some(waiter) => waiter.wait().await,
                        None => future::pending().await,
                    }
                }, if credit_waiter.is_some() => {
                    if let (Some(credit), Some(waiter), Some(pool)) =
                        (&conn_credit, credit_waiter.as_mut(), &self.memory_pool)
                    {
                        credit.tick_send_window(pool, &serving_peer_id, serving_peer_floor);
                        waiter.rearm();
                    }
                }
                _ = async {
                    match session_deadline {
                        Some(deadline) => zakura_rt::time::sleep_until(deadline).await,
                        None => future::pending().await,
                    }
                } => pending_sessions.expire(Instant::now()),
                _ = freshness_reaper(freshness_rx.clone(), limits.idle_timeout), if run_freshness_reaper => {
                    connection.close(VarInt::from_u32(ZAKURA_CLOSE_NEUTRAL), b"idle");
                    close_cause
                        .record(CloseOutcome::neutral(NeutralCause::Timeout, "idle_timeout"));
                    break;
                }
                Some(exited) = session_exit_rx.recv() => {
                    // Stop tracking the dead generation, or a legitimate reopen of the
                    // same kind would look like a duplicate stream and kill the whole
                    // connection, taking block sync and gossip down with header sync.
                    if let Some(delay) =
                        session_io.on_exit(exited, connection_token.is_cancelled())
                    {
                        debug!(
                            stream_kind = exited.stream.kind,
                            stream_version = exited.stream.version,
                            session_id = exited.session_id,
                            ?delay,
                            "scheduling Zakura ordered stream reopen"
                        );
                    }
                }
                Some((kind, ())) = session_io.waits.next(), if !session_io.waits.is_empty() => {
                    let Some(stream) =
                        session_io.on_wait_fired(kind, connection_token.is_cancelled())
                    else {
                        continue;
                    };

                    let demand = self.registry.session_demand(
                        stream.kind,
                        conn_id,
                        accepted_capabilities,
                        &peer_id,
                        context.direction,
                    );
                    if !session_io.on_reopen_demand(stream.kind, demand) {
                        continue;
                    }

                    // The peer paces `open_bi` and the prelude write through
                    // stream credit, so they run beside this loop. The service
                    // is reserved only once the streams are ready.
                    let attempt = session_io.begin_attempt(stream.kind);
                    let open = &open_context;
                    local_opens.get_or_insert_with(FuturesUnordered::new).push(
                        async move { (stream, attempt, self.open_session_streams(open, stream).await) }
                            .boxed(),
                    );
                }
                Some((stream, attempt, opened)) = async { local_opens.as_mut()?.next().await },
                    if local_opens.as_ref().is_some_and(|opens| !opens.is_empty()) =>
                {
                    let opened = match opened {
                        Ok(members)
                            if !session_io
                                .accepts_local_open(stream.kind, connection_token.is_cancelled()) =>
                        {
                            // A peer session took the service while this open
                            // ran. Dropping the members resets their streams.
                            drop(members);
                            session_io.on_open_failed(stream.kind, attempt);
                            continue;
                        }
                        Ok(members) => self.admit_opened_session(
                            &open_context,
                            stream,
                            members,
                            &mut workers,
                            &mut message_buckets,
                        ),
                        Err(error) => Err(error),
                    };
                    match opened {
                        Ok(admitted) => {
                            session_io.on_opened(admitted.kind, admitted.session_id, attempt);
                            #[cfg(any(test, zakura_sim))]
                            let admitted = session_io.with_origin(conn_id, admitted, true);
                            let service_streams = admitted.into_service_streams();
                            let admitted_capabilities = self.registry.add_escalated_peer(
                                Peer::new_with_service_streams(
                                    conn_id,
                                    peer_id.clone(),
                                    remote_ip,
                                    accepted_capabilities,
                                    context.direction,
                                    service_streams,
                                    connection_token.clone(),
                                    close_cause.clone(),
                                )
                                .with_load(load.clone())
                                .with_rtt_reader(Some(connection.rtt_reader()))
                                .with_connection_credit(conn_credit.clone()),
                            );
                            cleanup_guard.add_admitted_capabilities(admitted_capabilities);
                        }
                        Err(error) => {
                            session_io.on_open_failed(stream.kind, attempt);
                            let delay = session_io.schedule_transport_backoff(stream.kind);
                            debug!(
                                ?error,
                                stream_kind = stream.kind,
                                ?delay,
                                "retrying Zakura ordered stream reopen after local failure"
                            );
                        }
                    }
                }
                Some(joined) = workers.join_next() => {
                    if let Err(error) = joined {
                        debug!(?error, "Zakura stream worker exited unexpectedly");
                    }
                }
                setup = async {
                    incoming_setup.as_mut().expect("setup branch is enabled only while reading").await
                }, if incoming_setup.is_some() => {
                    incoming_setup = None;
                    let Some(setup) = setup else { continue; };
                    if setup.session.as_ref().is_some_and(|(layout, _)| {
                        session_io.rejects_remote_offer(layout.primary().kind)
                    }) {
                        continue;
                    }
                    let mut admission = StreamAdmission {
                        is_initiator: context.is_initiator,
                        direction: context.direction,
                        conn: conn.clone(),
                        peer_id: &peer_id,
                        stream_sem: &stream_sem,
                        resources: &connection_resources,
                        open_limiter: &mut open_limiter,
                        message_buckets: &mut message_buckets,
                        workers: &mut workers,
                        limits,
                        accepted_capabilities,
                        connection_token: connection_token.clone(),
                        close_cause: close_cause.clone(),
                        freshness_tx: freshness_tx.clone(),
                        session_funding: session_funding.as_deref(),
                    };
                    if let Some(admitted) = self.finish_bi_stream_setup(
                        setup, &mut admission, per_stream_queue_depth,
                        session_exit_tx.clone(), &mut pending_sessions,
                    ) {
                        let kind = admitted.kind;

                        // A stream kind we never negotiated is a protocol
                        // fault: tear the connection down (unchanged
                        // strictness).
                        let Some(completion) =
                            session_io.on_remote_complete(kind, admitted.session_id)
                        else {
                            debug!(
                                stream_kind = kind,
                                "closing peer after unexpected ordered stream"
                            );
                            close_cause.record(CloseOutcome::neutral(
                                NeutralCause::IncompatibleConfiguration,
                                "unexpected_stream",
                            ));
                            connection_token.cancel();
                            continue;
                        };
                        let demand = match completion {
                            RemoteCompletion::ParkCollision => {
                                debug!(
                                    stream_kind = kind,
                                    "winning ordered-stream collision; parking peer's duplicate"
                                );
                                admitted.cancel_token.cancel();
                                continue;
                            }
                            // For a collision we lost, demand is implied
                            // because we already opened the same kind.
                            RemoteCompletion::AdoptCollision => SessionDemand::OpenNow,
                            // Honour the owning service's current per-peer
                            // demand.
                            RemoteCompletion::CheckDemand => {
                                self.registry.reserved_session_demand(
                                    kind, conn_id, accepted_capabilities, &peer_id, context.direction,
                                )
                            }
                        };
                        // Parking frees this kind, both for a later re-offer by
                        // the peer and for a local demand re-check.
                        if !session_io.on_remote_demand(kind, admitted.session_id, demand) {
                            metrics::counter!(
                                "zakura.p2p.stream.parked.no_demand",
                                "stream_kind" => stream_kind_label(kind),
                            )
                            .increment(1);
                            info!(
                                ?peer_id,
                                stream_kind = kind,
                                "locally parking ordered service stream because the service has no demand"
                            );
                            admitted.cancel_token.cancel();
                            continue;
                        }

                        #[cfg(any(test, zakura_sim))]
                        let admitted = session_io.with_origin(conn_id, admitted, false);
                        let service_streams = admitted.into_service_streams();
                        // We keep the full accepted capability context so
                        // discovery can make cross-service ownership
                        // decisions; disconnect fanout still uses the
                        // registry's returned admitted mask. When this is
                        // a lost collision, `add_escalated_peer` →
                        // `add_peer` replaces our own opened session for
                        // this peer (see `can_admit_peer`).
                        let admitted_capabilities = self.registry.add_escalated_peer(
                            Peer::new_with_service_streams(
                                conn_id,
                                peer_id.clone(),
                                remote_ip,
                                accepted_capabilities,
                                context.direction,
                                service_streams,
                                connection_token.clone(),
                                close_cause.clone(),
                            )
                            .with_load(load.clone())
                            .with_rtt_reader(Some(connection.rtt_reader()))
                            .with_connection_credit(conn_credit.clone()),
                        );
                        cleanup_guard.add_admitted_capabilities(admitted_capabilities);
                    }
                }
                accepted = connection.accept_bi(), if incoming_setup.is_none() => {
                    match accepted {
                        Ok((send, recv)) => {
                            let mut admission = StreamAdmission {
                                is_initiator: context.is_initiator,
                                direction: context.direction,
                                conn: conn.clone(),
                                peer_id: &peer_id,
                                stream_sem: &stream_sem,
                                resources: &connection_resources,
                                open_limiter: &mut open_limiter,
                                message_buckets: &mut message_buckets,
                                workers: &mut workers,
                                limits,
                                accepted_capabilities,
                                connection_token: connection_token.clone(),
                                close_cause: close_cause.clone(),
                                freshness_tx: freshness_tx.clone(),
                                session_funding: session_funding.as_deref(),
                            };
                            incoming_setup = self.begin_bi_stream_setup(send, recv, &mut admission);
                        }
                        Err(error) => {
                            debug!(?error, "Zakura connection stopped accepting streams");
                            close_cause.record(CloseOutcome::neutral(
                                NeutralCause::IoFailure,
                                "accept_failed",
                            ));
                            break;
                        }
                    }
                }
                outbound = outbound_rx.recv() => {
                    let Some(outbound) = outbound else {
                        close_cause.record(CloseOutcome::neutral(
                            NeutralCause::LocalCancellation,
                            "outbound_closed",
                        ));
                        break;
                    };
                    match outbound {
                        ZakuraOutboundFrame::Request {
                            stream_kind,
                            request_id,
                            message_type,
                            flags,
                            payload,
                            started,
                            mut completion,
                        } => {
                            let Some(stream) = self.registry.stream_for_kind(stream_kind) else {
                                let _ = completion.send(Err(format!(
                                    "Zakura outbound request stream kind {stream_kind} is not registered"
                                )
                                .into()));
                                continue;
                            };
                            if let Some(started) = started {
                                let _ = started.send(());
                            }
                            let result = zakura_rt::select! {
                                biased;
                                _ = completion.closed() => {
                                    Err(OutboundRequestError::Local(
                                        "Zakura outbound request receiver dropped".into(),
                                    ))
                                }
                                result = write_outbound_request_frame(
                                    &connection,
                                    &connection_resources,
                                    limits,
                                    stream,
                                    request_id,
                                    message_type,
                                    flags,
                                    payload,
                                ) => result,
                            };
                            match result {
                                Ok(frames) => {
                                    let _ = completion.send(Ok(frames));
                                }
                                Err(OutboundRequestError::Local(error)) => {
                                    let _ = completion.send(Err(error));
                                }
                                Err(OutboundRequestError::Fatal(error)) => {
                                    connection.close(
                                        VarInt::from_u32(ZAKURA_CLOSE_BAD_PRELUDE),
                                        b"malformed response",
                                    );
                                    close_cause.record(CloseOutcome::neutral(
                                        NeutralCause::IoFailure,
                                        "bad_response",
                                    ));
                                    connection_token.cancel();
                                    let _ = completion.send(Err(error));
                                }
                            }
                        }
                    }
                }
            }
        }

        connection_token.cancel();
        // An open still in flight resets its streams and returns its permits.
        drop(local_opens);
        drop(incoming_setup);
        drop(pending_sessions);
        let drained = drain_stream_workers(&mut workers).await;
        if !drained {
            debug!("aborted Zakura stream workers that outlasted the teardown drain");
        }
        self.resources.report_usage(limits_owner_bound);
        cleanup_guard.cleanup_registered_peer().await;
        metrics::counter!("zakura.p2p.conn.closed.neutral").increment(1);
        conn.trace_connection(
            "closed.neutral",
            None,
            None,
            Some(close_cause.get_or("cancelled")),
        );
        #[cfg(any(test, zakura_sim))]
        if drained {
            close_cause.torn_down();
        }
        Ok(())
    }

    /// Open `stream`'s service session under one traced attempt.
    async fn open_traced_session(
        &self,
        open: &SessionOpenContext<'_>,
        stream: Stream,
        session_io: &mut ServiceSessionIO,
        workers: &mut JoinSet<()>,
        message_buckets: &mut MessageRateBuckets,
    ) -> Result<AdmittedSession, ZakuraHandlerError> {
        let attempt = session_io.begin_attempt(stream.kind);
        let opened = match self.open_session_streams(open, stream).boxed().await {
            Ok(members) => {
                self.admit_opened_session(open, stream, members, workers, message_buckets)
            }
            Err(error) => Err(error),
        };
        match &opened {
            Ok(admitted) => session_io.on_opened(admitted.kind, admitted.session_id, attempt),
            Err(_) => session_io.on_open_failed(stream.kind, attempt),
        }
        opened
    }

    /// Open every member stream of `stream`'s service session and write their
    /// preludes. This is only transport IO, which the peer paces through
    /// stream credit; [`Self::admit_opened_session`] reserves the service.
    async fn open_session_streams(
        &self,
        open: &SessionOpenContext<'_>,
        stream: Stream,
    ) -> Result<Vec<OpenedStream>, ZakuraHandlerError> {
        let layout = self
            .registry
            .session_layout(stream)
            .expect("only negotiated persistent streams open service sessions");
        if layout.primary() != stream {
            return Err(ZakuraHandlerError::InvalidServiceSession);
        }
        let wire_id = layout.is_multi_stream().then(random_stream_session_seed);
        let mut opened = Vec::with_capacity(layout.streams().len());
        for member in layout.streams().iter().copied() {
            opened.push(
                self.open_ordered_stream(
                    open.connection,
                    member,
                    wire_id,
                    open.stream_sem,
                    open.resources,
                    open.limits,
                )
                .await?,
            );
        }
        Ok(opened)
    }

    /// Reserve `stream`'s service for the opened `members` and start the
    /// session's workers. A refused reservation drops the members, which
    /// resets their streams.
    fn admit_opened_session(
        &self,
        open: &SessionOpenContext<'_>,
        stream: Stream,
        members: Vec<OpenedStream>,
        workers: &mut JoinSet<()>,
        message_buckets: &mut MessageRateBuckets,
    ) -> Result<AdmittedSession, ZakuraHandlerError> {
        let reservation = self
            .registry
            .service_for_kind(stream.kind)
            .expect("a selected stream has an owning service")
            .reserve_session(open.direction)
            .map_err(|_| ZakuraHandlerError::SessionFull)?;
        // The service admitted the session, so its base is held now.
        if let Some(funding) = open.session_funding {
            funding
                .fund_session(stream.capability)
                .map_err(|_| ZakuraHandlerError::SessionFull)?;
        }
        let prepared = members.into_iter().map(|member| {
            let mut member = self.prepare_opened_stream(
                member,
                message_buckets,
                open.limits,
                open.connection_token.clone(),
                open.close_cause.clone(),
                open.freshness_tx.clone(),
                open.conn.clone(),
                open.peer_id.clone(),
            );
            member.set_session_resources(reservation.clone());
            member
        });
        Ok(spawn_service_session(
            workers,
            prepared,
            open.per_stream_queue_depth,
            true,
            open.session_exit_tx.clone(),
        ))
    }

    /// Start at most one peer-paced setup read per connection. The returned
    /// future owns its stream permit but no service reservation.
    fn begin_bi_stream_setup(
        &self,
        mut send: SendStream,
        recv: RecvStream,
        admission: &mut StreamAdmission<'_>,
    ) -> Option<BoxFuture<'static, Option<IncomingStreamSetup>>> {
        let stream_id = self.next_stream_id.fetch_add(1, Ordering::Relaxed);
        let Ok(permit) = admission.stream_sem.clone().try_acquire_owned() else {
            let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_RESOURCE));
            metrics::counter!("zakura.p2p.stream.rejected.semaphore").increment(1);
            admission
                .conn
                .trace_stream("rejected.semaphore", stream_id, None);
            return None;
        };
        let owner = admission.resources.owner();

        // Charge the per-connection stream-open rate token immediately after
        // acquiring a concurrency permit, before parsing the prelude or running
        // the kind/capability/mode checks below. Each of those rejections resets
        // the stream only and keeps the connection, so charging the token here is
        // what bounds protocol-invalid stream churn (bad prelude, unknown kind,
        // unnegotiated capability): otherwise an authenticated peer could open
        // such streams indefinitely without ever spending open-rate budget.
        if !admission.open_limiter.try_take() {
            let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_RATE_LIMIT));
            metrics::counter!("zakura.p2p.stream.rejected.open_rate").increment(1);
            admission
                .conn
                .trace_stream("rejected.open_rate", stream_id, None);
            return None;
        }

        let mut io = SetupIo::new(send, recv);
        let registry = self.registry.clone();
        let conn = admission.conn.clone();
        let limits = admission.limits;
        let accepted_capabilities = admission.accepted_capabilities;
        let is_initiator = admission.is_initiator;
        let close_cause = admission.close_cause.clone();
        let connection_token = admission.connection_token.clone();
        #[cfg(any(test, zakura_sim))]
        if let Some(tap) = close_cause.tap() {
            tap.emit(super::TransportEvent::StreamSetupStarted {
                conn_id: tap.conn_id(),
                quic_stream: io.streams().0.id(),
                rate: limits.stream_open_rate_per_second,
            });
        }
        Some(Box::pin(async move {
            let (send, recv) = io.streams();
            let prelude = match read_stream_prelude(recv, limits.prelude_timeout).await {
                Ok(prelude) => prelude,
                Err(error) => {
                    #[cfg(any(test, zakura_sim))]
                    report_prelude_judged(
                        &close_cause,
                        send,
                        Some(super::handshake::PreludeRejection::BadPrelude),
                    );
                    debug!(?error, "rejecting Zakura stream with bad prelude");
                    let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_BAD_PRELUDE));
                    metrics::counter!("zakura.p2p.stream.rejected.prelude").increment(1);
                    conn.trace_stream("rejected.prelude", stream_id, None);
                    return None;
                }
            };
            let stream_kind = stream_kind_label(prelude.stream_kind);

            let Some(stream) = registry.stream(prelude.stream_kind, prelude.stream_version) else {
                debug!(
                    stream_kind = prelude.stream_kind,
                    stream_version = prelude.stream_version,
                    "rejecting Zakura stream with unknown kind or unsupported version"
                );
                let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_UNKNOWN_STREAM));
                metrics::counter!(
                    "zakura.p2p.stream.rejected.unknown_kind",
                    "stream_kind" => stream_kind,
                )
                .increment(1);
                conn.trace_stream("rejected.unknown_kind", stream_id, Some(stream_kind));
                #[cfg(any(test, zakura_sim))]
                {
                    let rejection = super::handshake::PreludeRejection::UnknownKind;
                    report_prelude_judged(&close_cause, send, Some(rejection));
                    report_stream_refused(&close_cause, send, &prelude, rejection.reason());
                }
                return None;
            };

            if accepted_capabilities & stream.capability != stream.capability {
                debug!(
                    stream_kind = prelude.stream_kind,
                    stream_version = prelude.stream_version,
                    accepted_capabilities = accepted_capabilities,
                    "rejecting Zakura stream that was not negotiated for this peer"
                );
                let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_UNKNOWN_STREAM));
                metrics::counter!(
                    "zakura.p2p.stream.rejected.unnegotiated_capability",
                    "stream_kind" => stream_kind,
                )
                .increment(1);
                conn.trace_stream(
                    "rejected.unnegotiated_capability",
                    stream_id,
                    Some(stream_kind),
                );
                #[cfg(any(test, zakura_sim))]
                {
                    let rejection = super::handshake::PreludeRejection::UnnegotiatedCapability;
                    report_prelude_judged(&close_cause, send, Some(rejection));
                    report_stream_refused(&close_cause, send, &prelude, rejection.reason());
                }
                return None;
            }

            if stream.mode == StreamMode::Persistent
                && registry
                    .persistent_streams_for_negotiated(accepted_capabilities)
                    .iter()
                    .all(|selected| *selected != stream)
            {
                debug!(
                    stream_kind = prelude.stream_kind,
                    stream_version = prelude.stream_version,
                    "rejecting a lower ordered-stream version than the mutually selected version"
                );
                let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_UNKNOWN_STREAM));
                metrics::counter!(
                    "zakura.p2p.stream.rejected.unselected_version",
                    "stream_kind" => stream_kind,
                )
                .increment(1);
                conn.trace_stream("rejected.unselected_version", stream_id, Some(stream_kind));
                #[cfg(any(test, zakura_sim))]
                {
                    let rejection = super::handshake::PreludeRejection::UnselectedVersion;
                    report_prelude_judged(&close_cause, send, Some(rejection));
                    report_stream_refused(&close_cause, send, &prelude, rejection.reason());
                }
                return None;
            }

            if stream.mode != StreamMode::RequestResponse && prelude.request_id.is_some() {
                debug!("rejecting non-request Zakura stream with request id");
                let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_BAD_PRELUDE));
                let rejection = super::handshake::PreludeRejection::UnexpectedRequestId;
                close_cause.record(rejection.close_outcome(&prelude));
                #[cfg(any(test, zakura_sim))]
                report_prelude_judged(&close_cause, send, Some(rejection));
                connection_token.cancel();
                metrics::counter!("zakura.p2p.stream.rejected.unexpected_request_id").increment(1);
                conn.trace_stream(
                    "rejected.unexpected_request_id",
                    stream_id,
                    Some(stream_kind),
                );
                return None;
            }

            if stream.mode == StreamMode::RequestResponse && prelude.request_id.is_none() {
                debug!("rejecting Zakura request stream without request id");
                let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_BAD_PRELUDE));
                let rejection = super::handshake::PreludeRejection::RequestWithoutId;
                close_cause.record(rejection.close_outcome(&prelude));
                #[cfg(any(test, zakura_sim))]
                report_prelude_judged(&close_cause, send, Some(rejection));
                connection_token.cancel();
                metrics::counter!("zakura.p2p.stream.rejected.request_without_id").increment(1);
                conn.trace_stream("rejected.request_without_id", stream_id, Some(stream_kind));
                return None;
            }

            // The prelude cap becomes our outbound frame cap for this stream,
            // so it must fit a frame header and at least one payload byte.
            if !frame_cap_carries_payload(prelude.max_frame_bytes) {
                debug!(
                    max_frame_bytes = prelude.max_frame_bytes,
                    "rejecting Zakura stream whose frame cap cannot carry a payload"
                );
                let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_BAD_PRELUDE));
                let rejection = super::handshake::PreludeRejection::UndersizedFrameCap;
                close_cause.record(rejection.close_outcome(&prelude));
                #[cfg(any(test, zakura_sim))]
                report_prelude_judged(&close_cause, send, Some(rejection));
                connection_token.cancel();
                metrics::counter!("zakura.p2p.stream.rejected.undersized_frame_cap").increment(1);
                conn.trace_stream(
                    "rejected.undersized_frame_cap",
                    stream_id,
                    Some(stream_kind),
                );
                return None;
            }

            if stream.mode == StreamMode::Persistent
                && !may_open_ordered_stream(registry.session_policy(stream.kind), !is_initiator)
            {
                debug!(
                    stream_kind = stream.kind,
                    "rejecting an ineligible ordered-stream opener"
                );
                let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_BAD_PRELUDE));
                let _ = recv.stop(VarInt::from_u32(ZAKURA_CLOSE_BAD_PRELUDE));
                let rejection = super::handshake::PreludeRejection::IneligibleOpener;
                close_cause.record(rejection.close_outcome(&prelude));
                #[cfg(any(test, zakura_sim))]
                report_prelude_judged(&close_cause, send, Some(rejection));
                connection_token.cancel();
                return None;
            }

            #[cfg(any(test, zakura_sim))]
            report_prelude_judged(&close_cause, send, None);
            metrics::counter!(
                "zakura.p2p.stream.accepted",
                "stream_kind" => stream_kind,
            )
            .increment(1);
            conn.trace_stream("accepted", stream_id, Some(stream_kind));

            let session = if let Some(layout) = registry.session_layout(stream) {
                let mut bytes = [0u8; 8];
                if layout.is_multi_stream()
                    && !matches!(
                        timeout(limits.prelude_timeout, recv.read_exact(&mut bytes)).await,
                        Ok(Ok(()))
                    )
                {
                    let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_BAD_PRELUDE));
                    let _ = recv.stop(VarInt::from_u32(ZAKURA_CLOSE_BAD_PRELUDE));
                    return None;
                }
                Some((layout, u64::from_le_bytes(bytes)))
            } else {
                None
            };
            Some(IncomingStreamSetup {
                io,
                permit,
                owner,
                stream_id,
                stream,
                prelude,
                session,
            })
        }))
    }

    /// Admit a decoded prelude without waiting on the peer. Lifecycle events
    /// stay reactor-owned while the setup future reads bytes independently.
    fn finish_bi_stream_setup(
        &self,
        incoming: IncomingStreamSetup,
        admission: &mut StreamAdmission<'_>,
        per_stream_queue_depth: usize,
        session_exit_tx: mpsc::UnboundedSender<SessionExit>,
        pending_sessions: &mut PendingSessions,
    ) -> Option<AdmittedSession> {
        let IncomingStreamSetup {
            io,
            permit,
            owner,
            stream_id,
            stream,
            prelude,
            session,
        } = incoming;
        let (mut send, mut recv) = io.take();
        let Some(stream_resources) = admission.resources.stream(stream.kind, owner) else {
            // Admission checked the kind's capability, and the connection gates
            // every negotiated kind, so this is a local inconsistency.
            let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_RESOURCE));
            let _ = recv.stop(VarInt::from_u32(ZAKURA_CLOSE_RESOURCE));
            return None;
        };
        let resources = if let Some((layout, _)) = &session {
            let reserved = pending_sessions
                .reserve_or_share(layout, &self.registry, admission.direction)
                .and_then(|resources| {
                    // The service admitted the session, so its base is held now.
                    match admission.session_funding {
                        Some(funding) => funding
                            .fund_session(layout.primary().capability)
                            .map(|()| resources)
                            .map_err(|_| crate::zakura::SessionFull),
                        None => Ok(resources),
                    }
                });
            match reserved {
                Ok(resources) => resources,
                Err(_) => {
                    let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_RESOURCE));
                    let _ = recv.stop(VarInt::from_u32(ZAKURA_CLOSE_RESOURCE));
                    return None;
                }
            }
        } else {
            None
        };
        let layout_kind = session
            .as_ref()
            .map_or(prelude.stream_kind, |(layout, _)| layout.primary().kind);
        let message_bucket = message_bucket_for(
            admission.message_buckets,
            layout_kind,
            admission.limits.message_rate_per_second,
            RealClock,
        );
        let cadence = cadence_scope(admission.message_buckets, stream, layout_kind);
        let stream_token = admission.connection_token.child_token();

        let context = StreamWorkerContext {
            conn: admission.conn.clone(),
            peer_id: admission.peer_id.clone(),
            stream_id,
            _permit: permit,
            limits: admission.limits,
            inbound_frame_cap: inbound_frame_cap_for_stream(&admission.limits, stream),
            messages: stream.messages,
            queue_depths: stream.queue_depths,
            write_policy: stream.write_policy,
            session_resources: resources,
            outbound_frame_cap: peer_accepted_frame_cap(
                &admission.limits,
                stream,
                prelude.max_frame_bytes,
            ),
            message_bucket,
            cadence,
            precheck: PrecheckSlot::default(),
            resources: stream_resources,
            connection_token: admission.connection_token.clone(),
            stream_token,
            close_cause: admission.close_cause.clone(),
            freshness_tx: admission.freshness_tx.clone(),
            kind_version: (stream.kind, stream.version),
        };

        if let Some((layout, wire_id)) = session {
            let prepared = PreparedStream::new(send, recv, stream, prelude, context);
            return match pending_sessions.insert(&layout, wire_id, prepared) {
                Ok(Some(streams)) => Some(spawn_service_session(
                    admission.workers,
                    streams,
                    per_stream_queue_depth,
                    false,
                    session_exit_tx,
                )),
                Ok(None) => None,
                Err(error) => {
                    debug!(?error, "rejecting mismatched service session");
                    admission.close_cause.record(CloseOutcome::fault(
                        prelude.stream_kind,
                        prelude.stream_version,
                        CloseRow::CONN_CLOSE_17,
                    ));
                    admission.connection_token.cancel();
                    None
                }
            };
        }
        context.resources.owner.advance(OwnerPhase::Installed);
        admission.workers.spawn_named(
            "request_stream_worker",
            request_stream_worker(send, recv, prelude, context, self.registry.clone()),
        );
        None
    }

    async fn register_and_serve(
        &self,
        connection: Connection,
        peer_id: ZakuraPeerId,
        remote_ip: Option<IpAddr>,
        context: ConnectionServeContext,
    ) -> Result<(), ZakuraHandlerError> {
        // Production endpoints are direct-only, so every admitted peer needs an IP slot.
        if remote_ip.is_none() {
            debug!(
                ?peer_id,
                "rejecting native connection without an attributable IP"
            );
            #[cfg(any(test, zakura_sim))]
            self.handshake_refused(&connection, "unattributed_ip");
            connection.close(VarInt::from_u32(ZAKURA_CLOSE_RESOURCE), b"unattributed IP");
            return Ok(());
        }

        // Bound the precheck by the demanded service sessions that this connection can admit.
        // Count each symmetric session regardless of which endpoint opens it.
        let escalation_streams = self.registry.persistent_streams_for_escalation(
            context.accepted_capabilities,
            &peer_id,
            context.direction,
        );
        let ordered_stream_count = escalation_streams.len();
        if ordered_stream_count > usize::from(context.limits.max_open_streams) {
            debug!(
                max_open_streams = context.limits.max_open_streams,
                ordered_stream_count,
                "rejecting Zakura peer before registration because negotiated ordered streams exceed max-open-streams"
            );
            #[cfg(any(test, zakura_sim))]
            self.handshake_refused(&connection, "resource_ordered_streams");
            connection.close(VarInt::from_u32(ZAKURA_CLOSE_RESOURCE), b"ordered streams");
            return Ok(());
        }
        if usize::from(context.limits.max_inbound_queue_depth)
            < minimum_ordered_queue_capacity(
                &self
                    .registry
                    .persistent_streams_for_negotiated(context.accepted_capabilities),
            )
        {
            debug!(
                max_inbound_queue_depth = context.limits.max_inbound_queue_depth,
                ordered_stream_count,
                "rejecting Zakura peer before registration because inbound queue depth cannot be split"
            );
            #[cfg(any(test, zakura_sim))]
            self.handshake_refused(&connection, "resource_queue_split");
            connection.close(VarInt::from_u32(ZAKURA_CLOSE_RESOURCE), b"queue split");
            return Ok(());
        }

        // One counter names the connection, so its session leases and its
        // registration share the ID. `None` means IDs ran out, and
        // registration below refuses the connection.
        let reserved_conn_id = self.supervisor.reserve_conn_id();
        let session_funding = if let (Some(pool), Some(plan), Some(conn_id)) =
            (&self.memory_pool, &self.memory_plan, reserved_conn_id)
        {
            let admitted_sessions = escalation_streams
                .iter()
                .fold(0, |capabilities, stream| capabilities | stream.capability);
            let custom_kinds_negotiated = self
                .registry
                .services_for_negotiated(context.accepted_capabilities)
                .iter()
                .flat_map(|service| service.streams().iter())
                .any(|stream| {
                    stream.capability & context.accepted_capabilities != 0
                        && plan.custom_kinds.contains(&stream.kind)
                });
            let cost = plan.bases.connection_cost(
                context.accepted_capabilities,
                admitted_sessions,
                custom_kinds_negotiated,
            );
            let funded_sessions = super::memory::SessionBases::funded_sessions(
                context.accepted_capabilities,
                admitted_sessions,
            );
            // A preferred gateway leases from the places held for it, as it
            // claims a held connection slot.
            let gateway = peer_id.node_id().is_some_and(|node| {
                self.supervisor
                    .ip_slots
                    .counts()
                    .preferred_gateways()
                    .contains(&node)
            });
            match SessionFunding::acquire(
                pool,
                plan,
                &self.session_directions,
                conn_id,
                super::memory::SessionSide::new(context.direction, gateway),
                cost.total(),
                funded_sessions,
                &self.shutdown,
                self.limits.control_timeout,
            )
            .await
            {
                Ok(funding) => Some(Arc::new(funding)),
                Err(refusal) => {
                    debug!(
                        ?peer_id,
                        %refusal,
                        direction = context.direction.trace_label(),
                        requested_bytes = cost.total(),
                        "rejecting Zakura peer because Consumer::Sessions cannot fund transport and reactor session bases together"
                    );
                    metrics::counter!("zakura.p2p.conn.rejected.session_memory").increment(1);
                    #[cfg(any(test, zakura_sim))]
                    self.handshake_refused(&connection, "session_memory");
                    connection.close(VarInt::from_u32(ZAKURA_CLOSE_RESOURCE), b"session memory");
                    if let Some(report) = context.registration_report {
                        let _ = report.send(DialRegistration::LocalLimit);
                    }
                    return Ok(());
                }
            }
        } else {
            None
        };

        let (outbound_tx, outbound_rx) =
            mpsc::channel(usize::from(context.limits.max_inbound_queue_depth));
        let close_cause = CloseCause::new();
        #[cfg(any(test, zakura_sim))]
        self.observe_close(&close_cause, &connection);
        let outbound_handle = ZakuraPeerHandle {
            peer_id: peer_id.clone(),
            sender: outbound_tx,
        };
        let connection_token = self.shutdown.child_token();
        // Every way the connection's token is cancelled reaches this hook, so
        // it's the one place a close with no recorded cause is decided.
        #[cfg(zakura_sim)]
        close_cause.decide_when_cancelled(&connection_token);
        let registration = self
            .supervisor
            .register_with_reserved_conn_id(
                reserved_conn_id,
                peer_id,
                &connection.remote_earlier_ids(),
                remote_ip,
                context.transcript_hash,
                outbound_handle,
                connection_token.clone(),
                close_cause.clone(),
                context.accepted_capabilities,
            )
            .await;
        // Before the guard's drop below, which can be a scheduling point, so
        // the connection's events carry its `conn_id` from registration on.
        #[cfg(any(test, zakura_sim))]
        if let ZakuraRegistration::Registered { conn_id, .. } = &registration {
            self.observe_registered(*conn_id, &close_cause, &connection);
            transport_observe!(
                self.transport_observer,
                super::TransportEvent::Handshake {
                    remote: connection.admitted_addr(),
                    shared_id: connection.shared_id(),
                    stage: "registered",
                    validation: None,
                    error: None,
                    elapsed: None,
                }
            );
        }
        #[cfg(zakura_sim)]
        if matches!(&registration, ZakuraRegistration::Registered { .. }) {
            self.pause_handshake(super::HandshakePhase::Registered)
                .await;
        }
        // A registered connection now counts as registered in `ip_slots`.
        // It keeps its cadence token, so reconnecting costs a token.
        drop(context.control_handshake);
        if let ZakuraRegistration::Registered {
            conn_id,
            peer_id,
            disconnect_token,
            ..
        } = &registration
        {
            if self.capability_epoch.load(Ordering::SeqCst) != context.capability_epoch {
                #[cfg(any(test, zakura_sim))]
                self.registration_refused(&close_cause, &connection, "capabilities_changed");
                connection.close(
                    VarInt::from_u32(ZAKURA_CLOSE_NEUTRAL),
                    b"capabilities changed",
                );
                RegisteredPeerCleanupGuard::new(
                    self.supervisor.clone(),
                    self.registry.clone(),
                    peer_id.clone(),
                    *conn_id,
                    disconnect_token.clone(),
                )
                .cleanup_registered_peer()
                .await;
                transport_observe!(
                    self.transport_observer,
                    super::TransportEvent::Handshake {
                        remote: connection.admitted_addr(),
                        shared_id: connection.shared_id(),
                        stage: "registration-released",
                        validation: None,
                        error: None,
                        elapsed: None,
                    }
                );
                // A local decision closed the peer, so the dialer retries
                // without backing off.
                if let Some(report) = context.registration_report {
                    let _ = report.send(DialRegistration::LocalLimit);
                }
                return Ok(());
            }
        }
        if let Some(report) = context.registration_report {
            let reported = match &registration {
                ZakuraRegistration::Registered { .. } => Some(DialRegistration::Registered),
                ZakuraRegistration::Rejected(
                    ZakuraRejectReason::ResourceLimit | ZakuraRejectReason::TemporaryUnavailable,
                ) => Some(DialRegistration::LocalLimit),
                ZakuraRegistration::Rejected(_) | ZakuraRegistration::Duplicate { .. } => None,
            };
            if let Some(reported) = reported {
                // The receiver is gone when the caller stopped waiting for the result.
                let _ = report.send(reported);
            }
        }

        match registration {
            ZakuraRegistration::Registered {
                conn_id,
                peer_id,
                remote_ip,
                disconnect_token,
            } => {
                metrics::counter!("zakura.p2p.conn.accepted", "role" => context.role).increment(1);
                context.conn.trace_connection(
                    "accepted",
                    Some(context.role),
                    Some(context.direction.trace_label()),
                    None,
                );
                let cleanup_guard = RegisteredPeerCleanupGuard::new(
                    self.supervisor.clone(),
                    self.registry.clone(),
                    peer_id.clone(),
                    conn_id,
                    disconnect_token.clone(),
                )
                .with_session_funding(session_funding);
                self.serve_connection(
                    connection,
                    peer_id,
                    remote_ip,
                    outbound_rx,
                    RegisteredConnectionServeContext {
                        limits: context.limits,
                        metered_epoch: context.metered_epoch,
                        conn: context.conn,
                        conn_id,
                        connection_token: disconnect_token,
                        close_cause,
                        accepted_capabilities: context.accepted_capabilities,
                        is_initiator: context.role == "initiator",
                        i_open_collision_winner: context.i_open_collision_winner,
                        direction: context.direction,
                    },
                    cleanup_guard,
                )
                .await
            }
            ZakuraRegistration::Duplicate { peer_id } => {
                debug!(?peer_id, "closing duplicate Zakura peer neutrally");
                metrics::counter!("zakura.p2p.conn.duplicate").increment(1);
                context.conn.trace_connection(
                    "duplicate",
                    Some(context.role),
                    Some(context.direction.trace_label()),
                    None,
                );
                #[cfg(any(test, zakura_sim))]
                self.registration_refused(&close_cause, &connection, "duplicate");
                connection.close(VarInt::from_u32(ZAKURA_CLOSE_NEUTRAL), b"duplicate");
                Ok(())
            }
            ZakuraRegistration::Rejected(reason) => {
                debug!(
                    ?reason,
                    "closing Zakura peer neutrally after registration rejection"
                );
                context.conn.trace_connection(
                    "rejected.admission",
                    Some(context.role),
                    Some(context.direction.trace_label()),
                    Some(reject_reason_label(reason)),
                );
                #[cfg(any(test, zakura_sim))]
                self.registration_refused(&close_cause, &connection, reject_reason_label(reason));
                connection.close(VarInt::from_u32(ZAKURA_CLOSE_RESOURCE), b"registration");
                Ok(())
            }
        }
    }
}

/// One IP may hold at most this fraction of the pending-handshake budget, so
/// a few addresses can't refuse every other inbound peer. With the default
/// budget of 32, an IP gets 4 handshakes at a time.
const PENDING_HANDSHAKE_SHARES_PER_IP: usize = 8;

/// Retries an unvalidated source, so spoofed Initials cannot fill a budget,
/// and refuses a validated one. Counts the decision under its metric.
fn retry_or_refuse(
    incoming: &IncomingInfo,
    retried: &'static str,
    rejected: &'static str,
) -> Admit {
    if incoming.validated {
        metrics::counter!(rejected).increment(1);
        Admit::Refuse
    } else {
        metrics::counter!(retried).increment(1);
        Admit::Retry
    }
}

impl Acceptor for ZakuraProtocolHandler {
    /// Applies zakura-quic ADM-3 rules 2 and 4 plus the connection cap, each
    /// within inbound's share, before any handshake work. The endpoint applies
    /// rules 3 and 5 itself, and Zakura has no native IP ban list for rule 1.
    fn admit(&self, incoming: &IncomingInfo) -> Admit {
        if self.shutdown.is_cancelled() {
            return Admit::Ignore;
        }
        let max_handshakes_per_ip =
            (self.limits.max_pending_handshakes / PENDING_HANDSHAKE_SHARES_PER_IP).max(1);
        // One lock section decides every count, so no check sees another's
        // stale value.
        let mut counts = self.supervisor.ip_slots.counts();
        if counts.ip_admission(
            incoming.remote.ip(),
            incoming.pending_from_ip,
            max_handshakes_per_ip,
            self.supervisor.max_connections_per_ip,
            incoming.validated,
        ) != IpAdmission::Open
        {
            // Require validation before consuming the last slot, including
            // the redial slot, so spoofed Initials cannot fill an IP's budget.
            return retry_or_refuse(
                incoming,
                "zakura.p2p.conn.retried.per_ip",
                "zakura.p2p.conn.rejected.per_ip",
            );
        }
        // Every pending transport handshake is inbound, so it counts against
        // both the inbound share and the whole handshake budget.
        if counts.pending_full(incoming.pending_total, self.limits.max_pending_handshakes) {
            // As above: spoofed Initials could fill the budget, so an
            // unvalidated source gets a Retry instead of a refusal.
            return retry_or_refuse(
                incoming,
                "zakura.p2p.conn.retried.pending_handshake",
                "zakura.p2p.conn.rejected.pending_handshake",
            );
        }
        // TLS 1.3 proves the caller's node ID only in its last flight, after
        // this node has done its handshake work, so a held place that no
        // preferred gateway fills has to admit callers whose ID is not known
        // yet. The claim after the handshake (`claim_inbound`) decides by the
        // proven ID: a caller that turns out to be ordinary is refused there
        // and keeps its cadence charges, so held places give no free
        // handshakes.
        if !counts.inbound_place_free(self.limits.max_connections) {
            metrics::counter!("zakura.p2p.conn.rejected.admission").increment(1);
            return Admit::Refuse;
        }
        // The machine's transport reservations can exceed QUIC's pending
        // count, so the claim checks the pending budget again.
        if !counts.claim_transport(self.limits.max_pending_handshakes) {
            return retry_or_refuse(
                incoming,
                "zakura.p2p.conn.retried.pending_handshake",
                "zakura.p2p.conn.rejected.pending_handshake",
            );
        }
        // Each source validation state has its own ledger, so a spoofed
        // Initial, which is never validated, can't take a validated source's
        // tokens. An unvalidated source that runs out gets a stateless Retry
        // below, which an honest peer answers with a validated Initial.
        let cadence = counts.admit_cadence(
            incoming.remote.ip(),
            incoming.validated,
            self.supervisor.max_connections_per_ip,
            self.limits
                .total_connections()
                .saturating_add(self.limits.max_pending_handshakes),
            Instant::now(),
        );
        if cadence != super::HandshakeCadenceAdmission::Admit {
            counts.release_transport();
        }
        #[cfg(any(test, zakura_sim))]
        let hosts = counts.cadence_hosts(incoming.validated);
        drop(counts);
        #[cfg(any(test, zakura_sim))]
        self.observe_handshake_cadence(
            incoming.remote,
            incoming.validated,
            match cadence {
                super::HandshakeCadenceAdmission::Admit => "admitted",
                super::HandshakeCadenceAdmission::Exhausted => "exhausted",
                super::HandshakeCadenceAdmission::Capacity => "capacity",
                super::HandshakeCadenceAdmission::NodeExhausted => "node-exhausted",
            },
            hosts,
        );
        match cadence {
            super::HandshakeCadenceAdmission::Admit => Admit::Accept,
            super::HandshakeCadenceAdmission::Exhausted
            | super::HandshakeCadenceAdmission::Capacity => retry_or_refuse(
                incoming,
                "zakura.p2p.conn.retried.cadence",
                "zakura.p2p.conn.rejected.cadence",
            ),
            super::HandshakeCadenceAdmission::NodeExhausted => retry_or_refuse(
                incoming,
                "zakura.p2p.conn.retried.node_cadence",
                "zakura.p2p.conn.rejected.node_cadence",
            ),
        }
    }

    fn cancel_admission(&self, incoming: &IncomingInfo) {
        self.finish_admission(false);
        self.refund_cadence(incoming.remote, incoming.validated);
    }

    fn finish_admission(&self, handed_off: bool) {
        if !handed_off {
            self.supervisor.ip_slots.counts().release_transport();
        }
    }

    fn alpns(&self) -> Vec<Vec<u8>> {
        supported_p2p_v2_alpns()
    }

    /// Counts pending handshakes by [`admission_host`], the key every per-IP
    /// count here uses, so one IPv6 /64 gets one share of the budget.
    fn pending_group(&self, ip: IpAddr) -> IpAddr {
        admission_host(ip)
    }

    fn handle(&self, connection: Connection) -> BoxFuture<'static, ()> {
        let handler = self.clone();
        let claim = handler.claim_inbound(&connection, true);
        if claim.is_err() {
            // TLS already ran. Return the pending slot and keep both rate charges.
            handler.finish_admission(false);
        }
        Box::pin(async move { handler.serve_inbound(connection, claim).await })
    }
}

/// The budget an inbound claim found full.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InboundRefusal {
    /// The connection cap or its inbound share.
    Admission,
    /// The ordinary slots, with the rest held for preferred gateways.
    Hold,
    /// The pending-handshake budget or its inbound share.
    PendingHandshake,
}

/// An inbound connection's share of the connection cap and its per-IP control
/// handshake slot, held from `Acceptor::handle` on.
struct InboundClaim {
    pending: PendingHandshakeGuard,
    _admission: ConnectionAdmissionGuard,
    control_handshake: ControlHandshakeGuard,
}

/// Loopback IPv4 address the native Zakura endpoint binds to when no
/// `zakura.listen_addr` is configured, so the unset (dial-out-only) state does
/// not expose the P2P_V2_ALPN surface on all interfaces.
const ZAKURA_LOOPBACK_BIND_V4: SocketAddrV4 = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0);
/// Loopback IPv6 counterpart of [`ZAKURA_LOOPBACK_BIND_V4`].
const ZAKURA_LOOPBACK_BIND_V6: SocketAddrV6 = SocketAddrV6::new(Ipv6Addr::LOCALHOST, 0, 0, 0);

/// Returns the native endpoint's bind addresses.
///
/// When `listen_addr` is set, the endpoint binds exactly that address so the
/// node has a stable, advertisable Zakura endpoint. When it is unset, the
/// endpoint binds loopback-only for **both** IPv4 and IPv6 rather than an
/// unspecified address. An unspecified bind would expose the native
/// P2P_V2_ALPN handshake/session surface of an unconfigured, dial-out-only node
/// on every interface on an OS-assigned ephemeral port (zakura-quic SOCK-1).
fn native_bind_addrs(listen_addr: Option<SocketAddr>) -> Vec<SocketAddr> {
    match listen_addr {
        Some(addr) => vec![addr],
        None => vec![
            SocketAddr::V4(ZAKURA_LOOPBACK_BIND_V4),
            SocketAddr::V6(ZAKURA_LOOPBACK_BIND_V6),
        ],
    }
}

/// Returns the endpoint's dialable direct addresses.
///
/// A socket bound to an unspecified address lists each local interface
/// address of its family with the bound port (zakura-quic SOCK-10).
fn local_direct_addrs(quic: &QuicEndpoint) -> Vec<SocketAddr> {
    let mut interface_ips = None;
    let mut addrs = Vec::new();
    for bound in quic.local_addrs() {
        if !bound.ip().is_unspecified() {
            addrs.push(bound);
            continue;
        }
        let ips = interface_ips.get_or_insert_with(zakura_quic::sys::local_interface_ips);
        addrs.extend(
            ips.iter()
                .filter(|ip| ip.is_ipv4() == bound.is_ipv4())
                // A link-local IPv6 address needs a scope ID to be dialable.
                .filter(|ip| !matches!(ip, IpAddr::V6(v6) if v6.is_unicast_link_local()))
                .map(|ip| SocketAddr::new(*ip, bound.port())),
        );
    }
    addrs.sort_unstable();
    addrs.dedup();
    addrs
}

fn discovery_direct_addrs(config: &Config, local_node_id: NodeId) -> Vec<SocketAddr> {
    let Some(listen_addr) = config.zakura.listen_addr else {
        return Vec::new();
    };
    let mut direct_addrs = Vec::new();

    if !listen_addr.ip().is_unspecified() {
        direct_addrs.push(listen_addr);
    }
    if let Some(external_addr) = config.external_addr {
        direct_addrs.push(SocketAddr::new(external_addr.ip(), listen_addr.port()));
    }
    for entry in &config.zakura.bootstrap_peers {
        let Ok(node_addr) = super::discovery::parse_bootstrap_peer(entry) else {
            continue;
        };
        if node_addr.id == local_node_id {
            direct_addrs.extend(node_addr.direct);
        }
    }

    direct_addrs.sort_unstable();
    direct_addrs.dedup();
    direct_addrs
}

/// Whether a listening node's signed discovery record carries no address.
/// The record keeps only addresses that peers may dial, so peers drop a record
/// left with none. A wildcard listener without `external_addr` advertises none.
fn advertises_no_discovery_address(config: &Config, advertised: &[SocketAddr]) -> bool {
    config.zakura.listen_addr.is_some() && advertised.is_empty()
}

/// Counts the distinct parseable bootstrap identities other than `local_node_id`.
pub(crate) fn remote_bootstrap_peer_count(
    bootstrap_peers: &[String],
    local_node_id: impl Into<Option<NodeId>>,
) -> usize {
    let local_node_id = local_node_id.into();
    bootstrap_peers
        .iter()
        .filter_map(|entry| super::discovery::parse_bootstrap_peer(entry).ok())
        .filter(|node_addr| Some(node_addr.id) != local_node_id)
        .map(|node_addr| node_addr.id)
        .collect::<HashSet<_>>()
        .len()
}

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
struct HeaderCapabilityEpochs {
    applied_ready: Option<zakura_node_services::sync_lifecycle::LifecycleEpoch>,
}

impl HeaderCapabilityEpochs {
    fn from_initial(demand: &zakura_node_services::sync_lifecycle::SyncServiceDemand) -> Self {
        let applied_ready = demand.header.capability_epoch();
        Self { applied_ready }
    }

    fn observe(
        &mut self,
        demand: &zakura_node_services::sync_lifecycle::SyncServiceDemand,
    ) -> Option<zakura_node_services::sync_lifecycle::LifecycleEpoch> {
        let epoch = demand.header.capability_epoch()?;
        if self.applied_ready.is_some_and(|applied| epoch <= applied) {
            return None;
        }
        self.applied_ready = Some(epoch);
        Some(epoch)
    }
}

/// Enable header sync for new handshakes.
/// Disconnect connections negotiated before the capability became available.
///
/// A connection has immutable negotiated capabilities.
/// A connection established before checkpoint bootstrap cannot open a header-sync stream.
async fn enable_header_sync_and_renegotiate(
    handler: &ZakuraProtocolHandler,
    supervisor: &ZakuraSupervisorHandle,
    capability_epoch: zakura_node_services::sync_lifecycle::LifecycleEpoch,
) -> usize {
    handler.set_header_sync_enabled(true);

    let peers = supervisor.registered_ids().await;
    let mut disconnected = 0;
    for peer in &peers {
        disconnected += usize::from(supervisor.disconnect_peer(peer).await);
    }

    if disconnected > 0 {
        metrics::counter!("zakura.p2p.header_sync.capability_reconnects")
            .increment(u64::try_from(disconnected).expect("peer count always fits in u64"));
        info!(
            disconnected,
            capability_epoch = capability_epoch.get(),
            "reconnecting peers to negotiate header sync after checkpoint bootstrap"
        );
    }

    disconnected
}

fn provided_services_with_custom(custom_services: &[CustomService]) -> Vec<ZakuraServiceId> {
    let mut provided_services = discovery::default_advertised_services();
    provided_services.extend(
        custom_services
            .iter()
            .flat_map(|custom| custom.provides.iter().cloned()),
    );
    provided_services
}

fn sought_services_with_custom(custom_services: &[CustomService]) -> Vec<ZakuraServiceId> {
    let mut sought_services: Vec<_> = custom_services
        .iter()
        .flat_map(|custom| custom.seeks.iter().cloned())
        .collect();
    sought_services.sort_unstable();
    sought_services.dedup();
    sought_services
}

/// Start a Zakura endpoint and router when P2P v2 is enabled.
#[cfg(test)]
pub async fn spawn_zakura_endpoint(
    config: &Config,
    sink_factory: impl FnOnce(ZakuraSupervisorHandle, ZakuraTrace) -> Arc<dyn Service> + Send + 'static,
) -> Result<Option<ZakuraEndpoint>, BoxError> {
    spawn_zakura_endpoint_with_header_sync_driver(config, sink_factory, None).await
}

/// Start a Zakura endpoint with an externally driven header-sync reactor.
pub async fn spawn_zakura_endpoint_with_header_sync_driver(
    config: &Config,
    sink_factory: impl FnOnce(ZakuraSupervisorHandle, ZakuraTrace) -> Arc<dyn Service> + Send + 'static,
    header_sync_driver_startup: Option<ZakuraHeaderSyncDriverStartup>,
) -> Result<Option<ZakuraEndpoint>, BoxError> {
    spawn_zakura_endpoint_with_services(
        config,
        sink_factory,
        header_sync_driver_startup,
        Vec::new(),
    )
    .await
}

/// Start a Zakura endpoint with optional runtime services.
pub async fn spawn_zakura_endpoint_with_services(
    config: &Config,
    sink_factory: impl FnOnce(ZakuraSupervisorHandle, ZakuraTrace) -> Arc<dyn Service> + Send + 'static,
    header_sync_driver_startup: Option<ZakuraHeaderSyncDriverStartup>,
    custom_services: Vec<CustomService>,
) -> Result<Option<ZakuraEndpoint>, BoxError> {
    spawn_zakura_endpoint_inner(
        config,
        Box::new(sink_factory),
        header_sync_driver_startup,
        custom_services,
        None,
        None,
        None,
        None,
    )
    .await
}

/// Start a Zakura endpoint on `peer_registry`, with `tx-relay` if
/// `tx_relay` is given and BPv1 if `bpv1` is given. The discovery dialer
/// prefers the mining gateways `miner_gateways` ranks, or none without it.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn spawn_zakura_endpoint_with_peer_registry(
    config: &Config,
    sink_factory: impl FnOnce(ZakuraSupervisorHandle, ZakuraTrace) -> Arc<dyn Service> + Send + 'static,
    header_sync_driver_startup: Option<ZakuraHeaderSyncDriverStartup>,
    custom_services: Vec<CustomService>,
    tx_relay: Option<super::TxRelayStartup>,
    bpv1: Option<super::Bpv1Startup>,
    peer_registry: PeerRegistry,
    miner_gateways: Option<watch::Receiver<super::MinerGatewayRanking>>,
) -> Result<Option<ZakuraEndpoint>, BoxError> {
    spawn_zakura_endpoint_inner(
        config,
        Box::new(sink_factory),
        header_sync_driver_startup,
        custom_services,
        tx_relay,
        bpv1,
        Some(peer_registry),
        miner_gateways,
    )
    .await
}

/// The startup warning to log when the preferred places the sessions share funds,
/// `funded_places`, are no more than the `always_preferred` IDs the operator configured the
/// node to always prefer, or `None` when they leave room for ranked gateways.
///
/// Those IDs take places first, so the gateways recent blocks name then get none, and with no
/// funded place at all no always-preferred ID is held either. The endpoint checks this at
/// startup against [`MemoryPlan::funded_gateway_places`](super::memory::MemoryPlan::funded_gateway_places).
fn unfunded_always_preferred_warning(
    always_preferred: usize,
    funded_places: usize,
) -> Option<&'static str> {
    if always_preferred == 0 || funded_places > always_preferred {
        None
    } else if funded_places == 0 {
        Some(
            "the peer memory budget funds no preferred connection places, so none is held for \
             the IDs this node is configured to always prefer; raise [memory] budget",
        )
    } else {
        Some(
            "the peer memory budget funds no more preferred connection places than the IDs this \
             node is configured to always prefer, so those IDs take every place and the gateways \
             recent blocks name get none; raise [memory] budget",
        )
    }
}

/// Starts the endpoint. `sink_factory` is boxed so this large function is compiled once for
/// every caller's factory type. `tx_relay` and `bpv1` start once the memory pool they lease
/// from exists.
#[allow(clippy::too_many_arguments)]
async fn spawn_zakura_endpoint_inner(
    config: &Config,
    sink_factory: Box<dyn FnOnce(ZakuraSupervisorHandle, ZakuraTrace) -> Arc<dyn Service> + Send>,
    header_sync_driver_startup: Option<ZakuraHeaderSyncDriverStartup>,
    mut custom_services: Vec<CustomService>,
    tx_relay: Option<super::TxRelayStartup>,
    bpv1: Option<super::Bpv1Startup>,
    peer_registry: Option<PeerRegistry>,
    miner_gateways: Option<watch::Receiver<super::MinerGatewayRanking>>,
) -> Result<Option<ZakuraEndpoint>, BoxError> {
    if !config.v2_p2p() {
        return Ok(None);
    }
    // `tx-relay`'s shares are charged exactly while its service is registered,
    // whatever `[network.zakura.tx_relay] enabled` says.
    let tx_relay_registered = tx_relay.is_some()
        || custom_services
            .iter()
            .any(|custom| custom.tx_relay_sessions.is_some());
    let registered_config;
    let config = if config.zakura.tx_relay.enabled == tx_relay_registered {
        config
    } else {
        let mut adjusted = config.clone();
        adjusted.zakura.tx_relay.enabled = tx_relay_registered;
        registered_config = adjusted;
        &registered_config
    };

    if config.zakura.nat_traversal {
        return Err(ZAKURA_NAT_TRAVERSAL_REMOVED.into());
    }
    // Fail before binding or advertising anything; the block-sync reactor
    // checks the same config again at its own construction boundary.
    config
        .zakura
        .block_sync
        .validate()
        .map_err(super::BlockSyncStartError::InvalidConfig)?;
    let secret_key = zakura_secret_key(config)?;
    let local_node_id = secret_key.public();
    // A custom kind with its own share, such as `tx-relay`'s, is counted
    // once, with the native kinds.
    let named_kinds = config.zakura.reactor_shares(&config.network).kinds;
    let tx_relay_kinds = tx_relay
        .as_ref()
        .map_or(&[][..], |_| &super::tx_relay::wire::TX_RELAY_STREAMS[..]);
    let custom_kinds: Vec<u16> = custom_services
        .iter()
        .flat_map(|custom| custom.service.streams())
        .chain(tx_relay_kinds)
        .map(|stream| stream.kind)
        .filter(|kind| !named_kinds.iter().any(|(named, _)| named == kind))
        .collect();
    let memory_plan = super::memory::MemoryPlan::size_for_node(
        config.zakura.memory_budget.p2p_budget_b(),
        config,
        &config.network,
        Some(local_node_id),
        &custom_kinds,
    )?;
    memory_plan.publish_metrics();
    let memory_pool = memory_plan.build_pool();
    custom_services.extend(tx_relay.map(|startup| startup.start(memory_pool.clone())));
    let bpv1 = bpv1.map(|startup| {
        let (service, handle) = startup.start(memory_pool.clone());
        custom_services.push(service);
        handle
    });
    let block_sync_memory = super::BlockSyncMemory::new(
        memory_pool.clone(),
        config
            .zakura
            .memory_budget
            .caps
            .look_ahead
            .map(super::memory::ByteSize::bytes),
    );
    info!(plan = %memory_plan, "sized and enforced Zakura startup memory plan");
    for warning in &memory_plan.override_warnings {
        warn!(
            setting = %warning.setting,
            configured = %warning.configured_value,
            excess_bytes = warning.excess_bytes,
            plan_value = %warning.plan_value,
            budget_b = memory_plan.pool_config.budget_b,
            "operator setting can exceed [memory] budget: {warning}"
        );
    }
    let limits = memory_plan.local_limits(config);
    if let Some(ranking) = &miner_gateways {
        let always_preferred = ranking.borrow().always_preferred.len();
        let funded_places = memory_plan.funded_gateway_places();
        if let Some(warning) = unfunded_always_preferred_warning(always_preferred, funded_places) {
            warn!(always_preferred, funded_places, "{warning}");
        }
    }
    let node_envelope = crate::zakura::node_envelope::NodeEnvelope::for_memory_plan(
        config,
        &limits,
        &custom_kinds,
        header_sync_driver_startup
            .as_ref()
            .map_or(0, |startup| startup.decoded_backend_blocks),
    )?;
    debug!(
        owners = ?node_envelope.owners,
        retained_bytes_bound = %node_envelope.retained_bytes_bound(),
        request_executions = node_envelope.request_executions,
        decoded_blocks = node_envelope.decoded_blocks,
        decoded_pipeline_bytes = node_envelope.decoded_pipeline_bytes,
        serve_output_bytes = %node_envelope.serve_output_bytes,
        "Zakura node resource envelope"
    );
    validate_idle_invariant(&limits)?;
    let discovery_secret_key = secret_key.clone();
    let supervisor = match peer_registry {
        Some(peer_registry) => ZakuraSupervisorHandle::new_with_peer_registry(
            config.zakura.max_connections_per_ip(),
            peer_registry,
        ),
        None => ZakuraSupervisorHandle::new(config.zakura.max_connections_per_ip()),
    }
    .with_tx_relay_sessions(
        custom_services
            .iter()
            .find_map(|custom| custom.tx_relay_sessions.clone()),
    );
    let tracer = config
        .zakura
        .trace_dir
        .clone()
        .map(zakura_jsonl_trace::JsonlTracer::spawn)
        .unwrap_or_else(zakura_jsonl_trace::JsonlTracer::noop);
    let trace = ZakuraTrace::new(tracer, zakura_jsonl_trace::node_id());
    trace.emit_memory_plan(&memory_plan);
    let handshake_config = ZakuraHandshakeConfig::for_network_with_dev_cohort(
        &config.network,
        config.zakura.dev_network.as_deref(),
    );
    let sought_services = sought_services_with_custom(&custom_services);
    config.move_legacy_zakura_discovery_sequence_file(
        &discovery_secret_key.public_key(),
        &discovery_secret_key.public(),
    );
    let discovery = discovery::build_discovery_handle(
        discovery_secret_key,
        Some(discovery::AuthoredSequenceFile::new(
            config.zakura_discovery_sequence_file(&local_node_id),
        )),
        discovery_direct_addrs(config, local_node_id),
        provided_services_with_custom(&custom_services),
        &handshake_config,
        limits.max_connections,
        remote_bootstrap_peer_count(&config.zakura.bootstrap_peers, local_node_id),
        supervisor.subscribe(),
    )?;
    if advertises_no_discovery_address(config, &discovery.current_self_record().body.direct_addrs) {
        warn!(
            listen_addr = ?config.zakura.listen_addr,
            external_addr = ?config.external_addr,
            "this node advertises no discovery address that peers can dial, so they will not \
             learn of it through discovery; give network.zakura.listen_addr a fixed nonzero \
             port, and set network.external_addr to this node's public IP address, which peers \
             dial at that port"
        );
    }
    let anchor = config.zakura.header_sync.anchor(&config.network)?;
    let frontiers = header_sync_driver_startup.as_ref().map_or(
        FullStateFrontiers {
            finalized_height: anchor.0,
            verified_block_tip: anchor.0,
            verified_block_hash: anchor.1,
        },
        |startup| startup.frontiers,
    );
    let best_header_tip = header_sync_driver_startup
        .as_ref()
        .map_or(Some(anchor), |startup| startup.best_header_tip);
    let mut startup = HeaderSyncStartup::new(
        config.network.clone(),
        anchor,
        frontiers,
        best_header_tip,
        config
            .zakura
            .header_sync
            .clone()
            .for_node_connections(limits.total_connections()),
        limits.max_frame_bytes,
        limits.total_connections(),
    );
    let (header_sync_fatal_tx, header_sync_fatal_rx) = mpsc::unbounded_channel();
    startup.fatal_events = Some(header_sync_fatal_tx);
    startup.use_direct_port();
    startup.trace = trace.clone();
    startup.committed_snapshots = header_sync_driver_startup
        .as_ref()
        .map(|driver| driver.committed_snapshots.clone());
    startup.vct_root_repairs = header_sync_driver_startup
        .as_ref()
        .and_then(|driver| driver.vct_root_repairs.clone());
    if let Some(driver) = header_sync_driver_startup.as_ref() {
        startup.header_chain_port = driver.header_chain_port.clone();
    }
    let header_sync_shutdown = CancellationToken::new();
    startup.shutdown = header_sync_shutdown.clone();
    startup.serving_pool = Some(memory_pool.clone());
    let (header_sync, header_sync_actions, header_sync_task) = spawn_header_sync_reactor(startup)?;
    let block_sync_driver_enabled = header_sync_driver_startup.is_some();
    let (block_sync, block_sync_actions, block_sync_task) =
        if let Some(driver_startup) = header_sync_driver_startup.as_ref() {
            let best_header_tip = driver_startup.best_header_tip.unwrap_or(anchor);
            let mut startup = BlockSyncStartup::new_with_committed_views(
                BlockSyncFrontiers {
                    finalized_height: driver_startup.frontiers.finalized_height,
                    verified_block_tip: driver_startup.frontiers.verified_block_tip,
                    verified_block_hash: driver_startup.verified_block_tip_hash,
                },
                best_header_tip,
                driver_startup.committed_views.clone(),
                config
                    .zakura
                    .block_sync
                    .decoded_block_pool(driver_startup.decoded_backend_blocks)?,
                config
                    .zakura
                    .block_sync
                    .clone()
                    .for_node_connections(limits.total_connections()),
                block_sync_memory.clone(),
            )
            .with_range_source(driver_startup.block_range_source.clone());
            startup.shutdown = header_sync_shutdown.clone();
            startup.trace = trace.clone();
            if let Some(claims) = driver_startup.body_claims.clone() {
                startup = startup.with_body_claims(claims);
            }
            // With `checkpoint_sync = false` the verifier stops checkpointing
            // lower than this, so the cap only ever misses heights, never caps
            // a checkpoint range.
            startup.max_checkpoint_height = Some(config.network.checkpoint_list().max_height());
            let (handle, actions, task) = spawn_block_sync_reactor(startup.with_retention(
                driver_startup.retained_block_height.clone(),
                config.network.genesis_hash(),
            ))?;
            (Some(handle), Some(actions), Some(task))
        } else {
            (None, None, None)
        };
    let discovery_service = Arc::new(
        super::DiscoveryService::with_sync_services(
            discovery.clone(),
            header_sync.clone(),
            block_sync.clone(),
            trace.clone(),
            limits.total_connections(),
        )
        .with_memory_pool(memory_pool.clone()),
    );
    let legacy_service = sink_factory(supervisor.clone(), trace.clone());
    let service_demand = header_sync_driver_startup
        .as_ref()
        .map(|startup| startup.service_demand.clone());
    let registry = service_registry(
        &supervisor,
        Some(header_sync.clone()),
        block_sync.clone(),
        config
            .zakura
            .block_sync
            .clone()
            .for_node_connections(limits.total_connections()),
        block_sync_memory,
        legacy_service,
        discovery_service,
        service_demand.clone(),
        custom_services,
    );
    // Refuse shares that can't hold a registered frame before binding, so the
    // handler's assertion of the same rule (`NodeResources::new`) can't stop
    // startup. Bind a fixed
    // address when configured so this node has a stable, advertisable Zakura
    // endpoint; otherwise bind loopback-only so the unset (dial-out-only) case
    // does not expose the native P2P_V2_ALPN surface on all interfaces.
    let bound = registry.and_then(|registry| {
        check_reactor_shares(
            &limits.reactor_shares(),
            registry_streams(&registry),
            limits.max_frame_bytes,
        )?;
        let bind = limits.quic_bind_config(native_bind_addrs(config.zakura.listen_addr));
        let quic = QuicEndpoint::bind(secret_key, &bind, &limits.quic)?;
        super::discovery::check_bootstrap_dials_fit(
            &config.zakura.bootstrap_peers,
            local_node_id,
            bind.dial_reserve,
            |node_addr| quic.dial_attempt_count(node_addr),
        )?;
        Ok((registry, quic))
    });
    let (registry, quic) = match bound {
        Ok(bound) => bound,
        Err(error) => {
            header_sync_shutdown.cancel();
            return Err(error);
        }
    };
    let mut tasks = vec![header_sync_task];
    if let Some(task) = block_sync_task {
        tasks.push(task);
    }
    #[cfg(any(test, feature = "zakura-testkit"))]
    let header_sync_actions = if block_sync_driver_enabled {
        Some(Arc::new(AsyncMutex::new(Some(header_sync_actions))))
    } else {
        let action_driver_task = zakura_rt::task::spawn_named(
            "header_sync_actions",
            drive_header_sync_actions(
                header_sync_actions,
                header_sync.clone(),
                supervisor.clone(),
                header_sync_shutdown.clone(),
            ),
        );
        tasks.push(action_driver_task);
        None
    };
    #[cfg(not(any(test, feature = "zakura-testkit")))]
    drop(header_sync_actions);
    let block_sync_actions = block_sync_actions
        .filter(|_| block_sync_driver_enabled)
        .map(|actions| Arc::new(AsyncMutex::new(Some(actions))));
    let header_sync_tasks = Arc::new(HeaderSyncBackgroundTasks {
        shutdown: header_sync_shutdown,
        tasks: AsyncMutex::new(tasks),
    });
    let header_sync_ready = service_demand
        .as_ref()
        .is_some_and(|demand| demand.borrow().header.is_enabled());
    let handler = ZakuraProtocolHandler::new_with_registry_and_trace(
        supervisor.clone(),
        config.network.clone(),
        handshake_config,
        limits.clone(),
        registry,
        trace,
    )
    .with_local_node_id(local_node_id)
    .with_memory_pool(memory_plan, memory_pool);
    handler.set_header_sync_enabled(header_sync_ready);
    if handler.trace.is_enabled() {
        let trace = handler.trace.clone();
        let expose_peer_addresses = config.expose_peer_addresses;
        quic.set_conn_observer(Arc::new(move |sample| {
            trace.emit_quic_conn(sample, expose_peer_addresses)
        }));
    }
    if let Err(error) = quic.serve(handler.clone()) {
        header_sync_tasks.shutdown.cancel();
        return Err(error.into());
    }
    let endpoint = ZakuraEndpoint {
        quic,
        supervisor,
        handler,
        header_sync: Some(header_sync),
        block_sync,
        header_sync_tasks: Some(header_sync_tasks),
        header_sync_fatal_events: Some(Arc::new(AsyncMutex::new(Some(header_sync_fatal_rx)))),
        #[cfg(any(test, feature = "zakura-testkit"))]
        header_sync_actions,
        block_sync_actions,
        bpv1,
        upgrade_dials: Arc::new(StdMutex::new(HashMap::new())),
        upgrade_alpn_floors: Arc::new(StdMutex::new(HashMap::new())),
    };
    let startup_shutdown = endpoint.background_shutdown_token().drop_guard();

    if let Some(mut service_demand) = service_demand {
        let capability_handler = endpoint.handler.clone();
        let capability_supervisor = endpoint.supervisor.clone();
        let shutdown = endpoint.background_shutdown_token();
        let task = spawn_erased_named("header_capabilities", async move {
            let mut epochs = HeaderCapabilityEpochs::from_initial(&service_demand.borrow());
            loop {
                let ready_epoch = epochs.observe(&service_demand.borrow());
                if let Some(epoch) = ready_epoch {
                    enable_header_sync_and_renegotiate(
                        &capability_handler,
                        &capability_supervisor,
                        epoch,
                    )
                    .await;
                }
                zakura_rt::select! {
                    biased;
                    _ = shutdown.cancelled() => break,
                    changed = service_demand.changed() => {
                        if changed.is_err() {
                            break;
                        }
                    }
                }
            }
        });
        endpoint.push_header_sync_task(task).await;
    }

    // Report each stream kind's held and gated bytes. The report reads one
    // semaphore per gate on a timer, so frames do no extra work. The simulator
    // keeps its schedule free of this timer.
    #[cfg(not(zakura_sim))]
    {
        let shutdown = endpoint.background_shutdown_token();
        let resources = endpoint.handler.resources.clone();
        let task = spawn_erased_named("transport_usage_report", async move {
            let mut ticks = zakura_rt::time::interval(TRANSPORT_USAGE_REPORT_INTERVAL);
            ticks.set_missed_tick_behavior(zakura_rt::time::MissedTickBehavior::Delay);
            loop {
                zakura_rt::select! {
                    biased;
                    _ = shutdown.cancelled() => break,
                    _ = ticks.tick() => resources.usage().record(),
                }
            }
        });
        endpoint.push_header_sync_task(task).await;
    }

    // Log our own dial address so operators can hand
    // out `<node_id>@<direct_addr>` for other nodes' `zakura.bootstrap_peers`.
    // Tracked under the endpoint shutdown owner and shutdown-aware so it cannot
    // outlive endpoint teardown while awaiting address resolution.
    {
        let shutdown = endpoint.background_shutdown_token();
        let log_endpoint = endpoint.clone();
        let task = spawn_erased_named("dial_address_log", async move {
            zakura_rt::select! {
                biased;
                _ = shutdown.cancelled() => {}
                node_addr = log_endpoint.node_addr() => {
                    let direct_addresses: Vec<String> = node_addr
                        .direct
                        .iter()
                        .map(|addr| addr.to_string())
                        .collect();
                    info!(
                        node_id = %node_addr.id,
                        ?direct_addresses,
                        "Zakura P2P endpoint ready; advertise <node_id>@<direct_addr> as a bootstrap peer",
                    );
                }
            }
        });
        endpoint.push_header_sync_task(task).await;
    }

    // Restore the peers the previous run knew before the dialer starts, so a
    // restart reconnects to them instead of only to the bootstrap peers.
    if let Some(saver) = discovery::start_discovery_book_cache(
        &discovery,
        config,
        endpoint.background_shutdown_token(),
    )
    .await
    {
        endpoint.push_header_sync_task(saver).await;
    }
    super::discovery::insert_static_bootstrap_candidates(
        &discovery,
        &config.zakura.bootstrap_peers,
    )
    .await;
    // Track the maintained bootstrap redial tasks and the discovery candidate
    // dialer under the endpoint shutdown owner so `ZakuraEndpoint::shutdown`
    // drains them. Previously their handles were dropped, leaving detached loops
    // that kept retrying dials against a torn-down router while holding endpoint
    // clones past teardown.
    for task in spawn_native_bootstrap_dialer(
        endpoint.clone(),
        config.zakura.bootstrap_peers.clone(),
        limits.clone(),
    ) {
        endpoint.push_header_sync_task(task).await;
    }
    let discovery_dialer = discovery::spawn_native_discovery_dialer(
        endpoint.clone(),
        discovery,
        limits,
        sought_services,
        miner_gateways,
    );
    endpoint.push_header_sync_task(discovery_dialer).await;
    startup_shutdown.disarm();
    Ok(Some(endpoint))
}

/// The key of an upgrade dial's ALPN-mismatch floor: the hint's node ID and its
/// sorted, deduplicated addresses.
///
/// zakura-quic DIAL-5 keys the floor by node ID, because a discovery record is
/// signed by its node. A legacy upgrade hint is not: an ALPN mismatch fails
/// before TLS authenticates the node ID, so any legacy peer could plant a floor
/// on another node's ID. Keying the whole hint limits a planted floor to the
/// hint its own sender gave.
fn upgrade_hint(node_addr: &NodeAddr) -> NodeAddr {
    let mut direct = node_addr.direct.clone();
    direct.sort_unstable();
    direct.dedup();
    NodeAddr {
        id: node_addr.id,
        direct,
    }
}

/// Bounds a whole multi-address dial so the transport's own limits decide it:
/// the last attempt starts after the stagger and ends at the handshake
/// deadline (zakura-quic DIAL-3, DIAL-4). `control_timeout` is the floor.
fn native_dial_timeout(config: &QuicConfig, addrs: usize, control_timeout: Duration) -> Duration {
    let later_attempts = u32::try_from(addrs.saturating_sub(1)).unwrap_or(u32::MAX);
    let last_start = config.dial_stagger().saturating_mul(later_attempts);
    let attempt = config
        .handshake_timeout()
        .unwrap_or_else(|| config.idle_timeout());
    control_timeout.max(last_start.saturating_add(attempt))
}

/// Report that an incoming stream with `prelude` on `send` was reset for
/// `reason` while the connection stays open.
#[cfg(any(test, zakura_sim))]
fn report_prelude_judged(
    close_cause: &CloseCause,
    send: &SendStream,
    rejection: Option<super::handshake::PreludeRejection>,
) {
    if let Some(tap) = close_cause.tap() {
        tap.emit(super::TransportEvent::PreludeJudged {
            conn_id: tap.conn_id(),
            quic_stream: send.id(),
            rejection,
        });
    }
}

#[cfg(any(test, zakura_sim))]
fn report_stream_refused(
    close_cause: &CloseCause,
    send: &SendStream,
    prelude: &StreamPrelude,
    reason: &'static str,
) {
    if let Some(tap) = close_cause.tap() {
        tap.emit(super::TransportEvent::StreamRefused {
            conn_id: tap.conn_id(),
            quic_stream: send.id(),
            kind: prelude.stream_kind,
            version: prelude.stream_version,
            reason,
        });
    }
}

/// The trace ID of a native connection, shared with the `quic_conn` table.
fn trace_conn_id(connection: &Connection) -> u64 {
    // usize is at most 64 bits on every supported target.
    connection.stable_id() as u64
}

/// How a dialed connection's registration ended, as the dial reports it to
/// its discovery attempt or maintained redial.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum DialRegistration {
    /// A dial to an earlier ID found the authenticated peer already registered.
    AlreadyConnected,
    /// The supervisor registered the peer on this connection.
    Registered,
    /// This node's admission limits refused to register the peer, or a local
    /// capability change closed its registration.
    LocalLimit,
}

/// What TLS proved about the remote end of one dial.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DialAuthenticated {
    /// The address whose handshake TLS authenticated.
    pub(crate) addr: SocketAddr,
    /// The ID the node answered with. It isn't the dialed ID when the node
    /// has added keys since the dialer learned that one (zakura-quic TLS-7).
    pub(crate) node_id: NodeId,
}

/// The channels on which a discovery or maintained dial reports its progress.
pub(crate) struct DialReport {
    /// Receives what TLS authenticated, before the control handshake starts.
    pub(crate) authenticated: oneshot::Sender<DialAuthenticated>,
    /// Receives the registration or the local reason for ending this dial
    /// (duplicate or capacity). Other pre-registration failures drop the sender.
    pub(crate) registration: oneshot::Sender<DialRegistration>,
}

/// Selects whether a native dial may use the transport's reserved capacity.
pub(crate) enum NativeDialKind {
    /// Bootstrap and maintained dials may use every live slot.
    Maintained,
    /// Discovery leaves room for an inbound connection and maintained dials.
    Discovery { ordinary_cap: usize },
}

/// Dials and serves a maintained native connection using the full transport cap.
pub(crate) async fn serve_native_dial_connection(
    endpoint: &ZakuraEndpoint,
    node_addr: NodeAddr,
    limits: &ZakuraLocalLimits,
) -> Result<(), ZakuraHandlerError> {
    serve_reported_native_dial_connection(
        endpoint,
        node_addr,
        limits,
        None,
        NativeDialKind::Maintained,
    )
    .await
}

/// Dials `node_addr` and serves the connection, like [`serve_native_dial_connection`].
///
/// Sends the admitted IP on `report` once TLS authenticates the remote node at
/// one of the dialed addresses, before the control handshake starts. Then
/// reports how registration ended, before the connection is served.
pub(crate) async fn serve_reported_native_dial_connection(
    endpoint: &ZakuraEndpoint,
    node_addr: NodeAddr,
    limits: &ZakuraLocalLimits,
    report: Option<DialReport>,
    kind: NativeDialKind,
) -> Result<(), ZakuraHandlerError> {
    let dialed = node_addr.id;
    let ordinary_cap = match kind {
        NativeDialKind::Maintained => endpoint.handler.limits.max_connections,
        NativeDialKind::Discovery { ordinary_cap } => {
            ordinary_cap.min(endpoint.handler.limits.max_connections)
        }
    };
    let mut admission = endpoint
        .handler
        .claim_dial_connection(dialed, ordinary_cap)
        .map_err(|refusal| {
            if refusal == SlotRefusal::Hold {
                metrics::counter!("zakura.p2p.conn.dial.refused.hold").increment(1);
            }
            ZakuraHandlerError::ResourceLimit(ADMISSION_LIMIT)
        })?;
    let dial_timeout = native_dial_timeout(
        endpoint.quic.config(),
        node_addr.direct.len(),
        limits.control_timeout,
    );
    let alpns = supported_p2p_v2_alpns();
    let offered: Vec<&[u8]> = alpns.iter().map(Vec::as_slice).collect();
    let dial = async {
        match kind {
            NativeDialKind::Maintained => endpoint.quic.connect(node_addr, &offered).await,
            NativeDialKind::Discovery { .. } => {
                endpoint.quic.connect_discovery(node_addr, &offered).await
            }
        }
    };
    let connection = match timeout(dial_timeout, dial)
        .await
        .map_err(|_| ZakuraHandlerError::Timeout("native dial"))?
    {
        Ok(connection) => connection,
        // zakura-quic ADM-12: this node's dial ceiling, so discovery doesn't
        // back off from the peer.
        Err(zakura_quic::ConnectError::ConnectionLimit) => {
            return Err(ZakuraHandlerError::ResourceLimit(
                TRANSPORT_CONNECTIONS_LIMIT,
            ));
        }
        Err(error) => return Err(error.into()),
    };
    #[cfg(zakura_sim)]
    endpoint.handler.observe_handshake_slot(&connection);
    #[cfg(any(test, zakura_sim))]
    {
        admission.observation = endpoint.handler.handshake_observation(&connection);
    }
    let remote_node_id = connection.remote_id();
    let registration_report = report.map(|report| {
        // The receiver is gone when the caller stopped waiting for the result.
        let _ = report.authenticated.send(DialAuthenticated {
            addr: connection.admitted_addr(),
            node_id: remote_node_id,
        });
        report.registration
    });
    let peer_id = ZakuraPeerId::from_node_id(remote_node_id);
    if remote_node_id != dialed {
        // The node added keys since this node learned `dialed`, and answers
        // under the ID it has now (zakura-quic TLS-7, DIAL-9).
        endpoint.supervisor.learn_move(dialed, remote_node_id);
        if endpoint.supervisor.is_registered(&peer_id) {
            // The node already has a connection, which nothing could tell
            // before TLS showed which node this dial reached. Close this one
            // before the control handshake: the other end registers a
            // connection as soon as it has acknowledged that handshake, and
            // its duplicate rule would then cancel its side of the registered
            // one, once that is older than
            // `ZAKURA_SAME_IP_DUPLICATE_EVICT_MIN_AGE`. A dial to an earlier
            // ID must leave this locally visible registration alone.
            #[cfg(any(test, zakura_sim))]
            endpoint
                .handler
                .handshake_refused(&connection, "moved-peer-already-registered");
            if let Some(report) = registration_report {
                let _ = report.send(DialRegistration::AlreadyConnected);
            }
            connection.close(VarInt::from_u32(ZAKURA_CLOSE_NEUTRAL), b"duplicate");
            return Ok(());
        }
        // A connection to the node that is still in its own handshake meets
        // this one in `register`, like any simultaneous open.
    }
    let earlier = connection.remote_earlier_ids();
    let proven = ProvenNode::new(remote_node_id, &earlier);
    if admission.outbound_connected(proven).is_err() {
        // The dial claimed a held place for a gateway the node doesn't
        // answer for, or the node makes its gateway ambiguous. Counting
        // the displaced slot as ordinary would exceed the cap or its inbound share.
        metrics::counter!("zakura.p2p.conn.dial.refused.hold").increment(1);
        #[cfg(any(test, zakura_sim))]
        endpoint
            .handler
            .handshake_refused(&connection, "moved-peer-hold");
        connection.close(
            VarInt::from_u32(ZAKURA_CLOSE_RESOURCE),
            ADMISSION_LIMIT.as_bytes(),
        );
        return Err(ZakuraHandlerError::ResourceLimit(AUTHENTICATED_SLOT_LIMIT));
    }
    let conn = ZakuraConnTrace::new(
        &endpoint.handler.trace,
        trace_conn_id(&connection),
        &peer_id,
    );
    let local_node_id = endpoint.local_node_id();
    let local_peer_id = ZakuraPeerId::from_node_id(local_node_id);
    let (negotiated, capability_epoch) = {
        let _handshake = sim_chain!(endpoint.handler
            .claim_pending_handshake(false)
            .ok_or(ZakuraHandlerError::ResourceLimit(PENDING_HANDSHAKE_LIMIT))
            => inspect_err(|error| endpoint.handler.handshake_refused(&connection, error)))?;
        #[cfg(any(test, zakura_sim))]
        let _handshake = {
            let mut guard = _handshake;
            guard.observation = endpoint.handler.handshake_observation(&connection);
            guard
        };
        let _owner = endpoint
            .handler
            .resources
            .owners()
            .enter(OwnerPhase::Handshake);
        let (handshake_config, capability_epoch) = endpoint.handler.handshake_snapshot();
        transport_observe!(
            endpoint.handler.transport_observer,
            super::TransportEvent::HandshakeCapabilities {
                remote: connection.admitted_addr(),
                shared_id: connection.shared_id(),
                supported: handshake_config.supported_capabilities,
            }
        );
        #[cfg(zakura_sim)]
        endpoint
            .handler
            .pause_handshake(super::HandshakePhase::Snapshot)
            .await;
        let result = run_native_initiator_handshake(
            &connection,
            limits,
            &handshake_config,
            &local_peer_id,
            &endpoint.handler.trace,
            &conn,
        )
        .await;
        #[cfg(any(test, zakura_sim))]
        endpoint
            .handler
            .observe_control_result(&connection, "ack", &result);
        (
            sim_chain!(result => inspect_err(|error| endpoint.handler.handshake_refused(&connection, error)))?,
            capability_epoch,
        )
    };
    transport_observe!(
        endpoint.handler.transport_observer,
        super::TransportEvent::Negotiated {
            remote: connection.admitted_addr(),
            local: negotiated.local_capabilities,
            offered: negotiated.offered_capabilities,
            negotiated: negotiated.accepted_capabilities,
        }
    );
    let conn_limits = limits.clamp(&negotiated.limits);
    // Charge the connection to the address whose handshake won, not the first
    // address the record advertised: a record can list an unreachable decoy as
    // its first direct address to escape the per-IP cap while the connection is
    // served over a different (shared) address (zakura-quic DIAL-6).
    let remote_ip = Some(connection.admitted_ip());
    let direction = ServicePeerDirection::Outbound;
    endpoint
        .handler
        .register_and_serve(
            connection,
            peer_id,
            remote_ip,
            ConnectionServeContext {
                limits: conn_limits,
                accepted_capabilities: negotiated.accepted_capabilities,
                metered_epoch: negotiated.metered_epoch,
                capability_epoch,
                role: "initiator",
                direction,
                transcript_hash: native_connection_transcript_hash(
                    direction,
                    &local_node_id,
                    &remote_node_id,
                ),
                i_open_collision_winner: i_open_collision_winner(&local_node_id, &remote_node_id),
                conn,
                control_handshake: None,
                registration_report,
            },
        )
        .await
}

/// The protocol version the connection's ALPN selected (COMPAT-8). Both
/// control hellos must name it.
fn negotiated_zakura_protocol(connection: &Connection) -> Result<u16, ZakuraHandlerError> {
    zakura_protocol_for_alpn(connection.alpn())
        .ok_or(ZakuraValidationError::UnsupportedVersion("zakura protocol").into())
}

#[cfg(any(test, feature = "zakura-testkit"))]
pub(crate) async fn run_native_initiator_handshake_without_trace(
    connection: &Connection,
    limits: &ZakuraLocalLimits,
    handshake_config: &ZakuraHandshakeConfig,
    local_peer_id: &ZakuraPeerId,
) -> Result<NativeHandshakeNegotiated, ZakuraHandlerError> {
    run_native_initiator_handshake(
        connection,
        limits,
        handshake_config,
        local_peer_id,
        &ZakuraTrace::noop(),
        &ZakuraConnTrace::placeholder(),
    )
    .await
}

/// Runs the initiator's control exchange within one `control_timeout`.
///
/// The responder applies the same overall deadline. Without it, a responder
/// that paces each step holds this dial's pending-handshake permit for one
/// timeout per step.
async fn run_native_initiator_handshake(
    connection: &Connection,
    limits: &ZakuraLocalLimits,
    handshake_config: &ZakuraHandshakeConfig,
    local_peer_id: &ZakuraPeerId,
    trace: &ZakuraTrace,
    conn: &ZakuraConnTrace,
) -> Result<NativeHandshakeNegotiated, ZakuraHandlerError> {
    timeout(
        limits.control_timeout,
        run_native_initiator_control_steps(
            connection,
            limits,
            handshake_config,
            local_peer_id,
            trace,
            conn,
        ),
    )
    .await
    .unwrap_or(Err(ZakuraHandlerError::Timeout("control handshake")))
}

async fn run_native_initiator_control_steps(
    connection: &Connection,
    limits: &ZakuraLocalLimits,
    handshake_config: &ZakuraHandshakeConfig,
    local_peer_id: &ZakuraPeerId,
    _trace: &ZakuraTrace,
    conn: &ZakuraConnTrace,
) -> Result<NativeHandshakeNegotiated, ZakuraHandlerError> {
    conn.trace_handshake(
        "control.started",
        "initiator",
        None,
        handshake_config.network_label(),
    );
    let protocol = negotiated_zakura_protocol(connection)?;
    let (mut send, mut recv) = timeout(limits.control_timeout, connection.open_bi())
        .await
        .map_err(|_| ZakuraHandlerError::Timeout("open control stream"))??;
    let mut local_nonce = [0; 32];
    zakura_rt::rand::rng().fill_bytes(&mut local_nonce);

    let hello = ZakuraControlHello {
        magic: CONTROL_HELLO_MAGIC,
        control_version: CONTROL_VERSION,
        selected_zakura_protocol: protocol,
        handshake_path: ZakuraHandshakePath::Native,
        role: ZakuraControlRole::Initiator,
        network_id: handshake_config.network_id,
        chain_id: handshake_config.chain_id,
        iroh_node_id: local_peer_id.as_bytes().to_vec(),
        peer_nonce: local_nonce,
        initiator_upgrade_nonce: [0; 32],
        responder_upgrade_nonce: [0; 32],
        legacy_upgrade_transcript: [0; 32],
        capabilities: handshake_config.supported_capabilities,
        required_channels: 0,
        initial_limits: limits.initial_limits(),
    };

    let metered_epoch = Instant::now();
    write_control_payload(&mut send, &hello.encode()?, limits.control_timeout).await?;
    let ack_bytes = read_control_payload(
        &mut recv,
        handshake_config.max_control_frame_bytes,
        limits.control_timeout,
    )
    .await?;
    let ack = ZakuraControlAck::decode(&ack_bytes)?;
    ack.validate(
        protocol,
        local_nonce,
        ack.peer_nonce,
        hello.capabilities,
        &limits.initial_limits(),
        handshake_config,
    )?;
    conn.trace_handshake(
        "control.succeeded",
        "initiator",
        Some(ack.selected_zakura_protocol),
        handshake_config.network_label(),
    );
    Ok(NativeHandshakeNegotiated {
        limits: ack.accepted_limits,
        accepted_capabilities: ack.accepted_capabilities,
        metered_epoch,
        #[cfg(any(test, zakura_sim))]
        local_capabilities: handshake_config.supported_capabilities,
        #[cfg(any(test, zakura_sim))]
        offered_capabilities: ack.accepted_capabilities,
    })
}

#[allow(clippy::too_many_arguments)]
#[cfg(test)]
fn spawn_persistent_stream_worker(
    workers: &mut JoinSet<()>,
    send: SendStream,
    recv: RecvStream,
    stream: Stream,
    prelude: StreamPrelude,
    context: StreamWorkerContext,
    queue_depth: usize,
    opened_locally: bool,
    session_exit_tx: mpsc::UnboundedSender<SessionExit>,
) -> AdmittedSession {
    spawn_service_session(
        workers,
        vec![PreparedStream::new(send, recv, stream, prelude, context)],
        queue_depth,
        opened_locally,
        session_exit_tx,
    )
}

fn bounded_stream_queue_depths(
    transport_depth: usize,
    service: Option<StreamQueueDepths>,
) -> (usize, usize) {
    service.map_or((transport_depth, transport_depth), |depths| {
        (
            transport_depth.min(depths.inbound.max(1)),
            transport_depth.min(depths.outbound.max(1)),
        )
    })
}

#[derive(Debug, Error)]
#[error("Zakura outbound frame write timed out")]
struct OrderedFrameWriteTimeout;

/// A queued ordered-stream frame that the writer refused before writing it.
#[derive(Debug, Error)]
enum OrderedFrameEncodeError {
    #[error(
        "Zakura outbound ordered frame payload {payload_len} exceeds negotiated \
         max_message_bytes {max_message_bytes}"
    )]
    OverMessageCap {
        payload_len: usize,
        max_message_bytes: u32,
    },
    #[error("Zakura outbound ordered frame does not fit the stream's frame cap: {0}")]
    Encode(ZakuraProtocolError),
}

#[cfg(test)]
async fn persistent_stream_worker(
    send: SendStream,
    recv: RecvStream,
    prelude: StreamPrelude,
    context: StreamWorkerContext,
    inbound_tx: mpsc::Sender<AdmittedFrame>,
    outbound_rx: FramedWorkerRecv,
    queue_depth_limit: usize,
) {
    persistent_stream_worker_with_policy(
        send,
        recv,
        prelude,
        context,
        inbound_tx,
        outbound_rx,
        queue_depth_limit,
        None,
        None,
    )
    .await;
}

#[allow(clippy::too_many_arguments)]
async fn persistent_stream_worker_with_policy(
    mut send: SendStream,
    recv: RecvStream,
    prelude: StreamPrelude,
    context: StreamWorkerContext,
    inbound_tx: mpsc::Sender<AdmittedFrame>,
    outbound_rx: FramedWorkerRecv,
    queue_depth_limit: usize,
    failure_cause: Option<OrderedStreamFailureCause>,
    application_drain: Option<Arc<Barrier>>,
) {
    let context = Arc::new(context);
    let stream_kind = prelude.stream_kind;

    // Reading and forwarding may both block. Keep them independent of writes
    // so waiting for inbound channel space cannot stall an outgoing response.
    // A dedicated reader also preserves partial frame reads across outbound writes.
    let (close_tx, mut close_rx) = mpsc::channel::<(u32, CloseOutcome)>(1);
    let inbound_closed = inbound_tx.clone();
    let reader_context = Arc::clone(&context);
    let reader_failure_cause = failure_cause.clone();
    let reader = AbortOnDropHandle::new(spawn_erased_named("stream_reader", async move {
        // Session teardown must interrupt a blocked writer without waiting for it
        // to poll the reader's terminal event. Preserve the close cause first.
        let _cancel_session_on_exit = reader_context.stream_token.clone().drop_guard();
        let mut recv = recv;
        let waiting_since = Instant::now();
        zakura_rt::select! {
            biased;
            _ = reader_context.connection_token.cancelled() => return,
            _ = reader_context.stream_token.cancelled() => return,
            _ = reader_context.precheck.ready() => {}
            _ = inbound_tx.closed() => reader_context.precheck.start(),
        }
        reader_context.credit_read_pause(waiting_since.elapsed());
        let paced_dispatch = reader_context
            .cadence
            .as_ref()
            .is_some_and(|scope| scope.parks);
        loop {
            let frame = zakura_rt::select! {
                biased;
                _ = reader_context.connection_token.cancelled() => break,
                _ = reader_context.stream_token.cancelled() => break,
                frame = read_admitted_frame(
                    &mut recv,
                    reader_context.inbound_frame_cap,
                    sim_chain!(FrameFilter::new(reader_context.messages, InboundReader::Persistent)
                        .with_precheck(reader_context.precheck.get())
                        => with_tap(reader_context.precheck.tap())),
                    reader_context.limits.idle_timeout,
                    // A persistent ordered stream is legitimately quiet between
                    // frames; do not let an inter-frame gap time out and cancel
                    // the whole connection. Connection-level idleness is owned by
                    // the freshness reaper and the QUIC idle timeout, and the
                    // cancellation tokens above end this read on teardown.
                    None,
                    &reader_context.resources.raw,
                ) => frame,
            };
            // Charge ingress admission once, before forwarding to the service.
            // A full service channel stops this reader from granting QUIC credit.
            let error = match frame {
                Ok(admitted) => {
                    // The reader numbered the frame it just read, so the
                    // service's held-frame rows can name it.
                    #[cfg(any(test, zakura_sim))]
                    let admitted = AdmittedFrame {
                        seq: reader_context.precheck.tap().map(|tap| tap.reading()),
                        ..admitted
                    };
                    let _ = reader_context.freshness_tx.send(Instant::now());
                    // While the reader waited for raw capacity, the peer kept
                    // sending into transport buffers, as during a queue wait below.
                    reader_context.credit_read_pause(admitted.grant.waited());
                    // Charge paced work only when it has a service slot. A token
                    // taken before a full-queue wait could outlive its burst.
                    let slot = if paced_dispatch {
                        zakura_rt::select! {
                            biased;
                            _ = reader_context.connection_token.cancelled() => break,
                            _ = reader_context.stream_token.cancelled() => break,
                            slot = inbound_tx.reserve() => slot.ok(),
                        }
                    } else {
                        None
                    };
                    let admission =
                        admit_inbound_message(&admitted.frame, &reader_context, stream_kind);
                    // A parked frame holds back the rest of this stream, so QUIC
                    // flow control slows the sender. Cancellation ends the wait.
                    if admission == InboundMessageAdmission::Park
                        && !reader_context.park_for_cadence(&admitted.frame).await
                    {
                        break;
                    }
                    match admission.ordered_error() {
                        None => {
                            if paced_dispatch && slot.is_none() {
                                continue;
                            }
                            #[cfg(any(test, zakura_sim))]
                            if let Some(tap) = reader_context.precheck.tap() {
                                tap.queue_tag();
                            }
                            let waiting_since = Instant::now();
                            let forwarded = if let Some(slot) = slot {
                                slot.send(admitted);
                                true
                            } else {
                                zakura_rt::select! {
                                    biased;
                                    _ = reader_context.connection_token.cancelled() => break,
                                    _ = reader_context.stream_token.cancelled() => break,
                                    result = inbound_tx.send(admitted) => result.is_ok(),
                                }
                            };
                            // While this reader waited for its own handler, the
                            // peer kept sending into transport buffers. Credit the
                            // pause so the buffered burst never counts against it.
                            reader_context.credit_read_pause(waiting_since.elapsed());
                            if !forwarded {
                                #[cfg(any(test, zakura_sim))]
                                if let Some(tap) = reader_context.precheck.tap() {
                                    tap.unqueue_tag();
                                }
                                // Local receiver closure leaves retained senders and
                                // queued writes alive. Keep bounded ingress checks and
                                // real peer-close detection until the writers retire.
                                continue;
                            }
                            // Queue depth is capped by the negotiated u16 limit, so
                            // this integer is exactly representable as f64.
                            metrics::gauge!(
                                "zakura.p2p.queue.depth",
                                "stream_kind" => stream_kind_label(stream_kind),
                            )
                            .set(queue_depth_limit.saturating_sub(inbound_tx.capacity()) as f64);
                            continue;
                        }
                        Some(error) => error,
                    }
                }
                Err(error) => {
                    // Emit the oversize-desync diagnostic here, at the read, so
                    // it is recorded even if the main loop tears the worker down
                    // for an outbound write that stopped first.
                    if let Some((payload_len, frame_len, max_frame_bytes)) =
                        error.oversize_frame_details()
                    {
                        reader_context.conn.trace_rate_limit(
                            "frame.oversize",
                            reader_context.stream_id,
                            stream_kind_label(stream_kind),
                            Some(payload_len),
                            Some(frame_len),
                            Some(max_frame_bytes),
                        );
                    }
                    error
                }
            };
            let close = ordered_read_close(reader_context.stream(), &error);
            let session_failure = match error {
                ZakuraHandlerError::Closed => Some(OrderedStreamFailure::RemoteClose),
                ZakuraHandlerError::Timeout(_) => Some(OrderedStreamFailure::ReadTimeout),
                _ => None,
            };
            // Publish before the drop guard cancels the session or the service sees EOF.
            if !reader_context.stream_token.is_cancelled() {
                if let (Some(cause), Some(failure)) = (&reader_failure_cause, session_failure) {
                    cause.record(failure);
                }
            }
            if let Some((code, cause)) = close {
                // The reader also closes the connection if the writer is blocked.
                let _ = close_tx.send((code, cause)).await;
                reader_context.close_cause.record(cause);
                reader_context.connection_token.cancel();
            }
            break;
        }
    }));

    let mut outbound_rx = Some(outbound_rx);
    let mut drained = false;
    loop {
        zakura_rt::select! {
            biased;
            _ = context.connection_token.cancelled() => break,
            _ = context.stream_token.cancelled() => break,
            _ = inbound_closed.closed(), if outbound_rx.is_none() => {
                drained = true;
                // An idle member must not cancel another member's queued writes
                // or retained application handles. Faults still interrupt draining.
                if let Some(barrier) = &application_drain {
                    zakura_rt::select! {
                        biased;
                        _ = context.connection_token.cancelled() => {},
                        _ = context.stream_token.cancelled() => {},
                        _ = barrier.wait() => {},
                    }
                }
                break;
            }
            outbound = async {
                match outbound_rx.as_mut() {
                    Some(outbound_rx) => outbound_rx.recv().await,
                    None => future::pending().await,
                }
            } => {
                match outbound {
                    Some(queued_frame) => {
                        // Cancellation resets the stream before a replacement can write.
                        let result = zakura_rt::select! {
                            biased;
                            _ = context.connection_token.cancelled() => break,
                            _ = context.stream_token.cancelled() => break,
                            // Encode before the request claim, so a frame that
                            // doesn't fit starts no exchange and its session can
                            // retire without closing the connection.
                            result = queued_frame.encode_and_write_with(
                                |frame| encode_ordered_frame(
                                    frame, context.limits, context.outbound_frame_cap,
                                ),
                                |encoded| async {
                                let result = sim_arg!(write_encoded_ordered_frame(
                                    &mut send, encoded, context.write_policy,
                                ); context.precheck.tap()).await;
                                // A failed request claim can cancel the session on
                                // drop. Record the cause while it is still alive.
                                if !context.stream_token.is_cancelled() {
                                    if let (Err(error), Some(cause)) = (&result, &failure_cause) {
                                        if error.is::<OrderedFrameWriteTimeout>() {
                                            cause.record(OrderedStreamFailure::WriteTimeout);
                                        } else if ordered_stream_write_was_stopped(error) {
                                            cause.record(OrderedStreamFailure::RemoteClose);
                                        }
                                    }
                                }
                                result
                            }) => result,
                        };
                        if let Err(error) = result {
                            if error.is::<OrderedFrameWriteTimeout>() {
                                debug!(stream_kind, stream_id = context.stream_id,
                                    "retiring Zakura service session after stream write timeout");
                                break;
                            }
                            if ordered_stream_write_was_stopped(&error) {
                                debug!(?error, "closing Zakura ordered stream after peer stopped receiving");
                                break;
                            }
                            // A local frame that does not fit the stream is not a
                            // connection fault. Retire only this session, as the
                            // request worker resets only its own stream.
                            if error.is::<OrderedFrameEncodeError>() {
                                debug!(stream_kind, stream_id = context.stream_id, ?error,
                                    "retiring Zakura service session after a local frame encode error");
                                let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_OVERSIZE));
                                context.conn.trace_stream(
                                    "reset.local_oversize",
                                    context.stream_id,
                                    Some(stream_kind_label(stream_kind)),
                                );
                                break;
                            }
                            debug!(stream_kind, stream_id = context.stream_id, ?error, "closing Zakura ordered stream writer");
                            let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_BAD_PRELUDE));
                            context.close_cause.record(CloseOutcome::neutral(
                                NeutralCause::IoFailure,
                                "ordered_write_error",
                            ));
                            context.connection_token.cancel();
                            break;
                        }
                    }
                    None => {
                        outbound_rx = None;
                    }
                }
            }
            close = close_rx.recv() => {
                if let Some((code, cause)) = close {
                    // The reader already emitted any oversize-desync diagnostic.
                    if code == ZAKURA_CLOSE_BAD_PRELUDE {
                        debug!(?cause, "closing Zakura stream worker");
                    }
                    let _ = send.reset(VarInt::from_u32(code));
                    context.close_cause.record(cause);
                    context.connection_token.cancel();
                }
                break;
            }
        }
    }

    context.resources.owner.advance(OwnerPhase::Retiring);
    // Never leave a partial frame followed by a graceful FIN.
    if drained {
        // A FIN preserves completed writes in QUIC's send buffer.
        let _ = send.finish();
    } else {
        let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_NEUTRAL));
    }
    context.stream_token.cancel();
    reader.abort();
    // Keep the stream permit until the reader has actually dropped its buffers.
    let _ = reader.await;
}

/// How the ordered worker closes on an error its reader forwarded: the
/// stream's reset code and the connection close cause to record. A clean
/// close or a frame timeout ends only the session.
fn ordered_read_close(
    stream: (u16, u16),
    error: &ZakuraHandlerError,
) -> Option<(u32, CloseOutcome)> {
    let code = match error {
        ZakuraHandlerError::Closed | ZakuraHandlerError::Timeout(_) => return None,
        ZakuraHandlerError::Oversize => ZAKURA_CLOSE_OVERSIZE,
        ZakuraHandlerError::RateLimited | ZakuraHandlerError::ByteRateOverrun => {
            ZAKURA_CLOSE_RATE_LIMIT
        }
        _ => ZAKURA_CLOSE_BAD_PRELUDE,
    };
    Some((code, read_close_outcome(ReadPath::Ordered, stream, error)))
}

/// The frame reader that hit a read error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReadPath {
    /// The reader of a persistent ordered stream.
    Ordered,
    /// The responder reading a request stream's one request frame.
    Request,
}

/// The close outcome for a frame-read error on `path`, on the `(kind,
/// version)` stream.
///
/// A broken frame rule or truncated payload assigns a fault. A rate limit
/// or transport failure assigns a neutral cause. The ordered reader handles
/// an orderly end and both readers handle frame timeouts before this mapping.
fn read_close_outcome(
    path: ReadPath,
    (kind, version): (u16, u16),
    error: &ZakuraHandlerError,
) -> CloseOutcome {
    use zakura_quic::ReadExactError::FinishedEarly;
    use ZakuraHandlerError::*;

    let fault = |row| CloseOutcome::fault(kind, version, row);
    match (path, error) {
        (_, RejectedFrame { rejection, .. }) => fault(rejection.close_row()),
        (_, OversizeFrame { .. }) => fault(CloseRow::REG_CLOSE_1),
        (_, QuicRead(FinishedEarly(_)))
        | (ReadPath::Ordered, Oversize)
        | (ReadPath::Ordered, ByteRateOverrun) => fault(CloseRow::CONN_CLOSE_18),
        (ReadPath::Ordered, RateLimited) => CloseOutcome::ORDERED_RATE_LIMITED,
        (ReadPath::Ordered, _) => {
            CloseOutcome::neutral(NeutralCause::IoFailure, "ordered_read_transport")
        }
        (ReadPath::Request, _) => {
            CloseOutcome::neutral(NeutralCause::IoFailure, "request_read_transport")
        }
    }
}

fn ordered_stream_write_was_stopped(error: &BoxError) -> bool {
    matches!(
        error.downcast_ref::<zakura_quic::WriteError>(),
        Some(zakura_quic::WriteError::Stopped(_))
    )
}

/// Runs a request's service call until it answers, the connection closes or
/// the requester stops the response stream, and returns `None` for a close or
/// a stop.
///
/// The select polls the call before the stop. A call that returns its protocol
/// rejection before its first `Pending` therefore reports it even when the
/// stop is already recorded.
async fn call_until_stopped<T>(
    connection_token: &CancellationToken,
    call: impl future::Future<Output = T>,
    stopped: std::pin::Pin<&mut impl future::Future>,
) -> Option<T> {
    zakura_rt::select! {
        biased;
        _ = connection_token.cancelled() => None,
        response = call => Some(response),
        _ = stopped => None,
    }
}

async fn request_stream_worker(
    mut send: SendStream,
    mut recv: RecvStream,
    prelude: StreamPrelude,
    context: StreamWorkerContext,
    registry: Arc<ServiceRegistry>,
) {
    let Some(request_id) = prelude.request_id else {
        let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_BAD_PRELUDE));
        context.close_cause.record(CloseOutcome::fault(
            prelude.stream_kind,
            prelude.stream_version,
            CloseRow::CONN_CLOSE_15,
        ));
        context.connection_token.cancel();
        return;
    };

    let frame = zakura_rt::select! {
        biased;
        _ = context.connection_token.cancelled() => return,
        frame = read_admitted_frame(
            &mut recv,
            context.inbound_frame_cap,
            FrameFilter::new(context.messages, InboundReader::Responder),
            context.limits.idle_timeout,
            // A request stream carries its request frame immediately after the
            // prelude, so a peer that opens one and then goes silent is treated
            // as a stalled one-shot stream and bounded by the idle timeout.
            Some(context.limits.idle_timeout),
            &context.resources.raw,
        ) => frame,
    };

    // The raw frame stays funded until its service's call ends.
    let (frame, raw_grant) = match frame {
        Ok(AdmittedFrame { frame, grant, .. }) => (frame, grant),
        Err(ZakuraHandlerError::Timeout(_)) => {
            let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_NEUTRAL));
            let _ = recv.stop(VarInt::from_u32(ZAKURA_CLOSE_NEUTRAL));
            return;
        }
        Err(error) => {
            debug!(
                ?error,
                "closing Zakura request stream with invalid request frame"
            );
            let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_BAD_PRELUDE));
            context.close_cause.record(read_close_outcome(
                ReadPath::Request,
                context.stream(),
                &error,
            ));
            context.connection_token.cancel();
            return;
        }
    };

    let _ = context.freshness_tx.send(Instant::now());
    match admit_inbound_message(&frame, &context, prelude.stream_kind) {
        InboundMessageAdmission::Admit => {}
        InboundMessageAdmission::Park => {
            // The raw frame keeps its grant while the request waits.
            if !context.park_for_cadence(&frame).await {
                return;
            }
        }
        InboundMessageAdmission::Oversize => {
            let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_OVERSIZE));
            context.close_cause.record(CloseOutcome::fault(
                prelude.stream_kind,
                prelude.stream_version,
                CloseRow::CONN_CLOSE_22,
            ));
            context.connection_token.cancel();
            return;
        }
        InboundMessageAdmission::ByteRateOverrun => {
            // The layout keeps metered rows off request streams, so this is
            // unreachable today. Close as a persistent reader would.
            let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_RATE_LIMIT));
            context.close_cause.record(CloseOutcome::fault(
                prelude.stream_kind,
                prelude.stream_version,
                CloseRow::CONN_CLOSE_18,
            ));
            context.connection_token.cancel();
            return;
        }
        InboundMessageAdmission::Throttled => {
            // The admission path already counted/traced the over-rate frame.
            // Do not reject request streams here: request/response peers also
            // will not resend a dropped frame, and backpressure is safer than
            // data loss while block sync is catching up.
        }
    }

    let response_frame_cap = context.outbound_frame_cap;
    // Fund the whole response before the service runs, so a full output gate stops
    // the request here and no result waits unfunded. The stream's permit and
    // the raw frame's grant stay held during this wait.
    let Some(bound) = response_bound(
        prelude.stream_kind,
        &frame,
        context.limits,
        response_frame_cap,
        context.resources.output.capacity(),
    ) else {
        debug!(
            response_frame_cap,
            "Zakura request needs more output than its kind's gate holds"
        );
        let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_RESOURCE));
        return;
    };
    // A requester that gives up stops the response stream. The stop ends the
    // output wait and the service's call, so the abandoned request releases
    // its funding and no longer runs its backend work. The connection task
    // already answered the stop with RESET_STREAM.
    //
    // A stop gives up the answer, not the protocol: a request the service
    // would reject as protocol-invalid still closes the connection. A stop
    // during the output wait runs the service's checks on the frame. A stop
    // during the call can win only after the call's first `Pending`, because
    // the select polls the call first, and a service returns its protocol
    // rejections before then (`RequestResponseService::request_frame`).
    let stopped = send.stopped();
    zakura_rt::pin!(stopped);
    let mut output = zakura_rt::select! {
        biased;
        _ = context.connection_token.cancelled() => return,
        output = context.resources.output.admit(bound.bytes) => output,
        stop = &mut stopped => {
            if stop.is_ok() {
                if let Err(SinkReject::Protocol(row, error)) =
                    registry.check_request(prelude.stream_kind, &frame)
                {
                    debug!(
                        ?error,
                        "Zakura inbound sink rejected a stopped protocol-invalid request"
                    );
                    context.close_cause.record(CloseOutcome::fault(
                        prelude.stream_kind,
                        prelude.stream_version,
                        row,
                    ));
                    context.connection_token.cancel();
                }
            }
            return;
        }
    };

    // The raw frame's grant ends with the service's call, which cancelling the
    // connection or stopping the stream also ends.
    let Some(response) = call_until_stopped(
        &context.connection_token,
        registry.request(
            context.peer_id.clone(),
            prelude.stream_kind,
            request_id,
            response_frame_cap,
            context.limits.max_message_bytes,
            frame,
        ),
        stopped,
    )
    .await
    else {
        return;
    };
    drop(raw_grant);
    let response_frames = match response {
        Ok(frames) => frames,
        Err(SinkReject::Protocol(row, error)) => {
            debug!(
                ?error,
                "Zakura inbound sink rejected protocol-invalid request"
            );
            let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_BAD_PRELUDE));
            context.close_cause.record(CloseOutcome::fault(
                prelude.stream_kind,
                prelude.stream_version,
                row,
            ));
            context.connection_token.cancel();
            return;
        }
        Err(SinkReject::Local(error)) => {
            debug!(
                ?error,
                "Zakura inbound sink could not answer request locally"
            );
            let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_RESOURCE));
            return;
        }
    };

    // The result is the worker's own retained memory, and `output` funds it through
    // the write from its kind's output gate. A result above its bound has no
    // funding, so it never reaches the write. The bytes the service did not use
    // return to the gate now. No bound exceeds the gate, so one stalled response
    // strands at most its kind's output share.
    let result_bytes = response_frames.iter().fold(0, |bytes: u64, frame| {
        bytes.saturating_add(response_frame_bytes(frame.payload.len()))
    });
    if result_bytes > bound.bytes {
        debug!(
            result_bytes,
            bound_bytes = bound.bytes,
            "Zakura request service returned more than its response bound"
        );
        let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_RESOURCE));
        return;
    }
    output.shrink_to(result_bytes);
    for frame in response_frames {
        // Each write can wait on the peer's send credit, so it ends with the
        // connection instead of holding teardown's drain.
        let written = zakura_rt::select! {
            biased;
            _ = context.connection_token.cancelled() => return,
            written = write_response_frame(
                &mut send,
                frame,
                context.limits,
                response_frame_cap,
            ) => written,
        };
        if let Err(error) = written {
            debug!(?error, "failed to write Zakura request response frame");
            let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_BAD_PRELUDE));
            return;
        }
    }

    let _ = send.finish();
}

/// Waits for `workers` to end, then aborts the rest and waits for those to
/// end. Returns whether every worker ended before the abort.
///
/// An aborted worker keeps its owners until its task actually ends. Teardown
/// settles them before its connection returns its admission slot, so a
/// replacement does not run beside the old generation's stream owners. Each
/// wait has one deadline, [`STREAM_WORKER_DRAIN_TIMEOUT`] after it starts, so
/// workers that end one after another can't extend it. A task that yields to
/// no abort within the timeout cannot hold the slot, and the owner ledger
/// keeps counting its owners.
async fn drain_stream_workers(workers: &mut JoinSet<()>) -> bool {
    join_stream_workers(workers, "Zakura stream worker failed during shutdown").await;
    let drained = workers.is_empty();
    workers.abort_all();
    join_stream_workers(workers, "aborted Zakura stream worker ended").await;
    drained
}

/// Joins `workers` until none is left or [`STREAM_WORKER_DRAIN_TIMEOUT`]
/// passes, logging each failed worker with `failure`.
async fn join_stream_workers(workers: &mut JoinSet<()>, failure: &'static str) {
    let deadline = Instant::now() + STREAM_WORKER_DRAIN_TIMEOUT;
    while let Some(joined) = timeout(
        deadline.saturating_duration_since(Instant::now()),
        workers.join_next(),
    )
    .await
    .ok()
    .flatten()
    {
        if let Err(error) = joined {
            debug!(?error, "{failure}");
        }
    }
}

async fn freshness_reaper(mut freshness_rx: watch::Receiver<Instant>, idle_timeout: Duration) {
    loop {
        let last = *freshness_rx.borrow_and_update();
        let elapsed = last.elapsed();
        if elapsed >= idle_timeout {
            return;
        }
        zakura_rt::select! {
            _ = zakura_rt::time::sleep(idle_timeout - elapsed) => return,
            changed = freshness_rx.changed() => {
                if changed.is_err() {
                    return;
                }
            }
        }
    }
}

async fn read_stream_prelude(
    recv: &mut RecvStream,
    prelude_timeout: Duration,
) -> Result<StreamPrelude, ZakuraHandlerError> {
    let mut fixed = [0; STREAM_PRELUDE_FIXED_BYTES];
    timeout(prelude_timeout, recv.read_exact(&mut fixed))
        .await
        .map_err(|_| ZakuraHandlerError::Timeout("stream prelude"))??;

    let mut fixed_reader = &fixed[..];
    let mut magic = [0; 4];
    fixed_reader.read_exact(&mut magic)?;
    if magic != STREAM_PRELUDE_MAGIC {
        return Err(ZakuraProtocolError::InvalidMagic.into());
    }
    let stream_kind = fixed_reader.read_u16::<LittleEndian>()?;
    let stream_version = fixed_reader.read_u16::<LittleEndian>()?;
    let request_id = match fixed[STREAM_PRELUDE_REQUEST_ID_FLAG_OFFSET] {
        0 => None,
        1 => {
            let mut bytes = [0; STREAM_PRELUDE_REQUEST_ID_BYTES];
            timeout(prelude_timeout, recv.read_exact(&mut bytes))
                .await
                .map_err(|_| ZakuraHandlerError::Timeout("stream request id"))??;
            let mut request_id_reader = &bytes[..];
            Some(request_id_reader.read_u64::<LittleEndian>()?)
        }
        flag => return Err(ZakuraProtocolError::InvalidFlag(flag).into()),
    };

    let mut cap = [0; STREAM_PRELUDE_CAP_BYTES];
    timeout(prelude_timeout, recv.read_exact(&mut cap))
        .await
        .map_err(|_| ZakuraHandlerError::Timeout("stream prelude cap"))??;
    let mut cap_reader = &cap[..];
    let max_frame_bytes = cap_reader.read_u32::<LittleEndian>()?;

    Ok(StreamPrelude {
        magic,
        stream_kind,
        stream_version,
        request_id,
        max_frame_bytes,
    })
}

/// Read one length-prefixed frame from an ordered/request stream.
///
/// `read_timeout` always bounds a frame that is *in progress*: once its first
/// byte has arrived, a peer cannot stall the rest of the header or the payload
/// past this deadline.
///
/// `first_byte_timeout` bounds only the wait for the *next* frame to begin.
/// Passing `None` waits indefinitely for that first byte -- correct for a
/// long-lived ordered stream that is legitimately quiet between frames (e.g. the
/// gossip stream while syncing far below the tip, or header sync after the
/// header frontier has caught up while bodies download). Treating that
/// inter-frame quiet as a fatal read timeout would `connection_token.cancel()`
/// the whole connection -- dropping every active sibling stream on it -- even
/// though the connection-level [`freshness_reaper`] (which resets on ANY
/// stream's activity) and the QUIC idle timeout already own connection-level
/// idleness. Callers race this future against their cancellation tokens, so a
/// genuinely dead connection is still torn down promptly. Passing `Some(_)` is
/// for one-shot streams (a request/response stream where the request, or a
/// response frame, is expected promptly).
async fn read_frame(
    recv: &mut RecvStream,
    max_frame_bytes: u32,
    filter: FrameFilter<'_>,
    read_timeout: Duration,
    first_byte_timeout: Option<Duration>,
) -> Result<Frame, ZakuraHandlerError> {
    read_frame_under(
        recv,
        max_frame_bytes,
        filter,
        read_timeout,
        first_byte_timeout,
        |_| future::ready(()),
    )
    .await
    .map(|(frame, ())| frame)
}

/// Read one frame as [`read_frame`] does, with `gate` admitting its bytes after
/// the header's checks and before the payload's allocation. The grant holds
/// the payload's QUIC credit until it drops.
async fn read_admitted_frame(
    recv: &mut RecvStream,
    max_frame_bytes: u32,
    filter: FrameFilter<'_>,
    read_timeout: Duration,
    first_byte_timeout: Option<Duration>,
    gate: &FrameGate,
) -> Result<AdmittedFrame, ZakuraHandlerError> {
    read_frame_under(
        recv,
        max_frame_bytes,
        filter,
        read_timeout,
        first_byte_timeout,
        |bytes| gate.admit(bytes),
    )
    .await
    .map(|(frame, grant)| AdmittedFrame {
        frame,
        grant,
        #[cfg(any(test, zakura_sim))]
        seq: None,
    })
}

/// What funds a frame's payload from its allocation on.
trait PayloadAdmission {
    /// Keep the QUIC credit of the `payload_len` bytes that `recv` reads next,
    /// if this admission funds them.
    fn hold_credit(&mut self, recv: &mut RecvStream, payload_len: usize);
}

impl PayloadAdmission for () {
    fn hold_credit(&mut self, _recv: &mut RecvStream, _payload_len: usize) {}
}

impl PayloadAdmission for FrameGrant {
    fn hold_credit(&mut self, recv: &mut RecvStream, payload_len: usize) {
        FrameGrant::hold_credit(self, recv, payload_len);
    }
}

/// The frame read itself. `admit` runs once the header passed its checks, and
/// its result funds the allocation that follows and holds the payload's QUIC
/// credit. A stalled peer's in-progress read and a local wait for capacity
/// have separate bounds: `read_timeout` covers only the peer's bytes.
async fn read_frame_under<G, Fut>(
    recv: &mut RecvStream,
    max_frame_bytes: u32,
    filter: FrameFilter<'_>,
    read_timeout: Duration,
    first_byte_timeout: Option<Duration>,
    admit: impl FnOnce(u64) -> Fut,
) -> Result<(Frame, G), ZakuraHandlerError>
where
    G: PayloadAdmission,
    Fut: future::Future<Output = G>,
{
    let mut header = [0; FRAME_HEADER_BYTES];
    // The first header byte is the boundary between "waiting for the next frame"
    // and "reading a frame in progress". Wait for it under `first_byte_timeout`
    // (or indefinitely when `None`); nothing is consumed until it arrives, so a
    // caller that drops this future on cancellation cannot desync the stream.
    let first_byte = recv.read_exact(&mut header[..1]);
    match first_byte_timeout {
        Some(first_byte_timeout) => match timeout(first_byte_timeout, first_byte).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => return Err(ZakuraHandlerError::Closed),
            Err(_) => return Err(ZakuraHandlerError::Timeout("frame header")),
        },
        None => match first_byte.await {
            Ok(()) => {}
            Err(_) => return Err(ZakuraHandlerError::Closed),
        },
    }
    // The frame has started: the remaining header bytes are bounded so a peer
    // cannot stall mid-frame.
    match timeout(read_timeout, recv.read_exact(&mut header[1..])).await {
        Ok(Ok(())) => {}
        Ok(Err(_)) => return Err(ZakuraHandlerError::Closed),
        Err(_) => return Err(ZakuraHandlerError::Timeout("frame header")),
    }
    let mut reader = &header[..];
    let message_type = reader.read_u16::<LittleEndian>()?;
    let flags = reader.read_u16::<LittleEndian>()?;
    let payload_len = usize::try_from(reader.read_u32::<LittleEndian>()?)
        .expect("u32 payload lengths fit usize on supported targets");
    #[cfg(any(test, zakura_sim))]
    filter.header_read(message_type, flags, payload_len);
    let max_frame_bytes =
        usize::try_from(max_frame_bytes).expect("u32 frame cap fits usize on supported targets");
    // The stream's message table can only tighten the negotiated cap. Check it
    // before allocating the payload.
    let max_frame_bytes = filter
        .check_header(message_type, flags, payload_len, max_frame_bytes)
        .map_err(|rejection| {
            metrics::counter!("zakura.p2p.frame.rejected", "reason" => rejection.label())
                .increment(1);
            ZakuraHandlerError::RejectedFrame {
                message_type,
                rejection,
            }
        })?;
    let frame_len = FRAME_HEADER_BYTES.saturating_add(payload_len);
    if frame_len > max_frame_bytes {
        metrics::counter!("zakura.p2p.ratelimit.frame.oversize").increment(1);
        return Err(ZakuraHandlerError::OversizeFrame {
            payload_len,
            frame_len,
            max_frame_bytes,
        });
    }
    let mut admission = admit(frame_wire_bytes(payload_len)).await;
    // Before the payload read, so the hold rides in that read's command.
    admission.hold_credit(recv, payload_len);
    let payload = read_frame_payload(recv, payload_len, read_timeout).await?;
    #[cfg(any(test, zakura_sim))]
    filter.frame_read();
    Ok((
        Frame {
            message_type,
            flags,
            payload,
        },
        admission,
    ))
}

/// Reads a frame's `payload_len` payload bytes.
///
/// The buffer grows as bytes arrive, at most doubling and never past
/// `payload_len`. A peer that announces a large frame and stalls makes the
/// node hold only about twice what it sent, which QUIC credit already
/// covers, instead of the whole announced length.
async fn read_frame_payload(
    recv: &mut RecvStream,
    payload_len: usize,
    read_timeout: Duration,
) -> Result<Vec<u8>, ZakuraHandlerError> {
    let read = async {
        let mut payload = Vec::new();
        while payload.len() < payload_len {
            let remaining = payload_len - payload.len();
            let Some(chunk) = recv.read_chunk(remaining).await? else {
                return Err(zakura_quic::ReadExactError::FinishedEarly(payload.len()));
            };
            if payload.capacity() - payload.len() < chunk.len() {
                payload.reserve_exact(payload.len().max(chunk.len()).min(remaining));
            }
            payload.extend_from_slice(&chunk);
        }
        Ok(payload)
    };
    match timeout(read_timeout, read)
        .await
        .map_err(|_| ZakuraHandlerError::Timeout("frame payload"))?
    {
        Ok(payload) => Ok(payload),
        // A peer can reset a pair during a partial frame. Reset ends that stream;
        // a normal FIN with a truncated payload still reports a protocol error.
        Err(zakura_quic::ReadExactError::ReadError(zakura_quic::ReadError::Reset(_))) => {
            Err(ZakuraHandlerError::Closed)
        }
        Err(error) => Err(error.into()),
    }
}

async fn read_control_payload(
    recv: &mut RecvStream,
    max_bytes: u32,
    read_timeout: Duration,
) -> Result<Vec<u8>, ZakuraHandlerError> {
    // Control payloads (Hello/Ack) carry a hard 16 KiB cap that is otherwise only
    // enforced later in ZakuraControlHello/Ack::decode. Clamp the caller-supplied
    // frame cap (the configured `max_control_frame_bytes`, 1 MiB by default) to it
    // here so a peer cannot make us allocate/read a payload up to the much larger
    // frame cap before decode rejects it as oversized.
    //
    // `MAX_CONTROL_PAYLOAD_BYTES` (16 KiB) is a small compile-time constant, so the
    // cast to u32 cannot truncate; `.min` also preserves any tighter negotiated cap.
    let max_bytes = max_bytes.min(MAX_CONTROL_PAYLOAD_BYTES as u32);
    let mut len_bytes = [0; CONTROL_LENGTH_BYTES];
    timeout(read_timeout, recv.read_exact(&mut len_bytes))
        .await
        .map_err(|_| ZakuraHandlerError::Timeout("control length"))??;
    let len = (&len_bytes[..]).read_u32::<LittleEndian>()?;
    if len == 0 || len > max_bytes {
        return Err(ZakuraHandlerError::Oversize);
    }
    let mut bytes = vec![0; len as usize];
    timeout(read_timeout, recv.read_exact(&mut bytes))
        .await
        .map_err(|_| ZakuraHandlerError::Timeout("control payload"))??;
    Ok(bytes)
}

async fn write_control_payload(
    send: &mut SendStream,
    bytes: &[u8],
    write_timeout: Duration,
) -> Result<(), ZakuraHandlerError> {
    let mut len = Vec::with_capacity(CONTROL_LENGTH_BYTES);
    len.write_u32::<LittleEndian>(bytes.len() as u32)?;
    timeout(write_timeout, send.write_all(&len))
        .await
        .map_err(|_| ZakuraHandlerError::Timeout("control length write"))??;
    timeout(write_timeout, send.write_all(bytes))
        .await
        .map_err(|_| ZakuraHandlerError::Timeout("control payload write"))??;
    let _ = send.finish();
    Ok(())
}

#[cfg(test)]
async fn write_ordered_frame(
    send: &mut SendStream,
    frame: Frame,
    limits: ZakuraConnectionLimits,
    max_frame_bytes: u32,
) -> Result<(), BoxError> {
    let encoded = encode_ordered_frame(frame, limits, max_frame_bytes)?;
    write_encoded_ordered_frame(
        send,
        encoded,
        StreamWritePolicy::Timeout(OUTBOUND_STREAM_WRITE_TIMEOUT),
        None,
    )
    .await
}

/// An ordered-stream frame encoded under its stream's frame cap.
struct EncodedOrderedFrame {
    bytes: Vec<u8>,
    #[cfg(any(test, zakura_sim))]
    message_type: u16,
}

/// Encodes one ordered-stream frame, or refuses it with
/// [`OrderedFrameEncodeError`] if it exceeds the message cap or
/// `max_frame_bytes`.
fn encode_ordered_frame(
    frame: Frame,
    limits: ZakuraConnectionLimits,
    max_frame_bytes: u32,
) -> Result<EncodedOrderedFrame, BoxError> {
    // Mirror `write_response_frame`: a persistent ordered-stream frame whose
    // payload exceeds the peer's negotiated `max_message_bytes` would be
    // rejected by the peer as oversize, so reject it locally before wasting
    // encode/write work rather than writing a frame the peer cannot accept. The
    // handshake clamps `max_frame_bytes` and `max_message_bytes` independently,
    // so the frame cap alone does not bound this.
    if frame.payload.len() > limits.max_message_bytes as usize {
        return Err(OrderedFrameEncodeError::OverMessageCap {
            payload_len: frame.payload.len(),
            max_message_bytes: limits.max_message_bytes,
        }
        .into());
    }
    let bytes = frame
        .encode(max_frame_bytes)
        .map_err(OrderedFrameEncodeError::Encode)?;
    // Return only the encoded bytes: the write can wait on QUIC credit, and
    // holding both copies until then doubles every queued body.
    Ok(EncodedOrderedFrame {
        bytes,
        #[cfg(any(test, zakura_sim))]
        message_type: frame.message_type,
    })
}

/// Writes one encoded ordered-stream frame under `write_policy`. In test and
/// simulator builds, `tap` reports when its write starts and finishes.
async fn write_encoded_ordered_frame(
    send: &mut SendStream,
    encoded: EncodedOrderedFrame,
    write_policy: StreamWritePolicy,
    #[cfg(any(test, zakura_sim))] tap: Option<&Arc<super::StreamTap>>,
) -> Result<(), BoxError> {
    #[cfg(any(test, zakura_sim))]
    let message_type = encoded.message_type;
    let frame = encoded.bytes;
    #[cfg(any(test, zakura_sim))]
    let seq = tap.map(|tap| tap.write_started(message_type));
    match write_policy {
        StreamWritePolicy::UntilCancelled => send.write_chunk(Bytes::from(frame)).await?,
        StreamWritePolicy::Timeout(duration) => {
            timeout(duration, send.write_chunk(Bytes::from(frame)))
                .await
                .map_err(|_| -> BoxError { Box::new(OrderedFrameWriteTimeout) })??
        }
    }
    #[cfg(any(test, zakura_sim))]
    if let (Some(tap), Some(seq)) = (tap, seq) {
        tap.written(seq, message_type);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn write_outbound_request_frame(
    connection: &Connection,
    resources: &ConnectionResources,
    limits: ZakuraConnectionLimits,
    stream: Stream,
    request_id: u64,
    message_type: u16,
    flags: u16,
    payload: Vec<u8>,
) -> Result<ResponseFrames, OutboundRequestError> {
    timeout(
        OUTBOUND_REQUEST_RESPONSE_TIMEOUT,
        write_outbound_request_frame_inner(
            connection,
            resources,
            limits,
            stream,
            request_id,
            message_type,
            flags,
            payload,
        ),
    )
    .await
    .map_err(|_| OutboundRequestError::Local("Zakura outbound request/response timed out".into()))?
}

#[allow(clippy::too_many_arguments)]
async fn write_outbound_request_frame_inner(
    connection: &Connection,
    resources: &ConnectionResources,
    limits: ZakuraConnectionLimits,
    stream: Stream,
    request_id: u64,
    message_type: u16,
    flags: u16,
    payload: Vec<u8>,
) -> Result<ResponseFrames, OutboundRequestError> {
    // The legacy request stream validates responses with a legacy-message-specific budget.
    let budget = LegacyResponseBudget::from_request(message_type, &payload, limits)?;
    // Fund the whole response before the stream opens. The read state's precheck
    // keeps each frame within this budget before the reader allocates its payload.
    let grant = resources
        .admit_compat_response(budget.retained_bytes())
        .await;
    let mut legacy_state = LegacyResponseReadState::new(budget);
    let (mut send, mut recv) = timeout(OUTBOUND_STREAM_WRITE_TIMEOUT, connection.open_bi())
        .await
        .map_err(|_| -> BoxError { "Zakura outbound request stream open timed out".into() })
        .map_err(OutboundRequestError::Local)?
        .map_err(|error| OutboundRequestError::Local(Box::new(error)))?;
    let outbound_frame_cap = application_frame_cap(&limits, stream);
    let inbound_frame_cap = inbound_frame_cap_for_stream(&limits, stream);
    let prelude = StreamPrelude {
        magic: STREAM_PRELUDE_MAGIC,
        stream_kind: stream.kind,
        stream_version: stream.version,
        request_id: Some(request_id),
        max_frame_bytes: inbound_frame_cap,
    };
    let frame = Frame {
        message_type,
        flags,
        payload,
    };
    let prelude = prelude.encode().map_err(|error| {
        OutboundRequestError::Local(BoxError::from(format!("failed to encode prelude: {error}")))
    })?;
    let frame = frame
        .encode(outbound_frame_cap)
        .map_err(|error| OutboundRequestError::Local(Box::new(error)))?;
    timeout(OUTBOUND_STREAM_WRITE_TIMEOUT, send.write_all(&prelude))
        .await
        .map_err(|_| -> BoxError { "Zakura outbound request prelude write timed out".into() })
        .map_err(OutboundRequestError::Local)?
        .map_err(|error| OutboundRequestError::Local(Box::new(error)))?;
    timeout(
        OUTBOUND_STREAM_WRITE_TIMEOUT,
        send.write_chunk(Bytes::from(frame)),
    )
    .await
    .map_err(|_| -> BoxError { "Zakura outbound request frame write timed out".into() })
    .map_err(OutboundRequestError::Local)?
    .map_err(|error| OutboundRequestError::Local(Box::new(error)))?;
    let _ = send.finish();

    let mut items = Vec::new();
    loop {
        match read_frame(
            &mut recv,
            inbound_frame_cap,
            FrameFilter::new(stream.messages, InboundReader::Requester)
                .with_precheck(Some(&legacy_state)),
            limits.idle_timeout,
            // This is the requester side of a one-shot legacy request/response:
            // the responder streams its frames promptly, so a silent gap before
            // the next response frame is bounded by the idle timeout.
            Some(limits.idle_timeout),
        )
        .await
        {
            Ok(frame) => {
                if let Some(item) = legacy_state.validate_frame(request_id, frame)? {
                    items.push(item);
                    if legacy_state.singleton_inventory_complete() {
                        legacy_state.finish()?;
                        // The response codec checks the requested hash or ID
                        // immediately after this handoff. Do not let a peer
                        // retain the decoded item by withholding stream FIN.
                        let _ = recv.stop(VarInt::from_u32(ZAKURA_CLOSE_NEUTRAL));
                        return Ok(ResponseFrames { items, grant });
                    }
                }
            }
            Err(ZakuraHandlerError::Closed) => {
                legacy_state.finish()?;
                return Ok(ResponseFrames { items, grant });
            }
            Err(ZakuraHandlerError::Timeout(_)) => {
                return Err(OutboundRequestError::Local(Box::new(
                    ZakuraHandlerError::Timeout("outbound response"),
                )));
            }
            Err(error @ ZakuraHandlerError::OversizeFrame { .. })
            | Err(error @ ZakuraHandlerError::Oversize) => {
                return Err(OutboundRequestError::Fatal(Box::new(error)));
            }
            Err(error) => return Err(OutboundRequestError::Fatal(Box::new(error))),
        }
    }
}

#[derive(Debug)]
enum OutboundRequestError {
    Local(BoxError),
    Fatal(BoxError),
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum LegacyResponseKind {
    Blocks,
    Transactions,
    BlockHashes,
    BlockHeaders,
    TransactionIds,
    Pong,
    Nil,
}

#[derive(Copy, Clone, Debug)]
struct LegacyResponseBudget {
    kind: LegacyResponseKind,
    max_items: usize,
    max_frames: usize,
    max_bytes: usize,
    max_message_bytes: usize,
}

impl LegacyResponseBudget {
    fn from_request(
        message_type: u16,
        payload: &[u8],
        limits: ZakuraConnectionLimits,
    ) -> Result<Self, OutboundRequestError> {
        let kind = match message_type {
            LEGACY_REQUEST_BLOCKS_BY_HASH => LegacyResponseKind::Blocks,
            LEGACY_REQUEST_TRANSACTIONS_BY_ID => LegacyResponseKind::Transactions,
            LEGACY_REQUEST_FIND_BLOCKS => LegacyResponseKind::BlockHashes,
            LEGACY_REQUEST_FIND_HEADERS => LegacyResponseKind::BlockHeaders,
            LEGACY_REQUEST_MEMPOOL_TRANSACTION_IDS => LegacyResponseKind::TransactionIds,
            LEGACY_REQUEST_PING => LegacyResponseKind::Pong,
            LEGACY_REQUEST_PUSH_TRANSACTION => LegacyResponseKind::Nil,
            _ => {
                return Err(OutboundRequestError::Local(
                    format!("unsupported legacy request message type: {message_type}").into(),
                ));
            }
        };
        let max_message_bytes = usize::try_from(limits.max_message_bytes)
            .map_err(|error| OutboundRequestError::Local(Box::new(error)))?;
        let max_inventory_items = usize::try_from(MAX_TX_INV_IN_SENT_MESSAGE)
            .map_err(|error| OutboundRequestError::Local(Box::new(error)))?;
        let (max_items, max_frames, max_bytes) = match kind {
            LegacyResponseKind::Blocks | LegacyResponseKind::Transactions => {
                let item_count = legacy_inventory_count(payload, kind)?;
                let max_frames = item_count
                    .saturating_mul(LEGACY_RESPONSE_MAX_FRAMES_PER_ITEM)
                    .saturating_add(1);
                let item_bytes = match kind {
                    LegacyResponseKind::Blocks => LEGACY_BLOCK_HASH_BYTES,
                    LegacyResponseKind::Transactions => LEGACY_INVENTORY_HASH_BYTES,
                    _ => unreachable!("matched inventory response kind"),
                };
                let missing_bytes = LEGACY_RESPONSE_REQUEST_ID_BYTES
                    .saturating_add(LEGACY_COMPACT_SIZE_PREFIX_BYTES)
                    .saturating_add(item_count.saturating_mul(item_bytes));
                let max_bytes = item_count
                    .saturating_mul(max_message_bytes)
                    .saturating_add(missing_bytes)
                    .max(LEGACY_RESPONSE_CHUNK_HEADER_BYTES);
                (item_count, max_frames, max_bytes)
            }
            LegacyResponseKind::BlockHashes => {
                let max_bytes = LEGACY_RESPONSE_REQUEST_ID_BYTES
                    .saturating_add(LEGACY_COMPACT_SIZE_PREFIX_BYTES)
                    .saturating_add(
                        MAX_FIND_BLOCKS_RESPONSE_HASHES.saturating_mul(LEGACY_BLOCK_HASH_BYTES),
                    );
                (MAX_FIND_BLOCKS_RESPONSE_HASHES, 1, max_bytes)
            }
            LegacyResponseKind::BlockHeaders => (
                MAX_HEADERS_PER_MESSAGE,
                1,
                LEGACY_RESPONSE_REQUEST_ID_BYTES.saturating_add(max_message_bytes),
            ),
            LegacyResponseKind::TransactionIds => {
                let max_bytes = LEGACY_RESPONSE_REQUEST_ID_BYTES
                    .saturating_add(LEGACY_COMPACT_SIZE_PREFIX_BYTES)
                    .saturating_add(
                        max_inventory_items.saturating_mul(LEGACY_INVENTORY_HASH_BYTES),
                    );
                (max_inventory_items, 1, max_bytes)
            }
            LegacyResponseKind::Pong | LegacyResponseKind::Nil => {
                (1, 1, LEGACY_RESPONSE_REQUEST_ID_BYTES)
            }
        };

        // Clamp the retained-frame byte budget to a fixed operational aggregate
        // cap. For Blocks/Transactions the derived `max_bytes` scales with the
        // requested inventory count (`item_count * max_message_bytes`), so a
        // request naming the protocol-max inventory count would otherwise let a
        // hostile responder accumulate tens of GiB of validated frames in the
        // accepted-frame `Vec` before `decode_response` runs. The inbound
        // responder already bounds a single response to the same aggregate, so
        // this only rejects responses an honest peer would never send.
        let max_bytes = max_bytes.min(COMPAT_RESPONSE_MAX_AGGREGATE_BYTES);

        Ok(Self {
            kind,
            max_items,
            max_frames,
            max_bytes,
            max_message_bytes,
        })
    }

    /// The output the local responder can need for this request.
    ///
    /// This differs from the budget's own byte limit, which checks a peer's
    /// response. A peer negotiates the caps here, so this limit charges every
    /// frame's overhead and never trusts a small cap to make a response small. A
    /// one-frame response's encoder builds its whole payload before any write
    /// checks the frame cap, so its bound ignores that cap.
    fn response_bound(&self, max_frame_bytes: u32) -> ResponseBound {
        let one_frame = |payload_bytes: usize| ResponseBound {
            bytes: response_frame_bytes(payload_bytes),
        };
        match self.kind {
            LegacyResponseKind::Blocks | LegacyResponseKind::Transactions => {
                let message_cap = u32::try_from(self.max_message_bytes).unwrap_or(u32::MAX);
                let Some(chunk_bytes) =
                    super::legacy_gossip::response_chunk_bytes(max_frame_bytes, message_cap)
                else {
                    return ResponseBound::MAX;
                };
                self.inventory_bound(chunk_bytes)
            }
            LegacyResponseKind::TransactionIds => one_frame(
                LEGACY_RESPONSE_REQUEST_ID_BYTES
                    .saturating_add(LEGACY_COMPACT_SIZE_PREFIX_BYTES)
                    .saturating_add(self.max_items.saturating_mul(LEGACY_WTX_INVENTORY_BYTES)),
            ),
            LegacyResponseKind::BlockHashes
            | LegacyResponseKind::BlockHeaders
            | LegacyResponseKind::Pong
            | LegacyResponseKind::Nil => one_frame(self.max_bytes),
        }
    }

    /// The output of a block or transaction response that splits each object into
    /// frames carrying `chunk_bytes` object bytes.
    ///
    /// Each item is either one object of up to a protocol message, or a missing-item
    /// entry that is smaller than that. All missing entries share one more frame.
    /// The encoder refuses a response above the aggregate cap, and each frame it
    /// emits within that cap carries at least one object byte after its chunk header.
    /// An object's last frame can be partial, so the objects within the cap fill at
    /// most one frame per `chunk_bytes`, plus one partial frame per item.
    fn inventory_bound(&self, chunk_bytes: usize) -> ResponseBound {
        let chunks_per_item = MAX_PROTOCOL_MESSAGE_LEN.div_ceil(chunk_bytes);
        let item_payload_bytes = MAX_PROTOCOL_MESSAGE_LEN
            .saturating_add(chunks_per_item.saturating_mul(LEGACY_RESPONSE_CHUNK_HEADER_BYTES));
        let payload_bytes = self
            .max_items
            .saturating_mul(item_payload_bytes)
            .saturating_add(LEGACY_RESPONSE_REQUEST_ID_BYTES)
            .saturating_add(LEGACY_COMPACT_SIZE_PREFIX_BYTES)
            .min(COMPAT_RESPONSE_MAX_AGGREGATE_BYTES);
        let most_frames = COMPAT_RESPONSE_MAX_AGGREGATE_BYTES
            / LEGACY_RESPONSE_CHUNK_HEADER_BYTES.saturating_add(1);
        let filled_frames =
            (COMPAT_RESPONSE_MAX_AGGREGATE_BYTES / chunk_bytes).saturating_add(self.max_items);
        let frames = self
            .max_items
            .saturating_mul(chunks_per_item)
            .min(filled_frames)
            .min(most_frames)
            .saturating_add(1);
        ResponseBound {
            // Widening usize to u64 is lossless on supported targets.
            bytes: payload_bytes
                .saturating_add(frames.saturating_mul(RESPONSE_FRAME_OVERHEAD_BYTES))
                as u64,
        }
    }

    /// A conservative serialized-byte charge: the whole wire response plus
    /// two item-sized working buffers for chunk reassembly.
    ///
    /// Completed inventory items replace their chunks. The charge lasts until
    /// the requester hands the decoded response to its caller; decoded payloads
    /// are outside it.
    fn retained_bytes(&self) -> u64 {
        let copies = match self.kind {
            LegacyResponseKind::Blocks | LegacyResponseKind::Transactions => {
                self.max_bytes.min(self.max_message_bytes).saturating_mul(2)
            }
            LegacyResponseKind::BlockHashes
            | LegacyResponseKind::BlockHeaders
            | LegacyResponseKind::TransactionIds
            | LegacyResponseKind::Pong
            | LegacyResponseKind::Nil => 0,
        };
        // Widening usize to u64 is lossless on supported targets.
        self.max_bytes.saturating_add(copies) as u64
    }
}

/// The memory one queued response frame holds beyond its payload: its wire header,
/// plus two slots in the response's `Vec<Frame>`, each of which owns a payload's
/// vector. A growing vector can hold as many spare slots as filled ones.
///
/// A small frame cap splits a response into many frames, and each frame's slot
/// exists before the first write, so a response bound counts it per frame.
const RESPONSE_FRAME_OVERHEAD_BYTES: usize = FRAME_HEADER_BYTES + 2 * std::mem::size_of::<Frame>();

/// The output one queued response frame with `payload_len` payload bytes needs.
fn response_frame_bytes(payload_len: usize) -> u64 {
    // Widening usize to u64 is lossless on supported targets.
    payload_len.saturating_add(RESPONSE_FRAME_OVERHEAD_BYTES) as u64
}

/// The most output one response to a request can need.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct ResponseBound {
    /// Payload bytes, plus each frame's [`RESPONSE_FRAME_OVERHEAD_BYTES`].
    bytes: u64,
}

impl ResponseBound {
    /// The bound of a request whose response has no derivable limit: any number of
    /// frames, up to the compatibility response cap.
    const MAX: Self = Self {
        // Widening usize to u64 is lossless on supported targets.
        bytes: COMPAT_RESPONSE_MAX_AGGREGATE_BYTES as u64,
    };
}

/// The most output this service's response to `frame` can need under these caps,
/// or `None` if that exceeds its kind's output gate of `gate_bytes`.
///
/// A response with no derivable limit gets [`ResponseBound::MAX`], or the whole gate
/// if that is smaller, so the worker refuses a larger result instead of charging it
/// less than it holds. A request that the legacy budget cannot parse gets that bound
/// too, so the service stays the one owner that rejects it.
fn response_bound(
    stream_kind: u16,
    frame: &Frame,
    limits: ZakuraConnectionLimits,
    max_frame_bytes: u32,
    gate_bytes: u64,
) -> Option<ResponseBound> {
    let underived = ResponseBound {
        bytes: ResponseBound::MAX.bytes.min(gate_bytes),
    };
    if stream_kind != LEGACY_REQUEST_STREAM_KIND {
        return Some(underived);
    }
    let Ok(budget) = LegacyResponseBudget::from_request(frame.message_type, &frame.payload, limits)
    else {
        return Some(underived);
    };
    Some(budget.response_bound(max_frame_bytes)).filter(|bound| bound.bytes <= gate_bytes)
}

#[derive(Debug)]
struct LegacyResponseReadState {
    budget: LegacyResponseBudget,
    frames: usize,
    bytes: usize,
    items: usize,
    active_chunk_type: Option<u16>,
    active_chunk: Vec<u8>,
    /// A NIL answered an inventory request; see `validate_nil`.
    inventory_nil: bool,
}

impl super::regulation::ResponsePrecheck for LegacyResponseReadState {
    fn check(&self, _message_type: u16, payload_len: usize) -> Result<(), FrameRejection> {
        if self.frames >= self.budget.max_frames {
            return Err(FrameRejection::Unsolicited);
        }
        let remaining = self.budget.max_bytes.saturating_sub(self.bytes);
        if payload_len > remaining {
            return Err(FrameRejection::AboveReservation {
                // The aggregate response cap keeps this value below u64::MAX.
                bytes: remaining as u64,
            });
        }
        Ok(())
    }
}

impl LegacyResponseReadState {
    fn new(budget: LegacyResponseBudget) -> Self {
        Self {
            budget,
            frames: 0,
            bytes: 0,
            items: 0,
            active_chunk_type: None,
            active_chunk: Vec::new(),
            inventory_nil: false,
        }
    }

    fn validate_frame(
        &mut self,
        request_id: u64,
        frame: Frame,
    ) -> Result<Option<LegacyResponseItem>, OutboundRequestError> {
        if frame.flags != 0 {
            return Err(OutboundRequestError::Fatal(
                format!("unsupported legacy response flags: {}", frame.flags).into(),
            ));
        }
        if self.inventory_nil {
            return Err(OutboundRequestError::Fatal(
                "legacy response frame after inventory nil".into(),
            ));
        }

        self.frames = self.frames.saturating_add(1);
        if self.frames > self.budget.max_frames {
            return Err(OutboundRequestError::Fatal(
                "too many legacy response frames".into(),
            ));
        }

        self.bytes = self.bytes.saturating_add(frame.payload.len());
        if self.bytes > self.budget.max_bytes {
            return Err(OutboundRequestError::Fatal(
                "legacy response exceeded cumulative byte budget".into(),
            ));
        }

        match frame.message_type {
            LEGACY_RESPONSE_BLOCK => {
                return self.validate_available_chunk(
                    request_id,
                    &frame,
                    LegacyResponseKind::Blocks,
                );
            }
            LEGACY_RESPONSE_TRANSACTION => {
                return self.validate_available_chunk(
                    request_id,
                    &frame,
                    LegacyResponseKind::Transactions,
                );
            }
            LEGACY_RESPONSE_MISSING_BLOCKS => {
                self.validate_missing(request_id, &frame.payload, LegacyResponseKind::Blocks)
            }
            LEGACY_RESPONSE_MISSING_TRANSACTIONS => {
                self.validate_missing(request_id, &frame.payload, LegacyResponseKind::Transactions)
            }
            LEGACY_RESPONSE_BLOCK_HASHES => self.validate_id_prefixed_hashes(
                request_id,
                &frame.payload,
                LegacyResponseKind::BlockHashes,
            ),
            LEGACY_RESPONSE_BLOCK_HEADERS => self.validate_headers(request_id, &frame.payload),
            LEGACY_RESPONSE_TRANSACTION_IDS => self.validate_id_prefixed_hashes(
                request_id,
                &frame.payload,
                LegacyResponseKind::TransactionIds,
            ),
            LEGACY_RESPONSE_PONG => {
                self.validate_id_only(request_id, &frame.payload, LegacyResponseKind::Pong)
            }
            LEGACY_RESPONSE_NIL => self.validate_nil(request_id, &frame.payload),
            message_type => Err(OutboundRequestError::Fatal(
                format!("unknown legacy response message type: {message_type}").into(),
            )),
        }?;
        Ok(Some(LegacyResponseItem::Frame(frame)))
    }

    /// A singleton inventory response is complete after its available item or
    /// missing-item record. Other requests still use stream EOF as their end.
    fn singleton_inventory_complete(&self) -> bool {
        matches!(
            self.budget.kind,
            LegacyResponseKind::Blocks | LegacyResponseKind::Transactions
        ) && self.budget.max_items == 1
            && self.items == 1
            && !self.inventory_nil
            && self.active_chunk_type.is_none()
    }

    /// Checks the response once its stream has ended or its singleton inventory
    /// item is complete.
    ///
    /// Ending mid-item is `Local`: an honest responder resets the stream when a
    /// frame write times out, and the requester sees that the same way as a
    /// peer that stops early. A peer can already decline with an empty
    /// response, so failing closed here would only disconnect slow honest peers.
    fn finish(self) -> Result<(), OutboundRequestError> {
        if self.inventory_nil {
            return Err(OutboundRequestError::Local(
                "legacy nil response to inventory request".into(),
            ));
        }
        if self.active_chunk_type.is_some() {
            return Err(OutboundRequestError::Local(
                "incomplete legacy response chunk".into(),
            ));
        }
        Ok(())
    }

    fn validate_available_chunk(
        &mut self,
        request_id: u64,
        frame: &Frame,
        kind: LegacyResponseKind,
    ) -> Result<Option<LegacyResponseItem>, OutboundRequestError> {
        if self.budget.kind != kind {
            return Err(OutboundRequestError::Fatal(
                "legacy response kind does not match request".into(),
            ));
        }

        let (response_id, is_last, bytes) = legacy_response_chunk_header(&frame.payload)?;
        if response_id != request_id {
            return Err(OutboundRequestError::Fatal(
                format!(
                    "wrong legacy response request id: expected {request_id}, got {response_id}"
                )
                .into(),
            ));
        }

        match self.active_chunk_type {
            Some(active) if active != frame.message_type => {
                return Err(OutboundRequestError::Fatal(
                    "interleaved legacy response chunks".into(),
                ));
            }
            None => self.active_chunk_type = Some(frame.message_type),
            _ => {}
        }

        let new_len = self.active_chunk.len().saturating_add(bytes.len());
        if new_len > self.budget.max_message_bytes {
            return Err(OutboundRequestError::Fatal(
                "legacy response item exceeded message byte cap".into(),
            ));
        }
        self.active_chunk.extend_from_slice(bytes);

        if is_last {
            let item = self.decode_completed_item(kind)?;
            self.active_chunk_type = None;
            self.active_chunk.clear();
            self.add_items(1)?;
            Ok(Some(item))
        } else {
            Ok(None)
        }
    }

    fn decode_completed_item(
        &self,
        kind: LegacyResponseKind,
    ) -> Result<LegacyResponseItem, OutboundRequestError> {
        let mut remaining = self.active_chunk.as_slice();
        let item = match kind {
            LegacyResponseKind::Blocks => Block::zcash_deserialize_bounded(&mut remaining)
                .map(|block| LegacyResponseItem::Block(Arc::new(block))),
            LegacyResponseKind::Transactions => {
                Transaction::zcash_deserialize_bounded(&mut remaining)
                    .map(|transaction| LegacyResponseItem::Transaction(transaction.into()))
            }
            LegacyResponseKind::BlockHashes
            | LegacyResponseKind::BlockHeaders
            | LegacyResponseKind::TransactionIds
            | LegacyResponseKind::Pong
            | LegacyResponseKind::Nil => unreachable!("non-chunk legacy response kind"),
        }
        .map_err(|error| OutboundRequestError::Fatal(Box::new(error)))?;
        // A valid item padded with junk must not pass as a completed item.
        if !remaining.is_empty() {
            return Err(OutboundRequestError::Fatal(
                "legacy response item has trailing bytes".into(),
            ));
        }
        Ok(item)
    }

    fn validate_missing(
        &mut self,
        request_id: u64,
        payload: &[u8],
        kind: LegacyResponseKind,
    ) -> Result<(), OutboundRequestError> {
        let payload = self.begin_id_prefixed_response(kind, request_id, payload, "missing")?;
        let count = legacy_inventory_count(payload, kind)?;
        self.add_items(count)
    }

    fn validate_id_prefixed_hashes(
        &mut self,
        request_id: u64,
        payload: &[u8],
        kind: LegacyResponseKind,
    ) -> Result<(), OutboundRequestError> {
        let payload = self.begin_id_prefixed_response(kind, request_id, payload, "list")?;
        let count = legacy_inventory_count(payload, kind)?;
        self.add_items(count)?;
        if kind == LegacyResponseKind::BlockHashes {
            // An honest responder lists distinct chain hashes. Rejecting a repeat
            // here, rather than only when decoding, also catches a peer that
            // never finishes the stream.
            let hashes = &payload[payload
                .len()
                .saturating_sub(count.saturating_mul(LEGACY_BLOCK_HASH_BYTES))..];
            let mut seen = HashSet::with_capacity(count);
            let (hashes, _) = hashes.as_chunks::<LEGACY_BLOCK_HASH_BYTES>();
            if !hashes.iter().all(|hash| seen.insert(hash)) {
                return Err(OutboundRequestError::Fatal(
                    "legacy block hash response repeated a hash".into(),
                ));
            }
        }
        Ok(())
    }

    fn validate_headers(
        &mut self,
        request_id: u64,
        payload: &[u8],
    ) -> Result<(), OutboundRequestError> {
        let payload = self.begin_id_prefixed_response(
            LegacyResponseKind::BlockHeaders,
            request_id,
            payload,
            "headers",
        )?;
        let count = legacy_header_count(payload)?;
        self.add_items(count)
    }

    fn validate_id_only(
        &mut self,
        request_id: u64,
        payload: &[u8],
        kind: LegacyResponseKind,
    ) -> Result<(), OutboundRequestError> {
        let payload =
            self.begin_id_prefixed_response(kind, request_id, payload, "acknowledgement")?;
        if !payload.is_empty() {
            return Err(OutboundRequestError::Fatal(
                "legacy acknowledgement response has trailing bytes".into(),
            ));
        }
        self.add_items(1)
    }

    /// Validate a `MSG_RESPONSE_NIL` empty-result sentinel against the request kind.
    ///
    /// NIL is the empty result for chain-discovery and mempool queries (and the
    /// acknowledgement of a queued `PushTransaction`); `decode_response` turns it
    /// into that kind's empty `Response`. A malformed NIL, or NIL to a `Ping`
    /// (always answered locally), is `Fatal`.
    ///
    /// A lone NIL to `BlocksByHash` / `TransactionsById` is `Local`: an honest
    /// inbound service still in setup answers every request with NIL. Failing
    /// closed would disconnect it and buys nothing, since a peer can already
    /// decline with an empty response. The encoder never mixes NIL with
    /// inventory frames, so NIL is recorded here, any later frame is `Fatal`,
    /// and `finish` reports the `Local` error only once the stream ends.
    fn validate_nil(
        &mut self,
        request_id: u64,
        payload: &[u8],
    ) -> Result<(), OutboundRequestError> {
        if self.active_chunk_type.is_some() {
            return Err(OutboundRequestError::Fatal(
                "legacy nil response interleaved with response chunk".into(),
            ));
        }
        let (response_id, payload) = legacy_response_id(payload)?;
        if response_id != request_id {
            return Err(OutboundRequestError::Fatal(
                format!(
                    "wrong legacy nil response request id: expected {request_id}, got {response_id}"
                )
                .into(),
            ));
        }
        if !payload.is_empty() {
            return Err(OutboundRequestError::Fatal(
                "legacy nil response has trailing bytes".into(),
            ));
        }
        match self.budget.kind {
            LegacyResponseKind::BlockHashes
            | LegacyResponseKind::BlockHeaders
            | LegacyResponseKind::TransactionIds
            | LegacyResponseKind::Nil => {}
            // `validate_frame` has already counted this frame.
            LegacyResponseKind::Blocks | LegacyResponseKind::Transactions if self.frames == 1 => {
                self.inventory_nil = true;
                return Ok(());
            }
            LegacyResponseKind::Blocks | LegacyResponseKind::Transactions => {
                return Err(OutboundRequestError::Fatal(
                    "legacy nil response after inventory frames".into(),
                ));
            }
            LegacyResponseKind::Pong => {
                return Err(OutboundRequestError::Fatal(
                    "unexpected legacy nil response for ping request".into(),
                ));
            }
        }
        self.add_items(1)
    }

    fn begin_id_prefixed_response<'a>(
        &self,
        kind: LegacyResponseKind,
        request_id: u64,
        payload: &'a [u8],
        label: &'static str,
    ) -> Result<&'a [u8], OutboundRequestError> {
        if self.budget.kind != kind {
            return Err(OutboundRequestError::Fatal(
                format!("legacy {label} response kind does not match request").into(),
            ));
        }
        if self.active_chunk_type.is_some() {
            return Err(OutboundRequestError::Fatal(
                format!("legacy {label} response interleaved with response chunk").into(),
            ));
        }
        let (response_id, payload) = legacy_response_id(payload)?;
        if response_id != request_id {
            return Err(OutboundRequestError::Fatal(
                format!(
                    "wrong legacy {label} response request id: expected {request_id}, got {response_id}"
                )
                    .into(),
            ));
        }
        Ok(payload)
    }

    fn add_items(&mut self, count: usize) -> Result<(), OutboundRequestError> {
        self.items = self.items.saturating_add(count);
        if self.items > self.budget.max_items {
            return Err(OutboundRequestError::Fatal(
                "legacy response contained more items than requested".into(),
            ));
        }
        Ok(())
    }
}

fn legacy_inventory_count(
    payload: &[u8],
    kind: LegacyResponseKind,
) -> Result<usize, OutboundRequestError> {
    let mut reader = Cursor::new(payload);
    let count = usize::from(
        CompactSizeMessage::zcash_deserialize(&mut reader)
            .map_err(|error| OutboundRequestError::Fatal(Box::new(error)))?,
    );

    match kind {
        LegacyResponseKind::Blocks | LegacyResponseKind::BlockHashes => {
            let consumed = usize::try_from(reader.position())
                .map_err(|error| OutboundRequestError::Fatal(Box::new(error)))?;
            let expected_len =
                consumed.saturating_add(count.saturating_mul(LEGACY_BLOCK_HASH_BYTES));
            if expected_len != payload.len() {
                return Err(OutboundRequestError::Fatal(
                    "legacy block inventory has trailing or truncated bytes".into(),
                ));
            }
        }
        LegacyResponseKind::Transactions | LegacyResponseKind::TransactionIds => {
            for _ in 0..count {
                let inventory = InventoryHash::zcash_deserialize(&mut reader)
                    .map_err(|error| OutboundRequestError::Fatal(Box::new(error)))?;
                if inventory.unmined_tx_id().is_none() {
                    return Err(OutboundRequestError::Fatal(
                        "legacy transaction inventory contained a non-transaction item".into(),
                    ));
                }
            }
            reject_legacy_inventory_trailing(payload, &reader)?;
        }
        LegacyResponseKind::BlockHeaders | LegacyResponseKind::Pong | LegacyResponseKind::Nil => {
            return Err(OutboundRequestError::Local(
                "legacy response kind does not use inventory counts".into(),
            ));
        }
    }

    Ok(count)
}

fn legacy_header_count(payload: &[u8]) -> Result<usize, OutboundRequestError> {
    let mut reader = Cursor::new(payload);
    let count = usize::from(
        CompactSizeMessage::zcash_deserialize(&mut reader)
            .map_err(|error| OutboundRequestError::Fatal(Box::new(error)))?,
    );
    if count > MAX_HEADERS_PER_MESSAGE {
        return Err(OutboundRequestError::Fatal(
            "legacy headers response exceeded header count cap".into(),
        ));
    }
    for _ in 0..count {
        CountedHeader::zcash_deserialize(&mut reader)
            .map_err(|error| OutboundRequestError::Fatal(Box::new(error)))?;
    }
    reject_legacy_inventory_trailing(payload, &reader)?;
    Ok(count)
}

fn reject_legacy_inventory_trailing(
    payload: &[u8],
    reader: &Cursor<&[u8]>,
) -> Result<(), OutboundRequestError> {
    if usize::try_from(reader.position())
        .map_err(|error| OutboundRequestError::Fatal(Box::new(error)))?
        != payload.len()
    {
        return Err(OutboundRequestError::Fatal(
            "legacy transaction inventory has trailing bytes".into(),
        ));
    }
    Ok(())
}

fn legacy_response_id(payload: &[u8]) -> Result<(u64, &[u8]), OutboundRequestError> {
    if payload.len() < LEGACY_RESPONSE_REQUEST_ID_BYTES {
        return Err(OutboundRequestError::Fatal(
            "truncated legacy response id".into(),
        ));
    }
    let mut id = [0; LEGACY_RESPONSE_REQUEST_ID_BYTES];
    id.copy_from_slice(&payload[..LEGACY_RESPONSE_REQUEST_ID_BYTES]);
    Ok((
        u64::from_le_bytes(id),
        &payload[LEGACY_RESPONSE_REQUEST_ID_BYTES..],
    ))
}

fn legacy_response_chunk_header(
    payload: &[u8],
) -> Result<(u64, bool, &[u8]), OutboundRequestError> {
    if payload.len() < LEGACY_RESPONSE_CHUNK_HEADER_BYTES {
        return Err(OutboundRequestError::Fatal(
            "truncated legacy response chunk".into(),
        ));
    }
    let (request_id, payload) = legacy_response_id(payload)?;
    Ok((request_id, payload[0] != 0, &payload[1..]))
}

async fn write_response_frame(
    send: &mut SendStream,
    frame: Frame,
    limits: ZakuraConnectionLimits,
    max_frame_bytes: u32,
) -> Result<(), ZakuraHandlerError> {
    if frame.payload.len() > limits.max_message_bytes as usize {
        return Err(ZakuraHandlerError::Oversize);
    }
    let encoded = frame.encode(max_frame_bytes)?;
    // Free the payload before a write that can wait on QUIC credit.
    drop(frame);
    let frame = encoded;
    timeout(
        OUTBOUND_STREAM_WRITE_TIMEOUT,
        send.write_chunk(Bytes::from(frame)),
    )
    .await
    .map_err(|_| ZakuraHandlerError::Timeout("frame write"))??;
    Ok(())
}

fn validate_idle_invariant(limits: &ZakuraLocalLimits) -> Result<(), ZakuraHandlerError> {
    if limits.keep_alive_interval >= limits.quic_idle_timeout {
        return Err(ZakuraHandlerError::InvalidLocalLimits);
    }
    if limits.initial_limits().idle_timeout_millis as u128 >= limits.quic_idle_timeout.as_millis() {
        return Err(ZakuraHandlerError::InvalidLocalLimits);
    }
    Ok(())
}

fn zakura_secret_key(config: &Config) -> Result<NodeSecretKey, ZakuraHandlerError> {
    // Loads the configured key, or loads/generates+persists a stable key under the
    // cache dir so the node keeps a consistent NodeId across restarts.
    config
        .zakura_secret_key()
        .map_err(|_| ZakuraHandlerError::InvalidSecretKey)
}

fn stream_kind_label(stream_kind: u16) -> &'static str {
    match stream_kind {
        0 => "control",
        1 => "request",
        LEGACY_GOSSIP_STREAM_KIND => "gossip",
        LEGACY_REQUEST_STREAM_KIND => "legacy_request",
        DISCOVERY_STREAM_KIND => "discovery",
        HEADER_SYNC_STREAM_KIND => "header_sync",
        ZAKURA_STREAM_BLOCK_SYNC => "block_sync",
        super::tx_relay::wire::TX_RELAY_DATA_KIND => "tx_relay_data",
        super::tx_relay::wire::TX_RELAY_STATE_KIND => "tx_relay_state",
        super::bpv1::wire::BPV1_CONTROL_KIND => "bpv1_control",
        super::bpv1::wire::BPV1_DATA_KIND => "bpv1_data",
        _ => "unknown",
    }
}

fn application_frame_cap(limits: &ZakuraConnectionLimits, stream: Stream) -> u32 {
    limits.max_frame_bytes.min(stream.frame_cap)
}

fn peer_accepted_frame_cap(
    limits: &ZakuraConnectionLimits,
    stream: Stream,
    peer_max_frame_bytes: u32,
) -> u32 {
    application_frame_cap(limits, stream).min(peer_max_frame_bytes)
}

/// Frame cap for reading frames received from a peer, never larger than the
/// message cap allows.
///
/// On ordered/request streams and requester-side responses, a frame payload *is*
/// the message. A peer can negotiate `max_frame_bytes > max_message_bytes` (the
/// two caps are clamped independently in `ZakuraLocalLimits::clamp`), so the cap
/// handed to `read_frame` must also be limited to the message size. Otherwise a
/// frame whose `payload_len` falls between the two limits is allocated and read
/// in full before the later message-level validation rejects or decodes it.
fn inbound_frame_cap_for_stream(limits: &ZakuraConnectionLimits, stream: Stream) -> u32 {
    let frame_header_bytes =
        u32::try_from(FRAME_HEADER_BYTES).expect("frame header byte count fits in u32");
    application_frame_cap(limits, stream)
        .min(limits.max_message_bytes.saturating_add(frame_header_bytes))
}

fn minimum_ordered_queue_capacity(streams: &[Stream]) -> usize {
    // Regulation needs independent room for a response and requests/control.
    // Layouts without a message table keep one slot per stream.
    let slots = if streams.iter().any(|stream| stream.messages.is_some()) {
        2
    } else {
        1
    };
    streams.len().saturating_mul(slots)
}

fn per_stream_inbound_queue_depth(
    max_inbound_queue_depth: u16,
    ordered_stream_count: usize,
) -> usize {
    let total = usize::from(max_inbound_queue_depth).max(1);
    if ordered_stream_count == 0 {
        return total;
    }

    total.saturating_div(ordered_stream_count).max(1)
}

fn should_run_freshness_reaper(
    ordered_stream_count: usize,
    request_response_stream_count: usize,
) -> bool {
    ordered_stream_count > 0 || request_response_stream_count == 0
}

/// The common stream version used by services whose declarations remain at version 1.
#[cfg(test)]
const ZAKURA_STREAM_VERSION_1: u16 = 1;

/// Returns whether the handler can serve a stream with this kind and version.
///
/// A peer controls both fields of the prelude, so an unknown kind or an
/// unsupported version of a known kind must be rejected before the stream
/// consumes a worker, a stream permit, queue depth, or rate budget. Keeping
/// this in one place means [`stream_kind_label`] (used for metrics/trace) and
/// admission agree on what "known" means.
#[cfg(test)]
fn is_supported_stream(registry: &ServiceRegistry, stream_kind: u16, stream_version: u16) -> bool {
    registry
        .capability_for_stream(stream_kind, stream_version)
        .is_some()
}

/// One message-rate [`TokenBucket`] shared by every stream worker serving the
/// same stream kind on one connection.
///
/// A worker holds this briefly (no `.await` while locked) to spend one token
/// per decoded frame, so N concurrent same-kind streams draw from a single
/// per-connection budget instead of N independent ones (FLUP-014).
type SharedMessageBucket<C = RealClock> = Arc<StdMutex<TokenBucket<C>>>;

/// Per-connection message-rate state, shared across that connection's workers.
///
/// Streams without a message table draw from one bucket per validated stream
/// kind. Created lazily (FLUP-015 rejects unknown kinds before this point, so
/// every key here is a known kind). Streams with a table charge only the rows
/// that declare a cadence, in the connection's [`CadenceBuckets`].
struct MessageRateBuckets<C: Clock = RealClock> {
    streams: HashMap<u16, SharedMessageBucket<C>>,
    cadence: SharedCadenceBuckets<C>,
}

impl<C: Clock + Default> MessageRateBuckets<C> {
    /// Buckets for a connection with no stream receive window, whose byte
    /// buckets earn from now, as tests use.
    #[cfg(test)]
    fn new() -> Self {
        Self::for_connection(0, C::default().now())
    }

    /// Buckets for a connection whose stream receive window is
    /// `receive_window`, which deepens its byte buckets, and whose byte
    /// buckets earn from `metered_epoch`.
    fn for_connection(receive_window: u64, metered_epoch: Instant) -> Self {
        Self {
            streams: HashMap::new(),
            cadence: Arc::new(StdMutex::new(
                CadenceBuckets::new(C::default())
                    .with_receive_window(receive_window)
                    .with_metered_epoch(metered_epoch),
            )),
        }
    }
}

impl<C: Clock> MessageRateBuckets<C> {
    /// Number of per-kind stream buckets.
    #[cfg(test)]
    fn len(&self) -> usize {
        self.streams.len()
    }
}

/// One connection's cadence buckets, shared by its readers.
type SharedCadenceBuckets<C = RealClock> = Arc<StdMutex<CadenceBuckets<C>>>;

/// A tabled stream's view of its connection's cadence buckets.
#[derive(Clone, Debug)]
struct CadenceScope {
    buckets: SharedCadenceBuckets,
    /// The primary stream kind of the stream's layout, which keys its buckets.
    layout: u16,
    /// Whether the stream's reader waits for a token when a bucket is empty.
    /// See [`parks_on_exhaustion`].
    parks: bool,
}

/// The cadence scope for `stream` in `layout`, if the stream has a table.
fn cadence_scope(
    buckets: &MessageRateBuckets,
    stream: Stream,
    layout: u16,
) -> Option<CadenceScope> {
    stream.messages.map(|rules| CadenceScope {
        buckets: buckets.cadence.clone(),
        layout,
        parks: parks_on_exhaustion(rules),
    })
}

/// Returns the shared message-rate bucket for `stream_kind` on this connection,
/// creating it sized from `message_rate_per_second` on first use.
///
/// Two workers serving the same kind get [`Arc::clone`]s of the same bucket, so
/// their `try_take` calls draw from one budget (FLUP-014). Generic over the
/// clock so tests can drive deterministic refills with a `TestClock`.
fn message_bucket_for<C: Clock>(
    buckets: &mut MessageRateBuckets<C>,
    stream_kind: u16,
    message_rate_per_second: u32,
    clock: C,
) -> SharedMessageBucket<C> {
    buckets
        .streams
        .entry(stream_kind)
        .or_insert_with(|| {
            Arc::new(StdMutex::new(TokenBucket::with_clock(
                message_rate_per_second,
                clock,
            )))
        })
        .clone()
}

#[derive(Clone, Debug)]
struct TokenBucket<C = RealClock> {
    capacity: u32,
    tokens: u32,
    refill_per_second: u32,
    last_refill: Instant,
    clock: C,
}

impl TokenBucket<RealClock> {
    fn new(refill_per_second: u32) -> Self {
        Self::with_clock(refill_per_second, RealClock)
    }
}

impl<C: Clock> TokenBucket<C> {
    fn with_clock(refill_per_second: u32, clock: C) -> Self {
        let capacity = refill_per_second.max(1);
        Self {
            capacity,
            tokens: capacity,
            refill_per_second: capacity,
            last_refill: clock.now(),
            clock,
        }
    }

    fn try_take(&mut self) -> bool {
        self.refill(self.clock.now());
        if self.tokens == 0 {
            return false;
        }
        self.tokens -= 1;
        true
    }

    fn refill(&mut self, now: Instant) {
        let elapsed = now.saturating_duration_since(self.last_refill);
        let elapsed_nanos = elapsed.as_nanos();
        let new_tokens =
            elapsed_nanos.saturating_mul(u128::from(self.refill_per_second)) / 1_000_000_000;
        if new_tokens == 0 {
            return;
        }
        let new_tokens = new_tokens.min(u128::from(u32::MAX)) as u32;
        self.tokens = self.capacity.min(self.tokens.saturating_add(new_tokens));
        self.last_refill = now;
    }
}

/// Errors produced by the Zakura protocol handler.
#[derive(Debug, Error)]
pub enum ZakuraHandlerError {
    /// The stream's message table rejected a frame from its header.
    #[error("rejected a frame of message type {message_type}: {rejection}")]
    RejectedFrame {
        /// The frame's message type.
        message_type: u16,
        /// The broken rule.
        rejection: FrameRejection,
    },
    /// Two ordered stream roles failed to name one complete session.
    #[error("invalid Zakura service session")]
    InvalidServiceSession,
    /// A bounded read/write timed out.
    #[error("Zakura {0} timed out")]
    Timeout(&'static str),
    /// A peer-controlled payload exceeded its cap.
    #[error("Zakura payload exceeded its cap")]
    Oversize,
    /// A peer declared a frame larger than the effective receiver cap.
    #[error(
        "Zakura frame length {frame_len} exceeded cap {max_frame_bytes} \
         (payload length {payload_len})"
    )]
    OversizeFrame {
        /// Declared frame payload length.
        payload_len: usize,
        /// Full frame length including the fixed header.
        frame_len: usize,
        /// Effective frame cap used before reading the payload.
        max_frame_bytes: usize,
    },
    /// The peer closed the stream or connection.
    #[error("Zakura stream closed")]
    Closed,
    /// The configured bootstrap peer is malformed.
    #[error("invalid Zakura bootstrap peer")]
    InvalidBootstrapPeer,
    /// The configured node secret key is malformed.
    #[error("invalid Zakura node secret key")]
    InvalidSecretKey,
    /// Local Zakura limits violate an invariant.
    #[error("invalid Zakura local limits")]
    InvalidLocalLimits,
    /// A local resource cap rejected the operation.
    #[error("Zakura resource limit exceeded: {0}")]
    ResourceLimit(&'static str),
    /// Another connection reserved the last service session slot.
    #[error("ordered service session capacity is full")]
    SessionFull,
    /// The peer exceeded its per-kind inbound message rate.
    #[error("Zakura message rate exceeded")]
    RateLimited,
    /// The peer sent a metered row past its connection's byte bucket.
    #[error("Zakura byte rate exceeded")]
    ByteRateOverrun,
    /// QUIC connection error.
    #[error(transparent)]
    QuicConnection(#[from] zakura_quic::ConnectionError),
    /// QUIC dial error.
    #[error(transparent)]
    QuicConnect(#[from] zakura_quic::ConnectError),
    /// QUIC stream write error.
    #[error(transparent)]
    QuicWrite(#[from] zakura_quic::WriteError),
    /// QUIC stream read error.
    #[error(transparent)]
    QuicRead(#[from] zakura_quic::ReadExactError),
    /// Closed stream.
    #[error(transparent)]
    QuicClosedStream(#[from] zakura_quic::ClosedStream),
    /// Zakura wire format error.
    #[error(transparent)]
    Protocol(#[from] ZakuraProtocolError),
    /// Zakura validation error.
    #[error(transparent)]
    Validation(#[from] super::ZakuraValidationError),
    /// I/O error while encoding or decoding local buffers.
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// The [`ZakuraHandlerError::ResourceLimit`] a dial returns when the admission
/// semaphore is full, before it sends any packet.
const ADMISSION_LIMIT: &str = "admission";
/// TLS proved the dial reached a node that cannot keep the claimed slot.
pub(super) const AUTHENTICATED_SLOT_LIMIT: &str = "authenticated connection slot";
const PENDING_HANDSHAKE_LIMIT: &str = "pending handshake";
/// The [`ZakuraHandlerError::ResourceLimit`] a dial returns when the transport
/// cannot fit the dial under its connection ceiling, before any packet
/// (zakura-quic ADM-12).
const TRANSPORT_CONNECTIONS_LIMIT: &str = "transport connections";

impl ZakuraHandlerError {
    /// Whether a dial stopped at one of this node's own caps before sending
    /// any packet, so retrying it costs the peer nothing. A pending-handshake
    /// limit isn't one: it fires after QUIC and TLS with the peer completed.
    pub(crate) fn is_pre_dial_limit(&self) -> bool {
        matches!(
            self,
            Self::ResourceLimit(limit)
                if *limit == ADMISSION_LIMIT || *limit == TRANSPORT_CONNECTIONS_LIMIT
        )
    }

    /// Whether a dial failed because the peer speaks another ALPN (zakura-quic DIAL-5).
    pub(crate) fn is_alpn_mismatch(&self) -> bool {
        matches!(
            self,
            Self::QuicConnect(zakura_quic::ConnectError::AlpnMismatch)
        )
    }

    fn oversize_frame_details(&self) -> Option<(u64, u64, u64)> {
        let Self::OversizeFrame {
            payload_len,
            frame_len,
            max_frame_bytes,
        } = self
        else {
            return None;
        };

        Some((
            u64::try_from(*payload_len).unwrap_or(u64::MAX),
            u64::try_from(*frame_len).unwrap_or(u64::MAX),
            u64::try_from(*max_frame_bytes).unwrap_or(u64::MAX),
        ))
    }
}

#[cfg(all(test, not(zakura_sim)))]
mod tests {
    mod admission_rate;
    mod bounded_decoding;
    mod cadence_park;
    pub(super) mod connection;
    mod frame_lifetime;
    #[cfg(not(zakura_sim))]
    mod handshake_cost;
    mod moved_identity;
    mod preferred_hold;
    mod quic_progress;
    mod read_close_cause;
    mod teardown;
    mod transport_observer;
    use super::*;
    use crate::zakura::P2P_V2_ALPN;
    use crate::{
        protocol::internal::{InventoryResponse, Response},
        zakura::{
            legacy_gossip::{
                LegacyGossipError, LegacyRequestFrame, LegacyRequestKind, LegacyResponseCodec,
                RequestedInventory,
            },
            testkit::{await_until, LocalEndpointFactory, ZakuraTestNode},
            Event, HeaderSyncMisbehavior, PeerSession, ServicePeerLimits,
            LOCAL_MAX_CONTROL_FRAME_BYTES, LOCAL_MAX_MESSAGE_BYTES, MAX_BS_FRAME_BYTES,
            MAX_HS_MESSAGE_BYTES, ZAKURA_CAP_BLOCK_SYNC_REGULATED, ZAKURA_CAP_DISCOVERY,
            ZAKURA_CAP_HEADER_SYNC, ZAKURA_CAP_LEGACY_GOSSIP, ZAKURA_HEADER_SYNC_STREAM_VERSION,
            ZAKURA_REGULATED_BLOCK_SYNC_STREAM_VERSION,
        },
        P2pStack,
    };
    use zakura_chain::{
        block::{self, Block},
        serialization::{ZcashDeserialize, MAX_PROTOCOL_MESSAGE_LEN},
        transaction::{self, UnminedTxId},
    };
    use zakura_test::vectors::{BLOCK_MAINNET_GENESIS_BYTES, BLOCK_TESTNET_141042_BYTES};

    /// The stream window check compares against block sync's frame cap, so no
    /// other native stream kind may carry a larger frame.
    #[test]
    fn block_sync_carries_the_largest_native_frame() {
        use crate::zakura::header_sync::v9::{CommonMessage, RegtestMessage};

        let streams: Vec<Stream> = super::super::legacy_gossip::legacy_gossip_streams()
            .iter()
            .chain(&CommonMessage::LAYOUT)
            .chain(&RegtestMessage::LAYOUT)
            .copied()
            .collect();
        for stream in &streams {
            assert!(
                stream.frame_cap <= MAX_BS_FRAME_BYTES,
                "stream kind {} carries {} bytes, more than a block-sync frame",
                stream.kind,
                stream.frame_cap
            );
        }
        // Discovery's frame is its largest peers payload and a frame header.
        let discovery = super::super::discovery::wire::PEERS_MAX_BYTES + FRAME_HEADER_BYTES;
        assert!(discovery <= MAX_BS_FRAME_BYTES as usize);
    }

    /// Regression for the mainnet dual-stack body-sync stall on
    /// `temp-zakura-sync-test-7` during run `20260714T073939Z-cca353fd1287`.
    /// Body sync stopped at height 2,725,606 with available download capacity.
    /// Header sync continued to tip while block sync lost all peers.
    ///
    /// The transport caused the stall. Block sync parks a peer that misses its
    /// no-progress liveness deadline. The transport redials the peer before the
    /// `no_progress_peer_cooldown` expires. The transport asks block sync for
    /// demand only during connection setup.
    /// Block sync refuses the parked peer.
    /// Header sync and discovery keep the connection healthy.
    /// The transport therefore does not redial it after the cooldown.
    /// Repeating this sequence reduced the block-sync peer count to zero.
    /// The node still retained healthy connections.
    ///
    /// The test drives `serve_connection` and `BlockSyncService` over a local QUIC connection.
    /// It parks the peer through the liveness path.
    /// After the cooldown, block sync must receive a stream on the existing connection.
    #[tokio::test]
    async fn parked_block_sync_peer_gets_a_stream_when_its_cooldown_lapses() -> Result<(), BoxError>
    {
        const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
        const COOLDOWN: Duration = Duration::from_secs(15);

        let _guard = zakura_test::init();

        let genesis = Block::zcash_deserialize(&BLOCK_MAINNET_GENESIS_BYTES[..])?;
        let anchor = (block::Height(0), genesis.hash());
        let frontiers = FullStateFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: genesis.hash(),
        };
        let node = |seed| {
            ZakuraTestNode::builder(seed)
                .header_sync_driver(Config::default().network, anchor, frontiers, Some(anchor))
                .spawn()
        };
        let dialer = node(140).await?;
        let listener = node(141).await?;

        let listener_addr = listener.node_addr().await;
        let listener_peer = ZakuraPeerId::new(listener_addr.id.as_bytes().to_vec())?;
        let block_sync = dialer
            .block_sync()
            .expect("the header-sync driver spawns the block-sync reactor");

        // Block sync evicts and parks the peer after its no-progress deadline.
        // Keep the cooldown longer than connection setup so the first assertion
        // tests a live park even when the dial is slow.
        let parked_at = std::time::Instant::now();
        block_sync.park_peer_for_test(&listener_peer, COOLDOWN);
        dialer
            .connect_native_to_addr(listener_addr, CONNECT_TIMEOUT)
            .await?;

        // The park remains active.
        // Withhold block sync from this connection.
        assert!(
            parked_at.elapsed() < COOLDOWN,
            "connection setup outlasted the test cooldown: {:?}",
            parked_at.elapsed(),
        );
        assert_eq!(
            block_sync.peer_snapshot().outbound_peers,
            0,
            "a parked peer must not be given a block-sync stream while its cooldown runs",
        );

        // The connection stays healthy after the cooldown.
        // The transport must recheck demand to restore the block-sync peer.
        await_until(
            "block sync opens a stream to the peer whose park expired",
            Duration::from_secs(30),
            || block_sync.peer_snapshot().outbound_peers == 1,
        )
        .await?;

        dialer.shutdown().await;
        listener.shutdown().await;
        Ok(())
    }

    #[test]
    fn header_capability_mask_tracks_explicit_runtime_enablement() {
        let handler = ZakuraProtocolHandler::new(
            ZakuraSupervisorHandle::new(1),
            Network::Mainnet,
            ZakuraHandshakeConfig::for_network(&Network::Mainnet),
            ZakuraLocalLimits::from_config(&Config::default()),
        );
        handler.supported_capabilities.store(
            ZAKURA_CAP_DISCOVERY | ZAKURA_CAP_HEADER_SYNC,
            Ordering::Relaxed,
        );

        handler.set_header_sync_enabled(false);
        assert_eq!(
            handler.current_handshake_config().supported_capabilities,
            ZAKURA_CAP_DISCOVERY
        );

        handler.set_header_sync_enabled(true);
        assert_eq!(
            handler.current_handshake_config().supported_capabilities,
            ZAKURA_CAP_DISCOVERY | ZAKURA_CAP_HEADER_SYNC
        );
        assert_eq!(handler.handshake_snapshot().1, 2, "each change bumps once");

        // Repeating the current setting keeps in-flight negotiations current.
        handler.set_header_sync_enabled(true);
        assert_eq!(handler.handshake_snapshot().1, 2);
        handler.set_header_sync_enabled(false);
        handler.set_header_sync_enabled(false);
        assert_eq!(handler.handshake_snapshot().1, 3);
    }

    #[test]
    fn header_capability_epoch_is_applied_once_and_rejects_stale_demand() {
        use zakura_node_services::sync_lifecycle::{
            BlockServiceDemand, HeaderServiceDemand, LifecycleEpoch, SyncServiceDemand,
        };

        let disabled = SyncServiceDemand {
            header: HeaderServiceDemand::Disabled {
                runtime_epoch: LifecycleEpoch::INITIAL,
            },
            block: BlockServiceDemand::ServingAndApplying {
                apply_epoch: LifecycleEpoch::INITIAL,
            },
        };
        let mut epochs = HeaderCapabilityEpochs::from_initial(&disabled);
        let ready = SyncServiceDemand {
            header: HeaderServiceDemand::Enabled {
                capability_epoch: LifecycleEpoch::new(2),
            },
            ..disabled
        };
        assert_eq!(epochs.observe(&ready), Some(LifecycleEpoch::new(2)));
        assert_eq!(epochs.observe(&ready), None);
        let stale = SyncServiceDemand {
            header: HeaderServiceDemand::Enabled {
                capability_epoch: LifecycleEpoch::new(1),
            },
            ..disabled
        };
        assert_eq!(epochs.observe(&stale), None);
    }

    #[tokio::test]
    async fn enabling_header_sync_reconnects_peers_negotiated_without_it() {
        let supervisor = ZakuraSupervisorHandle::new(4);
        let handler = ZakuraProtocolHandler::new(
            supervisor.clone(),
            Network::Mainnet,
            ZakuraHandshakeConfig::for_network(&Network::Mainnet),
            ZakuraLocalLimits::from_config(&Config::default()),
        );
        handler.set_header_sync_enabled(false);

        let first_token = CancellationToken::new();
        let second_token = CancellationToken::new();
        register_test_peer(&supervisor, test_peer(31), first_token.clone()).await;
        register_test_peer(&supervisor, test_peer(32), second_token.clone()).await;

        assert_eq!(
            enable_header_sync_and_renegotiate(
                &handler,
                &supervisor,
                zakura_node_services::sync_lifecycle::LifecycleEpoch::new(1),
            )
            .await,
            2
        );
        assert_ne!(
            handler.current_handshake_config().supported_capabilities & ZAKURA_CAP_HEADER_SYNC,
            0
        );
        assert!(first_token.is_cancelled());
        assert!(second_token.is_cancelled());
    }

    /// With no configured `zakura.listen_addr`, the native endpoint must bind
    /// loopback-only. Otherwise an unspecified bind (`0.0.0.0:0` / `[::]:0`)
    /// exposes the experimental P2P_V2_ALPN handshake/session surface on every
    /// interface on an OS-assigned ephemeral port, even though the unset state is
    /// documented as dial-out only.
    #[tokio::test]
    async fn unset_listen_addr_binds_loopback_not_unspecified() {
        let limits = ZakuraLocalLimits::from_config(&Config::default());
        let endpoint = QuicEndpoint::bind(
            NodeSecretKey::generate(),
            &limits.quic_bind_config(native_bind_addrs(None)),
            &limits.quic,
        )
        .expect("loopback bind should succeed");

        let sockets = endpoint.local_addrs();
        assert!(
            !sockets.is_empty(),
            "endpoint should bind at least one socket"
        );
        for socket in &sockets {
            assert!(
                socket.ip().is_loopback(),
                "unset listen_addr must bind loopback only, but bound {socket} \
                 (exposes P2P_V2_ALPN on all interfaces)"
            );
        }

        endpoint.shutdown().await;
    }

    #[tokio::test]
    async fn configured_listen_addr_binds_only_requested_family() -> Result<(), BoxError> {
        let limits = ZakuraLocalLimits::from_config(&Config::default());
        for addr in [
            ZAKURA_LOOPBACK_BIND_V4.into(),
            ZAKURA_LOOPBACK_BIND_V6.into(),
        ] {
            let endpoint = QuicEndpoint::bind(
                LocalEndpointFactory::secret_key(90210),
                &limits.quic_bind_config(native_bind_addrs(Some(addr))),
                &limits.quic,
            )?;
            let sockets = endpoint.local_addrs();
            assert_eq!(sockets.len(), 1);
            assert_eq!(sockets[0].ip(), addr.ip());
            endpoint.shutdown().await;
        }
        Ok(())
    }

    #[tokio::test]
    async fn occupied_listen_addr_fails_without_changing_ports() -> Result<(), BoxError> {
        let occupied = std::net::UdpSocket::bind(ZAKURA_LOOPBACK_BIND_V4)?;
        let addr = occupied.local_addr()?;
        let limits = ZakuraLocalLimits::from_config(&Config::default());
        assert!(QuicEndpoint::bind(
            LocalEndpointFactory::secret_key(90211),
            &limits.quic_bind_config(native_bind_addrs(Some(addr))),
            &limits.quic,
        )
        .is_err());
        Ok(())
    }

    #[tokio::test]
    async fn wildcard_bind_advertises_interface_addresses_with_the_bound_port() {
        let limits = ZakuraLocalLimits::from_config(&Config::default());
        let endpoint = QuicEndpoint::bind(
            LocalEndpointFactory::secret_key(90212),
            &limits.quic_bind_config(vec!["0.0.0.0:0".parse().expect("valid address")]),
            &limits.quic,
        )
        .expect("wildcard bind should succeed");
        let port = endpoint.local_addrs()[0].port();

        let addrs = local_direct_addrs(&endpoint);
        assert!(
            addrs.contains(&SocketAddr::from((Ipv4Addr::LOCALHOST, port))),
            "loopback is up on every test host, so it must be listed: {addrs:?}"
        );
        assert!(addrs
            .iter()
            .all(|addr| addr.is_ipv4() && !addr.ip().is_unspecified() && addr.port() == port));

        endpoint.shutdown().await;
    }

    #[test]
    fn discovery_uses_external_ip_with_the_native_listen_port() {
        let secret_key = NodeSecretKey::generate();
        let mut config = Config::default();
        config.zakura.listen_addr = Some("0.0.0.0:8234".parse().expect("test address parses"));
        config.external_addr = Some("203.0.113.42:8233".parse().expect("test address parses"));
        config.zakura.bootstrap_peers.clear();

        assert_eq!(
            discovery_direct_addrs(&config, secret_key.public()),
            vec!["203.0.113.42:8234".parse().expect("test address parses")]
        );
    }

    #[test]
    fn a_listener_that_peers_cannot_dial_is_reported() {
        let secret_key = NodeSecretKey::generate();
        let local_node_id = secret_key.public();
        let mut config = Config::default();
        config.zakura.bootstrap_peers.clear();
        let reported = |config: &Config| {
            let (_connected_tx, connected) = watch::channel(Vec::new());
            let discovery = discovery::build_discovery_handle(
                secret_key.clone(),
                None,
                discovery_direct_addrs(config, local_node_id),
                Vec::new(),
                &ZakuraHandshakeConfig::for_network_with_dev_cohort(&config.network, None),
                config.zakura.effective_max_connections(),
                0,
                connected,
            )
            .expect("the test self-record builds");
            advertises_no_discovery_address(
                config,
                &discovery.current_self_record().body.direct_addrs,
            )
        };

        config.zakura.listen_addr = None;
        config.external_addr = None;
        assert!(
            !reported(&config),
            "a node that does not listen advertises nothing"
        );

        config.zakura.listen_addr = Some("0.0.0.0:8234".parse().expect("test address parses"));
        assert!(
            reported(&config),
            "a wildcard listener advertises no address"
        );

        config.external_addr = Some("93.184.216.34:8233".parse().expect("test address parses"));
        assert!(!reported(&config));

        config.zakura.listen_addr = Some("10.0.0.5:8234".parse().expect("test address parses"));
        assert!(
            !reported(&config),
            "the record drops the private address and keeps the public one"
        );

        config.external_addr = None;
        assert!(reported(&config), "peers must not dial a private address");

        config.zakura.listen_addr = Some("0.0.0.0:0".parse().expect("test address parses"));
        config.external_addr = Some("93.184.216.34:8233".parse().expect("test address parses"));
        assert!(
            reported(&config),
            "peers dial the external IP at the listen port, so port 0 is unusable"
        );
    }

    #[test]
    fn discovery_uses_matching_local_bootstrap_address_and_counts_only_remote_peers() {
        let local_secret_key = NodeSecretKey::generate();
        let remote_secret_key = NodeSecretKey::generate();
        let local_node_id = local_secret_key.public();
        let remote_node_id = remote_secret_key.public();
        let local_entry = format!("{local_node_id}@198.51.100.7:8234");
        let remote_entry = format!("{remote_node_id}@198.51.100.8:8234");
        let mut config = Config::default();
        config.zakura.listen_addr = Some("0.0.0.0:8234".parse().expect("test address parses"));
        config.external_addr = None;
        config.zakura.bootstrap_peers = vec![
            local_entry,
            remote_entry.clone(),
            remote_entry,
            "malformed".to_string(),
        ];

        assert_eq!(
            discovery_direct_addrs(&config, local_node_id),
            vec!["198.51.100.7:8234".parse().expect("test address parses")]
        );
        assert_eq!(
            remote_bootstrap_peer_count(&config.zakura.bootstrap_peers, local_node_id),
            1
        );
    }

    #[derive(Debug, Clone)]
    struct CaptureConnection {
        alpn: &'static [u8],
        connection_tx: mpsc::Sender<Connection>,
        stream_tx: mpsc::Sender<(SendStream, RecvStream)>,
    }

    impl Acceptor for CaptureConnection {
        fn admit(&self, _incoming: &IncomingInfo) -> Admit {
            Admit::Accept
        }

        fn alpns(&self) -> Vec<Vec<u8>> {
            vec![self.alpn.to_vec()]
        }

        fn handle(&self, connection: Connection) -> BoxFuture<'static, ()> {
            let capture = self.clone();
            Box::pin(async move {
                let _ = capture.connection_tx.send(connection.clone()).await;
                for _ in 0..2 {
                    let Ok(streams) = connection.accept_bi().await else {
                        break;
                    };
                    let _ = capture.stream_tx.send(streams).await;
                }
            })
        }
    }

    #[derive(Debug)]
    struct NoopService;

    impl Service for NoopService {
        fn name(&self) -> &'static str {
            "noop"
        }

        fn streams(&self) -> &[Stream] {
            &[]
        }

        fn add_peer(&self, _peer: Peer) {}

        fn remove_peer(&self, _peer: &ZakuraPeerId, _conn_id: ZakuraConnId) {}
    }

    #[derive(Debug)]
    struct DeclaredStreamService {
        streams: Vec<Stream>,
    }

    impl Service for DeclaredStreamService {
        fn name(&self) -> &'static str {
            "declared-stream"
        }

        fn streams(&self) -> &[Stream] {
            &self.streams
        }

        fn add_peer(&self, _peer: Peer) {}

        fn remove_peer(&self, _peer: &ZakuraPeerId, _conn_id: ZakuraConnId) {}
    }

    #[derive(Debug)]
    struct OrderedStreamService {
        stream: Stream,
        sessions: mpsc::UnboundedSender<(FramedRecv, FramedSend)>,
    }

    impl Service for OrderedStreamService {
        fn name(&self) -> &'static str {
            "ordered-stream"
        }

        fn streams(&self) -> &[Stream] {
            std::slice::from_ref(&self.stream)
        }

        fn add_peer(&self, mut peer: Peer) {
            if let Some(session) = peer.take_stream(self.stream.kind) {
                let _ = self.sessions.send(session);
            }
        }

        fn remove_peer(&self, _peer: &ZakuraPeerId, _conn_id: ZakuraConnId) {}
    }

    #[derive(Debug)]
    struct GenerationGuardedRecordingService {
        streams: Vec<Stream>,
        active: std::sync::Mutex<HashMap<ZakuraPeerId, (ZakuraConnId, CancellationToken)>>,
        disconnected: std::sync::Mutex<Vec<(ZakuraPeerId, ZakuraConnId)>>,
    }

    impl GenerationGuardedRecordingService {
        fn new(streams: Vec<Stream>) -> Arc<Self> {
            Arc::new(Self {
                streams,
                active: std::sync::Mutex::new(HashMap::new()),
                disconnected: std::sync::Mutex::new(Vec::new()),
            })
        }

        fn active_conn(&self, peer: &ZakuraPeerId) -> Option<ZakuraConnId> {
            self.active
                .lock()
                .expect("recording service active map is never poisoned")
                .get(peer)
                .map(|(conn_id, _token)| *conn_id)
        }

        fn active_token_cancelled(&self, peer: &ZakuraPeerId) -> Option<bool> {
            self.active
                .lock()
                .expect("recording service active map is never poisoned")
                .get(peer)
                .map(|(_conn_id, token)| token.is_cancelled())
        }

        fn disconnected(&self) -> Vec<(ZakuraPeerId, ZakuraConnId)> {
            self.disconnected
                .lock()
                .expect("recording service disconnected list is never poisoned")
                .clone()
        }
    }

    impl Service for GenerationGuardedRecordingService {
        fn name(&self) -> &'static str {
            "generation-recording"
        }

        fn streams(&self) -> &[Stream] {
            &self.streams
        }

        fn add_peer(&self, peer: Peer) {
            let mut active = self
                .active
                .lock()
                .expect("recording service active map is never poisoned");
            if active
                .get(&peer.id)
                .is_some_and(|(active_conn_id, _token)| *active_conn_id > peer.conn_id)
            {
                peer.service_cancel_token().cancel();
                return;
            }
            if let Some((_old_conn_id, old_token)) =
                active.insert(peer.id.clone(), (peer.conn_id, peer.service_cancel_token()))
            {
                old_token.cancel();
            }
        }

        fn remove_peer(&self, peer: &ZakuraPeerId, conn_id: ZakuraConnId) {
            let removed = {
                let mut active = self
                    .active
                    .lock()
                    .expect("recording service active map is never poisoned");
                if active
                    .get(peer)
                    .is_some_and(|(active_conn_id, _token)| *active_conn_id == conn_id)
                {
                    active.remove(peer)
                } else {
                    None
                }
            };

            if let Some((_active_conn_id, token)) = removed {
                token.cancel();
                self.disconnected
                    .lock()
                    .expect("recording service disconnected list is never poisoned")
                    .push((peer.clone(), conn_id));
            }
        }
    }

    fn test_peer(byte: u8) -> ZakuraPeerId {
        ZakuraPeerId::new(vec![byte; 32]).expect("32-byte node id is valid")
    }

    fn test_conn_id() -> ZakuraConnId {
        static NEXT_TEST_CONN_ID: AtomicU64 = AtomicU64::new(1);
        NEXT_TEST_CONN_ID.fetch_add(1, Ordering::Relaxed)
    }

    fn registered_conn_id(registration: ZakuraRegistration) -> ZakuraConnId {
        match registration {
            ZakuraRegistration::Registered { conn_id, .. } => conn_id,
            other => panic!("expected peer registration to succeed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn handoff_wait_returns_exact_generation_and_rejects_duplicates() {
        let supervisor = ZakuraSupervisorHandle::new(1);
        let peer_id = test_peer(6);
        let ZakuraConnectionRegistrationWaitStart::Waiting(registration_wait) = supervisor
            .begin_connection_registration_wait(&peer_id)
            .await
        else {
            panic!("the first handoff must reserve the peer identity");
        };
        assert!(
            matches!(
                supervisor
                    .begin_connection_registration_wait(&peer_id)
                    .await,
                ZakuraConnectionRegistrationWaitStart::Duplicate,
            ),
            "a concurrent handoff cannot share one connection registration",
        );

        let (outbound_tx, _outbound_rx) = mpsc::channel(1);
        let registration = supervisor
            .register(
                test_conn_id(),
                peer_id.clone(),
                &[],
                Some("192.0.2.6".parse().expect("test IP parses")),
                [6; TRANSCRIPT_HASH_BYTES],
                ZakuraPeerHandle::new_for_tests(peer_id.clone(), outbound_tx),
                CancellationToken::new(),
                CloseCause::new(),
                ZAKURA_CAP_LEGACY_GOSSIP,
            )
            .await;
        let conn_id = registered_conn_id(registration);

        assert_eq!(
            registration_wait.wait(Duration::from_secs(1)).await,
            Some(conn_id),
        );
        assert!(
            matches!(
                supervisor
                    .begin_connection_registration_wait(&peer_id)
                    .await,
                ZakuraConnectionRegistrationWaitStart::Duplicate,
            ),
            "an incumbent connection makes a later legacy handoff a duplicate",
        );
    }

    #[test]
    fn native_duplicate_tie_breaker_converges_for_simultaneous_open() {
        let node_a = LocalEndpointFactory::secret_key(1).public();
        let node_b = LocalEndpointFactory::secret_key(2).public();
        let a_outbound =
            native_connection_transcript_hash(ServicePeerDirection::Outbound, &node_a, &node_b);
        let a_inbound =
            native_connection_transcript_hash(ServicePeerDirection::Inbound, &node_a, &node_b);
        let b_outbound =
            native_connection_transcript_hash(ServicePeerDirection::Outbound, &node_b, &node_a);
        let b_inbound =
            native_connection_transcript_hash(ServicePeerDirection::Inbound, &node_b, &node_a);

        assert_eq!(a_outbound, b_inbound);
        assert_eq!(a_inbound, b_outbound);
        assert_ne!(a_outbound, a_inbound);

        let winning_key = a_outbound.min(a_inbound);
        let losing_key = a_outbound.max(a_inbound);
        let peer = test_peer(7);
        let mut supervisor_a = ZakuraPeerSupervisor::default();
        let mut supervisor_b = ZakuraPeerSupervisor::default();
        assert!(matches!(
            supervisor_a.register_authenticated(peer.clone(), a_outbound),
            AuthenticatedPeerRegistration::Registered
        ));
        let _ = supervisor_a.register_authenticated(peer.clone(), a_inbound);
        assert!(matches!(
            supervisor_b.register_authenticated(peer.clone(), b_inbound),
            AuthenticatedPeerRegistration::Registered
        ));
        let _ = supervisor_b.register_authenticated(peer.clone(), b_outbound);

        assert!(matches!(
            supervisor_a.register_authenticated(peer.clone(), winning_key),
            AuthenticatedPeerRegistration::Duplicate
        ));
        assert!(matches!(
            supervisor_b.register_authenticated(peer.clone(), winning_key),
            AuthenticatedPeerRegistration::Duplicate
        ));
        assert!(matches!(
            supervisor_a.register_authenticated(peer.clone(), losing_key),
            AuthenticatedPeerRegistration::Duplicate
        ));
        assert!(matches!(
            supervisor_b.register_authenticated(peer, losing_key),
            AuthenticatedPeerRegistration::Duplicate
        ));
    }

    /// A disconnected peer leaves request selection at once, before its
    /// connection task deregisters it.
    #[tokio::test]
    async fn disconnected_peer_leaves_outbound_selection_before_cleanup() {
        let supervisor = ZakuraSupervisorHandle::new(1);
        let peer = test_peer(12);
        let (outbound_tx, _outbound_rx) = mpsc::channel(1);
        registered_conn_id(
            supervisor
                .register(
                    test_conn_id(),
                    peer.clone(),
                    &[],
                    Some("203.0.113.12".parse().expect("test ip parses")),
                    [0; TRANSCRIPT_HASH_BYTES],
                    ZakuraPeerHandle::new_for_tests(peer.clone(), outbound_tx),
                    CancellationToken::new(),
                    CloseCause::new(),
                    ZAKURA_CAP_LEGACY_GOSSIP,
                )
                .await,
        );
        assert_eq!(supervisor.outbound_peer_handles().await.len(), 1);

        assert!(supervisor.disconnect_peer(&peer).await);
        assert_eq!(supervisor.registered_ids().await, vec![peer]);
        assert!(supervisor.outbound_peer_handles().await.is_empty());
    }

    #[tokio::test]
    async fn upgraded_winner_registers_second_loser_cleanup_is_generation_guarded() {
        let supervisor = ZakuraSupervisorHandle::new(1);
        let peer = test_peer(11);
        let remote_ip: IpAddr = "203.0.113.11".parse().expect("test ip parses");
        let losing_trace_conn = 100;
        let winning_trace_conn = 1;
        let losing_hash = [0x80; TRANSCRIPT_HASH_BYTES];
        let winning_hash = [0x10; TRANSCRIPT_HASH_BYTES];
        let losing_token = CancellationToken::new();
        let winning_token = CancellationToken::new();

        async fn register_with_hash(
            supervisor: &ZakuraSupervisorHandle,
            conn_id: ZakuraConnId,
            peer: &ZakuraPeerId,
            remote_ip: IpAddr,
            transcript_hash: [u8; TRANSCRIPT_HASH_BYTES],
            token: CancellationToken,
        ) -> ZakuraRegistration {
            let (outbound_tx, _outbound_rx) = mpsc::channel(1);
            let outbound_handle = ZakuraPeerHandle::new_for_tests(peer.clone(), outbound_tx);
            supervisor
                .register(
                    conn_id,
                    peer.clone(),
                    &[],
                    Some(remote_ip),
                    transcript_hash,
                    outbound_handle,
                    token,
                    CloseCause::new(),
                    ZAKURA_CAP_LEGACY_GOSSIP | ZAKURA_CAP_HEADER_SYNC,
                )
                .await
        }

        let losing_conn = registered_conn_id(
            register_with_hash(
                &supervisor,
                losing_trace_conn,
                &peer,
                remote_ip,
                losing_hash,
                losing_token.clone(),
            )
            .await,
        );
        let winning_conn = registered_conn_id(
            register_with_hash(
                &supervisor,
                winning_trace_conn,
                &peer,
                remote_ip,
                winning_hash,
                winning_token.clone(),
            )
            .await,
        );
        assert!(
            winning_trace_conn < losing_trace_conn,
            "the test deliberately inverts raw trace-id order",
        );
        assert!(
            winning_conn > losing_conn,
            "service generations follow successful registration order",
        );

        assert!(
            losing_token.is_cancelled(),
            "upgraded winner explicitly cancels the incumbent loser",
        );
        assert!(
            !winning_token.is_cancelled(),
            "the winning replacement remains live",
        );

        supervisor.deregister(&peer, losing_conn).await;

        {
            let state = supervisor.inner.lock().await;
            let entry = state
                .active_by_peer
                .get(&peer)
                .expect("winner remains registered after loser cleanup");
            assert_eq!(entry.conn_id, winning_conn);
            assert_eq!(state.host(entry.conn_id), Some(remote_ip));
            assert_eq!(state.ip_slots.registered(remote_ip), 1);
            state.debug_assert_accounting();
        }
        assert_eq!(supervisor.registered_ids().await, vec![peer.clone()]);
        assert_eq!(supervisor.outbound_peer_handles().await.len(), 1);

        supervisor.deregister(&peer, winning_conn).await;
        let state = supervisor.inner.lock().await;
        assert!(state.active_by_peer.is_empty());
        assert_eq!(state.ip_slots.registered_total(), 0);
        state.debug_assert_accounting();
    }

    #[tokio::test]
    async fn upgraded_replacement_from_full_different_ip_is_rejected() {
        let supervisor = ZakuraSupervisorHandle::new(1);
        let peer_a = test_peer(21);
        let peer_b = test_peer(22);
        let ip1: IpAddr = "203.0.113.21".parse().expect("test ip parses");
        let ip2: IpAddr = "203.0.113.22".parse().expect("test ip parses");
        let peer_a_losing_conn = test_conn_id();
        let peer_b_conn = test_conn_id();
        let peer_a_winning_conn = test_conn_id();
        let peer_a_losing_hash = [0x80; TRANSCRIPT_HASH_BYTES];
        let peer_a_winning_hash = [0x10; TRANSCRIPT_HASH_BYTES];
        let peer_b_hash = [0x40; TRANSCRIPT_HASH_BYTES];
        let peer_a_losing_token = CancellationToken::new();
        let peer_b_token = CancellationToken::new();
        let peer_a_winning_token = CancellationToken::new();

        let peer_a_losing_generation = registered_conn_id(
            register_test_peer_with_hash_and_ip(
                &supervisor,
                peer_a_losing_conn,
                &peer_a,
                Some(ip1),
                peer_a_losing_hash,
                peer_a_losing_token.clone(),
            )
            .await,
        );
        let peer_b_generation = registered_conn_id(
            register_test_peer_with_hash_and_ip(
                &supervisor,
                peer_b_conn,
                &peer_b,
                Some(ip2),
                peer_b_hash,
                peer_b_token.clone(),
            )
            .await,
        );

        let registration = register_test_peer_with_hash_and_ip(
            &supervisor,
            peer_a_winning_conn,
            &peer_a,
            Some(ip2),
            peer_a_winning_hash,
            peer_a_winning_token.clone(),
        )
        .await;
        assert!(
            matches!(
                registration,
                ZakuraRegistration::Rejected(ZakuraRejectReason::ResourceLimit)
            ),
            "an upgraded same-peer replacement from a different full IP bucket must be rejected",
        );
        assert!(
            !peer_a_losing_token.is_cancelled(),
            "the incumbent stays live because the target IP bucket has no room",
        );
        assert!(
            !peer_b_token.is_cancelled(),
            "the unrelated peer that fills the target IP bucket stays live",
        );
        assert!(
            !peer_a_winning_token.is_cancelled(),
            "the rejected replacement was never registered",
        );

        let state = supervisor.inner.lock().await;
        assert_eq!(
            state
                .active_by_peer
                .get(&peer_a)
                .expect("peer A incumbent remains registered")
                .conn_id,
            peer_a_losing_generation
        );
        assert_eq!(
            state
                .active_by_peer
                .get(&peer_b)
                .expect("peer B remains registered")
                .conn_id,
            peer_b_generation
        );
        assert_eq!(state.ip_slots.registered(ip1), 1);
        assert_eq!(state.ip_slots.registered(ip2), 1);
        state.debug_assert_accounting();
    }

    #[tokio::test]
    async fn loser_cleanup_does_not_disconnect_upgraded_winner_service_session() {
        let supervisor = ZakuraSupervisorHandle::new(2);
        let peer = test_peer(23);
        let remote_ip: IpAddr = "203.0.113.23".parse().expect("test ip parses");
        let losing_conn = test_conn_id();
        let winning_conn = test_conn_id();
        let losing_hash = [0x80; TRANSCRIPT_HASH_BYTES];
        let winning_hash = [0x10; TRANSCRIPT_HASH_BYTES];
        let losing_token = CancellationToken::new();
        let winning_token = CancellationToken::new();

        let losing_conn = registered_conn_id(
            register_test_peer_with_hash_and_ip(
                &supervisor,
                losing_conn,
                &peer,
                Some(remote_ip),
                losing_hash,
                losing_token.clone(),
            )
            .await,
        );
        let winning_conn = registered_conn_id(
            register_test_peer_with_hash_and_ip(
                &supervisor,
                winning_conn,
                &peer,
                Some(remote_ip),
                winning_hash,
                winning_token.clone(),
            )
            .await,
        );
        assert!(
            winning_conn > losing_conn,
            "winner must have a later service generation",
        );

        let stream = Stream {
            kind: 61,
            version: 1,
            frame_cap: 1024,
            capability: ZAKURA_CAP_LEGACY_GOSSIP,
            ..Stream::PERSISTENT
        };
        let service = GenerationGuardedRecordingService::new(vec![stream]);
        let registry = Arc::new(
            ServiceRegistry::new(vec![service.clone()])
                .expect("test service declares a valid stream"),
        );
        let (_inbound_tx, inbound_rx) = crate::zakura::framed_channel(1);
        let (outbound_tx, _outbound_rx) = crate::zakura::framed_channel(1);
        let streams = HashMap::from([(stream.kind, (inbound_rx, outbound_tx))]);
        let winning_service_token = CancellationToken::new();

        registry.add_peer(Peer::new_with_conn_id_and_direction(
            winning_conn,
            peer.clone(),
            Some(remote_ip),
            ZAKURA_CAP_LEGACY_GOSSIP,
            ServicePeerDirection::Inbound,
            streams,
            winning_service_token.clone(),
        ));
        assert_eq!(service.active_conn(&peer), Some(winning_conn));

        let (_stale_inbound_tx, stale_inbound_rx) = crate::zakura::framed_channel(1);
        let (stale_outbound_tx, _stale_outbound_rx) = crate::zakura::framed_channel(1);
        let stale_streams = HashMap::from([(stream.kind, (stale_inbound_rx, stale_outbound_tx))]);
        let stale_service_token = CancellationToken::new();
        registry.add_peer(Peer::new_with_conn_id_and_direction(
            losing_conn,
            peer.clone(),
            Some(remote_ip),
            ZAKURA_CAP_LEGACY_GOSSIP,
            ServicePeerDirection::Inbound,
            stale_streams,
            stale_service_token.clone(),
        ));
        assert_eq!(
            service.active_conn(&peer),
            Some(winning_conn),
            "stale loser add must not overwrite the winner's service session",
        );
        assert!(
            matches!(service.active_token_cancelled(&peer), Some(false)),
            "stale loser add must not cancel the winner's service token",
        );

        registry.remove_peer(&peer, losing_conn, ZAKURA_CAP_LEGACY_GOSSIP);
        supervisor.deregister(&peer, losing_conn).await;

        assert_eq!(
            service.active_conn(&peer),
            Some(winning_conn),
            "stale loser cleanup must not remove the winner's service session",
        );
        assert!(
            service.disconnected().is_empty(),
            "no service disconnect should be emitted for the winner generation",
        );
        assert!(
            matches!(service.active_token_cancelled(&peer), Some(false)),
            "the winner's service session token must remain live",
        );

        {
            let state = supervisor.inner.lock().await;
            let entry = state
                .active_by_peer
                .get(&peer)
                .expect("winner remains registered after loser cleanup");
            assert_eq!(entry.conn_id, winning_conn);
            assert_eq!(state.ip_slots.registered(remote_ip), 1);
            state.debug_assert_accounting();
        }

        registry.remove_peer(&peer, winning_conn, ZAKURA_CAP_LEGACY_GOSSIP);
        supervisor.deregister(&peer, winning_conn).await;
    }

    #[tokio::test]
    async fn registered_peer_cleanup_guard_drop_deregisters_and_allows_redial() {
        let supervisor = ZakuraSupervisorHandle::new(2);
        let peer = test_peer(24);
        let transcript_hash = [0x24; TRANSCRIPT_HASH_BYTES];
        let disconnect_token = CancellationToken::new();
        let conn_id = registered_conn_id(
            register_test_peer_with_hash_and_ip(
                &supervisor,
                test_conn_id(),
                &peer,
                None,
                transcript_hash,
                disconnect_token.clone(),
            )
            .await,
        );

        let duplicate = register_test_peer_with_hash_and_ip(
            &supervisor,
            test_conn_id(),
            &peer,
            None,
            transcript_hash,
            CancellationToken::new(),
        )
        .await;
        assert!(
            matches!(duplicate, ZakuraRegistration::Duplicate { .. }),
            "an exact same-direction redial is a duplicate while the incumbent is registered",
        );

        let stream = Stream {
            kind: 62,
            version: 1,
            frame_cap: 1024,
            capability: ZAKURA_CAP_LEGACY_GOSSIP,
            ..Stream::PERSISTENT
        };
        let service = GenerationGuardedRecordingService::new(vec![stream]);
        let registry = Arc::new(
            ServiceRegistry::new(vec![service.clone()])
                .expect("test service declares a valid stream"),
        );
        let (_inbound_tx, inbound_rx) = crate::zakura::framed_channel(1);
        let (outbound_tx, _outbound_rx) = crate::zakura::framed_channel(1);
        registry.add_peer(Peer::new_with_conn_id_and_direction(
            conn_id,
            peer.clone(),
            None,
            ZAKURA_CAP_LEGACY_GOSSIP,
            ServicePeerDirection::Inbound,
            HashMap::from([(stream.kind, (inbound_rx, outbound_tx))]),
            disconnect_token.clone(),
        ));
        assert_eq!(service.active_conn(&peer), Some(conn_id));

        let mut cleanup_guard = RegisteredPeerCleanupGuard::new(
            supervisor.clone(),
            registry,
            peer.clone(),
            conn_id,
            disconnect_token.clone(),
        );
        cleanup_guard.add_admitted_capabilities(ZAKURA_CAP_LEGACY_GOSSIP);
        drop(cleanup_guard);

        assert!(
            disconnect_token.is_cancelled(),
            "drop cleanup cancels the registered connection token",
        );
        assert_eq!(
            service.active_conn(&peer),
            None,
            "drop cleanup removes admitted service state synchronously",
        );
        assert_eq!(service.disconnected(), vec![(peer.clone(), conn_id)]);

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if supervisor.registered_ids().await.is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("drop cleanup deregisters the supervisor entry promptly");

        let redial = register_test_peer_with_hash_and_ip(
            &supervisor,
            test_conn_id(),
            &peer,
            None,
            transcript_hash,
            CancellationToken::new(),
        )
        .await;
        assert!(
            matches!(redial, ZakuraRegistration::Registered { .. }),
            "after drop cleanup, the deterministic same-direction redial can register again",
        );
    }

    #[tokio::test]
    async fn registration_generation_exhaustion_rejects_without_mutating_state() {
        let supervisor = ZakuraSupervisorHandle::new(2);
        let peer = test_peer(25);

        supervisor
            .next_registration_id
            .store(u64::MAX, zakura_rt::sync::atomic::Ordering::Relaxed);

        let registration = register_test_peer_with_hash_and_ip(
            &supervisor,
            test_conn_id(),
            &peer,
            None,
            [0x25; TRANSCRIPT_HASH_BYTES],
            CancellationToken::new(),
        )
        .await;

        assert!(
            matches!(
                registration,
                ZakuraRegistration::Rejected(ZakuraRejectReason::TemporaryUnavailable)
            ),
            "exhausted registration generations reject the new connection instead of panicking",
        );
        assert!(
            supervisor.registered_ids().await.is_empty(),
            "failed registration must not mutate active supervisor state",
        );
    }

    /// fix-18: an operator who sets the default receive window keeps it as the
    /// handshake share; only an unset window takes the control share.
    #[test]
    fn a_set_receive_window_is_the_handshake_share_even_at_the_default() {
        let network = Network::Mainnet;
        let mut config = ZakuraConfig::default();
        assert_eq!(
            config.reactor_shares(&network).control_receive_bytes,
            CONTROL_RECEIVE_BYTES
        );
        config.quic.receive_window_bytes = Some(DEFAULT_ZAKURA_RECEIVE_WINDOW);
        assert_eq!(
            config.reactor_shares(&network).control_receive_bytes,
            u64::from(DEFAULT_ZAKURA_RECEIVE_WINDOW)
        );
    }

    /// The `[network.zakura.quic]` defaults must keep the transport values
    /// that regulation sizing and the stream-conformance properties assume.
    #[test]
    fn quic_config_defaults_match_the_zakura_transport_constants() {
        let quic = QuicConfig::default();
        assert_eq!(
            quic.stream_receive_window_bytes,
            DEFAULT_ZAKURA_STREAM_RECEIVE_WINDOW
        );
        assert_eq!(quic.receive_window(), DEFAULT_ZAKURA_RECEIVE_WINDOW);
        assert_eq!(quic.send_window_bytes, DEFAULT_ZAKURA_SEND_WINDOW);
        assert_eq!(quic.idle_timeout(), DEFAULT_ZAKURA_QUIC_IDLE_TIMEOUT);
        assert_eq!(
            quic.keep_alive_interval(),
            DEFAULT_ZAKURA_KEEP_ALIVE_INTERVAL
        );
    }

    #[test]
    fn ordered_stream_collision_winner_is_mirror_stable() {
        // Both endpoints compute the collision winner from the same node IDs.
        // Their answers must be complementary.
        // One endpoint keeps its opened stream.
        // The other endpoint adopts the peer's stream.
        let node_a = LocalEndpointFactory::secret_key(1).public();
        let node_b = LocalEndpointFactory::secret_key(2).public();
        assert_ne!(node_a, node_b);
        assert_ne!(
            i_open_collision_winner(&node_a, &node_b),
            i_open_collision_winner(&node_b, &node_a),
            "exactly one side must win a same-kind ordered-stream collision",
        );
        // The winner is deterministic: the lexicographically smaller node id.
        let a_wins = node_a.as_bytes() < node_b.as_bytes();
        assert_eq!(i_open_collision_winner(&node_a, &node_b), a_wins);
    }

    fn header_sync_test_session(peer: ZakuraPeerId) -> (PeerSession, crate::zakura::FramedRecv) {
        let (send, recv) = crate::zakura::framed_channel(32);
        (
            PeerSession::from_parts(peer, send, CancellationToken::new()),
            recv,
        )
    }

    fn header_sync_startup(shutdown: CancellationToken) -> HeaderSyncStartup {
        let network = Network::Mainnet;
        let anchor = (block::Height(0), network.genesis_hash());
        let mut startup = HeaderSyncStartup::new(
            network,
            anchor,
            FullStateFrontiers {
                finalized_height: anchor.0,
                verified_block_tip: anchor.0,
                verified_block_hash: anchor.1,
            },
            Some(anchor),
            ZakuraHeaderSyncConfig::default(),
            LOCAL_MAX_MESSAGE_BYTES,
            crate::zakura::DEFAULT_ZAKURA_MAX_CONNECTIONS,
        );
        startup.shutdown = shutdown;
        startup
    }

    async fn register_test_peer(
        supervisor: &ZakuraSupervisorHandle,
        peer: ZakuraPeerId,
        disconnect_token: CancellationToken,
    ) -> ZakuraConnId {
        let (outbound_tx, _outbound_rx) = mpsc::channel(1);
        let outbound_handle = ZakuraPeerHandle::new_for_tests(peer.clone(), outbound_tx);
        let registration = supervisor
            .register(
                test_conn_id(),
                peer.clone(),
                &[],
                None,
                [peer.as_bytes()[0]; TRANSCRIPT_HASH_BYTES],
                outbound_handle,
                disconnect_token,
                CloseCause::new(),
                ZAKURA_CAP_LEGACY_GOSSIP | ZAKURA_CAP_HEADER_SYNC,
            )
            .await;

        registered_conn_id(registration)
    }

    async fn register_test_peer_with_hash_and_ip(
        supervisor: &ZakuraSupervisorHandle,
        conn_id: ZakuraConnId,
        peer: &ZakuraPeerId,
        remote_ip: Option<IpAddr>,
        transcript_hash: [u8; TRANSCRIPT_HASH_BYTES],
        disconnect_token: CancellationToken,
    ) -> ZakuraRegistration {
        let (outbound_tx, _outbound_rx) = mpsc::channel(1);
        let outbound_handle = ZakuraPeerHandle::new_for_tests(peer.clone(), outbound_tx);
        supervisor
            .register(
                conn_id,
                peer.clone(),
                &[],
                remote_ip,
                transcript_hash,
                outbound_handle,
                disconnect_token,
                CloseCause::new(),
                ZAKURA_CAP_LEGACY_GOSSIP | ZAKURA_CAP_HEADER_SYNC,
            )
            .await
    }

    #[test]
    fn local_limits_clamp_negotiated_values_down() {
        let config = Config::default();
        let limits = ZakuraLocalLimits::from_config(&config);
        let negotiated = ZakuraAcceptedLimits {
            max_frame_bytes: u32::MAX,
            max_message_bytes: u32::MAX,
            max_open_streams: u16::MAX,
            max_inbound_queue_depth: u16::MAX,
            idle_timeout_millis: u32::MAX,
        };

        let clamped = limits.clamp(&negotiated);

        assert_eq!(clamped.max_frame_bytes, limits.max_frame_bytes);
        assert_eq!(clamped.max_message_bytes, limits.max_message_bytes);
        assert_eq!(clamped.max_open_streams, limits.max_open_streams);
        assert_eq!(
            clamped.max_inbound_queue_depth,
            limits.max_inbound_queue_depth
        );
        assert!(clamped.idle_timeout < limits.quic_idle_timeout);
    }

    #[test]
    fn the_largest_valid_cwnd_cap_derives_a_bindable_stream_window() {
        let mut config = Config::default();
        let fixed = super::super::BLOCK_SYNC_REACTOR_BUDGET.receive_total()
            - config.zakura.block_sync.bbr_max_cwnd_bytes;
        config.zakura.block_sync.bbr_max_cwnd_bytes =
            zakura_quic::MAX_STREAM_RECEIVE_WINDOW_BYTES - fixed;
        assert_eq!(config.zakura.block_sync.validate(), Ok(()));

        let limits = ZakuraLocalLimits::from_config(&config);

        assert_eq!(
            u64::from(limits.quic.stream_receive_window_bytes),
            zakura_quic::MAX_STREAM_RECEIVE_WINDOW_BYTES
        );
        assert_eq!(limits.quic.validate(), Ok(()));
    }

    #[test]
    fn inbound_queue_depth_is_split_across_ordered_streams() {
        assert_eq!(per_stream_inbound_queue_depth(64, 2), 32);
        assert_eq!(per_stream_inbound_queue_depth(63, 2), 31);
        assert_eq!(per_stream_inbound_queue_depth(64, 0), 64);
        assert!(per_stream_inbound_queue_depth(63, 2) * 2 <= 63);
    }

    #[test]
    fn request_response_only_peers_do_not_use_ordered_stream_freshness_reaper() {
        assert!(!should_run_freshness_reaper(0, 1));
        assert!(should_run_freshness_reaper(1, 1));
        assert!(should_run_freshness_reaper(1, 0));
        assert!(should_run_freshness_reaper(0, 0));
    }

    /// Teardown's drain has one deadline for each wait. Workers that ignore
    /// cancellation and end one after another, each before a fresh
    /// per-worker timeout would fire, can't hold the connection's admission
    /// permit past twice the drain timeout, however many there are.
    #[tokio::test(start_paused = true)]
    async fn trickling_workers_cannot_extend_the_teardown_drain() {
        let step = STREAM_WORKER_DRAIN_TIMEOUT * 9 / 10;
        for count in [4, 64] {
            let admission = Arc::new(Semaphore::new(1));
            let permit = admission
                .clone()
                .try_acquire_owned()
                .expect("the test takes the only permit");
            let mut workers = JoinSet::new();
            for worker in 1..=count {
                workers.spawn(zakura_rt::time::sleep(step * worker));
            }
            let cancelled = Instant::now();
            let teardown = tokio::spawn(async move {
                let drained = drain_stream_workers(&mut workers).await;
                drop(permit);
                (drained, workers.len())
            });

            let _returned = admission.acquire().await.expect("the test never closes it");
            let held = cancelled.elapsed();
            assert!(
                held <= 2 * STREAM_WORKER_DRAIN_TIMEOUT,
                "{count} trickling workers held the connection slot for {held:?}"
            );
            let (drained, left) = teardown.await.expect("the teardown task never panics");
            assert!(!drained, "the later workers outlast the first wait");
            assert_eq!(left, 0, "the abort ends every sleeping worker");
        }
    }

    #[tokio::test]
    async fn v2_p2p_false_leaves_header_sync_disabled() -> Result<(), BoxError> {
        let _guard = zakura_test::init();
        let config = Config::for_test(P2pStack::Legacy);

        let endpoint = spawn_zakura_endpoint(&config, |_supervisor, _trace| {
            Arc::new(NoopService) as Arc<dyn Service>
        })
        .await?;

        assert!(endpoint.is_none());
        Ok(())
    }

    /// An endpoint started without the `tx-relay` service isn't charged for
    /// its shares, though `[network.zakura.tx_relay] enabled` defaults to true.
    #[tokio::test]
    async fn an_endpoint_without_tx_relay_is_not_charged_for_it() -> Result<(), BoxError> {
        let _guard = zakura_test::init();
        let mut config = Config::for_test(P2pStack::Dual);
        config.zakura.bootstrap_peers = Vec::new();
        assert!(config.zakura.tx_relay.enabled);

        let endpoint = spawn_zakura_endpoint(&config, |_supervisor, _trace| {
            Arc::new(NoopService) as Arc<dyn Service>
        })
        .await?
        .expect("a dual stack starts the v2 endpoint");

        let plan = endpoint
            .memory_plan()
            .expect("the endpoint sizes a memory plan at startup");
        assert_eq!(
            plan.bases.tx_relay,
            super::super::memory::ReactorBase::default()
        );
        Ok(())
    }

    #[tokio::test]
    async fn an_unfundable_block_sync_config_fails_before_the_endpoint_binds() {
        let _guard = zakura_test::init();
        let mut config = Config::for_test(P2pStack::Dual);
        config.zakura.block_sync.max_reorder_lookahead_bytes = 0;

        let error = spawn_zakura_endpoint(&config, |_supervisor, _trace| {
            Arc::new(NoopService) as Arc<dyn Service>
        })
        .await
        .expect_err("an unfundable block-sync config must not start the endpoint");
        assert!(
            error
                .to_string()
                .contains("invalid zakura.block_sync config"),
            "unexpected error: {error}",
        );
    }

    /// A `Config` built in code skips config-load validation. The node's
    /// limits replace its stream window with the largest reactor share, so a
    /// window below a block-sync frame still starts the endpoint, and no
    /// native frame exceeds its share.
    #[tokio::test]
    async fn a_programmatic_stream_window_takes_the_reactor_shares() {
        let _guard = zakura_test::init();
        let mut config = Config::for_test(P2pStack::Dual);
        config.zakura.quic.stream_receive_window_bytes = 64 * 1024;

        let limits = ZakuraLocalLimits::from_config(&config);
        assert_eq!(
            u64::from(limits.quic.stream_receive_window_bytes),
            limits.reactor_shares().stream_receive_window()
        );
        let endpoint = spawn_zakura_endpoint(&config, |_supervisor, _trace| {
            Arc::new(NoopService) as Arc<dyn Service>
        })
        .await
        .expect("the reactor shares fund every native frame");
        drop(endpoint);
    }

    /// Startup refuses a bootstrap identity whose dial would need more slots
    /// than the dial reserve (8 at the defaults), counting only the
    /// addresses the bound endpoint would dial.
    #[tokio::test]
    async fn startup_refuses_a_bootstrap_dial_wider_than_the_reserve() {
        let _guard = zakura_test::init();
        let remote = NodeSecretKey::generate().public();
        let config_with = |addrs: &[String]| {
            let mut config = Config::for_test(P2pStack::Zakura);
            config.zakura.bootstrap_peers = addrs
                .iter()
                .map(|addr| format!("{remote}@{addr}"))
                .collect();
            config
        };
        let v4 = |port: u16| format!("127.0.0.1:{port}");
        let mapped = |port: u16| format!("[::ffff:127.0.0.1]:{port}");
        let spawn = |config: Config| async move {
            spawn_zakura_endpoint(&config, |_supervisor, _trace| {
                Arc::new(NoopService) as Arc<dyn Service>
            })
            .await
        };

        let nine = (8233..8242).map(v4).collect::<Vec<_>>();
        let error = spawn(config_with(&nine))
            .await
            .expect_err("a nine-address dial can't fit the eight-slot reserve");
        assert!(
            error.to_string().contains("zakura.bootstrap_peers"),
            "unexpected error: {error}",
        );

        // Sixteen configured addresses, but the mapped ones fold into their
        // IPv4 forms, so the dial makes eight attempts.
        let folded = (8233..8241)
            .flat_map(|port| [v4(port), mapped(port)])
            .collect::<Vec<_>>();
        let endpoint = spawn(config_with(&folded))
            .await
            .expect("eight attempts fit the reserve");
        drop(endpoint);
    }

    /// Config load checks only native kinds, so startup must refuse a custom
    /// kind whose frame exceeds its share.
    #[tokio::test]
    async fn a_custom_frame_above_its_share_fails_startup_and_names_the_kind() {
        const CUSTOM_STREAM: Stream = Stream {
            kind: 64,
            version: 1,
            frame_cap: LOCAL_MAX_MESSAGE_BYTES,
            capability: 1 << 16,
            ..Stream::PERSISTENT
        };

        let _guard = zakura_test::init();
        let mut config = Config::for_test(P2pStack::Dual);
        config.zakura.quic.stream_receive_window_bytes = MAX_BS_FRAME_BYTES;
        config
            .zakura
            .validate_stream_receive_window()
            .expect("the window holds every native frame");
        let (sessions, _session_rx) = mpsc::unbounded_channel();

        let error = spawn_zakura_endpoint_with_services(
            &config,
            |_supervisor, _trace| Arc::new(NoopService),
            None,
            vec![CustomService {
                service: Arc::new(OrderedStreamService {
                    stream: CUSTOM_STREAM,
                    sessions,
                }),
                provides: Vec::new(),
                seeks: Vec::new(),
                tx_relay_sessions: None,
            }],
        )
        .await
        .expect_err("a custom frame above its share must not start the endpoint");
        assert!(
            error.to_string().contains("stream kind 64's receive share"),
            "unexpected error: {error}",
        );
    }

    #[tokio::test]
    async fn v2_p2p_true_starts_header_sync_handle() -> Result<(), BoxError> {
        let _guard = zakura_test::init();
        let config = Config::for_test(P2pStack::Dual);

        let endpoint = spawn_zakura_endpoint(&config, |_supervisor, _trace| {
            Arc::new(NoopService) as Arc<dyn Service>
        })
        .await?
        .expect("v2_p2p is enabled in test config");

        assert!(endpoint.header_sync().is_some());
        endpoint.shutdown().await;
        Ok(())
    }

    /// A full transport stops discovery dials even while connection slots
    /// remain, so the dialer can't spin on `ConnectionLimit` (zakura-quic
    /// ADM-12).
    #[tokio::test]
    async fn a_full_transport_reports_no_dial_capacity() -> Result<(), BoxError> {
        let _guard = zakura_test::init();
        let mut config = Config::for_test(P2pStack::Dual);
        // No bootstrap dials, so nothing else takes a connection slot.
        config.zakura.bootstrap_peers = Vec::new();
        config.zakura.max_connections = Some(2);
        config.zakura.max_pending_handshakes = 1;
        let endpoint = spawn_zakura_endpoint(&config, |_supervisor, _trace| {
            Arc::new(NoopService) as Arc<dyn Service>
        })
        .await?
        .expect("v2_p2p is enabled in test config");
        assert!(endpoint.discovery_admission_capacity() > 0);

        // Dials to a socket that never answers hold every live slot: two
        // ordinary connections, the held gateway places and one handshake.
        let live = 2 + crate::zakura::MAX_PREFERRED_GATEWAYS + 1;
        let silent = std::net::UdpSocket::bind("127.0.0.1:0")?;
        let target = NodeAddr::with_addrs(
            zakura_quic::NodeSecretKey::generate().public(),
            vec![silent.local_addr()?],
        );
        let mut dials = Vec::new();
        for _ in 0..live {
            let (quic, target) = (endpoint.quic.clone(), target.clone());
            dials.push(tokio::spawn(async move {
                quic.connect(target, &[P2P_V2_ALPN]).await
            }));
        }
        while endpoint.quic.connection_count() < live {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        assert!(endpoint.handler.free_connection_slots() > 0);
        assert_eq!(endpoint.discovery_admission_capacity(), 0);
        for dial in dials {
            dial.abort();
        }
        endpoint.shutdown().await;
        Ok(())
    }

    /// Only limits a dial hits before sending a packet are local: the
    /// pending-handshake limit follows a completed QUIC handshake with the
    /// peer, so redials must count it.
    #[test]
    fn only_limits_before_the_first_packet_are_pre_dial() {
        assert!(ZakuraHandlerError::ResourceLimit(ADMISSION_LIMIT).is_pre_dial_limit());
        assert!(ZakuraHandlerError::ResourceLimit(TRANSPORT_CONNECTIONS_LIMIT).is_pre_dial_limit());
        assert!(!ZakuraHandlerError::ResourceLimit("pending handshake").is_pre_dial_limit());
        assert!(!ZakuraHandlerError::Timeout("native dial").is_pre_dial_limit());
    }

    #[tokio::test]
    async fn endpoint_shutdown_stops_header_sync_task() -> Result<(), BoxError> {
        let _guard = zakura_test::init();
        let config = Config::for_test(P2pStack::Dual);
        let endpoint = spawn_zakura_endpoint(&config, |_supervisor, _trace| {
            Arc::new(NoopService) as Arc<dyn Service>
        })
        .await?
        .expect("v2_p2p is enabled in test config");
        let header_sync = endpoint
            .header_sync()
            .expect("header sync handle exists for a v2 endpoint");

        endpoint.shutdown().await;

        let (session, _recv) = header_sync_test_session(
            ZakuraPeerId::new(vec![5u8; 32]).expect("32-byte node id is valid"),
        );
        let send_result = tokio::time::timeout(
            Duration::from_secs(1),
            header_sync.send(Event::PeerConnected(session)),
        )
        .await
        .expect("send returns promptly after header-sync shutdown");
        assert!(send_result.is_err());

        Ok(())
    }

    /// A maintained native dial loop (shared by configured bootstrap peers, the
    /// legacy->Zakura upgrade hand-off, and `spawn_native_dial`) targets an
    /// unreachable peer, so its `maintain` policy retries forever and never
    /// exits on its own. `ZakuraEndpoint::shutdown` must still stop it: the task
    /// holds an endpoint clone, so the supervisor registration watch stays open
    /// and only the endpoint shutdown token can end the loop. Without that
    /// signal the detached loop outlives shutdown and keeps dialing.
    #[tokio::test]
    async fn endpoint_shutdown_stops_maintained_native_dial_loop() -> Result<(), BoxError> {
        let _guard = zakura_test::init();
        let config = Config::for_test(P2pStack::Dual);
        let endpoint = spawn_zakura_endpoint(&config, |_supervisor, _trace| {
            Arc::new(NoopService) as Arc<dyn Service>
        })
        .await?
        .expect("v2_p2p is enabled in test config");

        // 192.0.2.0/24 is TEST-NET-1 (RFC 5737): guaranteed unreachable, so the
        // maintained loop stays in connect/backoff and never finishes on its own.
        let unreachable_addr: SocketAddr = "192.0.2.1:65535".parse().expect("valid test address");
        let unreachable = NodeAddr::with_addrs(
            LocalEndpointFactory::secret_key(987_654).public(),
            [unreachable_addr],
        );
        let dial = endpoint.spawn_native_dial(unreachable);

        // Let the maintained loop start before tearing the endpoint down.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !dial.is_finished(),
            "maintained dial loop to an unreachable peer should still be running"
        );

        endpoint.shutdown().await;

        tokio::time::timeout(Duration::from_secs(5), dial)
            .await
            .expect("maintained native dial loop must exit after endpoint shutdown")
            .expect("native dial task must not panic");
        Ok(())
    }

    /// The discovery candidate dialer is a long-lived loop that holds an
    /// endpoint clone, so its supervisor registration watch never closes on its
    /// own. `ZakuraEndpoint::shutdown` must still stop it via the endpoint
    /// shutdown token; otherwise it outlives teardown, polling the discovery
    /// book and attempting dials against a torn-down router.
    #[tokio::test]
    async fn endpoint_shutdown_stops_discovery_candidate_dialer() -> Result<(), BoxError> {
        let _guard = zakura_test::init();
        let config = Config::for_test(P2pStack::Dual);
        let endpoint = spawn_zakura_endpoint(&config, |_supervisor, _trace| {
            Arc::new(NoopService) as Arc<dyn Service>
        })
        .await?
        .expect("v2_p2p is enabled in test config");

        let limits = ZakuraLocalLimits::from_config(&config);
        let handshake = ZakuraHandshakeConfig::for_network(&config.network);
        let discovery = crate::zakura::discovery::build_discovery_handle(
            NodeSecretKey::generate(),
            None,
            Vec::new(),
            crate::zakura::discovery::default_advertised_services(),
            &handshake,
            limits.max_connections,
            0,
            endpoint.supervisor().subscribe(),
        )?;
        let dialer = crate::zakura::discovery::spawn_native_discovery_dialer(
            endpoint.clone(),
            discovery,
            limits,
            Vec::new(),
            None,
        );

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !dialer.is_finished(),
            "discovery candidate dialer should still be running"
        );

        endpoint.shutdown().await;

        tokio::time::timeout(Duration::from_secs(5), dialer)
            .await
            .expect("discovery candidate dialer must exit after endpoint shutdown")
            .expect("discovery dialer task must not panic");
        Ok(())
    }

    /// A malicious legacy responder can return a syntactically valid
    /// `P2pV2UpgradeAccept` (a unique, real iroh node id with a parseable but
    /// unreachable direct address) that never completes native Zakura
    /// registration. After the upgrade hand-off wait times out, no
    /// maintain-forever native dial task or `upgrade_dials` entry may survive
    /// its in-flight attempt;
    /// otherwise repeating the failed upgrade with distinct node ids grows
    /// maintained dials and outbound QUIC traffic without bound.
    ///
    /// Regression test for `claude-legacy-upgrade-maintained-dial-leak`. Uses a
    /// paused clock so the 15s appear-timeout elapses without real waiting; the
    /// dial target is RFC 5737 TEST-NET-1, which never connects.
    #[tokio::test(start_paused = true)]
    async fn failed_legacy_upgrade_does_not_leak_maintained_dial() -> Result<(), BoxError> {
        let _guard = zakura_test::init();
        let config = Config::for_test(P2pStack::Dual);
        let endpoint = spawn_zakura_endpoint(&config, |_supervisor, _trace| {
            Arc::new(NoopService) as Arc<dyn Service>
        })
        .await?
        .expect("v2_p2p is enabled in test config");

        // The Accept the attacker would advertise: a real 32-byte node id
        // (so `node_addr_from_hints` builds a `NodeAddr` and the dial spawns)
        // pointing at an unreachable address that never registers.
        let node_id = LocalEndpointFactory::secret_key(0x0BAD_C0DE)
            .public()
            .as_bytes()
            .to_vec();
        let peer_id = ZakuraPeerId::new(node_id.clone()).expect("32-byte node id is valid");
        let direct_addresses = vec![b"192.0.2.1:1".to_vec()];

        let connector =
            crate::zakura::ZakuraHandshakeConnector::new_with_endpoint(endpoint.clone());
        let handoff = connector
            .spawn_zakura_dial_to_hints_and_wait(&peer_id, &node_id, &direct_addresses)
            .await;

        assert!(
            matches!(handoff, crate::zakura::ZakuraNativeHandoff::Failed),
            "an unreachable upgrade peer must not report a completed hand-off",
        );
        // The cancelled dial starts no new attempt, so its in-flight one is
        // the last.
        upgrade_dials_end(&endpoint).await;

        endpoint.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn maintained_upgrade_dial_keeps_its_original_handoff_owner() -> Result<(), BoxError> {
        let _guard = zakura_test::init();
        let config = Config::for_test(P2pStack::Dual);
        let endpoint = spawn_zakura_endpoint(&config, |_supervisor, _trace| {
            Arc::new(NoopService) as Arc<dyn Service>
        })
        .await?
        .expect("v2_p2p is enabled in test config");
        let node_id = LocalEndpointFactory::secret_key(0x0BAD_CAFE).public();
        let peer_id = ZakuraPeerId::new(node_id.as_bytes().to_vec())?;
        let node_addr = NodeAddr::with_addrs(
            node_id,
            ["192.0.2.2:1".parse().expect("test direct address parses")],
        );

        assert_eq!(
            endpoint.start_upgrade_native_dial(node_addr.clone()),
            crate::zakura::ZakuraUpgradeDialStart::Started,
        );
        assert_eq!(
            endpoint.start_upgrade_native_dial(node_addr),
            crate::zakura::ZakuraUpgradeDialStart::AlreadyRunning,
        );

        endpoint.cancel_upgrade_native_dial(&peer_id);
        endpoint.shutdown().await;
        Ok(())
    }

    /// Spawns a Zakura endpoint, and a loopback QUIC peer that serves an ALPN
    /// no Zakura node offers. Returns both and the peer's hint.
    async fn endpoint_and_other_cohort(
        config: Config,
        seed: u64,
    ) -> Result<(ZakuraEndpoint, QuicEndpoint, NodeAddr), BoxError> {
        let endpoint = spawn_zakura_endpoint(&config, |_supervisor, _trace| {
            Arc::new(NoopService) as Arc<dyn Service>
        })
        .await?
        .expect("v2_p2p is enabled in test config");
        let other_cohort = LocalEndpointFactory::new().endpoint(seed).await?;
        let (connection_tx, _connection_rx) = mpsc::channel(1);
        let (stream_tx, _stream_rx) = mpsc::channel(1);
        other_cohort.serve(CaptureConnection {
            alpn: b"/zakura/another-cohort/0",
            connection_tx,
            stream_tx,
        })?;
        let hint = LocalEndpointFactory::node_addr(&other_cohort).await;
        Ok((endpoint, other_cohort, hint))
    }

    /// Waits until `endpoint` owns no upgrade dial. A cancelled dial ends
    /// within the bound of its in-flight attempt.
    async fn upgrade_dials_end(endpoint: &ZakuraEndpoint) {
        timeout(Duration::from_secs(30), async {
            while endpoint.upgrade_dial_count() > 0 {
                zakura_rt::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the upgrade dial ends within one bounded attempt");
    }

    /// An upgrade dial to a peer-supplied address that speaks another ALPN
    /// ends at once, and no upgrade dial to the same hint starts again until
    /// the ten-minute floor ends (zakura-quic DIAL-5, V12 F-562, F-563). The
    /// node ID alone is unauthenticated, so the same ID at other addresses
    /// still dials.
    #[tokio::test]
    async fn alpn_mismatched_upgrade_dial_ends_and_holds_the_hint_floor() -> Result<(), BoxError> {
        let _guard = zakura_test::init();
        let (endpoint, _other_cohort, hint) =
            endpoint_and_other_cohort(Config::for_test(P2pStack::Dual), 0x0A17_0001).await?;
        let peer_id = ZakuraPeerId::new(hint.id.as_bytes().to_vec())?;

        assert_eq!(
            endpoint.start_upgrade_native_dial(hint.clone()),
            crate::zakura::ZakuraUpgradeDialStart::Started,
        );
        upgrade_dials_end(&endpoint).await;

        tokio::time::pause();
        let reordered = NodeAddr::with_addrs(hint.id, hint.direct.iter().rev().copied());
        assert_eq!(
            endpoint.start_upgrade_native_dial(reordered),
            crate::zakura::ZakuraUpgradeDialStart::AlpnBackoff,
        );
        tokio::time::advance(ZAKURA_ALPN_MISMATCH_BACKOFF - Duration::from_secs(1)).await;
        assert_eq!(
            endpoint.start_upgrade_native_dial(hint.clone()),
            crate::zakura::ZakuraUpgradeDialStart::AlpnBackoff,
        );
        let planted = NodeAddr::with_addrs(
            hint.id,
            ["192.0.2.3:1".parse().expect("test direct address parses")],
        );
        assert_eq!(
            endpoint.start_upgrade_native_dial(planted),
            crate::zakura::ZakuraUpgradeDialStart::Started,
            "a floor from one hint blocked the same node ID at other addresses",
        );
        endpoint.cancel_upgrade_native_dial(&peer_id);
        upgrade_dials_end(&endpoint).await;
        tokio::time::advance(Duration::from_secs(2)).await;
        assert_eq!(
            endpoint.start_upgrade_native_dial(hint),
            crate::zakura::ZakuraUpgradeDialStart::Started,
        );

        endpoint.cancel_upgrade_native_dial(&peer_id);
        endpoint.shutdown().await;
        Ok(())
    }

    /// A hand-off cancelled while a silent address delays the dial still
    /// starts the floor when another address of the hint mismatches.
    #[tokio::test]
    async fn cancelled_upgrade_dial_still_holds_a_delayed_alpn_floor() -> Result<(), BoxError> {
        let _guard = zakura_test::init();
        let mut config = Config::for_test(P2pStack::Dual);
        config.zakura.quic.handshake_timeout_secs = Some(2);
        let (endpoint, _other_cohort, hint) =
            endpoint_and_other_cohort(config, 0x0A17_0002).await?;
        let peer_id = ZakuraPeerId::new(hint.id.as_bytes().to_vec())?;
        // A bound socket that never answers holds its attempt to the
        // handshake deadline.
        let silent = std::net::UdpSocket::bind("127.0.0.1:0")?;
        let hint = NodeAddr::with_addrs(
            hint.id,
            std::iter::once(silent.local_addr()?).chain(hint.direct.iter().copied()),
        );

        assert_eq!(
            endpoint.start_upgrade_native_dial(hint.clone()),
            crate::zakura::ZakuraUpgradeDialStart::Started,
        );
        // The legacy handshake timeout drops the hand-off before the silent
        // attempt ends.
        zakura_rt::time::sleep(Duration::from_millis(500)).await;
        endpoint.cancel_upgrade_native_dial(&peer_id);
        upgrade_dials_end(&endpoint).await;

        assert_eq!(
            endpoint.start_upgrade_native_dial(hint),
            crate::zakura::ZakuraUpgradeDialStart::AlpnBackoff,
            "cancelling the hand-off discarded the mismatch",
        );
        endpoint.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn custom_ordered_service_round_trips_over_zakura() -> Result<(), BoxError> {
        const CUSTOM_STREAM: Stream = Stream {
            kind: 64,
            version: 1,
            frame_cap: 64 * 1024,
            capability: 1 << 16,
            ..Stream::PERSISTENT
        };

        let _guard = zakura_test::init();
        let service_id = ZakuraServiceId::new("zakura.test.ordered.v1")?;
        let listen_addr = "127.0.0.1:0".parse().expect("valid loopback address");
        let server_identity = tempfile::tempdir()?;
        let client_identity = tempfile::tempdir()?;
        let (server_sessions, mut server_session_rx) = mpsc::unbounded_channel();
        let (client_sessions, mut client_session_rx) = mpsc::unbounded_channel();

        let mut server_config = Config::for_test(P2pStack::Dual);
        server_config.identity_dir = server_identity.path().to_owned();
        server_config.zakura.listen_addr = Some(listen_addr);
        server_config.zakura.bootstrap_peers.clear();
        let server = spawn_zakura_endpoint_with_services(
            &server_config,
            |_supervisor, _trace| Arc::new(NoopService),
            None,
            vec![CustomService {
                service: Arc::new(OrderedStreamService {
                    stream: CUSTOM_STREAM,
                    sessions: server_sessions,
                }),
                provides: vec![service_id.clone()],
                seeks: Vec::new(),
                tx_relay_sessions: None,
            }],
        )
        .await?
        .expect("test server uses Zakura");
        let server_addr = server.node_addr().await;
        let server_direct = server_addr
            .direct
            .iter()
            .copied()
            .find(|addr| addr.ip().is_loopback())
            .ok_or("test server has no loopback address")?;

        let mut client_config = Config::for_test(P2pStack::Dual);
        client_config.identity_dir = client_identity.path().to_owned();
        client_config.zakura.listen_addr = Some(listen_addr);
        client_config.zakura.bootstrap_peers = vec![format!("{}@{server_direct}", server_addr.id)];
        let client = spawn_zakura_endpoint_with_services(
            &client_config,
            |_supervisor, _trace| Arc::new(NoopService),
            None,
            vec![CustomService {
                service: Arc::new(OrderedStreamService {
                    stream: CUSTOM_STREAM,
                    sessions: client_sessions,
                }),
                provides: Vec::new(),
                seeks: vec![service_id],
                tx_relay_sessions: None,
            }],
        )
        .await?
        .expect("test client uses Zakura");

        let round_trip = timeout(Duration::from_secs(10), async {
            let (mut server_recv, server_send) = server_session_rx
                .recv()
                .await
                .ok_or_else(|| -> BoxError { "server custom stream did not open".into() })?;
            let (mut client_recv, client_send) = client_session_rx
                .recv()
                .await
                .ok_or_else(|| -> BoxError { "client custom stream did not open".into() })?;
            let outbound = Frame {
                message_type: 1_001,
                flags: 3,
                payload: b"custom client frame".to_vec(),
            };
            client_send.send(outbound.clone()).await?;
            let received = server_recv
                .recv()
                .await
                .ok_or_else(|| -> BoxError { "server custom stream closed".into() })?;
            assert_eq!(received, outbound);

            let response = Frame {
                message_type: 2_002,
                flags: 5,
                payload: b"custom server frame".to_vec(),
            };
            server_send.send(response.clone()).await?;
            let received = client_recv
                .recv()
                .await
                .ok_or_else(|| -> BoxError { "client custom stream closed".into() })?;
            assert_eq!(received, response);
            Ok::<_, BoxError>(())
        })
        .await
        .map_err(|_| -> BoxError { "custom ordered stream round trip timed out".into() })?;
        client.shutdown().await;
        server.shutdown().await;
        round_trip
    }

    #[tokio::test]
    async fn lazy_escalation_service_demand_respects_directional_caps() -> Result<(), BoxError> {
        let _guard = zakura_test::init();
        let shutdown = CancellationToken::new();
        let mut startup = header_sync_startup(shutdown.clone());
        startup.config.peer_limits = ServicePeerLimits {
            max_inbound_peers: Some(0),
            max_outbound_peers: Some(1),
            ..ServicePeerLimits::default()
        };
        let (header_sync, _actions, reactor_task) = spawn_header_sync_reactor(startup)?;
        let service = HeaderSyncService::new(header_sync);
        let peer = test_peer(17);

        assert!(
            !service.wants_peer(&peer, ZAKURA_CAP_HEADER_SYNC, ServicePeerDirection::Inbound,),
            "a full inbound cap must not open a new header-sync stream"
        );
        assert!(
            service.wants_peer(
                &peer,
                ZAKURA_CAP_HEADER_SYNC,
                ServicePeerDirection::Outbound,
            ),
            "an outbound slot should still allow lazy header-sync escalation"
        );

        shutdown.cancel();
        reactor_task.await?;
        Ok(())
    }

    #[tokio::test]
    async fn header_session_waits_for_coordinator_capability_epoch() -> Result<(), BoxError> {
        use zakura_node_services::sync_lifecycle::{
            BlockServiceDemand, HeaderServiceDemand, LifecycleEpoch, SyncServiceDemand,
        };

        let shutdown = CancellationToken::new();
        let (header_sync, _actions, reactor_task) =
            spawn_header_sync_reactor(header_sync_startup(shutdown.clone()))?;
        let disabled = SyncServiceDemand {
            header: HeaderServiceDemand::Disabled {
                runtime_epoch: LifecycleEpoch::INITIAL,
            },
            block: BlockServiceDemand::ServingOnly {
                apply_epoch: LifecycleEpoch::INITIAL,
            },
        };
        let (demand_tx, demand_rx) = watch::channel(disabled);
        let service = HeaderSyncService::new(header_sync).with_service_demand(Some(demand_rx));
        let peer = test_peer(171);
        assert!(!service.wants_peer(
            &peer,
            ZAKURA_CAP_HEADER_SYNC,
            ServicePeerDirection::Outbound,
        ));
        let SessionDemand::WaitForChange(changed) = service.session_demand(
            test_conn_id(),
            &peer,
            ZAKURA_CAP_HEADER_SYNC,
            ServicePeerDirection::Outbound,
        ) else {
            panic!("disabled coordinator demand must park the ordered header session");
        };

        demand_tx
            .send(SyncServiceDemand {
                header: HeaderServiceDemand::Enabled {
                    capability_epoch: LifecycleEpoch::new(1),
                },
                ..disabled
            })
            .expect("the header service retains its demand receiver");
        tokio::time::timeout(Duration::from_secs(1), changed)
            .await
            .expect("the capability epoch wakes ordered-session demand");
        assert!(service.wants_peer(
            &peer,
            ZAKURA_CAP_HEADER_SYNC,
            ServicePeerDirection::Outbound,
        ));

        shutdown.cancel();
        reactor_task.await?;
        Ok(())
    }

    #[tokio::test]
    async fn header_sync_peer_connected_has_immediately_ready_outbound_source(
    ) -> Result<(), BoxError> {
        let shutdown = CancellationToken::new();
        let mut startup = header_sync_startup(shutdown.clone());
        let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
        let snapshot = zakura_header_chain::EngineSnapshot {
            mode: zakura_header_chain::EngineMode::Integrated,
            state_version: zakura_header_chain::StateVersion::new(1),
            header_generation: zakura_header_chain::HeaderGeneration::new(1),
            verified_generation: zakura_header_chain::VerifiedGeneration::new(1),
            frontiers: zakura_header_chain::FrontierSet {
                finalized: anchor,
                header_best: anchor,
                verified_best: anchor,
            },
            header_best_score: zakura_header_chain::ChainScore::new(
                zakura_header_chain::SuffixWork::zero(),
                anchor.hash,
            ),
            oldest_retained_height: anchor.height,
            alarms: Default::default(),
        };
        let (_snapshot_tx, snapshot_rx) = watch::channel(Some(snapshot));
        startup.committed_snapshots = Some(snapshot_rx);
        let (header_sync, _actions, reactor_task) = spawn_header_sync_reactor(startup)?;
        let service = HeaderSyncService::new(header_sync);
        let peer = test_peer(18);
        let connection_cancel = CancellationToken::new();
        let service_cancel = connection_cancel.child_token();
        let (_inbound_tx, inbound_rx) = crate::zakura::framed_channel(8);
        let (outbound_tx, mut outbound_rx) = crate::zakura::framed_channel(8);
        let session_id = 7;
        let streams = HashMap::from([(
            ZAKURA_STREAM_HEADER_SYNC,
            ServiceStream::new(
                session_id,
                ZAKURA_HEADER_SYNC_STREAM_VERSION,
                inbound_rx,
                outbound_tx,
                service_cancel,
            ),
        )]);

        service.add_peer(Peer::new_with_service_streams(
            test_conn_id(),
            peer,
            None,
            ZAKURA_CAP_HEADER_SYNC,
            ServicePeerDirection::Inbound,
            streams,
            connection_cancel.clone(),
            CloseCause::new(),
        ));

        let frame = tokio::time::timeout(Duration::from_secs(1), outbound_rx.recv())
            .await
            .expect("the first outbound source is immediately ready")
            .expect("the admitted source remains open");
        assert!(matches!(
            crate::zakura::wire_codec::decode_frame::<
                crate::zakura::header_sync::v9::CommonMessage,
            >(&frame)
            .expect("the first outbound frame decodes"),
            crate::zakura::header_sync::v9::Message::Status(_)
        ));
        assert!(!connection_cancel.is_cancelled());

        shutdown.cancel();
        reactor_task.await?;
        Ok(())
    }

    /// A header-sync frame read that times out ends the session as a stall,
    /// not a disconnect. The first stall leaves the connection up. A second
    /// read timeout on the same connection closes it under the stall policy.
    #[tokio::test(start_paused = true)]
    async fn header_sync_read_timeouts_follow_the_stall_rule() -> Result<(), BoxError> {
        use crate::zakura::regulation::STALL_POLICY;

        let shutdown = CancellationToken::new();
        let mut startup = header_sync_startup(shutdown.clone());
        let cooldown = startup.config.unproductive_peer_cooldown;
        let anchor = zakura_header_chain::Frontier::new(startup.anchor.0, startup.anchor.1);
        let snapshot = zakura_header_chain::EngineSnapshot {
            mode: zakura_header_chain::EngineMode::Integrated,
            state_version: zakura_header_chain::StateVersion::new(1),
            header_generation: zakura_header_chain::HeaderGeneration::new(1),
            verified_generation: zakura_header_chain::VerifiedGeneration::new(1),
            frontiers: zakura_header_chain::FrontierSet {
                finalized: anchor,
                header_best: anchor,
                verified_best: anchor,
            },
            header_best_score: zakura_header_chain::ChainScore::new(
                zakura_header_chain::SuffixWork::zero(),
                anchor.hash,
            ),
            oldest_retained_height: anchor.height,
            alarms: Default::default(),
        };
        let (_snapshot_tx, snapshot_rx) = watch::channel(Some(snapshot));
        startup.committed_snapshots = Some(snapshot_rx);
        let (header_sync, _actions, reactor_task) = spawn_header_sync_reactor(startup)?;
        let service = HeaderSyncService::new(header_sync);
        let peer = test_peer(19);
        let conn_id = test_conn_id();
        let connection_cancel = CancellationToken::new();
        let close_cause = CloseCause::new();

        for session_id in [7, 8] {
            let (inbound_tx, inbound_rx) = crate::zakura::framed_channel(8);
            let failure = OrderedStreamFailureCause::default();
            let (outbound_tx, mut outbound_rx) = crate::zakura::framed_channel(8);
            let streams = HashMap::from([(
                ZAKURA_STREAM_HEADER_SYNC,
                ServiceStream::new(
                    session_id,
                    ZAKURA_HEADER_SYNC_STREAM_VERSION,
                    inbound_rx.with_failure_cause(failure.clone()),
                    outbound_tx,
                    connection_cancel.child_token(),
                ),
            )]);
            service.add_peer(Peer::new_with_service_streams(
                conn_id,
                peer.clone(),
                None,
                ZAKURA_CAP_HEADER_SYNC,
                ServicePeerDirection::Inbound,
                streams,
                connection_cancel.clone(),
                close_cause.clone(),
            ));
            tokio::time::timeout(Duration::from_secs(1), outbound_rx.recv())
                .await?
                .ok_or("the admitted session sends its status")?;

            let open_now = || {
                matches!(
                    service.session_demand(
                        conn_id,
                        &peer,
                        ZAKURA_CAP_HEADER_SYNC,
                        ServicePeerDirection::Inbound,
                    ),
                    SessionDemand::OpenNow
                )
            };
            assert!(open_now());
            // The transport reader records the timeout, then ends the stream.
            failure.record(OrderedStreamFailure::ReadTimeout);
            drop(inbound_tx);
            // The stall starts the readmission cooldown, which a plain
            // disconnect would not.
            tokio::time::timeout(Duration::from_secs(1), async {
                while open_now() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await?;
            if session_id == 7 {
                assert!(!connection_cancel.is_cancelled());
                assert_eq!(close_cause.get(), None);
                tokio::time::advance(cooldown).await;
            }
        }
        assert!(connection_cancel.is_cancelled());
        assert_eq!(close_cause.get(), Some(STALL_POLICY));

        shutdown.cancel();
        reactor_task.await?;
        Ok(())
    }

    #[tokio::test]
    async fn supervisor_disconnect_peer_cancels_registered_token() -> Result<(), BoxError> {
        let supervisor = ZakuraSupervisorHandle::new(4);
        let peer = test_peer(8);
        let disconnect_token = CancellationToken::new();
        register_test_peer(&supervisor, peer.clone(), disconnect_token.clone()).await;

        assert!(supervisor.disconnect_peer(&peer).await);
        tokio::time::timeout(Duration::from_secs(1), disconnect_token.cancelled())
            .await
            .expect("disconnect token is cancelled promptly");
        assert!(!supervisor.disconnect_peer(&test_peer(9)).await);

        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn duplicate_evicts_stale_incumbent_but_keeps_fresh_one() -> Result<(), BoxError> {
        // A duplicate connection for an identity that already has one either
        // means the peer restarted (incumbent is a dead, stale connection that
        // should be evicted so the redial can reclaim the slot) or that two
        // connections raced at startup (simultaneous open, both fresh, must NOT
        // be evicted or they flap). Incumbent age distinguishes the two.
        let supervisor = ZakuraSupervisorHandle::new(4);

        async fn register_duplicate(
            supervisor: &ZakuraSupervisorHandle,
            peer: &ZakuraPeerId,
            token: CancellationToken,
        ) -> ZakuraRegistration {
            let (outbound_tx, _outbound_rx) = mpsc::channel(1);
            let outbound_handle = ZakuraPeerHandle::new_for_tests(peer.clone(), outbound_tx);
            supervisor
                .register(
                    test_conn_id(),
                    peer.clone(),
                    &[],
                    None,
                    [peer.as_bytes()[0]; TRANSCRIPT_HASH_BYTES],
                    outbound_handle,
                    token,
                    CloseCause::new(),
                    ZAKURA_CAP_LEGACY_GOSSIP | ZAKURA_CAP_HEADER_SYNC,
                )
                .await
        }

        // Fresh incumbent: an immediate duplicate is a simultaneous-open race and
        // must not evict it.
        let fresh_peer = test_peer(8);
        let fresh_incumbent = CancellationToken::new();
        register_test_peer(&supervisor, fresh_peer.clone(), fresh_incumbent.clone()).await;
        let registration =
            register_duplicate(&supervisor, &fresh_peer, CancellationToken::new()).await;
        assert!(matches!(registration, ZakuraRegistration::Duplicate { .. }));
        assert!(
            !fresh_incumbent.is_cancelled(),
            "a young incumbent is kept so simultaneous-open races do not flap",
        );

        // Stale incumbent: once it is older than the threshold, a duplicate
        // evicts it so a restarted peer's redial can reclaim the slot.
        let stale_peer = test_peer(9);
        let stale_incumbent = CancellationToken::new();
        register_test_peer(&supervisor, stale_peer.clone(), stale_incumbent.clone()).await;
        tokio::time::advance(ZAKURA_DUPLICATE_EVICT_MIN_AGE + Duration::from_secs(1)).await;
        let newcomer = CancellationToken::new();
        let registration = register_duplicate(&supervisor, &stale_peer, newcomer.clone()).await;
        assert!(matches!(registration, ZakuraRegistration::Duplicate { .. }));
        assert!(
            stale_incumbent.is_cancelled(),
            "a stale incumbent is evicted so the restarted peer's redial reclaims the slot",
        );
        assert!(
            !newcomer.is_cancelled(),
            "the rejected newcomer's token is never registered, so it is left to redial",
        );

        Ok(())
    }

    // SECURITY AUDIT (candidate claude-per-ip-cap-blocks-stale-duplicate-eviction /
    // codex-ip-cap-blocks-stale-duplicate-eviction /
    // subset-admission-identity-state-ip-cap-blocks-stale-duplicate-eviction):
    // SR-4 liveness.
    //
    // Outbound/native-direct dials register with `remote_ip = Some(ip)`, so the
    // per-IP cap precheck runs. A same-peer redial from an IP already at the cap
    // must NOT be rejected as a resource limit before the duplicate stale-eviction
    // path runs: the incumbent already occupies the only per-IP slot, so a duplicate
    // for the same identity cannot consume an additional slot. If the precheck
    // rejects it first, a dead incumbent keeps its own peer blocked until the QUIC
    // idle timeout (~150s) instead of being evicted in milliseconds.
    #[tokio::test(start_paused = true)]
    async fn same_peer_duplicate_at_per_ip_cap_still_evicts_stale_incumbent() -> Result<(), BoxError>
    {
        async fn register_from_ip(
            supervisor: &ZakuraSupervisorHandle,
            peer: &ZakuraPeerId,
            ip: IpAddr,
            token: CancellationToken,
        ) -> ZakuraRegistration {
            let (outbound_tx, _outbound_rx) = mpsc::channel(1);
            let outbound_handle = ZakuraPeerHandle::new_for_tests(peer.clone(), outbound_tx);
            supervisor
                .register(
                    test_conn_id(),
                    peer.clone(),
                    &[],
                    Some(ip),
                    [peer.as_bytes()[0]; TRANSCRIPT_HASH_BYTES],
                    outbound_handle,
                    token,
                    CloseCause::new(),
                    ZAKURA_CAP_LEGACY_GOSSIP | ZAKURA_CAP_HEADER_SYNC,
                )
                .await
        }

        let supervisor = ZakuraSupervisorHandle::new(1);
        let ip: IpAddr = "203.0.113.9".parse().expect("test ip parses");
        let peer = test_peer(42);

        // Incumbent: a native-direct dial registers from the IP and fills the
        // cap-1 per-IP slot.
        let incumbent = CancellationToken::new();
        let registration = register_from_ip(&supervisor, &peer, ip, incumbent.clone()).await;
        assert!(
            matches!(registration, ZakuraRegistration::Registered { .. }),
            "the first connection from the IP registers",
        );

        // The incumbent ages past the stale-eviction threshold: it is now a likely
        // dead connection left behind by a peer restart.
        tokio::time::advance(ZAKURA_DUPLICATE_EVICT_MIN_AGE + Duration::from_secs(1)).await;

        // The same peer redials from the same IP. The IP is already at cap 1, but
        // the duplicate must still reach the stale-eviction path and cancel the dead
        // incumbent so the redial reclaims the slot in milliseconds.
        let newcomer = CancellationToken::new();
        let registration = register_from_ip(&supervisor, &peer, ip, newcomer.clone()).await;
        assert!(
            matches!(registration, ZakuraRegistration::Duplicate { .. }),
            "a same-peer redial from a capped IP must reach duplicate handling, not be \
             rejected as a resource limit before stale eviction can run",
        );
        assert!(
            incumbent.is_cancelled(),
            "the stale incumbent must be evicted so the restarted peer's redial reclaims \
             the slot in milliseconds instead of waiting for the QUIC idle timeout",
        );
        assert!(
            !newcomer.is_cancelled(),
            "the rejected newcomer's token is never registered, so it is left to redial",
        );

        Ok(())
    }

    #[tokio::test]
    async fn ipv4_embedded_ipv6_addresses_share_the_supervisor_ip_bucket() {
        async fn register_from_ip(
            supervisor: &ZakuraSupervisorHandle,
            peer: ZakuraPeerId,
            ip: IpAddr,
        ) -> ZakuraRegistration {
            let (outbound_tx, _outbound_rx) = mpsc::channel(1);
            let outbound_handle = ZakuraPeerHandle::new_for_tests(peer.clone(), outbound_tx);
            supervisor
                .register(
                    test_conn_id(),
                    peer.clone(),
                    &[],
                    Some(ip),
                    [peer.as_bytes()[0]; TRANSCRIPT_HASH_BYTES],
                    outbound_handle,
                    CancellationToken::new(),
                    CloseCause::new(),
                    ZAKURA_CAP_DISCOVERY,
                )
                .await
        }

        let ipv4: IpAddr = "93.184.216.34".parse().expect("IPv4 test address parses");
        let mapped: IpAddr = "::ffff:93.184.216.34"
            .parse()
            .expect("mapped test address parses");
        let compatible: IpAddr = "::93.184.216.34"
            .parse()
            .expect("compatible test address parses");
        let six_to_four: IpAddr = "2002:5db8:d822::"
            .parse()
            .expect("6to4 test address parses");
        let teredo: IpAddr = "2001:0:c000:22d::a247:27dd"
            .parse()
            .expect("Teredo test address parses");
        let nat64: IpAddr = "64:ff9b::5db8:d822"
            .parse()
            .expect("NAT64 test address parses");
        let supervisor = ZakuraSupervisorHandle::new(1);

        assert!(matches!(
            register_from_ip(&supervisor, test_peer(60), six_to_four).await,
            ZakuraRegistration::Registered { .. }
        ));

        for alias in [ipv4, mapped, compatible, six_to_four, teredo, nat64] {
            assert!(
                !supervisor.can_accept_remote_ip_with_in_flight(alias, 0),
                "{alias} must share the occupied IPv4 identity bucket"
            );
        }

        assert!(matches!(
            register_from_ip(&supervisor, test_peer(61), ipv4).await,
            ZakuraRegistration::Rejected(ZakuraRejectReason::ResourceLimit)
        ));
    }

    // A restarted or resyncing peer redials from its stable IP while its previous
    // connection still lingers as the incumbent. Waiting out the full
    // `ZAKURA_DUPLICATE_EVICT_MIN_AGE` (300s) locks that peer out of block sync
    // until the dead connection's QUIC idle timeout reaps it. A same-IP duplicate
    // therefore evicts the incumbent on the much shorter
    // `ZAKURA_SAME_IP_DUPLICATE_EVICT_MIN_AGE` gate — while still keeping a
    // just-registered incumbent so simultaneous-open races do not flap, and while
    // a *different*-IP duplicate keeps the long gate.
    #[tokio::test(start_paused = true)]
    async fn same_ip_duplicate_uses_short_eviction_gate() -> Result<(), BoxError> {
        async fn register_from_ip(
            supervisor: &ZakuraSupervisorHandle,
            peer: &ZakuraPeerId,
            ip: Option<IpAddr>,
            token: CancellationToken,
        ) -> ZakuraRegistration {
            let (outbound_tx, _outbound_rx) = mpsc::channel(1);
            let outbound_handle = ZakuraPeerHandle::new_for_tests(peer.clone(), outbound_tx);
            supervisor
                .register(
                    test_conn_id(),
                    peer.clone(),
                    &[],
                    ip,
                    [peer.as_bytes()[0]; TRANSCRIPT_HASH_BYTES],
                    outbound_handle,
                    token,
                    CloseCause::new(),
                    ZAKURA_CAP_LEGACY_GOSSIP | ZAKURA_CAP_HEADER_SYNC,
                )
                .await
        }

        let supervisor = ZakuraSupervisorHandle::new(4);
        let ip_a: IpAddr = "203.0.113.10".parse().expect("test ip parses");
        let peer = test_peer(50);

        // Incumbent registered from ip_a.
        let incumbent = CancellationToken::new();
        let registration =
            register_from_ip(&supervisor, &peer, Some(ip_a), incumbent.clone()).await;
        assert!(matches!(
            registration,
            ZakuraRegistration::Registered { .. }
        ));

        // A same-IP duplicate below the short gate keeps the incumbent so a genuine
        // simultaneous-open race (both connections fresh) does not flap.
        tokio::time::advance(ZAKURA_SAME_IP_DUPLICATE_EVICT_MIN_AGE - Duration::from_secs(1)).await;
        let early = CancellationToken::new();
        let registration = register_from_ip(&supervisor, &peer, Some(ip_a), early.clone()).await;
        assert!(matches!(registration, ZakuraRegistration::Duplicate { .. }));
        assert!(
            !incumbent.is_cancelled(),
            "a same-IP duplicate below the short gate keeps the incumbent",
        );

        // Past the short same-IP gate — but far below the 300s gate — a same-IP
        // duplicate evicts the stale incumbent so the restarted peer's redial
        // reclaims the slot in seconds.
        tokio::time::advance(Duration::from_secs(2)).await;
        let redial = CancellationToken::new();
        let registration = register_from_ip(&supervisor, &peer, Some(ip_a), redial.clone()).await;
        assert!(matches!(registration, ZakuraRegistration::Duplicate { .. }));
        assert!(
            incumbent.is_cancelled(),
            "a same-IP duplicate past the short gate evicts the stale incumbent",
        );
        assert!(
            !redial.is_cancelled(),
            "the rejected newcomer's token is never registered, so it is left to redial",
        );

        // Contrast: a *different*-IP duplicate keeps the long gate, so at the same
        // small age it does not evict — only a same-IP redial gets the fast path.
        let peer2 = test_peer(51);
        let ip_b: IpAddr = "203.0.113.11".parse().expect("test ip parses");
        let ip_c: IpAddr = "203.0.113.12".parse().expect("test ip parses");
        let incumbent2 = CancellationToken::new();
        let registration =
            register_from_ip(&supervisor, &peer2, Some(ip_b), incumbent2.clone()).await;
        assert!(matches!(
            registration,
            ZakuraRegistration::Registered { .. }
        ));
        tokio::time::advance(ZAKURA_SAME_IP_DUPLICATE_EVICT_MIN_AGE + Duration::from_secs(1)).await;
        let diff_ip = CancellationToken::new();
        let registration = register_from_ip(&supervisor, &peer2, Some(ip_c), diff_ip.clone()).await;
        assert!(matches!(registration, ZakuraRegistration::Duplicate { .. }));
        assert!(
            !incumbent2.is_cancelled(),
            "a different-IP duplicate keeps the long gate and does not evict a young incumbent",
        );

        Ok(())
    }

    #[tokio::test]
    async fn header_sync_misbehavior_action_does_not_disconnect_peer() -> Result<(), BoxError> {
        let _guard = zakura_test::init();
        let reactor_shutdown = CancellationToken::new();
        let startup = header_sync_startup(reactor_shutdown.clone());
        let (header_sync, _reactor_actions, reactor_task) = spawn_header_sync_reactor(startup)?;
        let supervisor = ZakuraSupervisorHandle::new(4);
        let peer = test_peer(10);
        let disconnect_token = CancellationToken::new();
        register_test_peer(&supervisor, peer.clone(), disconnect_token.clone()).await;

        let (actions_tx, actions_rx) = mpsc::channel(4);
        let driver_shutdown = CancellationToken::new();
        let driver_task = tokio::spawn(drive_header_sync_actions(
            actions_rx,
            header_sync,
            supervisor,
            driver_shutdown.clone(),
        ));
        actions_tx
            .send(HeaderSyncAction::Misbehavior {
                peer,
                reason: HeaderSyncMisbehavior::MalformedMessage,
            })
            .await?;

        assert!(
            tokio::time::timeout(Duration::from_millis(100), disconnect_token.cancelled())
                .await
                .is_err(),
            "misbehavior is record-only and must not cancel the registered connection",
        );

        driver_shutdown.cancel();
        driver_task.await?;
        reactor_shutdown.cancel();
        reactor_task.await?;
        Ok(())
    }

    /// Only a body that proves tampering closes the supplier's connection.
    #[tokio::test]
    async fn block_sync_misbehavior_disconnects_only_a_proven_tampered_body() {
        use crate::zakura::BlockSyncMisbehavior;
        use zakura_header_chain::BodyCommitmentKind;

        let mismatch = |kind| {
            BlockSyncMisbehavior::BodyPayloadMismatch(zakura_header_chain::BodyPayloadMismatch {
                evidence: zakura_header_chain::EvidenceId::from_digest([1; 32]),
                requested: block::Hash([2; 32]),
                delivered: block::Hash([2; 32]),
                kind,
                source: zakura_header_chain::SourceId::from_digest([3; 32]),
            })
        };
        let cases = [
            (mismatch(BodyCommitmentKind::AuthDataRoot), true),
            (
                mismatch(BodyCommitmentKind::CHAIN_HISTORY_BLOCK_TX_AUTH_COMMITMENT),
                true,
            ),
            (mismatch(BodyCommitmentKind::TransactionMerkleRoot), true),
            // A different header at the requested height can be a fork response.
            (mismatch(BodyCommitmentKind::HeaderHash), false),
            // These depend on local trees or supplied auxiliary roots.
            (
                mismatch(BodyCommitmentKind::Other("final_sapling_root")),
                false,
            ),
            (
                mismatch(BodyCommitmentKind::Other("chain_history_root")),
                false,
            ),
            (BlockSyncMisbehavior::InvalidBlock, false),
            (BlockSyncMisbehavior::StatusSpam, false),
        ];
        let supervisor = ZakuraSupervisorHandle::new(cases.len());
        for (index, (reason, disconnects)) in cases.into_iter().enumerate() {
            let peer = test_peer(u8::try_from(index + 20).expect("the case count fits in u8"));
            let disconnect_token = CancellationToken::new();
            let conn_id =
                register_test_peer(&supervisor, peer.clone(), disconnect_token.clone()).await;

            assert_eq!(
                supervisor
                    .enforce_block_sync_misbehavior(&peer, conn_id, &reason)
                    .await,
                disconnects,
                "{reason:?}"
            );
            assert_eq!(disconnect_token.is_cancelled(), disconnects, "{reason:?}");
            if disconnects {
                assert!(
                    !supervisor
                        .enforce_block_sync_misbehavior(&peer, conn_id, &reason)
                        .await,
                    "a connection already closing is not closed again: {reason:?}"
                );
            }
        }
        assert!(
            !supervisor
                .enforce_block_sync_misbehavior(
                    &test_peer(250),
                    0,
                    &mismatch(BodyCommitmentKind::AuthDataRoot)
                )
                .await,
            "a peer that already left has nothing to close"
        );
    }

    #[tokio::test]
    async fn delayed_block_verdict_matches_the_supplying_registration() {
        use crate::zakura::BlockSyncMisbehavior;
        use zakura_header_chain::{BodyCommitmentKind, BodyPayloadMismatch, EvidenceId, SourceId};

        let supervisor = ZakuraSupervisorHandle::new(4);
        let peer = test_peer(26);
        let reason = BlockSyncMisbehavior::BodyPayloadMismatch(BodyPayloadMismatch {
            evidence: EvidenceId::from_digest([1; 32]),
            requested: block::Hash([2; 32]),
            delivered: block::Hash([2; 32]),
            kind: BodyCommitmentKind::AuthDataRoot,
            source: SourceId::from_digest([3; 32]),
        });
        let supplier_cancel = CancellationToken::new();
        let supplier = register_test_peer(&supervisor, peer.clone(), supplier_cancel.clone()).await;

        let replacement_cancel = CancellationToken::new();
        let replacement_cause = CloseCause::new();
        let (outbound, _outbound_recv) = mpsc::channel(1);
        let replacement = registered_conn_id(
            supervisor
                .register(
                    test_conn_id(),
                    peer.clone(),
                    &[],
                    None,
                    [0; TRANSCRIPT_HASH_BYTES],
                    ZakuraPeerHandle::new_for_tests(peer.clone(), outbound),
                    replacement_cancel.clone(),
                    replacement_cause.clone(),
                    ZAKURA_CAP_BLOCK_SYNC_REGULATED,
                )
                .await,
        );
        assert!(supplier_cancel.is_cancelled());
        assert!(
            !supervisor
                .enforce_block_sync_misbehavior(&peer, supplier, &reason)
                .await
        );
        assert!(!replacement_cancel.is_cancelled());
        assert_eq!(replacement_cause.get(), None);
        assert_eq!(supervisor.outbound_peer_handles().await.len(), 1);

        assert!(
            supervisor
                .enforce_block_sync_misbehavior(&peer, replacement, &reason)
                .await
        );
        assert_eq!(
            replacement_cause.get(),
            Some(CloseOutcome::fault(
                ZAKURA_STREAM_BLOCK_SYNC,
                ZAKURA_REGULATED_BLOCK_SYNC_STREAM_VERSION,
                CloseRow::BSYNC_CLOSE_18,
            ))
        );
        assert!(
            !supervisor
                .enforce_block_sync_misbehavior(&peer, replacement, &reason)
                .await
        );
        supervisor.deregister(&peer, replacement).await;
        assert!(
            !supervisor
                .enforce_block_sync_misbehavior(&peer, replacement, &reason)
                .await
        );
    }

    /// Exercise the production worker with a full application receive channel.
    /// Sending must stay live, QUIC must eventually stop the bounded sender, and
    /// another stream must retain connection credit until the application resumes.
    #[tokio::test]
    async fn paused_ordered_reads_preserve_writes_and_sibling_credit() -> Result<(), BoxError> {
        const ALPN: &[u8] = b"/zakura/testkit/paused-reader/0";
        const FRAME_BYTES: usize = 1024 * 1024;
        let local = ZakuraLocalLimits::from_config(&Config::default());
        // The 1 MiB frames fill the stream receive window, plus a margin the
        // sender cannot write.
        let window_frames = local.quic.stream_receive_window_bytes as usize / FRAME_BYTES;
        let frame_count = window_frames + 8;
        // After negotiation the connection window holds y_ctl beside the
        // largest stream window, so a paused stream leaves sibling credit.
        let server = LocalEndpointFactory::with_limits(&local)
            .quic_config(QuicConfig {
                receive_window_bytes: Some(
                    local
                        .quic
                        .receive_window()
                        .saturating_add(local.quic.stream_receive_window_bytes),
                ),
                ..local.quic.clone()
            })
            .endpoint(52)
            .await?;
        let (conn_tx, _conn_rx) = mpsc::channel(1);
        let (stream_tx, mut stream_rx) = mpsc::channel(2);
        server.serve(CaptureConnection {
            alpn: ALPN,
            connection_tx: conn_tx,
            stream_tx,
        })?;
        // A small sender buffer makes write completion reflect receiver credit.
        let client = LocalEndpointFactory::with_limits(&local)
            .quic_config(QuicConfig {
                send_window_bytes: 64 * 1024,
                ..local.quic.clone()
            })
            .endpoint(53)
            .await?;
        let address = LocalEndpointFactory::node_addr(&server).await;
        let connection =
            timeout(Duration::from_secs(10), client.connect(address, &[ALPN])).await??;
        let (mut sender, mut receiver) = connection.open_bi().await?;
        let frame = Frame {
            message_type: 3,
            flags: 0,
            payload: vec![0; FRAME_BYTES],
        };
        let limits = test_connection_limits();
        let stream = Stream {
            kind: ZAKURA_STREAM_BLOCK_SYNC,
            version: ZAKURA_REGULATED_BLOCK_SYNC_STREAM_VERSION,
            frame_cap: 2_000_009,
            capability: ZAKURA_CAP_BLOCK_SYNC_REGULATED,
            ..Stream::PERSISTENT
        };
        let encoded = frame.encode(stream.frame_cap)?;
        // Opening a QUIC stream becomes visible to the receiver after the first bytes.
        sender.write_all(&encoded[..1]).await?;
        let (send, recv) = timeout(Duration::from_secs(5), stream_rx.recv())
            .await?
            .unwrap();
        let cancel = CancellationToken::new();
        let stream_cancel = cancel.child_token();
        let (freshness_tx, _freshness_rx) = watch::channel(Instant::now());
        let context = StreamWorkerContext {
            conn: ZakuraConnTrace::without_peer(1),
            peer_id: test_peer(52),
            stream_id: 1,
            _permit: Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap(),
            limits,
            inbound_frame_cap: stream.frame_cap,
            messages: None,
            queue_depths: None,
            write_policy: StreamWritePolicy::Timeout(OUTBOUND_STREAM_WRITE_TIMEOUT),
            session_resources: None,
            outbound_frame_cap: stream.frame_cap,
            message_bucket: Arc::new(std::sync::Mutex::new(TokenBucket::new(128))),
            cadence: None,
            precheck: PrecheckSlot::default(),
            resources: StreamResources::unconstrained(),
            connection_token: cancel.clone(),
            stream_token: stream_cancel.clone(),
            close_cause: CloseCause::new(),
            freshness_tx,
            kind_version: (stream.kind, stream.version),
        };
        let prelude = StreamPrelude {
            magic: STREAM_PRELUDE_MAGIC,
            stream_kind: stream.kind,
            stream_version: stream.version,
            request_id: None,
            max_frame_bytes: stream.frame_cap,
        };
        let (inbound_tx, mut inbound_rx) = mpsc::channel(1);
        let (outbound_tx, outbound_rx) = worker_framed_channel(1);
        let mut worker = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(
            persistent_stream_worker(send, recv, prelude, context, inbound_tx, outbound_rx, 1),
        ));
        let (progress_tx, mut progress_rx) = watch::channel(0);
        let mut sending = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
            sender.write_all(&encoded[1..]).await.unwrap();
            progress_tx.send_replace(1);
            for sent in 2..=frame_count {
                sender.write_all(&encoded).await.unwrap();
                progress_tx.send_replace(sent);
            }
            sender
        }));
        timeout(Duration::from_secs(10), async {
            while *progress_rx.borrow_and_update() < window_frames {
                progress_rx.changed().await.unwrap();
            }
        })
        .await
        .expect("the sender fills the default stream receive window");
        assert!(
            timeout(Duration::from_millis(100), &mut sending)
                .await
                .is_err(),
            "QUIC must stop the sender while application reads are paused"
        );
        assert_eq!(inbound_rx.len(), 1);

        let response = Frame {
            message_type: 4,
            flags: 0,
            payload: vec![7; 9],
        };
        let producer = crate::zakura::regulation::SlotBudget::new(1).unwrap();
        let ownership = Arc::new(producer.try_reserve().unwrap());
        outbound_tx.try_reserve_guarded().unwrap().send(
            response.clone(),
            crate::zakura::transport::FrameGuard::new(ownership),
        );
        let written = timeout(
            Duration::from_secs(2),
            read_frame(
                &mut receiver,
                stream.frame_cap,
                FrameFilter::new(None, InboundReader::Persistent),
                Duration::from_secs(2),
                None,
            ),
        )
        .await??;
        assert_eq!(written, response);
        assert_eq!(
            producer.reserved(),
            0,
            "the response write releases serving capacity"
        );

        let (mut sibling_send, _sibling_recv) = connection.open_bi().await?;
        timeout(Duration::from_secs(2), sibling_send.write_all(b"sibling")).await??;
        let (_, mut sibling_read) = timeout(Duration::from_secs(2), stream_rx.recv())
            .await?
            .unwrap();
        let mut bytes = [0; 7];
        timeout(Duration::from_secs(2), sibling_read.read_exact(&mut bytes)).await??;
        assert_eq!(&bytes, b"sibling");

        timeout(Duration::from_secs(10), async {
            for _ in 0..frame_count {
                assert_eq!(inbound_rx.recv().await.unwrap().frame, frame);
            }
        })
        .await
        .expect("resuming application reads drains all frames in order");
        let mut sender = timeout(Duration::from_secs(2), &mut sending).await??;
        assert_eq!(*progress_rx.borrow(), frame_count);
        // Cancellation must also work while the reader waits on a full channel.
        let empty = Frame {
            message_type: 2,
            flags: 0,
            payload: vec![0; 9],
        }
        .encode(stream.frame_cap)?;
        sender.write_all(&empty).await?;
        sender.write_all(&empty).await?;
        timeout(Duration::from_secs(2), async {
            while inbound_rx.len() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        stream_cancel.cancel();
        timeout(Duration::from_secs(2), &mut worker).await??;
        assert!(
            !cancel.is_cancelled(),
            "local stream cancellation preserves the connection"
        );
        connection.close(0u32.into(), b"done");
        client.shutdown().await;
        server.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn stream_cancel_closes_ordered_worker_without_connection_cancel() -> Result<(), BoxError>
    {
        const ALPN: &[u8] = b"/zakura/testkit/stream-cancel/0";

        let _guard = zakura_test::init();
        let local = ZakuraLocalLimits::from_config(&Config::default());
        let server = LocalEndpointFactory::with_limits(&local)
            .quic_config(QuicConfig {
                send_window_bytes: 64 * 1024,
                ..local.quic.clone()
            })
            .endpoint(50)
            .await?;
        let (conn_tx, mut conn_rx) = mpsc::channel(1);
        let (stream_tx, mut stream_rx) = mpsc::channel(2);
        server.serve(CaptureConnection {
            alpn: ALPN,
            connection_tx: conn_tx,
            stream_tx,
        })?;
        let client = LocalEndpointFactory::with_limits(&local)
            .quic_config(QuicConfig {
                stream_receive_window_bytes: 64 * 1024,
                ..local.quic.clone()
            })
            .endpoint(51)
            .await?;
        let server_addr = LocalEndpointFactory::node_addr(&server).await;

        let client_conn = timeout(
            Duration::from_secs(10),
            client.connect(server_addr, &[ALPN]),
        )
        .await
        .expect("client connects to loopback capture endpoint")?;
        let server_conn = timeout(Duration::from_secs(10), conn_rx.recv())
            .await
            .expect("server connection is captured")
            .expect("capture handler sends the accepted connection");
        drop(server_conn);
        let (mut client_send, mut client_recv) =
            timeout(Duration::from_secs(1), client_conn.open_bi())
                .await
                .expect("client opens the worker stream")?;
        let test_frame = Frame {
            message_type: 1,
            flags: 0,
            payload: Vec::new(),
        }
        .encode(LOCAL_MAX_CONTROL_FRAME_BYTES)?;
        timeout(Duration::from_secs(1), client_send.write_all(&test_frame))
            .await
            .expect("client writes the worker stream frame")?;
        let (server_send, server_recv) = timeout(Duration::from_secs(1), stream_rx.recv())
            .await
            .expect("server accepts the worker stream")
            .expect("capture handler sends the worker stream");

        let mut limits = test_connection_limits();
        limits.idle_timeout = Duration::from_millis(50);
        let stream_kind = ZAKURA_STREAM_BLOCK_SYNC;
        let connection_token = CancellationToken::new();
        let stream_token = connection_token.child_token();
        let (freshness_tx, _freshness_rx) = watch::channel(Instant::now());
        let permit = Arc::new(Semaphore::new(1))
            .try_acquire_owned()
            .expect("test semaphore starts with one permit");
        let stream = Stream {
            kind: stream_kind,
            version: ZAKURA_STREAM_VERSION_1,
            frame_cap: 2_000_009,
            capability: ZAKURA_CAP_BLOCK_SYNC_REGULATED,
            ..Stream::PERSISTENT
        };
        let context = StreamWorkerContext {
            conn: ZakuraConnTrace::without_peer(1),
            peer_id: test_peer(55),
            stream_id: 1,
            _permit: permit,
            limits,
            inbound_frame_cap: inbound_frame_cap_for_stream(&limits, stream),
            messages: None,
            queue_depths: None,
            write_policy: StreamWritePolicy::Timeout(OUTBOUND_STREAM_WRITE_TIMEOUT),
            session_resources: None,
            outbound_frame_cap: application_frame_cap(&limits, stream),
            message_bucket: Arc::new(std::sync::Mutex::new(TokenBucket::new(128))),
            cadence: None,
            precheck: PrecheckSlot::default(),
            resources: StreamResources::unconstrained(),
            connection_token: connection_token.clone(),
            stream_token: stream_token.clone(),
            close_cause: CloseCause::new(),
            freshness_tx,
            kind_version: (stream.kind, stream.version),
        };
        let prelude = StreamPrelude {
            magic: STREAM_PRELUDE_MAGIC,
            stream_kind,
            stream_version: ZAKURA_STREAM_VERSION_1,
            request_id: None,
            max_frame_bytes: inbound_frame_cap_for_stream(&limits, stream),
        };
        let mut workers = JoinSet::new();
        let (session_exit_tx, mut session_exit_rx) = mpsc::unbounded_channel();
        let admitted = spawn_persistent_stream_worker(
            &mut workers,
            server_send,
            server_recv,
            stream,
            prelude,
            context,
            1,
            true,
            session_exit_tx,
        );

        let response = Frame {
            message_type: 3,
            flags: 0,
            payload: vec![7; 1024 * 1024],
        };
        admitted.streams[0].send.try_send(response.clone()).unwrap();
        // Reading the header proves the write has started. The much smaller
        // QUIC windows keep the remaining payload blocked until we drain it.
        let mut header = [0; FRAME_HEADER_BYTES];
        timeout(Duration::from_secs(2), client_recv.read_exact(&mut header)).await??;
        assert_eq!(
            &header[..],
            &response.encode(stream.frame_cap)?[..FRAME_HEADER_BYTES]
        );
        admitted.cancel_token.cancel();
        let mut payload = vec![0; response.payload.len()];
        assert!(
            timeout(Duration::from_secs(2), client_recv.read_exact(&mut payload))
                .await?
                .is_err(),
            "session cancellation resets an unfinished frame"
        );
        // The exit must be reported, or the connection loop never prunes the dead
        // generation and never reopens the stream.
        let exited = timeout(Duration::from_secs(1), session_exit_rx.recv())
            .await
            .expect("stream cancellation reports worker exit")
            .expect("exit channel stays open");
        assert_eq!(exited.stream, stream);
        assert_eq!(exited.session_id, 1);
        assert!(exited.opened_locally);
        timeout(Duration::from_secs(1), workers.join_next())
            .await
            .expect("stream cancellation stops the worker promptly")
            .expect("worker exit is observed")?;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !connection_token.is_cancelled(),
            "stream-local cancellation must not cancel the shared connection"
        );

        let (mut sibling_send, _sibling_recv) =
            timeout(Duration::from_secs(1), client_conn.open_bi())
                .await
                .expect("connection survives to open a sibling stream")?;
        timeout(Duration::from_secs(1), sibling_send.write_all(&test_frame))
            .await
            .expect("client writes the sibling stream frame")?;
        timeout(Duration::from_secs(1), stream_rx.recv())
            .await
            .expect("server accepts the sibling stream")
            .expect("capture handler sends the sibling stream");

        client_conn.close(0u32.into(), b"done");
        client.shutdown().await;
        server.shutdown().await;
        Ok(())
    }

    /// Regression for the ~150s mainnet Zakura peer churn
    /// (`run-f406a987-churn-20260619T032010Z`): a long-lived ordered stream that
    /// is legitimately quiet between frames -- e.g. the gossip stream while
    /// syncing far below the tip -- must not tear down the whole connection it
    /// shares with an actively-transferring stream.
    ///
    /// Trace forensics showed every serving peer's connection `closed.neutral`
    /// with reason `cancelled` at a lifetime of exactly 150.1-150.3s (the QUIC /
    /// app idle timeout), then a reconnect + duplicate-arbitration storm, even
    /// though block sync was downloading at ~99 blk/s the whole time. The cause:
    /// the ordered-stream reader used `idle_timeout` as its per-frame read
    /// deadline AND treated that deadline as a fatal error, so the first quiet
    /// ordered stream `connection_token.cancel()`ed the entire connection at
    /// `idle_timeout` -- dropping the active sibling transfer -- even though the
    /// connection-level freshness reaper (which resets on ANY stream's activity)
    /// would never have reaped it. That is why the close reason is `cancelled`
    /// (worker-initiated) and not `idle_timeout` (reaper).
    ///
    /// This drives the production `persistent_stream_worker` over a real local
    /// QUIC connection with two ordered streams that share one
    /// `connection_token` and one freshness watch channel, exactly as
    /// `serve_connection` wires them, plus the real `freshness_reaper`. One
    /// stream is kept busy (mirrors block sync); the other stays quiet (mirrors
    /// gossip). The connection must survive well past `idle_timeout`.
    #[tokio::test]
    async fn quiet_ordered_stream_does_not_cancel_connection_with_active_sibling(
    ) -> Result<(), BoxError> {
        const ALPN: &[u8] = b"/zakura/testkit/quiet-ordered-stream/0";

        let _guard = zakura_test::init();
        let server = LocalEndpointFactory::new().endpoint(80).await?;
        let (conn_tx, _conn_rx) = mpsc::channel(1);
        let (stream_tx, mut stream_rx) = mpsc::channel(2);
        server.serve(CaptureConnection {
            alpn: ALPN,
            connection_tx: conn_tx,
            stream_tx,
        })?;
        let client = LocalEndpointFactory::new().endpoint(81).await?;
        let server_addr = LocalEndpointFactory::node_addr(&server).await;

        let client_conn = timeout(
            Duration::from_secs(10),
            client.connect(server_addr, &[ALPN]),
        )
        .await
        .expect("client connects to loopback capture endpoint")?;

        let frame = Frame {
            message_type: 1,
            flags: 0,
            payload: Vec::new(),
        }
        .encode(LOCAL_MAX_CONTROL_FRAME_BYTES)?;

        // Both streams need one initial frame so the server's `accept_bi`
        // observes the stream and we can spawn its worker; after that only the
        // active stream keeps sending. The client send halves are retained for
        // the whole test so neither stream is `Closed` -- the quiet stream must
        // sit in an inter-frame read, not a peer-initiated close.
        let (mut active_send, _active_recv) =
            timeout(Duration::from_secs(1), client_conn.open_bi())
                .await
                .expect("client opens the active stream")?;
        timeout(Duration::from_secs(1), active_send.write_all(&frame))
            .await
            .expect("client writes the active stream's first frame")?;
        let (active_server_send, active_server_recv) =
            timeout(Duration::from_secs(1), stream_rx.recv())
                .await
                .expect("server accepts the active stream")
                .expect("capture handler forwards the active stream");

        let (mut quiet_send, _quiet_recv) = timeout(Duration::from_secs(1), client_conn.open_bi())
            .await
            .expect("client opens the quiet stream")?;
        timeout(Duration::from_secs(1), quiet_send.write_all(&frame))
            .await
            .expect("client writes the quiet stream's only frame")?;
        let (quiet_server_send, quiet_server_recv) =
            timeout(Duration::from_secs(1), stream_rx.recv())
                .await
                .expect("server accepts the quiet stream")
                .expect("capture handler forwards the quiet stream");

        // Shared per-connection state, exactly as `serve_connection` builds it:
        // one cancellation token and one freshness watch channel feeding every
        // worker and the reaper.
        let idle_timeout = Duration::from_millis(250);
        let mut limits = test_connection_limits();
        limits.idle_timeout = idle_timeout;
        let stream_kind = LEGACY_GOSSIP_STREAM_KIND;
        let connection_token = CancellationToken::new();
        let (freshness_tx, freshness_rx) = watch::channel(Instant::now());

        let spawn_ordered_worker = |send: SendStream, recv: RecvStream, stream_id: u64| {
            let permit = Arc::new(Semaphore::new(1))
                .try_acquire_owned()
                .expect("test semaphore starts with one permit");
            let stream = Stream {
                kind: stream_kind,
                version: ZAKURA_STREAM_VERSION_1,
                frame_cap: LOCAL_MAX_CONTROL_FRAME_BYTES,
                capability: ZAKURA_CAP_LEGACY_GOSSIP,
                ..Stream::PERSISTENT
            };
            let context = StreamWorkerContext {
                conn: ZakuraConnTrace::without_peer(1),
                peer_id: test_peer(80),
                stream_id,
                _permit: permit,
                limits,
                inbound_frame_cap: inbound_frame_cap_for_stream(&limits, stream),
                messages: None,
                queue_depths: None,
                write_policy: StreamWritePolicy::Timeout(OUTBOUND_STREAM_WRITE_TIMEOUT),
                session_resources: None,
                outbound_frame_cap: application_frame_cap(&limits, stream),
                message_bucket: Arc::new(std::sync::Mutex::new(TokenBucket::new(
                    limits.message_rate_per_second,
                ))),
                cadence: None,
                precheck: PrecheckSlot::default(),
                resources: StreamResources::unconstrained(),
                connection_token: connection_token.clone(),
                stream_token: connection_token.child_token(),
                close_cause: CloseCause::new(),
                freshness_tx: freshness_tx.clone(),
                kind_version: (stream.kind, stream.version),
            };
            let prelude = StreamPrelude {
                magic: STREAM_PRELUDE_MAGIC,
                stream_kind,
                stream_version: ZAKURA_STREAM_VERSION_1,
                request_id: None,
                max_frame_bytes: inbound_frame_cap_for_stream(&limits, stream),
            };
            // Drain the inbound side so the worker never blocks forwarding a
            // read frame to a full service channel (which would itself stop the
            // reader and confound the test). The outbound side is unused here.
            let (inbound_tx, mut inbound_rx) = mpsc::channel(16);
            let (_outbound_tx, outbound_rx) = worker_framed_channel(1);
            tokio::spawn(async move { while inbound_rx.recv().await.is_some() {} });
            tokio::spawn(persistent_stream_worker(
                send,
                recv,
                prelude,
                context,
                inbound_tx,
                outbound_rx,
                16,
            ))
        };

        let _active_worker = spawn_ordered_worker(active_server_send, active_server_recv, 1);
        let _quiet_worker = spawn_ordered_worker(quiet_server_send, quiet_server_recv, 2);

        // Model `serve_connection`'s idle race: the connection ends either when a
        // worker cancels the shared token or when the freshness reaper fires
        // (which `serve_connection` follows with `connection_token.cancel()`).
        // So `connection_token.is_cancelled()` captures BOTH teardown modes.
        let serve = tokio::spawn({
            let connection_token = connection_token.clone();
            async move {
                tokio::select! {
                    biased;
                    _ = connection_token.cancelled() => {}
                    _ = freshness_reaper(freshness_rx, idle_timeout) => connection_token.cancel(),
                }
            }
        });

        // The active sibling transfers a frame well within `idle_timeout`, so the
        // connection-level freshness stays fresh and the reaper must never fire.
        let keepalive_frame = frame.clone();
        let keepalive = tokio::spawn(async move {
            for _ in 0..16 {
                tokio::time::sleep(Duration::from_millis(80)).await;
                if active_send.write_all(&keepalive_frame).await.is_err() {
                    break;
                }
            }
        });

        // Wait well past `idle_timeout`; the buggy reader tears the connection
        // down at ~`idle_timeout` (~250ms) despite the active sibling.
        tokio::time::sleep(idle_timeout * 4).await;
        assert!(
            !connection_token.is_cancelled(),
            "a quiet ordered stream must not cancel a connection whose sibling stream is \
             actively transferring; the shared connection_token was cancelled within {:?}",
            idle_timeout * 4,
        );

        connection_token.cancel();
        keepalive.abort();
        let _ = serve.await;
        client_conn.close(0u32.into(), b"done");
        client.shutdown().await;
        server.shutdown().await;
        Ok(())
    }

    /// Regression for `claude-outbound-write-ignores-message-cap` (persistent
    /// ordered-stream facet).
    ///
    /// `write_ordered_frame` sized outbound persistent-stream frames against the
    /// negotiated frame cap only. The handshake clamps `max_frame_bytes` and
    /// `max_message_bytes` independently, so a peer can negotiate a small message
    /// cap with a large frame cap. A persistent service that queues a frame
    /// larger than that message cap then has it encoded and written, and the peer
    /// rejects it as oversize and disconnects us, wasting the encode/write.
    /// `write_ordered_frame` must mirror `write_response_frame` and reject an
    /// over-message-cap frame locally before encoding/writing it, while still
    /// writing frames within the cap.
    #[tokio::test]
    async fn write_ordered_frame_rejects_payload_over_message_cap() -> Result<(), BoxError> {
        const ALPN: &[u8] = b"/zakura/testkit/ordered-message-cap/0";

        let _guard = zakura_test::init();
        let server = LocalEndpointFactory::new().endpoint(74).await?;
        let (conn_tx, _conn_rx) = mpsc::channel(1);
        let (stream_tx, _stream_rx) = mpsc::channel(2);
        server.serve(CaptureConnection {
            alpn: ALPN,
            connection_tx: conn_tx,
            stream_tx,
        })?;
        let client = LocalEndpointFactory::new().endpoint(75).await?;
        let server_addr = LocalEndpointFactory::node_addr(&server).await;

        let client_conn = timeout(
            Duration::from_secs(10),
            client.connect(server_addr, &[ALPN]),
        )
        .await
        .expect("client connects to the ordered-message-cap endpoint")?;
        let (mut send, _recv) = timeout(Duration::from_secs(1), client_conn.open_bi())
            .await
            .expect("client opens an ordered stream")?;

        // A small negotiated message cap with a large frame cap, as the handshake
        // permits.
        let mut limits = test_connection_limits();
        limits.max_message_bytes = 256;
        let stream = Stream {
            kind: DISCOVERY_STREAM_KIND,
            version: ZAKURA_STREAM_VERSION_1,
            frame_cap: LOCAL_MAX_CONTROL_FRAME_BYTES,
            capability: ZAKURA_CAP_DISCOVERY,
            ..Stream::PERSISTENT
        };
        let frame_cap = application_frame_cap(&limits, stream);

        // A payload above the message cap but well within the frame cap, so only
        // the message cap can reject it.
        let oversized = Frame {
            message_type: 1,
            flags: 0,
            payload: vec![0xab; 4096],
        };
        assert!(
            oversized.payload.len() <= frame_cap as usize,
            "test payload must fit the frame cap so only the message cap can reject it"
        );

        let result = write_ordered_frame(&mut send, oversized, limits, frame_cap).await;
        assert!(
            result.is_err(),
            "write_ordered_frame must reject a payload over the negotiated max_message_bytes \
             before encoding/writing it, mirroring write_response_frame; got {result:?}"
        );

        // A payload within the negotiated message cap must still be written.
        let within_cap = Frame {
            message_type: 1,
            flags: 0,
            payload: vec![0xcd; 128],
        };
        write_ordered_frame(&mut send, within_cap, limits, frame_cap)
            .await
            .expect("a frame within the negotiated message cap must still be written");

        client_conn.close(0u32.into(), b"done");
        client.shutdown().await;
        server.shutdown().await;
        Ok(())
    }

    // Regression for `claude-control-payload-late-hard-cap`: the native control
    // hello/ack reads passed the configured `max_control_frame_bytes` (1 MiB
    // default) to `read_control_payload`, so a peer could force allocation/read
    // of a control payload between the 16 KiB hard cap and 1 MiB before
    // ZakuraControlHello/Ack::decode rejected it. The reader must instead reject
    // an oversized control length on the length prefix alone, before reading the
    // body, while still accepting payloads within the 16 KiB hard cap.
    #[tokio::test]
    async fn read_control_payload_enforces_hard_cap_before_reading_body() -> Result<(), BoxError> {
        const ALPN: &[u8] = b"/zakura/testkit/control-hard-cap/0";

        let _guard = zakura_test::init();
        let server = LocalEndpointFactory::new().endpoint(70).await?;
        let (conn_tx, _conn_rx) = mpsc::channel(1);
        let (stream_tx, mut stream_rx) = mpsc::channel(2);
        server.serve(CaptureConnection {
            alpn: ALPN,
            connection_tx: conn_tx,
            stream_tx,
        })?;
        let client = LocalEndpointFactory::new().endpoint(71).await?;
        let server_addr = LocalEndpointFactory::node_addr(&server).await;

        let client_conn = timeout(
            Duration::from_secs(10),
            client.connect(server_addr, &[ALPN]),
        )
        .await
        .expect("client connects to the control-hard-cap endpoint")?;

        // A control length above the 16 KiB hard cap but below the 1 MiB frame
        // cap the production responder/initiator pass. The body is never sent: a
        // correct reader must reject on the length prefix alone.
        let oversized_len =
            u32::try_from(MAX_CONTROL_PAYLOAD_BYTES + 1).expect("control hard cap + 1 fits in u32");
        let (mut over_send, _over_recv) = timeout(Duration::from_secs(1), client_conn.open_bi())
            .await
            .expect("client opens the oversized control stream")?;
        timeout(
            Duration::from_secs(1),
            over_send.write_all(&oversized_len.to_le_bytes()),
        )
        .await
        .expect("client writes the oversized control length")?;
        let _ = over_send.finish();

        let (_over_server_send, mut over_server_recv) =
            timeout(Duration::from_secs(1), stream_rx.recv())
                .await
                .expect("server accepts the oversized control stream")
                .expect("capture handler forwards the oversized control stream");

        // Pass exactly what production passes: the configured frame cap (1 MiB),
        // NOT the 16 KiB hard cap. The fix clamps internally to the hard cap.
        let oversized = read_control_payload(
            &mut over_server_recv,
            LOCAL_MAX_CONTROL_FRAME_BYTES,
            Duration::from_secs(2),
        )
        .await;
        assert!(
            matches!(oversized, Err(ZakuraHandlerError::Oversize)),
            "a control length over the 16 KiB hard cap must be rejected as Oversize \
             on the length prefix, before the body is read; got {oversized:?}"
        );

        // A control payload within the hard cap must still be read normally, so
        // the clamp does not break legitimate sub-16 KiB control frames.
        let valid_body = vec![0xa5u8; 128];
        let valid_len = u32::try_from(valid_body.len()).expect("128 fits in u32");
        let (mut ok_send, _ok_recv) = timeout(Duration::from_secs(1), client_conn.open_bi())
            .await
            .expect("client opens the in-cap control stream")?;
        timeout(
            Duration::from_secs(1),
            ok_send.write_all(&valid_len.to_le_bytes()),
        )
        .await
        .expect("client writes the in-cap control length")?;
        timeout(Duration::from_secs(1), ok_send.write_all(&valid_body))
            .await
            .expect("client writes the in-cap control body")?;
        let _ = ok_send.finish();

        let (_ok_server_send, mut ok_server_recv) =
            timeout(Duration::from_secs(1), stream_rx.recv())
                .await
                .expect("server accepts the in-cap control stream")
                .expect("capture handler forwards the in-cap control stream");
        let read_back = read_control_payload(
            &mut ok_server_recv,
            LOCAL_MAX_CONTROL_FRAME_BYTES,
            Duration::from_secs(2),
        )
        .await
        .expect("a control payload within the hard cap is read");
        assert_eq!(
            read_back, valid_body,
            "an in-cap control payload must round-trip unchanged"
        );

        client_conn.close(0u32.into(), b"done");
        client.shutdown().await;
        server.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn initiator_refuses_an_ack_that_widens_its_capability_offer() -> Result<(), BoxError> {
        let _guard = zakura_test::init();
        let server = LocalEndpointFactory::new().endpoint(72).await?;
        let (conn_tx, _conn_rx) = mpsc::channel(1);
        let (stream_tx, mut stream_rx) = mpsc::channel(2);
        server.serve(CaptureConnection {
            alpn: P2P_V2_ALPN,
            connection_tx: conn_tx,
            stream_tx,
        })?;
        let client = LocalEndpointFactory::new().endpoint(73).await?;
        let server_addr = LocalEndpointFactory::node_addr(&server).await;
        let client_conn = timeout(
            Duration::from_secs(10),
            client.connect(server_addr, &[P2P_V2_ALPN]),
        )
        .await??;
        let limits = ZakuraLocalLimits::from_config(&Config::default());
        let mut config = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
        config.supported_capabilities = ZAKURA_CAP_HEADER_SYNC;
        let local_peer = ZakuraPeerId::new(client.local_id().as_bytes().to_vec())?;

        // Each round acks the hello on a fresh control stream with `extra`
        // added to the offer.
        for (extra, accepted) in [(0, true), (ZAKURA_CAP_BLOCK_SYNC_REGULATED, false)] {
            let handshake = run_native_initiator_handshake_without_trace(
                &client_conn,
                &limits,
                &config,
                &local_peer,
            );
            let respond = async {
                let (mut send, mut recv) =
                    stream_rx.recv().await.ok_or("missing control stream")?;
                let hello = ZakuraControlHello::decode(
                    &read_control_payload(
                        &mut recv,
                        LOCAL_MAX_CONTROL_FRAME_BYTES,
                        Duration::from_secs(2),
                    )
                    .await?,
                )?;
                let ack = ZakuraControlAck {
                    magic: CONTROL_ACK_MAGIC,
                    control_version: CONTROL_VERSION,
                    selected_zakura_protocol: hello.selected_zakura_protocol,
                    peer_nonce: [9; 32],
                    remote_peer_nonce: hello.peer_nonce,
                    accepted_capabilities: hello.capabilities | extra,
                    accepted_channels: 0,
                    accepted_limits: hello.initial_limits,
                };
                write_control_payload(&mut send, &ack.encode()?, Duration::from_secs(2)).await?;
                Ok::<_, BoxError>(send)
            };
            let (negotiated, responded) = timeout(Duration::from_secs(5), async {
                tokio::join!(handshake, respond)
            })
            .await?;
            // The responder returns its send half so dropping it can't reset
            // the ack before the initiator reads it.
            responded?;
            if accepted {
                assert_eq!(negotiated?.accepted_capabilities, ZAKURA_CAP_HEADER_SYNC);
            } else {
                assert!(
                    matches!(
                        negotiated,
                        Err(ZakuraHandlerError::Validation(
                            ZakuraValidationError::UnofferedCapability
                        ))
                    ),
                    "an ack outside the offer must fail the handshake; got {negotiated:?}"
                );
            }
        }

        client_conn.close(0u32.into(), b"done");
        client.shutdown().await;
        server.shutdown().await;
        Ok(())
    }

    /// A responder that paces its ack's length and payload, each within the
    /// per-step deadline, still fails the initiator at the overall deadline.
    #[tokio::test]
    async fn initiator_bounds_a_paced_ack_by_one_deadline() -> Result<(), BoxError> {
        let _guard = zakura_test::init();
        let server = LocalEndpointFactory::new().endpoint(74).await?;
        let (conn_tx, _conn_rx) = mpsc::channel(1);
        let (stream_tx, mut stream_rx) = mpsc::channel(1);
        server.serve(CaptureConnection {
            alpn: P2P_V2_ALPN,
            connection_tx: conn_tx,
            stream_tx,
        })?;
        let client = LocalEndpointFactory::new().endpoint(75).await?;
        let server_addr = LocalEndpointFactory::node_addr(&server).await;
        let client_conn = timeout(
            Duration::from_secs(10),
            client.connect(server_addr, &[P2P_V2_ALPN]),
        )
        .await??;
        let mut limits = ZakuraLocalLimits::from_config(&Config::default());
        limits.control_timeout = Duration::from_secs(1);
        let config = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
        let local_peer = ZakuraPeerId::new(client.local_id().as_bytes().to_vec())?;

        let handshake = run_native_initiator_handshake_without_trace(
            &client_conn,
            &limits,
            &config,
            &local_peer,
        );
        let respond = async {
            let (mut send, mut recv) = stream_rx.recv().await.ok_or("missing control stream")?;
            let hello = ZakuraControlHello::decode(
                &read_control_payload(
                    &mut recv,
                    LOCAL_MAX_CONTROL_FRAME_BYTES,
                    Duration::from_secs(2),
                )
                .await?,
            )?;
            let ack = ZakuraControlAck {
                magic: CONTROL_ACK_MAGIC,
                control_version: CONTROL_VERSION,
                selected_zakura_protocol: hello.selected_zakura_protocol,
                peer_nonce: [9; 32],
                remote_peer_nonce: hello.peer_nonce,
                accepted_capabilities: hello.capabilities,
                accepted_channels: 0,
                accepted_limits: hello.initial_limits,
            }
            .encode()?;
            let mut len = Vec::with_capacity(CONTROL_LENGTH_BYTES);
            len.write_u32::<LittleEndian>(u32::try_from(ack.len())?)?;
            zakura_rt::time::sleep(Duration::from_millis(600)).await;
            send.write_all(&len).await?;
            zakura_rt::time::sleep(Duration::from_millis(600)).await;
            // The initiator may already have closed the stream.
            let _ = send.write_all(&ack).await;
            Ok::<_, BoxError>(send)
        };
        let (negotiated, responded) = timeout(Duration::from_secs(5), async {
            tokio::join!(handshake, respond)
        })
        .await?;
        responded?;
        assert!(
            matches!(
                negotiated,
                Err(ZakuraHandlerError::Timeout("control handshake"))
            ),
            "a paced ack must fail at the overall deadline; got {negotiated:?}"
        );

        client_conn.close(0u32.into(), b"done");
        client.shutdown().await;
        server.shutdown().await;
        Ok(())
    }

    /// A reader's context for one stream of the regulation test family.
    fn tabled_context(
        messages: Option<&'static [MessageRule]>,
        rate_per_second: u32,
    ) -> StreamWorkerContext {
        let buckets = MessageRateBuckets::new();
        let (freshness_tx, _freshness_rx) = watch::channel(Instant::now());
        let stream = Stream {
            kind: 900,
            version: 1,
            messages,
            ..Stream::PERSISTENT
        };
        let cancel = CancellationToken::new();
        StreamWorkerContext {
            conn: ZakuraConnTrace::without_peer(1),
            peer_id: test_peer(52),
            stream_id: 1,
            _permit: Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap(),
            limits: test_connection_limits(),
            inbound_frame_cap: 1024,
            messages,
            queue_depths: None,
            write_policy: StreamWritePolicy::UntilCancelled,
            session_resources: None,
            outbound_frame_cap: 1024,
            message_bucket: Arc::new(std::sync::Mutex::new(TokenBucket::new(rate_per_second))),
            cadence: cadence_scope(&buckets, stream, 900),
            precheck: PrecheckSlot::default(),
            resources: StreamResources::unconstrained(),
            connection_token: cancel.clone(),
            stream_token: cancel.child_token(),
            close_cause: CloseCause::new(),
            freshness_tx,
            kind_version: (stream.kind, stream.version),
        }
    }

    fn probe_frame(message_type: u16, payload_len: usize) -> Frame {
        Frame {
            message_type,
            flags: 0,
            payload: vec![1; payload_len],
        }
    }

    #[test]
    fn a_metered_row_past_its_byte_bucket_closes_the_connection() {
        const METERED: MessageRule = MessageRule {
            name: "metered",
            message_type: 40,
            payload: crate::zakura::PayloadLen::between(1, 512),
            role: crate::zakura::MessageRole::Metered {
                rate: crate::zakura::ByteRate {
                    bytes_per_second: 1,
                    opening_allowance: 512,
                    sender_allowance: 512,
                },
            },
        };
        // A fresh connection's bucket holds the opening allowance.
        let context = tabled_context(Some(&[METERED]), 1);
        assert_eq!(
            admit_inbound_message(&probe_frame(40, 512), &context, 900),
            InboundMessageAdmission::Admit
        );
        assert_eq!(
            admit_inbound_message(&probe_frame(40, 1), &context, 900),
            InboundMessageAdmission::ByteRateOverrun
        );
        assert_eq!(
            ordered_read_close((900, 1), &ZakuraHandlerError::ByteRateOverrun),
            Some((
                ZAKURA_CLOSE_RATE_LIMIT,
                CloseOutcome::fault(900, 1, CloseRow::CONN_CLOSE_18)
            ))
        );
    }

    #[test]
    fn tabled_streams_charge_only_the_rows_that_declare_a_cadence() {
        use crate::zakura::regulation::test_family::{message_type, EVERY_30_SECONDS, RULES};

        // A one-message stream bucket would throttle the second frame of a
        // legacy stream. A tabled stream never touches it.
        let context = tabled_context(Some(RULES), 1);
        for message_type in [message_type::PART, message_type::DONE, message_type::GET] {
            for _ in 0..4096 {
                assert_eq!(
                    admit_inbound_message(&probe_frame(message_type, 4), &context, 900),
                    InboundMessageAdmission::Admit
                );
            }
        }
        for _ in 0..EVERY_30_SECONDS.capacity {
            assert_eq!(
                admit_inbound_message(&probe_frame(message_type::STATUS, 4), &context, 900),
                InboundMessageAdmission::Admit
            );
        }
        assert_eq!(
            admit_inbound_message(&probe_frame(message_type::STATUS, 4), &context, 900),
            InboundMessageAdmission::Admit
        );
        // Each cadence row has its own bucket.
        assert_eq!(
            admit_inbound_message(&probe_frame(message_type::PING, 4), &context, 900),
            InboundMessageAdmission::Admit
        );

        let legacy = tabled_context(None, 1);
        assert_eq!(
            admit_inbound_message(&probe_frame(message_type::PART, 4), &legacy, 900),
            InboundMessageAdmission::Admit
        );
        assert_eq!(
            admit_inbound_message(&probe_frame(message_type::PART, 4), &legacy, 900),
            InboundMessageAdmission::Throttled
        );
    }

    #[test]
    fn a_read_pause_credits_the_streams_cadence_rows() {
        use crate::zakura::regulation::test_family::{message_type, EVERY_30_SECONDS, RULES};

        let context = tabled_context(Some(RULES), 1);
        let tokens = || {
            context
                .cadence
                .as_ref()
                .unwrap()
                .buckets
                .lock()
                .unwrap()
                .tokens(900, message_type::STATUS)
        };
        let status = probe_frame(message_type::STATUS, 4);
        for _ in 0..EVERY_30_SECONDS.capacity {
            admit_inbound_message(&status, &context, 900);
        }
        assert_eq!(
            admit_inbound_message(&status, &context, 900),
            InboundMessageAdmission::Admit
        );
        assert_eq!(tokens(), Some(0));
        context.credit_read_pause(EVERY_30_SECONDS.refill_interval * 3);
        assert_eq!(tokens(), Some(3));
        for remaining in (0..3).rev() {
            assert_eq!(
                admit_inbound_message(&status, &context, 900),
                InboundMessageAdmission::Admit
            );
            assert_eq!(tokens(), Some(remaining));
        }
        assert_eq!(
            admit_inbound_message(&status, &context, 900),
            InboundMessageAdmission::Admit
        );
    }

    /// Replay the burst from 23 sends spaced 30 seconds apart during a
    /// 660-second stream stall. A sibling can keep the connection alive.
    #[tokio::test]
    async fn cadence_bursts_beyond_the_outage_capacity_stay_connected() -> Result<(), BoxError> {
        use crate::zakura::regulation::test_family::{decode, message_type, Probe, RULES};

        const ALPN: &[u8] = b"/zakura/testkit/cadence-burst/0";
        let _guard = zakura_test::init();
        let server = LocalEndpointFactory::new().endpoint(83).await?;
        let (conn_tx, _conn_rx) = mpsc::channel(8);
        let (stream_tx, mut stream_rx) = mpsc::channel(8);
        server.serve(CaptureConnection {
            alpn: ALPN,
            connection_tx: conn_tx,
            stream_tx,
        })?;
        let client = LocalEndpointFactory::new().endpoint(84).await?;
        let stream = Stream {
            kind: 900,
            messages: Some(RULES),
            ..Stream::PERSISTENT
        };
        // The 22-frame control and larger bursts use fresh connection buckets.
        for count in [22u32, 23, 64] {
            let connection = timeout(
                Duration::from_secs(5),
                client.connect(LocalEndpointFactory::node_addr(&server).await, &[ALPN]),
            )
            .await??;
            let (mut sender, _receiver) =
                timeout(Duration::from_secs(2), connection.open_bi()).await??;
            let mut bytes = Vec::new();
            for value in 0..count {
                bytes.extend_from_slice(&message_type::STATUS.to_le_bytes());
                bytes.extend_from_slice(&0u16.to_le_bytes());
                bytes.extend_from_slice(&4u32.to_le_bytes());
                bytes.extend_from_slice(&value.to_le_bytes());
            }
            timeout(Duration::from_secs(2), sender.write_all(&bytes)).await??;
            let (send, recv) = timeout(Duration::from_secs(2), stream_rx.recv())
                .await?
                .unwrap();
            let context = tabled_context(Some(RULES), 1);
            let cancel = context.connection_token.clone();
            let prelude = StreamPrelude {
                magic: STREAM_PRELUDE_MAGIC,
                stream_kind: stream.kind,
                stream_version: stream.version,
                request_id: None,
                max_frame_bytes: stream.frame_cap,
            };
            let mut workers = JoinSet::new();
            let (exit_tx, _exit_rx) = mpsc::unbounded_channel();
            let mut admitted = spawn_persistent_stream_worker(
                &mut workers,
                send,
                recv,
                stream,
                prelude,
                context,
                1,
                false,
                exit_tx,
            );
            for value in 0..count {
                let frame = timeout(Duration::from_secs(2), admitted.streams[0].recv.recv())
                    .await?
                    .expect("the reader forwards every buffered announcement");
                assert_eq!(decode(&frame), Probe::Status(value));
            }
            assert!(!cancel.is_cancelled());
            cancel.cancel();
            timeout(Duration::from_secs(2), workers.join_next()).await?;
            connection.close(0u32.into(), b"done");
        }
        client.shutdown().await;
        server.shutdown().await;
        Ok(())
    }

    /// The reader checks each response header against the attached
    /// reservations and fails before it waits for the payload.
    #[tokio::test]
    async fn response_prechecks_apply_before_payload_reads() -> Result<(), BoxError> {
        use crate::zakura::regulation::{
            test_family::{message_type, RULES},
            ReservationPool, Reservations, ResponseCap, SharedReservations,
        };

        let stream = Stream {
            kind: 900,
            version: 1,
            frame_cap: 1024,
            capability: 1 << 48,
            messages: Some(RULES),
            ..Stream::PERSISTENT
        };
        let pool = ReservationPool::new(4)?;
        let shared = SharedReservations::new(Reservations::<u32>::new(RULES, 4));
        let filter = FrameFilter::new(stream.messages, InboundReader::Persistent);

        const ALPN: &[u8] = b"/zakura/testkit/response-precheck/0";
        let _guard = zakura_test::init();
        let server = LocalEndpointFactory::new().endpoint(81).await?;
        let (conn_tx, _conn_rx) = mpsc::channel(8);
        let (stream_tx, mut stream_rx) = mpsc::channel(8);
        server.serve(CaptureConnection {
            alpn: ALPN,
            connection_tx: conn_tx,
            stream_tx,
        })?;
        let client = LocalEndpointFactory::new().endpoint(82).await?;
        let server_addr = LocalEndpointFactory::node_addr(&server).await;

        // The capture takes one stream per connection, so each case connects.
        let read_header = |message_type: u16, payload_len: u32| {
            let (client, server_addr) = (client.clone(), server_addr.clone());
            async move {
                let connection =
                    timeout(Duration::from_secs(5), client.connect(server_addr, &[ALPN])).await??;
                let (mut send, _recv) =
                    timeout(Duration::from_secs(2), connection.open_bi()).await??;
                let mut header = Vec::with_capacity(FRAME_HEADER_BYTES);
                header.extend_from_slice(&message_type.to_le_bytes());
                header.extend_from_slice(&0u16.to_le_bytes());
                header.extend_from_slice(&payload_len.to_le_bytes());
                timeout(Duration::from_secs(2), send.write_all(&header)).await??;
                Ok::<_, BoxError>((connection, send))
            }
        };
        let rejected = |result: Result<Frame, ZakuraHandlerError>| match result {
            Err(ZakuraHandlerError::RejectedFrame { rejection, .. }) => Some(rejection),
            _ => None,
        };

        // No reservation: a response header is unsolicited.
        let _open = read_header(message_type::PART, 10).await?;
        let (_, mut recv) = timeout(Duration::from_secs(2), stream_rx.recv())
            .await?
            .unwrap();
        let result = timeout(
            Duration::from_secs(1),
            read_frame(
                &mut recv,
                stream.frame_cap,
                filter.with_precheck(Some(&shared)),
                Duration::from_secs(5),
                Some(Duration::from_secs(5)),
            ),
        )
        .await
        .expect("a refused header fails without waiting for payload bytes");
        assert_eq!(rejected(result), Some(FrameRejection::Unsolicited));

        // A reservation with ten bytes left: eleven are refused.
        shared.lock().reserve(
            1,
            message_type::GET,
            ResponseCap {
                frames: 1,
                bytes: 10,
            },
            pool.try_entry().unwrap(),
        )?;
        let _open = read_header(message_type::PART, 11).await?;
        let (_, mut recv) = timeout(Duration::from_secs(2), stream_rx.recv())
            .await?
            .unwrap();
        let result = timeout(
            Duration::from_secs(1),
            read_frame(
                &mut recv,
                stream.frame_cap,
                filter.with_precheck(Some(&shared)),
                Duration::from_secs(5),
                Some(Duration::from_secs(5)),
            ),
        )
        .await
        .expect("a refused header fails without waiting for payload bytes");
        assert_eq!(
            rejected(result),
            Some(FrameRejection::AboveReservation { bytes: 10 })
        );

        // A peer sends its response header before the service receives its
        // handle. The production reader must wait for precheck attachment.
        let _open = read_header(message_type::PART, 11).await?;
        let (send, recv) = timeout(Duration::from_secs(2), stream_rx.recv())
            .await?
            .unwrap();
        let mut context = tabled_context(Some(RULES), 128);
        context.limits.idle_timeout = Duration::from_secs(30);
        let cancel = context.connection_token.clone();
        let prelude = StreamPrelude {
            magic: STREAM_PRELUDE_MAGIC,
            stream_kind: stream.kind,
            stream_version: stream.version,
            request_id: None,
            max_frame_bytes: stream.frame_cap,
        };
        let mut workers = JoinSet::new();
        let (exit_tx, _exit_rx) = mpsc::unbounded_channel();
        let admitted = spawn_persistent_stream_worker(
            &mut workers,
            send,
            recv,
            stream,
            prelude,
            context,
            1,
            false,
            exit_tx,
        );
        // Give the independent reader time to consume the already sent header
        // if its startup gate is missing.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!cancel.is_cancelled());
        assert!(admitted.streams[0]
            .recv
            .attach_precheck(Arc::new(shared.clone())));
        timeout(Duration::from_secs(1), cancel.cancelled())
            .await
            .expect("the attached precheck rejects the first header without its payload");
        timeout(Duration::from_secs(1), workers.join_next()).await?;

        // Legacy streams have no rows, but their request budget still rejects
        // a header before the peer supplies any payload bytes.
        let mut legacy = LegacyResponseReadState::new(
            LegacyResponseBudget::from_request(LEGACY_REQUEST_PING, &[], test_connection_limits())
                .unwrap(),
        );
        for (payload_len, expected) in [
            (9, FrameRejection::AboveReservation { bytes: 8 }),
            (0, FrameRejection::Unsolicited),
        ] {
            let _open = read_header(LEGACY_RESPONSE_PONG, payload_len).await?;
            let (_, mut recv) = timeout(Duration::from_secs(2), stream_rx.recv())
                .await?
                .unwrap();
            let result = timeout(
                Duration::from_secs(1),
                read_frame(
                    &mut recv,
                    stream.frame_cap,
                    FrameFilter::new(None, InboundReader::Requester).with_precheck(Some(&legacy)),
                    Duration::from_secs(5),
                    Some(Duration::from_secs(5)),
                ),
            )
            .await
            .expect("the legacy budget rejects a header without reading its payload");
            assert_eq!(rejected(result), Some(expected));
            if legacy.frames == 0 {
                legacy
                    .validate_frame(
                        1,
                        Frame {
                            message_type: LEGACY_RESPONSE_PONG,
                            flags: 0,
                            payload: 1u64.to_le_bytes().to_vec(),
                        },
                    )
                    .expect("the first pong fits its request budget");
            }
        }

        // Requests are not responses: the precheck does not apply.
        let (_connection, mut send) = read_header(message_type::GET, 4).await?;
        timeout(Duration::from_secs(2), send.write_all(&[0; 4])).await??;
        let (_, mut recv) = timeout(Duration::from_secs(2), stream_rx.recv())
            .await?
            .unwrap();
        let frame = timeout(
            Duration::from_secs(2),
            read_frame(
                &mut recv,
                stream.frame_cap,
                filter.with_precheck(Some(&shared)),
                Duration::from_secs(5),
                Some(Duration::from_secs(5)),
            ),
        )
        .await??;
        assert_eq!(frame.message_type, message_type::GET);
        Ok(())
    }

    /// Stream 6 queues a fixed number of frames for its routine, however much
    /// the live reservations owe. The precheck here admits every body, as a
    /// request window of any size would, and the peer sends full-size bodies
    /// at the largest negotiable transport depth. The reader still holds only
    /// the declared depth plus the frame it waits to forward. The rest waits
    /// in QUIC flow control. When the routine takes a body and holds it, the
    /// reader admits exactly one more, so the session never holds more than
    /// the depth plus two.
    #[tokio::test]
    async fn block_sync_queues_a_fixed_number_of_bodies_whatever_the_reservations_owe(
    ) -> Result<(), BoxError> {
        use crate::zakura::{
            block_sync_streams, regulation::ResponsePrecheck, LOCAL_MAX_INBOUND_QUEUE_DEPTH,
        };
        use std::sync::atomic::{AtomicUsize, Ordering};

        /// Admits every response header and counts what the reader admitted.
        #[derive(Debug, Default)]
        struct OwesEverything {
            frames: AtomicUsize,
            bytes: AtomicUsize,
        }

        impl ResponsePrecheck for OwesEverything {
            fn check(&self, _message_type: u16, payload_len: usize) -> Result<(), FrameRejection> {
                self.frames.fetch_add(1, Ordering::SeqCst);
                self.bytes.fetch_add(payload_len, Ordering::SeqCst);
                Ok(())
            }
        }

        // The largest `Block` payload stream 6 accepts: its tag and a
        // 2,000,000-byte body.
        const BODY_PAYLOAD: usize = 2_000_001;
        const SENT: usize = 32;
        let stream = block_sync_streams()[0];
        let depth = stream
            .queue_depths
            .expect("stream 6 declares its inbound depth")
            .inbound;
        let held = depth + 1;
        // The resource ledger's raw-frame term counts these and the routine's
        // body: about 20 MB per session.
        assert!((held + 1) * BODY_PAYLOAD < 21_000_000);
        assert!(held < SENT, "the peer sends more than the queue holds");

        const ALPN: &[u8] = b"/zakura/testkit/block-sync-queue-depth/0";
        let _guard = zakura_test::init();
        let server = LocalEndpointFactory::new().endpoint(83).await?;
        let (conn_tx, _conn_rx) = mpsc::channel(8);
        let (stream_tx, mut stream_rx) = mpsc::channel(8);
        server.serve(CaptureConnection {
            alpn: ALPN,
            connection_tx: conn_tx,
            stream_tx,
        })?;
        let client = LocalEndpointFactory::new().endpoint(84).await?;
        let server_addr = LocalEndpointFactory::node_addr(&server).await;
        let connection =
            timeout(Duration::from_secs(5), client.connect(server_addr, &[ALPN])).await??;
        let (mut peer_send, _peer_recv) =
            timeout(Duration::from_secs(2), connection.open_bi()).await??;
        let writer = tokio::spawn(async move {
            let mut frame = Vec::with_capacity(FRAME_HEADER_BYTES + BODY_PAYLOAD);
            frame.extend_from_slice(&3u16.to_le_bytes());
            frame.extend_from_slice(&0u16.to_le_bytes());
            frame.extend_from_slice(&u32::try_from(BODY_PAYLOAD)?.to_le_bytes());
            frame.push(3);
            frame.resize(FRAME_HEADER_BYTES + BODY_PAYLOAD, 0);
            for _ in 0..SENT {
                peer_send.write_all(&frame).await?;
            }
            Ok::<_, BoxError>(peer_send)
        });
        let (send, recv) = timeout(Duration::from_secs(2), stream_rx.recv())
            .await?
            .unwrap();

        let mut context = tabled_context(stream.messages, 1024);
        context.limits.idle_timeout = Duration::from_secs(30);
        context.inbound_frame_cap = stream.frame_cap;
        context.queue_depths = stream.queue_depths;
        let cancel = context.connection_token.clone();
        let prelude = StreamPrelude {
            magic: STREAM_PRELUDE_MAGIC,
            stream_kind: stream.kind,
            stream_version: stream.version,
            request_id: None,
            max_frame_bytes: stream.frame_cap,
        };
        let mut workers = JoinSet::new();
        let (exit_tx, _exit_rx) = mpsc::unbounded_channel();
        let mut admitted = spawn_persistent_stream_worker(
            &mut workers,
            send,
            recv,
            stream,
            prelude,
            context,
            usize::from(LOCAL_MAX_INBOUND_QUEUE_DEPTH),
            false,
            exit_tx,
        );
        let precheck = Arc::new(OwesEverything::default());
        assert!(admitted.streams[0].recv.attach_precheck(precheck.clone()));

        let admitted_frames = || precheck.frames.load(Ordering::SeqCst);
        await_until(
            "the reader fills its queue",
            Duration::from_secs(10),
            || admitted_frames() >= held,
        )
        .await?;
        // Give the reader time to read past its bound if it could.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(admitted_frames(), held);
        assert_eq!(precheck.bytes.load(Ordering::SeqCst), held * BODY_PAYLOAD);
        assert!(!cancel.is_cancelled(), "a full queue is backpressure");

        // The routine takes one body and keeps it, as it does while it waits
        // for decode capacity. The reader admits exactly one more.
        let frame = timeout(Duration::from_secs(2), admitted.streams[0].recv.recv())
            .await?
            .expect("the reader forwarded the first body");
        assert_eq!(frame.payload.len(), BODY_PAYLOAD);
        await_until(
            "the reader refills one slot",
            Duration::from_secs(10),
            || admitted_frames() > held,
        )
        .await?;
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(admitted_frames(), held + 1);
        assert!(!cancel.is_cancelled());

        writer.abort();
        cancel.cancel();
        timeout(Duration::from_secs(2), workers.join_next()).await?;
        connection.close(0u32.into(), b"done");
        client.shutdown().await;
        server.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn message_tables_apply_before_payload_reads() -> Result<(), BoxError> {
        use crate::zakura::{block_sync_streams, MessageRole, PayloadLen};

        const REQUEST: MessageRule = MessageRule {
            name: "request",
            message_type: 2,
            payload: PayloadLen::exact(9),
            role: MessageRole::Request {
                max_in_flight: 1,
                cadence: None,
            },
        };
        const RESPONSE: MessageRule = MessageRule {
            name: "response",
            message_type: 3,
            payload: PayloadLen::between(1, 99),
            role: MessageRole::Response {
                request: REQUEST.message_type,
                ends_exchange: true,
            },
        };
        let stream = Stream {
            messages: Some(&[REQUEST, RESPONSE]),
            ..block_sync_streams()[0]
        };
        Stream::check_layout(&[stream])?;
        let filter = FrameFilter::new(stream.messages, InboundReader::Persistent);

        const ALPN: &[u8] = b"/zakura/testkit/message-tables/0";
        let _guard = zakura_test::init();
        let server = LocalEndpointFactory::new().endpoint(79).await?;
        let (conn_tx, _conn_rx) = mpsc::channel(8);
        let (stream_tx, mut stream_rx) = mpsc::channel(8);
        server.serve(CaptureConnection {
            alpn: ALPN,
            connection_tx: conn_tx,
            stream_tx,
        })?;
        let client = LocalEndpointFactory::new().endpoint(80).await?;
        let server_addr = LocalEndpointFactory::node_addr(&server).await;

        // Only send headers and keep the send sides open. The reader must reject
        // each one before waiting for a payload that the peer has not supplied.
        let oversize = |cap| move |result: &Result<Frame, ZakuraHandlerError>| matches!(result, Err(ZakuraHandlerError::OversizeFrame { max_frame_bytes, .. }) if *max_frame_bytes == cap);
        let rejected = |expected| move |result: &Result<Frame, ZakuraHandlerError>| matches!(result, Err(ZakuraHandlerError::RejectedFrame { rejection, .. }) if *rejection == expected);
        let cases: [(
            u16,
            u16,
            u32,
            u32,
            Box<dyn Fn(&Result<Frame, ZakuraHandlerError>) -> bool>,
        ); 7] = [
            // Eight frame-header bytes plus the request row's nine-byte maximum.
            (2, 0, 10, stream.frame_cap, Box::new(oversize(17))),
            (2, 0, u32::MAX, stream.frame_cap, Box::new(oversize(17))),
            // A tighter negotiated cap still applies to a legal request.
            (2, 0, 9, 16, Box::new(oversize(16))),
            (3, 0, 100, stream.frame_cap, Box::new(oversize(107))),
            (
                7,
                0,
                1,
                stream.frame_cap,
                Box::new(rejected(FrameRejection::UnknownMessageType)),
            ),
            (
                2,
                1,
                9,
                stream.frame_cap,
                Box::new(rejected(FrameRejection::ReservedFlags)),
            ),
            (
                2,
                0,
                8,
                stream.frame_cap,
                Box::new(rejected(FrameRejection::PayloadTooShort { min: 9 })),
            ),
        ];
        for (message_type, flags, payload_len, frame_cap, expected) in cases {
            let connection = timeout(
                Duration::from_secs(5),
                client.connect(server_addr.clone(), &[ALPN]),
            )
            .await??;
            let (mut send, _recv) = timeout(Duration::from_secs(2), connection.open_bi()).await??;
            let mut header = Vec::with_capacity(FRAME_HEADER_BYTES);
            header.extend_from_slice(&message_type.to_le_bytes());
            header.extend_from_slice(&flags.to_le_bytes());
            header.extend_from_slice(&payload_len.to_le_bytes());
            timeout(Duration::from_secs(2), send.write_all(&header)).await??;
            let (_, mut recv) = timeout(Duration::from_secs(2), stream_rx.recv())
                .await?
                .unwrap();
            let result = timeout(
                Duration::from_secs(1),
                read_frame(
                    &mut recv,
                    frame_cap,
                    filter,
                    Duration::from_secs(5),
                    Some(Duration::from_secs(5)),
                ),
            )
            .await
            .expect("a rejected header fails without waiting for payload bytes");
            assert!(
                expected(&result),
                "type {message_type} flags {flags} length {payload_len}: {result:?}"
            );
        }

        // Frames within their rows read normally, at both bounds.
        let connection =
            timeout(Duration::from_secs(5), client.connect(server_addr, &[ALPN])).await??;
        let (mut send, _recv) = timeout(Duration::from_secs(2), connection.open_bi()).await??;
        let frames = [
            Frame {
                message_type: 2,
                flags: 0,
                payload: vec![7; 9],
            },
            Frame {
                message_type: 3,
                flags: 0,
                payload: vec![8; 1],
            },
            Frame {
                message_type: 3,
                flags: 0,
                payload: vec![9; 99],
            },
        ];
        for frame in &frames {
            timeout(
                Duration::from_secs(2),
                send.write_all(&frame.encode(stream.frame_cap)?),
            )
            .await??;
        }
        let (_, mut recv) = timeout(Duration::from_secs(2), stream_rx.recv())
            .await?
            .unwrap();
        for expected in frames {
            let frame = timeout(
                Duration::from_secs(2),
                read_frame(
                    &mut recv,
                    stream.frame_cap,
                    filter,
                    Duration::from_secs(2),
                    Some(Duration::from_secs(2)),
                ),
            )
            .await??;
            assert_eq!(frame, expected);
        }
        client.shutdown().await;
        server.shutdown().await;
        Ok(())
    }

    // claude-late-message-cap-allocation: read_frame checks only
    // frame_len > max_frame_bytes before it reads the payload, while the smaller
    // max_message_bytes is enforced later in admit_inbound_message. A peer can
    // negotiate max_frame_bytes > max_message_bytes (the caps are clamped
    // independently), so on every admitted ordered/request stream it could force
    // read_frame to allocate and read a payload between the two limits before the
    // message cap rejects it. The inbound read path must instead be handed a cap
    // already limited to the message size, so an over-message frame is rejected on
    // its header alone, before the payload is allocated and read.
    #[tokio::test]
    async fn inbound_frame_cap_rejects_over_message_frame_before_reading_payload(
    ) -> Result<(), BoxError> {
        const ALPN: &[u8] = b"/zakura/testkit/late-message-cap/0";
        // The negotiated frame cap is far larger than the message cap: exactly the
        // precondition the finding requires (caps allowed to diverge).
        const MAX_FRAME_BYTES: u32 = 64 * 1024;
        const MAX_MESSAGE_BYTES: u32 = 1024;
        // A payload between the message cap and the frame cap. admit_inbound_message
        // would reject it, but only after read_frame allocated and read it.
        const OVER_MESSAGE_PAYLOAD_LEN: u32 = 2048;
        let stream = Stream {
            kind: LEGACY_GOSSIP_STREAM_KIND,
            version: ZAKURA_STREAM_VERSION_1,
            frame_cap: LOCAL_MAX_CONTROL_FRAME_BYTES,
            capability: ZAKURA_CAP_LEGACY_GOSSIP,
            ..Stream::PERSISTENT
        };

        let limits = ZakuraConnectionLimits {
            max_frame_bytes: MAX_FRAME_BYTES,
            max_message_bytes: MAX_MESSAGE_BYTES,
            ..test_connection_limits()
        };
        // Production now passes the message-limited inbound cap; the raw
        // application cap (what the unfixed read path used) stays at the frame cap.
        let inbound_cap = inbound_frame_cap_for_stream(&limits, stream);
        let raw_cap = application_frame_cap(&limits, stream);
        assert!(
            inbound_cap < raw_cap,
            "the inbound cap must be tighter than the raw frame cap when the caps diverge \
             (inbound_cap={inbound_cap}, raw_cap={raw_cap})"
        );

        let _guard = zakura_test::init();
        let server = LocalEndpointFactory::new().endpoint(72).await?;
        let (conn_tx, _conn_rx) = mpsc::channel(2);
        let (stream_tx, mut stream_rx) = mpsc::channel(4);
        server.serve(CaptureConnection {
            alpn: ALPN,
            connection_tx: conn_tx,
            stream_tx,
        })?;
        let client = LocalEndpointFactory::new().endpoint(73).await?;
        let server_addr = LocalEndpointFactory::node_addr(&server).await;

        // A frame header (message_type, flags, payload_len) with no payload bytes.
        let frame_header = |payload_len: u32| -> Vec<u8> {
            let mut header = Vec::with_capacity(FRAME_HEADER_BYTES);
            header.extend_from_slice(&0u16.to_le_bytes());
            header.extend_from_slice(&0u16.to_le_bytes());
            header.extend_from_slice(&payload_len.to_le_bytes());
            header
        };

        // CaptureConnection forwards at most two streams per connection, so the
        // two oversized streams share one connection and the in-cap stream uses
        // another.
        let conn_a = timeout(
            Duration::from_secs(10),
            client.connect(server_addr.clone(), &[ALPN]),
        )
        .await
        .expect("client connects for the oversized streams")?;

        // Stream 1: oversized header read with the message-limited inbound cap.
        // The fix rejects it as Oversize from the header alone, before allocating.
        let (mut over_send, _over_recv) =
            timeout(Duration::from_secs(1), conn_a.open_bi())
                .await
                .expect("client opens the oversized inbound-cap stream")?;
        timeout(
            Duration::from_secs(1),
            over_send.write_all(&frame_header(OVER_MESSAGE_PAYLOAD_LEN)),
        )
        .await
        .expect("client writes the oversized frame header")?;
        let _ = over_send.finish();
        let (_s1_send, mut s1_recv) = timeout(Duration::from_secs(1), stream_rx.recv())
            .await
            .expect("server accepts the oversized inbound-cap stream")
            .expect("capture handler forwards the oversized inbound-cap stream");
        let rejected = read_frame(
            &mut s1_recv,
            inbound_cap,
            FrameFilter::new(None, InboundReader::Persistent),
            Duration::from_secs(2),
            Some(Duration::from_secs(2)),
        )
        .await;
        assert!(
            matches!(rejected, Err(ZakuraHandlerError::OversizeFrame { .. })),
            "a frame whose payload exceeds max_message_bytes must be rejected as Oversize \
             on the header alone with the inbound (message-limited) cap, before the payload \
             is allocated and read; got {rejected:?}"
        );

        // Stream 2: the SAME oversized header read with the raw frame cap an
        // unfixed path used. It passes the frame-cap size check, so read_frame
        // reads the body (failing only because
        // the body was never sent) -- i.e. NOT rejected as Oversize. This is the
        // allocate-before-reject amplification the fix removes.
        let (mut raw_send, _raw_recv) = timeout(Duration::from_secs(1), conn_a.open_bi())
            .await
            .expect("client opens the oversized raw-cap stream")?;
        timeout(
            Duration::from_secs(1),
            raw_send.write_all(&frame_header(OVER_MESSAGE_PAYLOAD_LEN)),
        )
        .await
        .expect("client writes the oversized frame header again")?;
        let _ = raw_send.finish();
        let (_s2_send, mut s2_recv) = timeout(Duration::from_secs(1), stream_rx.recv())
            .await
            .expect("server accepts the oversized raw-cap stream")
            .expect("capture handler forwards the oversized raw-cap stream");
        let allocated = read_frame(
            &mut s2_recv,
            raw_cap,
            FrameFilter::new(None, InboundReader::Persistent),
            Duration::from_secs(2),
            Some(Duration::from_secs(2)),
        )
        .await;
        assert!(
            allocated.is_err()
                && !matches!(
                    allocated,
                    Err(ZakuraHandlerError::Oversize | ZakuraHandlerError::OversizeFrame { .. })
                ),
            "with the raw frame cap the same oversized frame passes the size check and \
             read_frame proceeds to allocate/read the payload (it is not rejected as \
             Oversize), proving the message cap is enforced too late; got {allocated:?}"
        );

        // Stream 3 (fresh connection): a frame within the message cap must still
        // round-trip with the inbound cap, so the clamp rejects nothing legitimate.
        let conn_b = timeout(
            Duration::from_secs(10),
            client.connect(server_addr, &[ALPN]),
        )
        .await
        .expect("client connects for the in-cap stream")?;
        let valid_payload = vec![0x5au8; (MAX_MESSAGE_BYTES / 2) as usize];
        let mut valid_bytes =
            frame_header(u32::try_from(valid_payload.len()).expect("in-cap payload len fits u32"));
        valid_bytes.extend_from_slice(&valid_payload);
        let (mut ok_send, _ok_recv) = timeout(Duration::from_secs(1), conn_b.open_bi())
            .await
            .expect("client opens the in-cap stream")?;
        timeout(Duration::from_secs(1), ok_send.write_all(&valid_bytes))
            .await
            .expect("client writes the in-cap frame")?;
        let _ = ok_send.finish();
        let (_s3_send, mut s3_recv) = timeout(Duration::from_secs(1), stream_rx.recv())
            .await
            .expect("server accepts the in-cap stream")
            .expect("capture handler forwards the in-cap stream");
        let frame = read_frame(
            &mut s3_recv,
            inbound_cap,
            FrameFilter::new(None, InboundReader::Persistent),
            Duration::from_secs(2),
            Some(Duration::from_secs(2)),
        )
        .await
        .expect("a frame within the message cap is read with the inbound cap");
        assert_eq!(
            frame.payload, valid_payload,
            "an in-cap frame payload must round-trip unchanged"
        );

        conn_a.close(0u32.into(), b"done");
        conn_b.close(0u32.into(), b"done");
        client.shutdown().await;
        server.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn invalid_prelude_stream_churn_charges_open_rate_token() -> Result<(), BoxError> {
        // claude-invalid-stream-prelude-churn-bypasses-open-rate: a peer that
        // opens a stream naming an unregistered kind is reset stream-only and the
        // connection is kept. The per-connection stream-open rate token MUST be
        // charged for that protocol-invalid open; otherwise an authenticated peer
        // could churn bad-prelude/unknown-kind/unnegotiated stream opens forever
        // without ever spending open-rate budget. This drives admit_bi_stream
        // over a real QUIC transport and asserts the token was consumed even
        // though the stream itself was rejected as unknown-kind.
        const ALPN: &[u8] = b"/zakura/testkit/open-rate-churn/0";

        let _guard = zakura_test::init();
        let server = LocalEndpointFactory::new().endpoint(90).await?;
        let (conn_tx, _conn_rx) = mpsc::channel(1);
        let (stream_tx, mut stream_rx) = mpsc::channel(2);
        server.serve(CaptureConnection {
            alpn: ALPN,
            connection_tx: conn_tx,
            stream_tx,
        })?;
        let client = LocalEndpointFactory::new().endpoint(91).await?;
        let server_addr = LocalEndpointFactory::node_addr(&server).await;

        let client_conn = timeout(
            Duration::from_secs(10),
            client.connect(server_addr, &[ALPN]),
        )
        .await
        .expect("client connects to the open-rate-churn endpoint")?;

        // A well-formed prelude that names an unregistered stream kind. It parses
        // cleanly, so admission reaches the unknown-kind reject (a stream-only
        // reset that keeps the connection) rather than the bad-prelude path.
        let prelude = StreamPrelude {
            magic: STREAM_PRELUDE_MAGIC,
            stream_kind: 9,
            stream_version: 1,
            request_id: None,
            max_frame_bytes: 1024,
        };
        let (mut client_send, _client_recv) =
            timeout(Duration::from_secs(1), client_conn.open_bi())
                .await
                .expect("client opens the unknown-kind stream")?;
        timeout(
            Duration::from_secs(1),
            client_send.write_all(&prelude.encode()?),
        )
        .await
        .expect("client writes the unknown-kind prelude")?;
        let _ = client_send.finish();

        let (server_send, server_recv) = timeout(Duration::from_secs(1), stream_rx.recv())
            .await
            .expect("server accepts the unknown-kind stream")
            .expect("capture handler forwards the unknown-kind stream");

        let supervisor = ZakuraSupervisorHandle::new(16);
        let handler = ZakuraProtocolHandler::new(
            supervisor,
            Network::Mainnet,
            ZakuraHandshakeConfig::for_network(&Network::Mainnet),
            ZakuraLocalLimits::from_config(&Config::default()),
        );

        let peer_id = test_peer(9);
        let stream_sem = Arc::new(Semaphore::new(16));
        // A full bucket with a known capacity so the token charge is observable
        // as an exact decrement.
        let mut open_limiter = TokenBucket::new(4);
        let mut message_buckets = MessageRateBuckets::new();
        let mut workers = JoinSet::new();
        let connection_token = CancellationToken::new();
        let (freshness_tx, _freshness_rx) = watch::channel(Instant::now());

        let mut admission = StreamAdmission {
            is_initiator: false,
            direction: ServicePeerDirection::Inbound,
            conn: ZakuraConnTrace::placeholder(),
            peer_id: &peer_id,
            stream_sem: &stream_sem,
            resources: &ConnectionResources::unconstrained(&[]),
            open_limiter: &mut open_limiter,
            message_buckets: &mut message_buckets,
            workers: &mut workers,
            limits: test_connection_limits(),
            accepted_capabilities: 0,
            connection_token: connection_token.clone(),
            close_cause: CloseCause::new(),
            freshness_tx,
            session_funding: None,
        };
        let (session_exit_tx, _session_exit_rx) = mpsc::unbounded_channel();
        let admitted = handler
            .admit_bi_stream(
                server_send,
                server_recv,
                &mut admission,
                16,
                session_exit_tx,
                &mut PendingSessions::default(),
            )
            .await;

        assert!(
            admitted.is_none(),
            "an unknown-kind stream must be rejected, not admitted"
        );
        assert!(
            !connection_token.is_cancelled(),
            "an unknown-kind stream is reset stream-only and must keep the connection alive"
        );
        assert_eq!(
            admission.open_limiter.tokens, 3,
            "the protocol-invalid stream open must spend exactly one open-rate token \
             (capacity 4 -> 3); before the fix the unknown-kind reject returned before \
             reaching the limiter, leaving the bucket full at 4"
        );

        client.shutdown().await;
        server.shutdown().await;
        Ok(())
    }

    #[test]
    fn local_application_frame_cap_admits_default_header_sync_response() {
        let limits = ZakuraLocalLimits::from_config(&Config::default());
        let default_header_sync_frame_bytes =
            u32::try_from(MAX_HS_MESSAGE_BYTES.saturating_add(FRAME_HEADER_BYTES))
                .expect("header-sync frame cap fits in u32");

        assert!(limits.max_frame_bytes >= default_header_sync_frame_bytes);
        assert!(limits.initial_limits().max_frame_bytes >= default_header_sync_frame_bytes);
    }

    #[test]
    fn ordered_stream_stopped_write_is_stream_local() {
        let error: BoxError = zakura_quic::WriteError::Stopped(VarInt::from_u32(0)).into();

        assert!(ordered_stream_write_was_stopped(&error));
    }

    #[test]
    fn declared_stream_frame_caps_are_authoritative() {
        let limits = ZakuraLocalLimits::from_config(&Config::default());
        let negotiated = limits.clamp(&ZakuraAcceptedLimits {
            max_frame_bytes: u32::MAX,
            max_message_bytes: u32::MAX,
            max_open_streams: u16::MAX,
            max_inbound_queue_depth: u16::MAX,
            idle_timeout_millis: u32::MAX,
        });
        const CUSTOM_FRAME_CAP: u32 = 64 * 1024;
        let custom_stream = Stream {
            kind: 42,
            version: ZAKURA_STREAM_VERSION_1,
            frame_cap: CUSTOM_FRAME_CAP,
            capability: 1 << 20,
            ..Stream::REQUEST_RESPONSE
        };

        assert_eq!(
            application_frame_cap(&negotiated, custom_stream),
            CUSTOM_FRAME_CAP,
            "a custom declaration below the negotiated cap must remain authoritative"
        );
        assert_eq!(
            peer_accepted_frame_cap(&negotiated, custom_stream, CUSTOM_FRAME_CAP / 2),
            CUSTOM_FRAME_CAP / 2,
            "writes on peer-opened streams must also honor the peer's advertised receive cap"
        );
        assert_eq!(
            peer_accepted_frame_cap(&negotiated, custom_stream, CUSTOM_FRAME_CAP * 2),
            CUSTOM_FRAME_CAP,
            "a peer cannot raise the locally declared stream cap"
        );

        let at_cap = Frame {
            message_type: 1,
            flags: 0,
            payload: vec![
                0;
                usize::try_from(CUSTOM_FRAME_CAP)
                    .expect("custom test cap fits usize")
                    .saturating_sub(FRAME_HEADER_BYTES)
            ],
        };
        let over_cap = Frame {
            message_type: 1,
            flags: 0,
            payload: vec![0; at_cap.payload.len() + 1],
        };

        assert!(
            at_cap
                .encode(application_frame_cap(&negotiated, custom_stream))
                .is_ok(),
            "a frame exactly at the declared custom cap must be accepted"
        );
        assert!(
            over_cap
                .encode(application_frame_cap(&negotiated, custom_stream))
                .is_err(),
            "a frame one byte over the declared custom cap must be rejected"
        );
    }

    #[test]
    fn block_sync_needs_two_slots_per_ordered_stream() {
        let block_sync = crate::zakura::block_sync_streams()[0];
        let other = Stream {
            kind: 5,
            messages: None,
            ..block_sync
        };
        assert_eq!(minimum_ordered_queue_capacity(&[other]), 1);
        assert_eq!(minimum_ordered_queue_capacity(&[block_sync]), 2);
        assert_eq!(minimum_ordered_queue_capacity(&[block_sync, other]), 4);
    }

    #[test]
    fn token_bucket_rejects_churn_until_refill() {
        let clock = crate::zakura::testkit::TestClock::new();
        let mut bucket = TokenBucket::with_clock(2, clock.clone());

        assert!(bucket.try_take());
        assert!(bucket.try_take());
        assert!(!bucket.try_take());
        clock.advance(Duration::from_millis(500));
        assert!(bucket.try_take());
        assert!(!bucket.try_take());
        clock.advance(Duration::from_millis(500));
        assert!(bucket.try_take());
    }

    #[test]
    fn supported_stream_accepts_registered_kinds_at_declared_version_only() {
        let streams = vec![
            Stream {
                kind: LEGACY_GOSSIP_STREAM_KIND,
                version: ZAKURA_STREAM_VERSION_1,
                frame_cap: 1024,
                capability: ZAKURA_CAP_LEGACY_GOSSIP,
                ..Stream::PERSISTENT
            },
            Stream {
                kind: LEGACY_REQUEST_STREAM_KIND,
                version: ZAKURA_STREAM_VERSION_1,
                frame_cap: 1024,
                capability: ZAKURA_CAP_LEGACY_GOSSIP,
                ..Stream::REQUEST_RESPONSE
            },
            Stream {
                kind: DISCOVERY_STREAM_KIND,
                version: ZAKURA_STREAM_VERSION_1,
                frame_cap: 1024,
                capability: ZAKURA_CAP_DISCOVERY,
                ..Stream::PERSISTENT
            },
            Stream {
                kind: HEADER_SYNC_STREAM_KIND,
                version: ZAKURA_HEADER_SYNC_STREAM_VERSION,
                frame_cap: 1024,
                capability: ZAKURA_CAP_HEADER_SYNC,
                ..Stream::PERSISTENT
            },
            Stream {
                kind: ZAKURA_STREAM_BLOCK_SYNC,
                version: ZAKURA_STREAM_VERSION_1,
                frame_cap: MAX_BS_FRAME_BYTES,
                capability: crate::zakura::ZAKURA_CAP_BLOCK_SYNC_REGULATED,
                ..Stream::PERSISTENT
            },
        ];
        let services = streams
            .into_iter()
            .map(|stream| -> Arc<dyn Service> {
                Arc::new(DeclaredStreamService {
                    streams: vec![stream],
                })
            })
            .collect();
        let registry =
            ServiceRegistry::new(services).expect("test registry declares unique stream kinds");

        for (kind, version) in [
            (LEGACY_GOSSIP_STREAM_KIND, ZAKURA_STREAM_VERSION_1),
            (LEGACY_REQUEST_STREAM_KIND, ZAKURA_STREAM_VERSION_1),
            (DISCOVERY_STREAM_KIND, ZAKURA_STREAM_VERSION_1),
            (HEADER_SYNC_STREAM_KIND, ZAKURA_HEADER_SYNC_STREAM_VERSION),
            (ZAKURA_STREAM_BLOCK_SYNC, ZAKURA_STREAM_VERSION_1),
        ] {
            assert!(
                is_supported_stream(&registry, kind, version),
                "registered kind {kind} at declared version {version} must be supported"
            );
            assert!(
                !is_supported_stream(&registry, kind, 0),
                "registered kind {kind} at version 0 must be rejected"
            );
            assert!(
                !is_supported_stream(&registry, kind, u16::MAX),
                "registered kind {kind} at an unsupported version must be rejected"
            );
        }

        assert!(
            !is_supported_stream(&registry, HEADER_SYNC_STREAM_KIND, ZAKURA_STREAM_VERSION_1),
            "header-sync v1 is rejected because native header sync uses exact version matching"
        );
        assert!(
            !is_supported_stream(&registry, HEADER_SYNC_STREAM_KIND, 5),
            "header-sync v5 is rejected after the expanded Ironwood root-record wire break"
        );
        assert!(
            !is_supported_stream(&registry, HEADER_SYNC_STREAM_KIND, 8),
            "the predecessor header-sync stream version is rejected"
        );
        for retired in [1 << 4, 1 << 5] {
            assert!(
                registry
                    .persistent_streams_for_negotiated(retired)
                    .is_empty(),
                "a retired header-sync capability opens no header-sync stream"
            );
        }

        assert_eq!(stream_kind_label(2), "gossip");
        assert_eq!(stream_kind_label(3), "legacy_request");
        assert_eq!(stream_kind_label(4), "discovery");
        assert_eq!(stream_kind_label(5), "header_sync");
        assert_eq!(stream_kind_label(6), "block_sync");

        for kind in [0u16, 1, 7, 255, u16::MAX] {
            assert!(
                !is_supported_stream(&registry, kind, ZAKURA_STREAM_VERSION_1),
                "unknown kind {kind} must be rejected even at version 1"
            );
        }

        for kind in [7u16, 255, u16::MAX] {
            assert_eq!(stream_kind_label(kind), "unknown");
        }
    }

    #[test]
    fn legacy_transport_response_validator_accepts_codec_frames() -> Result<(), BoxError> {
        let block = Arc::new(Block::zcash_deserialize(
            BLOCK_TESTNET_141042_BYTES.as_slice(),
        )?);
        let header = block::CountedHeader {
            header: block.header.clone(),
        };
        let tx_id = legacy_tx_id(7);

        assert_codec_frames_validate_at_transport(
            LegacyRequestFrame::BlocksByHash(vec![block.hash()]),
            LegacyRequestKind::Blocks,
            Response::Blocks(vec![InventoryResponse::Available((block.into(), None))]),
        )?;
        assert_codec_frames_validate_at_transport(
            LegacyRequestFrame::TransactionsById(vec![tx_id]),
            LegacyRequestKind::Transactions,
            Response::Transactions(vec![InventoryResponse::Missing(tx_id)]),
        )?;
        assert_codec_frames_validate_at_transport(
            LegacyRequestFrame::FindBlocks {
                known_blocks: vec![block_hash(1)],
                stop: None,
            },
            LegacyRequestKind::FindBlocks,
            Response::BlockHashes(vec![block_hash(2)]),
        )?;
        assert_codec_frames_validate_at_transport(
            LegacyRequestFrame::FindHeaders {
                known_blocks: vec![block_hash(1)],
                stop: None,
            },
            LegacyRequestKind::FindHeaders,
            Response::BlockHeaders(vec![header.header.clone().into()]),
        )?;
        assert_codec_frames_validate_at_transport(
            LegacyRequestFrame::MempoolTransactionIds,
            LegacyRequestKind::MempoolTransactionIds,
            Response::TransactionIds(vec![tx_id]),
        )?;
        assert_codec_frames_validate_at_transport(
            LegacyRequestFrame::Ping,
            LegacyRequestKind::Ping,
            Response::Pong(Duration::ZERO),
        )?;
        assert_codec_frames_validate_at_transport(
            LegacyRequestFrame::PushTransaction(empty_v5_transaction(8).into()),
            LegacyRequestKind::PushTransaction,
            Response::Nil,
        )?;

        // The inbound service answers an empty FindBlocks/FindHeaders/
        // MempoolTransactionIds with `Response::Nil`, so a lone nil frame must
        // round-trip (encode -> transport validate -> decode) for those data
        // request kinds too, not only PushTransaction.
        assert_codec_frames_validate_at_transport(
            LegacyRequestFrame::FindBlocks {
                known_blocks: vec![block_hash(1)],
                stop: None,
            },
            LegacyRequestKind::FindBlocks,
            Response::Nil,
        )?;
        assert_codec_frames_validate_at_transport(
            LegacyRequestFrame::FindHeaders {
                known_blocks: vec![block_hash(1)],
                stop: None,
            },
            LegacyRequestKind::FindHeaders,
            Response::Nil,
        )?;
        assert_codec_frames_validate_at_transport(
            LegacyRequestFrame::MempoolTransactionIds,
            LegacyRequestKind::MempoolTransactionIds,
            Response::Nil,
        )?;

        Ok(())
    }

    /// Completed chunks hand their decoded allocation to the response codec,
    /// while requested-inventory binding still runs at response assembly.
    #[test]
    fn legacy_transport_reuses_decoded_inventory() -> Result<(), BoxError> {
        use zakura_node_services::block_memory::BlockHandle;

        let limits = test_connection_limits();
        let request_id = 99;
        let block = Arc::new(Block::zcash_deserialize(
            BLOCK_TESTNET_141042_BYTES.as_slice(),
        )?);
        let transaction = transaction::UnminedTx::from(block.transactions[0].clone());
        let missing_hash = block_hash(99);
        let missing_id = legacy_tx_id(99);
        let cases = [
            (
                LegacyRequestFrame::BlocksByHash(vec![block.hash(), missing_hash]),
                LegacyRequestKind::Blocks,
                Response::Blocks(vec![
                    InventoryResponse::Available((block.clone().into(), None)),
                    InventoryResponse::Missing(missing_hash),
                ]),
                RequestedInventory::Blocks([block.hash(), missing_hash].into_iter().collect()),
            ),
            (
                LegacyRequestFrame::TransactionsById(vec![transaction.id(), missing_id]),
                LegacyRequestKind::Transactions,
                Response::Transactions(vec![
                    InventoryResponse::Available((transaction.clone(), None)),
                    InventoryResponse::Missing(missing_id),
                ]),
                RequestedInventory::Transactions(
                    [transaction.id(), missing_id].into_iter().collect(),
                ),
            ),
        ];

        for (request, kind, response, requested) in cases {
            let request = request.encode_frame()?;
            let budget =
                LegacyResponseBudget::from_request(request.message_type, &request.payload, limits)
                    .map_err(|error| -> BoxError { format!("{error:?}").into() })?;
            let item_bytes = match kind {
                LegacyRequestKind::Blocks => BLOCK_TESTNET_141042_BYTES.len(),
                LegacyRequestKind::Transactions => transaction.size(),
                _ => unreachable!("the cases only fetch inventory"),
            };
            let frame_cap = u32::try_from(
                FRAME_HEADER_BYTES + LEGACY_RESPONSE_CHUNK_HEADER_BYTES + item_bytes.div_ceil(2),
            )?;
            let frames = LegacyResponseCodec::encode_response(
                request_id,
                response.clone(),
                frame_cap,
                limits.max_message_bytes,
            )?;
            assert!(frames.len() > 2, "the available item spans multiple chunks");
            let mut state = LegacyResponseReadState::new(budget);
            let mut items = Vec::new();
            for frame in frames {
                if let Some(item) = state
                    .validate_frame(request_id, frame)
                    .map_err(|error| -> BoxError { format!("{error:?}").into() })?
                {
                    items.push(item);
                }
            }
            state
                .finish()
                .map_err(|error| -> BoxError { format!("{error:?}").into() })?;
            assert_eq!(
                items.len(),
                2,
                "raw chunks are replaced by one decoded item"
            );

            let decoded = match &items[0] {
                LegacyResponseItem::Block(block) => LegacyResponseItem::Block(block.clone()),
                LegacyResponseItem::Transaction(transaction) => {
                    LegacyResponseItem::Transaction(transaction.clone())
                }
                LegacyResponseItem::Frame(_) => panic!("the reader decoded the available item"),
            };
            let unrelated = match kind {
                LegacyRequestKind::Blocks => RequestedInventory::Blocks(Default::default()),
                LegacyRequestKind::Transactions => {
                    RequestedInventory::Transactions(Default::default())
                }
                _ => unreachable!("the cases only fetch inventory"),
            };
            assert!(matches!(
                LegacyResponseCodec::decode_response(request_id, kind, [decoded], Some(&unrelated),),
                Err(LegacyGossipError::UnsolicitedBlock(_)
                    | LegacyGossipError::UnsolicitedTransaction(_))
            ));

            // Keep a handle only to verify that assembly reuses the allocation.
            let (decoded_block, decoded_transaction) = match &items[0] {
                LegacyResponseItem::Block(block) => (Some(block.clone()), None),
                LegacyResponseItem::Transaction(transaction) => {
                    (None, Some(transaction.transaction().clone()))
                }
                LegacyResponseItem::Frame(_) => unreachable!("the item is decoded"),
            };
            let assembled =
                LegacyResponseCodec::decode_response(request_id, kind, items, Some(&requested))?;
            assert_eq!(assembled, response);
            match assembled {
                Response::Blocks(blocks) => {
                    let InventoryResponse::Available((block, None)) = &blocks[0] else {
                        panic!("the available block remains shared");
                    };
                    let BlockHandle::Shared(block) = &**block else {
                        panic!("decoded block is shared");
                    };
                    assert!(Arc::ptr_eq(block, &decoded_block.unwrap()));
                }
                Response::Transactions(transactions) => {
                    let InventoryResponse::Available((transaction, None)) = &transactions[0] else {
                        panic!("the first transaction is available");
                    };
                    assert!(Arc::ptr_eq(
                        transaction.transaction(),
                        &decoded_transaction.unwrap(),
                    ));
                }
                _ => unreachable!("the cases only fetch inventory"),
            }
        }
        Ok(())
    }

    /// A block response whose stream ends mid-item is request-local, but a list
    /// frame interleaved into the unfinished item stays connection-fatal.
    #[test]
    fn stream_ending_mid_item_is_local_and_interleaving_is_fatal() {
        let request_id: u64 = 7;
        let state = || {
            LegacyResponseReadState::new(LegacyResponseBudget {
                kind: LegacyResponseKind::Blocks,
                max_items: 1,
                max_frames: 4,
                max_bytes: MAX_PROTOCOL_MESSAGE_LEN,
                max_message_bytes: MAX_PROTOCOL_MESSAGE_LEN,
            })
        };
        let mut payload = request_id.to_le_bytes().to_vec();
        payload.push(0);
        payload.extend_from_slice(&[0; 16]);
        let first_chunk = Frame {
            message_type: LEGACY_RESPONSE_BLOCK,
            flags: 0,
            payload,
        };

        let mut ended = state();
        ended
            .validate_frame(request_id, first_chunk.clone())
            .expect("a non-final chunk is accepted");
        let finished = ended.finish();
        assert!(
            matches!(finished, Err(OutboundRequestError::Local(_))),
            "a stream ending mid-item must be Local; got {finished:?}"
        );

        let mut interleaved = state();
        interleaved
            .validate_frame(request_id, first_chunk.clone())
            .expect("a non-final chunk is accepted");
        let mut missing = request_id.to_le_bytes().to_vec();
        missing.push(0);
        let missing = Frame {
            message_type: LEGACY_RESPONSE_MISSING_BLOCKS,
            flags: 0,
            payload: missing,
        };
        let result = interleaved.validate_frame(request_id, missing.clone());
        assert!(
            matches!(result, Err(OutboundRequestError::Fatal(_))),
            "a frame interleaved into an unfinished item must stay Fatal; got {result:?}"
        );
    }

    #[test]
    fn legacy_header_precheck_tracks_remaining_bytes_across_frames() {
        let mut state = LegacyResponseReadState::new(LegacyResponseBudget {
            kind: LegacyResponseKind::Blocks,
            max_items: 1,
            max_frames: 10,
            max_bytes: 18,
            max_message_bytes: 1024,
        });
        let mut payload = 1u64.to_le_bytes().to_vec();
        payload.push(0); // An empty missing-block inventory.
        let frame = Frame {
            message_type: LEGACY_RESPONSE_MISSING_BLOCKS,
            flags: 0,
            payload,
        };
        for remaining in [18, 9, 0] {
            let filter =
                FrameFilter::new(None, InboundReader::Requester).with_precheck(Some(&state));
            assert_eq!(
                filter.check_header(frame.message_type, 0, remaining, 1024),
                Ok(1024),
            );
            assert_eq!(
                filter.check_header(frame.message_type, 0, remaining + 1, 1024),
                Err(FrameRejection::AboveReservation {
                    bytes: u64::try_from(remaining).unwrap(),
                }),
            );
            if remaining != 0 {
                state.validate_frame(1, frame.clone()).unwrap();
            }
        }
    }

    /// The reader rejects trailing bytes before handing off decoded inventory.
    #[test]
    fn legacy_completed_item_rejects_trailing_bytes() -> Result<(), BoxError> {
        let limits = test_connection_limits();
        let request_id = 99;
        let transaction = transaction::UnminedTx::from(empty_v5_transaction(24));
        let block = Arc::new(Block::zcash_deserialize(
            BLOCK_MAINNET_GENESIS_BYTES.as_slice(),
        )?);
        let cases = [
            (
                LegacyRequestFrame::TransactionsById(vec![transaction.id()]),
                Response::Transactions(vec![InventoryResponse::Available((transaction, None))]),
            ),
            (
                LegacyRequestFrame::BlocksByHash(vec![block.hash()]),
                Response::Blocks(vec![InventoryResponse::Available((block.into(), None))]),
            ),
        ];
        for (request, response) in cases {
            let request = request.encode_frame()?;
            let budget =
                LegacyResponseBudget::from_request(request.message_type, &request.payload, limits)
                    .map_err(|error| -> BoxError { format!("{error:?}").into() })?;
            let mut frames = LegacyResponseCodec::encode_response(
                request_id,
                response,
                limits.max_frame_bytes,
                limits.max_message_bytes,
            )?;
            let [frame] = frames.as_mut_slice() else {
                panic!("a small inventory item encodes as one frame");
            };

            let mut state = LegacyResponseReadState::new(budget);
            state
                .validate_frame(request_id, frame.clone())
                .map_err(|error| -> BoxError { format!("{error:?}").into() })?;

            frame.payload.extend_from_slice(&[0; 16]);
            let mut state = LegacyResponseReadState::new(budget);
            assert!(matches!(
                state.validate_frame(request_id, frame.clone()),
                Err(OutboundRequestError::Fatal(_)),
            ));
        }

        Ok(())
    }

    fn assert_codec_frames_validate_at_transport(
        request: LegacyRequestFrame,
        request_kind: LegacyRequestKind,
        response: Response,
    ) -> Result<(), BoxError> {
        let limits = test_connection_limits();
        let request_id = 99;
        let request_frame = request.encode_frame()?;
        let budget = LegacyResponseBudget::from_request(
            request_frame.message_type,
            &request_frame.payload,
            limits,
        )
        .map_err(|error| -> BoxError { format!("{error:?}").into() })?;
        let frames = LegacyResponseCodec::encode_response(
            request_id,
            response,
            limits.max_frame_bytes,
            limits.max_message_bytes,
        )?;
        let expected =
            LegacyResponseCodec::decode_response(request_id, request_kind, frames.clone(), None)?;

        let mut state = LegacyResponseReadState::new(budget);
        let mut items = Vec::new();
        for frame in &frames {
            FrameFilter::new(None, InboundReader::Requester)
                .with_precheck(Some(&state))
                .check_header(
                    frame.message_type,
                    frame.flags,
                    frame.payload.len(),
                    usize::MAX,
                )?;
            if let Some(item) = state
                .validate_frame(request_id, frame.clone())
                .map_err(|error| -> BoxError { format!("{error:?}").into() })?
            {
                items.push(item);
            }
        }
        state
            .finish()
            .map_err(|error| -> BoxError { format!("{error:?}").into() })?;
        assert_eq!(
            LegacyResponseCodec::decode_response(request_id, request_kind, items, None)?,
            expected,
        );

        Ok(())
    }

    fn test_connection_limits() -> ZakuraConnectionLimits {
        let max_protocol_message_len =
            u32::try_from(MAX_PROTOCOL_MESSAGE_LEN).expect("protocol message length fits in u32");

        ZakuraConnectionLimits {
            max_frame_bytes: max_protocol_message_len,
            max_message_bytes: max_protocol_message_len,
            max_open_streams: 16,
            max_inbound_queue_depth: 16,
            idle_timeout: Duration::from_secs(1),
            prelude_timeout: Duration::from_secs(1),
            control_timeout: Duration::from_secs(1),
            stream_open_rate_per_second: 16,
            message_rate_per_second: 128,
        }
    }

    fn block_hash(byte: u8) -> block::Hash {
        block::Hash([byte; 32])
    }

    fn legacy_tx_id(byte: u8) -> UnminedTxId {
        UnminedTxId::from_legacy_id(transaction::Hash([byte; 32]))
    }

    fn empty_v5_transaction(byte: u8) -> transaction::Transaction {
        transaction::Transaction::V5 {
            network_upgrade: zakura_chain::parameters::NetworkUpgrade::Nu5,
            lock_time: transaction::LockTime::min_lock_time_timestamp(),
            expiry_height: block::Height(u32::from(byte)),
            inputs: Vec::new(),
            outputs: Vec::new(),
            sapling_shielded_data: None,
            orchard_shielded_data: None,
        }
    }

    #[test]
    fn same_kind_streams_share_one_connection_message_budget() {
        // FLUP-014: two workers serving the SAME kind on ONE connection must
        // draw from a single shared bucket, so opening a second stream does not
        // hand the peer a fresh full budget. Driven by TestClock so the aggregate
        // is asserted on return values, not timing or metrics.
        let clock = crate::zakura::testkit::TestClock::new();
        let mut buckets: MessageRateBuckets<crate::zakura::testkit::TestClock> =
            MessageRateBuckets::new();
        let rate = 4;

        // Two streams of the same kind get clones of the SAME bucket.
        let stream_a = message_bucket_for(&mut buckets, 1, rate, clock.clone());
        let stream_b = message_bucket_for(&mut buckets, 1, rate, clock.clone());
        assert!(
            Arc::ptr_eq(&stream_a, &stream_b),
            "same kind must reuse one shared bucket"
        );
        assert_eq!(
            buckets.len(),
            1,
            "no extra bucket created for the same kind"
        );

        // The aggregate across both handles is capped at the single-bucket budget.
        let take = |bucket: &SharedMessageBucket<crate::zakura::testkit::TestClock>| {
            bucket
                .lock()
                .expect("test bucket mutex is never poisoned")
                .try_take()
        };
        let mut accepted = 0;
        for _ in 0..rate as usize * 4 {
            if take(&stream_a) {
                accepted += 1;
            }
            if take(&stream_b) {
                accepted += 1;
            }
        }
        assert_eq!(
            accepted, rate as usize,
            "the second same-kind stream must NOT get a fresh full budget"
        );

        // After a full refill window the shared budget recovers to one capacity,
        // still shared across both streams (not doubled).
        clock.advance(Duration::from_secs(1));
        let mut refilled = 0;
        for _ in 0..rate as usize * 4 {
            if take(&stream_a) {
                refilled += 1;
            }
            if take(&stream_b) {
                refilled += 1;
            }
        }
        assert_eq!(
            refilled, rate as usize,
            "refill restores one shared budget, not one-per-stream"
        );
    }

    #[test]
    fn different_kinds_get_independent_message_budgets() {
        // FLUP-014: the bucket is keyed per stream-kind, so distinct known kinds
        // do not contend for the same budget.
        let clock = crate::zakura::testkit::TestClock::new();
        let mut buckets: MessageRateBuckets<crate::zakura::testkit::TestClock> =
            MessageRateBuckets::new();

        let request = message_bucket_for(&mut buckets, 1, 2, clock.clone());
        let gossip = message_bucket_for(&mut buckets, 2, 2, clock.clone());
        assert!(
            !Arc::ptr_eq(&request, &gossip),
            "distinct kinds must not share a bucket"
        );
        assert_eq!(buckets.len(), 2);

        let take = |bucket: &SharedMessageBucket<crate::zakura::testkit::TestClock>| {
            bucket
                .lock()
                .expect("test bucket mutex is never poisoned")
                .try_take()
        };
        // Drain the request budget; gossip must be untouched.
        assert!(take(&request));
        assert!(take(&request));
        assert!(!take(&request));
        assert!(take(&gossip));
        assert!(take(&gossip));
        assert!(!take(&gossip));
    }

    #[test]
    fn idle_invariant_keeps_app_timeout_below_quic_timeout() {
        let limits = ZakuraLocalLimits::from_config(&Config::default());

        validate_idle_invariant(&limits).expect("default Zakura limits satisfy idle invariant");
        assert!(
            (limits.initial_limits().idle_timeout_millis as u128)
                < limits.quic_idle_timeout.as_millis()
        );
    }

    // SECURITY AUDIT (candidate claude-legacy-requester-response-frame-growth /
    // trace-gossip-response-reassembler-frame-vector-growth): SR-4 amplification.
    //
    // `write_outbound_request_frame_inner` accumulates every validated response
    // Frame into a `Vec<Frame>` until the responder closes the stream, then hands
    // the whole vector to `decode_response`. The only thing bounding how much it
    // retains is `LegacyResponseBudget::max_bytes`, which `validate_frame` checks
    // as a cumulative byte budget. For Blocks/Transactions that budget was
    // derived as `item_count * max_message_bytes`, so a request naming the
    // protocol-max inventory count (`MAX_TX_INV_IN_SENT_MESSAGE` = 25_000) handed
    // a hostile responder a ~50 GiB retained-frame budget before any decode.
    //
    // The inbound responder already caps a single response's cumulative payload
    // at `COMPAT_RESPONSE_MAX_AGGREGATE_BYTES` (8 * MAX_PROTOCOL_MESSAGE_LEN), so
    // an honest peer never sends more than that. The requester must clamp its
    // retained-frame budget to the same operational aggregate. This test asserts
    // the budget is bounded regardless of requested item count; it FAILS before
    // the fix (budget ~= 50 GiB) and passes after. Do not weaken it to pass.
    #[test]
    fn requester_response_budget_is_capped_for_large_inventory_request() {
        let limits = test_connection_limits();
        assert_eq!(
            limits.max_message_bytes as usize, MAX_PROTOCOL_MESSAGE_LEN,
            "fixture should negotiate the protocol-max message cap so the unclamped \
             budget is maximal",
        );

        let max_items =
            usize::try_from(MAX_TX_INV_IN_SENT_MESSAGE).expect("inventory cap fits in usize");

        // A BlocksByHash request naming the protocol-max inventory count must not
        // grant a retained-frame byte budget above the operational aggregate cap.
        let blocks = LegacyRequestFrame::BlocksByHash(vec![block_hash(7); max_items])
            .encode_frame()
            .expect("max-inventory blocks request encodes");
        let blocks_budget =
            LegacyResponseBudget::from_request(blocks.message_type, &blocks.payload, limits)
                .expect("budget derives from a max-inventory blocks request");
        assert!(
            blocks_budget.max_bytes <= COMPAT_RESPONSE_MAX_AGGREGATE_BYTES,
            "BlocksByHash retained-frame budget {} must be clamped to the aggregate cap {}",
            blocks_budget.max_bytes,
            COMPAT_RESPONSE_MAX_AGGREGATE_BYTES,
        );

        // The transaction-fetch path scales identically and must be clamped too.
        let txs = LegacyRequestFrame::TransactionsById(vec![legacy_tx_id(7); max_items])
            .encode_frame()
            .expect("max-inventory transactions request encodes");
        let txs_budget = LegacyResponseBudget::from_request(txs.message_type, &txs.payload, limits)
            .expect("budget derives from a max-inventory transactions request");
        assert!(
            txs_budget.max_bytes <= COMPAT_RESPONSE_MAX_AGGREGATE_BYTES,
            "TransactionsById retained-frame budget {} must be clamped to the aggregate cap {}",
            txs_budget.max_bytes,
            COMPAT_RESPONSE_MAX_AGGREGATE_BYTES,
        );

        // The clamp must only remove the unbounded tail: a modest request still
        // has to accept at least one full negotiated message, otherwise we would
        // wrongly reject honest single-item responses.
        let small = LegacyRequestFrame::BlocksByHash(vec![block_hash(1)])
            .encode_frame()
            .expect("single-item blocks request encodes");
        let small_budget =
            LegacyResponseBudget::from_request(small.message_type, &small.payload, limits)
                .expect("budget derives from a single-item blocks request");
        assert!(
            small_budget.max_bytes >= limits.max_message_bytes as usize,
            "a single-item request must still permit one full response message; budget was {}",
            small_budget.max_bytes,
        );
        assert!(
            small_budget.max_bytes <= COMPAT_RESPONSE_MAX_AGGREGATE_BYTES,
            "even a single-item budget stays within the aggregate cap",
        );
    }

    /// A lone, well-formed NIL answer to an inventory fetch is request-local, so
    /// the peer stays connected and the caller falls back; NIL to a Ping, NIL
    /// with the wrong request id, and NIL mixed with other frames in either
    /// order stay connection-fatal.
    /// A `FindBlocks` response may carry the responder's 500-hash limit, and
    /// one more hash is connection-fatal.
    #[test]
    fn find_blocks_response_is_bounded_by_responder_limit() {
        let limits = test_connection_limits();
        let request_id = 5;
        let request = LegacyRequestFrame::FindBlocks {
            known_blocks: vec![block_hash(1)],
            stop: None,
        }
        .encode_frame()
        .expect("a one-hash locator is within the request limits");

        for (count, fatal) in [
            (MAX_FIND_BLOCKS_RESPONSE_HASHES, false),
            (MAX_FIND_BLOCKS_RESPONSE_HASHES + 1, true),
        ] {
            let hashes = (0..count)
                .map(|index| {
                    let mut hash = [0; 32];
                    hash[..8].copy_from_slice(
                        &u64::try_from(index)
                            .expect("at most 501 hashes, so the index fits u64")
                            .to_le_bytes(),
                    );
                    block::Hash(hash)
                })
                .collect();
            let frame = LegacyResponseCodec::encode_response(
                request_id,
                Response::BlockHashes(hashes),
                limits.max_frame_bytes,
                limits.max_message_bytes,
            )
            .expect("501 hashes are within the 25,000-item encoding cap")
            .pop()
            .expect("a 501-hash list fits in one frame under the test limits");
            let mut state = LegacyResponseReadState::new(
                LegacyResponseBudget::from_request(request.message_type, &request.payload, limits)
                    .expect("a FindBlocks request has a known response kind"),
            );

            let result = state.validate_frame(request_id, frame.clone());
            if fatal {
                assert!(
                    matches!(result, Err(OutboundRequestError::Fatal(_))),
                    "{count} hashes must be Fatal; got {result:?}"
                );
            } else {
                assert!(result.is_ok(), "{count} hashes must pass; got {result:?}");
            }
        }

        // A repeated hash is rejected as the frame arrives, before the stream
        // ends, so a peer cannot avoid it by holding the stream open.
        let frame = LegacyResponseCodec::encode_response(
            request_id,
            Response::BlockHashes(vec![block_hash(1), block_hash(2), block_hash(1)]),
            limits.max_frame_bytes,
            limits.max_message_bytes,
        )
        .expect("three hashes are within the 25,000-item encoding cap")
        .pop()
        .expect("a three-hash list fits in one frame under the test limits");
        let mut state = LegacyResponseReadState::new(
            LegacyResponseBudget::from_request(request.message_type, &request.payload, limits)
                .expect("a FindBlocks request has a known response kind"),
        );
        let result = state.validate_frame(request_id, frame.clone());
        assert!(
            matches!(result, Err(OutboundRequestError::Fatal(_))),
            "a repeated hash must be Fatal; got {result:?}"
        );
    }

    #[test]
    fn nil_response_is_local_for_inventory_and_fatal_for_ping() {
        let limits = test_connection_limits();
        let request_id = 99;

        let cases = [
            (LegacyRequestFrame::BlocksByHash(vec![block_hash(1)]), false),
            (
                LegacyRequestFrame::TransactionsById(vec![legacy_tx_id(2)]),
                false,
            ),
            (LegacyRequestFrame::Ping, true),
        ];

        for (request, fatal) in cases {
            let request_frame = request.encode_frame().expect("request frame encodes");
            let budget = || {
                LegacyResponseBudget::from_request(
                    request_frame.message_type,
                    &request_frame.payload,
                    limits,
                )
                .expect("budget derives from request")
            };
            let nil = |id| {
                LegacyResponseCodec::encode_response(
                    id,
                    Response::Nil,
                    limits.max_frame_bytes,
                    limits.max_message_bytes,
                )
                .expect("nil response encodes")
                .pop()
                .expect("nil encodes to one frame")
            };

            let mut lone = LegacyResponseReadState::new(budget());
            let result = lone
                .validate_frame(request_id, nil(request_id))
                .and_then(|_| lone.finish());
            if fatal {
                assert!(
                    matches!(result, Err(OutboundRequestError::Fatal(_))),
                    "NIL to {request:?} must be Fatal; got {result:?}",
                );
            } else {
                assert!(
                    matches!(result, Err(OutboundRequestError::Local(_))),
                    "a lone NIL to {request:?} must be Local; got {result:?}",
                );
            }

            let wrong_id = LegacyResponseReadState::new(budget())
                .validate_frame(request_id, nil(request_id + 1));
            assert!(
                matches!(wrong_id, Err(OutboundRequestError::Fatal(_))),
                "a wrong-id NIL to {request:?} must stay Fatal; got {wrong_id:?}",
            );

            if let LegacyRequestFrame::BlocksByHash(hashes) = &request {
                let missing = || {
                    LegacyResponseCodec::encode_response(
                        request_id,
                        Response::Blocks(vec![InventoryResponse::Missing(hashes[0])]),
                        limits.max_frame_bytes,
                        limits.max_message_bytes,
                    )
                    .expect("missing response encodes")
                    .pop()
                    .expect("a missing entry encodes to one frame")
                };

                let mut after_items = LegacyResponseReadState::new(budget());
                after_items
                    .validate_frame(request_id, missing())
                    .expect("a missing entry is accepted");
                let result = after_items.validate_frame(request_id, nil(request_id));
                assert!(
                    matches!(result, Err(OutboundRequestError::Fatal(_))),
                    "NIL after inventory frames must stay Fatal; got {result:?}",
                );

                let mut before_items = LegacyResponseReadState::new(budget());
                before_items
                    .validate_frame(request_id, nil(request_id))
                    .expect("a first NIL is held until the stream ends");
                let result = before_items.validate_frame(request_id, missing());
                assert!(
                    matches!(result, Err(OutboundRequestError::Fatal(_))),
                    "inventory frames after NIL must stay Fatal; got {result:?}",
                );
            }
        }
    }

    #[tokio::test]
    async fn nat_traversal_fails_startup_and_names_the_spec() {
        let mut config = Config::for_test(P2pStack::Zakura);
        config.zakura.nat_traversal = true;
        let error = spawn_zakura_endpoint(&config, |_, _| Arc::new(NoopService))
            .await
            .expect_err("nat_traversal = true must fail at startup");
        let message = error.to_string();
        assert!(message.contains("hole punching"), "{message}");
        assert!(message.contains("docs/specs/zakura-quic.md"), "{message}");
    }

    #[tokio::test]
    async fn inbound_connection_is_charged_to_its_admitted_ip() -> Result<(), BoxError> {
        let _guard = zakura_test::init();
        tokio::time::timeout(Duration::from_secs(30), async {
            #[derive(Clone, Debug)]
            struct CaptureAccepted {
                handler: ZakuraProtocolHandler,
                accepted: mpsc::Sender<Connection>,
            }
            impl Acceptor for CaptureAccepted {
                fn admit(&self, incoming: &IncomingInfo) -> Admit {
                    self.handler.admit(incoming)
                }

                fn cancel_admission(&self, incoming: &IncomingInfo) {
                    self.handler.cancel_admission(incoming);
                }

                fn finish_admission(&self, handed_off: bool) {
                    self.handler.finish_admission(handed_off);
                }

                fn alpns(&self) -> Vec<Vec<u8>> {
                    self.handler.alpns()
                }

                fn handle(&self, connection: Connection) -> BoxFuture<'static, ()> {
                    let serving = self.handler.handle(connection.clone());
                    let capture = self.clone();
                    Box::pin(async move {
                        if capture.accepted.send(connection).await.is_ok() {
                            serving.await;
                        }
                    })
                }
            }
            let limits = ZakuraLocalLimits::from_config(&Config::default());
            // Both same-identity connections sit in their control handshakes at
            // once, and each counts against the IP (zakura-quic ADM-3), so the
            // cap must hold two.
            let supervisor = ZakuraSupervisorHandle::new(2);
            let server = LocalEndpointFactory::with_limits(&limits)
                .endpoint(883)
                .await?;
            let (accepted_tx, mut accepted_rx) = mpsc::channel(3);
            let handler = ZakuraProtocolHandler::new(
                supervisor.clone(),
                Network::Mainnet,
                ZakuraHandshakeConfig::for_network(&Network::Mainnet),
                limits.clone(),
            )
            .with_local_node_id(server.local_id());
            server.serve(CaptureAccepted {
                handler,
                accepted: accepted_tx,
            })?;
            let server_addr = LocalEndpointFactory::node_addr(&server).await;
            // Separate endpoints reuse one identity but bind different UDP ports.
            let first = LocalEndpointFactory::with_limits(&limits)
                .endpoint(884)
                .await?;
            let second = LocalEndpointFactory::with_limits(&limits)
                .endpoint(884)
                .await?;
            let first_conn = first.connect(server_addr.clone(), &[P2P_V2_ALPN]).await?;
            let first_accepted = accepted_rx.recv().await.expect("first accept is captured");
            let second_conn = second.connect(server_addr.clone(), &[P2P_V2_ALPN]).await?;
            let second_accepted = accepted_rx.recv().await.expect("second accept is captured");
            let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
            assert_eq!(first_accepted.admitted_ip(), ip);
            assert_eq!(second_accepted.admitted_ip(), ip);
            let config = ZakuraHandshakeConfig::for_network(&Network::Mainnet);
            let peer = ZakuraPeerId::new(second.local_id().as_bytes().to_vec())?;
            // Complete the second connection's control handshake first.
            run_native_initiator_handshake_without_trace(&second_conn, &limits, &config, &peer)
                .await?;
            while supervisor.registered_ids().await.is_empty() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            assert_eq!(
                supervisor.inner.lock().await.ip_slots.registered(ip),
                1,
                "the registered connection must consume the IP slot"
            );
            // Duplicate admission can close the transport before the client reads the ack.
            let _ =
                run_native_initiator_handshake_without_trace(&first_conn, &limits, &config, &peer)
                    .await;
            assert!(matches!(first_conn.closed().await,
                zakura_quic::ConnectionError::ApplicationClosed(close)
                    if close.error_code == VarInt::from_u32(ZAKURA_CLOSE_NEUTRAL)
                        && close.reason.as_ref() == b"duplicate"));
            assert_eq!(supervisor.registered_ids().await, vec![peer]);
            assert_eq!(
                supervisor.inner.lock().await.ip_slots.registered(ip),
                1,
                "closing the duplicate must preserve the incumbent's IP slot"
            );

            // A second identity takes the IP's other slot.
            let third = LocalEndpointFactory::with_limits(&limits)
                .endpoint(885)
                .await?;
            let third_conn = third.connect(server_addr.clone(), &[P2P_V2_ALPN]).await?;
            let third_peer = ZakuraPeerId::new(third.local_id().as_bytes().to_vec())?;
            run_native_initiator_handshake_without_trace(
                &third_conn,
                &limits,
                &config,
                &third_peer,
            )
            .await?;
            while supervisor.registered_ids().await.len() < 2 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }

            let fourth = LocalEndpointFactory::with_limits(&limits)
                .endpoint(886)
                .await?;
            // A full IP with no handshake in flight still gets one QUIC
            // handshake, in case it is a registered node redialing.
            // Registration then rejects the new identity.
            let fourth_peer = ZakuraPeerId::new(fourth.local_id().as_bytes().to_vec())?;
            let rejected = match fourth.connect(server_addr, &[P2P_V2_ALPN]).await {
                Ok(fourth_conn) => run_native_initiator_handshake_without_trace(
                    &fourth_conn,
                    &limits,
                    &config,
                    &fourth_peer,
                )
                .await
                .is_err(),
                Err(error) => matches!(error, zakura_quic::ConnectError::Refused),
            };
            assert!(
                rejected,
                "a new identity from the same IP must be turned away at the IP cap"
            );
            assert_eq!(supervisor.registered_ids().await.len(), 2);
            fourth.shutdown().await;
            third.shutdown().await;
            first.shutdown().await;
            second.shutdown().await;
            server.shutdown().await;
            Ok(())
        })
        .await
        .expect("admitted IP accounting test timed out")
    }

    // SECURITY AUDIT (candidate claude-inbound-per-ip-cap-bypassed /
    // codex-inbound-per-ip-cap-bypass): SR-4 admission.
    //
    // `accept_connection` used to register every inbound peer with
    // `remote_ip = None`, and `register` only consults the per-IP counts when
    // `remote_ip` is `Some`, so the per-IP connection cap was enforced for native
    // outbound dials (which pass a real IP) but entirely bypassed for inbound
    // accepts: one source IP could authenticate as many distinct node ids and
    // fill the global connection budget despite `max_connections_per_ip`
    // (default 1). Inbound accepts now pass the admitted source IP into
    // `register`, and the acceptor refuses a source IP at its cap before any
    // handshake work (zakura-quic ADM-3).
    //
    // This guard drives the real production acceptor over loopback QUIC: two
    // distinct authenticated identities dial from the same source IP
    // (127.0.0.1). The per-IP cap is 1 while global admission keeps its default
    // (well above 1), so the second identity can only be turned away by the
    // per-IP cap, not the global gate. It never registers, leaving exactly one
    // registered peer.
    #[tokio::test]
    async fn inbound_accept_enforces_per_ip_cap() -> Result<(), BoxError> {
        let _guard = zakura_test::init();

        // The register-level invariant the accept path now relies on: with a real
        // source IP, the per-IP cap rejects a second distinct identity with
        // `ResourceLimit`.
        async fn try_register(
            supervisor: &ZakuraSupervisorHandle,
            peer: &ZakuraPeerId,
            remote_ip: Option<IpAddr>,
        ) -> ZakuraRegistration {
            let (outbound_tx, _outbound_rx) = mpsc::channel(1);
            let outbound_handle = ZakuraPeerHandle::new_for_tests(peer.clone(), outbound_tx);
            supervisor
                .register(
                    test_conn_id(),
                    peer.clone(),
                    &[],
                    remote_ip,
                    [peer.as_bytes()[0]; TRANSCRIPT_HASH_BYTES],
                    outbound_handle,
                    CancellationToken::new(),
                    CloseCause::new(),
                    ZAKURA_CAP_LEGACY_GOSSIP | ZAKURA_CAP_HEADER_SYNC,
                )
                .await
        }
        let ip: IpAddr = "203.0.113.7".parse().expect("test ip parses");
        let supervisor = ZakuraSupervisorHandle::new(1);
        assert!(
            matches!(
                try_register(&supervisor, &test_peer(1), Some(ip)).await,
                ZakuraRegistration::Registered { .. }
            ),
            "first identity from the IP registers",
        );
        assert!(
            matches!(
                try_register(&supervisor, &test_peer(2), Some(ip)).await,
                ZakuraRegistration::Rejected(ZakuraRejectReason::ResourceLimit)
            ),
            "a second distinct identity from the same IP must be Rejected(ResourceLimit) at cap 1",
        );

        // End-to-end: drive the production accept path with a per-IP cap of 1 and a
        // strictly larger global cap so per-IP admission is what turns away the
        // second same-IP identity.
        let limits = ZakuraLocalLimits::from_config(&Config::default());
        assert!(
            limits.max_connections > 1,
            "global admission cap must exceed the per-IP cap so the second same-IP identity is \
             turned away by the per-IP cap rather than the global gate",
        );
        let server_ep = LocalEndpointFactory::with_limits(&limits)
            .endpoint(880)
            .await?;
        let supervisor = ZakuraSupervisorHandle::new(1);
        let handler = ZakuraProtocolHandler::new(
            supervisor.clone(),
            Network::Mainnet,
            ZakuraHandshakeConfig::for_network(&Network::Mainnet),
            limits.clone(),
        )
        .with_local_node_id(server_ep.local_id());
        server_ep.serve(handler)?;
        // The factory binds IPv4 loopback only, so both clients share one source
        // IP (127.0.0.1) -- the single-source-IP shape of the finding.
        let server_addr = LocalEndpointFactory::node_addr(&server_ep).await;

        // Establish the QUIC connection only; the native handshake is driven
        // separately so a per-IP rejection mid-handshake (the second identity)
        // can be observed instead of aborting the test.
        async fn connect_native(
            server_addr: &NodeAddr,
            seed: u64,
            limits: &ZakuraLocalLimits,
        ) -> Result<(QuicEndpoint, Connection), BoxError> {
            let endpoint = LocalEndpointFactory::with_limits(limits)
                .endpoint(seed)
                .await?;

            let connection = endpoint
                .connect(server_addr.clone(), &[P2P_V2_ALPN])
                .await?;
            Ok((endpoint, connection))
        }
        async fn run_handshake(
            endpoint: &QuicEndpoint,
            connection: &Connection,
            limits: &ZakuraLocalLimits,
        ) -> Result<(), BoxError> {
            let config = ZakuraHandshakeConfig::for_network(&Config::default().network);
            let local_peer_id = ZakuraPeerId::new(endpoint.local_id().as_bytes().to_vec())?;
            run_native_initiator_handshake_without_trace(
                connection,
                limits,
                &config,
                &local_peer_id,
            )
            .await?;
            Ok(())
        }

        // First identity registers and claims the only per-IP slot. Hold the
        // endpoint/connection so the peer stays registered for the second dial.
        let (_ep1, _conn1) = connect_native(&server_addr, 881, &limits).await?;
        run_handshake(&_ep1, &_conn1, &limits).await?;
        let mut first_registered = 0;
        for _ in 0..200 {
            first_registered = supervisor.registered_ids().await.len();
            if first_registered >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert_eq!(
            first_registered, 1,
            "the first inbound identity from the source IP must register (a real source IP was \
             resolved from the endpoint and counted against the per-IP cap)",
        );

        // Second distinct identity from the same source IP. A full IP still
        // gets one handshake, in case it is the first node redialing, so the
        // QUIC handshake may complete; registration then rejects the new
        // identity. (Before the fix the accept passed remote_ip = None, so
        // this identity registered and one source IP could exhaust the global
        // budget.)
        let rejected = match connect_native(&server_addr, 882, &limits).await {
            Ok((endpoint, connection)) => run_handshake(&endpoint, &connection, &limits)
                .await
                .is_err(),
            Err(error) => matches!(
                error.downcast_ref::<zakura_quic::ConnectError>(),
                Some(zakura_quic::ConnectError::Refused)
            ),
        };
        let registered = supervisor.registered_ids().await.len();
        assert!(
            rejected && registered == 1,
            "a second distinct identity from the same source IP must be turned away by the \
             per-IP cap (rejected={rejected}, registered={registered}); before the fix the \
             inbound accept passed remote_ip = None, so both identities registered and one \
             source IP could exhaust the connection budget",
        );

        server_ep.shutdown().await;
        Ok(())
    }

    // V12 F-305593: the whole-dial timeout used to be a fixed 10 s, which cut
    // off staggered attempts and longer handshake deadlines.
    #[test]
    fn native_dial_timeout_covers_the_last_staggered_attempt() {
        let control_timeout = Duration::from_secs(10);
        let slow = QuicConfig {
            dial_stagger_ms: 5_000,
            handshake_timeout_secs: Some(60),
            ..QuicConfig::default()
        };
        assert_eq!(
            native_dial_timeout(&slow, 4, control_timeout),
            Duration::from_secs(75)
        );
        let unset = QuicConfig {
            handshake_timeout_secs: None,
            ..QuicConfig::default()
        };
        assert_eq!(
            native_dial_timeout(&unset, 1, control_timeout),
            unset.idle_timeout()
        );
        assert_eq!(
            native_dial_timeout(&QuicConfig::default(), 1, control_timeout),
            control_timeout
        );
    }

    /// ADM-3 rule 2 before TLS: a full IP gets one handshake at a time for a
    /// possible same-node redial; more are refused when the source is
    /// validated and retried when it may be spoofed; and one IP can't take
    /// more than its share of the pending-handshake budget.
    #[tokio::test]
    async fn per_ip_admission_retries_unvalidated_and_allows_one_redial() -> Result<(), BoxError> {
        let _guard = zakura_test::init();
        let ip: IpAddr = "203.0.113.9".parse().expect("test ip parses");
        let limits = ZakuraLocalLimits::from_config(&Config::default());
        let supervisor = ZakuraSupervisorHandle::new(1);
        let handler = ZakuraProtocolHandler::new(
            supervisor.clone(),
            Network::Mainnet,
            ZakuraHandshakeConfig::for_network(&Network::Mainnet),
            limits.clone(),
        );
        let peer = test_peer(1);
        let (outbound_tx, _outbound_rx) = mpsc::channel(1);
        let registered = supervisor
            .register(
                test_conn_id(),
                peer.clone(),
                &[],
                Some(ip),
                [1; TRANSCRIPT_HASH_BYTES],
                ZakuraPeerHandle::new_for_tests(peer, outbound_tx),
                CancellationToken::new(),
                CloseCause::new(),
                ZAKURA_CAP_LEGACY_GOSSIP,
            )
            .await;
        assert!(matches!(registered, ZakuraRegistration::Registered { .. }));

        let attempt = |validated: bool, pending_from_ip: usize| IncomingInfo {
            remote: SocketAddr::new(ip, 8234),
            validated,
            pending_total: pending_from_ip,
            pending_from_ip,
        };
        assert_eq!(handler.admit(&attempt(false, 0)), Admit::Retry);
        assert_eq!(handler.admit(&attempt(true, 0)), Admit::Accept);
        assert_eq!(handler.admit(&attempt(true, 1)), Admit::Refuse);
        assert_eq!(handler.admit(&attempt(false, 1)), Admit::Retry);

        let other: IpAddr = "198.51.100.3".parse().expect("test ip parses");
        let share = limits.max_pending_handshakes / PENDING_HANDSHAKE_SHARES_PER_IP;
        let busy = |pending_from_ip: usize| IncomingInfo {
            remote: SocketAddr::new(other, 8234),
            validated: true,
            pending_total: pending_from_ip,
            pending_from_ip,
        };
        let open = ZakuraSupervisorHandle::new(64);
        let handler = ZakuraProtocolHandler::new(
            open,
            Network::Mainnet,
            ZakuraHandshakeConfig::for_network(&Network::Mainnet),
            limits,
        );
        assert_eq!(
            handler.admit(&IncomingInfo {
                validated: false,
                ..busy(share - 2)
            }),
            Admit::Accept
        );
        assert_eq!(
            handler.admit(&IncomingInfo {
                validated: false,
                ..busy(share - 1)
            }),
            Admit::Retry
        );
        assert_eq!(handler.admit(&busy(share - 1)), Admit::Accept);
        assert_eq!(handler.admit(&busy(share)), Admit::Refuse);
        Ok(())
    }

    /// Spoofed Initials carry unvalidated sources. Draining a victim's
    /// unvalidated cadence, or filling the unvalidated ledger with other
    /// addresses, earns the spoofed sources a stateless Retry, never a
    /// refusal, and leaves a validated Initial from the victim its own
    /// cadence (zakura#2337 review).
    #[tokio::test(start_paused = true)]
    async fn spoofed_initials_never_lock_out_a_validated_source() {
        let victim: IpAddr = "203.0.113.9".parse().expect("test ip parses");
        let limits = ZakuraLocalLimits::from_config(&Config::default());
        let handler = ZakuraProtocolHandler::new(
            ZakuraSupervisorHandle::new(DEFAULT_ZAKURA_MAX_CONNS_PER_IP),
            Network::Mainnet,
            ZakuraHandshakeConfig::for_network(&Network::Mainnet),
            limits.clone(),
        );
        let attempt = |ip: IpAddr, validated: bool| IncomingInfo {
            remote: SocketAddr::new(ip, 8234),
            validated,
            pending_total: 0,
            pending_from_ip: 0,
        };
        // Each spoofed attempt fails at once and returns its transport
        // reservation, with no cadence refund.
        let spoof = |ip: IpAddr| {
            let admitted = handler.admit(&attempt(ip, false));
            if admitted == Admit::Accept {
                handler.finish_admission(false);
            }
            admitted
        };
        for _ in 0..DEFAULT_ZAKURA_MAX_CONNS_PER_IP {
            assert_eq!(spoof(victim), Admit::Accept);
        }
        assert_eq!(spoof(victim), Admit::Retry);
        assert_eq!(handler.admit(&attempt(victim, true)), Admit::Accept);
        handler.finish_admission(false);

        let ledger = limits
            .max_connections
            .saturating_add(limits.max_pending_handshakes);
        for host in 0..ledger {
            let ip = IpAddr::V4(std::net::Ipv4Addr::from(
                0x0a00_0000 + u32::try_from(host).expect("the ledger fits u32"),
            ));
            for _ in 0..DEFAULT_ZAKURA_MAX_CONNS_PER_IP {
                spoof(ip);
            }
        }
        let fresh: IpAddr = "198.51.100.3".parse().expect("test ip parses");
        assert_eq!(
            spoof(fresh),
            Admit::Retry,
            "the unvalidated node bucket and ledger are spent"
        );
        assert_eq!(handler.admit(&attempt(fresh, true)), Admit::Accept);
        handler.finish_admission(false);
    }

    #[test]
    fn last_per_ip_slot_requires_validation() {
        let ip = "203.0.113.9".parse().unwrap();
        for cap in [1, 2, 16] {
            let supervisor = ZakuraSupervisorHandle::new(cap);
            let limits = ZakuraLocalLimits::from_config(&Config::default());
            let share = (limits.max_pending_handshakes / PENDING_HANDSHAKE_SHARES_PER_IP).max(1);
            let handler = ZakuraProtocolHandler::new(
                supervisor.clone(),
                Network::Mainnet,
                ZakuraHandshakeConfig::for_network(&Network::Mainnet),
                limits,
            );
            let slots = cap.min(share);
            for controls in 0..slots {
                let _guards: Vec<_> = (0..controls)
                    .map(|_| supervisor.enter_control_handshake(ip))
                    .collect();
                let incoming = IncomingInfo {
                    remote: SocketAddr::new(ip, 8234),
                    validated: false,
                    pending_total: slots - controls - 1,
                    pending_from_ip: slots - controls - 1,
                };
                assert_eq!(handler.admit(&incoming), Admit::Retry);
                assert_eq!(
                    handler.admit(&IncomingInfo {
                        validated: true,
                        ..incoming
                    }),
                    Admit::Accept
                );
            }
        }
    }

    async fn register_test_peer_from(
        supervisor: &ZakuraSupervisorHandle,
        peer: &ZakuraPeerId,
        ip: &str,
        token: CancellationToken,
    ) -> ZakuraRegistration {
        let (outbound_tx, _outbound_rx) = mpsc::channel(1);
        supervisor
            .register(
                test_conn_id(),
                peer.clone(),
                &[],
                Some(ip.parse().expect("test ip parses")),
                [peer.as_bytes()[0]; TRANSCRIPT_HASH_BYTES],
                ZakuraPeerHandle::new_for_tests(peer.clone(), outbound_tx),
                token,
                CloseCause::new(),
                ZAKURA_CAP_LEGACY_GOSSIP,
            )
            .await
    }

    /// #645: every address in a global IPv6 /64 shares one per-IP cap, so a
    /// host that holds a /64 can't fill the node by using a fresh address per
    /// identity. Another /64 and unique-local addresses keep their own caps.
    #[tokio::test]
    async fn ipv6_slash_64_shares_one_per_ip_cap() {
        let supervisor = ZakuraSupervisorHandle::new(2);
        let register = |byte: u8, ip: &'static str| {
            let supervisor = supervisor.clone();
            async move {
                register_test_peer_from(&supervisor, &test_peer(byte), ip, CancellationToken::new())
                    .await
            }
        };
        assert!(matches!(
            register(1, "2001:db8:a:b::1").await,
            ZakuraRegistration::Registered { .. }
        ));
        assert!(matches!(
            register(2, "2001:db8:a:b::2").await,
            ZakuraRegistration::Registered { .. }
        ));
        assert!(matches!(
            register(3, "2001:db8:a:b:ffff:ffff:ffff:3").await,
            ZakuraRegistration::Rejected(ZakuraRejectReason::ResourceLimit)
        ));
        let full_slash_64 = "2001:db8:a:b::99".parse().expect("test ip parses");
        assert!(!supervisor.can_accept_remote_ip_with_in_flight(full_slash_64, 0));

        let other_slash_64 = "2001:db8:a:c::1".parse().expect("test ip parses");
        assert!(supervisor.can_accept_remote_ip_with_in_flight(other_slash_64, 1));
        assert!(!supervisor.can_accept_remote_ip_with_in_flight(other_slash_64, 2));
        assert!(matches!(
            register(4, "2001:db8:a:c::1").await,
            ZakuraRegistration::Registered { .. }
        ));
        for (byte, unique_local) in [(5, "fd00::1"), (6, "fd00::2"), (7, "fd00::3")] {
            assert!(
                matches!(
                    register(byte, unique_local).await,
                    ZakuraRegistration::Registered { .. }
                ),
                "{unique_local} has its own cap",
            );
        }
    }

    /// Registration charges the /64, but reports the peer's own address,
    /// which discovery matches exactly against the addresses the peer
    /// advertises.
    #[tokio::test]
    async fn registration_reports_the_address_not_its_slash_64() {
        let supervisor = ZakuraSupervisorHandle::new(2);
        let registration = register_test_peer_from(
            &supervisor,
            &test_peer(1),
            "2001:db8:a:b::7",
            CancellationToken::new(),
        )
        .await;
        let ZakuraRegistration::Registered { remote_ip, .. } = registration else {
            panic!("a fresh peer registers: {registration:?}");
        };
        assert_eq!(
            remote_ip,
            Some("2001:db8:a:b::7".parse().expect("test ip parses"))
        );
        assert_eq!(
            supervisor
                .ip_slots
                .registered("2001:db8:a:b::".parse().expect("test ip parses")),
            1
        );
    }

    /// A restarted peer that comes back on another address of its /64, as
    /// RFC 8981 temporary addresses do, still takes the same-IP redial path:
    /// it passes a full cap and evicts its stale incumbent on the short gate.
    #[tokio::test(start_paused = true)]
    async fn redial_from_the_same_slash_64_is_a_same_ip_duplicate() {
        let supervisor = ZakuraSupervisorHandle::new(1);
        let peer = test_peer(60);
        let incumbent = CancellationToken::new();
        assert!(matches!(
            register_test_peer_from(&supervisor, &peer, "2001:db8:a:b::1", incumbent.clone()).await,
            ZakuraRegistration::Registered { .. }
        ));
        tokio::time::advance(ZAKURA_SAME_IP_DUPLICATE_EVICT_MIN_AGE + Duration::from_secs(1)).await;
        assert!(matches!(
            register_test_peer_from(
                &supervisor,
                &peer,
                "2001:db8:a:b::2",
                CancellationToken::new()
            )
            .await,
            ZakuraRegistration::Duplicate { .. }
        ));
        assert!(incumbent.is_cancelled());
    }

    /// The acceptor charges control handshakes and the transport's pending
    /// handshakes to the /64 too, so one /64 gets one share of the handshake
    /// budget however many addresses it uses.
    #[test]
    fn handshakes_from_one_slash_64_count_together() {
        let supervisor = ZakuraSupervisorHandle::new(2);
        let handler = ZakuraProtocolHandler::new(
            supervisor.clone(),
            Network::Mainnet,
            ZakuraHandshakeConfig::for_network(&Network::Mainnet),
            ZakuraLocalLimits::from_config(&Config::default()),
        );
        let ip = |ip: &str| -> IpAddr { ip.parse().expect("test ip parses") };
        assert_eq!(
            handler.pending_group(ip("2001:db8:a:b::7")),
            ip("2001:db8:a:b::")
        );
        let _controls = [
            supervisor.enter_control_handshake(ip("2001:db8:a:b::1")),
            supervisor.enter_control_handshake(ip("2001:db8:a:b::2")),
        ];
        let attempt = |remote: &str| IncomingInfo {
            remote: SocketAddr::new(ip(remote), 8234),
            validated: true,
            pending_total: 0,
            pending_from_ip: 0,
        };
        assert_eq!(handler.admit(&attempt("2001:db8:a:b::3")), Admit::Refuse);
        assert_eq!(handler.admit(&attempt("2001:db8:a:c::3")), Admit::Accept);
    }

    /// fix-32 and fix-37: inbound connections lease at most the inbound part
    /// of the sessions share, so a dial still funds its bases. A session that
    /// a service admits later adds its session part once, and closing the
    /// connection returns every byte.
    #[tokio::test]
    async fn session_funding_keeps_the_dial_reserve_and_releases_everything() {
        use crate::zakura::memory::{inbound_sessions_share, MemoryPlan, SessionDirections};

        let config = Config::default();
        let plan = MemoryPlan::size(64 << 30, &config, &Network::Mainnet)
            .expect("64 GiB fits the default plan");
        let pool = plan.build_pool();
        let directions = Arc::new(zakura_rt::sync::Mutex::new(SessionDirections::default()));
        let shutdown = CancellationToken::new();
        let negotiated = ZAKURA_CAP_HEADER_SYNC | ZAKURA_CAP_BLOCK_SYNC_REGULATED;
        let cost = plan
            .bases
            .connection_cost(negotiated, ZAKURA_CAP_HEADER_SYNC, false);
        let fund = |conn_id, direction| {
            SessionFunding::acquire(
                &pool,
                &plan,
                &directions,
                conn_id,
                direction,
                cost.total(),
                ZAKURA_CAP_HEADER_SYNC,
                &shutdown,
                Duration::from_millis(10),
            )
        };

        let mut inbound = Vec::new();
        loop {
            match fund(
                inbound.len() as ZakuraConnId,
                crate::zakura::memory::SessionSide::Inbound,
            )
            .await
            {
                Ok(funding) => inbound.push(funding),
                Err(SessionFundingRefusal::DirectionFull) => break,
                Err(other) => panic!("inbound refused by {other} before the dial reserve"),
            }
        }
        assert!(!inbound.is_empty());
        let inbound_bytes = lock_session_directions(&directions).inbound;
        assert!(inbound_bytes <= inbound_sessions_share(plan.sessions_share));
        assert!(inbound_bytes + cost.total() > inbound_sessions_share(plan.sessions_share));

        let dial = fund(u64::MAX, crate::zakura::memory::SessionSide::Outbound)
            .await
            .expect("the dial reserve funds a dial");
        let leased = pool.snapshot().total_leased;
        dial.fund_session(ZAKURA_CAP_BLOCK_SYNC_REGULATED)
            .expect("the share funds the block-sync session");
        dial.fund_session(ZAKURA_CAP_BLOCK_SYNC_REGULATED)
            .expect("a funded session owes nothing");
        assert_eq!(
            pool.snapshot().total_leased,
            leased + plan.bases.block_sync.session
        );

        dial.release();
        drop(dial);
        drop(inbound);
        assert_eq!(pool.snapshot().total_leased, 0);
        assert_eq!(
            *lock_session_directions(&directions),
            SessionDirections::default()
        );
    }

    /// BP-c-1: a connection that negotiates BPv1 funds BPv1's session base in
    /// its one sessions lease, and closing it returns every byte.
    #[tokio::test]
    async fn session_funding_admits_a_bpv1_connection() {
        use crate::zakura::{
            memory::{Consumer, MemoryPlan, SessionBases, SessionDirections},
            ZAKURA_CAP_BPV1,
        };

        let mut config = Config::default();
        config.zakura.bpv1.enabled = true;
        let plan = MemoryPlan::size(64 << 30, &config, &Network::Mainnet)
            .expect("64 GiB fits the BPv1 plan");
        assert!(plan.bases.bpv1.session > 0);
        let pool = plan.build_pool();
        let directions = Arc::new(zakura_rt::sync::Mutex::new(SessionDirections::default()));
        let negotiated = ZAKURA_CAP_HEADER_SYNC | ZAKURA_CAP_BPV1;
        let cost = plan.bases.connection_cost(negotiated, negotiated, false);

        let funding = SessionFunding::acquire(
            &pool,
            &plan,
            &directions,
            1,
            crate::zakura::memory::SessionSide::Outbound,
            cost.total(),
            SessionBases::funded_sessions(negotiated, negotiated),
            &CancellationToken::new(),
            Duration::from_millis(10),
        )
        .await
        .expect("the plan funds a BPv1 connection");
        assert_eq!(pool.consumer_leased(Consumer::Sessions), cost.total());
        funding
            .fund_session(ZAKURA_CAP_BPV1)
            .expect("admission funded BPv1's session");
        assert_eq!(pool.consumer_leased(Consumer::Sessions), cost.total());

        drop(funding);
        assert_eq!(pool.snapshot().total_leased, 0);
    }

    /// ADM-3's inbound share (#1367): once inbound connections or inbound
    /// handshakes fill three quarters of their caps, the acceptor turns new
    /// inbound attempts away, and a dial still finds the reserved quarter.
    #[test]
    fn inbound_load_leaves_the_outbound_reserve() {
        // The limits production binds with: the plan's connection cap `N_cap`.
        let config = Config::default();
        let plan = crate::zakura::memory::MemoryPlan::size(64 << 30, &config, &Network::Mainnet)
            .expect("64 GiB fits the default plan");
        let limits = plan.local_limits(&config);
        assert_eq!(limits.max_connections, 256);
        assert_eq!(limits.max_inbound_connections(), 192);
        assert_eq!(limits.max_inbound_handshakes(), 24);
        let handler = ZakuraProtocolHandler::new(
            ZakuraSupervisorHandle::new(limits.max_connections),
            Network::Mainnet,
            ZakuraHandshakeConfig::for_network(&Network::Mainnet),
            limits.clone(),
        );
        let attempt = |validated: bool, pending_total: usize| IncomingInfo {
            remote: SocketAddr::new("203.0.113.9".parse().expect("test ip parses"), 8234),
            validated,
            pending_total,
            pending_from_ip: 0,
        };

        // Pending transport handshakes count against the inbound share, which
        // fills before the whole budget does.
        assert_eq!(handler.admit(&attempt(true, 23)), Admit::Accept);
        handler.finish_admission(false);
        assert_eq!(handler.admit(&attempt(true, 24)), Admit::Refuse);
        assert_eq!(handler.admit(&attempt(false, 24)), Admit::Retry);

        // So do inbound control handshakes, and dials keep the rest.
        let inbound_handshakes: Vec<_> = (0..24)
            .map(|_| {
                handler
                    .claim_pending_handshake(true)
                    .expect("inbound share has room")
            })
            .collect();
        assert_eq!(handler.admit(&attempt(true, 0)), Admit::Refuse);
        assert_eq!(handler.pending_handshakes.available_permits(), 8);
        drop(inbound_handshakes);

        // Inbound connections stop at their share, and dials keep the rest.
        let inbound: Vec<_> = (0..192)
            .map(|_| {
                handler
                    .claim_connection(NodeSecretKey::generate().public(), true)
                    .expect("inbound share has room")
            })
            .collect();
        assert!(handler
            .claim_connection(NodeSecretKey::generate().public(), true)
            .is_err());
        assert_eq!(
            handler.supervisor.ip_slots.counts().inbound_connections(),
            192
        );
        assert_eq!(handler.admit(&attempt(true, 0)), Admit::Refuse);
        let dials: Vec<_> = (0..64)
            .map(|_| {
                handler
                    .claim_connection(NodeSecretKey::generate().public(), false)
                    .expect("dial reserve has room")
            })
            .collect();
        assert!(handler
            .claim_connection(NodeSecretKey::generate().public(), false)
            .is_err());
        assert_eq!(handler.supervisor.ip_slots.counts().connections(), 256);
        assert_eq!(handler.free_connection_slots(), 0);
        drop((inbound, dials));
        assert_eq!(handler.supervisor.ip_slots.counts().connections(), 0);
        assert_eq!(
            handler.supervisor.ip_slots.counts().inbound_connections(),
            0
        );
        assert_eq!(handler.admit(&attempt(true, 0)), Admit::Accept);
    }

    #[test]
    fn pending_transport_reservations_bound_outbound_control_claims() {
        let limits = ZakuraLocalLimits::from_config(&Config::default());
        let handler = ZakuraProtocolHandler::new(
            ZakuraSupervisorHandle::new(limits.max_connections),
            Network::Mainnet,
            ZakuraHandshakeConfig::for_network(&Network::Mainnet),
            limits,
        );
        for pending in 0..24 {
            let incoming = IncomingInfo {
                remote: SocketAddr::new(
                    IpAddr::V4(std::net::Ipv4Addr::new(
                        192,
                        0,
                        2,
                        u8::try_from(pending + 1).unwrap(),
                    )),
                    8234,
                ),
                validated: true,
                pending_total: pending,
                pending_from_ip: 0,
            };
            assert_eq!(handler.admit(&incoming), Admit::Accept);
        }
        let dials: Vec<_> = (0..8)
            .map(|_| handler.claim_pending_handshake(false).unwrap())
            .collect();
        assert!(handler.claim_pending_handshake(false).is_none());
        let transferred = handler
            .claim_pending_handshake_with_transfer(true, true)
            .unwrap();
        handler.finish_admission(true);
        assert!(handler.claim_pending_handshake(false).is_none());
        drop(transferred);
        let ninth = handler.claim_pending_handshake(false).unwrap();
        assert!(handler.claim_pending_handshake(false).is_none());
        for _ in 0..23 {
            handler.finish_admission(false);
        }
        drop((ninth, dials));
        let counts = handler.supervisor.ip_slots.counts();
        assert_eq!(counts.transport_pending(), 0);
        assert_eq!(counts.pending(), 0);
    }

    /// Dials may use inbound's unused share, and once they fill the whole cap
    /// inbound attempts are refused, as before the reserve.
    #[test]
    fn dials_may_use_the_inbound_share() {
        let limits = ZakuraLocalLimits::from_config(&Config::default());
        let handler = ZakuraProtocolHandler::new(
            ZakuraSupervisorHandle::new(limits.max_connections),
            Network::Mainnet,
            ZakuraHandshakeConfig::for_network(&Network::Mainnet),
            limits.clone(),
        );
        let _dials: Vec<_> = (0..limits.max_connections)
            .map(|_| {
                handler
                    .claim_connection(NodeSecretKey::generate().public(), false)
                    .expect("dials may take every slot")
            })
            .collect();
        assert_eq!(
            handler.supervisor.ip_slots.counts().connections(),
            limits.max_connections
        );
        assert_eq!(
            handler.admit(&IncomingInfo {
                remote: SocketAddr::new("203.0.113.9".parse().expect("test ip parses"), 8234),
                validated: true,
                pending_total: 0,
                pending_from_ip: 0,
            }),
            Admit::Refuse
        );
    }

    /// The real inbound paths hold the inbound share: `claim_inbound` holds a
    /// connection slot from it until the claim drops, and the responder
    /// handshake holds a handshake slot from it while it runs, so dials keep
    /// the reserve in both.
    #[tokio::test]
    async fn inbound_paths_hold_the_inbound_share() -> Result<(), BoxError> {
        let _guard = zakura_test::init();
        let mut limits = ZakuraLocalLimits::from_config(&Config::default());
        limits.max_connections = 4;
        limits.max_pending_handshakes = 4;
        let handler = ZakuraProtocolHandler::new(
            ZakuraSupervisorHandle::new(64),
            Network::Mainnet,
            ZakuraHandshakeConfig::for_network(&Network::Mainnet),
            limits.clone(),
        );
        assert_eq!(limits.max_inbound_connections(), 3);
        assert_eq!(limits.max_inbound_handshakes(), 3);

        let server = LocalEndpointFactory::with_limits(&limits)
            .endpoint(0x1367_0001)
            .await?;
        let (connection_tx, mut connection_rx) = mpsc::channel(8);
        let (stream_tx, _stream_rx) = mpsc::channel(8);
        server.serve(CaptureConnection {
            alpn: P2P_V2_ALPN,
            connection_tx,
            stream_tx,
        })?;
        let server_addr = LocalEndpointFactory::node_addr(&server).await;
        let client = LocalEndpointFactory::with_limits(&limits)
            .endpoint(0x1367_0002)
            .await?;
        let mut client_connections = Vec::new();
        let mut inbound = Vec::new();
        for _ in 0..5 {
            client_connections.push(client.connect(server_addr.clone(), &[P2P_V2_ALPN]).await?);
            inbound.push(
                timeout(Duration::from_secs(10), connection_rx.recv())
                    .await?
                    .expect("the capture acceptor sends each accepted connection"),
            );
        }

        // Connection slots: the fourth inbound claim finds its share full
        // while a dial still finds a slot.
        let claims: Vec<_> = inbound[..3]
            .iter()
            .map(|connection| handler.claim_inbound(connection, false))
            .collect();
        assert!(claims.iter().all(Result::is_ok));
        assert!(matches!(
            handler.claim_inbound(&inbound[3], false),
            Err(InboundRefusal::Admission)
        ));
        assert_eq!(handler.free_connection_slots(), 1);
        drop(claims);
        assert_eq!(handler.free_connection_slots(), 4);

        // Handshake slots: a responder handshake waiting for a hello that
        // never comes holds one inbound and one shared slot.
        let responder = |connection: Connection| {
            let handler = handler.clone();
            async move {
                let peer = ZakuraPeerId::new(connection.remote_id().as_bytes().to_vec())?;
                let conn = ZakuraConnTrace::new(&handler.trace, trace_conn_id(&connection), &peer);
                handler
                    .run_native_responder_handshake_with_permit(
                        &connection,
                        &peer,
                        &conn,
                        handler
                            .claim_pending_handshake(true)
                            .expect("the inbound handshake share has room"),
                    )
                    .await
                    .map(|_| ())
            }
        };
        let waiting = tokio::spawn(responder(inbound[0].clone()));
        timeout(Duration::from_secs(10), async {
            while handler.inbound_handshakes.available_permits() == 3 {
                zakura_rt::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await?;
        assert_eq!(handler.inbound_handshakes.available_permits(), 2);
        assert_eq!(handler.pending_handshakes.available_permits(), 3);
        waiting.abort();
        let _ = waiting.await;
        assert_eq!(handler.inbound_handshakes.available_permits(), 3);
        assert_eq!(handler.pending_handshakes.available_permits(), 4);

        // With the inbound share full, an inbound claim is refused for its
        // pending handshake without keeping a connection or shared slot.
        let full = handler
            .inbound_handshakes
            .clone()
            .try_acquire_many_owned(3)
            .expect("the test holds free permits");
        assert!(matches!(
            handler.claim_inbound(&inbound[4], false),
            Err(InboundRefusal::PendingHandshake)
        ));
        assert_eq!(handler.pending_handshakes.available_permits(), 4);
        assert_eq!(handler.free_connection_slots(), 4);
        drop(full);
        Ok(())
    }

    /// An inbound claim can lose its slot while TLS runs. The refusal returns
    /// the pending permit and keeps both charges for the completed TLS start.
    #[tokio::test]
    async fn a_local_refusal_after_tls_keeps_both_cadence_charges() -> Result<(), BoxError> {
        let _guard = zakura_test::init();
        let limits = ZakuraLocalLimits::from_config(&Config::default());
        let handler = ZakuraProtocolHandler::new(
            ZakuraSupervisorHandle::new(DEFAULT_ZAKURA_MAX_CONNS_PER_IP),
            Network::Mainnet,
            ZakuraHandshakeConfig::for_network(&Network::Mainnet),
            limits.clone(),
        );
        let server = LocalEndpointFactory::with_limits(&limits)
            .endpoint(0x1367_0003)
            .await?;
        let (connection_tx, mut connection_rx) = mpsc::channel(1);
        let (stream_tx, _stream_rx) = mpsc::channel(1);
        server.serve(CaptureConnection {
            alpn: P2P_V2_ALPN,
            connection_tx,
            stream_tx,
        })?;
        let client = LocalEndpointFactory::with_limits(&limits)
            .endpoint(0x1367_0004)
            .await?;
        let _client_connection = client
            .connect(
                LocalEndpointFactory::node_addr(&server).await,
                &[P2P_V2_ALPN],
            )
            .await?;
        let inbound = timeout(Duration::from_secs(10), connection_rx.recv())
            .await?
            .expect("the capture acceptor sends the accepted connection");
        tokio::time::pause();
        let validated = inbound.source_validated();
        let incoming = IncomingInfo {
            remote: inbound.admitted_addr(),
            validated,
            pending_total: 0,
            pending_from_ip: 0,
        };

        assert_eq!(handler.admit(&incoming), Admit::Accept);
        handler.cancel_admission(&incoming);
        assert_eq!(handler.supervisor.ip_slots.counts().transport_pending(), 0);
        // Account for the captured TLS start as the real acceptor would.
        assert_eq!(handler.admit(&incoming), Admit::Accept);
        let inbound_peers: Vec<_> = (0..limits.max_inbound_connections())
            .map(|_| {
                handler
                    .claim_connection(NodeSecretKey::generate().public(), true)
                    .expect("the inbound share has room")
            })
            .collect();
        let refusal = handler.handle(inbound);
        assert_eq!(handler.supervisor.ip_slots.counts().transport_pending(), 0);
        drop(inbound_peers);

        for _ in 1..DEFAULT_ZAKURA_MAX_CONNS_PER_IP {
            assert_eq!(handler.admit(&incoming), Admit::Accept);
            handler.finish_admission(false);
        }
        assert_ne!(handler.admit(&incoming), Admit::Accept);
        // The node's bucket kept all 16 charges, so 8 tokens remain. They
        // are its reserve, where each other address gets one.
        let other_host = |host: u8| IncomingInfo {
            remote: std::net::SocketAddr::from(([198, 51, 100, host], 8234)),
            ..incoming
        };
        for host in 0..8 {
            assert_eq!(handler.admit(&other_host(host)), Admit::Accept);
            handler.finish_admission(false);
        }
        assert_ne!(handler.admit(&other_host(8)), Admit::Accept);
        timeout(Duration::from_secs(10), refusal).await?;
        Ok(())
    }

    #[test]
    fn outbound_reserve_rounds_down_but_keeps_one_slot() {
        for (cap, inbound) in [
            (0, 0),
            (1, 1),
            (2, 1),
            (3, 2),
            (4, 3),
            (7, 6),
            (8, 6),
            (16, 12),
            (32, 24),
            (92, 69),
        ] {
            assert_eq!(without_outbound_reserve(cap), inbound, "cap {cap}");
        }
    }

    // V12 F-67: the handler refused every attempt at the global pending cap,
    // before the transport's Retry threshold could apply, so a valid config
    // with `retry_threshold >= max_pending_handshakes` never sent a Retry.
    #[test]
    fn full_pending_budget_retries_unvalidated_attempts() {
        let limits = ZakuraLocalLimits::from_config(&Config::default());
        let handler = ZakuraProtocolHandler::new(
            ZakuraSupervisorHandle::new(64),
            Network::Mainnet,
            ZakuraHandshakeConfig::for_network(&Network::Mainnet),
            limits.clone(),
        );
        // Spread the pending total over other IPs so the per-IP share stays open.
        let attempt = |validated: bool| IncomingInfo {
            remote: SocketAddr::new("203.0.113.9".parse().expect("test ip parses"), 8234),
            validated,
            pending_total: limits.max_pending_handshakes,
            pending_from_ip: 0,
        };
        assert_eq!(handler.admit(&attempt(false)), Admit::Retry);
        assert_eq!(handler.admit(&attempt(true)), Admit::Refuse);
    }

    // V12 F-305585: the transport stops counting an inbound connection when TLS
    // finishes, and the control handshake that follows used to count only
    // against the global `pending_handshakes` budget. One IP could therefore
    // stall many TLS-complete connections and exceed its per-IP cap. Those
    // connections now count against their IP until registration.
    #[tokio::test]
    async fn stalled_control_handshakes_count_against_the_per_ip_cap() -> Result<(), BoxError> {
        let _guard = zakura_test::init();
        let limits = ZakuraLocalLimits::from_config(&Config::default());
        let server_ep = LocalEndpointFactory::with_limits(&limits)
            .endpoint(890)
            .await?;
        let supervisor = ZakuraSupervisorHandle::new(2);
        let handler = ZakuraProtocolHandler::new(
            supervisor.clone(),
            Network::Mainnet,
            ZakuraHandshakeConfig::for_network(&Network::Mainnet),
            limits.clone(),
        )
        .with_local_node_id(server_ep.local_id());
        server_ep.serve(handler)?;
        let server_addr = LocalEndpointFactory::node_addr(&server_ep).await;

        // Two identities from 127.0.0.1 finish TLS and never send the control hello.
        let mut stalled = Vec::new();
        for seed in [891, 892] {
            let endpoint = LocalEndpointFactory::with_limits(&limits)
                .endpoint(seed)
                .await?;
            let connection = endpoint
                .connect(server_addr.clone(), &[P2P_V2_ALPN])
                .await?;
            stalled.push((endpoint, connection));
        }
        let loopback: IpAddr = Ipv4Addr::LOCALHOST.into();
        let control_count = || supervisor.ip_slots.charged(loopback);
        await_until(
            "two stalled control handshakes",
            Duration::from_secs(5),
            || control_count() == 2,
        )
        .await?;

        // A third identity from the same IP is refused before TLS.
        let third = LocalEndpointFactory::with_limits(&limits)
            .endpoint(893)
            .await?;
        let refused = matches!(
            third.connect(server_addr.clone(), &[P2P_V2_ALPN]).await,
            Err(zakura_quic::ConnectError::Refused)
        );
        assert!(
            refused,
            "stalled control handshakes must fill the per-IP cap"
        );

        // Closing the stalled connections releases their per-IP slots.
        drop(stalled);
        await_until(
            "released control handshakes",
            Duration::from_secs(15),
            || control_count() == 0,
        )
        .await?;

        server_ep.shutdown().await;
        Ok(())
    }
}
