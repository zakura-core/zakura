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
    budget_admitted_connections, budgeted_max_connections, budgeted_quic_bytes, ConnectionMemory,
    PeerMemoryBudgetError, DEFAULT_ZAKURA_PEER_MEMORY_BUDGET_BYTES,
    ZAKURA_CLOSING_CONNECTIONS_PER_LIVE,
};
use super::regulation::{CadenceBuckets, CadenceCharge, PrecheckSlot};
use super::trace::{reject_reason_label, ZakuraTrace};
use super::transport::{
    check_reactor_shares, frame_wire_bytes, sim_arg, sim_chain, transport_observe,
    worker_framed_channel, AdmittedFrame, CompatResponseGrant, ConnectionResources, FrameGate,
    FrameGrant, FramedWorkerRecv, HandshakeCounts, IpAdmission, NodeResources,
    OrderedStreamFailure, OrderedStreamFailureCause, OwnerBound, OwnerGuard, OwnerPhase,
    ReactorBudget, ReactorShares, ResourceEnvelope, SessionLayout, StreamResources,
    TransportLimits, COMPAT_EXCHANGE_BUDGET, COMPAT_RESPONSE_MAX_AGGREGATE_BYTES,
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
        ZAKURA_HEADER_SYNC_STREAM_VERSION, ZAKURA_STREAM_BLOCK_SYNC, ZAKURA_STREAM_HEADER_SYNC,
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
pub const DEFAULT_MAINNET_ZAKURA_BOOTSTRAP_PEERS: &[&str] = &[
    "1398f62c6d1a457c51ba6a4b5f3dbd2f69fca93216218dc8997e416bd17d93ca@165.22.54.66:8234",
    "fd1724385aa0c75b64fb78cd602fa1d991fdebf76b13c58ed702eac835e9f618@104.131.184.123:8234",
    "9ec67ad6834bc2ca0d659c240e042d3446c37cabcc092b527d459c87d938b4a4@159.65.183.89:8234",
    "bd3dc5d2a3d44c6bf90e364bf446231dbf9737e38a562ccf9e91ea631ea59b22@143.244.184.176:8234",
    "14ab98fa0c4b07d40119e1dbc9f3c36d20c8f226ae5ba4216218a2034f148e57@159.203.38.10:8234",
    "681d21b18644cd82ec13256a97f92bec1fff815683ef6f65dc7c993f098a4fe5@64.227.44.93:8234",
    "058b3f20dc9bef7bb447f94d7663d793cfbc036720f97e52d7f13661b21818e1@161.35.156.226:8234",
    "291323d78eb7186c3fa225ef5e305e95363e0ef06d42dca91bd4ef0254aed1ae@139.59.64.115:8234",
    "85e425233a68697d4be91dd5d542305a8a327cd06d992d53c0913cef2fa75084@168.144.173.250:8234",
];
/// Default native Zakura bootstrap peers for Testnet.
pub const DEFAULT_TESTNET_ZAKURA_BOOTSTRAP_PEERS: &[&str] = &[
    "57ad39fad4f0bca46cf1ea831772a99d5027b372fef2be5a0ea68e1b5bb4da49@167.99.103.111:8234",
    "2bbb907b5d90598ef49f2e637066586b311a64587479be6ed43e8388587fcd2a@164.92.209.78:8234",
    "50999835f48f4a048c0e9042e5332844c9673943d7fab1f7e993bae698c27ea3@206.189.148.0:8234",
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
/// A paused stream may consume at most half the connection receive window,
/// leaving credit for another service stream.
pub const DEFAULT_ZAKURA_STREAM_RECEIVE_WINDOW: u32 = 16 * 1024 * 1024;
/// QUIC connection receive window used by Zakura endpoints.
pub const DEFAULT_ZAKURA_RECEIVE_WINDOW: u32 = 32 * 1024 * 1024;
/// QUIC send window used by Zakura endpoints.
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
    /// Total concurrent Zakura connections, inbound plus outbound.
    ///
    /// Inbound peers leave a quarter of it, rounded down but at least one
    /// slot once it is 2 or more, free for this node's dials (see
    /// [`ZakuraLocalLimits::max_inbound_connections`]).
    /// [`peer_memory_budget_bytes`](Self::peer_memory_budget_bytes) lowers a
    /// larger value at startup.
    pub max_connections: usize,
    /// Bytes of QUIC flow-control credit and queued datagrams that native
    /// connections together may make this node hold.
    ///
    /// An admitted QUIC connection can hold its receive window, which is the
    /// `[network.zakura.quic]` receive window plus every reactor's receive
    /// share, and its send window: 141,937,353 bytes at the Mainnet defaults.
    /// A connection in its handshake holds only the two QUIC windows, 64 MiB.
    /// Each bound socket can also queue 256 MiB of datagrams, however many
    /// connections it serves (zakura-quic CTRL-26 lists what this leaves out).
    /// The node reserves one socket queue per bound socket and
    /// `max_pending_handshakes` handshakes, and funds the rest divided by the
    /// admitted connection's cost: 96 connections on the one socket
    /// `listen_addr` binds. A larger `max_connections` is lowered at startup.
    /// A closed connection releases its stream buffers and costs nothing more
    /// until it drains.
    /// Startup fails when the budget admits fewer than 16 connections, or
    /// twice the discovery headroom plus one (19 with Mainnet's 9 bootstrap
    /// peers), or fewer than `max_connections` when that is smaller.
    pub peer_memory_budget_bytes: u64,
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
    /// Also the QUIC connections that
    /// [`peer_memory_budget_bytes`](Self::peer_memory_budget_bytes) funds
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
            max_connections: DEFAULT_ZAKURA_MAX_CONNECTIONS,
            peer_memory_budget_bytes: DEFAULT_ZAKURA_PEER_MEMORY_BUDGET_BYTES,
            max_connections_per_ip: DEFAULT_ZAKURA_MAX_CONNS_PER_IP,
            max_pending_handshakes: DEFAULT_ZAKURA_MAX_PENDING_HANDSHAKES,
            stream_open_rate_per_second: DEFAULT_ZAKURA_STREAM_OPEN_RATE_PER_SECOND,
            message_rate_per_second: DEFAULT_ZAKURA_MESSAGE_RATE_PER_SECOND,
            trace_dir: None,
            trace_peer_names: BTreeMap::new(),
            header_sync: ZakuraHeaderSyncConfig::default(),
            block_sync: ZakuraBlockSyncConfig::default(),
            tx_relay: super::ZakuraTxRelayConfig::default(),
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
    /// The handshake share is the `[network.zakura.quic]` receive window. The
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
            receive: u64::MAX,
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
        ReactorShares {
            kinds,
            ..ReactorShares::from_quic(&self.quic)
        }
    }

    /// What one connection on `network` costs the peer memory budget, when it
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
            .map(|(_, share)| share.receive)
            .chain(custom_kinds.iter().map(|kind| shares.share(*kind).receive))
            .fold(0_u64, u64::saturating_add);
        ConnectionMemory::new(
            shares.control_receive_bytes,
            kinds,
            self.quic.send_window_bytes,
        )
    }

    /// The QUIC memory the node can hold on `network` with `max_connections`
    /// admitted: every admitted connection, the handshake headroom, and the
    /// bound sockets' queues. Each connection negotiates the native kinds and
    /// `custom_kinds`. The peer memory budget is its ceiling.
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

    /// Return the connection cap on `network` after the peer memory budget
    /// (see [`budgeted_max_connections`]), or an error naming the keys when
    /// the budget admits too few connections.
    ///
    /// The floor counts distinct bootstrap identities, as discovery does.
    /// Without the node key it can't tell whether one of them is this node, so
    /// it counts one fewer. It also charges only the native stream kinds. With
    /// both choices, it never refuses a config that
    /// [`Self::budgeted_max_connections_for_node`] accepts.
    pub fn budgeted_max_connections(
        &self,
        network: &Network,
    ) -> Result<usize, PeerMemoryBudgetError> {
        self.budgeted_max_connections_excluding(network, None, &[])
    }

    /// Like [`Self::budgeted_max_connections`], but leaves `local_node_id` out
    /// of the bootstrap count, exactly as discovery does, and charges each
    /// connection for `custom_kinds` too.
    pub fn budgeted_max_connections_for_node(
        &self,
        network: &Network,
        local_node_id: NodeId,
        custom_kinds: &[u16],
    ) -> Result<usize, PeerMemoryBudgetError> {
        self.budgeted_max_connections_excluding(network, Some(local_node_id), custom_kinds)
    }

    fn budgeted_max_connections_excluding(
        &self,
        network: &Network,
        local_node_id: Option<NodeId>,
        custom_kinds: &[u16],
    ) -> Result<usize, PeerMemoryBudgetError> {
        budgeted_max_connections(
            self.max_connections,
            self.peer_memory_budget_bytes,
            self.bound_socket_count(),
            self.connection_memory(network, custom_kinds),
            match local_node_id {
                Some(local_node_id) => {
                    remote_bootstrap_peer_count(&self.bootstrap_peers, local_node_id)
                }
                // One entry may be this node's own.
                None => remote_bootstrap_peer_count(&self.bootstrap_peers, None).saturating_sub(1),
            },
            self.max_pending_handshakes.max(1),
        )
    }

    /// Returns how many sockets the native endpoint binds for this config (see
    /// `native_bind_addrs`), each with its own datagram queue.
    fn bound_socket_count(&self) -> u64 {
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
    pub fn apply_network_defaults(&mut self, network: &Network) {
        if self.bootstrap_peers
            != bootstrap_peers_to_strings(DEFAULT_MAINNET_ZAKURA_BOOTSTRAP_PEERS)
        {
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
    /// Total concurrent Zakura connection cap.
    pub max_connections: usize,
    /// Concurrent control handshakes cap.
    pub max_pending_handshakes: usize,
    /// Per-connection QUIC/app idle timeout.
    pub quic_idle_timeout: Duration,
    /// QUIC keepalive interval.
    pub keep_alive_interval: Duration,
    /// Stream prelude read timeout.
    pub prelude_timeout: Duration,
    /// Control handshake deadline: it bounds each read and write, and the
    /// whole exchange in both directions.
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
}

impl ZakuraLocalLimits {
    /// Build local handler limits from network configuration and handshake
    /// policy, for a node that serves only the native stream kinds.
    pub fn from_config(config: &Config) -> Self {
        Self::for_node(config, &[])
    }

    /// Like [`Self::from_config`], for a node that also serves `custom_kinds`.
    /// The peer memory budget charges each connection for them.
    pub fn for_node(config: &Config, custom_kinds: &[u16]) -> Self {
        let handshake = ZakuraHandshakeConfig::for_network(&config.network);
        Self {
            quic: config.zakura.quic.clone(),
            // The floor only decides whether endpoint startup refuses the
            // config, so the cap is the budget's count either way.
            max_connections: config
                .zakura
                .max_connections
                .max(1)
                .min(budget_admitted_connections(
                    config.zakura.peer_memory_budget_bytes,
                    config.zakura.bound_socket_count(),
                    config
                        .zakura
                        .connection_memory(&config.network, custom_kinds),
                    config.zakura.max_pending_handshakes.max(1),
                )),
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
        }
        .with_reactor_shares(config.zakura.reactor_shares(&config.network))
    }

    /// These limits with `shares`, and the QUIC windows that carry them: the
    /// connection window starts at the handshake share, and the stream window
    /// holds the largest receive share.
    ///
    /// The connection cap stays, so only [`Self::for_node`] calls this, with
    /// the shares that the cap was budgeted from.
    fn with_reactor_shares(mut self, shares: ReactorShares) -> Self {
        self.quic.receive_window_bytes =
            u32::try_from(shares.control_receive_bytes).unwrap_or(u32::MAX);
        self.quic.stream_receive_window_bytes =
            u32::try_from(shares.stream_receive_window()).unwrap_or(u32::MAX);
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
            ReactorBudget {
                receive: sum.receive.saturating_add(share.receive),
                output: sum.output.saturating_add(share.output),
            }
        });
        let compat_exchange_bytes = if kinds.contains(&LEGACY_REQUEST_STREAM_KIND) {
            // Widening usize to u64 is lossless on supported targets.
            COMPAT_EXCHANGE_BUDGET
                .receive
                .saturating_mul(super::legacy_gossip::LEGACY_CLIENT_REQUEST_SLOTS as u64)
        } else {
            0
        };
        TransportLimits {
            max_open_streams: self.max_open_streams,
            control_receive_bytes: shares.control_receive_bytes,
            connection_shares,
            compat_exchange_bytes,
            send_window: self.quic.send_window_bytes,
            max_connections: self.max_connections,
            max_pending_handshakes: self.max_pending_handshakes,
        }
    }

    /// The most owners that these limits allow at once.
    pub(crate) fn owner_bound(&self) -> OwnerBound {
        OwnerBound::new(
            self.max_pending_handshakes,
            self.max_connections,
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
    /// of the live slots (93 at the defaults), counted until each connection
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
            .max_connections
            .saturating_add(self.max_pending_handshakes);
        let max_inbound_connections = self
            .max_inbound_connections()
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
    pub(crate) fn local_node_id(&self) -> NodeId {
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
    /// inbound capacity (zakura-quic ADM-12): none without an admission
    /// permit, otherwise the transport's free discovery slots. A dial claims
    /// a slot for every address it tries, or fails with none claimed.
    pub(crate) fn discovery_admission_capacity(&self) -> usize {
        if self.handler.admission.available_permits() == 0 {
            return 0;
        }
        self.quic.discovery_capacity()
    }

    /// How many dials discovery can start now: the handler's free admission
    /// permits. Each dial holds one permit from its start, or fails as a
    /// local limit without one.
    pub(crate) fn discovery_dial_permits(&self) -> usize {
        self.handler.admission.available_permits()
    }

    /// Holds `count` of the handler's admission permits until the returned
    /// permit drops.
    #[cfg(test)]
    pub(crate) fn hold_admission_permits(&self, count: u32) -> OwnedSemaphorePermit {
        self.handler
            .admission
            .clone()
            .try_acquire_many_owned(count)
            .expect("the test holds free permits")
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
    registration_tx: broadcast::Sender<ZakuraConnectionRegistration>,
    pending_handoffs: Arc<StdMutex<HashMap<ZakuraPeerId, ZakuraHandoffId>>>,
    peer_registry: Option<PeerRegistry>,
    /// The same counts the supervisor state charges, read by the QUIC acceptor.
    ip_slots: IpSlots,
    max_connections_per_ip: usize,
    /// `tx-relay`'s live-session record, or `None` when `tx-relay` is not
    /// registered.
    tx_relay_sessions: Option<super::TxRelaySessions>,
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

/// Releases connection permits and their machine charge under the same lock.
struct ConnectionAdmissionGuard {
    slots: IpSlots,
    shared: Option<OwnedSemaphorePermit>,
    inbound: Option<OwnedSemaphorePermit>,
    #[cfg(any(test, zakura_sim))]
    observation: Option<HandshakeObservation>,
}

impl Drop for ConnectionAdmissionGuard {
    fn drop(&mut self) {
        let mut counts = self.slots.counts();
        let inbound = self.inbound.is_some();
        drop(self.shared.take());
        drop(self.inbound.take());
        counts.release_connection(inbound);
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
    active_by_peer: HashMap<ZakuraPeerId, ZakuraPeerConnectionEntry>,
    ip_slots: IpSlots,
    next_handoff_id: ZakuraHandoffId,
    max_connections_per_ip: usize,
    next_registration_id: ZakuraConnId,
}

#[derive(Debug)]
struct ZakuraPeerConnectionEntry {
    /// Monotonic supervisor registration generation used by services to ignore
    /// stale add/remove work from a superseded connection.
    conn_id: ZakuraConnId,
    outbound_handle: ZakuraPeerHandle,
    disconnect_token: CancellationToken,
    registered_at: Instant,
    /// The [`admission_host`] this connection is charged to, if its IP is
    /// known.
    host: Option<IpAddr>,
}

impl ZakuraSupervisorState {
    fn increment_ip(&mut self, remote_ip: Option<IpAddr>) {
        if let Some(remote_ip) = remote_ip {
            self.ip_slots.add_registered(remote_ip);
        }
    }

    fn decrement_ip(&mut self, remote_ip: Option<IpAddr>) {
        if let Some(remote_ip) = remote_ip {
            self.ip_slots.remove_registered(remote_ip);
        }
    }

    /// Checks that the per-IP registered totals match the active peer
    /// registrations with known IPs, in debug builds.
    ///
    /// Simulator builds skip it. Its `IpSlots` lock is a scheduler point
    /// there, so a debug-only lock would give debug and release builds
    /// different schedules for the same seed.
    #[cfg(all(debug_assertions, not(zakura_sim)))]
    fn debug_assert_accounting(&self) {
        debug_assert_eq!(
            self.ip_slots.registered_total(),
            self.active_by_peer
                .values()
                .filter(|entry| entry.host.is_some())
                .count(),
            "Zakura per-IP registered totals must match active peer registrations with known IPs",
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
                Some("192.0.2.6".parse().expect("test IP parses")),
                [6; TRANSCRIPT_HASH_BYTES],
                handle,
                CancellationToken::new(),
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
                next_handoff_id: 1,
                max_connections_per_ip,
                next_registration_id: 1,
            })),
            shutdown: CancellationToken::new(),
            peer_set_tx: watch::channel(Vec::new()).0,
            registration_tx,
            pending_handoffs: Arc::new(StdMutex::new(HashMap::new())),
            peer_registry,
            ip_slots,
            max_connections_per_ip,
            tx_relay_sessions: None,
        }
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
        reason: &crate::zakura::BlockSyncMisbehavior,
    ) -> bool {
        if !reason.proves_tampered_body() {
            return false;
        }
        let token = {
            let state = self.inner.lock().await;
            state
                .active_by_peer
                .get(peer_id)
                .map(|entry| entry.disconnect_token.clone())
        };
        // Count only connections this call closes, not a peer that already left
        // or a connection that is already shutting down.
        let disconnected = token.is_some_and(|token| {
            let already_closing = token.is_cancelled();
            token.cancel();
            !already_closing
        });
        if disconnected {
            metrics::counter!("sync.block.peer.tampered_body_disconnect").increment(1);
        }
        info!(
            ?peer_id,
            ?reason,
            disconnected,
            "disconnecting Zakura peer that served a tampered block body"
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

    #[allow(clippy::too_many_arguments)]
    async fn register(
        &self,
        _trace_conn_id: ZakuraConnId,
        peer_id: ZakuraPeerId,
        remote_ip: Option<IpAddr>,
        transcript_hash: [u8; TRANSCRIPT_HASH_BYTES],
        outbound_handle: ZakuraPeerHandle,
        disconnect_token: CancellationToken,
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
            .is_some_and(|entry| entry.host == host);
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
        if state.next_registration_id == u64::MAX {
            metrics::counter!("zakura.p2p.conn.rejected.admission").increment(1);
            return ZakuraRegistration::Rejected(ZakuraRejectReason::TemporaryUnavailable);
        }

        match state
            .supervisor
            .register_authenticated(peer_id.clone(), transcript_hash)
        {
            AuthenticatedPeerRegistration::Registered => {
                let conn_id = state.next_registration_id;
                state.next_registration_id += 1;
                self.pending_handoffs
                    .lock()
                    .expect("Zakura pending handoff mutex is never poisoned")
                    .remove(&peer_id);
                let entry = ZakuraPeerConnectionEntry {
                    conn_id,
                    outbound_handle,
                    disconnect_token,
                    registered_at: Instant::now(),
                    host,
                };
                if let Some(old_entry) = state.active_by_peer.insert(peer_id.clone(), entry) {
                    state.decrement_ip(old_entry.host);
                    old_entry.disconnect_token.cancel();
                    metrics::counter!("zakura.p2p.conn.duplicate.evicted_upgraded").increment(1);
                }
                state.increment_ip(host);
                state.debug_assert_accounting();
                let registered_ids: Vec<_> = state.active_by_peer.keys().cloned().collect();
                set_active_connection_gauge(registered_ids.len());
                self.peer_set_tx.send_replace(registered_ids);
                if let Some(peer_registry) = &self.peer_registry {
                    peer_registry.register_native_connection(peer_id.clone(), conn_id, remote_ip);
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

    async fn deregister(&self, peer_id: &ZakuraPeerId, conn_id: ZakuraConnId) {
        let mut state = self.inner.lock().await;
        let Some(entry) = state.active_by_peer.get(peer_id) else {
            state.debug_assert_accounting();
            return;
        };
        if entry.conn_id != conn_id {
            state.debug_assert_accounting();
            return;
        }
        let entry = state
            .active_by_peer
            .remove(peer_id)
            .expect("peer entry exists because it was just checked");
        state.decrement_ip(entry.host);
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

#[derive(Debug)]
struct RegisteredPeerCleanupGuard {
    supervisor: ZakuraSupervisorHandle,
    registry: Arc<ServiceRegistry>,
    peer_id: ZakuraPeerId,
    conn_id: ZakuraConnId,
    disconnect_token: CancellationToken,
    admitted_capabilities: u64,
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
            armed: true,
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

        let supervisor = self.supervisor.clone();
        let peer_id = self.peer_id.clone();
        let conn_id = self.conn_id;
        let _task = zakura_rt::task::try_spawn(erased(async move {
            supervisor.deregister(&peer_id, conn_id).await;
        }));
    }
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
}

struct ConnectionServeContext {
    limits: ZakuraConnectionLimits,
    accepted_capabilities: u64,
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
}

impl StreamWorkerContext {
    /// Credit a pause in which this reader waited for its own handler.
    fn credit_read_pause(&self, paused: Duration) {
        let (Some(cadence), Some(rules)) = (&self.cadence, self.messages) else {
            return;
        };
        if paused.is_zero() {
            return;
        }
        cadence
            .buckets
            .lock()
            .expect("Zakura cadence bucket mutex is never poisoned")
            .credit_pause(cadence.layout, rules, paused);
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
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum InboundMessageAdmission {
    Admit,
    Oversize,
    Throttled,
    /// A metered row overran its connection's byte bucket.
    ByteRateOverrun,
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
            |rule, payload_len| {
                cadence
                    .buckets
                    .lock()
                    .expect("Zakura cadence bucket mutex is never poisoned")
                    .charge_frame(cadence.layout, rule, payload_len)
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
/// calls `exhausted`, and an empty bucket doesn't refuse the frame.
///
/// # Panics
///
/// If no row of `rules` has the frame's type. The frame filter admits only
/// rows of the stream's table.
pub(crate) fn admit_tabled_message(
    frame: &Frame,
    rules: &[MessageRule],
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
    // transport stall. Exhaustion cannot prove a sender violation.
    if charge == CadenceCharge::Exhausted {
        exhausted();
    }
    InboundMessageAdmission::Admit
}

impl InboundMessageAdmission {
    /// The error an ordered stream's worker closes with for this result, or
    /// `None` if it admits the frame.
    pub(crate) fn ordered_error(self) -> Option<ZakuraHandlerError> {
        match self {
            InboundMessageAdmission::Admit => None,
            InboundMessageAdmission::Oversize => Some(ZakuraHandlerError::Oversize),
            InboundMessageAdmission::Throttled => Some(ZakuraHandlerError::RateLimited),
            InboundMessageAdmission::ByteRateOverrun => Some(ZakuraHandlerError::ByteRateOverrun),
        }
    }

    /// The close cause the ordered stream reader records for this result,
    /// or `None` if it admits the frame.
    #[cfg(any(test, zakura_sim))]
    pub(crate) fn ordered_close_cause(self) -> Option<&'static str> {
        self.ordered_error()
            .map(|error| read_close_cause(ReadPath::Ordered, &error))
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct NativeHandshakeNegotiated {
    pub(crate) limits: ZakuraAcceptedLimits,
    pub(crate) accepted_capabilities: u64,
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
            )?,
            None => BlockSyncService::new(block_sync_config)?,
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

/// zakura-quic acceptor for the Zakura `p2p-v2/3` ALPN.
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
    /// Every established or handshaking connection, in either direction.
    admission: Arc<Semaphore>,
    /// Inbound connections, which also hold an `admission` permit.
    inbound_admission: Arc<Semaphore>,
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
            admission: Arc::new(Semaphore::new(limits.max_connections)),
            inbound_admission: Arc::new(Semaphore::new(limits.max_inbound_connections())),
            pending_handshakes: Arc::new(Semaphore::new(limits.max_pending_handshakes)),
            inbound_handshakes: Arc::new(Semaphore::new(limits.max_inbound_handshakes())),
            resources,
            shutdown: CancellationToken::new(),
            limits,
            local_node_id: None,
            #[cfg(any(test, zakura_sim))]
            transport_observer: None,
        }
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
            self.limits.max_pending_handshakes,
            self.supervisor.max_connections_per_ip,
            (&self.admission, &self.inbound_admission),
            self.limits.max_connections,
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

    /// Returns an inbound attempt's cadence token. `action` names the reason in
    /// the sim's cadence observation.
    fn refund_cadence(&self, remote: SocketAddr, validated: bool, action: &'static str) {
        let mut counts = self.supervisor.ip_slots.counts();
        counts.refund_cadence(remote.ip(), validated, Instant::now());
        #[cfg(any(test, zakura_sim))]
        let hosts = counts.cadence_hosts(validated);
        drop(counts);
        #[cfg(any(test, zakura_sim))]
        self.observe_handshake_cadence(remote, validated, action, hosts);
        #[cfg(not(any(test, zakura_sim)))]
        let _ = action;
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
                    .max_connections
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
        let admission = self
            .claim_connection(true)
            .ok_or(InboundRefusal::Admission)?;
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
            InboundRefusal::PendingHandshake => {
                metrics::counter!("zakura.p2p.conn.rejected.pending_handshake").increment(1);
                ("pending_handshake", PENDING_HANDSHAKE_LIMIT)
            }
        };
        #[cfg(any(test, zakura_sim))]
        match refusal {
            InboundRefusal::Admission => self.handshake_refused(connection, label),
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

    /// Claims connection permits and their machine charge as one transition.
    fn claim_connection(&self, inbound: bool) -> Option<ConnectionAdmissionGuard> {
        let mut counts = self.supervisor.ip_slots.counts();
        let inbound_permit = if inbound {
            Some(self.inbound_admission.clone().try_acquire_owned().ok()?)
        } else {
            None
        };
        let shared = self.admission.clone().try_acquire_owned().ok()?;
        if !counts.claim_connection(inbound, self.limits.max_connections) {
            return None;
        }
        Some(ConnectionAdmissionGuard {
            #[cfg(any(test, zakura_sim))]
            observation: None,
            slots: self.supervisor.ip_slots.clone(),
            shared: Some(shared),
            inbound: inbound_permit,
        })
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

        // The overall deadline bounds peers that pace each control step.
        let result = match timeout(
            self.limits.control_timeout,
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
        let stream_sem = Arc::new(Semaphore::new(usize::from(limits.max_open_streams)));
        let limits_owner_bound = self.limits.owner_bound();
        let connection_resources = self.resources.connection(accepted_capabilities);
        self.resources.report_usage(limits_owner_bound);
        #[cfg(zakura_sim)]
        if let Some(observer) = &self.transport_observer {
            observer.connection(
                conn_id,
                connection_resources.probe(connection.load_probe(), &stream_sem, limits),
            );
        }
        // Set once, for the kinds this connection negotiated (zakura-quic
        // API-9). The handshake window stays inside it for the control stream.
        let _ = connection.set_limits(
            zakura_quic::ConnLimits::default()
                .with_receive_window(connection_resources.receive_window()),
        );
        let mut workers = JoinSet::new();
        let (session_exit_tx, mut session_exit_rx) = mpsc::unbounded_channel();
        let mut pending_sessions = PendingSessions::default();
        let mut incoming_setup: Option<BoxFuture<'static, Option<IncomingStreamSetup>>> = None;
        let mut open_limiter = TokenBucket::new(limits.stream_open_rate_per_second);
        let mut message_buckets = MessageRateBuckets::with_receive_window(u64::from(
            self.limits.quic.stream_receive_window_bytes,
        ));
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
            close_cause.record("resource_ordered_streams");
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
            close_cause.record("resource_queue_split");
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
        };

        if negotiated_ordered_streams.is_empty() {
            self.registry
                .add_peer(Peer::new_with_conn_id_and_direction_and_close_cause(
                    conn_id,
                    peer_id.clone(),
                    remote_ip,
                    accepted_capabilities,
                    context.direction,
                    HashMap::new(),
                    connection_token.clone(),
                    close_cause.clone(),
                ));
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
                        close_cause.record("stream_setup_failed");
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
                    .with_rtt_reader(Some(connection.rtt_reader())),
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
        loop {
            let session_deadline = pending_sessions.deadline();
            zakura_rt::select! {
                biased;
                _ = connection_token.cancelled() => break,
                _ = async {
                    match session_deadline {
                        Some(deadline) => zakura_rt::time::sleep_until(deadline).await,
                        None => future::pending().await,
                    }
                } => pending_sessions.expire(Instant::now()),
                _ = freshness_reaper(freshness_rx.clone(), limits.idle_timeout), if run_freshness_reaper => {
                    connection.close(VarInt::from_u32(ZAKURA_CLOSE_NEUTRAL), b"idle");
                    close_cause.record("idle_timeout");
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
                                .with_rtt_reader(Some(connection.rtt_reader())),
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
                            close_cause.record("unexpected_stream");
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
                            .with_rtt_reader(Some(connection.rtt_reader())),
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
                            };
                            incoming_setup = self.begin_bi_stream_setup(send, recv, &mut admission);
                        }
                        Err(error) => {
                            debug!(?error, "Zakura connection stopped accepting streams");
                            close_cause.record("accept_failed");
                            break;
                        }
                    }
                }
                outbound = outbound_rx.recv() => {
                    let Some(outbound) = outbound else {
                        close_cause.record("outbound_closed");
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
                                    close_cause.record("bad_response");
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
                close_cause.record(rejection.reason());
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
                close_cause.record(rejection.reason());
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
                close_cause.record(rejection.reason());
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
                close_cause.record(rejection.reason());
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
            match pending_sessions.reserve_or_share(layout, &self.registry, admission.direction) {
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
                    admission.close_cause.record("invalid_service_session");
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
        let ordered_stream_count = self
            .registry
            .persistent_streams_for_escalation(
                context.accepted_capabilities,
                &peer_id,
                context.direction,
            )
            .len();
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
            .register(
                context.conn.id,
                peer_id,
                remote_ip,
                context.transcript_hash,
                outbound_handle,
                connection_token.clone(),
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
        drop(context.control_handshake);
        // Registration refunds the attempt's cadence token, even when a local
        // capability change closes the connection below.
        if matches!(registration, ZakuraRegistration::Registered { .. })
            && context.direction == ServicePeerDirection::Inbound
        {
            self.refund_cadence(
                connection.admitted_addr(),
                connection.source_validated(),
                "registered",
            );
        }
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
                );
                self.serve_connection(
                    connection,
                    peer_id,
                    remote_ip,
                    outbound_rx,
                    RegisteredConnectionServeContext {
                        limits: context.limits,
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
        if counts.connections_full(true, self.limits.max_connections)
            || self.admission.available_permits() == 0
            || self.inbound_admission.available_permits() == 0
        {
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
                .max_connections
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
        }
    }

    fn cancel_admission(&self, incoming: &IncomingInfo) {
        self.finish_admission(false);
        self.refund_cadence(incoming.remote, incoming.validated, "cancelled");
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
            handler.finish_admission(false);
            // Dials took the room during TLS, so the refusal is local.
            handler.refund_cadence(
                connection.admitted_addr(),
                connection.source_validated(),
                "refused",
            );
        }
        Box::pin(async move { handler.serve_inbound(connection, claim).await })
    }
}

/// The budget an inbound claim found full.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InboundRefusal {
    /// The connection cap or its inbound share.
    Admission,
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
fn remote_bootstrap_peer_count(
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
    )
    .await
}

pub(crate) async fn spawn_zakura_endpoint_with_peer_registry(
    config: &Config,
    sink_factory: impl FnOnce(ZakuraSupervisorHandle, ZakuraTrace) -> Arc<dyn Service> + Send + 'static,
    header_sync_driver_startup: Option<ZakuraHeaderSyncDriverStartup>,
    custom_services: Vec<CustomService>,
    peer_registry: PeerRegistry,
) -> Result<Option<ZakuraEndpoint>, BoxError> {
    spawn_zakura_endpoint_inner(
        config,
        Box::new(sink_factory),
        header_sync_driver_startup,
        custom_services,
        Some(peer_registry),
    )
    .await
}

/// Starts the endpoint. `sink_factory` is boxed so this large function is compiled once for
/// every caller's factory type.
async fn spawn_zakura_endpoint_inner(
    config: &Config,
    sink_factory: Box<dyn FnOnce(ZakuraSupervisorHandle, ZakuraTrace) -> Arc<dyn Service> + Send>,
    header_sync_driver_startup: Option<ZakuraHeaderSyncDriverStartup>,
    custom_services: Vec<CustomService>,
    peer_registry: Option<PeerRegistry>,
) -> Result<Option<ZakuraEndpoint>, BoxError> {
    if !config.v2_p2p() {
        return Ok(None);
    }

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
    let custom_kinds: Vec<u16> = custom_services
        .iter()
        .flat_map(|custom| custom.service.streams().iter().map(|stream| stream.kind))
        .filter(|kind| !named_kinds.iter().any(|(named, _)| named == kind))
        .collect();
    let budgeted_max_connections = config.zakura.budgeted_max_connections_for_node(
        &config.network,
        local_node_id,
        &custom_kinds,
    )?;
    if budgeted_max_connections < config.zakura.max_connections {
        warn!(
            max_connections = config.zakura.max_connections,
            budgeted_max_connections,
            peer_memory_budget_bytes = config.zakura.peer_memory_budget_bytes,
            connection_memory = ?config.zakura.connection_memory(&config.network, &custom_kinds),
            "lowering [network.zakura] max_connections to what peer_memory_budget_bytes \
             admits at the configured QUIC windows and reactor shares; raise the budget or \
             lower max_connections to silence this"
        );
    }
    let limits = ZakuraLocalLimits::for_node(config, &custom_kinds);
    let node_envelope = crate::zakura::node_envelope::NodeEnvelope::for_node(
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
    let handshake_config = ZakuraHandshakeConfig::for_network_with_dev_cohort(
        &config.network,
        config.zakura.dev_network.as_deref(),
    );
    let sought_services = sought_services_with_custom(&custom_services);
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
            .for_node_connections(limits.max_connections),
        limits.max_frame_bytes,
        limits.max_connections,
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
                    .for_node_connections(limits.max_connections),
            )
            .with_range_source(driver_startup.block_range_source.clone());
            startup.shutdown = header_sync_shutdown.clone();
            startup.trace = trace.clone();
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
    let discovery_service = Arc::new(super::DiscoveryService::with_sync_services(
        discovery.clone(),
        header_sync.clone(),
        block_sync.clone(),
        trace.clone(),
        limits.max_connections,
    ));
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
            .for_node_connections(limits.max_connections),
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
    .with_local_node_id(local_node_id);
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
    /// The supervisor registered the peer on this connection.
    Registered,
    /// This node's admission limits refused to register the peer, or a local
    /// capability change closed its registration.
    LocalLimit,
}

/// The channels on which a discovery or maintained dial reports its progress.
pub(crate) struct DialReport {
    /// Receives the address whose handshake TLS authenticated, before the
    /// control handshake starts.
    pub(crate) admitted_addr: oneshot::Sender<SocketAddr>,
    /// Receives how registration ended. The dial drops it unsent when it ends
    /// before registration, or when this node refuses the peer for a reason
    /// other than a local limit.
    pub(crate) registration: oneshot::Sender<DialRegistration>,
}

/// Selects whether a native dial may use the transport's reserved capacity.
pub(crate) enum NativeDialKind {
    /// Bootstrap and maintained dials may use every live slot.
    Maintained,
    /// Discovery leaves room for an inbound connection and maintained dials.
    Discovery,
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
    let _admission = endpoint
        .handler
        .claim_connection(false)
        .ok_or(ZakuraHandlerError::ResourceLimit(ADMISSION_LIMIT))?;
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
            NativeDialKind::Discovery => endpoint.quic.connect_discovery(node_addr, &offered).await,
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
    let _admission = {
        let mut guard = _admission;
        guard.observation = endpoint.handler.handshake_observation(&connection);
        guard
    };
    let registration_report = report.map(|report| {
        // The receiver is gone when the caller stopped waiting for the result.
        let _ = report.admitted_addr.send(connection.admitted_addr());
        report.registration
    });
    let remote_node_id = connection.remote_id();
    let peer_id = ZakuraPeerId::from_node_id(remote_node_id);
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
    let (error_tx, mut error_rx) = mpsc::channel::<ZakuraHandlerError>(1);
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
                    let _ = reader_context.freshness_tx.send(Instant::now());
                    // While the reader waited for raw capacity, the peer kept
                    // sending into transport buffers, as during a queue wait below.
                    reader_context.credit_read_pause(admitted.grant.waited());
                    match admit_inbound_message(&admitted.frame, &reader_context, stream_kind)
                        .ordered_error()
                    {
                        None => {
                            #[cfg(any(test, zakura_sim))]
                            if let Some(tap) = reader_context.precheck.tap() {
                                tap.queue_tag();
                            }
                            let waiting_since = Instant::now();
                            let forwarded = zakura_rt::select! {
                                biased;
                                _ = reader_context.connection_token.cancelled() => break,
                                _ = reader_context.stream_token.cancelled() => break,
                                result = inbound_tx.send(admitted) => result,
                            };
                            // While this reader waited for its own handler, the
                            // peer kept sending into transport buffers. Credit the
                            // pause so the buffered burst never counts against it.
                            reader_context.credit_read_pause(waiting_since.elapsed());
                            if forwarded.is_err() {
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
            // Any error is terminal. `Closed` is a clean peer-initiated close;
            // every other error must disconnect the peer, under the cause
            // `read_close_cause` gives it. Forward the error first so the main
            // loop can map it to a reset code, then cancel the connection
            // ourselves so the disconnect is guaranteed even if the main loop
            // tore the worker down for a stopped outbound write before
            // processing it.
            let must_disconnect = !matches!(error, ZakuraHandlerError::Closed);
            if !must_disconnect && !reader_context.stream_token.is_cancelled() {
                if let Some(cause) = &reader_failure_cause {
                    // Publish the cause before waking either the service EOF or
                    // the sibling worker's cancellation path.
                    cause.record(OrderedStreamFailure::RemoteClose);
                }
            }
            let cause = read_close_cause(ReadPath::Ordered, &error);
            let _ = error_tx.send(error).await;
            if must_disconnect {
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
                            context.close_cause.record("ordered_write_error");
                            context.connection_token.cancel();
                            break;
                        }
                    }
                    None => {
                        outbound_rx = None;
                    }
                }
            }
            error = error_rx.recv() => {
                if let Some(error) = error {
                    if let Some((code, cause)) = ordered_read_close(&error) {
                        // The reader already emitted any oversize-desync diagnostic.
                        if code == ZAKURA_CLOSE_BAD_PRELUDE {
                            debug!(?error, "closing Zakura stream worker");
                        }
                        let _ = send.reset(VarInt::from_u32(code));
                        context.close_cause.record(cause);
                        context.connection_token.cancel();
                    }
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
/// stream's reset code and the close cause to record. `None` is a clean
/// close, which resets and records nothing.
fn ordered_read_close(error: &ZakuraHandlerError) -> Option<(u32, &'static str)> {
    let code = match error {
        ZakuraHandlerError::Closed => return None,
        ZakuraHandlerError::Oversize => ZAKURA_CLOSE_OVERSIZE,
        ZakuraHandlerError::RateLimited | ZakuraHandlerError::ByteRateOverrun => {
            ZAKURA_CLOSE_RATE_LIMIT
        }
        _ => ZAKURA_CLOSE_BAD_PRELUDE,
    };
    Some((code, read_close_cause(ReadPath::Ordered, error)))
}

/// The frame reader that hit a read error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReadPath {
    /// The reader of a persistent ordered stream.
    Ordered,
    /// The responder reading a request stream's one request frame.
    Request,
}

/// The close cause for a frame-read error on `path`.
///
/// Only the `*_read_rejected` causes are peer faults: the frame broke its
/// stream's table or frame cap, or ended before its declared length. Timeout
/// and transport causes are neutral, and every unlisted error (such as `Io`)
/// is transport. A request stream that closes before its request frame ends is
/// transport too. The ordered path never records `Closed`, which is a clean close.
///
/// The ordered reader and its worker race to record one error, and
/// [`CloseCause`] keeps the first writer, so both record through this. That
/// includes the reader's admission failures, which keep their own causes.
fn read_close_cause(path: ReadPath, error: &ZakuraHandlerError) -> &'static str {
    use zakura_quic::ReadExactError::FinishedEarly;
    use ZakuraHandlerError::*;

    let rejected = matches!(
        error,
        RejectedFrame { .. } | OversizeFrame { .. } | QuicRead(FinishedEarly(_))
    );
    match (path, error) {
        (ReadPath::Ordered, Oversize) => "ordered_oversize",
        (ReadPath::Ordered, RateLimited) => "ordered_rate_limited",
        (ReadPath::Ordered, ByteRateOverrun) => "ordered_byte_rate_overrun",
        (ReadPath::Ordered, _) if rejected => "ordered_read_rejected",
        (ReadPath::Request, _) if rejected => "request_read_rejected",
        (ReadPath::Ordered, Timeout(_)) => "ordered_read_timeout",
        (ReadPath::Request, Timeout(_)) => "request_read_timeout",
        (ReadPath::Ordered, _) => "ordered_read_transport",
        (ReadPath::Request, _) => "request_read_transport",
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
        context.close_cause.record("request_without_id");
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
        Ok(AdmittedFrame { frame, grant }) => (frame, grant),
        Err(error) => {
            debug!(
                ?error,
                "closing Zakura request stream with invalid request frame"
            );
            let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_BAD_PRELUDE));
            context
                .close_cause
                .record(read_close_cause(ReadPath::Request, &error));
            context.connection_token.cancel();
            return;
        }
    };

    let _ = context.freshness_tx.send(Instant::now());
    match admit_inbound_message(&frame, &context, prelude.stream_kind) {
        InboundMessageAdmission::Admit => {}
        InboundMessageAdmission::Oversize => {
            let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_OVERSIZE));
            context.close_cause.record("request_oversize");
            context.connection_token.cancel();
            return;
        }
        InboundMessageAdmission::ByteRateOverrun => {
            // The layout keeps metered rows off request streams, so this is
            // unreachable today. Close as a persistent reader would.
            let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_RATE_LIMIT));
            context.close_cause.record("request_byte_rate_overrun");
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
                if let Err(SinkReject::Protocol(error)) =
                    registry.check_request(prelude.stream_kind, &frame)
                {
                    debug!(
                        ?error,
                        "Zakura inbound sink rejected a stopped protocol-invalid request"
                    );
                    context.close_cause.record("request_protocol_reject");
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
        Err(SinkReject::Protocol(error)) => {
            debug!(
                ?error,
                "Zakura inbound sink rejected protocol-invalid request"
            );
            let _ = send.reset(VarInt::from_u32(ZAKURA_CLOSE_BAD_PRELUDE));
            context.close_cause.record("request_protocol_reject");
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
    .map(|(frame, grant)| AdmittedFrame { frame, grant })
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
    let mut payload = vec![0; payload_len];
    read_frame_payload(recv, &mut payload, read_timeout).await?;
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

async fn read_frame_payload(
    recv: &mut RecvStream,
    payload: &mut [u8],
    read_timeout: Duration,
) -> Result<(), ZakuraHandlerError> {
    match timeout(read_timeout, recv.read_exact(payload))
        .await
        .map_err(|_| ZakuraHandlerError::Timeout("frame payload"))?
    {
        Ok(()) => Ok(()),
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
    /// Buckets for a connection with no stream receive window, as tests use.
    #[cfg(test)]
    fn new() -> Self {
        Self::with_receive_window(0)
    }

    /// Buckets for a connection whose stream receive window is
    /// `receive_window`, which deepens its byte buckets.
    fn with_receive_window(receive_window: u64) -> Self {
        Self {
            streams: HashMap::new(),
            cadence: Arc::new(StdMutex::new(
                CadenceBuckets::new(C::default()).with_receive_window(receive_window),
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
}

/// The cadence scope for `stream` in `layout`, if the stream has a table.
fn cadence_scope(
    buckets: &MessageRateBuckets,
    stream: Stream,
    layout: u16,
) -> Option<CadenceScope> {
    stream.messages.map(|_| CadenceScope {
        buckets: buckets.cadence.clone(),
        layout,
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
#[path = "../../tests/unit/zakura/handler/mod.rs"]
mod tests;
