use std::{
    collections::HashSet,
    fmt,
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::time::Instant;

use indexmap::IndexSet;
use zakura_chain::{
    block,
    transaction::{UnminedTx, UnminedTxId},
};
use zakura_node_services::block_memory::BlockHandle;

use super::super::types::Nonce;
use crate::{zakura::ZakuraPeerId, PeerSocketAddr};

#[cfg(any(test, feature = "proptest-impl"))]
use proptest::{arbitrary::any, collection::vec, strategy::Strategy};
#[cfg(any(test, feature = "proptest-impl"))]
use proptest_derive::Arbitrary;

#[cfg(any(test, feature = "proptest-impl"))]
fn arbitrary_block_hashes() -> impl Strategy<Value = IndexSet<block::Hash>> {
    vec(any::<block::Hash>(), 0..100).prop_map(|hashes| hashes.into_iter().collect())
}

/// The lifetime of a possible second block request, shared by the downloader and peer set.
#[derive(Clone, Debug)]
pub struct BlockHedge {
    started: Instant,
    state: Arc<Mutex<BlockDownloadState>>,
}

#[derive(Debug, Default)]
struct BlockDownloadState {
    first: Option<FirstDispatch>,
    hedge: HedgeDispatch,
    satisfied: bool,
    legacy_timed_out: bool,
    legacy_written_before_timeout: bool,
}

#[derive(Debug, Default, Eq, PartialEq)]
enum HedgeDispatch {
    #[default]
    Open,
    Closed,
    Dispatched,
}

/// The first handoff of this download to a peer. Native peers have no legacy address or score.
#[derive(Clone, Copy, Debug)]
pub(crate) struct FirstDispatch {
    pub(crate) at: Instant,
    pub(crate) legacy_peer: Option<PeerSocketAddr>,
    pub(crate) score: Option<Duration>,
}

impl BlockHedge {
    /// How long after starting a download the peer set may dispatch a second request.
    pub const WINDOW: Duration = Duration::from_secs(5);

    /// Open a second-request window for the download started at `started`.
    pub fn new(started: Instant) -> Self {
        Self {
            started,
            state: Arc::default(),
        }
    }

    fn state(&self) -> std::sync::MutexGuard<'_, BlockDownloadState> {
        self.state
            .lock()
            .expect("block download state is never poisoned")
    }

    /// Stop new legacy hedges when the primary switches to native or finishes.
    /// Repeated calls report the same outcome: whether a hedge was already dispatched.
    pub fn close(&self) -> bool {
        let mut state = self.state();
        if state.hedge == HedgeDispatch::Open {
            state.hedge = HedgeDispatch::Closed;
        }
        state.hedge == HedgeDispatch::Dispatched
    }

    /// Return the age of the download while another request may still be dispatched.
    pub fn elapsed_if_open(&self) -> Option<Duration> {
        let state = self.state();
        let age = self.started.elapsed();
        (state.hedge == HedgeDispatch::Open && !state.satisfied && age < Self::WINDOW)
            .then_some(age)
    }

    /// Claim dispatch at most once, unless completion or expiry has closed the window.
    pub fn claim_dispatch(&self) -> bool {
        let mut state = self.state();
        if state.hedge != HedgeDispatch::Open
            || state.satisfied
            || self.started.elapsed() >= Self::WINDOW
        {
            return false;
        }
        state.hedge = HedgeDispatch::Dispatched;
        true
    }

    /// Record that a reply contained the requested block. Already-dispatched exchanges can drain.
    pub fn mark_satisfied(&self) {
        self.state().satisfied = true;
    }

    pub(crate) fn is_satisfied(&self) -> bool {
        self.state().satisfied
    }

    pub(crate) fn first_dispatch(&self) -> Option<FirstDispatch> {
        self.state().first
    }

    /// Publish the dual-stack deadline before the legacy future is dropped.
    pub(crate) fn legacy_timeout(&self) {
        self.state().legacy_timed_out = true;
    }

    pub(crate) fn legacy_request_written(&self) {
        let mut state = self.state();
        state.legacy_written_before_timeout = !state.legacy_timed_out;
    }

    pub(crate) fn timed_out_after_write(&self) -> bool {
        let state = self.state();
        state.legacy_timed_out && state.legacy_written_before_timeout
    }

    /// Commit a primary or fallback to its selected peer, ordered against a winning reply.
    /// The closure only enqueues work in a ready service or reserved channel slot.
    pub(crate) fn dispatch_if_needed<T>(
        &self,
        peer: Option<PeerSocketAddr>,
        score: Option<Duration>,
        dispatch: impl FnOnce() -> T,
    ) -> Result<T, crate::BoxError> {
        let mut state = self.state();
        if peer.is_some() && state.legacy_timed_out {
            return Err("legacy block request expired".into());
        }
        if state.satisfied {
            return Err("block already delivered".into());
        }
        if state.first.is_none() && score.is_none() && state.hedge == HedgeDispatch::Open {
            metrics::counter!("pool.route_inv.hedge.unscored_first.count").increment(1);
            // The first dispatch fixes the score used by every later comparison.
            state.hedge = HedgeDispatch::Closed;
        }
        state.first.get_or_insert_with(|| FirstDispatch {
            at: Instant::now(),
            legacy_peer: peer,
            score,
        });
        Ok(dispatch())
    }
}

impl PartialEq for BlockHedge {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.state, &other.state)
    }
}

impl Eq for BlockHedge {}

/// Authenticated source for inventory advertised through Zebra's internal network API.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
#[cfg_attr(any(test, feature = "proptest-impl"), derive(Arbitrary))]
pub enum PeerSource {
    /// A peer connected through the legacy TCP socket transport.
    LegacySocket(PeerSocketAddr),

    /// A peer authenticated by the Zakura P2P v2 transport.
    Zakura(ZakuraPeerId),
}

impl From<PeerSocketAddr> for PeerSource {
    fn from(peer_addr: PeerSocketAddr) -> Self {
        Self::LegacySocket(peer_addr)
    }
}

/// A block to advertise, with what the advertiser needs to size the broadcast.
///
/// Local callers that hold the block fill in `size`. An `inv` received from a peer carries only
/// the hash, so it is `None` there.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[cfg_attr(any(test, feature = "proptest-impl"), derive(Arbitrary))]
pub struct AdvertisedBlock {
    /// The block hash.
    pub hash: block::Hash,
    /// The block's serialized size in bytes, if known. Legacy gossip sizes the broadcast's
    /// advert window from it.
    pub size: Option<u32>,
}

impl AdvertisedBlock {
    /// Returns an advertisement of the block with `hash` and serialized `size`.
    pub fn new(hash: block::Hash, size: usize) -> Self {
        Self {
            hash,
            // A size that does not fit is unknown, rather than a misleadingly small block.
            size: u32::try_from(size).ok(),
        }
    }
}

impl From<block::Hash> for AdvertisedBlock {
    /// Returns an advertisement of the block with `hash` and unknown size.
    fn from(hash: block::Hash) -> Self {
        Self { hash, size: None }
    }
}

/// A network request, represented in internal format.
///
/// The network layer aims to abstract away the details of the Bitcoin wire
/// protocol into a clear request/response API. Each [`Request`] documents the
/// possible [`Response`s](super::Response) it can generate; it is fine (and
/// recommended!) to match on the expected responses and treat the others as
/// `unreachable!()`, since their return indicates a bug in the network code.
///
/// # Cancellations
///
/// The peer set handles cancelled requests (i.e., requests where the future
/// returned by `Service::call` is dropped before it resolves) on a best-effort
/// basis. Requests are routed to a particular peer connection, and then
/// translated into Zcash protocol messages and sent over the network. If a
/// request is cancelled after it is submitted but before it is processed by a
/// peer connection, no messages will be sent. Otherwise, if it is cancelled
/// while waiting for a response, the peer connection resets its state and makes
/// a best-effort attempt to ignore any messages responsive to the cancelled
/// request, subject to limitations in the underlying Zcash protocol.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(any(test, feature = "proptest-impl"), derive(Arbitrary))]
pub enum Request {
    /// Disconnect a legacy peer without changing its misbehavior score or ban status.
    /// The peer set handles this request locally and returns [`super::Response::Nil`].
    DisconnectPeer(PeerSocketAddr),

    /// Requests additional peers from the server.
    ///
    /// # Response
    ///
    /// Returns [`Response::Peers`](super::Response::Peers).
    Peers,

    /// Heartbeats triggered on peer connection start.
    ///
    /// This is included as a bit of a hack, it should only be used
    /// internally for connection management. You should not expect to
    /// be firing or handling `Ping` requests or `Pong` responses.
    #[doc(hidden)]
    Ping(Nonce),

    /// Request block data by block hashes.
    ///
    /// The ordered set deduplicates block hashes while preserving request order.
    /// Inbound services use that order when applying response limits, and
    /// outbound services separately track pending responses in a `HashSet`.
    ///
    /// If this requests a recently-advertised block, the peer set will make a
    /// best-effort attempt to route the request to a peer that advertised the
    /// block. This routing is only used for request sets of size 1.
    /// Otherwise, it is routed using the normal load-balancing strategy.
    ///
    /// The list contains zero or more block hashes.
    ///
    /// # Returns
    ///
    /// Returns [`Response::Blocks`](super::Response::Blocks).
    BlocksByHash(
        #[cfg_attr(
            any(test, feature = "proptest-impl"),
            proptest(strategy = "arbitrary_block_hashes()")
        )]
        IndexSet<block::Hash>,
    ),

    /// Request block data by hashes on a connection that negotiated early block relay.
    ///
    /// Only the connection creates this request, for an inbound `getdata` from a peer that set
    /// [`PeerServices::NODE_COMPACT_BLOCKS`](crate::PeerServices::NODE_COMPACT_BLOCKS)
    /// while this node set it too. The inbound service may serve early-relay bodies before
    /// their contextual commit; it must never serve them for [`Request::BlocksByHash`].
    /// Outbound callers use [`Request::BlocksByHash`].
    ///
    /// # Returns
    ///
    /// Returns [`Response::Blocks`](super::Response::Blocks).
    EarlyBlocksByHash(
        #[cfg_attr(
            any(test, feature = "proptest-impl"),
            proptest(strategy = "arbitrary_block_hashes()")
        )]
        IndexSet<block::Hash>,
    ),

    /// Request block data by block hashes from a known advertising peer.
    ///
    /// This is used by authenticated transports that can safely route a
    /// gossiped-inventory download back to the peer that advertised it.
    ///
    /// The legacy peer set prefers ready peers that advertised the inventory. If there are none,
    /// it sends a single-block request to a ready legacy `source` that isn't missing the block.
    ///
    /// # Returns
    ///
    /// Returns [`Response::Blocks`](super::Response::Blocks).
    BlocksByHashFrom {
        /// Requested block hashes.
        #[cfg_attr(
            any(test, feature = "proptest-impl"),
            proptest(strategy = "arbitrary_block_hashes()")
        )]
        hashes: IndexSet<block::Hash>,
        /// Peer that advertised the inventory.
        source: PeerSource,
    },

    /// First request for an advertised block with a shared second-request window.
    BlockByHashPrimary {
        /// The block to request.
        hash: block::Hash,
        /// Preferred source when known.
        source: Option<PeerSource>,
        /// Shared with the downloader and the optional second request.
        #[cfg_attr(
            any(test, feature = "proptest-impl"),
            proptest(value = "BlockHedge::new(Instant::now())")
        )]
        attempt: BlockHedge,
    },

    /// Request one block a legacy peer announced, as a second request while a first request for
    /// it is still outstanding.
    ///
    /// The legacy peer set considers only ready peers that announced the block, have no request
    /// outstanding, and did not get the first request. It sends the request to the one expected
    /// to answer soonest, but only if that peer should deliver the block clearly before the first
    /// request does, or if the first request has run well past its expected time (see
    /// `[network.block_source_score] second_request`). It sends at most one such request per
    /// block.
    ///
    /// # Returns
    ///
    /// Returns [`Response::Blocks`](super::Response::Blocks), or fails at once with
    /// [`NoFasterBlockSource`](crate::NoFasterBlockSource) when no peer was asked, so the caller
    /// can ask again later.
    BlockByHashHedge {
        /// The block to request.
        hash: block::Hash,
        /// Shared dispatch window, checked by the peer set after any queue wait.
        #[cfg_attr(
            any(test, feature = "proptest-impl"),
            proptest(value = "BlockHedge::new(Instant::now())")
        )]
        attempt: BlockHedge,
    },

    /// Request transactions by their unmined transaction ID.
    ///
    /// v4 transactions use a legacy transaction ID, and
    /// v5 transactions use a witnessed transaction ID.
    ///
    /// The set deduplicates transaction IDs and supports pending-response
    /// membership checks.
    ///
    /// If this requests a recently-advertised transaction, the peer set will
    /// make a best-effort attempt to route the request to a peer that advertised
    /// the transaction. This routing is only used for request sets of size 1.
    /// Otherwise, it is routed using the normal load-balancing strategy.
    ///
    /// The list contains zero or more unmined transaction IDs.
    ///
    /// # Returns
    ///
    /// Returns [`Response::Transactions`](super::Response::Transactions).
    TransactionsById(HashSet<UnminedTxId>),

    /// Request transactions by their unmined transaction ID from a known
    /// advertising peer.
    ///
    /// # Returns
    ///
    /// Returns [`Response::Transactions`](super::Response::Transactions).
    TransactionsByIdFrom {
        /// Requested transaction IDs.
        ids: HashSet<UnminedTxId>,
        /// Peer that advertised the inventory.
        source: PeerSource,
    },

    /// Request block hashes of subsequent blocks in the chain, given hashes of
    /// known blocks.
    ///
    /// The known blocks list contains zero or more block hashes.
    ///
    /// # Returns
    ///
    /// Returns
    /// [`Response::BlockHashes`](super::Response::BlockHashes).
    ///
    /// # Warning
    ///
    /// This is implemented by sending a `getblocks` message. Bitcoin nodes
    /// respond to `getblocks` with an `inv` message containing a list of the
    /// subsequent blocks. However, Bitcoin nodes *also* send `inv` messages
    /// unsolicited in order to gossip new blocks to their peers. These gossip
    /// messages can race with the response to a `getblocks` request, and there
    /// is no way for the network layer to distinguish them. For this reason, the
    /// response may occasionally contain a single hash of a new chain tip rather
    /// than a list of hashes of subsequent blocks. We believe that unsolicited
    /// `inv` messages will always have exactly one block hash, so the connection
    /// also hands a one-hash response to the inbound service as a
    /// [`Request::AdvertiseBlock`], and the gossip downloader may fetch that
    /// block as well.
    FindBlocks {
        /// Hashes of known blocks, ordered from highest height to lowest height.
        //
        // TODO: make this into an IndexMap - an ordered unique list of hashes (#2244)
        known_blocks: Vec<block::Hash>,
        /// Optionally, the last block hash to request.
        stop: Option<block::Hash>,
    },

    /// Request headers of subsequent blocks in the chain, given hashes of
    /// known blocks.
    ///
    /// The known blocks list contains zero or more block hashes.
    ///
    /// # Returns
    ///
    /// Returns
    /// [`Response::BlockHeaders`](super::Response::BlockHeaders).
    FindHeaders {
        /// Hashes of known blocks, ordered from highest height to lowest height.
        //
        // TODO: make this into an IndexMap - an ordered unique list of hashes (#2244)
        known_blocks: Vec<block::Hash>,
        /// Optionally, the last header to request.
        stop: Option<block::Hash>,
    },

    /// Push an unmined transaction to a remote peer, without advertising it to them first.
    ///
    /// This is implemented by sending an unsolicited `tx` message.
    ///
    /// The second field is the source peer that sent us this `tx`:
    /// `Some(source)` when the push came from a remote peer, and `None` when
    /// Zebra originates the push itself. Used by the mempool downloader to
    /// enforce a per-peer queue cap. See `GHSA-4fc2-h7jh-287c`.
    ///
    /// # Returns
    ///
    /// Returns [`Response::Nil`](super::Response::Nil).
    PushTransaction(UnminedTx, Option<PeerSource>),

    /// A block that a remote peer pushed to us, without advertising it first.
    ///
    /// Connections create this request from an unsolicited `block` message and send it to the
    /// inbound service, which verifies the block as if it had downloaded it. Nodes push the
    /// blocks they mine, as [`Request::AdvertiseMinedBlock`] describes. They push relayed blocks
    /// only if [`crate::Config::push_relayed_blocks`] is set.
    ///
    /// The second field is the peer that pushed the block. Used by the inbound block
    /// downloader to enforce its per-peer queue cap.
    ///
    /// # Returns
    ///
    /// Returns [`Response::Nil`](super::Response::Nil).
    PushBlock(
        #[cfg_attr(
            any(test, feature = "proptest-impl"),
            proptest(strategy = "any::<std::sync::Arc<block::Block>>().prop_map(Into::into)")
        )]
        BlockHandle,
        Option<PeerSource>,
    ),

    /// Advertise a set of unmined transactions to all peers.
    ///
    /// Both Zebra and zcashd sometimes advertise multiple transactions at once.
    ///
    /// This is implemented by sending an `inv` message containing the unmined
    /// transaction IDs, allowing the remote peer to choose whether to download
    /// them. Remote peers who choose to download the transaction will generate a
    /// [`Request::TransactionsById`] against the "inbound" service passed to
    /// [`init`](crate::init).
    ///
    /// v4 transactions use a legacy transaction ID, and
    /// v5 transactions use a witnessed transaction ID.
    ///
    /// The list contains zero or more transaction IDs.
    ///
    /// The peer set routes this request specially, sending it to *half of*
    /// the available peers.
    ///
    /// The second field is the source peer that sent us this `inv`:
    /// `Some(source)` when the advertisement was relayed from a remote peer,
    /// and `None` when Zebra originates the advertisement itself (e.g. the
    /// mempool gossip task). Used by the mempool downloader to enforce a
    /// per-peer queue cap. See `GHSA-4fc2-h7jh-287c`.
    ///
    /// # Returns
    ///
    /// Returns [`Response::Nil`](super::Response::Nil).
    AdvertiseTransactionIds(HashSet<UnminedTxId>, Option<PeerSource>),

    /// Advertise a block to all peers.
    ///
    /// This is implemented by sending an `inv` message containing the
    /// block hash, allowing the remote peer to choose whether to download
    /// it. Remote peers who choose to download the block will generate a
    /// [`Request::BlocksByHash`] against the "inbound" service passed to
    /// [`init`](crate::init).
    ///
    /// Legacy gossip progressively advertises to connected peers in random order.
    /// A bounded number of advertisements await block uploads at once. That window is sized
    /// from the block's serialized size, so the first peers receive a large block sooner.
    /// If [`crate::Config::push_relayed_blocks`] is set, legacy gossip also pushes the block
    /// to the initial group's peers that accept pushes, like [`Request::AdvertiseMinedBlock`].
    ///
    /// The second field is the source peer that sent us this `inv`:
    /// `Some(source)` when the advertisement was relayed from a remote peer,
    /// and `None` when Zebra originates the advertisement itself (for
    /// example from the sync gossip task). Consumers use the source to
    /// apply per-peer policies such as the inbound download per-IP cap.
    ///
    /// # Returns
    ///
    /// Returns [`Response::Nil`](super::Response::Nil).
    AdvertiseBlock(AdvertisedBlock, Option<PeerSource>),

    /// Advertise a block to connected peers, paced and pushed like [`Request::AdvertiseBlock`].
    /// Tip gossip uses [`Request::AdvertiseBlock`] or [`Request::AdvertiseMinedBlock`].
    AdvertiseBlockToAll(AdvertisedBlock),

    /// Advertise a locally mined block, prioritizing measured heartbeat RTT.
    /// Every fourth advertisement goes to a random remaining peer instead.
    /// Legacy gossip holds a bounded number of slots through block uploads.
    /// In the initial group, legacy gossip pushes the block to peers that accept pushes,
    /// then advertises it: see [`Request::PushBlock`].
    /// Native gossip uses its existing broadcast policy.
    AdvertiseMinedBlock(AdvertisedBlock),

    /// Advertise a block that passed early-relay checks to legacy peers that negotiated early
    /// block relay, paced like [`Request::AdvertiseBlock`].
    ///
    /// The block must already be servable from the early-relay cache. Connections without
    /// early block relay, and the Zakura P2P v2 stack, never receive this advertisement.
    /// Returns at once when no connected peer negotiated early block relay.
    ///
    /// # Returns
    ///
    /// Returns [`Response::Nil`](super::Response::Nil).
    AdvertiseEarlyBlock(AdvertisedBlock),

    /// Request the contents of this node's mempool.
    ///
    /// # Returns
    ///
    /// Returns [`Response::TransactionIds`](super::Response::TransactionIds).
    MempoolTransactionIds,
}

impl fmt::Display for Request {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(&match self {
            Request::DisconnectPeer(_) => "DisconnectPeer".to_string(),
            Request::Peers => "Peers".to_string(),
            Request::Ping(_) => "Ping".to_string(),

            Request::BlocksByHash(hashes) => {
                format!("BlocksByHash({})", hashes.len())
            }
            Request::BlocksByHashFrom { hashes, .. } => {
                format!("BlocksByHashFrom({})", hashes.len())
            }
            Request::EarlyBlocksByHash(hashes) => {
                format!("EarlyBlocksByHash({})", hashes.len())
            }
            Request::BlockByHashPrimary { .. } => "BlockByHashPrimary".to_string(),
            Request::BlockByHashHedge { .. } => "BlockByHashHedge".to_string(),
            Request::TransactionsById(ids) => format!("TransactionsById({})", ids.len()),
            Request::TransactionsByIdFrom { ids, .. } => {
                format!("TransactionsByIdFrom({})", ids.len())
            }

            Request::FindBlocks { known_blocks, stop } => format!(
                "FindBlocks {{ known_blocks: {}, stop: {} }}",
                known_blocks.len(),
                if stop.is_some() { "Some" } else { "None" },
            ),
            Request::FindHeaders { known_blocks, stop } => format!(
                "FindHeaders {{ known_blocks: {}, stop: {} }}",
                known_blocks.len(),
                if stop.is_some() { "Some" } else { "None" },
            ),

            Request::PushTransaction(_, _) => "PushTransaction".to_string(),
            Request::PushBlock(block, _) => format!("PushBlock({})", block.hash()),
            Request::AdvertiseTransactionIds(ids, _) => {
                format!("AdvertiseTransactionIds({})", ids.len())
            }

            Request::AdvertiseBlock(_, _) => "AdvertiseBlock".to_string(),
            Request::AdvertiseBlockToAll(_) => "AdvertiseBlockToAll".to_string(),
            Request::AdvertiseMinedBlock(_) => "AdvertiseMinedBlock".to_string(),
            Request::AdvertiseEarlyBlock(_) => "AdvertiseEarlyBlock".to_string(),
            Request::MempoolTransactionIds => "MempoolTransactionIds".to_string(),
        })
    }
}

impl Request {
    /// Returns the Zebra internal request type as a string.
    pub fn command(&self) -> &'static str {
        match self {
            Request::DisconnectPeer(_) => "DisconnectPeer",
            Request::Peers => "Peers",
            Request::Ping(_) => "Ping",

            Request::BlocksByHash(_)
            | Request::BlocksByHashFrom { .. }
            | Request::BlockByHashPrimary { .. }
            | Request::BlockByHashHedge { .. } => "BlocksByHash",
            Request::EarlyBlocksByHash(_) => "EarlyBlocksByHash",
            Request::TransactionsById(_) | Request::TransactionsByIdFrom { .. } => {
                "TransactionsById"
            }

            Request::FindBlocks { .. } => "FindBlocks",
            Request::FindHeaders { .. } => "FindHeaders",

            Request::PushTransaction(_, _) => "PushTransaction",
            Request::PushBlock(_, _) => "PushBlock",
            Request::AdvertiseTransactionIds(_, _) => "AdvertiseTransactionIds",

            Request::AdvertiseBlock(_, _)
            | Request::AdvertiseBlockToAll(_)
            | Request::AdvertiseMinedBlock(_) => "AdvertiseBlock",
            Request::AdvertiseEarlyBlock(_) => "AdvertiseEarlyBlock",
            Request::MempoolTransactionIds => "MempoolTransactionIds",
        }
    }

    /// Returns true if the request is for block or transaction inventory downloads.
    pub fn is_inventory_download(&self) -> bool {
        matches!(
            self,
            Request::BlocksByHash(_)
                | Request::BlocksByHashFrom { .. }
                | Request::EarlyBlocksByHash(_)
                | Request::BlockByHashPrimary { .. }
                | Request::BlockByHashHedge { .. }
                | Request::TransactionsById(_)
                | Request::TransactionsByIdFrom { .. }
        )
    }

    /// Returns the block hash inventory downloads from the request, if any.
    pub fn block_hash_inventory(&self) -> HashSet<block::Hash> {
        match self {
            Request::BlocksByHash(block_hashes)
            | Request::EarlyBlocksByHash(block_hashes)
            | Request::BlocksByHashFrom {
                hashes: block_hashes,
                ..
            } => block_hashes.iter().copied().collect(),
            Request::BlockByHashPrimary { hash, .. } | Request::BlockByHashHedge { hash, .. } => {
                HashSet::from([*hash])
            }
            _ => HashSet::new(),
        }
    }

    /// Returns the transaction ID inventory downloads from the request, if any.
    pub fn transaction_id_inventory(&self) -> HashSet<UnminedTxId> {
        match self {
            Request::TransactionsById(transaction_ids)
            | Request::TransactionsByIdFrom {
                ids: transaction_ids,
                ..
            } => transaction_ids.clone(),
            _ => HashSet::new(),
        }
    }

    /// Ordinary single-block inventory sent to a peer once a route is selected.
    pub fn block_by_hash(hash: block::Hash, source: Option<PeerSource>) -> Self {
        let hashes = std::iter::once(hash).collect();
        match source {
            Some(source) => Self::BlocksByHashFrom { hashes, source },
            None => Self::BlocksByHash(hashes),
        }
    }

    /// Returns the source attached to a source-aware inventory request.
    pub fn inventory_source(&self) -> Option<PeerSource> {
        match self {
            Request::BlocksByHashFrom { source, .. }
            | Request::TransactionsByIdFrom { source, .. } => Some(source.clone()),
            Request::BlockByHashPrimary { source, .. } => source.clone(),
            _ => None,
        }
    }
}
