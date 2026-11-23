//! The simulator's view of `tx-relay`: what each session's routine read,
//! did and holds after each step. Only the simulator's build has it, so the
//! release build does no extra work.
//!
//! A routine reports each step under the node's lock, so the steps of all
//! sessions reach the observer in the order they happened.

use std::sync::Arc;

use zakura_chain::transaction::UnminedTxId;
use zakura_node_services::mempool::port::PoolHandle;

pub use super::wire::{
    TxRelayMessage, HAVE_RATE, MAX_HAVE_ENTRIES, TX_RELAY_DATA_KIND, TX_RELAY_STATE_KIND,
    TX_RELAY_STREAMS, ZAKURA_CAP_TX_RELAY,
};
use super::{
    machine,
    service::{MayCool, TxRelayService},
    sessions::TxRelaySessions,
    tx_relay_service_id, ZakuraTxRelayConfig,
};
use crate::zakura::{memory::MemoryPool, CloseOutcome, CustomService, ZakuraPeerId};

/// The protocol's largest transaction, which our `Hello` names.
pub const MAX_TX_BYTES: u32 = super::wire::MAX_TX_BYTES;

/// The most requests a peer may have whose answers the IO has not started,
/// and the largest request window a session can have.
pub const MAX_SERVES: usize = machine::MAX_SERVES;

/// A new session's request window.
pub const INITIAL_WINDOW: usize = machine::INITIAL_WINDOW;

/// Ids one session may hold pending.
pub const PENDING_PER_PEER: usize = machine::PENDING_PER_PEER;

/// Ids one session remembers its peer announcing or supplying.
pub const KNOWN_CAP: usize = machine::KNOWN_CAP;

/// Hears every [`TxRelayEvent`], in order.
pub type TxRelayObserver = Arc<dyn Fn(TxRelayEvent) + Send + Sync>;

/// One thing a session's routine did or read.
#[derive(Clone, Debug)]
pub enum TxRelayEvent {
    /// A session opened. A parked one ended at once.
    Opened {
        /// The session.
        session: u64,
        /// Its peer.
        peer: ZakuraPeerId,
        /// Its peer's park let it in, so a stall closes its connection.
        readmitted: bool,
        /// Its peer's park still ran, so it ended before it began.
        parked: bool,
    },
    /// The routine decoded a message from the peer, before its step. A `Tx`
    /// is reported as it arrives, before its body decodes.
    Received {
        /// The session.
        session: u64,
        /// What arrived.
        message: Received,
    },
    /// The machine took one input.
    Stepped {
        /// The session.
        session: u64,
        /// What the step did, in order.
        effects: Vec<SimEffect>,
        /// What the session holds after it.
        state: SimState,
    },
    /// A `Have` went to the session's data writer.
    HaveSent {
        /// The session.
        session: u64,
        /// Its ids and sizes.
        entries: Vec<(UnminedTxId, u32)>,
    },
}

/// A peer message, as the routine decoded it.
#[derive(Clone, Debug)]
pub enum Received {
    /// `Hello`.
    Hello,
    /// `Have`, with its ids and sizes.
    Have(Vec<(UnminedTxId, u32)>),
    /// `GetTxs`.
    GetTxs(UnminedTxId),
    /// `Tx`, with the body's size.
    Tx {
        /// The id in front of the body.
        id: UnminedTxId,
        /// The body's size.
        size: u32,
    },
    /// `NotFound`.
    NotFound(UnminedTxId),
}

/// One effect of a step on its session, or a close of another.
#[derive(Clone, Debug)]
pub enum SimEffect {
    /// A `GetTxs` for the id.
    Ask(UnminedTxId),
    /// The body of the id, for the data writer.
    Serve(UnminedTxId),
    /// A body went to the pool.
    Submit {
        /// The id its reservation was granted for.
        requested: Option<UnminedTxId>,
        /// The body's own id.
        body: UnminedTxId,
        /// The body's size.
        size: u32,
        /// Whether its peer was cooling at the step's time.
        cooling: bool,
    },
    /// The session ended and its connection stays: a stall or a park.
    Park,
    /// A session's connection closes.
    Close {
        /// The session whose connection closes.
        session: u64,
        /// The recorded outcome.
        outcome: CloseOutcome,
    },
}

/// What one session holds after a step.
#[derive(Clone, Copy, Debug, Default)]
pub struct SimState {
    /// The requests the peer's `Hello` serves, once it arrived.
    pub hello_serves: Option<usize>,
    /// The session's request window.
    pub window: usize,
    /// Ids reserving or requested on the session.
    pub asking: usize,
    /// Ids reserving, requested or awaiting a verdict on the session.
    pub in_flight: usize,
    /// What its peer's ended sessions left: reserve calls and submissions.
    pub unsettled: usize,
    /// The peer's requests whose answers the IO has not started.
    pub serving: usize,
    /// Ids held for the peer in the wanted table.
    pub pending: usize,
    /// Ids held for the peer while it waits for the go or a `Hello`.
    pub waiting: usize,
    /// Ids the session remembers the peer announcing or supplying.
    pub known: usize,
    /// Whether the peer is cooling at the step's time.
    pub cooling: bool,
}

/// [`super::tx_relay_service`], reporting each session's steps to
/// `observer`.
pub fn tx_relay_service_observed(
    pool: PoolHandle,
    memory_pool: MemoryPool,
    may_cool: MayCool,
    config: &ZakuraTxRelayConfig,
    observer: TxRelayObserver,
) -> CustomService {
    let sessions = TxRelaySessions::default();
    let service: Arc<TxRelayService> = TxRelayService::spawn_observed(
        pool,
        memory_pool,
        may_cool,
        config,
        Default::default(),
        sessions.clone(),
        observer,
    );
    CustomService {
        service,
        provides: vec![tx_relay_service_id()],
        seeks: vec![tx_relay_service_id()],
        tx_relay_sessions: Some(sessions),
    }
}
