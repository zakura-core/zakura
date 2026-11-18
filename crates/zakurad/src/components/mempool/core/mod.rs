//! The pool core, run by zakurad behind `[mempool] implementation = "core"`.
//!
//! The core itself is [`zakura_mempool::io::MempoolIO`], which knows nothing
//! of tokio, the peer set or the verifier. This module connects it to the
//! node:
//!
//! - [`verifier`]: the core's verifications, through the transaction
//!   verifier, and the verifier's own pool handle, which answers output
//!   lookups at once from the published pool.
//! - [`adapter`]: the `mempool::Request` service every other component
//!   already calls, served on top of the port, with the fetches it needs.
//! - [`follower`]: the tasks that follow the published pool: the announcer
//!   and the indexer's change stream.
//! - [`activation`]: whether the pool is on, decided as today's pool decides.
//!
//! The mempool handle other components take keeps its type, so nothing
//! outside `start.rs` knows which pool runs.

use std::{collections::HashSet, future::Future, net::IpAddr, pin::Pin, sync::Arc};

use tokio::{sync::watch, task::JoinHandle};
use tower::{buffer::Buffer, timeout::Timeout, util::BoxService};

use zakura_chain::{
    block::{self, Height},
    chain_tip::ChainTip,
};
use zakura_jsonl_trace::JsonlEventEmitter;
use zakura_mempool::io::{ChainFeed, MempoolIO};
use zakura_network::{self as zn, PeerSocketAddr};
use zakura_node_services::{
    mempool::{
        port::{self, pool_port, ChainEvent, ConnectedBlock},
        MempoolTxSubscriber, Request, Response,
    },
    task::{spawn_buffer, spawn_erased},
};
use zakura_state::{ChainTipChange, LatestChainTip, TipAction};

use crate::components::sync::SyncStatus;

use super::{
    downloads::{
        MAX_INBOUND_CONCURRENCY, TRANSACTION_DOWNLOAD_TIMEOUT, TRANSACTION_VERIFY_TIMEOUT,
    },
    gossip::MEMPOOL_CHANGE_CHANNEL_CAPACITY,
    trace::{MempoolTrace, TipProcessing},
    Config, Outbound, ReadState, TxVerifier,
};
use crate::BoxError;

mod activation;
mod adapter;
mod follower;
mod verifier;

/// The mempool service type every component takes.
pub(crate) type MempoolService = Buffer<
    Request,
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<Response, BoxError>> + Send>>,
>;

/// What [`start`] gives `start.rs`.
pub(crate) struct CoreParts {
    /// The `mempool::Request` service, for every component that takes the
    /// mempool.
    pub mempool: MempoolService,
    /// The transaction verifier's pool handle. It answers output lookups at
    /// once, from the published pool.
    pub verifier_pool: MempoolService,
    /// The indexer's change stream.
    pub subscriber: MempoolTxSubscriber,
    /// The core's task.
    pub core_task: JoinHandle<Result<(), BoxError>>,
    /// The tasks that decide whether the pool is on and follow what it
    /// publishes.
    pub follower_task: JoinHandle<Result<(), BoxError>>,
}

/// The peers the pool never charges: zcashd-compat sidecars and configured
/// unconditional peers, as today.
#[derive(Clone, Debug, Default)]
pub(crate) struct Exempt {
    /// The canonical IPs of zcashd-compat sidecars.
    pub zcashd_compat: HashSet<IpAddr>,
    /// Configured peers exempt from mempool bans, cooldowns and disconnects.
    pub unconditional: zn::UnconditionalPeers,
}

impl Exempt {
    /// Whether the pool must not charge `peer`.
    pub fn contains(&self, peer: PeerSocketAddr) -> bool {
        self.zcashd_compat.contains(&peer.ip().to_canonical())
            || self.unconditional.contains(peer.ip())
    }
}

/// The pool core's settings, from the node's `[mempool]` section, read as
/// today's pool reads them.
pub(crate) fn core_config(config: &Config) -> zakura_mempool::Config {
    zakura_mempool::Config {
        tx_cost_limit: config.tx_cost_limit,
        eviction_memory_time: config.eviction_memory_time,
        max_datacarrier_bytes: config
            .max_datacarrier_bytes
            .unwrap_or(super::config::DEFAULT_MAX_DATACARRIER_BYTES),
    }
}

/// Starts the pool core and everything that connects it to the node.
#[allow(clippy::too_many_arguments)]
pub(crate) fn start(
    config: &Config,
    expose_peer_addresses: bool,
    outbound: Outbound,
    read_state: ReadState,
    tx_verifier: TxVerifier,
    sync_status: SyncStatus,
    latest_chain_tip: LatestChainTip,
    chain_tip_change: ChainTipChange,
    misbehavior: zn::MisbehaviorReporter,
    exempt: Exempt,
    trace: JsonlEventEmitter,
) -> CoreParts {
    let best_tip: Arc<dyn Fn() -> Option<block::Hash> + Send + Sync> = {
        let latest_chain_tip = latest_chain_tip.clone();
        Arc::new(move || latest_chain_tip.best_tip_hash())
    };
    let activation = activation::Activation::new(
        config,
        sync_status,
        latest_chain_tip,
        chain_tip_change.clone(),
    );
    let (active_tx, active_rx) = tokio::sync::watch::channel(false);
    let Assembled {
        mempool,
        verifier_pool,
        subscriber,
        core,
        follower,
        ..
    } = assemble(
        config,
        expose_peer_addresses,
        outbound,
        read_state,
        chain_tip_change,
        tx_verifier,
        {
            let currentness = activation.currentness();
            Arc::new(move |height| currentness.may_cool(height))
        },
        best_tip,
        misbehavior,
        exempt,
        active_rx,
    );
    // The same `mempool.jsonl` row today's pool writes for each tip it
    // applies.
    let trace = MempoolTrace(trace);
    let core = core.on_tip_applied(move |applied| record_tip(&trace, applied));
    let core_task = spawn_erased(async move {
        core.run().await;
        Ok::<(), BoxError>(())
    });
    let follower_task = spawn_erased(async move {
        tokio::select! {
            result = follower.run() => result,
            result = activation.run(active_tx) => result,
        }
    });

    CoreParts {
        mempool,
        verifier_pool,
        subscriber,
        core_task,
        follower_task,
    }
}

/// The state's tip changes, as the core's chain feed.
struct TipFeed(ChainTipChange);

impl ChainFeed for TipFeed {
    fn next(&mut self) -> Pin<Box<dyn Future<Output = Option<ChainEvent>> + Send + '_>> {
        Box::pin(async move {
            // Waiting on a watch loses nothing when it is dropped.
            let action = self.0.wait_for_tip_change().await.ok()?;
            Some(match action {
                TipAction::Grow { block } => ChainEvent::Grow(ConnectedBlock {
                    hash: block.hash,
                    height: block.height,
                    transactions: block.transactions,
                    transaction_hashes: block.transaction_hashes,
                }),
                TipAction::Reset { height, hash } => {
                    ChainEvent::Reset(port::ChainTip { hash, height })
                }
            })
        })
    }
}

/// The pool core and its connections, built but not running.
pub(crate) struct Assembled {
    /// The `mempool::Request` service.
    pub mempool: MempoolService,
    /// The transaction verifier's pool handle.
    pub verifier_pool: MempoolService,
    /// The indexer's change stream.
    pub subscriber: MempoolTxSubscriber,
    /// The core, to run.
    pub core: MempoolIO<zakura_consensus::error::TransactionError, verifier::ConsensusVerifier>,
    /// The announcer and change stream, to run.
    pub follower: follower::Follower,
}

/// Builds the pool core and its connections. The core follows
/// `chain_tip_change`, whose first action names the tip. It is on while
/// `active` holds `true`, and the adapter starts a cooldown only when
/// `may_cool` allows it for the verdict's tip. [`start`] decides both as
/// today's pool does.
#[allow(clippy::too_many_arguments)]
pub(crate) fn assemble(
    config: &Config,
    expose_peer_addresses: bool,
    outbound: Outbound,
    read_state: ReadState,
    chain_tip_change: ChainTipChange,
    tx_verifier: TxVerifier,
    may_cool: Arc<dyn Fn(Height) -> bool + Send + Sync>,
    best_tip: Arc<dyn Fn() -> Option<block::Hash> + Send + Sync>,
    misbehavior: zn::MisbehaviorReporter,
    exempt: Exempt,
    active: watch::Receiver<bool>,
) -> Assembled {
    let (handle, inbox) = pool_port();

    let adapter_read_state = read_state.clone();
    let verifier = verifier::ConsensusVerifier::new(
        Timeout::new(tx_verifier, TRANSACTION_VERIFY_TIMEOUT),
        read_state,
        config.max_transaction_bytes,
        chain_tip_change.network().clone(),
    );
    let core = MempoolIO::new(
        &core_config(config),
        // A clone starts behind the tip, so its first action is a reset
        // that names it.
        Box::new(TipFeed(chain_tip_change.clone())),
        verifier,
        inbox,
        active,
    );

    let (changes, _) = tokio::sync::broadcast::channel(MEMPOOL_CHANGE_CHANNEL_CAPACITY);
    let subscriber = MempoolTxSubscriber::new(changes.clone());
    let follower = follower::Follower::new(
        handle.reader(),
        outbound.clone(),
        subscriber.clone(),
        changes,
        MEMPOOL_CHANGE_CHANNEL_CAPACITY,
    );

    let adapter = adapter::CoreMempool::new(
        handle.clone(),
        Timeout::new(outbound.clone(), TRANSACTION_DOWNLOAD_TIMEOUT),
        outbound,
        may_cool,
        best_tip,
        adapter_read_state,
        config.max_transaction_bytes,
        expose_peer_addresses,
        misbehavior,
        Arc::new(exempt),
    );
    let mempool = spawn_buffer(BoxService::new(adapter), MAX_INBOUND_CONCURRENCY);
    let verifier_pool = spawn_buffer(
        BoxService::new(verifier::VerifierPool::new(handle.reader())),
        MAX_INBOUND_CONCURRENCY,
    );

    Assembled {
        mempool,
        verifier_pool,
        subscriber,
        core,
        follower,
    }
}

/// Writes today's `mempool_tip_processed` row for a tip the core applied:
/// a reset or a grow, the tip, the pool after it, and what a reset
/// requeued. Today's pool counts as requeued its pool and its pending
/// downloads and verifications from before the tip, and writes zero for a
/// tip that grew by one block.
fn record_tip(trace: &MempoolTrace, applied: &zakura_mempool::io::TipApplied) {
    trace.tip_processed(
        if applied.reset {
            TipProcessing::Reset
        } else {
            TipProcessing::Grow
        },
        (applied.tip.hash, applied.tip.height),
        applied.pooled,
        if applied.reset {
            applied.held_before
        } else {
            0
        },
        applied.took,
    );
}

#[cfg(test)]
mod trace_tests {
    use zakura_jsonl_trace::{JsonlEventEmitter, JsonlTracer};
    use zakura_mempool::io::TipApplied;
    use zakura_node_services::mempool::port::ChainTip;

    use super::*;

    /// Core mode writes today's tip row: a reset counts what the pool held
    /// before it as requeued, and a grow writes zero.
    #[test]
    fn core_mode_writes_todays_tip_row() {
        let (rows_tx, mut rows) = tokio::sync::mpsc::channel(4);
        let trace = MempoolTrace(JsonlEventEmitter::new(JsonlTracer::new(rows_tx), "test"));
        for (byte, reset, held_before) in [(1, true, 3), (2, false, 3)] {
            record_tip(
                &trace,
                &TipApplied {
                    tip: ChainTip {
                        hash: block::Hash([byte; 32]),
                        height: Height(u32::from(byte)),
                    },
                    reset,
                    pooled: 2,
                    held_before,
                    took: std::time::Duration::from_millis(1),
                },
            );
        }
        let mut written = Vec::new();
        while let Ok(row) = rows.try_recv() {
            let row: serde_json::Value =
                serde_json::from_slice(&row.line).expect("each trace row is JSON");
            written.push(row);
        }
        assert_eq!(written.len(), 2, "one row for each tip: {written:?}");
        for (row, (byte, action, requeued)) in
            written.iter().zip([(1u8, "reset", 3), (2, "grow", 0)])
        {
            assert_eq!(row["event"], "mempool_tip_processed");
            assert_eq!(row["action"], action);
            assert_eq!(row["tip_hash"], block::Hash([byte; 32]).to_string());
            assert_eq!(row["tx_count_after"], 2);
            assert_eq!(row["requeued"], requeued, "{row}");
        }
    }
}
