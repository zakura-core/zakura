//! Memory-pool observer tap: logs [`Observation::Pool`] rows and feeds
//! [`PoolGaugeModel`] (and optionally block sync's [`OwnerModel`]) at each
//! simulation step boundary (`03 §4`).

use std::{
    cell::RefCell,
    mem,
    rc::Rc,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, PoisonError, Weak,
    },
};

use zakura_network::zakura::memory::{PoolEvent, PoolObserver};
use zakura_rt::sim::RunId;
use zakura_scheduler::SimInstant;

use super::block_node::view::BlockSyncView;
use crate::{
    domain::{
        block_sync::OwnerInput,
        memory_pool::{fact_from_event, PoolGaugeModel},
    },
    evidence::{Checkpoint, EvidenceId, EvidenceKind, LiveEvidence},
    ActionSource, Boundary, Identity, Observation, ObservationLog, Sim,
};

/// The run's reader of one node's memory-pool lease events.
pub struct PoolTap {
    state: Arc<TapState>,
    checkpoint: Checkpoint,
    id: EvidenceId,
    node: String,
    delivered: u64,
    /// Pool gauge folded at each step boundary.
    pool: Rc<RefCell<PoolGaugeModel>>,
    /// Optional block-sync view whose owner model receives the combined live
    /// `BlockSync + Consensus` lease total at each boundary (`mem-2`).
    block_view: Option<Rc<RefCell<BlockSyncView>>>,
    /// Optional atomic counter of `BlockSync + Consensus` receive-credit bytes
    /// held in `ConnectionCredit` QUIC drain queues (`02 §6`).
    draining_block_receive_bytes: Option<Arc<AtomicU64>>,
    /// Number of log entries already scanned for `Observation::Session` rows.
    scanned_sessions: usize,
}

struct TapState {
    run: RunId,
    observer: Weak<TapObserver>,
    refused: AtomicBool,
    buffer: Mutex<Buffer>,
}

#[derive(Default)]
struct Buffer {
    last: u64,
    events: Vec<(u64, PoolEvent)>,
}

impl TapState {
    fn buffer(&self) -> std::sync::MutexGuard<'_, Buffer> {
        self.buffer.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl LiveEvidence for TapState {
    fn last_seq(&self) -> u64 {
        self.buffer().last
    }

    fn refused_outside_run(&self) -> bool {
        self.refused.load(Ordering::Relaxed)
    }

    fn health(&self) -> Option<&'static str> {
        (self.observer.strong_count() == 0)
            .then_some("PoolGone: the tapped memory pool dropped its observer during the run")
    }
}

struct TapObserver(Arc<TapState>);

impl std::fmt::Debug for TapObserver {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("PoolTapObserver")
    }
}

impl PoolObserver for TapObserver {
    fn observe(&self, event: PoolEvent) {
        let state = &self.0;
        if zakura_rt::sim::run_id() != Some(state.run) {
            state.refused.store(true, Ordering::Relaxed);
            return;
        }
        let mut buffer = state.buffer();
        buffer.last = buffer
            .last
            .checked_add(1)
            .expect("a run records fewer than 2^64 pool events");
        let seq = buffer.last;
        buffer.events.push((seq, event));
    }
}

impl PoolTap {
    /// Create a pool tap for `node`, feeding `pool` (and optionally updating
    /// `block_view`'s `OwnerModel` with the live `BlockSync + Consensus` lease
    /// total after each step).
    pub fn new(
        sim: &mut Sim,
        node: &str,
        pool: Rc<RefCell<PoolGaugeModel>>,
        block_view: Option<Rc<RefCell<BlockSyncView>>>,
    ) -> (Arc<dyn PoolObserver>, PoolTap) {
        let run = zakura_rt::sim::run_id().expect("a Sim's scheduler enters a runtime run");
        let observer = Arc::new_cyclic(|observer| {
            TapObserver(Arc::new(TapState {
                run,
                observer: observer.clone(),
                refused: AtomicBool::new(false),
                buffer: Mutex::default(),
            }))
        });
        let state = observer.0.clone();
        let live: Weak<dyn LiveEvidence> = Arc::<TapState>::downgrade(&state);
        let checkpoint = Checkpoint::new(
            EvidenceKind::MemoryPool {
                node: node.to_owned(),
            },
            live,
        );
        let id = sim.register(checkpoint.clone());
        let tap = PoolTap {
            state,
            checkpoint,
            id,
            node: node.to_owned(),
            delivered: 0,
            pool,
            block_view,
            draining_block_receive_bytes: None,
            scanned_sessions: 0,
        };
        (observer, tap)
    }

    /// Attach the memory pool's preemption-free draining receive-credit counter
    /// so `BUDGET_LEDGERS_AGREE` accounts for unspent QUIC `MAX_DATA` leases
    /// parked in `ConnectionCredit` (`02 §6`).
    pub fn with_draining_block_receive_bytes(mut self, counter: Arc<AtomicU64>) -> Self {
        self.draining_block_receive_bytes = Some(counter);
        self
    }
}

impl ActionSource for PoolTap {
    fn enabled(&self, _: SimInstant, _: &mut Vec<(Identity, u64)>) {}

    fn boundaries(&self, _: SimInstant, _: &mut Vec<Boundary>) {}

    fn execute(&mut self, _: u64, _: &mut ObservationLog) {
        unreachable!("a pool tap offers no actions");
    }

    fn observe(&mut self, log: &mut ObservationLog) -> Result<(), String> {
        if log.run() != self.id.run {
            return Err("ForeignRun: memory-pool tap registered with another run".to_owned());
        }
        let events = mem::take(&mut self.state.buffer().events);
        let last = events.last().map_or(self.delivered, |(seq, _)| *seq);
        let mut sent = Vec::with_capacity(events.len());
        let mut facts = Vec::with_capacity(events.len());
        for (seq, event) in &events {
            let fact = fact_from_event(event);
            sent.push(Observation::Pool {
                instance: self.id.index,
                seq: *seq,
                node: self.node.clone(),
                event: fact.clone(),
            });
            facts.push(fact);
        }
        for obs in &sent {
            log.push(obs.clone());
        }
        self.delivered = last;
        self.checkpoint.record(log.step(), &sent, last);

        let block_leased = {
            let mut gauge = self.pool.borrow_mut();
            for fact in &facts {
                gauge.observe(fact);
            }
            let entries = log.entries();
            for entry in &entries[self.scanned_sessions..] {
                if let Observation::Session { connection, .. } = &entry.obs {
                    gauge.observe_session(*connection);
                }
            }
            self.scanned_sessions = entries.len();
            gauge.block_sync_total_leased()
        };

        if let Some(view) = &self.block_view {
            let draining = self
                .draining_block_receive_bytes
                .as_ref()
                .map_or(0, |counter| counter.load(Ordering::Acquire));
            view.borrow_mut().owners.apply(OwnerInput::PoolBlockLeases {
                leased: block_leased,
                draining,
            });
        }

        Ok(())
    }

    fn recovery(&self, _now: SimInstant, _out: &mut crate::RecoveryOffer) {}

    fn enter_drain(&mut self, _log: &mut ObservationLog) {}

    fn holds_work(&self) -> Option<String> {
        None
    }
}

impl Drop for PoolTap {
    fn drop(&mut self) {
        self.checkpoint.dropped();
    }
}
