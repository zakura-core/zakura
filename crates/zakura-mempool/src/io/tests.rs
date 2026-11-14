//! Tests of the pool core task, through the port, on a real runtime.
//!
//! The verifier here reads the pool the way the real one does, through the
//! published snapshot. A spend of a test transaction counts as found only if
//! the snapshot offers its output. It reads the snapshot when the core calls
//! it, which is inside the step that starts the job. So a test that passes
//! shows the core published that step's batch before it started the job.

use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};

use futures::poll;
use proptest::test_runner::TestRunner;
use zakura_chain::{
    transaction::{self, UnminedTx, UnminedTxId, VerifiedUnminedTx},
    transparent::OutPoint,
};
use zakura_node_services::{
    block_memory::TransactionHandle,
    mempool::port::{
        chain_transitions, pool_port, AwaitOutputError, ChainReason, ChainTip, ChainTransition,
        ChainTransitionSender, ConnectedBlock, Intent, LocalFault, PoolHandle, PoolReader,
        RejectionScope, Unreserved, Verdict,
    },
};
use zakura_rt::{
    collections::{HashMap, HashSet},
    sync::{watch, Notify},
};

use super::{PoolCore, Verifier};
use crate::{
    config::Config,
    machine::{
        tests::{build, tip, TestVerifyError, Tx},
        Limits, Outcome,
    },
};

/// How long a test waits for the core to answer.
const ANSWER: Duration = Duration::from_secs(5);

/// A verifier that reads the pool through the published snapshot. It can be
/// told to hold a transaction forever or to panic on it.
#[derive(Clone)]
struct SnapshotVerifier {
    pool: PoolReader,
    txs: Arc<HashMap<UnminedTxId, VerifiedUnminedTx>>,
    hold: Arc<Mutex<HashSet<UnminedTxId>>>,
    panic_on: Arc<Mutex<HashSet<UnminedTxId>>>,
    /// Test transactions the chain has mined. A spend of one is found on the
    /// chain.
    mined: Arc<Mutex<HashSet<transaction::Hash>>>,
    /// A verification of one of these waits for its gate to open, once.
    gates: Arc<Mutex<HashMap<UnminedTxId, Arc<Notify>>>>,
    /// How many times each transaction was verified.
    calls: Arc<Mutex<HashMap<UnminedTxId, usize>>>,
    /// Run once while the verifier builds the future for a transaction,
    /// which is inside the core's step.
    hooks: Arc<Mutex<HashMap<UnminedTxId, Box<dyn FnOnce() + Send>>>>,
    /// Transactions whose future the verifier panics while building.
    panic_building: Arc<Mutex<HashSet<UnminedTxId>>>,
    /// How long every verification takes, if set.
    delay: Arc<Mutex<Option<Duration>>>,
    /// Verifications running now, and the most that ran at once.
    running: Arc<std::sync::atomic::AtomicUsize>,
    peak: Arc<std::sync::atomic::AtomicUsize>,
}

impl Verifier<TestVerifyError> for SnapshotVerifier {
    fn verify(
        &self,
        tx: UnminedTx,
        _tip: ChainTip,
        _revalidation: bool,
        _after_reset: bool,
    ) -> Pin<Box<dyn Future<Output = Outcome<TestVerifyError>> + Send>> {
        let id = tx.id();
        let hook = self
            .hooks
            .lock()
            .expect("no test panics holding it")
            .remove(&id);
        if let Some(hook) = hook {
            hook();
        }
        assert!(
            !self
                .panic_building
                .lock()
                .expect("no test panics holding it")
                .remove(&id),
            "the verifier panics while it builds this future"
        );
        *self
            .calls
            .lock()
            .expect("no test panics holding it")
            .entry(id)
            .or_default() += 1;
        let gate = self
            .gates
            .lock()
            .expect("no test panics holding it")
            .remove(&id);
        let mined = self.mined.clone();
        let hold = self
            .hold
            .lock()
            .expect("no test panics holding it")
            .contains(&id);
        let panics = self
            .panic_on
            .lock()
            .expect("no test panics holding it")
            .contains(&id);
        // The pool as the core published it when it started this job.
        let snapshot = self
            .pool
            .snapshot()
            .expect("the core publishes before it starts a job");
        let txs = self.txs.clone();
        let delay = *self.delay.lock().expect("no test panics holding it");
        let (running, peak) = (self.running.clone(), self.peak.clone());
        Box::pin(async move {
            if let Some(gate) = gate {
                gate.notified().await;
            }
            if let Some(delay) = delay {
                use std::sync::atomic::Ordering;
                let now = running.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(delay).await;
                running.fetch_sub(1, Ordering::SeqCst);
            }
            if hold {
                std::future::pending::<()>().await;
            }
            assert!(!panics, "the verifier panics on this transaction");
            // The chain is read only now, inside the generation readings.
            let mined = mined.lock().expect("no test panics holding it").clone();
            let known: HashSet<_> = txs.keys().map(UnminedTxId::mined_id).collect();
            let (from_pool, missing): (Vec<OutPoint>, Vec<OutPoint>) = tx
                .transaction()
                .spent_outpoints()
                .filter(|outpoint| {
                    known.contains(&outpoint.hash) && !mined.contains(&outpoint.hash)
                })
                .partition(|outpoint| snapshot.created_output(outpoint).is_some());
            if missing.is_empty() {
                Outcome::Pass {
                    tx: txs[&id].clone(),
                    pool_inputs: from_pool,
                }
            } else {
                Outcome::MissingInputs {
                    missing,
                    error: TestVerifyError,
                }
            }
        })
    }
}

/// A running core and what drives it.
struct Harness {
    handle: PoolHandle,
    chain: Arc<Mutex<ChainTransitionSender>>,
    core: tokio::task::JoinHandle<()>,
    active: watch::Sender<bool>,
    verifier: SnapshotVerifier,
    height: u32,
    /// What the core reported for each chain event it applied.
    tips: Mutex<std::sync::mpsc::Receiver<super::TipApplied>>,
}

impl Harness {
    /// Starts a core at height 100 that knows `txs`.
    fn start(txs: &[Tx]) -> Self {
        Self::start_at(txs, Some(tip(100, 0)))
    }

    /// Starts a core at height 100 that knows `txs` and holds at most five
    /// undecided transactions.
    fn start_small(txs: &[Tx]) -> Self {
        Self::start_with(
            txs,
            Some(tip(100, 0)),
            Limits {
                pending: 5,
                revalidating: 4,
            },
        )
    }

    /// Starts a core at `start_tip` that knows `txs`.
    fn start_at(txs: &[Tx], start_tip: Option<ChainTip>) -> Self {
        Self::start_with(txs, start_tip, Limits::default())
    }

    /// Starts a core at `start_tip` that knows `txs`, with `limits`.
    fn start_with(txs: &[Tx], start_tip: Option<ChainTip>, limits: Limits) -> Self {
        let chain = chain_transitions(start_tip);
        let transitions = chain.transitions();
        let (start, events) = transitions
            .subscribe()
            .expect("the test is the only consumer");
        let (handle, inbox) = pool_port();
        let (active, active_rx) = watch::channel(true);
        let verifier = SnapshotVerifier {
            pool: handle.reader(),
            txs: Arc::new(txs.iter().map(|tx| (tx.id(), tx.real.clone())).collect()),
            hold: Arc::default(),
            panic_on: Arc::default(),
            mined: Arc::default(),
            gates: Arc::default(),
            calls: Arc::default(),
            hooks: Arc::default(),
            panic_building: Arc::default(),
            delay: Arc::default(),
            running: Arc::default(),
            peak: Arc::default(),
        };
        let core = PoolCore::with_limits(
            &Config {
                tx_cost_limit: u64::MAX / 2,
                ..Config::default()
            },
            start,
            events,
            transitions.generation(),
            verifier.clone(),
            inbox,
            active_rx,
            limits,
        );
        let (tips_tx, tips) = std::sync::mpsc::channel();
        let core = core.on_tip_applied(move |applied| {
            let _ = tips_tx.send(applied.clone());
        });
        let core = tokio::spawn(core.run());
        Self {
            handle,
            chain: Arc::new(Mutex::new(chain)),
            core,
            active,
            verifier,
            height: 100,
            tips: Mutex::new(tips),
        }
    }

    /// Waits for the core's report of the next chain event it applies.
    async fn next_tip(&self) -> super::TipApplied {
        for _ in 0..1_000 {
            if let Ok(applied) = self
                .tips
                .lock()
                .expect("no test panics holding it")
                .try_recv()
            {
                return applied;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        panic!("the core applies the chain event");
    }

    /// Reserves and submits `tx` from this node, and returns the verdict.
    async fn submit(&self, tx: &Tx) -> Verdict {
        let reservation = self
            .handle
            .reserve(tx.id(), Intent::Deliver)
            .await
            .expect("the core grants a reservation");
        tokio::time::timeout(
            ANSWER,
            self.handle.submit(reservation, tx.real.transaction.clone()),
        )
        .await
        .expect("the core answers in time")
    }

    /// Mines `txs` in one new block on fork 0.
    fn mine(&mut self, txs: &[&Tx]) {
        self.change(0, 0, txs);
    }

    /// Disconnects `disconnect` blocks and connects one new block on `fork`
    /// that mines `txs`. The chain is marked as changing before anything else
    /// happens, as the state writer does.
    fn change(&mut self, disconnect: u32, fork: u8, txs: &[&Tx]) {
        self.change_under(disconnect, fork, txs, false);
    }

    /// [`Harness::change`], where the new block's height activates new
    /// rules if `rules_change`.
    fn change_under(&mut self, disconnect: u32, fork: u8, txs: &[&Tx], rules_change: bool) {
        let mut chain = self.chain.lock().expect("no test panics holding it");
        chain.begin_change();
        {
            let mut mined = self
                .verifier
                .mined
                .lock()
                .expect("no test panics holding it");
            if disconnect > 0 {
                // The tests do not track what each block mined.
                mined.clear();
            }
            mined.extend(txs.iter().map(|tx| tx.id().mined_id()));
        }
        let before = chain.tip().expect("the test chain has a tip");
        let height = self.height - disconnect + 1;
        self.height = height;
        let after = tip(height, fork);
        chain.finish_change(after, |_, generation| {
            Some(ChainTransition {
                before: Some(before),
                disconnected: (0..disconnect)
                    .map(|back| tip(before.height.0 - back, 0xff))
                    .collect(),
                connected: vec![ConnectedBlock {
                    hash: after.hash,
                    height: after.height,
                    transactions: txs
                        .iter()
                        .map(|tx| {
                            TransactionHandle::Shared(tx.real.transaction.transaction().clone())
                        })
                        .collect(),
                    transaction_hashes: txs.iter().map(|tx| tx.id().mined_id()).collect(),
                }],
                after,
                generation,
                rules_change,
            })
        });
    }

    /// Holds the next verification of `tx` until the returned gate opens.
    fn gate(&self, tx: &Tx) -> Arc<Notify> {
        let gate = Arc::new(Notify::new());
        self.verifier
            .gates
            .lock()
            .expect("no test panics holding it")
            .insert(tx.id(), gate.clone());
        gate
    }

    /// How many times `tx` has been verified.
    fn calls(&self, tx: &Tx) -> usize {
        self.verifier
            .calls
            .lock()
            .expect("no test panics holding it")
            .get(&tx.id())
            .copied()
            .unwrap_or(0)
    }

    /// Waits until `tx` has been verified `count` times.
    async fn calls_reach(&self, tx: &Tx, count: usize) {
        tokio::time::timeout(ANSWER, async {
            while self.calls(tx) < count {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("the verification starts in time");
    }

    /// Submits `tx` from this node in a task of its own, and returns the
    /// task's handle.
    async fn submit_in_background(&self, tx: &Tx) -> tokio::task::JoinHandle<Verdict> {
        let reservation = self
            .handle
            .reserve(tx.id(), Intent::Deliver)
            .await
            .expect("the core grants a reservation");
        let handle = self.handle.clone();
        let body = tx.real.transaction.clone();
        tokio::spawn(async move { handle.submit(reservation, body).await })
    }

    /// Waits until the published snapshot satisfies `done`.
    async fn snapshot_until(
        &self,
        done: impl Fn(&zakura_node_services::mempool::port::PoolSnapshot) -> bool,
    ) {
        tokio::time::timeout(ANSWER, async {
            loop {
                if self
                    .handle
                    .snapshot()
                    .is_some_and(|snapshot| done(&snapshot))
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("the snapshot reaches the state in time");
    }

    /// Makes one round trip through the core, so every command sent before
    /// this has been handled.
    async fn round_trip(&self) {
        self.handle.rejected_ids(HashSet::default()).await;
    }
}

/// Two transactions: a parent with two outputs and a child that spends the
/// first.
fn parent_and_child() -> (Tx, Tx) {
    let mut runner = TestRunner::deterministic();
    let parent = build(&mut runner, &[], 2, None);
    let child = build(
        &mut runner,
        &[OutPoint {
            hash: parent.id().mined_id(),
            index: 0,
        }],
        1,
        None,
    );
    (parent, child)
}

#[tokio::test]
async fn a_submission_is_admitted_and_published() {
    let (parent, child) = parent_and_child();
    let harness = Harness::start(&[parent.clone(), child]);

    assert!(matches!(harness.submit(&parent).await, Verdict::Admitted));

    let view = harness
        .handle
        .view(Default::default())
        .expect("the core has published");
    assert!(view.snapshot.transaction(&parent.id().mined_id()).is_some());
    assert!(matches!(
        view.changes,
        zakura_node_services::mempool::port::ChangesSince::Entries(entries)
            if entries.iter().any(|entry| entry.change == zakura_node_services::mempool::port::Change::Added(parent.id()))
    ));
}

#[tokio::test]
async fn a_child_woken_by_its_parent_reads_a_pool_that_holds_the_parent() {
    let (parent, child) = parent_and_child();
    let harness = Harness::start(&[parent.clone(), child.clone()]);

    // The child first: its parent is missing, so it parks.
    let reservation = harness
        .handle
        .reserve(child.id(), Intent::Deliver)
        .await
        .expect("the core grants a reservation");
    let handle = harness.handle.clone();
    let child_tx = child.real.transaction.clone();
    let child_verdict = tokio::spawn(async move { handle.submit(reservation, child_tx).await });

    // The parent's admission wakes the child in the same step. If the core
    // started the child's job before it published that step, the child's
    // verification would not see the parent, and would park again.
    assert!(matches!(harness.submit(&parent).await, Verdict::Admitted));
    let verdict = tokio::time::timeout(ANSWER, child_verdict)
        .await
        .expect("the child is decided in time")
        .expect("the child's task does not panic");
    assert!(matches!(verdict, Verdict::Admitted), "{verdict:?}");
}

#[tokio::test]
async fn an_output_wait_registered_first_is_answered_at_admission() {
    let (parent, _) = parent_and_child();
    let harness = Harness::start(std::slice::from_ref(&parent));
    let outpoint = OutPoint {
        hash: parent.id().mined_id(),
        index: 1,
    };

    let wait = harness.handle.await_output(outpoint);
    tokio::pin!(wait);
    assert!(poll!(&mut wait).is_pending(), "nothing creates it yet");
    harness.round_trip().await;

    assert!(matches!(harness.submit(&parent).await, Verdict::Admitted));
    let output = tokio::time::timeout(ANSWER, wait)
        .await
        .expect("the wait is answered in time")
        .expect("the admission answers it");
    assert_eq!(
        Some(&output),
        parent.real.transaction.transaction().outputs().get(1)
    );
}

#[tokio::test]
async fn an_output_wait_after_admission_is_answered_at_once() {
    let (parent, _) = parent_and_child();
    let harness = Harness::start(std::slice::from_ref(&parent));
    assert!(matches!(harness.submit(&parent).await, Verdict::Admitted));

    let output = harness
        .handle
        .await_output(OutPoint {
            hash: parent.id().mined_id(),
            index: 0,
        })
        .await
        .expect("the pool holds the output");
    assert_eq!(
        Some(&output),
        parent.real.transaction.transaction().outputs().first()
    );
}

#[tokio::test]
async fn a_block_that_mines_a_pooled_transaction_removes_it() {
    let (parent, child) = parent_and_child();
    let mut harness = Harness::start(&[parent.clone(), child.clone()]);
    assert!(matches!(harness.submit(&parent).await, Verdict::Admitted));
    assert!(matches!(harness.submit(&child).await, Verdict::Admitted));

    harness.mine(&[&parent]);
    let height = harness.height;
    harness
        .snapshot_until(|snapshot| snapshot.tip.height.0 == height)
        .await;
    let snapshot = harness.handle.snapshot().expect("published");
    assert!(snapshot.transaction(&parent.id().mined_id()).is_none());
    // Its child spends a chain output now, and stays.
    assert!(snapshot.transaction(&child.id().mined_id()).is_some());
}

/// Each chain event the core applies is reported for the node's trace: a
/// one-block growth with what stays visible, and a reorg as a reset with
/// what waits hidden to be verified again.
#[tokio::test]
async fn each_applied_chain_event_is_reported_for_the_trace() {
    let (parent, child) = parent_and_child();
    let mut harness = Harness::start(&[parent.clone(), child.clone()]);
    assert!(matches!(harness.submit(&parent).await, Verdict::Admitted));
    assert!(matches!(harness.submit(&child).await, Verdict::Admitted));

    harness.mine(&[&parent]);
    let grew = harness.next_tip().await;
    assert!(!grew.reset, "{grew:?}");
    assert_eq!(grew.tip.height.0, harness.height);
    assert_eq!((grew.visible, grew.hidden), (1, 0), "the child stays");

    harness.change(1, 1, &[]);
    let reorged = harness.next_tip().await;
    assert!(reorged.reset, "{reorged:?}");
    assert_eq!(
        (reorged.visible, reorged.hidden),
        (0, 1),
        "the child is checked again"
    );

    // While the pool is off, a chain event is remembered and not applied,
    // so it writes no row, as today's pool writes none while disabled.
    harness.active.send_replace(false);
    harness.round_trip().await;
    harness.mine(&[]);
    harness.round_trip().await;
    assert!(
        harness
            .tips
            .lock()
            .expect("no test panics holding it")
            .try_recv()
            .is_err(),
        "no row for a tip the pool did not apply"
    );
}

#[tokio::test]
async fn turning_the_pool_off_answers_every_undecided_submission() {
    let (parent, _) = parent_and_child();
    let harness = Harness::start(std::slice::from_ref(&parent));
    harness
        .verifier
        .hold
        .lock()
        .expect("no test panics holding it")
        .insert(parent.id());

    let reservation = harness
        .handle
        .reserve(parent.id(), Intent::Deliver)
        .await
        .expect("the core grants a reservation");
    let handle = harness.handle.clone();
    let tx = parent.real.transaction.clone();
    let verdict = tokio::spawn(async move { handle.submit(reservation, tx).await });
    harness.round_trip().await;

    harness.active.send_replace(false);
    let verdict = tokio::time::timeout(ANSWER, verdict)
        .await
        .expect("the submission is answered in time")
        .expect("the task does not panic");
    assert!(
        matches!(verdict, Verdict::LocalFault(LocalFault::Deactivated)),
        "{verdict:?}"
    );
    harness.snapshot_until(|snapshot| !snapshot.active).await;
    assert!(matches!(
        harness
            .handle
            .await_output(OutPoint {
                hash: parent.id().mined_id(),
                index: 0
            })
            .await,
        Err(AwaitOutputError::Inactive)
    ));
}

#[tokio::test]
async fn turning_the_pool_off_cancels_a_fetch_still_out() {
    let (parent, _) = parent_and_child();
    let harness = Harness::start(std::slice::from_ref(&parent));
    let fetch = harness
        .handle
        .reserve(parent.id(), Intent::Fetch)
        .await
        .expect("the node has room");
    let cancel = fetch.cancellation();
    assert!(!cancel.is_cancelled());

    harness.active.send_replace(false);
    tokio::time::timeout(ANSWER, cancel.cancelled())
        .await
        .expect("the fetch is told to stop when the pool turns off");
}

#[tokio::test]
async fn a_verifier_that_panics_is_a_local_fault() {
    let (parent, _) = parent_and_child();
    let harness = Harness::start(std::slice::from_ref(&parent));
    harness
        .verifier
        .panic_on
        .lock()
        .expect("no test panics holding it")
        .insert(parent.id());

    let verdict = harness.submit(&parent).await;
    assert!(
        matches!(verdict, Verdict::LocalFault(LocalFault::VerifyPanicked)),
        "{verdict:?}"
    );
}

#[tokio::test]
async fn a_dropped_reservation_frees_its_slot() {
    let mut runner = TestRunner::deterministic();
    let txs: Vec<Tx> = (0..6).map(|_| build(&mut runner, &[], 1, None)).collect();
    let harness = Harness::start_small(&txs);

    let mut held = Vec::new();
    for tx in &txs[..5] {
        held.push(
            harness
                .handle
                .reserve(tx.id(), Intent::Fetch)
                .await
                .expect("the node has room"),
        );
    }
    assert!(matches!(
        harness.handle.reserve(txs[5].id(), Intent::Fetch).await,
        Err(Unreserved::NoRoom)
    ));

    drop(held.pop());
    let _granted = harness
        .handle
        .reserve(txs[5].id(), Intent::Fetch)
        .await
        .expect("the dropped reservation's slot is free again");
}

/// Waits for a spawned submission's verdict.
async fn verdict_of(task: tokio::task::JoinHandle<Verdict>) -> Verdict {
    tokio::time::timeout(ANSWER, task)
        .await
        .expect("the submission is answered in time")
        .expect("the submission's task does not panic")
}

#[tokio::test]
async fn a_block_with_a_conflicting_transaction_removes_the_pooled_one() {
    let mut runner = TestRunner::deterministic();
    let chain_output = OutPoint {
        hash: transaction::Hash([0xc1; 32]),
        index: 0,
    };
    let pooled = build(&mut runner, &[chain_output], 1, None);
    let rival = build(&mut runner, &[chain_output], 1, None);
    let mut harness = Harness::start(&[pooled.clone(), rival.clone()]);
    assert!(matches!(harness.submit(&pooled).await, Verdict::Admitted));

    // The rival was never in the pool, and spends what the pooled one does.
    harness.mine(&[&rival]);
    let height = harness.height;
    harness
        .snapshot_until(|snapshot| snapshot.tip.height.0 == height)
        .await;
    assert!(harness
        .handle
        .snapshot()
        .expect("published")
        .transaction(&pooled.id().mined_id())
        .is_none());
    assert!(matches!(
        harness
            .handle
            .reserve(pooled.id(), Intent::Deliver)
            .await,
        Err(Unreserved::Rejected(rejection))
            if rejection.scope == RejectionScope::EffectsChain(ChainReason::DuplicateSpend)
    ));
}

#[tokio::test]
async fn a_verification_that_spans_a_block_runs_again() {
    let (parent, _) = parent_and_child();
    let mut harness = Harness::start(std::slice::from_ref(&parent));
    let gate = harness.gate(&parent);

    let verdict = harness.submit_in_background(&parent).await;
    harness.calls_reach(&parent, 1).await;
    // The block lands while the first verification is still looking.
    harness.mine(&[]);
    gate.notify_one();

    assert!(matches!(verdict_of(verdict).await, Verdict::Admitted));
    assert_eq!(
        harness.calls(&parent),
        2,
        "the first outcome straddled a change, so the core checked again"
    );
}

#[tokio::test]
async fn a_chain_that_reorgs_and_comes_back_during_a_verification_runs_it_again() {
    let (parent, _) = parent_and_child();
    let mut harness = Harness::start(std::slice::from_ref(&parent));
    let gate = harness.gate(&parent);

    let verdict = harness.submit_in_background(&parent).await;
    harness.calls_reach(&parent, 1).await;
    // A to B and back to A. The tip ends where it started, but the
    // generation does not, so the outcome is not trusted.
    harness.change(1, 1, &[]);
    harness.change(1, 0, &[]);
    gate.notify_one();

    assert!(matches!(verdict_of(verdict).await, Verdict::Admitted));
    assert!(harness.calls(&parent) >= 2);
}

#[tokio::test]
async fn a_child_submitted_before_its_parent_is_mined_is_decided_after_the_block() {
    let (parent, child) = parent_and_child();
    let mut harness = Harness::start(&[parent.clone(), child.clone()]);

    let verdict = harness.submit_in_background(&child).await;
    harness.calls_reach(&child, 1).await;
    harness.round_trip().await;

    // The parent is never pooled. A block mines it, and the child spends
    // its output from the chain.
    harness.mine(&[&parent]);
    assert!(matches!(verdict_of(verdict).await, Verdict::Admitted));
}

#[tokio::test(start_paused = true)]
async fn a_revalidation_that_panics_is_tried_again_later() {
    let (parent, _) = parent_and_child();
    let mut harness = Harness::start(std::slice::from_ref(&parent));
    assert!(matches!(harness.submit(&parent).await, Verdict::Admitted));
    harness
        .verifier
        .panic_on
        .lock()
        .expect("no test panics holding it")
        .insert(parent.id());

    // A reorg hides the pool, and its revalidation panics.
    harness.change(1, 1, &[]);
    harness.calls_reach(&parent, 2).await;
    harness
        .snapshot_until(|snapshot| snapshot.transaction(&parent.id().mined_id()).is_none())
        .await;

    harness
        .verifier
        .panic_on
        .lock()
        .expect("no test panics holding it")
        .clear();
    // The retry comes after its backoff, and then it passes.
    harness
        .snapshot_until(|snapshot| snapshot.transaction(&parent.id().mined_id()).is_some())
        .await;
    assert!(harness.calls(&parent) >= 3);
}

#[tokio::test]
async fn a_submission_cancelled_after_it_is_queued_frees_its_slot() {
    let mut runner = TestRunner::deterministic();
    let txs: Vec<Tx> = (0..6).map(|_| build(&mut runner, &[], 1, None)).collect();
    let harness = Harness::start_small(&txs);

    let mut held = Vec::new();
    for tx in &txs[..5] {
        held.push(
            harness
                .handle
                .reserve(tx.id(), Intent::Fetch)
                .await
                .expect("the node has room"),
        );
    }
    // The submission is queued for the core, then its caller gives up
    // before the core takes it.
    let reservation = held.pop().expect("five were reserved");
    let body = txs[4].real.transaction.clone();
    let mut submit = Box::pin(harness.handle.submit(reservation, body));
    assert!(poll!(&mut submit).is_pending(), "the command is queued");
    drop(submit);

    let _granted = harness
        .handle
        .reserve(txs[5].id(), Intent::Fetch)
        .await
        .expect("the cancelled submission's slot is free again");
}

#[tokio::test]
async fn a_verifier_that_panics_while_building_its_future_is_a_local_fault() {
    let (parent, child) = parent_and_child();
    let harness = Harness::start(&[parent.clone(), child.clone()]);
    harness
        .verifier
        .panic_building
        .lock()
        .expect("no test panics holding it")
        .insert(parent.id());

    let verdict = harness.submit(&parent).await;
    assert!(
        matches!(verdict, Verdict::LocalFault(LocalFault::VerifyPanicked)),
        "{verdict:?}"
    );
    // The core is still serving.
    assert!(matches!(harness.submit(&parent).await, Verdict::Admitted));
}

#[tokio::test]
async fn dropping_every_outside_handle_stops_the_core() {
    let (parent, _) = parent_and_child();
    let harness = Harness::start(std::slice::from_ref(&parent));
    assert!(matches!(harness.submit(&parent).await, Verdict::Admitted));

    // The verifier the core owns reads the pool through a reader, which does
    // not keep the core running.
    let Harness { handle, core, .. } = harness;
    drop(handle);
    tokio::time::timeout(ANSWER, core)
        .await
        .expect("the core stops once every handle is gone")
        .expect("the core does not panic");
}

#[tokio::test]
async fn a_pool_with_no_tip_stays_off_until_the_chain_names_one() {
    let (parent, _) = parent_and_child();
    let mut harness = Harness::start_at(std::slice::from_ref(&parent), None);
    harness.snapshot_until(|snapshot| !snapshot.active).await;
    assert!(matches!(
        harness.handle.reserve(parent.id(), Intent::Deliver).await,
        Err(Unreserved::Inactive)
    ));

    // The first block names a tip, and the pool turns on at it.
    harness.height = 0;
    {
        let mut chain = harness.chain.lock().expect("no test panics holding it");
        chain.begin_change();
        let after = tip(1, 0);
        chain.finish_change(after, |_, _| None);
    }
    harness
        .snapshot_until(|snapshot| snapshot.active && snapshot.tip.height.0 == 1)
        .await;
    assert!(matches!(harness.submit(&parent).await, Verdict::Admitted));
}

#[tokio::test]
async fn a_chain_change_before_the_worker_first_looks_runs_the_job_again() {
    let (parent, _) = parent_and_child();
    let harness = Harness::start(std::slice::from_ref(&parent));
    // While the core starts the job, inside its step, a block lands. The job
    // was bound to the tip before it, so its result cannot be trusted at the
    // new tip, though both of the worker's readings agree with each other.
    let chain = harness.chain.clone();
    harness
        .verifier
        .hooks
        .lock()
        .expect("no test panics holding it")
        .insert(
            parent.id(),
            Box::new(move || {
                let mut chain = chain.lock().expect("no test panics holding it");
                let before = chain.tip().expect("the test chain has a tip");
                let after = tip(before.height.0 + 1, 0);
                chain.begin_change();
                chain.finish_change(after, |_, generation| {
                    Some(ChainTransition {
                        before: Some(before),
                        disconnected: Vec::new(),
                        connected: vec![ConnectedBlock {
                            hash: after.hash,
                            height: after.height,
                            transactions: Vec::new(),
                            transaction_hashes: Arc::new([]),
                        }],
                        after,
                        generation,
                        rules_change: false,
                    })
                });
            }),
        );

    assert!(matches!(harness.submit(&parent).await, Verdict::Admitted));
    assert_eq!(
        harness.calls(&parent),
        2,
        "the job bound to the old tip ran again at the new one"
    );
}

/// The reorg half of the recovery gate. After a reorg the core gets every
/// pooled transaction back as it does after an upgrade: both hide the whole
/// pool, both get it all back in about the same time, and neither runs more
/// verifications at once than the revalidation share. Today's pool resets
/// the same way after both, so the boundary test that compares the two pools
/// after an upgrade covers a reorg as well.
#[tokio::test(flavor = "multi_thread")]
async fn a_reorg_recovers_the_pool_as_an_upgrade_does() {
    use std::sync::atomic::Ordering;
    use zakura_node_services::mempool::port::MAX_REVALIDATING;

    const POOLED: usize = 1_500;
    const DELAY: Duration = Duration::from_millis(40);
    let mut runner = TestRunner::deterministic();
    let txs: Vec<Tx> = (0..POOLED)
        .map(|_| build(&mut runner, &[], 1, None))
        .collect();

    let mut recoveries = Vec::new();
    for reorg in [false, true] {
        let mut harness = Harness::start(&txs);
        for tx in &txs {
            assert!(matches!(harness.submit(tx).await, Verdict::Admitted));
        }
        *harness
            .verifier
            .delay
            .lock()
            .expect("no test panics holding it") = Some(DELAY);
        let started = tokio::time::Instant::now();
        if reorg {
            harness.change(1, 1, &[]);
        } else {
            harness.change_under(0, 0, &[], true);
        }
        let applied = harness.next_tip().await;
        assert!(
            applied.reset,
            "the change hides the pool to verify it again"
        );
        assert_eq!(applied.hidden, POOLED, "every pooled transaction is hidden");
        tokio::time::timeout(Duration::from_secs(60), async {
            while harness
                .handle
                .snapshot()
                .is_none_or(|snapshot| snapshot.transactions.len() < POOLED)
            {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("the core gets every transaction back");
        recoveries.push((
            started.elapsed(),
            harness.verifier.peak.load(Ordering::SeqCst),
        ));
    }
    let [(upgrade, upgrade_peak), (reorg, reorg_peak)] = recoveries[..] else {
        unreachable!("two runs");
    };
    for peak in [upgrade_peak, reorg_peak] {
        assert!(peak <= MAX_REVALIDATING, "within the share: {recoveries:?}");
    }
    // Allow for scheduling noise: one verification's delay either way.
    assert!(
        reorg <= upgrade + DELAY && upgrade <= reorg + DELAY,
        "a reorg recovers as an upgrade does: {recoveries:?}"
    );
}
