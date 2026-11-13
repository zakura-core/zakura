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
    transaction::{UnminedTx, UnminedTxId, VerifiedUnminedTx},
    transparent::OutPoint,
};
use zakura_node_services::{
    block_memory::TransactionHandle,
    mempool::port::{
        chain_transitions, pool_port, AwaitOutputError, ChainTip, ChainTransition,
        ChainTransitionSender, ConnectedBlock, Intent, LocalFault, NoRoom, PoolHandle, Source,
        Supplier, Unreserved, Verdict,
    },
    mempool::QueueSource,
};
use zakura_rt::{
    collections::{HashMap, HashSet},
    sync::watch,
};

use super::{PoolCore, Verifier};
use crate::{
    config::Config,
    machine::{
        tests::{build, tip, TestVerifyError, Tx},
        Outcome,
    },
};

/// How long a test waits for the core to answer.
const ANSWER: Duration = Duration::from_secs(5);

/// A verifier that reads the pool through the published snapshot. It can be
/// told to hold a transaction forever or to panic on it.
#[derive(Clone)]
struct SnapshotVerifier {
    handle: PoolHandle,
    txs: Arc<HashMap<UnminedTxId, VerifiedUnminedTx>>,
    hold: Arc<Mutex<HashSet<UnminedTxId>>>,
    panic_on: Arc<Mutex<HashSet<UnminedTxId>>>,
}

impl Verifier<TestVerifyError> for SnapshotVerifier {
    fn verify(
        &self,
        tx: UnminedTx,
        _tip: ChainTip,
        _revalidation: bool,
    ) -> Pin<Box<dyn Future<Output = Outcome<TestVerifyError>> + Send>> {
        let id = tx.id();
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
            .handle
            .snapshot()
            .expect("the core publishes before it starts a job");
        let known: HashSet<_> = self.txs.keys().map(UnminedTxId::mined_id).collect();
        let (from_pool, missing): (Vec<OutPoint>, Vec<OutPoint>) = tx
            .transaction()
            .spent_outpoints()
            .filter(|outpoint| known.contains(&outpoint.hash))
            .partition(|outpoint| snapshot.created_output(outpoint).is_some());
        let outcome = if missing.is_empty() {
            Outcome::Pass {
                tx: self.txs[&id].clone(),
                pool_inputs: from_pool,
            }
        } else {
            Outcome::MissingInputs {
                missing,
                error: TestVerifyError,
            }
        };
        Box::pin(async move {
            if hold {
                std::future::pending::<()>().await;
            }
            assert!(!panics, "the verifier panics on this transaction");
            outcome
        })
    }
}

/// A running core and what drives it.
struct Harness {
    handle: PoolHandle,
    chain: ChainTransitionSender,
    active: watch::Sender<bool>,
    verifier: SnapshotVerifier,
    height: u32,
}

impl Harness {
    /// Starts a core at height 100 that knows `txs`.
    fn start(txs: &[Tx]) -> Self {
        let chain = chain_transitions(Some(tip(100, 0)));
        let transitions = chain.transitions();
        let (start, events) = transitions
            .subscribe()
            .expect("the test is the only consumer");
        let (handle, inbox) = pool_port();
        let (active, active_rx) = watch::channel(true);
        let verifier = SnapshotVerifier {
            handle: handle.clone(),
            txs: Arc::new(txs.iter().map(|tx| (tx.id(), tx.real.clone())).collect()),
            hold: Arc::default(),
            panic_on: Arc::default(),
        };
        let core = PoolCore::new(
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
        );
        tokio::spawn(core.run());
        Self {
            handle,
            chain,
            active,
            verifier,
            height: 100,
        }
    }

    /// Reserves and submits `tx` from this node, and returns the verdict.
    async fn submit(&self, tx: &Tx) -> Verdict {
        let reservation = self
            .handle
            .reserve(tx.id(), Source::Node, Intent::Deliver)
            .await
            .expect("the core grants a reservation");
        tokio::time::timeout(
            ANSWER,
            self.handle
                .submit(reservation, tx.real.transaction.clone(), Supplier::Nobody),
        )
        .await
        .expect("the core answers in time")
    }

    /// Mines `txs` in one new block.
    fn mine(&mut self, txs: &[&Tx]) {
        self.height += 1;
        let after = tip(self.height, 0);
        let before = tip(self.height - 1, 0);
        self.chain.begin_change();
        self.chain.finish_change(after, |_, generation| {
            Some(ChainTransition {
                before: Some(before),
                disconnected: Vec::new(),
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
                rules_change: false,
            })
        });
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
        .reserve(child.id(), Source::Node, Intent::Deliver)
        .await
        .expect("the core grants a reservation");
    let handle = harness.handle.clone();
    let child_tx = child.real.transaction.clone();
    let child_verdict =
        tokio::spawn(async move { handle.submit(reservation, child_tx, Supplier::Nobody).await });

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
        .reserve(parent.id(), Source::Node, Intent::Deliver)
        .await
        .expect("the core grants a reservation");
    let handle = harness.handle.clone();
    let tx = parent.real.transaction.clone();
    let verdict =
        tokio::spawn(async move { handle.submit(reservation, tx, Supplier::Nobody).await });
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
    let harness = Harness::start(&txs);
    let peer = Source::Peer(QueueSource::LegacySocket(([10, 0, 0, 1], 8233).into()));

    let mut held = Vec::new();
    for tx in &txs[..5] {
        held.push(
            harness
                .handle
                .reserve(tx.id(), peer.clone(), Intent::Fetch)
                .await
                .expect("the peer has room"),
        );
    }
    assert!(matches!(
        harness
            .handle
            .reserve(txs[5].id(), peer.clone(), Intent::Fetch)
            .await,
        Err(Unreserved::NoRoom(NoRoom::Source))
    ));

    drop(held.pop());
    let _granted = harness
        .handle
        .reserve(txs[5].id(), peer, Intent::Fetch)
        .await
        .expect("the dropped reservation's slot is free again");
}
