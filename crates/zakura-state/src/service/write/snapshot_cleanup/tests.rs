//! Ownership and lifecycle checks use gates rather than elapsed-time assertions.

use std::{
    sync::{mpsc, Arc, Mutex},
    thread::{self, ThreadId},
    time::{Duration, Instant},
};

use tokio::sync::watch;
use zakura_chain::{
    block::{self, Height},
    chain_tip::ChainTip,
    parameters::Network,
    serialization::ZcashDeserializeInto,
    transaction, transparent,
    value_balance::ValueBalance,
};

use super::SnapshotCleanup;
use crate::{
    arbitrary::Prepare,
    service::{
        non_finalized_state::{Chain, NonFinalizedState},
        ChainTipSender,
    },
    TransactionLocation,
};

mod benchmark;

const TIMEOUT: Duration = Duration::from_secs(10);

struct Probe {
    version: u8,
    dropped: mpsc::Sender<(u8, ThreadId)>,
    gate: Arc<Mutex<mpsc::Receiver<()>>>,
}

impl Drop for Probe {
    fn drop(&mut self) {
        let _ = self.dropped.send((self.version, thread::current().id()));
        if thread::current().name() == Some("state-cleanup") {
            // The timeout also releases the worker if the test panics before opening the gate.
            let _ = self.gate.lock().unwrap().recv_timeout(TIMEOUT);
        }
    }
}

#[test]
fn publication_continues_during_cleanup_and_busy_worker_falls_back() {
    let cleanup = SnapshotCleanup::new();
    let (dropped, events) = mpsc::channel();
    let (release, gate) = mpsc::channel();
    let gate = Arc::new(Mutex::new(gate));
    let probe = |version| Probe {
        version,
        dropped: dropped.clone(),
        gate: gate.clone(),
    };
    let (sender, receiver) = watch::channel(probe(1));
    let caller = thread::current().id();

    // Thread startup can race the first handoff. Retry until the real worker accepts one.
    let deadline = Instant::now() + TIMEOUT;
    loop {
        cleanup.publish(&sender, probe(2));
        let (_, disposer) = events.recv_timeout(TIMEOUT).unwrap();
        if disposer != caller {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "cleanup worker must become available"
        );
        thread::yield_now();
    }
    assert_eq!(receiver.borrow().version, 2);
    assert!(receiver.has_changed().unwrap());

    cleanup.publish(&sender, probe(3));
    assert_eq!(events.recv_timeout(TIMEOUT).unwrap(), (2, caller));
    assert_eq!(receiver.borrow().version, 3);

    // Joining while disposal is paused must finish only after we release it.
    let (joined, completion) = mpsc::channel();
    let joiner = thread::spawn(move || {
        drop(cleanup);
        joined.send(()).unwrap();
    });
    assert!(completion.try_recv().is_err());
    release.send(()).unwrap();
    completion.recv_timeout(TIMEOUT).unwrap();
    joiner.join().unwrap();
    assert!(events.try_recv().is_err(), "no retired snapshot backlog");
    drop(receiver);
    drop(sender);
    assert_eq!(events.recv_timeout(TIMEOUT).unwrap(), (3, caller));
    assert!(
        events.try_recv().is_err(),
        "each snapshot is disposed exactly once"
    );
}

#[test]
fn unavailable_and_disconnected_workers_dispose_inline() {
    let (tx, rx) = mpsc::sync_channel(0);
    drop(rx);
    for sender in [None, Some(tx)] {
        let cleanup = SnapshotCleanup {
            sender,
            worker: None,
        };
        let (dropped, events) = mpsc::channel();
        let (_release, gate) = mpsc::channel();
        let gate = Arc::new(Mutex::new(gate));
        cleanup.retire(Probe {
            version: 1,
            dropped,
            gate,
        });
        assert_eq!(
            events.recv_timeout(TIMEOUT).unwrap(),
            (1, thread::current().id())
        );
    }
}

#[test]
fn no_receivers_preserves_the_stored_value() {
    let cleanup = SnapshotCleanup::new();
    let (sender, receiver) = watch::channel(1);
    drop(receiver);
    cleanup.publish(&sender, 2);
    assert_eq!(*sender.borrow(), 1);
    let receiver = sender.subscribe();
    cleanup.publish(&sender, 3);
    assert_eq!(*receiver.borrow(), 3);
}

#[test]
fn receiver_closure_race_matches_send_contract() {
    let cleanup = SnapshotCleanup::new();
    for _ in 0..100 {
        let (sender, receiver) = watch::channel(1);
        thread::scope(|scope| {
            scope.spawn(move || drop(receiver));
            cleanup.publish(&sender, 2);
        });
        // `send` checks presence before swapping, so either result is permitted in this race.
        assert!([1, 2].contains(&*sender.borrow()));
    }
}

#[test]
fn early_return_and_unwind_join_the_worker() {
    let finished = Arc::new(std::sync::atomic::AtomicBool::new(false));
    for unwind in [false, true] {
        finished.store(false, std::sync::atomic::Ordering::SeqCst);
        let (sender, receiver) = mpsc::sync_channel::<()>(0);
        let flag = finished.clone();
        let worker = thread::spawn(move || {
            assert!(receiver.recv().is_err());
            flag.store(true, std::sync::atomic::Ordering::SeqCst);
        });
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _cleanup = SnapshotCleanup {
                sender: Some(sender),
                worker: Some(worker),
            };
            if unwind {
                panic!("simulated writer failure");
            }
        }));
        assert_eq!(result.is_err(), unwind);
        assert!(finished.load(std::sync::atomic::Ordering::SeqCst));
    }
}

#[test]
fn failed_worker_join_does_not_panic() {
    let cleanup = SnapshotCleanup::<()> {
        sender: None,
        worker: Some(thread::spawn(|| panic!("simulated cleanup failure"))),
    };
    drop(cleanup);
}

fn genesis() -> Arc<block::Block> {
    zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES
        .as_slice()
        .zcash_deserialize_into()
        .unwrap()
}

/// Populate actual state structures without requiring a chain database or valid fake history.
/// These fixtures exercise ownership and allocation, not consensus acceptance.
fn snapshot(forks: u8, entries: u32) -> NonFinalizedState {
    let network = Network::Mainnet;
    let mut state = NonFinalizedState::new(&network);
    let base = genesis().prepare().test_with_zero_chain_pool_change();
    for fork in 0..forks {
        let mut chain = Chain::new(
            &network,
            Height(0),
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
            ValueBalance::zero(),
        );
        for height in 0_u32..1_000 {
            let mut block = base.clone();
            block.height = Height(height);
            let mut hash = [fork; 32];
            hash[..4].copy_from_slice(&height.to_le_bytes());
            block.hash = block::Hash(hash);
            chain.height_by_hash.insert(block.hash, block.height);
            chain.blocks.insert(block.height, block);
        }
        for i in 0..entries {
            let mut hash = [fork; 32];
            hash[..4].copy_from_slice(&i.to_le_bytes());
            let hash = transaction::Hash(hash);
            let height = Height(i % 1_000);
            chain
                .tx_loc_by_hash
                .insert(hash, TransactionLocation::from_usize(height, 0));
            let output =
                transparent::Output::new(1.try_into().unwrap(), transparent::Script::new(&[0; 25]));
            chain.created_utxos.insert(
                transparent::OutPoint { hash, index: 0 },
                transparent::OrderedUtxo::new(output, height, 0),
            );
        }
        state.insert_test_chain(Arc::new(chain));
    }
    state
}

#[test]
fn reader_keeps_old_chain_after_publication_and_cleanup() {
    let cleanup = SnapshotCleanup::new();
    let old = snapshot(1, 10);
    let retained = old.best_chain().unwrap().clone();
    let weak = Arc::downgrade(&retained);
    let old_hash = retained.non_finalized_tip_hash();
    let (sender, receiver) = watch::channel(old);
    let replacement = NonFinalizedState::new(&Network::Mainnet);
    cleanup.publish(&sender, replacement);
    drop(cleanup);
    assert!(receiver.borrow().best_chain().is_none());
    assert_eq!(retained.non_finalized_tip_hash(), old_hash);
    assert_eq!(retained.created_utxos.len(), 10);
    drop(retained);
    assert!(weak.upgrade().is_none());
}

#[test]
fn tip_publication_and_operator_reset_keep_reader_state_in_sync() {
    let cleanup = SnapshotCleanup::new();
    let state = snapshot(1, 10);
    let (sender, receiver) = watch::channel(NonFinalizedState::new(&Network::Mainnet));
    let (mut tip_sender, latest, _) = ChainTipSender::new(None, &Network::Mainnet);
    let height = super::super::update_latest_chain_channels(
        &state,
        &mut tip_sender,
        &sender,
        None,
        &cleanup,
    );
    assert_eq!(latest.best_tip_height(), Some(height));
    assert_eq!(
        latest.best_tip_hash(),
        receiver.borrow().best_tip().map(|(_, hash)| hash)
    );

    let mut finalized = crate::service::finalized_state::FinalizedState::new(
        &crate::Config::ephemeral(),
        &Network::Mainnet,
    )
    .unwrap();
    let genesis = genesis();
    finalized
        .commit_finalized_direct(genesis.clone().into(), None, None, "snapshot cleanup test")
        .unwrap();
    let empty = NonFinalizedState::new(&Network::Mainnet);
    super::super::update_channels_after_operator_change(
        &empty,
        &finalized,
        &mut tip_sender,
        &sender,
        None,
        &cleanup,
    );
    assert!(receiver.borrow().best_chain().is_none());
    assert_eq!(latest.best_tip_hash(), Some(genesis.hash()));

    // Missing snapshot receivers must not suppress independent tip notifications.
    drop(receiver);
    super::super::update_latest_chain_channels(&state, &mut tip_sender, &sender, None, &cleanup);
    assert_eq!(latest.best_tip_height(), Some(height));
}
