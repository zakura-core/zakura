//! The block writer's ordered best-chain changes, through the state service.

use std::{sync::Arc, time::Duration};

use tokio::time::timeout;
use tower::{buffer::Buffer, util::BoxService, ServiceExt};
use zakura_chain::{
    block::{self, Block},
    parameters::Network,
    serialization::ZcashDeserializeInto,
};
use zakura_node_services::mempool::port::{
    ChainEvent, ChainEvents, ChainTip, ChainTransitions, Generation, GenerationRead,
};

use crate::{
    arbitrary::Prepare,
    service::{ReadStateService, StateService},
    tests::{setup::transaction_v4_from_coinbase, FakeChainHelper},
    CheckpointVerifiedBlock, Config, Request,
};

const LIMIT: Duration = Duration::from_secs(10);

/// A fresh Mainnet state, its read service, and the transitions handle.
async fn fresh_state() -> (StateService, ReadStateService, ChainTransitions) {
    let crate::InitializedState {
        state, read_state, ..
    } = StateService::new(
        Config::ephemeral(),
        &Network::Mainnet,
        block::Height::MAX,
        0,
    )
    .await
    .expect("the ephemeral state opens");
    let transitions = read_state
        .chain_transitions()
        .expect("a state with a writer has transitions")
        .clone();
    (state, read_state, transitions)
}

/// Commit Mainnet blocks 0 and 1 through the checkpoint path, and return the
/// template for a fake block 2 whose only transaction is a v4 coinbase.
async fn finalize_genesis_and_block_1(state: &mut StateService) -> Arc<Block> {
    for (_, bytes) in zakura_test::vectors::MAINNET_BLOCKS.range(0..=1) {
        let block: Arc<Block> = bytes.zcash_deserialize_into().expect("the vector decodes");
        state
            .queue_and_commit_to_finalized_state(CheckpointVerifiedBlock::from(block))
            .await
            .expect("the writer answers")
            .expect("the checkpoint block commits");
    }
    let mut template: Block = zakura_test::vectors::BLOCK_MAINNET_2_BYTES
        .zcash_deserialize_into()
        .expect("the vector decodes");
    template.transactions = vec![Arc::new(transaction_v4_from_coinbase(
        &template.transactions[0],
    ))];
    Arc::new(template)
}

/// Commit `block` through the non-finalized path.
async fn commit(state: &mut StateService, block: &Arc<Block>) {
    timeout(
        LIMIT,
        state.queue_and_commit_to_non_finalized_state(block.clone().prepare(), None),
    )
    .await
    .expect("the commit finishes")
    .expect("the writer answers")
    .expect("the block commits");
}

/// The tip a block is at `height`.
fn tip_of(block: &Block, height: u32) -> ChainTip {
    ChainTip {
        hash: block.hash(),
        height: block::Height(height),
    }
}

/// Replays transitions onto a chain of tips, checking each one starts where
/// the last ended and moves the generation on by one.
struct Replay {
    chain: Vec<ChainTip>,
    generation: Generation,
}

impl Replay {
    /// Apply every queued event. Panics on a resync, which none of these
    /// tests should cause.
    fn drain(&mut self, events: &mut ChainEvents) -> usize {
        let mut applied = 0;
        while let Some(event) = events.try_next() {
            let ChainEvent::Transition(transition) = event else {
                panic!("unexpected resync: {event:?}");
            };
            assert_eq!(transition.before, self.chain.last().copied());
            assert_ne!(
                transition.before,
                Some(transition.after),
                "a transition moves the tip"
            );
            for gone in &transition.disconnected {
                assert_eq!(self.chain.pop().as_ref(), Some(gone));
            }
            for block in &transition.connected {
                let next = self.chain.last().map_or(0, |tip| tip.height.0 + 1);
                assert_eq!(block.height, block::Height(next));
                assert_eq!(block.transactions.len(), block.transaction_hashes.len());
                for (transaction, hash) in block
                    .transactions
                    .iter()
                    .zip(block.transaction_hashes.iter())
                {
                    assert_eq!(transaction.hash(), *hash);
                }
                self.chain.push(ChainTip {
                    hash: block.hash,
                    height: block.height,
                });
            }
            assert_eq!(self.chain.last(), Some(&transition.after));
            assert_eq!(transition.generation, Generation(self.generation.0 + 1));
            self.generation = transition.generation;
            applied += 1;
        }
        applied
    }

    /// Check the replayed chain is the state's best chain.
    fn assert_matches(&self, read: &ReadStateService) {
        let (height, hash) = read.best_tip().expect("the state has a tip");
        assert_eq!(self.chain.last(), Some(&ChainTip { hash, height }));
        let published = read.latest_non_finalized_state();
        for tip in &self.chain {
            let found = published
                .best_chain()
                .and_then(|chain| chain.hash_by_height(tip.height))
                .or_else(|| read.db.hash(tip.height));
            assert_eq!(
                found,
                Some(tip.hash),
                "best chain differs at {:?}",
                tip.height
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn replaying_the_transitions_rebuilds_the_best_chain_through_reorgs() {
    let _init_guard = zakura_test::init();
    let (mut state, read, transitions) = fresh_state().await;
    let (start, mut events) = transitions.subscribe().expect("no consumer yet");
    assert_eq!(start.tip, None);
    let mut replay = Replay {
        chain: Vec::new(),
        generation: start.generation,
    };

    // Two checkpoint commits, each a change.
    let template = finalize_genesis_and_block_1(&mut state).await;
    assert_eq!(replay.drain(&mut events), 2);
    replay.assert_matches(&read);

    // Two forks off block 1. Each switch of the best tip is one transition.
    let siblings = template.make_fake_siblings(2);
    let (a2, b2) = (&siblings[0], &siblings[1]);
    let b3 = b2.make_fake_child();
    let a3 = a2.make_fake_child();
    let a4 = a3.make_fake_child();
    let mut tips_seen = 0;
    for block in [a2, b2, &b3, &a3, &a4] {
        let before = read.best_tip();
        commit(&mut state, block).await;
        if read.best_tip() != before {
            tips_seen += 1;
        }
    }
    assert_eq!(replay.drain(&mut events), tips_seen);
    replay.assert_matches(&read);
    assert_eq!(replay.chain.last(), Some(&tip_of(&a4, 4)));
    assert_eq!(
        transitions.generation().read(),
        GenerationRead::Stable(replay.generation)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn operator_commands_change_the_generation_and_disconnect_in_order() {
    let _init_guard = zakura_test::init();
    let (mut state, read, transitions) = fresh_state().await;
    let template = finalize_genesis_and_block_1(&mut state).await;
    let block_1 = read.best_tip().expect("block 1 is the tip");
    let a2 = template.make_fake_siblings(1).remove(0);
    let a3 = a2.make_fake_child();
    commit(&mut state, &a2).await;
    commit(&mut state, &a3).await;

    let (start, mut events) = transitions.subscribe().expect("no consumer yet");
    assert_eq!(start.tip, Some(tip_of(&a3, 3)));
    let mut replay = Replay {
        chain: vec![
            ChainTip {
                hash: block_1.1,
                height: block_1.0,
            },
            tip_of(&a2, 2),
            tip_of(&a3, 3),
        ],
        generation: start.generation,
    };
    let state = Buffer::new(BoxService::new(state), 10);
    let call = |request| {
        let state = state.clone();
        async move {
            timeout(LIMIT, state.oneshot(request))
                .await
                .expect("the request finishes")
                .expect("the request succeeds");
        }
    };

    // Invalidating the tip right after subscribing is a disconnect only.
    call(Request::InvalidateBlock(a3.hash())).await;
    let Some(ChainEvent::Transition(transition)) = events.try_next() else {
        panic!("invalidating the tip changes the best chain");
    };
    assert_eq!(transition.before, start.tip);
    assert_eq!(transition.disconnected, vec![tip_of(&a3, 3)]);
    assert!(transition.connected.is_empty());
    assert_eq!(transition.after, tip_of(&a2, 2));
    replay.chain.pop();
    replay.generation = transition.generation;
    assert_eq!(transition.generation, Generation(start.generation.0 + 1));

    // Reconsidering it connects it again.
    call(Request::ReconsiderBlock(a3.hash())).await;
    assert_eq!(replay.drain(&mut events), 1);
    replay.assert_matches(&read);

    // Invalidating the lowest non-finalized block empties the non-finalized
    // state, so the finalized tip is the best tip again.
    call(Request::InvalidateBlock(a2.hash())).await;
    assert_eq!(replay.drain(&mut events), 1);
    assert_eq!(replay.chain.len(), 1);
    replay.assert_matches(&read);
    assert_eq!(
        transitions.generation().read(),
        GenerationRead::Stable(Generation(start.generation.0 + 3))
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reader_holding_the_old_state_sees_the_generation_changing() {
    let _init_guard = zakura_test::init();
    let (mut state, read, transitions) = fresh_state().await;
    let template = finalize_genesis_and_block_1(&mut state).await;
    let cell = transitions.generation();
    let GenerationRead::Stable(before) = cell.read() else {
        panic!("no change is in progress");
    };

    // A reader holds the published non-finalized state. The writer cannot make
    // the new block readable until it lets go.
    let (held_tx, held_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let reader = {
        let read = read.clone();
        std::thread::spawn(move || {
            read.non_finalized_state_receiver
                .borrow_mapped(|published| {
                    held_tx
                        .send(published.best_tip())
                        .expect("the test waits for the hold");
                    release_rx.recv().expect("the test releases the hold");
                })
        })
    };
    assert_eq!(held_rx.recv().expect("the reader holds the state"), None);

    let a2 = template.make_fake_siblings(1).remove(0);
    let response = state.queue_and_commit_to_non_finalized_state(a2.clone().prepare(), None);

    let deadline = std::time::Instant::now() + LIMIT;
    while cell.read() != GenerationRead::Changing {
        assert!(
            std::time::Instant::now() < deadline,
            "the writer begins the change before it publishes the new state"
        );
        std::thread::sleep(Duration::from_millis(1));
    }

    release_tx.send(()).expect("the reader is waiting");
    reader.join().expect("the reader does not panic");
    timeout(LIMIT, response)
        .await
        .expect("the commit finishes")
        .expect("the writer answers")
        .expect("the block commits");
    assert_eq!(
        cell.read(),
        GenerationRead::Stable(Generation(before.0 + 1))
    );
    assert_eq!(read.best_tip(), Some((block::Height(2), a2.hash())));
}
