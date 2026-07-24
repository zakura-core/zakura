//! Deterministic interleavings for coherent best-chain read views.

use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

use tokio::{sync::watch, time::timeout};
use tower::ServiceExt;
use zakura_chain::{
    amount::{Amount, NonNegative},
    block::{Block, Height},
    parameters::{Network::Mainnet, NetworkUpgrade},
    serialization::ZcashDeserializeInto,
    transaction::{LockTime, Transaction},
    transparent,
};
use zakura_test::prelude::Result;

use crate::{
    arbitrary::Prepare,
    config::StorageMode,
    service::{
        finalized_state::{FinalizedState, HeaderRootAuthState, HighestCompletedCheckpoint},
        non_finalized_state::NonFinalizedState,
        BestChainReadViewCapturePhase, BestChainReadViewTestHook, ReadStateService,
        VctRootRepairStatus,
    },
    tests::FakeChainHelper,
    CheckpointVerifiedBlock, Config, PruningConfig, ReadRequest, ReadResponse, WatchReceiver,
};

fn continuous_mainnet_blocks() -> Vec<Arc<Block>> {
    zakura_test::vectors::CONTINUOUS_MAINNET_BLOCKS
        .values()
        .map(|bytes| bytes.zcash_deserialize_into().unwrap())
        .collect()
}

fn finalized_state() -> FinalizedState {
    FinalizedState::new(&crate::Config::ephemeral(), &Mainnet)
        .expect("opening an ephemeral finalized state succeeds")
}

fn commit_finalized(state: &mut FinalizedState, block: Arc<Block>) {
    state
        .commit_finalized_direct(
            CheckpointVerifiedBlock::from(block).into(),
            None,
            None,
            "atomic best-chain read-view test",
        )
        .expect("continuous test block commits to finalized state");
}

fn read_service(
    finalized: &FinalizedState,
    non_finalized: NonFinalizedState,
) -> (ReadStateService, watch::Sender<NonFinalizedState>) {
    let (non_finalized_sender, non_finalized_receiver) = watch::channel(non_finalized);
    let (_checkpoint_sender, checkpoint_receiver) =
        watch::channel(None::<HighestCompletedCheckpoint>);
    let (_repair_sender, repair_receiver) = watch::channel(VctRootRepairStatus::default());
    let (_header_root_auth_sender, header_root_auth_receiver) =
        watch::channel(None::<HeaderRootAuthState>);

    let read_state = ReadStateService::new(
        finalized,
        None,
        WatchReceiver::new(non_finalized_receiver),
        checkpoint_receiver,
        None,
        repair_receiver,
        header_root_auth_receiver,
    );

    (read_state, non_finalized_sender)
}

fn outpoint(block: &Block) -> transparent::OutPoint {
    transparent::OutPoint {
        hash: block.transactions[0].hash(),
        index: 0,
    }
}

/// Builds a linked block for state-interleaving tests using the current transaction envelope.
///
/// The continuous genesis vectors use V1 transactions, but non-finalized state starts above the
/// mandatory Canopy checkpoint and intentionally rejects V1-V3. These tests bypass semantic
/// consensus checks and preserve the vectors' transparent values while exercising V6 state
/// accounting. Committing each block still drives the real non-finalized chain implementation.
fn non_finalized_child(parent: &Arc<Block>) -> Arc<Block> {
    let mut child = parent.make_fake_child();
    let block = Arc::make_mut(&mut child);

    for transaction in &mut block.transactions {
        let inputs = transaction.inputs().to_vec();
        let mut outputs = transaction.outputs().to_vec();
        let transparent::Input::Coinbase { height, .. } = &inputs[0] else {
            panic!("the first transaction must be coinbase");
        };
        // V6 coinbase input data is not enough to make the txid unique, so bind the
        // synthetic block height into an output script that is committed by the txid.
        let mut lock_script = outputs[0].lock_script.as_raw_bytes().to_vec();
        lock_script.extend_from_slice(&height.0.to_le_bytes());
        outputs[0].lock_script = transparent::Script::new(&lock_script);

        *Arc::make_mut(transaction) = Transaction::V6 {
            network_upgrade: NetworkUpgrade::Nu6_3,
            lock_time: LockTime::unlocked(),
            expiry_height: Height(0),
            inputs,
            outputs,
            sapling_shielded_data: None,
            orchard_shielded_data: None,
            ironwood_shielded_data: None,
        };
    }

    child
}

fn with_coinbase_tag(mut block: Arc<Block>, tag: u8) -> Arc<Block> {
    let mutable_block = Arc::make_mut(&mut block);
    let transaction = Arc::make_mut(&mut mutable_block.transactions[0]);
    let transparent::Input::Coinbase { data, .. } = &mut transaction.inputs_mut()[0] else {
        panic!("the first transaction must be coinbase");
    };
    data.push(tag);
    let output = &mut transaction.outputs_mut()[0];
    let mut lock_script = output.lock_script.as_raw_bytes().to_vec();
    lock_script.push(tag);
    output.lock_script = transparent::Script::new(&lock_script);

    block
}

fn with_transparent_spend(
    mut block: Arc<Block>,
    spent_outpoint: transparent::OutPoint,
    replacement_output: transparent::Output,
) -> Arc<Block> {
    Arc::make_mut(&mut block)
        .transactions
        .push(Arc::new(Transaction::V6 {
            network_upgrade: NetworkUpgrade::Nu6_3,
            lock_time: LockTime::unlocked(),
            expiry_height: Height(0),
            inputs: vec![transparent::Input::PrevOut {
                outpoint: spent_outpoint,
                unlock_script: transparent::Script::new(&[]),
                sequence: u32::MAX,
            }],
            outputs: vec![replacement_output],
            sapling_shielded_data: None,
            orchard_shielded_data: None,
            ironwood_shielded_data: None,
        }));

    block
}

fn with_zero_value_transparent_output(
    mut block: Arc<Block>,
    tag: u8,
) -> (Arc<Block>, transparent::OutPoint, transparent::Output) {
    let output = transparent::Output::new(
        Amount::<NonNegative>::zero(),
        transparent::Script::new(&[tag]),
    );
    let transaction = Arc::new(Transaction::V6 {
        network_upgrade: NetworkUpgrade::Nu6_3,
        lock_time: LockTime::unlocked(),
        expiry_height: Height(0),
        inputs: vec![],
        outputs: vec![output.clone()],
        sapling_shielded_data: None,
        orchard_shielded_data: None,
        ironwood_shielded_data: None,
    });
    let outpoint = transparent::OutPoint {
        hash: transaction.hash(),
        index: 0,
    };
    Arc::make_mut(&mut block).transactions.push(transaction);

    (block, outpoint, output)
}

async fn query_unspent_output(
    read_state: ReadStateService,
    outpoint: transparent::OutPoint,
) -> crate::BestChainUnspentOutput {
    let response = read_state
        .oneshot(ReadRequest::BestChainUnspentOutput(outpoint))
        .await
        .expect("coherent state request succeeds");

    let ReadResponse::BestChainUnspentOutput(Some(output)) = response else {
        panic!("expected an unspent best-chain output");
    };

    output
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn decomposed_gettxout_reads_can_mix_real_chain_views() -> Result<()> {
    let _init_guard = zakura_test::init();
    let blocks = continuous_mainnet_blocks();
    let mut finalized = finalized_state();
    commit_finalized(&mut finalized, blocks[0].clone());
    commit_finalized(&mut finalized, blocks[1].clone());
    let (read_state, _non_finalized_sender) =
        read_service(&finalized, NonFinalizedState::new(&Mainnet));

    let ReadResponse::Tip(Some(old_tip)) = read_state
        .clone()
        .oneshot(ReadRequest::Tip)
        .await
        .expect("tip request succeeds")
    else {
        panic!("expected a populated tip");
    };
    commit_finalized(&mut finalized, blocks[2].clone());
    let ReadResponse::Transaction(Some(transaction)) = read_state
        .oneshot(ReadRequest::Transaction(blocks[1].transactions[0].hash()))
        .await
        .expect("transaction request succeeds")
    else {
        panic!("expected a mined transaction");
    };

    let (old_tip_height, old_tip_hash) = old_tip;
    assert_eq!(old_tip_hash, blocks[1].hash());
    assert_eq!(transaction.tx.hash(), blocks[1].transactions[0].hash());
    assert_eq!(transaction.confirmations, 2);
    let confirmations_from_old_tip = 1 + old_tip_height.0 - transaction.height.0;
    assert_eq!(confirmations_from_old_tip, 1);
    assert_ne!(transaction.confirmations, confirmations_from_old_tip);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn coherent_view_retries_non_finalized_tip_advance() -> Result<()> {
    let _init_guard = zakura_test::init();
    let blocks = continuous_mainnet_blocks();
    let mut finalized = finalized_state();
    commit_finalized(&mut finalized, blocks[0].clone());
    let block1 = non_finalized_child(&blocks[0]);
    let block2 = non_finalized_child(&block1);

    let mut non_finalized = NonFinalizedState::new(&Mainnet);
    non_finalized.commit_new_chain(block1.clone().prepare(), &finalized)?;
    let mut advanced = non_finalized.clone();
    advanced.commit_block(block2.clone().prepare(), &finalized)?;
    let (read_state, non_finalized_sender) = read_service(&finalized, non_finalized);
    let (hook, entered, resume, visits) =
        BestChainReadViewTestHook::new(BestChainReadViewCapturePhase::BeforeFinalizedSnapshot);
    let read_state = read_state.with_best_chain_read_view_test_hook(hook);
    let request = tokio::spawn(query_unspent_output(read_state, outpoint(&block1)));

    entered
        .recv_timeout(Duration::from_secs(10))
        .expect("request reaches the capture hook");
    non_finalized_sender
        .send(advanced)
        .expect("read-state receiver remains open");
    resume.send(()).expect("capture hook remains open");
    let output = timeout(Duration::from_secs(10), request)
        .await
        .expect("request completes after release")
        .expect("request task does not panic");

    assert_eq!(output.tip_hash, block2.hash());
    assert_eq!(output.transaction.tx.hash(), block1.transactions[0].hash());
    assert_eq!(output.transaction.confirmations, 2);
    assert!(visits.load(Ordering::SeqCst) >= 2);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn coherent_view_retries_when_finalized_output_becomes_spent() -> Result<()> {
    let _init_guard = zakura_test::init();
    let blocks = continuous_mainnet_blocks();
    let mut finalized = finalized_state();
    commit_finalized(&mut finalized, blocks[0].clone());

    // Seed a zero-value non-coinbase UTXO through the checkpoint-verified direct-commit
    // fixture path. The coherent read and non-finalized spend accounting remain real.
    let (finalized_output_block, queried_outpoint, replacement_output) =
        with_zero_value_transparent_output(non_finalized_child(&blocks[0]), 0x51);
    commit_finalized(&mut finalized, finalized_output_block.clone());

    let mut original = NonFinalizedState::new(&Mainnet);
    let mut tip = non_finalized_child(&finalized_output_block);
    original.commit_new_chain(tip.clone().prepare(), &finalized)?;
    for _height in 3..=100 {
        tip = non_finalized_child(&tip);
        original.commit_block(tip.clone().prepare(), &finalized)?;
    }

    let spending_tip = with_transparent_spend(
        non_finalized_child(&tip),
        queried_outpoint,
        replacement_output,
    );
    let mut spent = original.clone();
    spent.commit_block(spending_tip.prepare(), &finalized)?;

    let (read_state, non_finalized_sender) = read_service(&finalized, original);
    let (hook, entered, resume, visits) =
        BestChainReadViewTestHook::new(BestChainReadViewCapturePhase::BeforeFinalizedSnapshot);
    let read_state = read_state.with_best_chain_read_view_test_hook(hook);
    let request = tokio::spawn(async move {
        read_state
            .oneshot(ReadRequest::BestChainUnspentOutput(queried_outpoint))
            .await
    });

    entered
        .recv_timeout(Duration::from_secs(10))
        .expect("request reaches the capture hook");
    non_finalized_sender
        .send(spent)
        .expect("read-state receiver remains open");
    resume.send(()).expect("capture hook remains open");
    let response = timeout(Duration::from_secs(10), request)
        .await
        .expect("request completes after release")
        .expect("request task does not panic")
        .expect("coherent state request succeeds");

    assert_eq!(response, ReadResponse::BestChainUnspentOutput(None));
    assert!(visits.load(Ordering::SeqCst) >= 2);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn coherent_view_retries_same_height_reorg() -> Result<()> {
    let _init_guard = zakura_test::init();
    let blocks = continuous_mainnet_blocks();
    let mut finalized = finalized_state();
    commit_finalized(&mut finalized, blocks[0].clone());

    let old_tip = non_finalized_child(&blocks[0]).set_work(10);
    let new_tip = with_coinbase_tag(non_finalized_child(&blocks[0]).set_work(20), 1);
    assert_ne!(
        old_tip.transactions[0].hash(),
        new_tip.transactions[0].hash()
    );
    assert_ne!(old_tip.hash(), new_tip.hash());

    let mut non_finalized = NonFinalizedState::new(&Mainnet);
    non_finalized.commit_new_chain(old_tip.clone().prepare(), &finalized)?;
    let mut reorged = non_finalized.clone();
    reorged.commit_new_chain(new_tip.clone().prepare(), &finalized)?;
    let (read_state, non_finalized_sender) = read_service(&finalized, non_finalized);
    let (hook, entered, resume, visits) =
        BestChainReadViewTestHook::new(BestChainReadViewCapturePhase::BeforeFinalizedSnapshot);
    let read_state = read_state.with_best_chain_read_view_test_hook(hook);
    let request = tokio::spawn(query_unspent_output(read_state, outpoint(&new_tip)));

    entered
        .recv_timeout(Duration::from_secs(10))
        .expect("request reaches the capture hook");
    non_finalized_sender
        .send(reorged)
        .expect("read-state receiver remains open");
    resume.send(()).expect("capture hook remains open");
    let output = timeout(Duration::from_secs(10), request)
        .await
        .expect("request completes after release")
        .expect("request task does not panic");

    assert_eq!(output.tip_hash, new_tip.hash());
    assert_eq!(output.transaction.tx.hash(), new_tip.transactions[0].hash());
    assert_eq!(output.transaction.confirmations, 1);
    assert!(visits.load(Ordering::SeqCst) >= 2);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn coherent_view_detects_same_height_reorg_aba() -> Result<()> {
    let _init_guard = zakura_test::init();
    let blocks = continuous_mainnet_blocks();
    let mut finalized = finalized_state();
    commit_finalized(&mut finalized, blocks[0].clone());

    let old_tip = non_finalized_child(&blocks[0]).set_work(10);
    let intermediate_tip = non_finalized_child(&blocks[0]).set_work(20);
    assert_eq!(
        old_tip.transactions[0].hash(),
        intermediate_tip.transactions[0].hash()
    );

    let mut original = NonFinalizedState::new(&Mainnet);
    original.commit_new_chain(old_tip.clone().prepare(), &finalized)?;
    let mut reorged = original.clone();
    reorged.commit_new_chain(intermediate_tip.prepare(), &finalized)?;
    let (read_state, non_finalized_sender) = read_service(&finalized, original.clone());
    let (hook, entered, resume, visits) =
        BestChainReadViewTestHook::new(BestChainReadViewCapturePhase::BeforeFinalizedSnapshot);
    let read_state = read_state.with_best_chain_read_view_test_hook(hook);
    let request = tokio::spawn(query_unspent_output(read_state, outpoint(&old_tip)));

    entered
        .recv_timeout(Duration::from_secs(10))
        .expect("request reaches the capture hook");
    non_finalized_sender
        .send(reorged)
        .expect("read-state receiver remains open");
    non_finalized_sender
        .send(original)
        .expect("read-state receiver remains open");
    resume.send(()).expect("capture hook remains open");
    let output = timeout(Duration::from_secs(10), request)
        .await
        .expect("request completes after release")
        .expect("request task does not panic");

    assert_eq!(output.tip_hash, old_tip.hash());
    assert_eq!(output.transaction.tx.hash(), old_tip.transactions[0].hash());
    assert_eq!(output.transaction.confirmations, 1);
    assert!(visits.load(Ordering::SeqCst) >= 2);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn coherent_view_returns_error_after_bounded_retries() -> Result<()> {
    let _init_guard = zakura_test::init();
    let blocks = continuous_mainnet_blocks();
    let mut finalized = finalized_state();
    commit_finalized(&mut finalized, blocks[0].clone());
    let block1 = non_finalized_child(&blocks[0]);
    let block2 = non_finalized_child(&block1);

    let mut original = NonFinalizedState::new(&Mainnet);
    original.commit_new_chain(block1.clone().prepare(), &finalized)?;
    let mut advanced = original.clone();
    advanced.commit_block(block2.prepare(), &finalized)?;
    let (read_state, non_finalized_sender) = read_service(&finalized, original.clone());
    let (hook, entered, resume, visits) = BestChainReadViewTestHook::every_visit(
        BestChainReadViewCapturePhase::BeforeFinalizedSnapshot,
    );
    let read_state = read_state.with_best_chain_read_view_test_hook(hook);
    let request_outpoint = outpoint(&block1);
    let request = tokio::spawn(async move {
        read_state
            .oneshot(ReadRequest::BestChainUnspentOutput(request_outpoint))
            .await
    });

    for next_state in [advanced.clone(), original, advanced] {
        entered
            .recv_timeout(Duration::from_secs(10))
            .expect("request reaches every capture attempt");
        non_finalized_sender
            .send(next_state)
            .expect("read-state receiver remains open");
        resume.send(()).expect("capture hook remains open");
    }

    let response = timeout(Duration::from_secs(10), request)
        .await
        .expect("request returns after bounded retries")
        .expect("request task does not panic");
    let error = response.expect_err("unstable capture returns a normal state error");

    assert_eq!(visits.load(Ordering::SeqCst), 3);
    assert!(error
        .to_string()
        .contains("best chain changed while capturing a coherent read view"));

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn coherent_view_pins_finalized_only_advance() {
    let _init_guard = zakura_test::init();
    let blocks = continuous_mainnet_blocks();
    let mut finalized = finalized_state();
    commit_finalized(&mut finalized, blocks[0].clone());
    commit_finalized(&mut finalized, blocks[1].clone());

    let (read_state, _non_finalized_sender) =
        read_service(&finalized, NonFinalizedState::new(&Mainnet));
    let (hook, entered, resume, _visits) =
        BestChainReadViewTestHook::new(BestChainReadViewCapturePhase::AfterFinalizedSnapshot);
    let read_state = read_state.with_best_chain_read_view_test_hook(hook);
    let request = tokio::spawn(query_unspent_output(read_state, outpoint(&blocks[1])));

    entered
        .recv_timeout(Duration::from_secs(10))
        .expect("request reaches the capture hook");
    commit_finalized(&mut finalized, blocks[2].clone());
    resume.send(()).expect("capture hook remains open");
    let output = timeout(Duration::from_secs(10), request)
        .await
        .expect("request completes after release")
        .expect("request task does not panic");

    assert_eq!(output.tip_hash, blocks[1].hash());
    assert_eq!(
        output.transaction.tx.hash(),
        blocks[1].transactions[0].hash()
    );
    assert_eq!(output.transaction.confirmations, 1);
    assert_eq!(
        finalized.db.tip().expect("live state has a tip").1,
        blocks[2].hash()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn coherent_view_survives_non_finalized_to_finalized_transition() -> Result<()> {
    let _init_guard = zakura_test::init();
    let blocks = continuous_mainnet_blocks();
    let mut finalized = finalized_state();
    commit_finalized(&mut finalized, blocks[0].clone());
    let block1 = non_finalized_child(&blocks[0]);

    let mut non_finalized = NonFinalizedState::new(&Mainnet);
    non_finalized.commit_new_chain(block1.clone().prepare(), &finalized)?;
    let (read_state, _non_finalized_sender) = read_service(&finalized, non_finalized);
    let (hook, entered, resume, _visits) =
        BestChainReadViewTestHook::new(BestChainReadViewCapturePhase::AfterFinalizedSnapshot);
    let read_state = read_state.with_best_chain_read_view_test_hook(hook);
    let request = tokio::spawn(query_unspent_output(read_state, outpoint(&block1)));

    entered
        .recv_timeout(Duration::from_secs(10))
        .expect("request reaches the capture hook");
    commit_finalized(&mut finalized, block1.clone());
    resume.send(()).expect("capture hook remains open");
    let output = timeout(Duration::from_secs(10), request)
        .await
        .expect("request completes after release")
        .expect("request task does not panic");

    assert_eq!(output.tip_hash, block1.hash());
    assert_eq!(output.transaction.tx.hash(), block1.transactions[0].hash());
    assert_eq!(output.transaction.confirmations, 1);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn coherent_view_uses_newer_finalized_tip_during_read_only_catch_up() -> Result<()> {
    let _init_guard = zakura_test::init();
    let blocks = continuous_mainnet_blocks();
    let mut finalized = finalized_state();
    commit_finalized(&mut finalized, blocks[0].clone());
    let stale_tip = non_finalized_child(&blocks[0]);
    let finalized_tip = non_finalized_child(&stale_tip);

    let mut non_finalized = NonFinalizedState::new(&Mainnet);
    non_finalized.commit_new_chain(stale_tip.clone().prepare(), &finalized)?;
    let (read_state, _non_finalized_sender) = read_service(&finalized, non_finalized);

    commit_finalized(&mut finalized, stale_tip);
    commit_finalized(&mut finalized, finalized_tip.clone());

    let output = query_unspent_output(read_state, outpoint(&finalized_tip)).await;

    assert_eq!(output.tip_hash, finalized_tip.hash());
    assert_eq!(
        output.transaction.tx.hash(),
        finalized_tip.transactions[0].hash()
    );
    assert_eq!(output.transaction.confirmations, 1);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn coherent_view_does_not_resurrect_output_spent_after_stale_chain_tip() -> Result<()> {
    let _init_guard = zakura_test::init();
    let blocks = continuous_mainnet_blocks();
    let mut finalized = finalized_state();
    commit_finalized(&mut finalized, blocks[0].clone());
    let (created_block, queried_outpoint, replacement_output) =
        with_zero_value_transparent_output(non_finalized_child(&blocks[0]), 0x52);

    let mut non_finalized = NonFinalizedState::new(&Mainnet);
    non_finalized.commit_new_chain(created_block.clone().prepare(), &finalized)?;
    let (read_state, _non_finalized_sender) = read_service(&finalized, non_finalized);

    commit_finalized(&mut finalized, created_block.clone());
    let spending_block = with_transparent_spend(
        non_finalized_child(&created_block),
        queried_outpoint,
        replacement_output,
    );
    commit_finalized(&mut finalized, spending_block);

    let response = read_state
        .oneshot(ReadRequest::BestChainUnspentOutput(queried_outpoint))
        .await
        .expect("coherent state request succeeds");

    assert_eq!(response, ReadResponse::BestChainUnspentOutput(None));

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn coherent_view_preserves_empty_state_error() {
    let _init_guard = zakura_test::init();
    let finalized = finalized_state();
    let (read_state, _non_finalized_sender) =
        read_service(&finalized, NonFinalizedState::new(&Mainnet));
    let missing = transparent::OutPoint {
        hash: zakura_chain::transaction::Hash([0; 32]),
        index: 0,
    };

    let error = read_state
        .oneshot(ReadRequest::BestChainUnspentOutput(missing))
        .await
        .expect_err("an empty state remains an RPC error");

    assert!(error.to_string().contains("No blocks in state"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn coherent_view_returns_none_for_missing_output_in_nonempty_state() {
    let _init_guard = zakura_test::init();
    let blocks = continuous_mainnet_blocks();
    let mut finalized = finalized_state();
    commit_finalized(&mut finalized, blocks[0].clone());
    let (read_state, _non_finalized_sender) =
        read_service(&finalized, NonFinalizedState::new(&Mainnet));
    let missing = transparent::OutPoint {
        hash: zakura_chain::transaction::Hash([0; 32]),
        index: 0,
    };

    let response = read_state
        .oneshot(ReadRequest::BestChainUnspentOutput(missing))
        .await
        .expect("a missing output in nonempty state is a normal response");

    assert_eq!(response, ReadResponse::BestChainUnspentOutput(None));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn coherent_view_errors_when_unspent_output_transaction_was_pruned() {
    let _init_guard = zakura_test::init();
    let blocks = continuous_mainnet_blocks();
    let config = Config {
        storage_mode: StorageMode::Pruned(PruningConfig { tx_retention: 5 }),
        ..Config::ephemeral()
    };
    let mut finalized =
        FinalizedState::new_with_debug_without_storage_validation(&config, &Mainnet, false, false)
            .expect("opening an ephemeral pruned state succeeds")
            .with_checkpoint_raw_tx_retention(Height(10), &config);

    for block in &blocks {
        commit_finalized(&mut finalized, block.clone());
    }

    let pruned_outpoint = outpoint(&blocks[1]);
    assert!(finalized.db.utxo(&pruned_outpoint).is_some());
    assert!(finalized.db.transaction(pruned_outpoint.hash).is_none());
    let (read_state, _non_finalized_sender) =
        read_service(&finalized, NonFinalizedState::new(&Mainnet));

    let error = read_state
        .oneshot(ReadRequest::BestChainUnspentOutput(pruned_outpoint))
        .await
        .expect_err("an unspent output without its pruned transaction is not JSON null");

    assert!(error
        .to_string()
        .contains("creating transaction is unavailable in the captured state view"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn coherent_view_recaptures_after_non_finalized_channel_closes() -> Result<()> {
    let _init_guard = zakura_test::init();
    let blocks = continuous_mainnet_blocks();
    let mut finalized = finalized_state();
    commit_finalized(&mut finalized, blocks[0].clone());
    let block1 = non_finalized_child(&blocks[0]);
    let block2 = non_finalized_child(&block1);

    let mut original = NonFinalizedState::new(&Mainnet);
    original.commit_new_chain(block1.clone().prepare(), &finalized)?;
    let mut advanced = original.clone();
    advanced.commit_block(block2.clone().prepare(), &finalized)?;
    let (read_state, non_finalized_sender) = read_service(&finalized, original);
    let (hook, entered, resume, visits) =
        BestChainReadViewTestHook::new(BestChainReadViewCapturePhase::BeforeFinalizedSnapshot);
    let read_state = read_state.with_best_chain_read_view_test_hook(hook);
    let request = tokio::spawn(query_unspent_output(read_state, outpoint(&block1)));

    entered
        .recv_timeout(Duration::from_secs(10))
        .expect("request reaches the capture hook");
    non_finalized_sender
        .send(advanced)
        .expect("read-state receiver remains open");
    drop(non_finalized_sender);
    resume.send(()).expect("capture hook remains open");
    let output = timeout(Duration::from_secs(10), request)
        .await
        .expect("request completes after channel closure")
        .expect("request task does not panic");

    assert_eq!(output.tip_hash, block2.hash());
    assert_eq!(output.transaction.tx.hash(), block1.transactions[0].hash());
    assert_eq!(output.transaction.confirmations, 2);
    assert_eq!(visits.load(Ordering::SeqCst), 1);

    Ok(())
}
