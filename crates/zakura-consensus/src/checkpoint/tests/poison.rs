//! Reproductions for same-hash checkpoint body poisoning.
//!
//! From NU5, transaction IDs do not commit to authorizing data. A peer can change a
//! transparent unlocking script without changing the transaction ID, the Merkle root, or the
//! block hash. The checkpoint verifier accepts that body, and only the state's
//! `hashBlockCommitments` check rejects it.
//!
//! These tests drive a real [`CheckpointVerifier`] into a real state service.

use std::{
    collections::{BTreeMap, HashSet},
    sync::{Arc, Mutex},
    time::Duration,
};

use color_eyre::eyre::{eyre, Report};
use futures::future::BoxFuture;
use tokio::{sync::watch, time::timeout};
use tower::{buffer::Buffer, util::BoxService, Service, ServiceExt};

use zakura_chain::{
    amount::Amount,
    block::{
        self, genesis::regtest_genesis_block, Block, ChainHistoryBlockTxAuthCommitmentHash,
        CHAIN_HISTORY_ACTIVATION_RESERVED,
    },
    history_tree::HistoryTree,
    parameters::{
        testnet::{ConfiguredActivationHeights, ConfiguredCheckpoints, Parameters},
        Network, NetworkUpgrade,
    },
    transaction::{LockTime, Transaction},
    transparent,
};
use zakura_state as zs;

use crate::checkpoint::{CheckpointVerifier, VerifyCheckpointError};

type StateBuffer = Buffer<zs::Request, BoxFuture<'static, Result<zs::Response, zs::BoxError>>>;

/// The production default for `sync.checkpoint_verify_concurrency_limit`.
const CHECKPOINT_VERIFY_CONCURRENCY_LIMIT: usize = 1000;

/// How long a test waits before it treats a verifier future as stranded.
const STRANDED_AFTER: Duration = Duration::from_secs(5);

/// A generated NU5 chain whose blocks spend the previous block's coinbase output.
struct PoisonFixture {
    network: Network,
    blocks: Vec<Arc<Block>>,
    checkpoints: Vec<(block::Height, block::Hash)>,
}

impl PoisonFixture {
    /// Builds `len` blocks after genesis, with checkpoints at genesis and `checkpoint_heights`.
    fn new(len: u32, checkpoint_heights: &[u32]) -> Result<Self, Report> {
        let genesis = regtest_genesis_block();
        let target_difficulty_limit = genesis
            .header
            .difficulty_threshold
            .to_expanded()
            .ok_or_else(|| eyre!("regtest genesis difficulty threshold is valid"))?;
        let build = |checkpoints: Vec<(block::Height, block::Hash)>| {
            Parameters::build()
                .with_network_name("PoisonTest")?
                .with_genesis_hash(genesis.hash())?
                .with_activation_heights(ConfiguredActivationHeights {
                    nu5: Some(1),
                    ..Default::default()
                })?
                .clear_funding_streams()
                .with_slow_start_interval(block::Height::MIN)
                .with_disable_pow(true)
                .with_target_difficulty_limit(target_difficulty_limit)?
                .with_checkpoints(ConfiguredCheckpoints::HeightsAndHashes(checkpoints))?
                .to_network()
                .map_err(|error| eyre!("{error:?}"))
        };

        // History tree updates depend only on the activation heights.
        let chain_network = build(vec![(block::Height(0), genesis.hash())])?;

        let sapling_root = zakura_chain::sapling::tree::NoteCommitmentTree::default().root();
        let orchard_root = zakura_chain::orchard::tree::NoteCommitmentTree::default().root();
        let ironwood_root = zakura_chain::ironwood::tree::NoteCommitmentTree::default().root();

        let mut history_tree = HistoryTree::default();
        let mut blocks = vec![genesis.clone()];
        for height in 1..=len {
            let parent = blocks.last().expect("genesis is present").clone();
            let mut child = Block::clone(&parent);

            let mut coinbase = child.transactions[0].clone();
            match Arc::make_mut(&mut coinbase).inputs_mut().get_mut(0) {
                Some(transparent::Input::Coinbase { height: h, .. }) => *h = block::Height(height),
                _ => return Err(eyre!("the regtest genesis transaction is a coinbase")),
            }
            child.transactions = vec![coinbase];

            // Genesis outputs are unspendable, so spending starts at height 2.
            if height >= 2 {
                let outpoint = transparent::OutPoint {
                    hash: parent.transactions[0].hash(),
                    index: 0,
                };
                let spend = Transaction::V5 {
                    network_upgrade: NetworkUpgrade::Nu5,
                    lock_time: LockTime::unlocked(),
                    expiry_height: block::Height(0),
                    inputs: vec![transparent::Input::PrevOut {
                        outpoint,
                        unlock_script: transparent::Script::new(&[0x51]),
                        sequence: u32::MAX,
                    }],
                    outputs: vec![transparent::Output::new(
                        Amount::try_from(0)?,
                        transparent::Script::new(&[0x51]),
                    )],
                    sapling_shielded_data: None,
                    orchard_shielded_data: None,
                };
                child.transactions.push(Arc::new(spend));
            }

            let header = Arc::make_mut(&mut child.header);
            header.previous_block_hash = parent.hash();
            header.merkle_root = child.transactions.iter().cloned().collect();

            let history_root = if height == 1 {
                CHAIN_HISTORY_ACTIVATION_RESERVED.into()
            } else {
                history_tree
                    .hash()
                    .ok_or_else(|| eyre!("the history tree starts at height 1"))?
            };
            let commitment: [u8; 32] = ChainHistoryBlockTxAuthCommitmentHash::from_commitments(
                &history_root,
                &child.auth_data_root(),
            )
            .into();
            Arc::make_mut(&mut child.header).commitment_bytes = commitment.into();

            let child = Arc::new(child);
            history_tree.push(
                &chain_network,
                child.clone(),
                &sapling_root,
                &orchard_root,
                &ironwood_root,
            )?;
            blocks.push(child);
        }

        let checkpoints: Vec<_> = std::iter::once(0)
            .chain(checkpoint_heights.iter().copied())
            .map(|height| (block::Height(height), blocks[height as usize].hash()))
            .collect();
        let network = build(checkpoints.clone())?;

        Ok(Self {
            network,
            blocks,
            checkpoints,
        })
    }

    /// Returns `blocks[height]` with a changed unlocking script.
    ///
    /// The change keeps the transaction ID and the block hash, but changes the auth-data root.
    fn forge(&self, height: u32) -> Arc<Block> {
        let honest = &self.blocks[height as usize];
        let mut forged = Block::clone(honest);
        let spend = Arc::make_mut(&mut forged.transactions[1]);
        let Some(transparent::Input::PrevOut { unlock_script, .. }) = spend.inputs_mut().get_mut(0)
        else {
            unreachable!("blocks above height 1 have a transparent spend");
        };
        *unlock_script = transparent::Script::new(&[0x52]);
        let forged = Arc::new(forged);

        assert_eq!(
            forged.hash(),
            honest.hash(),
            "the forgery keeps the block hash"
        );
        assert_eq!(
            forged.header.merkle_root, honest.header.merkle_root,
            "the forgery keeps the transaction Merkle root",
        );
        assert_ne!(
            forged.auth_data_root(),
            honest.auth_data_root(),
            "the forgery changes the auth-data root",
        );
        forged
    }

    fn hash(&self, height: u32) -> block::Hash {
        self.blocks[height as usize].hash()
    }

    fn max_checkpoint_height(&self) -> block::Height {
        self.checkpoints.last().expect("genesis is a checkpoint").0
    }

    /// Opens a real state, using the production checkpoint concurrency default.
    async fn state(&self) -> Result<StateBuffer, Report> {
        let (state, _, _, _) = zs::init(
            zs::Config::ephemeral(),
            &self.network,
            self.max_checkpoint_height(),
            CHECKPOINT_VERIFY_CONCURRENCY_LIMIT,
        )
        .await
        .map_err(|error| eyre!("{error:?}"))?;
        Ok(Buffer::new(BoxService::new(state), 50))
    }
}

/// Holds selected checkpoint commits until a forged body has been rejected.
///
/// This models the commit tasks that the checkpoint verifier spawns. Those tasks reach the
/// state in any order, so one slow task can reach the state after the writer rejects an
/// ancestor.
#[derive(Clone)]
struct HoldUntilRejected {
    held: Arc<Mutex<HashSet<block::Hash>>>,
    rejected: watch::Receiver<bool>,
}

impl HoldUntilRejected {
    fn wait(&self, hash: block::Hash) -> BoxFuture<'static, ()> {
        let hold = self
            .held
            .lock()
            .expect("lock is not poisoned")
            .remove(&hash);
        let mut rejected = self.rejected.clone();
        Box::pin(async move {
            if hold {
                rejected
                    .wait_for(|rejected| *rejected)
                    .await
                    .expect("the test keeps the sender");
            }
        })
    }
}

fn hold_selected_commits<S>(
    state: S,
    hold: HoldUntilRejected,
    rejected_hash: block::Hash,
    rejected: watch::Sender<bool>,
) -> impl Service<
    zs::Request,
    Response = zs::Response,
    Error = zs::BoxError,
    Future = BoxFuture<'static, Result<zs::Response, zs::BoxError>>,
> + Clone
       + Send
       + 'static
where
    S: Service<zs::Request, Response = zs::Response, Error = zs::BoxError> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    let rejected = Arc::new(rejected);
    tower::service_fn(move |request: zs::Request| {
        let state = state.clone();
        let hold = hold.clone();
        let rejected = rejected.clone();
        let future: BoxFuture<'static, _> = Box::pin(async move {
            let commit_hash = match &request {
                zs::Request::CommitCheckpointVerifiedBlock(block) => Some(block.hash),
                _ => None,
            };
            if let Some(hash) = commit_hash {
                hold.wait(hash).await;
            }
            let response: Result<zs::Response, zs::BoxError> = state.oneshot(request).await;
            if commit_hash == Some(rejected_hash) && response.is_err() {
                rejected.send_replace(true);
            }
            response
        });
        future
    })
}

async fn submit<V>(verifier: &mut V, block: Arc<Block>) -> V::Future
where
    V: Service<Arc<Block>, Response = block::Hash, Error = VerifyCheckpointError>,
{
    verifier
        .ready()
        .await
        .expect("the checkpoint verifier is always ready")
        .call(block)
}

/// A same-hash forged body strands every descendant that reaches the state after the rejection.
///
/// Root cause A: the state removes each queued block as it sends it to the writer. After the
/// writer rejects the forged body, the state rewinds to the parent, but the queue no longer holds
/// the rejected hash, so later descendants never drain and never receive a response. The checkpoint
/// verifier has already removed those descendants from its own queue, so it waits for them before
/// it forwards an honest copy of the forged block.
///
/// Root cause B: near the final checkpoint, the state also reports the rejected hash as known.
#[tokio::test(flavor = "multi_thread")]
async fn forged_body_strands_descendants_that_reach_state_after_the_rejection() -> Result<(), Report>
{
    let _init_guard = zakura_test::init();

    const RANGE_END: u32 = 10;
    const FORGED: u32 = 3;
    const SLOW: u32 = 5;

    let fixture = PoisonFixture::new(RANGE_END, &[RANGE_END])?;
    let state = fixture.state().await?;
    let (rejected_sender, rejected) = watch::channel(false);
    let hold = HoldUntilRejected {
        held: Arc::new(Mutex::new(HashSet::from([fixture.hash(SLOW)]))),
        rejected,
    };
    let wrapped = hold_selected_commits(state.clone(), hold, fixture.hash(FORGED), rejected_sender);
    let mut verifier = CheckpointVerifier::from_list(
        fixture.checkpoints.clone(),
        &fixture.network,
        None,
        wrapped,
    )?;

    let genesis = submit(&mut verifier, fixture.blocks[0].clone()).await;
    assert_eq!(timeout(STRANDED_AFTER, genesis).await??, fixture.hash(0));

    // The syncer submits the whole range. One body is forged.
    let mut pending = BTreeMap::new();
    for height in 1..=RANGE_END {
        let block = if height == FORGED {
            fixture.forge(FORGED)
        } else {
            fixture.blocks[height as usize].clone()
        };
        pending.insert(height, submit(&mut verifier, block).await);
    }

    let mut results = BTreeMap::new();
    for (height, future) in pending {
        results.insert(height, timeout(STRANDED_AFTER, future).await);
    }

    for height in 1..FORGED {
        assert!(
            matches!(results[&height], Ok(Ok(hash)) if hash == fixture.hash(height)),
            "height {height} commits: {:?}",
            results[&height],
        );
    }
    assert!(
        matches!(&results[&FORGED], Ok(Err(error)) if error.is_auth_commitment_mismatch()),
        "the forged body fails its commitment: {:?}",
        results[&FORGED],
    );
    for height in FORGED + 1..SLOW {
        assert!(
            matches!(&results[&height], Ok(Err(error)) if error.is_descendant_of_auth_commitment_mismatch()),
            "height {height} reached the writer and gets the retryable error: {:?}",
            results[&height],
        );
    }
    let stranded: Vec<u32> = results
        .iter()
        .filter(|(_, result)| result.is_err())
        .map(|(height, _)| *height)
        .collect();
    assert_eq!(
        stranded,
        (SLOW..=RANGE_END).collect::<Vec<_>>(),
        "every block from the slow commit onward never gets a response",
    );

    let tip = state.clone().oneshot(zs::Request::Tip).await;
    assert!(
        matches!(tip, Ok(zs::Response::Tip(Some((height, _)))) if height == block::Height(FORGED - 1)),
        "the state tip stays at the forged block's parent: {tip:?}",
    );

    // Root cause B: the uncommitted forged hash is reported as known.
    let known = state
        .clone()
        .oneshot(zs::Request::KnownBlock(fixture.hash(FORGED)))
        .await;
    assert!(
        matches!(
            known,
            Ok(zs::Response::KnownBlock(Some(zs::KnownBlock::WriteChannel)))
        ),
        "the rejected hash stays in the sent-hash set: {known:?}",
    );

    // The syncer requeues the forged block and its rejected descendants.
    // The honest body cannot commit, because the verifier waits for the stranded heights.
    let mut retried = Vec::new();
    for height in FORGED..SLOW {
        retried.push((
            height,
            submit(&mut verifier, fixture.blocks[height as usize].clone()).await,
        ));
    }
    let mut retried_waiting = Vec::new();
    for (height, future) in retried {
        match timeout(STRANDED_AFTER, future).await {
            Err(_) => retried_waiting.push(height),
            Ok(result) => {
                panic!("height {height} resolved without the stranded heights: {result:?}")
            }
        }
    }
    assert_eq!(retried_waiting, (FORGED..SLOW).collect::<Vec<_>>());

    let known = state
        .clone()
        .oneshot(zs::Request::KnownBlock(fixture.hash(FORGED)))
        .await;
    assert!(
        matches!(
            known,
            Ok(zs::Response::KnownBlock(Some(zs::KnownBlock::WriteChannel)))
        ),
        "the rejected hash is still reported as known while it is uncommitted: {known:?}",
    );

    // Legacy sync recovers only when its verify timeout makes it download the stranded heights.
    let mut recovered = Vec::new();
    for height in SLOW..=RANGE_END {
        recovered.push((
            height,
            submit(&mut verifier, fixture.blocks[height as usize].clone()).await,
        ));
    }
    for (height, future) in recovered {
        // The first copy can commit before the second copy reaches the state.
        // The syncer treats a finalized duplicate as a success.
        let result = timeout(STRANDED_AFTER, future).await;
        assert!(
            match &result {
                Ok(Ok(hash)) => *hash == fixture.hash(height),
                Ok(Err(error)) => error.duplicate_location() == Some(&zs::KnownBlock::Finalized),
                Err(_) => false,
            },
            "height {height} commits after the stranded heights are downloaded again: {result:?}",
        );
    }
    let tip = state.clone().oneshot(zs::Request::Tip).await;
    assert!(
        matches!(tip, Ok(zs::Response::Tip(Some((height, _)))) if height == block::Height(RANGE_END)),
        "{tip:?}",
    );

    Ok(())
}

/// Measures how often a forged body strands descendants without any injected delay.
///
/// Run with `--ignored --nocapture`. The test submits a full 400-block range and counts the
/// verifier futures that never resolve.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn forged_body_strand_rate_under_natural_scheduling() -> Result<(), Report> {
    let _init_guard = zakura_test::init();

    const RANGE_END: u32 = 400;
    let trials: usize = std::env::var("POISON_TRIALS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(10);

    let fixture = PoisonFixture::new(RANGE_END, &[RANGE_END])?;

    for forged_height in [2, 5, 10, 25, 50, 200, 399] {
        let mut stranded_trials = 0;
        let mut stranded_blocks = Vec::new();
        for _ in 0..trials {
            let state = fixture.state().await?;
            let mut verifier = CheckpointVerifier::from_list(
                fixture.checkpoints.clone(),
                &fixture.network,
                None,
                state.clone(),
            )?;

            let genesis = submit(&mut verifier, fixture.blocks[0].clone()).await;
            timeout(STRANDED_AFTER, genesis).await??;

            let mut pending = Vec::new();
            for height in 1..=RANGE_END {
                let block = if height == forged_height {
                    fixture.forge(forged_height)
                } else {
                    fixture.blocks[height as usize].clone()
                };
                pending.push(submit(&mut verifier, block).await);
            }
            let results = futures::future::join_all(
                pending
                    .into_iter()
                    .map(|future| timeout(STRANDED_AFTER, future)),
            )
            .await;
            let stranded = results.iter().filter(|result| result.is_err()).count();
            if stranded > 0 {
                stranded_trials += 1;
            }
            stranded_blocks.push(stranded);
        }

        #[allow(clippy::print_stderr)]
        {
            eprintln!(
                "forged height {forged_height}: {stranded_trials}/{trials} trials stranded \
                 descendants; stranded counts {stranded_blocks:?}"
            );
        }
    }

    Ok(())
}

/// At ZIP 234 heights, a descendant of a rejected body fails while it waits for its parent's
/// block info. Root cause C: that error is not classified as a descendant of a forged body.
#[test]
fn await_block_info_rejection_is_not_classified_as_a_forged_body_descendant() {
    let error = VerifyCheckpointError::CommitCheckpointVerified(
        zs::AwaitBlockInfoError::Rejected {
            hash: block::Hash([7; 32]),
        }
        .into(),
    );

    assert!(!error.is_descendant_of_auth_commitment_mismatch());
}

/// Holds checkpoint commits for chosen hashes until a chosen commit finishes.
#[derive(Clone, Default)]
struct CommitGate {
    holds: Arc<Mutex<Vec<(HashSet<block::Hash>, watch::Receiver<bool>)>>>,
    triggers: Arc<Mutex<Vec<(block::Hash, bool, watch::Sender<bool>)>>>,
}

impl CommitGate {
    /// Holds the next commit of each hash in `held` until `trigger` commits with
    /// `trigger_ok` success.
    fn hold(
        &self,
        held: impl IntoIterator<Item = block::Hash>,
        trigger: block::Hash,
        trigger_ok: bool,
    ) {
        let (sender, receiver) = watch::channel(false);
        self.holds
            .lock()
            .expect("lock is not poisoned")
            .push((held.into_iter().collect(), receiver));
        self.triggers
            .lock()
            .expect("lock is not poisoned")
            .push((trigger, trigger_ok, sender));
    }

    fn wait(&self, hash: block::Hash) -> BoxFuture<'static, ()> {
        let receiver = self
            .holds
            .lock()
            .expect("lock is not poisoned")
            .iter_mut()
            .find_map(|(held, receiver)| held.remove(&hash).then(|| receiver.clone()));
        Box::pin(async move {
            if let Some(mut receiver) = receiver {
                receiver
                    .wait_for(|released| *released)
                    .await
                    .expect("the gate keeps the sender");
            }
        })
    }

    fn finished(&self, hash: block::Hash, ok: bool) {
        for (trigger, trigger_ok, sender) in
            self.triggers.lock().expect("lock is not poisoned").iter()
        {
            if *trigger == hash && *trigger_ok == ok {
                sender.send_replace(true);
            }
        }
    }
}

fn gated_state<S>(
    state: S,
    gate: CommitGate,
) -> impl Service<
    zs::Request,
    Response = zs::Response,
    Error = zs::BoxError,
    Future = BoxFuture<'static, Result<zs::Response, zs::BoxError>>,
> + Clone
       + Send
       + 'static
where
    S: Service<zs::Request, Response = zs::Response, Error = zs::BoxError> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    tower::service_fn(move |request: zs::Request| {
        let state = state.clone();
        let gate = gate.clone();
        let future: BoxFuture<'static, _> = Box::pin(async move {
            let commit_hash = match &request {
                zs::Request::CommitCheckpointVerifiedBlock(block) => Some(block.hash),
                _ => None,
            };
            if let Some(hash) = commit_hash {
                gate.wait(hash).await;
            }
            let response: Result<zs::Response, zs::BoxError> = state.oneshot(request).await;
            if let Some(hash) = commit_hash {
                gate.finished(hash, response.is_ok());
            }
            response
        });
        future
    })
}

/// Spawns a verifier call, so the test can check later whether it ever finished.
async fn spawn_submit<V>(
    verifier: &mut V,
    block: Arc<Block>,
) -> tokio::task::JoinHandle<Result<block::Hash, VerifyCheckpointError>>
where
    V: Service<Arc<Block>, Response = block::Hash, Error = VerifyCheckpointError>,
    V::Future: Send + 'static,
{
    tokio::spawn(submit(verifier, block).await)
}

/// Waits briefly, then splits `handles` into finished results and still-pending heights.
async fn settle(
    handles: Vec<(
        u32,
        tokio::task::JoinHandle<Result<block::Hash, VerifyCheckpointError>>,
    )>,
) -> (
    BTreeMap<u32, Result<block::Hash, VerifyCheckpointError>>,
    Vec<(
        u32,
        tokio::task::JoinHandle<Result<block::Hash, VerifyCheckpointError>>,
    )>,
) {
    tokio::time::sleep(STRANDED_AFTER).await;
    let mut finished = BTreeMap::new();
    let mut pending = Vec::new();
    for (height, handle) in handles {
        if handle.is_finished() {
            finished.insert(height, handle.await.expect("verifier calls do not panic"));
        } else {
            pending.push((height, handle));
        }
    }
    (finished, pending)
}

/// Native block sync resubmits a whole checkpoint range after a forged body. That recovery
/// commits the range but leaks never-answered commits into the state queue.
///
/// After the honest copy of the forged block commits, the state drains the old stranded copies of
/// its descendants, because they are still keyed by their parent hashes. A fresh copy of the same
/// height that reaches the state after that drain is keyed by a parent the state has already sent,
/// so it never drains and never gets a response. Native block sync keeps each such submission in
/// its 401-block window, so repeated attacks shrink the window until a range no longer fits.
#[tokio::test(flavor = "multi_thread")]
async fn range_resubmission_strands_fresh_copies_behind_drained_old_copies() -> Result<(), Report> {
    let _init_guard = zakura_test::init();

    const FORGED: u32 = 3;
    const SLOW: u32 = 5;

    // The range ends at a checkpoint below the final checkpoint, so no handoff clears the queue.
    let fixture = PoisonFixture::new(30, &[10, 20, 30])?;
    let state = fixture.state().await?;
    let gate = CommitGate::default();
    gate.hold([fixture.hash(SLOW)], fixture.hash(FORGED), false);
    let mut verifier = CheckpointVerifier::from_list(
        fixture.checkpoints.clone(),
        &fixture.network,
        None,
        gated_state(state.clone(), gate.clone()),
    )?;

    let genesis = submit(&mut verifier, fixture.blocks[0].clone()).await;
    timeout(STRANDED_AFTER, genesis).await??;

    let mut first = Vec::new();
    for height in 1..=10 {
        let block = if height == FORGED {
            fixture.forge(FORGED)
        } else {
            fixture.blocks[height as usize].clone()
        };
        first.push((height, spawn_submit(&mut verifier, block).await));
    }
    let (finished, old_stranded) = settle(first).await;
    assert_eq!(
        finished.keys().copied().collect::<Vec<_>>(),
        (1..SLOW).collect::<Vec<_>>()
    );
    assert_eq!(
        old_stranded
            .iter()
            .map(|(height, _)| *height)
            .collect::<Vec<_>>(),
        (SLOW..=10).collect::<Vec<_>>(),
    );

    // Block sync downloads the whole range again. The fresh descendants reach the state after the
    // honest copy of the forged block's child commits.
    gate.hold(
        (SLOW..=10).map(|height| fixture.hash(height)),
        fixture.hash(SLOW - 1),
        true,
    );
    let mut fresh = Vec::new();
    for height in FORGED..=10 {
        fresh.push((
            height,
            spawn_submit(&mut verifier, fixture.blocks[height as usize].clone()).await,
        ));
    }
    let (fresh_finished, fresh_stranded) = settle(fresh).await;
    for height in FORGED..SLOW {
        assert!(
            matches!(&fresh_finished.get(&height), Some(Ok(hash)) if *hash == fixture.hash(height)),
            "fresh height {height} commits: {:?}",
            fresh_finished.get(&height),
        );
    }

    // The old stranded copies commit the range.
    let (old_finished, old_still_stranded) = settle(old_stranded).await;
    assert!(old_still_stranded.is_empty());
    for (height, result) in &old_finished {
        assert!(
            matches!(result, Ok(hash) if *hash == fixture.hash(*height)),
            "old copy of height {height} commits once its parent commits: {result:?}",
        );
    }
    let tip = state.clone().oneshot(zs::Request::Tip).await;
    assert!(
        matches!(tip, Ok(zs::Response::Tip(Some((height, _)))) if height == block::Height(10)),
        "{tip:?}",
    );

    // Every fresh copy of a stranded height is now a never-answered commit.
    assert_eq!(
        fresh_stranded
            .iter()
            .map(|(height, _)| *height)
            .collect::<Vec<_>>(),
        (SLOW..=10).collect::<Vec<_>>(),
    );

    // Later ranges still commit, and the leaked commits stay pending.
    let mut next = Vec::new();
    for height in 11..=20 {
        next.push((
            height,
            spawn_submit(&mut verifier, fixture.blocks[height as usize].clone()).await,
        ));
    }
    let (next_finished, next_pending) = settle(next).await;
    assert!(next_pending.is_empty());
    assert!(
        next_finished.values().all(Result::is_ok),
        "{next_finished:?}"
    );
    let (_, fresh_still_stranded) = settle(fresh_stranded).await;
    assert_eq!(
        fresh_still_stranded.len(),
        (SLOW..=10).count(),
        "the leaked commits never resolve before the checkpoint handoff",
    );

    Ok(())
}

/// Measures native-sync slot leaks without any injected delay.
///
/// Each trial forges height 2 of a 400-block range and, if descendants strand, resubmits the whole
/// range as native block sync does. It reports old stranded copies and fresh copies that never
/// resolve. Run with `--ignored --nocapture`.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn range_resubmission_leak_rate_under_natural_scheduling() -> Result<(), Report> {
    let _init_guard = zakura_test::init();

    const RANGE_END: u32 = 400;
    const FORGED: u32 = 2;
    let trials: usize = std::env::var("POISON_TRIALS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(10);

    let fixture = PoisonFixture::new(2 * RANGE_END, &[RANGE_END, 2 * RANGE_END])?;

    let mut report = Vec::new();
    for _ in 0..trials {
        let state = fixture.state().await?;
        let mut verifier = CheckpointVerifier::from_list(
            fixture.checkpoints.clone(),
            &fixture.network,
            None,
            state.clone(),
        )?;
        let genesis = submit(&mut verifier, fixture.blocks[0].clone()).await;
        timeout(STRANDED_AFTER, genesis).await??;

        let mut first = Vec::new();
        for height in 1..=RANGE_END {
            let block = if height == FORGED {
                fixture.forge(FORGED)
            } else {
                fixture.blocks[height as usize].clone()
            };
            first.push((height, spawn_submit(&mut verifier, block).await));
        }
        let (_, old_stranded) = settle(first).await;
        if old_stranded.is_empty() {
            report.push((0, 0, 0));
            continue;
        }

        let mut fresh = Vec::new();
        for height in FORGED..=RANGE_END {
            fresh.push((
                height,
                spawn_submit(&mut verifier, fixture.blocks[height as usize].clone()).await,
            ));
        }
        let (_, fresh_stranded) = settle(fresh).await;
        let old_stranded_count = old_stranded.len();
        let (_, old_still_stranded) = settle(old_stranded).await;
        let old_count = old_still_stranded.len();
        report.push((old_stranded_count, old_count, fresh_stranded.len()));
    }

    #[allow(clippy::print_stderr)]
    {
        eprintln!(
            "(old copies stranded, old copies still stranded after resubmission, fresh copies leaked) per trial; \
             (0, 0, 0) means nothing stranded: {report:?}"
        );
    }

    Ok(())
}
