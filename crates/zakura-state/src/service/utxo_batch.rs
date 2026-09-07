//! Batched informational UTXO reads and missing-output dependencies.

use std::{collections::HashMap, sync::Arc};

use futures::{future::BoxFuture, stream::FuturesUnordered, FutureExt, StreamExt};
use tokio::sync::Semaphore;
use tracing::Span;

use zakura_chain::{diagnostic::CodeTimer, transparent};

use crate::{
    constants::{MAX_CONCURRENT_UTXO_BATCH_READS, MAX_UTXO_BATCH_SIZE},
    request::TimedSpan,
    BoxError, Response,
};

use super::StateService;

/// Owns the blocking-read lifetime, including after the request is cancelled.
#[derive(Clone, Debug)]
pub(super) struct UtxoReadBudget(Arc<Semaphore>);

impl Default for UtxoReadBudget {
    fn default() -> Self {
        Self(Arc::new(Semaphore::new(MAX_CONCURRENT_UTXO_BATCH_READS)))
    }
}

impl UtxoReadBudget {
    async fn run<T: Send + 'static>(
        &self,
        read: impl FnOnce() -> Result<T, BoxError> + Send + 'static,
    ) -> Result<T, BoxError> {
        let permit = self
            .0
            .clone()
            .acquire_owned()
            .await
            .expect("the UTXO read semaphore is never closed");
        let timed_span = TimedSpan::new(CodeTimer::start_desc("utxo_batch"), Span::current());
        timed_span
            .spawn_blocking(move || {
                // Dropping the request cannot release a permit held by a running read.
                let _permit = permit;
                read()
            })
            .await
    }
}

impl StateService {
    pub(super) fn await_utxos(
        &mut self,
        outpoints: Vec<transparent::OutPoint>,
    ) -> BoxFuture<'static, Result<Response, BoxError>> {
        if outpoints.len() > MAX_UTXO_BATCH_SIZE {
            return async { Err("UTXO batch exceeds MAX_UTXO_BATCH_SIZE".into()) }.boxed();
        }

        // Subscribe before reading either queued blocks or committed state. An
        // output arriving during the read must still satisfy its dependency.
        let mut pending = HashMap::new();
        for outpoint in outpoints {
            pending
                .entry(outpoint)
                .or_insert_with(|| self.pending_utxos.queue(outpoint));
        }
        let mut available = HashMap::new();
        pending.retain(|outpoint, _| {
            let utxo = self
                .non_finalized_state_queued_blocks
                .utxo(outpoint)
                .or_else(|| self.non_finalized_block_write_sent_hashes.utxo(outpoint));
            if let Some(utxo) = utxo {
                self.pending_utxos.respond(outpoint, utxo.clone());
                available.insert(*outpoint, utxo);
                false
            } else {
                true
            }
        });

        let state = self.read_service.clone();
        let mut changes = state.non_finalized_state_receiver.clone();
        changes.mark_as_seen();
        let mut notifications = self.pending_utxos.clone();
        async move {
            let mut missing: std::collections::HashSet<_> = pending.keys().copied().collect();
            let mut waits = FuturesUnordered::new();
            for (outpoint, response) in pending {
                waits.push(async move { (outpoint, response.await) });
            }
            while !missing.is_empty() {
                let outpoints: Vec<_> = missing.iter().copied().collect();
                let read_state = state.clone();
                let budget = state.utxo_read_budget.clone();
                let read = budget.run(move || {
                    let chains = read_state.latest_non_finalized_state();
                    let mut found = HashMap::new();
                    let mut on_disk = Vec::new();
                    for outpoint in outpoints {
                        if let Some(utxo) = chains.any_utxo(&outpoint) {
                            found.insert(outpoint, utxo);
                        } else {
                            on_disk.push(outpoint);
                        }
                    }
                    found.extend(read_state.db.utxos(&on_disk));
                    Ok(found)
                });
                tokio::pin!(read);
                let mut read_finished = false;
                loop {
                    tokio::select! {
                        biased;
                        Some((outpoint, response)) = waits.next() => {
                            let Response::Utxo(utxo) = response? else {
                                unreachable!("pending UTXOs return Utxo responses")
                            };
                            missing.remove(&outpoint);
                            available.insert(outpoint, utxo);
                            if missing.is_empty() {
                                return Ok(Response::Utxos(available));
                            }
                        }
                        found = &mut read, if !read_finished => {
                            read_finished = true;
                            for (outpoint, utxo) in found? {
                                notifications.respond(&outpoint, utxo);
                            }
                        }
                        changed = changes.changed(), if read_finished => {
                            changed?;
                            break;
                        }
                    }
                }
            }
            Ok(Response::Utxos(available))
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use zakura_chain::{
        block::{Block, Height},
        parameters::Network,
        serialization::ZcashDeserializeInto,
    };

    use super::*;
    use crate::Config;

    #[tokio::test]
    async fn read_budget_survives_request_cancellation_and_is_shared() {
        let budget = UtxoReadBudget(Arc::new(Semaphore::new(1)));
        let shared_budget = budget.clone();
        let (started, start) = tokio::sync::oneshot::channel();
        let (release, released) = std::sync::mpsc::channel();
        let mut first = Box::pin(budget.run(move || {
            started.send(()).unwrap();
            released.recv_timeout(Duration::from_secs(30)).unwrap();
            Ok(())
        }));
        assert!(futures::poll!(&mut first).is_pending());
        tokio::time::timeout(Duration::from_secs(30), start)
            .await
            .unwrap()
            .unwrap();
        drop(first);
        assert_eq!(budget.0.available_permits(), 0);

        let mut second = Box::pin(shared_budget.run(|| Ok(())));
        assert!(futures::poll!(&mut second).is_pending());
        release.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(30), second)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(budget.0.available_permits(), 1);
    }

    #[tokio::test]
    async fn notifications_during_read_admission_are_not_lost() {
        let _init_guard = zakura_test::init();
        let (mut state, _, _, _) =
            StateService::new(Config::ephemeral(), &Network::Mainnet, Height::MAX, 0)
                .await
                .unwrap();
        let block: Block = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
            .zcash_deserialize_into()
            .unwrap();
        let expected: HashMap<_, _> = block.transactions[0]
            .outputs()
            .iter()
            .enumerate()
            .map(|(index, output)| {
                (
                    transparent::OutPoint {
                        hash: block.transactions[0].hash(),
                        index: index.try_into().unwrap(),
                    },
                    transparent::Utxo::new(output.clone(), Height(1), true),
                )
            })
            .collect();
        let budget = state.read_service.utxo_read_budget.clone();
        let permits = budget
            .0
            .clone()
            .acquire_many_owned(MAX_CONCURRENT_UTXO_BATCH_READS.try_into().unwrap())
            .await
            .unwrap();
        let mut lookup = state.await_utxos(expected.keys().copied().collect());
        assert!(futures::poll!(&mut lookup).is_pending());
        for (outpoint, utxo) in &expected {
            state.pending_utxos.respond(outpoint, utxo.clone());
        }
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(30), lookup)
                .await
                .unwrap()
                .unwrap(),
            Response::Utxos(expected)
        );
        drop(permits);
    }

    #[tokio::test]
    async fn missing_outputs_release_reads_and_cancel_dependencies() {
        let _init_guard = zakura_test::init();
        let (mut state, _, _, _) =
            StateService::new(Config::ephemeral(), &Network::Mainnet, Height::MAX, 0)
                .await
                .unwrap();
        let outpoint = transparent::OutPoint {
            hash: [42; 32].into(),
            index: 0,
        };
        let budget = state.read_service.utxo_read_budget.clone();
        let mut lookup = state.await_utxos(vec![outpoint, outpoint]);
        assert_eq!(
            state.pending_utxos.len(),
            1,
            "duplicate dependencies must share one subscription"
        );
        assert!(futures::poll!(&mut lookup).is_pending());
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                assert!(futures::poll!(&mut lookup).is_pending());
                if budget.0.available_permits() == MAX_CONCURRENT_UTXO_BATCH_READS {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        // An independent read can complete while this batch still waits for an output.
        tokio::time::timeout(Duration::from_secs(30), budget.run(|| Ok(())))
            .await
            .unwrap()
            .unwrap();
        drop(lookup);
        state.pending_utxos.prune();
        assert_eq!(state.pending_utxos.len(), 0);

        assert!(state
            .await_utxos(vec![outpoint; MAX_UTXO_BATCH_SIZE + 1])
            .await
            .is_err());
        assert_eq!(
            state.pending_utxos.len(),
            0,
            "reject oversized batches before subscribing"
        );
        let _permits = budget
            .0
            .clone()
            .acquire_many_owned(MAX_CONCURRENT_UTXO_BATCH_READS.try_into().unwrap())
            .await
            .unwrap();
        assert_eq!(
            state.await_utxos(Vec::new()).await.unwrap(),
            Response::Utxos(HashMap::new())
        );
    }

    #[tokio::test]
    async fn committed_outputs_wake_waiters_and_batch_hits_broadcast() {
        use crate::CheckpointVerifiedBlock;

        let _init_guard = zakura_test::init();
        let (mut state, _, _, _) =
            StateService::new(Config::ephemeral(), &Network::Mainnet, Height::MAX, 0)
                .await
                .unwrap();
        let block: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
            .zcash_deserialize_into()
            .unwrap();
        let outpoint = transparent::OutPoint::from_usize(block.transactions[0].hash(), 0);
        let expected =
            transparent::Utxo::new(block.transactions[0].outputs()[0].clone(), Height(1), true);
        // Admission happened before this request subscribed.
        state.pending_utxos.respond(&outpoint, expected.clone());
        let mut lookup = state.await_utxos(vec![outpoint]);
        let budget = state.read_service.utxo_read_budget.clone();
        assert!(futures::poll!(&mut lookup).is_pending());
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                assert!(futures::poll!(&mut lookup).is_pending());
                if budget.0.available_permits() == MAX_CONCURRENT_UTXO_BATCH_READS {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        for (_, bytes) in zakura_test::vectors::MAINNET_BLOCKS.range(0..=1) {
            let block: Arc<Block> = bytes.zcash_deserialize_into().unwrap();
            state
                .queue_and_commit_to_finalized_state(CheckpointVerifiedBlock::from(block))
                .await
                .unwrap()
                .unwrap();
        }
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(30), lookup)
                .await
                .unwrap()
                .unwrap(),
            Response::Utxos(HashMap::from([(outpoint, expected.clone())]))
        );
        assert_eq!(state.pending_utxos.len(), 0);

        let earlier = state.pending_utxos.queue(outpoint);
        state.await_utxos(vec![outpoint]).await.unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(30), earlier)
                .await
                .unwrap()
                .unwrap(),
            Response::Utxo(expected)
        );
        assert_eq!(state.pending_utxos.len(), 0);
    }

    #[tokio::test]
    async fn reconsidered_outputs_wake_existing_waiters() {
        use crate::{arbitrary::Prepare, CheckpointVerifiedBlock};

        let _init_guard = zakura_test::init();
        tokio::time::timeout(Duration::from_secs(30), async {
            use zakura_chain::{
                parameters::testnet::{
                    ConfiguredActivationHeights, ConfiguredCheckpoints, Parameters,
                },
                transaction::Transaction,
            };
            let genesis: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES
                .zcash_deserialize_into()
                .unwrap();
            let network = Parameters::build()
                .with_genesis_hash(genesis.hash())
                .unwrap()
                .with_checkpoints(ConfiguredCheckpoints::HeightsAndHashes(
                    [(Height(0), genesis.hash())].into_iter().collect(),
                ))
                .unwrap()
                .with_activation_heights(ConfiguredActivationHeights {
                    canopy: Some(1),
                    ..Default::default()
                })
                .unwrap()
                .with_disable_pow(true)
                .clear_funding_streams()
                .to_network()
                .unwrap();
            let (mut state, _, _, _) =
                StateService::new(Config::ephemeral(), &network, Height(0), 0)
                    .await
                    .unwrap();
            state
                .queue_and_commit_to_finalized_state(CheckpointVerifiedBlock::from(genesis))
                .await
                .unwrap()
                .unwrap();
            let block: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
                .zcash_deserialize_into()
                .unwrap();
            let mut block = (*block).clone();
            let original = &block.transactions[0];
            block.transactions[0] = Arc::new(Transaction::V4 {
                inputs: original.inputs().to_vec(),
                outputs: original.outputs().to_vec(),
                lock_time: zakura_chain::transaction::LockTime::unlocked(),
                expiry_height: Height(0),
                joinsplit_data: None,
                sapling_shielded_data: None,
            });
            Arc::make_mut(&mut block.header).merkle_root = block.transactions.iter().collect();
            let block = Arc::new(block);
            state
                .queue_and_commit_to_non_finalized_state(block.clone().prepare())
                .await
                .unwrap()
                .unwrap();
            state
                .send_invalidate_block(block.hash())
                .await
                .unwrap()
                .unwrap();
            state
                .non_finalized_block_write_sent_hashes
                .remove(&block.hash());
            let outpoint = transparent::OutPoint::from_usize(block.transactions[0].hash(), 0);
            let mut lookup = state.await_utxos(vec![outpoint]);
            assert!(futures::poll!(&mut lookup).is_pending());
            loop {
                assert!(futures::poll!(&mut lookup).is_pending());
                if state.read_service.utxo_read_budget.0.available_permits()
                    == MAX_CONCURRENT_UTXO_BATCH_READS
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
            state
                .send_reconsider_block(block.hash())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                lookup.await.unwrap(),
                Response::Utxos(HashMap::from([(
                    outpoint,
                    transparent::Utxo::new(
                        block.transactions[0].outputs()[0].clone(),
                        Height(1),
                        true
                    ),
                )]))
            );
            assert_eq!(state.pending_utxos.len(), 0);
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn finalized_batches_match_serial_lookups_and_queued_outputs_skip_reads() {
        use crate::{arbitrary::Prepare, CheckpointVerifiedBlock};

        let _init_guard = zakura_test::init();
        let (mut state, _, _, _) =
            StateService::new(Config::ephemeral(), &Network::Mainnet, Height::MAX, 0)
                .await
                .unwrap();
        let mut outpoints = Vec::new();
        for (_, bytes) in zakura_test::vectors::MAINNET_BLOCKS.range(0..=10) {
            let block: Arc<Block> = bytes.zcash_deserialize_into().unwrap();
            for tx in &block.transactions {
                outpoints.extend(
                    tx.outputs()
                        .iter()
                        .enumerate()
                        .map(|(index, _)| transparent::OutPoint::from_usize(tx.hash(), index)),
                );
            }
            state
                .queue_and_commit_to_finalized_state(CheckpointVerifiedBlock::from(block))
                .await
                .unwrap()
                .unwrap();
        }
        outpoints.extend_from_within(..);
        outpoints.push(transparent::OutPoint {
            hash: [43; 32].into(),
            index: 0,
        });
        outpoints.push(transparent::OutPoint {
            hash: outpoints[1].hash,
            index: 100,
        });
        let expected: HashMap<_, _> = outpoints
            .iter()
            .filter_map(|outpoint| {
                state
                    .read_service
                    .db
                    .utxo(outpoint)
                    .map(|utxo| (*outpoint, utxo.utxo))
            })
            .collect();
        assert!(!expected.is_empty());
        assert_eq!(state.read_service.db.utxos(&outpoints), expected);

        let queued: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_419201_BYTES
            .zcash_deserialize_into()
            .unwrap();
        let queued = queued.prepare();
        let expected: HashMap<_, _> = queued
            .new_outputs
            .iter()
            .take(MAX_UTXO_BATCH_SIZE)
            .map(|(outpoint, utxo)| (*outpoint, utxo.utxo.clone()))
            .collect();
        let (send, _receive) = tokio::sync::oneshot::channel();
        state
            .non_finalized_state_queued_blocks
            .queue((queued, send));
        let budget = state.read_service.utxo_read_budget.clone();
        let _permits = budget
            .0
            .clone()
            .acquire_many_owned(MAX_CONCURRENT_UTXO_BATCH_READS.try_into().unwrap())
            .await
            .unwrap();
        assert_eq!(
            tokio::time::timeout(
                Duration::from_secs(30),
                state.await_utxos(expected.keys().copied().collect())
            )
            .await
            .unwrap()
            .unwrap(),
            Response::Utxos(expected)
        );
    }
}
