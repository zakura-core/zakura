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
        async move {
            if !pending.is_empty() {
                let outpoints: Vec<_> = pending.keys().copied().collect();
                let budget = state.utxo_read_budget.clone();
                let found = budget
                    .run(move || {
                        let chains = state.latest_non_finalized_state();
                        let mut found = HashMap::new();
                        let mut on_disk = Vec::new();
                        for outpoint in outpoints {
                            if let Some(utxo) = chains.any_utxo(&outpoint) {
                                found.insert(outpoint, utxo);
                            } else {
                                on_disk.push(outpoint);
                            }
                        }
                        found.extend(state.db.utxos(&on_disk));
                        Ok(found)
                    })
                    .await?;
                available.extend(found);
            }

            // Reads have finished and released their permits. Missing dependencies
            // wait only on notifications from the state writer.
            let mut waits = FuturesUnordered::new();
            for (outpoint, response) in pending {
                if !available.contains_key(&outpoint) {
                    waits.push(async move { (outpoint, response.await) });
                }
            }
            while let Some((outpoint, response)) = waits.next().await {
                let Response::Utxo(utxo) = response? else {
                    unreachable!("pending UTXOs return Utxo responses")
                };
                available.insert(outpoint, utxo);
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
        drop(permits);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(30), lookup)
                .await
                .unwrap()
                .unwrap(),
            Response::Utxos(expected)
        );
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
