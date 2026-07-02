//! Synthetic-peer lifecycle + serve loop for the block-sync fuzzer.
//!
//! Each peer connects at its scheduled time through the real `add_peer` path, serves
//! the node's `GetBlocks` under its [`ServeProfile`], and optionally disconnects (peer
//! churn). The node's real `PeerRoutine` drives the request side; this only models the
//! far end of the wire.

use std::{future, sync::Arc, time::Duration};

use rand::Rng;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use zebra_chain::block;

use super::scenario::{peer_rng, PeerSpec};
use super::sleep_or_cancel;
use crate::zakura::testkit::mock_blocksync::SyntheticBlockCorpus;
use crate::zakura::testkit::{SyntheticBlockSyncPeer, SyntheticBlockSyncPeers};
use crate::zakura::BlockSyncMessage;

/// Spawn a peer's full lifecycle: wait `connect_at`, attach through the real
/// `add_peer`, serve until `disconnect_at`/shutdown, then disconnect.
pub(crate) fn spawn_peer_lifecycle(
    peers: Arc<SyntheticBlockSyncPeers>,
    corpus: SyntheticBlockCorpus,
    spec: PeerSpec,
    scenario_seed: u64,
    tip_hash: block::Hash,
    shutdown: CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        if sleep_or_cancel(&shutdown, spec.connect_at).await {
            return;
        }
        let mut peer = match peers.add_peer(spec.peer_id(), spec.status(tip_hash)).await {
            Ok(peer) => peer,
            Err(error) => {
                tracing::warn!(?error, id = spec.id_byte, "fuzz peer failed to attach");
                return;
            }
        };

        let disconnect_after = spec
            .disconnect_at
            .map(|at| at.saturating_sub(spec.connect_at));
        serve_loop(
            &mut peer,
            &corpus,
            &spec,
            scenario_seed,
            &shutdown,
            disconnect_after,
        )
        .await;
        peer.cancel();
    })
}

/// Serve the node's `GetBlocks` requests until shutdown or the optional disconnect
/// deadline (relative to this peer's connect time).
async fn serve_loop(
    peer: &mut SyntheticBlockSyncPeer,
    corpus: &SyntheticBlockCorpus,
    spec: &PeerSpec,
    scenario_seed: u64,
    shutdown: &CancellationToken,
    disconnect_after: Option<Duration>,
) {
    let mut rng = peer_rng(scenario_seed, spec);
    let mut responses: u64 = 0;

    let disconnect = async {
        match disconnect_after {
            Some(duration) => tokio::time::sleep(duration).await,
            None => future::pending::<()>().await,
        }
    };
    tokio::pin!(disconnect);

    loop {
        let message = tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = &mut disconnect => return,
            message = peer.recv() => message,
        };
        let message = match message {
            Ok(Some(message)) => message,
            Ok(None) | Err(_) => return,
        };
        let BlockSyncMessage::GetBlocks {
            start_height,
            count,
        } = message
        else {
            continue;
        };
        responses = responses.saturating_add(1);

        // Silent drop: no response at all, exercising the node's request-timeout path.
        let drop_p = spec.serve.drop_probability.clamp(0.0, 1.0);
        if drop_p > 0.0 && rng.gen_bool(drop_p) {
            continue;
        }

        // Withheld range: this peer is missing it.
        if let Some((low, high)) = spec.serve.withhold {
            if start_height >= low && start_height <= high {
                let _ = peer
                    .send(BlockSyncMessage::RangeUnavailable {
                        start_height,
                        count,
                    })
                    .await;
                continue;
            }
        }

        // Periodic stall.
        if let Some(gap) = spec.serve.idle_gap {
            if gap.every_responses > 0
                && responses.is_multiple_of(gap.every_responses)
                && !gap.duration.is_zero()
                && sleep_or_cancel(shutdown, gap.duration).await
            {
                return;
            }
        }

        let mut blocks = corpus.blocks_in_range(start_height, count, spec.servable_high);
        if blocks.is_empty() {
            let _ = peer
                .send(BlockSyncMessage::RangeUnavailable {
                    start_height,
                    count,
                })
                .await;
            continue;
        }
        if spec.serve.reorder {
            blocks.reverse();
        }

        if !spec.serve.first_block_is_zero() {
            let delay = spec.serve.first_block_latency.sample(&mut rng);
            if sleep_or_cancel(shutdown, delay).await {
                return;
            }
        }

        let mut returned = 0u32;
        for (_, block, block_bytes) in &blocks {
            // Byte-accurate serve when a bandwidth is set: the block's transmission time
            // is `bytes / bandwidth`. Otherwise fall back to the fixed per-block latency.
            let per_block_delay = match spec.serve.bandwidth_bytes_per_sec {
                Some(bandwidth) => Duration::from_secs_f64(*block_bytes as f64 / bandwidth as f64),
                None if !spec.serve.per_block_is_zero() => {
                    spec.serve.per_block_latency.sample(&mut rng)
                }
                None => Duration::ZERO,
            };
            if !per_block_delay.is_zero() && sleep_or_cancel(shutdown, per_block_delay).await {
                return;
            }
            if peer
                .send(BlockSyncMessage::Block(block.clone()))
                .await
                .is_err()
            {
                return;
            }
            returned = returned.saturating_add(1);
        }
        let _ = peer
            .send(BlockSyncMessage::BlocksDone {
                start_height,
                returned,
            })
            .await;
    }
}
