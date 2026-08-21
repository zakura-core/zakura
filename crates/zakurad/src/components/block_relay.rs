//! Shared peer block relay lifecycle.

use std::{sync::Arc, time::Duration};

use tokio::sync::{mpsc, oneshot};
use tower::{Service, ServiceExt};

use zakura_chain::block;
use zakura_network::PeerSource;
use zakura_rpc::{BlockRelayEvent, BlockRelaySource, PendingBlockRegistry};

/// Node-local services used to retain and advertise a relay-authorized peer block.
#[derive(Clone, Debug)]
pub(crate) struct PeerRelayContext {
    pub(crate) pending_blocks: PendingBlockRegistry,
    pub(crate) sender: mpsc::Sender<BlockRelayEvent>,
}

/// Verify a peer block and advertise it after semantic relay authorization.
///
/// Once consensus authorizes relay, this function transfers verification to an
/// owned task. Cancellation of the original inbound request cannot abandon the
/// contextual commit or its pending-body claim.
pub(crate) async fn verify_peer_block<V>(
    verifier: V,
    block: Arc<block::Block>,
    advertiser: Option<PeerSource>,
    relay: PeerRelayContext,
) -> Result<block::Hash, V::Error>
where
    V: Service<zakura_consensus::Request, Response = block::Hash> + Send + 'static,
    V::Error: Send + 'static,
    V::Future: Send + 'static,
{
    let hash = block.hash();
    let height = block
        .coinbase_height()
        .expect("semantic relay candidates have a coinbase height");
    let (lifecycle_handle, lifecycle) = zakura_state::BlockLifecycleHandle::new();
    let mut verification = Box::pin(verifier.oneshot(
        zakura_consensus::Request::CommitWithLifecycle {
            block: block.clone(),
            lifecycle,
        },
    ));

    enum AuthorizationRace<T, E> {
        Authorized(Result<(), zakura_state::BlockLifecycleResult>),
        Verification(Result<T, E>),
    }

    let race = tokio::select! {
        biased;
        authorized = lifecycle_handle.wait_for(
            zakura_state::BlockLifecycleMilestone::RelayAuthorized,
        ) => AuthorizationRace::Authorized(authorized),
        result = &mut verification => AuthorizationRace::Verification(result),
    };

    match race {
        AuthorizationRace::Verification(result) => result,
        AuthorizationRace::Authorized(authorized) => {
            let source = BlockRelaySource::Peer {
                authorized_at: std::time::Instant::now(),
                advertiser,
            };
            let mut early_result = None;
            let mut pending_claim = None;
            if authorized.is_ok() {
                if let Some(admission) = relay.pending_blocks.admit(block) {
                    if admission.relay_reserved {
                        let (advertised, receiver) = oneshot::channel();
                        let event = BlockRelayEvent::Early {
                            hash,
                            height,
                            source: source.clone(),
                            advertised,
                        };
                        if relay.sender.try_send(event).is_ok() {
                            early_result = Some(receiver);
                        } else {
                            admission.claim.cancel_relay_reservation();
                        }
                    }
                    pending_claim = Some(admission.claim);
                }
            }

            tokio::spawn(async move {
                let result = verification.await;
                let committed = result.is_ok();
                if let Some(claim) = pending_claim {
                    claim.settle(committed);
                }

                tokio::spawn(async move {
                    let early_advertised = match early_result {
                        Some(receiver) => tokio::time::timeout(Duration::from_secs(20), receiver)
                            .await
                            .ok()
                            .and_then(Result::ok)
                            .unwrap_or(false),
                        None => false,
                    };
                    let event = if committed {
                        BlockRelayEvent::Committed {
                            hash,
                            height,
                            early_advertised,
                            source,
                        }
                    } else {
                        BlockRelayEvent::Failed {
                            hash,
                            height,
                            early_advertised,
                            source,
                        }
                    };
                    let _ = relay.sender.send(event).await;
                });

                result
            })
            .await
            .expect("owned peer block verification task must not panic")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::Notify;
    use tower::service_fn;
    use zakura_chain::{block::Block, serialization::ZcashDeserializeInto};

    fn test_block() -> Arc<Block> {
        zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES
            .zcash_deserialize_into()
            .expect("the genesis test vector is valid")
    }

    fn peer() -> PeerSource {
        PeerSource::Zakura(
            zakura_network::zakura::ZakuraPeerId::new(vec![7; 32]).expect("test peer id is valid"),
        )
    }

    fn verifier(
        hash: block::Hash,
        finish: Arc<Notify>,
    ) -> tower::util::BoxCloneService<
        zakura_consensus::Request,
        block::Hash,
        zakura_consensus::BoxError,
    > {
        tower::util::BoxCloneService::new(service_fn(move |request| {
            let finish = finish.clone();
            async move {
                let zakura_consensus::Request::CommitWithLifecycle { lifecycle, .. } = request
                else {
                    panic!("peer relay must use a lifecycle commit")
                };
                lifecycle.reach(zakura_state::BlockLifecycleMilestone::RelayAuthorized);
                finish.notified().await;
                Ok(hash)
            }
        }))
    }

    #[tokio::test]
    async fn authorized_relay_owns_verification_after_caller_cancellation() {
        let block = test_block();
        let hash = block.hash();
        let registry = PendingBlockRegistry::default();
        let finish = Arc::new(Notify::new());
        let (sender, mut receiver) = mpsc::channel(4);
        let task = tokio::spawn(verify_peer_block(
            verifier(hash, finish.clone()),
            block.clone(),
            Some(peer()),
            PeerRelayContext {
                pending_blocks: registry.clone(),
                sender,
            },
        ));

        let BlockRelayEvent::Early { advertised, .. } = receiver
            .recv()
            .await
            .expect("semantic authorization emits early relay")
        else {
            panic!("expected early relay event")
        };
        assert_eq!(registry.get(hash), Some(block));
        let _ = advertised.send(true);
        task.abort();
        finish.notify_one();

        assert!(matches!(
            receiver.recv().await,
            Some(BlockRelayEvent::Committed {
                hash: committed_hash,
                early_advertised: true,
                ..
            }) if committed_hash == hash
        ));
        assert_eq!(registry.get(hash), None);
    }

    #[tokio::test]
    async fn rejected_early_enqueue_releases_reservation_and_falls_back_after_commit() {
        let block = test_block();
        let hash = block.hash();
        let height = block.coinbase_height().expect("test block has a height");
        let registry = PendingBlockRegistry::default();
        let finish = Arc::new(Notify::new());
        let (sender, mut receiver) = mpsc::channel(1);
        sender
            .try_send(BlockRelayEvent::Committed {
                hash: block::Hash([0x44; 32]),
                height,
                early_advertised: false,
                source: BlockRelaySource::Peer {
                    authorized_at: std::time::Instant::now(),
                    advertiser: None,
                },
            })
            .expect("dummy event fills the gossip queue");
        let task = tokio::spawn(verify_peer_block(
            verifier(hash, finish.clone()),
            block.clone(),
            Some(peer()),
            PeerRelayContext {
                pending_blocks: registry.clone(),
                sender,
            },
        ));

        while registry.get(hash).is_none() {
            tokio::task::yield_now().await;
        }
        let replacement = registry
            .admit(block)
            .expect("a duplicate can claim the retained body");
        assert!(
            replacement.relay_reserved,
            "queue rejection releases only the relay reservation"
        );
        replacement.claim.cancel_relay_reservation();
        drop(replacement);
        finish.notify_one();
        let _dummy = receiver.recv().await.expect("dummy event remains queued");

        assert!(matches!(
            receiver.recv().await,
            Some(BlockRelayEvent::Committed {
                hash: committed_hash,
                early_advertised: false,
                ..
            }) if committed_hash == hash
        ));
        assert!(matches!(
            task.await.expect("helper task does not panic"),
            Ok(committed_hash) if committed_hash == hash
        ));
    }

    #[tokio::test]
    async fn gossip_pressure_preserves_consensus_invalid_errors() {
        let block = test_block();
        let hash = block.hash();
        let height = block.coinbase_height().expect("test block has a height");
        let registry = PendingBlockRegistry::default();
        let finish = Arc::new(Notify::new());
        let verifier = service_fn({
            let finish = finish.clone();
            move |request| {
                let finish = finish.clone();
                async move {
                    let zakura_consensus::Request::CommitWithLifecycle { lifecycle, .. } = request
                    else {
                        panic!("peer relay must use a lifecycle commit")
                    };
                    lifecycle.reach(zakura_state::BlockLifecycleMilestone::RelayAuthorized);
                    finish.notified().await;
                    Err::<block::Hash, zakura_consensus::BoxError>(Box::new(
                        zakura_consensus::VerifyBlockError::Block {
                            source: zakura_consensus::BlockError::NoTransactions,
                        },
                    ))
                }
            }
        });
        let (sender, mut receiver) = mpsc::channel(1);
        sender
            .try_send(BlockRelayEvent::Committed {
                hash: block::Hash([0x55; 32]),
                height,
                early_advertised: false,
                source: BlockRelaySource::Peer {
                    authorized_at: std::time::Instant::now(),
                    advertiser: None,
                },
            })
            .expect("dummy event fills the gossip queue");
        let task = tokio::spawn(verify_peer_block(
            verifier,
            block,
            Some(peer()),
            PeerRelayContext {
                pending_blocks: registry.clone(),
                sender,
            },
        ));

        while registry.get(hash).is_none() {
            tokio::task::yield_now().await;
        }
        finish.notify_one();
        let error = task
            .await
            .expect("helper task does not panic")
            .expect_err("the verifier rejects the block");
        let error = error
            .downcast_ref::<zakura_consensus::VerifyBlockError>()
            .expect("the helper returns the original verifier error");
        assert_eq!(
            error.misbehavior_score(),
            zakura_network::constants::MAX_PEER_MISBEHAVIOR_SCORE,
        );

        let _dummy = receiver.recv().await.expect("dummy event remains queued");
        assert!(matches!(
            receiver.recv().await,
            Some(BlockRelayEvent::Failed {
                hash: failed_hash,
                early_advertised: false,
                ..
            }) if failed_hash == hash
        ));
        assert_eq!(registry.get(hash), None);
    }
}
