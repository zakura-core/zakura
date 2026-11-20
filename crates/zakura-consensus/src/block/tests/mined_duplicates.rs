//! Equal-work mining priority when a peer and a miner submit the same block.

use super::*;

/// Changes the authorizing bytes without changing the header or coinbase height.
fn mined_body_variant(block: &Arc<Block>, tag: u8) -> Arc<Block> {
    let mut variant = block.as_ref().clone();
    let coinbase = Arc::make_mut(&mut variant.transactions[0]);
    let zakura_chain::transparent::Input::Coinbase { data, .. } = &mut coinbase.inputs_mut()[0]
    else {
        panic!("the fixture starts with a coinbase input");
    };
    data.push(tag);
    Arc::new(variant)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_mined_duplicate_keeps_priority_only_for_an_overlapping_write_of_its_body() {
    let _init_guard = zakura_test::init();
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        #[derive(Copy, Clone, Debug, PartialEq, Eq)]
        enum Delivery {
            PeerFirst,
            LocalFirst,
            ReceiptTableFull,
            DifferentLocalBody,
            AlreadyCommitted,
        }
        use Delivery::*;
        for delivery in [
            PeerFirst,
            LocalFirst,
            ReceiptTableFull,
            DifferentLocalBody,
            AlreadyCommitted,
        ] {
            let mut tips = EqualWorkTips::new().await;
            let (rival, mined) = (tips.a.clone(), tips.b.clone());
            tips.admit_header(&rival);
            let mut other_deliveries = Vec::new();
            if delivery == ReceiptTableFull {
                for tag in 0..4 {
                    let body = mined_body_variant(&mined, tag);
                    other_deliveries.push(tips.verifier.call(Request::Commit(body.into())));
                }
            }
            let local_body = if delivery == DifferentLocalBody {
                mined_body_variant(&mined, 10)
            } else {
                mined.clone()
            };
            let local_request = Request::CommitMined {
                size: local_body.zcash_serialized_size(),
                block: local_body.into(),
                admission: zs::BlockAdmission::pending(),
            };
            let peer_request = Request::Commit(mined.clone().into());
            let local = if delivery == AlreadyCommitted {
                tips.verifier.call(peer_request).await.unwrap();
                tips.verifier.call(local_request)
            } else {
                let (peer, local) = if delivery != LocalFirst {
                    (
                        tips.verifier.call(peer_request),
                        tips.verifier.call(local_request),
                    )
                } else {
                    let local = tips.verifier.call(local_request);
                    (tips.verifier.call(peer_request), local)
                };
                peer.await.unwrap();
                local
            };
            assert!(local.await.unwrap_err().is_duplicate_request());
            assert_eq!(tips.tip_hash(), Some(mined.hash()));
            tips.verifier
                .call(Request::Commit(rival.clone().into()))
                .await
                .unwrap();
            let expected = if matches!(delivery, AlreadyCommitted | DifferentLocalBody) {
                &rival
            } else {
                &mined
            };
            assert_eq!(tips.tip_hash(), Some(expected.hash()), "{delivery:?}");
        }
    })
    .await
    .expect("duplicate submissions must finish");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cancelled_peer_write_preserves_priority_only_if_the_mined_submission_observes_commit() {
    let _init_guard = zakura_test::init();
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        for cancel_local in [false, true] {
            let mut tips = EqualWorkTips::new().await;
            let (rival, mined) = (tips.a.clone(), tips.b.clone());
            let hash = mined.hash();
            let writer = tips.verifier.state_service.clone();
            let state = writer.clone();
            let (writes, mut queued) = tokio::sync::mpsc::unbounded_channel();
            let confirming = Arc::new(tokio::sync::Notify::new());
            let confirmation = confirming.clone();
            tips.verifier.state_service =
                crate::transaction::boxed_state(service_fn(move |request| {
                    let (state, writes, confirmation) =
                        (state.clone(), writes.clone(), confirmation.clone());
                    async move {
                        match request {
                            zs::Request::CommitSemanticallyVerifiedBlock(block)
                                if block.hash == hash =>
                            {
                                writes.send(block).unwrap();
                                std::future::pending().await
                            }
                            zs::Request::CommitSemanticallyVerifiedBlockWithAdmission {
                                block,
                                ..
                            } if block.hash == hash => Err(zs::CommitBlockError::Duplicate {
                                hash_or_height: Some(hash.into()),
                                location: zs::KnownBlock::WriteChannel,
                            }
                            .into()),
                            other @ zs::Request::AwaitBlockInfo(h) if h == hash => {
                                confirmation.notify_one();
                                state.oneshot(other).await
                            }
                            other => state.oneshot(other).await,
                        }
                    }
                }));
            tips.admit_header(&rival);

            let peer = tokio::spawn(tips.verifier.call(Request::Commit(mined.clone().into())));
            let pending_write = queued.recv().await.unwrap();
            let local = tips.verifier.call(Request::CommitMined {
                size: mined.zcash_serialized_size(),
                block: mined.clone().into(),
                admission: zs::BlockAdmission::pending(),
            });
            peer.abort();
            assert!(peer.await.unwrap_err().is_cancelled());

            // State owns the peer's write. Hold it until the duplicate is waiting for proof.
            let local = tokio::spawn(local);
            confirming.notified().await;
            if cancel_local {
                local.abort();
            }
            let committed = writer
                .oneshot(zs::Request::CommitSemanticallyVerifiedBlock(pending_write))
                .await
                .unwrap();
            assert!(matches!(committed, zs::Response::Committed(h) if h == hash));
            if cancel_local {
                assert!(local.await.unwrap_err().is_cancelled());
            } else {
                assert!(local.await.unwrap().unwrap_err().is_duplicate_request());
            }
            tips.verifier
                .call(Request::Commit(rival.clone().into()))
                .await
                .unwrap();
            let expected = if cancel_local { rival.hash() } else { hash };
            assert_eq!(tips.tip_hash(), Some(expected));
        }
    })
    .await
    .expect("the writer and local confirmation must finish");
}

#[tokio::test(start_paused = true)]
async fn duplicate_confirmation_requires_a_successful_write_of_the_exact_body_before_timeout() {
    let b: Arc<Block> = zakura_test::vectors::BLOCK_MAINNET_1_BYTES
        .zcash_deserialize_into()
        .unwrap();
    let submitted = BlockHandle::from(b.clone());
    let hash = b.hash();
    let other = BlockHandle::from(mined_body_variant(&b, 1));

    let wrong_body = service_fn(move |request| {
        let other = other.clone();
        async move {
            Ok::<_, BoxError>(match request {
                zs::Request::AwaitBlockInfo(_) => zs::Response::BlockInfo(Some(Default::default())),
                zs::Request::AnyChainBlock(_) => zs::Response::Block(Some(other)),
                _ => panic!("unexpected confirmation request"),
            })
        }
    });
    assert!(!committed_mined_body(wrong_body, hash, submitted.clone()).await);

    let rejected = service_fn(|_| async { Err::<zs::Response, BoxError>("write rejected".into()) });
    assert!(!committed_mined_body(rejected, hash, submitted.clone()).await);
    let stalled = service_fn(|_| std::future::pending::<Result<zs::Response, BoxError>>());
    assert!(!committed_mined_body(stalled, hash, submitted).await);
}
