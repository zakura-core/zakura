/// Download a substituted signature and its honest replacement through QUIC, consensus, and state.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_signature_replacement_preserves_durable_header_selection() {
    use std::num::NonZeroU64;
    use zakura_chain::{
        chain_tip::ChainTip,
        serialization::{ZcashDeserialize, ZcashSerialize},
    };
    use zakura_header_chain::{Frontier, HeaderWorkAuthority, InsertHeaders, TargetCompletion};
    use zakura_network::zakura::{
        testkit::ZakuraTestNode, BlockRangeRead, BlockRangeReadResult, BlockRangeSource,
        FullStateFrontiers, ZAKURA_CAP_BLOCK_SYNC_REGULATED,
    };
    use zakura_state as zs;

    #[derive(Debug)]
    struct BodySource {
        body: Arc<block::Block>,
        calls: Arc<AtomicUsize>,
        release: Option<Arc<tokio::sync::Notify>>,
    }
    impl BlockRangeSource for BodySource {
        /// Keep the production serving lease while a test supplier waits or serializes its body.
        fn read(
            &self,
            request: BlockRangeRead,
        ) -> futures::future::BoxFuture<
            'static,
            Result<BlockRangeReadResult, zakura_network::BoxError>,
        > {
            let body = self.body.clone();
            let calls = self.calls.clone();
            let release = self.release.clone();
            Box::pin(async move {
                let mut blocks = Vec::new();
                if request.start_height == body.coinbase_height().unwrap()
                    && request.lease.try_start()
                {
                    calls.fetch_add(1, Ordering::SeqCst);
                    if let Some(release) = release {
                        release.notified().await;
                    }
                    let bytes = body.zcash_serialize_to_vec()?;
                    assert!(bytes.len() <= usize::try_from(request.max_body_bytes).unwrap());
                    blocks.push((request.start_height, bytes));
                }
                Ok(BlockRangeReadResult {
                    blocks,
                    lease: request.lease,
                })
            })
        }
    }

    const PHASE: &str = "ZAKURA_AUTH_RECOVERY_PHASE";
    const DIRECTORY: &str = "ZAKURA_AUTH_RECOVERY_DIRECTORY";
    let phase = std::env::var(PHASE).ok();
    if phase.is_none() {
        let cache = tempfile::tempdir().unwrap();
        for phase in ["persist", "reopen"] {
            let output = tokio::time::timeout(Duration::from_secs(90),
                tokio::process::Command::new(std::env::current_exe().unwrap())
                    .arg("commands::start::zakura_header_sync_driver_tests::native_signature_replacement_preserves_durable_header_selection")
                    .args(["--exact", "--nocapture"])
                    .env(PHASE, phase).env(DIRECTORY, cache.path())
                    .kill_on_drop(true).output()).await.unwrap().unwrap();
            assert!(
                output.status.success(),
                "{phase}: {}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        return;
    }
    let _guard = zakura_test::init();
    tokio::time::timeout(Duration::from_secs(60), async {
        let (mut network, mut prefix, honest, bad) =
            zakura_chain::block::tests::generate::authorization_substitution_fixture();
        let cache = std::path::PathBuf::from(std::env::var_os(DIRECTORY).unwrap());
        if phase.as_deref() == Some("reopen") {
            let bytes: Vec<Vec<u8>> =
                serde_json::from_slice(&std::fs::read(cache.join("prefix")).unwrap()).unwrap();
            prefix = bytes
                .iter()
                .map(|bytes| block::Block::zcash_deserialize(bytes.as_slice()).unwrap())
                .collect();
            network =
                zakura_chain::block::tests::generate::authorization_substitution_network(&prefix);
        } else {
            let bytes: Vec<_> = prefix
                .iter()
                .map(|block| block.zcash_serialize_to_vec().unwrap())
                .collect();
            std::fs::write(cache.join("prefix"), serde_json::to_vec(&bytes).unwrap()).unwrap();
        }
        let honest = Arc::new(honest);
        let parent = prefix.last().unwrap();
        let anchor = Frontier::new(parent.coinbase_height().unwrap(), parent.hash());

        let config = zs::Config {
            cache_dir: cache.clone(),
            ephemeral: false,
            debug_skip_non_finalized_state_backup_task: true,
            enable_zakura_header_seed_from_committed_blocks: true,
            ..zs::Config::default()
        };
        let (state, read, tip, tip_change, authority) =
            zs::init_with_header_chain_body_evidence(config.clone(), &network, anchor.height, 100)
                .await
                .unwrap();
        let state = tower::buffer::Buffer::new(state, 64);
        if phase.as_deref() == Some("reopen") {
            let handoff = super::zakura::SyncCoordinator::new();
            let _startup = zakura_header_sync_driver_startup(
                state.clone(),
                read.clone(),
                authority.clone(),
                &network,
                &handoff,
            )
            .await
            .unwrap();
            let expected = block::Hash(
                std::fs::read(cache.join("expected-tip"))
                    .unwrap()
                    .try_into()
                    .unwrap(),
            );
            let snapshot = read
                .subscribe_header_chain_snapshots()
                .borrow()
                .clone()
                .unwrap();
            assert_eq!(
                snapshot.frontiers.header_best,
                Frontier::new(block::Height(105), expected)
            );
            return;
        }
        for block in prefix {
            state
                .clone()
                .oneshot(zs::Request::CommitCheckpointVerifiedBlock(
                    Arc::new(block).into(),
                ))
                .await
                .unwrap();
        }
        let handoff = super::zakura::SyncCoordinator::new();
        let _startup = zakura_header_sync_driver_startup(
            state.clone(),
            read.clone(),
            authority.clone(),
            &network,
            &handoff,
        )
        .await
        .unwrap();
        let zs::ReadResponse::HeaderValidationLease(Some(lease)) = read
            .clone()
            .oneshot(zs::ReadRequest::HeaderValidationLease {
                parent_hash: anchor.hash,
            })
            .await
            .unwrap()
        else {
            panic!("parent validation lease")
        };
        let rules = zakura_header_chain::HeaderRules::for_validation_lease(&lease).unwrap();
        let mut headers = vec![honest.header.clone()];
        for n in 0..3 {
            let mut header = **headers.last().unwrap();
            header.previous_block_hash = headers.last().unwrap().hash();
            header.time += chrono::Duration::seconds(150);
            header.nonce.0[0] = n + 10;
            headers.push(Arc::new(header));
        }
        let target = Frontier::new(block::Height(105), headers.last().unwrap().hash());
        let batch = zakura_header_chain::prepare_headers(
            zakura_header_chain::HeaderBatchInput::new(&headers),
            lease.parent(),
            &rules,
            &zakura_header_chain::SystemClock,
        )
        .unwrap();
        let snapshot = read
            .subscribe_header_chain_snapshots()
            .borrow()
            .clone()
            .unwrap();
        let owner = HeaderWorkAuthority::for_target(&snapshot, target.hash)
            .bind(1, NonZeroU64::new(1).unwrap())
            .into();
        state
            .clone()
            .oneshot(zs::Request::ApplyHeaderChainInsert {
                prepared: authority.from_registered_header_attempt(Box::new(InsertHeaders {
                    owner,
                    source: zakura_header_chain::SourceId::from_digest([7; 32]),
                    parent_hash: anchor.hash,
                    target_tip_hash: target.hash,
                    completion: TargetCompletion::TargetComplete {
                        common_ancestor: anchor,
                    },
                    batch,
                    aux: Vec::new(),
                })),
            })
            .await
            .unwrap();
        let frontiers = FullStateFrontiers {
            finalized_height: anchor.height,
            verified_block_tip: anchor.height,
            verified_block_hash: anchor.hash,
        };
        let victim = ZakuraTestNode::builder(96100)
            .supported_capabilities(ZAKURA_CAP_BLOCK_SYNC_REGULATED)
            .header_sync_driver(
                network.clone(),
                (anchor.height, anchor.hash),
                frontiers,
                Some((target.height, target.hash)),
            )
            .block_committed_views(read.subscribe_header_chain_views())
            .spawn()
            .await
            .unwrap();
        let release = Arc::new(tokio::sync::Notify::new());
        let bad_calls = Arc::new(AtomicUsize::new(0));
        let good_calls = Arc::new(AtomicUsize::new(0));
        let supplier_frontiers = FullStateFrontiers {
            finalized_height: anchor.height,
            verified_block_tip: block::Height(102),
            verified_block_hash: honest.hash(),
        };
        let bad_peer = ZakuraTestNode::builder(96101)
            .supported_capabilities(ZAKURA_CAP_BLOCK_SYNC_REGULATED)
            .header_sync_driver(
                network.clone(),
                (block::Height(102), honest.hash()),
                FullStateFrontiers {
                    finalized_height: block::Height(102),
                    ..supplier_frontiers
                },
                Some((target.height, target.hash)),
            )
            .block_range_source(Arc::new(BodySource {
                body: Arc::new(bad),
                calls: bad_calls.clone(),
                release: Some(release.clone()),
            }))
            .spawn()
            .await
            .unwrap();
        let good_peer = ZakuraTestNode::builder(96102)
            .supported_capabilities(ZAKURA_CAP_BLOCK_SYNC_REGULATED)
            .header_sync_driver(
                network.clone(),
                (block::Height(102), honest.hash()),
                FullStateFrontiers {
                    finalized_height: block::Height(102),
                    ..supplier_frontiers
                },
                Some((target.height, target.hash)),
            )
            .block_range_source(Arc::new(BodySource {
                body: honest.clone(),
                calls: good_calls.clone(),
                release: None,
            }))
            .spawn()
            .await
            .unwrap();
        let (verifier, transaction_verifier, background, _) = zakura_consensus::router::init_test(
            zakura_consensus::Config::default(),
            &network,
            state.clone(),
        )
        .await;
        let invalid_writes = Arc::new(AtomicUsize::new(0));
        let capture = invalid_writes.clone();
        let writer = state.clone();
        let header_writer = BoxCloneService::new(service_fn(move |request: zs::Request| {
            if matches!(request, zs::Request::RecordHeaderChainBodyInvalid { .. }) {
                capture.fetch_add(1, Ordering::SeqCst);
            }
            writer.clone().oneshot(request)
        }));
        let (stop, stopped) = oneshot::channel();
        let driver = tokio::spawn(drive_block_sync_actions(
            victim.take_block_sync_actions().await.unwrap(),
            victim.supervisor(),
            None,
            victim.block_sync().unwrap(),
            tip.clone(),
            read.clone(),
            Some(header_writer),
            Some(authority.clone()),
            verifier.clone(),
            anchor.height,
            sync::MIN_CHECKPOINT_CONCURRENCY_LIMIT,
            sync::MIN_CONCURRENCY_LIMIT,
            sync::DEFAULT_ZAKURA_BLOCK_APPLY_CONCURRENCY_LIMIT,
            zakura_network::zakura::ZakuraTrace::noop(),
            None,
            handoff,
            async move {
                let _ = stopped.await;
            },
        ));
        victim
            .connect_native(&bad_peer, Duration::from_secs(5))
            .await
            .unwrap();
        while bad_calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        victim
            .connect_native(&good_peer, Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(victim.supervisor().registered_ids().await.len(), 2);
        release.notify_one();
        while tip.best_tip_hash() != Some(honest.hash()) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(good_calls.load(Ordering::SeqCst) > 0);
        assert_eq!(invalid_writes.load(Ordering::SeqCst), 0);
        assert_eq!(
            read.subscribe_header_chain_snapshots()
                .borrow()
                .as_ref()
                .unwrap()
                .frontiers
                .header_best,
            target
        );
        let _ = stop.send(());
        driver.await.unwrap();
        victim.shutdown().await;
        bad_peer.shutdown().await;
        good_peer.shutdown().await;
        drop((
            verifier,
            transaction_verifier,
            state,
            read,
            tip,
            tip_change,
            authority,
            _startup,
        ));
        background.state_checkpoint_verify_handle.await.unwrap();
        std::fs::write(cache.join("expected-tip"), target.hash.0).unwrap();
    })
    .await
    .expect("native signature replacement and restart must finish");
}
