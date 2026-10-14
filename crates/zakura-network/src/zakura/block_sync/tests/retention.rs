use super::*;

struct RetentionHarness {
    retained: Option<watch::Sender<block::Height>>,
    handle: BlockSyncHandle,
    _actions: mpsc::Receiver<BlockSyncAction>,
    service: BlockSyncService,
    task: JoinHandle<()>,
}

impl RetentionHarness {
    fn new(retained: u32) -> Self {
        Self::from_startup(
            retained,
            BlockSyncStartup::inert(ZakuraBlockSyncConfig::default()),
        )
    }

    fn from_startup(retained: u32, mut startup: BlockSyncStartup) -> Self {
        let blocks = mainnet_blocks_1_to_3();
        let config = startup.config.clone();
        startup.frontiers = BlockSyncFrontiers {
            finalized_height: block::Height(3),
            verified_block_tip: block::Height(3),
            verified_block_hash: blocks[2].hash(),
        };
        startup.best_header_tip = (block::Height(3), blocks[2].hash());
        let (retained, receiver) = watch::channel(block::Height(retained));
        let (handle, actions, task) = spawn_block_sync_reactor(startup.with_retention(
            receiver,
            zakura_chain::parameters::Network::Mainnet.genesis_hash(),
        ))
        .expect("valid block-sync config");
        let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
        Self {
            retained: Some(retained),
            handle,
            _actions: actions,
            service,
            task,
        }
    }

    async fn connect(&self, id: u8) -> (ZakuraPeerId, FramedSend, FramedRecv, BlockSyncStatus) {
        let peer = peer(id);
        let (inbound, receiver) = framed_channel(16);
        let (sender, mut outbound) = framed_channel(16);
        self.service.add_peer(Peer::new_with_direction(
            peer.clone(),
            None,
            ZAKURA_CAP_BLOCK_SYNC_REGULATED,
            ServicePeerDirection::Outbound,
            HashMap::from([(ZAKURA_STREAM_BLOCK_SYNC, (receiver, sender))]),
            CancellationToken::new(),
        ));
        let advertised = wait_for_outbound_status(&mut outbound).await;
        inbound
            .send(
                BlockSyncMessage::Status(BlockSyncStatus {
                    servable_low: block::Height(0),
                    servable_high: block::Height(0),
                    tip_hash: zakura_chain::parameters::Network::Mainnet.genesis_hash(),
                    max_blocks_per_response: 1,
                    max_inflight_requests: 1,
                    max_response_bytes: MAX_BS_RESPONSE_BYTES,
                })
                .encode_frame()
                .expect("status encodes"),
            )
            .await
            .expect("status queues");
        (peer, inbound, outbound, advertised)
    }

    /// Waits until the local advertisement starts at `low`, without waiting for
    /// the 30-second wire cadence.
    async fn local_status_with_low(&self, low: u32) -> BlockSyncStatus {
        await_until(
            "the retained range is advertised",
            Duration::from_secs(5),
            || self.handle.local_status().servable_low == block::Height(low),
        )
        .await
        .expect("the retained range is advertised");
        self.handle.local_status()
    }
}

impl Drop for RetentionHarness {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[tokio::test]
async fn retention_initial_status_preserves_archive_and_pruned_ranges() {
    let blocks = mainnet_blocks_1_to_3();
    for (retained, low, high, hash) in [
        (0, 0, 3, blocks[2].hash()),
        (2, 2, 3, blocks[2].hash()),
        (
            4,
            0,
            0,
            zakura_chain::parameters::Network::Mainnet.genesis_hash(),
        ),
    ] {
        let harness = RetentionHarness::new(retained);
        let (_, _inbound, _outbound, advertised) = harness.connect(0xe1).await;
        assert_eq!(advertised.servable_low, block::Height(low));
        assert_eq!(advertised.servable_high, block::Height(high));
        assert_eq!(advertised.tip_hash, hash);
        let local = harness.handle.local_status();
        assert_eq!(
            (local.servable_low, local.servable_high, local.tip_hash),
            (advertised.servable_low, advertised.servable_high, hash)
        );
    }
}

#[tokio::test]
async fn retention_refreshes_without_tip_growth_and_survives_publisher_shutdown() {
    let mut harness = RetentionHarness::new(2);
    harness
        .retained
        .as_ref()
        .expect("publisher exists")
        .send(block::Height(3))
        .expect("reactor subscribes");
    let advertised = harness.local_status_with_low(3).await;
    assert_eq!(advertised.servable_high, block::Height(3));

    drop(harness.retained.take());
    let (_, _inbound, _outbound, advertised_after_close) = harness.connect(0xe4).await;
    assert_eq!(
        (
            advertised_after_close.servable_low,
            advertised_after_close.servable_high
        ),
        (block::Height(3), block::Height(3))
    );
}

#[tokio::test]
async fn retention_coalesces_corrections_at_the_status_deadline() {
    use crate::zakura::{BlockRangeRead, BlockRangeReadResult, BlockRangeSource};

    #[derive(Debug)]
    struct StatusOnlySource;

    impl BlockRangeSource for StatusOnlySource {
        fn read(
            &self,
            _request: BlockRangeRead,
        ) -> futures::future::BoxFuture<'static, Result<BlockRangeReadResult, crate::BoxError>>
        {
            panic!("Status delivery must not read blocks")
        }
    }

    let startup = BlockSyncStartup::inert(ZakuraBlockSyncConfig::default())
        .with_range_source(Arc::new(StatusOnlySource));
    let harness = RetentionHarness::from_startup(1, startup);
    let resources = harness
        .service
        .reserve_session(ServicePeerDirection::Outbound)
        .unwrap()
        .unwrap();
    resources.admitted();
    let (inbound, receiver) = framed_channel(16);
    let (sender, mut outbound) = framed_channel(2);
    let connected_at = time::Instant::now();
    harness.service.add_peer(Peer::new_with_service_streams(
        0,
        peer(0xeb),
        None,
        ZAKURA_CAP_BLOCK_SYNC_REGULATED,
        ServicePeerDirection::Outbound,
        HashMap::from([(
            ZAKURA_STREAM_BLOCK_SYNC,
            crate::zakura::transport::ServiceStream::new(
                0,
                ZAKURA_REGULATED_BLOCK_SYNC_STREAM_VERSION,
                receiver,
                sender.with_session_resources(Some(resources)),
                CancellationToken::new(),
            ),
        )]),
        CancellationToken::new(),
        crate::zakura::CloseCause::default(),
    ));
    let initial = wait_for_outbound_status(&mut outbound).await;
    assert_eq!(initial.servable_low, block::Height(1));
    send_inbound(
        &inbound,
        BlockSyncMessage::Status(BlockSyncStatus {
            servable_low: block::Height(0),
            servable_high: block::Height(0),
            ..initial
        }),
    )
    .await;

    harness
        .handle
        .send(BlockSyncEvent::StateFrontiersChanged(BlockSyncFrontiers {
            finalized_height: block::Height(3),
            verified_block_tip: block::Height(4),
            verified_block_hash: block::Hash([4; 32]),
        }))
        .await
        .expect("tip growth queues");
    let retained = harness.retained.as_ref().expect("publisher exists");
    retained.send(block::Height(2)).expect("reactor subscribes");
    await_until("first correction", Duration::from_secs(1), || {
        harness.handle.local_status().servable_low == block::Height(2)
    })
    .await
    .unwrap();
    retained.send(block::Height(3)).expect("reactor subscribes");
    await_until("latest correction", Duration::from_secs(1), || {
        harness.handle.local_status().servable_low == block::Height(3)
    })
    .await
    .unwrap();

    // The reactor uses wall time, so exercise the real connection deadline.
    assert!(
        time::timeout_at(connected_at + Duration::from_secs(29), outbound.recv())
            .await
            .is_err(),
        "range corrections must obey the shared Status cadence"
    );
    // Read directly so the helper's one-second timeout cannot race the cadence deadline.
    let frame = time::timeout(Duration::from_secs(3), outbound.recv())
        .await
        .expect("the latest range is sent at the 30-second deadline")
        .expect("the outbound stream stays open");
    let BlockSyncMessage::Status(latest) = BlockSyncMessage::decode_frame(frame).unwrap() else {
        panic!("the retained-range correction is a Status");
    };
    assert_eq!(latest.servable_low, block::Height(3));
    assert_eq!(latest.servable_high, block::Height(4));
    assert!(time::timeout(Duration::from_millis(150), outbound.recv())
        .await
        .is_err());
}

#[tokio::test]
async fn retention_above_tip_advertises_only_genesis_until_retained_tip_arrives() {
    let harness = RetentionHarness::new(4);
    let (_, _inbound, _outbound, advertised) = harness.connect(0xe5).await;
    assert_eq!(
        (advertised.servable_low, advertised.servable_high),
        (block::Height(0), block::Height(0))
    );
    harness
        .handle
        .send(BlockSyncEvent::ChainTipGrow(BlockSyncFrontiers {
            finalized_height: block::Height(4),
            verified_block_tip: block::Height(4),
            verified_block_hash: block::Hash([4; 32]),
        }))
        .await
        .expect("tip update queues");
    let advertised = harness.local_status_with_low(4).await;
    assert_eq!(advertised.servable_high, block::Height(4));
    assert_eq!(advertised.tip_hash, block::Hash([4; 32]));
}

#[tokio::test]
async fn retention_advertisement_keeps_pruned_heights_out_of_download_requests() {
    let serving = RetentionHarness::new(2);
    let blocks = mainnet_blocks_1_to_3();
    let config = immediate_body_download_config();
    let (_tip, tip_rx) = watch::channel((block::Height(3), blocks[2].hash()));
    let startup = BlockSyncStartup::new(
        BlockSyncFrontiers {
            finalized_height: block::Height(0),
            verified_block_tip: block::Height(0),
            verified_block_hash: block::Hash([0; 32]),
        },
        (block::Height(3), blocks[2].hash()),
        tip_rx,
        config.clone(),
    );
    let (handle, mut actions, task) =
        spawn_block_sync_reactor(startup).expect("valid block-sync config");
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
    let (_, _inbound, mut outbound) = connect_peer_with_status_message(
        &service,
        &mut actions,
        0xe6,
        serving.handle.local_status(),
    )
    .await;
    handle
        .send(BlockSyncEvent::NeededBlocks(vec![
            block_meta(&blocks[0]),
            block_meta(&blocks[1]),
        ]))
        .await
        .expect("needed blocks queue");
    assert_eq!(
        wait_for_outbound_getblocks(&mut outbound).await,
        (block::Height(2), 1)
    );
    assert!(
        handle
            .routine_wiring
            .as_ref()
            .expect("download wiring exists")
            .work
            .pending_contains(block::Height(1)),
        "the peer's pruning must not discard our need to download older blocks from another peer"
    );
    task.abort();
}

#[tokio::test]
async fn retention_deferred_status_applies_after_peer_goes_quiet() {
    let config = ZakuraBlockSyncConfig::default();
    let (handle, mut actions, task) =
        spawn_block_sync_reactor(BlockSyncStartup::inert(config.clone()))
            .expect("valid block-sync config");
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
    let mut advertised = BlockSyncStatus {
        servable_low: block::Height(1),
        servable_high: block::Height(3),
        ..status()
    };
    let (peer, inbound, _outbound) =
        connect_peer_with_status_message(&service, &mut actions, 0xe7, advertised).await;
    let registry = &handle
        .routine_wiring
        .as_ref()
        .expect("download wiring exists")
        .registry;

    for low in [2, 3] {
        advertised.servable_low = block::Height(low);
        send_inbound(&inbound, BlockSyncMessage::Status(advertised)).await;
        await_until(
            "updated peer retention floor",
            Duration::from_secs(5),
            || {
                registry.candidate_snapshot()
                    == vec![(peer.clone(), true, block::Height(low), block::Height(3))]
            },
        )
        .await
        .expect("the update applies without another incoming frame");
    }
    task.abort();
}
