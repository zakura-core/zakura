use super::*;

struct RetentionHarness {
    retained: Option<watch::Sender<block::Height>>,
    handle: BlockSyncHandle,
    actions: mpsc::Receiver<BlockSyncAction>,
    service: BlockSyncService,
    task: JoinHandle<()>,
}

impl RetentionHarness {
    fn new(retained: u32) -> Self {
        Self::with_refresh_interval(retained, Duration::from_millis(10))
    }

    fn with_refresh_interval(retained: u32, status_refresh_interval: Duration) -> Self {
        let blocks = mainnet_blocks_1_to_3();
        let config = ZakuraBlockSyncConfig {
            status_refresh_interval,
            ..ZakuraBlockSyncConfig::default()
        };
        let mut startup = BlockSyncStartup::inert(config.clone());
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
        ));
        let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
        Self {
            retained: Some(retained),
            handle,
            actions,
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
            ZAKURA_CAP_BLOCK_SYNC,
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

    fn assert_no_storage_query(&mut self) {
        while let Ok(action) = self.actions.try_recv() {
            assert!(
                !matches!(action, BlockSyncAction::QueryBlocksByHeightRange { .. }),
                "a pruned range must not query storage"
            );
        }
    }
}

impl Drop for RetentionHarness {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn request(inbound: &FramedSend, start: u32) {
    inbound
        .send(
            BlockSyncMessage::GetBlocks {
                start_height: block::Height(start),
                count: 1,
            }
            .encode_frame()
            .expect("request encodes"),
        )
        .await
        .expect("request queues");
}

async fn status_with_low(outbound: &mut FramedRecv, low: u32) -> BlockSyncStatus {
    time::timeout(Duration::from_secs(5), async {
        loop {
            let status = wait_for_outbound_status(outbound).await;
            if status.servable_low == block::Height(low) {
                return status;
            }
        }
    })
    .await
    .expect("the retained range is advertised")
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
        assert_eq!(harness.handle.local_status(), advertised);
    }
}

#[tokio::test]
async fn retention_rejects_pruned_reads_and_still_serves_retained_blocks() {
    let blocks = mainnet_blocks_1_to_3();
    let mut harness = RetentionHarness::new(2);
    let (peer, inbound, mut outbound, _) = harness.connect(0xe2).await;
    request(&inbound, 1).await;
    assert_eq!(
        wait_for_outbound_range_unavailable(&mut outbound).await,
        (block::Height(1), 1)
    );
    harness.assert_no_storage_query();

    let genesis = mainnet_block(&zakura_test::vectors::BLOCK_MAINNET_GENESIS_BYTES);
    for (height, body) in [(2, blocks[1].clone()), (0, genesis)] {
        request(&inbound, height).await;
        match next_action(&mut harness.actions).await {
            BlockSyncAction::QueryBlocksByHeightRange { start, count, .. } => {
                assert_eq!((start, count), (block::Height(height), 1));
            }
            other => panic!("expected a retained storage query, got {other:?}"),
        }
        harness
            .handle
            .send(BlockSyncEvent::BlockRangeResponseReady {
                peer: peer.clone(),
                start_height: block::Height(height),
                requested_count: 1,
                blocks: vec![(
                    block::Height(height),
                    body.clone(),
                    usize::try_from(block_size(&body)).expect("fixture size fits usize"),
                )],
            })
            .await
            .expect("storage result queues");
        assert_eq!(
            wait_for_outbound_block(&mut outbound).await.hash(),
            body.hash()
        );
        assert_eq!(
            wait_for_outbound_blocks_done(&mut outbound).await,
            (block::Height(height), 1)
        );
    }
}

#[tokio::test]
async fn retention_refreshes_without_tip_growth_and_survives_publisher_shutdown() {
    let mut harness = RetentionHarness::new(2);
    let (_, inbound, mut outbound, _) = harness.connect(0xe3).await;
    harness
        .retained
        .as_ref()
        .expect("publisher exists")
        .send(block::Height(3))
        .expect("reactor subscribes");
    let advertised = status_with_low(&mut outbound, 3).await;
    assert_eq!(advertised.servable_high, block::Height(3));
    assert_eq!(harness.handle.local_status(), advertised);
    request(&inbound, 2).await;
    assert_eq!(
        wait_for_outbound_range_unavailable(&mut outbound).await,
        (block::Height(2), 1)
    );
    harness.assert_no_storage_query();

    drop(harness.retained.take());
    let (_, _inbound, _outbound, advertised_after_close) = harness.connect(0xe4).await;
    assert_eq!(advertised_after_close, advertised);
}

#[tokio::test]
async fn retention_refreshes_at_meter_deadline_between_periodic_ticks() {
    let interval = Duration::from_secs(1);
    let harness = RetentionHarness::with_refresh_interval(1, interval);
    let (_, _inbound, mut outbound, _) = harness.connect(0xea).await;
    wait_for_outbound_status(&mut outbound).await;

    // Offset the first change from reactor startup. Its meter reopens halfway
    // between periodic ticks, so polling alone would miss the deadline.
    time::sleep(interval / 2).await;
    let retained = harness.retained.as_ref().expect("publisher exists");
    retained.send(block::Height(2)).expect("reactor subscribes");
    status_with_low(&mut outbound, 2).await;

    retained.send(block::Height(3)).expect("reactor subscribes");
    assert!(
        time::timeout(interval * 3 / 4, outbound.recv())
            .await
            .is_err(),
        "the pending retention change must respect the refresh window"
    );
    let advertised = time::timeout(interval / 2, status_with_low(&mut outbound, 3))
        .await
        .expect("the pending change must advertise at the meter deadline, before the next tick");
    assert_eq!(advertised.servable_high, block::Height(3));
    assert_eq!(harness.handle.local_status(), advertised);
}

#[tokio::test]
async fn retention_retries_latest_status_only_for_peer_with_full_outbound_queue() {
    let harness = RetentionHarness::new(1);
    let (inbound, receiver) = framed_channel(16);
    let (sender, mut outbound) = framed_channel(1);
    harness.service.add_peer(Peer::new_with_direction(
        peer(0xe8),
        None,
        ZAKURA_CAP_BLOCK_SYNC,
        ServicePeerDirection::Outbound,
        HashMap::from([(ZAKURA_STREAM_BLOCK_SYNC, (receiver, sender.clone()))]),
        CancellationToken::new(),
    ));
    let initial_status = wait_for_outbound_status(&mut outbound).await;
    send_inbound(&inbound, BlockSyncMessage::Status(status())).await;
    wait_for_outbound_status(&mut outbound).await;

    let (_, _healthy_inbound, mut healthy_outbound, _) = harness.connect(0xe9).await;
    wait_for_outbound_status(&mut healthy_outbound).await;

    sender
        .try_send(
            BlockSyncMessage::Status(initial_status)
                .encode_frame()
                .expect("filler status encodes"),
        )
        .expect("the established peer's outbound queue fills");
    for low in [2, 3] {
        harness
            .retained
            .as_ref()
            .expect("publisher exists")
            .send(block::Height(low))
            .expect("reactor subscribes");
        let advertised = status_with_low(&mut healthy_outbound, low).await;
        assert_eq!(advertised.servable_high, block::Height(3));
        assert_eq!(harness.handle.local_status(), advertised);
    }

    assert_eq!(
        wait_for_outbound_status(&mut outbound).await,
        initial_status
    );
    let retried = wait_for_outbound_status(&mut outbound).await;
    assert_eq!(retried.servable_low, block::Height(3));
    assert_eq!(retried.servable_high, block::Height(3));
    for outbound in [&mut outbound, &mut healthy_outbound] {
        assert!(
            time::timeout(Duration::from_millis(50), outbound.recv())
                .await
                .is_err(),
            "successfully queued updates must not keep retrying"
        );
    }
}

#[tokio::test]
async fn retention_corrects_a_debounced_tip_and_retries_the_latest_range_after_queue_pressure() {
    let harness = RetentionHarness::with_refresh_interval(1, Duration::from_secs(30));
    let (inbound, receiver) = framed_channel(16);
    let (sender, mut outbound) = framed_channel(1);
    harness.service.add_peer(Peer::new_with_direction(
        peer(0xeb),
        None,
        ZAKURA_CAP_BLOCK_SYNC,
        ServicePeerDirection::Outbound,
        HashMap::from([(ZAKURA_STREAM_BLOCK_SYNC, (receiver, sender.clone()))]),
        CancellationToken::new(),
    ));
    let initial = wait_for_outbound_status(&mut outbound).await;
    send_inbound(&inbound, BlockSyncMessage::Status(status())).await;
    wait_for_outbound_status(&mut outbound).await;
    let (_, _healthy_inbound, mut healthy_outbound, _) = harness.connect(0xec).await;
    wait_for_outbound_status(&mut healthy_outbound).await;

    harness
        .handle
        .send(BlockSyncEvent::StateFrontiersChanged(BlockSyncFrontiers {
            finalized_height: block::Height(3),
            verified_block_tip: block::Height(4),
            verified_block_hash: block::Hash([4; 32]),
        }))
        .await
        .expect("tip growth queues");
    assert_eq!(
        wait_for_outbound_status(&mut outbound).await.servable_high,
        block::Height(4)
    );
    wait_for_outbound_status(&mut healthy_outbound).await;
    sender
        .try_send(
            BlockSyncMessage::Status(initial)
                .encode_frame()
                .expect("filler encodes"),
        )
        .expect("one peer's queue fills");

    let retained = harness.retained.as_ref().expect("publisher exists");
    retained.send(block::Height(2)).expect("reactor subscribes");
    time::timeout(
        Duration::from_millis(500),
        status_with_low(&mut healthy_outbound, 2),
    )
    .await
    .expect("pruning must not wait for the consumed 30-second growth allowance");

    retained.send(block::Height(3)).expect("reactor subscribes");
    await_until("latest local floor", Duration::from_secs(1), || {
        harness.handle.local_status().servable_low == block::Height(3)
    })
    .await
    .expect("local status tracks the current range even while a peer's queue is full");
    assert_eq!(wait_for_outbound_status(&mut outbound).await, initial);
    let latest = time::timeout(
        Duration::from_millis(500),
        status_with_low(&mut outbound, 3),
    )
    .await
    .expect("the drained queue retries the latest range without another prune or tip event");
    assert_eq!(latest.servable_high, block::Height(4));
    assert!(time::timeout(Duration::from_millis(150), outbound.recv())
        .await
        .is_err());
}

#[tokio::test]
async fn retention_above_tip_serves_only_genesis_until_retained_tip_arrives() {
    let mut harness = RetentionHarness::new(4);
    let (_, inbound, mut outbound, advertised) = harness.connect(0xe5).await;
    assert_eq!(
        (advertised.servable_low, advertised.servable_high),
        (block::Height(0), block::Height(0))
    );
    request(&inbound, 1).await;
    assert_eq!(
        wait_for_outbound_range_unavailable(&mut outbound).await,
        (block::Height(1), 1)
    );
    harness.assert_no_storage_query();

    request(&inbound, 0).await;
    match next_action(&mut harness.actions).await {
        BlockSyncAction::QueryBlocksByHeightRange { start, count, .. } => {
            assert_eq!((start, count), (block::Height(0), 1));
        }
        other => panic!("expected a genesis query, got {other:?}"),
    }
    harness
        .handle
        .send(BlockSyncEvent::ChainTipGrow(BlockSyncFrontiers {
            finalized_height: block::Height(4),
            verified_block_tip: block::Height(4),
            verified_block_hash: block::Hash([4; 32]),
        }))
        .await
        .expect("tip update queues");
    let advertised = status_with_low(&mut outbound, 4).await;
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
    let (handle, mut actions, task) = spawn_block_sync_reactor(startup);
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
        spawn_block_sync_reactor(BlockSyncStartup::inert(config.clone()));
    let service = BlockSyncService::new_with_handle_for_test(config, handle.clone());
    let mut advertised = BlockSyncStatus {
        servable_low: block::Height(1),
        servable_high: block::Height(3),
        ..status()
    };
    let (peer, inbound, mut outbound) =
        connect_peer_with_status_message(&service, &mut actions, 0xe7, advertised).await;
    wait_for_outbound_status(&mut outbound).await;
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
