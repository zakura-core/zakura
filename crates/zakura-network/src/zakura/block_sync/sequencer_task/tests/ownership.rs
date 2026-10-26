use super::*;
use zakura_chain::serialization::ZcashSerialize;
use zakura_test::vectors::{BLOCK_MAINNET_2_BYTES, BLOCK_MAINNET_3_BYTES};

/// The applying and in-flight submission gauges, and the view admission reads,
/// equal sums over the bodies the sequencer holds through acceptance,
/// submission, detachment, completion, commit, and reset. The gauges are read
/// before the view is published, so each must follow the sequencer on its own.
#[tokio::test]
async fn byte_gauges_and_view_follow_sequencer_owners() {
    let recorder = super::super::super::tests::gauge_recorder::GaugeRecorder::default();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let frontiers = genesis_frontiers();
    let input_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let input_decoded_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let (_body_tx, body_rx) = mpsc::channel(1);
    let (_control_tx, control_rx) = mpsc::unbounded_channel();
    let (actions, mut actions_rx) = mpsc::channel(8);
    let (view_tx, view_rx) = watch::channel(initial_view(frontiers));
    let mut task = SequencerTask::new(
        Sequencer::new(block::Height(0), 2),
        ByteBudget::new(1),
        Arc::new(WorkQueue::new(block::Height(0))),
        Arc::new(PeerRegistry::new()),
        actions,
        ThroughputMeter::new(Instant::now()),
        frontiers,
        Some(super::super::test_work_scope()),
        crate::zakura::header_sync::SeededRetryJitter::new([0; 32]),
        Arc::new(crate::zakura::block_sync::BlockSyncClock),
        body_rx,
        control_rx,
        input_bytes.clone(),
        input_decoded_bytes.clone(),
        view_tx,
        Duration::from_secs(60),
        ZakuraTrace::noop(),
    );
    let check = |task: &mut SequencerTask, label: &str| {
        let owners = task.sequencer.ownership_snapshot();
        let applying: u64 = owners.applying.iter().map(|(_, bytes)| bytes).sum();
        let submissions: u64 = owners.submissions.iter().map(|owner| owner.bytes).sum();
        assert_eq!(
            (
                recorder.bytes("sync.block.applying.buffered_bytes"),
                recorder.bytes("sync.block.submission.in_flight_bytes"),
            ),
            (applying, submissions),
            "gauges after {label}"
        );
        task.publish_view();
        let view = *view_rx.borrow();
        assert_eq!(
            view.applying_buffered_bytes, applying,
            "applying bytes after {label}"
        );
        assert_eq!(
            view.in_flight_submission_bytes, submissions,
            "submission bytes after {label}"
        );
        owners
    };

    for (height, bytes) in [
        BLOCK_MAINNET_1_BYTES.as_slice(),
        BLOCK_MAINNET_2_BYTES.as_slice(),
        BLOCK_MAINNET_3_BYTES.as_slice(),
    ]
    .into_iter()
    .zip(1..)
    .map(|(bytes, height)| (block::Height(height), bytes))
    {
        let block: Arc<block::Block> = Arc::new(bytes.zcash_deserialize_into().unwrap());
        let raw: Arc<[u8]> = block.zcash_serialize_to_vec().unwrap().into();
        let mut body = SequencedBody::new_queued(
            super::super::test_work_owner(),
            zakura_header_chain::SourceId::from_digest([1; 32]),
            height,
            block.hash(),
            block.header.previous_block_hash,
            BufferedBlockBody::from_decoded_block(block, Some(raw)),
            100 * u64::from(height.0),
            ZakuraPeerId::new(vec![1; 32]).unwrap(),
            Instant::now(),
            input_bytes.clone(),
            input_decoded_bytes.clone(),
        );
        body.leave_queue();
        task.handle_accept_body(body);
    }
    assert_eq!(check(&mut task, "acceptance").applying.len(), 3);

    // The two-body submission window submits heights 1 and 2.
    task.submit_pending_blocks().await;
    let submitted: Vec<_> = std::iter::from_fn(|| actions_rx.try_recv().ok())
        .filter_map(|action| match action {
            BlockSyncAction::SubmitBlock {
                owner,
                source,
                token,
                block,
            } => Some((owner, source, token, block)),
            _ => None,
        })
        .collect();
    assert_eq!(submitted.len(), 2);
    assert_eq!(check(&mut task, "submission").submissions.len(), 2);

    // Height 2 leaves `applying`, but the driver still holds its submission.
    task.sequencer.remove_applying(block::Height(2));
    let owners = check(&mut task, "detachment");
    assert!(owners
        .submissions
        .iter()
        .any(|owner| owner.height == block::Height(2) && owner.detached));

    let (owner, source, token, block) = &submitted[1];
    assert!(task.sequencer.finish_submission(
        *owner,
        *source,
        *token,
        block::Height(2),
        block.hash()
    ));
    check(&mut task, "detached completion");

    task.sequencer.advance_verified_tip(block::Height(1), true);
    check(&mut task, "commit");

    task.destructive_reset_to(block::Height(0), false);
    check(&mut task, "reset");

    // Completing the remaining submission leaves no holder.
    let (owner, source, token, block) = &submitted[0];
    task.sequencer
        .finish_submission(*owner, *source, *token, block::Height(1), block.hash());
    let owners = check(&mut task, "final completion");
    assert!(owners.applying.is_empty());
    assert!(owners.submissions.is_empty());
}
