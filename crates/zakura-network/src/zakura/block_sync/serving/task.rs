use super::super::trace::{self, BlockTraceEvent};
use super::*;
use crate::zakura::block_sync::{
    peer_registry::PeerRegistry,
    serving_regulation::{GetBlocksRequest, GetBlocksServingPermit, GetBlocksServingSession},
};
use crate::zakura::{FramedRecv, FramedSend, SinkReject, ZakuraTrace};

/// The two streams may arrive in either order. Bound the initial Status wait,
/// including a session that sends no request at all.
const STATUS_SETUP_TIMEOUT: Duration = Duration::from_secs(10);

pub(in crate::zakura::block_sync) async fn serve_requests(
    session: BlockSyncPeerSession,
    mut requests: FramedRecv,
    admission: GetBlocksServingSession,
    registry: Arc<PeerRegistry>,
    local_status: watch::Receiver<BlockSyncStatus>,
    source: Option<Arc<dyn BlockRangeSource>>,
    trace: ZakuraTrace,
) -> Result<(), SinkReject> {
    let cancel = session.cancel_token();
    tokio::select! {
        biased;
        () = cancel.cancelled() => Ok(()),
        result = async {
            let mut ready = session.subscribe_remote_status();
            let deadline = time::Instant::now() + STATUS_SETUP_TIMEOUT;
            let mut pending = None;
            while !*ready.borrow_and_update() {
                tokio::select! {
                    biased;
                    () = time::sleep_until(deadline) => return Err(local_error("block-sync Status setup timed out")),
                    changed = ready.changed() => changed.map_err(SinkReject::local)?,
                    frame = requests.recv(), if pending.is_none() => {
                        let Some(frame) = frame else { return Ok(()); };
                        pending = Some(decode_request(&admission, frame)?);
                    }
                }
            }

            let sender = session.data_sender();
            loop {
                let request = match pending.take() {
                    Some(request) => request,
                    None => {
                        let Some(frame) = requests.recv().await else { return Ok(()); };
                        decode_request(&admission, frame)?
                    }
                };
                let started = Instant::now();
                let mut permit = admission.admit_request(&request).await;
                if cancel.is_cancelled() || !registry.owns_generation(session.peer_id(), session.session_id()) {
                    return Ok(());
                }
                let status = *local_status.borrow();
                let count = if request.start_height < status.servable_low {
                    0
                } else {
                    status.servable_high.0.checked_sub(request.start_height.0)
                        .and_then(|last| last.checked_add(1)).unwrap_or(0)
                        .min(request.count).min(status.max_blocks_per_response)
                };
                let result = match source.as_ref().filter(|_| count > 0) {
                    Some(source) => Some(source.read_range(BlockRangeRead {
                        start: request.start_height, count,
                        max_response_bytes: status.max_response_bytes,
                        lease: permit.work_lease(),
                    }).await),
                    None => None,
                };
                let prepare_elapsed = started.elapsed();
                let send_started = Instant::now();
                let mut returned = 0u32;
                let mut bytes = 0u64;
                match result {
                    Some(Ok(result)) => {
                        let BlockRangeReadResult { blocks, _lease } = result;
                        for (height, block, size) in blocks {
                            let next_bytes = bytes.checked_add(u64::try_from(size).unwrap_or(u64::MAX));
                            if returned >= count
                                || request.start_height.0.checked_add(returned).map(block::Height) != Some(height)
                                || next_bytes.is_none_or(|bytes| bytes > u64::from(status.max_response_bytes)) {
                                break;
                            }
                            send_response(&sender, &mut permit, BlockSyncMessage::Block(block)).await?;
                            bytes = next_bytes.expect("the response size was checked");
                            returned += 1;
                        }
                    }
                    Some(Err(error)) => tracing::debug!(peer = ?session.peer_id(), ?error, "GetBlocks storage read failed"),
                    None => {},
                }
                let ending = if returned == 0 {
                    BlockSyncMessage::RangeUnavailable { start_height: request.start_height, count: request.count }
                } else {
                    BlockSyncMessage::BlocksDone { start_height: request.start_height, returned }
                };
                send_response(&sender, &mut permit, ending).await?;
                trace.emit_event(|| BlockTraceEvent::build(bs_trace::BLOCK_RANGE_RESPONSE_SENT, |row| {
                    row.peer = Some(trace::peer(session.peer_id()));
                    row.range_start = Some(trace::height(request.start_height));
                    row.range_count = Some(u64::from(returned));
                    row.expected_count = Some(u64::from(count));
                    row.serialized_bytes = Some(bytes);
                    row.reason = Some(if returned == 0 { "range_unavailable" } else { "blocks_done" });
                    row.prepare_elapsed_ms = Some(trace::elapsed_ms(prepare_elapsed));
                    row.send_elapsed_ms = Some(trace::elapsed_ms(send_started.elapsed()));
                    row.elapsed_ms = Some(trace::elapsed_ms(started.elapsed()));
                }));
                // Dropping the producer lets the next request wait for the last
                // frame guard. Retaining it while waiting would deadlock this peer.
            }
        } => result,
    }
}

fn decode_request(
    admission: &GetBlocksServingSession,
    frame: Frame,
) -> Result<GetBlocksRequest, SinkReject> {
    admission
        .decode_request(frame)
        .map_err(SinkReject::protocol)
}

pub(in crate::zakura::block_sync) async fn send_response(
    sender: &FramedSend,
    permit: &mut GetBlocksServingPermit,
    message: BlockSyncMessage,
) -> Result<(), SinkReject> {
    let slot = sender
        .reserve_guarded()
        .await
        .map_err(|_| local_error("block-sync data queue closed"))?;
    // The blocking encode owns a lease even if its async caller is aborted.
    // Its returned frame and lease are dropped together if delivery disappears.
    let is_block = matches!(&message, BlockSyncMessage::Block(_));
    let lease = permit.work_lease();
    let (frame, _lease) = tokio::task::spawn_blocking(move || {
        if lease.is_cancelled() {
            return Err(local_error("block-sync serving cancelled"));
        }
        let frame = message.encode_frame().map_err(SinkReject::local)?;
        Ok((frame, lease))
    })
    .await
    .map_err(SinkReject::local)??;
    let bytes = u64::try_from(frame.payload.len()).map_err(SinkReject::local)?;
    if !permit.can_queue_frame(bytes) {
        return Err(local_error(
            "encoded GetBlocks response exceeded its admitted byte cap",
        ));
    }
    slot.send(frame, permit.frame_guard(bytes));
    if is_block {
        metrics::counter!("sync.block.body.served").increment(1);
    }
    Ok(())
}

fn local_error(message: &'static str) -> SinkReject {
    SinkReject::local(io::Error::other(message))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::zakura::{
        block_sync::serving_regulation::GetBlocksServingRegulator,
        transport::{worker_framed_channel, FramedWorkerRecv},
    };

    fn setup() -> (
        FramedSend,
        FramedWorkerRecv,
        GetBlocksServingRegulator,
        GetBlocksServingPermit,
    ) {
        let regulator = GetBlocksServingRegulator::new(ZakuraBlockSyncConfig::default());
        let permit = regulator
            .session(ZakuraPeerId::new(vec![1; 32]).unwrap())
            .admit_now(1)
            .unwrap()
            .commit();
        let (sender, receiver) = worker_framed_channel(1);
        (sender, receiver, regulator, permit)
    }

    fn invalid_terminal() -> BlockSyncMessage {
        BlockSyncMessage::BlocksDone {
            start_height: block::Height(1),
            returned: u32::MAX,
        }
    }

    #[tokio::test]
    async fn queue_pressure_waits_before_encoding_and_cancellation_returns_capacity() {
        let (sender, mut receiver, regulator, mut permit) = setup();
        sender
            .send(
                BlockSyncMessage::RangeUnavailable {
                    start_height: block::Height(1),
                    count: 1,
                }
                .encode_frame()
                .unwrap(),
            )
            .await
            .unwrap();
        assert!(time::timeout(
            Duration::from_millis(20),
            send_response(&sender, &mut permit, invalid_terminal())
        )
        .await
        .is_err());
        drop(receiver.recv().await.unwrap());
        assert_eq!(sender.capacity(), 1);
        assert_eq!(regulator.snapshot().node_active, 1);
        drop(permit);
        assert_eq!(regulator.snapshot().node_active, 0);
    }

    #[tokio::test]
    async fn encoding_failure_returns_queue_space_and_success_retains_frame_ownership() {
        let (sender, mut receiver, regulator, mut permit) = setup();
        assert!(send_response(&sender, &mut permit, invalid_terminal())
            .await
            .is_err());
        assert_eq!(sender.capacity(), 1);
        send_response(
            &sender,
            &mut permit,
            BlockSyncMessage::RangeUnavailable {
                start_height: block::Height(1),
                count: 1,
            },
        )
        .await
        .unwrap();
        drop(permit);
        assert_eq!(regulator.snapshot().node_active, 1);
        drop(receiver.recv().await.unwrap());
        assert_eq!(regulator.snapshot().node_active, 0);
    }

    #[tokio::test]
    async fn closed_queue_releases_the_response_producer() {
        let (sender, receiver, regulator, mut permit) = setup();
        drop(receiver);
        let result = send_response(&sender, &mut permit, invalid_terminal()).await;
        assert!(format!("{result:?}").contains("data queue closed"));
        drop(permit);
        assert_eq!(regulator.snapshot().node_active, 0);
    }
}
