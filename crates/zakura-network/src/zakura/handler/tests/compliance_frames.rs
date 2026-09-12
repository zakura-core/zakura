//! Shared frame-boundary probes using production declarations and real QUIC.

use super::*;
use crate::zakura::BlockSyncMessage;

const ALPN: &[u8] = b"/zakura/test/compliance-frames/1";
const DEADLINE: Duration = Duration::from_secs(5);

#[derive(Debug)]
struct CaptureFrames(mpsc::Sender<(SendStream, RecvStream)>);

impl ProtocolHandler for CaptureFrames {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        for _ in 0..128 {
            let Ok(stream) = connection.accept_bi().await else {
                break;
            };
            if self.0.send(stream).await.is_err() {
                break;
            }
        }
        Ok(())
    }
}

struct Pair {
    connection: Connection,
    client: Endpoint,
    router: Router,
    streams: mpsc::Receiver<(SendStream, RecvStream)>,
}

impl Pair {
    async fn new() -> Result<Self, BoxError> {
        let server = LocalEndpointFactory::new().endpoint(89601).await?;
        let (stream_tx, streams) = mpsc::channel(128);
        let router = Router::builder(server)
            .accept(ALPN, CaptureFrames(stream_tx))
            .spawn();
        let client = LocalEndpointFactory::new().endpoint(89602).await?;
        let address = LocalEndpointFactory::node_addr(router.endpoint()).await;
        let connection = timeout(DEADLINE, client.connect(address, ALPN)).await??;
        Ok(Self {
            connection,
            client,
            router,
            streams,
        })
    }

    async fn begin(&mut self, bytes: &[u8]) -> Result<(SendStream, RecvStream), BoxError> {
        let (mut send, _) = timeout(DEADLINE, self.connection.open_bi()).await??;
        timeout(DEADLINE, send.write_all(bytes)).await??;
        let (_, recv) = timeout(DEADLINE, self.streams.recv())
            .await?
            .ok_or("stream capture closed")?;
        Ok((send, recv))
    }

    async fn finish(self) -> Result<(), BoxError> {
        self.connection.close(0u32.into(), b"test complete");
        self.client.close().await;
        self.router.shutdown().await?;
        Ok(())
    }
}

fn header(kind: u16, flags: u16, payload_len: u32) -> Vec<u8> {
    [
        kind.to_le_bytes().as_slice(),
        flags.to_le_bytes().as_slice(),
        payload_len.to_le_bytes().as_slice(),
    ]
    .concat()
}

#[tokio::test]
async fn f01_all_getblocks_messages_reject_oversized_headers_before_payload_reads(
) -> Result<(), BoxError> {
    let service = BlockSyncService::new(ZakuraBlockSyncConfig::default());
    let mut pair = Pair::new().await?;
    let mut failures = Vec::new();
    for (kind, cap) in [(2u16, 9u32), (3, 2_000_001), (4, 9), (5, 9)] {
        let role = service.streams()[usize::from(kind == 2)];
        for (length, frame_cap, expected_cap) in [
            (cap + 1, role.frame_cap, cap + 8),
            (u32::MAX, role.frame_cap, cap + 8),
            (cap, cap + 7, cap + 7),
        ] {
            let (_send, mut recv) = pair.begin(&header(kind, 0, length)).await?;
            let result = timeout(
                Duration::from_millis(100),
                read_frame(
                    &mut recv,
                    frame_cap,
                    service.message_payload_limits(role),
                    service.message_types(role),
                    service.allowed_frame_flags(role),
                    DEADLINE,
                    None,
                ),
            )
            .await;
            if !matches!(result, Ok(Err(ZakuraHandlerError::OversizeFrame { max_frame_bytes, .. })) if max_frame_bytes == usize::try_from(expected_cap)?)
            {
                failures.push(format!(
                    "kind={kind}, length={length}, cap={frame_cap}: {result:?}"
                ));
            }
        }
    }
    pair.finish().await?;
    assert!(
        failures.is_empty(),
        "F01 early production caps: {failures:#?}"
    );
    Ok(())
}

#[tokio::test]
async fn f01_nonzero_flags_fail_before_payload_allocation_or_wait() -> Result<(), BoxError> {
    let service = BlockSyncService::new(ZakuraBlockSyncConfig::default());
    let mut pair = Pair::new().await?;
    let mut failures = Vec::new();
    for kind in [2u16, 3, 4, 5] {
        let role = service.streams()[usize::from(kind == 2)];
        for flags in [1, u16::MAX] {
            let (_send, mut recv) = pair.begin(&header(kind, flags, 9)).await?;
            let result = timeout(
                Duration::from_millis(100),
                read_frame(
                    &mut recv,
                    role.frame_cap,
                    service.message_payload_limits(role),
                    service.message_types(role),
                    service.allowed_frame_flags(role),
                    DEADLINE,
                    None,
                ),
            )
            .await;
            if !matches!(result, Ok(Err(_))) {
                failures.push(format!("kind={kind}, flags={flags}: {result:?}"));
            }
        }
    }
    pair.finish().await?;
    assert!(
        failures.is_empty(),
        "F01 header flags must reject before reading payload: {failures:#?}"
    );
    Ok(())
}

#[tokio::test]
async fn t03_all_header_and_payload_splits_resume_without_peer_fault() -> Result<(), BoxError> {
    let service = BlockSyncService::new(ZakuraBlockSyncConfig::default());
    let role = service.streams()[1];
    let payload = BlockSyncMessage::GetBlocks {
        start_height: block::Height(100),
        count: 128,
    }
    .encode()?;
    let mut encoded = header(2, 0, u32::try_from(payload.len())?);
    encoded.extend_from_slice(&payload);
    let mut pair = Pair::new().await?;
    for split in 1..encoded.len() {
        let (mut send, mut recv) = pair.begin(&encoded[..split]).await?;
        let reading = read_frame(
            &mut recv,
            role.frame_cap,
            service.message_payload_limits(role),
            service.message_types(role),
            service.allowed_frame_flags(role),
            DEADLINE,
            None,
        );
        tokio::pin!(reading);
        assert!(
            timeout(Duration::from_millis(2), &mut reading)
                .await
                .is_err(),
            "T03 incomplete split={split}"
        );
        timeout(DEADLINE, send.write_all(&encoded[split..])).await??;
        let frame = timeout(DEADLINE, reading).await??;
        assert_eq!(frame.payload, payload, "T03 split={split}");
        assert!(pair.connection.close_reason().is_none());
    }
    pair.finish().await
}

#[tokio::test]
async fn t03_truncated_or_reset_frames_never_reach_the_codec_as_complete() -> Result<(), BoxError> {
    let service = BlockSyncService::new(ZakuraBlockSyncConfig::default());
    let role = service.streams()[1];
    let payload = BlockSyncMessage::GetBlocks {
        start_height: block::Height(100),
        count: 128,
    }
    .encode()?;
    let mut encoded = header(2, 0, u32::try_from(payload.len())?);
    encoded.extend_from_slice(&payload);
    let mut pair = Pair::new().await?;
    for split in 1..encoded.len() {
        for reset in [false, true] {
            let (mut send, mut recv) = pair.begin(&encoded[..split]).await?;
            let reading = read_frame(
                &mut recv,
                role.frame_cap,
                service.message_payload_limits(role),
                service.message_types(role),
                service.allowed_frame_flags(role),
                DEADLINE,
                None,
            );
            tokio::pin!(reading);
            assert!(timeout(Duration::from_millis(2), &mut reading)
                .await
                .is_err());
            if reset {
                send.reset(0u32.into())?;
            } else {
                send.finish()?;
            }
            assert!(
                timeout(DEADLINE, reading).await?.is_err(),
                "T03 split={split}, reset={reset}"
            );
            assert!(pair.connection.close_reason().is_none());
        }
    }
    pair.finish().await
}

#[tokio::test]
async fn t03_in_progress_frame_has_a_finite_read_deadline() -> Result<(), BoxError> {
    let service = BlockSyncService::new(ZakuraBlockSyncConfig::default());
    let role = service.streams()[1];
    let mut pair = Pair::new().await?;
    for prefix in [vec![2], header(2, 0, 9)] {
        let (_send, mut recv) = pair.begin(&prefix).await?;
        let result = timeout(
            DEADLINE,
            read_frame(
                &mut recv,
                role.frame_cap,
                service.message_payload_limits(role),
                service.message_types(role),
                service.allowed_frame_flags(role),
                Duration::from_millis(20),
                None,
            ),
        )
        .await?;
        assert!(
            matches!(result, Err(ZakuraHandlerError::Timeout(_))),
            "T03: {result:?}"
        );
    }
    pair.finish().await
}
