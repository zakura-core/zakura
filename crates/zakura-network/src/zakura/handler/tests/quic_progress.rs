//! Exercise the pinned transport without application queues or serving policy.

use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bidirectional_transfers_exceed_flow_control_windows() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    const ALPN: &[u8] = b"/zakura/testkit/quic-progress/0";
    // Larger than the default send, stream, and connection windows. Both sides
    // must receive acknowledgements and new flow-control credit to finish.
    const BYTES: usize = 64 * 1024 * 1024;
    let limits = ZakuraLocalLimits::from_config(&Config::default());
    let server = LocalEndpointFactory::with_transport_config(limits.transport_config())
        .endpoint(92341)
        .await?;
    let client = LocalEndpointFactory::with_transport_config(limits.transport_config())
        .endpoint(92342)
        .await?;
    let (connection_tx, mut connection_rx) = mpsc::channel(1);
    let (stream_tx, mut stream_rx) = mpsc::channel(1);
    let router = Router::builder(server)
        .accept(
            ALPN,
            CaptureConnection {
                connection_tx,
                stream_tx,
            },
        )
        .spawn();
    let address = LocalEndpointFactory::node_addr(router.endpoint()).await;
    let connection = timeout(Duration::from_secs(10), client.connect(address, ALPN)).await??;
    let remote = timeout(Duration::from_secs(5), connection_rx.recv())
        .await?
        .unwrap();
    let (mut send_a, recv_a) = timeout(Duration::from_secs(5), connection.open_bi()).await??;
    // Make the stream visible to accept_bi before starting the bulk transfer.
    timeout(Duration::from_secs(5), send_a.write_all(&[42])).await??;
    let (mut send_b, recv_b) = timeout(Duration::from_secs(5), stream_rx.recv())
        .await?
        .unwrap();

    let transfer = async {
        tokio::try_join!(
            async {
                send_a.write_all(&vec![42; BYTES]).await?;
                send_a.finish()?;
                Ok::<_, BoxError>(())
            },
            async {
                send_b.write_all(&vec![43; BYTES]).await?;
                send_b.finish()?;
                Ok::<_, BoxError>(())
            },
            drain_stream(recv_a, BYTES, 43),
            drain_stream(recv_b, BYTES + 1, 42),
        )?;
        Ok::<_, BoxError>(())
    };
    timeout(Duration::from_secs(30), transfer)
        .await
        .unwrap_or_else(|_| {
            panic!(
                "QUIC transfer stalled: local={:?}; remote={:?}",
                connection.stats(),
                remote.stats()
            )
        })?;
    connection.close(0u32.into(), b"done");
    client.close().await;
    router.shutdown().await?;
    Ok(())
}

pub(super) async fn drain_stream(
    mut recv: RecvStream,
    expected: usize,
    byte: u8,
) -> Result<(), BoxError> {
    let mut buffer = vec![0; 64 * 1024];
    let mut received = 0;
    while let Some(count) = recv.read(&mut buffer).await? {
        assert!(buffer[..count].iter().all(|value| *value == byte));
        received += count;
        assert!(received <= expected);
    }
    assert_eq!(received, expected);
    Ok(())
}
