//! Transport owners survive application closure and release after final state.

use super::*;
use crate::zakura::regulation::{SlotBudget, SlotPermit};

const DEADLINE: Duration = Duration::from_secs(30);
const ALPN: &[u8] = b"/zakura/test/transport-owner/1";

struct Owner {
    permit: Option<SlotPermit>,
    released: Option<oneshot::Sender<()>>,
}

impl Drop for Owner {
    fn drop(&mut self) {
        drop(self.permit.take());
        if let Some(released) = self.released.take() {
            let _ = released.send(());
        }
    }
}

fn owner(budget: &SlotBudget) -> (Box<dyn std::any::Any + Send + Sync>, oneshot::Receiver<()>) {
    let (released, received) = oneshot::channel();
    (
        Box::new(Owner {
            permit: Some(budget.try_reserve().unwrap()),
            released: Some(released),
        }),
        received,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn closed_transport_keeps_its_owner_while_an_unread_receive_half_exists(
) -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    const WINDOW: u32 = 256 * 1024;
    let config = ZakuraLocalLimits::from_config(&Config::default())
        .transport_config_builder()
        .stream_receive_window(WINDOW.into())
        .receive_window((2 * WINDOW).into())
        .build();
    let server = LocalEndpointFactory::with_transport_config(config.clone())
        .endpoint(971_030)
        .await?;
    let client = LocalEndpointFactory::with_transport_config(config)
        .endpoint(971_031)
        .await?;
    server.set_alpns(vec![ALPN.to_vec()]);
    let budget = SlotBudget::new(1).unwrap();
    let (reservation, mut released) = owner(&budget);
    let (connection, remote) = timeout(DEADLINE, async {
        tokio::try_join!(
            async { Ok::<_, BoxError>(client.connect(server.addr(), ALPN).await?) },
            async {
                let incoming = server.accept().await.ok_or("endpoint closed")?;
                Ok::<_, BoxError>(incoming.accept_owned(None, reservation)?.await?)
            },
        )
    })
    .await??;
    let (mut send, peer_receive) = timeout(DEADLINE, connection.open_bi()).await??;
    timeout(DEADLINE, async {
        send.write_all(&vec![42; usize::try_from(WINDOW)?]).await?;
        send.finish()?;
        assert_eq!(send.stopped().await?, None);
        Ok::<_, BoxError>(())
    })
    .await??;
    // All bytes and FIN were acknowledged before the receiver was admitted.
    let (unused_send, unread) = timeout(DEADLINE, remote.accept_bi()).await??;
    remote.close(0u32.into(), b"owner lifetime test");
    timeout(DEADLINE, remote.closed()).await?;
    drop(unused_send);
    drop(remote);
    assert_eq!(budget.reserved(), 1);
    assert!(budget.try_reserve().is_none());
    assert!(matches!(
        released.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    drop(unread);
    timeout(DEADLINE, released).await??;
    assert_eq!(budget.reserved(), 0);
    drop((send, peer_receive, connection));
    timeout(DEADLINE, client.close()).await?;
    timeout(DEADLINE, server.close()).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transport_owner_releases_on_preconstruction_error_and_cancelled_handshake(
) -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let config = ZakuraLocalLimits::from_config(&Config::default()).transport_config();
    let server = LocalEndpointFactory::with_transport_config(config.clone())
        .endpoint(971_032)
        .await?;
    let client = LocalEndpointFactory::with_transport_config(config)
        .endpoint(971_033)
        .await?;
    server.set_alpns(vec![ALPN.to_vec()]);
    let budget = SlotBudget::new(1).unwrap();
    let (reservation, released) = owner(&budget);
    assert!(client
        .connect_with_owner(
            server.addr(),
            b"",
            iroh::endpoint::ConnectOptions::new(),
            reservation,
        )
        .await
        .is_err());
    timeout(DEADLINE, released).await??;
    assert_eq!(budget.reserved(), 0);

    let (reservation, released) = owner(&budget);
    let connecting = timeout(
        DEADLINE,
        client.connect_with_owner(
            server.addr(),
            ALPN,
            iroh::endpoint::ConnectOptions::new(),
            reservation,
        ),
    )
    .await??;
    // The server never accepts this handshake. Cancellation must eventually
    // return the reservation even though the caller never obtains a Connection.
    drop(connecting);
    timeout(DEADLINE, client.close()).await?;
    timeout(DEADLINE, released).await??;
    assert_eq!(budget.reserved(), 0);
    timeout(DEADLINE, server.close()).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_router_reserves_transport_before_handshake() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let identity = tempfile::tempdir()?;
    let mut config = Config::for_test(P2pStack::Dual);
    config.identity_dir = identity.path().to_owned();
    config.zakura.bootstrap_peers.clear();
    config.zakura.max_connections = 1;
    let node = spawn_zakura_endpoint(&config, |_, _| Arc::new(NoopService))
        .await?
        .ok_or("native endpoint disabled")?;
    let client = LocalEndpointFactory::default().endpoint(971_034).await?;
    let address = node.node_addr().await;

    let owner = node.handler.reserve_transport()?;
    assert!(!node.has_native_admission_capacity());
    assert!(
        timeout(DEADLINE, client.connect(address.clone(), P2P_V2_ALPN))
            .await?
            .is_err()
    );
    assert_eq!(node.handler.admission.available_permits(), 1);

    drop(owner);
    let connection = timeout(DEADLINE, client.connect(address, P2P_V2_ALPN)).await??;
    // No application control hello has been sent. Transport is already charged.
    assert_eq!(node.handler.transport_admission.available_permits(), 0);
    connection.close(0u32.into(), b"admission test");
    drop(connection);
    let reclaimed = timeout(
        DEADLINE,
        node.handler.transport_admission.clone().acquire_owned(),
    )
    .await??;
    drop(reclaimed);
    timeout(DEADLINE, client.close()).await?;
    timeout(DEADLINE, node.shutdown()).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn closing_inbound_transport_blocks_native_dial_until_last_handle_retires(
) -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let identity = tempfile::tempdir()?;
    let mut config = Config::for_test(P2pStack::Dual);
    config.identity_dir = identity.path().to_owned();
    config.zakura.bootstrap_peers.clear();
    config.zakura.max_connections = 1;
    let node = spawn_zakura_endpoint(&config, |_, _| Arc::new(NoopService))
        .await?
        .ok_or("native endpoint disabled")?;
    let server = LocalEndpointFactory::default().endpoint(971_035).await?;
    let client = LocalEndpointFactory::default().endpoint(971_036).await?;
    let (connection_tx, mut connection_rx) = mpsc::channel(1);
    let (stream_tx, mut stream_rx) = mpsc::channel(1);
    let router = Router::builder(server)
        .incoming_admission(node.handler.incoming_transport_admission())
        .accept(
            ALPN,
            CaptureConnection {
                connection_tx,
                stream_tx,
            },
        )
        .spawn();
    let connection = timeout(DEADLINE, client.connect(router.endpoint().addr(), ALPN)).await??;
    let remote = timeout(DEADLINE, connection_rx.recv())
        .await?
        .ok_or("no connection")?;
    let (mut send, peer_receive) = timeout(DEADLINE, connection.open_bi()).await??;
    timeout(DEADLINE, send.write_all(b"held after close")).await??;
    send.finish()?;
    assert_eq!(timeout(DEADLINE, send.stopped()).await??, None);
    let (unused_send, unread) = timeout(DEADLINE, stream_rx.recv())
        .await?
        .ok_or("no stream")?;
    remote.close(0u32.into(), b"closing admission test");
    timeout(DEADLINE, remote.closed()).await?;
    drop((unused_send, remote));
    assert_eq!(node.handler.admission.available_permits(), 1);
    let limits = ZakuraLocalLimits::from_config(&config);
    let result = serve_native_dial_connection(&node, client.addr(), &limits).await;
    assert!(matches!(
        result,
        Err(ZakuraHandlerError::ResourceLimit("transport admission"))
    ));
    assert_eq!(node.handler.transport_admission.available_permits(), 0);

    drop(unread);
    let reclaimed = timeout(
        DEADLINE,
        node.handler.transport_admission.clone().acquire_owned(),
    )
    .await??;
    drop(reclaimed);
    assert!(node.has_native_admission_capacity());
    drop((send, peer_receive, connection));
    timeout(DEADLINE, router.shutdown()).await??;
    timeout(DEADLINE, client.close()).await?;
    timeout(DEADLINE, node.shutdown()).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stopped_local_stream_reopens_only_after_transport_final_offset() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    let config = ZakuraLocalLimits::from_config(&Config::default())
        .transport_config_builder()
        .max_concurrent_bidi_streams(16u32.into())
        .max_concurrent_local_bidi_streams(1u32.into())
        .max_concurrent_local_uni_streams(0u32.into())
        .build();
    let server = LocalEndpointFactory::with_transport_config(config.clone())
        .endpoint(971_037)
        .await?;
    let client = LocalEndpointFactory::with_transport_config(config)
        .endpoint(971_038)
        .await?;
    server.set_alpns(vec![ALPN.to_vec()]);
    let (connection, remote) = timeout(DEADLINE, async {
        tokio::try_join!(
            async { Ok::<_, BoxError>(client.connect(server.addr(), ALPN).await?) },
            async { Ok::<_, BoxError>(server.accept().await.ok_or("endpoint closed")?.await?) },
        )
    })
    .await??;
    let (mut send, mut receive) = timeout(DEADLINE, connection.open_bi()).await??;
    timeout(DEADLINE, send.write_all(b"request")).await??;
    send.finish()?;
    let (mut peer_send, mut peer_receive) = timeout(DEADLINE, remote.accept_bi()).await??;
    assert_eq!(
        timeout(DEADLINE, peer_receive.read_to_end(7)).await??,
        b"request"
    );
    assert_eq!(timeout(DEADLINE, send.stopped()).await??, None);
    timeout(DEADLINE, peer_send.write_all(b"x")).await??;
    timeout(DEADLINE, receive.read_exact(&mut [0u8; 1])).await??;
    receive.stop(0u32.into())?;
    assert_eq!(
        timeout(DEADLINE, peer_send.stopped()).await??,
        Some(0u32.into())
    );

    let opening = connection.open_bi();
    tokio::pin!(opening);
    assert!(timeout(Duration::from_millis(100), &mut opening)
        .await
        .is_err());
    // RESET supplies the final offset and wakes the already waiting open_bi.
    peer_send.reset(0u32.into())?;
    let replacement = timeout(DEADLINE, &mut opening).await??;
    drop(replacement);
    connection.close(0u32.into(), b"stream limit test");
    drop((send, receive, peer_send, peer_receive, remote));
    timeout(DEADLINE, client.close()).await?;
    timeout(DEADLINE, server.close()).await?;
    Ok(())
}
