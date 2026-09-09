//! Standalone transport acceptance measurements. These are excluded from ordinary CI
//! even when that lane runs ignored tests.

use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "standalone 240-second impaired-link activation gate"]
async fn paired_download_completes_with_request_pressure_and_packet_loss() -> Result<(), BoxError> {
    eprintln!(
        "paired matched download, 32000 requests, 50ms RTT, 1% loss: {:?}",
        download_over_link(true, true).await?
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "standalone combined impairment and paused-service activation gate"]
async fn paired_download_completes_with_request_pressure_paused_service_and_loss(
) -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    eprintln!(
        "paired matched download with pressure, paused service, and loss: {:?}",
        run_download(Workload {
            pressure: true,
            impaired: true,
            paused_siblings: 1,
            ..Workload::default()
        })
        .await?
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "standalone saturation recovery gate using the default 32-second liveness deadline"]
async fn sustained_saturation_cleans_up_and_retries_on_a_fresh_peer() -> Result<(), BoxError> {
    let _guard = zakura_test::init();
    eprintln!(
        "matched retry on a fresh peer: {:?}",
        run_download(Workload {
            paused_siblings: 2,
            recover_on_fresh_peer: true,
            ..Workload::default()
        })
        .await?
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "standalone repeated saturation, cleanup, and matched retry measurement"]
async fn twenty_saturated_connections_release_capacity_and_complete_retries() -> Result<(), BoxError>
{
    let _guard = zakura_test::init();
    for round in 1..=20 {
        let elapsed = run_download(Workload {
            paused_siblings: 2,
            recover_on_fresh_peer: true,
            ..Workload::default()
        })
        .await?;
        eprintln!("matched saturation retry {round}/20: {elapsed:?}");
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "standalone impaired-link throughput diagnostic; does not establish the activation gate"]
async fn raw_download_with_packet_loss_measures_transport_throughput() -> Result<(), BoxError> {
    const BYTES: usize = 4 * 1024 * 1024;
    let limits = ZakuraLocalLimits::from_config(&Config::default());
    let server = LocalEndpointFactory::with_transport_config(limits.transport_config())
        .endpoint(94201)
        .await?;
    let client = LocalEndpointFactory::with_transport_config(limits.transport_config())
        .endpoint(94202)
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
    let proxy =
        link::ImpairedLink::new(*address.direct_addresses().find(|a| a.is_ipv4()).unwrap()).await?;
    let remote_id = address.node_id;
    let address = NodeAddr::new(remote_id).with_direct_addresses([proxy.address]);
    let connection = timeout(DEADLINE, client.connect(address, ALPN)).await??;
    let remote = timeout(DEADLINE, connection_rx.recv()).await?.unwrap();
    let (mut send, mut recv) = connection.open_bi().await?;
    send.write_all(&[1]).await?;
    let (mut response, _request) = timeout(DEADLINE, stream_rx.recv()).await?.unwrap();
    let started = Instant::now();
    timeout(DEADLINE, async {
        tokio::try_join!(
            async {
                response.write_all(&vec![42; BYTES]).await?;
                response.finish()?;
                Ok::<_, BoxError>(())
            },
            async {
                let mut chunk = [0; 64 * 1024];
                let mut received = 0;
                while let Some(count) = recv.read(&mut chunk).await? {
                    assert!(chunk[..count].iter().all(|byte| *byte == 42));
                    received += count;
                }
                assert_eq!(received, BYTES);
                Ok::<_, BoxError>(())
            }
        )?;
        Ok::<_, BoxError>(())
    })
    .await??;
    let elapsed = started.elapsed();
    proxy.verify_path(&client, remote_id, u64::try_from(BYTES)?);
    eprintln!(
        "raw 4 MiB download, 50ms RTT, 1% loss: {elapsed:?}; sender {:?}",
        remote.stats()
    );
    connection.close(0u32.into(), b"done");
    client.close().await;
    router.shutdown().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "transport acceptance gate: twenty matched downloads and real reopen backoff"]
async fn twenty_pair_reopens_complete_matched_downloads_under_request_pressure(
) -> Result<(), BoxError> {
    eprintln!(
        "twenty matched downloads under request pressure: {:?}",
        download_rounds(true, false, 20).await?
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "standalone twenty-round impaired-link activation gate"]
async fn twenty_pair_reopens_complete_matched_downloads_under_request_pressure_and_loss(
) -> Result<(), BoxError> {
    eprintln!(
        "twenty matched downloads under request pressure and loss: {:?}",
        download_rounds(true, true, 20).await?
    );
    Ok(())
}
