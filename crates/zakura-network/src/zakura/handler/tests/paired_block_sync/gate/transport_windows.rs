//! Compare transport credit policies without changing production defaults.

use super::*;

const MIB: u32 = 1024 * 1024;
const SAMPLES: usize = 5;

#[derive(Clone, Copy)]
struct Windows {
    name: &'static str,
    stream_bytes: u32,
    connection_bytes: u32,
    remote_streams: u32,
}

impl Windows {
    fn config(self) -> QuicTransportConfig {
        ZakuraLocalLimits::from_config(&Config::default())
            .transport_config_builder()
            .max_concurrent_bidi_streams(self.remote_streams.into())
            .stream_receive_window(self.stream_bytes.into())
            .receive_window(self.connection_bytes.into())
            .build()
    }
}

// Freeze the receive settings from the pre-compliance transport. The fixture,
// serving policy, build and send window stay identical across measurements.
const BASELINE: Windows = Windows {
    name: "baseline",
    stream_bytes: 16 * MIB,
    connection_bytes: 32 * MIB,
    remote_streams: 1024,
};
const CANDIDATES: [Windows; 3] = [
    Windows {
        name: "256KiB",
        stream_bytes: MIB / 4,
        connection_bytes: 8 * MIB,
        remote_streams: 16,
    },
    Windows {
        name: "1MiB",
        stream_bytes: MIB,
        connection_bytes: 32 * MIB,
        remote_streams: 16,
    },
    Windows {
        name: "4MiB",
        stream_bytes: 4 * MIB,
        connection_bytes: 128 * MIB,
        remote_streams: 16,
    },
];

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "standalone five-sample window experiment on lossless and impaired links"]
async fn compare_uniform_receive_windows() -> Result<(), BoxError> {
    use std::io::Write;

    let _guard = zakura_test::init();
    let mut journal = std::env::var_os("ZAKURA_TRANSPORT_MEASUREMENTS")
        .map(std::fs::File::create_new)
        .transpose()?;
    if let Some(journal) = &mut journal {
        writeln!(
            journal,
            "impaired,policy,sample,stream_bytes,connection_bytes,remote_streams,elapsed_ns"
        )?;
    }
    for impaired in [false, true] {
        let mut samples = [[Duration::ZERO; 4]; SAMPLES];
        for (sample, durations) in samples.iter_mut().enumerate() {
            // Rotate order so one configuration is not always measured first.
            let policies = [BASELINE, CANDIDATES[0], CANDIDATES[1], CANDIDATES[2]];
            for offset in 0..policies.len() {
                let index = (sample + offset) % policies.len();
                let policy = policies[index];
                let elapsed = run_download(Workload {
                    transport: Some(policy.config()),
                    impaired,
                    ..Workload::default()
                })
                .await?;
                durations[index] = elapsed;
                eprintln!(
                    "window sample: impaired={impaired} policy={} sample={} elapsed_ns={}",
                    policy.name,
                    sample + 1,
                    elapsed.as_nanos(),
                );
                if let Some(journal) = &mut journal {
                    writeln!(
                        journal,
                        "{impaired},{},{},{},{},{},{}",
                        policy.name,
                        sample + 1,
                        policy.stream_bytes,
                        policy.connection_bytes,
                        policy.remote_streams,
                        elapsed.as_nanos(),
                    )?;
                    journal.flush()?;
                }
            }
        }
        let medians: [Duration; 4] = std::array::from_fn(|policy| {
            let mut durations: [Duration; SAMPLES] =
                std::array::from_fn(|sample| samples[sample][policy]);
            durations.sort_unstable();
            durations[SAMPLES / 2]
        });
        for (index, policy) in CANDIDATES.iter().enumerate() {
            // Each sample transfers the same useful bytes and consumes every
            // ending on its original connection, so this is a throughput ratio.
            let ratio = medians[0].as_secs_f64() / medians[index + 1].as_secs_f64();
            eprintln!(
                "window median: impaired={impaired} policy={} throughput_ratio={ratio:.6} meets_90_percent={}",
                policy.name,
                ratio >= 0.9,
            );
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "standalone acknowledged-byte headroom experiment"]
async fn acknowledged_unread_streams_expose_connection_credit_exhaustion() -> Result<(), BoxError> {
    // Scale the old 2:1 policy down. The same accounting failure occurs after
    // exactly two acknowledged windows, without relying on UDP traffic totals.
    assert!(!unread_headroom(2, 2).await?);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "standalone acknowledged-byte headroom experiment"]
async fn connection_credit_covers_both_stream_directions_before_admission() -> Result<(), BoxError>
{
    // Four remotely opened streams and three locally opened streams remain
    // unread. The fourth local stream must progress before any sibling resumes.
    assert!(unread_headroom(7, 8).await?);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "standalone connection credit update threshold experiment"]
async fn summed_stream_windows_can_stall_before_the_next_credit_update() -> Result<(), BoxError> {
    // Eight paused streams leave one window for the extra local stream. That
    // stream cannot reach the connection's 9/8-window credit update threshold.
    assert!(!unread_headroom(8, 9).await?);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "standalone connection credit update threshold experiment"]
async fn headroom_includes_the_next_connection_credit_update() -> Result<(), BoxError> {
    assert!(unread_headroom(8, 10).await?);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "standalone maximum candidate stream credit experiment"]
async fn headroom_covers_all_candidate_streams_and_one_extra_local_stream() -> Result<(), BoxError>
{
    assert!(unread_headroom(32, 38).await?);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_policy_keeps_progress_with_all_sibling_streams_unread() -> Result<(), BoxError> {
    let limits = ZakuraLocalLimits::from_config(&Config::default());
    assert!(
        unread_headroom_with_config(
            32,
            limits.transport_config(),
            DEFAULT_ZAKURA_STREAM_RECEIVE_WINDOW,
            DEFAULT_ZAKURA_RECEIVE_WINDOW,
        )
        .await?
    );
    Ok(())
}

async fn unread_headroom(paused: usize, connection_windows: u32) -> Result<bool, BoxError> {
    const WINDOW: u32 = 64 * 1024;
    let config = Windows {
        name: "acknowledged-headroom",
        stream_bytes: WINDOW,
        connection_bytes: WINDOW * connection_windows,
        remote_streams: u32::try_from(paused.div_ceil(2).max(1))?,
    }
    .config();
    unread_headroom_with_config(paused, config, WINDOW, WINDOW * connection_windows).await
}

async fn unread_headroom_with_config(
    paused: usize,
    config: QuicTransportConfig,
    window_bytes: u32,
    connection_bytes: u32,
) -> Result<bool, BoxError> {
    const ALPN: &[u8] = b"/zakura/test/acknowledged-headroom/1";
    let server = LocalEndpointFactory::with_transport_config(config.clone())
        .endpoint(971_020)
        .await?;
    server.set_alpns(vec![ALPN.to_vec()]);
    let client = LocalEndpointFactory::with_transport_config(config)
        .endpoint(971_021)
        .await?;
    let address = server.addr();
    let (connection, remote) = timeout(DEADLINE, async {
        tokio::try_join!(
            async { Ok::<_, BoxError>(client.connect(address, ALPN).await?) },
            async {
                let incoming = server.accept().await.ok_or("endpoint closed")?;
                Ok::<_, BoxError>(incoming.accept()?.await?)
            },
        )
    })
    .await??;
    let result = timeout(DEADLINE, async {
        let payload = vec![42; usize::try_from(window_bytes)?];
        let mut unread = Vec::new();
        let mut unused_halves = Vec::new();
        for index in 0..paused {
            let recv = if index % 2 == 0 {
                let (mut send, unused_recv) = remote.open_bi().await?;
                send.write_all(&payload).await?;
                send.finish()?;
                // All bytes are acknowledged before accept_bi. Application
                // stream admission cannot undo this previously granted credit.
                assert_eq!(send.stopped().await?, None);
                let (unused_send, recv) = connection.accept_bi().await?;
                unused_halves.push((unused_send, unused_recv));
                recv
            } else {
                let (mut send, recv) = connection.open_bi().await?;
                send.write_all(&[99]).await?;
                send.finish()?;
                let (mut response, mut request) = remote.accept_bi().await?;
                assert_eq!(request.read_to_end(1).await?, [99]);
                response.write_all(&payload).await?;
                response.finish()?;
                assert_eq!(response.stopped().await?, None);
                recv
            };
            unread.push(recv);
        }
        let (mut request, response) = connection.open_bi().await?;
        request.write_all(&[99]).await?;
        request.finish()?;
        let (mut send, mut recv) = remote.accept_bi().await?;
        assert_eq!(recv.read_to_end(1).await?, [99]);
        let probe = async {
            // Exercise multiple connection-credit updates, not just one frame
            // that happens to fit in the initially available credit.
            let probe_bytes = usize::try_from(connection_bytes)? * 2;
            tokio::try_join!(
                async {
                    send.write_all(&vec![43; probe_bytes]).await?;
                    send.finish()?;
                    Ok::<_, BoxError>(())
                },
                super::super::super::quic_progress::drain_stream(response, probe_bytes, 43),
            )?;
            Ok::<_, BoxError>(())
        };
        tokio::pin!(probe);
        let progressed = match timeout(Duration::from_millis(500), &mut probe).await {
            Ok(result) => {
                result?;
                true
            }
            Err(_) => false,
        };
        // Resumption must complete the exact original streams and the same
        // pending probe, even for the deliberately exhausted negative control.
        for recv in unread {
            super::super::super::quic_progress::drain_stream(
                recv,
                usize::try_from(window_bytes)?,
                42,
            )
            .await?;
        }
        if !progressed {
            probe.await?;
        }
        assert!(connection.close_reason().is_none());
        assert!(remote.close_reason().is_none());
        drop(unused_halves);
        Ok::<_, BoxError>(progressed)
    })
    .await;
    connection.close(0u32.into(), b"headroom experiment finished");
    client.close().await;
    server.close().await;
    result?
}
