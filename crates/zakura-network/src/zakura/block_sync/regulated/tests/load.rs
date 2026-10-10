//! Manual capacity evidence. Fixture CPU/queue throughput is not a disk or QUIC benchmark.

use super::*;
use crate::zakura::block_sync::{
    tests::fake_block_at_height, ZakuraBlockSyncConfig, MAX_BS_RESPONSE_BYTES,
};

#[derive(Debug)]
struct FixtureSource {
    template: Arc<block::Block>,
    available: bool,
    delay: Duration,
}

impl Source for FixtureSource {
    fn read(&self, request: Read) -> BoxFuture<'static, Result<ReadResult, crate::BoxError>> {
        let template = self.template.clone();
        let available = self.available;
        let delay = self.delay;
        Box::pin(async move {
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            Ok(tokio::task::spawn_blocking(move || {
                let blocks = if request.lease.try_start() && available {
                    (0..request.count)
                        .map(|offset| {
                            let height = block::Height(request.start_height.0 + offset);
                            (height, fake_block_at_height(&template, height))
                        })
                        .collect()
                } else {
                    Vec::new()
                };
                ReadResult {
                    blocks,
                    lease: request.lease,
                }
            })
            .await?)
        })
    }
}

#[tokio::test]
#[ignore = "manual capacity evidence; requires ZAKURA_CAPACITY_PROBE=1"]
#[allow(clippy::print_stderr)]
async fn serving_capacity_candidates() {
    if std::env::var_os("ZAKURA_CAPACITY_PROBE").is_none() {
        return;
    }
    let template: Arc<block::Block> =
        Arc::new(BLOCK_MAINNET_1_BYTES.zcash_deserialize_into().unwrap());
    for window in [32, 128, 512, 1_500, 32_000] {
        let config = ZakuraBlockSyncConfig {
            max_inflight_requests: window,
            ..Default::default()
        };
        measure_waiting_getblocks_capacity(1, &config);
        for (shape, available, count, total, delay) in [
            ("unavailable", false, 1, 2048, Duration::ZERO),
            ("one_small_block", true, 1, 2048, Duration::ZERO),
            ("configured_128_block_range", true, 128, 128, Duration::ZERO),
            (
                "one_small_block_500ms_read",
                true,
                1,
                128,
                Duration::from_millis(500),
            ),
        ] {
            let config = ZakuraBlockSyncConfig {
                max_blocks_per_response: count,
                ..config.clone()
            };
            let source = Arc::new(FixtureSource {
                template: template.clone(),
                available,
                delay,
            });
            let serving = super::super::session::Serving::new(source, &config);
            let cancel = CancellationToken::new();
            let peer = ZakuraPeerId::new(vec![17; 32]).unwrap();
            let (send, mut output) = framed_channel(64);
            let mut session = serving.session(
                &peer,
                send,
                cancel.clone(),
                cancel.clone(),
                crate::zakura::CloseCause::default(),
            );
            let (mut issued, mut completed, mut bodies, mut wire_bytes) =
                (0_u32, 0_u32, 0_u32, 0_u64);
            let started = std::time::Instant::now();
            while completed < total {
                while issued < total && issued - completed < window {
                    session
                        .admit(block::Height(issued * count + 1), count)
                        .unwrap();
                    issued += 1;
                }
                let frame = tokio::time::timeout(Duration::from_secs(30), output.recv())
                    .await
                    .unwrap()
                    .unwrap();
                wire_bytes +=
                    u64::try_from(frame.payload.len() + crate::zakura::FRAME_HEADER_BYTES).unwrap();
                if frame.message_type == 3 {
                    bodies += 1;
                }
                if matches!(frame.message_type, 4 | 5) {
                    completed += 1;
                }
            }
            assert_eq!(bodies, if available { total * count } else { 0 });
            let elapsed = started.elapsed();
            let bytes_per_response = wire_bytes / u64::from(total);
            let bdp_entries = (2 * crate::zakura::regulation::sizing::bandwidth_delay_bytes())
                .div_ceil(bytes_per_response);
            eprintln!("fixture shape={shape} window={window} responses={total} seconds={:.6} wire_bytes={wire_bytes} bytes_per_response={bytes_per_response} requests_per_second={:.1} entries_for_twice_target_bdp={bdp_entries}", elapsed.as_secs_f64(), f64::from(total) / elapsed.as_secs_f64());
            cancel.cancel();
        }
    }
}

/// Compare the same queued requests before and after adding completion tracking
/// and GetBlocks range ownership. No worker runs inside a measurement.
#[tokio::test]
#[ignore = "manual allocation evidence; requires ZAKURA_CAPACITY_PROBE=1"]
#[allow(clippy::print_stderr)]
async fn waiting_commitment_allocation_breakdown() {
    if std::env::var_os("ZAKURA_CAPACITY_PROBE").is_none() {
        return;
    }
    let count = 64_000_u32;
    let peer = ZakuraPeerId::new(vec![18; 32]).unwrap();
    for tracked in [false, true] {
        let limits = capacity(MAX_BS_RESPONSE_BYTES);
        let producer = Arc::new(Server::new(store(&[], false), 1, MAX_BS_RESPONSE_BYTES).unwrap());
        let cancel = CancellationToken::new();
        let (send, output) = framed_channel(4);
        let serve = limits.session(
            producer,
            &peer,
            count / 2,
            send,
            cancel.clone(),
            cancel.clone(),
            crate::zakura::CloseCause::default(),
        );
        let completions = crate::zakura::regulation::Completions::default();
        let (_, measured) = zakura_test::allocations::measure(|| {
            for height in 1..=count {
                let range = Range::new(block::Height(height), 1).unwrap();
                if tracked {
                    serve
                        .admit_tracked(range, completions.track(u64::from(height)))
                        .unwrap();
                } else {
                    serve.admit(range).unwrap();
                }
            }
        });
        assert_eq!(serve.open(), count);
        eprintln!("queued_shared tracked={tracked} count={count} {measured:?}");
        cancel.cancel();
        drop((serve, output, completions));
        tokio::task::yield_now().await;
    }
    measure_waiting_getblocks_sessions(1);
}

#[test]
#[ignore = "manual allocation evidence; requires ZAKURA_CAPACITY_PROBE=1"]
#[allow(clippy::print_stderr)]
fn requester_reservation_allocations() {
    use super::super::requester::Requester;
    use crate::zakura::regulation::{ReservationPool, WriterFence};

    if std::env::var_os("ZAKURA_CAPACITY_PROBE").is_none() {
        return;
    }
    for entries in [16_384, 32_768] {
        for count in [1_u32, 128] {
            let hashes: Vec<_> = (0..entries * usize::try_from(count).unwrap())
                .map(|index| {
                    let mut bytes = [0; 32];
                    bytes[..8].copy_from_slice(&u64::try_from(index).unwrap().to_le_bytes());
                    block::Hash(bytes)
                })
                .collect();
            let pool = ReservationPool::new(entries).unwrap();
            let fence = WriterFence::new(
                CancellationToken::new(),
                crate::zakura::CloseCause::default(),
            );
            let (requester, measured) = zakura_test::allocations::measure(|| {
                let mut requester = Requester::new(entries);
                for (index, expected) in hashes
                    .chunks_exact(usize::try_from(count).unwrap())
                    .enumerate()
                {
                    let start = block::Height(u32::try_from(index).unwrap() * count + 1);
                    requester
                        .reserve(
                            Range::new(start, count).unwrap(),
                            expected,
                            MAX_BS_RESPONSE_BYTES,
                            pool.try_entry().unwrap(),
                            fence.open().unwrap(),
                        )
                        .unwrap();
                }
                requester
            });
            assert_eq!(pool.held(), entries);
            eprintln!("requester entries={entries} hashes_per_entry={count} {measured:?}");
            drop(requester);
            assert_eq!(pool.held(), 0);
        }
    }
}
