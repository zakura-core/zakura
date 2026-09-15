//! Exercise the same admission rules with block sync and a discovery test policy.
//!
//! An independent message count predicts the rate allowance. A stopped test
//! clock makes bursts and exhaustion deterministic, including on the real worker.
//! These checks cover admission. The services still own response matching and work.

use super::*;
use crate::zakura::{
    testkit::TestClock, BlockSyncMessage, MessageRatePolicy, MSG_BS_BLOCK, MSG_BS_BLOCKS_DONE,
    MSG_BS_GET_BLOCKS, MSG_BS_RANGE_UNAVAILABLE, MSG_BS_STATUS, MSG_DISCOVERY_GET_PEERS,
    MSG_DISCOVERY_GET_SERVICES, MSG_DISCOVERY_HELLO, MSG_DISCOVERY_PEERS, MSG_DISCOVERY_SERVICES,
};
use proptest::{prelude::*, test_runner::TestCaseError};

const PAYLOAD_LIMIT: usize = 64;

#[derive(Clone, Copy)]
struct MessageCase {
    stream: Stream,
    message_type: u16,
    rate_limited: bool,
}

fn context<C: Clock>(
    registry: &ServiceRegistry,
    stream: Stream,
    bucket: SharedMessageBucket<C>,
    clock: C,
) -> StreamWorkerContext<C> {
    let cancel = CancellationToken::new();
    let (freshness_tx, _) = watch::channel(clock.now());
    let mut limits = test_connection_limits();
    limits.max_message_bytes = u32::try_from(PAYLOAD_LIMIT).unwrap();
    StreamWorkerContext {
        conn: ZakuraConnTrace::without_peer(1),
        peer_id: test_peer(89),
        stream_id: 1,
        _permit: Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap(),
        limits,
        inbound_frame_cap: stream.frame_cap,
        message_payload_limits: registry.message_payload_limits(stream),
        message_types: registry.message_types(stream),
        message_rate_policy: registry.message_rate_policy(stream),
        allowed_frame_flags: registry.allowed_frame_flags(stream),
        queue_depths: None,
        write_policy: StreamWritePolicy::Timeout(Duration::from_secs(5)),
        session_resources: None,
        outbound_frame_cap: stream.frame_cap,
        message_bucket: bucket,
        connection_token: cancel.clone(),
        stream_token: cancel.child_token(),
        close_cause: CloseCause::new(),
        freshness_tx,
    }
}

fn block_sync() -> (ServiceRegistry, Vec<MessageCase>) {
    let service = Arc::new(BlockSyncService::new_for_test(
        ZakuraBlockSyncConfig::default(),
    ));
    let data = service.streams()[0];
    let requests = service.streams()[1];
    let cases = [
        (data, MSG_BS_STATUS, true),
        (requests, MSG_BS_GET_BLOCKS, false),
        (data, MSG_BS_BLOCK, false),
        (data, MSG_BS_BLOCKS_DONE, false),
        (data, MSG_BS_RANGE_UNAVAILABLE, false),
    ]
    .map(|(stream, message_type, rate_limited)| MessageCase {
        stream,
        message_type: u16::from(message_type),
        rate_limited,
    });
    (ServiceRegistry::new(vec![service]).unwrap(), cases.to_vec())
}

/// Demonstrate the response policy for another protocol without changing
/// production discovery's cadence or response handling in this PR.
#[derive(Debug)]
struct DiscoveryPolicy;

impl Service for DiscoveryPolicy {
    fn name(&self) -> &'static str {
        "discovery-admission-test"
    }
    fn streams(&self) -> &[Stream] {
        &[Stream {
            kind: DISCOVERY_STREAM_KIND,
            version: 1,
            capability: ZAKURA_CAP_DISCOVERY,
            frame_cap: 256,
            mode: StreamMode::Persistent,
        }]
    }
    fn message_rate_policy(&self, _: Stream) -> MessageRatePolicy {
        // Widening these u8 discriminators to u16 preserves their values.
        MessageRatePolicy::CapacityBounded(&[
            MSG_DISCOVERY_PEERS as u16,
            MSG_DISCOVERY_SERVICES as u16,
        ])
    }
    fn add_peer(&self, _: Peer) {}
    fn remove_peer(&self, _: &ZakuraPeerId, _: ZakuraConnId) {}
}

fn discovery() -> (ServiceRegistry, Vec<MessageCase>) {
    let service = Arc::new(DiscoveryPolicy);
    let stream = service.streams()[0];
    let cases = [
        (MSG_DISCOVERY_HELLO, true),
        (MSG_DISCOVERY_GET_PEERS, true),
        (MSG_DISCOVERY_GET_SERVICES, true),
        (MSG_DISCOVERY_PEERS, false),
        (MSG_DISCOVERY_SERVICES, false),
    ]
    .map(|(message_type, rate_limited)| MessageCase {
        stream,
        message_type: u16::from(message_type),
        rate_limited,
    });
    (ServiceRegistry::new(vec![service]).unwrap(), cases.to_vec())
}

/// The oracle counts only accepted, size-valid rate-limited messages between
/// full refills. It never reads the production bucket or the policy declaration.
fn check_history(
    registry: ServiceRegistry,
    cases: &[MessageCase],
    rate: u32,
    history: &[(usize, usize, bool)],
) -> Result<(), TestCaseError> {
    let clock = TestClock::new();
    let bucket = Arc::new(StdMutex::new(TokenBucket::with_clock(rate, clock.clone())));
    let contexts: Vec<_> = cases
        .iter()
        .map(|case| context(&registry, case.stream, bucket.clone(), clock.clone()))
        .collect();
    let mut remaining = rate;
    for &(choice, payload_len, refill) in history {
        if refill {
            clock.advance(Duration::from_secs(1));
            remaining = rate;
        }
        let index = choice % cases.len();
        let case = cases[index];
        let expected = if payload_len > PAYLOAD_LIMIT {
            InboundMessageAdmission::Oversize
        } else if !case.rate_limited {
            InboundMessageAdmission::Admit
        } else if remaining > 0 {
            remaining -= 1;
            InboundMessageAdmission::Admit
        } else {
            InboundMessageAdmission::Throttled
        };
        let frame = Frame {
            message_type: case.message_type,
            flags: 0,
            payload: vec![0; payload_len],
        };
        prop_assert_eq!(
            admit_inbound_message(&frame, &contexts[index], case.stream.kind),
            expected,
            "message={}, bytes={}, remaining={}",
            case.message_type,
            payload_len,
            remaining
        );
    }
    Ok(())
}

proptest! {
    #[test]
    fn generated_mixed_messages_preserve_rate_and_size_bounds(
        rate in 1u32..9,
        history in prop::collection::vec((0usize..256, 0usize..=PAYLOAD_LIMIT + 1, any::<bool>()), 1..256),
    ) {
        let (registry, cases) = block_sync();
        check_history(registry, &cases, rate, &history)?;
        let (registry, cases) = discovery();
        check_history(registry, &cases, rate, &history)?;
    }
}

#[test]
fn response_bursts_preserve_the_last_metadata_token_and_continue_after_exhaustion() {
    for (registry, cases) in [block_sync(), discovery()] {
        let mut history = Vec::new();
        for _ in 0..4096 {
            history.extend(cases.iter().enumerate().filter_map(|(index, case)| {
                (!case.rate_limited).then_some((index, PAYLOAD_LIMIT, false))
            }));
        }
        // Metadata still gets its one token after all those responses. The next
        // metadata message is rejected, while every capacity-bounded type works.
        history.extend([(0, PAYLOAD_LIMIT, false), (0, PAYLOAD_LIMIT, false)]);
        history.extend((0..cases.len()).map(|index| (index, PAYLOAD_LIMIT, false)));
        check_history(registry, &cases, 1, &history).unwrap();
    }
}

#[test]
fn unlisted_messages_and_services_keep_the_rate_limit() {
    #[derive(Debug)]
    struct DefaultService(Stream);
    impl Service for DefaultService {
        fn name(&self) -> &'static str {
            "default-admission-test"
        }
        fn streams(&self) -> &[Stream] {
            std::slice::from_ref(&self.0)
        }
        fn add_peer(&self, _: Peer) {}
        fn remove_peer(&self, _: &ZakuraPeerId, _: ZakuraConnId) {}
    }

    let (registry, mut cases) = block_sync();
    let stream = cases[0].stream;
    cases.push(MessageCase {
        stream,
        message_type: u16::MAX,
        rate_limited: true,
    });
    let unknown = cases.len() - 1;
    check_history(
        registry,
        &cases,
        1,
        &[(unknown, 0, false), (unknown, 0, false)],
    )
    .unwrap();
    check_history(
        ServiceRegistry::new(vec![Arc::new(DefaultService(stream))]).unwrap(),
        &[MessageCase {
            stream,
            message_type: u16::from(MSG_BS_BLOCK),
            rate_limited: true,
        }],
        1,
        &[(0, 0, false), (0, 0, false)],
    )
    .unwrap();
}

#[tokio::test]
async fn ordered_worker_keeps_metadata_rate_enforcement_and_its_close_cause() -> Result<(), BoxError>
{
    const DEADLINE: Duration = Duration::from_secs(5);
    const ALPN: &[u8] = b"/zakura/test/message-admission/1";
    let (registry, cases) = block_sync();
    let stream = cases[0].stream;
    let server = LocalEndpointFactory::new().endpoint(98101).await?;
    let (conn_tx, _conn_rx) = mpsc::channel(1);
    let (stream_tx, mut streams) = mpsc::channel(1);
    let router = Router::builder(server)
        .accept(
            ALPN,
            CaptureConnection {
                connection_tx: conn_tx,
                stream_tx,
            },
        )
        .spawn();
    let client = LocalEndpointFactory::new().endpoint(98102).await?;
    let connection = timeout(DEADLINE, client.connect(router.endpoint().addr(), ALPN)).await??;
    let (mut sender, _receiver) = timeout(DEADLINE, connection.open_bi()).await??;
    let status = BlockSyncMessage::Status(ZakuraBlockSyncConfig::default().initial_status())
        .encode_frame()?;
    timeout(
        DEADLINE,
        sender.write_all(&status.encode(stream.frame_cap)?),
    )
    .await??;
    let (send, recv) = timeout(DEADLINE, streams.recv())
        .await?
        .ok_or("missing worker stream")?;
    let clock = TestClock::new();
    let bucket = Arc::new(StdMutex::new(TokenBucket::with_clock(1, clock.clone())));
    let context = context(&registry, stream, bucket, clock);
    let cancel = context.connection_token.clone();
    let cause = context.close_cause.clone();
    let (inbound_tx, mut inbound) = mpsc::channel(1);
    let (_outbound, outbound_rx) = worker_framed_channel(1);
    let worker = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(persistent_stream_worker(
        send,
        recv,
        StreamPrelude {
            magic: STREAM_PRELUDE_MAGIC,
            stream_kind: stream.kind,
            stream_version: stream.version,
            request_id: None,
            max_frame_bytes: stream.frame_cap,
        },
        context,
        inbound_tx,
        outbound_rx,
        1,
    )));
    assert_eq!(
        timeout(DEADLINE, inbound.recv()).await?,
        Some(status.clone())
    );
    let ending = BlockSyncMessage::RangeUnavailable {
        start_height: block::Height(1),
        count: 1,
    }
    .encode_frame()?;
    for _ in 0..64 {
        timeout(
            DEADLINE,
            sender.write_all(&ending.encode(stream.frame_cap)?),
        )
        .await??;
        assert_eq!(
            timeout(DEADLINE, inbound.recv()).await?,
            Some(ending.clone())
        );
    }
    assert!(!cancel.is_cancelled());
    timeout(
        DEADLINE,
        sender.write_all(&status.encode(stream.frame_cap)?),
    )
    .await??;
    timeout(DEADLINE, cancel.cancelled()).await?;
    timeout(DEADLINE, worker).await??;
    assert_eq!(cause.get_or("missing cause"), "ordered_rate_limited");
    assert_eq!(timeout(DEADLINE, inbound.recv()).await?, None);
    connection.close(0u32.into(), b"test complete");
    client.close().await;
    router.shutdown().await?;
    Ok(())
}
