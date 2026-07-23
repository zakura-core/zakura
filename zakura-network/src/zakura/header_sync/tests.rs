use super::*;
use super::{
    config::*,
    error::*,
    events::*,
    header_root_auth::*,
    reactor::*,
    state::{
        BufferedHeaderRange, HeaderSyncCore, OutstandingPhase, OutstandingRange, PendingOperation,
        PendingRootAuth, RangePriority, RangePurpose, RangeRequest, RootAuthSource, VctRootRepair,
        RETAINED_ROOT_LOCAL_MAX_ATTEMPTS, ROOT_AUTH_MIN_BODY_LEAD, VCT_ROOT_REPAIR_BACKOFFS,
        VCT_ROOT_REPAIR_MAX_WALL_TIME,
    },
    validation::*,
    wire::*,
    work_queue::HeaderWorkState,
};
use crate::zakura::{
    framed_channel,
    testkit::{TraceCapture, TraceValue},
    trace::{header_sync_trace as hs_trace, HEADER_SYNC_TABLE},
    FramedSend, HeaderSyncServiceSummary, Peer, Service, ServicePeerDirection, ServicePeerLimits,
    ServicePeerSnapshot, ZakuraConnId, ZakuraHeaderSyncCandidateState, ZAKURA_CAP_HEADER_SYNC,
};
use chrono::Duration;
use metrics::{
    Counter, CounterFn, Gauge, GaugeFn, Histogram, Key, KeyName, Metadata, Recorder, SharedString,
    Unit,
};
use rand::rngs::OsRng;
use std::{
    collections::{BTreeMap, HashMap},
    sync::{Mutex, OnceLock},
};
use zakura_chain::{
    orchard,
    parallel::commitment_aux::BlockCommitmentRoots,
    parameters::{
        testnet::{
            ConfiguredActivationHeights, ConfiguredCheckpoints, Parameters, RegtestParameters,
        },
        Network,
    },
    sapling,
    serialization::{ZcashDeserializeInto, ZcashSerialize},
    work::{difficulty::CompactDifficulty, equihash::Solution},
};
use zakura_test::vectors::{
    BLOCK_MAINNET_1_BYTES, BLOCK_MAINNET_2_BYTES, BLOCK_MAINNET_3_BYTES, BLOCK_MAINNET_4_BYTES,
    BLOCK_MAINNET_GENESIS_BYTES, BLOCK_TESTNET_GENESIS_BYTES,
};

#[derive(Default)]
struct HeaderSyncMetricsRecorder {
    counters: Mutex<BTreeMap<String, u64>>,
    gauges: Mutex<BTreeMap<String, f64>>,
}

struct RecordedCounter {
    name: String,
    recorder: &'static HeaderSyncMetricsRecorder,
}

struct RecordedGauge {
    name: String,
    recorder: &'static HeaderSyncMetricsRecorder,
}

fn thread_metric_name(name: &str) -> String {
    format!("{:?}:{name}", std::thread::current().id())
}

impl CounterFn for RecordedCounter {
    fn increment(&self, value: u64) {
        let mut counters = self.recorder.counters.lock().expect("metrics mutex ok");
        let counter = counters.entry(self.name.clone()).or_default();
        *counter = counter.saturating_add(value);
    }

    fn absolute(&self, value: u64) {
        let mut counters = self.recorder.counters.lock().expect("metrics mutex ok");
        counters.insert(self.name.clone(), value);
    }
}

impl GaugeFn for RecordedGauge {
    fn increment(&self, value: f64) {
        let mut gauges = self.recorder.gauges.lock().expect("metrics mutex ok");
        let gauge = gauges.entry(thread_metric_name(&self.name)).or_default();
        *gauge += value;
    }

    fn decrement(&self, value: f64) {
        let mut gauges = self.recorder.gauges.lock().expect("metrics mutex ok");
        let gauge = gauges.entry(thread_metric_name(&self.name)).or_default();
        *gauge -= value;
    }

    fn set(&self, value: f64) {
        let mut gauges = self.recorder.gauges.lock().expect("metrics mutex ok");
        gauges.insert(thread_metric_name(&self.name), value);
    }
}

impl Recorder for HeaderSyncMetricsRecorder {
    fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn describe_histogram(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

    fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
        Counter::from_arc(Arc::new(RecordedCounter {
            name: key.name().to_string(),
            recorder: header_sync_metrics_recorder(),
        }))
    }

    fn register_gauge(&self, key: &Key, _metadata: &Metadata<'_>) -> Gauge {
        Gauge::from_arc(Arc::new(RecordedGauge {
            name: key.name().to_string(),
            recorder: header_sync_metrics_recorder(),
        }))
    }

    fn register_histogram(&self, _key: &Key, _metadata: &Metadata<'_>) -> Histogram {
        Histogram::noop()
    }
}

fn header_sync_metrics_recorder() -> &'static HeaderSyncMetricsRecorder {
    static RECORDER: OnceLock<HeaderSyncMetricsRecorder> = OnceLock::new();
    let recorder = RECORDER.get_or_init(HeaderSyncMetricsRecorder::default);
    let _ = metrics::set_global_recorder(recorder);
    recorder
}

fn metric_value(name: &str) -> u64 {
    let recorder = header_sync_metrics_recorder();
    recorder
        .counters
        .lock()
        .expect("metrics mutex ok")
        .get(name)
        .copied()
        .unwrap_or_default()
}

fn gauge_value(name: &str) -> f64 {
    let recorder = header_sync_metrics_recorder();
    recorder
        .gauges
        .lock()
        .expect("metrics mutex ok")
        .get(&thread_metric_name(name))
        .copied()
        .unwrap_or_default()
}

fn metric_snapshot(names: &[&'static str]) -> BTreeMap<&'static str, u64> {
    names
        .iter()
        .copied()
        .map(|name| (name, metric_value(name)))
        .collect()
}

fn assert_metric_incremented(snapshot: &BTreeMap<&'static str, u64>, name: &'static str) {
    assert!(
        metric_value(name) > snapshot.get(name).copied().unwrap_or_default(),
        "expected metric {name} to increment"
    );
}

fn mainnet_block(bytes: &[u8]) -> Arc<block::Block> {
    Arc::new(bytes.zcash_deserialize_into().expect("block vector parses"))
}

fn mainnet_header(bytes: &[u8]) -> Arc<block::Header> {
    mainnet_block(bytes).header.clone()
}

fn headers_message(headers: Vec<Arc<block::Header>>) -> HeaderSyncMessage {
    let start_height = headers
        .first()
        .map(|header| test_header_height(header.as_ref()))
        .unwrap_or(block::Height(1));
    headers_message_from(start_height, headers)
}

fn headers_message_from(
    start_height: block::Height,
    headers: Vec<Arc<block::Header>>,
) -> HeaderSyncMessage {
    let body_sizes = vec![0; headers.len()];
    let tree_aux_roots = roots_from_height(start_height, headers.len());
    HeaderSyncMessage::Headers {
        headers,
        body_sizes,
        tree_aux_roots,
    }
}

fn headers_message_with_sizes(
    headers: Vec<Arc<block::Header>>,
    body_sizes: Vec<u32>,
) -> HeaderSyncMessage {
    let start_height = headers
        .first()
        .map(|header| test_header_height(header.as_ref()))
        .unwrap_or(block::Height(1));
    let tree_aux_roots = roots_from_height(start_height, headers.len());
    HeaderSyncMessage::Headers {
        headers,
        body_sizes,
        tree_aux_roots,
    }
}

fn rootless_headers_message_from(
    start_height: block::Height,
    headers: Vec<Arc<block::Header>>,
) -> HeaderSyncMessage {
    let _ = start_height;
    let body_sizes = vec![0; headers.len()];
    HeaderSyncMessage::Headers {
        headers,
        body_sizes,
        tree_aux_roots: Vec::new(),
    }
}

fn finalized_headers_message(headers: Vec<Arc<block::Header>>) -> HeaderSyncMessage {
    let start_height = headers
        .first()
        .map(|header| test_header_height(header.as_ref()))
        .unwrap_or(block::Height(1));
    finalized_headers_message_from(start_height, headers)
}

fn finalized_headers_message_from(
    start_height: block::Height,
    headers: Vec<Arc<block::Header>>,
) -> HeaderSyncMessage {
    let body_sizes = vec![0; headers.len()];
    let tree_aux_roots = roots_from_height(start_height, headers.len());
    HeaderSyncMessage::Headers {
        headers,
        body_sizes,
        tree_aux_roots,
    }
}

fn finalized_headers_message_with_sizes(
    headers: Vec<Arc<block::Header>>,
    body_sizes: Vec<u32>,
) -> HeaderSyncMessage {
    let start_height = headers
        .first()
        .map(|header| test_header_height(header.as_ref()))
        .unwrap_or(block::Height(1));
    let tree_aux_roots = roots_from_height(start_height, headers.len());
    HeaderSyncMessage::Headers {
        headers,
        body_sizes,
        tree_aux_roots,
    }
}

fn root_at(height: block::Height) -> BlockCommitmentRoots {
    BlockCommitmentRoots {
        height,
        sapling_root: sapling::tree::NoteCommitmentTree::default().root(),
        orchard_root: orchard::tree::NoteCommitmentTree::default().root(),
        ironwood_root: zakura_chain::ironwood::tree::NoteCommitmentTree::default().root(),
        sapling_tx: 0,
        orchard_tx: 0,
        ironwood_tx: 0,
        auth_data_root: block::merkle::AuthDataRoot::from([0u8; 32]),
    }
}

fn test_header_height(header: &block::Header) -> block::Height {
    let hash = block::Hash::from(header);
    [
        (block::Height(0), &BLOCK_MAINNET_GENESIS_BYTES[..]),
        (block::Height(1), &BLOCK_MAINNET_1_BYTES[..]),
        (block::Height(2), &BLOCK_MAINNET_2_BYTES[..]),
        (block::Height(3), &BLOCK_MAINNET_3_BYTES[..]),
        (block::Height(4), &BLOCK_MAINNET_4_BYTES[..]),
    ]
    .into_iter()
    .find_map(|(height, bytes)| {
        (hash == block::Hash::from(mainnet_header(bytes).as_ref())).then_some(height)
    })
    .unwrap_or(block::Height(1))
}

fn roots_from_height(start_height: block::Height, count: usize) -> Vec<BlockCommitmentRoots> {
    (0..count)
        .map(|offset| {
            let offset = u32::try_from(offset).expect("test root count fits in u32");
            root_at(block::Height(start_height.0 + offset))
        })
        .collect()
}

async fn validate_headers_stateless_after_equihash_acceptance(
    headers: Vec<Arc<block::Header>>,
    context: HeaderSyncValidationContext<'_>,
) -> Result<(), HeaderSyncWireError> {
    validate_header_count(headers.len(), context.decode_context)?;
    validate_internal_continuity(&headers)?;
    validate_header_times(&headers, context.now, context.start_height)?;
    validate_solution_sizes(&headers, context.network)?;
    tokio::task::spawn_blocking(move || {
        for header in headers {
            let hash = block::Hash::from(header.as_ref());
            validate_difficulty_filter(hash, header.difficulty_threshold)?;
        }
        Ok(())
    })
    .await?
}

/// Shared non-zero request ID for codec tests that only need one in flight.
fn test_request_id() -> HeaderSyncRequestId {
    HeaderSyncRequestId::new(1).expect("non-zero id")
}

/// Send an inbound `GetHeaders` on the peer's current session.
///
/// Request IDs must strictly increase per session, so callers pass them explicitly.
async fn send_get_headers(
    fixture: &ReactorFixture,
    peer: &ZakuraPeerId,
    request_id: u64,
    start_height: block::Height,
    count: u32,
) {
    fixture
        .handle
        .send(HeaderSyncEvent::WireGetHeaders {
            peer: peer.clone(),
            session_id: 0,
            request_id: HeaderSyncRequestId::new(request_id).expect("non-zero id"),
            start_height,
            count,
            want_tree_aux_roots: false,
        })
        .await
        .unwrap();
}

/// Encode a correlated message under [`test_request_id`].
fn encode_correlated(message: &HeaderSyncMessage) -> Result<Vec<u8>, HeaderSyncWireError> {
    message.encode(Some(test_request_id()))
}

fn headers_context(count: u32, peer_cap: u32) -> HeaderSyncDecodeContext {
    HeaderSyncDecodeContext::for_headers_response(
        ExpectedHeadersResponse::new(test_request_id(), block::Height(1), count, false).unwrap(),
        peer_cap,
    )
}

fn finalized_headers_context(count: u32, peer_cap: u32) -> HeaderSyncDecodeContext {
    HeaderSyncDecodeContext::for_headers_response(
        ExpectedHeadersResponse::new(test_request_id(), block::Height(1), count, true).unwrap(),
        peer_cap,
    )
}

struct ReactorFixture {
    handle: HeaderSyncHandle,
    actions: mpsc::Receiver<HeaderSyncAction>,
    task: JoinHandle<()>,
    outbound_receivers: Mutex<Vec<crate::zakura::FramedRecv>>,
}

impl Drop for ReactorFixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn peer(byte: u8) -> ZakuraPeerId {
    ZakuraPeerId::new(vec![byte; 32]).expect("test peer id is within bounds")
}

fn commit_operation(
    peer: ZakuraPeerId,
    session_id: u64,
    request_id: HeaderSyncRequestId,
) -> HeaderSyncOperationIdentity {
    HeaderSyncOperationIdentity {
        wire_request: HeaderSyncWireRequestIdentity {
            peer,
            session_id,
            request_id,
        },
        op_kind: HeaderSyncOperationKind::CommitHeaders,
    }
}

fn node_peer() -> (ZakuraPeerId, iroh::NodeId) {
    let node_id = iroh::SecretKey::generate(OsRng).public();
    (
        ZakuraPeerId::new(node_id.as_bytes().to_vec()).expect("node id is a valid peer id"),
        node_id,
    )
}

fn advisory_header_summary(
    best_height: block::Height,
    inbound_slots_free: u16,
) -> HeaderSyncServiceSummary {
    HeaderSyncServiceSummary {
        best_height,
        best_hash: block::Hash([7; 32]),
        finalized_height: None,
        serving_headers: true,
        inbound_slots_free,
        inbound_slots_max: inbound_slots_free,
        outbound_slots_free: 1,
        outbound_slots_max: 1,
    }
}

fn regtest_network() -> Network {
    Network::new_regtest(Default::default())
}

fn checkpoint_testnet_with_hash(
    checkpoint_height: block::Height,
    checkpoint_hash: block::Hash,
) -> (Network, block::Hash) {
    let mainnet = Network::Mainnet;
    let network = Parameters::build()
        .with_network_name("HeadersyncCheckpointTest")
        .expect("custom network name is valid")
        .with_genesis_hash(mainnet.genesis_hash())
        .expect("mainnet genesis hash is valid")
        .with_activation_heights(ConfiguredActivationHeights {
            overwinter: Some(1),
            sapling: Some(2),
            blossom: Some(3),
            heartwood: Some(4),
            canopy: Some(4),
            ..Default::default()
        })
        .expect("custom activation heights are in order")
        .clear_funding_streams()
        .with_checkpoints(ConfiguredCheckpoints::HeightsAndHashes(vec![
            (block::Height(0), mainnet.genesis_hash()),
            (checkpoint_height, checkpoint_hash),
        ]))
        .expect("custom checkpoints are valid")
        .to_network()
        .expect("custom testnet parameters are valid");

    (network, checkpoint_hash)
}

fn checkpoint_regtest(checkpoint_height: block::Height) -> (Network, block::Hash) {
    let checkpoint_hash = block::Hash::from(mainnet_header(&BLOCK_MAINNET_1_BYTES).as_ref());
    checkpoint_regtest_with_hash(checkpoint_height, checkpoint_hash)
}

fn checkpoint_regtest_with_hash(
    checkpoint_height: block::Height,
    checkpoint_hash: block::Hash,
) -> (Network, block::Hash) {
    let default_regtest = regtest_network();
    let params = RegtestParameters {
        checkpoints: Some(ConfiguredCheckpoints::HeightsAndHashes(vec![
            (block::Height(0), default_regtest.genesis_hash()),
            (checkpoint_height, checkpoint_hash),
        ])),
        ..Default::default()
    };

    (Network::new_regtest(params), checkpoint_hash)
}

fn startup_for(
    network: Network,
    anchor: (block::Height, block::Hash),
    best_header_tip: Option<(block::Height, block::Hash)>,
) -> HeaderSyncStartup {
    let mut startup = HeaderSyncStartup::new(
        network,
        anchor,
        HeaderSyncFrontiers {
            finalized_height: anchor.0,
            verified_block_tip: anchor.0,
            verified_block_hash: anchor.1,
        },
        best_header_tip,
        ZakuraHeaderSyncConfig::default(),
        LOCAL_MAX_MESSAGE_BYTES,
    );
    startup.range_state_actions_enabled = true;
    startup.inbound_new_block_acceptance_enabled = true;
    startup
}

#[test]
fn startup_new_is_passive_until_local_hooks_are_wired() {
    let network = Network::Mainnet;
    let anchor = (block::Height(0), network.genesis_hash());
    let startup = HeaderSyncStartup::new(
        network,
        anchor,
        HeaderSyncFrontiers {
            finalized_height: anchor.0,
            verified_block_tip: anchor.0,
            verified_block_hash: anchor.1,
        },
        Some(anchor),
        ZakuraHeaderSyncConfig::default(),
        LOCAL_MAX_MESSAGE_BYTES,
    );

    assert!(!startup.range_state_actions_enabled);
    assert!(!startup.inbound_new_block_acceptance_enabled);
}

#[test]
fn startup_new_uses_configured_status_refresh_interval() {
    let network = Network::Mainnet;
    let anchor = (block::Height(0), network.genesis_hash());
    let status_refresh_interval = std::time::Duration::from_secs(17);
    let startup = HeaderSyncStartup::new(
        network,
        anchor,
        HeaderSyncFrontiers {
            finalized_height: anchor.0,
            verified_block_tip: anchor.0,
            verified_block_hash: anchor.1,
        },
        Some(anchor),
        ZakuraHeaderSyncConfig {
            status_refresh_interval,
            ..ZakuraHeaderSyncConfig::default()
        },
        LOCAL_MAX_MESSAGE_BYTES,
    );

    assert_eq!(startup.status_refresh_interval, status_refresh_interval);
}

#[test]
fn root_auth_ranges_overlap_once_and_stay_checkpoint_covered() {
    let network = Network::Mainnet;
    let anchor = (block::Height(0), network.genesis_hash());
    let mut startup = startup_for(
        network,
        anchor,
        Some((block::Height(10), block::Hash([10; 32]))),
    );
    startup.config.max_headers_per_response = 3;
    startup.header_root_auth = Some(HeaderRootAuthState {
        authenticated_height: block::Height(0),
        authenticated_hash: anchor.1,
        completed_checkpoint_height: block::Height(6),
        completed_checkpoint_hash: block::Hash([6; 32]),
    });
    let mut state = HeaderSyncCore::new(&startup).expect("startup is valid");

    assert_eq!(
        state.root_auth_hole_heights(
            &startup,
            startup
                .header_root_auth
                .expect("test authentication state exists")
        ),
        5
    );
    state.refresh_root_auth_range(&startup);
    let first = state
        .schedule
        .authenticate_roots
        .pop_front()
        .expect("checkpoint covers a root and successor witness");
    assert_eq!(first.start_height(), block::Height(1));
    assert_eq!(first.end_height(), block::Height(3));

    state.schedule.clear_root_auth();
    state.header_root_auth = Some(HeaderRootAuthState {
        authenticated_height: block::Height(2),
        authenticated_hash: block::Hash([2; 32]),
        ..startup
            .header_root_auth
            .expect("test authentication state exists")
    });
    state.refresh_root_auth_range(&startup);
    let second = state
        .schedule
        .authenticate_roots
        .front()
        .copied()
        .expect("next root-authentication batch is scheduled");
    assert_eq!(second.start_height(), first.end_height());
    assert_eq!(second.end_height(), block::Height(5));
    assert!(second.end_height() <= block::Height(6));
}

#[test]
fn root_auth_miss_prefetches_bounded_overlapping_ranges() {
    let network = Network::Mainnet;
    let anchor = (block::Height(0), network.genesis_hash());
    let mut startup = startup_for(
        network,
        anchor,
        Some((block::Height(10), block::Hash([10; 32]))),
    );
    startup.config.max_headers_per_response = 3;
    startup.header_root_auth = Some(HeaderRootAuthState {
        authenticated_height: block::Height(0),
        authenticated_hash: anchor.1,
        completed_checkpoint_height: block::Height(10),
        completed_checkpoint_hash: block::Hash([10; 32]),
    });
    let mut state = HeaderSyncCore::new(&startup).expect("startup is valid");

    assert_eq!(
        state.root_auth_hole_heights(
            &startup,
            startup
                .header_root_auth
                .expect("test authentication state exists")
        ),
        9
    );
    state.refresh_root_auth_range(&startup);

    let ranges: Vec<_> = state
        .schedule
        .authenticate_roots
        .iter()
        .map(|range| (range.start_height(), range.end_height(), range.anchor_hash))
        .collect();
    assert_eq!(
        ranges,
        vec![
            (block::Height(1), block::Height(3), Some(anchor.1)),
            (block::Height(3), block::Height(5), None),
            (block::Height(5), block::Height(7), None),
            (block::Height(7), block::Height(9), None),
            (block::Height(9), block::Height(10), None),
        ]
    );
}

#[test]
fn root_auth_frontier_advance_refills_released_fallback_capacity() {
    let network = Network::Mainnet;
    let anchor = (block::Height(0), network.genesis_hash());
    let mut startup = startup_for(
        network,
        anchor,
        Some((block::Height(100), block::Hash([100; 32]))),
    );
    startup.config.max_headers_per_response = 3;
    startup.header_root_auth = Some(HeaderRootAuthState {
        authenticated_height: anchor.0,
        authenticated_hash: anchor.1,
        completed_checkpoint_height: block::Height(100),
        completed_checkpoint_hash: block::Hash([100; 32]),
    });
    let mut state = HeaderSyncCore::new(&startup).expect("startup is valid");

    state.refresh_root_auth_range(&startup);
    let initial_resident = state
        .schedule
        .resident_heights_for(RangePriority::AuthenticateRoots);
    let initial_end = state
        .schedule
        .highest_end(RangePriority::AuthenticateRoots)
        .expect("initial fallback window is populated");

    let advanced = HeaderRootAuthState {
        authenticated_height: block::Height(3),
        authenticated_hash: block::Hash([3; 32]),
        ..startup
            .header_root_auth
            .expect("test authentication state exists")
    };
    state.header_root_auth = Some(advanced);
    state.prune_root_auth_pipeline(advanced, true);
    let pruned_resident = state
        .schedule
        .resident_heights_for(RangePriority::AuthenticateRoots);
    assert!(pruned_resident < initial_resident);

    state.refresh_root_auth_range(&startup);
    assert_eq!(
        state
            .schedule
            .resident_heights_for(RangePriority::AuthenticateRoots),
        initial_resident
    );
    assert!(
        state
            .schedule
            .highest_end(RangePriority::AuthenticateRoots)
            .expect("refilled fallback window has a high end")
            > initial_end
    );
}

#[test]
fn root_auth_does_not_schedule_without_checkpoint_covered_witness() {
    let network = Network::Mainnet;
    let anchor = (block::Height(0), network.genesis_hash());
    let mut startup = startup_for(
        network,
        anchor,
        Some((block::Height(5), block::Hash([5; 32]))),
    );
    startup.header_root_auth = Some(HeaderRootAuthState {
        authenticated_height: block::Height(4),
        authenticated_hash: block::Hash([4; 32]),
        completed_checkpoint_height: block::Height(5),
        completed_checkpoint_hash: block::Hash([5; 32]),
    });
    let mut state = HeaderSyncCore::new(&startup).expect("startup is valid");

    state.refresh_root_auth_range(&startup);

    assert!(state.schedule.authenticate_roots.is_empty());
}

#[test]
fn clamped_root_auth_request_never_enqueues_non_overlapping_suffix() {
    let auth = RangeRequest {
        range: CheckedHeaderRange::from_count(block::Height(1), 5).expect("test range is bounded"),
        anchor_hash: Some(Network::Mainnet.genesis_hash()),
        finalized: true,
        want_tree_aux_roots: true,
        priority: RangePriority::AuthenticateRoots,
    };
    assert_eq!(clamped_request_suffix(auth, 2, block::Height(10)), None);

    let forward = RangeRequest {
        priority: RangePriority::Forward,
        ..auth
    };
    let suffix = clamped_request_suffix(forward, 2, block::Height(10))
        .expect("rooted forward work keeps its overlapping suffix");
    assert_eq!(suffix.start_height(), block::Height(2));
    assert_eq!(suffix.count(), 4);
}

#[test]
fn retained_response_requires_exact_current_frontier_and_covered_witness() {
    let headers = vec![
        mainnet_header(&BLOCK_MAINNET_1_BYTES),
        mainnet_header(&BLOCK_MAINNET_2_BYTES),
    ];
    let payload = HeaderRangePayload::new(
        HeaderRangeEntry::from_parallel(
            block::Height(1),
            headers,
            vec![0, 0],
            roots_from_height(block::Height(1), 2),
        )
        .expect("test response vectors align"),
    )
    .expect("test response is contiguous");
    let auth = HeaderRootAuthState {
        authenticated_height: block::Height(0),
        authenticated_hash: Network::Mainnet.genesis_hash(),
        completed_checkpoint_height: block::Height(2),
        completed_checkpoint_hash: block::Hash([2; 32]),
    };

    let retained = retained_root_auth_range(auth, &payload, block::Height(2))
        .expect("exact covered response is retained-auth eligible");
    assert_eq!(retained.start_height(), block::Height(1));
    assert_eq!(retained.end_height(), block::Height(2));

    assert!(retained_root_auth_range(
        HeaderRootAuthState {
            authenticated_height: block::Height(1),
            ..auth
        },
        &payload,
        block::Height(2),
    )
    .is_none());
    assert!(retained_root_auth_range(
        HeaderRootAuthState {
            completed_checkpoint_height: block::Height(1),
            ..auth
        },
        &payload,
        block::Height(2),
    )
    .is_none());
    let one_entry = HeaderRangePayload::new(vec![payload.entries()[0].clone()])
        .expect("single entry is structurally valid");
    assert!(retained_root_auth_range(auth, &one_entry, block::Height(2)).is_none());
}

#[test]
fn retained_payload_waits_for_checkpoint_and_suppresses_fallback() {
    let network = Network::Mainnet;
    let anchor = (block::Height(0), network.genesis_hash());
    let mut startup = startup_for(
        network,
        anchor,
        Some((block::Height(4), block::Hash([4; 32]))),
    );
    let mut auth = HeaderRootAuthState {
        authenticated_height: anchor.0,
        authenticated_hash: anchor.1,
        completed_checkpoint_height: block::Height(1),
        completed_checkpoint_hash: block::Hash([1; 32]),
    };
    startup.header_root_auth = Some(auth);
    let mut state = HeaderSyncCore::new(&startup).expect("startup is coherent");
    let payload = HeaderRangePayload::new(
        HeaderRangeEntry::from_parallel(
            block::Height(1),
            vec![
                mainnet_header(&BLOCK_MAINNET_1_BYTES),
                mainnet_header(&BLOCK_MAINNET_2_BYTES),
            ],
            vec![0, 0],
            roots_from_height(block::Height(1), 2),
        )
        .expect("test response vectors align"),
    )
    .expect("test payload is contiguous");
    let wire_request = HeaderSyncWireRequestIdentity {
        peer: peer(230),
        session_id: 1,
        request_id: HeaderSyncRequestId::new(1).expect("request ID is non-zero"),
    };

    assert!(state.admit_retained_root_payload(wire_request, payload));
    assert!(state.retained_ready(auth, Instant::now()).is_none());
    state.refresh_root_auth_range(&startup);
    assert!(
        state.schedule.authenticate_roots.is_empty(),
        "retained open-bracket coverage prevents a duplicate fallback"
    );

    auth.completed_checkpoint_height = block::Height(2);
    auth.completed_checkpoint_hash = block::Hash([2; 32]);
    assert!(state.retained_ready(auth, Instant::now()).is_some());
    assert_eq!(state.retained_heights(), 2);
}

#[test]
fn root_auth_fallback_stops_at_first_retained_start() {
    let network = Network::Mainnet;
    let anchor = (block::Height(0), network.genesis_hash());
    let mut startup = startup_for(
        network,
        anchor,
        Some((block::Height(4), block::Hash([4; 32]))),
    );
    startup.config.max_headers_per_response = 2;
    startup.header_root_auth = Some(HeaderRootAuthState {
        authenticated_height: anchor.0,
        authenticated_hash: anchor.1,
        completed_checkpoint_height: block::Height(4),
        completed_checkpoint_hash: block::Hash([4; 32]),
    });
    let mut state = HeaderSyncCore::new(&startup).expect("startup is coherent");
    let payload = HeaderRangePayload::new(
        HeaderRangeEntry::from_parallel(
            block::Height(3),
            vec![
                mainnet_header(&BLOCK_MAINNET_3_BYTES),
                mainnet_header(&BLOCK_MAINNET_4_BYTES),
            ],
            vec![0, 0],
            roots_from_height(block::Height(3), 2),
        )
        .expect("test response vectors align"),
    )
    .expect("test payload is contiguous");
    assert!(state.admit_retained_root_payload(
        HeaderSyncWireRequestIdentity {
            peer: peer(233),
            session_id: 1,
            request_id: HeaderSyncRequestId::new(1).expect("request ID is non-zero"),
        },
        payload,
    ));
    assert_eq!(
        state.root_auth_hole_heights(
            &startup,
            startup
                .header_root_auth
                .expect("test authentication state exists")
        ),
        2
    );

    state.refresh_root_auth_range(&startup);

    let ranges: Vec<_> = state
        .schedule
        .authenticate_roots
        .iter()
        .map(|range| (range.start_height(), range.end_height()))
        .collect();
    assert_eq!(
        ranges,
        vec![
            (block::Height(1), block::Height(2)),
            (block::Height(2), block::Height(3)),
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn body_target_waits_for_authenticated_lead() {
    let network = Network::Mainnet;
    let anchor = (block::Height(0), network.genesis_hash());
    let best = (block::Height(800), block::Hash([8; 32]));
    let mut startup = startup_for(network, anchor, Some(best));
    startup.header_root_auth = Some(HeaderRootAuthState {
        authenticated_height: anchor.0,
        authenticated_hash: anchor.1,
        completed_checkpoint_height: best.0,
        completed_checkpoint_hash: best.1,
    });
    let mut fixture = spawn_test_reactor(startup);

    fixture
        .handle
        .send(HeaderSyncEvent::HeaderRootAuthStateChanged(Some(
            HeaderRootAuthState {
                authenticated_height: block::Height(ROOT_AUTH_MIN_BODY_LEAD.saturating_sub(1)),
                authenticated_hash: block::Hash([3; 32]),
                completed_checkpoint_height: best.0,
                completed_checkpoint_hash: best.1,
            },
        )))
        .await
        .unwrap();
    while tokio::time::timeout(std::time::Duration::from_millis(20), fixture.actions.recv())
        .await
        .is_ok()
    {}

    fixture
        .handle
        .send(HeaderSyncEvent::HeaderRootAuthStateChanged(Some(
            HeaderRootAuthState {
                authenticated_height: block::Height(ROOT_AUTH_MIN_BODY_LEAD),
                authenticated_hash: block::Hash([4; 32]),
                completed_checkpoint_height: best.0,
                completed_checkpoint_hash: best.1,
            },
        )))
        .await
        .unwrap();

    loop {
        if let HeaderSyncAction::HeaderAdvanced { height, hash } =
            next_action(&mut fixture.actions).await
        {
            assert_eq!(height, block::Height(ROOT_AUTH_MIN_BODY_LEAD));
            assert_eq!(hash, block::Hash([4; 32]));
            break;
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn verified_full_block_advances_header_tip_without_auth_lead() {
    let network = Network::Mainnet;
    let anchor = (block::Height(0), network.genesis_hash());
    let best = (block::Height(800), block::Hash([8; 32]));
    let mined = (block::Height(3), block::Hash([3; 32]));
    let mut startup = startup_for(network, anchor, Some(anchor));
    startup.header_root_auth = Some(HeaderRootAuthState {
        authenticated_height: anchor.0,
        authenticated_hash: anchor.1,
        completed_checkpoint_height: best.0,
        completed_checkpoint_hash: best.1,
    });
    let mut fixture = spawn_test_reactor(startup);

    // Drain startup queries / any initial actions.
    while tokio::time::timeout(std::time::Duration::from_millis(20), fixture.actions.recv())
        .await
        .is_ok()
    {}

    fixture
        .handle
        .send(HeaderSyncEvent::FullBlockCommitted {
            height: mined.0,
            hash: mined.1,
        })
        .await
        .unwrap();

    loop {
        if let HeaderSyncAction::HeaderAdvanced { height, hash } =
            next_action(&mut fixture.actions).await
        {
            assert_eq!(height, mined.0);
            assert_eq!(hash, mined.1);
            break;
        }
    }
    assert_eq!(fixture.handle.best_header_tip(), mined);
}

#[tokio::test(flavor = "current_thread")]
async fn root_auth_state_trace_records_exact_hole_height() {
    let network = Network::Mainnet;
    let anchor = (block::Height(0), network.genesis_hash());
    let best = (block::Height(4), block::Hash([4; 32]));
    let auth = HeaderRootAuthState {
        authenticated_height: anchor.0,
        authenticated_hash: anchor.1,
        completed_checkpoint_height: best.0,
        completed_checkpoint_hash: best.1,
    };
    let mut capture =
        TraceCapture::for_test("root_auth_state_trace_records_exact_hole_height").unwrap();
    let mut startup = startup_for(network, anchor, Some(best));
    startup.header_root_auth = Some(auth);
    startup.trace = ZakuraTrace::new(capture.tracer(), "01");
    let fixture = spawn_test_reactor(startup);

    fixture
        .handle
        .send(HeaderSyncEvent::HeaderRootAuthStateChanged(Some(auth)))
        .await
        .unwrap();
    tokio::task::yield_now().await;

    capture.flush().await;
    capture
        .reader()
        .unwrap()
        .table(HEADER_SYNC_TABLE.table())
        .assert_row(
            hs_trace::HEADER_ROOT_AUTH_DIAGNOSTICS,
            &[
                (hs_trace::HEIGHT, TraceValue::U64(0)),
                (hs_trace::BEST_HEADER_TIP, TraceValue::U64(4)),
                (hs_trace::ROOT_AUTH_HOLE_HEIGHTS, TraceValue::U64(3)),
            ],
        );

    let _ = capture.finish().await.unwrap();
}

#[test]
fn retained_admission_keeps_farthest_same_start_payload() {
    let network = Network::Mainnet;
    let anchor = (block::Height(0), network.genesis_hash());
    let mut startup = startup_for(network, anchor, None);
    startup.header_root_auth = Some(HeaderRootAuthState {
        authenticated_height: block::Height(0),
        authenticated_hash: anchor.1,
        completed_checkpoint_height: block::Height(6),
        completed_checkpoint_hash: block::Hash([6; 32]),
    });
    let mut state = HeaderSyncCore::new(&startup).expect("startup is coherent");
    let wire_request = |request_id| HeaderSyncWireRequestIdentity {
        peer: peer(231),
        session_id: 1,
        request_id: HeaderSyncRequestId::new(request_id).expect("request ID is non-zero"),
    };
    let payload = |count| {
        HeaderRangePayload::new(
            HeaderRangeEntry::from_parallel(
                block::Height(1),
                vec![mainnet_header(&BLOCK_MAINNET_1_BYTES); count],
                vec![0; count],
                roots_from_height(block::Height(1), count),
            )
            .expect("test response vectors align"),
        )
        .expect("test payload is contiguous")
    };

    let original_wire_request = wire_request(1);
    assert!(state.admit_retained_root_payload(original_wire_request.clone(), payload(3)));
    state
        .retained_roots
        .get_mut(&block::Height(1))
        .expect("original retained entry exists")
        .authenticating = true;
    assert!(!state.admit_retained_root_payload(wire_request(2), payload(2)));
    let replacement_wire_request = wire_request(3);
    assert!(state.admit_retained_root_payload(replacement_wire_request.clone(), payload(4)));
    assert!(
        state
            .remove_retained_root_if_owned(
                block::Height(1),
                &original_wire_request,
                "invalid_roots",
            )
            .is_none(),
        "an older authentication failure must not remove its replacement"
    );
    assert_eq!(
        state
            .retained_roots
            .get(&block::Height(1))
            .expect("same-start entry remains")
            .wire_request,
        replacement_wire_request
    );
}

#[test]
fn retained_admission_supersedes_queued_fallback() {
    let network = Network::Mainnet;
    let anchor = (block::Height(0), network.genesis_hash());
    let mut startup = startup_for(
        network,
        anchor,
        Some((block::Height(4), block::Hash([4; 32]))),
    );
    startup.header_root_auth = Some(HeaderRootAuthState {
        authenticated_height: anchor.0,
        authenticated_hash: anchor.1,
        completed_checkpoint_height: block::Height(2),
        completed_checkpoint_hash: block::Hash([2; 32]),
    });
    let mut state = HeaderSyncCore::new(&startup).expect("startup is coherent");
    state.refresh_root_auth_range(&startup);
    assert_eq!(state.schedule.authenticate_roots.len(), 1);

    let payload = HeaderRangePayload::new(
        HeaderRangeEntry::from_parallel(
            block::Height(1),
            vec![
                mainnet_header(&BLOCK_MAINNET_1_BYTES),
                mainnet_header(&BLOCK_MAINNET_2_BYTES),
            ],
            vec![0, 0],
            roots_from_height(block::Height(1), 2),
        )
        .expect("test response vectors align"),
    )
    .expect("test payload is contiguous");
    assert!(state.admit_retained_root_payload(
        HeaderSyncWireRequestIdentity {
            peer: peer(232),
            session_id: 1,
            request_id: HeaderSyncRequestId::new(1).expect("request ID is non-zero"),
        },
        payload,
    ));

    assert!(state.schedule.authenticate_roots.is_empty());
}

#[test]
fn retained_store_does_not_pressure_evict_long_lead() {
    let network = Network::Mainnet;
    let anchor = (block::Height(0), network.genesis_hash());
    let mut startup = startup_for(network, anchor, None);
    startup.header_root_auth = Some(HeaderRootAuthState {
        authenticated_height: block::Height(0),
        authenticated_hash: anchor.1,
        completed_checkpoint_height: block::Height(40),
        completed_checkpoint_hash: block::Hash([40; 32]),
    });
    let mut state = HeaderSyncCore::new(&startup).expect("startup is coherent");

    for index in 0..16u32 {
        let start = block::Height(index.saturating_mul(2).saturating_add(1));
        let payload = HeaderRangePayload::new(
            HeaderRangeEntry::from_parallel(
                start,
                vec![
                    mainnet_header(&BLOCK_MAINNET_1_BYTES),
                    mainnet_header(&BLOCK_MAINNET_2_BYTES),
                ],
                vec![0, 0],
                roots_from_height(start, 2),
            )
            .expect("test response vectors align"),
        )
        .expect("test payload is contiguous");
        assert!(state.admit_retained_root_payload(
            HeaderSyncWireRequestIdentity {
                peer: peer(233),
                session_id: 1,
                request_id: HeaderSyncRequestId::new(u64::from(index) + 1)
                    .expect("request ID is non-zero"),
            },
            payload,
        ));
    }

    assert_eq!(state.retained_roots.len(), 16);
    assert_eq!(state.retained_heights(), 32);
}

/// Without live auth state no consumption or pruning path runs, so retention
/// must be refused: admitted payloads would accumulate unboundedly and the
/// eventual `None -> Some` watch transition clears the retained store anyway.
#[test]
fn retained_admission_requires_live_auth_state() {
    let network = Network::Mainnet;
    let anchor = (block::Height(0), network.genesis_hash());
    let startup = startup_for(network, anchor, None);
    let mut state = HeaderSyncCore::new(&startup).expect("startup is coherent");
    assert!(state.header_root_auth.is_none());

    let payload = HeaderRangePayload::new(
        HeaderRangeEntry::from_parallel(
            block::Height(1),
            vec![
                mainnet_header(&BLOCK_MAINNET_1_BYTES),
                mainnet_header(&BLOCK_MAINNET_2_BYTES),
            ],
            vec![0, 0],
            roots_from_height(block::Height(1), 2),
        )
        .expect("test response vectors align"),
    )
    .expect("test payload is contiguous");
    let wire_request = HeaderSyncWireRequestIdentity {
        peer: peer(234),
        session_id: 1,
        request_id: HeaderSyncRequestId::new(1).expect("request ID is non-zero"),
    };

    assert!(!state.admit_retained_root_payload(wire_request.clone(), payload.clone()));
    assert!(state.retained_roots.is_empty());

    // The identical payload is admitted once auth state is live.
    state.header_root_auth = Some(HeaderRootAuthState {
        authenticated_height: block::Height(0),
        authenticated_hash: anchor.1,
        completed_checkpoint_height: block::Height(6),
        completed_checkpoint_hash: block::Hash([6; 32]),
    });
    assert!(state.admit_retained_root_payload(wire_request, payload));
    assert_eq!(state.retained_roots.len(), 1);
}

#[test]
fn retained_local_retry_window_starts_on_failure_and_can_fallback_after_exhaustion() {
    let network = Network::Mainnet;
    let anchor = (block::Height(0), network.genesis_hash());
    let mut startup = startup_for(
        network,
        anchor,
        Some((block::Height(4), block::Hash([4; 32]))),
    );
    startup.header_root_auth = Some(HeaderRootAuthState {
        authenticated_height: anchor.0,
        authenticated_hash: anchor.1,
        completed_checkpoint_height: block::Height(2),
        completed_checkpoint_hash: block::Hash([2; 32]),
    });
    let mut state = HeaderSyncCore::new(&startup).expect("startup is coherent");
    let wire_request = HeaderSyncWireRequestIdentity {
        peer: peer(237),
        session_id: 1,
        request_id: HeaderSyncRequestId::new(1).expect("request ID is non-zero"),
    };
    let payload = HeaderRangePayload::new(
        HeaderRangeEntry::from_parallel(
            block::Height(1),
            vec![
                mainnet_header(&BLOCK_MAINNET_1_BYTES),
                mainnet_header(&BLOCK_MAINNET_2_BYTES),
            ],
            vec![0, 0],
            roots_from_height(block::Height(1), 2),
        )
        .expect("test response vectors align"),
    )
    .expect("test payload is contiguous");
    assert!(state.admit_retained_root_payload(wire_request.clone(), payload));
    let now = Instant::now();
    let retained = state
        .retained_roots
        .get_mut(&block::Height(1))
        .expect("retained entry exists");
    assert!(retained.local_retry_started_at.is_none());
    assert!(retained.retry_local(now));
    assert_eq!(retained.local_retry_started_at, Some(now));
    retained.local_attempts = RETAINED_ROOT_LOCAL_MAX_ATTEMPTS.saturating_sub(1);
    assert!(!retained.retry_local(now + std::time::Duration::from_secs(1)));
    assert!(retained.local_retry_exhausted);

    assert!(state
        .remove_retained_root_if_owned(block::Height(1), &wire_request, "local_retry_exhausted",)
        .is_some());
    state.refresh_root_auth_range(&startup);
    assert_eq!(state.schedule.authenticate_roots.len(), 1);
}

fn startup_with_timeout(
    network: Network,
    anchor: (block::Height, block::Hash),
    request_timeout: std::time::Duration,
) -> HeaderSyncStartup {
    let mut startup = startup_for(network, anchor, None);
    startup.request_timeout = request_timeout;
    startup
}

#[test]
fn startup_rejects_anchor_above_verified_block_tip() {
    let (network, checkpoint_hash) = checkpoint_regtest(block::Height(3));
    let mut startup = startup_for(
        network,
        (block::Height(3), checkpoint_hash),
        Some((block::Height(3), checkpoint_hash)),
    );
    startup.frontiers.verified_block_tip = block::Height(0);

    assert!(matches!(
        HeaderSyncCore::new(&startup),
        Err(HeaderSyncStartError::AnchorAboveVerifiedBlockTip {
            anchor_height: block::Height(3),
            verified_block_tip: block::Height(0),
        })
    ));
}

#[test]
fn startup_uses_verified_block_tip_when_stored_header_tip_is_stale() {
    let network = regtest_network();
    let verified_tip = block::Height(5);
    let verified_hash = block::Hash([5; 32]);
    let mut startup = startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        Some((block::Height(3), block::Hash([3; 32]))),
    );
    startup.frontiers.verified_block_tip = verified_tip;
    startup.frontiers.verified_block_hash = verified_hash;

    let state = HeaderSyncCore::new(&startup).expect("forward-only startup is coherent");

    assert_eq!(
        (state.best_header_tip, state.best_header_hash),
        (verified_tip, verified_hash)
    );
}

#[test]
fn commit_and_authentication_operations_from_one_request_are_distinct() {
    let network = regtest_network();
    let startup = startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    );
    let mut state = HeaderSyncCore::new(&startup).expect("test startup is coherent");
    let wire_request = HeaderSyncWireRequestIdentity {
        peer: peer(211),
        session_id: 7,
        request_id: HeaderSyncRequestId::new(9).expect("test request ID is non-zero"),
    };
    let commit = HeaderSyncOperationIdentity {
        wire_request: wire_request.clone(),
        op_kind: HeaderSyncOperationKind::CommitHeaders,
    };
    let authenticate = HeaderSyncOperationIdentity {
        wire_request,
        op_kind: HeaderSyncOperationKind::AuthenticateRoots,
    };
    let range = RangeRequest {
        range: CheckedHeaderRange::from_count(block::Height(1), 1)
            .expect("test range is non-empty"),
        anchor_hash: Some(network.genesis_hash()),
        finalized: false,
        want_tree_aux_roots: true,
        priority: RangePriority::Forward,
    };

    let pending = PendingOperation {
        range,
        purpose: RangePurpose::Sync,
        retention_candidate: None,
        root_auth: None,
        completion_observed: false,
    };
    state
        .pending_operations
        .insert(commit.clone(), pending.clone());
    state
        .pending_operations
        .insert(authenticate.clone(), pending.clone());

    assert_eq!(
        state.pending_operations.remove(&commit),
        Some(pending.clone())
    );
    assert_eq!(state.pending_operations.get(&authenticate), Some(&pending));
}

#[test]
fn stale_root_auth_waits_for_watch_before_rescheduling() {
    let network = Network::Mainnet;
    let anchor = (block::Height(0), network.genesis_hash());
    let mut startup = startup_for(
        network,
        anchor,
        Some((block::Height(3), block::Hash([3; 32]))),
    );
    startup.header_root_auth = Some(HeaderRootAuthState {
        authenticated_height: block::Height(0),
        authenticated_hash: anchor.1,
        completed_checkpoint_height: block::Height(3),
        completed_checkpoint_hash: block::Hash([3; 32]),
    });
    let mut state = HeaderSyncCore::new(&startup).expect("startup is coherent");
    state.root_auth_waiting_for_watch = true;

    state.refresh_root_auth_range(&startup);

    assert!(state.schedule.authenticate_roots.is_empty());
}

#[test]
fn session_retirement_cleans_auth_and_retained_payloads() {
    let network = Network::Mainnet;
    let mut startup = startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    );
    startup.header_root_auth = Some(HeaderRootAuthState {
        authenticated_height: block::Height(0),
        authenticated_hash: network.genesis_hash(),
        completed_checkpoint_height: block::Height(6),
        completed_checkpoint_hash: block::Hash([6; 32]),
    });
    let mut state = HeaderSyncCore::new(&startup).expect("startup is coherent");
    let peer = peer(212);
    let wire_request = HeaderSyncWireRequestIdentity {
        peer: peer.clone(),
        session_id: 7,
        request_id: HeaderSyncRequestId::new(10).expect("request ID is non-zero"),
    };
    let auth_operation = HeaderSyncOperationIdentity {
        wire_request: wire_request.clone(),
        op_kind: HeaderSyncOperationKind::AuthenticateRoots,
    };
    let commit_operation = HeaderSyncOperationIdentity {
        wire_request,
        op_kind: HeaderSyncOperationKind::CommitHeaders,
    };
    let auth_range = RangeRequest {
        range: CheckedHeaderRange::from_count(block::Height(1), 2).expect("test range is bounded"),
        anchor_hash: Some(network.genesis_hash()),
        finalized: true,
        want_tree_aux_roots: true,
        priority: RangePriority::AuthenticateRoots,
    };
    let payload = HeaderRangePayload::new(
        HeaderRangeEntry::from_parallel(
            block::Height(1),
            vec![
                mainnet_header(&BLOCK_MAINNET_1_BYTES),
                mainnet_header(&BLOCK_MAINNET_2_BYTES),
            ],
            vec![0, 0],
            roots_from_height(block::Height(1), 2),
        )
        .expect("test response vectors align"),
    )
    .expect("test payload is contiguous");
    state
        .schedule
        .mark_authenticating(auth_operation.clone(), auth_range);
    state.pending_operations.insert(
        auth_operation.clone(),
        PendingOperation {
            range: auth_range,
            purpose: RangePurpose::AuthenticateRoots,
            retention_candidate: None,
            root_auth: Some(PendingRootAuth {
                source: RootAuthSource::Fallback,
                expected: HeaderRootAuthState {
                    authenticated_height: block::Height(0),
                    authenticated_hash: network.genesis_hash(),
                    completed_checkpoint_height: block::Height(0),
                    completed_checkpoint_hash: network.genesis_hash(),
                },
            }),
            completion_observed: false,
        },
    );
    state.pending_operations.insert(
        commit_operation.clone(),
        PendingOperation {
            range: RangeRequest {
                priority: RangePriority::Forward,
                ..auth_range
            },
            purpose: RangePurpose::Sync,
            retention_candidate: Some(payload.clone()),
            root_auth: None,
            completion_observed: false,
        },
    );
    assert!(state.admit_retained_root_payload(auth_operation.wire_request.clone(), payload,));
    let buffered_range = RangeRequest {
        range: CheckedHeaderRange::from_count(block::Height(2), 2).expect("test range is bounded"),
        anchor_hash: None,
        ..auth_range
    };
    let buffered_peer = peer.clone();
    state
        .schedule
        .mark_assigned(buffered_peer.clone(), buffered_range);
    state.schedule.mark_buffered(buffered_peer, buffered_range);
    state.buffered.insert(
        (
            RangePriority::AuthenticateRoots,
            buffered_range.start_height(),
        ),
        BufferedHeaderRange {
            wire_request: auth_operation.wire_request.clone(),
            range: buffered_range,
            purpose: RangePurpose::AuthenticateRoots,
            payload: HeaderRangePayload::new(
                HeaderRangeEntry::from_parallel(
                    block::Height(2),
                    vec![
                        mainnet_header(&BLOCK_MAINNET_2_BYTES),
                        mainnet_header(&BLOCK_MAINNET_3_BYTES),
                    ],
                    vec![0, 0],
                    roots_from_height(block::Height(2), 2),
                )
                .expect("test response vectors align"),
            )
            .expect("test payload is contiguous"),
        },
    );

    state.retire_peer_session_auth(&peer, Some(7));

    assert!(!state.pending_operations.contains_key(&auth_operation));
    assert!(state.schedule.state(auth_range).is_none());
    assert!(state.schedule.authenticate_roots.contains(&auth_range));
    assert!(!state.buffered.contains_key(&(
        RangePriority::AuthenticateRoots,
        buffered_range.start_height()
    )));
    assert!(state.schedule.authenticate_roots.contains(&buffered_range));
    assert!(state
        .pending_operations
        .get(&commit_operation)
        .expect("canonical commit remains pending")
        .retention_candidate
        .is_none());
    assert!(state.retained_roots.is_empty());
}

#[test]
fn clear_inflight_root_auth_completes_on_advancement() {
    let network = Network::Mainnet;
    let startup = startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    );
    let mut state = HeaderSyncCore::new(&startup).expect("startup is coherent");
    let peer = peer(220);
    let auth_operation = HeaderSyncOperationIdentity {
        wire_request: HeaderSyncWireRequestIdentity {
            peer: peer.clone(),
            session_id: 1,
            request_id: HeaderSyncRequestId::new(1).expect("request ID is non-zero"),
        },
        op_kind: HeaderSyncOperationKind::AuthenticateRoots,
    };
    let commit_operation = HeaderSyncOperationIdentity {
        wire_request: HeaderSyncWireRequestIdentity {
            peer,
            session_id: 1,
            request_id: HeaderSyncRequestId::new(2).expect("request ID is non-zero"),
        },
        op_kind: HeaderSyncOperationKind::CommitHeaders,
    };
    let auth_range = RangeRequest {
        range: CheckedHeaderRange::from_count(block::Height(1), 2).expect("test range is bounded"),
        anchor_hash: Some(network.genesis_hash()),
        finalized: true,
        want_tree_aux_roots: true,
        priority: RangePriority::AuthenticateRoots,
    };
    state
        .schedule
        .mark_authenticating(auth_operation.clone(), auth_range);
    state.pending_operations.insert(
        auth_operation.clone(),
        PendingOperation {
            range: auth_range,
            purpose: RangePurpose::AuthenticateRoots,
            retention_candidate: None,
            root_auth: Some(PendingRootAuth {
                source: RootAuthSource::Fallback,
                expected: HeaderRootAuthState {
                    authenticated_height: block::Height(0),
                    authenticated_hash: network.genesis_hash(),
                    completed_checkpoint_height: block::Height(0),
                    completed_checkpoint_hash: network.genesis_hash(),
                },
            }),
            completion_observed: false,
        },
    );
    state.pending_operations.insert(
        commit_operation.clone(),
        PendingOperation {
            range: RangeRequest {
                priority: RangePriority::Forward,
                ..auth_range
            },
            purpose: RangePurpose::Sync,
            retention_candidate: None,
            root_auth: None,
            completion_observed: false,
        },
    );

    state.clear_inflight_root_auth(true);

    assert!(!state.pending_operations.contains_key(&auth_operation));
    assert!(state.pending_operations.contains_key(&commit_operation));
    assert!(state.schedule.state(auth_range).is_none());
    assert!(!state.schedule.authenticate_roots.contains(&auth_range));
}

#[test]
fn completed_inflight_root_auth_waits_for_driver_completion() {
    let network = Network::Mainnet;
    let startup = startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    );
    let mut state = HeaderSyncCore::new(&startup).expect("startup is coherent");
    let auth_operation = HeaderSyncOperationIdentity {
        wire_request: HeaderSyncWireRequestIdentity {
            peer: peer(222),
            session_id: 1,
            request_id: HeaderSyncRequestId::new(1).expect("request ID is non-zero"),
        },
        op_kind: HeaderSyncOperationKind::AuthenticateRoots,
    };
    let auth_range = RangeRequest {
        range: CheckedHeaderRange::from_count(block::Height(1), 2).expect("test range is bounded"),
        anchor_hash: Some(network.genesis_hash()),
        finalized: true,
        want_tree_aux_roots: true,
        priority: RangePriority::AuthenticateRoots,
    };
    state
        .schedule
        .mark_authenticating(auth_operation.clone(), auth_range);
    state.pending_operations.insert(
        auth_operation.clone(),
        PendingOperation {
            range: auth_range,
            purpose: RangePurpose::AuthenticateRoots,
            retention_candidate: None,
            root_auth: Some(PendingRootAuth {
                source: RootAuthSource::Fallback,
                expected: HeaderRootAuthState {
                    authenticated_height: block::Height(0),
                    authenticated_hash: network.genesis_hash(),
                    completed_checkpoint_height: block::Height(0),
                    completed_checkpoint_hash: network.genesis_hash(),
                },
            }),
            completion_observed: false,
        },
    );

    state.clear_completed_inflight_root_auth();

    assert!(state.pending_operations.contains_key(&auth_operation));
    assert!(matches!(
        state.schedule.state(auth_range),
        Some(HeaderWorkState::Committing { operation }) if operation == &auth_operation
    ));

    state
        .pending_operations
        .get_mut(&auth_operation)
        .expect("authentication remains pending")
        .completion_observed = true;
    state.clear_completed_inflight_root_auth();

    assert!(!state.pending_operations.contains_key(&auth_operation));
    assert!(state.schedule.state(auth_range).is_none());
}

#[test]
fn clear_inflight_root_auth_retires_without_requeue_on_rebase() {
    let network = Network::Mainnet;
    let startup = startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    );
    let mut state = HeaderSyncCore::new(&startup).expect("startup is coherent");
    let auth_operation = HeaderSyncOperationIdentity {
        wire_request: HeaderSyncWireRequestIdentity {
            peer: peer(221),
            session_id: 1,
            request_id: HeaderSyncRequestId::new(1).expect("request ID is non-zero"),
        },
        op_kind: HeaderSyncOperationKind::AuthenticateRoots,
    };
    let auth_range = RangeRequest {
        range: CheckedHeaderRange::from_count(block::Height(1), 2).expect("test range is bounded"),
        anchor_hash: Some(network.genesis_hash()),
        finalized: true,
        want_tree_aux_roots: true,
        priority: RangePriority::AuthenticateRoots,
    };
    state
        .schedule
        .mark_authenticating(auth_operation.clone(), auth_range);
    state.pending_operations.insert(
        auth_operation.clone(),
        PendingOperation {
            range: auth_range,
            purpose: RangePurpose::AuthenticateRoots,
            retention_candidate: None,
            root_auth: Some(PendingRootAuth {
                source: RootAuthSource::Fallback,
                expected: HeaderRootAuthState {
                    authenticated_height: block::Height(0),
                    authenticated_hash: network.genesis_hash(),
                    completed_checkpoint_height: block::Height(0),
                    completed_checkpoint_hash: network.genesis_hash(),
                },
            }),
            completion_observed: false,
        },
    );

    state.clear_inflight_root_auth(false);

    assert!(!state.pending_operations.contains_key(&auth_operation));
    assert!(state.schedule.state(auth_range).is_none());
    // Retire frees the slot without claiming success or retrying.
    assert!(!state.schedule.authenticate_roots.contains(&auth_range));
}

#[test]
fn prune_root_auth_pipeline_keeps_in_window_work() {
    let network = Network::Mainnet;
    let startup = startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    );
    let mut state = HeaderSyncCore::new(&startup).expect("startup is coherent");
    let behind = RangeRequest {
        range: CheckedHeaderRange::from_count(block::Height(1), 2).expect("test range is bounded"),
        anchor_hash: Some(network.genesis_hash()),
        finalized: true,
        want_tree_aux_roots: true,
        priority: RangePriority::AuthenticateRoots,
    };
    let in_window = RangeRequest {
        range: CheckedHeaderRange::from_count(block::Height(3), 2).expect("test range is bounded"),
        anchor_hash: None,
        finalized: true,
        want_tree_aux_roots: true,
        priority: RangePriority::AuthenticateRoots,
    };
    let past_checkpoint = RangeRequest {
        range: CheckedHeaderRange::from_count(block::Height(5), 2).expect("test range is bounded"),
        anchor_hash: None,
        finalized: true,
        want_tree_aux_roots: true,
        priority: RangePriority::AuthenticateRoots,
    };
    let forward = RangeRequest {
        range: CheckedHeaderRange::from_count(block::Height(1), 2).expect("test range is bounded"),
        anchor_hash: Some(network.genesis_hash()),
        finalized: false,
        want_tree_aux_roots: false,
        priority: RangePriority::Forward,
    };
    for range in [behind, in_window, past_checkpoint] {
        state
            .schedule
            .ensure(range, RangePriority::AuthenticateRoots);
    }
    state.schedule.ensure_forward(forward);

    let wire_request = HeaderSyncWireRequestIdentity {
        peer: peer(222),
        session_id: 1,
        request_id: HeaderSyncRequestId::new(1).expect("request ID is non-zero"),
    };
    let payload = HeaderRangePayload::new(
        HeaderRangeEntry::from_parallel(
            block::Height(1),
            vec![
                mainnet_header(&BLOCK_MAINNET_1_BYTES),
                mainnet_header(&BLOCK_MAINNET_2_BYTES),
            ],
            vec![0, 0],
            roots_from_height(block::Height(1), 2),
        )
        .expect("test response vectors align"),
    )
    .expect("test payload is contiguous");
    for range in [behind, in_window, past_checkpoint] {
        state.buffered.insert(
            (RangePriority::AuthenticateRoots, range.start_height()),
            BufferedHeaderRange {
                wire_request: wire_request.clone(),
                range,
                purpose: RangePurpose::AuthenticateRoots,
                payload: payload.clone(),
            },
        );
    }
    state.buffered.insert(
        (RangePriority::Forward, forward.start_height()),
        BufferedHeaderRange {
            wire_request: wire_request.clone(),
            range: forward,
            purpose: RangePurpose::Sync,
            payload: payload.clone(),
        },
    );

    state.prune_root_auth_pipeline(
        HeaderRootAuthState {
            authenticated_height: block::Height(2),
            authenticated_hash: block::Hash([2; 32]),
            completed_checkpoint_height: block::Height(4),
            completed_checkpoint_hash: block::Hash([4; 32]),
        },
        true,
    );

    assert!(!state.schedule.authenticate_roots.contains(&behind));
    assert!(state.schedule.authenticate_roots.contains(&in_window));
    assert!(state.schedule.authenticate_roots.contains(&past_checkpoint));
    assert!(state.schedule.forward.contains(&forward));
    assert!(!state
        .buffered
        .contains_key(&(RangePriority::AuthenticateRoots, behind.start_height())));
    assert!(state
        .buffered
        .contains_key(&(RangePriority::AuthenticateRoots, in_window.start_height())));
    assert!(!state.buffered.contains_key(&(
        RangePriority::AuthenticateRoots,
        past_checkpoint.start_height()
    )));
    assert!(state
        .buffered
        .contains_key(&(RangePriority::Forward, forward.start_height())));
}

#[test]
fn discard_root_auth_pipeline_clears_auth_lane_only() {
    let network = Network::Mainnet;
    let startup = startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    );
    let mut state = HeaderSyncCore::new(&startup).expect("startup is coherent");
    let auth_range = RangeRequest {
        range: CheckedHeaderRange::from_count(block::Height(1), 2).expect("test range is bounded"),
        anchor_hash: Some(network.genesis_hash()),
        finalized: true,
        want_tree_aux_roots: true,
        priority: RangePriority::AuthenticateRoots,
    };
    let forward = RangeRequest {
        range: CheckedHeaderRange::from_count(block::Height(1), 2).expect("test range is bounded"),
        anchor_hash: Some(network.genesis_hash()),
        finalized: false,
        want_tree_aux_roots: false,
        priority: RangePriority::Forward,
    };
    state
        .schedule
        .ensure(auth_range, RangePriority::AuthenticateRoots);
    state.schedule.ensure_forward(forward);

    let wire_request = HeaderSyncWireRequestIdentity {
        peer: peer(223),
        session_id: 1,
        request_id: HeaderSyncRequestId::new(1).expect("request ID is non-zero"),
    };
    let payload = HeaderRangePayload::new(
        HeaderRangeEntry::from_parallel(
            block::Height(1),
            vec![
                mainnet_header(&BLOCK_MAINNET_1_BYTES),
                mainnet_header(&BLOCK_MAINNET_2_BYTES),
            ],
            vec![0, 0],
            roots_from_height(block::Height(1), 2),
        )
        .expect("test response vectors align"),
    )
    .expect("test payload is contiguous");
    state.buffered.insert(
        (RangePriority::AuthenticateRoots, auth_range.start_height()),
        BufferedHeaderRange {
            wire_request: wire_request.clone(),
            range: auth_range,
            purpose: RangePurpose::AuthenticateRoots,
            payload: payload.clone(),
        },
    );
    state.buffered.insert(
        (RangePriority::Forward, forward.start_height()),
        BufferedHeaderRange {
            wire_request,
            range: forward,
            purpose: RangePurpose::Sync,
            payload,
        },
    );

    state.discard_root_auth_pipeline();

    assert!(state.schedule.authenticate_roots.is_empty());
    assert!(state.schedule.forward.contains(&forward));
    assert!(!state
        .buffered
        .keys()
        .any(|(priority, _)| { *priority == RangePriority::AuthenticateRoots }));
    assert!(state
        .buffered
        .contains_key(&(RangePriority::Forward, forward.start_height())));
}

#[tokio::test]
async fn peer_caps_reject_full_without_status_or_misbehavior_and_free_on_remove() {
    let network = Network::Mainnet;
    let anchor = (block::Height(0), network.genesis_hash());
    let mut startup = HeaderSyncStartup::new(
        network,
        anchor,
        HeaderSyncFrontiers {
            finalized_height: anchor.0,
            verified_block_tip: anchor.0,
            verified_block_hash: anchor.1,
        },
        Some(anchor),
        ZakuraHeaderSyncConfig {
            peer_limits: ServicePeerLimits {
                max_inbound_peers: 1,
                ..ServicePeerLimits::default()
            },
            ..ZakuraHeaderSyncConfig::default()
        },
        LOCAL_MAX_MESSAGE_BYTES,
    );
    startup.range_state_actions_enabled = false;
    let mut fixture = spawn_test_reactor(startup);
    let admitted = peer(11);
    let rejected = peer(12);

    connect_peer(&fixture, admitted.clone()).await;
    assert!(matches!(
        next_action(&mut fixture.actions).await,
        HeaderSyncAction::SendMessage {
            peer,
            msg: HeaderSyncMessage::Status(_),
            ..
        } if peer == admitted
    ));
    assert_eq!(fixture.handle.peer_snapshot().inbound_peers, 1);
    assert_eq!(fixture.handle.peer_snapshot().inbound_slots_free, 0);

    let rejected_cancel =
        connect_peer_with_direction(&fixture, rejected.clone(), ServicePeerDirection::Inbound)
            .await;
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        rejected_cancel.cancelled(),
    )
    .await
    .expect("rejected header-sync service session is locally parked");
    assert_eq!(fixture.handle.peer_snapshot().inbound_peers, 1);

    fixture
        .handle
        .send(HeaderSyncEvent::WireMessage {
            peer: rejected.clone(),
            msg: HeaderSyncMessage::Status(HeaderSyncStatus::default()),
        })
        .await
        .unwrap();
    while let Ok(Some(action)) =
        tokio::time::timeout(std::time::Duration::from_millis(50), fixture.actions.recv()).await
    {
        assert!(
            !matches!(
                action,
                HeaderSyncAction::SendMessage { ref peer, .. } if *peer == rejected
            ),
            "rejected peer must not receive header-sync scheduling state"
        );
        assert!(
            !matches!(
                action,
                HeaderSyncAction::Misbehavior { ref peer, .. } if *peer == rejected
            ),
            "locally rejected peer must not be scored as misbehaving"
        );
    }

    fixture
        .handle
        .send(HeaderSyncEvent::PeerDisconnected(admitted))
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    assert_eq!(fixture.handle.peer_snapshot().inbound_peers, 0);
    assert_eq!(fixture.handle.peer_snapshot().inbound_slots_free, 1);
}

#[tokio::test(flavor = "current_thread")]
async fn advisory_summary_status_mismatch_uses_status_without_misbehavior_and_backs_off() {
    let network = regtest_network();
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let (peer_id, peer_node_id) = node_peer();

    fixture
        .handle
        .send(HeaderSyncEvent::AdvisoryHeaderSummary {
            peer: peer_id.clone(),
            summary: advisory_header_summary(block::Height(10), 1),
        })
        .await
        .unwrap();
    assert!(fixture
        .handle
        .candidate_state()
        .backed_off_node_ids
        .is_empty());

    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(
        &fixture,
        peer_id.clone(),
        block::Height(0),
        block::Height(1),
        1,
        1,
    )
    .await;

    let mut saw_status_authoritative_request = false;
    let mut request_id = None;
    while let Ok(Some(action)) = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        fixture.actions.recv(),
    )
    .await
    {
        match action {
            HeaderSyncAction::Misbehavior { .. } => {
                panic!("summary/Status mismatch must not score misbehavior")
            }
            HeaderSyncAction::SendMessage {
                peer,
                request_id: sent_request_id,
                msg:
                    HeaderSyncMessage::GetHeaders {
                        start_height,
                        count,
                        want_tree_aux_roots: true,
                    },
            } if peer == peer_id => {
                assert_eq!(start_height, block::Height(1));
                assert_eq!(count, 1);
                request_id = sent_request_id;
                saw_status_authoritative_request = true;
                break;
            }
            _ => {}
        }
    }
    assert!(saw_status_authoritative_request);
    let request_id = request_id.expect("the peer received an outbound GetHeaders");

    send_headers(&fixture, &peer_id, request_id, headers_message(Vec::new())).await;
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    assert!(
        fixture
            .handle
            .candidate_state()
            .backed_off_node_ids
            .contains(&peer_node_id),
        "repeated unconfirmed advisory usefulness enters local non-punitive backoff"
    );
    assert_no_commit_or_misbehavior(&mut fixture.actions).await;
}

#[tokio::test(flavor = "current_thread")]
async fn advisory_backoff_is_pruned_on_peer_disconnected() {
    let network = regtest_network();
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let (peer_id, peer_node_id) = node_peer();

    fixture
        .handle
        .send(HeaderSyncEvent::AdvisoryHeaderSummary {
            peer: peer_id.clone(),
            summary: advisory_header_summary(block::Height(10), 1),
        })
        .await
        .unwrap();

    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(
        &fixture,
        peer_id.clone(),
        block::Height(0),
        block::Height(1),
        1,
        1,
    )
    .await;

    let mut request_id = None;
    while let Ok(Some(action)) = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        fixture.actions.recv(),
    )
    .await
    {
        if let HeaderSyncAction::SendMessage {
            peer,
            request_id: sent_request_id,
            msg: HeaderSyncMessage::GetHeaders { .. },
        } = action
        {
            if peer == peer_id {
                request_id = sent_request_id;
                break;
            }
        }
    }
    let request_id = request_id.expect("the peer received an outbound GetHeaders");

    send_headers(&fixture, &peer_id, request_id, headers_message(Vec::new())).await;
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    assert!(fixture
        .handle
        .candidate_state()
        .backed_off_node_ids
        .contains(&peer_node_id));

    fixture
        .handle
        .send(HeaderSyncEvent::PeerDisconnected(peer_id.clone()))
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    assert!(
        !fixture
            .handle
            .candidate_state()
            .backed_off_node_ids
            .contains(&peer_node_id),
        "disconnect prunes advisory backoff state"
    );
    assert_no_commit_or_misbehavior(&mut fixture.actions).await;
}

#[tokio::test(flavor = "current_thread")]
async fn admission_failure_after_advisory_selection_creates_no_outstanding_range() {
    let network = regtest_network();
    let anchor = (block::Height(0), network.genesis_hash());
    let mut startup = HeaderSyncStartup::new(
        network,
        anchor,
        HeaderSyncFrontiers {
            finalized_height: anchor.0,
            verified_block_tip: anchor.0,
            verified_block_hash: anchor.1,
        },
        Some(anchor),
        ZakuraHeaderSyncConfig {
            peer_limits: ServicePeerLimits {
                max_inbound_peers: 0,
                ..ServicePeerLimits::default()
            },
            ..ZakuraHeaderSyncConfig::default()
        },
        LOCAL_MAX_MESSAGE_BYTES,
    );
    startup.range_state_actions_enabled = true;
    let mut fixture = spawn_test_reactor(startup);
    let peer_id = peer(22);

    fixture
        .handle
        .send(HeaderSyncEvent::AdvisoryHeaderSummary {
            peer: peer_id.clone(),
            summary: advisory_header_summary(block::Height(10), 1),
        })
        .await
        .unwrap();
    let cancel =
        connect_peer_with_direction(&fixture, peer_id.clone(), ServicePeerDirection::Inbound).await;
    tokio::time::timeout(std::time::Duration::from_secs(1), cancel.cancelled())
        .await
        .expect("admission failure parks the service session");

    advertise_tip(
        &fixture,
        peer_id.clone(),
        block::Height(0),
        block::Height(10),
        1,
        1,
    )
    .await;

    while let Ok(Some(action)) =
        tokio::time::timeout(std::time::Duration::from_millis(50), fixture.actions.recv()).await
    {
        assert!(
            !matches!(
                action,
                HeaderSyncAction::SendMessage {
                    ref peer,
                    msg: HeaderSyncMessage::GetHeaders { .. },
                    ..
                } if *peer == peer_id
            ),
            "locally rejected advisory peer must not get outstanding range work"
        );
        assert!(
            !matches!(
                action,
                HeaderSyncAction::Misbehavior { ref peer, .. } if *peer == peer_id
            ),
            "admission failure is local and non-punitive"
        );
    }
}

fn spawn_test_reactor(startup: HeaderSyncStartup) -> ReactorFixture {
    let (handle, actions, task) = spawn_header_sync_reactor(startup).unwrap();
    ReactorFixture {
        handle,
        actions,
        task,
        outbound_receivers: Mutex::new(Vec::new()),
    }
}

async fn next_action(actions: &mut mpsc::Receiver<HeaderSyncAction>) -> HeaderSyncAction {
    tokio::time::timeout(std::time::Duration::from_secs(5), actions.recv())
        .await
        .expect("action arrives before timeout")
        .expect("reactor action channel stays open")
}

async fn next_non_query_action(actions: &mut mpsc::Receiver<HeaderSyncAction>) -> HeaderSyncAction {
    loop {
        let action = next_action(actions).await;
        if !matches!(
            action,
            HeaderSyncAction::QueryBestHeaderTip
                | HeaderSyncAction::QueryMissingBlockBodies { .. }
                | HeaderSyncAction::QueryHeadersByHeightRange { .. }
                | HeaderSyncAction::HeaderAdvanced { .. }
        ) {
            return action;
        }
    }
}

async fn next_query_headers_action(
    actions: &mut mpsc::Receiver<HeaderSyncAction>,
) -> HeaderSyncAction {
    loop {
        let action = next_action(actions).await;
        if matches!(action, HeaderSyncAction::QueryHeadersByHeightRange { .. }) {
            return action;
        }
    }
}

async fn next_outbound_get_headers(
    actions: &mut mpsc::Receiver<HeaderSyncAction>,
) -> (ZakuraPeerId, HeaderSyncRequestId, block::Height, u32) {
    loop {
        match next_non_query_action(actions).await {
            HeaderSyncAction::SendMessage {
                peer,
                request_id,
                msg:
                    HeaderSyncMessage::GetHeaders {
                        start_height,
                        count,
                        want_tree_aux_roots: true,
                    },
            } => {
                return (
                    peer,
                    request_id.expect("an outbound GetHeaders always carries a request ID"),
                    start_height,
                    count,
                )
            }
            HeaderSyncAction::Misbehavior { peer, reason } => {
                panic!("unexpected misbehavior from {peer:?}: {reason:?}")
            }
            _ => {}
        }
    }
}

/// Await the next outbound `GetHeaders` and return the request ID the session
/// allocated for it, so a test can echo that exact ID back in its response.
async fn next_get_headers_request_id(
    actions: &mut mpsc::Receiver<HeaderSyncAction>,
) -> HeaderSyncRequestId {
    loop {
        if let HeaderSyncAction::SendMessage {
            request_id,
            msg: HeaderSyncMessage::GetHeaders { .. },
            ..
        } = next_non_query_action(actions).await
        {
            return request_id.expect("an outbound GetHeaders always carries a request ID");
        }
    }
}

/// Deliver a `Headers` response correlated to `request_id` on the peer's session.
async fn send_headers(
    fixture: &ReactorFixture,
    peer: &ZakuraPeerId,
    request_id: HeaderSyncRequestId,
    msg: HeaderSyncMessage,
) {
    let HeaderSyncMessage::Headers {
        headers,
        body_sizes,
        tree_aux_roots,
    } = msg
    else {
        panic!("send_headers requires a Headers message");
    };
    let entries = if !headers.is_empty() && tree_aux_roots.is_empty() {
        headers
            .into_iter()
            .zip(body_sizes)
            .map(|(header, body_size)| HeaderRangeEntry {
                height: test_header_height(header.as_ref()),
                header,
                body_size,
                tree_aux_root: None,
            })
            .collect()
    } else {
        let start = tree_aux_roots
            .first()
            .map(|root| root.height)
            .unwrap_or(block::Height(0));
        HeaderRangeEntry::from_parallel(start, headers, body_sizes, tree_aux_roots)
            .expect("test response vectors align")
    };
    fixture
        .handle
        .send(HeaderSyncEvent::WireHeaders {
            wire_request: HeaderSyncWireRequestIdentity {
                peer: peer.clone(),
                session_id: 0,
                request_id,
            },
            entries,
        })
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn rooted_forward_requests_overlap_once_through_handoff() {
    let checkpoint_hash = block::Hash::from(mainnet_header(&BLOCK_MAINNET_4_BYTES).as_ref());
    let (network, _) = checkpoint_testnet_with_hash(block::Height(4), checkpoint_hash);
    let anchor = (block::Height(0), network.genesis_hash());
    let mut startup = startup_for(network, anchor, Some(anchor));
    startup.config.max_headers_per_response = 2;
    startup.header_root_auth = Some(HeaderRootAuthState {
        authenticated_height: anchor.0,
        authenticated_hash: anchor.1,
        completed_checkpoint_height: anchor.0,
        completed_checkpoint_hash: anchor.1,
    });
    let mut fixture = spawn_test_reactor(startup);
    let peer_id = peer(214);

    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(&fixture, peer_id.clone(), anchor.0, block::Height(8), 2, 4).await;

    let mut requests = BTreeMap::new();
    for _ in 0..3 {
        let (peer, request_id, start, count) =
            next_outbound_get_headers(&mut fixture.actions).await;
        assert_eq!(peer, peer_id);
        assert_eq!(count, 2);
        requests.insert(start, request_id);
    }
    assert_eq!(
        requests.keys().copied().collect::<Vec<_>>(),
        vec![block::Height(1), block::Height(2), block::Height(3)]
    );

    fixture.task.abort();
}

#[test]
fn rooted_forward_overlap_advances_past_intermediate_checkpoint() {
    let network = Parameters::build()
        .with_network_name("HsIntermediateOverlapTest")
        .expect("custom network name is valid")
        .with_genesis_hash(Network::Mainnet.genesis_hash())
        .expect("mainnet genesis hash is valid")
        .with_activation_heights(ConfiguredActivationHeights {
            overwinter: Some(1),
            sapling: Some(2),
            blossom: Some(3),
            heartwood: Some(4),
            canopy: Some(4),
            ..Default::default()
        })
        .expect("custom activation heights are in order")
        .clear_funding_streams()
        .with_checkpoints(ConfiguredCheckpoints::HeightsAndHashes(vec![
            (block::Height(0), Network::Mainnet.genesis_hash()),
            (block::Height(400), block::Hash([4; 32])),
            (block::Height(1_200), block::Hash([12; 32])),
        ]))
        .expect("custom checkpoints are valid")
        .to_network()
        .expect("custom testnet parameters are valid");
    let anchor = (block::Height(0), network.genesis_hash());
    let checkpoint = (block::Height(400), block::Hash([4; 32]));
    let mut startup = startup_for(network, anchor, Some(checkpoint));
    startup.config.max_headers_per_response = 600;
    startup.header_root_auth = Some(HeaderRootAuthState {
        authenticated_height: anchor.0,
        authenticated_hash: anchor.1,
        completed_checkpoint_height: checkpoint.0,
        completed_checkpoint_hash: checkpoint.1,
    });
    let mut state = HeaderSyncCore::new(&startup).expect("startup is coherent");
    let peer_id = peer(236);
    let payload = HeaderRangePayload::new(
        HeaderRangeEntry::from_parallel(
            block::Height(399),
            vec![
                mainnet_header(&BLOCK_MAINNET_1_BYTES),
                mainnet_header(&BLOCK_MAINNET_2_BYTES),
            ],
            vec![0, 0],
            roots_from_height(block::Height(399), 2),
        )
        .expect("test response vectors align"),
    )
    .expect("test payload is contiguous");
    assert!(state.admit_retained_root_payload(
        HeaderSyncWireRequestIdentity {
            peer: peer_id.clone(),
            session_id: 1,
            request_id: HeaderSyncRequestId::new(1).expect("request ID is non-zero"),
        },
        payload,
    ));
    let (send, _recv) = crate::zakura::framed_channel(32);
    let session = HeaderSyncPeerSession::from_parts_with_direction(
        peer_id.clone(),
        ServicePeerDirection::Inbound,
        send,
        CancellationToken::new(),
    );
    let mut peer_state = super::state::PeerHeaderState::new(
        session,
        anchor,
        600,
        2,
        std::time::Duration::from_secs(1),
        std::time::Duration::from_secs(1),
        std::time::Duration::from_secs(1),
    );
    peer_state.received_status = true;
    peer_state.advertised_tip = block::Height(1_000);
    state.peers.insert(peer_id, peer_state);

    state.refresh_forward_range(&startup);

    let first = state
        .schedule
        .forward
        .front()
        .expect("forward scheduling continues from retained checkpoint overlap");
    assert_eq!(first.start_height(), checkpoint.0);
    assert_eq!(first.count(), 600);
    assert_eq!(first.anchor_hash, None);
}

#[test]
fn refresh_forward_range_skips_one_height_retained_overlap_at_scheduled_tip() {
    // Reproduce the panic: retain-roots overlap continues from scheduled_end when
    // that end already equals the peer tip, so the next batch would be length 1
    // and `next_height(batch_start)..=batch_end` is inverted.
    let network = Parameters::build()
        .with_network_name("HsOneHeightOverlapPanic")
        .expect("custom network name is valid")
        .with_genesis_hash(Network::Mainnet.genesis_hash())
        .expect("mainnet genesis hash is valid")
        .with_activation_heights(ConfiguredActivationHeights {
            overwinter: Some(1),
            sapling: Some(2),
            blossom: Some(3),
            heartwood: Some(4),
            canopy: Some(4),
            ..Default::default()
        })
        .expect("custom activation heights are in order")
        .clear_funding_streams()
        .with_checkpoints(ConfiguredCheckpoints::HeightsAndHashes(vec![
            (block::Height(0), Network::Mainnet.genesis_hash()),
            (block::Height(400), block::Hash([4; 32])),
            (block::Height(1_200), block::Hash([12; 32])),
        ]))
        .expect("custom checkpoints are valid")
        .to_network()
        .expect("custom testnet parameters are valid");
    let anchor = (block::Height(0), network.genesis_hash());
    let tip = (block::Height(400), block::Hash([4; 32]));
    let peer_tip = block::Height(500);
    let mut startup = startup_for(network, anchor, Some(tip));
    startup.config.max_headers_per_response = 100;
    startup.header_root_auth = Some(HeaderRootAuthState {
        authenticated_height: tip.0,
        authenticated_hash: tip.1,
        completed_checkpoint_height: tip.0,
        completed_checkpoint_hash: tip.1,
    });
    let mut state = HeaderSyncCore::new(&startup).expect("startup is coherent");
    state.schedule.ensure_forward(RangeRequest {
        range: CheckedHeaderRange::from_bounds(block::Height(401), peer_tip)
            .expect("seeded forward range is bounded"),
        anchor_hash: None,
        finalized: false,
        want_tree_aux_roots: true,
        priority: RangePriority::Forward,
    });
    let peer_id = peer(237);
    let (send, _recv) = crate::zakura::framed_channel(32);
    let session = HeaderSyncPeerSession::from_parts_with_direction(
        peer_id.clone(),
        ServicePeerDirection::Inbound,
        send,
        CancellationToken::new(),
    );
    let mut peer_state = super::state::PeerHeaderState::new(
        session,
        tip,
        100,
        2,
        std::time::Duration::from_secs(1),
        std::time::Duration::from_secs(1),
        std::time::Duration::from_secs(1),
    );
    peer_state.received_status = true;
    peer_state.advertised_tip = peer_tip;
    state.peers.insert(peer_id, peer_state);

    state.refresh_forward_range(&startup);

    assert_eq!(
        state.schedule.highest_end(RangePriority::Forward),
        Some(peer_tip),
        "one-height retained overlap at the scheduled tip must stop without enqueueing"
    );
    assert_eq!(
        state.schedule.range_count(RangePriority::Forward),
        1,
        "refresh must not add a degenerate one-height overlap batch"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn committed_forward_payload_authenticates_without_fallback_request() {
    let header_2 = mainnet_header(&BLOCK_MAINNET_2_BYTES);
    let header_2_hash = block::Hash::from(header_2.as_ref());
    let checkpoint_hash = block::Hash::from(mainnet_header(&BLOCK_MAINNET_3_BYTES).as_ref());
    let (network, _) = checkpoint_testnet_with_hash(block::Height(3), checkpoint_hash);
    let anchor = (block::Height(0), network.genesis_hash());
    let mut startup = startup_for(network, anchor, Some(anchor));
    startup.config.max_headers_per_response = 2;
    startup.header_root_auth = Some(HeaderRootAuthState {
        authenticated_height: anchor.0,
        authenticated_hash: anchor.1,
        completed_checkpoint_height: block::Height(3),
        completed_checkpoint_hash: checkpoint_hash,
    });
    let mut fixture = spawn_test_reactor(startup);
    let peer_id = peer(234);

    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(&fixture, peer_id.clone(), anchor.0, block::Height(3), 2, 1).await;
    let (requested_peer, request_id, start, count) =
        next_outbound_get_headers(&mut fixture.actions).await;
    assert_eq!(requested_peer, peer_id);
    assert_eq!((start, count), (block::Height(1), 2));

    send_headers(
        &fixture,
        &peer_id,
        request_id,
        headers_message_from(
            block::Height(1),
            vec![mainnet_header(&BLOCK_MAINNET_1_BYTES), header_2],
        ),
    )
    .await;

    let commit_operation = loop {
        if let HeaderSyncAction::CommitHeaderRange { operation, .. } =
            next_non_query_action(&mut fixture.actions).await
        {
            break operation;
        }
    };
    fixture
        .handle
        .send(HeaderSyncEvent::HeaderRangeOperationCompleted {
            operation: commit_operation.clone(),
            tip_hash: header_2_hash,
        })
        .await
        .unwrap();

    let (auth_operation, payload) = loop {
        if let HeaderSyncAction::AuthenticateHeaderRoots {
            operation, payload, ..
        } = next_non_query_action(&mut fixture.actions).await
        {
            break (operation, payload);
        }
    };
    assert_eq!(auth_operation.wire_request, commit_operation.wire_request);
    assert_eq!(payload.range().start(), block::Height(1));
    assert_eq!(payload.range().end(), block::Height(2));

    fixture.task.abort();
}

async fn reactor_with_pending_retained_root_authentication(
    additional_peer: Option<ZakuraPeerId>,
) -> (
    ReactorFixture,
    HeaderSyncOperationIdentity,
    HeaderRootAuthState,
    ZakuraPeerId,
) {
    let header_1 = mainnet_header(&BLOCK_MAINNET_1_BYTES);
    let header_2 = mainnet_header(&BLOCK_MAINNET_2_BYTES);
    let header_2_hash = block::Hash::from(header_2.as_ref());
    let checkpoint_hash = block::Hash::from(mainnet_header(&BLOCK_MAINNET_3_BYTES).as_ref());
    let (network, checkpoint_hash) =
        checkpoint_testnet_with_hash(block::Height(3), checkpoint_hash);
    let anchor = (block::Height(0), network.genesis_hash());
    let auth = HeaderRootAuthState {
        authenticated_height: anchor.0,
        authenticated_hash: anchor.1,
        completed_checkpoint_height: block::Height(3),
        completed_checkpoint_hash: checkpoint_hash,
    };
    let mut startup = startup_for(network, anchor, Some(anchor));
    startup.config.max_headers_per_response = 2;
    startup.header_root_auth = Some(auth);
    let mut fixture = spawn_test_reactor(startup);
    let source_peer = peer(239);

    connect_peer(&fixture, source_peer.clone()).await;
    advertise_tip(
        &fixture,
        source_peer.clone(),
        anchor.0,
        block::Height(3),
        2,
        1,
    )
    .await;
    let (requested_peer, request_id, start, count) =
        next_outbound_get_headers(&mut fixture.actions).await;
    assert_eq!(requested_peer, source_peer);
    assert_eq!((start, count), (block::Height(1), 2));
    send_headers(
        &fixture,
        &source_peer,
        request_id,
        headers_message_from(block::Height(1), vec![header_1, header_2]),
    )
    .await;

    let commit_operation = loop {
        if let HeaderSyncAction::CommitHeaderRange { operation, .. } =
            next_non_query_action(&mut fixture.actions).await
        {
            break operation;
        }
    };
    fixture
        .handle
        .send(HeaderSyncEvent::HeaderRangeOperationCompleted {
            operation: commit_operation,
            tip_hash: header_2_hash,
        })
        .await
        .unwrap();
    let mut auth_operation = None;
    let mut next_range_observed = false;
    while auth_operation.is_none() || !next_range_observed {
        match next_non_query_action(&mut fixture.actions).await {
            HeaderSyncAction::AuthenticateHeaderRoots { operation, .. } => {
                auth_operation = Some(operation);
            }
            HeaderSyncAction::SendMessage {
                msg:
                    HeaderSyncMessage::GetHeaders {
                        start_height: block::Height(2),
                        want_tree_aux_roots: true,
                        ..
                    },
                ..
            } => {
                next_range_observed = true;
            }
            _ => {}
        }
    }

    if let Some(peer_id) = additional_peer {
        connect_peer(&fixture, peer_id.clone()).await;
        advertise_tip(&fixture, peer_id, anchor.0, block::Height(3), 2, 1).await;
    }

    (
        fixture,
        auth_operation.expect("root authentication action was observed"),
        auth,
        source_peer,
    )
}

#[tokio::test(flavor = "current_thread")]
async fn invalid_peer_root_auth_failure_scores_and_retries_avoiding_peer() {
    let retry_peer = peer(240);
    let (mut fixture, operation, _auth, source_peer) =
        reactor_with_pending_retained_root_authentication(Some(retry_peer.clone())).await;

    fixture
        .handle
        .send(HeaderSyncEvent::HeaderRootAuthenticationFailed {
            operation,
            kind: HeaderRootAuthenticationFailureKind::InvalidPeerRange,
        })
        .await
        .unwrap();

    let mut scored = false;
    loop {
        match next_non_query_action(&mut fixture.actions).await {
            HeaderSyncAction::Misbehavior { peer, reason } => {
                assert_eq!(peer, source_peer);
                assert_eq!(reason, HeaderSyncMisbehavior::InvalidRange);
                scored = true;
            }
            HeaderSyncAction::SendMessage {
                peer,
                msg:
                    HeaderSyncMessage::GetHeaders {
                        start_height,
                        count,
                        want_tree_aux_roots: true,
                    },
                ..
            } => {
                assert!(scored, "invalid roots are scored before retrying");
                assert_eq!(peer, retry_peer);
                assert_eq!((start_height, count), (block::Height(1), 2));
                break;
            }
            _ => {}
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn stale_root_auth_failure_waits_for_watch_without_scoring() {
    let (mut fixture, operation, auth, _source_peer) =
        reactor_with_pending_retained_root_authentication(None).await;

    fixture
        .handle
        .send(HeaderSyncEvent::HeaderRootAuthenticationFailed {
            operation,
            kind: HeaderRootAuthenticationFailureKind::Stale,
        })
        .await
        .unwrap();
    assert_no_root_authentication_or_misbehavior(&mut fixture.actions).await;

    fixture
        .handle
        .send(HeaderSyncEvent::HeaderRootAuthStateChanged(Some(auth)))
        .await
        .unwrap();
    loop {
        match next_non_query_action(&mut fixture.actions).await {
            HeaderSyncAction::AuthenticateHeaderRoots { .. } => break,
            HeaderSyncAction::Misbehavior { .. } => {
                panic!("stale root authentication failure must not score a peer")
            }
            _ => {}
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn stale_root_auth_failure_reschedules_when_watch_already_advanced() {
    let (mut fixture, operation, auth, _source_peer) =
        reactor_with_pending_retained_root_authentication(None).await;

    // Checkpoint-only watch update: auth tip unchanged so retained coverage stays
    // valid, but the launch snapshot no longer matches reactor-local state.
    let advanced = HeaderRootAuthState {
        completed_checkpoint_height: block::Height(4),
        completed_checkpoint_hash: block::Hash([4; 32]),
        ..auth
    };
    fixture
        .handle
        .send(HeaderSyncEvent::HeaderRootAuthStateChanged(Some(advanced)))
        .await
        .unwrap();
    assert_no_root_authentication_or_misbehavior(&mut fixture.actions).await;

    fixture
        .handle
        .send(HeaderSyncEvent::HeaderRootAuthenticationFailed {
            operation,
            kind: HeaderRootAuthenticationFailureKind::Stale,
        })
        .await
        .unwrap();

    loop {
        match next_non_query_action(&mut fixture.actions).await {
            HeaderSyncAction::AuthenticateHeaderRoots { expected_state, .. } => {
                assert_eq!(expected_state, advanced);
                break;
            }
            HeaderSyncAction::Misbehavior { .. } => {
                panic!("stale root authentication failure must not score a peer")
            }
            _ => {}
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn canonical_mismatch_root_auth_failure_drops_retained_and_retries_without_scoring() {
    let retry_peer = peer(241);
    let (mut fixture, operation, _auth, _source_peer) =
        reactor_with_pending_retained_root_authentication(Some(retry_peer.clone())).await;

    fixture
        .handle
        .send(HeaderSyncEvent::HeaderRootAuthenticationFailed {
            operation,
            kind: HeaderRootAuthenticationFailureKind::CanonicalMismatch {
                height: block::Height(1),
            },
        })
        .await
        .unwrap();

    loop {
        match next_non_query_action(&mut fixture.actions).await {
            HeaderSyncAction::Misbehavior { .. } => {
                panic!("canonical mismatch must not score a peer")
            }
            HeaderSyncAction::SendMessage {
                peer,
                msg:
                    HeaderSyncMessage::GetHeaders {
                        start_height,
                        count,
                        want_tree_aux_roots: true,
                    },
                ..
            } => {
                assert_eq!(peer, retry_peer);
                assert_eq!((start_height, count), (block::Height(1), 2));
                break;
            }
            HeaderSyncAction::AuthenticateHeaderRoots { .. } => {
                panic!("canonical mismatch must drop the retained payload")
            }
            _ => {}
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn local_root_auth_failure_retries_retained_payload_without_scoring() {
    let (mut fixture, operation, _auth, source_peer) =
        reactor_with_pending_retained_root_authentication(None).await;

    fixture
        .handle
        .send(HeaderSyncEvent::HeaderRootAuthenticationFailed {
            operation,
            kind: HeaderRootAuthenticationFailureKind::Local,
        })
        .await
        .unwrap();
    assert_no_root_authentication_or_misbehavior(&mut fixture.actions).await;

    loop {
        match next_non_query_action(&mut fixture.actions).await {
            HeaderSyncAction::AuthenticateHeaderRoots { operation, .. } => {
                assert_eq!(operation.wire_request.peer, source_peer);
                break;
            }
            HeaderSyncAction::Misbehavior { .. } => {
                panic!("local root authentication failure must not score a peer")
            }
            HeaderSyncAction::SendMessage {
                msg:
                    HeaderSyncMessage::GetHeaders {
                        want_tree_aux_roots: true,
                        ..
                    },
                ..
            } => panic!("retained local failure must not immediately fall back to the network"),
            _ => {}
        }
    }
}

async fn reactor_with_two_retained_root_batches(
) -> (ReactorFixture, HeaderSyncOperationIdentity, block::Hash) {
    let header_1 = mainnet_header(&BLOCK_MAINNET_1_BYTES);
    let header_1_hash = block::Hash::from(header_1.as_ref());
    let header_2 = mainnet_header(&BLOCK_MAINNET_2_BYTES);
    let header_2_hash = block::Hash::from(header_2.as_ref());
    let header_3 = mainnet_header(&BLOCK_MAINNET_3_BYTES);
    let header_3_hash = block::Hash::from(header_3.as_ref());
    let (network, _) = checkpoint_testnet_with_hash(block::Height(3), header_3_hash);
    let anchor = (block::Height(0), network.genesis_hash());
    let mut startup = startup_for(network, anchor, Some(anchor));
    startup.config.max_headers_per_response = 2;
    startup.header_root_auth = Some(HeaderRootAuthState {
        authenticated_height: anchor.0,
        authenticated_hash: anchor.1,
        completed_checkpoint_height: block::Height(3),
        completed_checkpoint_hash: header_3_hash,
    });
    let mut fixture = spawn_test_reactor(startup);
    let peer_id = peer(238);

    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(&fixture, peer_id.clone(), anchor.0, block::Height(3), 2, 1).await;
    let (_, first_request_id, first_start, first_count) =
        next_outbound_get_headers(&mut fixture.actions).await;
    assert_eq!((first_start, first_count), (block::Height(1), 2));
    send_headers(
        &fixture,
        &peer_id,
        first_request_id,
        headers_message_from(block::Height(1), vec![header_1, header_2.clone()]),
    )
    .await;

    let first_commit = loop {
        if let HeaderSyncAction::CommitHeaderRange { operation, .. } =
            next_non_query_action(&mut fixture.actions).await
        {
            break operation;
        }
    };
    fixture
        .handle
        .send(HeaderSyncEvent::HeaderRangeOperationCompleted {
            operation: first_commit,
            tip_hash: header_2_hash,
        })
        .await
        .unwrap();

    let mut first_auth = None;
    let mut second_request = None;
    while first_auth.is_none() || second_request.is_none() {
        match next_non_query_action(&mut fixture.actions).await {
            HeaderSyncAction::AuthenticateHeaderRoots { operation, .. } => {
                first_auth = Some(operation);
            }
            HeaderSyncAction::SendMessage {
                request_id,
                msg:
                    HeaderSyncMessage::GetHeaders {
                        start_height,
                        count,
                        want_tree_aux_roots: true,
                    },
                ..
            } => {
                second_request = Some((
                    request_id.expect("outbound root request has an ID"),
                    start_height,
                    count,
                ));
            }
            _ => {}
        }
    }
    let (second_request_id, second_start, second_count) =
        second_request.expect("second request was observed");
    assert_eq!((second_start, second_count), (block::Height(2), 2));
    send_headers(
        &fixture,
        &peer_id,
        second_request_id,
        headers_message_from(block::Height(2), vec![header_2, header_3]),
    )
    .await;

    let second_commit = loop {
        if let HeaderSyncAction::CommitHeaderRange {
            operation, payload, ..
        } = next_non_query_action(&mut fixture.actions).await
        {
            if payload.range().start() == block::Height(2) {
                break operation;
            }
        }
    };
    fixture
        .handle
        .send(HeaderSyncEvent::HeaderRangeOperationCompleted {
            operation: second_commit,
            tip_hash: header_3_hash,
        })
        .await
        .unwrap();

    (
        fixture,
        first_auth.expect("first authentication was observed"),
        header_1_hash,
    )
}

async fn assert_no_root_authentication(actions: &mut mpsc::Receiver<HeaderSyncAction>) {
    while let Ok(Some(action)) =
        tokio::time::timeout(std::time::Duration::from_millis(50), actions.recv()).await
    {
        assert!(
            !matches!(action, HeaderSyncAction::AuthenticateHeaderRoots { .. }),
            "a second durable root authentication was admitted early"
        );
    }
}

async fn assert_no_root_authentication_or_misbehavior(
    actions: &mut mpsc::Receiver<HeaderSyncAction>,
) {
    while let Ok(Some(action)) =
        tokio::time::timeout(std::time::Duration::from_millis(50), actions.recv()).await
    {
        assert!(
            !matches!(
                action,
                HeaderSyncAction::AuthenticateHeaderRoots { .. }
                    | HeaderSyncAction::Misbehavior { .. }
            ),
            "unexpected root authentication or peer score: {action:?}"
        );
    }
}

async fn expect_second_retained_root_authentication(
    actions: &mut mpsc::Receiver<HeaderSyncAction>,
) {
    loop {
        if let HeaderSyncAction::AuthenticateHeaderRoots { payload, .. } =
            next_non_query_action(actions).await
        {
            assert_eq!(payload.range().start(), block::Height(2));
            assert_eq!(payload.range().end(), block::Height(3));
            return;
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn root_auth_watch_waits_for_driver_completion_before_admitting_next_batch() {
    let (mut fixture, first_auth, authenticated_hash) =
        reactor_with_two_retained_root_batches().await;
    fixture
        .handle
        .send(HeaderSyncEvent::HeaderRootAuthStateChanged(Some(
            HeaderRootAuthState {
                authenticated_height: block::Height(1),
                authenticated_hash,
                completed_checkpoint_height: block::Height(3),
                completed_checkpoint_hash: block::Hash::from(
                    mainnet_header(&BLOCK_MAINNET_3_BYTES).as_ref(),
                ),
            },
        )))
        .await
        .unwrap();

    assert_no_root_authentication(&mut fixture.actions).await;

    fixture
        .handle
        .send(HeaderSyncEvent::HeaderRootAuthenticationCompleted {
            operation: first_auth,
        })
        .await
        .unwrap();
    expect_second_retained_root_authentication(&mut fixture.actions).await;
    fixture.task.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn root_auth_completion_waits_for_watch_before_admitting_next_batch() {
    let (mut fixture, first_auth, authenticated_hash) =
        reactor_with_two_retained_root_batches().await;
    fixture
        .handle
        .send(HeaderSyncEvent::HeaderRootAuthenticationCompleted {
            operation: first_auth,
        })
        .await
        .unwrap();

    assert_no_root_authentication(&mut fixture.actions).await;

    fixture
        .handle
        .send(HeaderSyncEvent::HeaderRootAuthStateChanged(Some(
            HeaderRootAuthState {
                authenticated_height: block::Height(1),
                authenticated_hash,
                completed_checkpoint_height: block::Height(3),
                completed_checkpoint_hash: block::Hash::from(
                    mainnet_header(&BLOCK_MAINNET_3_BYTES).as_ref(),
                ),
            },
        )))
        .await
        .unwrap();
    expect_second_retained_root_authentication(&mut fixture.actions).await;
    fixture.task.abort();
}

async fn assert_no_commit_or_misbehavior(actions: &mut mpsc::Receiver<HeaderSyncAction>) {
    while let Ok(Some(action)) =
        tokio::time::timeout(std::time::Duration::from_millis(50), actions.recv()).await
    {
        assert!(
            !matches!(
                action,
                HeaderSyncAction::CommitHeaderRange { .. } | HeaderSyncAction::Misbehavior { .. }
            ),
            "unexpected commit or misbehavior action: {action:?}"
        );
    }
}

async fn connect_peer(fixture: &ReactorFixture, peer_id: ZakuraPeerId) {
    connect_peer_with_direction(fixture, peer_id, ServicePeerDirection::Inbound).await;
}

async fn connect_peer_with_direction(
    fixture: &ReactorFixture,
    peer_id: ZakuraPeerId,
    direction: ServicePeerDirection,
) -> CancellationToken {
    let (send, recv) = crate::zakura::framed_channel(32);
    fixture
        .outbound_receivers
        .lock()
        .expect("test outbound receiver mutex ok")
        .push(recv);
    let cancel = CancellationToken::new();
    let session =
        HeaderSyncPeerSession::from_parts_with_direction(peer_id, direction, send, cancel.clone());
    fixture
        .handle
        .send(HeaderSyncEvent::PeerConnected(session))
        .await
        .unwrap();
    cancel
}

fn test_header_sync_handle() -> (HeaderSyncHandle, mpsc::UnboundedReceiver<HeaderSyncEvent>) {
    let (events, _events_rx) = mpsc::channel(16);
    let (lifecycle, lifecycle_rx) = mpsc::unbounded_channel();
    let (_tip_tx, tip) = watch::channel((block::Height(0), block::Hash([0; 32])));
    let (_peers_tx, peers) = watch::channel(ServicePeerSnapshot::default());
    let (_candidates_tx, candidates) = watch::channel(ZakuraHeaderSyncCandidateState::default());
    (
        HeaderSyncHandle {
            events,
            lifecycle,
            tip,
            peers,
            candidates,
        },
        lifecycle_rx,
    )
}

fn header_sync_peer_with_conn(
    peer_id: ZakuraPeerId,
    conn_id: ZakuraConnId,
    cancel_token: CancellationToken,
) -> (Peer, FramedSend) {
    let (peer_send, service_recv) = framed_channel(8);
    let (service_send, _peer_recv) = framed_channel(8);
    (
        Peer::new_with_conn_id_and_direction(
            conn_id,
            peer_id,
            None,
            ZAKURA_CAP_HEADER_SYNC,
            ServicePeerDirection::Outbound,
            HashMap::from([(ZAKURA_STREAM_HEADER_SYNC, (service_recv, service_send))]),
            cancel_token,
        ),
        peer_send,
    )
}

async fn wait_for_gauge(name: &str, expected: f64) {
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            if gauge_value(name) == expected {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("gauge reaches expected value before timeout");
}

#[tokio::test(flavor = "current_thread")]
async fn header_connectivity_gauges_track_membership_and_status_freshness() {
    let _ = header_sync_metrics_recorder();
    let network = regtest_network();
    let anchor = (block::Height(0), network.genesis_hash());
    let fixture = spawn_test_reactor(startup_for(network, anchor, None));
    let mut peers = fixture.handle.subscribe_peer_snapshot();
    let peer_id = peer(91);

    connect_peer(&fixture, peer_id.clone()).await;
    peers.changed().await.unwrap();
    assert_eq!(peers.borrow().inbound_peers, 1);
    wait_for_gauge("zakura.p2p.connected_peers", 1.0).await;
    wait_for_gauge("zakura.p2p.healthy_peers", 0.0).await;

    advertise_tip(
        &fixture,
        peer_id.clone(),
        block::Height(0),
        block::Height(1),
        1,
        1,
    )
    .await;
    wait_for_gauge("zakura.p2p.connected_peers", 1.0).await;
    wait_for_gauge("zakura.p2p.healthy_peers", 1.0).await;

    fixture
        .handle
        .send(HeaderSyncEvent::PeerDisconnected(peer_id))
        .await
        .unwrap();
    peers.changed().await.unwrap();
    assert_eq!(peers.borrow().inbound_peers, 0);
    wait_for_gauge("zakura.p2p.connected_peers", 0.0).await;
    wait_for_gauge("zakura.p2p.healthy_peers", 0.0).await;
}

#[tokio::test]
async fn stale_header_sync_teardown_keeps_replacement_session() {
    let (handle, mut lifecycle) = test_header_sync_handle();
    let service = HeaderSyncService::new(handle);
    let peer_id = peer(94);
    let old_conn_id = 1;
    let new_conn_id = 2;
    let old_cancel = CancellationToken::new();
    let new_cancel = CancellationToken::new();
    let (old_peer, _old_peer_send) =
        header_sync_peer_with_conn(peer_id.clone(), old_conn_id, old_cancel.clone());

    service.add_peer(old_peer);
    let _old_session = match lifecycle.recv().await {
        Some(HeaderSyncEvent::PeerConnected(session)) if session.peer_id() == &peer_id => session,
        event => panic!("expected old header-sync peer connection, got {event:?}"),
    };

    let (new_peer, _new_peer_send) =
        header_sync_peer_with_conn(peer_id.clone(), new_conn_id, new_cancel.clone());
    service.add_peer(new_peer);
    let _new_session = match lifecycle.recv().await {
        Some(HeaderSyncEvent::PeerConnected(session)) if session.peer_id() == &peer_id => session,
        event => panic!("expected replacement header-sync peer connection, got {event:?}"),
    };

    let (stale_peer, _stale_peer_send) =
        header_sync_peer_with_conn(peer_id.clone(), old_conn_id, CancellationToken::new());
    service.add_peer(stale_peer);

    service.remove_peer(&peer_id, old_conn_id);
    match tokio::time::timeout(std::time::Duration::from_millis(50), lifecycle.recv()).await {
        Err(_) => {}
        Ok(event) => {
            panic!("stale cleanup must not emit a header-sync lifecycle event: {event:?}");
        }
    }

    service.remove_peer(&peer_id, new_conn_id);
    assert!(matches!(
        lifecycle.recv().await,
        Some(HeaderSyncEvent::PeerDisconnected(disconnected)) if disconnected == peer_id
    ));
}

async fn advertise_tip(
    fixture: &ReactorFixture,
    peer_id: ZakuraPeerId,
    anchor_height: block::Height,
    tip_height: block::Height,
    max_headers_per_response: u32,
    max_inflight_requests: u16,
) {
    advertise_tip_with_hash(
        fixture,
        peer_id,
        anchor_height,
        tip_height,
        block::Hash([9; 32]),
        max_headers_per_response,
        max_inflight_requests,
    )
    .await;
}

async fn advertise_tip_with_hash(
    fixture: &ReactorFixture,
    peer_id: ZakuraPeerId,
    anchor_height: block::Height,
    tip_height: block::Height,
    tip_hash: block::Hash,
    max_headers_per_response: u32,
    max_inflight_requests: u16,
) {
    fixture
        .handle
        .send(HeaderSyncEvent::WireMessage {
            peer: peer_id,
            msg: HeaderSyncMessage::Status(HeaderSyncStatus {
                tip_height,
                tip_hash,
                anchor_height,
                max_headers_per_response,
                max_inflight_requests,
            }),
        })
        .await
        .unwrap();
}

#[test]
fn codec_round_trips_status() {
    let status = HeaderSyncStatus {
        tip_height: block::Height(10),
        tip_hash: block::Hash([9; 32]),
        anchor_height: block::Height(1),
        max_headers_per_response: DEFAULT_HS_RANGE,
        max_inflight_requests: DEFAULT_HS_MAX_INFLIGHT,
    };
    let message = HeaderSyncMessage::Status(status);

    let encoded = message.encode(None).unwrap();
    let (decoded, request_id) =
        HeaderSyncMessage::decode(&encoded, HeaderSyncDecodeContext::control()).unwrap();

    assert_eq!(decoded, message);
    assert_eq!(request_id, None);
}

#[test]
fn codec_round_trips_get_headers_request_id() {
    let request_id = HeaderSyncRequestId::new(42).expect("non-zero id");
    let message = HeaderSyncMessage::GetHeaders {
        start_height: block::Height(42),
        count: DEFAULT_HS_RANGE,
        want_tree_aux_roots: false,
    };

    let encoded = message.encode(Some(request_id)).unwrap();
    let (decoded, decoded_request_id) =
        HeaderSyncMessage::decode(&encoded, HeaderSyncDecodeContext::control()).unwrap();

    assert_eq!(decoded, message);
    assert_eq!(decoded_request_id, Some(request_id));
}

#[test]
fn get_headers_rejects_missing_and_zero_request_ids() {
    let request_id = HeaderSyncRequestId::new(1).expect("non-zero id");
    let message = HeaderSyncMessage::GetHeaders {
        start_height: block::Height(42),
        count: DEFAULT_HS_RANGE,
        want_tree_aux_roots: false,
    };

    assert!(matches!(
        message.encode(None),
        Err(HeaderSyncWireError::MissingRequestId {
            message: "GetHeaders"
        })
    ));

    let mut encoded = message
        .encode(Some(request_id))
        .expect("valid v7 request encodes");
    encoded[1..9].fill(0);
    assert!(matches!(
        HeaderSyncMessage::decode(&encoded, HeaderSyncDecodeContext::control(),),
        Err(HeaderSyncWireError::MissingRequestId {
            message: "GetHeaders"
        })
    ));
}

#[test]
fn codec_round_trips_headers_with_bounded_vector_and_request_id() {
    let request_id = HeaderSyncRequestId::new(43).expect("non-zero id");
    let headers = vec![mainnet_header(&BLOCK_MAINNET_1_BYTES)];
    let message = finalized_headers_message_with_sizes(headers, vec![123_456]);
    let expected = ExpectedHeadersResponse::new(request_id, block::Height(1), 1, true).unwrap();

    let encoded = message.encode(Some(request_id)).unwrap();
    let (decoded, decoded_request_id) = HeaderSyncMessage::decode(
        &encoded,
        HeaderSyncDecodeContext::for_headers_response(expected, 1),
    )
    .unwrap();

    assert_eq!(decoded, message);
    assert_eq!(decoded_request_id, Some(request_id));
}

#[test]
fn headers_rejects_missing_and_zero_request_ids() {
    let request_id = HeaderSyncRequestId::new(1).expect("non-zero id");
    let message = HeaderSyncMessage::Headers {
        headers: Vec::new(),
        body_sizes: Vec::new(),
        tree_aux_roots: Vec::new(),
    };
    let expected = ExpectedHeadersResponse::new(request_id, block::Height(1), 1, false)
        .expect("count is valid");

    assert!(matches!(
        message.encode(None),
        Err(HeaderSyncWireError::MissingRequestId { message: "Headers" })
    ));

    let mut encoded = message
        .encode(Some(request_id))
        .expect("valid v7 response encodes");
    encoded[1..9].fill(0);
    assert!(matches!(
        HeaderSyncMessage::decode(
            &encoded,
            HeaderSyncDecodeContext::for_headers_response(expected, 1),
        ),
        Err(HeaderSyncWireError::MissingRequestId { message: "Headers" })
    ));
}

#[test]
fn codec_round_trips_headers_with_unknown_body_size_sentinel() {
    let headers = vec![mainnet_header(&BLOCK_MAINNET_1_BYTES)];
    let message = finalized_headers_message_with_sizes(headers, vec![0]);

    let encoded = encode_correlated(&message).unwrap();
    let (decoded, _request_id) =
        HeaderSyncMessage::decode(&encoded, finalized_headers_context(1, 1)).unwrap();

    assert_eq!(decoded, message);
}

#[test]
fn decode_rejects_tree_aux_roots_when_not_requested() {
    let headers = vec![mainnet_header(&BLOCK_MAINNET_1_BYTES)];
    let message = finalized_headers_message_with_sizes(headers, vec![0]);
    let encoded = encode_correlated(&message).unwrap();

    // A response carrying tree-aux roots against a request that did not ask for
    // them (a non-finalized range) is rejected at decode before allocation.
    assert!(matches!(
        HeaderSyncMessage::decode(&encoded, headers_context(1, 1)),
        Err(HeaderSyncWireError::UnrequestedTreeAuxRoots)
    ));
}

#[test]
fn codec_round_trips_new_block() {
    let message = HeaderSyncMessage::NewBlock(mainnet_block(&BLOCK_MAINNET_1_BYTES));

    let encoded = message.encode(None).unwrap();
    let (decoded, request_id) =
        HeaderSyncMessage::decode(&encoded, HeaderSyncDecodeContext::control()).unwrap();

    assert_eq!(decoded, message);
    assert_eq!(request_id, None);
}

#[test]
fn codec_rejects_unknown_message_types_and_trailing_bytes() {
    assert!(matches!(
        HeaderSyncMessage::decode(&[99], HeaderSyncDecodeContext::control()),
        Err(HeaderSyncWireError::UnknownMessageType(99))
    ));

    let mut encoded = encode_correlated(&HeaderSyncMessage::GetHeaders {
        start_height: block::Height(1),
        count: 1,
        want_tree_aux_roots: false,
    })
    .unwrap();
    encoded.push(0);

    assert!(matches!(
        HeaderSyncMessage::decode(&encoded, HeaderSyncDecodeContext::control()),
        Err(HeaderSyncWireError::TrailingBytes)
    ));
}

#[test]
fn headers_codec_rejects_body_size_mismatch_truncation_and_trailing_bytes() {
    let headers = vec![mainnet_header(&BLOCK_MAINNET_1_BYTES)];
    let message = headers_message_with_sizes(headers.clone(), vec![100]);

    assert!(matches!(
        encode_correlated(&headers_message_with_sizes(headers.clone(), vec![100, 200])),
        Err(HeaderSyncWireError::BodySizeCountMismatch {
            headers: 1,
            body_sizes: 2,
        })
    ));

    assert!(matches!(
        encode_correlated(&HeaderSyncMessage::Headers {
            headers: headers.clone(),
            body_sizes: vec![100],
            tree_aux_roots: Vec::new(),
        }),
        Err(HeaderSyncWireError::TreeAuxRootCountMismatch {
            headers: 1,
            roots: 0,
        })
    ));

    let roots = [root_at(block::Height(1)), root_at(block::Height(3))];
    match validate_tree_aux_root_heights(block::Height(1), &roots) {
        Err(HeaderSyncWireError::TreeAuxRootHeightMismatch {
            offset,
            expected_height,
            root_height,
            first_root_height,
            last_root_height,
        }) => {
            assert_eq!(offset, 1);
            assert_eq!(expected_height, block::Height(2));
            assert_eq!(root_height, block::Height(3));
            assert_eq!(first_root_height, block::Height(1));
            assert_eq!(last_root_height, block::Height(3));
        }
        result => panic!("expected tree-aux root height mismatch, got {result:?}"),
    }

    let mut truncated_mid_size = encode_correlated(&message).unwrap();
    truncated_mid_size.pop();
    assert!(
        HeaderSyncMessage::decode(&truncated_mid_size, finalized_headers_context(1, 1)).is_err()
    );

    let mut truncated_mid_header = vec![MSG_HS_HEADERS];
    truncated_mid_header
        .write_u64::<LittleEndian>(test_request_id().get())
        .unwrap();
    truncated_mid_header.write_u32::<LittleEndian>(1).unwrap();
    truncated_mid_header.extend_from_slice(&[0; 8]);
    assert!(HeaderSyncMessage::decode(&truncated_mid_header, headers_context(1, 1)).is_err());

    let mut with_trailing = encode_correlated(&message).unwrap();
    with_trailing.push(0);
    assert!(matches!(
        HeaderSyncMessage::decode(&with_trailing, finalized_headers_context(1, 1)),
        Err(HeaderSyncWireError::TrailingBytes)
    ));
}

#[test]
fn decode_rejects_non_empty_headers_without_tree_aux_roots() {
    let headers = vec![mainnet_header(&BLOCK_MAINNET_1_BYTES)];
    let mut encoded = encode_correlated(&headers_message(headers)).unwrap();
    encoded
        [HEADER_SYNC_MESSAGE_TYPE_BYTES + HEADER_SYNC_REQUEST_ID_BYTES + HEADER_SYNC_COUNT_BYTES] =
        0;
    encoded.truncate(encoded.len() - HEADER_SYNC_BLOCK_COMMITMENT_ROOTS_BYTES);

    assert!(matches!(
        HeaderSyncMessage::decode(&encoded, finalized_headers_context(1, 1)),
        Err(HeaderSyncWireError::TreeAuxRootCountMismatch {
            headers: 1,
            roots: 0,
        })
    ));
}

#[test]
fn frame_decode_rejects_oversized_payload_length_before_allocating() {
    let mut bytes = Vec::new();
    bytes
        .write_u16::<LittleEndian>(u16::from(MSG_HS_STATUS))
        .unwrap();
    bytes.write_u16::<LittleEndian>(0).unwrap();
    bytes
        .write_u32::<LittleEndian>(MAX_HS_MESSAGE_BYTES as u32 + 1)
        .unwrap();

    assert!(Frame::decode(&bytes, MAX_HS_MESSAGE_BYTES as u32).is_err());
}

#[test]
fn decode_rejects_header_counts_over_contract_caps() {
    let mut encoded = vec![MSG_HS_HEADERS];
    encoded
        .write_u64::<LittleEndian>(test_request_id().get())
        .unwrap();
    encoded.write_u32::<LittleEndian>(MAX_HS_RANGE + 1).unwrap();
    encoded.write_u8(0).unwrap();
    assert!(matches!(
        HeaderSyncMessage::decode(&encoded, headers_context(MAX_HS_RANGE, MAX_HS_RANGE)),
        Err(HeaderSyncWireError::HeaderCountLimit { .. })
    ));

    let mut encoded = vec![MSG_HS_HEADERS];
    encoded
        .write_u64::<LittleEndian>(test_request_id().get())
        .unwrap();
    encoded.write_u32::<LittleEndian>(2).unwrap();
    encoded.write_u8(0).unwrap();
    assert!(matches!(
        HeaderSyncMessage::decode(&encoded, headers_context(1, MAX_HS_RANGE)),
        Err(HeaderSyncWireError::HeaderCountLimit { actual: 2, max: 1 })
    ));

    let mut encoded = vec![MSG_HS_HEADERS];
    encoded
        .write_u64::<LittleEndian>(test_request_id().get())
        .unwrap();
    encoded.write_u32::<LittleEndian>(2).unwrap();
    encoded.write_u8(0).unwrap();
    assert!(matches!(
        HeaderSyncMessage::decode(&encoded, headers_context(MAX_HS_RANGE, 1)),
        Err(HeaderSyncWireError::HeaderCountLimit { actual: 2, max: 1 })
    ));
}

#[test]
fn headers_codec_does_not_use_legacy_160_header_cap() {
    let header = mainnet_header(&BLOCK_MAINNET_1_BYTES);
    let headers = vec![header; 161];
    let message = finalized_headers_message(headers);

    let encoded = encode_correlated(&message).unwrap();
    let (decoded, _request_id) =
        HeaderSyncMessage::decode(&encoded, finalized_headers_context(161, 161)).unwrap();

    match decoded {
        HeaderSyncMessage::Headers {
            headers,
            body_sizes,
            tree_aux_roots,
        } => {
            assert_eq!(headers.len(), 161);
            assert_eq!(body_sizes, vec![0; 161]);
            assert_eq!(tree_aux_roots, roots_from_height(block::Height(1), 161));
        }
        _ => panic!("decoded message must be Headers"),
    }
}

#[test]
fn get_headers_rejects_invalid_counts() {
    assert!(encode_correlated(&HeaderSyncMessage::GetHeaders {
        start_height: block::Height(1),
        count: 0,
        want_tree_aux_roots: false,
    })
    .is_err());

    assert!(encode_correlated(&HeaderSyncMessage::GetHeaders {
        start_height: block::Height(1),
        count: MAX_HS_RANGE + 1,
        want_tree_aux_roots: false,
    })
    .is_err());
}

#[test]
fn advertised_defaults_and_clamping_match_design() {
    let config = ZakuraHeaderSyncConfig::default();
    assert_eq!(config.max_headers_per_response, DEFAULT_HS_RANGE);
    assert_eq!(config.max_inflight_requests, DEFAULT_HS_MAX_INFLIGHT);
    assert!(config.accept_new_blocks);
    assert_eq!(
        ZakuraHeaderSyncConfig {
            max_inflight_requests: u16::MAX,
            ..ZakuraHeaderSyncConfig::default()
        }
        .advertised_max_inflight_requests(),
        LOCAL_MAX_HS_INFLIGHT_PER_PEER
    );

    let status = HeaderSyncStatus {
        max_headers_per_response: MAX_HS_RANGE + 10,
        ..HeaderSyncStatus::default()
    };
    let encoded = HeaderSyncMessage::Status(status).encode(None).unwrap();
    let (decoded, _request_id) =
        HeaderSyncMessage::decode(&encoded, HeaderSyncDecodeContext::control()).unwrap();
    match decoded {
        HeaderSyncMessage::Status(status) => {
            assert_eq!(status.max_headers_per_response, MAX_HS_RANGE);
        }
        _ => panic!("decoded message must be Status"),
    }
}

#[test]
fn header_serialized_sizes_are_exact_and_message_cap_has_headroom() {
    let mainnet = mainnet_header(&BLOCK_MAINNET_GENESIS_BYTES);
    let mut mainnet_bytes = Vec::new();
    mainnet.zcash_serialize(&mut mainnet_bytes).unwrap();
    assert_eq!(mainnet_bytes.len(), COMMON_HEADER_BYTES);

    let testnet = mainnet_header(&BLOCK_TESTNET_GENESIS_BYTES);
    let mut testnet_bytes = Vec::new();
    testnet.zcash_serialize(&mut testnet_bytes).unwrap();
    assert_eq!(testnet_bytes.len(), COMMON_HEADER_BYTES);

    let mut regtest = *mainnet;
    regtest.solution = Solution::Regtest([0; 36]);
    let mut regtest_bytes = Vec::new();
    regtest.zcash_serialize(&mut regtest_bytes).unwrap();
    assert_eq!(regtest_bytes.len(), REGTEST_HEADER_BYTES);

    let default_response_bytes = HEADER_SYNC_MESSAGE_TYPE_BYTES
        + HEADER_SYNC_REQUEST_ID_BYTES
        + HEADER_SYNC_COUNT_BYTES
        + (COMMON_HEADER_BYTES
            + HEADER_SYNC_BODY_SIZE_BYTES
            + HEADER_SYNC_BLOCK_COMMITMENT_ROOTS_BYTES)
            * DEFAULT_HS_RANGE as usize;
    assert!(default_response_bytes < MAX_HS_MESSAGE_BYTES);
    assert!(MAX_HS_MESSAGE_BYTES < LOCAL_MAX_MESSAGE_BYTES as usize);
}

#[test]
fn request_and_serving_counts_are_clamped_by_byte_budget() {
    let count = clamp_header_sync_request_count(
        MAX_HS_RANGE,
        MAX_HS_RANGE,
        &Network::Mainnet,
        LOCAL_MAX_MESSAGE_BYTES,
        false,
    );

    assert!(count < MAX_HS_RANGE);
    let count_with_roots = clamp_header_sync_request_count(
        MAX_HS_RANGE,
        MAX_HS_RANGE,
        &Network::Mainnet,
        LOCAL_MAX_MESSAGE_BYTES,
        true,
    );
    assert!(count_with_roots < count);

    let config = ZakuraHeaderSyncConfig {
        max_headers_per_response: MAX_HS_RANGE,
        ..ZakuraHeaderSyncConfig::default()
    };
    assert_eq!(
        inbound_get_headers_count_limit(&config, &Network::Mainnet, LOCAL_MAX_MESSAGE_BYTES, false),
        count
    );
    assert_eq!(
        inbound_get_headers_count_limit(&config, &Network::Mainnet, LOCAL_MAX_MESSAGE_BYTES, true),
        count_with_roots
    );
}

#[tokio::test(flavor = "current_thread")]
async fn reactor_starts_from_storage_frontiers_and_publishes_watch() {
    let network = regtest_network();
    let best = (block::Height(7), block::Hash([7; 32]));
    let startup = HeaderSyncStartup::new(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        HeaderSyncFrontiers {
            finalized_height: block::Height(2),
            verified_block_tip: block::Height(5),
            verified_block_hash: block::Hash([5; 32]),
        },
        Some(best),
        ZakuraHeaderSyncConfig::default(),
        LOCAL_MAX_MESSAGE_BYTES,
    );
    let fixture = spawn_test_reactor(startup);

    assert_eq!(fixture.handle.best_header_tip(), best);
    assert_eq!(*fixture.handle.subscribe_tip().borrow(), best);
}

#[tokio::test(flavor = "current_thread")]
async fn restart_rebuilds_schedule_from_durable_best_tip_and_peer_status() {
    let network = regtest_network();
    let best = (block::Height(4), block::Hash([4; 32]));
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        Some(best),
    ));
    let peer_id = peer(41);

    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(
        &fixture,
        peer_id,
        block::Height(0),
        block::Height(8),
        DEFAULT_HS_RANGE,
        1,
    )
    .await;

    loop {
        if let HeaderSyncAction::SendMessage {
            msg:
                HeaderSyncMessage::GetHeaders {
                    start_height,
                    count,
                    want_tree_aux_roots: true,
                },
            ..
        } = next_non_query_action(&mut fixture.actions).await
        {
            assert_eq!(start_height, block::Height(5));
            assert_eq!(count, 4);
            break;
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn restart_prefetch_keeps_forward_tip_extension_active() {
    let network = Network::Mainnet;
    let anchor = (block::Height(0), network.genesis_hash());
    let best = (block::Height(4), block::Hash([4; 32]));
    let mut startup = startup_for(network, anchor, Some(best));
    startup.config.max_headers_per_response = 2;
    startup.header_root_auth = Some(HeaderRootAuthState {
        authenticated_height: anchor.0,
        authenticated_hash: anchor.1,
        completed_checkpoint_height: best.0,
        completed_checkpoint_hash: best.1,
    });
    let mut fixture = spawn_test_reactor(startup);
    let peer_id = peer(42);

    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(
        &fixture,
        peer_id,
        block::Height(0),
        block::Height(500),
        2,
        4,
    )
    .await;

    let mut starts = Vec::new();
    for _ in 0..4 {
        let (_, _, start, count) = next_outbound_get_headers(&mut fixture.actions).await;
        starts.push((start, count));
    }
    assert_eq!(
        starts,
        vec![
            (block::Height(1), 2),
            (block::Height(2), 2),
            (block::Height(3), 2),
            (block::Height(5), 2),
        ]
    );
}

fn mainnet_repair_event(generation: u64) -> HeaderSyncEvent {
    let block1 = mainnet_block(&BLOCK_MAINNET_1_BYTES);
    let block2 = mainnet_block(&BLOCK_MAINNET_2_BYTES);
    HeaderSyncEvent::VctRootRepairRequested {
        height: block::Height(1),
        generation,
        anchor_hash: Network::Mainnet.genesis_hash(),
        expected_hashes: vec![
            (block::Height(1), block1.hash()),
            (block::Height(2), block2.hash()),
        ],
    }
}

fn mainnet_repair_event_at_two(generation: u64) -> HeaderSyncEvent {
    let block1 = mainnet_block(&BLOCK_MAINNET_1_BYTES);
    let block2 = mainnet_block(&BLOCK_MAINNET_2_BYTES);
    let block3 = mainnet_block(&BLOCK_MAINNET_3_BYTES);
    HeaderSyncEvent::VctRootRepairRequested {
        height: block::Height(2),
        generation,
        anchor_hash: block1.hash(),
        expected_hashes: vec![
            (block::Height(2), block2.hash()),
            (block::Height(3), block3.hash()),
        ],
    }
}

#[test]
fn vct_repair_episode_enforces_attempt_and_time_bounds() {
    let mut repair = VctRootRepair::new(
        block::Height(1),
        1,
        Network::Mainnet.genesis_hash(),
        vec![
            (
                block::Height(1),
                mainnet_block(&BLOCK_MAINNET_1_BYTES).hash(),
            ),
            (
                block::Height(2),
                mainnet_block(&BLOCK_MAINNET_2_BYTES).hash(),
            ),
        ],
    )
    .expect("valid repair shape");

    for (attempt, backoff) in VCT_ROOT_REPAIR_BACKOFFS.iter().copied().enumerate() {
        assert!(repair.can_attempt(repair.next_attempt_at));
        let peer_id = peer(120 + u8::try_from(attempt).expect("attempt fits in u8"));
        repair.mark_attempt(peer_id.clone());
        let finished_at = repair.next_attempt_at;
        assert!(repair.finish_attempt(&peer_id, repair.generation, finished_at));
        assert_eq!(
            repair.next_attempt_at,
            finished_at + backoff,
            "each failure uses the backoff with the same zero-based attempt index"
        );
    }

    assert!(repair.exhausted);
    assert!(!repair.can_attempt(repair.next_attempt_at));

    let mut timed = VctRootRepair::new(
        block::Height(1),
        2,
        Network::Mainnet.genesis_hash(),
        vec![(
            block::Height(1),
            mainnet_block(&BLOCK_MAINNET_1_BYTES).hash(),
        )],
    )
    .expect("single-header handoff repair is valid");
    let now = Instant::now();
    timed.started_at = now - VCT_ROOT_REPAIR_MAX_WALL_TIME;
    assert!(timed.refresh_exhausted(now));
    assert!(timed.exhausted);
    assert!(!timed.refresh_exhausted(now));
    assert!(!timed.can_attempt(now));
}

#[test]
fn vct_repair_maintenance_ignores_retry_deadline_during_attempt() {
    let mut repair = VctRootRepair::new(
        block::Height(1),
        1,
        Network::Mainnet.genesis_hash(),
        vec![(
            block::Height(1),
            mainnet_block(&BLOCK_MAINNET_1_BYTES).hash(),
        )],
    )
    .expect("single-header handoff repair is valid");
    let retry_deadline = repair.next_attempt_at;
    let repair_deadline = repair.started_at + VCT_ROOT_REPAIR_MAX_WALL_TIME;

    assert_eq!(repair.next_maintenance_deadline(), retry_deadline);

    repair.mark_attempt(peer(129));

    assert_eq!(repair.next_maintenance_deadline(), repair_deadline);
}

#[test]
fn vct_repair_ignores_unrelated_peer_and_stale_generation_completions() {
    let mut repair = VctRootRepair::new(
        block::Height(1),
        1,
        Network::Mainnet.genesis_hash(),
        vec![(
            block::Height(1),
            mainnet_block(&BLOCK_MAINNET_1_BYTES).hash(),
        )],
    )
    .expect("single-header handoff repair is valid");
    let repair_peer = peer(130);
    repair.mark_attempt(repair_peer.clone());
    let next_attempt_at = repair.next_attempt_at;

    assert!(!repair.finish_attempt(
        &peer(131),
        repair.generation,
        repair.started_at + VCT_ROOT_REPAIR_MAX_WALL_TIME
    ));
    assert!(!repair.finish_attempt(
        &repair_peer,
        repair.generation.saturating_add(1),
        repair.started_at + VCT_ROOT_REPAIR_MAX_WALL_TIME
    ));
    assert_eq!(repair.in_flight.as_ref(), Some(&repair_peer));
    assert_eq!(repair.next_attempt_at, next_attempt_at);
    assert!(!repair.exhausted);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn vct_repair_wall_time_exhaustion_emits_operator_signal_without_an_attempt() {
    let metrics = metric_snapshot(&["sync.header.vct_repair.exhausted"]);
    let best = (
        block::Height(4),
        mainnet_block(&BLOCK_MAINNET_4_BYTES).hash(),
    );
    let fixture = spawn_test_reactor(startup_for(
        Network::Mainnet,
        (block::Height(0), Network::Mainnet.genesis_hash()),
        Some(best),
    ));

    fixture.handle.send(mainnet_repair_event(1)).await.unwrap();
    tokio::task::yield_now().await;
    tokio::time::advance(VCT_ROOT_REPAIR_MAX_WALL_TIME).await;

    // Connecting a peer without a Status runs the scheduler but cannot start an
    // attempt, exercising expiry on the otherwise quiet path.
    connect_peer(&fixture, peer(132)).await;
    tokio::task::yield_now().await;
    tokio::task::yield_now().await;

    assert_metric_incremented(&metrics, "sync.header.vct_repair.exhausted");
    assert_eq!(gauge_value("sync.header.vct_repair.stalled.height"), 1.0);
}

#[tokio::test(flavor = "current_thread")]
async fn vct_repair_bypasses_covered_range_and_commits_exact_h_and_successor() {
    let best = (
        block::Height(4),
        mainnet_block(&BLOCK_MAINNET_4_BYTES).hash(),
    );
    let mut fixture = spawn_test_reactor(startup_for(
        Network::Mainnet,
        (block::Height(0), Network::Mainnet.genesis_hash()),
        Some(best),
    ));
    let peer_id = peer(101);

    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(
        &fixture,
        peer_id.clone(),
        block::Height(0),
        block::Height(4),
        2,
        1,
    )
    .await;
    for height in 1..=4 {
        fixture
            .handle
            .send(HeaderSyncEvent::FullBlockCommitted {
                height: block::Height(height),
                hash: block::Hash([u8::try_from(height).expect("test height fits in u8"); 32]),
            })
            .await
            .unwrap();
    }
    fixture.handle.send(mainnet_repair_event(1)).await.unwrap();

    let (requested_peer, request_id, start_height, count) =
        next_outbound_get_headers(&mut fixture.actions).await;
    assert_eq!(requested_peer, peer_id);
    assert_eq!(start_height, block::Height(1));
    assert_eq!(count, 2);

    send_headers(
        &fixture,
        &peer_id,
        request_id,
        finalized_headers_message_from(
            block::Height(1),
            vec![
                mainnet_header(&BLOCK_MAINNET_1_BYTES),
                mainnet_header(&BLOCK_MAINNET_2_BYTES),
            ],
        ),
    )
    .await;

    loop {
        match next_action(&mut fixture.actions).await {
            HeaderSyncAction::CommitHeaderRange {
                operation,
                payload,
                finalized,
                ..
            } => {
                assert_eq!(operation.wire_request.peer, peer_id);
                assert_eq!(payload.range().start(), block::Height(1));
                assert_eq!(payload.headers().len(), 2);
                assert_eq!(payload.tree_aux_roots().map(|roots| roots.len()), Some(2));
                assert!(
                    !finalized,
                    "repair ranges are canonical but not checkpoint-terminating"
                );
                break;
            }
            HeaderSyncAction::Misbehavior { peer, reason } => {
                panic!("unexpected repair misbehavior from {peer:?}: {reason:?}");
            }
            _ => {}
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn vct_repair_scheduler_skips_peers_with_insufficient_response_capacity() {
    let best = (
        block::Height(4),
        mainnet_block(&BLOCK_MAINNET_4_BYTES).hash(),
    );
    let mut fixture = spawn_test_reactor(startup_for(
        Network::Mainnet,
        (block::Height(0), Network::Mainnet.genesis_hash()),
        Some(best),
    ));
    let low_capacity_peer = peer(101);
    let capable_peer = peer(102);

    for (peer_id, capacity) in [(low_capacity_peer, 1), (capable_peer.clone(), 2)] {
        connect_peer(&fixture, peer_id.clone()).await;
        advertise_tip(&fixture, peer_id, block::Height(0), best.0, capacity, 1).await;
    }

    fixture.handle.send(mainnet_repair_event(1)).await.unwrap();

    let (requested_peer, _request_id, start_height, count) =
        next_outbound_get_headers(&mut fixture.actions).await;
    assert_eq!(requested_peer, capable_peer);
    assert_eq!((start_height, count), (block::Height(1), 2));
}

#[tokio::test(flavor = "current_thread")]
async fn vct_repair_does_not_starve_root_auth_on_other_peer() {
    let best = (
        block::Height(4),
        mainnet_block(&BLOCK_MAINNET_4_BYTES).hash(),
    );
    let mut startup = startup_for(
        Network::Mainnet,
        (block::Height(0), Network::Mainnet.genesis_hash()),
        Some(best),
    );
    startup.config.max_headers_per_response = 2;
    startup.header_root_auth = Some(HeaderRootAuthState {
        authenticated_height: block::Height(0),
        authenticated_hash: Network::Mainnet.genesis_hash(),
        completed_checkpoint_height: best.0,
        completed_checkpoint_hash: best.1,
    });
    let mut fixture = spawn_test_reactor(startup);
    let repair_peer = peer(107);
    let auth_peer = peer(108);

    fixture.handle.send(mainnet_repair_event(1)).await.unwrap();
    connect_peer(&fixture, repair_peer.clone()).await;
    advertise_tip(
        &fixture,
        repair_peer.clone(),
        block::Height(0),
        best.0,
        2,
        1,
    )
    .await;
    let (assigned_repair_peer, _, _, _) = next_outbound_get_headers(&mut fixture.actions).await;
    assert_eq!(assigned_repair_peer, repair_peer);

    connect_peer(&fixture, auth_peer.clone()).await;
    advertise_tip(&fixture, auth_peer.clone(), block::Height(0), best.0, 2, 1).await;
    let (assigned_auth_peer, _, start_height, count) =
        next_outbound_get_headers(&mut fixture.actions).await;
    assert_eq!(assigned_auth_peer, auth_peer);
    assert_eq!((start_height, count), (block::Height(1), 2));
}

#[tokio::test(flavor = "current_thread")]
async fn vct_repair_timeout_retries_another_peer() {
    let metrics = metric_snapshot(&["sync.header.vct_repair.timeout"]);
    let best = (
        block::Height(4),
        mainnet_block(&BLOCK_MAINNET_4_BYTES).hash(),
    );
    let mut startup = startup_for(
        Network::Mainnet,
        (block::Height(0), Network::Mainnet.genesis_hash()),
        Some(best),
    );
    startup.request_timeout = std::time::Duration::from_millis(10);
    let mut fixture = spawn_test_reactor(startup);
    let first_peer = peer(109);
    let second_peer = peer(110);

    for peer_id in [&first_peer, &second_peer] {
        connect_peer(&fixture, peer_id.clone()).await;
        advertise_tip(&fixture, peer_id.clone(), block::Height(0), best.0, 2, 1).await;
    }
    fixture.handle.send(mainnet_repair_event(1)).await.unwrap();

    assert_eq!(
        next_outbound_get_headers(&mut fixture.actions).await.0,
        first_peer
    );
    assert_eq!(
        next_outbound_get_headers(&mut fixture.actions).await.0,
        second_peer
    );
    assert_metric_incremented(&metrics, "sync.header.vct_repair.timeout");
}

#[tokio::test(flavor = "current_thread")]
async fn vct_repair_disconnect_retries_another_peer() {
    let best = (
        block::Height(4),
        mainnet_block(&BLOCK_MAINNET_4_BYTES).hash(),
    );
    let mut startup = startup_for(
        Network::Mainnet,
        (block::Height(0), Network::Mainnet.genesis_hash()),
        Some(best),
    );
    startup.request_timeout = std::time::Duration::from_millis(10);
    let mut fixture = spawn_test_reactor(startup);
    let first_peer = peer(111);
    let second_peer = peer(112);

    for peer_id in [&first_peer, &second_peer] {
        connect_peer(&fixture, peer_id.clone()).await;
        advertise_tip(&fixture, peer_id.clone(), block::Height(0), best.0, 2, 1).await;
    }
    fixture.handle.send(mainnet_repair_event(1)).await.unwrap();
    assert_eq!(
        next_outbound_get_headers(&mut fixture.actions).await.0,
        first_peer
    );

    fixture
        .handle
        .send(HeaderSyncEvent::PeerDisconnected(first_peer))
        .await
        .unwrap();

    assert_eq!(
        next_outbound_get_headers(&mut fixture.actions).await.0,
        second_peer
    );
}

#[tokio::test(flavor = "current_thread")]
async fn vct_repair_commit_failure_retries_another_peer() {
    let best = (
        block::Height(4),
        mainnet_block(&BLOCK_MAINNET_4_BYTES).hash(),
    );
    let mut fixture = spawn_test_reactor(startup_for(
        Network::Mainnet,
        (block::Height(0), Network::Mainnet.genesis_hash()),
        Some(best),
    ));
    let first_peer = peer(113);
    let second_peer = peer(114);

    for peer_id in [&first_peer, &second_peer] {
        connect_peer(&fixture, peer_id.clone()).await;
        advertise_tip(&fixture, peer_id.clone(), block::Height(0), best.0, 2, 1).await;
    }
    fixture.handle.send(mainnet_repair_event(1)).await.unwrap();
    let (requested_peer, request_id, _, _) = next_outbound_get_headers(&mut fixture.actions).await;
    assert_eq!(requested_peer, first_peer);
    send_headers(
        &fixture,
        &first_peer,
        request_id,
        finalized_headers_message_from(
            block::Height(1),
            vec![
                mainnet_header(&BLOCK_MAINNET_1_BYTES),
                mainnet_header(&BLOCK_MAINNET_2_BYTES),
            ],
        ),
    )
    .await;

    let operation = loop {
        if let HeaderSyncAction::CommitHeaderRange { operation, .. } =
            next_action(&mut fixture.actions).await
        {
            break operation;
        }
    };
    fixture
        .handle
        .send(HeaderSyncEvent::HeaderRangeOperationFailed {
            operation: operation.clone(),
            kind: HeaderSyncCommitFailureKind::Local,
        })
        .await
        .unwrap();

    assert_eq!(
        next_outbound_get_headers(&mut fixture.actions).await.0,
        second_peer
    );
}

#[tokio::test(flavor = "current_thread")]
async fn vct_repair_scheduler_skips_advisory_backoff_across_episode_heights() {
    let best = (
        block::Height(4),
        mainnet_block(&BLOCK_MAINNET_4_BYTES).hash(),
    );
    let mut fixture = spawn_test_reactor(startup_for(
        Network::Mainnet,
        (block::Height(0), Network::Mainnet.genesis_hash()),
        Some(best),
    ));
    let backed_off_peer = peer(115);
    let eligible_peer = peer(116);

    fixture
        .handle
        .send(HeaderSyncEvent::AdvisoryHeaderSummary {
            peer: backed_off_peer.clone(),
            summary: advisory_header_summary(block::Height(10), 1),
        })
        .await
        .unwrap();
    connect_peer(&fixture, backed_off_peer.clone()).await;
    advertise_tip(
        &fixture,
        backed_off_peer.clone(),
        block::Height(0),
        best.0,
        2,
        1,
    )
    .await;
    fixture.handle.send(mainnet_repair_event(1)).await.unwrap();
    let (requested_peer, request_id, _, _) = next_outbound_get_headers(&mut fixture.actions).await;
    assert_eq!(requested_peer, backed_off_peer);
    send_headers(
        &fixture,
        &backed_off_peer,
        request_id,
        finalized_headers_message_from(block::Height(1), Vec::new()),
    )
    .await;

    connect_peer(&fixture, eligible_peer.clone()).await;
    fixture
        .handle
        .send(mainnet_repair_event_at_two(2))
        .await
        .unwrap();
    advertise_tip(
        &fixture,
        eligible_peer.clone(),
        block::Height(0),
        best.0,
        2,
        1,
    )
    .await;

    let (requested_peer, _request_id, start_height, count) =
        next_outbound_get_headers(&mut fixture.actions).await;
    assert_eq!(requested_peer, eligible_peer);
    assert_eq!((start_height, count), (block::Height(2), 2));
}

#[tokio::test(flavor = "current_thread")]
async fn vct_repair_rejects_noncanonical_response_before_commit() {
    let best = (
        block::Height(4),
        mainnet_block(&BLOCK_MAINNET_4_BYTES).hash(),
    );
    let mut fixture = spawn_test_reactor(startup_for(
        Network::Mainnet,
        (block::Height(0), Network::Mainnet.genesis_hash()),
        Some(best),
    ));
    let peer_id = peer(102);
    let mut event = mainnet_repair_event(1);
    if let HeaderSyncEvent::VctRootRepairRequested {
        expected_hashes, ..
    } = &mut event
    {
        expected_hashes[1].1 = block::Hash([99; 32]);
    }

    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(
        &fixture,
        peer_id.clone(),
        block::Height(0),
        block::Height(4),
        2,
        1,
    )
    .await;
    fixture.handle.send(event).await.unwrap();
    let (_requested_peer, request_id, start_height, count) =
        next_outbound_get_headers(&mut fixture.actions).await;
    assert_eq!((start_height, count), (block::Height(1), 2));

    send_headers(
        &fixture,
        &peer_id,
        request_id,
        finalized_headers_message_from(
            block::Height(1),
            vec![
                mainnet_header(&BLOCK_MAINNET_1_BYTES),
                mainnet_header(&BLOCK_MAINNET_2_BYTES),
            ],
        ),
    )
    .await;

    loop {
        match next_action(&mut fixture.actions).await {
            HeaderSyncAction::Misbehavior { peer, reason } => {
                assert_eq!(peer, peer_id);
                assert_eq!(reason, HeaderSyncMisbehavior::InvalidRange);
                break;
            }
            HeaderSyncAction::CommitHeaderRange { .. } => {
                panic!("noncanonical repair response must not be committed")
            }
            _ => {}
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn stale_vct_repair_response_is_dropped_without_peer_misbehavior() {
    let best = (
        block::Height(4),
        mainnet_block(&BLOCK_MAINNET_4_BYTES).hash(),
    );
    let mut fixture = spawn_test_reactor(startup_for(
        Network::Mainnet,
        (block::Height(0), Network::Mainnet.genesis_hash()),
        Some(best),
    ));
    let stale_peer = peer(103);
    let current_peer = peer(104);

    for peer_id in [&stale_peer, &current_peer] {
        connect_peer(&fixture, peer_id.clone()).await;
        advertise_tip(
            &fixture,
            peer_id.clone(),
            block::Height(0),
            block::Height(4),
            2,
            1,
        )
        .await;
    }

    fixture.handle.send(mainnet_repair_event(1)).await.unwrap();
    let (requested_peer, stale_request_id, start_height, count) =
        next_outbound_get_headers(&mut fixture.actions).await;
    assert_eq!(requested_peer, stale_peer);
    assert_eq!((start_height, count), (block::Height(1), 2));

    fixture.handle.send(mainnet_repair_event(2)).await.unwrap();
    let (requested_peer, current_request_id, start_height, count) =
        next_outbound_get_headers(&mut fixture.actions).await;
    assert_eq!(requested_peer, current_peer);
    assert_eq!((start_height, count), (block::Height(1), 2));

    for (peer_id, request_id) in [
        (stale_peer, stale_request_id),
        (current_peer.clone(), current_request_id),
    ] {
        send_headers(
            &fixture,
            &peer_id,
            request_id,
            finalized_headers_message_from(
                block::Height(1),
                vec![
                    mainnet_header(&BLOCK_MAINNET_1_BYTES),
                    mainnet_header(&BLOCK_MAINNET_2_BYTES),
                ],
            ),
        )
        .await;
    }

    loop {
        match next_action(&mut fixture.actions).await {
            HeaderSyncAction::CommitHeaderRange { operation, .. } => {
                assert_eq!(operation.wire_request.peer, current_peer);
                break;
            }
            HeaderSyncAction::Misbehavior { peer, reason } => {
                panic!("stale repair response reported {peer:?} for {reason:?}")
            }
            _ => {}
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn vct_repair_generation_change_keeps_tried_peers_at_same_height() {
    let best = (
        block::Height(4),
        mainnet_block(&BLOCK_MAINNET_4_BYTES).hash(),
    );
    let mut fixture = spawn_test_reactor(startup_for(
        Network::Mainnet,
        (block::Height(0), Network::Mainnet.genesis_hash()),
        Some(best),
    ));
    let first_peer = peer(103);
    let second_peer = peer(104);

    for peer_id in [&first_peer, &second_peer] {
        connect_peer(&fixture, peer_id.clone()).await;
        advertise_tip(&fixture, peer_id.clone(), block::Height(0), best.0, 2, 1).await;
    }

    fixture.handle.send(mainnet_repair_event(1)).await.unwrap();
    let (requested_peer, request_id, _, _) = next_outbound_get_headers(&mut fixture.actions).await;
    assert_eq!(requested_peer, first_peer);

    send_headers(
        &fixture,
        &first_peer,
        request_id,
        finalized_headers_message_from(
            block::Height(1),
            vec![
                mainnet_header(&BLOCK_MAINNET_1_BYTES),
                mainnet_header(&BLOCK_MAINNET_2_BYTES),
            ],
        ),
    )
    .await;

    let operation = loop {
        if let HeaderSyncAction::CommitHeaderRange { operation, .. } =
            next_action(&mut fixture.actions).await
        {
            break operation;
        }
    };
    fixture
        .handle
        .send(HeaderSyncEvent::HeaderRangeOperationCompleted {
            operation,
            tip_hash: mainnet_block(&BLOCK_MAINNET_2_BYTES).hash(),
        })
        .await
        .unwrap();

    fixture.handle.send(mainnet_repair_event(2)).await.unwrap();
    let (requested_peer, _request_id, start_height, count) =
        next_outbound_get_headers(&mut fixture.actions).await;
    assert_eq!(requested_peer, second_peer);
    assert_eq!((start_height, count), (block::Height(1), 2));
}

#[tokio::test(flavor = "current_thread")]
async fn vct_repair_scheduler_requires_an_idle_peer() {
    let network = regtest_network();
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let busy_peer = peer(105);
    let idle_peer = peer(106);

    connect_peer(&fixture, busy_peer.clone()).await;
    connect_peer(&fixture, idle_peer.clone()).await;
    advertise_tip(
        &fixture,
        busy_peer.clone(),
        block::Height(0),
        block::Height(10),
        2,
        2,
    )
    .await;
    let (requested_peer, _request_id, _, _) = next_outbound_get_headers(&mut fixture.actions).await;
    assert_eq!(requested_peer, busy_peer);
    while tokio::time::timeout(std::time::Duration::from_millis(25), fixture.actions.recv())
        .await
        .is_ok()
    {}

    fixture.handle.send(mainnet_repair_event(1)).await.unwrap();
    advertise_tip(
        &fixture,
        idle_peer.clone(),
        block::Height(0),
        block::Height(10),
        2,
        2,
    )
    .await;

    let (requested_peer, _request_id, start_height, count) =
        next_outbound_get_headers(&mut fixture.actions).await;
    assert_eq!(requested_peer, idle_peer);
    assert_eq!((start_height, count), (block::Height(1), 2));
}

#[tokio::test(flavor = "current_thread")]
async fn handle_sends_events_and_peer_connect_sends_status_first() {
    let network = regtest_network();
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let peer_id = peer(1);

    connect_peer(&fixture, peer_id.clone()).await;

    match next_non_query_action(&mut fixture.actions).await {
        HeaderSyncAction::SendMessage { peer, msg, .. } => {
            assert_eq!(peer, peer_id);
            assert!(matches!(msg, HeaderSyncMessage::Status(_)));
        }
        action => panic!("unexpected action: {action:?}"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn status_updates_peer_caps_and_scheduler_respects_them() {
    let network = regtest_network();
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let peer_id = peer(2);

    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(
        &fixture,
        peer_id.clone(),
        block::Height(0),
        block::Height(10),
        2,
        u16::MAX,
    )
    .await;

    let mut saw_get_headers = false;
    for _ in 0..4 {
        if let HeaderSyncAction::SendMessage {
            peer,
            msg:
                HeaderSyncMessage::GetHeaders {
                    start_height,
                    count,
                    want_tree_aux_roots: true,
                },
            ..
        } = next_non_query_action(&mut fixture.actions).await
        {
            assert_eq!(peer, peer_id);
            assert_eq!(start_height, block::Height(1));
            assert_eq!(count, 2);
            saw_get_headers = true;
            break;
        }
    }
    assert!(saw_get_headers);
}

#[tokio::test(flavor = "current_thread")]
async fn scheduler_fills_v7_outstanding_request_slots() {
    let network = regtest_network();
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let peer_id = peer(31);

    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(
        &fixture,
        peer_id.clone(),
        block::Height(0),
        block::Height(20),
        2,
        u16::MAX,
    )
    .await;

    let mut starts = HashSet::new();
    while let Ok(Some(action)) = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        fixture.actions.recv(),
    )
    .await
    {
        if let HeaderSyncAction::SendMessage {
            peer,
            msg:
                HeaderSyncMessage::GetHeaders {
                    start_height,
                    count,
                    ..
                },
            ..
        } = action
        {
            if peer == peer_id {
                assert_eq!(count, 2);
                assert!(starts.insert(start_height));
            }
        }
    }

    assert_eq!(starts.len(), 10);
    assert_eq!(starts.iter().copied().min(), Some(block::Height(1)));
    assert_eq!(starts.iter().copied().max(), Some(block::Height(19)));
}

#[tokio::test(flavor = "current_thread")]
async fn work_queue_assigns_each_forward_range_to_one_peer() {
    let network = regtest_network();
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let peers = [peer(3), peer(4), peer(5)];

    for peer_id in peers.clone() {
        connect_peer(&fixture, peer_id.clone()).await;
        advertise_tip(&fixture, peer_id, block::Height(0), block::Height(5), 5, 1).await;
    }

    let mut requested = HashSet::new();
    while requested.is_empty() {
        if let HeaderSyncAction::SendMessage {
            peer,
            msg:
                HeaderSyncMessage::GetHeaders {
                    start_height,
                    count,
                    want_tree_aux_roots: true,
                },
            ..
        } = next_non_query_action(&mut fixture.actions).await
        {
            assert_eq!(start_height, block::Height(1));
            assert_eq!(count, 5);
            requested.insert(peer);
        }
    }

    assert_eq!(requested.len(), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn covered_outstanding_range_does_not_commit_late_response() {
    let network = regtest_network();
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let peer_id = peer(33);
    let start = block::Height(1);
    let tip = block::Height(2);

    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(&fixture, peer_id.clone(), block::Height(0), tip, 2, 1).await;

    let (requested_peer, request_id, start_height, count) =
        next_outbound_get_headers(&mut fixture.actions).await;
    assert_eq!(requested_peer, peer_id);
    assert_eq!(start_height, start);
    assert_eq!(count, 2);

    for height in start.0..=tip.0 {
        fixture
            .handle
            .send(HeaderSyncEvent::FullBlockCommitted {
                height: block::Height(height),
                hash: block::Hash([u8::try_from(height).expect("test height fits in u8"); 32]),
            })
            .await
            .unwrap();
    }

    send_headers(
        &fixture,
        &peer_id,
        request_id,
        headers_message_from(
            start,
            vec![
                mainnet_header(&BLOCK_MAINNET_1_BYTES),
                mainnet_header(&BLOCK_MAINNET_2_BYTES),
            ],
        ),
    )
    .await;

    assert_no_commit_or_misbehavior(&mut fixture.actions).await;
}

#[tokio::test(flavor = "current_thread")]
async fn work_queue_splits_large_ranges_without_duplicate_ownership() {
    let network = Network::Mainnet;
    let first_checkpoint = network
        .checkpoint_list()
        .min_height_in_range(block::Height(1)..)
        .expect("mainnet has a checkpoint above genesis");
    let best_header_hash = block::Hash([3; 32]);
    let start = next_height(first_checkpoint).expect("checkpoint height has successor");
    let unclamped_tip = block::Height(
        start
            .0
            .checked_add(MAX_HS_RANGE)
            .expect("test range fits in height"),
    );
    let clamped_count = clamp_header_sync_request_count(
        MAX_HS_RANGE,
        MAX_HS_RANGE,
        &network,
        LOCAL_MAX_MESSAGE_BYTES,
        true,
    );
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        Some((first_checkpoint, best_header_hash)),
    ));
    let peers = [peer(37), peer(38), peer(39), peer(40)];

    for peer_id in peers.clone() {
        connect_peer(&fixture, peer_id.clone()).await;
        advertise_tip(
            &fixture,
            peer_id,
            block::Height(0),
            unclamped_tip,
            MAX_HS_RANGE,
            1,
        )
        .await;
    }

    let mut requested = HashMap::new();
    while let Ok(Some(action)) = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        fixture.actions.recv(),
    )
    .await
    {
        if let HeaderSyncAction::SendMessage {
            peer,
            msg:
                HeaderSyncMessage::GetHeaders {
                    start_height,
                    count,
                    want_tree_aux_roots: true,
                },
            ..
        } = action
        {
            assert!(count <= clamped_count);
            assert!(count > 0);
            assert!(
                requested.insert(start_height, peer).is_none(),
                "work queue must not assign one clamped chunk to multiple peers"
            );
        }
    }
    assert_eq!(requested.len(), peers.len());
    assert_eq!(requested.keys().copied().min(), Some(start));
}

#[tokio::test(flavor = "current_thread")]
async fn scheduler_starts_forward_work_above_checkpoint_anchor() {
    let (network, checkpoint_hash) = checkpoint_regtest(block::Height(3));
    let mut fixture = spawn_test_reactor(startup_for(
        network,
        (block::Height(3), checkpoint_hash),
        Some((block::Height(3), checkpoint_hash)),
    ));
    let peer_id = peer(6);

    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(
        &fixture,
        peer_id,
        block::Height(0),
        block::Height(8),
        DEFAULT_HS_RANGE,
        10,
    )
    .await;

    loop {
        if let HeaderSyncAction::SendMessage {
            msg:
                HeaderSyncMessage::GetHeaders {
                    start_height,
                    count,
                    want_tree_aux_roots: true,
                },
            ..
        } = next_non_query_action(&mut fixture.actions).await
        {
            assert_eq!(start_height, block::Height(4));
            assert_eq!(count, 5);
            break;
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn scheduler_does_not_backfill_below_checkpoint_anchor() {
    let (network, checkpoint_hash) = checkpoint_regtest(block::Height(3));
    let mut fixture = spawn_test_reactor(startup_for(
        network,
        (block::Height(3), checkpoint_hash),
        Some((block::Height(3), checkpoint_hash)),
    ));
    let peer_id = peer(7);

    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(
        &fixture,
        peer_id,
        block::Height(0),
        block::Height(3),
        DEFAULT_HS_RANGE,
        10,
    )
    .await;

    let unexpected_request = tokio::time::timeout(std::time::Duration::from_millis(50), async {
        loop {
            if let HeaderSyncAction::SendMessage {
                msg: HeaderSyncMessage::GetHeaders { start_height, .. },
                ..
            } = next_non_query_action(&mut fixture.actions).await
            {
                break start_height;
            }
        }
    })
    .await;
    assert!(
        unexpected_request.is_err(),
        "forward-only header sync must not request ranges below its startup base"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn forward_ranges_below_checkpoint_handoff_request_tree_aux_roots() {
    let network = Parameters::build()
        .with_network_name("HeadersyncRootWindowTest")
        .expect("custom network name is valid")
        .with_genesis_hash(Network::Mainnet.genesis_hash())
        .expect("mainnet genesis hash is valid")
        .with_activation_heights(ConfiguredActivationHeights {
            overwinter: Some(1),
            sapling: Some(2),
            blossom: Some(3),
            heartwood: Some(4),
            canopy: Some(4),
            ..Default::default()
        })
        .expect("custom activation heights are in order")
        .clear_funding_streams()
        .with_checkpoints(ConfiguredCheckpoints::HeightsAndHashes(vec![
            (block::Height(0), Network::Mainnet.genesis_hash()),
            (block::Height(400), block::Hash([4; 32])),
            (block::Height(1_200), block::Hash([12; 32])),
        ]))
        .expect("custom checkpoints are valid")
        .to_network()
        .expect("custom testnet parameters are valid");
    let first_checkpoint = block::Height(400);
    let first_checkpoint_hash = block::Hash([4; 32]);
    let mut capture =
        TraceCapture::for_test("forward_ranges_below_checkpoint_handoff_request_tree_aux_roots")
            .unwrap();
    let mut startup = startup_for(
        network,
        (block::Height(0), Network::Mainnet.genesis_hash()),
        Some((first_checkpoint, first_checkpoint_hash)),
    );
    startup.header_root_auth = Some(HeaderRootAuthState {
        authenticated_height: first_checkpoint,
        authenticated_hash: first_checkpoint_hash,
        completed_checkpoint_height: first_checkpoint,
        completed_checkpoint_hash: first_checkpoint_hash,
    });
    startup.trace = ZakuraTrace::new(capture.tracer(), "01");
    let mut fixture = spawn_test_reactor(startup);
    let peer_id = peer(77);

    connect_peer(&fixture, peer_id).await;
    advertise_tip(
        &fixture,
        peer(77),
        block::Height(0),
        block::Height(1_000),
        DEFAULT_HS_RANGE,
        10,
    )
    .await;

    loop {
        if let HeaderSyncAction::SendMessage {
            msg:
                HeaderSyncMessage::GetHeaders {
                    start_height,
                    count,
                    want_tree_aux_roots,
                },
            ..
        } = next_non_query_action(&mut fixture.actions).await
        {
            assert_eq!(start_height, block::Height(401));
            assert_eq!(count, 600);
            assert!(
                want_tree_aux_roots,
                "header ranges below the checkpoint handoff should carry roots"
            );
            break;
        }
    }

    capture.flush().await;
    let reader = capture.reader().unwrap();
    reader.table(HEADER_SYNC_TABLE.table()).assert_row(
        hs_trace::HEADER_GET_HEADERS_SENT,
        &[
            (hs_trace::RANGE_START, TraceValue::U64(401)),
            (hs_trace::RANGE_COUNT, TraceValue::U64(600)),
            (hs_trace::FINALIZED, TraceValue::Bool(false)),
            (hs_trace::WANT_TREE_AUX_ROOTS, TraceValue::Bool(true)),
            (hs_trace::RANGE_PRIORITY, TraceValue::Str("forward")),
            (hs_trace::VERIFIED_BLOCK_TIP, TraceValue::U64(0)),
            (hs_trace::FINALIZED_HEIGHT, TraceValue::U64(0)),
            (hs_trace::BEST_HEADER_TIP, TraceValue::U64(400)),
        ],
    );

    let _ = capture.finish().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn forward_ranges_above_handoff_do_not_request_tree_aux_roots() {
    let checkpoint_hash = block::Hash::from(mainnet_header(&BLOCK_MAINNET_3_BYTES).as_ref());
    let (network, _) = checkpoint_testnet_with_hash(block::Height(3), checkpoint_hash);
    let checkpoint = (block::Height(3), checkpoint_hash);
    let mut startup = startup_for(
        network,
        (block::Height(0), Network::Mainnet.genesis_hash()),
        Some(checkpoint),
    );
    startup.header_root_auth = Some(HeaderRootAuthState {
        authenticated_height: checkpoint.0,
        authenticated_hash: checkpoint.1,
        completed_checkpoint_height: checkpoint.0,
        completed_checkpoint_hash: checkpoint.1,
    });
    let mut fixture = spawn_test_reactor(startup);
    let peer_id = peer(235);

    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(&fixture, peer_id, block::Height(0), block::Height(4), 1, 1).await;

    loop {
        if let HeaderSyncAction::SendMessage {
            msg:
                HeaderSyncMessage::GetHeaders {
                    start_height,
                    want_tree_aux_roots,
                    ..
                },
            ..
        } = next_non_query_action(&mut fixture.actions).await
        {
            assert_eq!(start_height, block::Height(4));
            assert!(!want_tree_aux_roots);
            break;
        }
    }

    fixture.task.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn incoming_headers_match_outstanding_before_commit() {
    let checkpoint_hash = block::Hash::from(mainnet_header(&BLOCK_MAINNET_3_BYTES).as_ref());
    let (network, _) = checkpoint_testnet_with_hash(block::Height(3), checkpoint_hash);
    let first_checkpoint = block::Height(3);
    let start = block::Height(4);
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        Some((first_checkpoint, checkpoint_hash)),
    ));
    let peer_id = peer(8);

    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(&fixture, peer_id.clone(), block::Height(0), start, 1, 1).await;
    let request_id = next_get_headers_request_id(&mut fixture.actions).await;

    send_headers(
        &fixture,
        &peer_id,
        request_id,
        headers_message(vec![mainnet_header(&BLOCK_MAINNET_4_BYTES)]),
    )
    .await;

    match next_non_query_action(&mut fixture.actions).await {
        HeaderSyncAction::CommitHeaderRange {
            operation,
            payload,
            finalized,
            ..
        } => {
            assert_eq!(
                operation,
                commit_operation(peer_id, 0, request_id),
                "the commit action preserves the exact wire request identity"
            );
            assert_eq!(payload.range().start(), start);
            assert!(!finalized);
        }
        action => panic!("unexpected action: {action:?}"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn only_exact_commit_operation_completion_has_side_effects() {
    let checkpoint_hash = block::Hash::from(mainnet_header(&BLOCK_MAINNET_3_BYTES).as_ref());
    let (network, _) = checkpoint_testnet_with_hash(block::Height(3), checkpoint_hash);
    let start = block::Height(4);
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        Some((block::Height(3), checkpoint_hash)),
    ));
    let peer_id = peer(209);
    let mut tip = fixture.handle.subscribe_tip();

    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(&fixture, peer_id.clone(), block::Height(0), start, 1, 1).await;
    let request_id = next_get_headers_request_id(&mut fixture.actions).await;
    send_headers(
        &fixture,
        &peer_id,
        request_id,
        headers_message(vec![mainnet_header(&BLOCK_MAINNET_4_BYTES)]),
    )
    .await;
    let exact = loop {
        if let HeaderSyncAction::CommitHeaderRange { operation, .. } =
            next_non_query_action(&mut fixture.actions).await
        {
            break operation;
        }
    };

    let wrong_request = HeaderSyncOperationIdentity {
        wire_request: HeaderSyncWireRequestIdentity {
            request_id: HeaderSyncRequestId::new(
                request_id
                    .get()
                    .checked_add(1)
                    .expect("test request ID has room"),
            )
            .expect("incremented request ID is non-zero"),
            ..exact.wire_request.clone()
        },
        op_kind: HeaderSyncOperationKind::CommitHeaders,
    };
    let wrong_session = HeaderSyncOperationIdentity {
        wire_request: HeaderSyncWireRequestIdentity {
            session_id: exact.wire_request.session_id + 1,
            ..exact.wire_request.clone()
        },
        op_kind: HeaderSyncOperationKind::CommitHeaders,
    };
    let wrong_peer = HeaderSyncOperationIdentity {
        wire_request: HeaderSyncWireRequestIdentity {
            peer: peer(210),
            ..exact.wire_request.clone()
        },
        op_kind: HeaderSyncOperationKind::CommitHeaders,
    };
    let wrong_kind = HeaderSyncOperationIdentity {
        wire_request: exact.wire_request.clone(),
        op_kind: HeaderSyncOperationKind::AuthenticateRoots,
    };

    for operation in [wrong_request, wrong_session, wrong_peer, wrong_kind] {
        fixture
            .handle
            .send(HeaderSyncEvent::HeaderRangeOperationCompleted {
                operation,
                tip_hash: block::Hash([99; 32]),
            })
            .await
            .unwrap();
    }
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), tip.changed())
            .await
            .is_err(),
        "wrong operation identities must not advance the tip"
    );

    let committed_hash = mainnet_block(&BLOCK_MAINNET_4_BYTES).hash();
    fixture
        .handle
        .send(HeaderSyncEvent::HeaderRangeOperationCompleted {
            operation: exact.clone(),
            tip_hash: committed_hash,
        })
        .await
        .unwrap();
    tip.changed().await.unwrap();
    assert_eq!(*tip.borrow(), (start, committed_hash));

    fixture
        .handle
        .send(HeaderSyncEvent::HeaderRangeOperationCompleted {
            operation: exact,
            tip_hash: block::Hash([100; 32]),
        })
        .await
        .unwrap();
    tokio::task::yield_now().await;
    assert_eq!(
        *tip.borrow(),
        (start, committed_hash),
        "a duplicate completion is stale and side-effect free"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn rootless_non_empty_response_is_malformed() {
    let checkpoint_hash = block::Hash::from(mainnet_header(&BLOCK_MAINNET_3_BYTES).as_ref());
    let (network, _) = checkpoint_testnet_with_hash(block::Height(3), checkpoint_hash);
    let first_checkpoint = block::Height(3);
    let start = block::Height(4);
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        Some((first_checkpoint, checkpoint_hash)),
    ));
    let peer_id = peer(8);

    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(&fixture, peer_id.clone(), block::Height(0), start, 1, 1).await;
    let request_id = next_get_headers_request_id(&mut fixture.actions).await;

    send_headers(
        &fixture,
        &peer_id,
        request_id,
        rootless_headers_message_from(start, vec![mainnet_header(&BLOCK_MAINNET_4_BYTES)]),
    )
    .await;

    loop {
        match next_non_query_action(&mut fixture.actions).await {
            HeaderSyncAction::Misbehavior { peer, reason } => {
                assert_eq!(peer, peer_id);
                assert_eq!(reason, HeaderSyncMisbehavior::MalformedMessage);
                break;
            }
            HeaderSyncAction::CommitHeaderRange { .. } => {
                panic!("a rootless non-empty response must not commit")
            }
            _ => {}
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn headers_over_outstanding_contract_reports_response_too_long_without_flooding() {
    let network = Network::Mainnet;
    let first_checkpoint = network
        .checkpoint_list()
        .min_height_in_range(block::Height(1)..)
        .expect("mainnet has a checkpoint above genesis");
    let previous_hash = block::Hash([1; 32]);
    let start = next_height(first_checkpoint).expect("checkpoint height has successor");
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        Some((first_checkpoint, previous_hash)),
    ));
    let peer_id = peer(61);

    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(
        &fixture,
        peer_id.clone(),
        block::Height(0),
        block::Height(start.0 + 1),
        1,
        1,
    )
    .await;
    let request_id = next_get_headers_request_id(&mut fixture.actions).await;

    send_headers(
        &fixture,
        &peer_id,
        request_id,
        headers_message_from(
            start,
            vec![
                mainnet_header(&BLOCK_MAINNET_1_BYTES),
                mainnet_header(&BLOCK_MAINNET_2_BYTES),
            ],
        ),
    )
    .await;

    loop {
        match next_non_query_action(&mut fixture.actions).await {
            HeaderSyncAction::Misbehavior { peer, reason } => {
                assert_eq!(peer, peer_id);
                assert_eq!(reason, HeaderSyncMisbehavior::ResponseTooLong);
                break;
            }
            HeaderSyncAction::ForwardNewBlock { .. } => {
                panic!("backfill Headers must never produce tip-flood forwarding")
            }
            _ => {}
        }
    }
    assert_no_commit_or_misbehavior(&mut fixture.actions).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn matching_headers_are_statelessly_validated_before_commit() {
    let network = Network::Mainnet;
    let first_checkpoint = network
        .checkpoint_list()
        .min_height_in_range(block::Height(1)..)
        .expect("mainnet has a checkpoint above genesis");
    let two_before_checkpoint = block::Height(
        first_checkpoint
            .0
            .checked_sub(2)
            .expect("mainnet first checkpoint has two predecessors"),
    );
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        Some((two_before_checkpoint, block::Hash([1; 32]))),
    ));
    let peer_id = peer(32);

    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(
        &fixture,
        peer_id.clone(),
        block::Height(0),
        first_checkpoint,
        2,
        1,
    )
    .await;
    let request_id = next_get_headers_request_id(&mut fixture.actions).await;

    let mut bad_second = *mainnet_header(&BLOCK_MAINNET_2_BYTES);
    bad_second.previous_block_hash = block::Hash([7; 32]);
    send_headers(
        &fixture,
        &peer_id,
        request_id,
        headers_message_from(
            next_height(two_before_checkpoint).expect("has successor"),
            vec![mainnet_header(&BLOCK_MAINNET_1_BYTES), Arc::new(bad_second)],
        ),
    )
    .await;

    match next_non_query_action(&mut fixture.actions).await {
        HeaderSyncAction::Misbehavior { peer, reason } => {
            assert_eq!(peer, peer_id);
            assert_eq!(reason, HeaderSyncMisbehavior::InvalidRange);
        }
        action => panic!("unexpected action: {action:?}"),
    }
    assert_no_commit_or_misbehavior(&mut fixture.actions).await;
}

#[tokio::test(flavor = "current_thread")]
async fn truncated_finalized_suffix_still_checks_its_checkpoint_hash() {
    let headers = [
        mainnet_header(&BLOCK_MAINNET_1_BYTES),
        mainnet_header(&BLOCK_MAINNET_2_BYTES),
        mainnet_header(&BLOCK_MAINNET_3_BYTES),
    ];
    let (network, _) = checkpoint_testnet_with_hash(block::Height(3), block::Hash([9; 32]));
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let peer_id = peer(207);
    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(
        &fixture,
        peer_id.clone(),
        block::Height(0),
        block::Height(3),
        DEFAULT_HS_RANGE,
        1,
    )
    .await;
    let prefix_id = next_get_headers_request_id(&mut fixture.actions).await;
    send_headers(
        &fixture,
        &peer_id,
        prefix_id,
        headers_message(headers[..2].to_vec()),
    )
    .await;
    loop {
        if matches!(
            next_non_query_action(&mut fixture.actions).await,
            HeaderSyncAction::CommitHeaderRange { .. }
        ) {
            break;
        }
    }
    let (_, suffix_id, suffix_start, suffix_count) =
        next_outbound_get_headers(&mut fixture.actions).await;
    assert_eq!((suffix_start, suffix_count), (block::Height(3), 1));
    send_headers(
        &fixture,
        &peer_id,
        suffix_id,
        headers_message_from(block::Height(3), vec![headers[2].clone()]),
    )
    .await;

    loop {
        match next_non_query_action(&mut fixture.actions).await {
            HeaderSyncAction::Misbehavior {
                peer,
                reason: HeaderSyncMisbehavior::InvalidRange,
            } => {
                assert_eq!(peer, peer_id);
                break;
            }
            HeaderSyncAction::CommitHeaderRange { payload, .. }
                if payload.range().start() == block::Height(3) =>
            {
                panic!("a truncated suffix with the wrong checkpoint hash was committed")
            }
            _ => {}
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn unmatched_async_header_commit_failure_is_ignored() {
    let network = regtest_network();
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let peer_id = peer(62);

    connect_peer(&fixture, peer_id.clone()).await;
    fixture
        .handle
        .send(HeaderSyncEvent::HeaderRangeOperationFailed {
            operation: commit_operation(
                peer_id.clone(),
                0,
                HeaderSyncRequestId::new(1).expect("test request ID is non-zero"),
            ),
            kind: HeaderSyncCommitFailureKind::InvalidPeerRange,
        })
        .await
        .unwrap();

    while let Ok(Some(action)) =
        tokio::time::timeout(std::time::Duration::from_millis(50), fixture.actions.recv()).await
    {
        assert!(
            !matches!(action, HeaderSyncAction::Misbehavior { .. }),
            "an unmatched completion must not score a peer"
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn peer_disconnect_removes_outstanding_requests_for_that_peer() {
    let network = Network::Mainnet;
    let first_checkpoint = network
        .checkpoint_list()
        .min_height_in_range(block::Height(1)..)
        .expect("mainnet has a checkpoint above genesis");
    let previous_checkpoint_height =
        previous_height(first_checkpoint).expect("checkpoint above genesis has predecessor");
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        Some((previous_checkpoint_height, block::Hash([1; 32]))),
    ));
    let peer_id = peer(11);

    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(
        &fixture,
        peer_id.clone(),
        block::Height(0),
        first_checkpoint,
        1,
        1,
    )
    .await;
    let request_id = next_get_headers_request_id(&mut fixture.actions).await;

    fixture
        .handle
        .send(HeaderSyncEvent::PeerDisconnected(peer_id.clone()))
        .await
        .unwrap();
    // The disconnect dropped the peer's outstanding range along with its session, so
    // a response that was already in flight can no longer be correlated: it is dropped
    // without committing anything and without scoring a peer that is already gone.
    send_headers(
        &fixture,
        &peer_id,
        request_id,
        headers_message(vec![mainnet_header(&BLOCK_MAINNET_1_BYTES)]),
    )
    .await;

    assert_no_commit_or_misbehavior(&mut fixture.actions).await;
}

#[tokio::test(flavor = "current_thread")]
async fn timed_out_range_retries_with_another_peer() {
    let network = regtest_network();
    let mut fixture = spawn_test_reactor(startup_with_timeout(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        std::time::Duration::from_millis(1),
    ));
    let first_peer = peer(12);
    let second_peer = peer(13);

    connect_peer(&fixture, first_peer.clone()).await;
    advertise_tip(
        &fixture,
        first_peer,
        block::Height(0),
        block::Height(2),
        2,
        1,
    )
    .await;
    let _request_id = next_get_headers_request_id(&mut fixture.actions).await;

    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    connect_peer(&fixture, second_peer.clone()).await;
    advertise_tip(
        &fixture,
        second_peer.clone(),
        block::Height(0),
        block::Height(2),
        2,
        1,
    )
    .await;

    loop {
        if let HeaderSyncAction::SendMessage { peer, msg, .. } =
            next_non_query_action(&mut fixture.actions).await
        {
            if matches!(msg, HeaderSyncMessage::GetHeaders { .. }) {
                assert_eq!(peer, second_peer);
                break;
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_commit_failure_retries_without_peer_misbehavior() {
    let checkpoint_hash = block::Hash::from(mainnet_header(&BLOCK_MAINNET_3_BYTES).as_ref());
    let (network, _) = checkpoint_testnet_with_hash(block::Height(3), checkpoint_hash);
    let start = block::Height(4);
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        Some((block::Height(3), checkpoint_hash)),
    ));
    let first_peer = peer(35);
    let second_peer = peer(36);

    for peer_id in [first_peer.clone(), second_peer.clone()] {
        connect_peer(&fixture, peer_id.clone()).await;
        advertise_tip(&fixture, peer_id, block::Height(0), start, 1, 1).await;
    }

    let request_id = loop {
        if let HeaderSyncAction::SendMessage {
            peer,
            request_id,
            msg: HeaderSyncMessage::GetHeaders { .. },
        } = next_non_query_action(&mut fixture.actions).await
        {
            if peer == first_peer {
                break request_id.expect("an outbound GetHeaders always carries a request ID");
            }
        }
    };
    send_headers(
        &fixture,
        &first_peer,
        request_id,
        headers_message(vec![mainnet_header(&BLOCK_MAINNET_4_BYTES)]),
    )
    .await;
    let operation = loop {
        match next_non_query_action(&mut fixture.actions).await {
            HeaderSyncAction::Misbehavior { .. } => {
                panic!("valid headers must not be scored before local commit failure")
            }
            HeaderSyncAction::CommitHeaderRange {
                operation, payload, ..
            } => {
                assert_eq!(operation.wire_request.peer, first_peer);
                assert_eq!(payload.range().start(), start);
                assert_eq!(payload.headers().len(), 1);
                break operation;
            }
            _ => {}
        }
    };

    fixture
        .handle
        .send(HeaderSyncEvent::HeaderRangeOperationFailed {
            operation: operation.clone(),
            kind: HeaderSyncCommitFailureKind::Local,
        })
        .await
        .unwrap();

    loop {
        match next_non_query_action(&mut fixture.actions).await {
            HeaderSyncAction::Misbehavior { .. } => {
                panic!("local commit failure must not score peer")
            }
            HeaderSyncAction::SendMessage {
                peer,
                msg:
                    HeaderSyncMessage::GetHeaders {
                        start_height,
                        count,
                        want_tree_aux_roots: true,
                    },
                ..
            } if peer == first_peer || peer == second_peer => {
                assert_eq!(start_height, start);
                assert_eq!(count, 1);
                break;
            }
            _ => {}
        }
    }

    fixture
        .handle
        .send(HeaderSyncEvent::HeaderRangeOperationFailed {
            operation,
            kind: HeaderSyncCommitFailureKind::Local,
        })
        .await
        .unwrap();
    while let Ok(Some(action)) =
        tokio::time::timeout(std::time::Duration::from_millis(50), fixture.actions.recv()).await
    {
        assert!(
            !matches!(
                action,
                HeaderSyncAction::SendMessage {
                    msg: HeaderSyncMessage::GetHeaders { .. },
                    ..
                } | HeaderSyncAction::Misbehavior { .. }
            ),
            "a duplicate failure must not retry or score twice"
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn material_tip_advance_sends_rate_limited_unsolicited_status() {
    let network = regtest_network();
    let mut startup = startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    );
    startup.status_refresh_interval = std::time::Duration::from_secs(60);
    let mut fixture = spawn_test_reactor(startup);
    let peer_id = peer(14);

    connect_peer(&fixture, peer_id.clone()).await;
    loop {
        if matches!(
            next_non_query_action(&mut fixture.actions).await,
            HeaderSyncAction::SendMessage {
                msg: HeaderSyncMessage::Status(_),
                ..
            }
        ) {
            break;
        }
    }

    for height in [block::Height(1), block::Height(2)] {
        fixture
            .handle
            .send(HeaderSyncEvent::BestHeaderTipLoaded {
                tip_height: height,
                tip_hash: block::Hash(
                    [u8::try_from(height.0).expect("test heights fit in u8"); 32],
                ),
            })
            .await
            .unwrap();
    }

    let mut status_count = 0;
    while let Ok(Some(action)) =
        tokio::time::timeout(std::time::Duration::from_millis(20), fixture.actions.recv()).await
    {
        if matches!(
            action,
            HeaderSyncAction::SendMessage {
                msg: HeaderSyncMessage::Status(_),
                ..
            }
        ) {
            status_count += 1;
        }
    }

    assert_eq!(status_count, 1);
}

#[test]
fn peer_state_suppresses_redundant_status_until_session_reset() {
    let (send, _recv) = crate::zakura::framed_channel(32);
    let session = HeaderSyncPeerSession::from_parts_with_direction(
        peer(80),
        ServicePeerDirection::Inbound,
        send,
        CancellationToken::new(),
    );
    let mut peer_state = super::state::PeerHeaderState::new(
        session,
        (block::Height(0), block::Hash([0; 32])),
        DEFAULT_HS_RANGE,
        DEFAULT_HS_MAX_INFLIGHT,
        std::time::Duration::from_secs(1),
        std::time::Duration::from_secs(1),
        std::time::Duration::from_secs(1),
    );

    let status = HeaderSyncStatus {
        tip_height: block::Height(5),
        tip_hash: block::Hash([5; 32]),
        ..HeaderSyncStatus::default()
    };

    // Nothing has been sent yet, so the first status is always new.
    assert!(peer_state.status_differs_from_last_sent(status));
    peer_state.record_sent_status(status);

    // An identical status is redundant and must be suppressed.
    assert!(!peer_state.status_differs_from_last_sent(status));

    // A tip-advancing status differs and is sent.
    let advanced = HeaderSyncStatus {
        tip_height: block::Height(6),
        ..status
    };
    assert!(peer_state.status_differs_from_last_sent(advanced));

    // A same-height hash change (e.g. a reorg at the tip) also differs.
    let reorged = HeaderSyncStatus {
        tip_hash: block::Hash([9; 32]),
        ..status
    };
    assert!(peer_state.status_differs_from_last_sent(reorged));

    // Replacing the session forgets the last status, so an identical status is
    // resent — a fresh channel's remote has not received it and gates serving on it.
    peer_state.reset_sent_status();
    assert!(peer_state.status_differs_from_last_sent(status));
}

#[test]
fn response_before_publication_completion_is_not_reinstalled() {
    let (send, _recv) = crate::zakura::framed_channel(1);
    let session = HeaderSyncPeerSession::from_parts_with_direction(
        peer(81),
        ServicePeerDirection::Inbound,
        send,
        CancellationToken::new(),
    );
    let mut peer_state = super::state::PeerHeaderState::new(
        session,
        (block::Height(0), block::Hash([0; 32])),
        DEFAULT_HS_RANGE,
        DEFAULT_HS_MAX_INFLIGHT,
        std::time::Duration::from_secs(1),
        std::time::Duration::from_secs(1),
        std::time::Duration::from_secs(1),
    );
    let request_id = HeaderSyncRequestId::new(1).expect("non-zero request ID");
    let range = RangeRequest {
        range: CheckedHeaderRange::from_count(block::Height(1), 1)
            .expect("test range is non-empty"),
        anchor_hash: None,
        finalized: false,
        want_tree_aux_roots: true,
        priority: RangePriority::Forward,
    };
    peer_state.outstanding.push(OutstandingRange {
        wire_request: HeaderSyncWireRequestIdentity {
            peer: peer(82),
            session_id: 0,
            request_id,
        },
        range_request: range,
        deadline: Instant::now() + std::time::Duration::from_secs(1),
        purpose: RangePurpose::Sync,
        phase: OutstandingPhase::Publishing,
    });

    let _response = peer_state
        .remove_outstanding_by_request_id(request_id)
        .expect("response consumes the publishing request");
    complete_request_publication(
        &mut peer_state,
        request_id,
        Instant::now() + std::time::Duration::from_secs(2),
    );

    assert!(
        peer_state.outstanding.is_empty(),
        "the later successful completion must not recreate a consumed request"
    );
}

#[test]
fn completed_v7_inbound_request_id_cannot_be_reused() {
    let (send, _recv) = crate::zakura::framed_channel(32);
    let session = HeaderSyncPeerSession::from_parts_with_direction(
        peer(82),
        ServicePeerDirection::Inbound,
        send,
        CancellationToken::new(),
    );
    let mut peer_state = super::state::PeerHeaderState::new(
        session,
        (block::Height(0), block::Hash([0; 32])),
        DEFAULT_HS_RANGE,
        DEFAULT_HS_MAX_INFLIGHT,
        std::time::Duration::from_secs(1),
        std::time::Duration::from_secs(1),
        std::time::Duration::from_secs(1),
    );
    let first = HeaderSyncRequestId::new(1).expect("non-zero id");
    let second = HeaderSyncRequestId::new(2).expect("non-zero id");

    assert!(peer_state.try_start_serving_headers(2, first));
    assert!(peer_state.finish_serving_headers(first));
    assert!(!peer_state.try_start_serving_headers(2, first));
    assert!(peer_state.try_start_serving_headers(2, second));
}

#[tokio::test(flavor = "current_thread")]
async fn reconnect_resends_initial_status_after_session_reset() {
    let network = regtest_network();
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let peer_id = peer(72);

    // First connect: the peer receives its initial status.
    connect_peer(&fixture, peer_id.clone()).await;
    assert!(matches!(
        next_non_query_action(&mut fixture.actions).await,
        HeaderSyncAction::SendMessage {
            msg: HeaderSyncMessage::Status(_),
            ..
        }
    ));

    // Reconnecting installs a fresh session at the same frontier. Even though the
    // status is byte-identical to the one already sent, the new channel's remote
    // has not received it, so it must be resent rather than suppressed.
    connect_peer(&fixture, peer_id.clone()).await;
    assert!(matches!(
        next_non_query_action(&mut fixture.actions).await,
        HeaderSyncAction::SendMessage {
            msg: HeaderSyncMessage::Status(_),
            ..
        }
    ));
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn failed_status_publication_is_retry_paced() {
    header_sync_metrics_recorder();
    let send_failed_before = metric_value("sync.header.peer.status.send_failed");
    let mut capture = TraceCapture::for_test("failed_status_publication_is_retry_paced").unwrap();
    let network = regtest_network();
    let mut startup = startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    );
    startup.range_state_actions_enabled = false;
    startup.request_timeout = std::time::Duration::from_millis(10);
    startup.status_refresh_interval = std::time::Duration::from_secs(10);
    startup.trace = ZakuraTrace::new(capture.tracer(), "status-publication-retry");
    let fixture = spawn_test_reactor(startup);
    let peer_id = peer(74);
    let (send, mut recv) = crate::zakura::framed_channel(1);
    send.try_send(
        HeaderSyncMessage::Status(HeaderSyncStatus::default())
            .encode_frame(None)
            .expect("filler status frame encodes"),
    )
    .expect("outbound queue starts full");
    let cancel = CancellationToken::new();
    let session = HeaderSyncPeerSession::from_parts_with_direction(
        peer_id,
        ServicePeerDirection::Inbound,
        send,
        cancel,
    );
    fixture
        .handle
        .send(HeaderSyncEvent::PeerConnected(session))
        .await
        .unwrap();

    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            if fixture.handle.peer_snapshot().inbound_peers == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("peer is admitted while outbound queue is full");

    for _ in 0..3 {
        tokio::task::yield_now().await;
    }
    capture.flush().await;
    assert!(
        capture
            .reader()
            .unwrap()
            .table(HEADER_SYNC_TABLE.table())
            .count(hs_trace::HEADER_MAINTENANCE_WAKEUP)
            <= 1,
        "a failed status publication must not make maintenance spin"
    );

    let _ = recv.recv().await.expect("filler frame drains");
    tokio::time::advance(STATUS_PUBLICATION_RETRY_DELAY).await;
    let frame = tokio::time::timeout(std::time::Duration::from_secs(1), recv.recv())
        .await
        .expect("status retry arrives")
        .expect("outbound channel remains open");
    assert!(matches!(
        HeaderSyncMessage::decode_frame(frame, HeaderSyncDecodeContext::control())
            .expect("retry status decodes")
            .0,
        HeaderSyncMessage::Status(_)
    ));
    assert!(
        metric_value("sync.header.peer.status.send_failed") > send_failed_before,
        "a full outbound queue must increment the header-sync Status send-failure counter"
    );
    let _ = capture.finish().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn reconnect_clears_session_bound_outstanding_ranges() {
    let network = regtest_network();
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let peer_id = peer(73);

    connect_peer(&fixture, peer_id.clone()).await;
    assert!(matches!(
        next_non_query_action(&mut fixture.actions).await,
        HeaderSyncAction::SendMessage {
            msg: HeaderSyncMessage::Status(_),
            ..
        }
    ));
    advertise_tip(
        &fixture,
        peer_id.clone(),
        block::Height(0),
        block::Height(5),
        1,
        1,
    )
    .await;
    assert!(matches!(
        next_non_query_action(&mut fixture.actions).await,
        HeaderSyncAction::SendMessage {
            peer,
            msg: HeaderSyncMessage::GetHeaders {
                start_height: block::Height(1),
                count: 1,
                want_tree_aux_roots: true,
            },
            ..
        } if peer == peer_id
    ));

    connect_peer(&fixture, peer_id.clone()).await;
    assert!(matches!(
        next_non_query_action(&mut fixture.actions).await,
        HeaderSyncAction::SendMessage {
            msg: HeaderSyncMessage::Status(_),
            ..
        }
    ));
    advertise_tip(
        &fixture,
        peer_id.clone(),
        block::Height(0),
        block::Height(5),
        1,
        1,
    )
    .await;
    assert!(matches!(
        next_non_query_action(&mut fixture.actions).await,
        HeaderSyncAction::SendMessage {
            peer,
            msg: HeaderSyncMessage::GetHeaders {
                start_height: block::Height(1),
                count: 1,
                want_tree_aux_roots: true,
            },
            ..
        } if peer == peer_id
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn full_block_committed_covers_outstanding_height() {
    let network = regtest_network();
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let peer_id = peer(42);

    let cancel =
        connect_peer_with_direction(&fixture, peer_id.clone(), ServicePeerDirection::Inbound).await;
    advertise_tip(
        &fixture,
        peer_id.clone(),
        block::Height(0),
        block::Height(1),
        1,
        1,
    )
    .await;
    let _request_id = next_get_headers_request_id(&mut fixture.actions).await;

    fixture
        .handle
        .send(HeaderSyncEvent::FullBlockCommitted {
            height: block::Height(1),
            hash: block::Hash([1; 32]),
        })
        .await
        .unwrap();
    match next_action(&mut fixture.actions).await {
        HeaderSyncAction::HeaderAdvanced { height, hash } => {
            assert_eq!(height, block::Height(1));
            assert_eq!(hash, block::Hash([1; 32]));
        }
        action => panic!("full block commit must publish a header advance, got {action:?}"),
    }

    // The covered range's request ID is retired rather than the stream torn down: a
    // late response to it is matched to the retired ID and dropped, so it cannot be
    // mistaken for newer work. The peer therefore stays connected and usable.
    assert!(!cancel.is_cancelled());
    assert_eq!(fixture.handle.peer_snapshot().inbound_peers, 1);
    assert_no_commit_or_misbehavior(&mut fixture.actions).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inbound_unseen_valid_new_block_is_seen_and_forwarded_to_eligible_peers() {
    let network = Network::Mainnet;
    let block = mainnet_block(&BLOCK_MAINNET_1_BYTES);
    let hash = block.hash();
    let height = block.coinbase_height().expect("test block has height");
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let source = peer(46);
    let eligible = peer(47);
    let redundant = peer(48);

    for peer_id in [source.clone(), eligible.clone(), redundant.clone()] {
        connect_peer(&fixture, peer_id).await;
    }
    advertise_tip(
        &fixture,
        source.clone(),
        block::Height(0),
        block::Height(0),
        DEFAULT_HS_RANGE,
        1,
    )
    .await;
    advertise_tip(
        &fixture,
        eligible.clone(),
        block::Height(0),
        block::Height(0),
        DEFAULT_HS_RANGE,
        1,
    )
    .await;
    advertise_tip(
        &fixture,
        redundant.clone(),
        block::Height(0),
        height,
        DEFAULT_HS_RANGE,
        1,
    )
    .await;

    fixture
        .handle
        .send(HeaderSyncEvent::WireMessage {
            peer: source.clone(),
            msg: HeaderSyncMessage::NewBlock(block.clone()),
        })
        .await
        .unwrap();

    let mut saw_pipeline_fact = false;
    let mut forwarded = Vec::new();
    while let Ok(Some(action)) = tokio::time::timeout(
        std::time::Duration::from_millis(200),
        fixture.actions.recv(),
    )
    .await
    {
        match action {
            HeaderSyncAction::NewBlockReceived {
                peer,
                height: action_height,
                hash: action_hash,
                ..
            } => {
                assert_eq!(peer, source);
                assert_eq!(action_height, height);
                assert_eq!(action_hash, hash);
                saw_pipeline_fact = true;
                fixture
                    .handle
                    .send(HeaderSyncEvent::NewBlockAccepted {
                        peer: source.clone(),
                        height,
                        hash,
                        block: block.clone(),
                    })
                    .await
                    .unwrap();
            }
            HeaderSyncAction::ForwardNewBlock {
                source: action_source,
                peer,
                height: action_height,
                hash: action_hash,
                ..
            } => {
                assert_eq!(action_source, Some(source.clone()));
                assert_eq!(action_height, height);
                assert_eq!(action_hash, hash);
                forwarded.push(peer);
            }
            HeaderSyncAction::Misbehavior { peer, reason } => {
                panic!("valid NewBlock must not score {peer:?}: {reason:?}");
            }
            _ => {}
        }
    }

    assert!(saw_pipeline_fact);
    assert_eq!(forwarded, vec![eligible]);

    fixture
        .handle
        .send(HeaderSyncEvent::WireMessage {
            peer: source.clone(),
            msg: HeaderSyncMessage::NewBlock(block),
        })
        .await
        .unwrap();

    while let Ok(Some(action)) = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        fixture.actions.recv(),
    )
    .await
    {
        if matches!(
            action,
            HeaderSyncAction::ForwardNewBlock { .. }
                | HeaderSyncAction::NewBlockReceived { .. }
                | HeaderSyncAction::Misbehavior { .. }
        ) {
            panic!("duplicate NewBlock must be cheap-deduped without scoring: {action:?}");
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn accepted_non_best_chain_new_block_is_deduped_without_advancing_or_forwarding() {
    let network = Network::Mainnet;
    let block = mainnet_block(&BLOCK_MAINNET_1_BYTES);
    let hash = block.hash();
    let height = block.coinbase_height().expect("test block has height");
    let anchor = (block::Height(0), network.genesis_hash());
    let mut fixture = spawn_test_reactor(startup_for(network.clone(), anchor, None));
    let mut tip = fixture.handle.subscribe_tip();
    let source = peer(55);
    let would_be_destination = peer(56);

    // The destination's advertised tip is below the block height, so a
    // best-chain accept at this height WOULD forward to it.
    for peer_id in [source.clone(), would_be_destination.clone()] {
        connect_peer(&fixture, peer_id.clone()).await;
        advertise_tip(
            &fixture,
            peer_id,
            block::Height(0),
            block::Height(0),
            DEFAULT_HS_RANGE,
            1,
        )
        .await;
    }

    fixture
        .handle
        .send(HeaderSyncEvent::NewBlockAcceptedNonBestChain {
            peer: source.clone(),
            height,
            hash,
        })
        .await
        .unwrap();

    // A non-best-chain accept advances no frontier and forwards nothing.
    while let Ok(Some(action)) = tokio::time::timeout(
        std::time::Duration::from_millis(200),
        fixture.actions.recv(),
    )
    .await
    {
        if matches!(
            action,
            HeaderSyncAction::ForwardNewBlock { .. }
                | HeaderSyncAction::HeaderAdvanced { .. }
                | HeaderSyncAction::HeaderReanchored { .. }
        ) {
            panic!("non-best-chain accept must not advance frontiers or forward: {action:?}");
        }
    }
    assert_eq!(fixture.handle.best_header_tip(), anchor);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), tip.changed())
            .await
            .is_err(),
        "non-best-chain accept must not publish a new best header tip"
    );

    // The hash is remembered: a later wire NewBlock for it dedups without
    // re-entering the block pipeline or scoring the sender.
    fixture
        .handle
        .send(HeaderSyncEvent::WireMessage {
            peer: source,
            msg: HeaderSyncMessage::NewBlock(block),
        })
        .await
        .unwrap();
    while let Ok(Some(action)) = tokio::time::timeout(
        std::time::Duration::from_millis(200),
        fixture.actions.recv(),
    )
    .await
    {
        if matches!(
            action,
            HeaderSyncAction::NewBlockReceived { .. }
                | HeaderSyncAction::ForwardNewBlock { .. }
                | HeaderSyncAction::Misbehavior { .. }
        ) {
            panic!("seen non-best-chain block must be cheap-deduped without scoring: {action:?}");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_duplicate_new_block_dedups_pending_acceptance_without_scoring() {
    let network = Network::Mainnet;
    let block = mainnet_block(&BLOCK_MAINNET_1_BYTES);
    let hash = block.hash();
    let height = block.coinbase_height().expect("test block has height");
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let first_peer = peer(52);
    let duplicate_peer = peer(53);
    let eligible_peer = peer(54);

    for peer_id in [
        first_peer.clone(),
        duplicate_peer.clone(),
        eligible_peer.clone(),
    ] {
        connect_peer(&fixture, peer_id.clone()).await;
        advertise_tip(
            &fixture,
            peer_id,
            block::Height(0),
            block::Height(0),
            DEFAULT_HS_RANGE,
            1,
        )
        .await;
    }

    fixture
        .handle
        .send(HeaderSyncEvent::WireMessage {
            peer: first_peer.clone(),
            msg: HeaderSyncMessage::NewBlock(block.clone()),
        })
        .await
        .unwrap();

    loop {
        match next_non_query_action(&mut fixture.actions).await {
            HeaderSyncAction::NewBlockReceived {
                peer,
                height: action_height,
                hash: action_hash,
                ..
            } => {
                assert_eq!(peer, first_peer);
                assert_eq!(action_height, height);
                assert_eq!(action_hash, hash);
                break;
            }
            HeaderSyncAction::Misbehavior { peer, reason } => {
                panic!("first valid NewBlock must not score {peer:?}: {reason:?}");
            }
            _ => {}
        }
    }

    fixture
        .handle
        .send(HeaderSyncEvent::WireMessage {
            peer: duplicate_peer.clone(),
            msg: HeaderSyncMessage::NewBlock(block.clone()),
        })
        .await
        .unwrap();

    while let Ok(Some(action)) = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        fixture.actions.recv(),
    )
    .await
    {
        if matches!(
            action,
            HeaderSyncAction::NewBlockReceived { .. } | HeaderSyncAction::Misbehavior { .. }
        ) {
            panic!("pending duplicate NewBlock must not re-enter acceptance or score: {action:?}");
        }
    }

    fixture
        .handle
        .send(HeaderSyncEvent::NewBlockAccepted {
            peer: first_peer,
            height,
            hash,
            block,
        })
        .await
        .unwrap();

    let mut forwarded = HashSet::new();
    while forwarded.len() < 2 {
        match next_non_query_action(&mut fixture.actions).await {
            HeaderSyncAction::ForwardNewBlock {
                peer,
                height: action_height,
                hash: action_hash,
                ..
            } => {
                assert_eq!(action_height, height);
                assert_eq!(action_hash, hash);
                forwarded.insert(peer);
            }
            HeaderSyncAction::Misbehavior { peer, reason } => {
                panic!("accepted duplicate flow must not score {peer:?}: {reason:?}");
            }
            _ => {}
        }
    }
    assert_eq!(forwarded, HashSet::from([duplicate_peer, eligible_peer]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_full_block_commit_prevents_later_new_block_regossip() {
    let network = Network::Mainnet;
    let block = mainnet_block(&BLOCK_MAINNET_1_BYTES);
    let height = block.coinbase_height().expect("test block has height");
    let hash = block.hash();
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let source = peer(49);
    let destination = peer(50);

    for peer_id in [source.clone(), destination] {
        connect_peer(&fixture, peer_id).await;
    }
    fixture
        .handle
        .send(HeaderSyncEvent::FullBlockCommitted { height, hash })
        .await
        .unwrap();
    fixture
        .handle
        .send(HeaderSyncEvent::WireMessage {
            peer: source,
            msg: HeaderSyncMessage::NewBlock(block),
        })
        .await
        .unwrap();

    while let Ok(Some(action)) = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        fixture.actions.recv(),
    )
    .await
    {
        if matches!(action, HeaderSyncAction::ForwardNewBlock { .. }) {
            panic!("locally committed block must not be gossiped twice: {action:?}");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_and_malformed_new_block_report_disconnect() {
    let network = Network::Mainnet;
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let unknown_peer = peer(63);
    let invalid_peer = peer(51);
    let malformed_peer = peer(52);
    connect_peer(&fixture, invalid_peer.clone()).await;
    connect_peer(&fixture, malformed_peer.clone()).await;

    fixture
        .handle
        .send(HeaderSyncEvent::WireMessage {
            peer: unknown_peer.clone(),
            msg: HeaderSyncMessage::NewBlock(mainnet_block(&BLOCK_MAINNET_1_BYTES)),
        })
        .await
        .unwrap();
    loop {
        if let HeaderSyncAction::Misbehavior { peer, reason } =
            next_non_query_action(&mut fixture.actions).await
        {
            assert_eq!(peer, unknown_peer);
            assert_eq!(reason, HeaderSyncMisbehavior::UnknownPeer);
            break;
        }
    }

    let mut bad_block = (*mainnet_block(&BLOCK_MAINNET_1_BYTES)).clone();
    let mut bad_header = *bad_block.header;
    bad_header.nonce[0] ^= 1;
    bad_block.header = Arc::new(bad_header);
    fixture
        .handle
        .send(HeaderSyncEvent::WireMessage {
            peer: invalid_peer.clone(),
            msg: HeaderSyncMessage::NewBlock(Arc::new(bad_block)),
        })
        .await
        .unwrap();

    loop {
        if let HeaderSyncAction::Misbehavior { peer, reason } =
            next_non_query_action(&mut fixture.actions).await
        {
            assert_eq!(peer, invalid_peer);
            assert_eq!(reason, HeaderSyncMisbehavior::InvalidNewBlock);
            break;
        }
    }

    fixture
        .handle
        .send(HeaderSyncEvent::WireDecodeFailed {
            peer: malformed_peer.clone(),
            error: Arc::new(HeaderSyncWireError::UnknownMessageType(MSG_HS_NEW_BLOCK)),
        })
        .await
        .unwrap();

    loop {
        if let HeaderSyncAction::Misbehavior { peer, reason } =
            next_non_query_action(&mut fixture.actions).await
        {
            assert_eq!(peer, malformed_peer);
            assert_eq!(reason, HeaderSyncMisbehavior::MalformedMessage);
            break;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rapid_status_updates_and_new_block_spam_report_disconnect() {
    let network = Network::Mainnet;
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let status_peer = peer(53);
    let block_peer = peer(54);
    connect_peer(&fixture, status_peer.clone()).await;
    connect_peer(&fixture, block_peer.clone()).await;

    for _ in 0..2 {
        advertise_tip(
            &fixture,
            status_peer.clone(),
            block::Height(0),
            block::Height(1),
            DEFAULT_HS_RANGE,
            1,
        )
        .await;
    }

    loop {
        if let HeaderSyncAction::Misbehavior { peer, reason } =
            next_non_query_action(&mut fixture.actions).await
        {
            assert_eq!(peer, status_peer);
            assert_eq!(reason, HeaderSyncMisbehavior::StatusSpam);
            break;
        }
    }

    for bytes in [
        BLOCK_MAINNET_1_BYTES.as_slice(),
        BLOCK_MAINNET_2_BYTES.as_slice(),
    ] {
        fixture
            .handle
            .send(HeaderSyncEvent::WireMessage {
                peer: block_peer.clone(),
                msg: HeaderSyncMessage::NewBlock(mainnet_block(bytes)),
            })
            .await
            .unwrap();
    }

    loop {
        if let HeaderSyncAction::Misbehavior { peer, reason } =
            next_non_query_action(&mut fixture.actions).await
        {
            assert_eq!(peer, block_peer);
            assert_eq!(reason, HeaderSyncMisbehavior::NewBlockSpam);
            break;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rapid_advancing_status_updates_are_not_spam() {
    let network = Network::Mainnet;
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let status_peer = peer(55);
    connect_peer(&fixture, status_peer.clone()).await;

    advertise_tip(
        &fixture,
        status_peer.clone(),
        block::Height(0),
        block::Height(1),
        DEFAULT_HS_RANGE,
        1,
    )
    .await;
    advertise_tip(
        &fixture,
        status_peer,
        block::Height(0),
        block::Height(2),
        DEFAULT_HS_RANGE,
        1,
    )
    .await;

    while let Ok(Some(action)) = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        fixture.actions.recv(),
    )
    .await
    {
        if let HeaderSyncAction::Misbehavior { reason, .. } = action {
            panic!("advancing status update was reported as {reason:?}");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn same_height_hash_churn_is_status_spam() {
    let network = Network::Mainnet;
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let status_peer = peer(59);
    connect_peer(&fixture, status_peer.clone()).await;

    advertise_tip_with_hash(
        &fixture,
        status_peer.clone(),
        block::Height(0),
        block::Height(1),
        block::Hash([1; 32]),
        DEFAULT_HS_RANGE,
        1,
    )
    .await;
    advertise_tip_with_hash(
        &fixture,
        status_peer.clone(),
        block::Height(0),
        block::Height(1),
        block::Hash([2; 32]),
        DEFAULT_HS_RANGE,
        1,
    )
    .await;

    loop {
        if let HeaderSyncAction::Misbehavior { peer, reason } =
            next_non_query_action(&mut fixture.actions).await
        {
            assert_eq!(peer, status_peer);
            assert_eq!(reason, HeaderSyncMisbehavior::StatusSpam);
            break;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn same_height_hash_change_with_token_is_accepted() {
    let network = Network::Mainnet;
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let status_peer = peer(60);
    connect_peer(&fixture, status_peer.clone()).await;

    advertise_tip_with_hash(
        &fixture,
        status_peer,
        block::Height(0),
        block::Height(0),
        block::Hash([3; 32]),
        DEFAULT_HS_RANGE,
        1,
    )
    .await;

    while let Ok(Some(action)) = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        fixture.actions.recv(),
    )
    .await
    {
        if let HeaderSyncAction::Misbehavior { reason, .. } = action {
            panic!("same-height status update with a token was reported as {reason:?}");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn new_block_spam_does_not_poison_seen_cache() {
    let network = Network::Mainnet;
    let first_block = mainnet_block(&BLOCK_MAINNET_1_BYTES);
    let second_block = mainnet_block(&BLOCK_MAINNET_2_BYTES);
    let second_hash = second_block.hash();
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let spam_peer = peer(56);
    let honest_peer = peer(57);
    let destination = peer(58);

    for peer_id in [spam_peer.clone(), honest_peer.clone(), destination] {
        connect_peer(&fixture, peer_id).await;
    }

    fixture
        .handle
        .send(HeaderSyncEvent::WireMessage {
            peer: spam_peer.clone(),
            msg: HeaderSyncMessage::NewBlock(first_block),
        })
        .await
        .unwrap();
    loop {
        if matches!(
            next_non_query_action(&mut fixture.actions).await,
            HeaderSyncAction::NewBlockReceived { hash, .. } if hash != second_hash
        ) {
            break;
        }
    }

    fixture
        .handle
        .send(HeaderSyncEvent::WireMessage {
            peer: spam_peer.clone(),
            msg: HeaderSyncMessage::NewBlock(second_block.clone()),
        })
        .await
        .unwrap();
    loop {
        if let HeaderSyncAction::Misbehavior { peer, reason } =
            next_non_query_action(&mut fixture.actions).await
        {
            assert_eq!(peer, spam_peer);
            assert_eq!(reason, HeaderSyncMisbehavior::NewBlockSpam);
            break;
        }
    }

    fixture
        .handle
        .send(HeaderSyncEvent::WireMessage {
            peer: honest_peer.clone(),
            msg: HeaderSyncMessage::NewBlock(second_block),
        })
        .await
        .unwrap();

    loop {
        match next_non_query_action(&mut fixture.actions).await {
            HeaderSyncAction::NewBlockReceived { peer, hash, .. } if hash == second_hash => {
                assert_eq!(peer, honest_peer);
                break;
            }
            HeaderSyncAction::Misbehavior { peer, reason } => {
                panic!("honest retry must not be deduped or scored: {peer:?} {reason:?}");
            }
            _ => {}
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejected_new_block_does_not_forward_or_poison_seen_cache() {
    let network = Network::Mainnet;
    let block = mainnet_block(&BLOCK_MAINNET_1_BYTES);
    let hash = block.hash();
    let height = block.coinbase_height().expect("test block has height");
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let source = peer(59);
    let retry_peer = peer(60);
    let destination = peer(61);

    for peer_id in [source.clone(), retry_peer.clone(), destination] {
        connect_peer(&fixture, peer_id).await;
    }

    fixture
        .handle
        .send(HeaderSyncEvent::WireMessage {
            peer: source.clone(),
            msg: HeaderSyncMessage::NewBlock(block.clone()),
        })
        .await
        .unwrap();

    loop {
        if matches!(
            next_non_query_action(&mut fixture.actions).await,
            HeaderSyncAction::NewBlockReceived { peer, hash: action_hash, .. }
                if peer == source && action_hash == hash
        ) {
            break;
        }
    }

    fixture
        .handle
        .send(HeaderSyncEvent::NewBlockRejected {
            peer: source.clone(),
            hash,
        })
        .await
        .unwrap();

    loop {
        match next_non_query_action(&mut fixture.actions).await {
            HeaderSyncAction::Misbehavior { peer, reason } => {
                assert_eq!(peer, source);
                assert_eq!(reason, HeaderSyncMisbehavior::InvalidNewBlock);
                break;
            }
            HeaderSyncAction::ForwardNewBlock { .. } => {
                panic!("rejected NewBlock must not be forwarded");
            }
            _ => {}
        }
    }

    fixture
        .handle
        .send(HeaderSyncEvent::WireMessage {
            peer: retry_peer.clone(),
            msg: HeaderSyncMessage::NewBlock(block),
        })
        .await
        .unwrap();

    loop {
        match next_non_query_action(&mut fixture.actions).await {
            HeaderSyncAction::NewBlockReceived {
                peer,
                height: action_height,
                hash: action_hash,
                ..
            } => {
                assert_eq!(peer, retry_peer);
                assert_eq!(action_height, height);
                assert_eq!(action_hash, hash);
                break;
            }
            HeaderSyncAction::Misbehavior { peer, reason } => {
                panic!("retry after rejection must not be scored: {peer:?} {reason:?}");
            }
            _ => {}
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn inbound_get_headers_requires_status_and_respects_serving_cap() {
    let network = regtest_network();
    let mut startup = startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    );
    startup.config.max_headers_per_response = 3;
    startup.config.max_inflight_requests = 2;
    let mut fixture = spawn_test_reactor(startup);
    let no_status_peer = peer(59);
    let requester = peer(60);

    connect_peer(&fixture, no_status_peer.clone()).await;
    send_get_headers(&fixture, &no_status_peer, 1, block::Height(1), 1).await;
    loop {
        if let HeaderSyncAction::Misbehavior { peer, reason } =
            next_non_query_action(&mut fixture.actions).await
        {
            assert_eq!(peer, no_status_peer);
            assert_eq!(reason, HeaderSyncMisbehavior::GetHeadersSpam);
            break;
        }
    }

    connect_peer(&fixture, requester.clone()).await;
    advertise_tip(
        &fixture,
        requester.clone(),
        block::Height(0),
        block::Height(0),
        DEFAULT_HS_RANGE,
        1,
    )
    .await;

    for (request_id, start) in [(1, block::Height(1)), (2, block::Height(4))] {
        send_get_headers(&fixture, &requester, request_id, start, 3).await;
        match next_query_headers_action(&mut fixture.actions).await {
            HeaderSyncAction::QueryHeadersByHeightRange {
                peer,
                start: action_start,
                count,
                ..
            } => {
                assert_eq!(peer, requester);
                assert_eq!(action_start, start);
                assert_eq!(count, 3);
            }
            action => panic!("unexpected action: {action:?}"),
        }
    }

    // The serving cap is now full, so a third concurrent request is spam.
    send_get_headers(&fixture, &requester, 3, block::Height(7), 1).await;
    loop {
        if let HeaderSyncAction::Misbehavior { peer, reason } =
            next_non_query_action(&mut fixture.actions).await
        {
            assert_eq!(peer, requester);
            assert_eq!(reason, HeaderSyncMisbehavior::GetHeadersSpam);
            break;
        }
    }

    // Completing the first served request frees its slot.
    fixture
        .handle
        .send(HeaderSyncEvent::HeaderRangeResponseFinished {
            peer: requester.clone(),
            session_id: 0,
            request_id: HeaderSyncRequestId::new(1).expect("non-zero id"),
            start_height: block::Height(1),
            requested_count: 1,
            returned_count: 0,
        })
        .await
        .unwrap();
    send_get_headers(&fixture, &requester, 4, block::Height(8), 1).await;
    match next_query_headers_action(&mut fixture.actions).await {
        HeaderSyncAction::QueryHeadersByHeightRange {
            peer, start, count, ..
        } => {
            assert_eq!(peer, requester);
            assert_eq!(start, block::Height(8));
            assert_eq!(count, 1);
        }
        action => panic!("unexpected action: {action:?}"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn two_peers_serving_the_same_range_complete_independently() {
    let network = regtest_network();
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let first_peer = peer(242);
    let second_peer = peer(243);
    let request_id = HeaderSyncRequestId::new(1).expect("non-zero id");
    let (first_send, mut first_recv) = crate::zakura::framed_channel(8);
    let (second_send, mut second_recv) = crate::zakura::framed_channel(8);

    for (peer_id, send) in [
        (first_peer.clone(), first_send),
        (second_peer.clone(), second_send),
    ] {
        let session = HeaderSyncPeerSession::from_parts_with_direction(
            peer_id.clone(),
            ServicePeerDirection::Inbound,
            send,
            CancellationToken::new(),
        );
        fixture
            .handle
            .send(HeaderSyncEvent::PeerConnected(session))
            .await
            .unwrap();
        advertise_tip(
            &fixture,
            peer_id,
            block::Height(0),
            block::Height(0),
            DEFAULT_HS_RANGE,
            1,
        )
        .await;
    }
    for recv in [&mut first_recv, &mut second_recv] {
        tokio::time::timeout(std::time::Duration::from_secs(1), recv.recv())
            .await
            .expect("initial status arrives")
            .expect("v7 stream stays open");
    }

    for peer_id in [&first_peer, &second_peer] {
        fixture
            .handle
            .send(HeaderSyncEvent::WireGetHeaders {
                peer: peer_id.clone(),
                session_id: 0,
                request_id,
                start_height: block::Height(1),
                count: 2,
                want_tree_aux_roots: true,
            })
            .await
            .unwrap();
    }

    let mut queried = std::collections::HashSet::new();
    while queried.len() < 2 {
        match next_query_headers_action(&mut fixture.actions).await {
            HeaderSyncAction::QueryHeadersByHeightRange {
                peer,
                request_id: action_request_id,
                start,
                count,
                want_tree_aux_roots,
                ..
            } => {
                assert_eq!(action_request_id, request_id);
                assert_eq!((start, count), (block::Height(1), 2));
                assert!(want_tree_aux_roots);
                queried.insert(peer);
            }
            action => panic!("unexpected action: {action:?}"),
        }
    }
    assert_eq!(
        queried,
        std::collections::HashSet::from([first_peer.clone(), second_peer.clone()])
    );

    fixture
        .handle
        .send(HeaderSyncEvent::HeaderRangeResponseReady {
            peer: first_peer.clone(),
            session_id: 0,
            request_id,
            start_height: block::Height(1),
            requested_count: 2,
            want_tree_aux_roots: true,
            headers: Vec::new(),
            body_sizes: Vec::new(),
            tree_aux_roots: Vec::new(),
        })
        .await
        .unwrap();
    let first_frame = tokio::time::timeout(std::time::Duration::from_secs(1), first_recv.recv())
        .await
        .expect("first response arrives")
        .expect("first stream stays open");
    assert_eq!(
        HeaderSyncMessage::peek_headers_request_id(&first_frame.payload).unwrap(),
        request_id
    );
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), second_recv.recv())
            .await
            .is_err(),
        "first peer completion must not settle the second peer request"
    );

    fixture
        .handle
        .send(HeaderSyncEvent::HeaderRangeResponseReady {
            peer: second_peer,
            session_id: 0,
            request_id,
            start_height: block::Height(1),
            requested_count: 2,
            want_tree_aux_roots: true,
            headers: Vec::new(),
            body_sizes: Vec::new(),
            tree_aux_roots: Vec::new(),
        })
        .await
        .unwrap();
    let second_frame = tokio::time::timeout(std::time::Duration::from_secs(1), second_recv.recv())
        .await
        .expect("second response arrives")
        .expect("second stream stays open");
    assert_eq!(
        HeaderSyncMessage::peek_headers_request_id(&second_frame.payload).unwrap(),
        request_id
    );
}

#[tokio::test(flavor = "current_thread")]
async fn serving_responses_echo_request_ids_in_completion_order() {
    let network = regtest_network();
    let mut startup = startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    );
    startup.config.max_inflight_requests = 2;
    let mut fixture = spawn_test_reactor(startup);
    let peer_id = peer(79);
    let first_id = HeaderSyncRequestId::new(1).expect("non-zero id");
    let second_id = HeaderSyncRequestId::new(2).expect("non-zero id");
    let (send, mut recv) = crate::zakura::framed_channel(8);
    let session = HeaderSyncPeerSession::from_parts_with_direction(
        peer_id.clone(),
        ServicePeerDirection::Inbound,
        send,
        CancellationToken::new(),
    );
    fixture
        .handle
        .send(HeaderSyncEvent::PeerConnected(session))
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), recv.recv())
        .await
        .expect("initial status arrives")
        .expect("v7 stream stays open");
    advertise_tip(
        &fixture,
        peer_id.clone(),
        block::Height(0),
        block::Height(0),
        DEFAULT_HS_RANGE,
        2,
    )
    .await;

    for (request_id, start_height) in [(first_id, block::Height(1)), (second_id, block::Height(2))]
    {
        fixture
            .handle
            .send(HeaderSyncEvent::WireGetHeaders {
                peer: peer_id.clone(),
                session_id: 0,
                request_id,
                start_height,
                count: 1,
                want_tree_aux_roots: false,
            })
            .await
            .unwrap();
        match next_query_headers_action(&mut fixture.actions).await {
            HeaderSyncAction::QueryHeadersByHeightRange {
                request_id: action_id,
                start,
                ..
            } => {
                assert_eq!(action_id, request_id);
                assert_eq!(start, start_height);
            }
            action => panic!("unexpected action: {action:?}"),
        }
    }

    for (request_id, start_height) in [(second_id, block::Height(2)), (first_id, block::Height(1))]
    {
        fixture
            .handle
            .send(HeaderSyncEvent::HeaderRangeResponseReady {
                peer: peer_id.clone(),
                session_id: 0,
                request_id,
                start_height,
                requested_count: 1,
                want_tree_aux_roots: false,
                headers: Vec::new(),
                body_sizes: Vec::new(),
                tree_aux_roots: Vec::new(),
            })
            .await
            .unwrap();
        let frame = tokio::time::timeout(std::time::Duration::from_secs(1), recv.recv())
            .await
            .expect("headers response arrives")
            .expect("v7 stream stays open");
        assert_eq!(
            HeaderSyncMessage::peek_headers_request_id(&frame.payload).unwrap(),
            request_id
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn replacement_session_ignores_old_wire_response_with_reused_id() {
    let checkpoint_hash = block::Hash::from(mainnet_header(&BLOCK_MAINNET_3_BYTES).as_ref());
    let (network, _) = checkpoint_testnet_with_hash(block::Height(3), checkpoint_hash);
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        Some((block::Height(3), checkpoint_hash)),
    ));
    let peer_id = peer(80);
    let request_id = HeaderSyncRequestId::new(1).expect("non-zero id");
    let (old_send, _old_recv) = crate::zakura::framed_channel(8);
    let old_session = HeaderSyncPeerSession::from_parts_with_direction_and_session_id(
        peer_id.clone(),
        ServicePeerDirection::Inbound,
        old_send,
        CancellationToken::new(),
        1,
    );
    fixture
        .handle
        .send(HeaderSyncEvent::PeerConnected(old_session))
        .await
        .unwrap();
    fixture
        .handle
        .send(HeaderSyncEvent::SessionWireMessage {
            peer: peer_id.clone(),
            session_id: 1,
            msg: HeaderSyncMessage::Status(HeaderSyncStatus {
                tip_height: block::Height(4),
                tip_hash: block::Hash([4; 32]),
                anchor_height: block::Height(0),
                max_headers_per_response: 1,
                max_inflight_requests: 1,
            }),
        })
        .await
        .unwrap();
    let (requested_peer, _request_id, start_height, count) =
        next_outbound_get_headers(&mut fixture.actions).await;
    assert_eq!(
        (requested_peer, start_height, count),
        (peer_id.clone(), block::Height(4), 1)
    );

    let (new_send, _new_recv) = crate::zakura::framed_channel(8);
    let new_session = HeaderSyncPeerSession::from_parts_with_direction_and_session_id(
        peer_id.clone(),
        ServicePeerDirection::Inbound,
        new_send,
        CancellationToken::new(),
        2,
    );
    fixture
        .handle
        .send(HeaderSyncEvent::PeerConnected(new_session))
        .await
        .unwrap();
    fixture
        .handle
        .send(HeaderSyncEvent::SessionWireMessage {
            peer: peer_id.clone(),
            session_id: 2,
            msg: HeaderSyncMessage::Status(HeaderSyncStatus {
                tip_height: block::Height(4),
                tip_hash: block::Hash([4; 32]),
                anchor_height: block::Height(0),
                max_headers_per_response: 1,
                max_inflight_requests: 1,
            }),
        })
        .await
        .unwrap();
    let (requested_peer, _request_id, start_height, count) =
        next_outbound_get_headers(&mut fixture.actions).await;
    assert_eq!(
        (requested_peer, start_height, count),
        (peer_id.clone(), block::Height(4), 1)
    );

    let headers = vec![mainnet_header(&BLOCK_MAINNET_4_BYTES)];
    fixture
        .handle
        .send(HeaderSyncEvent::WireHeaders {
            wire_request: HeaderSyncWireRequestIdentity {
                peer: peer_id.clone(),
                session_id: 1,
                request_id,
            },
            entries: HeaderRangeEntry::from_parallel(
                block::Height(4),
                headers.clone(),
                vec![0],
                roots_from_height(block::Height(4), 1),
            )
            .expect("test response vectors align"),
        })
        .await
        .unwrap();
    fixture
        .handle
        .send(HeaderSyncEvent::WireHeaders {
            wire_request: HeaderSyncWireRequestIdentity {
                peer: peer_id.clone(),
                session_id: 2,
                request_id,
            },
            entries: HeaderRangeEntry::from_parallel(
                block::Height(4),
                headers,
                vec![0],
                roots_from_height(block::Height(4), 1),
            )
            .expect("test response vectors align"),
        })
        .await
        .unwrap();

    loop {
        match next_non_query_action(&mut fixture.actions).await {
            HeaderSyncAction::CommitHeaderRange {
                operation, payload, ..
            } => {
                assert_eq!(operation.wire_request.peer, peer_id);
                assert_eq!(operation.wire_request.session_id, 2);
                assert_eq!(payload.range().start(), block::Height(4));
                break;
            }
            HeaderSyncAction::Misbehavior { peer, reason } => {
                panic!("stale response must not affect replacement {peer:?}: {reason:?}")
            }
            _ => {}
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn replacement_session_ignores_old_state_completion_with_reused_id() {
    let network = regtest_network();
    let mut startup = startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    );
    startup.config.max_inflight_requests = 2;
    let mut fixture = spawn_test_reactor(startup);
    let peer_id = peer(81);
    let request_id = HeaderSyncRequestId::new(1).expect("non-zero id");
    let (old_send, _old_recv) = crate::zakura::framed_channel(8);
    let old_session = HeaderSyncPeerSession::from_parts_with_direction_and_session_id(
        peer_id.clone(),
        ServicePeerDirection::Inbound,
        old_send,
        CancellationToken::new(),
        1,
    );
    fixture
        .handle
        .send(HeaderSyncEvent::PeerConnected(old_session))
        .await
        .unwrap();
    fixture
        .handle
        .send(HeaderSyncEvent::SessionWireMessage {
            peer: peer_id.clone(),
            session_id: 1,
            msg: HeaderSyncMessage::Status(HeaderSyncStatus::default()),
        })
        .await
        .unwrap();
    fixture
        .handle
        .send(HeaderSyncEvent::WireGetHeaders {
            peer: peer_id.clone(),
            session_id: 1,
            request_id,
            start_height: block::Height(1),
            count: 1,
            want_tree_aux_roots: false,
        })
        .await
        .unwrap();
    assert!(matches!(
        next_query_headers_action(&mut fixture.actions).await,
        HeaderSyncAction::QueryHeadersByHeightRange { session_id: 1, .. }
    ));

    let (new_send, mut new_recv) = crate::zakura::framed_channel(8);
    let new_session = HeaderSyncPeerSession::from_parts_with_direction_and_session_id(
        peer_id.clone(),
        ServicePeerDirection::Inbound,
        new_send,
        CancellationToken::new(),
        2,
    );
    fixture
        .handle
        .send(HeaderSyncEvent::PeerConnected(new_session))
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), new_recv.recv())
        .await
        .expect("replacement status arrives")
        .expect("replacement stream stays open");
    fixture
        .handle
        .send(HeaderSyncEvent::HeaderRangeResponseReady {
            peer: peer_id.clone(),
            session_id: 1,
            request_id,
            start_height: block::Height(1),
            requested_count: 1,
            want_tree_aux_roots: false,
            headers: Vec::new(),
            body_sizes: Vec::new(),
            tree_aux_roots: Vec::new(),
        })
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), new_recv.recv())
            .await
            .is_err(),
        "old state completion must not send through replacement session"
    );

    fixture
        .handle
        .send(HeaderSyncEvent::SessionWireMessage {
            peer: peer_id.clone(),
            session_id: 2,
            msg: HeaderSyncMessage::Status(HeaderSyncStatus::default()),
        })
        .await
        .unwrap();
    fixture
        .handle
        .send(HeaderSyncEvent::WireGetHeaders {
            peer: peer_id.clone(),
            session_id: 2,
            request_id,
            start_height: block::Height(1),
            count: 1,
            want_tree_aux_roots: false,
        })
        .await
        .unwrap();
    assert!(matches!(
        next_query_headers_action(&mut fixture.actions).await,
        HeaderSyncAction::QueryHeadersByHeightRange { session_id: 2, .. }
    ));
    fixture
        .handle
        .send(HeaderSyncEvent::HeaderRangeResponseReady {
            peer: peer_id,
            session_id: 2,
            request_id,
            start_height: block::Height(1),
            requested_count: 1,
            want_tree_aux_roots: false,
            headers: Vec::new(),
            body_sizes: Vec::new(),
            tree_aux_roots: Vec::new(),
        })
        .await
        .unwrap();
    let frame = tokio::time::timeout(std::time::Duration::from_secs(1), new_recv.recv())
        .await
        .expect("current response arrives")
        .expect("replacement stream stays open");
    assert_eq!(
        HeaderSyncMessage::peek_headers_request_id(&frame.payload).unwrap(),
        request_id
    );
}

#[tokio::test(flavor = "current_thread")]
async fn inbound_get_headers_over_cap_disconnects_without_state_read() {
    let network = regtest_network();
    let mut startup = startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    );
    startup.config.max_headers_per_response = 3;
    let mut fixture = spawn_test_reactor(startup);
    let requester = peer(61);

    connect_peer(&fixture, requester.clone()).await;
    advertise_tip(
        &fixture,
        requester.clone(),
        block::Height(0),
        block::Height(0),
        DEFAULT_HS_RANGE,
        1,
    )
    .await;

    send_get_headers(&fixture, &requester, 1, block::Height(1), 4).await;

    loop {
        match next_action(&mut fixture.actions).await {
            HeaderSyncAction::QueryHeadersByHeightRange { .. } => {
                panic!("over-cap GetHeaders must not query state");
            }
            HeaderSyncAction::Misbehavior { peer, reason } => {
                assert_eq!(peer, requester);
                assert_eq!(reason, HeaderSyncMisbehavior::GetHeadersTooLong);
                break;
            }
            _ => {}
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn rejected_non_linking_range_traces_link_stage_and_error_kind() {
    let network = regtest_network();
    let anchor = (block::Height(0), network.genesis_hash());
    let mut capture =
        TraceCapture::for_test("rejected_non_linking_range_traces_link_stage_and_error_kind")
            .unwrap();
    let mut startup = startup_for(network, anchor, Some(anchor));
    startup.trace = ZakuraTrace::new(capture.tracer(), "01");
    let mut fixture = spawn_test_reactor(startup);
    let peer_id = peer(64);

    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(
        &fixture,
        peer_id.clone(),
        anchor.0,
        block::Height(1),
        DEFAULT_HS_RANGE,
        1,
    )
    .await;
    let (served_peer, request_id, start_height, count) =
        next_outbound_get_headers(&mut fixture.actions).await;
    assert_eq!(served_peer, peer_id);
    assert_eq!(start_height, block::Height(1));
    assert_eq!(count, 1);

    send_headers(
        &fixture,
        &peer_id,
        request_id,
        headers_message_from(
            block::Height(1),
            vec![mainnet_header(&BLOCK_MAINNET_2_BYTES)],
        ),
    )
    .await;

    match next_non_query_action(&mut fixture.actions).await {
        HeaderSyncAction::Misbehavior { peer, reason } => {
            assert_eq!(peer, peer_id);
            assert_eq!(reason, HeaderSyncMisbehavior::InvalidRange);
        }
        action => panic!("unexpected action: {action:?}"),
    }

    capture.flush().await;
    let reader = capture.reader().unwrap();
    let header_sync = reader.table(HEADER_SYNC_TABLE.table());
    let anchor_hash = format!("{}", anchor.1);
    header_sync.assert_row(
        hs_trace::HEADER_RANGE_REJECTED,
        &[
            (hs_trace::RANGE_START, TraceValue::U64(1)),
            (hs_trace::RANGE_COUNT, TraceValue::U64(1)),
            (hs_trace::ANCHOR_HASH, TraceValue::Str(&anchor_hash)),
            (hs_trace::VALIDATION_STAGE, TraceValue::Str("link")),
            (
                hs_trace::ERROR_KIND,
                TraceValue::Str("first_header_does_not_link"),
            ),
        ],
    );

    let _ = capture.finish().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn tree_aux_height_mismatch_traces_structured_diagnostics() {
    let network = regtest_network();
    let anchor = (block::Height(0), network.genesis_hash());
    let mut capture =
        TraceCapture::for_test("tree_aux_height_mismatch_traces_structured_diagnostics").unwrap();
    let mut startup = startup_for(network, anchor, Some(anchor));
    startup.trace = ZakuraTrace::new(capture.tracer(), "01");
    let mut fixture = spawn_test_reactor(startup);
    let peer_id = peer(65);

    fixture
        .handle
        .send(HeaderSyncEvent::WireProtocolFailure {
            peer: peer_id.clone(),
            reason: HeaderSyncMisbehavior::MalformedMessage,
            error: Arc::new(HeaderSyncWireError::TreeAuxRootHeightMismatch {
                offset: 7,
                expected_height: block::Height(108),
                root_height: block::Height(208),
                first_root_height: block::Height(201),
                last_root_height: block::Height(300),
            }),
        })
        .await
        .unwrap();

    match next_non_query_action(&mut fixture.actions).await {
        HeaderSyncAction::Misbehavior { peer, reason } => {
            assert_eq!(peer, peer_id);
            assert_eq!(reason, HeaderSyncMisbehavior::MalformedMessage);
        }
        action => panic!("unexpected action: {action:?}"),
    }

    capture.flush().await;
    let reader = capture.reader().unwrap();
    let header_sync = reader.table(HEADER_SYNC_TABLE.table());
    header_sync.assert_row(
        hs_trace::HEADER_EVENT_RECEIVED,
        &[
            (
                hs_trace::ERROR_KIND,
                TraceValue::Str("tree_aux_root_height_mismatch"),
            ),
            (hs_trace::ROOT_MISMATCH_OFFSET, TraceValue::U64(7)),
            (hs_trace::EXPECTED_ROOT_HEIGHT, TraceValue::U64(108)),
            (hs_trace::ACTUAL_ROOT_HEIGHT, TraceValue::U64(208)),
            (hs_trace::FIRST_ROOT_HEIGHT, TraceValue::U64(201)),
            (hs_trace::LAST_ROOT_HEIGHT, TraceValue::U64(300)),
        ],
    );
    header_sync.assert_sequence(&[
        hs_trace::HEADER_EVENT_RECEIVED,
        hs_trace::HEADER_PEER_VIOLATION,
        hs_trace::HEADER_PEER_VIOLATION,
        hs_trace::HEADER_PEER_VIOLATION_RECORDED,
        hs_trace::HEADER_ACTION_DISPATCHED,
    ]);

    let _ = capture.finish().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn header_sync_jsonl_trace_captures_status_range_dedup_and_violation_record() {
    let network = Network::Mainnet;
    let mut capture = TraceCapture::for_test(
        "header_sync_jsonl_trace_captures_status_range_dedup_and_violation_record",
    )
    .unwrap();
    let first_checkpoint = network
        .checkpoint_list()
        .min_height_in_range(block::Height(1)..)
        .expect("mainnet has a checkpoint above genesis");
    let checkpoint_hash = network
        .checkpoint_list()
        .hash(first_checkpoint)
        .expect("checkpoint height has a hash");
    let mut startup = startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        Some((first_checkpoint, checkpoint_hash)),
    );
    startup.trace = ZakuraTrace::new(capture.tracer(), "01");
    let mut fixture = spawn_test_reactor(startup);
    let peer_id = peer(55);

    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(
        &fixture,
        peer_id.clone(),
        block::Height(0),
        next_height(first_checkpoint).expect("checkpoint has a successor"),
        DEFAULT_HS_RANGE,
        1,
    )
    .await;
    let _ = next_get_headers_request_id(&mut fixture.actions).await;
    fixture
        .handle
        .send(HeaderSyncEvent::FullBlockCommitted {
            height: block::Height(1),
            hash: mainnet_block(&BLOCK_MAINNET_1_BYTES).hash(),
        })
        .await
        .unwrap();
    fixture
        .handle
        .send(HeaderSyncEvent::WireMessage {
            peer: peer_id.clone(),
            msg: HeaderSyncMessage::NewBlock(mainnet_block(&BLOCK_MAINNET_1_BYTES)),
        })
        .await
        .unwrap();
    fixture
        .handle
        .send(HeaderSyncEvent::WireDecodeFailed {
            peer: peer_id.clone(),
            error: Arc::new(HeaderSyncWireError::UnknownMessageType(99)),
        })
        .await
        .unwrap();
    fixture
        .handle
        .send(HeaderSyncEvent::WireProtocolFailure {
            peer: peer_id.clone(),
            reason: HeaderSyncMisbehavior::MalformedMessage,
            error: Arc::new(HeaderSyncWireError::TrailingBytes),
        })
        .await
        .unwrap();
    fixture
        .handle
        .send(HeaderSyncEvent::PeerDisconnected(peer_id))
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    capture.flush().await;
    let reader = capture.reader().unwrap();
    let header_sync = reader.table(HEADER_SYNC_TABLE.table());

    assert!(header_sync.count(hs_trace::HEADER_STATUS_SENT) >= 1);
    assert!(header_sync.count(hs_trace::HEADER_STATUS_RECEIVED) >= 1);
    assert!(header_sync.count(hs_trace::HEADER_GET_HEADERS_SENT) >= 1);
    assert!(header_sync.count(hs_trace::HEADER_NEW_BLOCK_DEDUPED) >= 1);
    assert!(header_sync.count(hs_trace::HEADER_PEER_VIOLATION_RECORDED) >= 1);
    header_sync.assert_row(
        hs_trace::HEADER_PEER_CONNECTED,
        &[(hs_trace::ACTIVE_CONNECTIONS, TraceValue::U64(1))],
    );
    header_sync.assert_row(
        hs_trace::HEADER_PEER_DISCONNECTED,
        &[(hs_trace::ACTIVE_CONNECTIONS, TraceValue::U64(0))],
    );
    header_sync.assert_row(
        hs_trace::HEADER_EVENT_RECEIVED,
        &[
            (hs_trace::KIND, TraceValue::Str("wire_decode_failed")),
            (
                hs_trace::ERROR_KIND,
                TraceValue::Str("unknown_message_type"),
            ),
        ],
    );
    header_sync.assert_row(
        hs_trace::HEADER_EVENT_RECEIVED,
        &[
            (hs_trace::KIND, TraceValue::Str("wire_protocol_failure")),
            (hs_trace::REASON, TraceValue::Str("malformed_message")),
            (hs_trace::ERROR_KIND, TraceValue::Str("trailing_bytes")),
        ],
    );

    for row in header_sync.rows() {
        assert!(
            row.get("block").is_none() && row.get("headers").is_none(),
            "header-sync trace rows must not contain full payloads: {row:?}"
        );
    }

    let _ = capture.finish().await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn header_sync_metrics_record_status_range_new_block_dedup_and_violation() {
    let metrics = [
        "sync.header.peer.status.sent",
        "sync.header.peer.status.received",
        "sync.header.request.sent",
        "sync.header.response.received",
        "sync.header.range.committed",
        "sync.header.tip.new_block.received",
        "sync.header.tip.new_block.deduped",
        "sync.header.peer.violation",
    ];
    let before = metric_snapshot(&metrics);

    let first_checkpoint = block::Height(3);
    let checkpoint_hash = block::Hash::from(mainnet_header(&BLOCK_MAINNET_3_BYTES).as_ref());
    let (network, _) = checkpoint_testnet_with_hash(first_checkpoint, checkpoint_hash);
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        Some((first_checkpoint, checkpoint_hash)),
    ));
    let peer_id = peer(56);

    connect_peer(&fixture, peer_id.clone()).await;
    match next_non_query_action(&mut fixture.actions).await {
        HeaderSyncAction::SendMessage {
            msg: HeaderSyncMessage::Status(_),
            ..
        } => {}
        action => panic!("unexpected action: {action:?}"),
    }

    advertise_tip(
        &fixture,
        peer_id.clone(),
        block::Height(0),
        block::Height(4),
        DEFAULT_HS_RANGE,
        1,
    )
    .await;
    let request_id = next_get_headers_request_id(&mut fixture.actions).await;

    send_headers(
        &fixture,
        &peer_id,
        request_id,
        headers_message(vec![mainnet_header(&BLOCK_MAINNET_4_BYTES)]),
    )
    .await;
    let (operation, committed_hash) = match next_non_query_action(&mut fixture.actions).await {
        HeaderSyncAction::CommitHeaderRange {
            operation, payload, ..
        } => {
            assert_eq!(
                payload.range().start(),
                next_height(first_checkpoint).expect("checkpoint has a successor")
            );
            (
                operation,
                block::Hash::from(payload.headers().last().expect("one header").as_ref()),
            )
        }
        action => panic!("unexpected action: {action:?}"),
    };

    fixture
        .handle
        .send(HeaderSyncEvent::HeaderRangeOperationCompleted {
            operation,
            tip_hash: committed_hash,
        })
        .await
        .unwrap();
    fixture
        .handle
        .send(HeaderSyncEvent::FullBlockCommitted {
            height: block::Height(1),
            hash: mainnet_block(&BLOCK_MAINNET_1_BYTES).hash(),
        })
        .await
        .unwrap();
    fixture
        .handle
        .send(HeaderSyncEvent::WireMessage {
            peer: peer_id.clone(),
            msg: HeaderSyncMessage::NewBlock(mainnet_block(&BLOCK_MAINNET_1_BYTES)),
        })
        .await
        .unwrap();
    fixture
        .handle
        .send(HeaderSyncEvent::WireDecodeFailed {
            peer: peer_id,
            error: Arc::new(HeaderSyncWireError::UnknownMessageType(99)),
        })
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    for metric in metrics {
        assert_metric_incremented(&before, metric);
    }
}

#[tokio::test(flavor = "current_thread")]
/// An empty `Headers` response to an outstanding range is a legitimate "I have
/// nothing" answer, so the range is retried rather than the peer disconnected.
///
/// Unsolicited `Headers` are rejected in the peer-owned pipe before they can reach
/// the reactor at all (see `pipe::tests::deliver_unsolicited_headers_rejects_without_expectation`),
/// so there is no reactor-level unsolicited case to exercise here.
async fn empty_headers_for_outstanding_range_retries_without_disconnect() {
    let network = regtest_network();
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let peer_id = peer(9);

    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(
        &fixture,
        peer_id.clone(),
        block::Height(0),
        block::Height(1),
        1,
        1,
    )
    .await;
    let request_id = next_get_headers_request_id(&mut fixture.actions).await;
    send_headers(&fixture, &peer_id, request_id, headers_message(Vec::new())).await;
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(200), async {
            loop {
                if matches!(
                    next_non_query_action(&mut fixture.actions).await,
                    HeaderSyncAction::SendMessage {
                        msg: HeaderSyncMessage::GetHeaders { .. },
                        ..
                    }
                ) {
                    break;
                }
            }
        })
        .await
        .is_err(),
        "empty responses use the one-second retry delay rather than the 50ms peer-avoidance delay"
    );
    // Periodic keepalive Status sends may interleave with the retry; the
    // property under test is that the range retries without a disconnect.
    loop {
        match next_non_query_action(&mut fixture.actions).await {
            HeaderSyncAction::SendMessage {
                msg: HeaderSyncMessage::GetHeaders { .. },
                ..
            } => break,
            HeaderSyncAction::SendMessage {
                msg: HeaderSyncMessage::Status(_),
                ..
            } => continue,
            action => panic!(
                "empty Headers for an outstanding range should retry without \
                 disconnecting, got: {action:?}"
            ),
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn one_silent_peer_retries_the_same_missing_range_indefinitely() {
    let network = regtest_network();
    let mut fixture = spawn_test_reactor(startup_with_timeout(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        std::time::Duration::from_millis(5),
    ));
    let peer_id = peer(90);

    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(
        &fixture,
        peer_id.clone(),
        block::Height(0),
        block::Height(1),
        1,
        1,
    )
    .await;

    for _ in 0..3 {
        let (request_peer, _request_id, start, count) =
            next_outbound_get_headers(&mut fixture.actions).await;
        assert_eq!(request_peer, peer_id);
        assert_eq!(start, block::Height(1));
        assert_eq!(count, 1);
        // Silence: let the request deadline expire. The work queue must return
        // the claim and make it eligible again after the short peer-local delay.
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn commit_failure_after_source_disconnect_retries_without_blocking_the_lane() {
    let checkpoint_hash = block::Hash::from(mainnet_header(&BLOCK_MAINNET_3_BYTES).as_ref());
    let (network, _) = checkpoint_testnet_with_hash(block::Height(3), checkpoint_hash);
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        Some((block::Height(3), checkpoint_hash)),
    ));
    let first_peer = peer(140);
    let second_peer = peer(141);

    for peer_id in [&first_peer, &second_peer] {
        connect_peer(&fixture, peer_id.clone()).await;
        advertise_tip(
            &fixture,
            peer_id.clone(),
            block::Height(0),
            block::Height(4),
            1,
            1,
        )
        .await;
    }
    let (source, request_id, start, count) = next_outbound_get_headers(&mut fixture.actions).await;
    assert_eq!(source, first_peer);
    assert_eq!((start, count), (block::Height(4), 1));
    send_headers(
        &fixture,
        &source,
        request_id,
        headers_message(vec![mainnet_header(&BLOCK_MAINNET_4_BYTES)]),
    )
    .await;
    let operation = loop {
        if let HeaderSyncAction::CommitHeaderRange { operation, .. } =
            next_non_query_action(&mut fixture.actions).await
        {
            break operation;
        }
    };

    fixture
        .handle
        .send(HeaderSyncEvent::PeerDisconnected(first_peer.clone()))
        .await
        .unwrap();
    fixture
        .handle
        .send(HeaderSyncEvent::HeaderRangeOperationFailed {
            operation,
            kind: HeaderSyncCommitFailureKind::Local,
        })
        .await
        .unwrap();

    let (retry_peer, _, retry_start, retry_count) =
        next_outbound_get_headers(&mut fixture.actions).await;
    assert_eq!(retry_peer, second_peer);
    assert_eq!((retry_start, retry_count), (start, count));
}

#[tokio::test(flavor = "current_thread")]
async fn partial_full_block_coverage_retires_old_request_and_requests_suffix() {
    let network = regtest_network();
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let peer_id = peer(142);
    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(
        &fixture,
        peer_id.clone(),
        block::Height(0),
        block::Height(2),
        2,
        1,
    )
    .await;
    let (_, old_request_id, start, count) = next_outbound_get_headers(&mut fixture.actions).await;
    assert_eq!((start, count), (block::Height(1), 2));

    fixture
        .handle
        .send(HeaderSyncEvent::FullBlockCommitted {
            height: block::Height(1),
            hash: mainnet_block(&BLOCK_MAINNET_1_BYTES).hash(),
        })
        .await
        .unwrap();

    let (_, _, retry_start, retry_count) = next_outbound_get_headers(&mut fixture.actions).await;
    assert_eq!((retry_start, retry_count), (block::Height(2), 1));
    send_headers(
        &fixture,
        &peer_id,
        old_request_id,
        headers_message_from(
            block::Height(1),
            vec![
                mainnet_header(&BLOCK_MAINNET_1_BYTES),
                mainnet_header(&BLOCK_MAINNET_2_BYTES),
            ],
        ),
    )
    .await;
    assert_no_commit_or_misbehavior(&mut fixture.actions).await;
}

#[tokio::test(flavor = "current_thread")]
async fn partial_coverage_recreates_an_interior_hole_before_a_later_batch() {
    let network = regtest_network();
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let peer_id = peer(202);
    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(&fixture, peer_id, block::Height(0), block::Height(4), 2, 2).await;

    let mut requests = Vec::new();
    while requests.len() < 2 {
        let (_, _, start, count) = next_outbound_get_headers(&mut fixture.actions).await;
        requests.push((start, count));
    }
    requests.sort_unstable();
    assert_eq!(requests, vec![(block::Height(1), 2), (block::Height(3), 2)]);

    fixture
        .handle
        .send(HeaderSyncEvent::FullBlockCommitted {
            height: block::Height(1),
            hash: mainnet_block(&BLOCK_MAINNET_1_BYTES).hash(),
        })
        .await
        .unwrap();

    let (_, _, retry_start, retry_count) = next_outbound_get_headers(&mut fixture.actions).await;
    assert_eq!((retry_start, retry_count), (block::Height(2), 1));
}

#[tokio::test(flavor = "current_thread")]
async fn partial_coverage_trims_and_commits_an_already_buffered_suffix() {
    let network = regtest_network();
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let peer_id = peer(203);
    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(
        &fixture,
        peer_id.clone(),
        block::Height(0),
        block::Height(4),
        2,
        2,
    )
    .await;

    let mut requests = HashMap::new();
    while requests.len() < 2 {
        let (_, request_id, start, count) = next_outbound_get_headers(&mut fixture.actions).await;
        assert_eq!(count, 2);
        requests.insert(start, request_id);
    }
    send_headers(
        &fixture,
        &peer_id,
        requests[&block::Height(3)],
        headers_message_with_sizes(
            vec![
                mainnet_header(&BLOCK_MAINNET_3_BYTES),
                mainnet_header(&BLOCK_MAINNET_4_BYTES),
            ],
            vec![33, 44],
        ),
    )
    .await;
    fixture
        .handle
        .send(HeaderSyncEvent::FullBlockCommitted {
            height: block::Height(3),
            hash: mainnet_block(&BLOCK_MAINNET_3_BYTES).hash(),
        })
        .await
        .unwrap();

    loop {
        if let HeaderSyncAction::CommitHeaderRange { payload, .. } =
            next_non_query_action(&mut fixture.actions).await
        {
            assert_eq!(payload.range().start(), block::Height(4));
            assert_eq!(
                payload.headers().cloned().collect::<Vec<_>>(),
                [mainnet_header(&BLOCK_MAINNET_4_BYTES)]
            );
            assert_eq!(payload.body_sizes().collect::<Vec<_>>(), [44]);
            assert_eq!(
                payload
                    .tree_aux_roots()
                    .and_then(|mut roots| roots.next().map(|root| root.height)),
                Some(block::Height(4))
            );
            break;
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn loaded_best_tip_reconciles_outstanding_and_buffered_work() {
    let network = regtest_network();
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let peer_id = peer(214);
    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(
        &fixture,
        peer_id.clone(),
        block::Height(0),
        block::Height(4),
        2,
        2,
    )
    .await;

    let mut requests = HashMap::new();
    while requests.len() < 2 {
        let (_, request_id, start, count) = next_outbound_get_headers(&mut fixture.actions).await;
        assert_eq!(count, 2);
        requests.insert(start, request_id);
    }
    let covered_request_id = requests[&block::Height(1)];
    send_headers(
        &fixture,
        &peer_id,
        requests[&block::Height(3)],
        headers_message_from(
            block::Height(3),
            vec![
                mainnet_header(&BLOCK_MAINNET_3_BYTES),
                mainnet_header(&BLOCK_MAINNET_4_BYTES),
            ],
        ),
    )
    .await;

    fixture
        .handle
        .send(HeaderSyncEvent::BestHeaderTipLoaded {
            tip_height: block::Height(3),
            tip_hash: mainnet_block(&BLOCK_MAINNET_3_BYTES).hash(),
        })
        .await
        .unwrap();

    loop {
        if let HeaderSyncAction::CommitHeaderRange { payload, .. } =
            next_non_query_action(&mut fixture.actions).await
        {
            assert_eq!(payload.range().start(), block::Height(4));
            assert_eq!(
                payload.headers().cloned().collect::<Vec<_>>(),
                [mainnet_header(&BLOCK_MAINNET_4_BYTES)]
            );
            break;
        }
    }

    send_headers(
        &fixture,
        &peer_id,
        covered_request_id,
        headers_message_from(
            block::Height(1),
            vec![
                mainnet_header(&BLOCK_MAINNET_1_BYTES),
                mainnet_header(&BLOCK_MAINNET_2_BYTES),
            ],
        ),
    )
    .await;
    assert_no_commit_or_misbehavior(&mut fixture.actions).await;
}

#[tokio::test(flavor = "current_thread")]
async fn partially_covered_failed_commit_requeues_its_uncovered_suffix() {
    let headers = [
        mainnet_header(&BLOCK_MAINNET_1_BYTES),
        mainnet_header(&BLOCK_MAINNET_2_BYTES),
        mainnet_header(&BLOCK_MAINNET_3_BYTES),
    ];
    let checkpoint_hash = block::Hash::from(headers[2].as_ref());
    let (network, _) = checkpoint_testnet_with_hash(block::Height(3), checkpoint_hash);
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let peer_id = peer(204);
    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(
        &fixture,
        peer_id.clone(),
        block::Height(0),
        block::Height(3),
        3,
        1,
    )
    .await;
    let (_, request_id, _start, _count) = next_outbound_get_headers(&mut fixture.actions).await;
    send_headers(
        &fixture,
        &peer_id,
        request_id,
        finalized_headers_message(headers.to_vec()),
    )
    .await;
    let operation = loop {
        if let HeaderSyncAction::CommitHeaderRange { operation, .. } =
            next_non_query_action(&mut fixture.actions).await
        {
            break operation;
        }
    };
    fixture
        .handle
        .send(HeaderSyncEvent::FullBlockCommitted {
            height: block::Height(1),
            hash: mainnet_block(&BLOCK_MAINNET_1_BYTES).hash(),
        })
        .await
        .unwrap();
    fixture
        .handle
        .send(HeaderSyncEvent::HeaderRangeOperationFailed {
            operation,
            kind: HeaderSyncCommitFailureKind::Local,
        })
        .await
        .unwrap();

    let (_, _, retry_start, retry_count) = next_outbound_get_headers(&mut fixture.actions).await;
    assert_eq!((retry_start, retry_count), (block::Height(2), 2));
}

#[tokio::test(flavor = "current_thread")]
async fn buffered_successor_drains_after_full_block_covers_its_predecessor() {
    let network = regtest_network();
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let peer_id = peer(143);
    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(
        &fixture,
        peer_id.clone(),
        block::Height(0),
        block::Height(2),
        1,
        2,
    )
    .await;

    let mut requests = HashMap::new();
    while requests.len() < 2 {
        let (_, request_id, start, count) = next_outbound_get_headers(&mut fixture.actions).await;
        assert_eq!(count, 1);
        requests.insert(start, request_id);
    }
    send_headers(
        &fixture,
        &peer_id,
        requests[&block::Height(2)],
        headers_message_from(
            block::Height(2),
            vec![mainnet_header(&BLOCK_MAINNET_2_BYTES)],
        ),
    )
    .await;
    fixture
        .handle
        .send(HeaderSyncEvent::FullBlockCommitted {
            height: block::Height(1),
            hash: mainnet_block(&BLOCK_MAINNET_1_BYTES).hash(),
        })
        .await
        .unwrap();

    loop {
        match next_non_query_action(&mut fixture.actions).await {
            HeaderSyncAction::CommitHeaderRange { payload, .. } => {
                assert_eq!(payload.range().start(), block::Height(2));
                assert_eq!(payload.headers().len(), 1);
                break;
            }
            HeaderSyncAction::Misbehavior { peer, reason } => {
                panic!("unexpected misbehavior from {peer:?}: {reason:?}")
            }
            _ => {}
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn ordered_drain_rejects_a_buffered_range_on_the_wrong_fork() {
    let checkpoint_hash = block::Hash::from(mainnet_header(&BLOCK_MAINNET_3_BYTES).as_ref());
    let (network, _) = checkpoint_testnet_with_hash(block::Height(3), checkpoint_hash);
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let peer_id = peer(208);
    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(
        &fixture,
        peer_id.clone(),
        block::Height(0),
        block::Height(3),
        1,
        2,
    )
    .await;
    let mut requests = HashMap::new();
    while requests.len() < 2 {
        let (_, request_id, start, count) = next_outbound_get_headers(&mut fixture.actions).await;
        assert_eq!(count, 1);
        requests.insert(start, request_id);
    }

    // Block 3 is internally/statelessly valid and has no wire-time anchor for
    // the height-2 batch, but it does not follow the eventual height-1 block.
    send_headers(
        &fixture,
        &peer_id,
        requests[&block::Height(2)],
        headers_message_from(
            block::Height(2),
            vec![mainnet_header(&BLOCK_MAINNET_3_BYTES)],
        ),
    )
    .await;
    send_headers(
        &fixture,
        &peer_id,
        requests[&block::Height(1)],
        headers_message_from(
            block::Height(1),
            vec![mainnet_header(&BLOCK_MAINNET_1_BYTES)],
        ),
    )
    .await;
    let operation = loop {
        if let HeaderSyncAction::CommitHeaderRange {
            operation, payload, ..
        } = next_non_query_action(&mut fixture.actions).await
        {
            if payload.range().start() == block::Height(1) {
                break operation;
            }
        }
    };
    fixture
        .handle
        .send(HeaderSyncEvent::HeaderRangeOperationCompleted {
            operation,
            tip_hash: block::Hash::from(mainnet_header(&BLOCK_MAINNET_1_BYTES).as_ref()),
        })
        .await
        .unwrap();

    loop {
        match next_non_query_action(&mut fixture.actions).await {
            HeaderSyncAction::Misbehavior {
                peer,
                reason: HeaderSyncMisbehavior::InvalidRange,
            } => {
                assert_eq!(peer, peer_id);
                break;
            }
            HeaderSyncAction::CommitHeaderRange { payload, .. }
                if payload.range().start() == block::Height(2) =>
            {
                panic!("ordered drain committed a buffered range on the wrong fork")
            }
            _ => {}
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn full_action_queue_preserves_buffer_until_commit_capacity_returns() {
    let checkpoint_hash = block::Hash::from(mainnet_header(&BLOCK_MAINNET_3_BYTES).as_ref());
    let (network, _) = checkpoint_testnet_with_hash(block::Height(3), checkpoint_hash);
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        Some((block::Height(3), checkpoint_hash)),
    ));
    let source = peer(200);
    connect_peer(&fixture, source.clone()).await;
    advertise_tip(
        &fixture,
        source.clone(),
        block::Height(0),
        block::Height(4),
        1,
        1,
    )
    .await;
    let (_, request_id, _, _) = next_outbound_get_headers(&mut fixture.actions).await;

    for id in 0..128 {
        connect_peer(&fixture, peer(id)).await;
    }
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while fixture.actions.len() < 128 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("peer status actions fill the action queue");
    assert_eq!(
        fixture.actions.len(),
        128,
        "test must saturate action queue"
    );
    send_headers(
        &fixture,
        &source,
        request_id,
        headers_message(vec![mainnet_header(&BLOCK_MAINNET_4_BYTES)]),
    )
    .await;
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while gauge_value("sync.header.work.buffered.count") != 1.0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the reactor reaches commit admission while its action queue is full");
    assert_eq!(gauge_value("sync.header.work.buffered.count"), 1.0);
    assert_eq!(fixture.actions.len(), 128, "commit admission stays blocked");

    // Freeing one action slot is the only wakeup. The capacity waiter must
    // admit the preserved buffer without requiring another peer event.
    let _ = fixture
        .actions
        .recv()
        .await
        .expect("status action fills queue");
    tokio::task::yield_now().await;
    for _ in 0..129 {
        if let HeaderSyncAction::CommitHeaderRange {
            operation, payload, ..
        } = next_action(&mut fixture.actions).await
        {
            assert_eq!(operation.wire_request.peer, source);
            assert_eq!(payload.range().start(), block::Height(4));
            assert_eq!(payload.headers().len(), 1);
            assert_no_commit_or_misbehavior(&mut fixture.actions).await;
            return;
        }
    }
    panic!("preserved header buffer was not committed after action capacity returned");
}

#[tokio::test(flavor = "current_thread")]
async fn peer_requester_waits_for_outbound_capacity_without_reencoding_or_polling() {
    header_sync_metrics_recorder();
    let network = regtest_network();
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let peer_id = peer(201);
    let (send, mut recv) = crate::zakura::framed_channel(1);
    let session = HeaderSyncPeerSession::from_parts_with_direction(
        peer_id.clone(),
        ServicePeerDirection::Inbound,
        send,
        CancellationToken::new(),
    );
    fixture
        .handle
        .send(HeaderSyncEvent::PeerConnected(session))
        .await
        .unwrap();
    advertise_tip(
        &fixture,
        peer_id.clone(),
        block::Height(0),
        block::Height(1),
        1,
        1,
    )
    .await;

    // The initial Status occupies the only real outbound slot. The requester
    // owns the range descriptor but must not publish a sent request yet.
    assert!(tokio::time::timeout(
        std::time::Duration::from_millis(20),
        next_outbound_get_headers(&mut fixture.actions),
    )
    .await
    .is_err());
    assert_eq!(
        gauge_value("sync.header.work.in_flight.count"),
        1.0,
        "the reactor assigns the range before transport publication can complete"
    );
    let _status_frame = recv.recv().await.expect("initial status frame is queued");

    let (request_peer, _, start, count) = next_outbound_get_headers(&mut fixture.actions).await;
    assert_eq!(request_peer, peer_id);
    assert_eq!((start, count), (block::Height(1), 1));
}

#[tokio::test(flavor = "current_thread")]
async fn publishing_request_times_out_while_waiting_for_outbound_capacity() {
    let metrics = metric_snapshot(&["sync.header.request.publication_timeout"]);
    let network = regtest_network();
    let mut fixture = spawn_test_reactor(startup_with_timeout(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        std::time::Duration::from_millis(50),
    ));
    let blocked_peer = peer(205);
    let (send, _recv) = crate::zakura::framed_channel(1);
    let blocked_cancel = CancellationToken::new();
    let session = HeaderSyncPeerSession::from_parts_with_direction(
        blocked_peer.clone(),
        ServicePeerDirection::Inbound,
        send,
        blocked_cancel.clone(),
    );
    fixture
        .handle
        .send(HeaderSyncEvent::PeerConnected(session))
        .await
        .unwrap();
    advertise_tip(
        &fixture,
        blocked_peer,
        block::Height(0),
        block::Height(1),
        1,
        1,
    )
    .await;

    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        blocked_cancel.cancelled(),
    )
    .await
    .expect("a publishing request cancels its blocked session");
    assert_metric_incremented(&metrics, "sync.header.request.publication_timeout");

    let replacement = peer(206);
    connect_peer(&fixture, replacement.clone()).await;
    advertise_tip(
        &fixture,
        replacement.clone(),
        block::Height(0),
        block::Height(1),
        1,
        1,
    )
    .await;
    let (retry_peer, _, retry_start, retry_count) =
        next_outbound_get_headers(&mut fixture.actions).await;
    assert_eq!(retry_peer, replacement);
    assert_eq!((retry_start, retry_count), (block::Height(1), 1));
}

#[tokio::test(flavor = "current_thread")]
async fn transport_closure_retries_work_and_disconnects_requester() {
    let network = regtest_network();
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let failed_peer = peer(207);
    let (send, recv) = crate::zakura::framed_channel(1);
    let failed_session = HeaderSyncPeerSession::from_parts_with_direction(
        failed_peer.clone(),
        ServicePeerDirection::Inbound,
        send,
        CancellationToken::new(),
    );
    drop(recv);
    fixture
        .handle
        .send(HeaderSyncEvent::PeerConnected(failed_session))
        .await
        .unwrap();
    advertise_tip(
        &fixture,
        failed_peer.clone(),
        block::Height(0),
        block::Height(1),
        1,
        1,
    )
    .await;

    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while {
            let snapshot = fixture.handle.peer_snapshot();
            snapshot.inbound_peers + snapshot.outbound_peers != 0
        } {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("closed transport stops its requester and removes the peer");

    let replacement = peer(208);
    connect_peer(&fixture, replacement.clone()).await;
    advertise_tip(
        &fixture,
        replacement.clone(),
        block::Height(0),
        block::Height(1),
        1,
        1,
    )
    .await;
    let (retry_peer, _, retry_start, retry_count) =
        next_outbound_get_headers(&mut fixture.actions).await;
    assert_eq!(retry_peer, replacement);
    assert_eq!((retry_start, retry_count), (block::Height(1), 1));
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn idle_reactor_does_not_create_a_permanent_maintenance_tick() {
    let mut capture =
        TraceCapture::for_test("idle_reactor_does_not_create_a_permanent_maintenance_tick")
            .unwrap();
    let network = regtest_network();
    let mut startup = startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    );
    startup.trace = ZakuraTrace::new(capture.tracer(), "idle-reactor");
    let _fixture = spawn_test_reactor(startup);
    tokio::task::yield_now().await;
    tokio::time::advance(std::time::Duration::from_secs(1)).await;
    tokio::task::yield_now().await;

    capture.flush().await;
    let reader = capture.reader().unwrap();
    assert_eq!(
        reader
            .table(HEADER_SYNC_TABLE.table())
            .count(hs_trace::HEADER_MAINTENANCE_WAKEUP),
        0,
        "an idle reactor should sleep until real work or maintenance is due"
    );
    let _ = capture.finish().await.unwrap();
}

#[test]
fn work_queue_transitions_have_one_explicit_owner() {
    use super::state::{RangePriority, RangeRequest};
    use super::work_queue::{HeaderWorkQueue, HeaderWorkState};

    let owner = peer(144);
    let range = RangeRequest {
        range: CheckedHeaderRange::from_count(block::Height(1), 2)
            .expect("test range is non-empty"),
        anchor_hash: Some(block::Hash([1; 32])),
        finalized: false,
        want_tree_aux_roots: true,
        priority: RangePriority::Forward,
    };
    let mut queue = HeaderWorkQueue::new();
    queue.ensure_forward(range);
    let pending = queue.forward.pop_front().expect("range is pending");
    queue.mark_assigned(owner.clone(), pending);
    assert!(matches!(
        queue.state(range),
        Some(HeaderWorkState::InFlight { peer }) if peer == &owner
    ));
    queue.mark_buffered(owner.clone(), range);
    assert!(matches!(
        queue.state(range),
        Some(HeaderWorkState::Buffered { peer }) if peer == &owner
    ));
    let operation = commit_operation(
        owner.clone(),
        7,
        HeaderSyncRequestId::new(1).expect("test request ID is non-zero"),
    );
    queue.mark_committing(operation.clone(), range);
    assert!(matches!(
        queue.state(range),
        Some(HeaderWorkState::Committing { operation: active }) if active == &operation
    ));
    queue.complete(range);
    assert!(queue.state(range).is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn loaded_best_tip_updates_tip_watch_and_does_not_advance_finality() {
    let network = regtest_network();
    let fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let mut tip = fixture.handle.subscribe_tip();
    let tip_hash = block::Hash([12; 32]);

    fixture
        .handle
        .send(HeaderSyncEvent::BestHeaderTipLoaded {
            tip_height: block::Height(1),
            tip_hash,
        })
        .await
        .unwrap();

    tip.changed().await.unwrap();
    assert_eq!(*tip.borrow(), (block::Height(1), tip_hash));
    assert_ne!(fixture.handle.best_header_tip().0, block::Height(0));
}

#[tokio::test(flavor = "current_thread")]
async fn forward_link_wedge_reanchors_to_verified_tip_without_banning() {
    let network = regtest_network();
    let verified = (block::Height(0), network.genesis_hash());
    let stranded_tip = (block::Height(3), block::Hash([3; 32]));
    let mut startup = HeaderSyncStartup::new(
        network.clone(),
        verified,
        HeaderSyncFrontiers {
            finalized_height: verified.0,
            verified_block_tip: verified.0,
            verified_block_hash: verified.1,
        },
        Some(stranded_tip),
        ZakuraHeaderSyncConfig::default(),
        LOCAL_MAX_MESSAGE_BYTES,
    );
    startup.range_state_actions_enabled = true;
    let mut fixture = spawn_test_reactor(startup);
    let mut tip = fixture.handle.subscribe_tip();
    let peers = [peer(61), peer(62)];

    for peer_id in peers.iter().cloned() {
        connect_peer(&fixture, peer_id.clone()).await;
        advertise_tip(
            &fixture,
            peer_id,
            verified.0,
            block::Height(4),
            DEFAULT_HS_RANGE,
            1,
        )
        .await;
    }

    for _ in 0..3 {
        let (served_peer, request_id, start_height, count) =
            next_outbound_get_headers(&mut fixture.actions).await;
        assert_eq!(start_height, block::Height(4));
        assert_eq!(count, 1);
        send_headers(
            &fixture,
            &served_peer,
            request_id,
            headers_message_from(start_height, vec![mainnet_header(&BLOCK_MAINNET_1_BYTES)]),
        )
        .await;
    }

    tip.changed().await.unwrap();
    assert_eq!(*tip.borrow(), verified);
    assert_eq!(fixture.handle.best_header_tip(), verified);

    let expected_start = verified.0.next().expect("genesis has a successor");
    let mut saw_reanchor_action = false;
    for _ in 0..8 {
        match next_non_query_action(&mut fixture.actions).await {
            HeaderSyncAction::HeaderReanchored { old, new } => {
                assert_eq!(old, stranded_tip);
                assert_eq!(new, verified);
                saw_reanchor_action = true;
            }
            HeaderSyncAction::SendMessage {
                msg:
                    HeaderSyncMessage::GetHeaders {
                        start_height,
                        count: _,
                        want_tree_aux_roots: true,
                    },
                ..
            } if saw_reanchor_action && start_height == expected_start => {
                assert_no_commit_or_misbehavior(&mut fixture.actions).await;
                return;
            }
            HeaderSyncAction::Misbehavior { peer, reason } => {
                panic!("unexpected misbehavior from {peer:?}: {reason:?}");
            }
            _ => {}
        }
    }
    panic!("after re-anchor, header sync did not emit the reanchor action and request forward from the verified tip");
}

#[tokio::test(flavor = "current_thread")]
async fn single_peer_forward_link_failures_do_not_reanchor_globally() {
    let network = regtest_network();
    let verified = (block::Height(0), network.genesis_hash());
    let stranded_tip = (block::Height(3), block::Hash([3; 32]));
    let mut startup = HeaderSyncStartup::new(
        network.clone(),
        verified,
        HeaderSyncFrontiers {
            finalized_height: verified.0,
            verified_block_tip: verified.0,
            verified_block_hash: verified.1,
        },
        Some(stranded_tip),
        ZakuraHeaderSyncConfig::default(),
        LOCAL_MAX_MESSAGE_BYTES,
    );
    startup.range_state_actions_enabled = true;
    let mut fixture = spawn_test_reactor(startup);
    let mut tip = fixture.handle.subscribe_tip();
    let peer_id = peer(63);

    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(
        &fixture,
        peer_id,
        verified.0,
        block::Height(4),
        DEFAULT_HS_RANGE,
        1,
    )
    .await;

    for _ in 0..3 {
        let (served_peer, request_id, start_height, count) =
            next_outbound_get_headers(&mut fixture.actions).await;
        assert_eq!(start_height, block::Height(4));
        assert_eq!(count, 1);
        send_headers(
            &fixture,
            &served_peer,
            request_id,
            headers_message_from(start_height, vec![mainnet_header(&BLOCK_MAINNET_1_BYTES)]),
        )
        .await;
    }

    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), tip.changed())
            .await
            .is_err(),
        "one peer alone must not lower the global header frontier"
    );
    assert_eq!(fixture.handle.best_header_tip(), stranded_tip);
    assert_no_commit_or_misbehavior(&mut fixture.actions).await;
}

#[tokio::test(flavor = "current_thread")]
async fn forward_genesis_backfill_reaches_checkpoint_before_finalized_commit() {
    let headers = [
        mainnet_header(&BLOCK_MAINNET_1_BYTES),
        mainnet_header(&BLOCK_MAINNET_2_BYTES),
        mainnet_header(&BLOCK_MAINNET_3_BYTES),
    ];
    let checkpoint_hash = block::Hash::from(headers[2].as_ref());
    let (network, _) = checkpoint_testnet_with_hash(block::Height(3), checkpoint_hash);
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let peer_id = peer(43);

    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(
        &fixture,
        peer_id.clone(),
        block::Height(0),
        block::Height(3),
        DEFAULT_HS_RANGE,
        1,
    )
    .await;
    let request_id = loop {
        if let HeaderSyncAction::SendMessage {
            request_id: sent_request_id,
            msg:
                HeaderSyncMessage::GetHeaders {
                    start_height,
                    count,
                    want_tree_aux_roots: true,
                },
            ..
        } = next_non_query_action(&mut fixture.actions).await
        {
            assert_eq!(start_height, block::Height(1));
            assert_eq!(count, 3);
            break sent_request_id.expect("an outbound GetHeaders always carries a request ID");
        }
    };

    send_headers(
        &fixture,
        &peer_id,
        request_id,
        finalized_headers_message(headers.to_vec()),
    )
    .await;

    match next_non_query_action(&mut fixture.actions).await {
        HeaderSyncAction::CommitHeaderRange {
            operation,
            payload,
            finalized,
            ..
        } => {
            assert_eq!(operation.wire_request.peer, peer_id);
            assert_eq!(operation.wire_request.request_id, request_id);
            assert_eq!(payload.range().start(), block::Height(1));
            assert_eq!(payload.headers().len(), 3);
            assert!(finalized);
        }
        action => panic!("unexpected action: {action:?}"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn truncated_finalized_backfill_commits_valid_prefix_and_requeues_suffix() {
    let headers = [
        mainnet_header(&BLOCK_MAINNET_1_BYTES),
        mainnet_header(&BLOCK_MAINNET_2_BYTES),
        mainnet_header(&BLOCK_MAINNET_3_BYTES),
    ];
    let checkpoint_hash = block::Hash::from(headers[2].as_ref());
    let (network, _) = checkpoint_testnet_with_hash(block::Height(3), checkpoint_hash);
    let mut fixture = spawn_test_reactor(startup_for(
        network.clone(),
        (block::Height(0), network.genesis_hash()),
        None,
    ));
    let peer_id = peer(44);

    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(
        &fixture,
        peer_id.clone(),
        block::Height(0),
        block::Height(3),
        DEFAULT_HS_RANGE,
        1,
    )
    .await;
    let request_id = next_get_headers_request_id(&mut fixture.actions).await;

    send_headers(
        &fixture,
        &peer_id,
        request_id,
        headers_message(headers[..2].to_vec()),
    )
    .await;

    match next_non_query_action(&mut fixture.actions).await {
        HeaderSyncAction::CommitHeaderRange {
            operation,
            payload,
            finalized,
            ..
        } => {
            assert_eq!(operation.wire_request.peer, peer_id);
            assert_eq!(operation.wire_request.request_id, request_id);
            assert_eq!(payload.range().start(), block::Height(1));
            assert_eq!(payload.headers().len(), 2);
            assert!(finalized);
        }
        action => panic!("unexpected action: {action:?}"),
    }
    let (suffix_peer, _, suffix_start, suffix_count) =
        next_outbound_get_headers(&mut fixture.actions).await;
    assert_eq!(suffix_peer, peer_id);
    assert_eq!((suffix_start, suffix_count), (block::Height(3), 1));
    assert_no_commit_or_misbehavior(&mut fixture.actions).await;
}

#[tokio::test(flavor = "current_thread")]
async fn header_response_that_does_not_link_to_anchor_is_misbehavior_before_commit() {
    let checkpoint_hash = block::Hash::from(mainnet_header(&BLOCK_MAINNET_3_BYTES).as_ref());
    let (network, _) = checkpoint_testnet_with_hash(block::Height(3), checkpoint_hash);
    let anchor = (block::Height(0), network.genesis_hash());
    let mut fixture = spawn_test_reactor(startup_for(network, anchor, Some(anchor)));
    let peer_id = peer(46);

    connect_peer(&fixture, peer_id.clone()).await;
    advertise_tip(
        &fixture,
        peer_id.clone(),
        block::Height(0),
        block::Height(4),
        DEFAULT_HS_RANGE,
        1,
    )
    .await;
    let request_id = next_get_headers_request_id(&mut fixture.actions).await;

    send_headers(
        &fixture,
        &peer_id,
        request_id,
        headers_message_from(
            block::Height(1),
            vec![mainnet_header(&BLOCK_MAINNET_2_BYTES)],
        ),
    )
    .await;

    match next_non_query_action(&mut fixture.actions).await {
        HeaderSyncAction::Misbehavior { peer, reason } => {
            assert_eq!(peer, peer_id);
            assert_eq!(reason, HeaderSyncMisbehavior::InvalidRange);
        }
        action => panic!("unexpected action: {action:?}"),
    }
    assert_no_commit_or_misbehavior(&mut fixture.actions).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stateless_validation_accepts_valid_contiguous_headers() {
    let headers = vec![mainnet_header(&BLOCK_MAINNET_1_BYTES)];
    let context = HeaderSyncValidationContext {
        network: &Network::Mainnet,
        now: Utc::now(),
        start_height: block::Height(1),
        decode_context: headers_context(1, DEFAULT_HS_RANGE),
    };

    validate_headers_stateless(headers, context).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stateless_validation_rejects_non_contiguous_and_future_headers() {
    let mut second = *mainnet_header(&BLOCK_MAINNET_1_BYTES);
    second.previous_block_hash = block::Hash([1; 32]);
    let headers = vec![
        mainnet_header(&BLOCK_MAINNET_GENESIS_BYTES),
        Arc::new(second),
    ];
    let context = HeaderSyncValidationContext {
        network: &Network::Mainnet,
        now: Utc::now(),
        start_height: block::Height(0),
        decode_context: headers_context(2, DEFAULT_HS_RANGE),
    };
    assert!(matches!(
        validate_headers_stateless(headers, context).await,
        Err(HeaderSyncWireError::NonContiguousHeaders)
    ));

    let mut future = *mainnet_header(&BLOCK_MAINNET_1_BYTES);
    future.time = Utc::now() + Duration::hours(3);
    let context = HeaderSyncValidationContext {
        network: &Network::Mainnet,
        now: Utc::now(),
        start_height: block::Height(1),
        decode_context: headers_context(1, DEFAULT_HS_RANGE),
    };
    assert!(matches!(
        validate_headers_stateless(vec![Arc::new(future)], context).await,
        Err(HeaderSyncWireError::Time(_))
    ));
}

#[test]
fn range_link_validation_rejects_non_linking_headers() {
    let genesis = mainnet_block(&BLOCK_MAINNET_GENESIS_BYTES);
    let block1 = mainnet_header(&BLOCK_MAINNET_1_BYTES);
    let block2 = mainnet_header(&BLOCK_MAINNET_2_BYTES);

    let mut bad_first = *block1;
    bad_first.previous_block_hash = block::Hash([1; 32]);
    assert!(matches!(
        validate_header_range_links(genesis.hash(), &[Arc::new(bad_first)]),
        Err(HeaderSyncWireError::FirstHeaderDoesNotLink)
    ));

    let mut bad_second = *block2;
    bad_second.previous_block_hash = block::Hash([2; 32]);
    assert!(matches!(
        validate_header_range_links(genesis.hash(), &[block1, Arc::new(bad_second)]),
        Err(HeaderSyncWireError::NonContiguousHeaders)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stateless_validation_rejects_bad_pow() {
    let mut bad_solution = *mainnet_header(&BLOCK_MAINNET_1_BYTES);
    bad_solution.nonce[0] ^= 1;
    let context = HeaderSyncValidationContext {
        network: &Network::Mainnet,
        now: Utc::now(),
        start_height: block::Height(1),
        decode_context: headers_context(1, DEFAULT_HS_RANGE),
    };
    assert!(matches!(
        validate_headers_stateless(vec![Arc::new(bad_solution)], context).await,
        Err(HeaderSyncWireError::Equihash(_))
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn new_block_stateless_validation_accepts_valid_mainnet_block() {
    validate_new_block_stateless(
        mainnet_block(&BLOCK_MAINNET_1_BYTES),
        &Network::Mainnet,
        Utc::now(),
        block::Height(1),
    )
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn new_block_stateless_validation_rejects_wrong_solution_size_and_bad_pow() {
    let mut wrong_solution_size = (*mainnet_block(&BLOCK_MAINNET_1_BYTES)).clone();
    let mut header = *wrong_solution_size.header;
    header.solution = Solution::Regtest([0; 36]);
    wrong_solution_size.header = Arc::new(header);

    assert!(matches!(
        validate_new_block_stateless(
            Arc::new(wrong_solution_size),
            &Network::Mainnet,
            Utc::now(),
            block::Height(1),
        )
        .await,
        Err(HeaderSyncWireError::WrongEquihashSolutionSize)
    ));

    let mut bad_pow = (*mainnet_block(&BLOCK_MAINNET_1_BYTES)).clone();
    let mut header = *bad_pow.header;
    header.nonce[0] ^= 1;
    bad_pow.header = Arc::new(header);

    assert!(matches!(
        validate_new_block_stateless(
            Arc::new(bad_pow),
            &Network::Mainnet,
            Utc::now(),
            block::Height(1),
        )
        .await,
        Err(HeaderSyncWireError::Equihash(_))
    ));
}

#[test]
fn difficulty_filter_rejects_hash_above_threshold() {
    let threshold =
        CompactDifficulty::from_bytes_in_display_order(&[0x01, 0x01, 0x00, 0x00]).unwrap();

    assert!(matches!(
        validate_difficulty_filter(block::Hash([0xff; 32]), threshold),
        Err(HeaderSyncWireError::DifficultyFilter { .. })
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stateless_header_validation_surfaces_difficulty_filter_after_equihash_acceptance() {
    let mut header = *mainnet_header(&BLOCK_MAINNET_1_BYTES);
    header.difficulty_threshold =
        CompactDifficulty::from_bytes_in_display_order(&[0x01, 0x01, 0x00, 0x00]).unwrap();
    let context = HeaderSyncValidationContext {
        network: &Network::Mainnet,
        now: Utc::now(),
        start_height: block::Height(1),
        decode_context: headers_context(1, DEFAULT_HS_RANGE),
    };

    assert!(matches!(
        validate_headers_stateless_after_equihash_acceptance(vec![Arc::new(header)], context).await,
        Err(HeaderSyncWireError::DifficultyFilter { .. })
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stateless_validation_rejects_wrong_solution_size_for_network() {
    let mut regtest_sized = *mainnet_header(&BLOCK_MAINNET_1_BYTES);
    regtest_sized.solution = Solution::Regtest([0; 36]);
    let context = HeaderSyncValidationContext {
        network: &Network::Mainnet,
        now: Utc::now(),
        start_height: block::Height(1),
        decode_context: headers_context(1, DEFAULT_HS_RANGE),
    };

    assert!(matches!(
        validate_headers_stateless(vec![Arc::new(regtest_sized)], context).await,
        Err(HeaderSyncWireError::WrongEquihashSolutionSize)
    ));
}

#[test]
fn pow_disabled_header_validation_accepts_common_and_short_solution_sizes() {
    let regtest = Network::new_regtest(Default::default());
    let custom_testnet = Parameters::build()
        .with_network_name("HeaderSyncNoPowSizeTest")
        .expect("custom testnet name is valid")
        .with_disable_pow(true)
        .to_network()
        .expect("custom testnet parameters are valid");
    let common_sized = mainnet_header(&BLOCK_MAINNET_1_BYTES);
    let mut short_sized = *common_sized;
    short_sized.solution = Solution::Regtest([0; 36]);

    for network in [&regtest, &custom_testnet] {
        validate_solution_sizes(std::slice::from_ref(&common_sized), network)
            .expect("PoW-disabled networks accept common-size solutions");
        validate_solution_sizes(&[Arc::new(short_sized)], network)
            .expect("PoW-disabled networks accept short solutions");
    }

    assert!(matches!(
        validate_solution_sizes(&[Arc::new(short_sized)], &Network::Mainnet),
        Err(HeaderSyncWireError::WrongEquihashSolutionSize)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn regtest_stateless_validation_skips_pow_filter() {
    let regtest = Network::new_regtest(Default::default());
    let mut header = *mainnet_header(&BLOCK_MAINNET_1_BYTES);
    header.difficulty_threshold =
        CompactDifficulty::from_bytes_in_display_order(&[0x01, 0x01, 0x00, 0x00]).unwrap();
    let context = HeaderSyncValidationContext {
        network: &regtest,
        now: Utc::now(),
        start_height: block::Height(1),
        decode_context: headers_context(1, DEFAULT_HS_RANGE),
    };

    validate_headers_stateless(vec![Arc::new(header)], context)
        .await
        .expect("regtest header sync leaves PoW enforcement to block verification");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn custom_testnet_disable_pow_skips_header_sync_pow_filter() {
    let network = Parameters::build()
        .with_network_name("HeaderSyncNoPowTest")
        .expect("custom testnet name is valid")
        .with_disable_pow(true)
        .to_network()
        .expect("custom testnet parameters are valid");
    assert!(network.disable_pow());
    assert!(!network.is_regtest());

    let mut header = *mainnet_header(&BLOCK_MAINNET_1_BYTES);
    header.nonce[0] ^= 1;
    header.difficulty_threshold =
        CompactDifficulty::from_bytes_in_display_order(&[0x01, 0x01, 0x00, 0x00]).unwrap();
    let context = HeaderSyncValidationContext {
        network: &network,
        now: Utc::now(),
        start_height: block::Height(1),
        decode_context: headers_context(1, DEFAULT_HS_RANGE),
    };

    validate_headers_stateless(vec![Arc::new(header)], context)
        .await
        .expect("custom disable_pow networks must skip native header-sync PoW checks");
}

#[tokio::test(flavor = "current_thread")]
async fn pow_validation_does_not_monopolize_the_runtime_thread() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let headers = vec![
        mainnet_header(&BLOCK_MAINNET_1_BYTES),
        mainnet_header(&BLOCK_MAINNET_2_BYTES),
        mainnet_header(&BLOCK_MAINNET_3_BYTES),
        mainnet_header(&BLOCK_MAINNET_4_BYTES),
    ];
    let context = HeaderSyncValidationContext {
        network: &Network::Mainnet,
        now: Utc::now(),
        start_height: block::Height(1),
        decode_context: headers_context(4, DEFAULT_HS_RANGE),
    };

    let ticks = Arc::new(AtomicUsize::new(0));
    let ticker_ticks = ticks.clone();
    let ticker = tokio::spawn(async move {
        loop {
            ticker_ticks.fetch_add(1, Ordering::SeqCst);
            tokio::task::yield_now().await;
        }
    });

    validate_headers_stateless(headers, context).await.unwrap();
    let progressed = ticks.load(Ordering::SeqCst);
    ticker.abort();

    assert!(
        progressed > 0,
        "reactor thread was blocked during PoW validation"
    );
}

#[test]
fn hostile_vectors_are_rejected_for_allocation_and_unsolicited_headers() {
    let mut encoded = vec![MSG_HS_HEADERS];
    encoded
        .write_u64::<LittleEndian>(test_request_id().get())
        .unwrap();
    encoded.write_u32::<LittleEndian>(u32::MAX).unwrap();
    encoded.write_u8(0).unwrap();
    assert!(matches!(
        HeaderSyncMessage::decode(&encoded, headers_context(MAX_HS_RANGE, MAX_HS_RANGE)),
        Err(HeaderSyncWireError::HeaderCountLimit { .. })
    ));

    // A well-formed `Headers` payload with no outstanding request is unsolicited.
    let mut encoded = vec![MSG_HS_HEADERS];
    encoded
        .write_u64::<LittleEndian>(test_request_id().get())
        .unwrap();
    encoded.write_u32::<LittleEndian>(1).unwrap();
    encoded.write_u8(0).unwrap();
    assert!(matches!(
        HeaderSyncMessage::decode(&encoded, HeaderSyncDecodeContext::control()),
        Err(HeaderSyncWireError::UnsolicitedHeaders)
    ));
}

/// Misbehavior is record-only: an `InvalidStatus` (formerly an immediate
/// disconnect) is still *recorded* as a `Misbehavior` action, but the peer's
/// session is **not** cancelled. Peer scoring no longer drives disconnects.
#[tokio::test]
async fn misbehavior_is_recorded_without_disconnecting_the_peer() {
    let network = Network::Mainnet;
    let anchor = (block::Height(0), network.genesis_hash());
    let mut startup = HeaderSyncStartup::new(
        network,
        anchor,
        HeaderSyncFrontiers {
            finalized_height: anchor.0,
            verified_block_tip: anchor.0,
            verified_block_hash: anchor.1,
        },
        Some(anchor),
        ZakuraHeaderSyncConfig::default(),
        LOCAL_MAX_MESSAGE_BYTES,
    );
    // Keep the test deterministic: no scheduling/state actions, so the only
    // actions enqueued are the ones we drive below.
    startup.range_state_actions_enabled = false;
    let mut fixture = spawn_test_reactor(startup);

    // Connect the peer we will flag as misbehaving and keep its session
    // cancellation token so we can confirm it is never cancelled.
    let probe = peer(7);
    let probe_cancel =
        connect_peer_with_direction(&fixture, probe.clone(), ServicePeerDirection::Inbound).await;

    // `anchor_height > tip_height` is an `InvalidStatus` misbehavior.
    let invalid_status = HeaderSyncMessage::Status(HeaderSyncStatus {
        tip_height: block::Height(0),
        tip_hash: block::Hash([0; 32]),
        anchor_height: block::Height(1),
        max_headers_per_response: 1,
        max_inflight_requests: 1,
    });
    fixture
        .handle
        .send(HeaderSyncEvent::WireMessage {
            peer: probe.clone(),
            msg: invalid_status,
        })
        .await
        .expect("event queues");

    // The violation is recorded: a `Misbehavior` action for the probe is emitted.
    loop {
        if let HeaderSyncAction::Misbehavior { peer, reason } =
            next_non_query_action(&mut fixture.actions).await
        {
            assert_eq!(peer, probe);
            assert_eq!(reason, HeaderSyncMisbehavior::InvalidStatus);
            break;
        }
    }

    // But the peer's session is never cancelled: misbehavior is record-only.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(
        !probe_cancel.is_cancelled(),
        "misbehavior is record-only: an InvalidStatus peer must NOT be disconnected",
    );
}
