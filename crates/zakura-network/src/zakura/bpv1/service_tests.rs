//! Stack-5 (`bpv1-io`) service, wire-trace, resource-envelope, and real-stream
//! conformance tests (level-8 and level-10).

use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use zakura_chain::{
    block::{self, Header, Height},
    serialization::ZcashSerialize,
};
use zakura_header_chain::{
    ChainScore, EngineMode, EngineSnapshot, Frontier, FrontierSet, HeaderGeneration, StateVersion,
    SuffixWork, VerifiedGeneration,
};
use zakura_node_services::body_claims::SharedBodyClaims;
use zakura_quic::NodeId;
use zakura_rt::{
    collections::HashMap,
    sync::{watch, CancellationToken},
};

use super::{
    blocks::{BlockPhase, HeaderAdmissionResult, VerifierVerdict, WorkId},
    ports::{HeaderAdmissionPort, PortFuture},
    profile::Profile,
    service::{Bpv1Handle, Bpv1Ports},
    test_support::{
        block_of, build_v6_coinbase, build_v6_tx, committed_coinbase, connect, connect_pair,
        node_id, pair, ports, regtest_nu7, regtest_nu7_at, solved_block, start, wait_until,
        ScriptedVerifier,
    },
    wire::{BPV1_CONTROL_KIND, BPV1_DATA_KIND, ZAKURA_CAP_BPV1},
};
use crate::{
    config::Config,
    zakura::{
        node_envelope::NodeEnvelope,
        transport::{framed_channel, Peer, Service},
        ServicePeerDirection, ZakuraLocalLimits, ZakuraPeerId,
    },
};

/// A durable header-chain snapshot with the given best header and verified tip.
fn engine_snapshot(header_best: Frontier, verified_best: Frontier) -> EngineSnapshot {
    EngineSnapshot {
        mode: EngineMode::Integrated,
        state_version: StateVersion::default(),
        header_generation: HeaderGeneration::new(0),
        verified_generation: VerifiedGeneration::new(0),
        frontiers: FrontierSet {
            finalized: verified_best,
            header_best,
            verified_best,
        },
        header_best_score: ChainScore::new(SuffixWork::zero(), header_best.hash),
        oldest_retained_height: verified_best.height,
        alarms: Default::default(),
    }
}

#[test]
fn config_default_is_opt_in_and_disabled_adds_zero_envelope_bytes() {
    let mut cfg = Config::default();
    assert!(!cfg.zakura.bpv1.enabled, "BPv1 must be opt-in by default");
    assert!(cfg.zakura.bpv1.validate().is_ok());
    // Hold max_connections fixed under a generous peer memory budget so both
    // envelopes compare the exact same connection count.
    cfg.zakura.max_connections = Some(8);

    let limits = ZakuraLocalLimits::from_config(&cfg);
    let env_disabled = NodeEnvelope::for_node(&cfg, &limits, &[], 0).expect("disabled envelope");
    assert!(env_disabled.quic_bytes > 0);

    let mut cfg_enabled = cfg.clone();
    cfg_enabled.zakura.bpv1.enabled = true;
    let limits_enabled = ZakuraLocalLimits::from_config(&cfg_enabled);
    assert_eq!(limits.max_connections, limits_enabled.max_connections);
    let env_enabled =
        NodeEnvelope::for_node(&cfg_enabled, &limits_enabled, &[], 0).expect("enabled envelope");

    // Each stream's writer holds one encoded frame outside its output gate.
    assert_eq!(
        super::config::BPV1_SESSION_HELD_OUTPUT_BYTES,
        u64::from(super::wire::CONTROL_FRAME_CAP) + u64::from(super::wire::DATA_FRAME_CAP),
    );
    let expected_delta = u128::from(limits_enabled.max_connections as u64)
        * u128::from(super::config::BPV1_SESSION_HELD_OUTPUT_BYTES);
    assert_eq!(
        env_enabled.serve_output_bytes - env_disabled.serve_output_bytes,
        expected_delta,
        "enabled BPv1 registers its exact dedicated held-output budget (dual-6)"
    );
    assert!(
        env_enabled.quic_bytes > env_disabled.quic_bytes,
        "enabled BPv1 registers its two persistent stream kinds in reactor_shares"
    );

    // The memory plan's session base `b_bpv1` covers the held output the
    // envelope registers per session and both output gates.
    let base = super::memory::SessionBase::new(&cfg_enabled.zakura.bpv1);
    let held_per_session = (env_enabled.serve_output_bytes - env_disabled.serve_output_bytes)
        / u128::from(limits_enabled.max_connections as u64);
    let gates = super::wire::BPV1_CONTROL_BUDGET.output + super::wire::BPV1_DATA_BUDGET.output;
    assert!(
        u128::from(base.session) >= held_per_session + u128::from(gates),
        "a session's base funds its held output and both output gates"
    );
}

#[test]
fn config_max_sessions_is_an_optional_cap_and_early_records_scale_by_session() {
    let config = super::config::ZakuraBpv1Config::default();
    // `max_sessions` is unset by default, so memory decides the session
    // count. A set value is a reactor cap, and zero is invalid.
    assert_eq!(config.max_sessions, None);
    assert!(config.admits_session(10_000));
    let mut capped = config.clone();
    capped.max_sessions = Some(2);
    assert!(capped.admits_session(1) && !capped.admits_session(2));
    capped.max_sessions = Some(0);
    assert!(capped.validate().is_err());

    assert_eq!(
        config.early_node_bytes_cap(3),
        3 * u64::try_from(config.early_peer_bytes_cap).expect("the default cap fits in u64"),
        "the node's early-record cap is sessions × early_peer_bytes_cap"
    );
}

#[test]
fn sampled_part_hash_is_deterministic_and_picks_about_one_in_sixteen() {
    use super::pull_trace::is_sampled_part;

    let block_id = block::Hash([0x33; 32]);
    let sampled: usize = (0..1024u32)
        .filter(|&i| is_sampled_part(&block_id, i))
        .count();
    assert!(
        (30..=110).contains(&sampled),
        "expected ~64 sampled parts out of 1024, got {sampled}"
    );
    assert_eq!(is_sampled_part(&block_id, 7), is_sampled_part(&block_id, 7));
}

#[tokio::test]
async fn bpv1_proposer_hook_handles_post_nu7_pre_nu7_reuse_and_pool_coinbase() {
    use zakura_node_services::bpv1_proposer::Bpv1ProposerHook;

    let network = regtest_nu7_at(10);
    let shutdown = CancellationToken::new();
    let (id_a, id_b) = (node_id(51), node_id(52));
    let tiny = |id| {
        start(
            Profile::TINY,
            network.clone(),
            id,
            Bpv1Ports::default(),
            &shutdown,
        )
    };
    let ((svc_a, handle_a), (svc_b, _handle_b)) = (tiny(id_a), tiny(id_b));
    connect(&svc_a, id_a, &svc_b, id_b, &shutdown);

    let lcr = [0x44; 32];
    let tx1 = build_v6_tx(Height(12), 1);
    let tx1_bytes = tx1.zcash_serialize_to_vec().unwrap();

    // 1. Pre-NU7 height (height 5 < nu7 activation 10) gets no commitment (decision-9).
    assert!(
        handle_a
            .prepare_template_commitment(lcr, Height(5), 2, &tx1_bytes)
            .is_none(),
        "pre-NU7 template must not receive a BPv1 commitment"
    );

    // 2. Post-NU7 height produces a 37-byte `BPV1 || 0x01 || C` null-data payload.
    let payload1 = handle_a
        .prepare_template_commitment(lcr, Height(12), 2, &tx1_bytes)
        .expect("post-NU7 template with live peer gets commitment");
    assert_eq!(&payload1[..5], b"BPV1\x01");

    // 3. Second call with same body (header-only template refresh) succeeds via opt-8 reuse.
    let payload2 = handle_a
        .prepare_template_commitment(lcr, Height(12), 2, &tx1_bytes)
        .expect("header-only refresh reuses encoding");
    assert_eq!(&payload2[..5], b"BPV1\x01");

    // 4. Pool-built coinbase without a BPv1 commitment returns false (falls back to existing relay).
    let pool_cb = build_v6_coinbase(Height(12), &[0x51], 1_000_000, vec![], &[]);
    let pool_block = Arc::new(block_of(
        lcr,
        vec![Arc::new(pool_cb), Arc::new(tx1.clone())],
    ));
    assert!(
        !Bpv1ProposerHook::publish_solved_block(&handle_a, pool_block, Height(12)),
        "pool-built coinbase without commitment must fall back to existing relay"
    );

    // 5. Solved block carrying the committed OP_RETURN BPV1 01 C output publishes via BPv1!
    let mut full_script = vec![0x6a, 0x25];
    full_script.extend_from_slice(&payload2);
    let cb = committed_coinbase(Height(12), &full_script);
    let solved = Arc::new(block_of(lcr, vec![Arc::new(cb), Arc::new(tx1)]));
    assert!(
        Bpv1ProposerHook::publish_solved_block(&handle_a, solved, Height(12)),
        "committed post-NU7 block must publish via BPv1ProposerHook"
    );

    shutdown.cancel();
}

#[tokio::test]
async fn bpv1_62_replaced_session_exit_keeps_the_replacement_and_shutdown_drains() {
    let (id_a, id_b) = (node_id(61), node_id(62));
    let shutdown = CancellationToken::new();
    let tiny = |id| {
        start(
            Profile::TINY,
            regtest_nu7(),
            id,
            Bpv1Ports::default(),
            &shutdown,
        )
    };
    let ((svc_a, handle_a), (svc_b, handle_b)) = (tiny(id_a), tiny(id_b));

    // Connect a session, then replace it with one for the same peer NodeId.
    // Old session exit must NOT clobber the replacement session.
    let old_cancel_a = CancellationToken::new();
    let old_cancel_b = CancellationToken::new();
    connect_pair(
        &svc_a,
        id_a,
        &svc_b,
        id_b,
        old_cancel_a.clone(),
        old_cancel_b.clone(),
        None,
    );
    assert_eq!(handle_a.live_sessions(), 1);

    let new_cancel_a = CancellationToken::new();
    let new_cancel_b = CancellationToken::new();
    connect_pair(
        &svc_a,
        id_a,
        &svc_b,
        id_b,
        new_cancel_a.clone(),
        new_cancel_b.clone(),
        None,
    );
    // Cancel old session explicitly and let it finish teardown.
    old_cancel_a.cancel();
    old_cancel_b.cancel();
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(
        handle_a.live_sessions(),
        1,
        "the old session's exit keeps the replacement session"
    );
    assert_eq!(handle_b.live_sessions(), 1);

    // A peer that never reads its data channel still shuts down cleanly.
    let id_stalled = node_id(63);
    let (_ctrl_tx_in, ctrl_rx_in) = framed_channel(1);
    let (ctrl_tx_out, _ctrl_rx_out) = framed_channel(1);
    let (_data_tx_in, data_rx_in) = framed_channel(1);
    let (data_tx_out, _data_rx_out_unpolled) = framed_channel(1);
    let mut stalled_streams = HashMap::new();
    stalled_streams.insert(BPV1_CONTROL_KIND, (ctrl_rx_in, ctrl_tx_out));
    stalled_streams.insert(BPV1_DATA_KIND, (data_rx_in, data_tx_out));
    let stalled_cancel = CancellationToken::new();
    svc_a.add_peer(Peer::new_with_conn_id_and_direction(
        99,
        ZakuraPeerId::from_node_id(id_stalled),
        None,
        ZAKURA_CAP_BPV1,
        ServicePeerDirection::Outbound,
        stalled_streams,
        stalled_cancel.clone(),
    ));
    assert_eq!(handle_a.live_sessions(), 2);

    shutdown.cancel();
}

/// Two services, A proposing to B, where B's verifier returns `verdict` and
/// B follows the committed snapshots the returned sender publishes.
fn proposer_and_follower(
    verdict: VerifierVerdict,
    shutdown: &CancellationToken,
) -> (
    Bpv1Handle,
    Bpv1Handle,
    watch::Sender<Option<EngineSnapshot>>,
) {
    let (snapshots_b, committed_snapshots) = watch::channel(None);
    let valid = ScriptedVerifier::new(VerifierVerdict::Valid);
    let ports_b = Bpv1Ports {
        committed_snapshots,
        ..ports(10, ScriptedVerifier::new(verdict), SharedBodyClaims::new())
    };
    let (handle_a, handle_b) = pair(
        Profile::TINY,
        ports(10, valid, SharedBodyClaims::new()),
        ports_b,
        shutdown,
    );
    (handle_a, handle_b, snapshots_b)
}

/// A committed tip past the propagation window retires a
/// done block, releases its parts (bpv1-52), and refuses later metadata for a
/// block below the window (bpv1-7).
#[tokio::test]
async fn fix2_committed_tip_retires_done_blocks_and_refuses_old_metadata() {
    let shutdown = CancellationToken::new();
    let (handle_a, handle_b, snapshots_b) =
        proposer_and_follower(VerifierVerdict::Valid, &shutdown);

    let done = solved_block(&handle_a, Height(10), 9);
    assert!(handle_a.publish_solved_block(&done, 10));
    wait_until("B commits the block", || {
        handle_b.block_phase(&done.hash()) == Some(BlockPhase::Done)
    })
    .await;
    assert!(
        handle_b.retained_part_bytes() > 0,
        "a done block keeps its parts charged until it retires"
    );

    let tip = Frontier::new(Height(100), block::Hash([0x64; 32]));
    snapshots_b.send_replace(Some(engine_snapshot(tip, tip)));
    wait_until("retirement frees the parts", || {
        handle_b.retained_part_bytes() == 0
    })
    .await;

    let old = solved_block(&handle_a, Height(10), 10);
    assert!(handle_a.publish_solved_block(&old, 10));
    assert!(
        handle_a.block_phase(&old.hash()).is_some(),
        "A published it"
    );
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        handle_b.block_phase(&old.hash()),
        None,
        "B refused metadata below its window"
    );
    shutdown.cancel();
}

/// When native block sync commits a block that fell back,
/// the committed snapshot ends its fallback and frees its parts without an
/// eviction.
#[tokio::test]
async fn fix4_committed_snapshot_ends_a_fallback_block() {
    let shutdown = CancellationToken::new();
    let (handle_a, handle_b, snapshots_b) =
        proposer_and_follower(VerifierVerdict::LocalCapacity, &shutdown);

    let fallen = solved_block(&handle_a, Height(10), 11);
    assert!(handle_a.publish_solved_block(&fallen, 10));
    wait_until("B falls back", || {
        handle_b.block_phase(&fallen.hash()) == Some(BlockPhase::Fallback)
    })
    .await;

    let committed = Frontier::new(Height(10), fallen.hash());
    snapshots_b.send_replace(Some(engine_snapshot(committed, committed)));
    wait_until("the commit retires the fallback", || {
        handle_b.block_phase(&fallen.hash()) == Some(BlockPhase::Retired)
    })
    .await;
    assert_eq!(handle_b.retained_part_bytes(), 0);
    shutdown.cancel();
}

/// Header admission that reports every header's parent missing until the
/// test admits the parent.
struct ParentGatedHeaderAdmission {
    parent_admitted: Arc<AtomicBool>,
    calls: Arc<AtomicUsize>,
}

impl HeaderAdmissionPort for ParentGatedHeaderAdmission {
    fn admit(
        &self,
        header: Arc<Header>,
        _source_peer: NodeId,
        _session_gen: u64,
        _work_id: WorkId,
    ) -> PortFuture<'_, HeaderAdmissionResult> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        let result = if self.parent_admitted.load(Ordering::Acquire) {
            HeaderAdmissionResult::Admitted { height: 10 }
        } else {
            HeaderAdmissionResult::MissingParent {
                parent_hash: header.previous_block_hash,
            }
        };
        Box::pin(async move { result })
    }
}

/// Metadata that arrives before its parent header waits, and the
/// committed snapshot that admits the parent wakes it, so the block assembles
/// instead of falling back.
#[tokio::test]
async fn fix3_parent_admitted_through_snapshots_wakes_pending_child() {
    let shutdown = CancellationToken::new();
    let parent_admitted = Arc::new(AtomicBool::new(false));
    let admissions = Arc::new(AtomicUsize::new(0));
    let (snapshots_b, committed_snapshots) = watch::channel(None);
    let verifier_b = ScriptedVerifier::new(VerifierVerdict::Valid);
    let ports_b = Bpv1Ports {
        header_admission: Arc::new(ParentGatedHeaderAdmission {
            parent_admitted: parent_admitted.clone(),
            calls: admissions.clone(),
        }),
        committed_snapshots,
        ..ports(10, verifier_b.clone(), SharedBodyClaims::new())
    };
    let valid = ScriptedVerifier::new(VerifierVerdict::Valid);
    let (handle_a, handle_b) = pair(
        Profile::TINY,
        ports(10, valid, SharedBodyClaims::new()),
        ports_b,
        &shutdown,
    );

    let height = Height(10);
    let solved = solved_block(&handle_a, height, 13);
    let parent = solved.header.previous_block_hash;
    let child = solved.hash();
    assert!(handle_a.publish_solved_block(&solved, height.0));

    // B parks the metadata: its parent header is missing.
    wait_until("B submits the header for admission", || {
        admissions.load(Ordering::Acquire) > 0
    })
    .await;
    assert_eq!(admissions.load(Ordering::Acquire), 1);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        handle_b.block_phase(&child),
        None,
        "child waits for its parent"
    );

    // Headerchain commits the parent as its best header.
    parent_admitted.store(true, Ordering::Release);
    let verified = Frontier::new(Height(8), block::Hash([0x08; 32]));
    snapshots_b.send_replace(Some(engine_snapshot(
        Frontier::new(Height(9), parent),
        verified,
    )));
    wait_until("B commits the child", || {
        handle_b.block_phase(&child) == Some(BlockPhase::Done)
    })
    .await;
    assert_eq!(
        verifier_b.seen(),
        vec![child],
        "B assembled the child itself"
    );

    shutdown.cancel();
}

/// The driver turns each committed frontier change into the
/// matching `BlockTable` inputs, and an unchanged frontier into none.
#[test]
fn frontier_inputs_follow_committed_frontier_changes() {
    use super::{blocks::BlockInput, service::frontier_inputs};

    let header = Frontier::new(Height(20), block::Hash([0x20; 32]));
    let verified = Frontier::new(Height(18), block::Hash([0x18; 32]));
    let first = engine_snapshot(header, verified).frontiers;
    let summary = |inputs: Vec<BlockInput>| {
        inputs
            .into_iter()
            .map(|input| match input {
                BlockInput::TipHeight { height } => format!("tip {height}"),
                BlockInput::FallbackDone { block_id } => format!("done {}", block_id.0[0]),
                BlockInput::ParentAdmitted { hash, height } => {
                    format!("parent {} {height}", hash.0[0])
                }
                other => panic!("unexpected input {other:?}"),
            })
            .collect::<Vec<_>>()
    };

    assert_eq!(
        summary(frontier_inputs(None, first)),
        ["tip 18", "done 24", "parent 32 20"]
    );
    assert!(frontier_inputs(Some(first), first).is_empty());

    let next_header = Frontier::new(Height(21), block::Hash([0x21; 32]));
    let only_header = engine_snapshot(next_header, verified).frontiers;
    assert_eq!(
        summary(frontier_inputs(Some(first), only_header)),
        ["parent 33 21"]
    );

    let next_verified = Frontier::new(Height(19), block::Hash([0x19; 32]));
    let only_verified = engine_snapshot(next_header, next_verified).frontiers;
    assert_eq!(
        summary(frontier_inputs(Some(only_header), only_verified)),
        ["tip 19", "done 25"]
    );
}

#[tokio::test]
async fn data_writer_delays_but_never_drops_behind_a_slow_reader() {
    use super::{
        codec::encode_body,
        service::{DataWriter, Written},
        wire::{message_type, DataQueueItem, WantEndReason},
    };

    // A one-frame channel stands in for a peer that stops reading.
    let (send, mut recv) = framed_channel(1);
    let mut writer = DataWriter::new(send);
    let block_id = block::Hash([3; 32]);
    let parts = encode_body(&Profile::TINY, &[5u8; 96], 2)
        .expect("TINY body encodes")
        .parts;
    for part in parts.iter().take(3) {
        writer.push(DataQueueItem::Part {
            block_id,
            stripe: 0,
            part: part.clone(),
            push: false,
        });
    }
    writer.push(DataQueueItem::Have {
        block_id,
        flags: super::wire::HaveFlags::Plain,
        indices: vec![0, 1, 2],
    });
    writer.push(DataQueueItem::WantEnd {
        want_id: 1,
        sent: 3,
        reason: WantEndReason::Served,
    });

    // The first part fills the channel; the second waits for the reader.
    assert!(matches!(writer.written().await, Some(Written::Part { .. })));
    writer.start();
    assert!(
        tokio::time::timeout(Duration::from_millis(50), writer.written())
            .await
            .is_err(),
        "a full output path makes the writer wait"
    );
    assert_eq!(writer.queue.len(), 3, "the waiting items stay queued");

    let reader = tokio::spawn(async move {
        let mut types = Vec::new();
        while types.len() < 5 {
            let frame = recv.recv().await.expect("the writer sends five frames");
            types.push(frame.message_type);
        }
        types
    });
    while writer.is_writing() {
        writer
            .written()
            .await
            .expect("the reader keeps the stream open");
        writer.start();
    }
    assert_eq!(
        reader.await.expect("the reader finishes"),
        vec![
            message_type::PART,
            message_type::PART,
            message_type::PART,
            message_type::HAVE,
            message_type::WANT_END,
        ],
        "every frame arrives, in order, with the Have and WantEnd behind the parts"
    );
}
