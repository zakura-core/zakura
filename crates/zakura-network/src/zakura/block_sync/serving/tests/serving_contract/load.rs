//! Bounded workload qualification. Limits belong to this declared configuration.

#![allow(
    clippy::print_stderr,
    reason = "retain measured load evidence in test logs"
)]

use super::*;
use zakura_test::resources::{load_rounds, ProcessUsage};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn l01_maximum_admitted_peers_keep_a_bounded_serving_envelope() {
    for shape in ["small", "large", "mixed"] {
        let mut source = ControlledSource::new(
            100,
            if shape == "small" { 128 } else { 20 },
            shape != "small",
            false,
        );
        if shape == "mixed" {
            let small = ControlledSource::new(101, 1, false, false);
            Arc::make_mut(&mut Arc::get_mut(&mut source).unwrap().encoded).insert(
                block::Height(101),
                small.encoded[&block::Height(101)].clone(),
            );
        }
        let config = config(2);
        let regulator = GetBlocksServingRegulator::new(config.clone());
        let registry = Arc::new(PeerRegistry::new());
        let mut peers: Vec<_> = (1..=8)
            .map(|peer| {
                source.shared_fixture(1, peer, config.clone(), regulator.clone(), registry.clone())
            })
            .collect();
        let rounds = load_rounds();
        let start = Instant::now();
        let before = ProcessUsage::sample().ok();
        let mut wire_bytes = 0usize;
        let maximum_decoded: u64 = source
            .encoded
            .values()
            .take_while(|encoded| {
                wire_bytes += encoded.len();
                wire_bytes <= usize::try_from(config.max_response_bytes).unwrap()
            })
            .map(|encoded| {
                block::Block::zcash_deserialize(encoded.as_slice())
                    .unwrap()
                    .attributed_memory_size_bytes()
            })
            .sum();
        // Two active owners, each with a decoded response, one lookahead and
        // bounded encoding growth. Immutable database fixture bytes are separate.
        let decoded_limit = 2 * maximum_decoded;
        for _ in 0..rounds {
            for f in &peers {
                request(f, 100, 128).await;
            }
            futures::future::join_all(
                peers
                    .iter_mut()
                    .map(|f| response(f, &source, 100, 128, config.max_response_bytes, 128)),
            )
            .await;
            assert!(source.probe.snapshot().peak_running <= 2);
            assert!(
                source.peak_decoded.load(Ordering::Relaxed) <= decoded_limit,
                "L01 live decoded results exceed configured workers times one full result"
            );
            assert!(regulator.snapshot().node_active <= 2);
            assert!(peers
                .iter()
                .all(|f| !f.session.cancel_token().is_cancelled()));
        }
        source.probe.wait_finished(rounds * 8).await;
        let after = ProcessUsage::sample().ok();
        eprintln!("L01 shape={shape}, peers=8, workers=2, output_slots=1, rounds={rounds}, request_count=128, response_cap={}, wall={:?}, cpu={:?}, process_peak_rss={:?}, storage={:?}, decoded_peak={}, decoded_limit={decoded_limit}",
            config.max_response_bytes, start.elapsed(), before.zip(after).map(|(before, after)| after.cpu.saturating_sub(before.cpu)), after.map(|usage| usage.peak_resident_bytes), source.probe.snapshot(), source.peak_decoded.load(Ordering::Relaxed));
        assert_eq!(regulator.locks_for_test().acquisitions, 8);
        eprintln!(
            "L01 actual admission registry lock: {:?}",
            regulator.locks_for_test()
        );
        for f in peers {
            f.finish().await;
        }
        assert_eq!(regulator.snapshot().node_active, 0);
        assert_eq!(source.probe.snapshot().running, 0);
        assert_eq!(source.live_decoded_bytes(), 0);
    }
}

async fn empty_fairness(inside_window: bool, tiny_success: bool) {
    let mut source = ControlledSource::new(100, 3, false, false);
    if inside_window && !tiny_success {
        Arc::make_mut(&mut Arc::get_mut(&mut source).unwrap().encoded).remove(&block::Height(101));
    }
    let config = config(1);
    let regulator = GetBlocksServingRegulator::new(config.clone());
    let registry = Arc::new(PeerRegistry::new());
    let mut busy = source.shared_fixture(1, 1, config.clone(), regulator.clone(), registry.clone());
    let mut runnable = source.shared_fixture(1, 2, config.clone(), regulator.clone(), registry);
    let iterations = load_rounds() * 64;
    let start = if inside_window || tiny_success {
        101
    } else {
        200
    };
    let busy_source = source.clone();
    let mut busy_task = AbortOnDropHandle::new(tokio::spawn(async move {
        for _ in 0..iterations {
            request(&busy, start, 1).await;
            response(
                &mut busy,
                &busy_source,
                start,
                1,
                config.max_response_bytes,
                128,
            )
            .await;
            assert!(!busy.session.cancel_token().is_cancelled());
        }
        busy.finish().await;
    }));
    request(&runnable, 100, 1).await;
    // The single worker must give this peer an opportunity while another peer
    // repeatedly completes legal empty/tiny exchanges without output pressure.
    time::timeout(
        DEADLINE,
        response(
            &mut runnable,
            &source,
            100,
            1,
            config.max_response_bytes,
            128,
        ),
    )
    .await
    .unwrap();
    time::timeout(Duration::from_secs(30), &mut busy_task)
        .await
        .unwrap()
        .unwrap();
    source
        .probe
        .wait_finished(if inside_window || tiny_success {
            iterations + 1
        } else {
            1
        })
        .await;
    assert_eq!(
        source.probe.snapshot().started,
        if inside_window || tiny_success {
            iterations + 1
        } else {
            1
        },
        "L02 outside-window requests do not invoke storage"
    );
    assert!(source.probe.snapshot().peak_running <= 1);
    runnable.finish().await;
    assert_eq!(regulator.snapshot().node_active, 0);
}

#[tokio::test]
async fn l02_outside_window_empty_responses_yield_without_storage() {
    empty_fairness(false, false).await;
}

#[tokio::test]
async fn l02_missing_inside_window_responses_yield_between_lookups() {
    empty_fairness(true, false).await;
}

#[tokio::test]
async fn l02_tiny_successful_responses_leave_a_runnable_peer_an_opportunity() {
    empty_fairness(true, true).await;
}

#[tokio::test]
async fn l03_nonreaders_and_reconnects_preserve_retained_work_and_recover() {
    for same_identity in [false, true] {
        let source = ControlledSource::new(100, 3, true, false);
        let config = config(2);
        let regulator = GetBlocksServingRegulator::new(config.clone());
        let registry = Arc::new(PeerRegistry::new());
        let mut old =
            source.shared_fixture(1, 1, config.clone(), regulator.clone(), registry.clone());
        request(&old, 100, 3).await;
        source.probe.wait_finished(1).await;
        let retained = time::timeout(DEADLINE, old.data.recv())
            .await
            .unwrap()
            .unwrap();
        old.task.abort();
        assert!(time::timeout(DEADLINE, &mut old.task)
            .await
            .unwrap()
            .unwrap_err()
            .is_cancelled());
        registry.remove_session(old.session.peer_id(), old.session.session_id());
        drop(old);
        assert_eq!(regulator.snapshot().node_active, 1);
        for attempt in 0..8u8 {
            let peer = if same_identity { 1 } else { attempt + 2 };
            let mut replacement = Fixture::with_resources(
                source.clone(),
                1,
                config.clone(),
                regulator.clone(),
                registry.clone(),
                peer,
            );
            // Incomplete Status setup is a separate bounded phase. It must not
            // clear the old generation or start database work.
            request(&replacement, 100, 3).await;
            tokio::task::yield_now().await;
            replacement.task.abort();
            assert!(time::timeout(DEADLINE, &mut replacement.task)
                .await
                .unwrap()
                .unwrap_err()
                .is_cancelled());
            registry.remove_session(
                replacement.session.peer_id(),
                replacement.session.session_id(),
            );
            drop(replacement);
            assert_eq!(
                regulator.snapshot().node_active,
                1,
                "L03 retained old write remains charged across setup churn"
            );
        }
        assert_eq!(source.probe.snapshot().started, 1);
        drop(retained);
        let mut replacement =
            source.shared_fixture(1, 1, config.clone(), regulator.clone(), registry);
        request(&replacement, 100, 3).await;
        response(
            &mut replacement,
            &source,
            100,
            3,
            config.max_response_bytes,
            128,
        )
        .await;
        replacement.finish().await;
        assert_eq!(regulator.snapshot().node_active, 0);
        assert_eq!(source.live_decoded_bytes(), 0);
    }
}
