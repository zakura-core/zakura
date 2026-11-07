//! Tests for the Prometheus metrics endpoint.

use std::time::{Duration, Instant};

use rand_10::{rngs::StdRng, RngExt, SeedableRng};

use super::{
    endpoint,
    recorder::PrometheusRecorder,
    summary::{RollingSummary, Sketch, MAX_BUCKETS, QUANTILES, RELATIVE_ACCURACY, WINDOW_DURATION},
    text::{sanitize_description, sanitize_label_key, sanitize_label_value, sanitize_metric_name},
};

/// The sanitizing cases from `metrics-exporter-prometheus` 0.17's tests.
#[test]
fn sanitizes_like_metrics_exporter_prometheus() {
    for (input, expected) in [
        ("*", "_"),
        ("\"", "_"),
        ("foo_bar", "foo_bar"),
        ("foo1_bar", "foo1_bar"),
        ("1foobar", "_foobar"),
        ("foo1:bar2", "foo1:bar2"),
        ("123", "_23"),
        (
            "zcash.chain.verified.block.height",
            "zcash_chain_verified_block_height",
        ),
    ] {
        assert_eq!(
            sanitize_metric_name(input),
            expected,
            "metric name {input:?}"
        );
    }

    for (input, expected) in [
        ("*", "_"),
        ("\"", "_"),
        (":", "_"),
        ("foo_bar", "foo_bar"),
        ("1foobar", "_foobar"),
        ("__foobar", "__foobar"),
        ("foo1bar2", "foo1bar2"),
        ("123", "_23"),
    ] {
        assert_eq!(sanitize_label_key(input), expected, "label key {input:?}");
    }

    for (input, expected) in [
        ("*", "*"),
        ("\"", "\\\""),
        ("\\", "\\\\"),
        ("\\\\", "\\\\"),
        ("\n", "\\n"),
        ("foo_bar", "foo_bar"),
        ("1foobar", "1foobar"),
    ] {
        assert_eq!(
            sanitize_label_value(input),
            expected,
            "label value {input:?}"
        );
    }

    for (input, expected) in [
        ("*", "*"),
        ("\"", "\""),
        ("\\", "\\\\"),
        ("\\\\", "\\\\"),
        ("\n", "\\n"),
        ("foo_bar", "foo_bar"),
        ("1foobar", "1foobar"),
    ] {
        assert_eq!(
            sanitize_description(input),
            expected,
            "description {input:?}"
        );
    }
}

/// Checks every published quantile of `samples` against the exact sample at the same rank.
fn assert_quantiles_are_accurate(samples: &[f64]) {
    let mut sketch = Sketch::default();
    for &sample in samples {
        sketch.add(sample);
    }
    let mut sorted = samples.to_vec();
    sorted.sort_by(f64::total_cmp);

    for q in QUANTILES {
        let estimate = sketch.quantile(q).expect("the sketch has samples");
        // The sketch reports the sample at rank `q * (count - 1)`, rounded down.
        let exact = sorted[(q * (sorted.len() - 1) as f64) as usize];
        if q == 0.0 || q == 1.0 {
            assert_eq!(estimate, exact, "quantile {q} is exact");
        } else {
            assert!(
                (estimate - exact).abs() <= exact.abs() * RELATIVE_ACCURACY * (1.0 + 1e-9),
                "quantile {q}: estimate {estimate} is not within {RELATIVE_ACCURACY} of {exact}",
            );
        }
    }
}

#[test]
fn quantiles_are_within_the_relative_accuracy() {
    let mut rng = StdRng::seed_from_u64(1);

    // Latencies spanning microseconds to minutes.
    let latencies: Vec<f64> = (0..50_000)
        .map(|_| 10f64.powf(rng.random_range(-6.0..2.0)))
        .collect();
    assert_quantiles_are_accurate(&latencies);

    // Sizes with many repeated values.
    let sizes: Vec<f64> = (0..10_000)
        .map(|_| f64::from(rng.random_range(0..2_000_000u32)))
        .collect();
    assert_quantiles_are_accurate(&sizes);

    // Negative, zero, and positive samples.
    let mixed: Vec<f64> = (0..10_000)
        .map(|i| match i % 3 {
            0 => -rng.random_range(1.0..1000.0),
            1 => 0.0,
            _ => rng.random_range(1.0..1000.0),
        })
        .collect();
    assert_quantiles_are_accurate(&mixed);

    assert_quantiles_are_accurate(&[42.0]);
}

#[test]
fn merged_sketches_match_a_single_sketch() {
    let mut rng = StdRng::seed_from_u64(2);
    let samples: Vec<f64> = (0..3_000).map(|_| rng.random_range(-10.0..1e6)).collect();

    let mut single = Sketch::default();
    let mut parts = [Sketch::default(), Sketch::default(), Sketch::default()];
    for (i, &sample) in samples.iter().enumerate() {
        single.add(sample);
        parts[i % 3].add(sample);
    }
    let mut merged = Sketch::default();
    for part in &parts {
        merged.merge(part);
    }

    for q in QUANTILES {
        assert_eq!(merged.quantile(q), single.quantile(q), "quantile {q}");
    }
}

#[test]
fn bucket_count_is_bounded_and_keeps_high_quantiles_accurate() {
    let mut sketch = Sketch::default();
    // Values from 1e-9 to 1e290 need far more than `MAX_BUCKETS` buckets.
    let samples: Vec<f64> = (0..300_000)
        .map(|i| 10f64.powf(-9.0 + f64::from(i) * 1e-3))
        .collect();
    for &sample in &samples {
        sketch.add(sample);
    }
    assert!(sketch.positive_bucket_count() <= MAX_BUCKETS);

    // The largest values keep their accuracy, because only the smallest buckets are merged.
    let exact = samples[(0.999 * (samples.len() - 1) as f64) as usize];
    let estimate = sketch.quantile(0.999).expect("the sketch has samples");
    assert!((estimate - exact).abs() <= exact * RELATIVE_ACCURACY * (1.0 + 1e-9));
}

#[test]
fn quantiles_cover_the_last_three_windows() {
    let start = Instant::now();
    let mut summary = RollingSummary::new(start);
    summary.add(1.0, start);
    summary.add(2.0, start + WINDOW_DURATION);
    summary.add(3.0, start + WINDOW_DURATION * 2);

    let recent = summary.recent(start + WINDOW_DURATION * 3 - Duration::from_millis(1));
    assert_eq!(recent.quantile(0.0), Some(1.0));
    assert_eq!(recent.quantile(1.0), Some(3.0));

    // A window is dropped once it started three windows ago.
    let recent = summary.recent(start + WINDOW_DURATION * 3);
    assert_eq!(recent.quantile(0.0), Some(2.0));

    // Old windows are dropped when new ones start, but the sum and count keep every sample.
    summary.add(4.0, start + WINDOW_DURATION * 10);
    let recent = summary.recent(start + WINDOW_DURATION * 10);
    assert_eq!(recent.quantile(0.0), Some(4.0));
    assert_eq!(summary.count(), 4);
    assert_eq!(summary.sum(), 10.0);

    // Non-finite samples are counted and summed, but not used for quantiles.
    summary.add(f64::INFINITY, start + WINDOW_DURATION * 10);
    assert_eq!(summary.count(), 5);
    assert_eq!(summary.sum(), f64::INFINITY);
    assert_eq!(
        summary.recent(start + WINDOW_DURATION * 10).quantile(1.0),
        Some(4.0)
    );

    assert_eq!(
        RollingSummary::new(start).recent(start).quantile(0.5),
        None,
        "an empty summary has no quantiles"
    );
}

#[test]
fn renders_like_metrics_exporter_prometheus() {
    let recorder = PrometheusRecorder::default();
    metrics::with_local_recorder(&recorder, || {
        metrics::describe_counter!("sync.blocks", "Blocks \"synced\"\nfrom peers");
        metrics::counter!("sync.blocks").increment(3);
        metrics::counter!("peer.messages", "kind" => "inv", "dir" => "in").increment(2);
        metrics::counter!("peer.messages", "kind" => "tx", "dir" => "in").increment(5);
        metrics::gauge!("state.tip.height").set(1234.5);
        metrics::gauge!("1bad-name", "bad key" => "va\"lue").set(-1.0);
        metrics::histogram!("rpc.latency", "method" => "getinfo").record(0.25);
        metrics::histogram!("rpc.latency", "method" => "getinfo").record(0.75);
        let _empty = metrics::histogram!("rpc.empty");
    });

    assert_eq!(
        recorder.render(Instant::now()),
        r#"# TYPE peer_messages counter
peer_messages{kind="inv",dir="in"} 2
peer_messages{kind="tx",dir="in"} 5

# HELP sync_blocks Blocks "synced"\nfrom peers
# TYPE sync_blocks counter
sync_blocks 3

# TYPE _bad_name gauge
_bad_name{bad_key="va\"lue"} -1

# TYPE state_tip_height gauge
state_tip_height 1234.5

# TYPE rpc_empty summary
rpc_empty{quantile="0"} 0
rpc_empty{quantile="0.5"} 0
rpc_empty{quantile="0.9"} 0
rpc_empty{quantile="0.95"} 0
rpc_empty{quantile="0.99"} 0
rpc_empty{quantile="0.999"} 0
rpc_empty{quantile="1"} 0
rpc_empty_sum 0
rpc_empty_count 0

# TYPE rpc_latency summary
rpc_latency{method="getinfo",quantile="0"} 0.25
rpc_latency{method="getinfo",quantile="0.5"} 0.24982341209247752
rpc_latency{method="getinfo",quantile="0.9"} 0.24982341209247752
rpc_latency{method="getinfo",quantile="0.95"} 0.24982341209247752
rpc_latency{method="getinfo",quantile="0.99"} 0.24982341209247752
rpc_latency{method="getinfo",quantile="0.999"} 0.24982341209247752
rpc_latency{method="getinfo",quantile="1"} 0.75
rpc_latency_sum{method="getinfo"} 1
rpc_latency_count{method="getinfo"} 2

"#
    );
}

#[tokio::test]
async fn endpoint_serves_the_metrics() {
    let recorder = PrometheusRecorder::default();
    metrics::with_local_recorder(&recorder, || {
        metrics::counter!("endpoint.test").increment(7);
    });
    let addr = endpoint::spawn("127.0.0.1:0".parse().expect("valid address"), recorder)
        .expect("the endpoint starts");

    let response = zakura_node_services::rpc_client::RpcRequestClient::new(addr)
        .get()
        .await
        .expect("the endpoint answers");

    assert_eq!(response.status(), 200);
    assert_eq!(response.headers()["content-type"], "text/plain");
    assert_eq!(
        String::from_utf8_lossy(response.body()),
        "# TYPE endpoint_test counter\nendpoint_test 7\n\n"
    );
}

#[test]
fn delayed_writer_preserves_live_windows() {
    use std::sync::{mpsc, Arc, Mutex};

    let start = Instant::now();
    let summary = Arc::new(Mutex::new(RollingSummary::new(start)));
    summary.lock().unwrap().add(1.0, start);
    let (captured_tx, captured_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();

    // Model record's timestamp-before-lock boundary without sleeping or depending on
    // mutex fairness: the older writer is suspended after capturing its timestamp.
    std::thread::scope(|scope| {
        let delayed_summary = Arc::clone(&summary);
        scope.spawn(move || {
            let now = start + WINDOW_DURATION - Duration::from_millis(1);
            captured_tx.send(()).unwrap();
            resume_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            delayed_summary.lock().unwrap().add(2.0, now);
        });
        captured_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        summary.lock().unwrap().add(100.0, start + WINDOW_DURATION);
        resume_tx.send(()).unwrap();
    });
    let mut summary = summary.lock().unwrap();
    summary.add(100.0, start + WINDOW_DURATION + Duration::from_millis(1));
    let recent = summary.recent(start + WINDOW_DURATION + Duration::from_millis(2));
    assert_eq!(recent.quantile(0.0), Some(1.0));
    assert_eq!(recent.quantile(1.0), Some(100.0));
    let median = recent.quantile(0.5).unwrap();
    assert!((median - 2.0).abs() <= 2.0 * RELATIVE_ACCURACY);
    assert_eq!(summary.count(), 4);
    assert_eq!(summary.sum(), 203.0);

    // Both samples from window zero expire together at the original boundary.
    assert_eq!(
        summary.recent(start + WINDOW_DURATION * 3).quantile(0.0),
        Some(100.0)
    );
}

#[test]
fn delayed_windows_do_not_displace_newer_windows() {
    let start = Instant::now();
    let mut summary = RollingSummary::new(start);
    for (index, value) in [(3, 3.0), (5, 5.0), (4, 4.0), (0, 100.0), (3, 30.0)] {
        summary.add(value, start + WINDOW_DURATION * index);
    }
    let recent = summary.recent(start + WINDOW_DURATION * 5);
    assert_eq!(recent.quantile(0.0), Some(3.0));
    assert_eq!(recent.quantile(1.0), Some(30.0));
    assert_eq!(summary.count(), 5);
    assert_eq!(summary.sum(), 142.0);
    assert_eq!(
        summary.recent(start + WINDOW_DURATION * 6).quantile(0.0),
        Some(4.0)
    );
    assert_eq!(
        summary.recent(start + WINDOW_DURATION * 8).quantile(0.5),
        None
    );
}

#[tokio::test]
async fn endpoint_preserves_health_and_other_paths() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let recorder = PrometheusRecorder::default();
    metrics::with_local_recorder(&recorder, || {
        metrics::counter!("endpoint.test").increment(7);
    });
    let addr = endpoint::spawn("127.0.0.1:0".parse().unwrap(), recorder).unwrap();
    for (method, path, expected) in [
        ("GET", "/health", "OK"),
        ("GET", "/health?probe=1", "OK"),
        ("POST", "/health", "OK"),
        (
            "GET",
            "/metrics",
            "# TYPE endpoint_test counter\nendpoint_test 7\n\n",
        ),
        (
            "GET",
            "/health/",
            "# TYPE endpoint_test counter\nendpoint_test 7\n\n",
        ),
        (
            "POST",
            "/other",
            "# TYPE endpoint_test counter\nendpoint_test 7\n\n",
        ),
    ] {
        let response = tokio::time::timeout(Duration::from_secs(5), async {
            let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
            let request = format!(
                "{method} {path} HTTP/1.1\r\n\
                 Host: localhost\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
            );
            stream.write_all(request.as_bytes()).await.unwrap();
            let mut response = String::new();
            stream.read_to_string(&mut response).await.unwrap();
            response
        })
        .await
        .expect("the endpoint answers within five seconds");
        let (headers, body) = response.split_once("\r\n\r\n").unwrap();
        assert!(
            headers.starts_with("HTTP/1.1 200 OK\r\n"),
            "{method} {path}: {headers}"
        );
        assert!(headers.contains("content-type: text/plain\r\n"));
        assert_eq!(body, expected, "{method} {path}");
    }
}
