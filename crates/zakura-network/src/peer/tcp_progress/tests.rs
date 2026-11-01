//! Tests for TCP upload progress sampling and the upload rate estimator.

use std::time::{Duration, Instant};

use super::*;

const SECOND: Duration = Duration::from_secs(1);

fn close(actual: Option<f64>, expected: f64) {
    let actual = actual.expect("the estimator has an estimate");
    assert!(
        (actual - expected).abs() < 1e-6 * expected.max(1.0),
        "estimate {actual} is not {expected}"
    );
}

#[test]
fn estimator_has_no_estimate_before_bytes_are_acknowledged() {
    let t0 = Instant::now();
    let mut estimator = UploadRateEstimator::new(SECOND);
    assert_eq!(estimator.bytes_per_second(), None);

    estimator.upload_started(t0);
    // Bytes acknowledged at the same instant the upload started cover no busy time, so they
    // wait for the next observation.
    estimator.record_acked(500, t0);
    assert_eq!(estimator.bytes_per_second(), None);

    estimator.record_acked(500, t0 + SECOND);
    close(estimator.bytes_per_second(), 1_000.0);
}

#[test]
fn estimator_ignores_time_with_no_upload_outstanding() {
    let t0 = Instant::now();
    let mut estimator = UploadRateEstimator::new(SECOND);

    estimator.upload_started(t0);
    estimator.record_acked(1_000_000, t0 + SECOND);
    estimator.upload_finished(t0 + SECOND);
    close(estimator.bytes_per_second(), 1_000_000.0);

    // Ten idle seconds, then another upload at the same rate.
    let t1 = t0 + 11 * SECOND;
    estimator.upload_started(t1);
    estimator.record_acked(1_000_000, t1 + SECOND);
    estimator.upload_finished(t1 + SECOND);
    close(estimator.bytes_per_second(), 1_000_000.0);
    assert_eq!(estimator.outstanding(), 0);
}

#[test]
fn estimator_counts_overlapping_uploads_busy_time_once() {
    let t0 = Instant::now();
    let mut estimator = UploadRateEstimator::new(SECOND);

    estimator.upload_started(t0);
    estimator.upload_started(t0 + SECOND / 2);
    assert_eq!(estimator.outstanding(), 2);
    estimator.record_acked(2_000_000, t0 + SECOND);
    close(estimator.bytes_per_second(), 2_000_000.0);

    // One upload finishing leaves the other one keeping the clock running.
    estimator.upload_finished(t0 + SECOND);
    estimator.record_acked(1_000_000, t0 + 2 * SECOND);
    let weight = 1.0 - (-1.0f64).exp();
    close(
        estimator.bytes_per_second(),
        2_000_000.0 + weight * (1_000_000.0 - 2_000_000.0),
    );
}

#[test]
fn estimator_weights_observations_by_busy_time() {
    let t0 = Instant::now();
    let mut estimator = UploadRateEstimator::new(2 * SECOND);
    estimator.upload_started(t0);
    estimator.record_acked(1_000, t0 + SECOND);

    // A 4 s observation at 3000 B/s moves the estimate 1 - e^-2 of the way there.
    estimator.record_acked(12_000, t0 + 5 * SECOND);
    let long_weight = 1.0 - (-2.0f64).exp();
    let after_long = 1_000.0 + long_weight * (3_000.0 - 1_000.0);
    close(estimator.bytes_per_second(), after_long);

    // A 100 ms observation at a much lower rate moves it only 1 - e^-0.05 of the way.
    estimator.record_acked(10, t0 + 5 * SECOND + SECOND / 10);
    let short_weight = 1.0 - (-0.05f64).exp();
    close(
        estimator.bytes_per_second(),
        after_long + short_weight * (100.0 - after_long),
    );
}

#[test]
fn estimator_with_zero_time_constant_tracks_the_latest_observation() {
    let t0 = Instant::now();
    let mut estimator = UploadRateEstimator::new(Duration::ZERO);
    estimator.upload_started(t0);
    estimator.record_acked(1_000, t0 + SECOND);
    estimator.record_acked(5_000, t0 + 2 * SECOND);
    close(estimator.bytes_per_second(), 5_000.0);
}

#[tokio::test]
async fn in_memory_streams_have_no_tcp_socket() {
    let (stream, _peer) = tokio::io::duplex(64);
    assert!(TcpSocket::from_transport(&stream).is_none());
}

#[cfg(target_os = "linux")]
mod linux {
    use std::sync::Arc;

    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        sync::Notify,
    };

    use super::*;

    /// The uploaded message: about the size of a large block body.
    const MESSAGE_LEN: usize = 2 * 1024 * 1024;

    /// The receiver's socket buffer. Small enough that the receiver's window, not the sender's
    /// buffer, limits how much of the message the peer can acknowledge before it reads.
    const RECEIVER_BUFFER: u32 = 64 * 1024;

    /// How long the receiver waits after the write returns before it starts reading.
    const READ_DELAY: Duration = Duration::from_millis(300);

    async fn connected_pair() -> (TcpStream, TcpStream) {
        let socket = tokio::net::TcpSocket::new_v4().expect("socket");
        // Set before listening, so accepted sockets inherit it and advertise a small window.
        socket
            .set_recv_buffer_size(RECEIVER_BUFFER)
            .expect("set receive buffer");
        socket
            .bind("127.0.0.1:0".parse().expect("address"))
            .expect("bind");
        let listener: TcpListener = socket.listen(1).expect("listen");
        let addr = listener.local_addr().expect("listener address");
        let (sender, accepted) = tokio::join!(TcpStream::connect(addr), listener.accept());
        (sender.expect("connect"), accepted.expect("accept").0)
    }

    /// A 2 MB write to a receiver that is not reading returns once its bytes are queued in the
    /// sender's kernel buffer. The watch reports the message acknowledged only after the
    /// receiver starts reading, and the acknowledged byte count tracks the receiver's progress.
    #[tokio::test]
    async fn upload_is_acked_after_the_receiver_drains_not_when_the_write_returns() {
        let (mut sender, mut receiver) = connected_pair().await;
        let progress = UploadProgress::new(
            TcpSocket::from_transport(&sender).expect("a TCP stream on Linux has a socket"),
        );

        // Reads nothing until told to, then drains the whole message.
        let start_reading = Arc::new(Notify::new());
        let reader = tokio::spawn({
            let start_reading = start_reading.clone();
            async move {
                start_reading.notified().await;
                let drain_started = Instant::now();
                let mut message = vec![0; MESSAGE_LEN];
                receiver
                    .read_exact(&mut message)
                    .await
                    .expect("the receiver reads the whole message");
                (drain_started, Instant::now())
            }
        });

        let message_len = u64::try_from(MESSAGE_LEN).expect("fits in u64");
        let watch = progress.start(message_len).expect("the socket is readable");
        let watcher = tokio::spawn(watch.wait_acked());

        tokio::time::timeout(
            Duration::from_secs(10),
            sender.write_all(&vec![7; MESSAGE_LEN]),
        )
        .await
        .expect("the message fits in the sender's kernel buffer")
        .expect("the write succeeds");
        let written = progress.sample().expect("the socket is readable");
        assert!(
            written.bytes_acked < message_len,
            "the write returned before the peer acknowledged the message: {written:?}"
        );
        assert!(
            written.notsent_bytes.expect("Linux 4.6 or later") > 0,
            "the kernel holds unsent bytes after the write returns: {written:?}"
        );
        assert!(!watcher.is_finished(), "the watch outlives the write");

        tokio::time::sleep(READ_DELAY).await;
        assert!(
            !watcher.is_finished(),
            "nothing is acknowledged past the receiver's window"
        );
        let waiting = progress.sample().expect("the socket is readable");
        assert!(
            waiting.bytes_acked < message_len / 2,
            "only the receiver's window is acknowledged before it reads: {waiting:?}"
        );
        start_reading.notify_one();

        let (drain_started, drained) = reader.await.expect("reader task");
        let report = tokio::time::timeout(Duration::from_secs(10), watcher)
            .await
            .expect("the watch ends")
            .expect("watch task");

        assert_eq!(report.outcome, UploadOutcome::Acked);
        assert_eq!(report.acked_message_bytes(), message_len);
        assert!(
            report.sample_count
                >= (READ_DELAY.as_millis() / SAMPLE_INTERVAL.as_millis()) as u64 / 2,
            "the watch sampled while the receiver waited: {report:?}"
        );
        let acked_at = report.started_at + report.elapsed;
        assert!(
            acked_at >= drain_started,
            "the message is acknowledged only after the receiver starts reading"
        );
        assert!(
            acked_at <= drained + 4 * SAMPLE_INTERVAL,
            "the watch sees the acknowledgement within a few samples of the drain"
        );

        // Zero-window probes may count as retransmissions, so only check the counter is read.
        assert!(report.retransmits().is_some());
    }

    /// Once the connection drops its socket handle, a running watch stops sampling.
    #[tokio::test]
    async fn watch_ends_when_the_connection_drops_its_socket() {
        let (sender, _receiver) = connected_pair().await;
        let progress = UploadProgress::new(TcpSocket::from_transport(&sender).expect("socket"));
        // A message that is never written is never acknowledged.
        let watch = progress.start(1_000).expect("the socket is readable");
        drop(progress);

        let report = tokio::time::timeout(Duration::from_secs(5), watch.wait_acked())
            .await
            .expect("the watch ends once the connection is gone");
        assert_eq!(report.outcome, UploadOutcome::Closed);
        assert_eq!(report.sample_count, 0);
        assert!(report.last_sample.is_none());
    }

    /// Duplicates stop when the budget is spent, and a closed connection's share returns to it.
    /// A watch that outlives its connection holds no share.
    #[tokio::test]
    async fn descriptor_budget_limits_duplicates_until_a_connection_closes() {
        let (sender, _receiver) = connected_pair().await;
        let budget = Arc::new(Semaphore::new(1));

        let first = TcpSocket::duplicate(&sender, &budget).expect("the budget has room");
        assert!(
            TcpSocket::duplicate(&sender, &budget).is_none(),
            "the budget is spent"
        );

        let weak = first.downgrade();
        drop(first);
        assert!(weak.sample().is_none(), "the duplicate closed");
        assert!(
            TcpSocket::duplicate(&sender, &budget).is_some(),
            "closing the connection returned its share"
        );
    }

    #[test]
    fn descriptor_budget_is_an_eighth_of_the_open_file_limit() {
        assert_eq!(descriptor_budget(Some(512)), 64);
        assert_eq!(descriptor_budget(Some(1024)), 128);
        assert_eq!(descriptor_budget(None), 128);
        assert_eq!(descriptor_budget(Some(0)), 1);
        // `RLIM_INFINITY`.
        assert_eq!(descriptor_budget(Some(u64::MAX)), 1 << 20);
    }

    /// A connection watches at most [`MAX_WATCHED_UPLOADS`] uploads at once.
    #[tokio::test]
    async fn watches_per_connection_are_bounded() {
        let (sender, _receiver) = connected_pair().await;
        let progress = UploadProgress::new(TcpSocket::from_transport(&sender).expect("socket"));

        let watches: Vec<_> = (0..MAX_WATCHED_UPLOADS)
            .map(|_| progress.start(1_000).expect("below the limit"))
            .collect();
        assert!(progress.start(1_000).is_none());

        drop(watches);
        assert!(progress.start(1_000).is_some());
    }
}

#[test]
fn upload_target_retries_acknowledgements_between_socket_reads() {
    let mut acknowledgements = [100, 150, 150].into_iter();
    let mut queues = [50, 50].into_iter();
    let (info, queued) = upload_start_snapshot(
        || {
            Some(TcpInfo {
                bytes_acked: acknowledgements.next().unwrap(),
                ..TcpInfo::default()
            })
        },
        || queues.next(),
    )
    .expect("the second snapshot has a stable ACK counter");
    assert_eq!(info.bytes_acked + queued, 200);
}

#[test]
fn upload_target_skips_continuously_changing_acknowledgements() {
    let mut bytes_acked = 0;
    let result = upload_start_snapshot(
        || {
            bytes_acked += 1;
            Some(TcpInfo {
                bytes_acked,
                ..TcpInfo::default()
            })
        },
        || Some(100),
    );
    assert!(result.is_none());
    assert_eq!(bytes_acked, 4);
}
