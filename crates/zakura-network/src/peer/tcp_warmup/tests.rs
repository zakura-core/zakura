//! Tests for the TCP warm-up coordinator and driver.

use std::net::Ipv4Addr;

use super::*;

const PEER: IpAddr = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1));

fn enabled_config() -> TcpWarmupConfig {
    TcpWarmupConfig {
        enabled: true,
        ..TcpWarmupConfig::default()
    }
}

/// Returns a coordinator whose tip last changed at `tip_changed_at`, and the sender that
/// reports later changes.
fn warmup(
    config: TcpWarmupConfig,
    tip_changed_at: Option<Instant>,
) -> (TcpWarmup, watch::Sender<Option<Instant>>) {
    let (tip_tx, tip_rx) = watch::channel(tip_changed_at);
    (TcpWarmup::new(config, tip_rx), tip_tx)
}

/// Writes `chunk` the way a connection does, and returns its pong sender.
fn write(chunk: WarmupChunk) -> oneshot::Sender<()> {
    chunk
        .written
        .send(())
        .expect("the driver waits for the write");
    chunk.pong
}

/// Answers each warm-up ping on `link` with an immediate pong, and returns the paddings in
/// the order they arrived once the driver closes the link.
async fn answer_all(mut link: mpsc::Receiver<WarmupChunk>) -> Vec<u32> {
    let mut paddings = Vec::new();
    while let Some(chunk) = link.recv().await {
        paddings.push(chunk.padding);
        let _ = write(chunk).send(());
    }
    paddings
}

/// Answers the probe on `link`, and returns the first padded ping without writing it.
async fn first_padded_ping(link: &mut mpsc::Receiver<WarmupChunk>) -> WarmupChunk {
    let probe = link.recv().await.expect("the driver sends the probe");
    assert_eq!(probe.padding, 0);
    let _ = write(probe).send(());
    let chunk = link.recv().await.expect("the driver sends a padded ping");
    assert!(chunk.padding > 0);
    chunk
}

#[test]
fn default_schedule_doubles_to_the_message_limit() {
    let schedule = TcpWarmupConfig::default().schedule();

    assert_eq!(
        schedule,
        [
            0,
            14_000,
            28_000,
            56_000,
            112_000,
            224_000,
            448_000,
            896_000,
            1_792_000,
            crate::constants::MAX_PING_PADDING,
        ]
    );
}

#[test]
fn schedule_with_equal_first_and_max_sends_one_padded_ping() {
    let config = TcpWarmupConfig {
        first_chunk_bytes: 1000,
        max_chunk_bytes: 1000,
        ..TcpWarmupConfig::default()
    };
    assert_eq!(config.schedule(), [0, 1000]);

    let config = TcpWarmupConfig {
        first_chunk_bytes: 1000,
        max_chunk_bytes: 3000,
        ..TcpWarmupConfig::default()
    };
    assert_eq!(config.schedule(), [0, 1000, 2000, 3000]);
}

#[test]
fn validation_rejects_settings_that_peers_or_connections_cannot_take() {
    assert!(TcpWarmupConfig::default().validate().is_ok());

    for config in [
        TcpWarmupConfig {
            max_chunk_bytes: crate::constants::MAX_PING_PADDING + 1,
            ..TcpWarmupConfig::default()
        },
        TcpWarmupConfig {
            first_chunk_bytes: 0,
            ..TcpWarmupConfig::default()
        },
        TcpWarmupConfig {
            first_chunk_bytes: 4096,
            max_chunk_bytes: 1024,
            ..TcpWarmupConfig::default()
        },
        TcpWarmupConfig {
            max_concurrent: 0,
            ..TcpWarmupConfig::default()
        },
        TcpWarmupConfig {
            chunk_timeout: Duration::ZERO,
            ..TcpWarmupConfig::default()
        },
        TcpWarmupConfig {
            chunk_timeout: crate::constants::REQUEST_TIMEOUT / 2,
            ..TcpWarmupConfig::default()
        },
    ] {
        assert!(config.validate().is_err(), "{config:?} should be rejected");
    }
}

#[tokio::test(start_paused = true)]
async fn a_claim_lasts_for_the_cooldown() {
    let (warmup, _tip) = warmup(enabled_config(), None);

    let mut claim = warmup.claim(PEER).expect("a new peer can be claimed");
    claim.keep();
    drop(claim);
    assert!(warmup.claim(PEER).is_none(), "a warmed peer is in cooldown");

    tokio::time::advance(enabled_config().peer_cooldown).await;
    assert!(
        warmup.claim(PEER).is_some(),
        "a peer can be warmed again after the cooldown"
    );
}

#[tokio::test(start_paused = true)]
async fn a_claim_without_padding_is_released() {
    let (warmup, _tip) = warmup(enabled_config(), None);

    let claim = warmup.claim(PEER).expect("a new peer can be claimed");
    assert!(
        warmup.claim(PEER).is_none(),
        "a warm-up in progress holds its claim"
    );
    drop(claim);
    assert!(
        warmup.claim(PEER).is_some(),
        "a warm-up that sent no padding does not start the cooldown"
    );
}

#[tokio::test(start_paused = true)]
async fn a_full_claim_map_forgets_its_oldest_peer() {
    let (warmup, _tip) = warmup(enabled_config(), None);

    let ips: Vec<IpAddr> = (0..=MAX_REMEMBERED_PEERS)
        .map(|index| {
            let index = u32::try_from(index).expect("the test count fits in u32");
            IpAddr::V4(Ipv4Addr::from(0x0a00_0000 + index))
        })
        .collect();
    for ip in &ips {
        warmup.claim(*ip).expect("each peer is new").keep();
        tokio::time::advance(Duration::from_millis(1)).await;
    }

    assert!(
        warmup.claim(ips[0]).is_some(),
        "the oldest peer was forgotten"
    );
    assert!(warmup.claim(ips[MAX_REMEMBERED_PEERS]).is_none());
}

#[tokio::test(start_paused = true)]
async fn run_sends_the_schedule_in_order_once_per_peer() {
    let (warmup, _tip) = warmup(enabled_config(), None);

    let (link, chunks) = mpsc::channel(WARMUP_QUEUE);
    let answers = tokio::spawn(answer_all(chunks));
    warmup.clone().run(PEER, link).await;
    assert_eq!(answers.await.unwrap(), enabled_config().schedule());

    let (link, chunks) = mpsc::channel(WARMUP_QUEUE);
    let answers = tokio::spawn(answer_all(chunks));
    warmup.run(PEER, link).await;
    assert!(
        answers.await.unwrap().is_empty(),
        "a second connection to a warmed peer is not warmed"
    );
}

#[tokio::test(start_paused = true)]
async fn run_stops_on_a_missing_pong_and_keeps_no_claim_without_padding() {
    let config = enabled_config();
    let (warmup, _tip) = warmup(config.clone(), None);

    let (link, mut chunks) = mpsc::channel::<WarmupChunk>(WARMUP_QUEUE);
    let started = Instant::now();
    let unanswered = tokio::spawn(async move {
        let probe = chunks.recv().await.expect("the driver sends the probe");
        assert_eq!(probe.padding, 0);
        let pong = write(probe);
        // Hold the pong sender and the link, so only the timeout ends the wait.
        let _ = chunks.recv().await;
        drop(pong);
    });
    warmup.clone().run(PEER, link).await;

    assert_eq!(started.elapsed(), config.chunk_timeout);
    unanswered.await.unwrap();
    assert!(
        warmup.claim(PEER).is_some(),
        "a warm-up that stopped after the probe can be retried"
    );
}

#[tokio::test(start_paused = true)]
async fn run_ends_when_the_connection_closes() {
    let (warmup, _tip) = warmup(enabled_config(), None);

    let (link, chunks) = mpsc::channel(WARMUP_QUEUE);
    drop(chunks);
    warmup.clone().run(PEER, link).await;

    assert!(warmup.claim(PEER).is_some());
}

#[tokio::test(start_paused = true)]
async fn padded_pings_wait_for_a_quiet_tip() {
    let config = enabled_config();
    let (warmup, tip) = warmup(config.clone(), Some(Instant::now()));

    let (link, mut chunks) = mpsc::channel(WARMUP_QUEUE);
    let driver = tokio::spawn(warmup.run(PEER, link));

    let probe = chunks
        .recv()
        .await
        .expect("the probe does not wait for the tip");
    let _ = write(probe).send(());

    // A tip change half-way through the quiet time restarts it.
    tokio::time::advance(config.tip_quiet / 2).await;
    let changed_at = Instant::now();
    tip.send_replace(Some(changed_at));

    let chunk = chunks.recv().await.expect("the first padded ping arrives");
    assert_eq!(chunk.padding, config.first_chunk_bytes);
    assert_eq!(Instant::now(), changed_at + config.tip_quiet);

    drop(chunk);
    drop(chunks);
    driver.await.unwrap();
}

/// A padded ping that stays queued behind other connection work until the chunk timeout is
/// never written, so it does not start the cooldown (V12 F-722).
#[tokio::test(start_paused = true)]
async fn queued_padding_that_is_never_written_keeps_no_claim() {
    let config = enabled_config();
    let (warmup, _tip) = warmup(config.clone(), None);

    let (link, mut chunks) = mpsc::channel(WARMUP_QUEUE);
    let driver = tokio::spawn(warmup.clone().run(PEER, link));
    let queued = first_padded_ping(&mut chunks).await;

    driver.await.unwrap();
    assert!(
        queued.written.send(()).is_err(),
        "the connection skips a ping whose warm-up stopped"
    );
    assert!(
        warmup.claim(PEER).is_some(),
        "padding that was never written does not start the cooldown"
    );
}

/// A padded ping that was written starts the cooldown, even if its pong never arrives.
#[tokio::test(start_paused = true)]
async fn written_padding_without_a_pong_keeps_the_claim() {
    let config = enabled_config();
    let (warmup, _tip) = warmup(config.clone(), None);

    let (link, mut chunks) = mpsc::channel(WARMUP_QUEUE);
    let driver = tokio::spawn(warmup.clone().run(PEER, link));
    let pong = write(first_padded_ping(&mut chunks).await);

    driver.await.unwrap();
    drop(pong);
    assert!(
        warmup.claim(PEER).is_none(),
        "written padding starts the cooldown"
    );
}
