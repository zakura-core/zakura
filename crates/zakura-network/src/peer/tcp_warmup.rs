//! Warms the send direction of new legacy TCP connections with padded pings.
//!
//! A new TCP connection starts in slow start, so the first full block it carries waits several
//! extra round trips for the congestion window to grow. Each warm-up sends the pings that
//! [`TcpWarmupConfig::schedule`] lists, one at a time, and sends the next only once the peer's
//! `pong` for the previous one arrives. Each padded ping fills the current window, so each one
//! about doubles it. Peers ignore a ping body's trailing bytes, so receivers need no change.
//!
//! [`TcpWarmup`] limits warm-ups across the node: one per peer IP per
//! [`TcpWarmupConfig::peer_cooldown`], at most [`TcpWarmupConfig::max_concurrent`] at a time,
//! and no padded ping until the best tip has been quiet for [`TcpWarmupConfig::tip_quiet`].
//! Inside a connection, a warm-up ping is the lowest-priority write (see
//! [`Connection::run`](super::Connection::run)).

use std::{
    collections::HashMap,
    net::IpAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::{
    sync::{mpsc, oneshot, watch, Semaphore},
    time::Instant,
};

use zakura_chain::chain_tip::ChainTip;

use crate::{config::TcpWarmupConfig, protocol::external::types::Nonce};

#[cfg(test)]
mod tests;

/// The most peer IPs whose last warm-up time is remembered.
///
/// When the map is full, the oldest entry is forgotten, so that IP can be warmed again early.
const MAX_REMEMBERED_PEERS: usize = 10_000;

/// The kernel setting that makes TCP restart slow start after an idle period.
const SLOW_START_AFTER_IDLE_PATH: &str = "/proc/sys/net/ipv4/tcp_slow_start_after_idle";

/// One warm-up ping, sent by the connection when it has nothing else to write.
#[derive(Debug)]
pub(crate) struct WarmupChunk {
    /// The ping nonce, which the peer's `pong` echoes.
    pub nonce: Nonce,
    /// The zero bytes after the nonce.
    pub padding: u32,
    /// Receives the time from the start of the ping's write to its `pong`.
    pub pong: oneshot::Sender<Duration>,
}

/// The queue size of a connection's warm-up channel. The driver waits for each `pong`, so it
/// queues at most one ping.
pub(crate) const WARMUP_QUEUE: usize = 1;

/// Coordinates the warm-ups of this node's legacy TCP connections.
#[derive(Clone, Debug)]
pub struct TcpWarmup {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    config: TcpWarmupConfig,
    /// The start time of each peer IP's last warm-up that sent padding, or that is in progress.
    claims: Mutex<HashMap<IpAddr, Instant>>,
    permits: Arc<Semaphore>,
    /// When the best tip last changed, or `None` if it has not changed since startup.
    tip_changed_at: watch::Receiver<Option<Instant>>,
}

impl TcpWarmup {
    /// Returns a coordinator that follows `latest_chain_tip`, or `None` if warm-up is disabled
    /// or the kernel setting would undo it.
    ///
    /// Must be called from within a Tokio runtime, because it spawns the tip watcher.
    pub fn spawn<C>(config: TcpWarmupConfig, latest_chain_tip: C) -> Option<Self>
    where
        C: ChainTip + Send + 'static,
    {
        if !config.enabled {
            return None;
        }
        let slow_start_after_idle = std::fs::read_to_string(SLOW_START_AFTER_IDLE_PATH)
            .ok()
            .map(|value| value.trim().to_string());
        if slow_start_after_idle.as_deref() != Some("0") && !config.ignore_slow_start_after_idle {
            warn!(
                ?slow_start_after_idle,
                "TCP warm-up is disabled: set net.ipv4.tcp_slow_start_after_idle = 0, \
                 or the kernel shrinks each warmed window again after an idle period"
            );
            return None;
        }

        let (tip_changed_tx, tip_changed_at) = watch::channel(None);
        tokio::spawn(watch_tip_changes(latest_chain_tip, tip_changed_tx));
        info!(?config, "warming new legacy TCP connections");
        Some(Self::new(config, tip_changed_at))
    }

    /// Returns a coordinator that waits for quiet periods in `tip_changed_at`.
    fn new(config: TcpWarmupConfig, tip_changed_at: watch::Receiver<Option<Instant>>) -> Self {
        Self {
            inner: Arc::new(Inner {
                permits: Arc::new(Semaphore::new(config.max_concurrent)),
                config,
                claims: Mutex::new(HashMap::new()),
                tip_changed_at,
            }),
        }
    }

    /// Warms one connection to `peer_ip` through `link`, unless that IP was warmed within the
    /// cooldown.
    ///
    /// Returns when the warm-up completes or stops. It stops if a `pong` takes longer than the
    /// chunk timeout, or if the connection closes, and it never closes the connection itself.
    pub(crate) async fn run(self, peer_ip: IpAddr, link: mpsc::Sender<WarmupChunk>) {
        let Some(mut claim) = self.claim(peer_ip) else {
            metrics::counter!("peer.tcp_warmup.skipped", "reason" => "cooldown").increment(1);
            return;
        };

        let permits = self.inner.permits.clone();
        let _permit = tokio::select! {
            permit = permits.acquire_owned() => permit.expect("the semaphore is never closed"),
            () = link.closed() => {
                metrics::counter!("peer.tcp_warmup.skipped", "reason" => "closed").increment(1);
                return;
            }
        };
        metrics::counter!("peer.tcp_warmup.started").increment(1);

        let config = &self.inner.config;
        let mut rtts = Vec::new();
        for padding in config.schedule() {
            if padding > 0 {
                tokio::select! {
                    () = self.wait_for_quiet_tip() => {}
                    () = link.closed() => return abort("closed", &rtts),
                }
            }

            let (pong_tx, pong_rx) = oneshot::channel();
            let chunk = WarmupChunk {
                nonce: Nonce::default(),
                padding,
                pong: pong_tx,
            };
            if link.send(chunk).await.is_err() {
                return abort("closed", &rtts);
            }
            if padding > 0 {
                claim.keep();
            }

            match tokio::time::timeout(config.chunk_timeout, pong_rx).await {
                Ok(Ok(rtt)) => {
                    metrics::counter!("peer.tcp_warmup.bytes_sent").increment(padding.into());
                    metrics::histogram!(
                        "peer.tcp_warmup.chunk_rtt_seconds",
                        "chunk" => padding.to_string(),
                    )
                    .record(rtt.as_secs_f64());
                    rtts.push((padding, rtt));
                }
                Ok(Err(_)) => return abort("closed", &rtts),
                Err(_) => return abort("timeout", &rtts),
            }
        }

        metrics::counter!("peer.tcp_warmup.completed").increment(1);
        debug!(?rtts, "warmed TCP connection");
    }

    /// Records a warm-up of `peer_ip` starting now, or returns `None` if one started within
    /// the cooldown.
    fn claim(&self, peer_ip: IpAddr) -> Option<Claim> {
        let now = Instant::now();
        let cooldown = self.inner.config.peer_cooldown;
        let mut claims = self.inner.claims.lock().expect("no panics while locked");

        if claims
            .get(&peer_ip)
            .is_some_and(|started| now.saturating_duration_since(*started) < cooldown)
        {
            return None;
        }

        if claims.len() >= MAX_REMEMBERED_PEERS && !claims.contains_key(&peer_ip) {
            claims.retain(|_, started| now.saturating_duration_since(*started) < cooldown);
        }
        if claims.len() >= MAX_REMEMBERED_PEERS && !claims.contains_key(&peer_ip) {
            let oldest = claims
                .iter()
                .min_by_key(|(_, started)| **started)
                .map(|(ip, _)| *ip);
            if let Some(oldest) = oldest {
                claims.remove(&oldest);
            }
        }
        claims.insert(peer_ip, now);

        Some(Claim {
            warmup: self.clone(),
            peer_ip,
            started: now,
            sent_padding: false,
        })
    }

    /// Waits until the best tip has not changed for the configured quiet time.
    async fn wait_for_quiet_tip(&self) {
        let mut tip_changed_at = self.inner.tip_changed_at.clone();
        loop {
            let Some(changed_at) = *tip_changed_at.borrow_and_update() else {
                return;
            };
            let quiet_at = changed_at + self.inner.config.tip_quiet;
            if Instant::now() >= quiet_at {
                return;
            }
            tokio::select! {
                () = tokio::time::sleep_until(quiet_at) => {}
                changed = tip_changed_at.changed() => {
                    if changed.is_err() {
                        // The tip watcher has stopped, so no later change can arrive.
                        tokio::time::sleep_until(quiet_at).await;
                    }
                }
            }
        }
    }
}

/// Records why a warm-up stopped early.
fn abort(reason: &'static str, rtts: &[(u32, Duration)]) {
    metrics::counter!("peer.tcp_warmup.aborted", "reason" => reason).increment(1);
    debug!(reason, ?rtts, "stopped warming TCP connection");
}

/// A peer IP's warm-up record. Dropping it forgets the record unless the warm-up sent padding,
/// so a connection that closes early does not stop the next one from warming.
#[derive(Debug)]
struct Claim {
    warmup: TcpWarmup,
    peer_ip: IpAddr,
    started: Instant,
    sent_padding: bool,
}

impl Claim {
    /// Keeps the record after the warm-up ends.
    fn keep(&mut self) {
        self.sent_padding = true;
    }
}

impl Drop for Claim {
    fn drop(&mut self) {
        if self.sent_padding {
            return;
        }
        let Ok(mut claims) = self.warmup.inner.claims.lock() else {
            return;
        };
        if claims.get(&self.peer_ip) == Some(&self.started) {
            claims.remove(&self.peer_ip);
        }
    }
}

/// Publishes the time of each best tip change until shutdown or until every warm-up has
/// stopped listening.
async fn watch_tip_changes<C: ChainTip>(
    mut latest_chain_tip: C,
    tip_changed_at: watch::Sender<Option<Instant>>,
) {
    loop {
        tokio::select! {
            changed = latest_chain_tip.best_tip_changed() => {
                if changed.is_err() {
                    return;
                }
            }
            () = tip_changed_at.closed() => return,
        }
        latest_chain_tip.mark_best_tip_seen();
        tip_changed_at.send_replace(Some(Instant::now()));
    }
}
