//! Supervised native Zakura dialing and redial policy.
//!
//! [`RedialState`] owns one peer's redial policy. [`RedialIO`] owns the dial
//! attempts and the registration watch, and traces each attempt's phases in
//! `discovery_dial_phase` rows.

use std::{future::Future, pin::Pin, time::Duration};

use zakura_quic::NodeAddr;
use zakura_rt::{sync::watch, time::Instant};

use super::trace::{DialOwner, DialPhase, DiscoveryDialPhaseEvent};
use crate::zakura::{ZakuraEndpoint, ZakuraLocalLimits, ZakuraPeerId, ZakuraTrace};

#[cfg(any(test, feature = "p2p-fuzz"))]
mod model;
mod state;
#[cfg(test)]
mod tests;
#[cfg(any(test, feature = "p2p-fuzz"))]
pub use model::{replay_redial_bytes, RedialReplaySummary};

use state::{RedialNext, RedialStart, RedialState};

/// A connection that served at least this long is treated as healthy, so the
/// next re-dial after it drops starts from the initial (fast) backoff again
/// instead of penalising a long-lived peer for an eventual disconnect.
pub(super) const ZAKURA_REDIAL_HEALTHY_CONNECTION: Duration = Duration::from_secs(60);

/// Controls how [`native_dial_supervised`] retries and re-dials a peer.
#[derive(Clone, Copy, Debug)]
pub(crate) struct RedialPolicy {
    initial_backoff: Duration,
    max_backoff: Duration,
    /// Stop after this many consecutive failed attempts; `None` retries forever.
    max_attempts: Option<usize>,
    /// How long an already-registered peer must remain registered before a
    /// connect-once dial treats the connection as healthy and exits.
    registered_settle_timeout: Duration,
    /// Re-dial again after a healthy connection drops. Configured bootstrap
    /// peers and legacy->Zakura upgrade hand-offs set this because they are the
    /// active owners of the Zakura dial.
    redial_after_drop: bool,
}

impl RedialPolicy {
    /// Maintain a connection indefinitely, re-dialing on drop (bootstrap peers).
    pub(crate) fn maintain(initial_backoff: Duration, max_backoff: Duration) -> Self {
        Self {
            initial_backoff,
            max_backoff,
            max_attempts: None,
            registered_settle_timeout: ZAKURA_REDIAL_HEALTHY_CONNECTION,
            redial_after_drop: true,
        }
    }

    /// Maintain a connection like [`Self::maintain`], but stop after `attempts`
    /// consecutive failed dials (legacy->Zakura upgrade hand-offs).
    ///
    /// A peer-supplied upgrade address must not get a dial that lives forever.
    /// A healthy connection resets the failure count, so a live peer keeps its
    /// dial through brief drops, and a peer that stops answering loses it.
    pub(crate) fn maintain_bounded(
        initial_backoff: Duration,
        max_backoff: Duration,
        attempts: usize,
    ) -> Self {
        Self {
            max_attempts: Some(attempts),
            ..Self::maintain(initial_backoff, max_backoff)
        }
    }

    /// Connect once, retrying only the initial dial up to `attempts` times.
    #[cfg(test)]
    fn connect_once(initial_backoff: Duration, max_backoff: Duration, attempts: usize) -> Self {
        Self::connect_once_with_registered_settle(
            initial_backoff,
            max_backoff,
            attempts,
            ZAKURA_REDIAL_HEALTHY_CONNECTION,
        )
    }

    #[cfg(test)]
    fn connect_once_with_registered_settle(
        initial_backoff: Duration,
        max_backoff: Duration,
        attempts: usize,
        registered_settle_timeout: Duration,
    ) -> Self {
        Self {
            initial_backoff,
            max_backoff,
            max_attempts: Some(attempts),
            registered_settle_timeout,
            redial_after_drop: false,
        }
    }
}

/// Outcome of one dial attempt, as seen by [`run_dial_supervisor`].
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum DialResult {
    /// Connected and served at least [`ZAKURA_REDIAL_HEALTHY_CONNECTION`].
    Healthy,
    /// Failed to establish, or served only briefly (e.g. a duplicate was closed).
    Failed,
    /// The peer speaks another ALPN; wait at least
    /// [`ZAKURA_ALPN_MISMATCH_BACKOFF`](crate::zakura::ZAKURA_ALPN_MISMATCH_BACKOFF)
    /// before the next dial.
    AlpnMismatch,
    /// This node hit one of its own limits before sending the peer anything
    /// (`ZakuraHandlerError::is_pre_dial_limit`). Retry after the initial
    /// backoff without counting an attempt or growing the backoff, so a full
    /// node doesn't give up on the peer. A limit hit after the handshake
    /// counts as a failure, so it can't redial the peer forever.
    LocalLimit,
}

/// Maintain a Zakura connection to `node_addr`, re-dialing with bounded backoff.
///
/// `native_bootstrap_dial` returns when the connection fails to establish or,
/// on success, when serving ends (the peer dropped or a duplicate was closed),
/// so a single loop covers both the initial connect, including the startup
/// race where the seed's endpoint is not yet listening, and reconnection after
/// a drop. The retry/backoff policy lives in [`run_dial_supervisor`]; this just
/// supplies the real dial attempt and the supervisor's registration watch.
pub(crate) async fn native_dial_supervised(
    endpoint: ZakuraEndpoint,
    node_addr: NodeAddr,
    limits: ZakuraLocalLimits,
    policy: RedialPolicy,
) {
    let Ok(peer_id) = ZakuraPeerId::new(node_addr.id.as_bytes().to_vec()) else {
        tracing::warn!(?node_addr, "invalid Zakura bootstrap node id; not dialing");
        return;
    };

    let shutdown = endpoint.background_shutdown_token();
    let registered = endpoint.supervisor().subscribe();
    let trace = endpoint.trace();
    let supervised = run_dial_supervisor(peer_id, registered, trace, policy, move || {
        let endpoint = endpoint.clone();
        let node_addr = node_addr.clone();
        let limits = limits.clone();
        Box::pin(async move {
            let started = Instant::now();
            match super::dialer::native_bootstrap_dial(&endpoint, node_addr, &limits).await {
                Ok(()) => DialResult::lasting(started.elapsed()),
                Err(error) if error.is_alpn_mismatch() => {
                    tracing::debug!(
                        ?error,
                        "Zakura peer speaks another protocol version; backing off"
                    );
                    DialResult::AlpnMismatch
                }
                Err(error) if error.is_pre_dial_limit() => {
                    tracing::debug!(?error, "Zakura native dial hit a local limit; will retry");
                    DialResult::LocalLimit
                }
                Err(error) => {
                    tracing::debug!(?error, "Zakura native dial failed; will retry");
                    DialResult::Failed
                }
            }
        }) as Pin<Box<dyn Future<Output = DialResult> + Send>>
    });

    // Endpoint shutdown cancels this token; stop maintaining the dial promptly
    // rather than looping on against a torn-down router. A `maintain` policy
    // never exits on its own (no max attempts), and the supervisor registration
    // watch stays open while this task holds an endpoint clone, so this is the
    // only signal that reliably ends the loop at teardown.
    zakura_rt::select! {
        biased;
        _ = shutdown.cancelled() => {}
        _ = supervised => {}
    }
}

/// Retry/backoff loop shared by configured bootstrap peers and the upgrade dial.
///
/// [`RedialState`] chooses each step; see [`RedialIO::run`]. `dial` is injected
/// so the loop is unit-testable without real network I/O.
async fn run_dial_supervisor<F>(
    peer_id: ZakuraPeerId,
    registered: watch::Receiver<Vec<ZakuraPeerId>>,
    trace: ZakuraTrace,
    policy: RedialPolicy,
    dial: F,
) where
    F: FnMut() -> Pin<Box<dyn Future<Output = DialResult> + Send>>,
{
    RedialIO {
        peer_id,
        registered,
        dial,
        trace,
        attempts: 0,
    }
    .run(RedialState::new(policy))
    .await
}

/// One supervised peer's runtime resources: its dial attempts and the
/// supervisor's registration watch. [`RedialState`] decides what they do.
struct RedialIO<F> {
    peer_id: ZakuraPeerId,
    registered: watch::Receiver<Vec<ZakuraPeerId>>,
    dial: F,
    trace: ZakuraTrace,
    /// Attempts this supervisor has started, which numbers the next one.
    attempts: u64,
}

impl<F> RedialIO<F>
where
    F: FnMut() -> Pin<Box<dyn Future<Output = DialResult> + Send>>,
{
    /// Runs attempts until `state` stops them or the registration watch closes.
    ///
    /// Before each dial it skips a peer that is already registered (it may have
    /// dialed us first) so the two directions do not churn duplicate connections.
    /// Exits when `policy.max_attempts` consecutive attempts fail, when a
    /// `connect_once` peer connects or finishes serving, or when the supervisor's
    /// registration watch closes (node shutdown).
    async fn run(mut self, mut state: RedialState) {
        loop {
            let (mut result, mut attempt) = match state.start(self.peer_is_registered()) {
                RedialStart::Dial => {
                    let mut attempt = self.start_attempt();
                    let result = self.watched_dial(&mut attempt).await;
                    attempt.finish_dial();
                    (result, attempt)
                }
                RedialStart::AwaitRegisteredSettle(timeout) => {
                    // Wait for it to deregister, then re-dial promptly.
                    zakura_rt::select! {
                        changed = self.registered.changed() => {
                            if changed.is_err() {
                                return;
                            }
                            state.registration_changed();
                            continue;
                        }
                        _ = zakura_rt::time::sleep(timeout) => {
                            return;
                        }
                    }
                }
                // A maintained dial owns this peer, so score the registration the
                // same way a dial this loop drove is scored. Two things matter.
                //
                // Wait for *this* peer to deregister rather than for any peer-set
                // change: on a busy node an unrelated peer connecting is not this
                // connection ending.
                //
                // Then require the registration to have lasted before crediting it
                // as healthy. A peer that shares no service beyond discovery is
                // closed as soon as its discovery exchange completes
                // (`discovery_exchange_complete`), and the deregistration for the
                // connection this loop just dialed often lands after the dial
                // future returns. Resetting the backoff on that registration made
                // every such peer re-dial at once, forever.
                RedialStart::ScoreRegistration => {
                    let mut attempt = self.start_attempt();
                    let Some(registered_for) = self.await_deregistration(&mut attempt).await else {
                        return;
                    };
                    (DialResult::lasting(registered_for), attempt)
                }
            };

            // The connection deregistration can land after the dial future returns.
            // Wait for the watch to catch up before applying this attempt's result,
            // so the residual registration does not become a second attempt on the
            // next loop iteration. A concurrent registration that remains healthy
            // can still upgrade a failed dial to a healthy connection.
            if state.waits_for_lagging_deregistration() && self.peer_is_registered() {
                let Some(registered_for) = self.await_deregistration(&mut attempt).await else {
                    return;
                };
                result = result.with_lagging_registration(registered_for);
            }

            match state.record(result) {
                RedialNext::Stop => return,
                RedialNext::RedialNow => {}
                // Back off, but wake early only if the peer appears in the supervisor
                // from another connection. Ignore unrelated peer-set changes,
                // including the deregistration from the failed attempt we just
                // observed.
                RedialNext::RetryAfter(wait) => {
                    if !sleep_unless_registered(&mut self.registered, &self.peer_id, wait).await {
                        return;
                    }
                }
            }
        }
    }

    /// Waits until the registered peer deregisters, and traces both changes.
    /// Returns how long it stayed, or `None` when the registration watch closes.
    async fn await_deregistration(&mut self, attempt: &mut TracedAttempt) -> Option<Duration> {
        attempt.observe_registration(true);
        let registered_for =
            wait_for_peer_deregistration(&mut self.registered, &self.peer_id).await?;
        attempt.observe_registration(false);
        Some(registered_for)
    }

    /// Numbers the next attempt and traces its start.
    fn start_attempt(&mut self) -> TracedAttempt {
        let attempt = TracedAttempt {
            trace: self.trace.clone(),
            peer_id: self.peer_id.clone(),
            attempt: self.attempts,
            registered: false,
            dialing: false,
        };
        self.attempts = self.attempts.wrapping_add(1);
        attempt.phase(DialPhase::Recorded);
        attempt
    }

    /// Runs one dial, and traces the peer's registrations while it runs.
    ///
    /// The trace reads a clone of the watch. A registration during the dial
    /// stays unseen on the loop's own receiver, so it still wakes a later
    /// backoff.
    async fn watched_dial(&mut self, attempt: &mut TracedAttempt) -> DialResult {
        attempt.dialing = true;
        let mut registrations = self.registered.clone();
        let mut watching = true;
        let mut dial = (self.dial)();
        let result = loop {
            zakura_rt::select! {
                biased;
                result = &mut dial => break result,
                changed = registrations.changed(), if watching => {
                    watching = changed.is_ok();
                    if watching {
                        attempt.observe_registration(registrations.borrow().contains(&self.peer_id));
                    }
                }
            }
        };
        attempt.observe_registration(self.registered.borrow().contains(&self.peer_id));
        result
    }

    /// Whether the peer is registered now, marking the watch seen.
    fn peer_is_registered(&mut self) -> bool {
        self.registered.borrow_and_update().contains(&self.peer_id)
    }
}

/// One attempt's `discovery_dial_phase` rows.
///
/// Dropping it during its dial traces how the dial ended: shutdown drops the
/// supervisor's future, and a panic unwinds through it.
struct TracedAttempt {
    trace: ZakuraTrace,
    peer_id: ZakuraPeerId,
    attempt: u64,
    /// Whether the last registration row showed the peer registered.
    registered: bool,
    /// Whether the attempt's dial is running.
    dialing: bool,
}

impl TracedAttempt {
    fn phase(&self, phase: DialPhase) {
        self.trace.emit_event(|| {
            DiscoveryDialPhaseEvent::new(
                self.peer_id.as_bytes(),
                DialOwner::Redial,
                self.attempt,
                phase,
            )
        });
    }

    /// Traces a change in whether the peer is registered.
    fn observe_registration(&mut self, registered: bool) {
        if registered != self.registered {
            self.registered = registered;
            self.phase(if registered {
                DialPhase::RegistrationSeen
            } else {
                DialPhase::RegistrationEnded
            });
        }
    }

    /// Traces that the attempt's dial returned its result.
    fn finish_dial(&mut self) {
        self.dialing = false;
        self.phase(DialPhase::WorkerFinished);
    }
}

impl Drop for TracedAttempt {
    fn drop(&mut self) {
        if self.dialing {
            self.phase(if std::thread::panicking() {
                DialPhase::WorkerPanicked
            } else {
                DialPhase::WorkerCancelled
            });
        }
    }
}

/// Sleeps for `wait`, waking early if `peer_id` registers from another
/// connection. Ignores unrelated peer-set changes. Returns `false` when the
/// registration watch closes (node shutdown).
async fn sleep_unless_registered(
    registered: &mut watch::Receiver<Vec<ZakuraPeerId>>,
    peer_id: &ZakuraPeerId,
    wait: Duration,
) -> bool {
    let sleep = zakura_rt::time::sleep(wait);
    zakura_rt::pin!(sleep);
    loop {
        zakura_rt::select! {
            changed = registered.changed() => {
                if changed.is_err() {
                    return false;
                }
                if registered.borrow().contains(peer_id) {
                    return true;
                }
            }
            _ = &mut sleep => return true,
        }
    }
}

/// Wait until `peer_id` leaves the registration watch.
///
/// Returns how long the peer remained registered after this observer first saw
/// it, or `None` when the sender closes while the peer remains registered.
async fn wait_for_peer_deregistration(
    registered: &mut watch::Receiver<Vec<ZakuraPeerId>>,
    peer_id: &ZakuraPeerId,
) -> Option<Duration> {
    let registered_at = Instant::now();
    loop {
        registered.changed().await.ok()?;
        if !registered.borrow_and_update().contains(peer_id) {
            return Some(registered_at.elapsed());
        }
    }
}
