//! Per-connection QUIC credit and send-window lease controller (`02 §3`, `02 §6`).
//!
//! Each admitted connection starts with its static base receive window
//! (control share + unsolicited receive bytes of its negotiated reactors) and
//! base send window. Live request leases raise the QUIC receive window via
//! `set_limits` (API-9) before the request goes out, and shrink it when the
//! request ends.
//!
//! Because granted QUIC credit cannot be revoked (`02 §6`), unspent receive
//! credit `d` stays charged on its [`PoolLease`] in a cumulative FIFO drain
//! queue until `ConnLoad.delivered_bytes` advances by `d` (or the connection
//! closes). Similarly, a send-window shrink keeps its `d`-byte [`PoolLease`]
//! until `ConnLoad.accepted_send_bytes` advances past the value recorded at
//! shrink (or the connection closes).

use std::{collections::VecDeque, fmt, sync::Arc};

use zakura_quic::{Conn as QuicConn, ConnLimits, ConnLoad};
use zakura_rt::sync::{
    counter::{AtomicU64, Ordering},
    Mutex,
};

use super::PoolLease;
use crate::zakura::ZakuraConnId;

/// Subset of [`ConnLoad`] fields read by the credit and send-window controller.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct CreditLoad {
    pub(crate) delivered_bytes: u64,
    pub(crate) accepted_send_bytes: u64,
    pub(crate) queued_send_bytes: u64,
    pub(crate) send_blocked_streams: u64,
    pub(crate) cwnd: u64,
}

impl From<ConnLoad> for CreditLoad {
    fn from(load: ConnLoad) -> Self {
        Self {
            delivered_bytes: load.delivered_bytes,
            accepted_send_bytes: load.accepted_send_bytes,
            queued_send_bytes: load.queued_send_bytes,
            send_blocked_streams: load.send_blocked_streams,
            cwnd: load.cwnd,
        }
    }
}

/// Receives `set_limits` updates and supplies [`CreditLoad`] snapshots.
///
/// Production connections use [`CreditSink::Quic`]; unit tests inject a
/// deterministic [`CreditSink::Mock`] to test credit growth and `delivered_bytes`
/// / `accepted_send_bytes` draining without binding sockets.
enum CreditSink {
    Quic(QuicConn),
    #[cfg(test)]
    Mock(Arc<MockCreditSink>),
}

impl fmt::Debug for CreditSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Quic(_) => f.write_str("CreditSink::Quic"),
            #[cfg(test)]
            Self::Mock(_) => f.write_str("CreditSink::Mock"),
        }
    }
}

impl CreditSink {
    fn set_limits(&self, limits: ConnLimits) -> bool {
        match self {
            Self::Quic(connection) => connection.set_limits(limits).is_ok(),
            #[cfg(test)]
            Self::Mock(mock) => mock.set_limits(limits),
        }
    }

    fn load(&self) -> Option<CreditLoad> {
        match self {
            Self::Quic(connection) => connection
                .close_reason()
                .is_none()
                .then(|| CreditLoad::from(connection.load())),
            #[cfg(test)]
            Self::Mock(mock) => mock.load(),
        }
    }
}

#[derive(Debug)]
struct ReceiveDrainEntry {
    /// Cumulative `ConnLoad.delivered_bytes` value at which this unspent
    /// receive-credit lease may be released (`02 §6`).
    release_at_delivered: u64,
    lease: PoolLease,
}

#[derive(Debug)]
struct SendDrainEntry {
    /// `ConnLoad.accepted_send_bytes` recorded when the send window shrank;
    /// once `accepted_send_bytes` strictly exceeds this value (the connection
    /// accepted any new byte after the shrink), quinn-proto's unacknowledged
    /// bytes have fallen below the new window (`02 §6`).
    accepted_at_shrink: u64,
    lease: PoolLease,
}

#[derive(Debug)]
struct CreditState {
    base_receive_window: u64,
    active_receive_credit: u64,
    receive_drain_frontier: u64,
    draining_receive: VecDeque<ReceiveDrainEntry>,
    base_send_window: u64,
    active_send_lease: Option<PoolLease>,
    draining_send: VecDeque<SendDrainEntry>,
    closed: bool,
}

impl CreditState {
    fn target_receive_window(&self) -> u64 {
        self.base_receive_window
            .saturating_add(self.active_receive_credit)
    }

    fn target_send_window(&self) -> u64 {
        self.base_send_window
            .saturating_add(self.active_send_lease.as_ref().map_or(0, PoolLease::bytes))
    }

    fn current_limits(&self) -> ConnLimits {
        ConnLimits::default()
            .with_receive_window(self.target_receive_window())
            .with_send_window(self.target_send_window())
    }

    fn drain_satisfied(&mut self, load: Option<CreditLoad>, dropped: &mut Vec<PoolLease>) {
        if self.closed {
            self.drain_all(dropped);
            return;
        }
        let Some(load) = load else {
            // The QUIC connection has closed; every draining credit lease is free.
            self.closed = true;
            self.drain_all(dropped);
            return;
        };
        while let Some(front) = self.draining_receive.front() {
            if load.delivered_bytes >= front.release_at_delivered {
                let entry = self
                    .draining_receive
                    .pop_front()
                    .expect("front existed immediately prior");
                dropped.push(entry.lease);
            } else {
                break;
            }
        }
        while let Some(front) = self.draining_send.front() {
            if load.accepted_send_bytes > front.accepted_at_shrink {
                let entry = self
                    .draining_send
                    .pop_front()
                    .expect("front existed immediately prior");
                dropped.push(entry.lease);
            } else {
                break;
            }
        }
    }

    fn drain_all(&mut self, dropped: &mut Vec<PoolLease>) {
        while let Some(entry) = self.draining_receive.pop_front() {
            dropped.push(entry.lease);
        }
        if let Some(lease) = self.active_send_lease.take() {
            dropped.push(lease);
        }
        while let Some(entry) = self.draining_send.pop_front() {
            dropped.push(entry.lease);
        }
    }
}

#[derive(Debug)]
struct ConnectionCreditInner {
    conn_id: ZakuraConnId,
    sink: CreditSink,
    state: Mutex<CreditState>,
    /// Live target receive window readable without preemption by sim boundary
    /// probes (`ConnectionProbe`).
    live_receive_window: Arc<AtomicU64>,
}

/// Shared per-connection receive-credit and send-window controller (`02 §3`, `02 §6`).
#[derive(Clone, Debug)]
pub(crate) struct ConnectionCredit {
    inner: Arc<ConnectionCreditInner>,
}

impl ConnectionCredit {
    /// Create a controller for one admitted QUIC connection starting at
    /// `base_receive_window` and `base_send_window`, and apply those limits
    /// immediately via `set_limits` (API-9).
    pub(crate) fn new_quic(
        conn_id: ZakuraConnId,
        connection: QuicConn,
        base_receive_window: u64,
        base_send_window: u64,
    ) -> Self {
        let credit = Self {
            inner: Arc::new(ConnectionCreditInner {
                conn_id,
                sink: CreditSink::Quic(connection),
                state: Mutex::new(CreditState {
                    base_receive_window,
                    active_receive_credit: 0,
                    receive_drain_frontier: 0,
                    draining_receive: VecDeque::new(),
                    base_send_window,
                    active_send_lease: None,
                    draining_send: VecDeque::new(),
                    closed: false,
                }),
                live_receive_window: Arc::new(AtomicU64::new(base_receive_window)),
            }),
        };
        let initial = ConnLimits::default()
            .with_receive_window(base_receive_window)
            .with_send_window(base_send_window);
        let _ = credit.inner.sink.set_limits(initial);
        credit
    }

    /// Connection identifier tagged on leases owned by this connection.
    #[allow(dead_code)]
    pub(crate) fn conn_id(&self) -> ZakuraConnId {
        self.inner.conn_id
    }

    /// Atomic live receive window for preemption-free simulator boundary probes.
    #[cfg(zakura_sim)]
    pub(crate) fn live_receive_window_atomic(&self) -> Arc<AtomicU64> {
        self.inner.live_receive_window.clone()
    }

    /// Current live target receive window (`base + active_receive_credit`).
    #[allow(dead_code)]
    pub(crate) fn live_receive_window(&self) -> u64 {
        self.inner.live_receive_window.load(Ordering::Acquire)
    }

    /// Current [`CreditLoad`] snapshot, or `None` once the QUIC connection has closed.
    #[allow(dead_code)]
    pub(crate) fn load(&self) -> Option<CreditLoad> {
        self.inner.sink.load()
    }

    /// Raise the connection's QUIC receive window by `bytes` before sending a
    /// leased request (`02 §3`).
    pub(crate) fn add_receive_credit(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let mut dropped = Vec::new();
        {
            let mut state = self
                .inner
                .state
                .lock()
                .expect("connection credit mutex is never poisoned");
            if state.closed {
                return;
            }
            let load = self.inner.sink.load();
            state.drain_satisfied(load, &mut dropped);
            if state.closed {
                return;
            }
            state.active_receive_credit = state.active_receive_credit.saturating_add(bytes);
            let limits = state.current_limits();
            self.inner
                .live_receive_window
                .store(state.target_receive_window(), Ordering::Release);
            if !self.inner.sink.set_limits(limits) {
                state.closed = true;
                state.drain_all(&mut dropped);
            }
        }
        drop(dropped);
    }

    /// Lower the connection's QUIC receive window by `total_credit` when a
    /// request (or one height of a request) completes or ends early (`02 §6`).
    ///
    /// If `unspent_lease` is `Some(lease)` with `lease.bytes() > 0` (meaning
    /// `d = lease.bytes()` of the granted credit were NOT delivered on the wire,
    /// e.g. `RangeUnavailable`, a shorter body, or a timeout), `lease` stays
    /// charged until `ConnLoad.delivered_bytes` advances by `d` or the
    /// connection closes.
    pub(crate) fn finish_receive_credit(
        &self,
        total_credit: u64,
        unspent_lease: Option<PoolLease>,
    ) {
        let mut dropped = Vec::new();
        {
            let mut state = self
                .inner
                .state
                .lock()
                .expect("connection credit mutex is never poisoned");
            state.active_receive_credit = state.active_receive_credit.saturating_sub(total_credit);
            let target_recv = state.target_receive_window();
            self.inner
                .live_receive_window
                .store(target_recv, Ordering::Release);

            if state.closed {
                if let Some(lease) = unspent_lease {
                    dropped.push(lease);
                }
                state.drain_all(&mut dropped);
            } else {
                let limits = state.current_limits();
                let open = self.inner.sink.set_limits(limits);
                let load = open.then(|| self.inner.sink.load()).flatten();
                state.drain_satisfied(load, &mut dropped);

                if let Some(lease) = unspent_lease {
                    let unspent_bytes = lease.bytes();
                    if unspent_bytes == 0 || state.closed {
                        dropped.push(lease);
                    } else if let Some(load) = load {
                        let start = state.receive_drain_frontier.max(load.delivered_bytes);
                        let release_at = start.saturating_add(unspent_bytes);
                        state.receive_drain_frontier = release_at;
                        state.draining_receive.push_back(ReceiveDrainEntry {
                            release_at_delivered: release_at,
                            lease,
                        });
                    } else {
                        state.closed = true;
                        dropped.push(lease);
                        state.drain_all(&mut dropped);
                    }
                }
            }
        }
        drop(dropped);
    }

    /// Poll `ConnLoad` and release any receive or send credit leases whose
    /// `delivered_bytes` / `accepted_send_bytes` drain condition is satisfied.
    pub(crate) fn poll_drain(&self) {
        let mut dropped = Vec::new();
        {
            let mut state = self
                .inner
                .state
                .lock()
                .expect("connection credit mutex is never poisoned");
            if state.draining_receive.is_empty() && state.draining_send.is_empty() {
                return;
            }
            let load = self.inner.sink.load();
            state.drain_satisfied(load, &mut dropped);
        }
        drop(dropped);
    }

    /// Whether any receive or send shrink leases are currently waiting to drain.
    pub(crate) fn has_draining(&self) -> bool {
        let state = self
            .inner
            .state
            .lock()
            .expect("connection credit mutex is never poisoned");
        !state.draining_receive.is_empty() || !state.draining_send.is_empty()
    }

    /// Release all held and draining credit leases on connection close (`02 §6`).
    pub(crate) fn close(&self) {
        let mut dropped = Vec::new();
        {
            let mut state = self
                .inner
                .state
                .lock()
                .expect("connection credit mutex is never poisoned");
            state.closed = true;
            state.drain_all(&mut dropped);
        }
        drop(dropped);
    }
}

/// RAII guard that calls [`ConnectionCredit::close`] when `serve_connection`
/// exits or unwinds, guaranteeing zero lingering credit leases after disconnect.
#[derive(Debug)]
pub(crate) struct CloseCreditOnDrop(pub(crate) Option<ConnectionCredit>);

impl Drop for CloseCreditOnDrop {
    fn drop(&mut self) {
        if let Some(credit) = self.0.take() {
            credit.close();
        }
    }
}

#[cfg(test)]
#[derive(Debug)]
struct MockCreditSink {
    inner: Mutex<MockCreditState>,
}

#[cfg(test)]
#[derive(Debug)]
struct MockCreditState {
    closed: bool,
    load: CreditLoad,
    history: Vec<ConnLimits>,
}

#[cfg(test)]
impl MockCreditSink {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(MockCreditState {
                closed: false,
                load: CreditLoad::default(),
                history: Vec::new(),
            }),
        })
    }

    fn set_limits(&self, limits: ConnLimits) -> bool {
        let mut guard = self.inner.lock().expect("mock credit mutex");
        if guard.closed {
            return false;
        }
        guard.history.push(limits);
        true
    }

    fn load(&self) -> Option<CreditLoad> {
        let guard = self.inner.lock().expect("mock credit mutex");
        (!guard.closed).then_some(guard.load)
    }

    fn advance_delivered(&self, bytes: u64) {
        let mut guard = self.inner.lock().expect("mock credit mutex");
        guard.load.delivered_bytes = guard.load.delivered_bytes.saturating_add(bytes);
    }

    fn close_conn(&self) {
        let mut guard = self.inner.lock().expect("mock credit mutex");
        guard.closed = true;
    }

    fn last_limits(&self) -> Option<ConnLimits> {
        let guard = self.inner.lock().expect("mock credit mutex");
        guard.history.last().copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::zakura::memory::{Consumer, ConsumerSpec, MemoryPool, PoolConfig};

    fn test_pool() -> MemoryPool {
        let config = PoolConfig::new(64 * 1024 * 1024, 0)
            .with_consumer(
                Consumer::BlockSync,
                ConsumerSpec::bulk(2 * 1024 * 1024, 32 * 1024 * 1024, 1),
            )
            .with_consumer(
                Consumer::Consensus,
                ConsumerSpec::reserved(16 * 1024 * 1024),
            );
        MemoryPool::new(config)
    }

    fn mock_credit(base_recv: u64, base_send: u64) -> (ConnectionCredit, Arc<MockCreditSink>) {
        let mock = MockCreditSink::new();
        let credit = ConnectionCredit {
            inner: Arc::new(ConnectionCreditInner {
                conn_id: 7,
                sink: CreditSink::Mock(mock.clone()),
                state: Mutex::new(CreditState {
                    base_receive_window: base_recv,
                    active_receive_credit: 0,
                    receive_drain_frontier: 0,
                    draining_receive: VecDeque::new(),
                    base_send_window: base_send,
                    active_send_lease: None,
                    draining_send: VecDeque::new(),
                    closed: false,
                }),
                live_receive_window: Arc::new(AtomicU64::new(base_recv)),
            }),
        };
        (credit, mock)
    }

    #[test]
    fn receive_credit_raises_window_and_drains_unspent_on_delivered_bytes() {
        let pool = test_pool();
        let base_recv = 4 * 1024 * 1024;
        let base_send = 2 * 1024 * 1024;
        let (credit, mock) = mock_credit(base_recv, base_send);

        // Request 1: lease 2 MiB, raise window to 6 MiB.
        let lease1 = pool
            .try_acquire(Consumer::BlockSync, Some(7), 2 * 1024 * 1024)
            .expect("lease1");
        credit.add_receive_credit(lease1.bytes());
        assert_eq!(credit.live_receive_window(), base_recv + 2 * 1024 * 1024);
        assert_eq!(
            mock.last_limits().unwrap().receive_window,
            Some(base_recv + 2 * 1024 * 1024)
        );

        // Peer answers with RangeUnavailable (0 delivered bytes): lower window
        // back to base, but keep the 2 MiB lease charged until delivered_bytes
        // advances by 2 MiB (02 §6).
        credit.finish_receive_credit(2 * 1024 * 1024, Some(lease1));
        assert_eq!(credit.live_receive_window(), base_recv);
        assert!(credit.has_draining());
        assert_eq!(
            pool.consumer_leased(Consumer::BlockSync),
            2 * 1024 * 1024,
            "unspent credit must stay charged before delivered_bytes advances"
        );

        // Partial delivery (1 MiB) does not release yet.
        mock.advance_delivered(1024 * 1024);
        credit.poll_drain();
        assert_eq!(pool.consumer_leased(Consumer::BlockSync), 2 * 1024 * 1024);

        // Overlapping shrink of 1 MiB queues cumulatively after the first 2 MiB.
        let lease2 = pool
            .try_acquire(Consumer::BlockSync, Some(7), 1024 * 1024)
            .expect("lease2");
        credit.add_receive_credit(lease2.bytes());
        credit.finish_receive_credit(1024 * 1024, Some(lease2));
        assert_eq!(pool.consumer_leased(Consumer::BlockSync), 3 * 1024 * 1024);

        // Delivering another 1 MiB (total 2 MiB) releases lease1, keeps lease2.
        mock.advance_delivered(1024 * 1024);
        credit.poll_drain();
        assert_eq!(pool.consumer_leased(Consumer::BlockSync), 1024 * 1024);

        // Delivering final 1 MiB (total 3 MiB) releases lease2.
        mock.advance_delivered(1024 * 1024);
        credit.poll_drain();
        assert_eq!(pool.consumer_leased(Consumer::BlockSync), 0);
        assert!(!credit.has_draining());
    }

    #[test]
    fn receive_credit_drains_immediately_on_connection_close() {
        let pool = test_pool();
        let (credit, mock) = mock_credit(4 * 1024 * 1024, 2 * 1024 * 1024);

        let lease = pool
            .try_acquire(Consumer::BlockSync, Some(7), 2 * 1024 * 1024)
            .expect("lease");
        credit.add_receive_credit(lease.bytes());
        credit.finish_receive_credit(2 * 1024 * 1024, Some(lease));
        assert_eq!(pool.consumer_leased(Consumer::BlockSync), 2 * 1024 * 1024);

        mock.close_conn();
        credit.poll_drain();
        assert_eq!(pool.consumer_leased(Consumer::BlockSync), 0);
        assert!(!credit.has_draining());
    }
}
