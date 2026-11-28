//! Per-connection QUIC receive credit and send-window leases (`02 §3`, `02 §6`).
//!
//! Each admitted connection starts with its static base receive window
//! (control share + unsolicited receive bytes of its negotiated reactors) and
//! base send window. Live request leases raise the QUIC receive window via
//! `set_limits` (API-9) before the request goes out, and lower it when the
//! request ends. Output waiting on the send window grows that window toward
//! `2 × cwnd`, up to the admitted send window, with a serving lease.
//!
//! Granted QUIC credit can't be revoked (`02 §6`). A lowered receive window
//! keeps its unspent lease until `ConnLoad.receive_credit_excess` shows the
//! peer can no longer use the credit. A lowered send window keeps its lease
//! until the connection accepts a byte after the shrink.
//!
//! [`CreditMachine`] decides all of this: each method takes one input and
//! returns what the shell applies. It holds no lease, lock or connection.
//! [`ConnectionCredit`] is the IO shell: it holds the leases in the order of
//! the machine's drain queues, applies `set_limits`, feeds `ConnLoad`
//! snapshots in, and drops released leases after its lock.
//!
//! The machine also says when it next needs a snapshot ([`CreditWake`]). It
//! ticks every [`CREDIT_TICK`] only while its send window is above the base
//! or output waits. Otherwise the connection task sleeps until a snapshot
//! could change a decision, or until another task adds a drain, so an idle
//! connection doesn't wake.

use std::{collections::VecDeque, fmt, future, sync::Arc};

use zakura_quic::{Conn as QuicConn, ConnLimits, ConnLoad};
use zakura_rt::sync::{watch, Mutex, Notify};

use super::{MemoryPool, PoolLease};
use crate::zakura::{ZakuraConnId, ZakuraPeerId};

/// How often the connection task calls [`CreditMachine::tick`] while the
/// machine asks for [`CreditWake::Tick`]: one `ConnLoad` publish interval, so
/// each tick can see a new snapshot.
pub(crate) const CREDIT_TICK: std::time::Duration = zakura_quic::LOAD_INTERVAL;

/// Ticks with no queued output and no accepted bytes before an expanded send
/// window shrinks back to its base. A pause between responses shorter than
/// this keeps the window, so a serving connection doesn't regrow it per
/// response.
pub(crate) const SEND_IDLE_TICKS: u32 = 4;

/// The [`ConnLoad`] fields the credit machine reads.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct CreditLoad {
    pub(crate) accepted_send_bytes: u64,
    pub(crate) queued_send_bytes: u64,
    pub(crate) send_blocked_streams: u64,
    pub(crate) cwnd: u64,
    pub(crate) limits_applied: u64,
    pub(crate) receive_credit_excess: u64,
}

impl CreditLoad {
    /// Whether output waits on the send window.
    fn output_waits(&self) -> bool {
        self.queued_send_bytes > 0 || self.send_blocked_streams > 0
    }
}

impl From<ConnLoad> for CreditLoad {
    fn from(load: ConnLoad) -> Self {
        Self {
            accepted_send_bytes: load.accepted_send_bytes,
            queued_send_bytes: load.queued_send_bytes,
            send_blocked_streams: load.send_blocked_streams,
            cwnd: load.cwnd,
            limits_applied: load.limits_applied,
            receive_credit_excess: load.receive_credit_excess,
        }
    }
}

/// The leases a [`CreditMachine`] step releases. Each drain queue releases
/// from its front, so a count names the leases.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[must_use]
pub(crate) struct DrainRelease {
    /// Receive drains to release, oldest first.
    pub(crate) receive: usize,
    /// Send drains to release, oldest first.
    pub(crate) send: usize,
    /// Whether the active send-window lease releases.
    pub(crate) active_send: bool,
}

/// What [`CreditMachine::finish_receive`] decided.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use]
pub(crate) struct FinishReceive {
    /// The limits to apply with `set_limits`, if they changed.
    pub(crate) limits: Option<ConnLimits>,
    /// Whether the unspent lease joins the back of the receive drains.
    /// Otherwise it releases at once: the connection closed, or it holds no
    /// bytes.
    pub(crate) drains: bool,
}

/// How a [`CreditMachine::tick`] changes the send window.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum SendStep {
    /// The window stays.
    #[default]
    Keep,
    /// Try to lease this many more bytes of send window from the serving
    /// pool, then call [`CreditMachine::send_grown`] or
    /// [`CreditMachine::send_refused`].
    Grow(u64),
    /// Move `bytes` off the active send-window lease into a new lease at the
    /// back of the send drains, then apply `limits`.
    Shrink { bytes: u64, limits: ConnLimits },
}

/// What one [`CreditMachine::tick`] decided.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[must_use]
pub(crate) struct TickStep {
    pub(crate) release: DrainRelease,
    /// The peer's new serving cap, if it changed.
    pub(crate) peer_cap: Option<u64>,
    pub(crate) send: SendStep,
}

/// When the connection task next steps a [`CreditMachine`] with a snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CreditWake {
    /// Every [`CREDIT_TICK`]: the send window is above its base, so the idle
    /// count runs, or output waits, so the window may grow.
    Tick,
    /// At the first snapshot that could change a decision: one where output
    /// waits, or, while leases drain, one whose drain fields differ from
    /// `drains_seen`, the snapshot the machine last read.
    Load { drains_seen: Option<CreditLoad> },
    /// Never: the connection closed.
    Never,
}

impl CreditWake {
    /// Whether `load` meets this wake's condition.
    pub(crate) fn wakes_on(&self, load: &CreditLoad) -> bool {
        match self {
            Self::Tick => true,
            Self::Load { drains_seen } => {
                load.output_waits()
                    || drains_seen.is_some_and(|seen| {
                        seen.limits_applied != load.limits_applied
                            || seen.receive_credit_excess != load.receive_credit_excess
                            || seen.accepted_send_bytes != load.accepted_send_bytes
                    })
            }
            Self::Never => false,
        }
    }
}

/// A lease that stays charged after its window shrank.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Drain {
    bytes: u64,
    /// The `set_limits` call that applied the shrink, once the shell sent it.
    seq: Option<u64>,
    /// For a send drain, `accepted_send_bytes` in the first snapshot that
    /// reflected the shrink.
    accepted_at_shrink: Option<u64>,
}

impl Drain {
    fn new(bytes: u64) -> Self {
        Self {
            bytes,
            seq: None,
            accepted_at_shrink: None,
        }
    }

    /// Whether a snapshot whose task applied call `applied` includes the
    /// shrink.
    fn reflected_by(&self, applied: u64) -> bool {
        self.seq.is_some_and(|seq| seq <= applied)
    }
}

/// The receive-credit and send-window policy of one connection (`02 §6`).
///
/// - The receive window is the base plus live request credit. A request that
///   ends lowers it at once, and its unspent lease drains: it stays charged
///   until a snapshot that reflects the shrink shows the remaining excess
///   credit is covered by the drains after it.
/// - With output waiting, the send window targets
///   `min(2 × cwnd, per-peer cap, max send window)`. It shrinks to the base after
///   [`SEND_IDLE_TICKS`] ticks with no waiting output and no accepted bytes.
///   A shrunk lease drains until the connection accepts a byte after a
///   snapshot reflected the shrink.
///
/// Drains queue in order: each method that adds one adds it at the back, and
/// each release takes from the front. The shell holds its leases in the same
/// order.
#[derive(Clone, Debug)]
pub(crate) struct CreditMachine {
    base_receive_window: u64,
    active_receive_credit: u64,
    receive_drains: VecDeque<Drain>,
    base_send_window: u64,
    /// The admitted send window (`[network.zakura.quic] send_window_bytes`),
    /// which the window never grows past.
    max_send_window: u64,
    active_send: u64,
    send_drains: VecDeque<Drain>,
    /// A [`SendStep::Grow`] awaits its answer.
    growing: bool,
    /// The serving cap last set for the peer.
    peer_cap: Option<u64>,
    /// `accepted_send_bytes` at the last tick.
    last_accepted: Option<u64>,
    /// The snapshot the machine last read.
    last_load: Option<CreditLoad>,
    idle_ticks: u32,
    closed: bool,
}

impl CreditMachine {
    pub(crate) fn new(
        base_receive_window: u64,
        base_send_window: u64,
        max_send_window: u64,
    ) -> Self {
        Self {
            base_receive_window,
            active_receive_credit: 0,
            receive_drains: VecDeque::new(),
            base_send_window,
            max_send_window,
            active_send: 0,
            send_drains: VecDeque::new(),
            growing: false,
            peer_cap: None,
            last_accepted: None,
            last_load: None,
            idle_ticks: 0,
            closed: false,
        }
    }

    /// The receive window: the base plus live request credit.
    pub(crate) fn receive_window(&self) -> u64 {
        self.base_receive_window
            .saturating_add(self.active_receive_credit)
    }

    /// The send window: the base plus the active send-window lease.
    pub(crate) fn send_window(&self) -> u64 {
        self.base_send_window.saturating_add(self.active_send)
    }

    /// The limits the connection should have now.
    pub(crate) fn limits(&self) -> ConnLimits {
        ConnLimits::default()
            .with_receive_window(self.receive_window())
            .with_send_window(self.send_window())
    }

    /// Whether any lease drains.
    pub(crate) fn has_drains(&self) -> bool {
        !self.receive_drains.is_empty() || !self.send_drains.is_empty()
    }

    /// When the machine next needs a snapshot.
    pub(crate) fn wake(&self) -> CreditWake {
        if self.closed {
            CreditWake::Never
        } else if self.active_send > 0 || self.last_load.is_some_and(|load| load.output_waits()) {
            CreditWake::Tick
        } else {
            CreditWake::Load {
                drains_seen: self
                    .has_drains()
                    .then(|| self.last_load.unwrap_or_default()),
            }
        }
    }

    /// A request leased `bytes` of response credit. Returns the limits to
    /// apply, if they changed.
    pub(crate) fn add_receive(&mut self, bytes: u64) -> Option<ConnLimits> {
        if self.closed || bytes == 0 {
            return None;
        }
        self.active_receive_credit = self.active_receive_credit.saturating_add(bytes);
        Some(self.limits())
    }

    /// A request that added `total` bytes of credit ended. `unspent` is the
    /// size of the lease the shell holds for the bytes the peer didn't
    /// deliver.
    pub(crate) fn finish_receive(&mut self, total: u64, unspent: Option<u64>) -> FinishReceive {
        self.active_receive_credit = self.active_receive_credit.saturating_sub(total);
        let drains = match unspent {
            Some(bytes) if !self.closed && bytes > 0 => {
                self.receive_drains.push_back(Drain::new(bytes));
                true
            }
            _ => false,
        };
        FinishReceive {
            limits: (!self.closed && (total > 0 || drains)).then(|| self.limits()),
            drains,
        }
    }

    /// The shell applied the last limits as `set_limits` call `seq`. If the
    /// call failed, the shell calls [`Self::close`] instead.
    pub(crate) fn limits_sent(&mut self, seq: u64) {
        // Untagged drains are the newest, at the back of each queue.
        for drain in self
            .receive_drains
            .iter_mut()
            .rev()
            .chain(self.send_drains.iter_mut().rev())
            .filter(|drain| drain.seq.is_none())
        {
            drain.seq = Some(seq);
        }
    }

    /// Reads a load snapshot to release drained leases.
    pub(crate) fn drain(&mut self, load: CreditLoad) -> DrainRelease {
        if self.closed {
            return DrainRelease::default();
        }
        self.last_load = Some(load);
        self.release_drained(&load)
    }

    /// Reads a periodic load snapshot to release drained leases and to size
    /// the send window. `per_peer_cap` is the serving per-peer cap and
    /// `peer_floor` the peer's serving floor: one largest response.
    pub(crate) fn tick(
        &mut self,
        load: CreditLoad,
        per_peer_cap: u64,
        peer_floor: u64,
    ) -> TickStep {
        if self.closed {
            return TickStep::default();
        }
        self.last_load = Some(load);
        let release = self.release_drained(&load);
        let (peer_cap, send) = self.size_send_window(&load, per_peer_cap, peer_floor);
        TickStep {
            release,
            peer_cap,
            send,
        }
    }

    /// The serving pool granted the last [`SendStep::Grow`] as a lease of
    /// `bytes`. Returns the limits to apply once the shell merges the lease
    /// into the active send-window lease, or `None` if the connection closed
    /// and the lease releases.
    pub(crate) fn send_grown(&mut self, bytes: u64) -> Option<ConnLimits> {
        self.growing = false;
        if self.closed {
            return None;
        }
        self.active_send = self.active_send.saturating_add(bytes);
        Some(self.limits())
    }

    /// The serving pool refused the last [`SendStep::Grow`].
    pub(crate) fn send_refused(&mut self) {
        self.growing = false;
    }

    /// The connection closed: no credit it granted can be spent any more, so
    /// every lease releases.
    pub(crate) fn close(&mut self) -> DrainRelease {
        if self.closed {
            return DrainRelease::default();
        }
        self.closed = true;
        let release = DrainRelease {
            receive: self.receive_drains.len(),
            send: self.send_drains.len(),
            active_send: self.active_send > 0,
        };
        self.receive_drains.clear();
        self.send_drains.clear();
        self.active_send = 0;
        release
    }

    /// Releases receive drains the snapshot's excess no longer needs, oldest
    /// first, and send drains whose connection accepted a byte since a
    /// snapshot reflected their shrink.
    fn release_drained(&mut self, load: &CreditLoad) -> DrainRelease {
        let mut release = DrainRelease::default();
        let applied = load.limits_applied;
        // A drain whose shrink the snapshot doesn't reflect isn't in its
        // excess, so only reflected drains (a prefix) cover the excess.
        let mut reflected: u64 = self
            .receive_drains
            .iter()
            .take_while(|drain| drain.reflected_by(applied))
            .map(|drain| drain.bytes)
            .sum();
        while let Some(front) = self.receive_drains.front() {
            if !front.reflected_by(applied) || reflected - front.bytes < load.receive_credit_excess
            {
                break;
            }
            reflected -= front.bytes;
            self.receive_drains.pop_front();
            release.receive += 1;
        }

        for drain in self
            .send_drains
            .iter_mut()
            .take_while(|drain| drain.reflected_by(applied))
        {
            drain
                .accepted_at_shrink
                .get_or_insert(load.accepted_send_bytes);
        }
        while let Some(front) = self.send_drains.front() {
            if !front
                .accepted_at_shrink
                .is_some_and(|accepted| load.accepted_send_bytes > accepted)
            {
                break;
            }
            self.send_drains.pop_front();
            release.send += 1;
        }
        release
    }

    /// Targets `min(2 × cwnd, per_peer_cap, max send window)` while output
    /// waits, keeps the
    /// window while bytes flow, and returns to the base after
    /// [`SEND_IDLE_TICKS`] idle ticks. Returns the peer's new serving cap, if
    /// it changed, and the window's change.
    fn size_send_window(
        &mut self,
        load: &CreditLoad,
        per_peer_cap: u64,
        peer_floor: u64,
    ) -> (Option<u64>, SendStep) {
        let base = self.base_send_window;
        let two_cwnd = load.cwnd.saturating_mul(2);
        let waiting = load.output_waits();
        let sending = self
            .last_accepted
            .is_some_and(|accepted| load.accepted_send_bytes > accepted);
        self.last_accepted = Some(load.accepted_send_bytes);
        self.idle_ticks = if waiting || sending {
            0
        } else {
            self.idle_ticks.saturating_add(1)
        };
        let target = if waiting {
            two_cwnd
                .min(per_peer_cap)
                .min(self.max_send_window)
                .max(base)
                - base
        } else if self.idle_ticks >= SEND_IDLE_TICKS {
            0
        } else {
            self.active_send
        };

        // `02 §4`: a serving peer's cap is `max(largest response, 2 × cwnd)`
        // of queued output plus its send-window lease.
        let cap = peer_floor
            .max(two_cwnd)
            .saturating_add(target)
            .min(per_peer_cap);
        let peer_cap = (self.peer_cap != Some(cap)).then_some(cap);
        self.peer_cap = Some(cap);

        let send = if target < self.active_send {
            let bytes = self.active_send - target;
            self.active_send = target;
            self.send_drains.push_back(Drain::new(bytes));
            SendStep::Shrink {
                bytes,
                limits: self.limits(),
            }
        } else if target > self.active_send && !self.growing {
            self.growing = true;
            SendStep::Grow(target - self.active_send)
        } else {
            SendStep::Keep
        };
        (peer_cap, send)
    }
}

/// A caller's own count of a drained lease's bytes, which the shell holds
/// with the lease and drops when the lease releases.
pub(crate) type DrainHold = Box<dyn fmt::Debug + Send>;

/// An unspent receive-credit lease the shell holds while it drains.
#[derive(Debug)]
struct DrainingReceive {
    /// Held for its drop, which returns the bytes to the pool.
    _lease: PoolLease,
    /// Held for its drop too. See [`DrainHold`].
    _hold: Option<DrainHold>,
}

impl DrainingReceive {
    fn new(lease: PoolLease, hold: Option<DrainHold>) -> Self {
        #[cfg(any(test, zakura_sim))]
        lease
            .pool()
            .add_draining_receive_bytes(lease.consumer(), lease.bytes());
        Self {
            _lease: lease,
            _hold: hold,
        }
    }
}

impl Drop for DrainingReceive {
    fn drop(&mut self) {
        #[cfg(any(test, zakura_sim))]
        self._lease
            .pool()
            .sub_draining_receive_bytes(self._lease.consumer(), self._lease.bytes());
    }
}

/// Leases a step released, which the shell drops after its lock so their
/// pool releases and wakeups run outside it.
#[derive(Debug, Default)]
#[must_use]
struct Released {
    receive: Vec<DrainingReceive>,
    send: Vec<PoolLease>,
}

/// A [`CreditMachine`] and the leases it decides on, held in the order of
/// the machine's drain queues.
#[derive(Debug)]
struct CreditShell {
    machine: CreditMachine,
    receive: VecDeque<DrainingReceive>,
    send: VecDeque<PoolLease>,
    active_send: Option<PoolLease>,
}

impl CreditShell {
    /// Applies `limits` with `set_limits` and tells the machine the call, or
    /// closes the machine if the connection is gone.
    fn set_limits(
        &mut self,
        connection: &QuicConn,
        limits: Option<ConnLimits>,
        released: &mut Released,
    ) {
        let Some(limits) = limits else {
            return;
        };
        match connection.set_limits(limits) {
            Ok(seq) => self.machine.limits_sent(seq),
            Err(_) => self.close(released),
        }
    }

    /// Moves the leases the machine released into `released`.
    fn release(&mut self, release: DrainRelease, released: &mut Released) {
        released
            .receive
            .extend(self.receive.drain(..release.receive));
        released.send.extend(self.send.drain(..release.send));
        if release.active_send {
            released.send.extend(self.active_send.take());
        }
    }

    fn close(&mut self, released: &mut Released) {
        let release = self.machine.close();
        self.release(release, released);
    }

    /// Moves `bytes` off the active send-window lease to the back of the send
    /// drains.
    fn drain_send(&mut self, bytes: u64) {
        let drained = match &mut self.active_send {
            Some(active) if active.bytes() == bytes => self.active_send.take(),
            Some(active) => active.split(bytes),
            None => None,
        };
        debug_assert!(
            drained.is_some(),
            "the machine drains only bytes the active lease holds"
        );
        self.send.extend(drained);
    }

    /// Merges a granted growth lease into the active send-window lease, or
    /// releases it if the connection closed.
    fn send_grown(&mut self, connection: &QuicConn, grown: PoolLease, released: &mut Released) {
        let Some(limits) = self.machine.send_grown(grown.bytes()) else {
            released.send.push(grown);
            return;
        };
        match &mut self.active_send {
            Some(active) => active.merge(grown),
            None => self.active_send = Some(grown),
        }
        self.set_limits(connection, Some(limits), released);
    }

    /// Checks that the shell holds one lease per machine drain, and an active
    /// send-window lease of the machine's size. The shell releases by
    /// position, so a misalignment would release the wrong leases.
    fn debug_assert_aligned(&self) {
        debug_assert_eq!(self.machine.receive_drains.len(), self.receive.len());
        debug_assert_eq!(self.machine.send_drains.len(), self.send.len());
        debug_assert_eq!(
            self.machine.active_send,
            self.active_send.as_ref().map_or(0, PoolLease::bytes)
        );
    }
}

#[derive(Debug)]
struct ConnectionCreditInner {
    conn_id: ZakuraConnId,
    connection: QuicConn,
    shell: Mutex<CreditShell>,
    /// Woken when another task changes the machine's [`CreditWake`].
    wake_changed: Notify,
    /// The machine's receive window, for the simulator's boundary probes.
    #[cfg(zakura_sim)]
    live_receive_window: Arc<zakura_rt::sync::counter::AtomicU64>,
}

/// The IO shell of one connection's [`CreditMachine`] (`02 §3`, `02 §6`).
#[derive(Clone, Debug)]
pub(crate) struct ConnectionCredit {
    inner: Arc<ConnectionCreditInner>,
}

impl ConnectionCredit {
    /// Create the credit of one admitted QUIC connection at
    /// `base_receive_window` and `base_send_window`, and apply those limits
    /// (API-9). The send window never grows past `max_send_window`.
    pub(crate) fn new_quic(
        conn_id: ZakuraConnId,
        connection: QuicConn,
        base_receive_window: u64,
        base_send_window: u64,
        max_send_window: u64,
    ) -> Self {
        let machine = CreditMachine::new(base_receive_window, base_send_window, max_send_window);
        let _ = connection.set_limits(machine.limits());
        Self {
            inner: Arc::new(ConnectionCreditInner {
                conn_id,
                connection,
                #[cfg(zakura_sim)]
                live_receive_window: Arc::new(zakura_rt::sync::counter::AtomicU64::new(
                    machine.receive_window(),
                )),
                shell: Mutex::new(CreditShell {
                    machine,
                    receive: VecDeque::new(),
                    send: VecDeque::new(),
                    active_send: None,
                }),
                wake_changed: Notify::new(),
            }),
        }
    }

    /// The live receive window, for the simulator's boundary probes.
    #[cfg(zakura_sim)]
    pub(crate) fn live_receive_window_atomic(&self) -> Arc<zakura_rt::sync::counter::AtomicU64> {
        self.inner.live_receive_window.clone()
    }

    /// The receive window the machine sets: the base plus live request
    /// credit.
    #[cfg(test)]
    pub(crate) fn receive_window(&self) -> u64 {
        self.lock().machine.receive_window()
    }

    /// Raise the connection's QUIC receive window by `bytes` before sending a
    /// leased request (`02 §3`).
    pub(crate) fn add_receive_credit(&self, bytes: u64) {
        self.step(|shell, connection, released| {
            let limits = shell.machine.add_receive(bytes);
            shell.set_limits(connection, limits, released);
        });
    }

    /// Lower the connection's QUIC receive window by `total_credit` when a
    /// request (or one height of a request) completes or ends early (`02 §6`).
    ///
    /// `unspent_lease` covers the credit the peer didn't deliver
    /// (`RangeUnavailable`, a shorter body, a timeout). It stays charged until
    /// the peer can no longer spend that credit, or the connection closes.
    pub(crate) fn finish_receive_credit(
        &self,
        total_credit: u64,
        unspent_lease: Option<PoolLease>,
    ) {
        self.finish_receive_credit_held(total_credit, unspent_lease, None);
    }

    /// [`Self::finish_receive_credit`], holding `hold` with the unspent lease
    /// until the lease releases.
    pub(crate) fn finish_receive_credit_held(
        &self,
        total_credit: u64,
        unspent_lease: Option<PoolLease>,
        hold: Option<DrainHold>,
    ) {
        self.step(|shell, connection, released| {
            let wake = shell.machine.wake();
            let finish = shell
                .machine
                .finish_receive(total_credit, unspent_lease.as_ref().map(PoolLease::bytes));
            if let Some(lease) = unspent_lease {
                let lease = DrainingReceive::new(lease, hold);
                if finish.drains {
                    shell.receive.push_back(lease);
                } else {
                    released.receive.push(lease);
                }
            }
            shell.set_limits(connection, finish.limits, released);
            // A new drain needs the connection task to watch snapshots.
            if shell.machine.wake() != wake {
                self.inner.wake_changed.notify_one();
            }
        });
    }

    /// Release the drained credit leases the latest `ConnLoad` allows.
    pub(crate) fn poll_drain(&self) {
        let load = self.load();
        self.step(|shell, _, released| {
            let release = match load {
                Some(load) => shell.machine.drain(load),
                None => shell.machine.close(),
            };
            shell.release(release, released);
        });
    }

    /// Run one [`CREDIT_TICK`]: release drained leases, set the peer's serving
    /// cap and size the send window, leasing growth from serving in `pool`.
    pub(crate) fn tick_send_window(&self, pool: &MemoryPool, peer: &ZakuraPeerId, peer_floor: u64) {
        let Some(load) = self.load() else {
            self.close();
            return;
        };
        let per_peer_cap = pool.serving_per_peer_cap();
        let (step, released) = self.locked(|shell, connection, released| {
            let step = shell.machine.tick(load, per_peer_cap, peer_floor);
            shell.release(step.release, released);
            if let SendStep::Shrink { bytes, limits } = step.send {
                shell.drain_send(bytes);
                shell.set_limits(connection, Some(limits), released);
            }
            step
        });
        if let Some(cap) = step.peer_cap {
            pool.set_serving_peer_cap(peer, cap);
        }
        drop(released);

        let SendStep::Grow(need) = step.send else {
            return;
        };
        let grown = pool.try_acquire_serving(peer, Some(self.inner.conn_id), peer_floor, need);
        self.step(|shell, connection, released| match grown {
            Ok(lease) => shell.send_grown(connection, lease, released),
            Err(_) => shell.machine.send_refused(),
        });
    }

    /// A waiter for this connection's next credit step, for the connection
    /// task.
    pub(crate) fn waiter(&self) -> CreditWaiter {
        let mut ticks = zakura_rt::time::interval(CREDIT_TICK);
        ticks.set_missed_tick_behavior(zakura_rt::time::MissedTickBehavior::Skip);
        CreditWaiter {
            wake: self.lock().machine.wake(),
            loads: self.inner.connection.subscribe_load(),
            credit: self.clone(),
            ticks,
        }
    }

    /// Release every credit lease on connection close (`02 §6`).
    pub(crate) fn close(&self) {
        self.step(|shell, _, released| shell.close(released));
    }

    /// The latest load snapshot, or `None` once the connection closed.
    fn load(&self) -> Option<CreditLoad> {
        let connection = &self.inner.connection;
        connection
            .close_reason()
            .is_none()
            .then(|| CreditLoad::from(connection.load()))
    }

    /// Runs `step` under the shell's lock, then drops the leases it released.
    fn step<T>(&self, step: impl FnOnce(&mut CreditShell, &QuicConn, &mut Released) -> T) -> T {
        let (out, released) = self.locked(step);
        drop(released);
        out
    }

    /// Runs `step` under the shell's lock and returns the leases it released,
    /// for the caller to drop after the lock.
    fn locked<T>(
        &self,
        step: impl FnOnce(&mut CreditShell, &QuicConn, &mut Released) -> T,
    ) -> (T, Released) {
        let mut released = Released::default();
        let mut shell = self.lock();
        let out = step(&mut shell, &self.inner.connection, &mut released);
        shell.debug_assert_aligned();
        #[cfg(zakura_sim)]
        self.inner.live_receive_window.store(
            shell.machine.receive_window(),
            zakura_rt::sync::counter::Ordering::Relaxed,
        );
        (out, released)
    }

    fn lock(&self) -> zakura_rt::sync::MutexGuard<'_, CreditShell> {
        self.inner
            .shell
            .lock()
            .expect("no credit step panics while holding the shell lock")
    }
}

/// Waits until a connection's [`CreditMachine`] next needs a step, as its
/// [`CreditWake`] says.
#[derive(Debug)]
pub(crate) struct CreditWaiter {
    credit: ConnectionCredit,
    wake: CreditWake,
    ticks: zakura_rt::time::Interval,
    loads: watch::Receiver<ConnLoad>,
}

impl CreditWaiter {
    /// Resolves when the machine next needs a step: at the next
    /// [`CREDIT_TICK`], or at the first snapshot that meets the wake's
    /// condition, or once another task changes the wake.
    pub(crate) async fn wait(&mut self) {
        match self.wake {
            CreditWake::Tick => {
                self.ticks.tick().await;
            }
            wake @ CreditWake::Load { .. } => {
                zakura_rt::select! {
                    () = self.credit.inner.wake_changed.notified() => {}
                    // A closed connection drops the sender; its step sees the close.
                    _ = self.loads.wait_for(|load| wake.wakes_on(&CreditLoad::from(*load))) => {}
                }
            }
            CreditWake::Never => future::pending().await,
        }
    }

    /// Reads the machine's wake after a step. A switch to ticking starts a
    /// full [`CREDIT_TICK`] after this step.
    pub(crate) fn rearm(&mut self) {
        let wake = self.credit.lock().machine.wake();
        if wake == CreditWake::Tick && self.wake != CreditWake::Tick {
            self.ticks.reset();
        }
        self.wake = wake;
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
mod tests;
