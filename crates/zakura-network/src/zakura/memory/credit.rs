//! Per-connection QUIC receive credit and send-window leases (`02 §3`, `02 §6`).
//!
//! Each admitted connection starts with its static base receive window
//! (control share + unsolicited receive bytes of its negotiated reactors) and
//! base send window. Live request leases raise the QUIC receive window via
//! `set_limits` (API-9) before the request goes out, and lower it when the
//! request ends. Output waiting on the send window grows that window toward
//! `2 × cwnd` with a serving lease.
//!
//! Granted QUIC credit can't be revoked (`02 §6`). A lowered receive window
//! keeps its unspent lease until `ConnLoad.receive_credit_excess` shows the
//! peer can no longer use the credit. A lowered send window keeps its lease
//! until the connection accepts a byte after the shrink.
//!
//! [`CreditMachine`] decides all of this from inputs and returns effects. It
//! holds no lease, lock or connection. [`ConnectionCredit`] is the IO shell: it
//! holds the leases by [`CreditLeaseId`], applies `set_limits`, feeds
//! `ConnLoad` snapshots in, and drops released leases after its lock.
//!
//! The machine also says when it next needs a snapshot ([`CreditWake`]). It
//! ticks every [`CREDIT_TICK`] only while its send window is above the base
//! or output waits. Otherwise the connection task sleeps until a snapshot
//! could change a decision, or until another task adds a drain, so an idle
//! connection doesn't wake.

use std::{collections::VecDeque, future, sync::Arc};

use zakura_quic::{Conn as QuicConn, ConnLimits, ConnLoad};
use zakura_rt::sync::{watch, Mutex, Notify};

use super::{PoolLease, ServingLease, ServingPool};
use crate::zakura::{ZakuraConnId, ZakuraPeerId};

/// How often the connection task feeds [`CreditInput::Tick`] while the
/// machine asks for [`CreditWake::Tick`]: one `ConnLoad` publish interval, so
/// each tick can see a new snapshot.
pub(crate) const CREDIT_TICK: std::time::Duration = zakura_quic::LOAD_INTERVAL;

/// Ticks with no queued output and no accepted bytes before an expanded send
/// window shrinks back to its base. A pause between responses shorter than
/// this keeps the window, so a serving connection doesn't regrow it per
/// response.
pub(crate) const SEND_IDLE_TICKS: u32 = 4;

/// Identifies a lease the shell holds for the machine.
pub(crate) type CreditLeaseId = u64;

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

/// One input to [`CreditMachine::step`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CreditInput {
    /// A request leased `bytes` of response credit.
    AddReceive { bytes: u64 },
    /// A request that added `total` bytes of credit ended. `unspent` names the
    /// lease the shell holds for the bytes the peer didn't deliver.
    FinishReceive {
        total: u64,
        unspent: Option<(CreditLeaseId, u64)>,
    },
    /// The shell sent the last [`CreditEffect::SetLimits`] as `set_limits`
    /// call `seq`.
    LimitsSent { seq: u64 },
    /// A load snapshot, read to release drained leases.
    Drain(CreditLoad),
    /// A periodic load snapshot, read to release drained leases and to size
    /// the send window.
    Tick {
        load: CreditLoad,
        /// The serving per-peer cap (`[memory.cap] serving_per_peer`).
        per_peer_cap: u64,
        /// The peer's serving floor: one largest response.
        peer_floor: u64,
    },
    /// The serving pool granted the last [`CreditEffect::GrowSend`] as lease
    /// `id` of `bytes`.
    SendGrown { id: CreditLeaseId, bytes: u64 },
    /// The serving pool refused the last [`CreditEffect::GrowSend`].
    SendRefused,
    /// The connection closed: no credit it granted can be spent any more.
    Closed,
}

/// One effect of [`CreditMachine::step`], which the shell applies in order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CreditEffect {
    /// Call `set_limits` with these limits, then feed back
    /// [`CreditInput::LimitsSent`], or [`CreditInput::Closed`] if it failed.
    SetLimits(ConnLimits),
    /// Set the peer's serving cap.
    SetPeerCap(u64),
    /// Try to lease `bytes` more send window from the serving pool, then feed
    /// back [`CreditInput::SendGrown`] or [`CreditInput::SendRefused`].
    GrowSend(u64),
    /// Merge growth lease `id` into the active send-window lease.
    MergeSend(CreditLeaseId),
    /// Move `bytes` off the active send-window lease into lease `id`, which
    /// drains.
    DrainSend { id: CreditLeaseId, bytes: u64 },
    /// Release the active send-window lease.
    ReleaseActiveSend,
    /// Release lease `id`.
    Release(CreditLeaseId),
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
    id: CreditLeaseId,
    bytes: u64,
    /// The `set_limits` call that applied the shrink, once the shell sent it.
    seq: Option<u64>,
    /// For a send drain, `accepted_send_bytes` in the first snapshot that
    /// reflected the shrink.
    accepted_at_shrink: Option<u64>,
}

impl Drain {
    fn new(id: CreditLeaseId, bytes: u64) -> Self {
        Self {
            id,
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
///   `min(2 × cwnd, per-peer cap)`. It shrinks to the base after
///   [`SEND_IDLE_TICKS`] ticks with no waiting output and no accepted bytes.
///   A shrunk lease drains until the connection accepts a byte after a
///   snapshot reflected the shrink.
#[derive(Clone, Debug)]
pub(crate) struct CreditMachine {
    base_receive_window: u64,
    active_receive_credit: u64,
    receive_drains: VecDeque<Drain>,
    base_send_window: u64,
    active_send: u64,
    send_drains: VecDeque<Drain>,
    /// A [`CreditEffect::GrowSend`] awaits its answer.
    growing: bool,
    /// The serving cap last set for the peer.
    peer_cap: Option<u64>,
    /// `accepted_send_bytes` at the last tick.
    last_accepted: Option<u64>,
    /// The snapshot the machine last read.
    last_load: Option<CreditLoad>,
    idle_ticks: u32,
    closed: bool,
    next_lease: CreditLeaseId,
}

impl CreditMachine {
    pub(crate) fn new(base_receive_window: u64, base_send_window: u64) -> Self {
        Self {
            base_receive_window,
            active_receive_credit: 0,
            receive_drains: VecDeque::new(),
            base_send_window,
            active_send: 0,
            send_drains: VecDeque::new(),
            growing: false,
            peer_cap: None,
            last_accepted: None,
            last_load: None,
            idle_ticks: 0,
            closed: false,
            next_lease: 0,
        }
    }

    /// A fresh ID for a lease the shell hands in.
    pub(crate) fn lease_id(&mut self) -> CreditLeaseId {
        let id = self.next_lease;
        self.next_lease += 1;
        id
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

    /// Takes one input and returns the effects the shell applies, in order.
    pub(crate) fn step(&mut self, input: CreditInput) -> Vec<CreditEffect> {
        let mut effects = Vec::new();
        match input {
            CreditInput::AddReceive { bytes } => {
                if !self.closed && bytes > 0 {
                    self.active_receive_credit = self.active_receive_credit.saturating_add(bytes);
                    effects.push(CreditEffect::SetLimits(self.limits()));
                }
            }
            CreditInput::FinishReceive { total, unspent } => {
                self.active_receive_credit = self.active_receive_credit.saturating_sub(total);
                let mut drains = false;
                if let Some((id, bytes)) = unspent {
                    if self.closed || bytes == 0 {
                        effects.push(CreditEffect::Release(id));
                    } else {
                        self.receive_drains.push_back(Drain::new(id, bytes));
                        drains = true;
                    }
                }
                if !self.closed && (total > 0 || drains) {
                    effects.push(CreditEffect::SetLimits(self.limits()));
                }
            }
            CreditInput::LimitsSent { seq } => {
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
            CreditInput::Drain(load) => {
                if !self.closed {
                    self.last_load = Some(load);
                    self.release_drained(&load, &mut effects);
                }
            }
            CreditInput::Tick {
                load,
                per_peer_cap,
                peer_floor,
            } => {
                if !self.closed {
                    self.last_load = Some(load);
                    self.release_drained(&load, &mut effects);
                    self.size_send_window(&load, per_peer_cap, peer_floor, &mut effects);
                }
            }
            CreditInput::SendGrown { id, bytes } => {
                self.growing = false;
                if self.closed {
                    effects.push(CreditEffect::Release(id));
                } else {
                    self.active_send = self.active_send.saturating_add(bytes);
                    effects.push(CreditEffect::MergeSend(id));
                    effects.push(CreditEffect::SetLimits(self.limits()));
                }
            }
            CreditInput::SendRefused => self.growing = false,
            CreditInput::Closed => {
                if !self.closed {
                    self.closed = true;
                    effects.extend(
                        self.receive_drains
                            .drain(..)
                            .chain(self.send_drains.drain(..))
                            .map(|drain| CreditEffect::Release(drain.id)),
                    );
                    if self.active_send > 0 {
                        self.active_send = 0;
                        effects.push(CreditEffect::ReleaseActiveSend);
                    }
                }
            }
        }
        effects
    }

    /// Releases receive drains the snapshot's excess no longer needs, oldest
    /// first, and send drains whose connection accepted a byte since a
    /// snapshot reflected their shrink.
    fn release_drained(&mut self, load: &CreditLoad, effects: &mut Vec<CreditEffect>) {
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
            effects.push(CreditEffect::Release(front.id));
            self.receive_drains.pop_front();
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
            effects.push(CreditEffect::Release(front.id));
            self.send_drains.pop_front();
        }
    }

    /// Targets `min(2 × cwnd, per_peer_cap)` while output waits, keeps the
    /// window while bytes flow, and returns to the base after
    /// [`SEND_IDLE_TICKS`] idle ticks.
    fn size_send_window(
        &mut self,
        load: &CreditLoad,
        per_peer_cap: u64,
        peer_floor: u64,
        effects: &mut Vec<CreditEffect>,
    ) {
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
            two_cwnd.min(per_peer_cap).max(base) - base
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
        if self.peer_cap != Some(cap) {
            self.peer_cap = Some(cap);
            effects.push(CreditEffect::SetPeerCap(cap));
        }

        if target < self.active_send {
            let bytes = self.active_send - target;
            let id = self.lease_id();
            self.active_send = target;
            self.send_drains.push_back(Drain::new(id, bytes));
            effects.push(CreditEffect::DrainSend { id, bytes });
            effects.push(CreditEffect::SetLimits(self.limits()));
        } else if target > self.active_send && !self.growing {
            self.growing = true;
            effects.push(CreditEffect::GrowSend(target - self.active_send));
        }
    }
}

/// An unspent receive-credit lease the shell holds while it drains.
#[derive(Debug)]
struct DrainingReceive {
    /// Held for its drop, which returns the bytes to the pool.
    _lease: PoolLease,
}

impl DrainingReceive {
    fn new(lease: PoolLease) -> Self {
        #[cfg(any(test, zakura_sim))]
        lease
            .pool()
            .add_draining_receive_bytes(lease.consumer(), lease.bytes());
        Self { _lease: lease }
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

/// The leases the shell holds for its machine.
#[derive(Debug, Default)]
struct HeldLeases {
    receive: Vec<(CreditLeaseId, DrainingReceive)>,
    send: Vec<(CreditLeaseId, ServingLease)>,
    active_send: Option<ServingLease>,
    growth: Option<(CreditLeaseId, ServingLease)>,
}

/// What the shell applies after it unlocks: lease drops and pool calls.
#[derive(Debug, Default)]
#[must_use]
struct Unlocked {
    released_receive: Vec<DrainingReceive>,
    released_send: Vec<ServingLease>,
    peer_cap: Option<u64>,
    grow: Option<u64>,
}

#[derive(Debug)]
struct CreditShell {
    machine: CreditMachine,
    held: HeldLeases,
}

impl CreditShell {
    /// Steps the machine with `input` and applies its effects, feeding back
    /// the inputs `set_limits` answers with.
    fn run(&mut self, connection: &QuicConn, input: CreditInput, unlocked: &mut Unlocked) {
        let mut next = Some(input);
        while let Some(input) = next.take() {
            for effect in self.machine.step(input) {
                match effect {
                    CreditEffect::SetLimits(limits) => {
                        debug_assert!(next.is_none(), "one set_limits per step");
                        next = Some(match connection.set_limits(limits) {
                            Ok(seq) => CreditInput::LimitsSent { seq },
                            Err(_) => CreditInput::Closed,
                        });
                    }
                    CreditEffect::SetPeerCap(cap) => unlocked.peer_cap = Some(cap),
                    CreditEffect::GrowSend(bytes) => unlocked.grow = Some(bytes),
                    CreditEffect::MergeSend(id) => {
                        let Some((_, grown)) = self.held.growth.take_if(|(held, _)| *held == id)
                        else {
                            debug_assert!(false, "the machine merges only the growth it was given");
                            continue;
                        };
                        match &mut self.held.active_send {
                            Some(active) => active.merge(grown),
                            None => self.held.active_send = Some(grown),
                        }
                    }
                    CreditEffect::DrainSend { id, bytes } => {
                        let drained = match &mut self.held.active_send {
                            Some(active) if active.bytes() == bytes => self.held.active_send.take(),
                            Some(active) => active.split(bytes),
                            None => None,
                        };
                        debug_assert!(
                            drained.is_some(),
                            "the machine drains only bytes the active lease holds"
                        );
                        if let Some(drained) = drained {
                            self.held.send.push((id, drained));
                        }
                    }
                    CreditEffect::ReleaseActiveSend => {
                        unlocked.released_send.extend(self.held.active_send.take());
                    }
                    CreditEffect::Release(id) => self.release(id, unlocked),
                }
            }
        }
    }

    fn release(&mut self, id: CreditLeaseId, unlocked: &mut Unlocked) {
        if let Some(index) = self.held.receive.iter().position(|(held, _)| *held == id) {
            unlocked
                .released_receive
                .push(self.held.receive.remove(index).1);
        } else if let Some(index) = self.held.send.iter().position(|(held, _)| *held == id) {
            unlocked.released_send.push(self.held.send.remove(index).1);
        } else if let Some((_, grown)) = self.held.growth.take_if(|(held, _)| *held == id) {
            unlocked.released_send.push(grown);
        } else {
            debug_assert!(false, "the machine releases only leases it was given");
        }
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
    /// (API-9).
    pub(crate) fn new_quic(
        conn_id: ZakuraConnId,
        connection: QuicConn,
        base_receive_window: u64,
        base_send_window: u64,
    ) -> Self {
        let machine = CreditMachine::new(base_receive_window, base_send_window);
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
                    held: HeldLeases::default(),
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

    /// Raise the connection's QUIC receive window by `bytes` before sending a
    /// leased request (`02 §3`).
    pub(crate) fn add_receive_credit(&self, bytes: u64) {
        self.step(CreditInput::AddReceive { bytes }).drop_leases();
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
        let unlocked = {
            let mut shell = self.lock();
            let wake = shell.machine.wake();
            let unspent = unspent_lease.map(|lease| {
                let id = shell.machine.lease_id();
                let bytes = lease.bytes();
                shell.held.receive.push((id, DrainingReceive::new(lease)));
                (id, bytes)
            });
            let unlocked = self.run_locked(
                &mut shell,
                CreditInput::FinishReceive {
                    total: total_credit,
                    unspent,
                },
            );
            // A new drain needs the connection task to watch snapshots.
            if shell.machine.wake() != wake {
                self.inner.wake_changed.notify_one();
            }
            unlocked
        };
        unlocked.drop_leases();
    }

    /// Release the drained credit leases the latest `ConnLoad` allows.
    pub(crate) fn poll_drain(&self) {
        let unlocked = {
            let mut shell = self.lock();
            let input = self.load().map_or(CreditInput::Closed, CreditInput::Drain);
            self.run_locked(&mut shell, input)
        };
        unlocked.drop_leases();
    }

    /// Run one [`CREDIT_TICK`]: release drained leases, set the peer's serving
    /// cap and size the send window, leasing growth from `serving_pool`.
    pub(crate) fn tick_send_window(
        &self,
        serving_pool: &ServingPool,
        peer: &ZakuraPeerId,
        peer_floor: u64,
    ) {
        let input = match self.load() {
            Some(load) => CreditInput::Tick {
                load,
                per_peer_cap: serving_pool.default_per_peer_cap(),
                peer_floor,
            },
            None => CreditInput::Closed,
        };
        let mut unlocked = self.step(input);
        let grow = unlocked.grow.take();
        if let Some(cap) = unlocked.peer_cap.take() {
            serving_pool.set_peer_cap(peer, cap);
        }
        unlocked.drop_leases();

        let Some(need) = grow else {
            return;
        };
        let grown = serving_pool.try_acquire(peer, Some(self.inner.conn_id), peer_floor, need);
        let unlocked = {
            let mut shell = self.lock();
            let input = match grown {
                Ok(lease) => {
                    let id = shell.machine.lease_id();
                    let bytes = lease.bytes();
                    shell.held.growth = Some((id, lease));
                    CreditInput::SendGrown { id, bytes }
                }
                Err(_) => CreditInput::SendRefused,
            };
            self.run_locked(&mut shell, input)
        };
        unlocked.drop_leases();
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
        self.step(CreditInput::Closed).drop_leases();
    }

    /// The latest load snapshot, or `None` once the connection closed.
    fn load(&self) -> Option<CreditLoad> {
        let connection = &self.inner.connection;
        connection
            .close_reason()
            .is_none()
            .then(|| CreditLoad::from(connection.load()))
    }

    fn step(&self, input: CreditInput) -> Unlocked {
        let mut shell = self.lock();
        self.run_locked(&mut shell, input)
    }

    fn run_locked(&self, shell: &mut CreditShell, input: CreditInput) -> Unlocked {
        let mut unlocked = Unlocked::default();
        shell.run(&self.inner.connection, input, &mut unlocked);
        #[cfg(zakura_sim)]
        self.inner.live_receive_window.store(
            shell.machine.receive_window(),
            zakura_rt::sync::counter::Ordering::Relaxed,
        );
        unlocked
    }

    fn lock(&self) -> zakura_rt::sync::MutexGuard<'_, CreditShell> {
        self.inner
            .shell
            .lock()
            .expect("no credit step panics while holding the shell lock")
    }
}

impl Unlocked {
    /// Drops the released leases, after the shell's lock, so their pool
    /// releases and wakeups run outside it.
    fn drop_leases(self) {
        debug_assert!(self.peer_cap.is_none() && self.grow.is_none());
        drop(self.released_receive);
        drop(self.released_send);
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
