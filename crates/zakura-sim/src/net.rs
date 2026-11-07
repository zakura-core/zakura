//! `Net`: the real simulated network as an action source, in place of
//! [`crate::fake::FakeNet`].
//!
//! It wraps a [`SimNet`]. The `Plan` picks every delivery and drop, time
//! stops where a datagram's window opens and at its deadline, and every
//! send and outcome goes to the log as an [`Observation::Datagram`].

use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    rc::Rc,
    time::Duration,
};

use zakura_quic::sim::{DatagramId, DropReason, Link, NetEvent, SimNet};
use zakura_scheduler::{hooks, SimInstant};

use crate::{
    observe::DatagramOutcome, ActionSource, Boundary, Identity, Observation, ObservationLog,
};

/// A [`SimNet`] as an action source. Clones share it, so a harness can keep
/// one to open the loss gate while the run holds another.
#[derive(Clone, Debug)]
pub struct Net(Rc<RefCell<State>>);

#[derive(Debug)]
struct State {
    sim: SimNet,
    /// One moment on the driver's clock, read with `sim_origin`.
    std_origin: std::time::Instant,
    /// The same moment on the run's clock.
    sim_origin: SimInstant,
    /// Whether a link drops nothing until [`Net::admitted`] opens it.
    loss_after_admission: bool,
    /// The links [`Net::admitted`] opened.
    admitted: BTreeSet<Link>,
    /// How many of the network's events have been logged.
    seen: usize,
    /// Each datagram sent and not yet gone: its length and send time.
    in_flight: BTreeMap<DatagramId, (u64, u64)>,
}

impl Net {
    /// A source over `sim`, which must not have sent anything yet. With
    /// `loss_after_admission`, no link drops a datagram until
    /// [`Net::admitted`] opens it.
    ///
    /// It reads the run's clock and the driver's together, so it must be
    /// made inside a run, as setup after `Sim::new` is.
    ///
    /// # Panics
    ///
    /// Outside a run, or if `sim` has already sent a datagram, whose send
    /// time the source can't know.
    pub fn new(sim: SimNet, loss_after_admission: bool) -> Self {
        let sim_origin = hooks::now().expect("a Net is made inside a run, after Sim::new");
        let std_origin = zakura_rt::time::Instant::now().into_std();
        assert!(
            !sim.events()
                .iter()
                .any(|event| matches!(event, NetEvent::Sent { .. })),
            "a Net is made before its network sends anything"
        );
        Net(Rc::new(RefCell::new(State {
            sim,
            std_origin,
            sim_origin,
            loss_after_admission,
            admitted: BTreeSet::new(),
            seen: 0,
            in_flight: BTreeMap::new(),
        })))
    }

    /// The network, for binding endpoints on it.
    pub fn sim(&self) -> SimNet {
        self.0.borrow().sim.clone()
    }

    /// Opens loss on both directed links between `a` and `b`, which a
    /// harness calls when the node admits a session between them. Without
    /// `loss_after_admission` every link is already open.
    pub fn admitted(&self, a: SocketAddr, b: SocketAddr) {
        let mut state = self.0.borrow_mut();
        state.admitted.insert(Link { src: a, dst: b });
        state.admitted.insert(Link { src: b, dst: a });
    }
}

impl State {
    /// `instant`, from the driver's clock, on the run's clock: the origin
    /// moved by the time between them, either way, saturating at the run's
    /// start and at the largest instant. M3's `sim_of` rule, with the origin
    /// fixed when the source was made.
    fn sim_of(&self, instant: std::time::Instant) -> SimInstant {
        match instant.checked_duration_since(self.std_origin) {
            Some(after) => self
                .sim_origin
                .checked_add(after)
                .unwrap_or(SimInstant::from_start(Duration::MAX)),
            None => SimInstant::from_start(
                self.sim_origin
                    .since_start()
                    .saturating_sub(self.std_origin.duration_since(instant)),
            ),
        }
    }

    /// `at`, from the run's clock, on the driver's clock: the inverse of
    /// [`State::sim_of`]. An instant before the driver's clock can hold
    /// saturates at the origin, which only a query before the run's start
    /// on a clock that began after it could reach.
    fn std_of(&self, at: SimInstant) -> std::time::Instant {
        if at >= self.sim_origin {
            self.std_origin + at.saturating_duration_since(self.sim_origin)
        } else {
            self.std_origin
                .checked_sub(self.sim_origin.saturating_duration_since(at))
                .unwrap_or(self.std_origin)
        }
    }

    /// The datagrams the plan may drop now: those its network allows,
    /// on links the gate has opened.
    fn droppable(&self) -> Vec<DatagramId> {
        let mut droppable = self.sim.droppable();
        if self.loss_after_admission {
            droppable.retain(|id| self.admitted.contains(&id.link));
        }
        droppable
    }
}

/// `link` as identities and observations name it: `<src>-><dst>`.
fn link_name(link: Link) -> String {
    format!("{}->{}", link.src, link.dst)
}

/// `t` in nanoseconds since the run's start.
fn nanos(t: SimInstant) -> u64 {
    // u64 nanoseconds cover 584 years of virtual time.
    u64::try_from(t.since_start().as_nanos()).unwrap_or(u64::MAX)
}

/// The `index`-th entry of a list `enabled` built at this step.
fn offered(list: Vec<DatagramId>, index: u64) -> DatagramId {
    usize::try_from(index)
        .ok()
        .and_then(|index| list.get(index).copied())
        .expect("`enabled` offered this datagram at this step, and nothing changed since")
}

impl ActionSource for Net {
    /// A `deliver` for each datagram the network may deliver at `now`, and
    /// a `drop` for each one it may drop.
    fn enabled(&self, now: SimInstant, out: &mut Vec<(Identity, u64)>) {
        let state = self.0.borrow();
        for (index, id) in (0..).zip(state.sim.deliverable_at(state.std_of(now))) {
            out.push((Identity::deliver(&link_name(id.link), id.seq), 2 * index));
        }
        for (index, id) in (0..).zip(state.droppable()) {
            out.push((Identity::drop(&link_name(id.link), id.seq), 2 * index + 1));
        }
    }

    /// Where the next delivery window opens, and the earliest deadline in
    /// flight. A deadline that's already due is `now`, so time can't move
    /// until a datagram goes.
    fn boundaries(&self, now: SimInstant, out: &mut Vec<Boundary>) {
        let state = self.0.borrow();
        if let Some(opens) = state.sim.next_eligible_after(state.std_of(now)) {
            out.push(Boundary::Opens(state.sim_of(opens)));
        }
        if let Some(limit) = state.sim.time_limit() {
            out.push(Boundary::Deadline(state.sim_of(limit).max(now)));
        }
    }

    /// Delivers or drops the datagram `enabled` offered. It makes no draws,
    /// and [`ActionSource::observe`] logs what happened.
    fn execute(&mut self, local: u64, _: &mut ObservationLog) {
        let (sim, deliver, id) = {
            let state = self.0.borrow();
            let deliver = local.is_multiple_of(2);
            let offers = if deliver {
                state.sim.deliverable()
            } else {
                state.droppable()
            };
            (state.sim.clone(), deliver, offered(offers, local / 2))
        };
        // Outside the borrow, since a delivery wakes a reader.
        let result = if deliver {
            sim.deliver(id)
        } else {
            sim.drop_datagram(id)
        };
        result.expect("the network allows what it offered at this step");
    }

    /// Logs each send since the last call, stamped with the step's time, and
    /// each delivery or drop. Sends happen only while a task runs, and time
    /// moves only in steps that run none, so the stamp is the send's time.
    fn observe(&mut self, log: &mut ObservationLog) -> Result<(), String> {
        let mut state = self.0.borrow_mut();
        let events = state.sim.events_since(state.seen);
        state.seen += events.len();
        let now = nanos(hooks::now().expect("a run observes its sources inside the run"));
        for event in events {
            let (id, outcome) = match event {
                NetEvent::Sent { id, len } => {
                    // usize always fits in u64 on supported targets.
                    state.in_flight.insert(id, (len as u64, now));
                    (id, DatagramOutcome::Sent)
                }
                NetEvent::Delivered { id } => (id, DatagramOutcome::Delivered),
                NetEvent::Dropped {
                    id,
                    reason: DropReason::Loss,
                } => (id, DatagramOutcome::Lost),
                NetEvent::Dropped {
                    id,
                    reason: DropReason::Unbound,
                } => (id, DatagramOutcome::Unbound),
                NetEvent::Bound { .. } | NetEvent::Unbound { .. } => continue,
            };
            let (len, sent_at) = match outcome {
                DatagramOutcome::Sent => state.in_flight[&id],
                _ => state.in_flight.remove(&id).ok_or_else(|| {
                    format!("datagram {id:?} left the network without a send the source saw")
                })?,
            };
            log.push(Observation::Datagram {
                id: id.seq,
                link: link_name(id.link),
                len,
                outcome,
                sent_at,
            });
        }
        Ok(())
    }
}
