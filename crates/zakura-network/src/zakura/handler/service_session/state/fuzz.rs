//! Machine test of the service-session state, driven by fuzz bytes.
//!
//! [`replay_service_session_bytes`] decodes a policy and a sequence of
//! connection-loop inputs, applies each input to [`PendingState`] and
//! [`ServiceSessionState`] for one three-member service, and checks both
//! machines' invariants after every input. It records what the connection
//! loop learns from each result (the offer the arrivals built, the generation
//! each direction installed, the wait it holds, the backoffs it got) and
//! checks each later result against those records. No record predicts a
//! result; the named tests in `tests.rs` pin which result each rule picks. A
//! violation panics with the property ID it breaks:
//!
//! - `session/offer-assembly`: one incomplete offer per service, whose deadline
//!   only its first member sets, and whose expiry or conflict defers the next.
//! - `session/one-active-generation`: at most one active generation in each
//!   opening direction, which only the exit of that exact generation clears.
//! - `session/legal-reopen`: only the designated opener reopens; the connection
//!   holds a reopen wait exactly while the state is waiting; backoffs grow and
//!   cap; retirement blocks every later wait.

use std::{sync::Arc, time::Duration};

use zakura_rt::time::Instant;

use super::{
    ConnectionRoles, DemandChange, DemandWait, MemberArrival, MemberReservation, PendingState,
    RemoteCompletion, RemoteDemand, ServiceSessionState, SessionExit,
    ORDERED_STREAM_REOPEN_BACKOFF, ORDERED_STREAM_REOPEN_BACKOFF_CAP,
};
use crate::zakura::{
    machine_test::MAX_HISTORY_BYTES, transport::SessionLayout, SessionOpening, SessionPolicy,
    Stream,
};

const OFFER_ASSEMBLY: &str = "session/offer-assembly";
const ONE_ACTIVE_GENERATION: &str = "session/one-active-generation";
const LEGAL_REOPEN: &str = "session/legal-reopen";

pub(super) const MEMBER_A: Stream = Stream {
    kind: 64,
    version: 1,
    frame_cap: 1024,
    capability: 1 << 16,
    ..Stream::PERSISTENT
};
pub(super) const MEMBER_B: Stream = Stream {
    kind: 65,
    ..MEMBER_A
};
pub(super) const MEMBER_C: Stream = Stream {
    kind: 67,
    ..MEMBER_A
};
/// A stream outside the layout.
pub(super) const FOREIGN: Stream = Stream {
    kind: 70,
    ..MEMBER_A
};
pub(super) const SETUP_TIMEOUT: Duration = Duration::from_millis(100);

/// The layout members, then the foreign stream.
const MEMBERS: [Stream; 4] = [MEMBER_A, MEMBER_B, MEMBER_C, FOREIGN];

/// The index of [`FOREIGN`] in [`MEMBERS`].
const FOREIGN_MEMBER: usize = 3;

/// The three-member service that every replay assembles.
pub(super) fn layout() -> SessionLayout {
    SessionLayout::from_validated(Arc::from([MEMBER_A, MEMBER_B, MEMBER_C]))
}

/// Counts of the rules one replay exercised.
///
/// A property whose counter stays zero across a campaign was never applicable.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ServiceSessionReplaySummary {
    /// Offers whose every member arrived.
    pub completed_offers: usize,
    /// Members the state refused as an invalid offer.
    pub invalid_members: usize,
    /// Members that named another offer than the incomplete one.
    pub conflicting_members: usize,
    /// Incomplete offers dropped at their deadline.
    pub expired_offers: usize,
    /// Members refused because an expiry or conflict deferred the service.
    pub deferred_members: usize,
    /// Complete offers parked because this endpoint won the collision.
    pub parked_collisions: usize,
    /// Complete offers adopted because this endpoint lost the collision.
    pub adopted_collisions: usize,
    /// Generations this endpoint opened.
    pub local_opens: usize,
    /// Exits of a generation that was no longer active.
    pub stale_exits: usize,
    /// Transport backoffs the state started.
    pub reopen_backoffs: usize,
    /// Demands that retired the service.
    pub retirements: usize,
}

/// Replay at most [`MAX_HISTORY_BYTES`] bytes as one connection's inputs to
/// one service's session state, checking every step against the records.
///
/// # Panics
///
/// Panics with the broken property ID when a result contradicts the records
/// or a machine invariant fails.
pub fn replay_service_session_bytes(data: &[u8]) -> ServiceSessionReplaySummary {
    let mut input = InputReader::new(&data[..data.len().min(MAX_HISTORY_BYTES)]);
    let (policy, roles) = draw_policy(&mut input);
    let mut harness = SessionHarness::new(policy, roles);
    while !input.is_exhausted() {
        let op = Op::draw(&mut input);
        harness.step(op);
    }
    harness.summary
}

/// Reads the replay's input choices from fuzz bytes, one draw per call.
///
/// Decoder v1 draws zero after exhaustion. Machine artifacts retain this
/// decoder; simulator Plans use a separate format.
#[derive(Debug)]
struct InputReader<'a> {
    bytes: &'a [u8],
}

impl<'a> InputReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    fn is_exhausted(&self) -> bool {
        self.bytes.is_empty()
    }

    fn byte(&mut self) -> u8 {
        let Some((&first, rest)) = self.bytes.split_first() else {
            return 0;
        };
        self.bytes = rest;
        first
    }

    fn flag(&mut self) -> bool {
        self.byte() & 1 == 1
    }

    /// A draw in `0..bound`. `bound` must be nonzero.
    fn below(&mut self, bound: u8) -> u8 {
        self.byte() % bound
    }

    /// A delay of up to `max_millis - 1` milliseconds.
    fn millis(&mut self, max_millis: u8) -> Duration {
        Duration::from_millis(u64::from(self.below(max_millis)))
    }
}

fn draw_policy(input: &mut InputReader<'_>) -> (SessionPolicy, ConnectionRoles) {
    let bits = input.byte();
    let policy = SessionPolicy {
        opening: if bits & 1 == 1 {
            SessionOpening::EitherSide
        } else {
            SessionOpening::InitiatorOnly
        },
        reopen: bits & 2 == 2,
    };
    let roles = ConnectionRoles {
        is_initiator: bits & 4 == 4,
        i_open_collision_winner: bits & 8 == 8,
    };
    (policy, roles)
}

/// One input the connection loop delivers, in the order it would deliver it.
#[derive(Clone, Debug)]
enum Op {
    /// A member finishes inbound setup after `advance`. A complete offer is
    /// adopted if `open_now`, else parked under a deferred demand.
    Arrive {
        member: usize,
        id: u64,
        advance: Duration,
        open_now: bool,
    },
    /// Every layout member of offer `id` arrives back to back, in layout
    /// order. If `racing`, the reopen wait fires and opens a local generation
    /// after the last member's stream is accepted but before its setup ends.
    Offer {
        id: u64,
        open_now: bool,
        racing: bool,
    },
    /// The pending deadline timer fires after `advance`.
    Expire { advance: Duration },
    /// The service reports demand outside any open.
    Demand(DemandWait),
    /// The service's reopen wait fires. The service then reports
    /// `OpenNow` if `open_now`, and a local open succeeds if `opens`.
    WaitFires { open_now: bool, opens: bool },
    /// Every worker of the `generation`-th admitted generation finishes.
    Exit { generation: usize },
}

impl Op {
    /// Mostly arrivals and whole offers; the other inputs less often.
    fn draw(input: &mut InputReader<'_>) -> Self {
        match input.below(16) {
            0..=4 => Self::Arrive {
                member: draw_member(input),
                id: draw_wire_id(input),
                advance: input.millis(60),
                open_now: input.flag(),
            },
            5..=7 => Self::Offer {
                id: draw_wire_id(input),
                open_now: input.flag(),
                racing: input.below(4) == 0,
            },
            8 => Self::Expire {
                advance: input.millis(150),
            },
            9 => Self::Demand(match input.below(4) {
                0 => DemandWait::OpenNow,
                1 | 2 => DemandWait::Later,
                _ => DemandWait::Retire,
            }),
            // A local open succeeds one time in three, so transport backoffs
            // accumulate up to their cap.
            10..=13 => Self::WaitFires {
                open_now: input.flag(),
                opens: input.below(3) == 0,
            },
            _ => Self::Exit {
                generation: usize::from(input.below(8)),
            },
        }
    }
}

/// Mostly a layout member; one draw in ten is the foreign stream.
fn draw_member(input: &mut InputReader<'_>) -> usize {
    match input.below(10) {
        9 => FOREIGN_MEMBER,
        draw => usize::from(draw % 3),
    }
}

/// Mostly one of two offers; one draw in ten is the invalid zero identifier.
fn draw_wire_id(input: &mut InputReader<'_>) -> u64 {
    match input.below(10) {
        9 => 0,
        draw => 1 + u64::from(draw % 2),
    }
}

/// Panic with `property` unless `holds`.
#[track_caller]
fn check(property: &str, holds: bool, fact: impl FnOnce() -> String) {
    assert!(holds, "{property} violated: {}", fact());
}

/// The incomplete offer, as the state's arrival results recorded it.
#[derive(Debug)]
struct Offer {
    id: u64,
    /// The indexes of the members whose arrival returned `Waiting`.
    members: Vec<usize>,
    /// When the first member arrived.
    first_at: Instant,
}

/// Whether the connection loop holds its one wait for the service, as
/// `ServiceSessionIO` keeps it.
#[derive(Debug, Default)]
struct WaitSlot(bool);

impl WaitSlot {
    fn apply(&mut self, change: DemandChange) {
        match change {
            DemandChange::Keep => check(LEGAL_REOPEN, self.0, || {
                "a demand kept a wait the connection does not hold".to_owned()
            }),
            DemandChange::Cancel => self.0 = false,
            DemandChange::Start => self.0 = true,
        }
    }
}

/// One admitted generation and whether its exit was delivered.
#[derive(Debug)]
struct Generation {
    id: u64,
    opened_locally: bool,
    exited: bool,
}

/// The machines, and what the connection loop recorded from their results.
///
/// The records hold only inputs and results: the offer the arrivals built,
/// the generations each direction installed, the wait the loop holds, and the
/// backoffs it received. No record predicts a result.
#[derive(Debug)]
struct SessionHarness {
    layout: SessionLayout,
    stream: Stream,
    policy: SessionPolicy,
    now: Instant,
    pending: PendingState,
    session: ServiceSessionState,
    offer: Option<Offer>,
    /// When the last conflict or expiry deferred the service.
    deferred_at: Option<Instant>,
    /// The generation installed in each direction: local, then remote.
    installed: [Option<u64>; 2],
    wait: WaitSlot,
    /// Whether the last demand that changed the wait retired the service.
    retired: bool,
    /// The last transport backoff the state returned.
    last_backoff: Option<Duration>,
    generations: Vec<Generation>,
    next_id: u64,
    summary: ServiceSessionReplaySummary,
}

impl SessionHarness {
    fn new(policy: SessionPolicy, roles: ConnectionRoles) -> Self {
        let stream = MEMBER_A;
        Self {
            layout: layout(),
            stream,
            policy,
            now: Instant::now(),
            pending: PendingState::default(),
            session: ServiceSessionState::new(stream, policy, roles),
            offer: None,
            deferred_at: None,
            installed: [None, None],
            wait: WaitSlot::default(),
            retired: false,
            last_backoff: None,
            generations: Vec::new(),
            next_id: 1,
            summary: ServiceSessionReplaySummary::default(),
        }
    }

    fn step(&mut self, op: Op) {
        match op {
            Op::Arrive {
                member,
                id,
                advance,
                open_now,
            } => self.arrive(member, id, advance, open_now),
            Op::Offer {
                id,
                open_now,
                racing,
            } => {
                for member in 0..3 {
                    if !self.accept() {
                        continue;
                    }
                    if racing && member == 2 {
                        self.wait_fires(true, true);
                    }
                    self.finish_setup(member, id, open_now);
                }
            }
            Op::Expire { advance } => self.expire(advance),
            Op::Demand(demand) => self.demand(demand),
            Op::WaitFires { open_now, opens } => self.wait_fires(open_now, opens),
            Op::Exit { generation } => self.exit(generation),
        }
        self.check_observations();
    }

    fn arrive(&mut self, member: usize, id: u64, advance: Duration, open_now: bool) {
        self.now += advance;
        if self.accept() {
            self.finish_setup(member, id, open_now);
        }
    }

    /// Whether the last conflict or expiry still defers the service.
    fn deferred(&self) -> bool {
        self.deferred_at
            .is_some_and(|at| self.now < at + SETUP_TIMEOUT)
    }

    /// A peer-opened member stream arrives. Returns whether its setup starts:
    /// the service neither rejects the offer nor defers the member.
    fn accept(&mut self) -> bool {
        let [_, remote] = self.installed;
        let rejects = self.session.rejects_remote_offer();
        // A second remote generation would break the one-generation bound.
        check(ONE_ACTIVE_GENERATION, rejects || remote.is_none(), || {
            format!("accepted an offer beside remote generation {remote:?}")
        });
        if rejects {
            return false;
        }
        let reservation = self.pending.reservation(self.stream.kind, self.now);
        let (deferred, offer) = (self.deferred(), self.offer.is_some());
        check(
            OFFER_ASSEMBLY,
            match reservation {
                MemberReservation::Deferred => deferred,
                MemberReservation::Share => offer,
                MemberReservation::Reserve => !offer,
            },
            || format!("{reservation:?}, deferred: {deferred}, offer pending: {offer}"),
        );
        if reservation == MemberReservation::Deferred {
            self.summary.deferred_members += 1;
            return false;
        }
        true
    }

    /// An accepted member finishes setup. A complete offer joins the service.
    fn finish_setup(&mut self, member: usize, id: u64, open_now: bool) {
        let arrival =
            self.pending
                .arrive(&self.layout, id, MEMBERS[member], SETUP_TIMEOUT, self.now);
        // Every result but `Waiting` drops the offer.
        let offer = self.offer.take();
        let (offer_id, mut members, first_at) = offer.map_or((id, Vec::new(), self.now), |offer| {
            (offer.id, offer.members, offer.first_at)
        });
        let duplicate = members.contains(&member);
        let late = self.now >= first_at + SETUP_TIMEOUT;
        let invalid = member == FOREIGN_MEMBER || id == 0;
        let fact = || {
            format!(
                "{arrival:?}: member {member}, id {id}, offer {offer_id} with {members:?}, \
                 late: {late}"
            )
        };
        let Ok(arrival) = arrival else {
            check(OFFER_ASSEMBLY, invalid || duplicate || late, fact);
            self.summary.invalid_members += 1;
            return;
        };
        if arrival == MemberArrival::Conflict {
            check(OFFER_ASSEMBLY, offer_id != id, fact);
            self.deferred_at = Some(self.now);
            self.summary.conflicting_members += 1;
            return;
        }
        check(
            OFFER_ASSEMBLY,
            offer_id == id && !invalid && !duplicate && !late,
            fact,
        );
        members.push(member);
        if arrival == MemberArrival::Waiting {
            check(OFFER_ASSEMBLY, members.len() < 3, || {
                format!("waiting with every member: {members:?}")
            });
            self.offer = Some(Offer {
                id,
                members,
                first_at,
            });
            return;
        }
        check(OFFER_ASSEMBLY, members.len() == 3, || {
            format!("complete with {members:?}")
        });
        self.summary.completed_offers += 1;

        let generation = self.admit(false);
        let [local, remote] = self.installed;
        let completion = self.session.remote_completed(generation);
        // A collision is a peer generation that completes while a local one
        // is installed. Named tests pin which side wins it.
        check(
            ONE_ACTIVE_GENERATION,
            (completion == RemoteCompletion::CheckDemand) == local.is_none(),
            || format!("{completion:?} with local generation {local:?}"),
        );
        if completion == RemoteCompletion::ParkCollision {
            check(
                ONE_ACTIVE_GENERATION,
                self.session.installed(false) == remote,
                || "parking a collision installed the peer's generation".to_owned(),
            );
            self.summary.parked_collisions += 1;
            return;
        }
        if completion == RemoteCompletion::AdoptCollision {
            self.summary.adopted_collisions += 1;
        }
        // As the connection loop does: a lost collision implies demand.
        let demand = if completion == RemoteCompletion::AdoptCollision || open_now {
            DemandWait::OpenNow
        } else {
            DemandWait::Later
        };
        let decision = self.session.remote_demand(generation, demand);
        match decision {
            RemoteDemand::Adopt => {
                check(LEGAL_REOPEN, demand == DemandWait::OpenNow, || {
                    format!("adopted under {demand:?}")
                });
                self.installed[1] = Some(generation);
                self.wait.apply(DemandChange::Cancel);
            }
            RemoteDemand::Park(change) => {
                check(LEGAL_REOPEN, demand != DemandWait::OpenNow, || {
                    "parked under OpenNow".to_owned()
                });
                check(
                    LEGAL_REOPEN,
                    change.is_none() || (self.policy.reopen && self.session.opens_locally()),
                    || format!("{change:?} on an endpoint that does not reopen"),
                );
                if let Some(change) = change {
                    self.demand_changed(change);
                }
            }
        }
    }

    /// Record a new admitted generation. A parked generation's exit still
    /// arrives later, so every admitted generation is recorded.
    fn admit(&mut self, opened_locally: bool) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        self.generations.push(Generation {
            id,
            opened_locally,
            exited: false,
        });
        id
    }

    fn expire(&mut self, advance: Duration) {
        self.now += advance;
        let expired = self.pending.expire(self.now);
        let due = self
            .offer
            .as_ref()
            .filter(|offer| offer.first_at + SETUP_TIMEOUT <= self.now)
            .map(|offer| (self.stream.kind, offer.id));
        check(
            OFFER_ASSEMBLY,
            expired.iter().all(|dropped| Some(*dropped) == due),
            || format!("expired {expired:?}, due {due:?}"),
        );
        check(
            OFFER_ASSEMBLY,
            self.pending.deadline().is_none_or(|at| at > self.now),
            || "an offer outlived its deadline".to_owned(),
        );
        if !expired.is_empty() {
            self.offer = None;
            self.deferred_at = Some(self.now);
        }
        self.summary.expired_offers += expired.len();
    }

    fn demand(&mut self, demand: DemandWait) {
        let change = self.session.schedule_demand(demand);
        check(
            LEGAL_REOPEN,
            (change == DemandChange::Cancel) == (demand == DemandWait::Retire),
            || format!("{demand:?} made {change:?}"),
        );
        if demand == DemandWait::Retire {
            self.summary.retirements += 1;
        }
        self.demand_changed(change);
    }

    /// Apply a demand's wait change, as the connection loop does.
    fn demand_changed(&mut self, change: DemandChange) {
        self.wait.apply(change);
        match change {
            DemandChange::Keep => {}
            DemandChange::Start => self.retired = false,
            DemandChange::Cancel => self.retired = true,
        }
    }

    /// Apply a transport backoff the state returned, as the connection loop
    /// does. Backoffs start at the base delay, never shrink, and stay under
    /// the cap; a named test pins the ladder.
    fn backoff(&mut self, delay: Option<Duration>) {
        let Some(delay) = delay else {
            return;
        };
        check(LEGAL_REOPEN, !self.wait.0 && !self.retired, || {
            format!(
                "a backoff while waiting: {}, retired: {}",
                self.wait.0, self.retired
            )
        });
        check(
            LEGAL_REOPEN,
            match self.last_backoff {
                None => delay == ORDERED_STREAM_REOPEN_BACKOFF,
                Some(last) => last <= delay && delay <= ORDERED_STREAM_REOPEN_BACKOFF_CAP,
            },
            || format!("backoff {delay:?} after {:?}", self.last_backoff),
        );
        self.last_backoff = Some(delay);
        self.wait.0 = true;
        self.summary.reopen_backoffs += 1;
    }

    fn wait_fires(&mut self, open_now: bool, opens: bool) {
        if !std::mem::take(&mut self.wait.0) {
            return;
        }
        self.session.clear_wait();
        let may_reopen = self.session.may_reopen(false);
        let installed = self.installed;
        check(
            LEGAL_REOPEN,
            !may_reopen || (self.session.opens_locally() && installed == [None, None]),
            || format!("may reopen beside {installed:?}"),
        );
        if !may_reopen {
            return;
        }
        if !open_now {
            let change = self.session.reopen_demand(DemandWait::Later);
            check(LEGAL_REOPEN, change == Some(DemandChange::Start), || {
                format!("a deferred demand with no wait made {change:?}")
            });
            if let Some(change) = change {
                self.demand_changed(change);
            }
            return;
        }
        let change = self.session.reopen_demand(DemandWait::OpenNow);
        check(LEGAL_REOPEN, change.is_none(), || {
            format!("OpenNow waits: {change:?}")
        });
        if opens {
            let generation = self.admit(true);
            self.session.opened(generation);
            self.installed[0] = Some(generation);
            self.wait.apply(DemandChange::Cancel);
            self.summary.local_opens += 1;
        } else {
            let delay = self.session.schedule_transport_backoff();
            check(LEGAL_REOPEN, delay.is_some() || self.retired, || {
                "a failed open left the service without a wait".to_owned()
            });
            self.backoff(delay);
        }
    }

    fn exit(&mut self, generation: usize) {
        let Some(generation) = self.generations.get_mut(generation) else {
            return;
        };
        if generation.exited {
            return;
        }
        generation.exited = true;
        let (id, opened_locally) = (generation.id, generation.opened_locally);
        let direction = usize::from(!opened_locally);
        let current = self.installed[direction] == Some(id);
        let outcome = self.session.exited(
            SessionExit {
                stream: self.stream,
                session_id: id,
                opened_locally,
            },
            false,
        );
        check(ONE_ACTIVE_GENERATION, outcome.current == current, || {
            format!("exit of {id} reported current {}", outcome.current)
        });
        if current {
            self.installed[direction] = None;
        } else {
            self.summary.stale_exits += 1;
        }
        let delay = outcome.reopen_after;
        let idle = self.installed == [None, None];
        check(
            LEGAL_REOPEN,
            delay.is_none()
                || (current && idle && self.policy.reopen && self.session.opens_locally()),
            || format!("exit of {id} reopens after {delay:?}"),
        );
        self.backoff(delay);
        // The designated opener that loses its last generation keeps a way
        // back, unless the service retired.
        if current && idle && self.policy.reopen && self.session.opens_locally() {
            check(LEGAL_REOPEN, self.wait.0 || self.retired, || {
                format!("exit of {id} left the service without a wait")
            });
        }
    }

    /// Check the machines' invariants and every fact they expose against the
    /// records.
    fn check_observations(&self) {
        self.pending.assert_invariants();
        self.session.assert_invariants();
        let deadline = self
            .offer
            .as_ref()
            .map(|offer| offer.first_at + SETUP_TIMEOUT);
        check(OFFER_ASSEMBLY, self.pending.deadline() == deadline, || {
            format!(
                "deadline {:?}, expected {deadline:?}",
                self.pending.deadline()
            )
        });
        let installed = [self.session.installed(true), self.session.installed(false)];
        check(ONE_ACTIVE_GENERATION, installed == self.installed, || {
            format!("installed {installed:?}, recorded {:?}", self.installed)
        });
        check(
            LEGAL_REOPEN,
            self.session.is_waiting() == self.wait.0,
            || {
                format!(
                    "waiting: {}, the connection holds a wait: {}",
                    self.session.is_waiting(),
                    self.wait.0
                )
            },
        );
    }
}

#[cfg(test)]
#[path = "../../../../../tests/unit/zakura/handler/service_session/state/fuzz/mod.rs"]
mod tests;
