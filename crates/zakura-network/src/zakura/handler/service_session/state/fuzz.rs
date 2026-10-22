//! Machine test of the service-session state, driven by fuzz bytes.
//!
//! [`replay_service_session_bytes`] decodes a policy and a sequence of
//! connection-loop inputs, applies each input to [`PendingState`] and
//! [`ServiceSessionState`] for one three-member service, and checks both
//! against an independent model after every input. A violation panics with the
//! property ID it breaks:
//!
//! - `session/offer-assembly`: one incomplete offer per service, whose deadline
//!   only its first member sets, and whose expiry or conflict defers the next.
//! - `session/one-active-generation`: at most one active generation in each
//!   opening direction, which only the exit of that exact generation clears.
//! - `session/legal-reopen`: only the designated opener reopens; the connection
//!   holds a reopen wait exactly while the state is waiting; backoffs grow and
//!   cap; retirement blocks every later wait.
//!
//! The model computes every expected result itself. It shares only the
//! machine's input and outcome types, never a production helper.

use std::{fmt::Debug, sync::Arc, time::Duration};

use zakura_rt::time::Instant;

use super::{
    ConnectionRoles, DemandChange, DemandWait, MemberArrival, MemberReservation, PendingState,
    RemoteCompletion, ServiceSessionState, SessionExit, SessionReopenState, SessionWaitReason,
};
use crate::zakura::{transport::SessionLayout, SessionOpening, SessionPolicy, Stream};

const OFFER_ASSEMBLY: &str = "session/offer-assembly";
const ONE_ACTIVE_GENERATION: &str = "session/one-active-generation";
const LEGAL_REOPEN: &str = "session/legal-reopen";

/// Input bytes beyond this bound are ignored, which bounds each replay.
const MAX_INPUT_BYTES: usize = 512;

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
    SessionLayout {
        streams: Arc::from([MEMBER_A, MEMBER_B, MEMBER_C]),
    }
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

/// Replay `data` as one connection's inputs to one service's session state,
/// checking every step against an independent model.
///
/// # Panics
///
/// Panics with the broken property ID when the state and the model disagree.
pub fn replay_service_session_bytes(data: &[u8]) -> ServiceSessionReplaySummary {
    let mut input = InputReader::new(&data[..data.len().min(MAX_INPUT_BYTES)]);
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
/// This reader stands in for the property-testing `Decisions` until that
/// lands. Each method is one draw, so replacing the reader with `Decisions`
/// changes no call site. An exhausted reader draws zero.
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

/// Panic with `property` unless the machine's result equals the model's.
#[track_caller]
fn check<T: Debug + PartialEq>(property: &str, machine: T, model: T) {
    assert_eq!(machine, model, "{property} violated");
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ModelWait {
    Idle,
    Demand,
    Transport,
    Retired,
}

/// An independent statement of the session rules for one three-member service.
#[derive(Debug)]
struct Model {
    opens_locally: bool,
    reopens: bool,
    winner: bool,
    /// The incomplete offer: its id, arrived member indexes, and deadline.
    offer: Option<(u64, Vec<usize>, Instant)>,
    deferred_until: Option<Instant>,
    local: Option<u64>,
    remote: Option<u64>,
    attempts: u32,
    wait: ModelWait,
}

impl Model {
    fn new(policy: SessionPolicy, roles: ConnectionRoles) -> Self {
        let opens_locally = if policy.opening == SessionOpening::EitherSide {
            roles.i_open_collision_winner
        } else {
            roles.is_initiator
        };
        Self {
            opens_locally,
            reopens: policy.reopen,
            winner: roles.i_open_collision_winner,
            offer: None,
            deferred_until: None,
            local: None,
            remote: None,
            attempts: 0,
            wait: ModelWait::Idle,
        }
    }

    fn rejects_offer(&self) -> bool {
        self.remote.is_some() || (self.winner && self.local.is_some())
    }

    fn reservation(&self, now: Instant) -> MemberReservation {
        match (self.deferred_until, &self.offer) {
            (Some(until), _) if until > now => MemberReservation::Deferred,
            (_, Some(_)) => MemberReservation::Share,
            _ => MemberReservation::Reserve,
        }
    }

    /// `None` means the offer is invalid and the connection closes.
    fn arrive(&mut self, member: usize, id: u64, now: Instant) -> Option<MemberArrival> {
        let offer = self.offer.take();
        if member == FOREIGN_MEMBER || id == 0 {
            return None;
        }
        let (offer_id, mut members, deadline) =
            offer.unwrap_or((id, Vec::new(), now + SETUP_TIMEOUT));
        if offer_id != id {
            self.deferred_until = Some(now + SETUP_TIMEOUT);
            return Some(MemberArrival::Conflict);
        }
        if now >= deadline || members.contains(&member) {
            return None;
        }
        members.push(member);
        if members.len() == 3 {
            return Some(MemberArrival::Complete);
        }
        self.offer = Some((offer_id, members, deadline));
        Some(MemberArrival::Waiting)
    }

    fn expire(&mut self, now: Instant) -> bool {
        if self.deferred_until.is_some_and(|until| until <= now) {
            self.deferred_until = None;
        }
        let expired = self.offer.as_ref().is_some_and(|offer| offer.2 <= now);
        if expired {
            self.offer = None;
            self.deferred_until = Some(now + SETUP_TIMEOUT);
        }
        expired
    }

    fn demand(&mut self, demand: DemandWait) -> DemandChange {
        match (demand, self.wait) {
            (DemandWait::Retire, _) => {
                self.wait = ModelWait::Retired;
                DemandChange::Cancel
            }
            (DemandWait::Later, ModelWait::Demand) => DemandChange::Keep,
            _ => {
                self.wait = ModelWait::Demand;
                DemandChange::Start
            }
        }
    }

    fn backoff(&mut self) -> Option<Duration> {
        if self.wait != ModelWait::Idle {
            return None;
        }
        let delay = (Duration::from_millis(250) * 2u32.pow(self.attempts.min(8)))
            .min(Duration::from_secs(8));
        self.attempts += 1;
        self.wait = ModelWait::Transport;
        Some(delay)
    }

    fn clear_wait(&mut self) {
        if matches!(self.wait, ModelWait::Demand | ModelWait::Transport) {
            self.wait = ModelWait::Idle;
        }
    }

    fn exit(&mut self, opened_locally: bool, id: u64) -> Option<Duration> {
        let slot = if opened_locally {
            &mut self.local
        } else {
            &mut self.remote
        };
        if *slot != Some(id) {
            return None;
        }
        *slot = None;
        let active = self.local.is_some() || self.remote.is_some();
        if !active && self.reopens && self.opens_locally {
            self.backoff()
        } else {
            None
        }
    }

    fn reopen_state(&self) -> SessionReopenState {
        match self.wait {
            ModelWait::Idle => SessionReopenState::Idle,
            ModelWait::Demand => SessionReopenState::Waiting(SessionWaitReason::Demand),
            ModelWait::Transport => SessionReopenState::Waiting(SessionWaitReason::Transport),
            ModelWait::Retired => SessionReopenState::Retired,
        }
    }
}

/// The connection loop's one wait per service, as `ServiceSessionIO` keeps it.
#[derive(Debug, Default)]
struct WaitSlot(Option<DemandOrBackoff>);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DemandOrBackoff {
    Demand,
    Backoff,
}

impl WaitSlot {
    fn apply(&mut self, change: DemandChange) {
        match change {
            DemandChange::Keep => {}
            DemandChange::Cancel => self.0 = None,
            DemandChange::Start => self.0 = Some(DemandOrBackoff::Demand),
        }
    }

    fn backoff(&mut self, delay: Option<Duration>) {
        if delay.is_some() {
            check(LEGAL_REOPEN, self.0, None);
            self.0 = Some(DemandOrBackoff::Backoff);
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

/// The machine, the model, and the connection-loop facts both observe.
#[derive(Debug)]
struct SessionHarness {
    layout: SessionLayout,
    stream: Stream,
    now: Instant,
    pending: PendingState,
    session: ServiceSessionState,
    wait: WaitSlot,
    model: Model,
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
            now: Instant::now(),
            pending: PendingState::default(),
            session: ServiceSessionState::new(stream, policy, roles),
            wait: WaitSlot::default(),
            model: Model::new(policy, roles),
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
            Op::Expire { advance } => {
                self.now += advance;
                let expired = self.pending.expire(self.now);
                let expected = self.model.expire(self.now);
                self.summary.expired_offers += expired.len();
                check(
                    OFFER_ASSEMBLY,
                    expired,
                    if expected {
                        vec![self.stream.kind]
                    } else {
                        Vec::new()
                    },
                );
            }
            Op::Demand(demand) => {
                let change = self.session.schedule_demand(demand);
                check(LEGAL_REOPEN, change, self.model.demand(demand));
                if demand == DemandWait::Retire {
                    self.summary.retirements += 1;
                }
                self.wait.apply(change);
            }
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

    /// A peer-opened member stream arrives. Returns whether its setup starts:
    /// the service neither rejects the offer nor defers the member.
    fn accept(&mut self) -> bool {
        let rejects = self.model.rejects_offer();
        check(
            ONE_ACTIVE_GENERATION,
            self.session.rejects_remote_offer(),
            rejects,
        );
        if rejects {
            return false;
        }
        let reservation = self.pending.reservation(self.stream.kind, self.now);
        check(
            OFFER_ASSEMBLY,
            reservation,
            self.model.reservation(self.now),
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
        let expected = self.model.arrive(member, id, self.now);
        check(OFFER_ASSEMBLY, arrival.ok(), expected);
        match expected {
            // Production closes the connection after an invalid offer. The
            // state stays defined, so the replay keeps exploring.
            None => {
                self.summary.invalid_members += 1;
                return;
            }
            Some(MemberArrival::Conflict) => {
                self.summary.conflicting_members += 1;
                return;
            }
            Some(MemberArrival::Waiting) => return,
            Some(MemberArrival::Complete) => self.summary.completed_offers += 1,
        }

        let generation = self.admit(false);
        let completion = self.session.remote_completed(generation);
        let collision = self.model.local.is_some();
        if collision && self.model.winner {
            check(
                ONE_ACTIVE_GENERATION,
                completion,
                RemoteCompletion::ParkCollision,
            );
            self.summary.parked_collisions += 1;
            return;
        }
        self.model.remote = Some(generation);
        if collision {
            check(
                ONE_ACTIVE_GENERATION,
                completion,
                RemoteCompletion::AdoptCollision,
            );
            self.summary.adopted_collisions += 1;
        } else {
            check(
                ONE_ACTIVE_GENERATION,
                completion,
                RemoteCompletion::CheckDemand,
            );
        }
        if collision || open_now {
            self.session.remote_adopted();
            self.wait.apply(DemandChange::Cancel);
            self.model.clear_wait();
        } else {
            let change = self.session.remote_parked(generation, DemandWait::Later);
            self.model.remote = None;
            let expected = (self.model.reopens && self.model.opens_locally)
                .then(|| self.model.demand(DemandWait::Later));
            check(LEGAL_REOPEN, change, expected);
            if let Some(change) = change {
                self.wait.apply(change);
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

    fn wait_fires(&mut self, open_now: bool, opens: bool) {
        if self.wait.0.take().is_none() {
            return;
        }
        self.session.wait_finished();
        self.model.clear_wait();
        let may_reopen =
            self.model.opens_locally && self.model.local.is_none() && self.model.remote.is_none();
        check(LEGAL_REOPEN, self.session.may_reopen(false), may_reopen);
        if !may_reopen {
            return;
        }
        if !open_now {
            let change = self.session.schedule_demand(DemandWait::Later);
            check(LEGAL_REOPEN, change, self.model.demand(DemandWait::Later));
            self.wait.apply(change);
        } else if opens {
            let generation = self.admit(true);
            self.session.opened(generation);
            self.wait.apply(DemandChange::Cancel);
            self.model.clear_wait();
            self.model.local = Some(generation);
            self.summary.local_opens += 1;
        } else {
            let delay = self.session.schedule_transport_backoff();
            check(LEGAL_REOPEN, delay, self.model.backoff());
            self.wait.backoff(delay);
            self.summary.reopen_backoffs += usize::from(delay.is_some());
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
        let stale = if opened_locally {
            self.model.local != Some(id)
        } else {
            self.model.remote != Some(id)
        };
        let before = (
            self.session.local_session_id,
            self.session.remote_session_id,
        );
        let exit = SessionExit {
            stream: self.stream,
            session_id: id,
            opened_locally,
        };
        let delay = self.session.exited(exit, false);
        check(LEGAL_REOPEN, delay, self.model.exit(opened_locally, id));
        if stale {
            self.summary.stale_exits += 1;
            check(
                ONE_ACTIVE_GENERATION,
                (
                    self.session.local_session_id,
                    self.session.remote_session_id,
                ),
                before,
            );
        }
        self.wait.backoff(delay);
        self.summary.reopen_backoffs += usize::from(delay.is_some());
    }

    /// Compare every fact the machine exposes with the model.
    fn check_observations(&self) {
        check(
            OFFER_ASSEMBLY,
            self.pending.deadline(),
            self.model.offer.as_ref().map(|offer| offer.2),
        );
        check(
            ONE_ACTIVE_GENERATION,
            self.session.local_session_id,
            self.model.local,
        );
        check(
            ONE_ACTIVE_GENERATION,
            self.session.remote_session_id,
            self.model.remote,
        );
        check(
            LEGAL_REOPEN,
            self.session.reopen_attempts,
            self.model.attempts,
        );
        check(
            LEGAL_REOPEN,
            self.session.is_waiting(),
            self.wait.0.is_some(),
        );
        check(
            LEGAL_REOPEN,
            self.session.reopen_state,
            self.model.reopen_state(),
        );
    }
}
