//! `header-sync/liveness`: `header-sync/storage` on hdr-10's and hdr-11's
//! liveness fixtures ([`LivenessScenario`]). Its one scenario byte picks the
//! fixture, and the rest of an input is Plan bytes. It checks
//! `header-sync/hold-release-wakes` besides `header-sync/storage`'s
//! properties, since only it arms the node's hold-wake evidence, and
//! `header-sync/bounded-adversary-progress`, since only its recovery
//! fixtures declare `BoundedAdversary` beside the recovery assumptions. It
//! also arms the node's publication evidence, so
//! `header-sync/state-publication-consistent` obliges each commit's
//! adoption here, and in hdr-12b's status fixtures each changed `Status` in
//! time.
//!
//! Its fixtures declare `BoundedAdversary` (Rule 13 of the hdr-9 to hdr-13
//! plan). Every peer is an honest mirror or a scripted control, so the
//! assumption holds after every step, and the declaration only turns on the
//! step loop's livelock limit at a tenth of the run's budget
//! ([`crate::harness::run_config`]). A reactor that spins at one instant
//! with an obligation open then violates `sim/no-livelock`, instead of
//! keeping the clock from the quiescence that would expire the obligation
//! and ending the run at its step budget. `header-sync/storage` itself
//! declares nothing: its honest runs take up to its whole 4,000-step budget
//! at one instant, so the limit would fail them. So this harness runs only
//! its named fixtures, never an arbitrary scenario, and its budget,
//! [`LIVENESS_BUDGET`], puts the limit at more than twice each fixture's
//! honest peak, which a test measures.
//!
//! Every fixture ends: once its obligations have concluded, a [`Finish`]
//! step stops every node, so the run ends idle rather than at its budget
//! (Rule 13).

use std::{rc::Rc, sync::Arc};

use serde::{Deserialize, Serialize};
use serde_json::json;
use zakura_rt::sync::watch;
use zakura_scheduler::SimInstant;
use zakura_storage::header::{controlled::ControlledPort, fixture::HeaderFixture};

use super::{
    recovery_assumptions, recovery_source, storage_config, storage_properties, storage_sources,
    Ports, Storage, StorageView,
};
use crate::{
    domain::{
        header_sync::{hold_release_wakes, open_retirement},
        header_sync_progress::{bounded_adversary_progress, recovery_deadline},
    },
    harness::{
        header_node::{
            adoption::AdoptionEvidence,
            ends::{EndEvidence, EndFact, EnvironmentAction},
            harness_commit,
            hold_wake::{HoldWakeEvidence, HoldWakeFact},
            policy::{Allow, Hold, OpKind, ReplyHold, Select, Trigger},
            publication::{Arming, PublicationEvidence, PublicationFact, STATUS_DEADLINE},
            recovery::{RecoveryEvidence, RecoveryProgress},
            retirement::{RetirementEvidence, Roles},
            Chain, ControlAction, ControlStep, DelayedPage, Fork, Policy, Reset, Topology,
        },
        header_sync::{Dialer, MirrorSpec, StorageScenario},
        Subsystem,
    },
    property::{Assume, AssumeRecord, Assumptions, EndViolation, HasView, Property, Sources},
    ActionSource, Boundary, Identity, ObservationLog, RunConfig, RunEnd, Sim,
};

/// The live steps a liveness run may take. The livelock limit is a tenth of
/// it, 5,000 steps at one instant. The fixtures' honest runs take at most
/// about 2,310 there, a relink departure's, measured over 16 plans each,
/// and a test keeps every fixture under half the limit.
pub const LIVENESS_BUDGET: u64 = 50_000;

/// The departure and timeout fixtures' mirror A, the owner, whose slot goes.
pub const OWNER: &str = "mirror-0";

/// The departure and timeout fixtures' mirror B, the taker, which must take
/// A's target up.
pub const TAKER: &str = "mirror-1";

/// When a departure or timeout fixture's script is done, from the run's
/// start: past every hold and deadline its honest run reaches, the latest
/// a late apply's, held 35 seconds after its submission at 20. A run whose
/// fixture never got ready ends here, as `NotApplicable`, rather than at
/// its budget. The late-page fixture's is [`LATE_PAGE_HORIZON`].
pub const FIXTURE_HORIZON: std::time::Duration = std::time::Duration::from_secs(60);

/// How long the late-page fixture holds A's first page, a second short of
/// the 30-second request deadline its round's start armed.
pub const LATE_PAGE_DELAY: std::time::Duration = std::time::Duration::from_secs(29);

/// When the late-page fixture lets A's held prepare go, in virtual
/// milliseconds: past the deadline the late page rearmed, 30 seconds after
/// a page that comes 29 seconds after a start within the first second.
const LATE_PAGE_PREPARE_MS: u64 = 70_000;

/// [`FIXTURE_HORIZON`] for the late-page fixture, past its prepare's
/// release.
pub const LATE_PAGE_HORIZON: std::time::Duration = std::time::Duration::from_secs(80);

/// How long a serving fixture's held read may stay pending, in virtual
/// milliseconds. The clock can't pass it, so the release and the read both
/// come well before the read's lease idles out and the mirror's request
/// deadline, both 30 seconds. Otherwise a Plan could let the lease expire,
/// and the read would complete unavailable, or let the mirror close the
/// subscription, both outside the fixture.
pub const SERVING_READ_DEADLINE_MS: u64 = 1_000;

/// A storage operation kind a hold fixture holds: the reactor's three.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum HeldKind {
    /// The first locator read.
    Locator,
    /// The first prepare.
    Prepare,
    /// The first apply.
    Apply,
}

impl HeldKind {
    /// Every kind, in order.
    pub const ALL: [HeldKind; 3] = [HeldKind::Locator, HeldKind::Prepare, HeldKind::Apply];

    /// The storage operation kind.
    pub fn op(self) -> OpKind {
        match self {
            HeldKind::Locator => OpKind::Locator,
            HeldKind::Prepare => OpKind::Prepare,
            HeldKind::Apply => OpKind::Apply,
        }
    }
}

/// Where mirror A's round is when its session departs in a departure
/// fixture.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Stage {
    /// Awaiting its locator, whose read is held.
    Awaiting,
    /// Receiving pages: its first request is reserved.
    Receiving,
    /// Preparing, with its prepare held.
    Preparing,
    /// Applying, with its apply held.
    Applying,
}

impl Stage {
    /// Every stage, in order.
    pub const ALL: [Stage; 4] = [
        Stage::Awaiting,
        Stage::Receiving,
        Stage::Preparing,
        Stage::Applying,
    ];

    /// The operation kind held in this stage, if any.
    fn held(self) -> Option<OpKind> {
        match self {
            Stage::Awaiting => Some(OpKind::Locator),
            Stage::Receiving => None,
            Stage::Preparing => Some(OpKind::Prepare),
            Stage::Applying => Some(OpKind::Apply),
        }
    }
}

/// How mirror A's session departs in a departure fixture.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Departs {
    /// The harness closes A's link, so the node ends its session.
    Close,
    /// The harness links A again, so the node replaces its session.
    Relink,
}

/// A recovery fixture (hdr-11): a storage scenario that ends in a
/// `Recover` of [`RECOVERY_SOURCE`], with no hostile prefix beyond the
/// fixture's own invalidation, so it's one of
/// `header-sync/bounded-adversary-progress`'s recovery controls. Every
/// header has the genesis target, so a chain's work is its length. `A<n>`
/// is the main branch's header at height `n` and `B<n>` branch 1's.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RecoveryFixture {
    /// hdr-8's one-successor fixture rebuilt in the storage harness: the
    /// node at A3 and its source at A4, its only peer, with no holds or
    /// faults, and `Recover` the first step. The objective is A4.
    Successor,
    /// [`RecoveryFixture::Successor`] with the `Recover` offered only once
    /// the node's first apply completes, so the baseline already holds A4
    /// and nothing is owed.
    SyncedFirst,
    /// The node at A3, its source at A5, and `mirror-1` on B4, a fork from
    /// A3 heavier than the baseline and lighter than A5. Adopting B4 doesn't
    /// meet the objective, A4 and A5.
    Fork,
    /// The node at A3, its source at A6, and `mirror-1` on A4 then B5, a
    /// fork that shares the objective's first header. Adopting A4 from
    /// either meets it.
    SharedPrefix,
    /// The node at A3 and its source at A4. The node first invalidates its
    /// own tip, A3, which the source still holds, so the baseline is A2.
    /// The `Recover` reconsiders A3, and A3 and A4 are the objective.
    Invalidation,
    /// The node at A3, its source at A4 and `mirror-1` on B4, a fork of
    /// equal work. Either tip meets the objective.
    EqualWork,
    /// The node on A2 then its own heavier branch B3 to B5, and its source
    /// at A4, a lighter separate fork from A2. The node first invalidates
    /// B3, so the baseline is at most A4. The `Recover` reconsiders B3, so
    /// the objective is the node's own branch, which it reselects without
    /// adopting the source's fork.
    Reconsideration,
}

impl RecoveryFixture {
    /// Every recovery fixture, in order.
    pub const ALL: [RecoveryFixture; 7] = [
        RecoveryFixture::Successor,
        RecoveryFixture::SyncedFirst,
        RecoveryFixture::Fork,
        RecoveryFixture::SharedPrefix,
        RecoveryFixture::Invalidation,
        RecoveryFixture::EqualWork,
        RecoveryFixture::Reconsideration,
    ];

    /// The storage scenario this fixture runs.
    pub fn storage(self) -> StorageScenario {
        let mirror = |chain| MirrorSpec {
            chain,
            dialer: Dialer::Mirror,
            max_headers_per_response: None,
        };
        let fork = |main, len| Chain {
            main,
            fork: Some(Fork { branch: 1, len }),
        };
        let invalidate = |depth| ControlStep {
            peer: "node".to_owned(),
            action: ControlAction::Reset(Reset::Invalidate { depth }),
            when: None,
        };
        let (node, mirrors, mut controls) = match self {
            RecoveryFixture::Successor | RecoveryFixture::SyncedFirst => {
                (Chain::main(3), vec![mirror(Chain::main(4))], Vec::new())
            }
            RecoveryFixture::Fork => (
                Chain::main(3),
                vec![mirror(Chain::main(5)), mirror(fork(3, 1))],
                Vec::new(),
            ),
            RecoveryFixture::SharedPrefix => (
                Chain::main(3),
                vec![mirror(Chain::main(6)), mirror(fork(4, 1))],
                Vec::new(),
            ),
            RecoveryFixture::Invalidation => (
                Chain::main(3),
                vec![mirror(Chain::main(4))],
                vec![invalidate(0)],
            ),
            RecoveryFixture::EqualWork => (
                Chain::main(3),
                vec![mirror(Chain::main(4)), mirror(fork(3, 1))],
                Vec::new(),
            ),
            RecoveryFixture::Reconsideration => (
                fork(2, 3),
                vec![mirror(Chain::main(4))],
                vec![invalidate(2)],
            ),
        };
        let when = (self == RecoveryFixture::SyncedFirst)
            .then(|| Trigger::Completed(Select::nth(OpKind::Apply, 1)));
        controls.push(ControlStep {
            peer: RECOVERY_SOURCE.to_owned(),
            action: ControlAction::Recover,
            when,
        });
        StorageScenario {
            node,
            mirrors,
            policy: Policy::converging(),
            controls,
            weights: Default::default(),
        }
    }
}

/// The recovery fixtures' source, the mirror their `Recover` names.
pub const RECOVERY_SOURCE: &str = "mirror-0";

/// One of the liveness fixtures. Each is a fixed storage scenario
/// ([`LivenessScenario::storage`]) that leaves the reactor one thing to wait
/// for (Rule 8).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum LivenessScenario {
    /// The node at A3 syncing from `mirror-0` at A6, its only peer. The
    /// node's first operation of the kind is held until a `Release` step
    /// opens it, which the Plan may take any time once that operation is
    /// submitted. The mirror is ahead of the node, so the node serves it
    /// nothing, and once the held reply is delivered nothing else is left to
    /// wake the reactor before the clock moves.
    Held(HeldKind),
    /// [`LivenessScenario::Held`], with the node also serving `mirror-1` at
    /// A1, and the release offered only once a page read for `mirror-1`
    /// has completed too, an unrelated completion, so the reply can land
    /// while the reactor is busy with other peers.
    HeldWhileServing(HeldKind),
    /// [`LivenessScenario::Held`] for the first apply, whose hold opens by
    /// its own trigger, two virtual seconds after its submission, rather
    /// than by a step.
    OpenedByTrigger,
    /// [`LivenessScenario::Held`] for the first apply, with its operation
    /// unheld and its reply held instead: it commits, and its reply reaches
    /// the reactor two virtual seconds later.
    ReplyHeld,
    /// [`LivenessScenario::Held`] for the first prepare, with a `Shutdown`
    /// step the Plan may take once that prepare completes, so under some
    /// plans the node begins stopping before its reactor handles the
    /// delivered reply.
    ShutdownAfterRelease,
    /// The departure fixture: the node at A3, with mirror A ([`OWNER`])
    /// and mirror B ([`TAKER`]) on one fixed chain whose tip, A6, is the
    /// target. B starts at A3 and grows to A6 once A's locator read is
    /// submitted, so A owns the target before B offers it, and is then
    /// relinked, so its new session's first `Status` carries the target at
    /// once. That admission readies the fixture. A's first locator read is
    /// held until then, so every arm of A's deadline comes after it. Then A
    /// departs, `by` a close or a relink, once B's new session is admitted
    /// and A's round reaches `stage`. The operation the stage holds, if any,
    /// is released once the reactor has seen A's session depart: at the
    /// replacement's admission, or once the clock moves after a close.
    Departure {
        /// Where A's round is when it departs.
        stage: Stage,
        /// How it departs.
        by: Departs,
    },
    /// The timeout fixture: the departure fixture with A's session live and
    /// A's first prepare held for 35 virtual seconds, past the request
    /// timeout, so production retires A's round for its deadline and the
    /// cached pass hands the target to B. With `late_apply`, the prepare
    /// runs 20 seconds after its submission and A's first apply is held for
    /// 35 seconds after its own, so the apply's dispatch rearms the
    /// deadline, and the round retires 30 seconds after that.
    Timeout {
        /// Whether the prepare runs late and the apply is held.
        late_apply: bool,
    },
    /// The timeout fixture with A's first page held [`LATE_PAGE_DELAY`] on
    /// its way to the node, so it arrives a second before the deadline its
    /// round's start armed and rearms it, and the run's clock crosses the
    /// first deadline with the round still live. A's prepare is held past
    /// the rearmed deadline, so production retires A's round 30 seconds
    /// after the page, and the cached pass hands the target to B.
    LatePage,
    /// The serving-read fixture: the node at A3 serving `mirror-0` at A1,
    /// its only peer, which subscribes to it. The publisher's first page
    /// read is held until a `Release` step, which the Plan may take any time
    /// once that read is submitted, long before the mirror's 30-second
    /// request deadline. The node is two headers ahead, fewer than one page
    /// and than the mirror's first credit, and no peer is ahead of the node,
    /// so once the held page reaches its job nothing else is left to wake
    /// the publisher.
    ServingHeld,
    /// [`LivenessScenario::ServingHeld`] with the held read failing as
    /// storage unavailable, a passing control: a failed read opens nothing,
    /// since the publisher may stall on it.
    ServingFailed,
    /// [`LivenessScenario::ServingHeld`] with the mirror at A3, level with
    /// the node, a passing control: the publisher reads no page, so nothing
    /// opens.
    ServingCaughtUp,
    /// A recovery fixture ([`RecoveryFixture`]).
    Recovery(RecoveryFixture),
    /// The node and `mirror-0` both at A3, with no other traffic but an
    /// invalidation of the node's tip, offered once the node admits the
    /// mirror's session, and then its reconsideration: two quiet resets,
    /// which only the node's committed watch tells the reactor of, since a
    /// new session would also make it read the watch.
    QuietReset,
    /// [`LivenessScenario::Held`] for the first apply, its hold opening by
    /// its own trigger two virtual seconds after its submission, and a
    /// `Shutdown` step the Plan may take once it's submitted, so under some
    /// plans the apply commits after the reactor stopped.
    HeldAcrossShutdown,
    /// A status fixture: the node and `mirror-0` both at A3, one honest
    /// reading mirror on an in-memory link with no flood, so its session's
    /// queue drains. Its session is owed its first `Status`.
    StatusStartup,
    /// [`LivenessScenario::StatusStartup`] with an invalidation of the
    /// node's tip offered once the node admits the session, inside the 1 s
    /// publication floor of the status it queued then, so the changed
    /// `Status` waits out the floor (hdr-9b's reset while publication is
    /// held). A reconsideration after it would let the reactor coalesce
    /// both into no change.
    StatusInsideFloor,
    /// [`LivenessScenario::StatusStartup`] with a harness commit of one
    /// header at each of the run's first [`STATUS_CHANGES`] seconds
    /// ([`Ticker`]), so the session's `Status` changes faster than the
    /// publisher's change bound, which it coalesces behind its floor.
    StatusChanging,
    /// [`LivenessScenario::StatusInsideFloor`] with a `Close` of the
    /// mirror's link offered once the invalidation commits, so under some
    /// plans the harness closes the session while its changed `Status`
    /// waits out the floor, which answers that debt.
    StatusClosedWhileOwed,
    /// [`LivenessScenario::StatusClosedWhileOwed`] with a `Relink` instead
    /// of the `Close`: the relink answers the old session's debt, and the
    /// new session is owed its own first `Status`.
    StatusRelinkedWhileOwed,
    /// [`LivenessScenario::StatusStartup`] with a `Relink` of the mirror
    /// once the node admits it, and a `Shutdown` step offered once the new
    /// link is made, so under some plans the node admits the new session,
    /// owing its first `Status`, after the shutdown began, which answers
    /// nothing later.
    StatusShutdownWhileRelinking,
    /// [`LivenessScenario::StatusStartup`] with a `Close` of the mirror's
    /// link offered at once and a `Shutdown` step once it closed, so under
    /// some plans the node admits the session after the harness closed its
    /// link, which owes nothing.
    StatusClosedBeforeAdmission,
}

/// How many harness commits [`LivenessScenario::StatusChanging`] makes, one a
/// second ([`Ticker`]).
pub const STATUS_CHANGES: u64 = 5;

impl LivenessScenario {
    /// The departure and timeout fixtures' mirrors, for the hand-off rule,
    /// or `None` for a hold fixture.
    pub fn roles(self) -> Option<Roles> {
        matches!(
            self,
            LivenessScenario::Departure { .. }
                | LivenessScenario::Timeout { .. }
                | LivenessScenario::LatePage
        )
        .then(|| Roles {
            owner: OWNER.to_owned(),
            taker: TAKER.to_owned(),
        })
    }

    /// Every fixture, in the order the scenario byte picks them.
    pub fn all() -> Vec<LivenessScenario> {
        let mut all: Vec<LivenessScenario> = HeldKind::ALL
            .into_iter()
            .map(LivenessScenario::Held)
            .collect();
        all.extend(
            HeldKind::ALL
                .into_iter()
                .map(LivenessScenario::HeldWhileServing),
        );
        all.extend([
            LivenessScenario::OpenedByTrigger,
            LivenessScenario::ReplyHeld,
            LivenessScenario::ShutdownAfterRelease,
        ]);
        all.extend(LivenessScenario::departures());
        all.extend([false, true].map(|late_apply| LivenessScenario::Timeout { late_apply }));
        all.push(LivenessScenario::LatePage);
        all.extend(LivenessScenario::serving());
        all.extend(RecoveryFixture::ALL.map(LivenessScenario::Recovery));
        all.extend(LivenessScenario::publication());
        all
    }

    /// hdr-12b's publication fixtures, in order.
    pub fn publication() -> [LivenessScenario; 9] {
        [
            LivenessScenario::QuietReset,
            LivenessScenario::HeldAcrossShutdown,
            LivenessScenario::StatusStartup,
            LivenessScenario::StatusInsideFloor,
            LivenessScenario::StatusChanging,
            LivenessScenario::StatusClosedWhileOwed,
            LivenessScenario::StatusRelinkedWhileOwed,
            LivenessScenario::StatusShutdownWhileRelinking,
            LivenessScenario::StatusClosedBeforeAdmission,
        ]
    }

    /// How many one-header harness commits the fixture's [`Ticker`] makes,
    /// one at each of the run's first seconds.
    pub fn ticks(self) -> u64 {
        match self {
            LivenessScenario::StatusChanging => STATUS_CHANGES,
            _ => 0,
        }
    }

    /// What the node's publication evidence keeps: every fixture's
    /// commits and adoptions, and a status fixture's `Status` debts.
    pub fn arming(self) -> Arming {
        Arming {
            armed: true,
            status: matches!(
                self,
                LivenessScenario::StatusShutdownWhileRelinking
                    | LivenessScenario::StatusClosedBeforeAdmission
            ) || matches!(self.kind(), FixtureKind::Publication { status: true, .. }),
        }
    }

    /// The hold fixtures, every one whose obligation a held reactor reply
    /// opens.
    pub fn holds() -> Vec<LivenessScenario> {
        LivenessScenario::all()
            .into_iter()
            .filter(|fixture| fixture.kind() == FixtureKind::Hold)
            .collect()
    }

    /// The serving-read fixture and its two passing controls.
    pub fn serving() -> [LivenessScenario; 3] {
        [
            LivenessScenario::ServingHeld,
            LivenessScenario::ServingFailed,
            LivenessScenario::ServingCaughtUp,
        ]
    }

    /// Which kind of fixture it is, for [`Finish`].
    fn kind(self) -> FixtureKind {
        match self {
            LivenessScenario::Departure { .. } => FixtureKind::Departure,
            LivenessScenario::Timeout { .. } | LivenessScenario::LatePage => FixtureKind::Timeout,
            LivenessScenario::ServingHeld
            | LivenessScenario::ServingFailed
            | LivenessScenario::ServingCaughtUp => FixtureKind::Serving,
            LivenessScenario::Recovery(_) => FixtureKind::Recovery,
            LivenessScenario::QuietReset => FixtureKind::Publication {
                commits: 2,
                status: false,
            },
            LivenessScenario::HeldAcrossShutdown
            | LivenessScenario::StatusShutdownWhileRelinking
            | LivenessScenario::StatusClosedBeforeAdmission => FixtureKind::Shutdown,
            LivenessScenario::StatusStartup => FixtureKind::Publication {
                commits: 0,
                status: true,
            },
            LivenessScenario::StatusInsideFloor
            | LivenessScenario::StatusClosedWhileOwed
            | LivenessScenario::StatusRelinkedWhileOwed => FixtureKind::Publication {
                commits: 1,
                status: true,
            },
            LivenessScenario::StatusChanging => FixtureKind::Publication {
                // Bounded by the constant's small value.
                commits: STATUS_CHANGES as usize,
                status: true,
            },
            _ => FixtureKind::Hold,
        }
    }

    /// When the fixture's script is done: [`LATE_PAGE_HORIZON`] for the
    /// late-page fixture, [`FIXTURE_HORIZON`] for every other.
    fn horizon(self) -> std::time::Duration {
        match self {
            LivenessScenario::LatePage => LATE_PAGE_HORIZON,
            _ => FIXTURE_HORIZON,
        }
    }

    /// The departure fixtures, each stage once by a close and once by a
    /// relink.
    pub fn departures() -> Vec<LivenessScenario> {
        Stage::ALL
            .into_iter()
            .flat_map(|stage| {
                [Departs::Close, Departs::Relink]
                    .map(|by| LivenessScenario::Departure { stage, by })
            })
            .collect()
    }

    /// The storage scenario this fixture runs.
    pub fn storage(self) -> StorageScenario {
        match self {
            LivenessScenario::Held(kind) => held(kind.op()),
            LivenessScenario::HeldWhileServing(kind) => {
                let mut scenario = held(kind.op());
                scenario.mirrors.push(MirrorSpec {
                    chain: Chain::main(1),
                    dialer: Dialer::Mirror,
                    max_headers_per_response: None,
                });
                scenario.controls[0].when = Some(Trigger::All(vec![
                    Trigger::Submitted(Select::nth(kind.op(), 1)),
                    Trigger::Completed(Select {
                        kind: Some(OpKind::Read),
                        ..Select::default()
                    }),
                ]));
                scenario
            }
            LivenessScenario::OpenedByTrigger => {
                let mut scenario = held(OpKind::Apply);
                scenario.policy.holds[0].until = Trigger::VirtualTime(2_000);
                scenario.controls.clear();
                scenario
            }
            LivenessScenario::ReplyHeld => {
                let mut scenario = held(OpKind::Apply);
                scenario.policy.holds.clear();
                scenario.policy.reply_holds = vec![ReplyHold {
                    select: Select::nth(OpKind::Apply, 1),
                    until: Trigger::VirtualTime(2_000),
                }];
                scenario.controls.clear();
                scenario
            }
            LivenessScenario::ShutdownAfterRelease => {
                let mut scenario = held(OpKind::Prepare);
                scenario.controls.push(ControlStep {
                    peer: "node".to_owned(),
                    action: ControlAction::Shutdown,
                    when: Some(Trigger::Completed(Select::nth(OpKind::Prepare, 1))),
                });
                scenario
            }
            LivenessScenario::Departure { stage, by } => departure(stage, by),
            LivenessScenario::Timeout { late_apply } => timeout(late_apply),
            LivenessScenario::LatePage => late_page(),
            LivenessScenario::ServingHeld => serving(Allow::Engine, 1),
            LivenessScenario::ServingFailed => serving(Allow::Unavailable, 1),
            LivenessScenario::ServingCaughtUp => serving(Allow::Engine, 3),
            LivenessScenario::Recovery(fixture) => fixture.storage(),
            LivenessScenario::QuietReset => StorageScenario {
                controls: vec![
                    node_step(
                        ControlAction::Reset(Reset::Invalidate { depth: 0 }),
                        Some(Trigger::SessionAdmitted("mirror-0".to_owned())),
                    ),
                    node_step(
                        ControlAction::Reset(Reset::Reconsider),
                        Some(Trigger::Reset("node".to_owned())),
                    ),
                ],
                ..level()
            },
            LivenessScenario::HeldAcrossShutdown => {
                let mut scenario = held(OpKind::Apply);
                scenario.policy.holds[0].until = Trigger::VirtualTime(2_000);
                scenario.controls = vec![node_step(
                    ControlAction::Shutdown,
                    Some(Trigger::Submitted(Select::nth(OpKind::Apply, 1))),
                )];
                scenario
            }
            LivenessScenario::StatusStartup => level(),
            LivenessScenario::StatusInsideFloor => StorageScenario {
                controls: vec![node_step(
                    ControlAction::Reset(Reset::Invalidate { depth: 0 }),
                    Some(Trigger::SessionAdmitted("mirror-0".to_owned())),
                )],
                ..level()
            },
            LivenessScenario::StatusClosedWhileOwed | LivenessScenario::StatusRelinkedWhileOwed => {
                let action = if self == LivenessScenario::StatusClosedWhileOwed {
                    ControlAction::Close
                } else {
                    ControlAction::Relink
                };
                let mut scenario = LivenessScenario::StatusInsideFloor.storage();
                scenario.controls.push(ControlStep {
                    peer: "mirror-0".to_owned(),
                    action,
                    when: Some(Trigger::Reset("node".to_owned())),
                });
                scenario
            }
            LivenessScenario::StatusShutdownWhileRelinking => StorageScenario {
                controls: vec![
                    ControlStep {
                        peer: "mirror-0".to_owned(),
                        action: ControlAction::Relink,
                        when: Some(Trigger::SessionAdmitted("mirror-0".to_owned())),
                    },
                    node_step(
                        ControlAction::Shutdown,
                        Some(Trigger::Count(
                            2,
                            Box::new(Trigger::Linked("mirror-0".to_owned())),
                        )),
                    ),
                ],
                ..level()
            },
            LivenessScenario::StatusClosedBeforeAdmission => StorageScenario {
                controls: vec![
                    ControlStep {
                        peer: "mirror-0".to_owned(),
                        action: ControlAction::Close,
                        when: None,
                    },
                    node_step(
                        ControlAction::Shutdown,
                        Some(Trigger::Closed("mirror-0".to_owned())),
                    ),
                ],
                ..level()
            },
            // Its commits are the ticker's, which a control step can't time:
            // the Plan may advance the clock past a step's trigger.
            LivenessScenario::StatusChanging => level(),
        }
    }
}

/// The node and `mirror-0` both at A3, with no control steps: once the
/// node admits the mirror's session, nothing is left to fetch or serve.
fn level() -> StorageScenario {
    StorageScenario {
        node: Chain::main(3),
        mirrors: vec![MirrorSpec {
            chain: Chain::main(3),
            dialer: Dialer::Mirror,
            max_headers_per_response: None,
        }],
        policy: Policy::converging(),
        controls: Vec::new(),
        weights: Default::default(),
    }
}

/// The node under test's control step `action`, offered once `when` holds,
/// or at once.
fn node_step(action: ControlAction, when: Option<Trigger>) -> ControlStep {
    ControlStep {
        peer: "node".to_owned(),
        action,
        when,
    }
}

/// The serving-read fixtures' storage scenario: the node at A3 serving one
/// mirror at A`mirror`, with the publisher's first page read held until a
/// `Release` step and then completing as `then` allows, within
/// [`SERVING_READ_DEADLINE_MS`] of its submission.
fn serving(then: Allow, mirror: u32) -> StorageScenario {
    StorageScenario {
        node: Chain::main(3),
        mirrors: vec![MirrorSpec {
            chain: Chain::main(mirror),
            dialer: Dialer::Mirror,
            max_headers_per_response: None,
        }],
        policy: Policy {
            holds: vec![Hold {
                select: Select::nth(OpKind::Read, 1),
                until: Trigger::Released,
                then: vec![then],
                deadline_ms: Some(SERVING_READ_DEADLINE_MS),
            }],
            ..Policy::converging()
        },
        controls: vec![ControlStep {
            peer: "node".to_owned(),
            action: ControlAction::Release { hold: 0 },
            when: Some(Trigger::Submitted(Select::nth(OpKind::Read, 1))),
        }],
        weights: Default::default(),
    }
}

/// [`LivenessScenario::Timeout`]'s storage scenario: the preparing
/// departure's, without A's departure or the release.
fn timeout(late_apply: bool) -> StorageScenario {
    let mut scenario = departure(Stage::Preparing, Departs::Close);
    scenario.controls.retain(|step| step.peer == TAKER);
    // The held call keeps the node's one local operation busy, which would
    // keep B's round from preparing, so it's let go once A's deadline has
    // passed.
    if late_apply {
        scenario.policy.holds[0].until = Trigger::VirtualTime(20_000);
        scenario.policy.holds.push(Hold {
            select: Select::nth(OpKind::Apply, 1),
            until: Trigger::VirtualTime(35_000),
            then: vec![Allow::Engine],
            deadline_ms: None,
        });
    } else {
        scenario.policy.holds[0].until = Trigger::VirtualTime(35_000);
    }
    scenario
}

/// [`LivenessScenario::LatePage`]'s storage scenario: the timeout
/// fixture's, with A's prepare held until [`LATE_PAGE_PREPARE_MS`]. Its
/// page's delay is the harness's, not storage's ([`Liveness::prepare`]).
fn late_page() -> StorageScenario {
    let mut scenario = timeout(false);
    scenario.policy.holds[0].until = Trigger::VirtualTime(LATE_PAGE_PREPARE_MS);
    scenario
}

/// [`LivenessScenario::Departure`]'s storage scenario.
fn departure(stage: Stage, by: Departs) -> StorageScenario {
    let (a, b) = (OWNER.to_owned(), TAKER.to_owned());
    let step = |peer: &str, action, when| ControlStep {
        peer: peer.to_owned(),
        action,
        when: Some(when),
    };
    let reached = match stage.held() {
        Some(kind) => Trigger::Submitted(Select::nth(kind, 1)),
        None => Trigger::ReservationOpened(a.clone()),
    };
    // The held operation is released only once the reactor has seen the
    // departure. A replacement's admission is that unit. A close reaches
    // the reactor some steps after the link closes, and always before the
    // clock moves, so its release waits for the clock.
    let (action, departed) = match by {
        Departs::Close => (
            ControlAction::Close,
            Trigger::All(vec![
                Trigger::Closed(a.clone()),
                Trigger::VirtualTime(1_000),
            ]),
        ),
        Departs::Relink => (
            ControlAction::Relink,
            Trigger::Count(2, Box::new(Trigger::SessionAdmitted(a.clone()))),
        ),
    };
    // B's tip change would reach the node only behind B's one-second
    // status floor, after the clock moves. A new session publishes its
    // first `Status` at once, so B is relinked once it has grown.
    let mut controls = vec![
        step(
            &b,
            ControlAction::Grow { len: 3 },
            Trigger::Submitted(Select::nth(OpKind::Locator, 1)),
        ),
        step(&b, ControlAction::Relink, Trigger::Grown(b.clone())),
        step(
            &a,
            action,
            Trigger::All(vec![
                Trigger::Count(2, Box::new(Trigger::SessionAdmitted(b.clone()))),
                reached,
            ]),
        ),
    ];
    let mut holds: Vec<_> = stage
        .held()
        .map(|kind| {
            controls.push(step("node", ControlAction::Release { hold: 0 }, departed));
            Hold {
                select: Select::nth(kind, 1),
                until: Trigger::Released,
                then: vec![Allow::Engine],
                deadline_ms: None,
            }
        })
        .into_iter()
        .collect();
    // A's first locator read waits for B's new session, which readies the
    // fixture, so A's round starts, and arms its deadline, only after. The
    // awaiting stage holds that read past A's departure already.
    if stage != Stage::Awaiting {
        holds.push(Hold {
            select: Select::nth(OpKind::Locator, 1),
            until: Trigger::Count(2, Box::new(Trigger::SessionAdmitted(b.clone()))),
            then: vec![Allow::Engine],
            deadline_ms: None,
        });
    }
    StorageScenario {
        node: Chain::main(3),
        mirrors: vec![
            MirrorSpec {
                chain: Chain::main(6),
                dialer: Dialer::Mirror,
                max_headers_per_response: None,
            },
            MirrorSpec {
                chain: Chain::main(3),
                dialer: Dialer::Mirror,
                max_headers_per_response: None,
            },
        ],
        policy: Policy {
            holds,
            ..Policy::converging()
        },
        controls,
        weights: Default::default(),
    }
}

/// [`LivenessScenario::Held`]'s storage scenario, for `kind`.
fn held(kind: OpKind) -> StorageScenario {
    StorageScenario {
        node: Chain::main(3),
        mirrors: vec![MirrorSpec {
            chain: Chain::main(6),
            dialer: Dialer::Mirror,
            max_headers_per_response: None,
        }],
        policy: Policy {
            holds: vec![Hold {
                select: Select::nth(kind, 1),
                until: Trigger::Released,
                then: vec![Allow::Engine],
                deadline_ms: None,
            }],
            ..Policy::converging()
        },
        controls: vec![ControlStep {
            peer: "node".to_owned(),
            action: ControlAction::Release { hold: 0 },
            when: Some(Trigger::Submitted(Select::nth(kind, 1))),
        }],
        weights: Default::default(),
    }
}

/// Ends a liveness run once its obligations have concluded, so it ends
/// idle rather than at its budget. That's once each held reply that reached
/// the reactor has been handled, and each served read's publisher has
/// queued a page, or the clock has moved past it, the fixture's key event
/// has happened, and every
/// `header-sync/peer-retirement-progress` obligation still open
/// ([`open_retirement`]) is past its deadline. A hold fixture's key event is
/// its first held reply, a serving fixture's its served read, or the clock
/// past [`FIXTURE_HORIZON`] for a control that serves none, a departure
/// fixture's the harness's close or
/// relink of [`OWNER`] with the clock past it, and a timeout fixture's the
/// clock past its horizon ([`LivenessScenario::horizon`]), and a recovery
/// fixture's `header-sync/bounded-adversary-progress` concluding or the
/// clock passing its deadline ([`RecoveryEvidence::progress`]). A departure
/// fixture whose departure never came, and a recovery fixture still
/// establishing, also end at the horizon. A `BeforeQuiescence` obligation concludes
/// by the first quiescence after its trigger, and the clock moves only then.
/// The fixture's own `Shutdown` step, if it has one, must be picked first.
/// Its pick begins the node under test's stop, as a `Shutdown` step's does,
/// and its task then stops every node ([`Topology::finish`]). It offers no
/// time boundary of its own, so it never changes when the clock moves before
/// then.
struct Finish {
    hold_wake: HoldWakeEvidence,
    publication: PublicationEvidence,
    ends: EndEvidence,
    retirement: RetirementEvidence,
    recovery: RecoveryProgressInputs,
    topology: Rc<Topology>,
    /// Which fixture it is, which says its key event.
    kind: FixtureKind,
    /// When its script is done ([`LivenessScenario::horizon`]).
    horizon: std::time::Duration,
    /// When the departure was first seen, once it was.
    departed_at: Option<SimInstant>,
    /// Whether the fixture has its own `Shutdown` step, which must come
    /// first.
    waits_for_shutdown: bool,
    /// Wakes the task that carries the finish out.
    to_task: watch::Sender<bool>,
    /// Set once the finish was picked.
    done: bool,
}

impl Finish {
    /// Whether the run's obligations have concluded at `now`, as
    /// [`Finish`] says.
    fn due(&self, now: SimInstant) -> bool {
        if self.done || (self.waits_for_shutdown && !self.topology.shutdown_picked()) {
            return false;
        }
        match self.kind {
            FixtureKind::Publication { commits, status } => {
                return self.publication_concluded(now, commits, status)
            }
            FixtureKind::Shutdown => return true,
            _ => {}
        }
        let facts = self.hold_wake.facts();
        let opened: Vec<(u64, SimInstant)> = facts
            .iter()
            .filter_map(|(_, fact)| match fact {
                HoldWakeFact::Opened { op, at, .. } | HoldWakeFact::Served { op, at, .. } => {
                    Some((*op, *at))
                }
                HoldWakeFact::Handled { .. } | HoldWakeFact::Queued { .. } => None,
            })
            .collect();
        let answered = |op: u64| {
            facts.iter().any(|(_, fact)| {
                matches!(
                    fact,
                    HoldWakeFact::Handled { op: answered, .. }
                        | HoldWakeFact::Queued { op: answered, .. } if *answered == op
                )
            })
        };
        let replies_concluded = opened.iter().all(|(op, at)| now > *at || answered(*op));
        let horizon = now > SimInstant::from_start(self.horizon);
        let key_event = match self.kind {
            FixtureKind::Hold => !opened.is_empty(),
            FixtureKind::Serving => !opened.is_empty() || horizon,
            FixtureKind::Departure => self.departed_at.is_some_and(|at| now > at) || horizon,
            FixtureKind::Timeout => horizon,
            FixtureKind::Recovery => match self.recovery.progress(&self.ends) {
                RecoveryProgress::Pending => horizon,
                RecoveryProgress::Concluded => true,
                RecoveryProgress::Owed { since } => now > recovery_deadline(since),
            },
            // Judged above, by their own commits and debts.
            FixtureKind::Publication { .. } | FixtureKind::Shutdown => true,
        };
        replies_concluded
            && key_event
            && open_retirement(&self.retirement, &self.ends)
                .into_iter()
                .all(|open| now > open.deadline)
    }

    /// Whether the node made at least `commits` commits, each adopted or
    /// with the clock past it, and, for a `status` fixture, has at least
    /// one `Status` debt, each paid or with the clock past its deadline.
    fn publication_concluded(&self, now: SimInstant, commits: usize, status: bool) -> bool {
        let facts = self.publication.facts();
        let mut made = 0;
        let mut debts = 0;
        let concluded = facts.iter().enumerate().all(|(index, (_, fact))| {
            let later = &facts[index..];
            match fact {
                PublicationFact::Committed { state_version, at } => {
                    made += 1;
                    now > *at
                        || later.iter().any(|(_, later)| {
                            matches!(later, PublicationFact::Adopted { state_version: adopted }
                                if adopted >= state_version)
                        })
                }
                PublicationFact::StatusOwed { session_id, at, .. } => {
                    debts += 1;
                    past_deadline(now, *at) || paid(later, *session_id)
                }
                _ => true,
            }
        });
        concluded && made >= commits && (!status || debts > 0)
    }
}

/// Whether `facts` hold a payment of `session_id`'s `Status` debt.
fn paid(facts: &[(u64, PublicationFact)], session_id: u64) -> bool {
    facts.iter().any(|(_, fact)| {
        matches!(fact, PublicationFact::StatusPaid { session_id: paid } if *paid == session_id)
    })
}

/// 1 ms past the deadline of a `Status` debt opened at `at`, the first
/// instant at which it has expired, if it's representable.
fn deadline_past(at: SimInstant) -> Option<SimInstant> {
    at.checked_add(STATUS_DEADLINE)?
        .checked_add(std::time::Duration::from_millis(1))
}

/// Whether `now` is past the deadline of a `Status` debt opened at `at`.
fn past_deadline(now: SimInstant, at: SimInstant) -> bool {
    deadline_past(at).is_some_and(|past| now >= past)
}

/// 1 ms past each unpaid `Status` debt's deadline in `publication`, still
/// ahead of `now`, for [`Deadlines`].
fn status_deadlines(publication: &PublicationEvidence, now: SimInstant) -> Vec<SimInstant> {
    let facts = publication.facts();
    facts
        .iter()
        .enumerate()
        .filter_map(|(index, (_, fact))| match fact {
            PublicationFact::StatusOwed { session_id, at, .. }
                if !paid(&facts[index..], *session_id) =>
            {
                deadline_past(*at)
            }
            _ => None,
        })
        .filter(|instant| *instant > now)
        .collect()
}

impl ActionSource for Finish {
    /// Offers `peer:node:finish` once, while [`Finish::due`].
    fn enabled(&self, now: SimInstant, out: &mut Vec<(Identity, u64)>) {
        if self.due(now) {
            out.push((Identity::peer("node", "finish"), 0));
        }
    }

    /// None, as [`Finish`] says.
    fn boundaries(&self, _now: SimInstant, _out: &mut Vec<Boundary>) {}

    /// Begins the stop and wakes the task.
    fn execute(&mut self, _local: u64, _log: &mut ObservationLog) {
        self.done = true;
        self.topology.begin_finish();
        self.to_task.send_replace(true);
    }

    /// Notes when the departure fixture's departure is first seen.
    fn observe(&mut self, _log: &mut ObservationLog) -> Result<(), String> {
        if self.kind == FixtureKind::Departure && self.departed_at.is_none() {
            let departed = self.ends.facts().into_iter().any(|(_, fact)| {
                matches!(
                    fact,
                    EndFact::Environment {
                        ref peer,
                        action: EnvironmentAction::Close | EnvironmentAction::Relink,
                        ..
                    } if peer == OWNER
                )
            });
            if departed {
                self.departed_at = Some(zakura_scheduler::hooks::now().unwrap_or_default());
            }
        }
        Ok(())
    }

    /// Nothing: it finishes no submitted work.
    fn recovery(&self, _now: SimInstant, _out: &mut crate::RecoveryOffer) {}

    /// Nothing: it has no holds.
    fn enter_drain(&mut self, _log: &mut ObservationLog) {}

    /// None: it holds no submitted work.
    fn holds_work(&self) -> Option<String> {
        None
    }
}

/// Which kind of liveness fixture a run is, for [`Finish`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FixtureKind {
    /// A hold fixture.
    Hold,
    /// A serving-read fixture or control.
    Serving,
    /// A departure fixture.
    Departure,
    /// A timeout fixture.
    Timeout,
    /// A recovery fixture.
    Recovery,
    /// An hdr-12b publication fixture: it waits for at least `commits`
    /// engine commits, each adopted or with the clock past it, so its
    /// adoption obligation has concluded at a quiescence, and, as a
    /// `status` fixture, for at least one `Status` debt, each paid or with
    /// the clock past its deadline.
    Publication {
        /// The commits the fixture makes.
        commits: usize,
        /// Whether it's a status fixture.
        status: bool,
    },
    /// An hdr-12b fixture that waits only for its own `Shutdown` step,
    /// after which every commit and debt opens nothing.
    Shutdown,
}

/// What [`RecoveryEvidence::progress`] reads besides the node's end facts:
/// its recovery and adoption evidence.
#[derive(Clone)]
struct RecoveryProgressInputs {
    recovery: RecoveryEvidence,
    adoption: AdoptionEvidence,
}

impl RecoveryProgressInputs {
    /// [`RecoveryEvidence::progress`] now.
    fn progress(&self, ends: &EndEvidence) -> RecoveryProgress {
        self.recovery.progress(&self.adoption, ends)
    }
}

/// Offers a time boundary one millisecond past each open request-deadline
/// arm's deadline ([`open_retirement`]), past an owed recovery's
/// ([`RecoveryEvidence::progress`]) and past each unpaid `Status` debt's
/// ([`status_deadlines`]), so the clock reaches it even when no other timer
/// would, and the registry can expire the obligation before the finish
/// could answer it (plan's acceptance: deadlines reach the clock). It
/// offers no action.
struct Deadlines {
    retirement: RetirementEvidence,
    ends: EndEvidence,
    recovery: RecoveryProgressInputs,
    /// The finish, whose unpaid `Status` debts' deadlines it also offers.
    publication: PublicationEvidence,
}

impl ActionSource for Deadlines {
    /// None: it only stops the clock.
    fn enabled(&self, _now: SimInstant, _out: &mut Vec<(Identity, u64)>) {}

    /// One millisecond past each open deadline still ahead of `now`.
    fn boundaries(&self, now: SimInstant, out: &mut Vec<Boundary>) {
        let owed = match self.recovery.progress(&self.ends) {
            RecoveryProgress::Owed { since } => Some(recovery_deadline(since)),
            RecoveryProgress::Pending | RecoveryProgress::Concluded => None,
        };
        out.extend(
            open_retirement(&self.retirement, &self.ends)
                .into_iter()
                .filter(|open| open.timed)
                .map(|open| open.deadline)
                .chain(owed)
                .filter_map(|deadline| deadline.checked_add(std::time::Duration::from_millis(1)))
                .filter(|at| *at > now)
                .map(Boundary::Opens),
        );
        out.extend(
            status_deadlines(&self.publication, now)
                .into_iter()
                .map(Boundary::Opens),
        );
    }

    /// Never called: it offers no action.
    fn execute(&mut self, _local: u64, _log: &mut ObservationLog) {
        unreachable!("the deadline source offers no action")
    }

    /// Nothing: it finishes no submitted work.
    fn recovery(&self, _now: SimInstant, _out: &mut crate::RecoveryOffer) {}

    /// Nothing: it has no holds.
    fn enter_drain(&mut self, _log: &mut ObservationLog) {}

    /// None: it holds no submitted work.
    fn holds_work(&self) -> Option<String> {
        None
    }
}

/// Commits one fixture header into the node under test's engine at each of
/// the run's first `ticks` seconds, as a `Commit` step does
/// ([`harness_commit`]). Each second is a deadline, so the clock can't pass
/// it before the commit, which a control step's trigger can't promise.
struct Ticker {
    port: Arc<ControlledPort>,
    fixture: HeaderFixture,
    adoption: AdoptionEvidence,
    topology: Rc<Topology>,
    /// The commits to make.
    ticks: u64,
    /// The commits made.
    made: u64,
}

impl Ticker {
    /// When the next commit is due, if one is left.
    fn due_at(&self) -> Option<SimInstant> {
        (self.made < self.ticks).then(|| {
            SimInstant::from_start(std::time::Duration::from_secs(self.made.saturating_add(1)))
        })
    }
}

impl ActionSource for Ticker {
    /// Offers `peer:node:tick` once the next commit is due.
    fn enabled(&self, now: SimInstant, out: &mut Vec<(Identity, u64)>) {
        if self.due_at().is_some_and(|at| now >= at) {
            out.push((Identity::peer("node", "tick"), 0));
        }
    }

    /// The next commit's instant, as a deadline.
    fn boundaries(&self, _now: SimInstant, out: &mut Vec<Boundary>) {
        out.extend(self.due_at().map(Boundary::Deadline));
    }

    /// Makes the commit, or fails the run with why it couldn't.
    fn execute(&mut self, _local: u64, _log: &mut ObservationLog) {
        self.made = self.made.saturating_add(1);
        if let Err(why) = harness_commit(&self.port, &self.fixture, &self.adoption, 1) {
            self.topology.fail(why);
        }
    }

    /// Nothing: a tick is new scenario work, as a `Commit` step is, so the
    /// drain makes no more of them.
    fn recovery(&self, _now: SimInstant, _out: &mut crate::RecoveryOffer) {}

    /// Nothing: it has no holds.
    fn enter_drain(&mut self, _log: &mut ObservationLog) {}

    /// None: each commit completes in its own step.
    fn holds_work(&self) -> Option<String> {
        None
    }
}

/// What every liveness fixture declares: `BoundedAdversary`, with no
/// hostile peer, as the module doc says. A test harness that wraps
/// [`Liveness`] declares it too.
pub fn assumptions<S>() -> Assumptions<S> {
    Assumptions::declare(vec![bounded_adversary()])
}

/// [`assumptions`]'s one record.
fn bounded_adversary() -> AssumeRecord {
    AssumeRecord {
        assume: Assume::BoundedAdversary,
        params: json!({ "hostile_peers": 0 }),
    }
}

/// `header-sync/storage` on a liveness fixture, with the hold-wake
/// evidence armed and `BoundedAdversary` declared.
pub struct Liveness(Storage);

impl HasView for Liveness {
    type View = StorageView;

    /// The storage harness's view.
    fn view(&self) -> StorageView {
        self.0.view()
    }

    /// The storage harness's end.
    fn ended(&self, end: &RunEnd) {
        self.0.ended(end);
    }

    /// The storage harness's end violations.
    fn end_violations(&self, log: &ObservationLog) -> Vec<EndViolation> {
        self.0.end_violations(log)
    }

    /// The inner harness's stop.
    fn stopping(&self) -> bool {
        self.0.stopping()
    }

    /// The storage harness's stamps.
    fn stamps_after(&self, after: Option<u64>) -> Option<Vec<u64>> {
        self.0.stamps_after(after)
    }
}

impl Subsystem for Liveness {
    const NAME: &'static str = "header-sync/liveness";
    // 2: hdr-11's recovery fixtures, and 3: hdr-12b's publication
    // fixtures after them, each of which grew [`LivenessScenario::all`] and
    // so moved what each scenario byte picks.
    const GENERATOR: u32 = 3;
    const SCENARIO_BYTES: usize = 1;
    type Scenario = LivenessScenario;
    type Prepared = Ports;

    /// [`storage_config`], with [`LIVENESS_BUDGET`].
    fn config(_: &LivenessScenario) -> RunConfig {
        RunConfig {
            budget: LIVENESS_BUDGET,
            ..storage_config()
        }
    }

    /// The storage harness's ports for the fixture, with the hold-wake
    /// evidence armed, the publication evidence armed as the fixture says
    /// ([`LivenessScenario::arming`]), a departure or timeout fixture's
    /// hand-off rule for its mirrors ([`LivenessScenario::roles`]), and the
    /// late-page fixture's page delay on [`OWNER`].
    fn prepare(scenario: &LivenessScenario) -> Ports {
        let retirement = scenario
            .roles()
            .map_or_else(RetirementEvidence::default, RetirementEvidence::armed);
        let delayed_page = (*scenario == LivenessScenario::LatePage).then(|| DelayedPage {
            mirror: OWNER.to_owned(),
            by: LATE_PAGE_DELAY,
        });
        Ports {
            hold_wake: HoldWakeEvidence::armed(),
            publication: PublicationEvidence::armed(scenario.arming()),
            retirement,
            delayed_page,
            ..Storage::prepare(&scenario.storage())
        }
    }

    /// The storage harness on the fixture, with its [`Finish`] source and
    /// the task that carries the finish out.
    fn build(sim: &mut Sim, scenario: &LivenessScenario, ports: Ports) -> Self {
        let hold_wake = ports.hold_wake.clone();
        let ends = ports.ends.clone();
        let retirement = ports.retirement.clone();
        let publication = ports.publication.clone();
        let fixture = ports.fixture.clone();
        let storage = scenario.storage();
        let storage_harness = Storage::build(sim, &storage, ports);
        let recovery = RecoveryProgressInputs {
            recovery: storage_harness.recovery.clone(),
            adoption: storage_harness.adoption.clone(),
        };
        let topology = storage_harness.topology.clone();
        if scenario.ticks() > 0 {
            sim.add_source(Box::new(Ticker {
                port: storage_harness.port.clone(),
                fixture,
                adoption: storage_harness.adoption.clone(),
                topology: topology.clone(),
                ticks: scenario.ticks(),
                made: 0,
            }));
        }
        let (to_task, mut finishing) = watch::channel(false);
        let task_topology = topology.clone();
        sim.scheduler().spawn_root("finish", async move {
            while !*finishing.borrow_and_update() {
                if finishing.changed().await.is_err() {
                    return;
                }
            }
            task_topology.finish();
        });
        sim.add_source(Box::new(Deadlines {
            retirement: retirement.clone(),
            ends: ends.clone(),
            recovery: recovery.clone(),
            publication: publication.clone(),
        }));
        sim.add_source(Box::new(Finish {
            hold_wake,
            publication,
            ends,
            retirement,
            recovery,
            topology,
            kind: scenario.kind(),
            horizon: scenario.horizon(),
            departed_at: None,
            waits_for_shutdown: storage
                .controls
                .iter()
                .any(|step| step.action == ControlAction::Shutdown),
            to_task,
            done: false,
        }));
        Liveness(storage_harness)
    }

    /// The storage harness's properties, then
    /// `header-sync/hold-release-wakes` and
    /// `header-sync/bounded-adversary-progress`, which apply only here.
    fn properties() -> Vec<Property<Self>> {
        let mut properties = storage_properties();
        properties.push(hold_release_wakes());
        properties.push(bounded_adversary_progress());
        properties
    }

    /// The storage harness's sources.
    fn sources() -> Sources {
        storage_sources()
    }

    /// [`assumptions`], and for a recovery fixture also the five recovery
    /// assumptions ([`recovery_assumptions`]), which hold as the storage
    /// harness says ([`RecoveryEvidence::holds`]).
    fn assumptions(scenario: &LivenessScenario) -> Assumptions<Self> {
        let Some(source) = recovery_source(&scenario.storage()) else {
            return assumptions();
        };
        let mut declared = vec![bounded_adversary()];
        declared.extend(recovery_assumptions(&source));
        Assumptions::declare(declared).holds_when(|liveness, assume| {
            assume == Assume::BoundedAdversary
                || liveness.0.recovery.holds(&liveness.0.ends, assume)
        })
    }

    /// The fixture [`LivenessScenario::all`] lists at the scenario byte's
    /// value, modulo their count.
    fn draw(prefix: &[u8]) -> LivenessScenario {
        let all = LivenessScenario::all();
        let byte = usize::from(prefix.first().copied().unwrap_or(0));
        all[byte % all.len()]
    }
}
