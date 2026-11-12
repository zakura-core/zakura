//! Header-sync harnesses. `header-sync/smoke` runs the reactor on the step
//! loop with inert storage and no peers. `header-sync/storage` runs a node
//! on controlled storage with mirror peers (M4), with the node's reactor
//! tapped. `header-sync/wire` runs the same node behind its real transport
//! on simulated QUIC, with scripted peers (M5-C), honest ones (M5-D) and
//! adversaries (M5-E1), checked by header sync's generated table properties
//! and `session/honest-never-punished` (P3d). `header-sync/release-wake`
//! runs `header-sync/storage` on the wake fixtures alone,
//! `header-sync/flood` on the flood fixtures, `header-sync/repair` on the
//! VCT repair fixtures, and `header-sync/liveness` on hdr-10's liveness
//! fixtures.

mod flood;
pub mod grammar;
pub mod liveness;
mod release_wake;
mod repair;

pub use flood::{Flood, FloodScenario, FLOODERS};
pub use liveness::Liveness;
pub use release_wake::{ReleaseWake, WakeScenario};
pub use repair::{RepairHarness, RepairScenario, REPAIR_HEIGHT, STALE_PEER, SUPPLIER, TIP};

use std::{any::Any, cell::RefCell, collections::BTreeSet, rc::Rc, sync::Arc};

use serde::{Deserialize, Serialize};
use zakura_chain::{block::Height, parameters::Network};
use zakura_header_chain::Frontier;
use zakura_network::zakura::{
    observe::{HeaderSyncObserver, ReactorEvent},
    spawn_header_sync_reactor,
    v9::RegtestMessage,
    FullStateFrontiers, HeaderSyncStartup, ZakuraHeaderSyncConfig, LOCAL_MAX_MESSAGE_BYTES,
};
use zakura_node_services::header_chain::InertHeaderChainPort;
use zakura_rt::sync::watch;
use zakura_scheduler::{SchedulerConfig, SimInstant};

use zakura_storage::header::{
    adapter::EnginePort, controlled::ControlledPort, fixture::HeaderFixture,
};

use super::{
    header_node::StartupFacts,
    header_node::{
        adoption::{AdoptionEvidence, SessionLife},
        authority::AuthorityEvidence,
        capacity::CapacityEvidence,
        chain_effect::ChainEffectEvidence,
        claim::{ClaimEvidence, SettlementEvidence},
        ends::EndEvidence,
        fault::{FaultCheck, FaultEvidence},
        fork_choice::{settled::ForkFixture, ForkChoiceEvidence},
        hold_wake::HoldWakeEvidence,
        outstanding::{admitted_peer_limit, OutstandingEvidence},
        policy::{self, Allow, Hold, OpKind, Select, Trigger},
        preload,
        publication::{Arming, PublicationEvidence},
        recovery::{RecoveryEvidence, RecoveryInputs, RecoveryProgress, RECOVERY_BOUND},
        reservation::ReservationEvidence,
        retirement::RetirementEvidence,
        round_life::RoundLifeEvidence,
        session::SessionEvidence,
        stale::StaleEvidence,
        stamp::Stamps,
        tap::ReactorTap,
        wake::WakeEvidence,
        Base, Chain, Control, ControlAction, ControlStep, DelayedPage, DispatchFlag, Effects, Fork,
        HeaderNode, Policy, PolicySource, Topology,
    },
    scenario::Rejection,
    wire_node::{TransportTap, WireNode},
    wire_plans::SavedPlan,
    NetScenario, Subsystem,
};
use crate::{
    domain::{
        header_sync::{
            chunk_budget_bound, commit_only_authorized, expected_chain_selected,
            outstanding_end_violations, outstanding_work_bounded, peer_retirement_progress,
            publication_end_violations, reset_invalidates_work, retirement_end_violations,
            stale_chain_effect_inert, stale_completion_inert, state_publication_consistent,
            teardown_end_violations, teardown_returns_baseline, AdoptionView, AuthorityView,
            BudgetView, ChainView, ForkView, HoldWakeView, OutstandingView, PublicationView,
            ResetView, RetirementView, StaleView, TeardownView, WakeView, OUTSTANDING_WORK_BOUNDED,
        },
        header_sync_progress::{bounded_adversary_progress, recovery_deadline},
        session::{honest_never_punished, HonestPeers},
    },
    observe::StorageFact,
    peer::{
        adversary::Act, honest::HonestChain, limit::ADVERSARY_RATE, peer_id, Dialer as PeerDialer,
        PeerAction, PeerCloses, PeerControl, PeerFailure, PeerRecords, PeerSpec, PeerTask,
        NODE_ADDR,
    },
    property::{
        Assume, AssumeRecord, Assumptions, CheckCtx, EndViolation, HasView, Kind, Pending,
        Property, PropertyId, Source, Sources, Status, Verdict,
    },
    storage::HEADER_SERVICE,
    table::{sender_properties, table_properties},
    ActionSource, Boundary, Identity, Observation, ObservationLog, RunConfig, RunEnd, Sim,
};

/// The smoke run's scenario. It has no knobs yet.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scenario {}

/// The header-sync reactor at Regtest genesis, with inert storage and no
/// peers. It checks only the step loop's properties, so a run shows that the
/// reactor runs on a `Plan` and shuts down.
pub struct Smoke {
    /// The reactor's handle and action channel. They live until the run
    /// ends, so the reactor doesn't see its inputs close.
    _inputs: Box<dyn Any>,
}

impl HasView for Smoke {
    type View = ();

    /// Nothing, since no property reads it.
    fn view(&self) {}
}

impl Subsystem for Smoke {
    const NAME: &'static str = "header-sync/smoke";
    const GENERATOR: u32 = 1;
    const SCENARIO_BYTES: usize = 0;
    type Scenario = Scenario;
    type Prepared = ();

    /// 2 workers, small stacks, and 2,000 live steps.
    fn config(_: &Scenario) -> RunConfig {
        RunConfig {
            scheduler: SchedulerConfig {
                workers: 2,
                // The reactor's stack needs are small, and a small stack keeps
                // the pool cheap.
                stack_size: 64 * 1024,
                ..SchedulerConfig::default()
            },
            budget: 2_000,
            drain_bound: 5_000,
            ..RunConfig::default()
        }
    }

    /// Nothing to set up.
    fn prepare(_: &Scenario) {}

    /// Spawns the reactor and a root task that joins it.
    fn build(sim: &mut Sim, _: &Scenario, (): ()) -> Self {
        let (handle, actions, reactor) =
            spawn_header_sync_reactor(startup()).expect("the Regtest genesis startup is valid");
        sim.scheduler().spawn_root("join", async move {
            let _ = reactor.await;
        });
        Smoke {
            _inputs: Box::new((handle, actions)),
        }
    }

    /// The only scenario.
    fn draw(_: &[u8]) -> Scenario {
        Scenario {}
    }
}

/// Header-sync startup on Regtest at genesis, with inert storage.
fn startup() -> HeaderSyncStartup {
    let network = Network::new_regtest(Default::default());
    let anchor = (Height(0), network.genesis_hash());
    let mut startup = HeaderSyncStartup::new(
        network,
        anchor,
        FullStateFrontiers {
            finalized_height: anchor.0,
            verified_block_tip: anchor.0,
            verified_block_hash: anchor.1,
        },
        Some(anchor),
        ZakuraHeaderSyncConfig::default(),
        LOCAL_MAX_MESSAGE_BYTES,
        zakura_network::zakura::DEFAULT_ZAKURA_MAX_CONNECTIONS,
    );
    startup.header_chain_port = Arc::new(InertHeaderChainPort);
    startup
}

/// A run reported a direct-dispatch violation: some reactor sent a storage
/// call to its action channel, so its port never saw it.
pub const DIRECT_DISPATCH: &str = "harness/direct-dispatch";

/// The node's storage operations get rising IDs, each completes at most
/// once, and only after its submission (M4.1).
pub const OPERATION_IDS: &str = "header-sync/storage-operation-ids";

/// Each header-peer fault the reactor took is penalized against its work's
/// session and starts its peer's readmission cooldown, and no session from
/// that peer is admitted while the cooldown lasts ([`FaultEvidence::check`]).
/// hdr-3's `fault-penalized-after-teardown` is its disconnect case.
pub const HEADER_FAULT_PENALIZED: &str = "header-sync/header-fault-penalized";

/// Which end of a mirror's link dials.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Dialer {
    /// The mirror dials the node.
    Mirror,
    /// The node dials the mirror.
    Node,
}

/// A mirror peer: real header-sync code on an engine port. It's named
/// `mirror-<i>` by its index in [`StorageScenario::mirrors`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MirrorSpec {
    /// The chain it holds when the run starts.
    pub chain: Chain,
    /// Which end of its link to the node dials.
    pub dialer: Dialer,
    /// The most headers its `Status` says it serves in one response, or
    /// production's default when unset. The node reserves each round's
    /// share from this, so a smaller value lets more rounds fit the chunk
    /// budget at once.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_headers_per_response: Option<u32>,
}

/// How much more work each header of fork branches 1 and 2 carries. A
/// branch at weight `w`, 0 to [`MAX_WEIGHT`], has the genesis target shifted
/// right by `w` bits, so each header has about `2^w` times the work. See
/// [`HeaderFixture::with_weight`].
///
/// [`MAX_WEIGHT`]: zakura_storage::header::fixture::MAX_WEIGHT
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Weights {
    /// Branch 1's weight.
    #[serde(default)]
    pub branch_1: u8,
    /// Branch 2's weight.
    #[serde(default)]
    pub branch_2: u8,
}

impl Weights {
    /// Whether every branch keeps the genesis target.
    pub fn is_unweighted(&self) -> bool {
        *self == Weights::default()
    }

    /// The Regtest fixture with these weights.
    ///
    /// # Panics
    ///
    /// If a weight is above
    /// [`MAX_WEIGHT`](zakura_storage::header::fixture::MAX_WEIGHT).
    pub fn fixture(self) -> HeaderFixture {
        HeaderFixture::regtest()
            .with_weight(1, self.branch_1)
            .with_weight(2, self.branch_2)
    }
}

/// The `header-sync/storage` scenario. A fuzz input's prefix encodes it as
/// [`grammar`] describes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageScenario {
    /// The chain the node under test holds when the run starts.
    pub node: Chain,
    /// The mirror peers, each linked once to the node at setup.
    pub mirrors: Vec<MirrorSpec>,
    /// The weights of fork branches 1 and 2, which every chain in the run
    /// shares.
    #[serde(default, skip_serializing_if = "Weights::is_unweighted")]
    pub weights: Weights,
    /// How the run completes the node's storage operations.
    pub policy: Policy,
    /// Link actions the `Plan` may take, in order.
    #[serde(default)]
    pub controls: Vec<ControlStep>,
}

impl Default for StorageScenario {
    /// The node at genesis and `mirror-0` at A5, under the converging policy.
    fn default() -> Self {
        StorageScenario {
            node: Chain::default(),
            mirrors: vec![MirrorSpec {
                chain: Chain::main(5),
                dialer: Dialer::Mirror,
                max_headers_per_response: None,
            }],
            policy: Policy::converging(),
            controls: Vec::new(),
            weights: Weights::default(),
        }
    }
}

impl StorageScenario {
    /// The node at A2 with `mirror-0` at A5, dialing, and `mirror-1` on a
    /// fork of A3, dialed by the node. Each mirror's link closes and
    /// relinks: `mirror-0`'s at once, `mirror-1`'s once `mirror-0` is
    /// linked again.
    pub fn close_relink() -> Self {
        StorageScenario {
            node: Chain::main(2),
            mirrors: vec![
                MirrorSpec {
                    chain: Chain::main(5),
                    dialer: Dialer::Mirror,
                    max_headers_per_response: None,
                },
                MirrorSpec {
                    chain: Chain {
                        main: 3,
                        fork: Some(Fork { branch: 1, len: 4 }),
                    },
                    dialer: Dialer::Node,
                    max_headers_per_response: None,
                },
            ],
            controls: vec![
                step("mirror-0", ControlAction::Close, None),
                step(
                    "mirror-0",
                    ControlAction::Relink,
                    Some(Trigger::Closed(mirror(0))),
                ),
                step(
                    "mirror-1",
                    ControlAction::Close,
                    Some(Trigger::Count(2, Box::new(Trigger::Linked(mirror(0))))),
                ),
                step(
                    "mirror-1",
                    ControlAction::Relink,
                    Some(Trigger::Closed(mirror(1))),
                ),
            ],
            ..StorageScenario::default()
        }
    }

    /// Three mirrors whose chains grow while the node syncs: `mirror-0`
    /// once the node applies its first headers, `mirror-2`'s fork once
    /// `mirror-0` has grown, then `mirror-0` again from A13, after which its
    /// link closes and relinks.
    pub fn grow() -> Self {
        StorageScenario {
            mirrors: vec![
                MirrorSpec {
                    chain: Chain::main(5),
                    dialer: Dialer::Mirror,
                    max_headers_per_response: None,
                },
                MirrorSpec {
                    chain: Chain::main(1),
                    dialer: Dialer::Node,
                    max_headers_per_response: None,
                },
                MirrorSpec {
                    chain: Chain {
                        main: 2,
                        fork: Some(Fork { branch: 2, len: 3 }),
                    },
                    dialer: Dialer::Mirror,
                    max_headers_per_response: None,
                },
            ],
            controls: vec![
                step(
                    "mirror-0",
                    ControlAction::Grow { len: 8 },
                    Some(Trigger::Handled(Select::nth(OpKind::Apply, 1))),
                ),
                step(
                    "mirror-2",
                    ControlAction::Grow { len: 3 },
                    Some(Trigger::Grown(mirror(0))),
                ),
                step("mirror-0", ControlAction::Grow { len: 2 }, None),
                step(
                    "mirror-0",
                    ControlAction::Close,
                    Some(Trigger::Count(2, Box::new(Trigger::Grown(mirror(0))))),
                ),
                step("mirror-0", ControlAction::Relink, None),
            ],
            ..StorageScenario::default()
        }
    }

    /// Four mirrors under [`Base::Any`] within 2 s, with holds that
    /// together offer every outcome: the first read until a second read
    /// completes, the first session's applies for 3 s, the second acquire
    /// until its caller drops it, and every locator until the first VCT
    /// context reply is taken.
    pub fn holds() -> Self {
        let read = Select {
            kind: Some(OpKind::Read),
            ..Select::default()
        };
        StorageScenario {
            node: Chain::main(1),
            mirrors: vec![
                MirrorSpec {
                    chain: Chain::main(6),
                    dialer: Dialer::Mirror,
                    max_headers_per_response: None,
                },
                MirrorSpec {
                    chain: Chain::main(4),
                    dialer: Dialer::Node,
                    max_headers_per_response: None,
                },
                MirrorSpec {
                    chain: Chain {
                        main: 1,
                        fork: Some(Fork { branch: 1, len: 5 }),
                    },
                    dialer: Dialer::Mirror,
                    max_headers_per_response: None,
                },
                MirrorSpec {
                    chain: Chain::default(),
                    dialer: Dialer::Node,
                    max_headers_per_response: None,
                },
            ],
            policy: Policy {
                base: Base::Any,
                bound_ms: Some(2_000),
                holds: vec![
                    Hold {
                        select: Select::nth(OpKind::Read, 1),
                        until: Trigger::Count(2, Box::new(Trigger::Completed(read))),
                        then: vec![Allow::Engine, Allow::Timeout],
                        deadline_ms: None,
                    },
                    Hold {
                        select: Select {
                            kind: Some(OpKind::Apply),
                            session: Some(1),
                            ..Select::default()
                        },
                        until: Trigger::VirtualTime(3_000),
                        then: vec![Allow::Unavailable, Allow::LocalResource, Allow::Rejected],
                        deadline_ms: None,
                    },
                    Hold {
                        select: Select::nth(OpKind::Acquire, 2),
                        until: Trigger::CallerDropped(Select::held()),
                        then: vec![Allow::Engine, Allow::Rejected],
                        deadline_ms: None,
                    },
                    Hold {
                        select: Select {
                            kind: Some(OpKind::Locator),
                            ..Select::default()
                        },
                        until: Trigger::ReplyTaken(Select::nth(OpKind::VctContext, 1)),
                        then: vec![Allow::Engine],
                        deadline_ms: None,
                    },
                ],
                reply_holds: Vec::new(),
                disruptions: Vec::new(),
            },
            ..StorageScenario::default()
        }
    }

    /// Two mirrors whose control steps wait for each trigger kind the
    /// grammar has: a storage event of each kind, a link closing, a link
    /// made, a growth and a virtual time. A hold of the first prepare waits
    /// for the first membership submission.
    pub fn triggers() -> Self {
        StorageScenario {
            node: Chain::main(3),
            mirrors: vec![
                MirrorSpec {
                    chain: Chain::main(9),
                    dialer: Dialer::Node,
                    max_headers_per_response: None,
                },
                MirrorSpec {
                    chain: Chain {
                        main: 3,
                        fork: Some(Fork { branch: 2, len: 6 }),
                    },
                    dialer: Dialer::Mirror,
                    max_headers_per_response: None,
                },
            ],
            policy: Policy {
                holds: vec![Hold {
                    select: Select::nth(OpKind::Prepare, 1),
                    until: Trigger::Submitted(Select::nth(OpKind::Membership, 1)),
                    then: vec![Allow::Engine, Allow::Timeout],
                    deadline_ms: None,
                }],
                ..Policy::converging()
            },
            controls: vec![
                step(
                    "mirror-0",
                    ControlAction::Close,
                    Some(Trigger::Submitted(Select::nth(OpKind::Locator, 2))),
                ),
                step(
                    "mirror-0",
                    ControlAction::Relink,
                    Some(Trigger::Closed(mirror(0))),
                ),
                step(
                    "mirror-1",
                    ControlAction::Grow { len: 4 },
                    Some(Trigger::Completed(Select::nth(OpKind::Read, 1))),
                ),
                step(
                    "mirror-1",
                    ControlAction::Close,
                    Some(Trigger::Grown(mirror(1))),
                ),
                step(
                    "mirror-1",
                    ControlAction::Relink,
                    Some(Trigger::ReplyTaken(Select::nth(OpKind::Acquire, 1))),
                ),
                step(
                    "mirror-0",
                    ControlAction::Grow { len: 2 },
                    Some(Trigger::Count(2, Box::new(Trigger::Linked(mirror(1))))),
                ),
                step(
                    "mirror-0",
                    ControlAction::Close,
                    Some(Trigger::Handled(Select::nth(OpKind::Apply, 1))),
                ),
                step(
                    "mirror-0",
                    ControlAction::Relink,
                    Some(Trigger::VirtualTime(6_000)),
                ),
                step(
                    "mirror-1",
                    ControlAction::Close,
                    Some(Trigger::CallerDropped(Select {
                        kind: Some(OpKind::Read),
                        ..Select::default()
                    })),
                ),
            ],
            weights: Weights::default(),
        }
    }
}

/// The header window the reactor keeps ahead of verified bodies, restated
/// from its `INTEGRATED_HEADER_BODY_WINDOW_V1`, which is the wire's
/// `MAX_HS_RANGE`.
pub const HEADER_BODY_WINDOW: u32 = 4_000;

/// The reactor's refill batch, restated from its `HEADER_REFILL_BATCH_V1`:
/// one maximum checkpoint gap plus its successor's first header. With less
/// window left than this, it withholds a target the window doesn't reach.
pub const HEADER_REFILL_BATCH: u32 = 401;

/// The most headers the fork fixture's mirror chains hold together. The
/// simulator never verifies a body, so the node's window stays the whole
/// [`HEADER_BODY_WINDOW`] minus its selected height and the headers it has
/// claimed. With every mirror chain counted, both stay at most this, so at
/// least `HEADER_BODY_WINDOW - 2 * FORK_FIXTURE_MAX_HEADERS` is always left,
/// more than a [`HEADER_REFILL_BATCH`]: no correct node waits on a body
/// that never comes, which would settle it short of the heaviest tip.
pub const FORK_FIXTURE_MAX_HEADERS: u32 = 1_000;

/// `scenario`'s mirrors, if it's the fork fixture that settled convergence
/// judges: the node at genesis, two or three mirrors whose initial chains
/// are forks from one ancestor, none a prefix of another, holding at most
/// [`FORK_FIXTURE_MAX_HEADERS`] headers together, storage success-only
/// within a 10 ms bound, no holds, disruptions or controls. A mirror runs header
/// sync too, so it may take a fork from the node later, but only one some
/// mirror held at the start, so no mirror ever holds more work than the
/// heaviest initial chain. See
/// [`fork_choice::settled`](super::header_node::fork_choice::settled).
pub fn fork_fixture(scenario: &StorageScenario) -> Option<ForkFixture> {
    let Policy {
        base,
        bound_ms,
        holds,
        reply_holds,
        disruptions,
    } = &scenario.policy;
    let declared = scenario.node == Chain::default()
        && (2..=3).contains(&scenario.mirrors.len())
        && *base == Base::SuccessOnly
        && bound_ms.is_some_and(|bound| bound <= 10)
        && holds.is_empty()
        && reply_holds.is_empty()
        && disruptions.is_empty()
        && scenario.controls.is_empty()
        && scenario
            .mirrors
            .iter()
            .map(|mirror| {
                mirror
                    .chain
                    .main
                    .saturating_add(mirror.chain.fork.map_or(0, |fork| fork.len))
            })
            .fold(0u32, u32::saturating_add)
            <= FORK_FIXTURE_MAX_HEADERS;
    if !declared {
        return None;
    }
    let fixture = scenario.weights.fixture();
    let paths: Vec<Vec<zakura_chain::block::Hash>> = scenario
        .mirrors
        .iter()
        .map(|mirror| {
            mirror
                .chain
                .segments(&fixture)
                .into_iter()
                .flat_map(|(_, headers)| headers.into_iter().map(|header| header.hash()))
                .collect()
        })
        .collect();
    let mut shared = None;
    for (index, left) in paths.iter().enumerate() {
        for right in &paths[index + 1..] {
            let common = left
                .iter()
                .zip(right)
                .take_while(|(left, right)| left == right)
                .count();
            // A chain that's a prefix of another isn't a fork of it.
            if common == left.len() || common == right.len() {
                return None;
            }
            if *shared.get_or_insert(common) != common {
                return None;
            }
        }
    }
    Some(ForkFixture {
        mirrors: scenario.mirrors.iter().map(|mirror| mirror.chain).collect(),
    })
}

/// `peer`'s control step `action`, offered once `when` holds.
fn step(peer: &str, action: ControlAction, when: Option<Trigger>) -> ControlStep {
    ControlStep {
        peer: peer.to_owned(),
        action,
        when,
    }
}

/// `mirror-<index>`, a [`StorageScenario`] mirror's name.
fn mirror(index: usize) -> String {
    format!("mirror-{index}")
}

/// A [`StorageScenario`] mirror's observer: the settled points' record of
/// its own reactor deciding on its session with the node, from its events,
/// and the publication check's record of it refusing the node's frames,
/// from its capacity handle, which the mirror's service reports refusals
/// to. Neither takes the session token or the resource probe.
#[derive(Debug)]
struct MirrorObserver {
    decisions: Arc<dyn HeaderSyncObserver>,
    refusals: Arc<dyn HeaderSyncObserver>,
}

impl HeaderSyncObserver for MirrorObserver {
    /// Hands `event` to the decisions record.
    fn observe(&self, event: ReactorEvent) {
        self.decisions.observe(event);
    }

    /// The refusals record's handle.
    fn capacity_observer(&self) -> Option<Arc<dyn HeaderSyncObserver>> {
        self.refusals.capacity_observer()
    }
}

/// The scenario's storage, with each actor's chain committed before the
/// run, and the node's session view. Cloning shares both, so a test can read
/// them after a run. One prepared `Ports` serves one run.
#[derive(Clone)]
pub struct Ports {
    /// The fixture every chain comes from.
    pub fixture: HeaderFixture,
    /// The node's controlled port.
    pub node: Arc<ControlledPort>,
    /// The clock the node's chain was preloaded at, before the run, which
    /// its headers' admission read: the host's, since no run had started.
    pub preloaded_at: chrono::DateTime<chrono::Utc>,
    /// Each mirror's engine port, in scenario order.
    pub mirrors: Vec<Arc<EnginePort>>,
    /// The node's session view, which its tap and links fill during the run.
    pub sessions: SessionEvidence,
    /// The node's header capacity accounting, which its tap fills during the
    /// run.
    pub reservations: ReservationEvidence,
    /// The node's resource probe reads, which its tap checks and keeps
    /// during the run.
    pub capacity: CapacityEvidence,
    /// The node's released rounds at each drain, which its tap keeps during
    /// the run.
    pub rounds: SettlementEvidence,
    /// The node's stale prepare and apply calls, which its tap finds during
    /// the run.
    pub stale_calls: StaleEvidence,
    /// The node's wake facts, which its tap finds during the run. Inert
    /// unless whoever prepared the ports declared a wake fixture
    /// ([`WakeEvidence::new`]).
    pub wake: WakeEvidence,
    /// The node's held replies and their handling, which its storage source
    /// finds during the run. Inert unless whoever prepared the ports armed it
    /// ([`HoldWakeEvidence::armed`]).
    pub hold_wake: HoldWakeEvidence,
    /// The harness's environment actions and the reactor's own exit, which
    /// the harness and the node's tap record during the run. It holds the
    /// node's stamps.
    pub ends: EndEvidence,
    /// When the node's reset check first applied, which its tap finds
    /// during the run.
    pub round_life: RoundLifeEvidence,
    /// When the node's departed-work check first applied, which its tap
    /// finds during the run.
    pub retirement: RetirementEvidence,
    /// When the node's stale-chain check first applied, which its tap
    /// finds during the run.
    pub chain_effect: ChainEffectEvidence,
    /// When the node's authority check first applied, which its tap finds
    /// during the run.
    pub authority: AuthorityEvidence,
    /// The reactor's container peaks and when the outstanding-work rule
    /// first applied, which its tap finds during the run.
    pub outstanding: OutstandingEvidence,
    /// The run's recovery, which a `Recover` step starts.
    pub recovery: RecoveryEvidence,
    /// The mirror whose first page on each link the harness holds back.
    /// `None` unless whoever prepared the ports set one.
    pub delayed_page: Option<DelayedPage>,
    /// When the node's fork-choice check first applied, which its tap finds
    /// during the run.
    pub fork_choice: ForkChoiceEvidence,
    /// When the node's publication check first applied, and the tip it
    /// still owes, which its tap finds during the run.
    pub publication: PublicationEvidence,
    /// Whether the harness ends a recovering run once its recovery
    /// concludes ([`RecoveryFinish`]). Set for a scenario with a `Recover`
    /// step. A harness with its own finish clears it.
    pub finishes: bool,
}

/// The node under test, named `node`, on a controlled port with mirror
/// peers, checked by [`storage_properties`].
pub struct Storage {
    port: Arc<ControlledPort>,
    dispatch: DispatchFlag,
    /// The node's adoption facts, from its tap.
    adoption: AdoptionEvidence,
    /// The node's probe reads, from its tap.
    capacity: CapacityEvidence,
    /// The claim rules the node's tap found broken.
    claims: ClaimEvidence,
    /// The node's released rounds, from its tap.
    rounds: SettlementEvidence,
    /// The node's stale prepare and apply calls, from its tap.
    stale_calls: StaleEvidence,
    /// The node's wake facts, from its tap.
    wake: WakeEvidence,
    /// The node's held replies and their handling, from its storage source.
    hold_wake: HoldWakeEvidence,
    /// The harness's environment actions and the reactor's own exit, from
    /// its tap.
    ends: EndEvidence,
    /// The node's stamps, from its tap.
    stamps: Stamps,
    /// When the node's reset check first applied, from its tap.
    round_life: RoundLifeEvidence,
    /// When the node's departed-work check first applied, from its tap.
    retirement: RetirementEvidence,
    /// When the node's stale-chain check first applied, from its tap.
    chain_effect: ChainEffectEvidence,
    /// When the node's authority check first applied, from its tap.
    authority: AuthorityEvidence,
    /// The reactor's container peaks, from its tap.
    outstanding: OutstandingEvidence,
    /// When the node's fork-choice check first applied, from its tap.
    fork_choice: ForkChoiceEvidence,
    /// When the node's publication check first applied, from its tap.
    publication: PublicationEvidence,
    /// Whom the node's storage failures blamed.
    faults: FaultEvidence,
    /// The nodes and links. They live until the run ends, so no reactor sees
    /// its inputs close.
    topology: Rc<Topology>,
    /// The run's recovery, once a `Recover` step is picked.
    recovery: RecoveryEvidence,
}

/// What [`Storage`]'s checks read between steps.
#[derive(Clone)]
pub struct StorageView {
    /// The node's port, for its pending operations and its engine's state.
    pub port: Arc<ControlledPort>,
    /// Whether any reactor dispatched a storage call externally.
    pub direct_dispatch_broken: bool,
    /// The peers the scenario marks `HonestPeer`, by index. Only
    /// `header-sync/wire` has peers to mark.
    pub honest_peers: Rc<BTreeSet<usize>>,
    /// The node's adopted state and what it published from it.
    pub adoption: AdoptionEvidence,
    /// Each of the node's sessions and how long it lasted. Only
    /// `header-sync/storage` knows them, from its links.
    pub sessions: Vec<SessionLife>,
    /// Whom the node's storage failures blamed, for the penalty rule
    /// ([`FaultEvidence::penalized`]).
    pub faults: FaultEvidence,
    /// The node's header budget probe reads, and how each boundary's read
    /// went.
    pub capacity: CapacityEvidence,
    /// The claim rules the node's tap found broken, at the step that found
    /// each.
    pub claims: ClaimEvidence,
    /// The node's released rounds at each drain.
    pub rounds: SettlementEvidence,
    /// The node's stale prepare and apply calls.
    pub stale_calls: StaleEvidence,
    /// The node's wake facts, which its tap finds during the run.
    pub wake: WakeEvidence,
    /// The run's recovery: its source, where it is, and once the conditions
    /// held, their record. Only `header-sync/storage` recovers.
    pub recovery: RecoveryEvidence,
    /// The node's held replies and their handling, which its storage source
    /// finds during the run.
    pub hold_wake: HoldWakeEvidence,
    /// The harness's environment actions and the reactor's own exit.
    pub ends: EndEvidence,
    /// The node's stamps, which every view-side fact above takes.
    pub stamps: Stamps,
    /// When the node's reset check first applied.
    pub round_life: RoundLifeEvidence,
    /// When the node's departed-work check first applied.
    pub retirement: RetirementEvidence,
    /// When the node's stale-chain check first applied.
    pub chain_effect: ChainEffectEvidence,
    /// When the node's authority check first applied.
    pub authority: AuthorityEvidence,
    /// Each of the reactor's container peaks, and when the outstanding-work
    /// rule first applied.
    pub outstanding: OutstandingEvidence,
    /// When the node's fork-choice check first applied. A harness that
    /// doesn't run the check leaves it never applied.
    pub fork_choice: ForkChoiceEvidence,
    /// When the node's publication check first applied, and the tip it
    /// still owes. A harness that doesn't run the check leaves it never
    /// applied.
    pub publication: PublicationEvidence,
}

impl PublicationView for StorageView {
    /// [`StorageView::claims`].
    fn claims(&self) -> &ClaimEvidence {
        &self.claims
    }

    /// [`StorageView::publication`].
    fn publication(&self) -> &PublicationEvidence {
        &self.publication
    }

    /// [`StorageView::ends`].
    fn ends(&self) -> &EndEvidence {
        &self.ends
    }
}

impl ForkView for StorageView {
    /// [`StorageView::claims`].
    fn claims(&self) -> &ClaimEvidence {
        &self.claims
    }

    /// [`StorageView::fork_choice`].
    fn fork_choice(&self) -> &ForkChoiceEvidence {
        &self.fork_choice
    }
}

impl AuthorityView for StorageView {
    /// [`StorageView::claims`].
    fn claims(&self) -> &ClaimEvidence {
        &self.claims
    }

    /// [`StorageView::authority`].
    fn authority(&self) -> &AuthorityEvidence {
        &self.authority
    }
}

impl OutstandingView for StorageView {
    /// [`StorageView::claims`].
    fn claims(&self) -> &ClaimEvidence {
        &self.claims
    }

    /// [`StorageView::outstanding`].
    fn outstanding(&self) -> &OutstandingEvidence {
        &self.outstanding
    }
}

impl RetirementView for StorageView {
    /// [`StorageView::claims`].
    fn claims(&self) -> &ClaimEvidence {
        &self.claims
    }

    /// [`StorageView::ends`].
    fn ends(&self) -> &EndEvidence {
        &self.ends
    }

    /// [`StorageView::retirement`].
    fn retirement(&self) -> &RetirementEvidence {
        &self.retirement
    }
}

impl ChainView for StorageView {
    /// [`StorageView::claims`].
    fn claims(&self) -> &ClaimEvidence {
        &self.claims
    }

    /// [`StorageView::chain_effect`].
    fn chain_effect(&self) -> &ChainEffectEvidence {
        &self.chain_effect
    }
}

impl ResetView for StorageView {
    /// [`StorageView::claims`].
    fn claims(&self) -> &ClaimEvidence {
        &self.claims
    }

    /// [`StorageView::round_life`].
    fn round_life(&self) -> &RoundLifeEvidence {
        &self.round_life
    }
}

impl BudgetView for StorageView {
    /// [`StorageView::capacity`].
    fn capacity(&self) -> &CapacityEvidence {
        &self.capacity
    }

    /// [`StorageView::claims`].
    fn claims(&self) -> &ClaimEvidence {
        &self.claims
    }
}

impl TeardownView for StorageView {
    /// [`StorageView::claims`].
    fn claims(&self) -> &ClaimEvidence {
        &self.claims
    }

    /// [`StorageView::rounds`].
    fn rounds(&self) -> &SettlementEvidence {
        &self.rounds
    }
}

impl StaleView for StorageView {
    /// [`StorageView::claims`].
    fn claims(&self) -> &ClaimEvidence {
        &self.claims
    }

    /// [`StorageView::stale_calls`].
    fn stale_calls(&self) -> &StaleEvidence {
        &self.stale_calls
    }
}

impl AdoptionView for StorageView {
    /// [`StorageView::adoption`].
    fn adoption(&self) -> &AdoptionEvidence {
        &self.adoption
    }
}

impl WakeView for StorageView {
    /// [`StorageView::wake`].
    fn wake(&self) -> &WakeEvidence {
        &self.wake
    }
}

impl HoldWakeView for StorageView {
    /// [`StorageView::hold_wake`].
    fn hold_wake(&self) -> &HoldWakeEvidence {
        &self.hold_wake
    }

    /// [`StorageView::ends`].
    fn ends(&self) -> &EndEvidence {
        &self.ends
    }
}

impl HonestPeers for StorageView {
    /// True if `index` is in [`StorageView::honest_peers`].
    fn is_honest(&self, index: usize) -> bool {
        self.honest_peers.contains(&index)
    }
}

impl HasView for Storage {
    type View = StorageView;

    /// The node's port, the dispatch guard's flag, the node's adoption facts
    /// and its sessions' lifetimes.
    fn view(&self) -> StorageView {
        StorageView {
            port: self.port.clone(),
            direct_dispatch_broken: self.dispatch.get(),
            honest_peers: Rc::default(),
            adoption: self.adoption.clone(),
            sessions: self.topology.sessions(),
            faults: self.faults.clone(),
            capacity: self.capacity.clone(),
            claims: self.claims.clone(),
            rounds: self.rounds.clone(),
            stale_calls: self.stale_calls.clone(),
            wake: self.wake.clone(),
            hold_wake: self.hold_wake.clone(),
            ends: self.ends.clone(),
            stamps: self.stamps.clone(),
            round_life: self.round_life.clone(),
            retirement: self.retirement.clone(),
            chain_effect: self.chain_effect.clone(),
            authority: self.authority.clone(),
            outstanding: self.outstanding.clone(),
            recovery: self.recovery.clone(),
            fork_choice: self.fork_choice.clone(),
            publication: self.publication.clone(),
        }
    }

    /// Records the run's end in the node's adoption, fault, round and
    /// publication evidence, for the tip, penalty and teardown rules.
    fn ended(&self, end: &RunEnd) {
        self.adoption.end(end);
        self.faults.end(end);
        self.rounds.end(end);
        self.outstanding.end(end);
        self.retirement.end(end);
        self.publication
            .end(super::header_node::adoption::settled(end));
    }

    /// [`fault_end_violations`], [`teardown_end_violations`],
    /// [`outstanding_end_violations`], [`retirement_end_violations`] and
    /// [`publication_end_violations`].
    fn end_violations(&self, log: &ObservationLog) -> Vec<EndViolation> {
        let mut found = fault_end_violations(&self.faults, log);
        found.extend(teardown_end_violations(&self.rounds));
        found.extend(outstanding_end_violations(&self.outstanding));
        found.extend(retirement_end_violations(&self.retirement));
        found.extend(publication_end_violations(&self.publication));
        found
    }

    /// Whether a `Shutdown` step was picked, which begins the node's stop.
    fn stopping(&self) -> bool {
        self.topology.shutdown_picked()
    }

    /// The node's stamps after `after` ([`Stamps::after`]), from every
    /// view-side store its tap and harness keep.
    fn stamps_after(&self, after: Option<u64>) -> Option<Vec<u64>> {
        Some(self.stamps.after(after))
    }
}

impl Subsystem for Storage {
    const NAME: &'static str = "header-sync/storage";
    const GENERATOR: u32 = grammar::GENERATOR;
    const SCENARIO_BYTES: usize = grammar::LAYOUT.prefix_len();
    type Scenario = StorageScenario;
    type Prepared = Ports;

    /// [`storage_config`], with [`RECOVERY_BUDGET`] for a scenario with a
    /// `Recover` step.
    fn config(scenario: &StorageScenario) -> RunConfig {
        match recovery_source(scenario) {
            Some(_) => RunConfig {
                budget: RECOVERY_BUDGET,
                ..storage_config()
            },
            None => storage_config(),
        }
    }

    /// Commits each actor's chain to its port.
    ///
    /// # Panics
    ///
    /// If a hold or control step waits for a trigger the node never records
    /// ([`policy::undriven`]), or a `Recover` step is out of place: only
    /// closes may follow it, and it follows no `Shutdown`, and no churn or
    /// flood, whose tasks keep acting after their step. A close after it is
    /// hdr-13's loss path, which only a typed scenario takes: the control
    /// source offers it from the `recovered` pick on, and the grammar can't
    /// encode it. Also if the policy declares a disruption with no `Recover`
    /// step to disrupt.
    fn prepare(scenario: &StorageScenario) -> Ports {
        let triggers = scenario
            .policy
            .holds
            .iter()
            .map(|hold| &hold.until)
            .chain(scenario.policy.reply_holds.iter().map(|hold| &hold.until))
            .chain(
                scenario
                    .controls
                    .iter()
                    .filter_map(|step| step.when.as_ref()),
            );
        if let Some(why) = triggers.filter_map(policy::undriven).next() {
            panic!("{why}");
        }
        if let Some(hold) = scenario
            .policy
            .reply_holds
            .iter()
            .find(|hold| hold.select.kind.is_some_and(|kind| kind != OpKind::Apply))
        {
            panic!("a reply hold picks only applies, but {hold:?} names another kind");
        }
        let released = |trigger: &Trigger| trigger.find(&|t| *t == Trigger::Released).is_some();
        if scenario
            .policy
            .reply_holds
            .iter()
            .map(|hold| &hold.until)
            .chain(
                scenario
                    .controls
                    .iter()
                    .filter_map(|step| step.when.as_ref()),
            )
            .any(released)
        {
            panic!("`released` holds only in a hold's own trigger");
        }
        let holds = scenario.policy.holds.len();
        if let Some(step) = scenario
            .controls
            .iter()
            .find(|step| matches!(step.action, ControlAction::Release { hold } if hold >= holds))
        {
            panic!("{step:?} releases a hold the policy's {holds} holds don't have");
        }
        if let Some(at) = scenario
            .controls
            .iter()
            .position(|step| step.action == ControlAction::Recover)
        {
            if let Some(after) = scenario.controls[at + 1..]
                .iter()
                .find(|step| step.action != ControlAction::Close)
            {
                panic!("only a close follows a recover, but {after:?} does");
            }
            if let Some(before) = scenario.controls[..at].iter().find(|step| {
                matches!(
                    step.action,
                    ControlAction::Shutdown | ControlAction::Churn | ControlAction::Flood
                )
            }) {
                panic!("a recover can't follow {before:?}");
            }
        }
        if recovery_source(scenario).is_none() && !scenario.policy.disruptions.is_empty() {
            panic!(
                "a disruption takes something away from a recovery, but {:?} has no recover",
                scenario.policy.disruptions
            );
        }
        let fixture = scenario.weights.fixture();
        let node = Arc::new(ControlledPort::new(&fixture));
        // Read just before, so a header the preload defers is deferred at
        // this clock too, short of a deadline inside the preload itself.
        let preloaded_at = zakura_rt::time::utc_now().into();
        preload(node.engine(), &fixture, &scenario.node);
        let mirrors = scenario
            .mirrors
            .iter()
            .map(|mirror| {
                let port = Arc::new(EnginePort::new(&fixture));
                preload(&port, &fixture, &mirror.chain);
                port
            })
            .collect();
        Ports {
            fixture,
            node,
            preloaded_at,
            mirrors,
            sessions: SessionEvidence::default(),
            reservations: ReservationEvidence::default(),
            capacity: CapacityEvidence::default(),
            rounds: SettlementEvidence::default(),
            stale_calls: StaleEvidence::default(),
            wake: WakeEvidence::default(),
            hold_wake: HoldWakeEvidence::default(),
            ends: EndEvidence::default(),
            round_life: RoundLifeEvidence::default(),
            retirement: RetirementEvidence::default(),
            chain_effect: ChainEffectEvidence::default(),
            authority: AuthorityEvidence::default(),
            outstanding: OutstandingEvidence::default(),
            recovery: if recovery_source(scenario).is_some() {
                RecoveryEvidence::bounded(&scenario.policy.disruptions)
            } else {
                RecoveryEvidence::default()
            },
            delayed_page: None,
            fork_choice: ForkChoiceEvidence::default(),
            // A flood or churn scenario's adoption is the watch-flood
            // properties' to judge, so it keeps no adoption facts there.
            publication: PublicationEvidence::armed(Arming {
                armed: !scenario
                    .controls
                    .iter()
                    .any(|step| matches!(step.action, ControlAction::Flood | ControlAction::Churn)),
                status: false,
            }),
            finishes: recovery_source(scenario).is_some(),
        }
    }

    /// Spawns the node on its controlled port under the scenario's policy,
    /// with its reactor tapped, then each mirror, links each mirror to the
    /// node once, and adds the `Control` source. Only the node is tapped,
    /// and its tap learns of a `Shutdown` step before the node stops. The tap
    /// and the links keep the node's session view in [`Ports::sessions`].
    fn build(sim: &mut Sim, scenario: &StorageScenario, ports: Ports) -> Self {
        let dispatch = DispatchFlag::default();
        let effects = Effects::default();
        let recovery = ports.recovery.clone();
        let storage = PolicySource::new(
            sim,
            ports.node.clone(),
            scenario.policy.clone(),
            effects.clone(),
        )
        .with_recovery(recovery.clone());
        let (observer, tap) = ReactorTap::with_evidence(
            sim,
            &ports.node,
            "node",
            ports.sessions.clone(),
            ports.reservations.clone(),
            ports.capacity.clone(),
            ports.rounds.clone(),
            ports.stale_calls.clone(),
            ports.ends.clone(),
        );
        tap.watch_wakes(ports.wake.clone());
        let adoption = tap.adoption();
        let claims = tap.claims();
        let ends = tap.ends();
        let stamps = tap.stamps();
        tap.watch_rounds(ports.round_life.clone());
        tap.watch_retirement(ports.retirement.clone());
        tap.watch_chain(ports.chain_effect.clone());
        let branches: Vec<u8> = scenario
            .mirrors
            .iter()
            .flat_map(|mirror| mirror.chain.branches())
            .chain(scenario.node.branches())
            .collect();
        tap.watch_authority(
            ports.authority.clone(),
            ports.fixture.clone(),
            branches.clone(),
            [],
        );
        tap.watch_outstanding(
            ports.outstanding.clone(),
            storage_peer_limit(),
            storage_departed_limit(),
        );
        tap.watch_fork_choice(
            ports.fork_choice.clone(),
            ports.fixture.clone(),
            branches,
            ports.preloaded_at,
            fork_fixture(scenario),
        );
        tap.watch_publication(ports.publication.clone());
        sim.on_frozen({
            let publication = ports.publication.clone();
            move || publication.freeze()
        });

        let shutdown = tap.shutdown();
        let attributions = storage.attributions();
        let cooldowns = tap.cooldowns();
        let mut storage = storage.with_tap(tap);
        if ports.hold_wake.is_armed() {
            storage = storage.with_hold_wakes(ports.hold_wake.clone());
        }
        let predicates = storage.predicates();
        sim.add_source(Box::new(storage));
        let mut nodes = vec![HeaderNode::spawn(
            sim,
            "node",
            1,
            ports.node.clone(),
            &ports.fixture,
            scenario.node.tip(&ports.fixture),
            dispatch.clone(),
            Some(observer),
        )];
        for (index, ((spec, port), seed)) in scenario
            .mirrors
            .iter()
            .zip(&ports.mirrors)
            .zip(2..)
            .enumerate()
        {
            let name = format!("mirror-{index}");
            let observer: Arc<dyn HeaderSyncObserver> = Arc::new(MirrorObserver {
                decisions: ports.fork_choice.mirror(&name, nodes[0].id.clone()),
                refusals: ports.publication.mirror(&name, nodes[0].id.clone()),
            });
            let mut config = ZakuraHeaderSyncConfig::default();
            if let Some(limit) = spec.max_headers_per_response {
                config.max_headers_per_response = limit;
            }
            nodes.push(HeaderNode::spawn_configured(
                sim,
                &name,
                seed,
                port.clone(),
                &ports.fixture,
                spec.chain.tip(&ports.fixture),
                dispatch.clone(),
                Some(observer),
                config,
            ));
        }
        let cooldown = nodes[0].startup.config.unproductive_peer_cooldown;
        let faults = FaultEvidence::new(attributions, cooldowns.clone(), cooldown);
        let names: Vec<_> = nodes
            .iter()
            .map(|node| (node.id.clone(), node.name.clone()))
            .collect();
        ports.publication.name_peers(names.clone());
        let topology = Rc::new(
            Topology::new(nodes, effects.clone())
                .with_tap(shutdown)
                .with_sessions(ports.sessions.clone())
                .with_delayed_page(ports.delayed_page.clone()),
        );
        let mut mirror_links = Vec::new();
        for (index, spec) in (1..).zip(&scenario.mirrors) {
            let link = match spec.dialer {
                Dialer::Mirror => topology.link(index, 0),
                Dialer::Node => topology.link(0, index),
            };
            mirror_links.push((link.conn_id, link.peer));
        }
        ports
            .fork_choice
            .watch_link_ends(effects.ended.clone(), mirror_links);
        let control = Control::new(
            sim,
            topology.clone(),
            ports.fixture.clone(),
            scenario.controls.clone(),
            predicates,
        )
        .with_port(ports.node.clone())
        .with_adoption(adoption.clone())
        .with_recovery(
            RecoveryInputs {
                node: ports.node.clone(),
                mirrors: (0..)
                    .zip(&ports.mirrors)
                    .map(|(index, port)| (format!("mirror-{index}"), port.clone()))
                    .collect(),
                sessions: ports.sessions.clone(),
                cooldowns,
                ends: ports.ends.clone(),
                cooldown,
                names,
            },
            recovery.clone(),
        );
        sim.add_source(Box::new(control));
        sim.add_source(Box::new(RecoveryDeadline {
            recovery: recovery.clone(),
            adoption: adoption.clone(),
            ends: ends.clone(),
        }));
        if ports.finishes {
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
            sim.add_source(Box::new(RecoveryFinish {
                recovery: recovery.clone(),
                adoption: adoption.clone(),
                ends: ends.clone(),
                topology: topology.clone(),
                to_task,
                step: 0,
                done: false,
            }));
        }
        Storage {
            port: ports.node,
            dispatch,
            adoption,
            capacity: ports.capacity,
            claims,
            rounds: ports.rounds,
            stale_calls: ports.stale_calls,
            wake: ports.wake,
            hold_wake: ports.hold_wake,
            round_life: ports.round_life,
            retirement: ports.retirement,
            chain_effect: ports.chain_effect,
            authority: ports.authority,
            outstanding: ports.outstanding,
            fork_choice: ports.fork_choice,
            publication: ports.publication,
            ends,
            stamps,
            faults,
            topology,
            recovery,
        }
    }

    /// [`storage_properties`], then
    /// `header-sync/bounded-adversary-progress`, which applies after a
    /// `Recover` step, so the fuzzer checks it on every input with one.
    fn properties() -> Vec<Property<Self>> {
        let mut properties = storage_properties();
        properties.push(bounded_adversary_progress());
        properties
    }

    /// [`storage_sources`].
    fn sources() -> Sources {
        storage_sources()
    }

    /// A scenario with a `Recover` step declares hdr-13's five assumptions
    /// ([`recovery_assumptions`]), none of which holds before the
    /// `recovered` pick ([`RecoveryEvidence::holds`]), and
    /// `BoundedAdversary` ([`recovered_bound`]). Others declare none.
    fn assumptions(scenario: &StorageScenario) -> Assumptions<Self> {
        let Some(source) = recovery_source(scenario) else {
            return Assumptions::none();
        };
        let mut declared = vec![recovered_bound()];
        declared.extend(recovery_assumptions(&source));
        Assumptions::declare(declared).holds_when(Storage::recovery_holds)
    }

    /// Whether [`grammar::decode`] reads `prefix` rather than rejecting its
    /// version byte.
    fn accepts(prefix: &[u8]) -> bool {
        grammar::decode(prefix).is_ok()
    }

    /// [`grammar::migrate`], for a generator-2 input.
    fn migrate(source: &[u8], generator: u32) -> Result<Vec<u8>, String> {
        match generator {
            2 => grammar::migrate(source),
            _ => Err(format!(
                "{} migrates generator 2 only, not {generator}",
                Self::NAME
            )),
        }
    }

    /// The scenario [`grammar::decode`] reads from `prefix`. A prefix
    /// written for another generator draws the default scenario.
    ///
    /// # Panics
    ///
    /// If `prefix` isn't [`Subsystem::SCENARIO_BYTES`] long, which breaks
    /// `draw`'s contract.
    fn draw(prefix: &[u8]) -> StorageScenario {
        match grammar::decode(prefix) {
            Ok(scenario) => scenario,
            Err(Rejection::Version { .. }) => StorageScenario::default(),
            Err(rejection @ Rejection::Length { .. }) => {
                panic!("`draw` takes exactly SCENARIO_BYTES bytes, but {rejection}")
            }
        }
    }
}

/// The `header-sync/wire` scenario.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WireScenario {
    /// The network: its seed, links and loss gate.
    #[serde(default)]
    pub net: NetScenario,
    /// The chain the node holds when the run starts.
    pub node: Chain,
    /// The scripted peers, named `peer-<i>` by index.
    pub peers: Vec<PeerSpec>,
    /// How the run completes the node's storage operations.
    pub policy: Policy,
}

impl WireScenario {
    /// The peers the scenario marks `HonestPeer`, by index: each that
    /// [`PeerSpec::is_honest`].
    pub fn honest_peers(&self) -> BTreeSet<usize> {
        (0..)
            .zip(&self.peers)
            .filter(|(_, peer)| peer.is_honest())
            .map(|(index, _)| index)
            .collect()
    }
}

impl Default for WireScenario {
    /// The node at genesis, dialed by one peer that offers header sync
    /// alone, under the converging policy.
    fn default() -> Self {
        WireScenario {
            net: NetScenario::default(),
            node: Chain::default(),
            peers: vec![PeerSpec::dialing()],
            policy: Policy::converging(),
        }
    }
}

/// The wire scenario's storage: the fixture, the node's controlled port
/// with its chain committed, and each honest peer's chain. Cloning shares
/// them, so a test can read them after a run.
#[derive(Clone)]
pub struct WirePorts {
    /// The fixture the node's chain comes from.
    pub fixture: HeaderFixture,
    /// The node's controlled port.
    pub node: Arc<ControlledPort>,
    /// Each peer's chain, by index, if it runs the honest script.
    pub chains: Vec<Option<Arc<HonestChain>>>,
    /// What the node's reactor was started with, once the run builds it.
    pub startup: Rc<RefCell<Option<StartupFacts>>>,
    /// The node's adoption facts, which its tap fills during the run, once
    /// the run builds it.
    pub adoption: Rc<RefCell<Option<AdoptionEvidence>>>,
    /// The node's session view, which its reactor and transport taps fill
    /// during the run, joined to own-9's facts. One prepared `WirePorts`
    /// serves one run.
    pub sessions: SessionEvidence,
    /// The peers' records the identity map hasn't placed yet.
    records: PeerRecords,
    /// The peers that closed their own connection.
    closed: PeerCloses,
    /// The node's header capacity accounting, which its tap fills during the
    /// run.
    pub reservations: ReservationEvidence,
    /// The node's resource probe reads, which its tap checks and keeps
    /// during the run.
    pub capacity: CapacityEvidence,
    /// The node's released rounds at each drain, which its tap keeps during
    /// the run.
    pub rounds: SettlementEvidence,
    /// The node's stale prepare and apply calls, which its tap finds during
    /// the run.
    pub stale_calls: StaleEvidence,
    /// The node's wake facts, which its tap finds during the run.
    pub wake: WakeEvidence,
    /// The node's held replies and their handling. Inert here, since this
    /// harness's storage source keeps no hold-wake check.
    pub hold_wake: HoldWakeEvidence,
    /// The node's end facts, which its tap records during the run. It holds
    /// the node's stamps.
    pub ends: EndEvidence,
    /// When the node's reset check first applied, which its tap finds
    /// during the run.
    pub round_life: RoundLifeEvidence,
    /// When the node's departed-work check first applied, which its tap
    /// finds during the run.
    pub retirement: RetirementEvidence,
    /// When the node's stale-chain check first applied, which its tap
    /// finds during the run.
    pub chain_effect: ChainEffectEvidence,
    /// When the node's authority check first applied, which its tap finds
    /// during the run.
    pub authority: AuthorityEvidence,
}

impl WirePorts {
    /// The node's selected header tip, read from its engine: the end-of-run
    /// check that sync converged, as M4's tests read it.
    pub fn selected_tip(&self) -> Frontier {
        self.node.engine().snapshot().frontiers.header_best
    }

    /// How many peer records are still waiting for their stream's identity
    /// rows. A run ends with some only if a peer wrote on a stream the node
    /// hasn't admitted yet, so a test whose streams all reach admission
    /// requires none.
    pub fn unplaced_records(&self) -> usize {
        self.records.borrow().len()
    }
}

/// The node under test, named `node`, on a controlled port behind its real
/// transport on simulated QUIC, with scripted peers. It's checked by
/// [`wire_properties`].
pub struct HeaderWire {
    port: Arc<ControlledPort>,
    dispatch: DispatchFlag,
    /// The peers the scenario marks `HonestPeer`.
    honest: Rc<BTreeSet<usize>>,
    /// The node's adoption facts, from its tap.
    adoption: AdoptionEvidence,
    /// The node's probe reads, from its tap.
    capacity: CapacityEvidence,
    /// The claim rules the node's tap found broken.
    claims: ClaimEvidence,
    /// The node's released rounds, from its tap.
    rounds: SettlementEvidence,
    /// The node's stale prepare and apply calls, from its tap.
    stale_calls: StaleEvidence,
    /// The node's wake facts, from its tap.
    wake: WakeEvidence,
    /// The node's held replies and their handling, from its storage source.
    hold_wake: HoldWakeEvidence,
    /// The harness's environment actions and the reactor's own exit, from
    /// its tap.
    ends: EndEvidence,
    /// The node's stamps, from its tap.
    stamps: Stamps,
    /// When the node's reset check first applied, from its tap.
    round_life: RoundLifeEvidence,
    /// When the node's departed-work check first applied, from its tap.
    retirement: RetirementEvidence,
    /// When the node's stale-chain check first applied, from its tap.
    chain_effect: ChainEffectEvidence,
    /// When the node's authority check first applied, from its tap.
    authority: AuthorityEvidence,
    /// Whom the node's storage failures blamed.
    faults: FaultEvidence,
    /// The node's session view, joined to own-9's facts.
    sessions: SessionEvidence,
    /// The peers that closed their own connection.
    closed: PeerCloses,
    /// The node and the peers' state. They live until the run ends, so no
    /// task sees its inputs close.
    _keep: Box<dyn Any>,
}

impl HasView for HeaderWire {
    type View = StorageView;

    /// The node's port, the dispatch guard's flag, the honest peers and the
    /// node's adoption facts. Its sessions run over the network, so the view
    /// gives no lifetimes.
    fn view(&self) -> StorageView {
        StorageView {
            port: self.port.clone(),
            direct_dispatch_broken: self.dispatch.get(),
            honest_peers: self.honest.clone(),
            adoption: self.adoption.clone(),
            sessions: Vec::new(),
            faults: self.faults.clone(),
            capacity: self.capacity.clone(),
            claims: self.claims.clone(),
            rounds: self.rounds.clone(),
            stale_calls: self.stale_calls.clone(),
            wake: self.wake.clone(),
            hold_wake: self.hold_wake.clone(),
            ends: self.ends.clone(),
            stamps: self.stamps.clone(),
            round_life: self.round_life.clone(),
            retirement: self.retirement.clone(),
            chain_effect: self.chain_effect.clone(),
            authority: self.authority.clone(),
            // The wire harness doesn't register outstanding-work-bounded.
            outstanding: OutstandingEvidence::default(),
            recovery: RecoveryEvidence::default(),
            // The wire harness doesn't judge fork choice or publication.
            fork_choice: ForkChoiceEvidence::default(),
            publication: PublicationEvidence::default(),
        }
    }

    /// Records the run's end in the node's adoption, fault and round
    /// evidence and its session view, for the tip, penalty and teardown
    /// rules and the own-9 end check.
    fn ended(&self, end: &RunEnd) {
        self.adoption.end(end);
        self.faults.end(end);
        self.sessions.end(end);
        self.rounds.end(end);
        self.retirement.end(end);
    }

    /// [`fault_end_violations`], [`teardown_end_violations`] and
    /// [`retirement_end_violations`].
    fn end_violations(&self, log: &ObservationLog) -> Vec<EndViolation> {
        let mut found = fault_end_violations(&self.faults, log);
        found.extend(teardown_end_violations(&self.rounds));
        found.extend(retirement_end_violations(&self.retirement));
        found
    }

    /// The node's stamps after `after` ([`Stamps::after`]), from every
    /// view-side store its tap keeps. Its adoption facts answer
    /// `header-sync/honest-progress`'s obligation.
    fn stamps_after(&self, after: Option<u64>) -> Option<Vec<u64>> {
        Some(self.stamps.after(after))
    }
}

impl Subsystem for HeaderWire {
    const NAME: &'static str = "header-sync/wire";
    // 3: scenario tag 0xFE 0x10 picks the lower-locator saved Plan.
    const GENERATOR: u32 = 3;
    const SCENARIO_BYTES: usize = 2;
    type Scenario = WireScenario;
    type Prepared = WirePorts;

    /// [`wire_config`], with twice its budget for a scenario with status
    /// bursts or an over-credit objects page. Emptying a `Status` bucket
    /// takes about 600 frames, and the node about 40 steps to read each. The
    /// node must read the whole 45 KB page before it can count objects, so
    /// with every datagram delivered at once, that run spends up to about
    /// 3,500 steps at 0 ns, past the livelock limit of a tenth of the
    /// budget.
    fn config(scenario: &WireScenario) -> RunConfig {
        let burst = scenario.peers.iter().any(|peer| {
            [Act::StatusBurst, Act::OverCreditObjects]
                .into_iter()
                .any(|act| peer.script.contains(&PeerAction::Act { act }))
        });
        let config = wire_config();
        RunConfig {
            budget: if burst {
                2 * config.budget
            } else {
                config.budget
            },
            ..config
        }
    }

    /// Commits the node's chain to its port, and builds each honest peer's
    /// chain.
    fn prepare(scenario: &WireScenario) -> WirePorts {
        let fixture = HeaderFixture::regtest();
        let node = Arc::new(ControlledPort::new(&fixture));
        preload(node.engine(), &fixture, &scenario.node);
        let chains = scenario
            .peers
            .iter()
            .map(|peer| {
                assert!(
                    peer.adversary.is_none() || peer.honest.is_some(),
                    "an adversary needs an honest chain"
                );
                assert!(
                    !peer.invalid_tip || peer.honest.is_some(),
                    "an invalid tip needs an honest chain"
                );
                peer.honest.as_ref().map(|honest| {
                    let start = honest.subscribe.as_ref().map_or(0, |spec| spec.start);
                    assert!(
                        start <= honest.chain,
                        "an honest peer subscribes from a height on its own chain"
                    );
                    Arc::new(if peer.invalid_tip {
                        HonestChain::with_invalid_tip(&fixture, honest.chain)
                    } else {
                        HonestChain::new(&fixture, honest.chain)
                    })
                })
            })
            .collect();
        WirePorts {
            fixture,
            node,
            chains,
            startup: Rc::default(),
            adoption: Rc::default(),
            sessions: SessionEvidence::default(),
            records: PeerRecords::default(),
            closed: PeerCloses::default(),
            reservations: ReservationEvidence::default(),
            capacity: CapacityEvidence::default(),
            rounds: SettlementEvidence::default(),
            stale_calls: StaleEvidence::default(),
            wake: WakeEvidence::default(),
            hold_wake: HoldWakeEvidence::default(),
            ends: EndEvidence::default(),
            round_life: RoundLifeEvidence::default(),
            retirement: RetirementEvidence::default(),
            chain_effect: ChainEffectEvidence::default(),
            authority: AuthorityEvidence::default(),
        }
    }

    /// Adds the network, then spawns the node as `header-sync/storage` does,
    /// with its reactor tapped, behind its transport, which its own tap
    /// reports. Both taps feed the node's session view in
    /// [`WirePorts::sessions`], which joins the reactor's sessions to
    /// own-9's facts. Then spawns each peer, and a dial for each peer the
    /// node dials, and adds the peers' action source.
    fn build(sim: &mut Sim, scenario: &WireScenario, ports: WirePorts) -> Self {
        let net = scenario.net.net();
        sim.add_source(Box::new(net.clone()));
        let dispatch = DispatchFlag::default();
        let storage = PolicySource::new(
            sim,
            ports.node.clone(),
            scenario.policy.clone(),
            Effects::default(),
        );
        let names: Vec<String> = (0..scenario.peers.len())
            .map(|index| format!("peer-{index}"))
            .collect();
        ports.sessions.expect_transport(
            (0..)
                .zip(&names)
                .map(|(index, name)| (peer_id(scenario.net.seed, index), name.clone()))
                .collect(),
        );
        let (observer, tap) = ReactorTap::with_evidence(
            sim,
            &ports.node,
            "node",
            ports.sessions.clone(),
            ports.reservations.clone(),
            ports.capacity.clone(),
            ports.rounds.clone(),
            ports.stale_calls.clone(),
            ports.ends.clone(),
        );
        let adoption = tap.adoption();
        let claims = tap.claims();
        let ends = tap.ends();
        let stamps = tap.stamps();
        tap.watch_rounds(ports.round_life.clone());
        tap.watch_retirement(ports.retirement.clone());
        tap.watch_chain(ports.chain_effect.clone());
        let declared = ports
            .chains
            .iter()
            .flatten()
            .filter_map(|chain| chain.invalid_link());
        // Every peer serves an honest chain on branch 0.
        tap.watch_authority(
            ports.authority.clone(),
            ports.fixture.clone(),
            scenario.node.branches(),
            declared,
        );

        let attributions = storage.attributions();
        let cooldowns = tap.cooldowns();
        sim.add_source(Box::new(storage.with_tap(tap)));
        let header = HeaderNode::spawn(
            sim,
            "node",
            1,
            ports.node.clone(),
            &ports.fixture,
            scenario.node.tip(&ports.fixture),
            dispatch.clone(),
            Some(observer),
        );
        *ports.startup.borrow_mut() = Some(header.startup.clone());
        *ports.adoption.borrow_mut() = Some(adoption.clone());
        let faults = FaultEvidence::new(
            attributions,
            cooldowns,
            header.startup.config.unproductive_peer_cooldown,
        );
        let (observer, tap) = TransportTap::with_sessions(
            sim,
            "node",
            net.clone(),
            names.clone(),
            Some(ports.sessions.clone()),
        );
        let dials = tap.dials();
        let shared_ids = tap.shared_ids();
        sim.add_source(Box::new(tap));
        let node = WireNode::spawn(
            sim,
            header,
            ports.fixture.network(),
            net.sim(),
            scenario.net.seed,
            observer,
            dials,
        );
        let failure = PeerFailure::default();
        let mut control = PeerControl::new(
            sim,
            "node",
            failure.clone(),
            ports.records.clone(),
            shared_ids,
        );
        for ((index, spec), name) in scenario.peers.iter().enumerate().zip(names) {
            let (readiness, commands) =
                control.add(&name, spec.script.clone(), spec.adversary.is_some());
            let (listening, listened) = zakura_rt::sync::watch::channel(false);
            let task = PeerTask {
                name,
                index,
                spec: spec.clone(),
                seed: scenario.net.seed,
                net: net.sim(),
                node: zakura_quic::NodeAddr::with_addrs(
                    zakura_quic::sim::SeededInputs::node_key(scenario.net.seed, 0).public(),
                    [NODE_ADDR],
                ),
                commands,
                readiness,
                failure: failure.clone(),
                listening,
                chain: ports.chains[index].clone(),
                records: ports.records.clone(),
                order: net.order(),
                closed: ports.closed.clone(),
                taken: 0,
            };
            sim.scheduler().spawn_root("peer", task.run());
            if spec.dialer == PeerDialer::Node {
                node.dial(sim, index, listened);
            }
        }
        sim.add_source(Box::new(control));
        HeaderWire {
            port: ports.node,
            dispatch,
            honest: Rc::new(scenario.honest_peers()),
            adoption,
            capacity: ports.capacity,
            claims,
            rounds: ports.rounds,
            stale_calls: ports.stale_calls,
            wake: ports.wake,
            hold_wake: ports.hold_wake,
            round_life: ports.round_life,
            retirement: ports.retirement,
            chain_effect: ports.chain_effect,
            authority: ports.authority,
            ends,
            stamps,
            faults,
            sessions: ports.sessions,
            closed: ports.closed,
            _keep: Box::new(node),
        }
    }

    /// [`wire_properties`].
    fn properties() -> Vec<Property<Self>> {
        wire_properties()
    }

    /// [`storage_sources`], plus the network's datagrams, the peers'
    /// frames and the node's closes.
    fn sources() -> Sources {
        Sources::observed(&[
            Source::Schedule,
            Source::Storage,
            Source::View,
            Source::Datagrams,
            Source::Frames,
            Source::Closes,
        ])
    }

    /// For a scenario with an adversary, `BoundedAdversary` at
    /// [`ADVERSARY_RATE`] actions per virtual second, the budget
    /// [`PeerControl`] holds every adversary to (M5-Q9). A scenario of
    /// honest and scripted peers has no hostile peer to bound. For a
    /// scenario with honest peers, `HonestPeer` with their names
    /// ([`WireScenario::honest_peers`]). For the [`progress_fixture`],
    /// also `BoundedDelivery` and `BoundedStorage`, which its link and
    /// storage policy enforce, and `StableMembership`, which holds while
    /// `peer-0`'s header session is the node's only membership
    /// ([`wire_holds`]).
    fn assumptions(scenario: &WireScenario) -> Assumptions<Self> {
        let mut declared = Vec::new();
        if scenario.peers.iter().any(|peer| peer.adversary.is_some()) {
            declared.push(AssumeRecord {
                assume: Assume::BoundedAdversary,
                params: serde_json::json!({ "actions_per_second": ADVERSARY_RATE }),
            });
        }
        let honest = scenario.honest_peers();
        if !honest.is_empty() {
            let names: Vec<String> = honest.iter().map(|index| format!("peer-{index}")).collect();
            declared.push(AssumeRecord {
                assume: Assume::HonestPeer,
                params: serde_json::json!({ "peers": names }),
            });
        }
        if progress_fixture(scenario) {
            declared.extend([
                AssumeRecord {
                    assume: Assume::BoundedDelivery,
                    params: serde_json::json!({
                        "max_delay_ms": PROGRESS_LINK.max_delay_ms,
                        "loss_budget": PROGRESS_LINK.loss_budget,
                    }),
                },
                AssumeRecord {
                    assume: Assume::BoundedStorage,
                    params: serde_json::json!({ "bound_ms": PROGRESS_STORAGE_BOUND_MS }),
                },
                AssumeRecord {
                    assume: Assume::StableMembership,
                    params: serde_json::json!({ "peer": "peer-0" }),
                },
            ]);
        }
        Assumptions::declare(declared).holds_when(wire_holds)
    }

    /// The saved Plan's scenario `prefix` tags ([`SavedPlan::tag`]), or
    /// the default scenario.
    fn draw(prefix: &[u8]) -> WireScenario {
        SavedPlan::from_tag(prefix).map_or_else(WireScenario::default, SavedPlan::scenario)
    }
}

/// The progress fixture's link, in both directions: 0 to 10 ms, in order,
/// with no loss (properties.md, `honest-progress`).
pub const PROGRESS_LINK: super::LinkSpec = super::LinkSpec {
    min_delay_ms: 0,
    max_delay_ms: 10,
    reorder: false,
    loss_budget: 0,
};

/// How long the progress fixture's storage may take over each operation.
pub const PROGRESS_STORAGE_BOUND_MS: u64 = 10;

/// Whether `scenario` is `header-sync/honest-progress`'s fixed fixture, the
/// only scenario that declares its assumptions. The node holds one chain
/// with no fork. One honest peer dials it and holds the same chain plus one
/// header, so it serves exactly that successor, with no subscription of its
/// own and no script beyond closing its connection. Every link is
/// [`PROGRESS_LINK`], and storage succeeds within
/// [`PROGRESS_STORAGE_BOUND_MS`] with no holds.
pub fn progress_fixture(scenario: &WireScenario) -> bool {
    let [peer] = &scenario.peers[..] else {
        return false;
    };
    let successor = scenario.node.main.checked_add(1);
    scenario.node.fork.is_none()
        && peer.is_honest()
        && peer.dialer == PeerDialer::Peer
        && peer
            .honest
            .as_ref()
            .is_some_and(|honest| honest.subscribe.is_none() && Some(honest.chain) == successor)
        && peer
            .script
            .iter()
            .all(|action| matches!(action, PeerAction::Close | PeerAction::Disconnect))
        && scenario.net.default_link == PROGRESS_LINK
        && scenario
            .net
            .links
            .iter()
            .all(|link| link.model == PROGRESS_LINK)
        && scenario.policy
            == Policy {
                base: Base::SuccessOnly,
                bound_ms: Some(PROGRESS_STORAGE_BOUND_MS),
                holds: Vec::new(),
                reply_holds: Vec::new(),
                disruptions: Vec::new(),
            }
}

/// Whether `assume` holds after a step of `wire`: `StableMembership` while
/// `peer-0` is the node's only membership
/// ([`SessionEvidence::stable_membership`]) and hasn't closed its own
/// connection, and every other assumption always, since the scenario
/// enforces what it declares. Only the environment breaks it
/// ([`EndedBy`](crate::property::EndedBy)): `peer-0`'s scripted close, or
/// another peer. A node that drops `peer-0` leaves it holding.
fn wire_holds(wire: &HeaderWire, assume: Assume) -> bool {
    match assume {
        Assume::StableMembership => {
            wire.sessions.stable_membership("peer-0") && !wire.closed.borrow().contains(&0)
        }
        _ => true,
    }
}

/// The wire run's config: the default scheduler, and room for QUIC
/// handshakes, stream setup and a few scripted actions.
pub fn wire_config() -> RunConfig {
    RunConfig {
        scheduler: SchedulerConfig::default(),
        budget: 30_000,
        // The longest drain measured on the saved Plans, which runs the QUIC
        // timers to the harness timer cap, is about 4,700 steps, and more
        // peers take more.
        drain_bound: 50_000,
        ..RunConfig::default()
    }
}

/// Offers a time boundary one millisecond past an owed recovery's deadline
/// ([`RecoveryEvidence::progress`]), so the clock reaches it even when no
/// other timer would, and the registry can expire
/// `header-sync/bounded-adversary-progress`'s obligation (the hdr-9 to
/// hdr-13 plan's acceptance: deadlines reach the clock). It offers no
/// action.
struct RecoveryDeadline {
    recovery: RecoveryEvidence,
    adoption: AdoptionEvidence,
    ends: EndEvidence,
}

impl ActionSource for RecoveryDeadline {
    /// None: it only stops the clock.
    fn enabled(&self, _now: SimInstant, _out: &mut Vec<(Identity, u64)>) {}

    /// One millisecond past the owed recovery's deadline, while that's still
    /// ahead of `now`.
    fn boundaries(&self, now: SimInstant, out: &mut Vec<Boundary>) {
        let RecoveryProgress::Owed { since } = self.recovery.progress(&self.adoption, &self.ends)
        else {
            return;
        };
        out.extend(
            recovery_deadline(since)
                .checked_add(std::time::Duration::from_millis(1))
                .filter(|at| *at > now)
                .map(Boundary::Opens),
        );
    }

    /// Never called: it offers no action.
    fn execute(&mut self, _local: u64, _log: &mut ObservationLog) {
        unreachable!("the recovery deadline source offers no action")
    }

    /// The same boundary, so the drain's clock reaches an owed recovery's
    /// deadline too, and nothing else.
    fn recovery(&self, now: SimInstant, out: &mut crate::RecoveryOffer) {
        let mut boundaries = Vec::new();
        self.boundaries(now, &mut boundaries);
        out.boundaries.extend(
            boundaries
                .into_iter()
                .filter_map(|boundary| match boundary {
                    Boundary::Opens(at) => Some(at),
                    Boundary::Deadline(_) => None,
                }),
        );
    }

    /// Nothing: it has no holds.
    fn enter_drain(&mut self, _log: &mut ObservationLog) {}

    /// None: it holds no work.
    fn holds_work(&self) -> Option<String> {
        None
    }
}

/// The steps a `header-sync/storage` run with a `Recover` step may take.
/// Its `BoundedAdversary` puts the livelock limit at a tenth of it, 5,000
/// steps at one instant, and honest recoveries take at most about 480 there
/// after the `recovered` pick, a grown source's, measured over 16 plans
/// each. A test keeps every recovery witness under half the limit. The run
/// usually ends far sooner, at its [`RecoveryFinish`].
pub const RECOVERY_BUDGET: u64 = 50_000;

/// What a scenario with a `Recover` step declares besides its five recovery
/// assumptions: `BoundedAdversary`, from the `recovered` pick on
/// ([`Storage::recovery_holds`]). Recovery then lets nothing hostile act,
/// so a node that keeps the clock from moving with an obligation open is
/// spinning on its own, and `sim/no-livelock` catches it rather than the
/// step budget ending the run (Rule 13 of the hdr-9 to hdr-13 plan). The
/// prefix before it is unbounded: honest storage runs can take thousands of
/// steps at one instant there, which the limit would fail.
pub fn recovered_bound() -> AssumeRecord {
    AssumeRecord {
        assume: Assume::BoundedAdversary,
        params: serde_json::json!({ "hostile_peers": 0, "from": "recovered" }),
    }
}

impl Storage {
    /// Whether `assume` holds after a step: `BoundedAdversary` from the
    /// `recovered` pick on ([`recovered_bound`]), and the recovery
    /// assumptions as [`RecoveryEvidence::holds`] says.
    pub fn recovery_holds(&self, assume: Assume) -> bool {
        match assume {
            Assume::BoundedAdversary => self.recovery.record().is_some(),
            assume => self.recovery.holds(&self.ends, assume),
        }
    }
}

/// Ends a recovering `header-sync/storage` run once its recovery concludes,
/// so it ends idle rather than at its budget (Rule 13): once
/// `header-sync/bounded-adversary-progress` has nothing owed
/// ([`RecoveryProgress::Concluded`]), once the clock has passed an owed
/// recovery's deadline, which its expiry needs, or, with no `Recover`
/// picked yet, once the run has taken [`storage_config`]'s budget, which a
/// prefix whose recover never comes ran to before. Its pick begins the node
/// under test's stop, as a `Shutdown` step's does, and a harness task then
/// stops every node ([`Topology::finish`]). It offers no time boundary.
///
/// A run that reaches [`RECOVERY_BUDGET`] after its `Recover` pick with no
/// `recovered` pick yet fails as a harness failure instead. Recovery's
/// transitions are quiet actions, offered only once no task is ready, so a
/// reactor that never stops being ready keeps the pick, and with it every
/// recovery assumption and `BoundedAdversary`, from ever coming. Nothing
/// would judge that run, and a budget end alone proves no missed deadline,
/// but it mustn't count as clean (Rule 13). Honest recoveries reach their
/// pick far sooner, though they can be busy for thousands of steps at one
/// instant first, which is why `BoundedAdversary` waits for the pick
/// ([`recovered_bound`]).
struct RecoveryFinish {
    recovery: RecoveryEvidence,
    adoption: AdoptionEvidence,
    ends: EndEvidence,
    topology: Rc<Topology>,
    /// Wakes the task that carries the finish out.
    to_task: watch::Sender<bool>,
    /// The last step observed.
    step: u64,
    /// Set once the finish was picked.
    done: bool,
}

impl RecoveryFinish {
    /// Whether the run's recovery has concluded at `now`, as
    /// [`RecoveryFinish`] says.
    fn due(&self, now: SimInstant) -> bool {
        if self.done {
            return false;
        }
        match self.recovery.progress(&self.adoption, &self.ends) {
            RecoveryProgress::Concluded => true,
            RecoveryProgress::Owed { since } => now > recovery_deadline(since),
            RecoveryProgress::Pending => {
                self.recovery.started().is_none() && self.step >= storage_config().budget
            }
        }
    }
}

impl ActionSource for RecoveryFinish {
    /// Offers `peer:node:finish` once, while [`RecoveryFinish::due`].
    fn enabled(&self, now: SimInstant, out: &mut Vec<(Identity, u64)>) {
        if self.due(now) {
            out.push((Identity::peer("node", "finish"), 0));
        }
    }

    /// None, as [`RecoveryFinish`] says.
    fn boundaries(&self, _now: SimInstant, _out: &mut Vec<Boundary>) {}

    /// Begins the stop and wakes the task.
    fn execute(&mut self, _local: u64, _log: &mut ObservationLog) {
        self.done = true;
        self.topology.begin_finish();
        self.to_task.send_replace(true);
    }

    /// Notes the step, for the prefix's budget, and fails the run at
    /// [`RECOVERY_BUDGET`] with the `recovered` pick still to come, as
    /// [`RecoveryFinish`] says.
    fn observe(&mut self, log: &mut ObservationLog) -> Result<(), String> {
        self.step = log.step();
        let waiting = self.recovery.started().is_some()
            && self.recovery.progress(&self.adoption, &self.ends) == RecoveryProgress::Pending;
        if waiting && self.step >= RECOVERY_BUDGET {
            return Err(format!(
                "the step budget ran out at step {} before the `recovered` pick, after the \
                 `Recover` pick at {:?}: a task never stopped being ready, so recovery's \
                 quiet transitions were never offered",
                self.step,
                self.recovery.started(),
            ));
        }
        Ok(())
    }

    /// Nothing: the finish begins the node's stop, new scenario work as a
    /// `Shutdown` step is, so the drain doesn't offer it. An owed recovery
    /// the drain reaches still expires at its deadline
    /// ([`RecoveryDeadline`]).
    fn recovery(&self, _now: SimInstant, _out: &mut crate::RecoveryOffer) {}

    /// Nothing: it has no holds.
    fn enter_drain(&mut self, _log: &mut ObservationLog) {}

    /// None: a picked finish is carried out by a task the drain runs.
    fn holds_work(&self) -> Option<String> {
        None
    }
}

/// The storage run's config: 2 workers, the default stack, which engine
/// planning needs, and room to sync and then idle on timers.
pub fn storage_config() -> RunConfig {
    RunConfig {
        scheduler: SchedulerConfig {
            workers: 2,
            ..SchedulerConfig::default()
        },
        budget: 4_000,
        // A drain took about 3,600 steps on the committed inputs, and over
        // 10,000 with 17 mirrors, so the bound leaves room for many more.
        drain_bound: 50_000,
        // No harness tail here waits on a timer, so the default zero cap
        // holds: controls run as queued, churn relinks by steps, and
        // `Settle` and a recovery's cooldown offer recovery boundaries.
        ..RunConfig::default()
    }
}

/// The family header sync's generated property IDs name.
pub const FAMILY: &str = "header-sync";

/// `header-sync/wire`'s properties: [`storage_properties`] less
/// [`OUTSTANDING_WORK_BOUNDED`], whose tap only the storage harness
/// watches, then
/// [`HEADER_FAULT_PENALIZED`], then header sync's generated receiver and
/// sender properties (M5-Q13), then `session/honest-never-punished`, then
/// `header-sync/honest-progress` (hdr-8), which only the
/// [`progress_fixture`] can apply. Header sync's adapter observes none of the downstream templates' effects, so it
/// registers none of them. `header-sync/storage` doesn't register
/// [`HEADER_FAULT_PENALIZED`]: none of its mirrors can send a header that
/// fails preparation, so no run or fuzz seed there could apply it.
pub fn wire_properties<S: HasView<View = StorageView>>() -> Vec<Property<S>> {
    let mut properties = storage_properties();
    properties.retain(|property| property.id.as_str() != OUTSTANDING_WORK_BOUNDED);
    properties.push(header_fault_penalized());
    properties.extend(table_properties(FAMILY, &RegtestMessage::LAYOUT));
    properties.extend(sender_properties(FAMILY, &RegtestMessage::LAYOUT));
    properties.push(honest_never_punished());
    properties.push(crate::domain::header_sync_progress::honest_progress());
    properties
}

/// The mirror `scenario`'s `Recover` step names, if it has one.
pub fn recovery_source(scenario: &StorageScenario) -> Option<String> {
    scenario
        .controls
        .iter()
        .find(|step| step.action == ControlAction::Recover)
        .map(|step| step.peer.clone())
}

/// The five assumptions a recovery from `source` declares: the recorded
/// membership stays, the source is honest, and storage, delivery and
/// scheduling stay bounded and fair.
pub fn recovery_assumptions(source: &str) -> Vec<AssumeRecord> {
    vec![
        AssumeRecord {
            assume: Assume::StableMembership,
            params: serde_json::json!({ "membership": "recorded" }),
        },
        AssumeRecord {
            assume: Assume::HonestPeer,
            params: serde_json::json!({ "peers": [source] }),
        },
        AssumeRecord {
            assume: Assume::BoundedStorage,
            params: serde_json::json!({ "max_ms": RECOVERY_BOUND.as_millis() }),
        },
        AssumeRecord {
            assume: Assume::BoundedDelivery,
            params: serde_json::json!({
                "link": "in-memory",
                "added_delay_ms": 0,
                "loss": "none",
            }),
        },
        AssumeRecord {
            assume: Assume::FairScheduling,
            params: serde_json::json!({ "contract": crate::record::CONTRACT_VERSION }),
        },
    ]
}

/// The storage harness's properties, [`OPERATION_IDS`],
/// [`DIRECT_DISPATCH`], [`CHUNK_BUDGET_BOUND`],
/// [`TEARDOWN_RETURNS_BASELINE`], [`STALE_COMPLETION_INERT`],
/// [`RESET_INVALIDATES_WORK`], [`STALE_CHAIN_EFFECT_INERT`],
/// [`COMMIT_ONLY_AUTHORIZED`], [`PEER_RETIREMENT_PROGRESS`],
/// [`EXPECTED_CHAIN_SELECTED`] and [`STATE_PUBLICATION_CONSISTENT`], all
/// active. A test harness that wraps [`Storage`] with its view registers
/// them too.
///
/// [`CHUNK_BUDGET_BOUND`]: crate::domain::header_sync::CHUNK_BUDGET_BOUND
/// [`TEARDOWN_RETURNS_BASELINE`]: crate::domain::header_sync::TEARDOWN_RETURNS_BASELINE
/// [`STALE_COMPLETION_INERT`]: crate::domain::header_sync::STALE_COMPLETION_INERT
/// [`RESET_INVALIDATES_WORK`]: crate::domain::header_sync::RESET_INVALIDATES_WORK
/// [`STALE_CHAIN_EFFECT_INERT`]: crate::domain::header_sync::STALE_CHAIN_EFFECT_INERT
/// [`COMMIT_ONLY_AUTHORIZED`]: crate::domain::header_sync::COMMIT_ONLY_AUTHORIZED
/// [`PEER_RETIREMENT_PROGRESS`]: crate::domain::header_sync::PEER_RETIREMENT_PROGRESS
/// [`EXPECTED_CHAIN_SELECTED`]: crate::domain::header_sync::EXPECTED_CHAIN_SELECTED
/// [`STATE_PUBLICATION_CONSISTENT`]: crate::domain::header_sync::STATE_PUBLICATION_CONSISTENT
pub fn storage_properties<S: HasView<View = StorageView>>() -> Vec<Property<S>> {
    vec![
        Property {
            id: PropertyId::new(OPERATION_IDS),
            claim: "the node's storage operations get rising IDs, and each completes at most \
                    once, after its submission",
            kind: Kind::Safety,
            assumes: &[],
            needs: &[Source::Storage],
            status: Status::Active,
            check: check_operation_ids::<S>,
            witness: None,
        },
        Property {
            id: PropertyId::new(DIRECT_DISPATCH),
            claim: "every storage call goes to the node's port",
            kind: Kind::Safety,
            assumes: &[],
            needs: &[Source::View],
            status: Status::Active,
            check: check_direct_dispatch::<S>,
            witness: None,
        },
        chunk_budget_bound(),
        teardown_returns_baseline(),
        stale_completion_inert(),
        reset_invalidates_work(),
        stale_chain_effect_inert(),
        commit_only_authorized(),
        outstanding_work_bounded(),
        peer_retirement_progress(),
        expected_chain_selected(),
        state_publication_consistent(),
    ]
}

/// The peers the storage harness's node admits at once, from the config
/// and connection limit [`HeaderNode::spawn`] gives it.
fn storage_peer_limit() -> usize {
    let limits = ZakuraHeaderSyncConfig::default().peer_limits;
    admitted_peer_limit(
        limits.max_inbound_peers,
        limits.max_outbound_peers,
        zakura_network::zakura::DEFAULT_ZAKURA_MAX_CONNECTIONS,
    )
}

/// The departed VCT repair suppliers whose history the storage harness's
/// node keeps, `max_inbound_peers + max_outbound_peers`, as the reactor's
/// `prune_vct_supplier_history` passes them from the config
/// [`HeaderNode::spawn`] gives it.
fn storage_departed_limit() -> usize {
    let limits = ZakuraHeaderSyncConfig::default().peer_limits;
    limits
        .max_inbound_peers
        .saturating_add(limits.max_outbound_peers)
}

/// What [`storage_properties`] read: the node's storage facts and the
/// harness's view, plus the step loop's task observations.
pub fn storage_sources() -> Sources {
    Sources::observed(&[Source::Schedule, Source::Storage, Source::View])
}

/// A submission or completion on the node's `service` storage, instance 0.
fn operation_fact<'o>(service: &str, obs: &'o Observation) -> Option<&'o StorageFact> {
    match obs {
        Observation::Storage {
            service: logged,
            instance: 0,
            fact:
                fact @ (StorageFact::Submitted { .. }
                | StorageFact::Completed { .. }
                | StorageFact::BlockSubmitted { .. }
                | StorageFact::BlockCompleted { .. }),
            ..
        } if logged == service => Some(fact),
        _ => None,
    }
}

/// Checks [`OPERATION_IDS`] on the node's header storage.
fn check_operation_ids<S: HasView>(cx: &mut CheckCtx<'_, S>) -> Verdict {
    check_service_operation_ids(HEADER_SERVICE, cx)
}

/// Checks that the node's `service` storage gives its operations rising
/// IDs and completes each at most once, after its submission, at each step
/// that logs a submission or completion there. A check keeps no state
/// between steps, so it replays the node's facts from the run's start, and
/// reports every finding among this step's, in log order.
pub(crate) fn check_service_operation_ids<S: HasView>(
    service: &str,
    cx: &mut CheckCtx<'_, S>,
) -> Verdict {
    if !cx
        .observations()
        .any(|obs| operation_fact(service, obs).is_some())
    {
        return Verdict::NotApplicable;
    }
    let mut ids = OperationIds::default();
    let mut findings = Vec::new();
    for entry in cx.log.entries() {
        let Some(fact) = operation_fact(service, &entry.obs) else {
            continue;
        };
        if let Some(finding) = ids.check(fact) {
            if entry.step == cx.step {
                findings.push(finding);
            }
        }
    }
    let mut findings = findings.into_iter();
    let Some((expected, actual)) = findings.next() else {
        return Verdict::Holds;
    };
    for (expected, actual) in findings {
        cx.record_violation(expected, actual);
    }
    cx.violated(expected, actual)
}

/// Checks [`DIRECT_DISPATCH`]: violated once the dispatch guard's flag is
/// set.
fn check_direct_dispatch<S: HasView<View = StorageView>>(cx: &mut CheckCtx<'_, S>) -> Verdict {
    if cx.view().direct_dispatch_broken {
        cx.violated(
            "every storage call goes to the node's port",
            "a reactor sent a call to its action channel",
        )
    } else {
        Verdict::Holds
    }
}

/// [`HEADER_FAULT_PENALIZED`], active.
fn header_fault_penalized<S: HasView<View = StorageView>>() -> Property<S> {
    Property {
        id: PropertyId::new(HEADER_FAULT_PENALIZED),
        claim: "every prepare or apply failure that is an automatic header-peer fault gets a \
                    Misbehavior against that work's session and starts its peer's readmission \
                    cooldown, and a reconnect within the cooldown is refused",
        kind: Kind::Safety,
        assumes: &[],
        needs: &[Source::Storage, Source::View],
        status: Status::Active,
        check: check_header_fault_penalized::<S>,
        witness: None,
    }
}

/// What [`HEADER_FAULT_PENALIZED`] expects, as its violations report it.
const FAULT_PENALIZED_EXPECTED: &str = "each header-peer fault the reactor took is penalized \
     against its session and cools its peer down, which no session from it skips";

/// Checks [`HEADER_FAULT_PENALIZED`] after each step, from the node's fault
/// evidence ([`FaultEvidence::check`]). Each break the step's entries show
/// is its own violation at this step. Something still open is pending
/// ([`Pending::FaultOpen`]), beside a break too: a penalty whose call is
/// still being handled, or a failed reply the reactor hasn't taken yet. A failure whose reply never
/// reached the reactor opens nothing. What's still open when a settled run
/// ends breaks the rule through [`fault_end_violations`], and at any other
/// end it's unsettled.
fn check_header_fault_penalized<S: HasView<View = StorageView>>(
    cx: &mut CheckCtx<'_, S>,
) -> Verdict {
    match cx.view().faults.check(cx.log.entries()) {
        FaultCheck::Nothing => Verdict::NotApplicable,
        FaultCheck::Holds => Verdict::Holds,
        FaultCheck::Open(_) => {
            cx.pending(Pending::FaultOpen);
            Verdict::Holds
        }
        FaultCheck::Broken { breaks, open } => {
            if open.is_some() {
                cx.pending(Pending::FaultOpen);
            }
            let mut breaks = breaks.into_iter();
            let first = breaks
                .next()
                .expect("FaultEvidence::check returns Broken only with a new break");
            for other in breaks {
                cx.record_violation(FAULT_PENALIZED_EXPECTED, other);
            }
            cx.violated(FAULT_PENALIZED_EXPECTED, first)
        }
    }
}

/// [`HEADER_FAULT_PENALIZED`]'s breaks at the run's end, one each, from
/// [`FaultEvidence::penalized`] over `log`, such as two failed replies the
/// reactor never took by the time a settled run ended. The end replay reads
/// the whole run again, so it finds every break the step check already
/// reported too. Each of those is left out once, by its words, which name
/// its operation ([`FaultEvidence::reported`]), so each break is reported
/// once (C12).
fn fault_end_violations(faults: &FaultEvidence, log: &ObservationLog) -> Vec<EndViolation> {
    let mut reported = faults.reported();
    faults
        .penalized(log.entries())
        .violated
        .into_iter()
        .filter(
            |actual| match reported.iter().position(|at_step| at_step == actual) {
                Some(at) => {
                    reported.swap_remove(at);
                    false
                }
                None => true,
            },
        )
        .map(|actual| EndViolation {
            property: PropertyId::new(HEADER_FAULT_PENALIZED),
            expected: FAULT_PENALIZED_EXPECTED.to_owned(),
            actual,
        })
        .collect()
}

/// The node's operation IDs so far, which [`check_operation_ids`] replays
/// its facts into.
#[derive(Debug, Default)]
struct OperationIds {
    /// The last submitted ID.
    submitted: u64,
    /// Every completed ID.
    completed: BTreeSet<u64>,
}

impl OperationIds {
    /// Records `fact`, and returns what's wrong with it, if anything, as
    /// (expected, actual).
    fn check(&mut self, fact: &StorageFact) -> Option<(String, String)> {
        match *fact {
            StorageFact::Submitted { id, .. } | StorageFact::BlockSubmitted { id, .. } => {
                let expected = self.submitted + 1;
                self.submitted = id;
                (id != expected).then(|| {
                    (
                        format!("operation {expected} submitted next"),
                        format!("operation {id} submitted"),
                    )
                })
            }
            StorageFact::Completed { id, .. } | StorageFact::BlockCompleted { id, .. } => {
                if id == 0 || id > self.submitted {
                    let actual = if id == 0 {
                        "operation 0 completed, an ID no operation gets".to_owned()
                    } else {
                        format!("operation {id} completed before its submission")
                    };
                    Some((
                        format!(
                            "a completion of a submitted operation, at most {}",
                            self.submitted
                        ),
                        actual,
                    ))
                } else if !self.completed.insert(id) {
                    Some((
                        format!("operation {id} completes once"),
                        format!("operation {id} completed again"),
                    ))
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observe::{StorageRequest, StorageResult};

    /// The fork fixture's window constants restate production's, and its
    /// header cap leaves more than a refill batch of window.
    #[test]
    fn the_fork_fixture_bound_restates_the_header_window() {
        assert_eq!(HEADER_BODY_WINDOW, zakura_network::zakura::MAX_HS_RANGE);
        let gap = zakura_chain::parameters::checkpoint::constants::MAX_CHECKPOINT_HEIGHT_GAP;
        assert_eq!(usize::try_from(HEADER_REFILL_BATCH).unwrap(), gap + 1);
        const { assert!(HEADER_BODY_WINDOW - 2 * FORK_FIXTURE_MAX_HEADERS > HEADER_REFILL_BATCH) };
    }

    /// The fork fixture's mirrors on `chains`, from genesis, under the
    /// fixture's policy.
    fn forks(chains: Vec<Chain>) -> StorageScenario {
        StorageScenario {
            node: Chain::default(),
            mirrors: chains
                .into_iter()
                .map(|chain| MirrorSpec {
                    chain,
                    dialer: Dialer::Mirror,
                    max_headers_per_response: None,
                })
                .collect(),
            policy: Policy {
                base: Base::SuccessOnly,
                bound_ms: Some(10),
                holds: Vec::new(),
                reply_holds: Vec::new(),
                disruptions: Vec::new(),
            },
            controls: Vec::new(),
            weights: Weights::default(),
        }
    }

    /// Forks from one ancestor within the header cap are the fork fixture.
    /// Past the cap, where a correct node's window could run out on bodies
    /// the simulator never verifies, they aren't: two mirrors sharing
    /// 4,000 main-line headers with one fork header each, for one. Nor are
    /// chains one of which is a prefix of another, forks from two different
    /// ancestors, one mirror, or a node off genesis.
    #[test]
    fn the_fork_fixture_declares_only_bounded_forks_from_one_ancestor() {
        let fork = |main, branch, len| Chain {
            main,
            fork: Some(Fork { branch, len }),
        };
        assert!(fork_fixture(&forks(vec![fork(0, 1, 5), fork(0, 2, 3)])).is_some());
        assert!(fork_fixture(&forks(vec![fork(10, 1, 490), fork(10, 2, 490)])).is_some());
        assert!(fork_fixture(&forks(vec![fork(10, 1, 491), fork(10, 2, 490)])).is_none());
        assert!(fork_fixture(&forks(vec![fork(4_000, 1, 1), fork(4_000, 2, 1)])).is_none());
        assert!(fork_fixture(&forks(vec![Chain::main(5), Chain::main(3)])).is_none());
        assert!(fork_fixture(&forks(vec![fork(2, 1, 2), fork(2, 2, 2), fork(0, 1, 2)])).is_none());
        assert!(fork_fixture(&forks(vec![fork(0, 1, 5)])).is_none());
        let mut off_genesis = forks(vec![fork(2, 1, 5), fork(2, 2, 3)]);
        off_genesis.node = Chain::main(1);
        assert!(fork_fixture(&off_genesis).is_none());
    }

    /// A submission of operation `id`.
    fn submitted(id: u64) -> StorageFact {
        StorageFact::Submitted {
            id,
            request: StorageRequest::ContinuationLocator,
            digest: String::new(),
            call: None,
        }
    }

    /// A completion of operation `id`.
    fn completed(id: u64) -> StorageFact {
        StorageFact::Completed {
            id,
            outcome: "engine".to_owned(),
            result: StorageResult::NoLocator,
            delivered: true,
            generation: 0,
            state: String::new(),
        }
    }

    /// `check` on `facts` in order, with each fact's finding.
    fn check_all(facts: &[StorageFact]) -> Vec<Option<(String, String)>> {
        let mut ids = OperationIds::default();
        facts.iter().map(|fact| ids.check(fact)).collect()
    }

    /// The finding `check` reports.
    fn finding(expected: &str, actual: &str) -> Option<(String, String)> {
        Some((expected.to_owned(), actual.to_owned()))
    }

    /// Logs operation `id`: a prepare from `aa` in session 3 that reactor
    /// call `id` submitted, failing with an `invalid_header` that blames
    /// `aa`, its reply reaching the reactor, and, if `taken`, taken by it.
    fn log_fault(
        log: &mut ObservationLog,
        attributions: &crate::storage::FaultAttributions,
        id: u64,
        taken: bool,
    ) {
        use crate::observe::{StorageBranch, StoragePoint, WorkOwner, WorkScope};

        let point = |height| StoragePoint {
            height,
            hash: String::new(),
        };
        attributions.insert(0, id, crate::storage::Blame::HeaderPeer("aa".to_owned()));
        let owner = WorkOwner {
            kind: "header".to_owned(),
            scope: WorkScope {
                header_generation: 1,
                branch: StorageBranch {
                    anchor: String::new(),
                    target: String::new(),
                },
            },
            verified_generation: None,
            body_work_epoch: None,
            session_id: 3,
            request_id: id,
        };
        let request = StorageRequest::PrepareHeaderTarget {
            owner,
            source: "aa".to_owned(),
            common_ancestor: point(0),
            target: point(1),
            headers: 1,
            work: None,
            first: None,
            hashes: vec!["bb".to_owned()],
        };
        let failed = StorageResult::PrepareFailed {
            category: "invalid_header".to_owned(),
        };
        let mut facts = vec![
            StorageFact::Submitted {
                id,
                request,
                digest: String::new(),
                call: Some(id),
            },
            StorageFact::Completed {
                id,
                outcome: "engine".to_owned(),
                result: failed,
                delivered: true,
                generation: 0,
                state: String::new(),
            },
        ];
        if taken {
            facts.push(StorageFact::ReplyTaken { id });
        }
        for fact in facts {
            log.push(Observation::Storage {
                service: HEADER_SERVICE.to_owned(),
                instance: 0,
                seq: 0,
                txn: 0,
                fact,
            });
        }
    }

    /// Logs the reactor's handling of call `call`'s reply, during which it
    /// reported nothing.
    fn log_handled(log: &mut ObservationLog, call: u64) {
        use crate::observe::ReactorFact;

        for event in [
            ReactorFact::CallEnded {
                call,
                end: "replied".to_owned(),
                op: Some(call),
            },
            ReactorFact::Handled {
                call,
                panicked: false,
            },
        ] {
            log.push(Observation::Reactor {
                instance: 0,
                seq: 0,
                storage_seq: 0,
                node: "node".to_owned(),
                event,
            });
        }
    }

    /// Fault evidence over `attributions`, for a run that ended settled.
    fn settled_faults(attributions: crate::storage::FaultAttributions) -> FaultEvidence {
        use crate::{IdleStatus, Outcome};

        let faults =
            FaultEvidence::new(attributions, Default::default(), std::time::Duration::ZERO);
        faults.end(&RunEnd {
            outcome: Outcome::Idle {
                status: IdleStatus::Quiescent,
            },
            steps: 1,
            violation: None,
            drain: None,
            harness: None,
            rest: Some(zakura_scheduler::Status::Quiescent),
        });
        faults
    }

    /// Asserts each of `actual` starts with its own `prefix`, in order.
    fn starts_with_each(actual: &[String], prefixes: &[&str]) {
        assert_eq!(actual.len(), prefixes.len(), "{actual:?}");
        for (actual, prefix) in actual.iter().zip(prefixes) {
            assert!(actual.starts_with(prefix), "{actual:?} vs {prefix:?}");
        }
    }

    /// Two header-peer faults whose replies reached the reactor, which
    /// never took either, are two breaks when a settled run ends, one
    /// [`HEADER_FAULT_PENALIZED`] end violation each.
    #[test]
    fn every_untaken_fault_is_an_end_violation() {
        let attributions = crate::storage::FaultAttributions::default();
        let mut log = ObservationLog::default();
        for id in [4, 7] {
            log_fault(&mut log, &attributions, id, false);
        }
        let found = fault_end_violations(&settled_faults(attributions), &log);
        assert!(
            found
                .iter()
                .all(|found| found.property == PropertyId::new(HEADER_FAULT_PENALIZED)),
            "{found:?}"
        );
        let actual: Vec<String> = found.into_iter().map(|found| found.actual).collect();
        starts_with_each(
            &actual,
            &["UntakenFault: operation 4 ", "UntakenFault: operation 7 "],
        );
    }

    /// A harness whose view holds only a recovery and the adoption and end
    /// evidence its progress reads, for driving
    /// `bounded-adversary-progress`'s check directly.
    struct RecoveryOnly {
        recovery: RecoveryEvidence,
        adoption: AdoptionEvidence,
        ends: EndEvidence,
    }

    impl HasView for RecoveryOnly {
        type View = StorageView;

        /// [`FaultsOnly`]'s empty view, with the harness's recovery,
        /// adoption and ends.
        fn view(&self) -> StorageView {
            StorageView {
                recovery: self.recovery.clone(),
                adoption: self.adoption.clone(),
                ends: self.ends.clone(),
                ..FaultsOnly(FaultEvidence::default()).view()
            }
        }
    }

    /// With the successor owed, a step at the run's budget fails the run as
    /// missing evidence while the clock is at or before the deadline, and
    /// not once it's past it, where the registry's expiry judges the
    /// obligation. A step before the budget fails nothing.
    #[test]
    fn a_budget_end_is_missing_evidence_only_before_the_deadline() {
        use crate::{
            check::{Phase, StepCtx},
            domain::header_sync_progress::{bounded_adversary_progress, recovery_deadline},
            harness::header_node::recovery::{tests::owed, RecoveryProgress},
        };
        use std::time::Duration;
        use zakura_scheduler::{SimInstant, Status as SchedulerStatus};

        let (recovery, adoption, ends) = owed();
        let harness = RecoveryOnly {
            recovery,
            adoption,
            ends,
        };
        let RecoveryProgress::Owed { since } =
            harness.recovery.progress(&harness.adoption, &harness.ends)
        else {
            panic!("the recovery owes its successor");
        };
        let deadline = recovery_deadline(since);
        let registered = [bounded_adversary_progress::<RecoveryOnly>()];
        let log = ObservationLog::default();
        let pick = crate::Identity::advance_time(SimInstant::ZERO);
        let missing = |step: u64, now: SimInstant| {
            let step = StepCtx {
                step,
                pick: &pick,
                enabled: &[],
                phase: Phase::Live,
                budget: 10,
                now,
                status: SchedulerStatus::Quiescent,
                quiescent: false,
                log: &log,
            };
            let mut cx = CheckCtx::new(&step, &harness, (&registered[0], &registered), None);
            (registered[0].check)(&mut cx);
            cx.into_parts().missing
        };
        let past = crate::harness::policy::after(deadline, Duration::from_millis(1));
        assert!(missing(10, deadline).is_some());
        assert!(missing(11, since).is_some());
        assert_eq!(missing(10, past), None);
        assert_eq!(missing(9, since), None);
    }

    /// A harness whose view holds only its fault evidence, for driving
    /// [`HEADER_FAULT_PENALIZED`]'s check directly.
    struct FaultsOnly(FaultEvidence);

    impl HasView for FaultsOnly {
        type View = StorageView;

        /// A view with the harness's fault evidence and empty everything
        /// else.
        fn view(&self) -> StorageView {
            StorageView {
                port: Arc::new(ControlledPort::new(&HeaderFixture::regtest())),
                direct_dispatch_broken: false,
                honest_peers: Rc::default(),
                adoption: AdoptionEvidence::default(),
                sessions: Vec::new(),
                faults: self.0.clone(),
                capacity: CapacityEvidence::default(),
                claims: ClaimEvidence::default(),
                rounds: SettlementEvidence::default(),
                stale_calls: StaleEvidence::default(),
                wake: WakeEvidence::default(),
                hold_wake: HoldWakeEvidence::default(),
                ends: EndEvidence::default(),
                stamps: Stamps::default(),
                round_life: RoundLifeEvidence::default(),
                retirement: RetirementEvidence::default(),
                chain_effect: ChainEffectEvidence::default(),
                authority: AuthorityEvidence::default(),
                outstanding: OutstandingEvidence::default(),
                recovery: RecoveryEvidence::default(),
                fork_choice: ForkChoiceEvidence::default(),
                publication: PublicationEvidence::default(),
            }
        }
    }

    /// Two penalties broken in one step are two violations at that step,
    /// and an untaken reply beside them leaves the property unsettled. A
    /// later check reports neither again. The settled end's replay finds
    /// both breaks again, in the same words, and the untaken reply, and
    /// reports only the untaken reply, since the step reported the breaks.
    #[test]
    fn the_fault_check_reports_every_break_at_a_step() {
        use crate::check::{Phase, StepCtx};
        use zakura_scheduler::{SimInstant, Status as SchedulerStatus};

        let attributions = crate::storage::FaultAttributions::default();
        let mut log = ObservationLog::default();
        for call in [1, 2] {
            log_fault(&mut log, &attributions, call, true);
            log_handled(&mut log, call);
        }
        log_fault(&mut log, &attributions, 3, false);
        let harness = FaultsOnly(settled_faults(attributions));
        let registered = [header_fault_penalized::<FaultsOnly>()];
        let pick = crate::Identity::advance_time(SimInstant::ZERO);
        let check = |step| {
            let step = StepCtx {
                step,
                pick: &pick,
                enabled: &[],
                phase: Phase::Live,
                budget: u64::MAX,
                now: SimInstant::ZERO,
                status: SchedulerStatus::Quiescent,
                quiescent: true,
                log: &log,
            };
            let mut cx = CheckCtx::new(&step, &harness, (&registered[0], &registered), None);
            let verdict = (registered[0].check)(&mut cx);
            let parts = cx.into_parts();
            let mut actual: Vec<String> = match verdict {
                Verdict::Violated(violation) => vec![violation.actual],
                _ => Vec::new(),
            };
            actual.extend(parts.recorded.into_iter().map(|violation| violation.actual));
            (actual, parts.pending)
        };
        let (stepped, pending) = check(0);
        starts_with_each(
            &stepped,
            &[
                "UnpenalizedFault: prepare operation 1 ",
                "UnpenalizedFault: prepare operation 2 ",
            ],
        );
        assert_eq!(pending, [Pending::FaultOpen].into());
        let (actual, pending) = check(1);
        assert!(actual.is_empty(), "{actual:?}");
        assert_eq!(pending, [Pending::FaultOpen].into());

        let replayed = harness.0.penalized(log.entries()).violated;
        starts_with_each(
            &replayed,
            &[
                "UnpenalizedFault: prepare operation 1 ",
                "UnpenalizedFault: prepare operation 2 ",
                "UntakenFault: operation 3 ",
            ],
        );
        assert_eq!(replayed[..2], stepped[..]);
        assert_eq!(harness.0.reported(), stepped);
        let ended: Vec<String> = fault_end_violations(&harness.0, &log)
            .into_iter()
            .map(|found| found.actual)
            .collect();
        starts_with_each(&ended, &["UntakenFault: operation 3 "]);
    }

    /// A harness with no view, for checks that read only the log.
    struct Logged;

    impl HasView for Logged {
        type View = ();

        /// Nothing.
        fn view(&self) {}
    }

    /// Logs each step's scripted storage facts on the node's header
    /// storage, from step 1, since the run also observes after setup.
    struct Facts {
        /// Each step's facts, in order.
        steps: std::collections::VecDeque<Vec<StorageFact>>,
        /// The last fact's sequence number.
        seq: u64,
    }

    impl crate::ActionSource for Facts {
        fn enabled(&self, _: zakura_scheduler::SimInstant, _: &mut Vec<(crate::Identity, u64)>) {}

        fn boundaries(&self, _: zakura_scheduler::SimInstant, _: &mut Vec<crate::Boundary>) {}

        fn execute(&mut self, _: u64, _: &mut ObservationLog) {
            unreachable!("the script offers no actions");
        }

        /// Logs the next step's facts, numbered after the last ones.
        fn observe(&mut self, log: &mut ObservationLog) -> Result<(), String> {
            if log.step() == 0 {
                return Ok(());
            }
            for fact in self.steps.pop_front().unwrap_or_default() {
                self.seq += 1;
                log.push(Observation::Storage {
                    service: HEADER_SERVICE.to_owned(),
                    instance: 0,
                    seq: self.seq,
                    txn: self.seq,
                    fact,
                });
            }
            Ok(())
        }

        /// Nothing: it finishes no submitted work.
        fn recovery(&self, _now: zakura_scheduler::SimInstant, _out: &mut crate::RecoveryOffer) {}

        /// Nothing: it has no holds.
        fn enter_drain(&mut self, _log: &mut ObservationLog) {}

        /// None: it holds no submitted work.
        fn holds_work(&self) -> Option<String> {
            None
        }
    }

    /// Runs [`OPERATION_IDS`] through the registry over one task that
    /// yields once per step of `steps`, each step logging its facts.
    fn run_operation_ids(steps: Vec<Vec<StorageFact>>) -> crate::RunReport {
        let mut sim = crate::Sim::new(RunConfig::default(), crate::PlanInput::Bytes(Vec::new()));
        let count = steps.len();
        sim.add_source(Box::new(Facts {
            steps: steps.into_iter().collect(),
            seq: 0,
        }));
        sim.scheduler().spawn_root("reactor", async move {
            for _ in 1..count {
                zakura_rt::task::yield_now().await;
            }
        });
        let property = Property {
            id: PropertyId::new(OPERATION_IDS),
            claim: "operation IDs",
            kind: Kind::Safety,
            assumes: &[],
            needs: &[Source::Storage],
            status: Status::Active,
            check: check_operation_ids::<Logged>,
            witness: None,
        };
        let registry = crate::property::Registry::new(vec![property]).expect("one property");
        let mut checker = crate::property::RegistryChecker::new(
            registry,
            Rc::new(Logged),
            crate::property::Assumptions::none(),
            &Sources::observed(&[Source::Schedule, Source::Storage]),
        );
        sim.run(&mut checker, &mut crate::MemorySink::default())
    }

    /// Every finding at one step survives into the run's report, not just
    /// the first: at step 2, operation 1 completes again and operation 3
    /// completes before its submission, and step 1's facts break nothing.
    #[test]
    fn every_operation_id_finding_at_a_step_is_reported() {
        let report = run_operation_ids(vec![
            vec![submitted(1), completed(1), submitted(2)],
            vec![completed(1), completed(3), completed(2)],
        ]);
        let property = &report.properties[0];
        assert_eq!(
            property.outcome,
            crate::PropertyOutcome::Violated { witness: vec![2] }
        );
        let found = |expected: &str, actual: &str| crate::Violation {
            property: OPERATION_IDS.to_owned(),
            step: 2,
            witness: vec![2],
            expected: expected.to_owned(),
            actual: actual.to_owned(),
        };
        assert_eq!(
            property.violations,
            [
                found("operation 1 completes once", "operation 1 completed again"),
                found(
                    "a completion of a submitted operation, at most 2",
                    "operation 3 completed before its submission"
                ),
            ]
        );
    }

    /// No M4 run trips [`OPERATION_IDS`], so each of its branches gets a
    /// hand-built fact sequence: a skipped submission, a completion before
    /// its submission, and a second completion. In-order facts pass.
    #[test]
    fn check_reports_each_operation_id_violation() {
        assert_eq!(
            check_all(&[submitted(1), completed(1), submitted(2), completed(2)]),
            [None, None, None, None]
        );
        assert_eq!(
            check_all(&[submitted(1), submitted(3)]),
            [
                None,
                finding("operation 2 submitted next", "operation 3 submitted")
            ]
        );
        assert_eq!(
            check_all(&[submitted(1), completed(2)]),
            [
                None,
                finding(
                    "a completion of a submitted operation, at most 1",
                    "operation 2 completed before its submission"
                )
            ]
        );
        assert_eq!(
            check_all(&[submitted(1), completed(1), completed(1)]),
            [
                None,
                None,
                finding("operation 1 completes once", "operation 1 completed again")
            ]
        );
    }
}
