//! `block-sync/progress`: step 10 for block sync on `block-sync/storage`.
//! An honest mirror, `mirror-0`, holds every body the node needs, while k
//! hostile mirrors take requests and serve them badly ([`Role`]).
//! [`ADVERSARY_PROGRESS`] holds the node to the deadline production's
//! constants give for k ([`block_progress_deadline`]).
//!
//! That deadline holds only while each stalled adversary stays parked, so
//! a prefix draws at most as many adversaries as fit inside the 180 s park
//! ([`draw_mix`]), and no scenario relinks one. The park stays as
//! production has it.
//!
//! The scenario prefix draws k and each adversary's role and timing. The
//! fixed role mixes are prefixes of the same input ([`Mix`]), which the
//! nightly sim runs (`tests/block_progress.rs`).

use std::time::Duration;

use zakura_network::zakura::{block_progress_deadline, BlockProgressUnbounded};
use zakura_scheduler::SimInstant;

use super::{
    block_node::MirrorFault,
    block_sync::{
        storage_config, storage_properties, storage_sources, BlockScenario, MirrorSpec, Ports,
        Storage,
    },
    header_sync::Dialer,
    ScenarioFamily, Subsystem,
};
use crate::{
    observe::{StorageFact, StorageRequest},
    property::{
        Assume, AssumeRecord, Assumptions, Capture, CheckCtx, HasView, Kind, LivenessBound,
        ObligationKey, Property, PropertyId, Source, Sources, Status, Subject, Verdict,
    },
    ActionSource, Boundary, Identity, Observation, ObservationLog, RunConfig, RunEnd, Sim,
};

/// Once the node starts block work, it submits every body it needs for
/// commit within the deadline production's constants give for the
/// adversaries beside the honest mirror.
pub const ADVERSARY_PROGRESS: &str = "block-sync/adversary-progress";

/// [`ADVERSARY_PROGRESS`]'s declared bound: production's 180 s park, which
/// every run's own deadline fits inside.
pub const PARK: Duration = Duration::from_secs(180);

/// The first byte of every `block-sync/progress` prefix: its generator.
pub const PROGRESS_VERSION: u8 = 1;

/// The most adversaries a prefix draws, before the park clamps them.
pub const MAX_ADVERSARIES: usize = 3;

/// The bytes that draw one adversary: its role and its timing.
const ADVERSARY_BYTES: usize = 2;

/// The length of a `block-sync/progress` prefix: its version byte, k, then
/// [`ADVERSARY_BYTES`] for each of up to [`MAX_ADVERSARIES`].
pub const PROGRESS_BYTES: usize = 2 + MAX_ADVERSARIES * ADVERSARY_BYTES;

/// How many reads a silent or unavailable adversary's timing byte lets it
/// serve before it turns: 0 to 3.
const SERVED_STEPS: u8 = 4;

/// How much each step of a liar's timing byte delays its answers. The
/// delays run from 0.5 s to 20 s, past the 8 s request timeout.
const DELAY_STEP_MS: u64 = 500;

/// How many delay steps a liar's timing byte picks among.
const DELAY_STEPS: u8 = 40;

/// One adversary's role.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    /// Serves its first few reads, then never answers another
    /// ([`MirrorFault::Silent`]).
    Silent,
    /// Serves its first few reads, then answers each with no blocks, so its
    /// ranges end unavailable ([`MirrorFault::Unavailable`]).
    Unavailable,
    /// Advertises production's request cap, takes every request up to it,
    /// and answers each late ([`MirrorFault::Slow`]).
    Liar,
}

impl Role {
    /// Every role, in the order a role byte picks them.
    pub const ALL: [Role; 3] = [Role::Silent, Role::Unavailable, Role::Liar];

    /// The role `byte` picks.
    fn from_byte(byte: u8) -> Role {
        Role::ALL[usize::from(byte) % Role::ALL.len()]
    }

    /// The byte that picks this role.
    fn byte(self) -> u8 {
        match self {
            Role::Silent => 0,
            Role::Unavailable => 1,
            Role::Liar => 2,
        }
    }

    /// The fault this role plays with `timing`.
    fn fault(self, timing: u8) -> MirrorFault {
        let served = u32::from(timing % SERVED_STEPS);
        match self {
            Role::Silent => MirrorFault::Silent { served },
            Role::Unavailable => MirrorFault::Unavailable { served },
            Role::Liar => MirrorFault::Slow {
                delay_ms: DELAY_STEP_MS * (1 + u64::from(timing % DELAY_STEPS)),
            },
        }
    }
}

/// One drawn adversary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Adversary {
    /// What it does.
    pub role: Role,
    /// Its role's timing ([`Role::fault`]).
    pub timing: u8,
}

impl Adversary {
    /// `role` with `timing`.
    pub const fn new(role: Role, timing: u8) -> Adversary {
        Adversary { role, timing }
    }
}

/// The default scenario's eight 2,000-byte blocks, all from the honest
/// mirror, `mirror-0`, then `adversaries` as `mirror-1` on. Every mirror
/// dials the node at setup.
pub fn progress_mix(adversaries: &[Adversary]) -> BlockScenario {
    let honest = MirrorSpec::new(Dialer::Mirror);
    let mirrors = std::iter::once(honest)
        .chain(adversaries.iter().map(|adversary| MirrorSpec {
            fault: Some(adversary.role.fault(adversary.timing)),
            ..MirrorSpec::new(Dialer::Mirror)
        }))
        .collect();
    let mut scenario = BlockScenario {
        mirrors,
        ..BlockScenario::default()
    };
    // Room to wait out one turn per adversary on timers.
    scenario.steps = Some(storage_config(&scenario).budget * 4);
    scenario
}

/// The bodies' bytes the honest mirror must move in `scenario`.
fn honest_bytes(scenario: &BlockScenario) -> u64 {
    u64::from(scenario.blocks - scenario.node_bodies) * u64::from(scenario.body_bytes)
}

/// The scenario the bytes after a prefix's version byte draw, read as zeros
/// past their end: k, modulo one more than [`MAX_ADVERSARIES`], then each
/// adversary's role and timing. k drops to the most adversaries whose
/// deadline fits inside the park ([`block_progress_deadline`]).
pub fn draw_mix(grammar: &[u8]) -> BlockScenario {
    let byte = |index: usize| grammar.get(index).copied().unwrap_or(0);
    let drawn = usize::from(byte(0)) % (MAX_ADVERSARIES + 1);
    let adversaries: Vec<Adversary> = (0..drawn)
        .map(|index| {
            let at = 1 + index * ADVERSARY_BYTES;
            Adversary::new(Role::from_byte(byte(at)), byte(at + 1))
        })
        .collect();
    let fits = (0..=drawn)
        .rev()
        .find(|k| deadline(&progress_mix(&adversaries[..*k])).is_ok())
        .unwrap_or(0);
    progress_mix(&adversaries[..fits])
}

/// The prefix that draws `adversaries` ([`draw_mix`]).
pub fn mix_prefix(adversaries: &[Adversary]) -> Vec<u8> {
    assert!(
        adversaries.len() <= MAX_ADVERSARIES,
        "a prefix draws at most 3 adversaries"
    );
    let mut prefix = vec![
        PROGRESS_VERSION,
        u8::try_from(adversaries.len()).expect("at most 3 adversaries"),
    ];
    for adversary in adversaries {
        prefix.extend([adversary.role.byte(), adversary.timing]);
    }
    prefix.resize(PROGRESS_BYTES, 0);
    prefix
}

/// [`ADVERSARY_PROGRESS`]'s deadline in `scenario`: production's figure for
/// its adversaries, the mirrors after `mirror-0`, and the bodies the honest
/// mirror moves.
pub fn deadline(scenario: &BlockScenario) -> Result<Duration, BlockProgressUnbounded> {
    let adversaries = u32::try_from(scenario.mirrors.len().saturating_sub(1)).unwrap_or(u32::MAX);
    block_progress_deadline(&scenario.node_config(), adversaries, honest_bytes(scenario))
}

/// A fixed role mix: a prefix of the drawn input, which the nightly sim
/// runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mix {
    /// Two silent adversaries, one silent from the start and one after a
    /// read.
    Silent,
    /// Two adversaries whose ranges end unavailable.
    Unavailable,
    /// Two liars, one answering inside the request timeout and one past it.
    Liars,
    /// One adversary of each role.
    Mixed,
}

impl Mix {
    /// Every mix.
    pub const ALL: [Mix; 4] = [Mix::Silent, Mix::Unavailable, Mix::Liars, Mix::Mixed];

    /// Its family name.
    pub fn name(self) -> &'static str {
        match self {
            Mix::Silent => "silent",
            Mix::Unavailable => "unavailable",
            Mix::Liars => "liars",
            Mix::Mixed => "mixed",
        }
    }

    /// Its prefix ([`mix_prefix`]).
    pub fn prefix(self) -> Vec<u8> {
        use Role::*;
        let adversaries: &[Adversary] = match self {
            Mix::Silent => &[Adversary::new(Silent, 0), Adversary::new(Silent, 1)],
            Mix::Unavailable => &[
                Adversary::new(Unavailable, 0),
                Adversary::new(Unavailable, 2),
            ],
            // 6 s and 15 s.
            Mix::Liars => &[Adversary::new(Liar, 11), Adversary::new(Liar, 29)],
            Mix::Mixed => &[
                Adversary::new(Silent, 0),
                Adversary::new(Unavailable, 1),
                Adversary::new(Liar, 29),
            ],
        };
        mix_prefix(adversaries)
    }

    /// Its scenario.
    pub fn scenario(self) -> BlockScenario {
        BlockProgress::draw(&self.prefix())
    }
}

/// What [`ADVERSARY_PROGRESS`] reads: the run's deadline and the last
/// height the node needs.
#[derive(Clone, Copy, Debug)]
pub struct ProgressView {
    /// [`deadline`].
    pub deadline: Result<Duration, BlockProgressUnbounded>,
    /// The chain's last height, whose submission ends the node's need.
    pub last: u32,
}

/// `block-sync/storage`'s node and mirrors on a drawn progress mix
/// ([`draw_mix`]).
pub struct BlockProgress {
    storage: Storage,
    view: ProgressView,
}

impl HasView for BlockProgress {
    type View = ProgressView;

    /// The run's deadline and last height.
    fn view(&self) -> ProgressView {
        self.view
    }

    /// The storage harness's.
    fn captured(&self) -> Capture {
        self.storage.captured()
    }

    /// The storage harness's.
    fn ended(&self, end: &RunEnd) {
        self.storage.ended(end);
    }
}

impl Subsystem for BlockProgress {
    const NAME: &'static str = "block-sync/progress";
    // A u8 always fits in a u32.
    const GENERATOR: u32 = PROGRESS_VERSION as u32;
    const SCENARIO_BYTES: usize = PROGRESS_BYTES;
    const COMPOSES: &'static [&'static str] = &[];
    type Scenario = BlockScenario;
    type Prepared = Ports;

    /// [`storage_config`].
    fn config(scenario: &BlockScenario) -> RunConfig {
        storage_config(scenario)
    }

    /// [`Storage`]'s.
    fn prepare(scenario: &BlockScenario) -> Ports {
        Storage::prepare(scenario)
    }

    /// [`Storage`]'s, then a time boundary one millisecond past the
    /// deadline, so the clock reaches it even when no timer would.
    fn build(sim: &mut Sim, scenario: &BlockScenario, ports: Ports) -> Self {
        let storage = Storage::build(sim, scenario, ports);
        let view = ProgressView {
            deadline: deadline(scenario),
            last: scenario.blocks,
        };
        if let Ok(deadline) = view.deadline {
            sim.add_source(Box::new(DeadlineBoundary {
                at: SimInstant::ZERO
                    .checked_add(deadline + Duration::from_secs(1))
                    .expect("a deadline inside the park fits the clock"),
            }));
        }
        BlockProgress { storage, view }
    }

    /// [`storage_properties`], then [`ADVERSARY_PROGRESS`].
    fn properties() -> Vec<Property<Self>> {
        let mut properties = storage_properties();
        properties.push(adversary_progress());
        properties
    }

    /// [`storage_sources`].
    fn sources() -> Sources {
        storage_sources()
    }

    /// `HonestPeer` for `mirror-0`, `BoundedStorage` for the converging
    /// policy, and `StableMembership`: no mirror relinks, so each parked
    /// adversary stays parked through the deadline, which fits inside the
    /// park. A prefix whose deadline outlasts the park declares nothing.
    fn assumptions(scenario: &BlockScenario) -> Assumptions<Self> {
        let Ok(deadline) = deadline(scenario) else {
            return Assumptions::declare(Vec::new());
        };
        Assumptions::declare(vec![
            AssumeRecord {
                assume: Assume::HonestPeer,
                params: serde_json::json!({ "peers": ["mirror-0"] }),
            },
            AssumeRecord {
                assume: Assume::BoundedStorage,
                params: serde_json::json!({ "policy": "converging" }),
            },
            AssumeRecord {
                assume: Assume::StableMembership,
                params: serde_json::json!({
                    "relinks": 0,
                    "deadline_ms": u64::try_from(deadline.as_millis()).unwrap_or(u64::MAX),
                    "park_ms": u64::try_from(PARK.as_millis()).unwrap_or(u64::MAX),
                }),
            },
        ])
    }

    /// Each fixed mix's prefix ([`Mix::prefix`]).
    fn families() -> Vec<ScenarioFamily> {
        Mix::ALL
            .into_iter()
            .map(|mix| ScenarioFamily::new(mix.name(), mix.prefix()))
            .collect()
    }

    /// Whether `prefix` starts with [`PROGRESS_VERSION`].
    fn accepts(prefix: &[u8]) -> bool {
        prefix.first() == Some(&PROGRESS_VERSION)
    }

    /// [`draw_mix`] of the bytes after [`PROGRESS_VERSION`], or the honest
    /// mirror alone for a prefix with another first byte.
    fn draw(prefix: &[u8]) -> BlockScenario {
        match prefix.split_first() {
            Some((&PROGRESS_VERSION, grammar)) => draw_mix(grammar),
            _ => progress_mix(&[]),
        }
    }
}

/// [`ADVERSARY_PROGRESS`], as `Draft` until Evan agrees to promote it.
pub fn adversary_progress() -> Property<BlockProgress> {
    Property {
        id: PropertyId::new(ADVERSARY_PROGRESS),
        claim: "once the node starts block work, it submits every body it needs for commit \
                within the deadline production's constants give for k adversaries (one \
                liveness turn per adversary and one for the honest mirror), while the honest \
                mirror serves, storage stays bounded and each parked adversary stays parked, \
                which holds only while the deadline fits inside the 180 s park",
        kind: Kind::Liveness {
            bound: LivenessBound::VirtualTime(PARK),
        },
        assumes: &[
            Assume::HonestPeer,
            Assume::BoundedStorage,
            Assume::StableMembership,
        ],
        needs: &[Source::Storage, Source::View],
        status: Status::Draft,
        check: check_adversary_progress,
        witness: None,
    }
}

/// The node's first block storage submission in `entries`, by step.
fn first_submission(entries: &[crate::observe::Entry]) -> Option<u64> {
    entries
        .iter()
        .find(|entry| {
            matches!(
                entry.obs,
                Observation::Storage {
                    fact: StorageFact::BlockSubmitted { .. },
                    ..
                }
            )
        })
        .map(|entry| entry.step)
}

/// Checks [`ADVERSARY_PROGRESS`]. At the step of the node's first storage
/// submission, it opens one obligation on the chain's last height under the
/// run's deadline, which the node's `SubmitBlock` of that height
/// discharges. The sequencer submits bodies in height order, so that one
/// comes last. Every other step is `NotApplicable`.
fn check_adversary_progress(cx: &mut CheckCtx<'_, BlockProgress>) -> Verdict {
    let submitted = cx.observations().any(|obs| {
        matches!(
            obs,
            Observation::Storage {
                fact: StorageFact::BlockSubmitted { .. },
                ..
            }
        )
    });
    if !submitted || first_submission(cx.log.entries()) != Some(cx.step) {
        return Verdict::NotApplicable;
    }
    let view = cx.view();
    let deadline = match view.deadline {
        Ok(deadline) => deadline,
        Err(why) => {
            cx.missing_evidence(format!(
                "production's constants give no progress deadline: {why:?}"
            ));
            return Verdict::NotApplicable;
        }
    };
    let mut obligation = cx.obligation(
        None,
        Subject::Owner(format!("height {}", view.last)),
        submitted_last,
    );
    obligation.bound = LivenessBound::VirtualTime(deadline);
    cx.oblige(obligation);
    Verdict::Holds
}

/// Whether `obs` is the node's `SubmitBlock` of the height the obligation
/// `key` names.
fn submitted_last(key: &ObligationKey, obs: &Observation) -> bool {
    let Subject::Owner(subject) = &key.subject else {
        return false;
    };
    let Some(last) = subject
        .strip_prefix("height ")
        .and_then(|height| height.parse::<u32>().ok())
    else {
        return false;
    };
    matches!(
        obs,
        Observation::Storage {
            fact: StorageFact::BlockSubmitted {
                request: StorageRequest::SubmitBlock {
                    height: Some(height),
                    ..
                },
                ..
            },
            ..
        } if *height == last
    )
}

/// Offers one time boundary, so the clock reaches the deadline even when no
/// timer would. It offers no action.
struct DeadlineBoundary {
    at: SimInstant,
}

impl ActionSource for DeadlineBoundary {
    /// None: it only moves the clock.
    fn enabled(&self, _now: SimInstant, _out: &mut Vec<(Identity, u64)>) {}

    /// The boundary, while it's ahead of `now`.
    fn boundaries(&self, now: SimInstant, out: &mut Vec<Boundary>) {
        if self.at > now {
            out.push(Boundary::Opens(self.at));
        }
    }

    /// Never called: it offers no action.
    fn execute(&mut self, _local: u64, _log: &mut ObservationLog) {
        unreachable!("the deadline boundary offers no action")
    }

    /// The same boundary, so the drain's clock reaches it too.
    fn recovery(&self, now: SimInstant, out: &mut crate::RecoveryOffer) {
        if self.at > now {
            out.boundaries.push(self.at);
        }
    }

    /// Nothing: it has no holds.
    fn enter_drain(&mut self, _log: &mut ObservationLog) {}

    /// None: it holds no work.
    fn holds_work(&self) -> Option<String> {
        None
    }
}
