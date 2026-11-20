//! `header-sync/progress`: step 10 on `header-sync/wire`'s progress fixture.
//! An honest peer offers the node its one-header successor while k
//! adversaries hold the node's attention, and
//! `header-sync/honest-progress` holds the node to the deadline production's
//! constants give for k ([`progress_bound`](super::progress_bound)).
//!
//! The scenario prefix draws k, the honest peer's dial delay, and each
//! adversary's role and timing ([`progress_mix`]). The fixed role mixes are
//! prefixes of the same input ([`Mix`]), which the nightly sim runs
//! (`tests/header_progress.rs`).
//!
//! Each adversary dials once, under its own key. #2604's production-machine
//! test `reopening_silent_servers_cannot_hold_the_honest_tip_past_the_deadline`
//! covers adversaries that reopen under fresh keys, which the simulated
//! peers can't yet do.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::{
    wire_assumptions, wire_config, wire_holds, wire_properties, HeaderWire, StorageView, WirePorts,
    WireScenario, PROGRESS_LINK, PROGRESS_STORAGE_BOUND_MS,
};
use crate::{
    harness::{
        header_node::{Base, Chain, Policy},
        NetScenario, ScenarioFamily, Subsystem,
    },
    peer::{
        adversary::AdversarySpec, handshake::options::HandshakeSpec, honest::HonestSpec, PeerSpec,
    },
    property::{Assumptions, Capture, EndViolation, HasView, Property, Sources},
    ObservationLog, RunConfig, RunEnd, Sim,
};

/// The headers the node holds when a progress run starts.
const NODE_TIP: u32 = 50;

/// The first byte of every `header-sync/progress` prefix: its generator.
pub const PROGRESS_VERSION: u8 = 1;

/// The most adversaries a prefix draws.
pub const MAX_ADVERSARIES: usize = 3;

/// The bytes that draw one adversary: its role, its dial delay and its
/// role's timing.
const ADVERSARY_BYTES: usize = 3;

/// The length of a `header-sync/progress` prefix: its version byte, k, the
/// honest peer's dial delay, then [`ADVERSARY_BYTES`] for each of up to
/// [`MAX_ADVERSARIES`].
pub const PROGRESS_BYTES: usize = 3 + MAX_ADVERSARIES * ADVERSARY_BYTES;

/// How much each dial-delay byte delays a dial.
const DIAL_STEP_MS: u64 = 100;

/// How much each timing byte spaces an intermittent adversary's pages,
/// beyond the first step.
const DRIP_STEP_MS: u64 = 500;

/// The intermittent pages' spacing steps a timing byte picks among, so the
/// spacing runs from 0.5 s to 40 s, past the 30 s request timeout.
const DRIP_STEPS: u8 = 80;

/// How many headers past the honest successor an intermittent adversary
/// holds, so it has pages to drip without reaching its target.
const DRIP_EXTRA: u32 = 3;

/// One adversary's role.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Role {
    /// Announces the honest successor, then serves no page.
    Silent,
    /// Announces a longer chain than the honest one, then serves no page.
    FalseTip,
    /// Announces a longer chain and serves it one header at a time, with
    /// its timing byte's spacing, never reaching its target.
    Intermittent,
    /// Announces and serves the honest successor's height with an invalid
    /// header there.
    Invalid,
}

impl Role {
    /// Every role, in the order a role byte picks them.
    pub const ALL: [Role; 4] = [
        Role::Silent,
        Role::FalseTip,
        Role::Intermittent,
        Role::Invalid,
    ];

    /// The role `byte` picks.
    fn from_byte(byte: u8) -> Role {
        Role::ALL[usize::from(byte) % Role::ALL.len()]
    }

    /// The byte that picks this role.
    fn byte(self) -> u8 {
        match self {
            Role::Silent => 0,
            Role::FalseTip => 1,
            Role::Intermittent => 2,
            Role::Invalid => 3,
        }
    }
}

/// One drawn adversary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Adversary {
    /// What it does.
    pub role: Role,
    /// Its dial delay, in [`DIAL_STEP_MS`] steps.
    pub delay: u8,
    /// Its role's timing: an intermittent adversary's page spacing, in
    /// [`DRIP_STEP_MS`] steps past the first. Other roles ignore it.
    pub timing: u8,
}

impl Adversary {
    /// `role`, dialing after `delay` steps, with `timing`.
    pub const fn new(role: Role, delay: u8, timing: u8) -> Adversary {
        Adversary {
            role,
            delay,
            timing,
        }
    }

    /// The adversary's peer: `index` among the adversaries, so false tips
    /// claim distinct chains.
    fn peer(self, index: u32) -> PeerSpec {
        let quiet = AdversarySpec {
            announce: true,
            serve: false,
            withhold: false,
            drip_ms: None,
        };
        let successor = NODE_TIP + 1;
        let (chain, adversary, invalid_tip) = match self.role {
            Role::Silent => (successor, Some(quiet), false),
            Role::FalseTip => (successor + 1 + index, Some(quiet), false),
            Role::Intermittent => (
                successor + DRIP_EXTRA,
                Some(AdversarySpec {
                    serve: true,
                    drip_ms: Some(DRIP_STEP_MS * (1 + u64::from(self.timing % DRIP_STEPS))),
                    ..quiet
                }),
                false,
            ),
            Role::Invalid => (successor, None, true),
        };
        PeerSpec {
            honest: Some(HonestSpec {
                chain,
                subscribe: None,
            }),
            adversary,
            invalid_tip,
            handshake: Some(dial_after(self.delay)),
            ..PeerSpec::dialing()
        }
    }
}

/// A handshake that only waits `delay` [`DIAL_STEP_MS`] steps before it
/// dials.
fn dial_after(delay: u8) -> HandshakeSpec {
    HandshakeSpec {
        dial_delay_ms: u64::from(delay) * DIAL_STEP_MS,
        ..HandshakeSpec::default()
    }
}

/// The progress fixture with an honest peer that dials after
/// `honest_delay` [`DIAL_STEP_MS`] steps, and `adversaries` after it, as
/// `peer-1` on.
pub fn progress_mix(honest_delay: u8, adversaries: &[Adversary]) -> WireScenario {
    let honest = PeerSpec {
        honest: Some(HonestSpec {
            chain: NODE_TIP + 1,
            subscribe: None,
        }),
        handshake: Some(dial_after(honest_delay)),
        ..PeerSpec::dialing()
    };
    let peers = std::iter::once(honest)
        .chain(
            (0..)
                .zip(adversaries)
                .map(|(index, adversary)| adversary.peer(index)),
        )
        .collect();
    WireScenario {
        net: NetScenario {
            default_link: PROGRESS_LINK,
            ..NetScenario::default()
        },
        node: Chain::main(NODE_TIP),
        peers,
        policy: Policy {
            base: Base::SuccessOnly,
            bound_ms: Some(PROGRESS_STORAGE_BOUND_MS),
            holds: Vec::new(),
            reply_holds: Vec::new(),
            disruptions: Vec::new(),
        },
        budget: None,
    }
}

/// The scenario the bytes after a prefix's version byte draw, read as zeros
/// past their end: k, modulo one more than [`MAX_ADVERSARIES`], then the
/// honest peer's dial delay, then each adversary's role, dial delay and
/// timing.
pub fn draw_mix(grammar: &[u8]) -> WireScenario {
    let byte = |index: usize| grammar.get(index).copied().unwrap_or(0);
    let k = usize::from(byte(0)) % (MAX_ADVERSARIES + 1);
    let adversaries: Vec<Adversary> = (0..k)
        .map(|index| {
            let at = 2 + index * ADVERSARY_BYTES;
            Adversary::new(Role::from_byte(byte(at)), byte(at + 1), byte(at + 2))
        })
        .collect();
    progress_mix(byte(1), &adversaries)
}

/// The prefix that draws `adversaries` beside an honest peer that dials
/// after `honest_delay` steps ([`draw_mix`]).
pub fn mix_prefix(honest_delay: u8, adversaries: &[Adversary]) -> Vec<u8> {
    assert!(
        adversaries.len() <= MAX_ADVERSARIES,
        "a prefix draws at most 3 adversaries"
    );
    let mut prefix = vec![
        PROGRESS_VERSION,
        u8::try_from(adversaries.len()).expect("at most 3 adversaries"),
        honest_delay,
    ];
    for adversary in adversaries {
        prefix.extend([adversary.role.byte(), adversary.delay, adversary.timing]);
    }
    prefix.resize(PROGRESS_BYTES, 0);
    prefix
}

/// A fixed role mix: a prefix of the drawn input, which the nightly sim
/// runs. In each, the adversaries dial before the honest peer, so their
/// offers wait ahead of its own.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mix {
    /// Three silent servers of the honest successor: the honest peer waits
    /// one stall per adversary.
    Silent,
    /// Two false tips and a silent server.
    FalseTips,
    /// Intermittent pages, one spaced inside the request timeout and one
    /// past it.
    Intermittent,
    /// Invalid content beside a silent server.
    Invalid,
    /// One adversary of each role but the invalid one.
    Mixed,
}

impl Mix {
    /// Every mix.
    pub const ALL: [Mix; 5] = [
        Mix::Silent,
        Mix::FalseTips,
        Mix::Intermittent,
        Mix::Invalid,
        Mix::Mixed,
    ];

    /// Its family name.
    pub fn name(self) -> &'static str {
        match self {
            Mix::Silent => "silent",
            Mix::FalseTips => "false-tips",
            Mix::Intermittent => "intermittent",
            Mix::Invalid => "invalid",
            Mix::Mixed => "mixed",
        }
    }

    /// Its prefix ([`mix_prefix`]). The honest peer dials 2 s in.
    pub fn prefix(self) -> Vec<u8> {
        use Role::*;
        let adversaries: &[Adversary] = match self {
            Mix::Silent => &[
                Adversary::new(Silent, 0, 0),
                Adversary::new(Silent, 1, 0),
                Adversary::new(Silent, 2, 0),
            ],
            Mix::FalseTips => &[
                Adversary::new(FalseTip, 0, 0),
                Adversary::new(FalseTip, 1, 0),
                Adversary::new(Silent, 2, 0),
            ],
            // 5 s and 35 s spacing.
            Mix::Intermittent => &[
                Adversary::new(Intermittent, 0, 9),
                Adversary::new(Intermittent, 1, 69),
            ],
            Mix::Invalid => &[Adversary::new(Invalid, 0, 0), Adversary::new(Silent, 1, 0)],
            Mix::Mixed => &[
                Adversary::new(Silent, 0, 0),
                Adversary::new(FalseTip, 1, 0),
                Adversary::new(Intermittent, 2, 69),
            ],
        };
        mix_prefix(20, adversaries)
    }

    /// Its scenario.
    pub fn scenario(self) -> WireScenario {
        HeaderProgress::draw(&self.prefix())
    }
}

/// `header-sync/wire`'s node on a drawn progress mix ([`draw_mix`]).
pub struct HeaderProgress(HeaderWire);

impl HasView for HeaderProgress {
    type View = StorageView;

    /// [`HeaderWire`]'s.
    fn view(&self) -> StorageView {
        self.0.view()
    }

    /// [`HeaderWire`]'s.
    fn captured(&self) -> Capture {
        self.0.captured()
    }

    /// [`HeaderWire`]'s.
    fn ended(&self, end: &RunEnd) {
        self.0.ended(end);
    }

    /// [`HeaderWire`]'s.
    fn end_violations(&self, log: &ObservationLog) -> Vec<EndViolation> {
        self.0.end_violations(log)
    }

    /// [`HeaderWire`]'s.
    fn stamps_after(&self, after: Option<u64>) -> Option<Vec<u64>> {
        self.0.stamps_after(after)
    }
}

impl Subsystem for HeaderProgress {
    const NAME: &'static str = "header-sync/progress";
    // A u8 always fits in a u32.
    const GENERATOR: u32 = PROGRESS_VERSION as u32;
    const SCENARIO_BYTES: usize = PROGRESS_BYTES;
    const COMPOSES: &'static [&'static str] = &[HeaderWire::NAME];
    type Scenario = WireScenario;
    type Prepared = WirePorts;

    /// [`wire_config`], with room for one stall-rule turn per adversary: each
    /// runs 30 virtual seconds of QUIC keep-alives and `Status` refreshes.
    fn config(scenario: &WireScenario) -> RunConfig {
        let config = wire_config();
        let turns = u64::try_from(scenario.peers.len()).unwrap_or(u64::MAX);
        RunConfig {
            budget: config.budget.saturating_mul(turns.max(1)),
            ..config
        }
    }

    /// [`HeaderWire`]'s.
    fn prepare(scenario: &WireScenario) -> WirePorts {
        HeaderWire::prepare(scenario)
    }

    /// [`HeaderWire`]'s.
    fn build(sim: &mut Sim, scenario: &WireScenario, ports: WirePorts) -> Self {
        HeaderProgress(HeaderWire::build(sim, scenario, ports))
    }

    /// [`wire_properties`], `header-sync/honest-progress` among them.
    fn properties() -> Vec<Property<Self>> {
        wire_properties()
    }

    /// [`HeaderWire`]'s.
    fn sources() -> Sources {
        HeaderWire::sources()
    }

    /// [`HeaderWire`]'s, which declare the progress fixture's.
    fn assumptions(scenario: &WireScenario) -> Assumptions<Self> {
        Assumptions::declare(wire_assumptions(scenario))
            .holds_when(|progress, assume| wire_holds(&progress.0, assume))
    }

    /// One family, `drawn`, whose tag is [`PROGRESS_VERSION`]: the bytes
    /// after it draw every mix, the fixed ones ([`Mix::prefix`]) among them.
    fn families() -> Vec<ScenarioFamily> {
        vec![ScenarioFamily::new("drawn", vec![PROGRESS_VERSION])]
    }

    /// Whether `prefix` starts with [`PROGRESS_VERSION`].
    fn accepts(prefix: &[u8]) -> bool {
        prefix.first() == Some(&PROGRESS_VERSION)
    }

    /// [`draw_mix`] of the bytes after [`PROGRESS_VERSION`], or the honest
    /// peer alone for a prefix with another first byte.
    fn draw(prefix: &[u8]) -> WireScenario {
        match prefix.split_first() {
            Some((&PROGRESS_VERSION, grammar)) => draw_mix(grammar),
            _ => progress_mix(0, &[]),
        }
    }
}

/// [`Duration`] of `steps` dial-delay steps, for tests that wait for a mix's
/// last dial.
pub fn dial_delay(steps: u8) -> Duration {
    Duration::from_millis(u64::from(steps) * DIAL_STEP_MS)
}
