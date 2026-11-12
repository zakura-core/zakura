//! A peer whose page passes the node's wire checks and fails preparation, on
//! `header-sync/wire`: it serves a chain whose tip header has a target easier
//! than Regtest allows ([`HonestChain::with_invalid_tip`]). A steered plan
//! can disconnect it while the node prepares that page.
//!
//! [`HonestChain::with_invalid_tip`]: zakura_sim::peer::honest::HonestChain::with_invalid_tip

use std::{
    cell::{OnceCell, RefCell},
    rc::Rc,
};

use super::steer::{before, Steer};
use zakura_network::zakura::ZakuraHeaderSyncConfig;
use zakura_sim::stores::EvidenceStores;
use zakura_sim::{
    harness::{
        header_node::{
            fault::{CooldownEvidence, FaultEvidence},
            policy::{Allow, Hold, OpKind, Select, Trigger},
            Chain, Policy,
        },
        header_sync::{wire_properties, HeaderWire, StorageView, WirePorts, WireScenario},
        Subsystem,
    },
    observe::{StorageFact, StorageRequest},
    peer::{honest::HonestSpec, PeerAction, PeerSpec},
    property::{EndViolation, HasView, Property, Sources},
    Observation, ObservationLog, RunConfig, RunEnd, Sim, StepCtx,
};

/// The node's chain at the start of each run.
pub const PRELOAD: u32 = 50;

/// The peer's chain, whose tip is invalid.
pub const CHAIN: u32 = 120;

/// The peer's disconnect.
pub const DISCONNECT: &str = "peer:peer-0:disconnect";

/// The peer's redial after its connection closed.
pub const REDIAL: &str = "peer:peer-0:redial";

/// How long after its submission the node's first preparation is held,
/// long enough for a disconnect picked meanwhile to reach the reactor.
pub const HOLD_MS: u64 = 2_000;

/// The node at [`PRELOAD`], dialed by `peer-0`, which serves [`CHAIN`]
/// headers with an invalid tip and then runs `script`. With `hold`, the
/// node's first preparation completes only [`HOLD_MS`] after its
/// submission, and then only by the engine.
pub fn scenario(script: Vec<PeerAction>, hold: bool) -> WireScenario {
    let holds = if hold {
        vec![Hold {
            select: Select::nth(OpKind::Prepare, 1),
            until: Trigger::VirtualTime(HOLD_MS),
            then: vec![Allow::Engine],
            deadline_ms: None,
        }]
    } else {
        Vec::new()
    };
    WireScenario {
        node: Chain::main(PRELOAD),
        peers: vec![PeerSpec {
            honest: Some(HonestSpec {
                chain: CHAIN,
                subscribe: None,
            }),
            invalid_tip: true,
            script,
            ..PeerSpec::dialing()
        }],
        policy: Policy {
            holds,
            ..Policy::converging()
        },
        ..WireScenario::default()
    }
}

/// True if the node submitted a preparation before `cx`'s step.
fn prepare_submitted(cx: &StepCtx<'_>) -> bool {
    before(cx).iter().any(|entry| {
        matches!(
            &entry.obs,
            Observation::Storage {
                instance: 0,
                fact: StorageFact::Submitted {
                    request: StorageRequest::PrepareHeaderTarget { .. },
                    ..
                },
                ..
            }
        )
    })
}

/// A steering rule: the peer may disconnect only once the node has
/// submitted a preparation, and then does as soon as it's offered.
pub fn disconnect_mid_prepare(cx: &StepCtx<'_>) -> Result<Steer, String> {
    let submitted = prepare_submitted(cx);
    let offered = cx
        .enabled
        .iter()
        .any(|enabled| enabled.identity.to_string() == DISCONNECT);
    if submitted && offered {
        return Ok(Steer::Pick(DISCONNECT.to_owned()));
    }
    if !submitted && cx.pick.to_string() == DISCONNECT {
        return cx
            .enabled
            .iter()
            .map(|enabled| enabled.identity.to_string())
            .find(|identity| identity != DISCONNECT)
            .map(Steer::Pick)
            .ok_or_else(|| "only the disconnect is enabled".to_owned());
    }
    Ok(Steer::Fine)
}

/// [`disconnect_mid_prepare`], then a redial as soon as one is offered. The
/// peer dials again a settle time after its close, while the cooldown its
/// fault started still holds.
pub fn redial_after_disconnect(cx: &StepCtx<'_>) -> Result<Steer, String> {
    match disconnect_mid_prepare(cx)? {
        Steer::Fine
            if cx
                .enabled
                .iter()
                .any(|enabled| enabled.identity.to_string() == REDIAL) =>
        {
            Ok(Steer::Pick(REDIAL.to_owned()))
        }
        steer => Ok(steer),
    }
}

/// The last view a run's checks read.
pub type LastView = Rc<RefCell<Option<StorageView>>>;

/// [`HeaderWire`], keeping the last view its checks read and recording the
/// run's end in it, so a test can judge the view-side evidence after the
/// run.
pub type Watched = Watching<false>;

/// [`Watched`], but its view's fault evidence never sees the node's
/// cooldowns, as a node that installs none would show. It's the planted
/// bypass of `header-sync/header-fault-penalized`.
pub type Uncooled = Watching<true>;

/// [`Watched`], or with `UNCOOLED`, [`Uncooled`].
pub struct Watching<const UNCOOLED: bool> {
    wire: HeaderWire,
    last: LastView,
    /// The fault evidence an [`Uncooled`] view gives, made at its first
    /// read: the node's attributions, with no cooldowns.
    uncooled: OnceCell<FaultEvidence>,
}

impl<const UNCOOLED: bool> HasView for Watching<UNCOOLED> {
    type View = StorageView;

    /// The wire harness's view, also kept as the last one read.
    fn view(&self) -> StorageView {
        let mut view = self.wire.view();
        if UNCOOLED {
            view.faults = self
                .uncooled
                .get_or_init(|| {
                    FaultEvidence::new(
                        view.faults.attributions().clone(),
                        CooldownEvidence::new(&EvidenceStores::default()),
                        view.faults.ends().clone(),
                        ZakuraHeaderSyncConfig::default().unproductive_peer_cooldown,
                    )
                })
                .clone();
        }
        *self.last.borrow_mut() = Some(view.clone());
        view
    }

    /// The wire harness's record of the run's end.
    fn ended(&self, end: &RunEnd) {
        self.wire.ended(end);
    }

    /// The wire harness's end-of-run violations, judged from the node's
    /// own fault evidence. An [`Uncooled`] view's break comes from its
    /// checks first, so the registry keeps that one.
    fn end_violations(&self, log: &ObservationLog) -> Vec<EndViolation> {
        self.wire.end_violations(log)
    }
}

impl<const UNCOOLED: bool> Subsystem for Watching<UNCOOLED> {
    const NAME: &'static str = if UNCOOLED {
        "test/header-wire-uncooled"
    } else {
        "test/header-wire-watched"
    };
    const GENERATOR: u32 = HeaderWire::GENERATOR;
    const SCENARIO_BYTES: usize = HeaderWire::SCENARIO_BYTES;
    type Scenario = WireScenario;
    type Prepared = (WirePorts, LastView);

    /// The wire harness's config.
    fn config(scenario: &WireScenario) -> RunConfig {
        HeaderWire::config(scenario)
    }

    /// The wire harness's ports, and where the last view goes.
    fn prepare(scenario: &WireScenario) -> Self::Prepared {
        (HeaderWire::prepare(scenario), LastView::default())
    }

    /// The wire harness.
    fn build(sim: &mut Sim, scenario: &WireScenario, (ports, last): Self::Prepared) -> Self {
        Watching {
            wire: HeaderWire::build(sim, scenario, ports),
            last,
            uncooled: OnceCell::new(),
        }
    }

    /// The wire harness's properties.
    fn properties() -> Vec<Property<Self>> {
        wire_properties()
    }

    /// The wire harness's sources.
    fn sources() -> Sources {
        HeaderWire::sources()
    }

    /// Unused: these scenarios are built by hand.
    fn draw(_: &[u8]) -> WireScenario {
        unreachable!("prepare-fault scenarios aren't drawn")
    }
}
