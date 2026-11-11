//! `header-sync/repair`: `header-sync/storage` on the repair fixtures
//! ([`RepairScenario`]), where the node under test runs a VCT body repair
//! against a mirror that, in all but one fixture, holds the repaired
//! headers' tree records. Its one
//! scenario byte picks the fixture, and the rest of an input is Plan bytes.
//!
//! Every fixture first makes one stale prepare, so
//! `header-sync/stale-completion-inert` applies, then publishes the repair.
//! In the first three, the repair round is kept when its peer leaves while
//! it prepares or applies, so its calls are never stale, and its apply
//! still succeeds. In [`RepairScenario::SupplierPruned`] the supplier lacks the tree
//! records, so the repair fails and charges it, and the supplier's
//! departure exercises the repair requirement's held history for
//! `header-sync/outstanding-work-bounded`.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use zakura_chain::block;
use zakura_storage::header::{adapter::EnginePort, controlled::ControlledPort};

use super::{
    storage_config, storage_properties, storage_sources, Dialer, MirrorSpec, Ports, Storage,
    StorageScenario, StorageView, Weights,
};
use crate::{
    harness::{
        header_node::{
            policy::{Allow, Hold, OpKind, Select, Trigger},
            preload, preload_with_tree_aux, Chain, ControlAction, ControlStep, Policy,
        },
        Subsystem,
    },
    property::{EndViolation, HasView, Property, Sources},
    ObservationLog, RunConfig, RunEnd, Sim,
};

/// The node's and the supplier's tip, and the node's checkpoint ceiling, so
/// the repair may cover every header the node holds.
pub const TIP: u32 = 3;

/// The height whose tree data the node reports missing. The repair covers
/// it and the selected suffix past it, up to [`TIP`].
pub const REPAIR_HEIGHT: u32 = 2;

/// The mirror that supplies the repair. It holds the repaired headers'
/// tree records in every fixture but [`RepairScenario::SupplierPruned`].
pub const SUPPLIER: &str = "mirror-0";

/// The mirror one header ahead whose round's prepare ends stale.
pub const STALE_PEER: &str = "mirror-1";

/// One of the repair fixtures. In each, [`STALE_PEER`] offers the node one
/// new header. Its round's prepare is held until its session ends, so the
/// prepare ends stale and its header never applies. Once that prepare is
/// handled, the node reports [`REPAIR_HEIGHT`]'s tree data missing, and
/// [`SUPPLIER`] serves the repair.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RepairScenario {
    /// The repair prepares and applies with its supplier connected.
    Applied,
    /// The supplier leaves while the repair prepares. The node keeps the
    /// round, and the prepare's reply goes on to the apply.
    PrepareDeparts,
    /// The supplier leaves while the repair applies. The node keeps the
    /// round, and the apply's reply settles it.
    ApplyDeparts,
    /// The supplier holds no tree records, so it answers the repair
    /// `HistoryPruned`, and the handler that retires the repair round
    /// charges it as tried. Its close is offered once that round has
    /// retired. The reactor handles the departure only after that handler
    /// finishes, so the charge comes first and the node holds the
    /// supplier's history as a departed one's. The step has no deadline,
    /// so a Plan that keeps it waiting until the step budget ends the run
    /// before the departure. The repair never applies.
    SupplierPruned,
}

impl RepairScenario {
    /// The storage scenario this fixture runs. The node's checkpoint
    /// ceiling and the supplier's records are set on their ports in
    /// [`RepairHarness::prepare`], since a storage scenario has neither.
    pub fn storage(self) -> StorageScenario {
        let prepare = |nth| Select::nth(OpKind::Prepare, nth);
        let apply = |nth| Select::nth(OpKind::Apply, nth);
        let ended = |peer: &str| Trigger::SessionEnded(peer.to_owned());
        let hold = |select, peer: &str| Hold {
            select,
            until: ended(peer),
            then: vec![Allow::Engine],
            deadline_ms: None,
        };
        let step = |peer: &str, action, when| ControlStep {
            peer: peer.to_owned(),
            action,
            when: Some(when),
        };
        // The stale round's prepare is the run's first. No apply comes
        // before the repair's, which is the run's first apply.
        let mut holds = vec![hold(prepare(1), STALE_PEER)];
        let mut controls = vec![
            step(
                STALE_PEER,
                ControlAction::Close,
                Trigger::Submitted(prepare(1)),
            ),
            step(
                "node",
                ControlAction::RepairStatus {
                    unavailable: Some(REPAIR_HEIGHT),
                    generation: 0,
                },
                Trigger::Handled(prepare(1)),
            ),
        ];
        let departs = match self {
            RepairScenario::Applied | RepairScenario::SupplierPruned => None,
            RepairScenario::PrepareDeparts => Some(prepare(2)),
            RepairScenario::ApplyDeparts => Some(apply(1)),
        };
        if self == RepairScenario::SupplierPruned {
            controls.push(step(
                SUPPLIER,
                ControlAction::Close,
                Trigger::Retired(SUPPLIER.to_owned()),
            ));
        }
        if let Some(select) = departs {
            holds.push(hold(select.clone(), SUPPLIER));
            controls.push(step(
                SUPPLIER,
                ControlAction::Close,
                Trigger::Submitted(select),
            ));
        }
        StorageScenario {
            node: Chain::main(TIP),
            mirrors: vec![
                MirrorSpec {
                    chain: Chain::main(TIP),
                    dialer: Dialer::Mirror,
                    max_headers_per_response: None,
                },
                MirrorSpec {
                    chain: Chain::main(TIP + 1),
                    dialer: Dialer::Mirror,
                    max_headers_per_response: None,
                },
            ],
            policy: Policy {
                holds,
                ..Policy::converging()
            },
            controls,
            weights: Weights::default(),
        }
    }
}

/// `header-sync/storage` on a [`RepairScenario`], with the node's
/// checkpoint ceiling at [`TIP`] and [`SUPPLIER`]'s headers carrying tree
/// records, except in [`RepairScenario::SupplierPruned`].
pub struct RepairHarness(Storage);

impl HasView for RepairHarness {
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

    /// The storage harness's wake facts' stamps.
    fn stamps_after(&self, after: Option<u64>) -> Option<Vec<u64>> {
        self.0.stamps_after(after)
    }
}

impl Subsystem for RepairHarness {
    const NAME: &'static str = "header-sync/repair";
    const GENERATOR: u32 = 2;
    const SCENARIO_BYTES: usize = 1;
    type Scenario = RepairScenario;
    type Prepared = Ports;

    /// [`storage_config`].
    fn config(_: &RepairScenario) -> RunConfig {
        storage_config()
    }

    /// The storage harness's ports, with the node's port resolving repairs
    /// up to [`TIP`] and [`SUPPLIER`]'s chain carrying the fixture's tree
    /// records, except in [`RepairScenario::SupplierPruned`].
    fn prepare(scenario: &RepairScenario) -> Ports {
        let storage = scenario.storage();
        let ports = Storage::prepare(&storage);
        let engine = EnginePort::new(&ports.fixture).with_checkpoint_ceiling(block::Height(TIP));
        let node = ControlledPort::over(Arc::new(engine));
        preload(node.engine(), &ports.fixture, &storage.node);
        let supplier = EnginePort::new(&ports.fixture);
        if *scenario == RepairScenario::SupplierPruned {
            preload(&supplier, &ports.fixture, &storage.mirrors[0].chain);
        } else {
            preload_with_tree_aux(&supplier, &ports.fixture, &storage.mirrors[0].chain);
        }
        let mut mirrors = ports.mirrors.clone();
        mirrors[0] = Arc::new(supplier);
        Ports {
            node: Arc::new(node),
            mirrors,
            ..ports
        }
    }

    /// The storage harness on the fixture.
    fn build(sim: &mut Sim, scenario: &RepairScenario, ports: Ports) -> Self {
        RepairHarness(Storage::build(sim, &scenario.storage(), ports))
    }

    /// The storage harness's properties.
    fn properties() -> Vec<Property<Self>> {
        storage_properties()
    }

    /// The storage harness's sources.
    fn sources() -> Sources {
        storage_sources()
    }

    /// The scenario byte modulo 4: 0 [`RepairScenario::Applied`], 1
    /// [`RepairScenario::PrepareDeparts`], 2
    /// [`RepairScenario::ApplyDeparts`], 3
    /// [`RepairScenario::SupplierPruned`].
    fn draw(prefix: &[u8]) -> RepairScenario {
        match prefix.first().map_or(0, |byte| byte % 4) {
            0 => RepairScenario::Applied,
            1 => RepairScenario::PrepareDeparts,
            2 => RepairScenario::ApplyDeparts,
            _ => RepairScenario::SupplierPruned,
        }
    }
}
