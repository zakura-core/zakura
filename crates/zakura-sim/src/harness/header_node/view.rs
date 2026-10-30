//! The header harnesses' owner view (M6a-2): one [`Component`] per row of
//! the tier's [`SourceManifest`], folded by the evidence hub at each step
//! boundary.
//!
//! The reactor reports its own state (M6a-3), folded from its settled
//! snapshots. The budget's counters and the selected chain come from their
//! boundary reads (M6a-7a), and the supervisor map and the header sessions
//! from theirs (M6a-7b, see [`super::reads`]). Each later component
//! registers its read, gives [`Reading`] its state, and wires its
//! existence evidence. Until then the
//! others stay `Initializing`, so a property that needs one never ends
//! `Held`.

use std::collections::BTreeMap;

use zakura_network::zakura::diag::{BudgetCounters, MapEntry, SessionEntry};
use zakura_storage::header::adapter::SelectedChain;

use crate::{
    harness::header_sync::SourceManifest,
    observe::ReactorSnapshot,
    property::{Capture, Component, ComponentKind, Read, TagPhase, Tagged},
};

/// A header component's read state: one variant per component that has a
/// read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Reading {
    /// The reactor's last settled snapshot.
    Reactor(ReactorSnapshot),
    /// The chunk budget's counters.
    Budget(BudgetCounters),
    /// The engine's committed snapshot's generation and frontiers.
    SelectedChain(SelectedChain),
    /// The supervisor's registered connections, by `conn_id`.
    SupervisorMap(Vec<MapEntry>),
    /// The session table of the header-sync service the registry holds,
    /// by connection and session ID.
    HeaderSessions {
        /// The service's allocation ID.
        service: u64,
        /// Each peer's current session.
        sessions: Vec<SessionEntry>,
    },
}

impl Tagged for Reading {
    /// None: no component lists tagged objects yet.
    fn in_flux(&self) -> Option<TagPhase> {
        match self {
            Reading::Reactor(_)
            | Reading::Budget(_)
            | Reading::SelectedChain(_)
            | Reading::SupervisorMap(_)
            | Reading::HeaderSessions { .. } => None,
        }
    }
}

/// What the evidence hub folds at each boundary.
pub(crate) trait Owners {
    /// Ends `step`'s boundary: fails if a fold this step failed, or a live
    /// registered read didn't complete at it.
    fn close(&mut self, step: u64) -> Result<(), String>;
}

/// The owners' state a header harness's checks read: each manifest row's
/// component, as of the last boundary the hub closed.
#[derive(Clone, Debug)]
pub struct HeaderSyncView<R = Reading> {
    manifest: SourceManifest,
    components: BTreeMap<ComponentKind, Component<R>>,
    closed: Option<u64>,
    failure: Option<String>,
}

impl<R: Clone + PartialEq> HeaderSyncView<R> {
    /// A view of `manifest`'s components, each `Initializing` with no read
    /// registered.
    pub fn new(manifest: SourceManifest) -> Self {
        let components = manifest
            .rows()
            .iter()
            .map(|row| (row.kind, Component::new(row.kind)))
            .collect();
        HeaderSyncView {
            manifest,
            components,
            closed: None,
            failure: None,
        }
    }

    /// The tier's manifest.
    pub fn manifest(&self) -> &SourceManifest {
        &self.manifest
    }

    /// The `kind` component, if the tier has it.
    pub fn component(&self, kind: ComponentKind) -> Option<&Component<R>> {
        self.components.get(&kind)
    }

    /// The step whose boundary the hub last closed, as
    /// [`HasView::captured`](crate::property::HasView::captured) reports it.
    pub fn captured(&self) -> Capture {
        self.closed.map_or(Capture::Missing, Capture::AfterStep)
    }

    /// Registers `kind`'s read. A failure ends the boundary as one.
    pub fn register(&mut self, kind: ComponentKind) {
        self.fold(kind, |component| component.register());
    }

    /// Records `kind`'s existence evidence at `step`.
    pub fn exists(&mut self, kind: ComponentKind, step: u64) {
        self.fold(kind, |component| component.exists(step));
    }

    /// Folds `kind`'s read at `step`'s boundary.
    pub fn read(&mut self, kind: ComponentKind, step: u64, read: Read<R>) {
        self.fold(kind, |component| component.read(step, read));
    }

    /// Ends `kind` at `step`'s boundary, seen with `state` or unsettled.
    pub fn end(&mut self, kind: ComponentKind, step: u64, state: Option<R>) {
        self.fold(kind, |component| component.end(step, state));
    }

    /// Runs `fold` on `kind`'s component, keeping the first failure. A
    /// component the manifest lacks is a failure.
    fn fold(
        &mut self,
        kind: ComponentKind,
        fold: impl FnOnce(&mut Component<R>) -> Result<(), String>,
    ) {
        let folded = match self.components.get_mut(&kind) {
            Some(component) => fold(component),
            None => Err(format!("{kind:?} isn't in the tier's manifest")),
        };
        if let Err(why) = folded {
            self.failure.get_or_insert(why);
        }
    }
}

impl<R: Clone + PartialEq> Owners for HeaderSyncView<R> {
    /// Every live registered read must complete at every boundary (rule 7).
    fn close(&mut self, step: u64) -> Result<(), String> {
        self.closed = Some(step);
        if let Some(why) = self.failure.take() {
            return Err(why);
        }
        match self
            .components
            .values()
            .find(|component| component.live() && component.fresh_through() != Some(step))
        {
            Some(component) => Err(format!(
                "SkippedRead: {:?}'s registered read didn't complete at step {step}",
                component.kind()
            )),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc};

    use super::*;
    use crate::{
        check::{Gap, PropertyOutcome},
        harness::{
            evidence_hub::EvidenceHub,
            header_sync::{SourceManifest, Storage, StorageScenario},
            Subsystem,
        },
        property::{
            require, require_every, Assume, AssumeRecord, Assumptions, CheckCtx, HasView, Kind,
            Property, PropertyId, Registry, RegistryChecker, Source, Sources, State, Status,
            Verdict,
        },
        ActionSource, Boundary, HarnessFailure, Identity, MemorySink, NullSink, ObservationLog,
        Plan, RunConfig, RunReport, Sim,
    };

    /// The component the scripted owner folds.
    const BUDGET: ComponentKind = ComponentKind::Budget;

    /// A view of the wire tier's components with `u64` states.
    type Owned = Rc<RefCell<HeaderSyncView<u64>>>;

    /// A harness whose view is the scripted owner's.
    struct Owners(Owned);

    impl HasView for Owners {
        type View = Owned;

        /// The shared view.
        fn view(&self) -> Owned {
            self.0.clone()
        }

        /// The boundary the hub last closed.
        fn captured(&self) -> Capture {
            self.0.borrow().captured()
        }
    }

    /// One fold the scripted owner makes on [`BUDGET`].
    #[derive(Clone)]
    enum Op {
        Register,
        Exists,
        Read(Read<u64>),
    }

    /// An owner that offers `steps` ticks and folds each op at its step's
    /// boundary, before the hub closes it.
    struct Script {
        view: Owned,
        steps: u64,
        taken: u64,
        ops: Vec<(u64, Op)>,
    }

    impl ActionSource for Script {
        fn enabled(&self, _: zakura_scheduler::SimInstant, out: &mut Vec<(Identity, u64)>) {
            if self.taken < self.steps {
                out.push((Identity::peer("owner", "tick"), 0));
            }
        }

        fn boundaries(&self, _: zakura_scheduler::SimInstant, _: &mut Vec<Boundary>) {}

        fn execute(&mut self, _: u64, _: &mut ObservationLog) {
            self.taken += 1;
        }

        fn observe(&mut self, log: &mut ObservationLog) -> Result<(), String> {
            let step = log.step();
            let mut view = self.view.borrow_mut();
            for (_, op) in self.ops.iter().filter(|(at, _)| *at == step) {
                match op.clone() {
                    Op::Register => view.register(BUDGET),
                    Op::Exists => view.exists(BUDGET, step),
                    Op::Read(read) => view.read(BUDGET, step, read),
                }
            }
            Ok(())
        }
    }

    /// Registered and shown to exist at setup, then read at each listed
    /// step.
    fn reads(reads: impl IntoIterator<Item = (u64, Read<u64>)>) -> Vec<(u64, Op)> {
        [(0, Op::Register), (0, Op::Exists)]
            .into_iter()
            .chain(reads.into_iter().map(|(at, read)| (at, Op::Read(read))))
            .collect()
    }

    /// Reads `value` at each of `steps`.
    fn current(steps: impl IntoIterator<Item = u64>, value: u64) -> Vec<(u64, Read<u64>)> {
        steps
            .into_iter()
            .map(|at| (at, Read::Current(value)))
            .collect()
    }

    /// Held at each of `steps`.
    fn held(steps: impl IntoIterator<Item = u64>) -> Vec<(u64, Read<u64>)> {
        steps
            .into_iter()
            .map(|at| {
                (
                    at,
                    Read::Held {
                        holder: "reactor.1".to_owned(),
                    },
                )
            })
            .collect()
    }

    /// `ops` over `steps` ticks under `properties`, with the hub closing
    /// each boundary. Returns the run and the view.
    fn run(
        ops: Vec<(u64, Op)>,
        steps: u64,
        properties: Vec<Property<Owners>>,
        sources: &Sources,
    ) -> (RunReport, Owned) {
        run_assuming(ops, steps, properties, sources, Assumptions::none())
    }

    /// [`run`], with the scenario declaring `assumptions`.
    fn run_assuming(
        ops: Vec<(u64, Op)>,
        steps: u64,
        properties: Vec<Property<Owners>>,
        sources: &Sources,
        assumptions: Assumptions<Owners>,
    ) -> (RunReport, Owned) {
        let view: Owned = Rc::new(RefCell::new(HeaderSyncView::new(SourceManifest::wire())));
        let mut sim = Sim::new(RunConfig::default(), Plan::from_fuzz(&[]));
        let mut hub = EvidenceHub::new();
        hub.owners(view.clone());
        sim.add_source(Box::new(Script {
            view: view.clone(),
            steps,
            taken: 0,
            ops,
        }));
        sim.add_source(Box::new(hub));
        let mut checker = RegistryChecker::new(
            Registry::new(properties).expect("the IDs are unique"),
            Rc::new(Owners(view.clone())),
            assumptions,
            sources,
        );
        (sim.run(&mut checker, &mut MemorySink::default()), view)
    }

    /// The wire tier's sources, with nothing else observed.
    fn wire() -> Sources {
        SourceManifest::wire().sources(Sources::default())
    }

    /// Holds whenever `require` hands out [`BUDGET`]'s value.
    fn check_require(cx: &mut CheckCtx<'_, Owners>) -> Verdict {
        let view = cx.view();
        let view = view.borrow();
        let budget = view
            .component(BUDGET)
            .expect("the wire tier has the budget");
        match require(cx, budget) {
            Some(_) => Verdict::Holds,
            None => Verdict::NotApplicable,
        }
    }

    /// Holds whenever `require_every` hands out [`BUDGET`]'s value.
    fn check_every(cx: &mut CheckCtx<'_, Owners>) -> Verdict {
        let view = cx.view();
        let view = view.borrow();
        let budget = view
            .component(BUDGET)
            .expect("the wire tier has the budget");
        match require_every(cx, budget) {
            Some(_) => Verdict::Holds,
            None => Verdict::NotApplicable,
        }
    }

    /// A safety property `id` that needs only [`BUDGET`], checked by
    /// `check`.
    fn needing(
        id: &'static str,
        check: fn(&mut CheckCtx<'_, Owners>) -> Verdict,
    ) -> Property<Owners> {
        Property {
            id: PropertyId::new(id),
            claim: "a test claim on a component",
            kind: Kind::Safety,
            assumes: &[],
            needs: &[Source::Component(BUDGET)],
            status: Status::Active,
            check,
            witness: None,
        }
    }

    /// The `require` property.
    fn requiring() -> Property<Owners> {
        needing("test/require", check_require)
    }

    /// The `require_every` property.
    fn requiring_every() -> Property<Owners> {
        needing("test/require-every", check_every)
    }

    /// Property `id`'s report.
    fn report<'r>(report: &'r RunReport, id: &str) -> &'r crate::PropertyReport {
        report
            .properties
            .iter()
            .find(|report| report.property == id)
            .expect("the property is reported")
    }

    /// An inconclusive outcome with `reason`.
    fn inconclusive(reason: &str) -> PropertyOutcome {
        PropertyOutcome::Inconclusive {
            reason: reason.to_owned(),
        }
    }

    /// `Storage`'s view holds the storage tier's components, each
    /// `Initializing` before any source observes, and the hub closes its
    /// boundaries, so after a run the view shows the last step's: the
    /// reactor, which reports its own state, has started, the budget, which
    /// its boundary read shows, exists, the selected chain exists from the
    /// harness's construction, and the rest, which have no read yet, are
    /// still `Initializing`.
    #[test]
    fn the_storage_tier_has_the_composite_view() {
        let scenario = StorageScenario::default();
        let mut sim = Sim::new(Storage::config(&scenario), Plan::from_fuzz(&[]));
        let storage = Storage::build(&mut sim, &scenario, Storage::prepare(&scenario));
        assert_eq!(storage.captured(), Capture::Missing, "no boundary yet");
        let kinds = |view: &HeaderSyncView, started: bool| {
            view.manifest()
                .rows()
                .iter()
                .map(|row| {
                    let component = view.component(row.kind).expect("a row has a component");
                    let read = started
                        && matches!(
                            row.kind,
                            ComponentKind::Reactor
                                | ComponentKind::Budget
                                | ComponentKind::SelectedChain
                        );
                    assert_eq!(
                        component.state() == State::Initializing,
                        !read,
                        "{:?}",
                        row.kind
                    );
                    row.kind
                })
                .collect::<Vec<_>>()
        };
        let before = kinds(&storage.view().owners.borrow(), false);
        assert_eq!(
            before,
            [
                ComponentKind::Reactor,
                ComponentKind::Budget,
                ComponentKind::CallHolders,
                ComponentKind::Transfers,
                ComponentKind::SelectedChain,
            ]
        );
        let report = sim.run(&mut crate::NoChecks, &mut NullSink);
        assert_eq!(report.end.harness, None);
        assert!(report.end.steps > 0, "the run takes steps");
        assert_eq!(storage.captured(), Capture::AfterStep(report.end.steps));
        assert_eq!(kinds(&storage.view().owners.borrow(), true), before);
    }

    thread_local! {
        /// The budget counters [`check_reactor_and_budget`] got, by step.
        static COHERENT: RefCell<Vec<(u64, zakura_network::zakura::diag::BudgetCounters)>> =
            const { RefCell::new(Vec::new()) };
    }

    /// Holds at a boundary where the reactor and the budget are both
    /// current, recording the budget's counters there.
    fn check_reactor_and_budget(cx: &mut CheckCtx<'_, Storage>) -> Verdict {
        use crate::property::{require_coherent, Available};

        let view = cx.view();
        let owners = view.owners.borrow();
        let reactor = owners
            .component(ComponentKind::Reactor)
            .expect("the storage tier has the reactor");
        let budget = owners
            .component(ComponentKind::Budget)
            .expect("the storage tier has the budget");
        if !require_coherent(cx, &[reactor, budget]) {
            return Verdict::NotApplicable;
        }
        match (require(cx, reactor), require(cx, budget)) {
            (
                Some(Available::Current(Reading::Reactor(_))),
                Some(Available::Current(Reading::Budget(counters))),
            ) => {
                COHERENT.with(|seen| seen.borrow_mut().push((cx.step, *counters)));
                Verdict::Holds
            }
            other => panic!("a coherent boundary hands out both values, not {other:?}"),
        }
    }

    /// A real `header-sync/storage` run hands a property the reactor's and
    /// the budget's current values together at coherent boundaries, the
    /// budget's counters included while the node holds the headers it's
    /// syncing from its mirror, and back at zero once it's done.
    ///
    /// This fails if `Storage`'s view gets no owners.
    #[test]
    fn the_storage_tier_reads_its_components() {
        let scenario = StorageScenario::default();
        let mut sim = Sim::new(Storage::config(&scenario), Plan::from_fuzz(&[]));
        let storage = Rc::new(Storage::build(
            &mut sim,
            &scenario,
            Storage::prepare(&scenario),
        ));
        let property = Property {
            id: PropertyId::new("test/reactor-and-budget"),
            claim: "a test claim on two components",
            kind: Kind::Safety,
            assumes: &[],
            needs: &[
                Source::Component(ComponentKind::Reactor),
                Source::Component(ComponentKind::Budget),
            ],
            status: Status::Active,
            check: check_reactor_and_budget,
            witness: None,
        };
        let mut checker = RegistryChecker::new(
            Registry::new(vec![property]).expect("the IDs are unique"),
            storage.clone(),
            Assumptions::none(),
            &Storage::sources(),
        );
        COHERENT.with(|seen| seen.borrow_mut().clear());
        let report = sim.run(&mut checker, &mut NullSink);
        assert_eq!(report.end.harness, None, "{:?}", report.end);
        let seen = COHERENT.with(|seen| seen.take());
        assert!(
            seen.iter()
                .any(|(_, counters)| counters.owned + counters.reserved > 0),
            "the node holds headers at some coherent boundary: {seen:?}"
        );
        assert!(
            seen.iter().all(|(_, counters)| counters.capacity == 4_000),
            "{seen:?}"
        );
        assert_eq!(
            seen.last()
                .map(|(_, counters)| counters.owned + counters.reserved),
            Some(0),
            "it holds nothing once synced"
        );
    }

    /// A harness that builds the node's port, the selected chain's
    /// existence evidence, without registering its read fails the run at
    /// the first boundary, in a run with no peers.
    ///
    /// This fails if the chain's existence needs no read.
    #[test]
    fn an_unregistered_selected_chain_fails() {
        let scenario = StorageScenario {
            mirrors: Vec::new(),
            ..StorageScenario::default()
        };
        let report = crate::harness::header_node::reads::without_selected_chain_read(|| {
            let mut sim = Sim::new(Storage::config(&scenario), Plan::from_fuzz(&[]));
            let _storage = Storage::build(&mut sim, &scenario, Storage::prepare(&scenario));
            sim.run(&mut crate::NoChecks, &mut NullSink)
        });
        assert!(
            matches!(&report.end.harness, Some(HarnessFailure::SourceEvidence { detail, .. })
                if detail == "SelectedChain's existence evidence arrived at step 0 with no read \
                    registered"),
            "{:?}",
            report.end
        );
        assert_eq!(report.end.steps, 0);
    }

    /// A transport observer that takes the supervisor map's
    /// `ReadRegistered` but drops its read fails the run at that row, even
    /// on a run where no connection ever registers to show the map exists.
    /// The same run with the read kept ends with no harness failure.
    ///
    /// This fails if a registration row without its read is accepted.
    #[test]
    fn a_registration_without_its_read_fails() {
        use crate::harness::{
            header_sync::{HeaderWire, WireScenario},
            wire_node::dropping_reads,
        };

        let scenario = WireScenario {
            peers: Vec::new(),
            ..WireScenario::default()
        };
        let run = || {
            let mut sim = Sim::new(HeaderWire::config(&scenario), Plan::from_fuzz(&[]));
            let _wire = HeaderWire::build(&mut sim, &scenario, HeaderWire::prepare(&scenario));
            sim.run(&mut crate::NoChecks, &mut NullSink)
        };
        let kept = run();
        assert_eq!(kept.end.harness, None, "{:?}", kept.end);
        assert!(kept.log.entries().iter().all(|entry| !matches!(
            entry.obs,
            crate::Observation::Transport {
                event: crate::observe::TransportFact::Registered,
                ..
            }
        )));
        let dropped = dropping_reads(run);
        assert!(
            matches!(&dropped.end.harness, Some(HarnessFailure::SourceEvidence { detail, .. })
                if detail.starts_with("UnhandedRead: supervisor-map's read-registered row")),
            "{:?}",
            dropped.end
        );
    }

    /// A property that holds at several boundaries, then needs a component
    /// in transition at the run's last, ends `state-unsettled`, with the
    /// last boundary as its gap.
    #[test]
    fn a_component_in_transition_at_the_end_blocks_held() {
        let (cut, _) = run(reads(current(0..=3, 1)), 3, vec![requiring()], &wire());
        assert_eq!(
            report(&cut, "test/require").outcome,
            PropertyOutcome::Held { applicable: 3 },
            "it holds at the current boundaries"
        );
        let ops = reads(current(0..=3, 1).into_iter().chain(held([4])));
        let (run, view) = run(ops, 4, vec![requiring()], &wire());
        assert_eq!(run.end.harness, None);
        assert_eq!(
            view.borrow().component(BUDGET).map(Component::state),
            Some(State::InTransition {
                since: 4,
                last: Some(&1)
            })
        );
        let report = report(&run, "test/require");
        assert_eq!(report.outcome, inconclusive("state-unsettled"));
        assert_eq!(
            report.gaps,
            [Gap {
                component: "Budget".to_owned(),
                from: 4,
                through: 4
            }]
        );
    }

    /// With no existence evidence, a component stays `Initializing`
    /// through the run's end, which leaves its property inconclusive, not
    /// failed. Evidence with no registered read fails the run.
    #[test]
    fn evidence_before_registration_fails_and_none_is_pending() {
        let (none, view) = run(Vec::new(), 3, vec![requiring()], &wire());
        assert_eq!(
            view.borrow().component(BUDGET).map(Component::state),
            Some(State::Initializing),
            "no evidence arrives"
        );
        assert_eq!(none.end.harness, None);
        assert_eq!(none.end.steps, 3);
        assert_eq!(view.borrow().captured(), Capture::AfterStep(3));
        assert_eq!(
            report(&none, "test/require").outcome,
            inconclusive("state-unsettled")
        );
        let (early, _) = run(vec![(2, Op::Exists)], 3, vec![requiring()], &wire());
        assert!(
            matches!(&early.end.harness, Some(HarnessFailure::SourceEvidence { detail, .. })
                if detail.contains("with no read registered")),
            "{:?}",
            early.end
        );
    }

    /// A registered read that doesn't complete at one boundary fails the
    /// run there, for a property that declares only the component.
    #[test]
    fn a_skipped_read_fails_the_run() {
        let ops = reads(current([0, 1, 2, 4, 5], 1));
        let (run, view) = run(ops, 5, vec![requiring()], &wire());
        assert_eq!(
            view.borrow()
                .component(BUDGET)
                .and_then(Component::fresh_through),
            Some(2),
            "it was read through step 2"
        );
        assert_eq!(run.end.steps, 3);
        assert!(
            matches!(&run.end.harness, Some(HarnessFailure::SourceEvidence { detail, .. })
                if detail == "SkippedRead: Budget's registered read didn't complete at step 3"),
            "{:?}",
            run.end
        );
    }

    /// A component in transition for three boundaries comes back current
    /// with its old value. A `require` property ends `Held`, with the gap
    /// listed. A `require_every` property ends `state-unobserved`.
    #[test]
    fn a_restored_transition_leaves_an_every_boundary_claim_open() {
        let ops = reads(
            current(0..=2, 7)
                .into_iter()
                .chain(held(3..=5))
                .chain(current(6..=7, 7)),
        );
        let (run, view) = run(ops, 7, vec![requiring(), requiring_every()], &wire());
        let view = view.borrow();
        let budget = view
            .component(BUDGET)
            .expect("the wire tier has the budget");
        assert_eq!(budget.state(), State::Current { state: &7 });
        assert_eq!(budget.seq(), 3, "current, in transition, current");
        assert_eq!(run.end.harness, None);
        let gap = [Gap {
            component: "Budget".to_owned(),
            from: 3,
            through: 5,
        }];
        let current = report(&run, "test/require");
        assert_eq!(current.outcome, PropertyOutcome::Held { applicable: 4 });
        assert_eq!(current.gaps, gap);
        let every = report(&run, "test/require-every");
        assert_eq!(every.outcome, inconclusive("state-unobserved"));
        assert_eq!(every.gaps, gap);
    }

    /// Holds unless the budget reads 2, the value at which
    /// [`gap_runs_end_where_the_check_skips_a_boundary`]'s scenario stops
    /// holding.
    fn unless_two(owners: &Owners, _: Assume) -> bool {
        let view = owners.0.borrow();
        let budget = view
            .component(BUDGET)
            .expect("the wire tier has the budget");
        budget.state() != State::Current { state: &2 }
    }

    /// Two transitions with a boundary between them where the property's
    /// assumption doesn't hold, so its check never runs there, are two gap
    /// runs, not one across the skipped boundary.
    #[test]
    fn gap_runs_end_where_the_check_skips_a_boundary() {
        let mut assuming = requiring();
        assuming.assumes = &[Assume::HonestPeer];
        let ops = reads(
            current(0..=2, 1)
                .into_iter()
                .chain(held([3]))
                .chain(current([4], 2))
                .chain(held([5]))
                .chain(current([6], 1)),
        );
        let declared = Assumptions::declare(vec![AssumeRecord {
            assume: Assume::HonestPeer,
            params: serde_json::Value::Null,
        }])
        .holds_when(unless_two);
        let (run, view) = run_assuming(ops, 6, vec![assuming], &wire(), declared);
        assert_eq!(run.end.harness, None);
        assert_eq!(view.borrow().component(BUDGET).map(Component::seq), Some(5));
        let report = report(&run, "test/require");
        assert_eq!(report.holds_since, Some(5), "step 4 broke the assumption");
        let gap = |at| Gap {
            component: "Budget".to_owned(),
            from: at,
            through: at,
        };
        assert_eq!(report.gaps, [gap(3), gap(5)]);
    }
}
