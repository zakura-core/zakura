//! Header-sync nodes for M4: real reactors and services, joined by
//! in-memory links, with one node's storage completed by the run.
//!
//! The node under test runs on a [`ControlledPort`] behind a
//! [`PolicySource`], so the `Plan` picks when and how each storage operation
//! completes. Mirror peers run the same code on a plain [`EnginePort`], whose
//! calls finish on first poll. Each actor's chain is committed before
//! `Sim::new` by [`preload`], since a run's scenario clock would otherwise
//! decide whether the fixture headers validate. [`policy`] holds individual
//! operations, and gates `Control` steps, until chosen events happen. A
//! [`tap::ReactorTap`] logs a node's reactor events beside its storage facts.

mod pair;
pub mod policy;
pub mod tap;

use std::{
    cell::{Cell, RefCell},
    collections::BTreeSet,
    future::Future,
    pin::pin,
    rc::Rc,
    sync::Arc,
    task::{Context, Poll, Waker},
    time::Duration,
};

use serde::{Deserialize, Serialize};
use zakura_chain::block;
use zakura_header_chain::{Frontier, VctRootRepairStatus};
use zakura_network::zakura::{
    framed_channel, observe::HeaderSyncObserver, spawn_header_sync_reactor, FullStateFrontiers,
    HeaderSyncHandle, HeaderSyncService, HeaderSyncStartup, Peer, Service, ServicePeerDirection,
    ZakuraHeaderSyncConfig, ZakuraPeerId, LOCAL_MAX_MESSAGE_BYTES, ZAKURA_CAP_HEADER_SYNC,
    ZAKURA_HEADER_SYNC_STREAM_VERSION, ZAKURA_STREAM_HEADER_SYNC,
};
use zakura_node_services::header_chain::{ApplyHeaderTargetOutcome, Port};
use zakura_quic::NodeSecretKey;
use zakura_rt::sync::{mpsc, watch, CancellationToken};
use zakura_scheduler::SimInstant;
use zakura_storage::header::{
    adapter::EnginePort, controlled::ControlledPort, fixture::HeaderFixture, Outcome,
};

use self::{
    policy::{Hold, LinkEffect, Predicates, Trigger},
    tap::ReactorTap,
};
use crate::{
    storage::HeaderStorage, ActionSource, Boundary, Identity, Observation, ObservationLog, Sim,
};

/// Frames each direction of a link buffers, enough for a page and control
/// frames in flight.
const LINK_DEPTH: usize = 8;

/// A chain an actor holds before its run: `main` headers on branch 0 from
/// genesis, then `fork`, if any, from that tip.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Chain {
    /// Headers on branch 0, from genesis.
    pub main: u32,
    /// A branch from the end of `main`.
    #[serde(default)]
    pub fork: Option<Fork>,
}

/// A branch of a [`Chain`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Fork {
    /// The fixture branch, which keeps its headers apart from every other
    /// branch's.
    pub branch: u8,
    /// Its headers.
    pub len: u32,
}

impl Chain {
    /// `main` headers on branch 0.
    pub fn main(main: u32) -> Self {
        Chain { main, fork: None }
    }

    /// Each segment of the chain with the frontier it extends, `main` first.
    /// Empty segments are left out.
    pub fn segments(&self, fixture: &HeaderFixture) -> Vec<(Frontier, Vec<Arc<block::Header>>)> {
        let anchor = fixture.anchor();
        let main = fixture.extend(anchor, self.main, 0);
        let fork_parent = main.last().map_or(anchor, |tip| {
            Frontier::new(block::Height(self.main), tip.hash())
        });
        let mut segments = vec![(anchor, main)];
        if let Some(fork) = self.fork {
            segments.push((
                fork_parent,
                fixture.extend(fork_parent, fork.len, fork.branch),
            ));
        }
        segments.retain(|(_, headers)| !headers.is_empty());
        segments
    }

    /// The chain's tip.
    pub fn tip(&self, fixture: &HeaderFixture) -> Frontier {
        self.segments(fixture)
            .last()
            .map_or(fixture.anchor(), |(parent, headers)| {
                let height = parent
                    .height
                    .0
                    .checked_add(u32::try_from(headers.len()).expect("fixture chains are short"))
                    .expect("fixture heights stay small");
                let tip = headers.last().expect("segments aren't empty");
                Frontier::new(block::Height(height), tip.hash())
            })
    }
}

/// Polls `future` once outside a run, where every engine port call finishes
/// on its first poll.
fn ready<F: Future>(future: F) -> F::Output {
    match pin!(future).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("an engine port call outside a run finishes on its first poll"),
    }
}

/// Commits `chain` to `port`. Call it before `Sim::new`: setup leaves a
/// controlled port pristine, and fixture headers validate the same against
/// any clock only outside a run.
pub fn preload(port: &EnginePort, fixture: &HeaderFixture, chain: &Chain) {
    for (request_id, (parent, headers)) in (1..).zip(chain.segments(fixture)) {
        let request = fixture.target_request(&port.snapshot(), parent, &headers, request_id);
        let prepared = ready(port.prepare_header_target(request)).expect("fixture headers prepare");
        ready(port.apply_header_target(prepared)).expect("fixture headers apply");
    }
}

/// Which outcomes the policy offers for operations no hold picks.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Base {
    /// Every outcome the contract allows, for safety sweeps.
    Any,
    /// `engine`, or `rejected` where that's the only legal outcome, so a
    /// correct node converges.
    SuccessOnly,
}

impl Base {
    /// Whether the base offers `outcome` for an operation whose legal
    /// outcomes are `legal`.
    pub fn offers(self, legal: &[Outcome], outcome: Outcome) -> bool {
        match self {
            Base::Any => true,
            Base::SuccessOnly => {
                outcome == Outcome::Engine || legal == [Outcome::Rejected].as_slice()
            }
        }
    }
}

/// How the run completes the node's storage operations. The scenario
/// records it, so a trace replays it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Policy {
    /// The outcomes offered.
    pub base: Base,
    /// If set, each operation must complete within this many milliseconds of
    /// its submission, and time stops there until it does. Convergence runs
    /// keep it well under the reactor's 30 s request deadline, so a slow
    /// storage call can't close a session that nothing reconnects.
    #[serde(default)]
    pub bound_ms: Option<u64>,
    /// Operations held apart from the base, each by the first hold that
    /// picks it.
    #[serde(default)]
    pub holds: Vec<Hold>,
}

impl Policy {
    /// [`Base::SuccessOnly`] within 5 s, the convergence policy.
    pub fn converging() -> Self {
        Policy {
            base: Base::SuccessOnly,
            bound_ms: Some(5_000),
            holds: Vec::new(),
        }
    }
}

/// What tasks did to a [`Topology`], shared with the [`PolicySource`] that
/// counts it for [`Trigger::Linked`], [`Trigger::Closed`] and
/// [`Trigger::Grown`].
#[derive(Clone, Default)]
pub struct Effects {
    /// Every link made, in order.
    pub links: Rc<RefCell<Vec<Link>>>,
    /// Each peer whose chain grew, once per committed growth, in order.
    pub grown: Rc<RefCell<Vec<String>>>,
    /// Why a `Control` step couldn't take effect, which ends the run as a
    /// harness failure: the scenario asked for something that can't happen.
    pub failure: Rc<RefCell<Option<String>>>,
}

/// `t` plus `d`, saturating at the largest instant.
fn after(t: SimInstant, d: Duration) -> SimInstant {
    t.checked_add(d)
        .unwrap_or(SimInstant::from_start(Duration::MAX))
}

/// A [`HeaderStorage`] whose offers follow a [`Policy`]. It changes its
/// [`Predicates`] only in `observe`, so `enabled` and `boundaries` stay
/// pure. With a [`ReactorTap`], it logs the node's reactor events after its
/// storage facts, paired with them, and its predicates count both.
pub struct PolicySource {
    storage: HeaderStorage,
    tap: Option<ReactorTap>,
    port: Arc<ControlledPort>,
    base: Base,
    bound: Option<Duration>,
    predicates: Rc<RefCell<Predicates>>,
    effects: Effects,
    /// How many links are recorded as made.
    linked: usize,
    /// The connections recorded as closed.
    closed: BTreeSet<u64>,
    /// How many growths are recorded.
    grown: usize,
}

impl PolicySource {
    /// `port`'s storage under `policy`, with its checkpoint registered with
    /// `sim`. Its predicates count `effects` too.
    pub fn new(sim: &mut Sim, port: Arc<ControlledPort>, policy: Policy, effects: Effects) -> Self {
        PolicySource {
            storage: HeaderStorage::new(sim, port.clone()),
            tap: None,
            port,
            base: policy.base,
            bound: policy.bound_ms.map(Duration::from_millis),
            predicates: Rc::new(RefCell::new(Predicates::new(policy.holds))),
            effects,
            linked: 0,
            closed: BTreeSet::new(),
            grown: 0,
        }
    }

    /// This source, draining `tap` at each observation, after storage, so
    /// each event's storage watermark is already logged and the tap pairs
    /// calls with that step's facts.
    pub fn with_tap(self, tap: ReactorTap) -> Self {
        PolicySource {
            tap: Some(tap),
            ..self
        }
    }

    /// The predicates, for a [`Control`] gated by the same counts.
    pub fn predicates(&self) -> Rc<RefCell<Predicates>> {
        self.predicates.clone()
    }
}

impl ActionSource for PolicySource {
    fn enabled(&self, now: SimInstant, out: &mut Vec<(Identity, u64)>) {
        let pending = self.port.pending();
        let predicates = self.predicates.borrow();
        let mut offered = Vec::new();
        self.storage.enabled(now, &mut offered);
        out.extend(offered.into_iter().filter(|(_, local)| {
            let (id, outcome) = crate::storage::decode(*local);
            pending
                .iter()
                .find(|op| op.id == id)
                .is_some_and(|op| predicates.offers(self.base, op, outcome))
        }));
    }

    fn boundaries(&self, now: SimInstant, out: &mut Vec<Boundary>) {
        self.storage.boundaries(now, out);
        let predicates = self.predicates.borrow();
        for op in self.port.pending() {
            predicates.boundaries(self.bound, &op, now, out);
        }
    }

    fn execute(&mut self, local: u64, log: &mut ObservationLog) {
        self.storage.execute(local, log);
    }

    fn observe(&mut self, log: &mut ObservationLog) -> Result<(), String> {
        let before = log.entries().len();
        let stored = self.storage.observe(log);
        // Drained even when storage failed, so a port panic the tap holds
        // still reaches the run's failure cell and outranks that failure.
        let tapped = match &mut self.tap {
            Some(tap) => {
                // The storage source appends only its own port's facts.
                tap.storage_facts(log.entries()[before..].iter().filter_map(
                    |entry| match &entry.obs {
                        Observation::Storage { seq, fact, .. } => Some((*seq, fact)),
                        _ => None,
                    },
                ));
                tap.observe(log)
            }
            None => Ok(()),
        };
        stored?;
        tapped?;
        let mut predicates = self.predicates.borrow_mut();
        // Only this source's storage and tap append here.
        for entry in &log.entries()[before..] {
            match &entry.obs {
                Observation::Storage { fact, .. } => predicates.record(fact),
                Observation::Reactor { event, .. } => predicates.record_reactor(event),
                _ => {}
            }
        }
        let links = self.effects.links.borrow();
        for link in &links[self.linked..] {
            predicates.record_link(LinkEffect::Linked, &link.peer);
        }
        self.linked = links.len();
        // A connection closes once, whoever cancels it and however often.
        for link in links.iter() {
            if link.is_closed() && self.closed.insert(link.conn_id) {
                predicates.record_link(LinkEffect::Closed, &link.peer);
            }
        }
        let grown = self.effects.grown.borrow();
        for peer in &grown[self.grown..] {
            predicates.record_link(LinkEffect::Grown, peer);
        }
        self.grown = grown.len();
        match self.effects.failure.borrow().as_ref() {
            Some(failure) => Err(format!("ControlFailed: {failure}")),
            None => Ok(()),
        }
    }
}

/// Set once any reactor sends an operation to its action channel, which
/// only `External` dispatch does. Then that reactor's storage calls never
/// reach its port, and the run's storage evidence would be silently empty.
pub type DispatchFlag = Rc<Cell<bool>>;

/// Spawns a task that waits on a reactor's action channel and sets `flag`
/// if anything arrives. It exits then, or when the channel closes.
pub fn guard_dispatch<T: 'static>(
    sim: &mut Sim,
    mut actions: mpsc::Receiver<T>,
    flag: DispatchFlag,
) {
    sim.scheduler().spawn_root("dispatch-guard", async move {
        if actions.recv().await.is_some() {
            flag.set(true);
        }
    });
}

/// One header-sync actor: a real reactor and service on its port.
pub struct HeaderNode {
    /// The actor's role name, `node` or `mirror-<i>`, which `Control`
    /// actions name.
    pub name: String,
    /// Its peer identity, which its links present.
    pub id: ZakuraPeerId,
    /// The reactor's handle.
    pub handle: HeaderSyncHandle,
    /// The port its reactor and publisher share.
    pub port: Arc<dyn Port>,
    service: HeaderSyncService,
    /// The VCT repair state the reactor follows. `Idle` unless a scenario
    /// sets it.
    pub vct_repairs: watch::Sender<VctRootRepairStatus>,
}

impl HeaderNode {
    /// Starts a reactor and its service on `port`, whose selected chain ends
    /// at `tip`, as `zakurad` wires them: production config and timers, the
    /// port's own watch as the committed snapshots, and the default dispatch,
    /// which is direct outside test builds. A guard sets `dispatch` if the
    /// reactor dispatches externally instead. Peer IDs come from `seed`. The
    /// reactor reports its events to `observer`, if one is given.
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        sim: &mut Sim,
        name: &str,
        seed: u8,
        port: Arc<dyn Port>,
        fixture: &HeaderFixture,
        tip: Frontier,
        dispatch: DispatchFlag,
        observer: Option<Arc<dyn HeaderSyncObserver>>,
    ) -> Self {
        let anchor = fixture.anchor();
        let mut startup = HeaderSyncStartup::new(
            fixture.network().clone(),
            (anchor.height, anchor.hash),
            FullStateFrontiers {
                finalized_height: anchor.height,
                verified_block_tip: anchor.height,
                verified_block_hash: anchor.hash,
            },
            Some((tip.height, tip.hash)),
            ZakuraHeaderSyncConfig::default(),
            LOCAL_MAX_MESSAGE_BYTES,
        );
        let (vct_repairs, repairs) = watch::channel(VctRootRepairStatus::default());
        startup.committed_snapshots = Some(port.selected_chain());
        startup.vct_root_repairs = Some(repairs);
        startup.header_chain_port = port.clone();
        startup.observer.0 = observer;
        let (handle, actions, reactor) =
            spawn_header_sync_reactor(startup).expect("a fixture startup is valid");
        guard_dispatch(sim, actions, dispatch);
        sim.scheduler().spawn_root("header-node", async move {
            let _ = reactor.await;
        });
        let node = NodeSecretKey::from_bytes(&[seed; 32]).public();
        HeaderNode {
            name: name.to_owned(),
            id: ZakuraPeerId::new(node.as_bytes().to_vec()).expect("a node id is a valid peer id"),
            service: HeaderSyncService::new(handle.clone()),
            handle,
            port,
            vct_repairs,
        }
    }
}

/// One connection between two nodes, by their index in a [`Topology`].
#[derive(Clone, Debug)]
pub struct Link {
    /// The name of the node under test's peer on it.
    pub peer: String,
    /// The node that dialed.
    pub dialer: usize,
    /// The node that was dialed.
    pub listener: usize,
    /// The connection's ID, the same on both sides.
    pub conn_id: u64,
    /// The header-sync session's ID, the same on both sides.
    pub session_id: u64,
    /// Each side's connection token, dialer first.
    tokens: [CancellationToken; 2],
}

impl Link {
    /// Closes the connection on both sides, as a transport does when either
    /// end goes away.
    pub fn close(&self) {
        for token in &self.tokens {
            token.cancel();
        }
    }

    /// Whether either side's connection token is cancelled: by
    /// [`Link::close`], or by a service that tears the connection down,
    /// as replacing a session with an unfinished exchange does. A
    /// replacement that retires only the old session leaves its connection
    /// open, so that isn't a closure.
    pub fn is_closed(&self) -> bool {
        self.tokens.iter().any(CancellationToken::is_cancelled)
    }
}

/// Every node in a run and the links between them.
pub struct Topology {
    /// The nodes, the node under test first.
    pub nodes: Vec<HeaderNode>,
    effects: Effects,
    /// The last connection and session ID handed out. A replacement needs
    /// both to be new, so one counter serves both.
    last_id: Cell<u64>,
}

impl Topology {
    /// `nodes`, with no links yet. Each link it makes, and each growth,
    /// goes in `effects`.
    pub fn new(nodes: Vec<HeaderNode>, effects: Effects) -> Self {
        Topology {
            nodes,
            effects,
            last_id: Cell::new(0),
        }
    }

    /// Extends the selected chain of the node named `peer` by `len` fixture
    /// headers from its tip, through its own port, as if it had mined them.
    /// It records the growth only once the mirror's published tip is the
    /// new one. Anything else, such as a resource stall that leaves the
    /// target unadmitted, is recorded as a failure instead. The node must
    /// be a mirror, on an engine port, that isn't applying anything else.
    async fn grow(&self, peer: &str, fixture: &HeaderFixture, len: u32) {
        let node = self
            .nodes
            .iter()
            .find(|node| node.name == peer)
            .unwrap_or_else(|| panic!("the scenario has a `{peer}`"));
        let published = || {
            node.port
                .selected_chain()
                .borrow()
                .as_ref()
                .map(|snapshot| snapshot.frontiers.header_best)
        };
        let snapshot = node
            .port
            .selected_chain()
            .borrow()
            .clone()
            .expect("an engine port publishes its snapshot from the start");
        let tip = snapshot.frontiers.header_best;
        let headers = fixture.extend(tip, len, 0);
        let target = Frontier::new(
            block::Height(
                tip.height
                    .0
                    .checked_add(len)
                    .expect("fixture heights stay small"),
            ),
            headers.last().expect("a growth has headers").hash(),
        );
        let request = fixture.target_request(&snapshot, tip, &headers, 1);
        let applied = match node.port.prepare_header_target(request).await {
            Ok(prepared) => node.port.apply_header_target(prepared).await,
            Err(error) => Err(error),
        };
        match (applied, published()) {
            (Ok(ApplyHeaderTargetOutcome::Applied), Some(now)) if now == target => {
                self.effects.grown.borrow_mut().push(peer.to_owned());
            }
            (applied, now) => {
                *self.effects.failure.borrow_mut() = Some(format!(
                    "growing {peer} to {target:?} gave {applied:?}, with its tip at {now:?}"
                ));
            }
        }
    }

    /// Connects `dialer` to `listener` with a new connection and session,
    /// each side's stream 5 at version 9 over a pair of in-memory channels.
    pub fn link(&self, dialer: usize, listener: usize) -> Link {
        let id = self.last_id.get() + 1;
        self.last_id.set(id);
        let (to_listener, from_dialer) = framed_channel(LINK_DEPTH);
        let (to_dialer, from_listener) = framed_channel(LINK_DEPTH);
        let tokens = [CancellationToken::new(), CancellationToken::new()];
        let sides = [
            (
                dialer,
                listener,
                ServicePeerDirection::Outbound,
                (from_listener, to_listener),
            ),
            (
                listener,
                dialer,
                ServicePeerDirection::Inbound,
                (from_dialer, to_dialer),
            ),
        ];
        for ((local, remote, direction, streams), token) in sides.into_iter().zip(&tokens) {
            self.nodes[local].service.add_peer(Peer::new_versioned(
                id,
                self.nodes[remote].id.clone(),
                ZAKURA_CAP_HEADER_SYNC,
                direction,
                ZAKURA_STREAM_HEADER_SYNC,
                ZAKURA_HEADER_SYNC_STREAM_VERSION,
                id,
                streams,
                token.clone(),
            ));
        }
        let peer = if dialer == 0 { listener } else { dialer };
        let link = Link {
            peer: self.nodes[peer].name.clone(),
            dialer,
            listener,
            conn_id: id,
            session_id: id,
            tokens,
        };
        self.effects.links.borrow_mut().push(link.clone());
        link
    }

    /// The newest link between the node under test and the node named
    /// `peer`.
    pub fn link_to(&self, peer: &str) -> Option<Link> {
        let peer = self.nodes.iter().position(|node| node.name == peer)?;
        self.effects
            .links
            .borrow()
            .iter()
            .rev()
            .find(|link| {
                (link.dialer, link.listener) == (0, peer)
                    || (link.dialer, link.listener) == (peer, 0)
            })
            .cloned()
    }
}

/// A harness action on a link.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ControlAction {
    /// Close the peer's newest link.
    Close,
    /// Link the peer again, with a new connection and session, in the
    /// newest link's direction.
    Relink,
    /// Extend the peer's chain by `len` headers from its tip. The peer must
    /// be a mirror.
    Grow {
        /// The new headers.
        len: u32,
    },
}

impl ControlAction {
    /// The action's name in its identity.
    fn name(self) -> &'static str {
        match self {
            ControlAction::Close => "close",
            ControlAction::Relink => "relink",
            ControlAction::Grow { .. } => "grow",
        }
    }
}

/// One scripted [`ControlAction`] on the link between the node under test
/// and `peer`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlStep {
    /// The peer's name.
    pub peer: String,
    /// What to do.
    pub action: ControlAction,
    /// Offered only once this holds. Always offered without one.
    #[serde(default)]
    pub when: Option<Trigger>,
}

/// Offers a scenario's scripted link actions in order, each once, as
/// `peer:<peer>:<action>`, so the `Plan` decides when each happens, once
/// its trigger holds. A harness task carries each one out on its next step,
/// since linking takes locks that tasks hold across scheduling points, where
/// only a task can wait. So a pick only queues the action, and triggers
/// count the effect.
pub struct Control {
    script: Vec<ControlStep>,
    predicates: Rc<RefCell<Predicates>>,
    /// The next step to offer.
    next: usize,
    to_task: mpsc::Sender<ControlStep>,
}

impl Control {
    /// A source for `script` on `topology`, whose chains come from
    /// `fixture`, gated by `predicates`, with its task spawned on `sim`.
    pub fn new(
        sim: &mut Sim,
        topology: Rc<Topology>,
        fixture: HeaderFixture,
        script: Vec<ControlStep>,
        predicates: Rc<RefCell<Predicates>>,
    ) -> Self {
        let (to_task, mut steps) = mpsc::channel::<ControlStep>(script.len().max(1));
        sim.scheduler().spawn_root("control", async move {
            while let Some(step) = steps.recv().await {
                let link = || {
                    topology
                        .link_to(&step.peer)
                        .unwrap_or_else(|| panic!("the scenario links `{}`", step.peer))
                };
                match step.action {
                    ControlAction::Close => link().close(),
                    ControlAction::Relink => {
                        let link = link();
                        topology.link(link.dialer, link.listener);
                    }
                    ControlAction::Grow { len } => topology.grow(&step.peer, &fixture, len).await,
                }
            }
        });
        Control {
            script,
            predicates,
            next: 0,
            to_task,
        }
    }
}

impl ActionSource for Control {
    fn enabled(&self, now: SimInstant, out: &mut Vec<(Identity, u64)>) {
        let Some(step) = self.script.get(self.next) else {
            return;
        };
        let open = step
            .when
            .as_ref()
            .is_none_or(|when| self.predicates.borrow().control_holds(when, now));
        if open {
            // A script is far shorter than u64.
            out.push((
                Identity::peer(&step.peer, step.action.name()),
                self.next as u64,
            ));
        }
    }

    fn boundaries(&self, now: SimInstant, out: &mut Vec<Boundary>) {
        if let Some(when) = self
            .script
            .get(self.next)
            .and_then(|step| step.when.as_ref())
        {
            self.predicates.borrow().control_boundaries(when, now, out);
        }
    }

    fn execute(&mut self, _: u64, _: &mut ObservationLog) {
        self.to_task
            .try_send(self.script[self.next].clone())
            .expect("the channel holds the whole script, and the task outlives the run");
        self.next += 1;
    }
}
