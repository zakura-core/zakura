//! Header-sync nodes for M4: real reactors and services, joined by
//! in-memory links, with one node's storage completed by the run.
//!
//! The node under test runs on a [`ControlledPort`] behind a
//! [`PolicySource`], so the `Plan` picks when and how each storage operation
//! completes. Mirror peers run the same code on a plain [`EnginePort`], whose
//! calls finish on first poll. Each actor's chain is committed before
//! `Sim::new` by [`preload`], since a run's scenario clock would otherwise
//! decide whether the fixture headers validate.

use std::{
    cell::{Cell, RefCell},
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
    framed_channel, spawn_header_sync_reactor, FullStateFrontiers, HeaderSyncHandle,
    HeaderSyncService, HeaderSyncStartup, Peer, Service, ServicePeerDirection,
    ZakuraHeaderSyncConfig, ZakuraPeerId, LOCAL_MAX_MESSAGE_BYTES, ZAKURA_CAP_HEADER_SYNC,
    ZAKURA_HEADER_SYNC_STREAM_VERSION, ZAKURA_STREAM_HEADER_SYNC,
};
use zakura_node_services::header_chain::Port;
use zakura_quic::NodeSecretKey;
use zakura_rt::sync::{mpsc, watch, CancellationToken};
use zakura_scheduler::SimInstant;
use zakura_storage::header::{
    adapter::EnginePort, controlled::ControlledPort, fixture::HeaderFixture, Outcome,
};

use crate::{storage::HeaderStorage, ActionSource, Boundary, Identity, ObservationLog, Sim};

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

/// Which outcomes the policy offers for operations no rule picks.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Base {
    /// Every outcome the contract allows, for safety sweeps.
    Any,
    /// `engine`, or `rejected` where that's the only legal outcome, so a
    /// correct node converges.
    SuccessOnly,
}

/// How the run completes the node's storage operations. The scenario
/// records it, so a trace replays it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
}

impl Policy {
    /// [`Base::SuccessOnly`] within 5 s, the convergence policy.
    pub fn converging() -> Self {
        Policy {
            base: Base::SuccessOnly,
            bound_ms: Some(5_000),
        }
    }
}

/// `t` plus `d`, saturating at the largest instant.
fn after(t: SimInstant, d: Duration) -> SimInstant {
    t.checked_add(d)
        .unwrap_or(SimInstant::from_start(Duration::MAX))
}

/// A [`HeaderStorage`] whose offers follow a [`Policy`]. It changes nothing
/// of its own, so `enabled` and `boundaries` stay pure.
pub struct PolicySource {
    storage: HeaderStorage,
    port: Arc<ControlledPort>,
    policy: Policy,
}

impl PolicySource {
    /// `port`'s storage under `policy`, with its checkpoint registered with
    /// `sim`.
    pub fn new(sim: &mut Sim, port: Arc<ControlledPort>, policy: Policy) -> Self {
        PolicySource {
            storage: HeaderStorage::new(sim, port.clone()),
            port,
            policy,
        }
    }

    /// Whether the policy offers `outcome` for an operation whose legal
    /// outcomes are `legal`.
    fn offers(&self, legal: &[Outcome], outcome: Outcome) -> bool {
        match self.policy.base {
            Base::Any => true,
            Base::SuccessOnly => {
                outcome == Outcome::Engine || legal == [Outcome::Rejected].as_slice()
            }
        }
    }
}

impl ActionSource for PolicySource {
    fn enabled(&self, now: SimInstant, out: &mut Vec<(Identity, u64)>) {
        let pending = self.port.pending();
        let mut offered = Vec::new();
        self.storage.enabled(now, &mut offered);
        out.extend(offered.into_iter().filter(|(_, local)| {
            let (id, outcome) = crate::storage::decode(*local);
            pending
                .iter()
                .find(|op| op.id == id)
                .is_some_and(|op| self.offers(&op.enabled, outcome))
        }));
    }

    fn boundaries(&self, now: SimInstant, out: &mut Vec<Boundary>) {
        self.storage.boundaries(now, out);
        if let Some(bound) = self.policy.bound_ms.map(Duration::from_millis) {
            for op in self.port.pending() {
                out.push(Boundary::Deadline(after(
                    now,
                    bound.saturating_sub(op.since_submit),
                )));
            }
        }
    }

    fn execute(&mut self, local: u64, log: &mut ObservationLog) {
        self.storage.execute(local, log);
    }

    fn observe(&mut self, log: &mut ObservationLog) -> Result<(), String> {
        self.storage.observe(log)
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
    /// reactor dispatches externally instead. Peer IDs come from `seed`.
    pub fn spawn(
        sim: &mut Sim,
        name: &str,
        seed: u8,
        port: Arc<dyn Port>,
        fixture: &HeaderFixture,
        tip: Frontier,
        dispatch: DispatchFlag,
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
        startup.header_chain_port = port;
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
            vct_repairs,
        }
    }
}

/// One live connection between two nodes, by their index in a
/// [`Topology`].
#[derive(Clone, Debug)]
pub struct Link {
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
}

/// Every node in a run and the links between them.
pub struct Topology {
    /// The nodes, the node under test first.
    pub nodes: Vec<HeaderNode>,
    links: RefCell<Vec<Link>>,
    /// The last connection and session ID handed out. A replacement needs
    /// both to be new, so one counter serves both.
    last_id: Cell<u64>,
}

impl Topology {
    /// `nodes`, with no links yet.
    pub fn new(nodes: Vec<HeaderNode>) -> Self {
        Topology {
            nodes,
            links: RefCell::default(),
            last_id: Cell::new(0),
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
        let link = Link {
            dialer,
            listener,
            conn_id: id,
            session_id: id,
            tokens,
        };
        self.links.borrow_mut().push(link.clone());
        link
    }

    /// The newest link between the node under test and the node named
    /// `peer`.
    pub fn link_to(&self, peer: &str) -> Option<Link> {
        let peer = self.nodes.iter().position(|node| node.name == peer)?;
        self.links
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
}

impl ControlAction {
    /// The action's name in its identity.
    fn name(self) -> &'static str {
        match self {
            ControlAction::Close => "close",
            ControlAction::Relink => "relink",
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
}

/// Offers a scenario's scripted link actions in order, each once, as
/// `peer:<peer>:<action>`, so the `Plan` decides when each happens. A
/// harness task carries each one out on its next step, since linking takes
/// locks that tasks hold across scheduling points, where only a task can
/// wait.
pub struct Control {
    script: Vec<ControlStep>,
    /// The next step to offer.
    next: usize,
    to_task: mpsc::Sender<ControlStep>,
}

impl Control {
    /// A source for `script` on `topology`, with its task spawned on `sim`.
    pub fn new(sim: &mut Sim, topology: Rc<Topology>, script: Vec<ControlStep>) -> Self {
        let (to_task, mut steps) = mpsc::channel::<ControlStep>(script.len().max(1));
        sim.scheduler().spawn_root("control", async move {
            while let Some(step) = steps.recv().await {
                let link = topology
                    .link_to(&step.peer)
                    .unwrap_or_else(|| panic!("the scenario links `{}`", step.peer));
                match step.action {
                    ControlAction::Close => link.close(),
                    ControlAction::Relink => {
                        topology.link(link.dialer, link.listener);
                    }
                }
            }
        });
        Control {
            script,
            next: 0,
            to_task,
        }
    }
}

impl ActionSource for Control {
    fn enabled(&self, _: SimInstant, out: &mut Vec<(Identity, u64)>) {
        if let Some(step) = self.script.get(self.next) {
            // A script is far shorter than u64.
            out.push((
                Identity::peer(&step.peer, step.action.name()),
                self.next as u64,
            ));
        }
    }

    fn boundaries(&self, _: SimInstant, _: &mut Vec<Boundary>) {}

    fn execute(&mut self, _: u64, _: &mut ObservationLog) {
        self.to_task
            .try_send(self.script[self.next].clone())
            .expect("the channel holds the whole script, and the task outlives the run");
        self.next += 1;
    }
}
