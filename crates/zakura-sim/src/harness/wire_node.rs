//! `WireNode`: M4's header-sync node behind a real `ZakuraProtocolHandler`
//! on a `zakura-quic` endpoint bound to the run's `SimNet` (M5-C), and the
//! [`TransportTap`] that logs what its transport reports.
//!
//! The reactor runs exactly as [`HeaderNode::spawn`] runs it, on M4's
//! controlled port, reactor tap and dispatch guard. In front of it sits a
//! handler whose registry holds header sync alone, built in the node's own
//! root task, so it draws its session-ID seed from the `Plan` at the same
//! point in every run. The node takes inbound dials through `quic.serve`
//! and dials out through the public `ZakuraEndpoint::dial`.
//!
//! [`WireEndpoint`] is that endpoint and handler for any service set, so
//! the discovery node (disc-2.1) runs behind the same transport and tap.
//!
//! The tap is the transport observer. It buffers each event, and the run
//! drains the buffer at every observation into [`Observation::Transport`],
//! checked through its checkpoint as reactor taps are. A connection's events
//! before registration carry only its remote address. Its `registered`
//! event joins that address to the connection's ID, and every admitted
//! session gets an [`Observation::Session`] row naming the peer that bound
//! the address. That's also when the tap opens loss on the session's links,
//! through [`Net::admitted`]. A dial of the node's that fails before any
//! connection registers is reported through the tap too, as `dial-failed`
//! keyed by the dialed address (M5-F). A tap with a session view also feeds
//! it every transport event as the transport reports it, for hdr-5's join of
//! the reactor's sessions to own-9's facts.

use std::{
    collections::BTreeMap,
    future::Future,
    mem,
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, PoisonError, Weak,
    },
};

use zakura_chain::parameters::Network;
use zakura_network::{
    zakura::{
        ConnectionProbe, ConnectionSample, HeaderCheck, HeaderSyncService, Service,
        ServiceRegistry, SessionStep, Transition, TransportEvent, TransportObserver, ZakuraConnId,
        ZakuraConnectionLimits, ZakuraEndpoint, ZakuraHandshakeConfig, ZakuraLocalLimits,
        ZakuraProtocolHandler, ZakuraSupervisorHandle, ZakuraTrace,
    },
    Config,
};
use zakura_quic::{
    sim::{SeededInputs, SimNet},
    NodeAddr, QuicEndpoint,
};
use zakura_rt::{sim::RunId, sync::watch};
use zakura_scheduler::SimInstant;

use super::header_node::{session::SessionEvidence, HeaderNode};
use crate::{
    evidence::{Checkpoint, EvidenceId, EvidenceKind, LiveEvidence},
    net::{Net, WireOrder},
    observe::{
        HeaderCheckFact, KindLoad, SessionStepFact, TransitionFact, TransportFact, TransportLimits,
    },
    peer::{key_index, peer_addr, peer_of, SharedIds, NODE_ADDR},
    ActionSource, Boundary, Identity, Observation, ObservationLog, Sim,
};

/// The node's network config: production defaults on the fixture's
/// network.
pub(crate) fn config(network: &Network) -> Config {
    Config {
        network: network.clone(),
        ..Config::default()
    }
}

/// The node's endpoint and the limits it dials with, once it's bound.
pub(crate) type Bound = Option<(ZakuraEndpoint, ZakuraLocalLimits)>;

/// The node under test on real QUIC.
pub struct WireNode {
    /// The reactor, its port and its handle.
    pub header: HeaderNode,
    endpoint: WireEndpoint,
}

impl WireNode {
    /// Binds `header`'s node to [`NODE_ADDR`] on `net`, with the key and
    /// endpoint inputs `seed` gives index 0, behind a handler that registers
    /// header sync alone, at production limits on `network`, and reports to
    /// `observer`, with failed dials to `dials`. The handler is built in the
    /// node's own root task.
    pub fn spawn(
        sim: &mut Sim,
        header: HeaderNode,
        network: &Network,
        net: SimNet,
        seed: [u8; 32],
        observer: Arc<dyn TransportObserver>,
        dials: DialReporter,
    ) -> Self {
        let service: Arc<dyn Service> = Arc::new(HeaderSyncService::new(header.handle.clone()));
        let endpoint = WireEndpoint::spawn(
            sim,
            network,
            net,
            seed,
            observer,
            dials,
            move |_, _| (vec![service], ()),
            |_, _, ()| async {},
        );
        WireNode { header, endpoint }
    }

    /// [`WireEndpoint::dial`].
    pub fn dial(&self, sim: &mut Sim, index: usize, listening: watch::Receiver<bool>) {
        self.endpoint.dial(sim, index, listening);
    }
}

/// A node's endpoint on real QUIC: the handler in front of the node's
/// service set, its transport tap's observer, and its dials.
pub struct WireEndpoint {
    seed: [u8; 32],
    /// Set once the node serves.
    endpoint: watch::Receiver<Bound>,
    /// Where its failed dials go, if a tap reports them.
    dials: Option<DialReporter>,
}

impl WireEndpoint {
    /// Binds a node to [`NODE_ADDR`] on `net`, with the key and endpoint
    /// inputs `seed` gives index 0, behind a handler that registers the
    /// services `services` builds from the node's supervisor and limits, at
    /// production limits on `network`, and reports to `observer`, with
    /// failed dials to `dials`. Once the node serves, it runs `started` on
    /// the endpoint, its limits and what `services` handed it, and keeps
    /// what that returns for the run. All of it runs in the node's own root
    /// task, so the handler draws its session-ID seed from the `Plan` at the
    /// same point in every run.
    #[allow(clippy::too_many_arguments)]
    pub fn spawn<T, F, K>(
        sim: &mut Sim,
        network: &Network,
        net: SimNet,
        seed: [u8; 32],
        observer: Arc<dyn TransportObserver>,
        dials: DialReporter,
        services: impl FnOnce(&ZakuraSupervisorHandle, &ZakuraLocalLimits) -> (Vec<Arc<dyn Service>>, T)
            + Send
            + 'static,
        started: impl FnOnce(ZakuraEndpoint, ZakuraLocalLimits, T) -> F + Send + 'static,
    ) -> Self
    where
        T: Send,
        F: Future<Output = K> + Send,
        K: Send,
    {
        WireEndpoint::spawn_at(
            sim,
            network,
            net,
            seed,
            None,
            observer,
            Some(dials),
            services,
            started,
        )
    }

    /// [`WireEndpoint::spawn`], binding the node at `index`: [`NODE_ADDR`]
    /// with key 0 for `None`, or peer `i`'s first address,
    /// [`peer_addr`]`(i, 0)`, with its key for `Some(i)`. A peer's failed
    /// dials go to `dials` if given.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn spawn_at<T, F, K>(
        sim: &mut Sim,
        network: &Network,
        net: SimNet,
        seed: [u8; 32],
        index: Option<usize>,
        observer: Arc<dyn TransportObserver>,
        dials: Option<DialReporter>,
        services: impl FnOnce(&ZakuraSupervisorHandle, &ZakuraLocalLimits) -> (Vec<Arc<dyn Service>>, T)
            + Send
            + 'static,
        started: impl FnOnce(ZakuraEndpoint, ZakuraLocalLimits, T) -> F + Send + 'static,
    ) -> Self
    where
        T: Send,
        F: Future<Output = K> + Send,
        K: Send,
    {
        WireEndpoint::spawn_configured(
            sim,
            config(network),
            net,
            seed,
            index,
            observer,
            dials,
            services,
            started,
        )
    }

    /// [`WireEndpoint::spawn_at`], with the node's network config `config`
    /// in place of production defaults on its network.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn spawn_configured<T, F, K>(
        sim: &mut Sim,
        config: Config,
        net: SimNet,
        seed: [u8; 32],
        index: Option<usize>,
        observer: Arc<dyn TransportObserver>,
        dials: Option<DialReporter>,
        services: impl FnOnce(&ZakuraSupervisorHandle, &ZakuraLocalLimits) -> (Vec<Arc<dyn Service>>, T)
            + Send
            + 'static,
        started: impl FnOnce(ZakuraEndpoint, ZakuraLocalLimits, T) -> F + Send + 'static,
    ) -> Self
    where
        T: Send,
        F: Future<Output = K> + Send,
        K: Send,
    {
        let (addr, key) = index.map_or((NODE_ADDR, 0), |index| {
            (peer_addr(index, 0), key_index(index))
        });
        let (bound, endpoint) = watch::channel(None);
        sim.scheduler().spawn_root("wire-node", async move {
            let limits = ZakuraLocalLimits::from_config(&config);
            let quic = QuicEndpoint::bind_with(
                SeededInputs::node_key(seed, key),
                &limits.quic_bind_config(vec![addr]),
                &limits.quic,
                net,
                SeededInputs::new(seed, key).endpoint_inputs(),
            )
            .expect("the node binds its address");
            let supervisor = ZakuraSupervisorHandle::new(config.zakura.max_connections_per_ip);
            let (services, handed) = services(&supervisor, &limits);
            let registry = ServiceRegistry::new(services).expect("the node's services register");
            let handler = ZakuraProtocolHandler::new_with_registry_and_trace(
                supervisor,
                config.network.clone(),
                ZakuraHandshakeConfig::for_network(&config.network),
                limits.clone(),
                Arc::new(registry),
                ZakuraTrace::noop(),
            )
            .with_local_node_id(quic.local_id())
            .with_transport_observer(observer);
            quic.serve(handler.clone()).expect("the node serves");
            let endpoint = ZakuraEndpoint::new(quic, handler);
            bound.send_replace(Some((endpoint.clone(), limits.clone())));
            // What the node started, and the endpoint, live as long as the
            // run.
            let _started = started(endpoint, limits, handed).await;
            std::future::pending::<()>().await;
        });
        WireEndpoint {
            seed,
            endpoint,
            dials,
        }
    }

    /// The endpoint and its limits, once it serves.
    pub(crate) fn bound(&self) -> watch::Receiver<Bound> {
        self.endpoint.clone()
    }

    /// Spawns a task that dials peer `index` once the node serves and
    /// `listening` says the peer does too, and serves the connection until
    /// it ends. A dial that fails is reported as `dial-failed` on the
    /// peer's address.
    pub fn dial(&self, sim: &mut Sim, index: usize, mut listening: watch::Receiver<bool>) {
        let mut endpoint = self.endpoint.clone();
        let remote = peer_addr(index, 0);
        let target = NodeAddr::with_addrs(
            SeededInputs::node_key(self.seed, key_index(index)).public(),
            [remote],
        );
        let dials = self.dials.clone();
        sim.scheduler().spawn_root("dial", async move {
            if listening.wait_for(|up| *up).await.is_err() {
                return;
            }
            let bound = match endpoint.wait_for(Option::is_some).await {
                Ok(bound) => bound.clone(),
                Err(_) => return,
            };
            let (endpoint, limits) = bound.expect("the wait returns a bound node");
            // A connection's own events show how it went once it registers.
            if let Err(error) = endpoint.dial(target, &limits).await {
                if let Some(dials) = dials {
                    dials.report(remote, error.to_string());
                }
            }
        });
    }
}

/// Reports the node's failed dials into its [`TransportTap`], in order with
/// the transport's own events.
#[derive(Clone)]
pub struct DialReporter(Arc<TapState>);

impl DialReporter {
    /// Buffers the failure of the dial to `remote` with `error`, or refuses
    /// it outside the tap's run, as the observer does.
    fn report(&self, remote: SocketAddr, error: String) {
        self.0.push(Reported::DialFailed { remote, error });
    }
}

/// The run's reader of one node's transport events. Dropping it while the
/// run is live fails the run, since events after that are never delivered.
pub struct TransportTap {
    state: Arc<TapState>,
    checkpoint: Checkpoint,
    /// Binds it to the run that holds its checkpoint.
    id: EvidenceId,
    node: String,
    /// The last event delivered to the log, 0 before any.
    delivered: u64,
    /// Opened for each admitted session's links.
    net: Net,
    /// Each peer's name, by index.
    peers: Vec<String>,
    /// Each registered connection's remote address.
    remotes: BTreeMap<u64, SocketAddr>,
    /// Where it hands over the shared ID of each `registered` and
    /// `handshake-refused` event it logs.
    shared_ids: SharedIds,
    /// The end it reads.
    role: TapRole,
    /// The last load it logged for each connection it samples.
    loads: BTreeMap<u64, TransportFact>,
}

/// The end of its connections a [`TransportTap`] reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TapRole {
    /// The node under test: it logs [`Observation::Transport`] rows, each
    /// admitted session's [`Observation::Session`] row, and opens loss on
    /// each session's links.
    Node,
    /// A peer: it logs [`Observation::PeerTransport`] rows and nothing more.
    Peer,
}

/// What the observer and the reader share.
struct TapState {
    /// The runtime run whose tasks may report events.
    run: RunId,
    /// The observer, which only the transport holds, so it's gone once the
    /// handler is.
    observer: Weak<TapObserver>,
    /// Set when something reported an event outside `run`.
    refused: AtomicBool,
    buffer: Mutex<Buffer>,
    /// The node's session view, fed each transport event as it's reported.
    sessions: Option<SessionEvidence>,
    /// Whether the tap samples each connection's resources and logs its
    /// shared ID (sim-9.3).
    sampling: bool,
    /// Each registered connection's resource probe, in registration order.
    probes: Mutex<Vec<(u64, ConnectionProbe)>>,
    /// The run's wire-event stamps, which each frame write takes as it's
    /// reported.
    order: WireOrder,
}

/// Events reported and not yet drained.
#[derive(Default)]
struct Buffer {
    /// The last event number handed out.
    last: u64,
    /// Each event with its number, in order.
    events: Vec<(u64, Reported)>,
}

/// One event in the tap's buffer: the transport's, or the wire node's own.
enum Reported {
    /// What the transport reported.
    Event(TransportEvent),
    /// A dial to `remote` failed with `error`.
    DialFailed { remote: SocketAddr, error: String },
    /// Connection `conn_id`'s shared ID, right after its registration.
    SharedId { conn_id: u64, id: [u8; 16] },
    /// Connection `conn_id`'s resources changed to `load`.
    Load { conn_id: u64, load: TransportFact },
    /// A frame write the transport reported, stamped with the run's
    /// [`WireOrder`] when it was reported rather than when it's drained.
    Written {
        conn_id: u64,
        quic_stream: u64,
        seq: u64,
        message_type: u16,
        order: u64,
    },
}

impl TapState {
    /// Locks the buffer. Every change completes under the lock, so a panic
    /// elsewhere can't leave it half changed.
    fn buffer(&self) -> std::sync::MutexGuard<'_, Buffer> {
        self.buffer.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Numbers and buffers `event`, or refuses it outside the tap's run. A
    /// transport event reaches the session view first, and a frame write
    /// takes its [`WireOrder`] stamp before it's buffered. No step yields, and
    /// no std lock is held across another call.
    fn push(&self, event: Reported) {
        if zakura_rt::sim::run_id() != Some(self.run) {
            self.refused.store(true, Ordering::Relaxed);
            return;
        }
        if let (Some(sessions), Reported::Event(event)) = (&self.sessions, &event) {
            sessions.transport(event);
        }
        let event = match event {
            Reported::Event(TransportEvent::FrameWritten {
                conn_id,
                quic_stream,
                seq,
                message_type,
            }) => Reported::Written {
                conn_id,
                quic_stream,
                seq,
                message_type,
                order: self.order.next(),
            },
            event => event,
        };
        let shared = match &event {
            Reported::Event(TransportEvent::Registered {
                conn_id, shared_id, ..
            }) if self.sampling => Some(Reported::SharedId {
                conn_id: *conn_id,
                id: *shared_id,
            }),
            _ => None,
        };
        let mut buffer = self.buffer();
        for event in std::iter::once(event).chain(shared) {
            buffer.last = buffer
                .last
                .checked_add(1)
                .expect("a run reports fewer than 2^64 events");
            let seq = buffer.last;
            buffer.events.push((seq, event));
        }
    }

    /// Keeps connection `conn_id`'s probe, or refuses it outside the tap's
    /// run, as [`TapState::push`] does.
    fn probe(&self, conn_id: u64, probe: ConnectionProbe) {
        if zakura_rt::sim::run_id() != Some(self.run) {
            self.refused.store(true, Ordering::Relaxed);
            return;
        }
        self.probes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((conn_id, probe));
    }
}

/// A transport tap publishes transport events.
impl LiveEvidence for TapState {
    /// The last event reported.
    fn last_seq(&self) -> u64 {
        self.buffer().last
    }

    /// Whether an event was reported outside the tap's run.
    fn refused_outside_run(&self) -> bool {
        self.refused.load(Ordering::Relaxed)
    }

    /// `TransportGone` once the observer is.
    fn health(&self) -> Option<&'static str> {
        (self.observer.strong_count() == 0)
            .then_some("TransportGone: the tapped transport dropped its observer during the run")
    }
}

/// The observer the transport holds.
struct TapObserver(Arc<TapState>);

impl std::fmt::Debug for TapObserver {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("TapObserver")
    }
}

impl TransportObserver for TapObserver {
    /// Buffers `event`, as [`TapState::push`] does.
    fn observe(&self, event: TransportEvent) {
        self.0.push(Reported::Event(event));
    }

    /// Keeps `probe` for the tap's samples, as [`TapState::probe`] does.
    fn connection(&self, conn_id: ZakuraConnId, probe: ConnectionProbe) {
        self.0.probe(conn_id, probe);
    }
}

impl TransportTap {
    /// A tap on the transport of the node named `node`, whose peers are
    /// named `peers` by index, opening each admitted session's links on
    /// `net`. Returns the observer, for the node's handler and nothing else,
    /// and the reader. Its checkpoint is registered with `sim` now.
    ///
    /// # Panics
    ///
    /// Outside a run, which can't happen while `sim` exists, or if `sim`
    /// already holds a transport tap.
    pub fn new(
        sim: &mut Sim,
        node: &str,
        net: Net,
        peers: Vec<String>,
    ) -> (Arc<dyn TransportObserver>, TransportTap) {
        TransportTap::with_sessions(sim, node, net, peers, None)
    }

    /// [`TransportTap::new`], also feeding every transport event to
    /// `sessions`, the node's session view, if given.
    ///
    /// # Panics
    ///
    /// As [`TransportTap::new`] does.
    pub fn with_sessions(
        sim: &mut Sim,
        node: &str,
        net: Net,
        peers: Vec<String>,
        sessions: Option<SessionEvidence>,
    ) -> (Arc<dyn TransportObserver>, TransportTap) {
        TransportTap::tap(sim, node, net, peers, sessions, TapRole::Node, false)
    }

    /// [`TransportTap::new`], also sampling each connection's resources
    /// after every step into a `load` row when they change, and logging each
    /// connection's shared ID after its registration (sim-9.3).
    ///
    /// # Panics
    ///
    /// As [`TransportTap::new`] does.
    pub fn sampling(
        sim: &mut Sim,
        node: &str,
        net: Net,
        peers: Vec<String>,
    ) -> (Arc<dyn TransportObserver>, TransportTap) {
        TransportTap::tap(sim, node, net, peers, None, TapRole::Node, true)
    }

    /// A sampling tap on the transport of the peer named `peer`, which logs
    /// [`Observation::PeerTransport`] rows and no session rows, and opens no
    /// links (sim-9.3). Returns the observer, for the peer's handler and
    /// nothing else, and the reader.
    ///
    /// # Panics
    ///
    /// Outside a run, which can't happen while `sim` exists.
    pub fn of_peer(
        sim: &mut Sim,
        peer: &str,
        net: Net,
    ) -> (Arc<dyn TransportObserver>, TransportTap) {
        TransportTap::tap(sim, peer, net, Vec::new(), None, TapRole::Peer, true)
    }

    /// A tap of `role` on the transport of the endpoint named `node`.
    fn tap(
        sim: &mut Sim,
        node: &str,
        net: Net,
        peers: Vec<String>,
        sessions: Option<SessionEvidence>,
        role: TapRole,
        sampling: bool,
    ) -> (Arc<dyn TransportObserver>, TransportTap) {
        let run = zakura_rt::sim::run_id().expect("a Sim's scheduler enters a runtime run");
        let observer = Arc::new_cyclic(|observer| {
            TapObserver(Arc::new(TapState {
                run,
                observer: observer.clone(),
                refused: AtomicBool::new(false),
                buffer: Mutex::default(),
                sessions,
                sampling,
                probes: Mutex::default(),
                order: net.order(),
            }))
        });
        let state = observer.0.clone();
        let live: Weak<dyn LiveEvidence> = Arc::<TapState>::downgrade(&state);
        let kind = match role {
            TapRole::Node => EvidenceKind::Transport {
                node: node.to_owned(),
            },
            TapRole::Peer => EvidenceKind::PeerTransport {
                peer: node.to_owned(),
            },
        };
        let checkpoint = Checkpoint::new(kind, live);
        let id = sim.register(checkpoint.clone());
        // Session rows name no tap, so the checkpoint covers them only
        // while the run holds one node tap.
        if role == TapRole::Node {
            assert_eq!(id.index, 0, "a run taps one node's transport");
        }
        let tap = TransportTap {
            state,
            checkpoint,
            id,
            node: node.to_owned(),
            delivered: 0,
            net,
            peers,
            remotes: BTreeMap::new(),
            shared_ids: SharedIds::default(),
            role,
            loads: BTreeMap::new(),
        };
        (observer, tap)
    }

    /// Buffers a `load` row for each probed connection whose resources
    /// changed since the row last logged for it, read inside a boundary
    /// read.
    fn sample(&mut self) {
        let probes = self
            .state
            .probes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        for (conn_id, probe) in probes {
            let load = load(&zakura_rt::sim::boundary_read(|| probe.read()));
            if self.loads.get(&conn_id) != Some(&load) {
                self.loads.insert(conn_id, load.clone());
                self.state.push(Reported::Load { conn_id, load });
            }
        }
    }

    /// The reporter the wire node's dials fail into.
    pub fn dials(&self) -> DialReporter {
        DialReporter(self.state.clone())
    }

    /// Where it hands over the shared ID of each `registered` and
    /// `handshake-refused` event it logs, for the peers' evidence.
    pub(crate) fn shared_ids(&self) -> SharedIds {
        self.shared_ids.clone()
    }

    /// The remote address of registered connection `conn_id`.
    fn remote(&self, conn_id: u64) -> Result<SocketAddr, String> {
        self.remotes.get(&conn_id).copied().ok_or_else(|| {
            format!("AmbiguousIdentity: connection {conn_id} reported before its registration")
        })
    }

    /// The name of the peer that binds `remote`.
    fn peer(&self, remote: SocketAddr) -> Result<&str, String> {
        peer_of(remote)
            .and_then(|index| self.peers.get(index))
            .map(String::as_str)
            .ok_or_else(|| format!("AmbiguousIdentity: no peer binds {remote}"))
    }

    /// `event`, numbered `seq`, as its connection, remote address and plain
    /// fields. A `registered` event records its connection's address first.
    /// It and a `handshake-refused` event hand over their shared ID.
    fn fact(
        &mut self,
        seq: u64,
        event: Reported,
    ) -> Result<(Option<u64>, SocketAddr, TransportFact), String> {
        let event = match event {
            Reported::Event(event) => event,
            Reported::DialFailed { remote, error } => {
                return Ok((None, remote, TransportFact::DialFailed { error }));
            }
            Reported::SharedId { conn_id, id } => {
                let id = id.iter().map(|byte| format!("{byte:02x}")).collect();
                return Ok((
                    Some(conn_id),
                    self.remote(conn_id)?,
                    TransportFact::SharedId { id },
                ));
            }
            Reported::Load { conn_id, load } => {
                return Ok((Some(conn_id), self.remote(conn_id)?, load));
            }
            Reported::Written {
                conn_id,
                quic_stream,
                seq,
                message_type,
                order,
            } => {
                return Ok((
                    Some(conn_id),
                    self.remote(conn_id)?,
                    TransportFact::FrameWritten {
                        quic_stream,
                        seq,
                        message_type,
                        order,
                    },
                ));
            }
        };
        Ok(match event {
            TransportEvent::Negotiated {
                remote,
                local,
                offered,
                negotiated,
            } => (
                None,
                remote,
                TransportFact::Negotiated {
                    local,
                    offered,
                    negotiated,
                },
            ),
            TransportEvent::HandshakeRefused {
                remote,
                shared_id,
                reason,
            } => {
                self.shared_ids.borrow_mut().insert(seq, shared_id);
                (None, remote, TransportFact::HandshakeRefused { reason })
            }
            TransportEvent::Registered {
                conn_id,
                remote,
                shared_id,
            } => {
                if self.remotes.insert(conn_id, remote).is_some() {
                    return Err(format!(
                        "AmbiguousIdentity: connection {conn_id} registered twice"
                    ));
                }
                self.shared_ids.borrow_mut().insert(seq, shared_id);
                (Some(conn_id), remote, TransportFact::Registered)
            }
            TransportEvent::Admitted {
                conn_id,
                quic_stream,
                kind,
                version,
                session_id,
                negotiated_limits,
                inbound_depth,
                outbound_depth,
            } => (
                Some(conn_id),
                self.remote(conn_id)?,
                TransportFact::Admitted {
                    quic_stream,
                    kind,
                    version,
                    session_id,
                    limits: limits(&negotiated_limits),
                    inbound_depth: depth(inbound_depth),
                    outbound_depth: depth(outbound_depth),
                },
            ),
            TransportEvent::HeaderRead {
                conn_id,
                quic_stream,
                seq,
                message_type,
                flags,
                payload_len,
            } => (
                Some(conn_id),
                self.remote(conn_id)?,
                TransportFact::HeaderRead {
                    quic_stream,
                    seq,
                    message_type,
                    flags,
                    payload_len,
                },
            ),
            TransportEvent::FrameRead {
                conn_id,
                quic_stream,
                seq,
            } => (
                Some(conn_id),
                self.remote(conn_id)?,
                TransportFact::FrameRead { quic_stream, seq },
            ),
            TransportEvent::Regulated {
                conn_id,
                quic_stream,
                transition,
            } => (
                Some(conn_id),
                self.remote(conn_id)?,
                TransportFact::Regulated {
                    quic_stream,
                    transition: self::transition(transition)?,
                },
            ),
            TransportEvent::StreamRefused {
                conn_id,
                quic_stream,
                kind,
                version,
                reason,
            } => (
                Some(conn_id),
                self.remote(conn_id)?,
                TransportFact::StreamRefused {
                    quic_stream,
                    kind,
                    version,
                    reason: reason.to_owned(),
                },
            ),
            TransportEvent::FrameWriteStarted {
                conn_id,
                quic_stream,
                seq,
                message_type,
            } => (
                Some(conn_id),
                self.remote(conn_id)?,
                TransportFact::FrameWriteStarted {
                    quic_stream,
                    seq,
                    message_type,
                },
            ),
            TransportEvent::FrameWritten { .. } => {
                unreachable!("`TapState::push` stamps every frame write as `Reported::Written`")
            }
            TransportEvent::CloseDecided {
                remote,
                conn_id,
                cause,
            } => (
                conn_id,
                remote,
                TransportFact::CloseDecided {
                    cause: cause.map(str::to_owned),
                },
            ),
            TransportEvent::TornDown { conn_id } => (
                Some(conn_id),
                self.remote(conn_id)?,
                TransportFact::TornDown,
            ),
            TransportEvent::SessionLifecycle {
                conn_id,
                kind,
                step,
            } => (
                Some(conn_id),
                self.remote(conn_id)?,
                TransportFact::SessionLifecycle {
                    kind,
                    step: session_step(step),
                },
            ),
            // The enum is non-exhaustive, and an event the log can't hold would
            // leave the evidence silently incomplete.
            unknown => return Err(format!("UnknownEvent: the tap can't encode {unknown:?}")),
        })
    }
}

/// `sample` as a `load` row. A connection whose QUIC handles are gone
/// reads as zero load.
fn load(sample: &ConnectionSample) -> TransportFact {
    let quic = sample.quic.as_ref();
    TransportFact::Load {
        receive_window: sample.receive_window,
        data_blocked: quic.map_or(0, |load| load.frame_rx.data_blocked),
        stream_data_blocked: quic.map_or(0, |load| load.frame_rx.stream_data_blocked),
        queued_send_bytes: quic.map_or(0, |load| load.queued_send_bytes),
        send_blocked_streams: quic.map_or(0, |load| load.send_blocked_streams),
        kinds: sample
            .kinds
            .iter()
            .map(|kind| KindLoad {
                kind: kind.kind,
                raw_held: kind.raw_held,
                raw_capacity: kind.raw_capacity,
                raw_excess: kind.raw_excess,
                output_held: kind.output_held,
                output_capacity: kind.output_capacity,
                output_excess: kind.output_excess,
            })
            .collect(),
    }
}

/// `step` in plain fields.
fn session_step(step: SessionStep) -> SessionStepFact {
    match step {
        SessionStep::AttemptStarted { attempt } => SessionStepFact::AttemptStarted { attempt },
        SessionStep::AttemptFailed { attempt } => SessionStepFact::AttemptFailed { attempt },
        SessionStep::MemberArrived { wire_id, member } => {
            SessionStepFact::MemberArrived { wire_id, member }
        }
        SessionStep::OfferExpired { wire_id } => SessionStepFact::OfferExpired { wire_id },
        SessionStep::GenerationInstalled {
            generation,
            attempt,
        } => SessionStepFact::GenerationInstalled {
            generation,
            attempt,
        },
        SessionStep::GenerationParked { generation } => {
            SessionStepFact::GenerationParked { generation }
        }
        SessionStep::GenerationExited {
            generation,
            opened_locally,
            current,
        } => SessionStepFact::GenerationExited {
            generation,
            opened_locally,
            current,
        },
    }
}

/// `limits` in plain fields.
fn limits(limits: &ZakuraConnectionLimits) -> TransportLimits {
    // Production timeouts are seconds to minutes, far inside u64
    // milliseconds.
    let ms = |d: std::time::Duration| u64::try_from(d.as_millis()).unwrap_or(u64::MAX);
    TransportLimits {
        max_frame_bytes: limits.max_frame_bytes,
        max_message_bytes: limits.max_message_bytes,
        max_open_streams: limits.max_open_streams,
        max_inbound_queue_depth: limits.max_inbound_queue_depth,
        idle_timeout_ms: ms(limits.idle_timeout),
        prelude_timeout_ms: ms(limits.prelude_timeout),
        control_timeout_ms: ms(limits.control_timeout),
        stream_open_rate_per_second: limits.stream_open_rate_per_second,
        message_rate_per_second: limits.message_rate_per_second,
    }
}

/// A queue depth as `u64`. A depth of `usize::MAX`, the service's
/// declaration for an unbounded queue, saturates.
fn depth(depth: usize) -> u64 {
    u64::try_from(depth).unwrap_or(u64::MAX)
}

/// `transition` in plain fields, or an error for one this build can't
/// encode.
fn transition(transition: Transition) -> Result<TransitionFact, String> {
    Ok(match transition {
        Transition::HeaderChecked { seq, outcome } => TransitionFact::HeaderChecked {
            seq,
            outcome: match outcome {
                HeaderCheck::Reserved { objects, bytes } => {
                    HeaderCheckFact::Reserved { objects, bytes }
                }
                HeaderCheck::AdmittedTerminal => HeaderCheckFact::AdmittedTerminal,
                HeaderCheck::Refused { reason } => HeaderCheckFact::Refused {
                    reason: reason.label().to_owned(),
                },
            },
        },
        Transition::PageClaimed {
            seq,
            objects,
            bytes,
        } => TransitionFact::PageClaimed {
            seq,
            objects,
            bytes,
        },
        Transition::EndClaimed { subscription } => TransitionFact::EndClaimed { subscription },
        Transition::PublicationEnded { subscription } => {
            TransitionFact::PublicationEnded { subscription }
        }
        Transition::OpenApplied {
            subscription,
            objects,
            bytes,
            seq,
        } => TransitionFact::OpenApplied {
            subscription,
            objects,
            bytes,
            seq,
        },
        Transition::CloseApplied { subscription } => TransitionFact::CloseApplied { subscription },
        Transition::GrantApplied {
            subscription,
            objects,
            bytes,
        } => TransitionFact::GrantApplied {
            subscription,
            objects,
            bytes,
        },
        Transition::OpenIssued {
            subscription,
            objects,
            bytes,
        } => TransitionFact::OpenIssued {
            subscription,
            objects,
            bytes,
        },
        Transition::OpenRetracted { subscription } => {
            TransitionFact::OpenRetracted { subscription }
        }
        Transition::GrantIssued {
            subscription,
            objects,
            bytes,
        } => TransitionFact::GrantIssued {
            subscription,
            objects,
            bytes,
        },
        Transition::CommitmentCounted { seq, message_type } => {
            TransitionFact::CommitmentCounted { seq, message_type }
        }
        Transition::CommitmentReleased { message_type } => {
            TransitionFact::CommitmentReleased { message_type }
        }
        unknown => return Err(format!("UnknownEvent: the tap can't encode {unknown:?}")),
    })
}

/// A tap offers no actions. Its observation drains the observer's buffer
/// into the log.
impl ActionSource for TransportTap {
    /// Nothing.
    fn enabled(&self, _: SimInstant, _: &mut Vec<(Identity, u64)>) {}

    /// Nothing.
    fn boundaries(&self, _: SimInstant, _: &mut Vec<Boundary>) {}

    /// Never called, since the tap offers nothing.
    fn execute(&mut self, _: u64, _: &mut ObservationLog) {
        unreachable!("a transport tap offers no actions");
    }

    /// Logs every event reported since the last observation, in order, each
    /// admitted session followed by its `Session` row, and records both in
    /// the checkpoint, so a row that goes missing or changes fails the run
    /// like a lost event. An event it can't encode or place fails the run.
    fn observe(&mut self, log: &mut ObservationLog) -> Result<(), String> {
        // Only the run holding the checkpoint checks what this delivers.
        if log.run() != self.id.run {
            return Err("ForeignRun: transport tap registered with another run".to_owned());
        }
        if self.state.sampling {
            self.sample();
        }
        let events = mem::take(&mut self.state.buffer().events);
        let last = events.last().map_or(self.delivered, |(seq, _)| *seq);
        let mut sent = Vec::with_capacity(events.len());
        for (seq, event) in events {
            let (connection, remote, event) = self.fact(seq, event)?;
            if self.role == TapRole::Peer {
                let obs = Observation::PeerTransport {
                    instance: self.id.index,
                    seq,
                    peer: self.node.clone(),
                    connection,
                    remote: remote.to_string(),
                    event,
                };
                log.push(obs.clone());
                sent.push(obs);
                continue;
            }
            let session = match (&event, connection) {
                (
                    TransportFact::Admitted {
                        quic_stream,
                        kind,
                        version,
                        session_id,
                        ..
                    },
                    Some(connection),
                ) => Some(Observation::Session {
                    connection,
                    peer: self.peer(remote)?.to_owned(),
                    quic_stream: *quic_stream,
                    kind: *kind,
                    version: *version,
                    session_id: *session_id,
                }),
                _ => None,
            };
            let obs = Observation::Transport {
                instance: self.id.index,
                seq,
                node: self.node.clone(),
                connection,
                remote: remote.to_string(),
                event,
            };
            log.push(obs.clone());
            sent.push(obs);
            if let Some(session) = session {
                self.net.admitted(NODE_ADDR, remote);
                log.push(session.clone());
                sent.push(session);
            }
        }
        self.delivered = last;
        // The run checks its own log against this, since the log this tap
        // was handed may not be the run's.
        self.checkpoint.record(log.step(), &sent, last);
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

impl Drop for TransportTap {
    /// Fails a run that's still live, since later events go undelivered.
    fn drop(&mut self) {
        self.checkpoint.dropped();
    }
}
