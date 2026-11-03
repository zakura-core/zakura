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
//! The tap is the transport observer. It buffers each event, and the run
//! drains the buffer at every observation into [`Observation::Transport`],
//! checked through its checkpoint as reactor taps are. A connection's events
//! before registration carry only its remote address. Its `registered`
//! event joins that address to the connection's ID, and every admitted
//! session gets an [`Observation::Session`] row naming the peer that bound
//! the address. That's also when the tap opens loss on the session's links,
//! through [`Net::admitted`]. A dial of the node's that fails before any
//! connection registers is reported through the tap too, as `dial-failed`
//! keyed by the dialed address (M5-F).

use std::{
    collections::BTreeMap,
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
        HeaderCheck, HeaderSyncService, Service, ServiceRegistry, Transition, TransportEvent,
        TransportObserver, ZakuraConnectionLimits, ZakuraEndpoint, ZakuraHandshakeConfig,
        ZakuraLocalLimits, ZakuraProtocolHandler, ZakuraSupervisorHandle, ZakuraTrace,
    },
    Config,
};
use zakura_quic::{
    sim::{SeededInputs, SimNet},
    NodeAddr, QuicEndpoint,
};
use zakura_rt::{sim::RunId, sync::watch};
use zakura_scheduler::SimInstant;

use super::header_node::HeaderNode;
use crate::{
    evidence::{Checkpoint, EvidenceId, EvidenceKind, LiveEvidence},
    net::Net,
    observe::{HeaderCheckFact, TransitionFact, TransportFact, TransportLimits},
    peer::{key_index, peer_addr, peer_of, NODE_ADDR},
    ActionSource, Boundary, Identity, Observation, ObservationLog, Sim,
};

/// The node's network config: production defaults on the fixture's
/// network.
fn config(network: &Network) -> Config {
    Config {
        network: network.clone(),
        ..Config::default()
    }
}

/// The node's endpoint and the limits it dials with, once it's bound.
type Bound = Option<(ZakuraEndpoint, ZakuraLocalLimits)>;

/// The node under test on real QUIC.
pub struct WireNode {
    /// The reactor, its port and its handle.
    pub header: HeaderNode,
    seed: [u8; 32],
    /// Set once the node serves.
    endpoint: watch::Receiver<Bound>,
    /// Where its failed dials go.
    dials: DialReporter,
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
        let (bound, endpoint) = watch::channel(None);
        let service: Arc<dyn Service> = Arc::new(HeaderSyncService::new(header.handle.clone()));
        let config = config(network);
        sim.scheduler().spawn_root("wire-node", async move {
            let limits = ZakuraLocalLimits::from_config(&config);
            let quic = QuicEndpoint::bind_with(
                SeededInputs::node_key(seed, 0),
                &limits.quic_bind_config(vec![NODE_ADDR]),
                &limits.quic,
                net,
                SeededInputs::new(seed, 0).endpoint_inputs(),
            )
            .expect("the node binds its address");
            let registry = ServiceRegistry::new(vec![service]).expect("header sync registers");
            let handler = ZakuraProtocolHandler::new_with_registry_and_trace(
                ZakuraSupervisorHandle::new(config.zakura.max_connections_per_ip),
                config.network.clone(),
                ZakuraHandshakeConfig::for_network(&config.network),
                limits.clone(),
                Arc::new(registry),
                ZakuraTrace::noop(),
            )
            .with_local_node_id(quic.local_id())
            .with_transport_observer(observer);
            quic.serve(handler.clone()).expect("the node serves");
            bound.send_replace(Some((ZakuraEndpoint::new(quic, handler), limits)));
            // The endpoint lives as long as the run.
            std::future::pending::<()>().await;
        });
        WireNode {
            header,
            seed,
            endpoint,
            dials,
        }
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
                dials.report(remote, error.to_string());
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
}

impl TapState {
    /// Locks the buffer. Every change completes under the lock, so a panic
    /// elsewhere can't leave it half changed.
    fn buffer(&self) -> std::sync::MutexGuard<'_, Buffer> {
        self.buffer.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Numbers and buffers `event`, or refuses it outside the tap's run.
    /// Neither step yields, and the buffer's std lock is never held across
    /// another call.
    fn push(&self, event: Reported) {
        if zakura_rt::sim::run_id() != Some(self.run) {
            self.refused.store(true, Ordering::Relaxed);
            return;
        }
        let mut buffer = self.buffer();
        buffer.last = buffer
            .last
            .checked_add(1)
            .expect("a run reports fewer than 2^64 events");
        let seq = buffer.last;
        buffer.events.push((seq, event));
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
        let run = zakura_rt::sim::run_id().expect("a Sim's scheduler enters a runtime run");
        let observer = Arc::new_cyclic(|observer| {
            TapObserver(Arc::new(TapState {
                run,
                observer: observer.clone(),
                refused: AtomicBool::new(false),
                buffer: Mutex::default(),
            }))
        });
        let state = observer.0.clone();
        let live: Weak<dyn LiveEvidence> = Arc::<TapState>::downgrade(&state);
        let checkpoint = Checkpoint::new(
            EvidenceKind::Transport {
                node: node.to_owned(),
            },
            live,
        );
        let id = sim.register(checkpoint.clone());
        // Session rows name no tap, so the checkpoint covers them only
        // while the run holds one.
        assert_eq!(id.index, 0, "a run taps one node's transport");
        let tap = TransportTap {
            state,
            checkpoint,
            id,
            node: node.to_owned(),
            delivered: 0,
            net,
            peers,
            remotes: BTreeMap::new(),
        };
        (observer, tap)
    }

    /// The reporter the wire node's dials fail into.
    pub fn dials(&self) -> DialReporter {
        DialReporter(self.state.clone())
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

    /// `event` as its connection, remote address and plain fields. A
    /// `registered` event records its connection's address first.
    fn fact(
        &mut self,
        event: Reported,
    ) -> Result<(Option<u64>, SocketAddr, TransportFact), String> {
        let event = match event {
            Reported::Event(event) => event,
            Reported::DialFailed { remote, error } => {
                return Ok((None, remote, TransportFact::DialFailed { error }));
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
            TransportEvent::HandshakeRefused { remote, reason } => {
                (None, remote, TransportFact::HandshakeRefused { reason })
            }
            TransportEvent::Registered { conn_id, remote } => {
                if self.remotes.insert(conn_id, remote).is_some() {
                    return Err(format!(
                        "AmbiguousIdentity: connection {conn_id} registered twice"
                    ));
                }
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
            TransportEvent::FrameWritten {
                conn_id,
                quic_stream,
                seq,
                message_type,
            } => (
                Some(conn_id),
                self.remote(conn_id)?,
                TransportFact::FrameWritten {
                    quic_stream,
                    seq,
                    message_type,
                },
            ),
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
            // The enum is non-exhaustive, and an event the log can't hold would
            // leave the evidence silently incomplete.
            unknown => return Err(format!("UnknownEvent: the tap can't encode {unknown:?}")),
        })
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
        let events = mem::take(&mut self.state.buffer().events);
        let last = events.last().map_or(self.delivered, |(seq, _)| *seq);
        let mut sent = Vec::with_capacity(events.len());
        for (seq, event) in events {
            let (connection, remote, event) = self.fact(event)?;
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
}

impl Drop for TransportTap {
    /// Fails a run that's still live, since later events go undelivered.
    fn drop(&mut self) {
        self.checkpoint.dropped();
    }
}
