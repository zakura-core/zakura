//! The evidence hub's boundary reads (M6a-7a, rule 1 of the observation
//! contract).
//!
//! A shared component's owner registers a read as it builds the component,
//! through its observer, which keeps it in a [`ReadSink`]. At each step
//! boundary, after the step's other records are written and before `Sim`
//! logs its task outcome, [`BoundaryReads`] runs every live read inside
//! [`zakura_rt::sim::boundary_read`], where any use of the executor or a
//! task wake panics. A read that panics fails the run as a harness failure. Each read folds into the view's component,
//! which moves its `fresh_through` to this boundary, and a read that
//! differs from the last one logged is logged as
//! [`Observation::ComponentRead`] with the next `seq`. A read of an owner
//! that dropped is `Gone`, which ends its component, and it isn't run
//! again.
//!
//! The budget's existence evidence is its reactor's first `Settled`, since
//! the reactor builds the budget before it. The harness registers the
//! selected chain's read itself, as it builds the node's port, so that
//! read's existence evidence is the harness's construction.
//!
//! On `header-sync/wire` the handler registers the supervisor map's read
//! through the transport observer (M6a-7b), and the map exists from the
//! first `Registered`. Each header-sync service registers its session
//! table's read through the reactor's observer, under its allocation ID.
//! The wire harness builds two services on one reactor handle, one kept by
//! its `HeaderNode` and one in the handler's registry, so the harness names
//! the registry's, and the hub runs that one's read only. The table exists
//! from the first header stream's `Admitted`. Every install and removal
//! record must name that service, and each registry fanout that reached
//! header sync must follow exactly one install or refusal for its
//! connection since the connection's previous fanout (see [`Installs`]).

use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
    rc::Rc,
    sync::{Arc, Mutex, PoisonError},
};

use zakura_network::zakura::{
    diag::{self, ComponentRead, OwnerState, ReadKey},
    ServicePeerDirection, ZAKURA_CAP_HEADER_SYNC, ZAKURA_STREAM_HEADER_SYNC,
};
use zakura_storage::header::controlled::ControlledPort;

use super::{
    tap::read_key,
    view::{HeaderSyncView, Reading},
};
use crate::{
    observe::{
        ComponentReading, ComponentValue, InstallRow, MapRow, ReactorFact, SessionRow,
        TransportFact,
    },
    property::{ComponentKind, Read},
    storage::{hex, point},
    Observation, ObservationLog,
};

/// The reads an observer was handed, in registration order, waiting for
/// the hub.
pub(crate) type ReadSink = Arc<Mutex<Vec<(ReadKey, ComponentRead)>>>;

thread_local! {
    /// Set while [`without_reads`] runs its closure.
    static READS_OFF: Cell<bool> = const { Cell::new(false) };
}

/// Runs `run` with the boundary reads of every hub built inside it turned
/// off: registrations are dropped, and no component they'd feed is
/// registered, so a run can be compared, or timed, with and without reads.
pub fn without_reads<T>(run: impl FnOnce() -> T) -> T {
    /// Turns reads back on when `run` returns or unwinds.
    struct On;

    impl Drop for On {
        fn drop(&mut self) {
            READS_OFF.with(|off| off.set(false));
        }
    }

    READS_OFF.with(|off| off.set(true));
    let _on = On;
    run()
}

/// A read as the hub runs it: what the view folds, or a failure for a read
/// it can't fold. The log's form is built only when the read changed (see
/// [`record`]), so an unchanged read costs no allocation.
type HarnessRead = Box<dyn Fn() -> Result<Read<Reading>, String>>;

#[cfg(test)]
thread_local! {
    /// Set while [`without_selected_chain_read`] runs its closure.
    static NO_CHAIN_READ: Cell<bool> = const { Cell::new(false) };
}

/// Runs `run` with every harness built inside it skipping the selected
/// chain's read registration, though the chain still exists.
#[cfg(test)]
pub(crate) fn without_selected_chain_read<T>(run: impl FnOnce() -> T) -> T {
    NO_CHAIN_READ.with(|skip| skip.set(true));
    let out = run();
    NO_CHAIN_READ.with(|skip| skip.set(false));
    out
}

/// One registered read the hub still runs.
struct Live {
    kind: ComponentKind,
    generation: u64,
    read: HarnessRead,
    /// The last read logged, if any.
    last: Option<Read<Reading>>,
    /// The changes logged so far.
    seq: u64,
}

/// The node reactor's registered reads, folded into the node's view.
pub(crate) struct BoundaryReads {
    sink: ReadSink,
    view: Rc<RefCell<HeaderSyncView>>,
    /// The node reactor's tap, whose first `Settled` shows the budget
    /// exists.
    reactor: u64,
    live: Vec<Live>,
    generations: BTreeMap<ReadKey, u64>,
    budget_exists: bool,
    /// The node transport tap's reads and its instance, on a tier with a
    /// transport.
    transport: Option<(ReadSink, u64)>,
    /// The allocation ID of the header-sync service the registry holds,
    /// whose session table the view reads.
    header_service: Option<u64>,
    map_exists: bool,
    sessions_exist: bool,
    installs: Installs,
    /// Built under [`without_reads`].
    off: bool,
}

impl BoundaryReads {
    /// The reads registered into `sink` by the reactor on tap `reactor`,
    /// folded into `view`, or none if `off`, as for a hub that writes
    /// nothing.
    pub(crate) fn new(
        sink: ReadSink,
        view: Rc<RefCell<HeaderSyncView>>,
        reactor: u64,
        off: bool,
    ) -> Self {
        BoundaryReads {
            sink,
            view,
            reactor,
            live: Vec::new(),
            generations: BTreeMap::new(),
            budget_exists: false,
            transport: None,
            header_service: None,
            map_exists: false,
            sessions_exist: false,
            installs: Installs::default(),
            off: off || READS_OFF.with(Cell::get),
        }
    }

    /// Takes the reads the node's transport tap, instance `instance`,
    /// keeps in `sink`, with their registrations in its rows.
    pub(crate) fn transport(&mut self, sink: ReadSink, instance: u64) {
        self.transport = Some((sink, instance));
    }

    /// Reads the session table of header-sync service `service`, the one
    /// the node's registry holds.
    pub(crate) fn header_sessions(&mut self, service: u64) {
        self.header_service = Some(service);
    }

    /// Starts running `read`, registered under `key`, if the view reads
    /// its component. Another service's session table isn't one.
    fn adopt(
        &mut self,
        view: &mut HeaderSyncView,
        key: ReadKey,
        read: ComponentRead,
    ) -> Result<(), String> {
        let kind = match &key {
            ReadKey::HeaderSessions { service } if Some(*service) != self.header_service => {
                return Ok(());
            }
            key => kind(key)?,
        };
        let generation = self.generations.entry(key.clone()).or_default();
        *generation += 1;
        view.register(kind);
        self.live.push(Live {
            kind,
            generation: *generation,
            read: Box::new(move || reading(&key, read())),
            last: None,
            seq: 0,
        });
        Ok(())
    }

    /// Takes this step's registrations and existence evidence, then runs
    /// every live read at the boundary, logging each that changed. Call it
    /// once the step's other records are in `log`.
    pub(crate) fn boundary(&mut self, log: &mut ObservationLog) -> Result<(), String> {
        let take = |sink: &ReadSink| {
            std::mem::take(&mut *sink.lock().unwrap_or_else(PoisonError::into_inner))
        };
        let registered = take(&self.sink);
        let transported = self.transport.as_ref().map(|(sink, _)| take(sink));
        if self.off {
            return Ok(());
        }
        let step = log.step();
        let view = self.view.clone();
        let mut view = view.borrow_mut();
        let transport = self.transport.as_ref().map(|(_, instance)| *instance);
        // In the order the owners reported them, so a component that shows
        // it exists before its read is registered fails, even within one
        // step. The observer is handed each read right before its
        // `ReadRegistered`, so the next one handed over is that row's. A
        // row with none means the observer dropped the read, which fails
        // the run at once, even if the component never shows it exists.
        let mut handed = registered.into_iter().peekable();
        let mut handed_transport = transported.unwrap_or_default().into_iter().peekable();
        for obs in log.of_step(step) {
            match obs {
                Observation::Reactor {
                    instance, event, ..
                } if *instance == self.reactor => match event {
                    ReactorFact::ReadRegistered { key: name, .. } => {
                        let (key, read) = handed
                            .next_if(|(key, _)| read_key(key).is_ok_and(|key| key == *name))
                            .ok_or_else(|| unhanded(name, step))?;
                        self.adopt(&mut view, key, read)?;
                    }
                    ReactorFact::Settled { .. } if !self.budget_exists => {
                        self.budget_exists = true;
                        view.exists(ComponentKind::Budget, step);
                    }
                    _ => {}
                },
                Observation::Transport {
                    instance,
                    connection,
                    event,
                    ..
                } if Some(*instance) == transport => {
                    match event {
                        TransportFact::ReadRegistered { key: name, .. } => {
                            let (key, read) = handed_transport
                                .next_if(|(key, _)| read_key(key).is_ok_and(|key| key == *name))
                                .ok_or_else(|| unhanded(name, step))?;
                            self.adopt(&mut view, key, read)?;
                        }
                        TransportFact::Registered if !self.map_exists => {
                            self.map_exists = true;
                            view.exists(ComponentKind::SupervisorMap, step);
                        }
                        TransportFact::Admitted { kind, .. }
                            if *kind == ZAKURA_STREAM_HEADER_SYNC && !self.sessions_exist =>
                        {
                            self.sessions_exist = true;
                            view.exists(ComponentKind::HeaderSessions, step);
                        }
                        _ => {}
                    }
                    if let Some(connection) = connection {
                        self.installs
                            .row(*connection, event, self.header_service)
                            .map_err(|why| format!("{why} at step {step}"))?;
                    }
                }
                _ => {}
            }
        }
        if let Some((key, _)) = handed.next().or_else(|| handed_transport.next()) {
            return Err(format!(
                "UnloggedRead: {key:?}'s read was handed over with no ReadRegistered row at \
                 step {step}"
            ));
        }
        // Each read runs on its own, so one that reaches the runtime fails
        // the run as a harness failure, after the mark is cleared, instead
        // of unwinding through `Sim`.
        let reads: Vec<Result<Result<Read<Reading>, String>, String>> = self
            .live
            .iter()
            .map(|live| {
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    zakura_rt::sim::boundary_read(|| (live.read)())
                }))
                .map_err(|panic| {
                    format!(
                        "FailedRead: {:?}'s read (generation {}) panicked at step {step}: {}",
                        live.kind,
                        live.generation,
                        panic_message(&*panic)
                    )
                })
            })
            .collect();
        for (live, read) in self.live.iter_mut().zip(reads) {
            let read = read??;
            if live.last.as_ref() != Some(&read) {
                live.seq += 1;
                log.push(Observation::ComponentRead {
                    component: format!("{:?}", live.kind),
                    generation: live.generation,
                    seq: live.seq,
                    read: record(&read),
                });
                live.last = Some(read.clone());
            }
            view.read(live.kind, step, read);
        }
        self.live.retain(|live| live.last != Some(Read::Gone));
        Ok(())
    }
    /// Registers the selected chain's read of the node's `port`, which the
    /// harness just built, so the chain exists from setup on. The read
    /// peeks the engine's committed snapshot with no scheduling point, and
    /// is held while a task holds the engine's lock.
    pub(crate) fn selected_chain(&mut self, port: &Arc<ControlledPort>) {
        if self.off {
            return;
        }
        let kind = ComponentKind::SelectedChain;
        let mut view = self.view.borrow_mut();
        #[cfg(test)]
        if NO_CHAIN_READ.with(Cell::get) {
            view.exists(kind, 0);
            return;
        }
        view.register(kind);
        view.exists(kind, 0);
        let port = Arc::downgrade(port);
        self.live.push(Live {
            kind,
            generation: 1,
            read: Box::new(move || {
                // Dropped before the read returns, and never the last one,
                // since the harness keeps the port.
                let Some(port) = port.upgrade() else {
                    return Ok(Read::Gone);
                };
                let peeked = port.engine().peek_snapshot();
                drop(port);
                Ok(match peeked {
                    Some(chain) => Read::Current(Reading::SelectedChain(chain)),
                    None => Read::Held {
                        holder: "a task".to_owned(),
                    },
                })
            }),
            last: None,
            seq: 0,
        });
    }
}

/// The failure for `name`'s `ReadRegistered` row at `step`, whose read the
/// observer never handed over.
fn unhanded(name: &str, step: u64) -> String {
    format!("UnhandedRead: {name}'s read-registered row at step {step} came with no read")
}

/// `direction` as the log names it.
fn direction(direction: ServicePeerDirection) -> String {
    match direction {
        ServicePeerDirection::Inbound => "inbound",
        ServicePeerDirection::Outbound => "outbound",
    }
    .to_owned()
}

/// A panic's message, if it has one.
fn panic_message(panic: &(dyn std::any::Any + Send)) -> &str {
    panic
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("a non-string panic")
}

/// The component `key` reads.
fn kind(key: &ReadKey) -> Result<ComponentKind, String> {
    match key {
        ReadKey::Budget => Ok(ComponentKind::Budget),
        ReadKey::SupervisorMap => Ok(ComponentKind::SupervisorMap),
        ReadKey::HeaderSessions { .. } => Ok(ComponentKind::HeaderSessions),
        unknown => Err(format!("UnknownRead: no component reads {unknown:?}")),
    }
}

/// `read`, of the component `key` names, as the view folds it.
fn reading(key: &ReadKey, read: diag::Read<OwnerState>) -> Result<Read<Reading>, String> {
    Ok(match (key, read) {
        (ReadKey::Budget, diag::Read::Current(OwnerState::Budget(counters))) => {
            Read::Current(Reading::Budget(counters))
        }
        (ReadKey::SupervisorMap, diag::Read::Current(OwnerState::SupervisorMap(entries))) => {
            Read::Current(Reading::SupervisorMap(entries))
        }
        (
            ReadKey::HeaderSessions { service },
            diag::Read::Current(OwnerState::HeaderSessions(sessions)),
        ) => Read::Current(Reading::HeaderSessions {
            service: *service,
            sessions,
        }),
        (key, diag::Read::Current(unknown)) => {
            return Err(format!(
                "UnknownRead: {key:?}'s read holds {unknown:?}, which no component reads"
            ))
        }
        (_, diag::Read::Held { holder }) => Read::Held { holder },
        (_, diag::Read::Gone) => Read::Gone,
    })
}

/// Each connection's header-sync install records since its last registry
/// fanout (M6a-7b). The header service installs or refuses inside the
/// registry's `add_peer` call, so each fanout that reached header sync
/// follows exactly one install record of its connection, and one that
/// didn't follows none. An `Installed` stands as an effect even if the
/// table changes again before a boundary.
#[derive(Default)]
struct Installs {
    /// Install records seen since each connection's last fanout.
    pending: BTreeMap<u64, u64>,
}

impl Installs {
    /// Folds `event`, a transport row of `connection`, where `service` is
    /// the header-sync service the registry holds.
    fn row(
        &mut self,
        connection: u64,
        event: &TransportFact,
        service: Option<u64>,
    ) -> Result<(), String> {
        let named = match event {
            TransportFact::Installed { service, .. }
            | TransportFact::InstallRefused { service, .. } => {
                *self.pending.entry(connection).or_default() += 1;
                *service
            }
            TransportFact::Removed { service, .. }
            | TransportFact::RemoveIgnored { service, .. } => *service,
            TransportFact::RegistryFanout { admitted_mask, .. } => {
                let installs = self.pending.remove(&connection).unwrap_or(0);
                let expected = u64::from(admitted_mask & ZAKURA_CAP_HEADER_SYNC != 0);
                if installs != expected {
                    return Err(format!(
                        "FanoutInstalls: connection {connection}'s fanout with mask \
                         {admitted_mask:#x} follows {installs} header-sync install records \
                         since its last, not {expected}"
                    ));
                }
                return Ok(());
            }
            _ => return Ok(()),
        };
        if Some(named) != service {
            return Err(format!(
                "ForeignService: connection {connection}'s session record names header-sync \
                 service {named}, but the registry holds {service:?}"
            ));
        }
        Ok(())
    }
}

/// `read` as the log holds it.
fn record(read: &Read<Reading>) -> ComponentReading {
    match read {
        // Counts of at most 4,000 headers fit u64.
        Read::Current(Reading::Budget(counters)) => {
            ComponentReading::Current(ComponentValue::Budget {
                capacity: counters.capacity as u64,
                reserved: counters.reserved as u64,
                owned: counters.owned as u64,
            })
        }
        Read::Current(Reading::SelectedChain(chain)) => {
            ComponentReading::Current(ComponentValue::SelectedChain {
                header_generation: chain.header_generation.get(),
                finalized: point(chain.frontiers.finalized),
                header_best: point(chain.frontiers.header_best),
                verified_best: point(chain.frontiers.verified_best),
            })
        }
        Read::Current(Reading::SupervisorMap(entries)) => {
            ComponentReading::Current(ComponentValue::SupervisorMap {
                entries: entries
                    .iter()
                    .map(|entry| MapRow {
                        connection: entry.conn_id,
                        peer: hex(&entry.peer.digest()),
                        remote_ip: entry.remote_ip.map(|ip| ip.to_string()),
                    })
                    .collect(),
            })
        }
        Read::Current(Reading::HeaderSessions { service, sessions }) => {
            ComponentReading::Current(ComponentValue::HeaderSessions {
                service: *service,
                sessions: sessions
                    .iter()
                    .map(|session| SessionRow {
                        peer: hex(&session.peer.digest()),
                        connection: session.conn_id,
                        session_id: session.session_id,
                        direction: direction(session.direction),
                        installed: session.installed.map(|facts| InstallRow {
                            negotiated: facts.negotiated,
                            stream_version: facts.stream_version,
                            quic_stream: facts.quic_stream,
                            direction: direction(facts.direction),
                        }),
                    })
                    .collect(),
            })
        }
        Read::Current(Reading::Reactor(_)) => {
            unreachable!("the reactor reports its own state and registers no read")
        }
        Read::Held { holder } => ComponentReading::Held {
            holder: holder.clone(),
        },
        Read::Gone => ComponentReading::Gone,
    }
}
