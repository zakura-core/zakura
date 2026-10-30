//! The evidence hub: one writer for everything the header harnesses'
//! sources observe, in the order it happened (M6a-1).
//!
//! Each record a source reports carries the run-wide position it was
//! emitted at ([`zakura_rt::sim::next_position`]): reactor and transport
//! events where their observers buffer them, storage facts where the
//! controlled port publishes them, datagram events where the network
//! records them, and peer records where the peer's task makes them. On a
//! header harness, every source hands its records to the hub instead of
//! the log, through an [`Out`]. The hub is added last, so at each
//! observation it has the whole step's records. It sorts them by position,
//! derives the rows they mean in that order, and writes the step through
//! [`ObservationLog::push_keyed`], so the log's physical order is the order
//! things happened.
//!
//! A derived row is keyed `(pos, k)` after the record it came from, so it
//! sits right after its source and before anything later: a `FrameJudged`
//! or `ConnectionClosed` after its transport event, a `WorkAdmitted` or
//! `UnframedWork` after the reactor record it came from (see
//! [`StagedWork`]), and a peer row placed
//! by the identity map after the record that let it resolve, naming the
//! peer record's own position as its origin. After writing, the hub feeds
//! the step to the storage policy's predicates in the same order.
//!
//! Once the step's task outcome is logged too, the hub's
//! [`settle`](ActionSource::settle) folds each tapped reactor's settled
//! state (see [`ReactorLife`]) and closes the owners' boundary.

use std::{cell::RefCell, collections::BTreeMap, mem, rc::Rc};

use super::header_node::{
    settled::ReactorLife,
    staging::StagedWork,
    tap::ReactorLink,
    view::{HeaderSyncView, Owners},
    PredicateFeed,
};
use crate::{
    peer::{PeerRecord, PeerRecords, WireEvidence},
    ActionSource, Boundary, HarnessFailure, Identity, Observation, ObservationLog,
};

/// The position a record the run's tasks emit now takes, from
/// [`zakura_rt::sim::next_position`]. In unit tests,
/// [`without_capture`] turns it off, with the hub, so a run can be
/// compared with its capture paths skipped.
pub(crate) fn position() -> Option<u64> {
    #[cfg(test)]
    if CAPTURE_OFF.with(std::cell::Cell::get) {
        return None;
    }
    zakura_rt::sim::next_position()
}

#[cfg(test)]
thread_local! {
    /// Set while [`without_capture`] runs its closure.
    static CAPTURE_OFF: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Runs `run` with [`position`] and every hub built inside it turned off:
/// sources write straight into the log, unstamped, and their task-side
/// capture paths take no position.
#[cfg(test)]
pub(crate) fn without_capture<T>(run: impl FnOnce() -> T) -> T {
    CAPTURE_OFF.with(|off| off.set(true));
    let out = run();
    CAPTURE_OFF.with(|off| off.set(false));
    out
}

/// One row a source observed, with the position it was emitted at and its
/// place among the rows at that position: 0 for the record itself, from 1
/// for a row the source derived from it.
#[derive(Clone, Debug)]
pub(crate) struct Row {
    pub(crate) pos: Option<u64>,
    pub(crate) sub: u32,
    pub(crate) obs: Observation,
}

impl Row {
    /// `obs`, as emitted at `pos`.
    pub(crate) fn at(pos: Option<u64>, obs: Observation) -> Self {
        Row { pos, sub: 0, obs }
    }
}

/// The rows the hub's sources handed it this step.
type Batch = Rc<RefCell<Vec<Row>>>;

/// Where an evidence source writes what it observed: straight into the log,
/// keyed as it's appended, or into an evidence hub's batch.
#[derive(Clone, Debug, Default)]
pub(crate) struct Out(Option<Batch>);

impl Out {
    /// Hands `rows` to the hub, or appends them to `log` in order.
    pub(crate) fn write(&self, log: &mut ObservationLog, rows: Vec<Row>) {
        match &self.0 {
            Some(batch) => batch.borrow_mut().extend(rows),
            None => rows.into_iter().for_each(|row| log.push(row.obs)),
        }
    }

    /// Whether a hub writes these rows.
    pub(crate) fn to_hub(&self) -> bool {
        self.0.is_some()
    }
}

/// The header harnesses' evidence writer. It offers no actions.
#[derive(Default)]
pub struct EvidenceHub {
    batch: Batch,
    /// The scripted peers' records and the identity map that places them,
    /// on `header-sync/wire`.
    peers: Option<Peers>,
    /// The storage policy's predicates, fed each written step.
    predicates: Option<Rc<RefCell<PredicateFeed>>>,
    /// The owner view the hub closes each boundary of.
    owners: Option<Rc<RefCell<dyn Owners>>>,
    /// Each tapped reactor's settled-state fold (M6a-3).
    reactors: Vec<Rc<RefCell<ReactorLife>>>,
    /// Built under [`without_capture`], so it writes nothing.
    #[cfg(test)]
    off: bool,
}

/// What the hub keeps for the scripted peers.
struct Peers {
    records: PeerRecords,
    evidence: WireEvidence,
    /// The reactor's staged work, joined to the peers' frames (M6a-5a).
    staged: StagedWork,
    /// Records drained and not yet placed, oldest first.
    pending: Vec<PeerRecord>,
}

/// One thing the hub orders: a source's row, or a peer's record.
#[allow(
    clippy::large_enum_variant,
    reason = "rows are nearly every item, and boxing each would allocate it again"
)]
enum Item {
    Row(Row),
    Emitted(PeerRecord),
}

impl Item {
    /// The position it was emitted at.
    fn pos(&self) -> Option<u64> {
        match self {
            Item::Row(row) => row.pos,
            Item::Emitted(record) => record.pos,
        }
    }

    /// Its place among the rows at its position.
    fn sub(&self) -> u32 {
        match self {
            Item::Row(row) => row.sub,
            Item::Emitted(_) => 0,
        }
    }
}

impl EvidenceHub {
    /// A hub with no sources yet.
    pub(crate) fn new() -> Self {
        EvidenceHub {
            #[cfg(test)]
            off: CAPTURE_OFF.with(std::cell::Cell::get),
            ..EvidenceHub::default()
        }
    }

    /// Where a source hands the hub its rows.
    pub(crate) fn out(&self) -> Out {
        #[cfg(test)]
        if self.off {
            return Out(None);
        }
        Out(Some(self.batch.clone()))
    }

    /// Places the scripted peers' `records` through the identity map.
    pub(crate) fn place_peers(&mut self, records: PeerRecords) {
        self.peers = Some(Peers {
            records,
            evidence: WireEvidence::default(),
            staged: StagedWork::default(),
            pending: Vec::new(),
        });
    }

    /// Feeds each written step to `predicates`.
    pub(crate) fn feed(&mut self, predicates: Rc<RefCell<PredicateFeed>>) {
        self.predicates = Some(predicates);
    }

    /// Closes each boundary of `owners` after writing its step.
    pub(crate) fn owners(&mut self, owners: Rc<RefCell<dyn Owners>>) {
        self.owners = Some(owners);
    }

    /// Folds `link`'s reactor into a component at each boundary, and into
    /// `view`'s reactor component too, if it's given. Returns the fold.
    pub(crate) fn reactor(
        &mut self,
        link: ReactorLink,
        view: Option<Rc<RefCell<HeaderSyncView>>>,
    ) -> Rc<RefCell<ReactorLife>> {
        let life = Rc::new(RefCell::new(ReactorLife::new(link, view)));
        self.reactors.push(life.clone());
        life
    }

    /// Closes the owners' boundary after `step`, if the hub has owners.
    fn close_owners(&self, step: u64) -> Result<(), String> {
        self.owners
            .as_ref()
            .map_or(Ok(()), |owners| owners.borrow_mut().close(step))
    }

    /// Orders this step's rows and the peers' new records by position, and
    /// derives the rows they mean, each keyed after the record it came from.
    /// Returns the step's rows in key order, and the first failure.
    fn order(&mut self, log: &ObservationLog) -> (Vec<Keyed>, Result<(), String>) {
        let mut items: Vec<Item> = mem::take(&mut *self.batch.borrow_mut())
            .into_iter()
            .map(Item::Row)
            .collect();
        if let Some(peers) = &self.peers {
            items.extend(peers.records.take().into_iter().map(Item::Emitted));
        }
        let mut failed = Ok(());
        // Outside a run nothing has a position, and the rows keep the order
        // they were handed over in, after the log's last key.
        let in_run = zakura_rt::sim::run_id().is_some();
        if let Some(item) = items.iter().find(|item| in_run && item.pos().is_none()) {
            failed = Err(match item {
                Item::Row(row) => {
                    format!("Unstamped: a source logged {:?} with no position", row.obs)
                }
                Item::Emitted(record) => {
                    format!("Unstamped: a peer recorded {record:?} with no position")
                }
            });
        }
        let (last, _) = log.last_key();
        let mut fallback = last;
        let mut keyed: Vec<(u64, u32, Item)> = items
            .into_iter()
            .map(|item| {
                let pos = item.pos().unwrap_or_else(|| {
                    fallback += 1;
                    fallback
                });
                (pos, item.sub(), item)
            })
            .collect();
        // Stable, so rows at one key keep their source's order.
        keyed.sort_by_key(|(pos, sub, _)| (*pos, *sub));
        // A source's own derived rows, such as a `Session` after its
        // `admitted`, come first at their position.
        let mut emitted_subs = BTreeMap::new();
        for (pos, sub, _) in &keyed {
            let max = emitted_subs.entry(*pos).or_insert(0);
            *max = (*max).max(*sub);
        }
        let mut rows = Vec::with_capacity(keyed.len());
        let mut at = Derive::default();
        for (pos, sub, item) in keyed {
            at.next(pos, emitted_subs[&pos]);
            let placeable = match item {
                Item::Row(row) => {
                    let derived = match self.peers.as_mut() {
                        Some(peers) => peers.evidence.row(&row.obs).and_then(|mut derived| {
                            derived.extend(peers.staged.row(&row.obs, &peers.evidence)?);
                            Ok(derived)
                        }),
                        None => Ok(Vec::new()),
                    };
                    let identity = matches!(
                        row.obs,
                        Observation::Transport { .. } | Observation::Session { .. }
                    );
                    rows.push(Keyed {
                        key: (pos, sub),
                        origin: None,
                        obs: row.obs,
                    });
                    match derived {
                        Ok(derived) => {
                            rows.extend(derived.into_iter().map(|obs| at.derived(pos, obs)))
                        }
                        Err(why) => {
                            failed = failed.and(Err(why));
                        }
                    }
                    identity
                }
                Item::Emitted(record) => {
                    // Logged in its own step, whether or not it resolves.
                    rows.push(Keyed {
                        key: (pos, sub),
                        origin: None,
                        obs: record.emitted(),
                    });
                    if let Some(peers) = self.peers.as_mut() {
                        peers.pending.push(record);
                    }
                    true
                }
            };
            if !placeable {
                continue;
            }
            let Some(peers) = self.peers.as_mut() else {
                continue;
            };
            match peers.evidence.place(&mut peers.pending) {
                Ok(placed) => rows.extend(placed.into_iter().map(|(origin, obs)| Keyed {
                    origin: Some(origin),
                    ..at.derived(pos, obs)
                })),
                Err(why) => failed = failed.and(Err(why)),
            }
        }
        if let Some(peers) = self.peers.as_mut() {
            peers.evidence.forget_unresolvable(&mut peers.pending);
            peers.records.set_unplaced(peers.pending.len());
        }
        rows.sort_by_key(|row| row.key);
        (rows, failed)
    }
}

/// One row the hub writes, with its log key and origin.
struct Keyed {
    key: (u64, u32),
    origin: Option<u64>,
    obs: Observation,
}

/// The next derived key at the position being ordered.
#[derive(Default)]
struct Derive {
    pos: u64,
    sub: u32,
}

impl Derive {
    /// Moves to an item at `pos`, whose source's own rows there end at
    /// `emitted`.
    fn next(&mut self, pos: u64, emitted: u32) {
        if pos != self.pos {
            self.pos = pos;
            self.sub = emitted;
        }
    }

    /// `obs`, derived from the record at `pos`, keyed after every row there
    /// so far.
    fn derived(&mut self, pos: u64, obs: Observation) -> Keyed {
        self.sub = self
            .sub
            .checked_add(1)
            .expect("a record derives fewer than 2^32 rows");
        Keyed {
            key: (pos, self.sub),
            origin: Some(pos),
            obs,
        }
    }
}

/// The hub offers no actions. Its observation writes the step.
impl ActionSource for EvidenceHub {
    /// Nothing.
    fn enabled(&self, _: zakura_scheduler::SimInstant, _: &mut Vec<(Identity, u64)>) {}

    /// Nothing.
    fn boundaries(&self, _: zakura_scheduler::SimInstant, _: &mut Vec<Boundary>) {}

    /// Never called, since the hub offers nothing.
    fn execute(&mut self, _: u64, _: &mut ObservationLog) {
        unreachable!("the evidence hub offers no actions");
    }

    /// Writes this step's rows in key order and feeds them to the
    /// predicates. Every row is written even when one fails, so a failure a
    /// source's record explains is logged with it.
    fn observe(&mut self, log: &mut ObservationLog) -> Result<(), String> {
        #[cfg(test)]
        if self.off {
            return Ok(());
        }
        let (rows, ordered) = self.order(log);
        let start = log.entries().len();
        let mut written = Ok(());
        for row in rows {
            if let Err(why) = log.push_keyed(row.key, row.origin, row.obs) {
                written = written.and(Err(why));
            }
        }
        let fed = match &self.predicates {
            Some(feed) => feed
                .borrow_mut()
                .feed(log.entries()[start..].iter().map(|entry| &entry.obs)),
            None => Ok(()),
        };
        ordered.and(written).and(fed)
    }

    /// Folds each reactor's rows and its task's outcome, now that the step
    /// is logged, then closes the owners' boundary.
    fn settle(&mut self, log: &ObservationLog) -> Result<(), HarnessFailure> {
        let mut settled = Ok(());
        for life in &self.reactors {
            settled = settled.and(life.borrow_mut().settle(log));
        }
        settled?;
        self.close_owners(log.step())
            .map_err(|detail| HarnessFailure::SourceEvidence {
                source: "evidence hub".to_owned(),
                detail,
            })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        net::{Ipv4Addr, SocketAddr},
        sync::Arc,
    };

    use zakura_chain::block;
    use zakura_network::zakura::{
        observe::{ReactorEvent, WaitCause},
        v9::RegtestMessage,
        ZakuraPeerId,
    };
    use zakura_node_services::header_chain::Port;
    use zakura_quic::{sim::LinkModel, Transmit, UdpIo};
    use zakura_storage::header::{controlled::ControlledPort, fixture::HeaderFixture};

    use super::*;
    use crate::{
        check::{Checker, Phase, StepCtx},
        end::{IdleStatus, Outcome, RunEnd},
        harness::{
            header_node::{tap::ReactorTap, Effects, Policy, PolicySource},
            NetScenario,
        },
        model::Label,
        observe::{DatagramOutcome, Entry, RowAt, StorageFact, StreamId, TransportFact},
        peer::{peer_addr, RecordBody},
        property::{Assumptions, HasView, Registry, RegistryChecker, Source, Sources},
        table::table_properties,
        HarnessFailure, MemorySink, NoChecks, Plan, PropertyOutcome, RunConfig, Sim,
    };

    /// Header sync's stream, kind 5 at version 9.
    const S5V9: StreamId = StreamId {
        kind: 5,
        version: 9,
    };

    /// A hub that places the scripted peers' records, and the records.
    fn hub() -> (EvidenceHub, PeerRecords) {
        let records = PeerRecords::default();
        let mut hub = EvidenceHub::new();
        hub.place_peers(records.clone());
        (hub, records)
    }

    /// Hands `rows` to `hub` in the order given, as its sources would.
    fn hand(hub: &EvidenceHub, log: &mut ObservationLog, rows: Vec<Row>) {
        hub.out().write(log, rows);
    }

    /// Row `obs` at `(pos, 0)`.
    fn at(pos: u64, obs: Observation) -> Row {
        Row::at(Some(pos), obs)
    }

    /// A transport event on connection 1 from peer 0's first address.
    fn transport(event: TransportFact) -> Observation {
        Observation::Transport {
            instance: 0,
            seq: 0,
            node: "node".to_owned(),
            connection: Some(1),
            remote: peer_addr(0, 0).to_string(),
            event,
        }
    }

    /// Connection 1's session on QUIC stream 4.
    fn session() -> Observation {
        Observation::Session {
            connection: 1,
            peer: "peer-0".to_owned(),
            quic_stream: 4,
            kind: S5V9.kind,
            version: S5V9.version,
            session_id: 7,
        }
    }

    /// A storage submission of operation `id`.
    fn submitted(id: u64) -> Observation {
        Observation::Storage {
            service: "header".to_owned(),
            instance: 0,
            seq: id,
            txn: id,
            fact: StorageFact::Submitted {
                id,
                request: crate::observe::StorageRequest::ContinuationLocator,
                digest: String::new(),
                call: None,
            },
        }
    }

    /// The node's close of connection 1 for a header fault.
    fn close() -> Observation {
        transport(TransportFact::CloseDecided {
            cause: Some("header_peer_fault".to_owned()),
        })
    }

    /// The log's observations, in order.
    fn logged(log: &ObservationLog) -> Vec<&Observation> {
        log.entries().iter().map(|entry| &entry.obs).collect()
    }

    /// A transport close emitted before the node admits work from a frame,
    /// but handed to the hub after it, as the reactor's source comes first,
    /// is logged first. `receiver-work-authorized` then judges the work by
    /// that order: admitted after the close.
    #[test]
    fn a_close_and_an_effect_keep_their_order() {
        let row = RowAt::new(S5V9, "headers");
        let mut log = ObservationLog::default();
        // A conformant page of exchange 7, judged earlier in the run.
        log.push(Observation::PeerFrame {
            connection: 1,
            stream: S5V9,
            quic_stream: Some(4),
            seq: 0,
            row: Some("headers".to_owned()),
            rule_stream: S5V9,
            label: Label::Conformant,
            exhausted: false,
            exchange: Some(7),
            payload: 300,
        });
        log.push(Observation::FrameJudged {
            connection: 1,
            stream: S5V9,
            quic_stream: Some(4),
            seq: 0,
        });
        let work = Observation::WorkAdmitted {
            connection: 1,
            frame: row.clone(),
            seq: 0,
            request: RowAt::new(S5V9, "subscribe-headers"),
            exchange: 7,
            objects: 1,
            bytes: 200,
            ends: false,
        };
        let (mut hub, _) = hub();
        hand(&hub, &mut log, vec![at(20, work), at(10, close())]);
        hub.observe(&mut log).expect("the step is whole");
        let entries = log.entries();
        let closed = entries
            .iter()
            .position(|entry| matches!(entry.obs, Observation::ConnectionClosed { .. }))
            .expect("the close is projected");
        let admitted = entries
            .iter()
            .position(|entry| matches!(entry.obs, Observation::WorkAdmitted { .. }))
            .expect("the work is logged");
        assert!(closed < admitted, "{:?}", logged(&log));
        // The frame is conformant, judged and big enough, so only the close
        // can fail the work.
        let verdict = crate::downstream::work_authorized(entries, admitted, &row)
            .expect("the check judges admitted work");
        assert!(
            verdict.is_err(),
            "work admitted after its connection's close fails"
        );
    }

    /// The hub writes a storage fact and a transport close in key order,
    /// whichever source handed it over first, and each derived row right
    /// after its record.
    #[test]
    fn the_sorter_orders_by_key() {
        for handed in [
            vec![at(5, submitted(1)), at(4, close())],
            vec![at(4, close()), at(5, submitted(1))],
        ] {
            let (mut hub, _) = hub();
            let mut log = ObservationLog::default();
            hand(&hub, &mut log, handed);
            hub.observe(&mut log).expect("the step is whole");
            let keys: Vec<(u64, u32)> = log.entries().iter().map(|e| (e.pos, e.sub)).collect();
            assert_eq!(keys, [(4, 0), (4, 1), (5, 0)]);
            assert!(
                matches!(
                    logged(&log)[..],
                    [
                        Observation::Transport { .. },
                        Observation::ConnectionClosed { .. },
                        Observation::Storage { .. }
                    ]
                ),
                "{:?}",
                logged(&log)
            );
            assert_eq!(
                log.entries()[1].origin,
                Some(4),
                "the close names its record"
            );
        }
    }

    /// Peer records on a stream the node never admits keep their own
    /// positions around the node's close of the connection, one before it
    /// and one after, and neither resolves, even once the connection is torn
    /// down, which doesn't fail the run.
    #[test]
    fn an_unresolved_peer_record_keeps_its_place_around_a_close() {
        let (mut hub, records) = hub();
        let mut log = ObservationLog::default();
        let frame = RecordBody::Frame {
            seq: 5,
            row: Some("headers".to_owned()),
            rule_stream: S5V9,
            label: Label::OverCredit,
            exhausted: true,
            exchange: Some(7),
            payload: 263,
        };
        for (pos, body) in [(5, frame), (15, RecordBody::Written { seq: 5 })] {
            records.push(PeerRecord {
                pos: Some(pos),
                remote: peer_addr(0, 0),
                quic_stream: Some(4),
                body,
            });
        }
        let registered = transport(TransportFact::Registered);
        hand(&hub, &mut log, vec![at(1, registered), at(10, close())]);
        hub.observe(&mut log)
            .expect("the stream may still be admitted");
        let rows: Vec<(u64, &Observation)> = log
            .entries()
            .iter()
            .map(|entry| (entry.pos, &entry.obs))
            .collect();
        assert!(
            matches!(
                rows[..],
                [
                    (1, Observation::Transport { .. }),
                    (5, Observation::PeerEmitted { .. }),
                    (10, Observation::Transport { .. }),
                    (10, Observation::ConnectionClosed { .. }),
                    (15, Observation::PeerEmitted { .. }),
                ]
            ),
            "{rows:?}"
        );
        assert_eq!(records.unplaced(), 2, "neither resolves");
        // Once the connection is torn down, neither can, and each keeps only
        // its row.
        hand(
            &hub,
            &mut log,
            vec![at(20, transport(TransportFact::TornDown))],
        );
        hub.observe(&mut log)
            .expect("an unresolved record doesn't fail the run");
        assert_eq!(records.unplaced(), 0, "both are forgotten");
        let emitted = log
            .entries()
            .iter()
            .filter(|entry| matches!(entry.obs, Observation::PeerEmitted { .. }))
            .count();
        assert_eq!(emitted, 2);
        // The frame's row keeps its whole record, since nothing else will.
        assert!(log.entries().iter().any(|entry| matches!(
            &entry.obs,
            Observation::PeerEmitted {
                body: crate::observe::EmittedBody::Frame {
                    rule_stream: S5V9,
                    label: Label::OverCredit,
                    exhausted: true,
                    exchange: Some(7),
                    ..
                },
                ..
            }
        )));
        assert!(!log
            .entries()
            .iter()
            .any(|entry| matches!(entry.obs, Observation::PeerWritten { .. })));
    }

    /// A source that hands the hub one row with no position at its first
    /// observation.
    struct Unstamped(Out);

    impl ActionSource for Unstamped {
        fn enabled(&self, _: zakura_scheduler::SimInstant, _: &mut Vec<(Identity, u64)>) {}

        fn boundaries(&self, _: zakura_scheduler::SimInstant, _: &mut Vec<Boundary>) {}

        fn execute(&mut self, _: u64, _: &mut ObservationLog) {}

        fn observe(&mut self, log: &mut ObservationLog) -> Result<(), String> {
            let row = Row::at(
                None,
                Observation::Fake {
                    source: "unstamped".to_owned(),
                    event: "emitted".to_owned(),
                },
            );
            self.0.write(log, vec![row]);
            Ok(())
        }
    }

    /// Outside a run nothing has a position: a `SimNet` event, a controlled
    /// port's fact, and a hub keys unstamped rows after the log's last key.
    /// Inside a run, a hub refuses an unstamped row as a harness failure.
    #[test]
    fn positions_are_absent_outside_a_run_and_required_inside() {
        assert_eq!(zakura_rt::sim::next_position(), None);
        let net = zakura_quic::sim::SimNet::new(LinkModel::default());
        let _socket = net
            .bind(SocketAddr::from((Ipv4Addr::LOCALHOST, 4000)))
            .expect("a fresh address binds");
        let events = net.positioned_events_since(0);
        assert!(!events.is_empty() && events.iter().all(|(_, pos)| pos.is_none()));
        let fixture = HeaderFixture::regtest();
        let port = ControlledPort::new(&fixture);
        let mut call = std::pin::pin!(port.selected_chain_contains(fixture.anchor().hash));
        let polled = std::future::Future::poll(
            call.as_mut(),
            &mut std::task::Context::from_waker(std::task::Waker::noop()),
        );
        assert!(polled.is_pending(), "the call waits for its completion");
        let facts = port.take_facts();
        assert!(!facts.is_empty() && facts.iter().all(|fact| fact.pos.is_none()));

        let (mut hub, _) = hub();
        let mut log = ObservationLog::default();
        log.push(Observation::Fake {
            source: "outside".to_owned(),
            event: "first".to_owned(),
        });
        let mut unstamped = Unstamped(hub.out());
        unstamped.observe(&mut log).expect("it hands the row over");
        hub.observe(&mut log)
            .expect("outside a run, a row needs no position");
        let keys: Vec<(u64, u32)> = log.entries().iter().map(|e| (e.pos, e.sub)).collect();
        assert_eq!(keys, [(1, 0), (2, 0)]);

        let mut sim = Sim::new(RunConfig::default(), Plan::from_fuzz(&[]));
        let hub = EvidenceHub::new();
        sim.add_source(Box::new(Unstamped(hub.out())));
        sim.add_source(Box::new(hub));
        let report = sim.run(&mut NoChecks, &mut MemorySink::default());
        assert!(
            matches!(
                &report.end.harness,
                Some(HarnessFailure::SourceEvidence { detail, .. }) if detail.starts_with("Unstamped")
            ),
            "{:?}",
            report.end
        );
    }

    /// A view-less harness, for the generated properties.
    struct Logged;

    impl HasView for Logged {
        type View = ();

        /// Nothing.
        fn view(&self) {}
    }

    /// `status/reserved-flags-closes`'s outcome when the node judges a
    /// `Status` with a reserved flag bit at `judged` and something closes
    /// its connection with a fault at `closed`, as one step's records.
    fn reserved_flags_outcome(judged: u64, closed: u64) -> PropertyOutcome {
        let (mut hub, records) = hub();
        let mut log = ObservationLog::default();
        let mut checker = RegistryChecker::new(
            Registry::new(table_properties::<Logged>(
                "header-sync",
                &RegtestMessage::LAYOUT,
            ))
            .expect("generated IDs are unique"),
            Rc::new(Logged),
            Assumptions::none(),
            &Sources::observed(&[Source::Frames, Source::Closes, Source::Storage]),
        );
        let status = RegtestMessage::ROWS
            .iter()
            .find(|rule| rule.name == "status")
            .expect("header sync has a status row");
        let check = |checker: &mut RegistryChecker<Logged>, log: &ObservationLog, step| {
            let pick = Identity::peer("peer-0", "act");
            checker.after_step(&StepCtx {
                step,
                pick: &pick,
                enabled: &[],
                phase: Phase::Live,
                now: zakura_scheduler::SimInstant::ZERO,
                status: zakura_scheduler::Status::Running,
                log,
            });
        };
        log.begin(1);
        records.push(PeerRecord {
            pos: Some(3),
            remote: peer_addr(0, 0),
            quic_stream: Some(4),
            body: RecordBody::Frame {
                seq: 0,
                row: Some("status".to_owned()),
                rule_stream: S5V9,
                label: Label::ReservedFlags,
                exhausted: false,
                exchange: None,
                payload: 0,
            },
        });
        let registered = transport(TransportFact::Registered);
        hand(&hub, &mut log, vec![at(1, registered), at(2, session())]);
        hub.observe(&mut log).expect("the frame is placed");
        check(&mut checker, &log, 1);
        log.begin(2);
        let header = transport(TransportFact::HeaderRead {
            quic_stream: 4,
            seq: 0,
            message_type: status.message_type,
            flags: 1,
            payload_len: 0,
        });
        let fault = Observation::ConnectionClosed {
            connection: 1,
            cause: "header_peer_fault".to_owned(),
        };
        hand(&hub, &mut log, vec![at(judged, header), at(closed, fault)]);
        hub.observe(&mut log).expect("the step is whole");
        check(&mut checker, &log, 2);
        let end = RunEnd {
            outcome: Outcome::Idle {
                status: IdleStatus::Quiescent,
            },
            steps: 2,
            violation: None,
            drain: None,
            harness: None,
        };
        checker
            .at_end(&end, &log)
            .into_iter()
            .find(|report| report.property == "header-sync/s5v9/status/reserved-flags-closes")
            .expect("the table generates the property")
            .outcome
    }

    /// A header judged before a close opens its consequence, which the
    /// close discharges. Judged after the close, it opens none. Either way
    /// the derived `FrameJudged` sits right after its `HeaderRead`.
    #[test]
    fn a_judgment_and_a_close_keep_their_order() {
        assert_eq!(
            reserved_flags_outcome(10, 20),
            PropertyOutcome::Held { applicable: 1 }
        );
        assert_eq!(
            reserved_flags_outcome(20, 10),
            PropertyOutcome::NotApplicable
        );
    }

    /// Sends one `len`-byte datagram from `socket` to `dst`.
    fn send(socket: &zakura_quic::sim::SimSocket, dst: SocketAddr, len: usize) {
        let transmit = Transmit {
            destination: dst,
            ecn: None,
            size: len,
            segment_size: None,
            src_ip: None,
        };
        socket
            .try_send(&transmit, &vec![0; len])
            .expect("a simulated send never fails");
    }

    /// A run with the network and a reactor tap behind a hub, and a task
    /// that spawns a task, reports a reactor event, sends a datagram,
    /// reports another and exits, all in one step. Returns the report.
    fn one_turn() -> crate::RunReport {
        let mut sim = Sim::new(RunConfig::default(), Plan::from_fuzz(&[]));
        let mut hub = EvidenceHub::new();
        let net = NetScenario::default().net();
        net.write_to(hub.out());
        sim.add_source(Box::new(net.clone()));
        let port = Arc::new(ControlledPort::new(&HeaderFixture::regtest()));
        let storage = PolicySource::new(
            &mut sim,
            port.clone(),
            Policy::converging(),
            Effects::default(),
        );
        let (observer, tap) = ReactorTap::new(&mut sim, &port, "node");
        sim.add_source(Box::new(storage.with_tap(tap).behind(&mut hub)));
        sim.add_source(Box::new(hub));
        let (a, b) = (
            SocketAddr::from((Ipv4Addr::new(10, 0, 0, 1), 4000)),
            SocketAddr::from((Ipv4Addr::new(10, 0, 0, 2), 4000)),
        );
        let (from, to) = (
            net.sim().bind(a).expect("a binds"),
            net.sim().bind(b).expect("b binds"),
        );
        let task_observer = observer.clone();
        let waiting = move |session_id| ReactorEvent::OfferWaiting {
            peer: ZakuraPeerId::new(vec![1; 32]).expect("32 bytes is a valid peer id"),
            session_id,
            target_tip_hash: block::Hash([2; 32]),
            cause: WaitCause::StagingFull,
        };
        sim.scheduler().spawn_root("t", async move {
            // A spawn reaches a point first, so the rest runs in one step.
            drop(zakura_rt::task::spawn_named("later", async {}));
            task_observer.emit(waiting(1));
            send(&from, b, 10);
            task_observer.emit(waiting(2));
        });
        let report = sim.run(&mut NoChecks, &mut MemorySink::default());
        drop((observer, to, port));
        report
    }

    /// A datagram a task sends between two of its reactor records is logged
    /// between them, though the network's source drains first.
    #[test]
    fn a_datagram_keeps_its_position_against_a_node_record() {
        let report = one_turn();
        assert_eq!(report.end.harness, None, "{:?}", report.end);
        let order: Vec<&str> = report
            .log
            .entries()
            .iter()
            .filter_map(|entry| match &entry.obs {
                Observation::Reactor { .. } => Some("reactor"),
                Observation::Datagram {
                    outcome: DatagramOutcome::Sent,
                    ..
                } => Some("sent"),
                _ => None,
            })
            .collect();
        assert_eq!(order, ["reactor", "sent", "reactor"]);
    }

    /// In a step where the reactor's task reports events, sends a datagram,
    /// spawns a task and exits, and the run first sees the new task ready,
    /// every row's key grows in the log's physical order, whichever source
    /// appended it.
    #[test]
    fn every_row_has_a_growing_key() {
        let report = one_turn();
        assert_eq!(report.end.harness, None, "{:?}", report.end);
        let entries = report.log.entries();
        let turn = entries
            .iter()
            .find(|entry| matches!(entry.obs, Observation::Reactor { .. }))
            .expect("the task reports reactor events")
            .step;
        let step: Vec<&Entry> = entries.iter().filter(|entry| entry.step == turn).collect();
        let has = |pred: fn(&Observation) -> bool| step.iter().any(|entry| pred(&entry.obs));
        assert!(has(|obs| matches!(obs, Observation::Datagram { .. })));
        assert!(has(|obs| matches!(obs, Observation::TaskExited { .. })));
        assert!(
            has(|obs| matches!(obs, Observation::TaskSpawned { task } if task.contains("later"))),
            "{step:?}"
        );
        for pair in entries.windows(2) {
            assert!(
                (pair[0].pos, pair[0].sub) < (pair[1].pos, pair[1].sub),
                "{pair:?}"
            );
        }
    }

    /// The step choices and every task stop of the saved honest-sync Plan,
    /// and its log, with capture on or, under [`without_capture`], off.
    fn honest_sync(capture: bool) -> (Vec<String>, Vec<String>, crate::RunReport) {
        use crate::harness::{
            header_sync::HeaderWire, run_prepared, wire_plans::SavedPlan, Subsystem,
        };
        let fixture = format!(
            "{}/tests/fixtures/header-wire-{}.bin",
            env!("CARGO_MANIFEST_DIR"),
            SavedPlan::HonestSync.name()
        );
        let input = std::fs::read(fixture).expect("the fixture is committed");
        let (prefix, bytes) = input.split_at(HeaderWire::SCENARIO_BYTES);
        let scenario = HeaderWire::draw(prefix);
        let run = || {
            let mut sink = MemorySink::default();
            let report = run_prepared::<HeaderWire>(
                &scenario,
                HeaderWire::prepare(&scenario),
                Plan::from_fuzz(bytes),
                &mut sink,
                None,
            )
            .unwrap_or_else(|error| panic!("{error}"));
            (sink, report)
        };
        let (sink, report) = if capture { run() } else { without_capture(run) };
        // Each step's choices, without the `obs` digest, which the capture
        // changes.
        let choices = sink
            .records
            .iter()
            .filter_map(|record| match record {
                crate::Record::Step(step) => Some(format!(
                    "{} {} {} {:?} {:?} {:?}",
                    step.step, step.pick, step.options, step.by, step.offset, step.draws
                )),
                _ => None,
            })
            .collect();
        let stops = report
            .log
            .entries()
            .iter()
            .filter(|entry| entry.obs.ran_task().is_some())
            .map(|entry| format!("{} {:?}", entry.step, entry.obs))
            .collect();
        (choices, stops, report)
    }

    /// Capture takes no scheduling point: the saved honest sync makes the
    /// same choices and stops its tasks at the same points with the hub
    /// and its task-side positions on as with them off. The run with them
    /// on reaches every capture path first: reactor and transport events,
    /// peer records, datagrams and storage facts, each with a position.
    #[test]
    fn positions_add_no_scheduling_point() {
        let (choices, stops, report) = honest_sync(true);
        assert_eq!(report.end.harness, None, "{:?}", report.end);
        let entries = report.log.entries();
        let reached = |pred: fn(&Observation) -> bool| {
            entries
                .iter()
                .any(|entry| entry.sub == 0 && pred(&entry.obs))
        };
        assert!(reached(|obs| matches!(obs, Observation::Reactor { .. })));
        assert!(reached(|obs| matches!(obs, Observation::Transport { .. })));
        assert!(reached(|obs| matches!(obs, Observation::Datagram { .. })));
        assert!(reached(|obs| matches!(obs, Observation::Storage { .. })));
        assert!(
            entries.iter().any(|entry| entry.sub > 0
                && entry.origin != Some(entry.pos)
                && matches!(entry.obs, Observation::PeerFrame { .. })),
            "a peer record resolves by its own position"
        );
        let (choices_off, stops_off, off) = honest_sync(false);
        assert_eq!(off.end.harness, None, "{:?}", off.end);
        assert!(
            !off.log.entries().iter().any(|entry| entry.sub > 0),
            "capture was off"
        );
        assert_eq!(choices.len(), choices_off.len());
        for (on, off) in choices.iter().zip(&choices_off) {
            assert_eq!(on, off);
        }
        assert_eq!(stops.len(), stops_off.len());
        for (on, off) in stops.iter().zip(&stops_off) {
            assert_eq!(on, off);
        }
    }

    /// Staged work's crafted evidence (M6a-5a): a node on connection 1's
    /// session 7, QUIC stream 4, whose reactor runs incarnation 1 for
    /// subscription 8, and the peer's conformant, judged page frames 0 to 3
    /// and its outcome frame 4.
    mod staged {
        use super::*;
        use crate::observe::{
            HeldDigest, ReactorFact, ReactorFrame, ReactorHolding, ReactorLease, ReactorPurpose,
            ReactorRequest, ReactorSnapshot, ReactorWork, ReactorWorkState, StorageBranch,
            UnframedCause, WorkOwner, WorkScope,
        };

        /// `event` as the node reactor's row.
        fn reactor(event: ReactorFact) -> Observation {
            Observation::Reactor {
                instance: 0,
                seq: 0,
                storage_seq: 0,
                node: "node".to_owned(),
                event,
            }
        }

        /// Incarnation 1's start, for subscription 8 on session 7.
        fn started() -> Observation {
            let scope = WorkScope {
                header_generation: 1,
                branch: StorageBranch {
                    anchor: "aa".into(),
                    target: "bb".into(),
                },
            };
            reactor(ReactorFact::Started {
                request: ReactorRequest {
                    peer: "01".into(),
                    session_id: 7,
                    request_id: 8,
                    incarnation: 1,
                    owner: WorkOwner {
                        kind: "header".into(),
                        scope,
                        verified_generation: None,
                        body_work_epoch: None,
                        session_id: 7,
                        request_id: 8,
                    },
                    target: "bb".into(),
                },
                purpose: ReactorPurpose::Normal,
            })
        }

        /// A made-up one-entry digest.
        fn entry(value: u64) -> HeldDigest {
            HeldDigest { value, len: 1 }
        }

        /// The digest of `entries`, composed as the node does.
        fn of(entries: &[HeldDigest]) -> HeldDigest {
            entries
                .iter()
                .fold(HeldDigest::default(), |digest, next| digest.then(*next))
        }

        /// Frame `seq` on QUIC stream 4, whose entries digest to `received`.
        fn frame(seq: u64, received: HeldDigest) -> ReactorFrame {
            ReactorFrame {
                quic_stream: 4,
                seq,
                payload_len: 300,
                received,
            }
        }

        /// Incarnation 1 staging frame `seq`'s `received`, holding `after`.
        fn staged(seq: u64, received: HeldDigest, after: HeldDigest) -> Observation {
            reactor(ReactorFact::Staged {
                incarnation: 1,
                frame: Some(frame(seq, received)),
                count: received.len,
                after,
            })
        }

        /// A settled snapshot where incarnation 1 holds `held`, with
        /// `leased` headers in its leases.
        fn settled(turn: u64, held: HeldDigest, leased: u64) -> Observation {
            let ReactorFact::Started { request, .. } = (match started() {
                Observation::Reactor { event, .. } => event,
                _ => unreachable!("a reactor row"),
            }) else {
                unreachable!("a start");
            };
            reactor(ReactorFact::Settled {
                turn,
                last: false,
                snapshot: Some(Box::new(ReactorSnapshot {
                    work: vec![ReactorWork {
                        peer: "01".into(),
                        session_id: 7,
                        target: "bb".into(),
                        state: ReactorWorkState::Active {
                            incarnation: 1,
                            request_id: 8,
                            owner: Box::new(request.owner),
                            purpose: ReactorPurpose::Normal,
                            target_phase: "receiving".into(),
                            held: held.len,
                            held_digest: held,
                            cursor: None,
                        },
                    }],
                    holdings: vec![ReactorHolding {
                        peer: "01".into(),
                        reservation: None,
                        leases: vec![ReactorLease {
                            lease: 2,
                            count: leased,
                        }],
                    }],
                    ..ReactorSnapshot::default()
                })),
            })
        }

        /// The peer's frames 0 to 4, conformant and judged: pages of
        /// subscription 8, then its outcome.
        fn peer_frames(log: &mut ObservationLog) {
            for seq in 0..5 {
                log.push(Observation::PeerFrame {
                    connection: 1,
                    stream: S5V9,
                    quic_stream: Some(4),
                    seq,
                    row: Some(
                        if seq < 4 {
                            "headers"
                        } else {
                            "headers-outcome"
                        }
                        .to_owned(),
                    ),
                    rule_stream: S5V9,
                    label: Label::Conformant,
                    exhausted: false,
                    exchange: Some(8),
                    payload: 300,
                });
                log.push(Observation::FrameJudged {
                    connection: 1,
                    stream: S5V9,
                    quic_stream: Some(4),
                    seq,
                });
            }
        }

        /// Writes the session, the start, then `rows`, as one step, and
        /// returns the log.
        fn write(rows: Vec<Observation>) -> Result<ObservationLog, String> {
            let (mut hub, _) = hub();
            let mut log = ObservationLog::default();
            peer_frames(&mut log);
            let rows = [session(), started()]
                .into_iter()
                .chain(rows)
                .enumerate()
                .map(|(pos, obs)| at(100 + pos as u64, obs))
                .collect();
            hand(&hub, &mut log, rows);
            hub.observe(&mut log).map(|()| log)
        }

        /// The `WorkAdmitted` and `UnframedWork` rows of `log`, with
        /// `receiver-work-authorized`'s verdict on each, for the page row.
        fn derived(log: &ObservationLog) -> Vec<(Observation, bool)> {
            let page = RowAt::new(S5V9, "headers");
            let entries = log.entries();
            (0..entries.len())
                .filter(|at| {
                    matches!(
                        entries[*at].obs,
                        Observation::WorkAdmitted { .. } | Observation::UnframedWork { .. }
                    )
                })
                .map(|at| {
                    let page_verdict = crate::downstream::work_authorized(entries, at, &page);
                    let outcome_verdict = crate::downstream::work_authorized(
                        entries,
                        at,
                        &RowAt::new(S5V9, "headers-outcome"),
                    );
                    let ok = page_verdict.or(outcome_verdict).map(|v| v.is_ok());
                    (entries[at].obs.clone(), ok.expect("the check judges it"))
                })
                .collect()
        }

        /// Held work's `UnframedWork`, with its cause and objects.
        fn unframed(cause: UnframedCause, objects: u64) -> Observation {
            Observation::UnframedWork {
                connection: 1,
                request: RowAt::new(S5V9, "subscribe-headers"),
                exchange: 8,
                objects,
                bytes: 0,
                cause,
            }
        }

        /// Page frame `seq`'s `WorkAdmitted`, of `objects` headers.
        fn admitted(seq: u64, objects: u64) -> Observation {
            Observation::WorkAdmitted {
                connection: 1,
                frame: RowAt::new(S5V9, "headers"),
                seq,
                request: RowAt::new(S5V9, "subscribe-headers"),
                exchange: 8,
                objects,
                bytes: 300,
                ends: false,
            }
        }

        /// Each staged page is admitted work right after its record, named
        /// by its frame on the session's connection, and
        /// `receiver-work-authorized` holds on it. Matching digests derive
        /// nothing else.
        #[test]
        fn staged_work_is_admitted_after_its_record() {
            let page = of(&[entry(1), entry(2)]);
            let log =
                write(vec![staged(0, page, page), settled(1, page, 2)]).expect("the step is whole");
            assert_eq!(derived(&log), [(admitted(0, 2), true)]);
            let entries = log.entries();
            let at = entries
                .iter()
                .position(|entry| matches!(entry.obs, Observation::WorkAdmitted { .. }))
                .expect("logged");
            assert!(matches!(
                entries[at - 1].obs,
                Observation::Reactor {
                    event: ReactorFact::Staged { .. },
                    ..
                }
            ));
            assert_eq!(entries[at].origin, Some(entries[at - 1].pos));
        }

        /// A second staging site consumes capacity for an entry and
        /// appends it, with no `Staged`. The next settled boundary holds
        /// more than the frames explain, which is `UnframedWork{Extra}`, and
        /// `receiver-work-authorized` fails on it while the framed page
        /// still passes. Its leases cover what it holds, so the reactor's
        /// work ledger accepts the boundary and the property, not a harness
        /// failure, judges it. An append with no lease is a budget break
        /// instead, which the ledger fails as `Unaccounted` (M6a-4).
        ///
        /// This fails if the check passes unframed work.
        #[test]
        fn staging_by_another_writer_reaches_the_downstream_check() {
            use crate::harness::header_node::work::WorkLedger;

            let page = of(&[entry(1), entry(2)]);
            let extra = page.then(entry(9));
            let log = write(vec![staged(0, page, page), settled(1, extra, 3)])
                .expect("the step is whole");
            assert_eq!(
                derived(&log),
                [
                    (admitted(0, 2), true),
                    (unframed(UnframedCause::Extra, 1), false)
                ]
            );

            // The ledger's view of the same boundary, after the round's
            // placement and binding.
            let ledger = |leased| {
                let mut ledger = WorkLedger::default();
                let facts = [
                    ReactorFact::Placed {
                        reservation: 1,
                        peer: "01".into(),
                        count: 3,
                        placement: None,
                    },
                    ReactorFact::Bound {
                        reservation: 1,
                        incarnation: 1,
                    },
                ];
                let rows = [started(), staged(0, page, page), settled(1, extra, leased)];
                let rows = rows.into_iter().map(|row| match row {
                    Observation::Reactor { event, .. } => event,
                    _ => unreachable!("reactor rows"),
                });
                let mut last = Ok(());
                for fact in facts.into_iter().chain(rows) {
                    ledger.fold(&fact).expect("each identity holds");
                    if let ReactorFact::Settled {
                        snapshot: Some(snapshot),
                        ..
                    } = &fact
                    {
                        last = ledger.settled(snapshot);
                    }
                }
                last
            };
            assert_eq!(ledger(3), Ok(()), "leased work reaches the property");
            assert!(
                ledger(2).is_err_and(|why| why.starts_with("Unaccounted")),
                "an unleased append is the budget's break"
            );
        }

        /// An entry replaced after its append, with the count unchanged,
        /// shows at the next settled boundary as `UnframedWork{Substituted}`,
        /// once while it lasts.
        ///
        /// This fails if the fold compares counts only.
        #[test]
        fn an_equal_count_substitution_is_seen() {
            let page = of(&[entry(1), entry(2)]);
            let substituted = of(&[entry(1), entry(3)]);
            let log = write(vec![
                staged(0, page, page),
                settled(1, substituted, 2),
                settled(2, substituted, 2),
            ])
            .expect("the step is whole");
            assert_eq!(
                derived(&log),
                [
                    (admitted(0, 2), true),
                    (unframed(UnframedCause::Substituted, 2), false)
                ]
            );
        }

        /// An entry replaced between the decode and the append keeps its
        /// count and frame, but the reactor's `after` differs from the
        /// fold's own expected digest, so it's unframed at the `Staged`.
        ///
        /// This fails if the fold adopts `after` as its expectation.
        #[test]
        fn a_substitution_before_the_append_is_seen() {
            let page = of(&[entry(1), entry(2)]);
            let appended = of(&[entry(1), entry(3)]);
            let log = write(vec![staged(0, page, appended), settled(1, appended, 2)])
                .expect("the step is whole");
            assert_eq!(
                derived(&log),
                [
                    (admitted(0, 2), true),
                    (unframed(UnframedCause::Substituted, 2), false)
                ]
            );
        }

        /// An older entry changed before the next page's append: that
        /// page's `after` covers the changed entry, so it differs from the
        /// fold's expectation, which the fold keeps, so a third page that
        /// extends the changed history differs too.
        ///
        /// This fails if a later append absorbs the change.
        #[test]
        fn a_substitution_during_a_later_append_is_seen() {
            let (first, second, third) = (entry(1), entry(2), entry(3));
            let changed = entry(7);
            let log = write(vec![
                staged(0, first, first),
                staged(1, second, of(&[changed, second])),
                staged(2, third, of(&[changed, second, third])),
            ])
            .expect("the step is whole");
            assert_eq!(
                derived(&log),
                [
                    (admitted(0, 1), true),
                    (admitted(1, 1), true),
                    (unframed(UnframedCause::Substituted, 2), false),
                    (admitted(2, 1), true),
                    (unframed(UnframedCause::Substituted, 3), false),
                ]
            );
        }

        /// A prepare's take restarts the expectation empty, so a take whose
        /// chain is what the frames delivered, and the empty held digest
        /// after it, derive nothing.
        ///
        /// This fails if the fold keeps its expectation across the take.
        #[test]
        fn a_take_restarts_the_expectation() {
            let page = of(&[entry(1), entry(2)]);
            let take = |chain| {
                reactor(ReactorFact::EntriesTaken {
                    incarnation: 1,
                    transfer: 1,
                    count: 2,
                    chain,
                })
            };
            let log = write(vec![
                staged(0, page, page),
                take(page),
                settled(1, HeldDigest::default(), 2),
            ])
            .expect("the step is whole");
            assert_eq!(derived(&log), [(admitted(0, 2), true)]);
            // A take of more than the frames delivered is unframed.
            let log = write(vec![
                staged(0, page, page),
                take(page.then(entry(9))),
                settled(1, HeldDigest::default(), 3),
            ])
            .expect("the step is whole");
            assert_eq!(
                derived(&log),
                [
                    (admitted(0, 2), true),
                    (unframed(UnframedCause::Extra, 1), false)
                ]
            );
        }

        /// Capacity consumed with nothing staged, as a settled snapshot's
        /// leases show it, is no admitted work: only a `Staged` admits.
        ///
        /// This fails if admitted work comes from the budget.
        #[test]
        fn consumed_but_unstaged_work_is_not_admitted() {
            let log = write(vec![settled(1, HeldDigest::default(), 2)]).expect("the step is whole");
            assert_eq!(derived(&log), []);
        }

        /// Each outcome that retired or retried its matching incarnation is
        /// one ending `WorkAdmitted` for its frame, and each ignored outcome
        /// is none.
        ///
        /// This fails if an outcome with no active work is admitted.
        #[test]
        fn each_qualifying_outcome_admits_once() {
            let outcome = |disposition: &str, incarnation| {
                reactor(ReactorFact::Outcome {
                    session_id: 7,
                    subscription: 8,
                    frame: Some(frame(4, HeldDigest::default())),
                    disposition: disposition.into(),
                    incarnation,
                })
            };
            let ending = Observation::WorkAdmitted {
                connection: 1,
                frame: RowAt::new(S5V9, "headers-outcome"),
                seq: 4,
                request: RowAt::new(S5V9, "subscribe-headers"),
                exchange: 8,
                objects: 0,
                bytes: 300,
                ends: true,
            };
            for (disposition, incarnation, admits) in [
                ("no-active", None, false),
                ("other-request", None, false),
                ("not-receiving", Some(1), false),
                ("retired", Some(1), true),
                ("retried", Some(1), true),
            ] {
                let log =
                    write(vec![outcome(disposition, incarnation)]).expect("the step is whole");
                let expected = if admits {
                    vec![(ending.clone(), true)]
                } else {
                    Vec::new()
                };
                assert_eq!(derived(&log), expected, "{disposition}");
            }
        }

        /// On the wire tier a staged page names a tagged frame of its
        /// session's own stream, and its session must be mapped. Each break
        /// is a harness failure.
        #[test]
        fn an_untagged_foreign_or_unmapped_stage_fails() {
            let page = entry(1);
            let untagged = reactor(ReactorFact::Staged {
                incarnation: 1,
                frame: None,
                count: 1,
                after: page,
            });
            let foreign = reactor(ReactorFact::Staged {
                incarnation: 1,
                frame: Some(ReactorFrame {
                    quic_stream: 8,
                    ..frame(0, page)
                }),
                count: 1,
                after: page,
            });
            for (row, name) in [(untagged, "UntaggedFrame"), (foreign, "ForeignFrame")] {
                let failed = write(vec![row]).expect_err("the stage can't be attributed");
                assert!(failed.starts_with(name), "{failed}");
            }
            let (mut hub, _) = hub();
            let mut log = ObservationLog::default();
            hand(
                &hub,
                &mut log,
                vec![at(1, started()), at(2, staged(0, page, page))],
            );
            let failed = hub.observe(&mut log).expect_err("no session row maps it");
            assert!(failed.starts_with("UnmappedSession"), "{failed}");
        }

        /// An ignored outcome is no admitted work, but on the wire tier it
        /// still names a tagged frame of its mapped session's stream. Each
        /// break is a harness failure.
        ///
        /// This fails if only qualifying outcomes are checked.
        #[test]
        fn an_ignored_outcome_still_needs_its_identity() {
            let outcome = |session_id, frame| {
                reactor(ReactorFact::Outcome {
                    session_id,
                    subscription: 8,
                    frame,
                    disposition: "no-active".into(),
                    incarnation: None,
                })
            };
            let tagged = Some(frame(4, HeldDigest::default()));
            assert_eq!(
                derived(&write(vec![outcome(7, tagged)]).expect("the step is whole")),
                []
            );
            for (row, name) in [
                (outcome(7, None), "UntaggedFrame"),
                (outcome(9, tagged), "UnmappedSession"),
            ] {
                let failed = write(vec![row]).expect_err("the outcome can't be attributed");
                assert!(failed.starts_with(name), "{failed}");
            }
        }
    }
}
