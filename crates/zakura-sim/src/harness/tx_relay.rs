//! `tx-relay/wire`: production `tx-relay` under the simulator (#2577).
//!
//! The node runs the real `tx-relay` service behind a real
//! `ZakuraProtocolHandler` on a `zakura-quic` endpoint bound to the run's
//! `SimNet`, through the [`WireEndpoint`] the other wire harnesses use.
//! Behind it is a small test pool on the pool port: it grants every
//! reservation, decides each body after a fixed verification delay, and
//! admits all but the scenario's bad bodies, whose verdict the scenario
//! picks. In front are scripted peers ([`crate::peer::tx_relay`]) that
//! dial the node and run a composed script each.
//!
//! The node's routines report each step through `tx-relay`'s simulator
//! hook, and the domain properties in [`crate::domain::tx_relay`] judge
//! those steps with what the pool and the peers report. The generated
//! table properties of both streams judge the peers' labelled frames
//! against the closes the transport tap logs.
//!
//! A prefix composes the scenario ([`composed_scenario`]): the pool's
//! delay and bad verdict, the node's own bodies, the links, and each
//! peer's answers and steps, drawn from `tx-relay`'s whole message
//! alphabet. A named witness is such a prefix ([`composed_prefix`]).
//! [`ROW_WITNESSES`] names one witness per fault row of `tx-relay`'s close
//! table, with an honest twin the node must not close, and a const check
//! fails the build if the close spec gains a `tx-relay` row it lacks.

use std::{collections::VecDeque, sync::Arc, time::Duration};

use serde::{Deserialize, Serialize};
use zakura_chain::{
    amount::Amount,
    block,
    parameters::Network,
    serialization::ZcashSerialize,
    transaction::{LockTime, Transaction, UnminedTx, UnminedTxId, VerifiedUnminedTx},
    transparent,
};
use zakura_network::{
    zakura::{
        tx_relay_sim::{tx_relay_service_observed, TxRelayObserver, INV_RATE, TX_RELAY_STREAMS},
        CloseRow, MayCool, Service, ZakuraLocalLimits,
    },
    Config,
};
use zakura_node_services::mempool::{
    port::{
        pool_port, ChainTip, Change, ChangeSeq, Charge, PoolCommand, PoolHandle, PoolInbox,
        PoolSnapshot, Rejection, RejectionScope, ReservationId, Verdict,
    },
    TransactionDependencies,
};
use zakura_quic::sim::SeededInputs;
use zakura_rt::sync::{oneshot, CancellationToken};
use zakura_scheduler::SchedulerConfig;

use super::{
    wire_node::{TransportTap, WireEndpoint},
    NetScenario, ScenarioFamily, Subsystem,
};
use crate::{
    domain::tx_relay::{self as properties, lock, RelayView, SharedRelay},
    peer::{
        tx_relay::{Answer, Bodies, HelloMode, RelayPeer, RelayPeerSpec, RelayStep},
        PeerControl, PeerFailure, PeerRecords, NODE_ADDR,
    },
    property::{HasView, Property, Source, Sources, Status, TriggerHistory},
    table::{generated_trigger, table_properties},
    ObservationLog, RunConfig, Sim,
};

/// The family `tx-relay`'s generated property IDs name.
pub const FAMILY: &str = "tx-relay";

/// Bodies each scripted peer holds, by peer: peer `i` holds
/// `i * HELD..(i + 1) * HELD` of the universe.
pub const HELD: usize = 8;

/// The most peers a scenario has, the helper included.
pub const MAX_PEERS: usize = 5;

/// The universe's first body this node holds itself.
pub const LOCAL: usize = MAX_PEERS * HELD;

/// The most bodies this node holds itself.
pub const MAX_LOCAL: usize = 20;

/// The universe's first bad body. Peer `i` holds bad bodies
/// `BAD + 2 * i` and `BAD + 2 * i + 1`.
pub const BAD: usize = LOCAL + MAX_LOCAL;

/// The universe's size.
pub const UNIVERSE: usize = BAD + 2 * MAX_PEERS;

/// The lock script size of the bodies this node holds itself: past a
/// peer's QUIC receive window, so a peer that stops reading blocks the
/// node's writer within one body.
const LOCAL_SCRIPT: usize = 80 * 1024;

/// The universe's bodies: transparent V4 transactions, each with its own
/// output value. The peers' and the bad bodies are of one small size, and
/// this node's own are larger than a peer's receive window.
pub fn universe() -> Vec<UnminedTx> {
    (0..UNIVERSE)
        .map(|index| {
            let value = 1_000 + i64::try_from(index).expect("a small universe");
            let script = if (LOCAL..BAD).contains(&index) {
                vec![0x51; LOCAL_SCRIPT]
            } else {
                vec![0x51]
            };
            UnminedTx::from(Transaction::V4 {
                inputs: Vec::new(),
                outputs: vec![transparent::Output {
                    value: Amount::try_from(value).expect("a small positive value"),
                    lock_script: transparent::Script::new(&script),
                }],
                lock_time: LockTime::unlocked(),
                expiry_height: block::Height(0),
                joinsplit_data: None,
                sapling_shielded_data: None,
            })
        })
        .collect()
}

/// The universe's bodies, serialized.
fn bodies(universe: &[UnminedTx]) -> Bodies {
    Arc::new(
        universe
            .iter()
            .map(|tx| {
                let bytes = tx
                    .transaction()
                    .zcash_serialize_to_vec()
                    .expect("a universe transaction serializes");
                (tx.id(), bytes)
            })
            .collect(),
    )
}

/// The verdict the pool gives a bad body.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum BadVerdict {
    /// Admitted, as any other body.
    #[default]
    Admit,
    /// Rejected, charging nobody.
    Reject,
    /// Rejected with a cooldown for its supplier: row 14 on a second one.
    Cooldown,
    /// Rejected with a ban for its supplier: row 13.
    Ban,
}

/// The `tx-relay/wire` scenario.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RelayScenario {
    /// The network: its seed, links and loss gate.
    #[serde(default)]
    pub net: NetScenario,
    /// How long the pool takes to decide a body, in milliseconds.
    pub verify_ms: u64,
    /// The verdict for the universe's bad bodies.
    pub bad: BadVerdict,
    /// Bodies this node holds from the start: the universe's first `local`
    /// from [`LOCAL`].
    pub local: usize,
    /// The scripted peers, named `peer-<i>` by index.
    pub peers: Vec<RelayPeerSpec>,
}

impl Default for RelayScenario {
    /// Two honest peers that announce and serve what they hold, and fetch
    /// what the node announces.
    fn default() -> Self {
        // Peer 0's `AnnounceAll` and `Fetch`, then peer 1's.
        let n = u8::try_from(CONFORMANT_ACTIONS).expect("a few steps");
        composed_scenario(&[0, 1, 0, 2, 6, 2 + n, 6 + n])
    }
}

/// The node's config: production defaults on Regtest, with `tx-relay` on,
/// block sync's congestion window and the default stream window cut to
/// 1 MiB, and the smallest QUIC send window. Block sync doesn't run here,
/// but the largest kind's share sets every stream's receive window, which
/// is the bulk of the `Inv` bucket's depth. The send window holds the
/// node's data writer on a body a peer doesn't read, so the answers behind
/// it queue unstarted.
pub fn node_config() -> Config {
    let mut config = Config {
        network: Network::new_regtest(Default::default()),
        ..Config::default()
    };
    config.zakura.tx_relay.enabled = true;
    config.zakura.quic.stream_receive_window_bytes = 1024 * 1024;
    config.zakura.block_sync.bbr_max_cwnd_bytes = 1024 * 1024;
    config.zakura.block_sync.floor_bypass_slots = 0;
    config.zakura.quic.send_window_bytes = 64 * 1024;
    config
}

/// The `Inv` byte bucket depth the node keeps for each connection: the
/// row's receiver depth over the stream receive window its limits set.
pub fn inv_depth(config: &Config) -> u64 {
    let limits = ZakuraLocalLimits::from_config(config);
    INV_RATE.receiver_depth(u64::from(limits.quic.stream_receive_window_bytes))
}

/// The test pool's state: the bodies it admitted and the decisions it owes.
struct TestPool {
    inbox: PoolInbox,
    admitted: Vec<VerifiedUnminedTx>,
    /// Submissions waiting for their verdict, oldest first, with when.
    deciding: VecDeque<(Duration, oneshot::Sender<Verdict>, UnminedTx)>,
    next_reservation: u64,
    verify: Duration,
    bad: BadVerdict,
    /// The ids of the bad bodies.
    bad_ids: Vec<UnminedTxId>,
    evidence: SharedRelay,
}

impl TestPool {
    /// Publishes what the pool holds, after `changes`.
    fn publish(&self, changes: Vec<Change>) {
        let transactions = self.admitted.clone();
        self.inbox.publish(changes, |last: ChangeSeq| {
            let mut snapshot = PoolSnapshot::new(
                ChainTip {
                    hash: block::Hash([0; 32]),
                    height: block::Height(0),
                },
                last,
                transactions,
                TransactionDependencies::default(),
            );
            snapshot.active = true;
            snapshot
        });
    }

    /// Admits `transaction`: the evidence hears of it before any routine
    /// can read the change.
    fn admit(&mut self, transaction: UnminedTx) {
        let id = transaction.id();
        lock(&self.evidence).admitted(id);
        // The pool doesn't verify: it pays every body a fee that passes the
        // mempool's checks for any body up to the protocol's size.
        let verified = VerifiedUnminedTx::new(
            transaction,
            Amount::try_from(1_000_000_000).expect("a valid test fee"),
            0,
            0,
            Arc::new(Vec::new()),
        )
        .expect("the test fee pays for any body up to the protocol's size");
        self.admitted.push(verified);
        self.publish(vec![Change::Added(id)]);
    }

    /// Decides `transaction`, as the scenario says for a bad body.
    fn decide(&mut self, reply: oneshot::Sender<Verdict>, transaction: UnminedTx) {
        let held = self
            .admitted
            .iter()
            .any(|verified| verified.transaction.id() == transaction.id());
        let charge = match self.bad {
            _ if !self.bad_ids.contains(&transaction.id()) => None,
            BadVerdict::Admit => None,
            BadVerdict::Reject => Some(Charge::None),
            BadVerdict::Cooldown => Some(Charge::Cooldown),
            BadVerdict::Ban => Some(Charge::Ban),
        };
        let verdict = match charge {
            Some(charge) => Verdict::Rejected(Rejection {
                scope: RejectionScope::Uncached,
                charge,
                tip: block::Height(0),
                detail: Arc::new(std::io::Error::other("the test pool's bad body")),
            }),
            None => Verdict::Admitted,
        };
        let admit = matches!(verdict, Verdict::Admitted) && !held;
        let _ = reply.send(verdict);
        if admit {
            self.admit(transaction);
        }
    }

    /// Serves the port until every handle is gone.
    async fn run(mut self, local: Vec<UnminedTx>) {
        for transaction in local {
            self.admit(transaction);
        }
        if self.admitted.is_empty() {
            self.publish(Vec::new());
        }
        loop {
            let due = self.deciding.front().map(|(at, ..)| *at);
            let command = match due {
                Some(at) => zakura_rt::select! {
                    command = self.inbox.next() => command,
                    () = sleep_until(at) => {
                        let (_, reply, transaction) = self
                            .deciding
                            .pop_front()
                            .expect("a due decision is queued");
                        self.decide(reply, transaction);
                        continue;
                    }
                },
                None => self.inbox.next().await,
            };
            let Some(command) = command else {
                return;
            };
            match command {
                PoolCommand::Reserve { tx_id, reply, .. } => {
                    self.next_reservation += 1;
                    let reservation = self.inbox.grant(
                        ReservationId(self.next_reservation),
                        tx_id,
                        CancellationToken::new(),
                    );
                    let _ = reply.send(Ok(reservation));
                }
                PoolCommand::Submit {
                    reservation,
                    transaction,
                    reply,
                } => {
                    drop(reservation);
                    let at = now() + self.verify;
                    self.deciding.push_back((at, reply, transaction));
                }
                PoolCommand::RejectedIds { reply, .. } => {
                    let _ = reply.send(Default::default());
                }
                PoolCommand::FetchFailed(_) | PoolCommand::Released(_) => {}
            }
        }
    }
}

/// The time since the run's start.
fn now() -> Duration {
    zakura_scheduler::hooks::now()
        .expect("the pool runs inside a run")
        .since_start()
}

/// Sleeps until `at` since the run's start.
async fn sleep_until(at: Duration) {
    zakura_rt::time::sleep(at.saturating_sub(now())).await;
}

/// `tx-relay`'s generated receiver properties over both streams, each
/// checked from the peers' labels and the node's closes, with its trigger
/// in the log. The sender templates aren't registered: no adapter labels
/// the node's own frames, and the domain properties hold its sending side.
pub fn wire_properties<S: HasView>() -> Vec<Property<S>> {
    table_properties(FAMILY, &TX_RELAY_STREAMS)
        .into_iter()
        .map(|property| Property {
            needs: &[Source::Frames, Source::Closes],
            witness: Some(TriggerHistory::LogOf(|id, log| {
                generated_trigger(&TX_RELAY_STREAMS, id, log)
            })),
            status: Status::Active,
            ..property
        })
        .collect()
}

/// The node, its pool and its scripted peers.
pub struct TxRelayWire {
    evidence: SharedRelay,
    /// Held for the whole run, so the pool never sees its handles go.
    _pool: PoolHandle,
}

impl HasView for TxRelayWire {
    type View = RelayView;

    /// The shared evidence.
    fn view(&self) -> RelayView {
        Arc::clone(&self.evidence)
    }
}

/// Spawns the node, its pool and every scripted peer of `scenario`, keeping
/// their evidence in `evidence`.
fn spawn(sim: &mut Sim, scenario: &RelayScenario, evidence: SharedRelay) -> TxRelayWire {
    let seed = scenario.net.seed;
    {
        let mut evidence = lock(&evidence);
        evidence.seed = seed;
        evidence.peers = scenario.peers.len();
    }
    let universe = universe();
    let bodies = bodies(&universe);
    let (handle, inbox) = pool_port();
    let pool = TestPool {
        inbox,
        admitted: Vec::new(),
        deciding: VecDeque::new(),
        next_reservation: 0,
        verify: Duration::from_millis(scenario.verify_ms),
        bad: scenario.bad,
        bad_ids: universe[BAD..].iter().map(UnminedTx::id).collect(),
        evidence: Arc::clone(&evidence),
    };
    let local = universe[LOCAL..LOCAL + scenario.local.min(MAX_LOCAL)].to_vec();
    sim.scheduler().spawn_root("pool", pool.run(local));
    let net = scenario.net.net();
    sim.add_source(Box::new(net.clone()));
    let names: Vec<String> = (0..scenario.peers.len())
        .map(|index| format!("peer-{index}"))
        .collect();
    let (observer, tap) = TransportTap::new(sim, "node", net.clone(), names.clone());
    let dials = tap.dials();
    let shared_ids = tap.shared_ids();
    sim.add_source(Box::new(tap));
    let config = node_config();
    let depth = inv_depth(&config);
    let relay: TxRelayObserver = {
        let evidence = Arc::clone(&evidence);
        Arc::new(move |event| lock(&evidence).step(event))
    };
    let pool_handle = handle.clone();
    let may_cool: MayCool = Arc::new(|_| true);
    let relay_config = config.zakura.tx_relay.clone();
    WireEndpoint::spawn_configured(
        sim,
        config,
        net.sim(),
        seed,
        None,
        None,
        observer,
        Some(dials),
        move |_, _| {
            let custom = tx_relay_service_observed(pool_handle, may_cool, &relay_config, relay);
            let service: Arc<dyn Service> = custom.service;
            (vec![service], ())
        },
        |_, _, ()| async {},
    );
    let failure = PeerFailure::default();
    let records = PeerRecords::default();
    let control = PeerControl::new(sim, "node", failure.clone(), records.clone(), shared_ids)
        .with_tx_relay(&TX_RELAY_STREAMS);
    for ((index, spec), name) in scenario.peers.iter().enumerate().zip(names) {
        let task = RelayPeer {
            index,
            name,
            seed,
            net: net.sim(),
            node: zakura_quic::NodeAddr::with_addrs(
                SeededInputs::node_key(seed, 0).public(),
                [NODE_ADDR],
            ),
            spec: spec.clone(),
            bodies: Arc::clone(&bodies),
            records: records.clone(),
            order: net.order(),
            failure: failure.clone(),
            evidence: Arc::clone(&evidence),
            inv_depth: depth,
        };
        sim.scheduler().spawn_root("peer", task.run());
    }
    sim.add_source(Box::new(control));
    TxRelayWire {
        evidence,
        _pool: handle,
    }
}

/// The step slots of a composed prefix, after its version byte and its
/// three bytes of choices.
pub const COMPOSED_SLOTS: usize = 20;

/// The first byte of every composed prefix: [`TxRelayWire`]'s generator.
/// Any other first byte draws the default scenario.
pub const COMPOSED_VERSION: u8 = 1;

/// The length of a composed prefix.
pub const COMPOSED_BYTES: usize = 4 + COMPOSED_SLOTS;

/// The steps a script that keeps every rule picks from.
const CONFORMANT_ACTIONS: usize = RelayStep::CONFORMANT.len();

/// The steps a script that may break rules picks from.
pub const COMPOSED_ACTIONS: usize = CONFORMANT_ACTIONS + RelayStep::FAULTS.len();

/// The steps a script picks from: [`RelayStep::CONFORMANT`], then
/// [`RelayStep::FAULTS`] if it may break rules.
fn actions(faults: bool) -> Vec<RelayStep> {
    let conformant = RelayStep::CONFORMANT.into_iter();
    if faults {
        conformant.chain(RelayStep::FAULTS).collect()
    } else {
        conformant.collect()
    }
}

/// The answers a peer picks from: the conformant ones, then the rest if it
/// may break rules.
fn answers(faults: bool) -> Vec<Answer> {
    Answer::ALL
        .into_iter()
        .filter(|answer| faults || answer.conformant())
        .collect()
}

/// The scenario the bytes after a composed prefix's version byte encode,
/// in fixed slots so a change to one byte changes one choice. Counting
/// from the byte after the version:
///
/// - Byte 0, the node and its pool: the verification delay (bits 0 and 1:
///   10, 200, 2,000 or 6,000 ms), the bad bodies' verdict (bits 2 and 3:
///   [`BadVerdict`] in order), the node's own bodies (bits 4 and 5: 0, 4,
///   16 or 20) and the links' delays (bits 6 and 7: 1 to 20 ms, 1 to 100
///   ms, 20 to 200 ms, or 1 to 1,000 ms).
/// - Byte 1, the peers: their count (bits 0 and 1, 1 to 4), an honest
///   helper peer after them (bit 2), whether each holds its two bad bodies
///   first (bit 3), the gap between dials (bits 4 and 5: 0, 200, 1,000 or
///   5,000 ms), and whether the scripts may break rules (bit 7).
/// - Byte 2, the answers: which one (bits 0 to 2, of the conformant ones
///   or of all once byte 1 lets the scripts break rules), the gap between
///   answers (bits 3 and 4: 0, 100, 1,000 or 3,000 ms, or 0 for an answer
///   that breaks a rule, so the node reads it before its deadline), a
///   `Hello` only as a step (bit 5), four fresh ids announced as each
///   session opens (bit 6), and whether every scripted peer answers so,
///   or only the first while the rest serve (bit 7).
/// - Each later byte: a step slot. 0 ends the scripts. Any other value
///   `v` gives step `(v - 1) % n` of the `n` the scripts pick from to peer
///   `(v - 1) / n` modulo the count.
///
/// The helper holds its own bodies, announces them all and fetches the
/// first the node announces, with a valid `Hello` and the `Serve` answer.
pub fn composed_scenario(prefix: &[u8]) -> RelayScenario {
    let byte = |index: usize| prefix.get(index).copied().unwrap_or(0);
    let (node, peers, answer) = (byte(0), byte(1), byte(2));
    let mut net = NetScenario::default();
    (net.default_link.min_delay_ms, net.default_link.max_delay_ms) = match node >> 6 {
        0 => (1, 20),
        1 => (1, 100),
        2 => (20, 200),
        _ => (1, 1_000),
    };
    let faults = peers & 0x80 != 0;
    let count = 1 + usize::from(peers & 3);
    let actions = actions(faults);
    let answers = answers(faults);
    let chosen = answers[usize::from(answer & 7) % answers.len()];
    let gap = if chosen.conformant() {
        [0, 100, 1_000, 3_000][usize::from((answer >> 3) & 3)]
    } else {
        0
    };
    let dial_gap = [0, 200, 1_000, 5_000][usize::from((peers >> 4) & 3)];
    let mut specs: Vec<RelayPeerSpec> = (0..count)
        .map(|index| {
            let held = index * HELD..(index + 1) * HELD;
            let bad = [BAD + 2 * index, BAD + 2 * index + 1];
            let holds = if peers & 8 != 0 {
                bad.into_iter().chain(held).collect()
            } else {
                held.collect()
            };
            RelayPeerSpec {
                dial_at_ms: dial_gap * u64::try_from(index).expect("a few peers"),
                holds,
                hello: if answer & 0x20 != 0 {
                    HelloMode::Scripted
                } else {
                    HelloMode::First
                },
                answer: if index == 0 || answer & 0x80 != 0 {
                    chosen
                } else {
                    Answer::Serve
                },
                answer_gap_ms: gap,
                announce_on_open: if answer & 0x40 != 0 { 4 } else { 0 },
                faults,
                steps: Vec::new(),
            }
        })
        .collect();
    for slot in (3..3 + COMPOSED_SLOTS)
        .map(byte)
        .take_while(|slot| *slot != 0)
    {
        let at = usize::from(slot - 1);
        specs[(at / actions.len()) % count]
            .steps
            .push(actions[at % actions.len()]);
    }
    if peers & 4 != 0 {
        let holds = (count * HELD..(count + 1) * HELD).collect();
        specs.push(RelayPeerSpec {
            steps: vec![
                RelayStep::AnnounceAll,
                RelayStep::Wait { secs: 10 },
                RelayStep::Fetch,
            ],
            ..RelayPeerSpec::honest(holds)
        });
    }
    RelayScenario {
        net,
        verify_ms: [10, 200, 2_000, 6_000][usize::from(node & 3)],
        bad: [
            BadVerdict::Admit,
            BadVerdict::Reject,
            BadVerdict::Cooldown,
            BadVerdict::Ban,
        ][usize::from((node >> 2) & 3)],
        local: [0, 4, 16, 20][usize::from((node >> 4) & 3)],
        peers: specs,
    }
}

/// The full composed prefix, [`COMPOSED_VERSION`] first, whose scenario
/// runs `steps`, each by its peer's index, under bytes `node`, `peers` and
/// `answer` ([`composed_scenario`]). Fails if `steps` holds more than
/// [`COMPOSED_SLOTS`] steps, a step `peers` doesn't allow, or a peer past
/// its count.
pub fn composed_prefix(
    node: u8,
    peers: u8,
    answer: u8,
    steps: &[(usize, RelayStep)],
) -> Result<[u8; COMPOSED_BYTES], String> {
    if steps.len() > COMPOSED_SLOTS {
        return Err(format!("{} steps, past {COMPOSED_SLOTS}", steps.len()));
    }
    let actions = actions(peers & 0x80 != 0);
    let count = 1 + usize::from(peers & 3);
    let mut prefix = [0; COMPOSED_BYTES];
    prefix[..4].copy_from_slice(&[COMPOSED_VERSION, node, peers, answer]);
    for (slot, (peer, step)) in prefix[4..].iter_mut().zip(steps) {
        if *peer >= count {
            return Err(format!("peer {peer}, past the {count} byte 1 gives"));
        }
        let index = actions
            .iter()
            .position(|action| action == step)
            .ok_or_else(|| format!("byte 1 {peers:#04x} allows no {step:?}"))?;
        *slot = u8::try_from(1 + peer * actions.len() + index)
            .map_err(|_| format!("peer {peer}'s {step:?} is past a byte's slots"))?;
    }
    Ok(prefix)
}

/// A named witness: the composed prefix it runs.
pub type Prefix = fn() -> [u8; COMPOSED_BYTES];

/// One fault row of `tx-relay`'s close table, with the composed prefix
/// whose run breaks it and the honest twin's, which keeps the rule the
/// witness breaks and must not close.
#[derive(Clone, Copy, Debug)]
pub struct RowWitness {
    /// The row.
    pub row: CloseRow,
    /// The witness's prefix.
    pub witness: Prefix,
    /// The honest twin's prefix.
    pub twin: Prefix,
}

/// Byte 1 of a row witness: one peer that may break rules, and an honest
/// helper.
const ROW_PEERS: u8 = 0x80 | 4;

/// A row witness's prefix: `steps` of peer 0 under `answer`, past a pool
/// that decides at once.
fn row_prefix(answer: Answer, hello: HelloMode, steps: &[RelayStep]) -> [u8; COMPOSED_BYTES] {
    let answer = answers(true)
        .iter()
        .position(|chosen| *chosen == answer)
        .expect("every answer is listed");
    let answer = u8::try_from(answer).expect("a few answers")
        | if hello == HelloMode::Scripted {
            0x20
        } else {
            0
        };
    let steps: Vec<(usize, RelayStep)> = steps.iter().map(|step| (0, *step)).collect();
    composed_prefix(0, ROW_PEERS, answer, &steps).expect("a row witness fits its grammar")
}

/// Waits a second.
const WAIT: RelayStep = RelayStep::Wait { secs: 1 };

/// One witness and one twin per fault row of `tx-relay`'s close table.
pub const ROW_WITNESSES: [RowWitness; 8] = [
    RowWitness {
        row: CloseRow::RELAY_CLOSE_3,
        witness: || row_prefix(Answer::Serve, HelloMode::First, &[RelayStep::MalformedInv]),
        twin: || {
            row_prefix(
                Answer::Serve,
                HelloMode::First,
                &[RelayStep::AnnounceFake { count: 1 }],
            )
        },
    },
    RowWitness {
        row: CloseRow::RELAY_CLOSE_4,
        witness: || row_prefix(Answer::Serve, HelloMode::First, &[RelayStep::Hello]),
        twin: || row_prefix(Answer::Serve, HelloMode::Scripted, &[RelayStep::Hello]),
    },
    RowWitness {
        row: CloseRow::RELAY_CLOSE_5,
        witness: || row_prefix(Answer::Serve, HelloMode::Scripted, &[RelayStep::BadHello]),
        twin: || row_prefix(Answer::Serve, HelloMode::Scripted, &[RelayStep::Hello]),
    },
    RowWitness {
        row: CloseRow::RELAY_CLOSE_6,
        witness: || row_prefix(Answer::Serve, HelloMode::Scripted, &[RelayStep::AskUnknown]),
        twin: || {
            row_prefix(
                Answer::Serve,
                HelloMode::Scripted,
                &[RelayStep::Hello, RelayStep::AskUnknown],
            )
        },
    },
    RowWitness {
        row: CloseRow::RELAY_CLOSE_9,
        witness: || {
            row_prefix(
                Answer::Serve,
                HelloMode::First,
                &[RelayStep::UnrequestedNotFound],
            )
        },
        twin: || {
            row_prefix(
                Answer::NotFound,
                HelloMode::First,
                &[RelayStep::Announce, WAIT],
            )
        },
    },
    RowWitness {
        row: CloseRow::RELAY_CLOSE_10,
        witness: || {
            row_prefix(
                Answer::WrongSize,
                HelloMode::First,
                &[RelayStep::Announce, WAIT],
            )
        },
        twin: || {
            row_prefix(
                Answer::Serve,
                HelloMode::First,
                &[RelayStep::Announce, WAIT],
            )
        },
    },
    RowWitness {
        row: CloseRow::RELAY_CLOSE_11,
        witness: || {
            row_prefix(
                Answer::WrongId,
                HelloMode::First,
                &[RelayStep::Announce, WAIT],
            )
        },
        twin: || {
            row_prefix(
                Answer::Serve,
                HelloMode::First,
                &[RelayStep::Announce, WAIT],
            )
        },
    },
    RowWitness {
        row: CloseRow::RELAY_CLOSE_12,
        witness: || {
            row_prefix(
                Answer::Undecodable,
                HelloMode::First,
                &[RelayStep::Announce, WAIT],
            )
        },
        twin: || {
            row_prefix(
                Answer::Serve,
                HelloMode::First,
                &[RelayStep::Announce, WAIT],
            )
        },
    },
];

/// How many times `needle` occurs in `haystack`.
const fn occurrences(haystack: &str, needle: &str) -> usize {
    let (haystack, needle) = (haystack.as_bytes(), needle.as_bytes());
    let mut count = 0;
    let mut at = 0;
    while at + needle.len() <= haystack.len() {
        let mut matched = 0;
        while matched < needle.len() && haystack[at + matched] == needle[matched] {
            matched += 1;
        }
        if matched == needle.len() {
            count += 1;
        }
        at += 1;
    }
    count
}

/// The close spec's `tx-relay` fault rows, as its constants define them.
const RELAY_ROWS: usize = occurrences(
    include_str!("../../../zakura-network/src/zakura/transport/close.rs"),
    "Self(\"relay-close-",
);

// A fault row without a witness fails the build.
const _: () = assert!(
    RELAY_ROWS == ROW_WITNESSES.len(),
    "each tx-relay fault row of the close spec needs a ROW_WITNESSES entry"
);

/// The neutral closes of `tx-relay`'s close table that a scenario
/// reaches, each with its recorded detail and a composed prefix whose run
/// closes so. Row 15, a cooldown table full, needs 20,000 cooling peers,
/// past any scenario.
pub const NEUTRAL_WITNESSES: [(&str, Prefix); 5] = [
    ("tx-relay: id requested twice", || {
        // A body of this node's, announced to the peer first, asked for
        // twice in one write.
        composed_prefix(
            0x10,
            ROW_PEERS,
            0,
            &[
                (0, RelayStep::Wait { secs: 10 }),
                (0, RelayStep::RequestTwice),
            ],
        )
        .expect("the witness fits its grammar")
    }),
    ("tx-relay: too many requests", || {
        // Twenty bodies of this node's, announced to the peer first.
        composed_prefix(
            0x30,
            ROW_PEERS,
            0,
            &[
                (0, RelayStep::Wait { secs: 10 }),
                (0, RelayStep::TooManyRequests),
            ],
        )
        .expect("the witness fits its grammar")
    }),
    ("tx-relay: banned transaction", || {
        // Bad bodies first, banned.
        composed_prefix(
            0x0C,
            0x80 | 8 | 4,
            0,
            &[(0, RelayStep::Announce), (0, WAIT)],
        )
        .expect("the witness fits its grammar")
    }),
    ("tx-relay: repeated cooldown", || {
        // Both bad bodies, each earning a cooldown, decided after both
        // arrive.
        composed_prefix(
            0x09,
            0x80 | 8 | 4,
            0,
            &[(0, RelayStep::AnnounceAll), (0, WAIT)],
        )
        .expect("the witness fits its grammar")
    }),
    ("tx-relay: unanswered request", STALL_WITNESS),
];

/// A peer that never answers: its first stall parks its session, and the
/// session its park readmits stalls again, which closes its connection.
pub const STALL_WITNESS: Prefix = || {
    composed_prefix(
        0,
        4,
        answer_bits(Answer::Never, false) | 0x40,
        &[(0, RelayStep::AnnounceAll)],
    )
    .expect("the witness fits its grammar")
};

/// The answer byte's bits that pick `chosen` among the answers a peer
/// with `faults` draws from.
fn answer_bits(chosen: Answer, faults: bool) -> u8 {
    let index = answers(faults)
        .iter()
        .position(|answer| *answer == chosen)
        .expect("every answer is listed");
    u8::try_from(index).expect("a few answers")
}

/// Peer 0's `steps` under node byte `node`, peers byte `peers` and answer
/// byte `answer`, with no helper unless `peers` asks for one.
fn steps_prefix(node: u8, peers: u8, answer: u8, steps: &[RelayStep]) -> [u8; COMPOSED_BYTES] {
    let steps: Vec<(usize, RelayStep)> = steps.iter().map(|step| (0, *step)).collect();
    composed_prefix(node, peers, answer, &steps).expect("a witness fits its grammar")
}

/// Waits ten seconds.
const WAIT_LONG: RelayStep = RelayStep::Wait { secs: 10 };

/// Each domain property's named witness, in the order
/// [`crate::domain::tx_relay::IDS`] lists them.
pub const DOMAIN_WITNESSES: [(&str, Prefix); 11] = [
    // A pool that takes 2 s to decide holds five bodies at once.
    (properties::REQUESTS_WITHIN_LIMIT, || {
        steps_prefix(0x02, 0, 0, &[RelayStep::AnnounceAll, WAIT_LONG])
    }),
    // Twenty bodies of the node's, announced, then eighteen asked for at
    // once while the peer reads nothing: sixteen wait unstarted, and the
    // last closes the connection.
    (properties::SERVING_WITHIN_LIMIT, || {
        steps_prefix(
            0x30,
            0x80,
            0,
            &[WAIT_LONG, RelayStep::TooManyRequests, WAIT_LONG],
        )
    }),
    // The peer never answers, so the node's requests drain nothing from
    // the set before its first stall parks the peer.
    (properties::PENDING_WITHIN_BOUND, || {
        steps_prefix(
            0,
            0x80,
            answer_bits(Answer::Never, true),
            &[RelayStep::FillPending, WAIT_LONG],
        )
    }),
    (properties::SKIP_SET_WITHIN_BOUND, || {
        steps_prefix(
            0,
            0x80,
            answer_bits(Answer::Never, true),
            &[RelayStep::FillKnown, WAIT_LONG],
        )
    }),
    // A body under another id than the one requested, which the node must
    // not submit.
    (properties::REQUESTED_BODIES_ONLY, || {
        (ROW_WITNESSES[6].witness)()
    }),
    // Bad bodies first, each earning a cooldown, answered a second apart:
    // the second arrives while its peer cools.
    (properties::COOLING_BODIES_DROPPED, || {
        steps_prefix(0x08, 0x08, 0x10, &[RelayStep::AnnounceAll, WAIT_LONG])
    }),
    (properties::STALLED_PEERS_PARKED, STALL_WITNESS),
    // An announcement before the peer's `Hello`, which the node holds.
    (properties::HELLO_FIRST, || {
        steps_prefix(
            0,
            0,
            0x20,
            &[RelayStep::Announce, WAIT, RelayStep::Hello, WAIT_LONG],
        )
    }),
    // The peer asks for a body it supplied, which the node never announces
    // back to it.
    (properties::NO_PROBE, || {
        steps_prefix(
            0,
            0,
            0,
            &[
                RelayStep::AnnounceAll,
                WAIT_LONG,
                RelayStep::Probe { tx: 0 },
                WAIT_LONG,
            ],
        )
    }),
    // Peer 0's bodies, admitted, go to peer 1.
    (properties::ANNOUNCE_AFTER_ADMISSION, || {
        composed_prefix(
            0,
            1,
            0,
            &[(0, RelayStep::AnnounceAll), (1, WAIT_LONG), (1, WAIT_LONG)],
        )
        .expect("the witness fits its grammar")
    }),
    (properties::FAULT_CLOSE_NAMES_A_BROKEN_ROW, || {
        (ROW_WITNESSES[5].witness)()
    }),
];

/// `tx-relay/wire`'s step budget: room for a script of [`COMPOSED_SLOTS`]
/// steps.
pub const BUDGET: u64 = 150_000;

/// The budget of a scenario whose peer never answers, or that fills a
/// pending set or a skip set: room for a park's three minutes, or for tens
/// of thousands of ids.
pub const LONG_BUDGET: u64 = 400_000;

impl Subsystem for TxRelayWire {
    const NAME: &'static str = "tx-relay/wire";
    // A u8 always fits in a u32.
    const GENERATOR: u32 = COMPOSED_VERSION as u32;
    const SCENARIO_BYTES: usize = COMPOSED_BYTES;
    type Scenario = RelayScenario;
    type Prepared = SharedRelay;

    /// The default scheduler, [`BUDGET`] or [`LONG_BUDGET`] steps, and a
    /// timer cap past a park, so the drain still reaches a readmitted
    /// session's stall.
    fn config(scenario: &RelayScenario) -> RunConfig {
        let long = scenario.peers.iter().any(|peer| {
            peer.answer == Answer::Never
                || peer
                    .steps
                    .iter()
                    .any(|step| matches!(step, RelayStep::FillPending | RelayStep::FillKnown))
        });
        let budget = if long { LONG_BUDGET } else { BUDGET };
        RunConfig {
            scheduler: SchedulerConfig::default(),
            budget,
            drain_bound: budget,
            harness_timer_cap: Duration::from_secs(300),
            ..RunConfig::default()
        }
    }

    /// Empty evidence, which a caller may keep to read after the run.
    fn prepare(_: &RelayScenario) -> SharedRelay {
        SharedRelay::default()
    }

    /// [`spawn`].
    fn build(sim: &mut Sim, scenario: &RelayScenario, evidence: SharedRelay) -> Self {
        spawn(sim, scenario, evidence)
    }

    /// The domain properties, then the generated ones.
    fn properties() -> Vec<Property<Self>> {
        let mut all = properties::properties();
        all.extend(wire_properties());
        all
    }

    /// The step loop's task observations, the network's datagrams, the
    /// peers' frames, the node's closes and the evidence the hook records.
    fn sources() -> Sources {
        Sources::observed(&[
            Source::Schedule,
            Source::Datagrams,
            Source::Frames,
            Source::Closes,
            Source::View,
        ])
    }

    /// One family, the grammar, whose tag is [`COMPOSED_VERSION`].
    fn families() -> Vec<ScenarioFamily> {
        vec![ScenarioFamily::new("composed", [COMPOSED_VERSION])]
    }

    /// The named witnesses whose runs take well under a second of the
    /// target's time: each fault row's witness and honest twin, the
    /// neutral closes but the stall, and the domain witnesses but the
    /// stall and the pending and skip-set fills.
    fn corpus() -> Vec<Vec<u8>> {
        let slow = [
            properties::STALLED_PEERS_PARKED,
            properties::PENDING_WITHIN_BOUND,
            properties::SKIP_SET_WITHIN_BOUND,
        ];
        let rows = ROW_WITNESSES
            .iter()
            .flat_map(|witness| [witness.witness, witness.twin]);
        let neutral = NEUTRAL_WITNESSES
            .iter()
            .filter(|(detail, _)| !detail.contains("unanswered"))
            .map(|(_, witness)| *witness);
        let domain = DOMAIN_WITNESSES
            .iter()
            .filter(|(id, _)| !slow.contains(id))
            .map(|(_, witness)| *witness);
        rows.chain(neutral)
            .chain(domain)
            .map(|witness| witness().to_vec())
            .collect()
    }

    /// Whether `prefix` starts with [`COMPOSED_VERSION`].
    fn accepts(prefix: &[u8]) -> bool {
        prefix.first() == Some(&COMPOSED_VERSION)
    }

    /// [`composed_scenario`] of the bytes after [`COMPOSED_VERSION`], or the
    /// default scenario for a prefix with another first byte.
    fn draw(prefix: &[u8]) -> RelayScenario {
        match prefix.split_first() {
            Some((&COMPOSED_VERSION, grammar)) => composed_scenario(grammar),
            _ => RelayScenario::default(),
        }
    }
}

/// Reads the log into the evidence, for a caller that reads the evidence
/// after a run.
pub fn read_log(evidence: &SharedRelay, log: &ObservationLog) {
    lock(evidence).read_log(log);
}
