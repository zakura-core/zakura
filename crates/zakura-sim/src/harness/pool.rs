//! M1.8: the pool core under the simulator.
//!
//! The real `PoolCore` task runs behind the real port, with small limits so
//! a short run fills them. Around it:
//!
//! - a modeled verifier that reads the pool through the published snapshot,
//!   as the real one does, and fails, bans, finds a block's transaction or
//!   misses an input as its transaction's script says. It can be slow.
//! - four legacy peers and this node submit through `PoolHandle`: bursts
//!   of fetch reservations, one offered again while the first ones verify,
//!   delivered bodies, a body one peer reserved and another supplied, a
//!   failed fetch, an abandoned reservation, a wait for an output, a
//!   violation, and a burst that fills the node's limit.
//! - a chain script grows the chain, mines a pooled transaction, can
//!   activate a network upgrade, reorganize, and lose its queue so the core
//!   resynchronizes. Each change marks the generation changing first, with
//!   a preemption point before it finishes, so a verification can read
//!   `Changing`.
//! - every publication is checked as the core makes it, through the port's
//!   simulator hook, and an observer follows the change log as a reader.
//!
//! The tasks record what they see in [`PoolEvidence`], which the properties
//! in [`crate::domain::pool`] read.

use std::{cell::RefCell, future::Future, pin::Pin, rc::Rc, sync::Arc, time::Duration};

use serde::{Deserialize, Serialize};
use zakura_chain::{
    amount::Amount,
    block::{self, Height},
    parameters::NetworkUpgrade,
    transaction::{self, LockTime, Transaction, UnminedTx, UnminedTxId, VerifiedUnminedTx},
    transparent::{self, OutPoint},
};
use zakura_mempool::{
    config::Config,
    io::{PoolCore, Verifier},
    machine::{Limits, Outcome},
    storage::{ExactTipRejectionError, RejectionError},
};
use zakura_node_services::{
    block_memory::TransactionHandle,
    mempool::port::{
        chain_transitions, pool_port, ChainTip, ChainTransition, ChainTransitionSender, Change,
        ChangeSeq, ChangesSince, Charge, ConnectedBlock, Intent, LocalFault, PoolHandle,
        PoolReader, PoolSnapshot, Reservation, Verdict,
    },
};
use zakura_rt::sync::watch;
use zakura_scheduler::SchedulerConfig;

use super::Subsystem;
use crate::{
    domain::pool::{self as properties, lock, PoolView, SharedEvidence, LIMITS},
    property::{EndViolation, HasView, Property, Source, Sources},
    ObservationLog, RunConfig, RunEnd, Sim,
};

/// The tip height a run starts at.
pub const START: u32 = 100;

/// The pool scenario: what the chain does, and how slow the verifier is.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PoolScenario {
    /// How long each verification takes, in milliseconds.
    pub verify_millis: u16,
    /// Whether one transition activates a network upgrade, which hides the
    /// pool for revalidation.
    pub rules_change: bool,
    /// Whether the chain reorganizes once the pool holds transactions.
    pub reorg: bool,
    /// Whether the core loses the transition queue once and resynchronizes.
    pub resync: bool,
}

impl Default for PoolScenario {
    /// A slow-enough verifier, a reorg and a resync.
    fn default() -> Self {
        PoolScenario {
            verify_millis: 20,
            rules_change: false,
            reorg: true,
            resync: true,
        }
    }
}

/// What the modeled verifier does with a transaction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Script {
    /// It is valid, given its parents.
    Valid,
    /// It is invalid, and earns its supplier a cooldown.
    Invalid,
    /// It is invalid, and earns its supplier a ban.
    Banned,
}

/// One transaction of the fixed universe.
struct Planned {
    /// The outputs it spends: a chain output's number, or a parent's index
    /// and output.
    spends: &'static [Spend],
    /// How many outputs it creates.
    outputs: u32,
    /// Its expiry height above [`START`], if it has one.
    expires_after: Option<u32>,
    script: Script,
}

/// One spent output.
#[derive(Clone, Copy)]
enum Spend {
    /// The chain output numbered so.
    Chain(u8),
    /// Output `.1` of the universe's transaction `.0`.
    Parent(usize, u32),
}

/// A parent with two outputs.
const T0: usize = 0;
/// T0's child.
const T1: usize = 1;
/// T0's other child.
const T2: usize = 2;
/// Spends the chain output T4 spends.
const T3: usize = 3;
/// Spends the chain output T3 spends.
const T4: usize = 4;
/// Expires three blocks after the start.
const T5: usize = 5;
/// Invalid, and earns its supplier a cooldown.
const T6: usize = 6;
/// Invalid, and earns its supplier a ban.
const T7: usize = 7;
/// The parent T9 waits for.
const T8: usize = 8;
/// A child that arrives before its parent, T8.
const T9: usize = 9;
/// A block mines it while it is pooled.
const T10: usize = 10;
/// The third of a burst that the source's limit refuses.
const T11: usize = 11;
/// Its fetch fails.
const T12: usize = 12;
/// Its reservation is abandoned.
const T13: usize = 13;
/// A late delivery.
const T14: usize = 14;
/// Invalid, and submitted by this node. Its verdict carries its charge all
/// the same, and the caller charges nobody.
const T15: usize = 15;
/// Seven bodies this node reserves at once, once the chain is quiet, so its
/// reservations alone reach the node's limit.
const FILL: [usize; 7] = [16, 17, 18, 19, 20, 21, 22];

/// The universe, by index.
const UNIVERSE: [Planned; 23] = [
    valid(&[Spend::Chain(0)], 2),
    valid(&[Spend::Parent(T0, 0)], 1),
    valid(&[Spend::Parent(T0, 1)], 1),
    valid(&[Spend::Chain(1)], 1),
    valid(&[Spend::Chain(1)], 1),
    Planned {
        spends: &[Spend::Chain(2)],
        outputs: 1,
        expires_after: Some(3),
        script: Script::Valid,
    },
    Planned {
        spends: &[Spend::Chain(3)],
        outputs: 1,
        expires_after: None,
        script: Script::Invalid,
    },
    Planned {
        spends: &[Spend::Chain(4)],
        outputs: 1,
        expires_after: None,
        script: Script::Banned,
    },
    valid(&[Spend::Chain(5)], 1),
    valid(&[Spend::Parent(T8, 0)], 1),
    valid(&[Spend::Chain(6)], 1),
    valid(&[Spend::Chain(7)], 1),
    valid(&[Spend::Chain(8)], 1),
    valid(&[Spend::Chain(9)], 1),
    valid(&[Spend::Chain(10)], 1),
    Planned {
        spends: &[Spend::Chain(11)],
        outputs: 1,
        expires_after: None,
        script: Script::Invalid,
    },
    valid(&[Spend::Chain(12)], 1),
    valid(&[Spend::Chain(13)], 1),
    valid(&[Spend::Chain(14)], 1),
    valid(&[Spend::Chain(15)], 1),
    valid(&[Spend::Chain(16)], 1),
    valid(&[Spend::Chain(17)], 1),
    valid(&[Spend::Chain(18)], 1),
];

/// A valid transaction that never expires.
const fn valid(spends: &'static [Spend], outputs: u32) -> Planned {
    Planned {
        spends,
        outputs,
        expires_after: None,
        script: Script::Valid,
    }
}

/// The universe's transactions, built in index order so a child names its
/// parent's id.
struct Universe {
    txs: Vec<VerifiedUnminedTx>,
}

impl Universe {
    /// Builds every transaction. Each pays a distinct output value, so two
    /// that spend the same output still differ.
    fn build() -> Self {
        let mut txs: Vec<VerifiedUnminedTx> = Vec::new();
        for (index, planned) in UNIVERSE.iter().enumerate() {
            let inputs = planned
                .spends
                .iter()
                .map(|spend| transparent::Input::PrevOut {
                    outpoint: match *spend {
                        Spend::Chain(n) => OutPoint {
                            hash: transaction::Hash([0xc0 ^ n; 32]),
                            index: 0,
                        },
                        Spend::Parent(parent, output) => OutPoint {
                            hash: txs[parent].transaction.id().mined_id(),
                            index: output,
                        },
                    },
                    unlock_script: transparent::Script::new(&[]),
                    sequence: 0,
                })
                .collect();
            let salt = i64::try_from(index).expect("the universe is small");
            let transaction = Transaction::V6 {
                network_upgrade: NetworkUpgrade::Nu6_3,
                lock_time: LockTime::unlocked(),
                // Zero is no expiry.
                expiry_height: Height(planned.expires_after.map_or(0, |after| START + after)),
                inputs,
                outputs: (0..planned.outputs)
                    .map(|_| transparent::Output {
                        value: Amount::try_from(1_000_000 + salt)
                            .expect("a small positive amount is valid"),
                        lock_script: p2pkh(),
                    })
                    .collect(),
                sapling_shielded_data: None,
                orchard_shielded_data: None,
                ironwood_shielded_data: None,
            };
            txs.push(
                VerifiedUnminedTx::new(
                    Arc::new(transaction).into(),
                    Amount::try_from(1_000_000).expect("a small positive amount is valid"),
                    0,
                    0,
                    Arc::new(Vec::new()),
                )
                .expect("the fixture fee passes the ZIP-317 checks"),
            );
        }
        Universe { txs }
    }

    /// The index of the transaction with id `id`.
    fn index(&self, id: UnminedTxId) -> Option<usize> {
        self.txs.iter().position(|tx| tx.transaction.id() == id)
    }

    /// The index of the transaction with mined id `hash`.
    fn index_of_hash(&self, hash: transaction::Hash) -> Option<usize> {
        self.txs
            .iter()
            .position(|tx| tx.transaction.id().mined_id() == hash)
    }

    /// Transaction `index`'s exact id.
    fn id(&self, index: usize) -> UnminedTxId {
        self.txs[index].transaction.id()
    }
}

/// A P2PKH lock script to a zero key hash, which is standard.
fn p2pkh() -> transparent::Script {
    let mut script = vec![0x76, 0xa9, 0x14];
    script.extend_from_slice(&[0; 20]);
    script.extend_from_slice(&[0x88, 0xac]);
    transparent::Script::new(&script)
}

/// The modeled verifier's error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SimVerifyError {
    /// The script says the transaction is invalid.
    Invalid,
    /// A spent output is in neither the chain nor the pool.
    MissingInput,
    /// The chain mined the transaction.
    Mined,
}

impl std::fmt::Display for SimVerifyError {
    /// Names the error.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let text = match self {
            SimVerifyError::Invalid => "the modeled verifier rejects the transaction",
            SimVerifyError::MissingInput => "a spent output is missing",
            SimVerifyError::Mined => "the chain mined the transaction",
        };
        f.write_str(text)
    }
}

impl std::error::Error for SimVerifyError {}

/// The modeled verifier.
#[derive(Clone)]
struct SimVerifier {
    /// A reader, so the verifier the core owns does not keep it running.
    pool: PoolReader,
    universe: Arc<Universe>,
    evidence: SharedEvidence,
    delay: Duration,
}

impl Verifier<SimVerifyError> for SimVerifier {
    /// Reads the pool as the core published it when it started this job,
    /// then, once polled, waits its delay and reads the modeled chain. The
    /// transaction counts as being verified until the job ends.
    fn verify(
        &self,
        tx: UnminedTx,
        _tip: ChainTip,
        revalidation: bool,
        _after_reset: bool,
    ) -> Pin<Box<dyn Future<Output = Outcome<SimVerifyError>> + Send>> {
        let snapshot = self.pool.snapshot();
        let verifier = self.clone();
        let verifying = self
            .universe
            .index(tx.id())
            .map(|index| Verifying::start(&self.evidence, index, revalidation));
        Box::pin(async move {
            let _verifying = verifying;
            if !verifier.delay.is_zero() {
                zakura_rt::time::sleep(verifier.delay).await;
            }
            let Some(index) = verifier.universe.index(tx.id()) else {
                return Outcome::LocalFault(LocalFault::StateUnavailable);
            };
            let mined = lock(&verifier.evidence).mined.clone();
            if mined.contains(&index) {
                return Outcome::AlreadyMined(Arc::new(SimVerifyError::Mined));
            }
            match UNIVERSE[index].script {
                Script::Invalid | Script::Banned => {
                    let charge = if UNIVERSE[index].script == Script::Invalid {
                        Charge::Cooldown
                    } else {
                        Charge::Ban
                    };
                    let mut evidence = lock(&verifier.evidence);
                    evidence.failed_outcomes += 1;
                    if charge == Charge::Cooldown {
                        evidence.cooldown_failures += 1;
                    }
                    return Outcome::Fail {
                        error: RejectionError::ExactTip(
                            ExactTipRejectionError::FailedVerification(SimVerifyError::Invalid),
                        ),
                        charge,
                    };
                }
                Script::Valid => {}
            }
            // A spend of the universe's own transaction is found in the
            // chain if a block mined it, or in the snapshot the job started
            // with. Every other spend is a chain output.
            let mut pool_inputs = Vec::new();
            let mut missing = Vec::new();
            for outpoint in tx.transaction().spent_outpoints() {
                let Some(parent) = verifier.universe.index_of_hash(outpoint.hash) else {
                    continue;
                };
                if mined.contains(&parent) {
                    continue;
                }
                if snapshot
                    .as_ref()
                    .is_some_and(|snapshot| snapshot.created_output(&outpoint).is_some())
                {
                    pool_inputs.push(outpoint);
                } else {
                    missing.push(outpoint);
                }
            }
            if !missing.is_empty() {
                return Outcome::MissingInputs {
                    missing,
                    error: SimVerifyError::MissingInput,
                };
            }
            lock(&verifier.evidence).passed.insert(index);
            Outcome::Pass {
                tx: verifier.universe.txs[index].clone(),
                pool_inputs,
            }
        })
    }
}

/// One transaction the verifier is checking, counted in
/// [`PoolEvidence::verifying`] until the job ends or is dropped.
///
/// [`PoolEvidence::verifying`]: crate::domain::pool::PoolEvidence::verifying
struct Verifying {
    evidence: SharedEvidence,
    index: usize,
}

impl Verifying {
    /// Counts `index` as being verified, a revalidation if `revalidation`.
    fn start(evidence: &SharedEvidence, index: usize, revalidation: bool) -> Self {
        let mut shared = lock(evidence);
        shared.verifying.insert(index, revalidation);
        shared.count_slots();
        Verifying {
            evidence: Arc::clone(evidence),
            index,
        }
    }
}

impl Drop for Verifying {
    /// Stops counting the transaction. A later job for the same transaction
    /// may lose its count too, which only undercounts.
    fn drop(&mut self) {
        lock(&self.evidence).verifying.remove(&self.index);
    }
}

/// One thing a submitter does.
#[derive(Clone, Copy, Debug)]
enum Op {
    /// Reserves each id to fetch it, holding what is granted, then submits
    /// each granted body.
    Burst(&'static [usize]),
    /// Reserves an id with its body in hand, and submits it.
    Deliver(usize),
    /// Reserves an id to fetch it, then reports the fetch failed.
    FetchFails(usize),
    /// Reserves an id, then drops the reservation.
    Abandon(usize),
    /// Waits this many seconds.
    Pause(u64),
    /// Waits for the chain script to finish, then this many seconds.
    AfterChain(u64),
}

/// A submitter: its task's name, and what it does. The core knows no peers,
/// so submitters differ only in what they submit and when.
struct Submitter {
    label: &'static str,
    script: &'static [Op],
}

/// The run's submitters.
fn submitters() -> Vec<Submitter> {
    vec![
        // Three fetches at once, then two children of the first.
        Submitter {
            label: "peer-a",
            script: &[
                Op::Burst(&[T0, T3, T11]),
                Op::Pause(4),
                Op::Burst(&[T1, T2]),
            ],
        },
        // A conflict with peer-a's T3, then an invalid body that earns a
        // cooldown.
        Submitter {
            label: "peer-b",
            script: &[Op::Pause(1), Op::Deliver(T4), Op::Deliver(T6)],
        },
        // A body that earns a ban, and a child before its parent.
        Submitter {
            label: "peer-c",
            script: &[Op::Deliver(T7), Op::Deliver(T9)],
        },
        // A late delivery.
        Submitter {
            label: "peer-d",
            script: &[Op::Pause(6), Op::Deliver(T14)],
        },
        // This node: the parent T9 waits for, an expiring body, the body a
        // block will mine, a failed fetch, an abandoned reservation, an
        // invalid body, and once the chain is quiet a burst past the node's
        // limit.
        Submitter {
            label: "local",
            script: &[
                Op::Pause(2),
                Op::Deliver(T8),
                Op::Deliver(T5),
                Op::Deliver(T10),
                Op::FetchFails(T12),
                Op::Abandon(T13),
                Op::Deliver(T15),
                Op::AfterChain(5),
                Op::Burst(&FILL),
            ],
        },
    ]
}

/// Runs one submitter's script.
async fn submit_all(
    submitter: Submitter,
    handle: PoolHandle,
    universe: Arc<Universe>,
    evidence: SharedEvidence,
) {
    let at = Submitting {
        handle: &handle,
        universe: &universe,
        evidence: &evidence,
    };
    for op in submitter.script {
        match *op {
            Op::Burst(ids) => {
                let mut granted = Vec::new();
                for &tx in ids {
                    if let Some(reservation) = at.reserve(tx, Intent::Fetch).await {
                        granted.push((tx, reservation));
                    }
                }
                for (tx, reservation) in granted {
                    at.submit(tx, reservation).await;
                }
            }
            Op::Deliver(tx) => {
                if let Some(reservation) = at.reserve(tx, Intent::Deliver).await {
                    at.submit(tx, reservation).await;
                }
            }
            Op::FetchFails(tx) => {
                if let Some(reservation) = at.reserve(tx, Intent::Fetch).await {
                    lock(&evidence).held.remove(&tx);
                    handle.fetch_failed(reservation).await;
                }
            }
            Op::Abandon(tx) => {
                if let Some(reservation) = at.reserve(tx, Intent::Fetch).await {
                    lock(&evidence).held.remove(&tx);
                    drop(reservation);
                }
            }
            Op::Pause(secs) => zakura_rt::time::sleep(Duration::from_secs(secs)).await,
            Op::AfterChain(secs) => {
                for _ in 0..600 {
                    if lock(&evidence).chain_done {
                        break;
                    }
                    zakura_rt::time::sleep(Duration::from_secs(1)).await;
                }
                zakura_rt::time::sleep(Duration::from_secs(secs)).await;
            }
        }
    }
}

/// One submitter's view of the run, for reserving and submitting.
#[derive(Clone, Copy)]
struct Submitting<'a> {
    handle: &'a PoolHandle,
    universe: &'a Universe,
    evidence: &'a SharedEvidence,
}

impl Submitting<'_> {
    /// Reserves `tx`, counting a grant as held.
    async fn reserve(&self, tx: usize, intent: Intent) -> Option<Reservation> {
        let answer = self.handle.reserve(self.universe.id(tx), intent).await;
        let reservation = answer.ok()?;
        let mut evidence = lock(self.evidence);
        evidence.held.insert(tx);
        evidence.count_slots();
        Some(reservation)
    }

    /// Submits `tx` under `reservation`, and checks the verdict's charge
    /// against `tx`'s script: an invalid body earns a cooldown, a banned one
    /// a ban, and a valid body the pool rejects for its own rules earns
    /// nothing. Who is charged is the caller's, which the core never knows.
    async fn submit(&self, tx: usize, reservation: Reservation) {
        let refusals_before = {
            let mut evidence = lock(self.evidence);
            evidence.held.remove(&tx);
            evidence.submits_started += 1;
            evidence.logged_refusals.get(&tx).copied().unwrap_or(0)
        };
        let body = self.universe.txs[tx].transaction.clone();
        let verdict = self.handle.submit(reservation, body).await;
        let mut evidence = lock(self.evidence);
        evidence.verifying.remove(&tx);
        evidence.submits_answered += 1;
        if matches!(verdict, Verdict::LocalFault(LocalFault::Stopped)) {
            // A dropped reply is `pool/verdict-for-every-submission`'s to
            // report. It says nothing about the charge.
            evidence.stopped.push(tx);
            return;
        }
        let charge = match &verdict {
            Verdict::Rejected(rejection) => Some(rejection.charge),
            _ => None,
        };
        let expected = match UNIVERSE[tx].script {
            Script::Valid => charge.map(|_| Charge::None),
            Script::Invalid => Some(Charge::Cooldown),
            Script::Banned => Some(Charge::Ban),
        };
        if let Some(expected) = expected {
            *evidence
                .charges_checked
                .entry(format!("{expected:?}"))
                .or_default() += 1;
            if charge != Some(expected) && evidence.wrong_charge.is_none() {
                evidence.wrong_charge = Some(format!(
                    "transaction {tx} got {verdict:?}, and its script earns {expected:?}"
                ));
            }
        }
        // The core publishes a step's changes before it answers, so an
        // admission is already in the log.
        if matches!(verdict, Verdict::Admitted) {
            evidence.admitted += 1;
        }
        if matches!(verdict, Verdict::Admitted)
            && !evidence.logged_additions.contains(&tx)
            && evidence.log_mismatch.is_none()
        {
            evidence.log_mismatch = Some(format!(
                "transaction {tx} was admitted, and no publication before its verdict logged it"
            ));
        }
        // Every other ending of an admission logs a refusal first, which the
        // indexer reports as today's invalidation. A pool turning off answers
        // without one, as today's sends none for its queue.
        let refused = matches!(verdict, Verdict::Rejected(_))
            || matches!(
                verdict,
                Verdict::LocalFault(fault) if fault != LocalFault::Deactivated
            );
        if refused {
            evidence.refusals_answered += 1;
            evidence.refused.insert(tx);
        }
        if refused
            && evidence.logged_refusals.get(&tx).copied().unwrap_or(0) == refusals_before
            && evidence.log_mismatch.is_none()
        {
            evidence.log_mismatch = Some(format!(
                "transaction {tx} got {verdict:?}, and no publication before its verdict logged its refusal"
            ));
        }
    }
}

/// The chain the script moves: its blocks above the start, and what each
/// mined.
struct Chain {
    sender: Rc<RefCell<ChainTransitionSender>>,
    blocks: Vec<Vec<usize>>,
    fork: u8,
}

impl Chain {
    /// The tip at `height` on the current fork.
    fn tip(&self, height: u32) -> ChainTip {
        tip(height, if height == START { 0 } else { self.fork })
    }

    /// The current tip.
    fn current(&self) -> ChainTip {
        let height = START + u32::try_from(self.blocks.len()).expect("the script is short");
        self.tip(height)
    }

    /// Disconnects `disconnect` blocks, then connects one block for each
    /// entry of `connect`, mining its transactions, as one transition.
    async fn change(
        &mut self,
        universe: &Universe,
        evidence: &SharedEvidence,
        disconnect: usize,
        connect: Vec<Vec<usize>>,
        rules_change: bool,
    ) {
        let before = self.current();
        let disconnected: Vec<ChainTip> = (0..disconnect)
            .map(|back| {
                let height = before.height.0 - u32::try_from(back).expect("a short reorg");
                self.tip(height)
            })
            .collect();
        if disconnect > 0 {
            self.fork = self.fork.wrapping_add(1);
        }
        self.sender.borrow().begin_change();
        // The new chain becomes readable: the verifier's view of what is
        // mined changes now, while the generation reads `Changing`. The
        // core may cancel any revalidation for this change, so none counts
        // from here on.
        {
            let mut evidence = lock(evidence);
            evidence.verifying.retain(|_, revalidation| !*revalidation);
            for _ in 0..disconnect {
                for tx in self.blocks.pop().unwrap_or_default() {
                    evidence.mined.remove(&tx);
                }
            }
            for block in &connect {
                evidence.mined.extend(block.iter().copied());
            }
        }
        let mut connected = Vec::new();
        for block in connect {
            self.blocks.push(block.clone());
            let at = self.current();
            let mined: std::collections::BTreeSet<usize> =
                self.blocks.iter().flatten().copied().collect();
            lock(evidence).mined_at.insert(at.hash.0, mined);
            connected.push(ConnectedBlock {
                hash: at.hash,
                height: at.height,
                transactions: block
                    .iter()
                    .map(|tx| {
                        TransactionHandle::Shared(
                            universe.txs[*tx].transaction.transaction().clone(),
                        )
                    })
                    .collect(),
                transaction_hashes: block.iter().map(|tx| universe.id(*tx).mined_id()).collect(),
            });
        }
        // A verification can run here and read `Changing`.
        zakura_rt::task::yield_now().await;
        let after = self.current();
        self.sender
            .borrow_mut()
            .finish_change(after, |before_tip, generation| {
                Some(ChainTransition {
                    before: before_tip,
                    disconnected,
                    connected,
                    after,
                    generation,
                    rules_change,
                })
            });
    }

    /// Loses the transition queue: the core gets a resync at the tip.
    async fn resync(&mut self, evidence: &SharedEvidence) {
        self.sender.borrow().begin_change();
        lock(evidence)
            .verifying
            .retain(|_, revalidation| !*revalidation);
        zakura_rt::task::yield_now().await;
        let tip = self.current();
        self.sender.borrow_mut().finish_unknown_change(Some(tip));
    }
}

/// A tip at `height` on fork `fork`.
fn tip(height: u32, fork: u8) -> ChainTip {
    let mut hash = [fork; 32];
    hash[..4].copy_from_slice(&height.to_be_bytes());
    ChainTip {
        hash: block::Hash(hash),
        height: Height(height),
    }
}

/// The visible transactions of `handle`'s latest snapshot, by index.
fn visible_now(handle: &PoolHandle, universe: &Universe) -> std::collections::BTreeSet<usize> {
    handle.snapshot().map_or_else(Default::default, |snapshot| {
        snapshot
            .transactions
            .iter()
            .filter_map(|tx| universe.index(tx.transaction.id()))
            .collect()
    })
}

/// Runs the chain script.
async fn run_chain(
    mut chain: Chain,
    scenario: PoolScenario,
    handle: PoolHandle,
    universe: Arc<Universe>,
    evidence: SharedEvidence,
) {
    let sleep = |secs| zakura_rt::time::sleep(Duration::from_secs(secs));
    sleep(2).await;
    chain
        .change(&universe, &evidence, 0, vec![Vec::new()], false)
        .await;
    if scenario.rules_change {
        sleep(3).await;
        chain
            .change(&universe, &evidence, 0, vec![Vec::new()], true)
            .await;
    }
    // Wait, within a bound, for the body a block will mine to be pooled.
    for _ in 0..60 {
        if visible_now(&handle, &universe).contains(&T10) {
            break;
        }
        sleep(1).await;
    }
    chain
        .change(&universe, &evidence, 0, vec![vec![T10], Vec::new()], false)
        .await;
    if scenario.reorg {
        sleep(5).await;
        let before = visible_now(&handle, &universe);
        {
            let mut evidence = lock(&evidence);
            if !before.is_empty() {
                evidence.reorgs_with_pool += 1;
                evidence.before_reorg = before;
            }
        }
        chain
            .change(&universe, &evidence, 1, vec![Vec::new(), Vec::new()], false)
            .await;
    }
    if scenario.resync {
        sleep(5).await;
        chain.resync(&evidence).await;
    }
    lock(&evidence).chain_done = true;
}

/// The pool a change log describes, replayed one entry at a time.
#[derive(Debug, Default)]
struct Replay {
    visible: std::collections::BTreeSet<usize>,
    hidden: std::collections::BTreeSet<usize>,
}

impl Replay {
    /// Applies `change`, or says why the entries before it do not allow it:
    /// an addition or refusal of a pooled transaction, a hiding of one that
    /// is not visible, a reveal of one that is not hidden, or a removal of
    /// one that is not pooled.
    fn apply(&mut self, universe: &Universe, change: Change) -> Result<(), String> {
        let id = match change {
            Change::Added(id) | Change::Hidden(id) | Change::Refused(id) => id,
            Change::Removed { id, .. } | Change::Revealed { id, .. } => id,
        };
        let Some(tx) = universe.index(id) else {
            return Err(format!(
                "{change:?} names a transaction outside the universe"
            ));
        };
        let (visible, hidden) = (self.visible.contains(&tx), self.hidden.contains(&tx));
        let allowed = match change {
            Change::Added(_) | Change::Refused(_) => !visible && !hidden,
            Change::Hidden(_) => visible,
            Change::Revealed { .. } => hidden,
            Change::Removed { .. } => visible || hidden,
        };
        if !allowed {
            let state = if visible {
                "visible"
            } else if hidden {
                "hidden"
            } else {
                "not pooled"
            };
            return Err(format!(
                "{change:?} for transaction {tx}, which was {state}"
            ));
        }
        match change {
            Change::Added(_) | Change::Revealed { .. } => {
                self.hidden.remove(&tx);
                self.visible.insert(tx);
            }
            Change::Hidden(_) => {
                self.visible.remove(&tx);
                self.hidden.insert(tx);
            }
            Change::Removed { .. } => {
                self.visible.remove(&tx);
                self.hidden.remove(&tx);
            }
            Change::Refused(_) => {}
        }
        Ok(())
    }
}

/// The visible transactions of `snapshot`, by index.
fn shown(snapshot: &PoolSnapshot, universe: &Universe) -> std::collections::BTreeSet<usize> {
    snapshot
        .transactions
        .iter()
        .filter_map(|tx| universe.index(tx.transaction.id()))
        .collect()
}

/// Checks one publication as the core makes it: its batches follow the log
/// so far, lead to exactly its snapshot, and the snapshot is a consistent
/// pool. Runs inside the core's publish, so it sees every snapshot.
fn check_publication(
    universe: &Universe,
    evidence: &SharedEvidence,
    replay: &mut Replay,
    last: &mut ChangeSeq,
    batches: &[Vec<Change>],
    snapshot: &PoolSnapshot,
) {
    let mut problem = None;
    let count: usize = batches.iter().map(Vec::len).sum();
    let count = u64::try_from(count).expect("one publication is short");
    if snapshot.last_change.0 != last.0 + count {
        problem = Some(format!(
            "the snapshot at {} follows {} with {count} changes",
            snapshot.last_change.0, last.0
        ));
    }
    *last = snapshot.last_change;
    let (mut removals, mut additions) = (0, 0);
    let (mut added, mut refused) = (Vec::new(), Vec::new());
    for change in batches.iter().flatten() {
        match change {
            Change::Removed { .. } => removals += 1,
            Change::Added(id) => {
                additions += 1;
                added.extend(universe.index(*id));
            }
            Change::Refused(id) => refused.extend(universe.index(*id)),
            _ => {}
        }
        if let Err(error) = replay.apply(universe, *change) {
            problem.get_or_insert(error);
        }
    }
    let shown = shown(snapshot, universe);
    if shown != replay.visible {
        problem.get_or_insert(format!(
            "the log leads to {:?} and the snapshot at {} holds {shown:?}",
            replay.visible, snapshot.last_change.0
        ));
    }
    let mut evidence = lock(evidence);
    let inconsistent = inconsistency(
        snapshot,
        universe,
        evidence.mined_at.get(&snapshot.tip.hash.0),
    );
    evidence.logs_checked += 1;
    evidence.snapshots_checked += 1;
    evidence.removals_seen += removals;
    evidence.additions_seen += additions;
    evidence.logged_additions.extend(added);
    for tx in refused {
        *evidence.logged_refusals.entry(tx).or_default() += 1;
    }
    if evidence.log_mismatch.is_none() {
        evidence.log_mismatch = problem;
    }
    if evidence.inconsistent.is_none() {
        evidence.inconsistent = inconsistent;
    }
    evidence.last_visible = shown;
    evidence.tip = snapshot.tip.height.0;
}

/// Follows the change log as a reader does, and checks that each view's
/// entries follow the ones before them and lead to its snapshot.
async fn observe(mut handle: PoolHandle, universe: Arc<Universe>, evidence: SharedEvidence) {
    let mut cursor = ChangeSeq(0);
    let mut replay = Replay::default();
    while handle.snapshot_changed().await {
        let Some(view) = handle.view(cursor) else {
            continue;
        };
        let snapshot = view.snapshot;
        let mut problem = None;
        let mut refused = Vec::new();
        match view.changes {
            ChangesSince::Entries(entries) => {
                for entry in entries {
                    if let Change::Refused(id) = entry.change {
                        refused.extend(universe.index(id));
                    }
                    if entry.seq.0 != cursor.0 + 1 && problem.is_none() {
                        problem = Some(format!(
                            "entry {} follows {} in the log",
                            entry.seq.0, cursor.0
                        ));
                    }
                    cursor = entry.seq;
                    if let Err(error) = replay.apply(&universe, entry.change) {
                        problem.get_or_insert(error);
                    }
                }
            }
            ChangesSince::Resync => {
                problem = Some("the observer fell off the log".to_owned());
            }
        }
        // The entries reach the snapshot, so a view that left off its last
        // entries, which may change nothing a snapshot shows, is seen.
        if cursor != snapshot.last_change && problem.is_none() {
            problem = Some(format!(
                "a reader's entries end at {} and its snapshot at {}",
                cursor.0, snapshot.last_change.0
            ));
        }
        cursor = snapshot.last_change;
        let shown = shown(&snapshot, &universe);
        if shown != replay.visible {
            problem.get_or_insert(format!(
                "a reader's log leads to {:?} and the snapshot at {} holds {shown:?}",
                replay.visible, snapshot.last_change.0
            ));
        }
        let mut evidence = lock(&evidence);
        evidence.logs_checked += 1;
        evidence.read_refusals.extend(refused);
        if evidence.log_mismatch.is_none() {
            evidence.log_mismatch = problem;
        }
    }
}

/// What makes `snapshot` inconsistent, if anything: two transactions that
/// spend one transparent output, an expired transaction, a transaction the
/// snapshot's chain (`mined`) mined, a transaction whose parent is
/// neither mined on the snapshot's chain (`mined`) nor visible, dependency
/// edges other than those the bodies imply, or a dependency group past
/// three.
fn inconsistency(
    snapshot: &PoolSnapshot,
    universe: &Universe,
    mined: Option<&std::collections::BTreeSet<usize>>,
) -> Option<String> {
    use std::collections::{BTreeMap, BTreeSet};
    let mut spent = BTreeMap::new();
    for tx in &snapshot.transactions {
        let index = universe.index(tx.transaction.id());
        for outpoint in tx.transaction.transaction().spent_outpoints() {
            if let Some(other) = spent.insert((outpoint.hash.0, outpoint.index), index) {
                return Some(format!(
                    "transactions {other:?} and {index:?} spend one output"
                ));
            }
        }
        if let Some(expiry) = tx.transaction.transaction().expiry_height() {
            if snapshot.tip.height >= expiry {
                return Some(format!(
                    "transaction {index:?} expired at {} and the tip is {}",
                    expiry.0, snapshot.tip.height.0
                ));
            }
        }
    }
    // The edges the bodies imply: a visible child to each visible parent.
    // A parent that is neither visible nor mined is missing.
    let visible = shown(snapshot, universe);
    let mut expected: BTreeMap<usize, BTreeSet<usize>> = BTreeMap::new();
    for tx in &snapshot.transactions {
        let Some(child) = universe.index(tx.transaction.id()) else {
            return Some("a transaction outside the universe".to_owned());
        };
        if mined.is_some_and(|mined| mined.contains(&child)) {
            return Some(format!(
                "transaction {child} is visible, and the snapshot's chain mined it"
            ));
        }
        for outpoint in tx.transaction.transaction().spent_outpoints() {
            let Some(parent) = universe.index_of_hash(outpoint.hash) else {
                continue;
            };
            if visible.contains(&parent) {
                expected.entry(child).or_default().insert(parent);
            } else if mined.is_some_and(|mined| !mined.contains(&parent)) {
                return Some(format!(
                    "transaction {child} spends transaction {parent}, which is neither mined nor visible"
                ));
            }
        }
    }
    let published = edges(universe, snapshot.dependencies.dependencies());
    if published != expected {
        return Some(format!(
            "the snapshot's dependencies are {published:?} and its bodies imply {expected:?}"
        ));
    }
    let mut inverse: BTreeMap<usize, BTreeSet<usize>> = BTreeMap::new();
    for (child, parents) in &expected {
        for parent in parents {
            inverse.entry(*parent).or_default().insert(*child);
        }
    }
    let dependents = edges(universe, snapshot.dependencies.dependents());
    if dependents != inverse {
        return Some(format!(
            "the snapshot's dependents are {dependents:?} and its bodies imply {inverse:?}"
        ));
    }
    // Each group's size, walking the implied edges both ways.
    let mut seen = BTreeSet::new();
    for start in &visible {
        if seen.contains(start) {
            continue;
        }
        let mut group = BTreeSet::new();
        let mut next = vec![*start];
        while let Some(tx) = next.pop() {
            if !group.insert(tx) {
                continue;
            }
            for linked in expected.get(&tx).into_iter().chain(inverse.get(&tx)) {
                next.extend(linked.iter().copied());
            }
        }
        if group.len() > 3 {
            return Some(format!(
                "a dependency group of {} transactions",
                group.len()
            ));
        }
        seen.extend(group);
    }
    None
}

/// A published dependency map as edges between universe indexes. A hash
/// outside the universe becomes `usize::MAX`, which no expected edge names.
fn edges<'a, T: IntoIterator<Item = &'a transaction::Hash>>(
    universe: &Universe,
    map: impl IntoIterator<Item = (&'a transaction::Hash, T)>,
) -> std::collections::BTreeMap<usize, std::collections::BTreeSet<usize>> {
    let index = |hash: &transaction::Hash| universe.index_of_hash(*hash).unwrap_or(usize::MAX);
    let mut edges: std::collections::BTreeMap<usize, std::collections::BTreeSet<usize>> =
        std::collections::BTreeMap::new();
    for (from, to) in map {
        edges
            .entry(index(from))
            .or_default()
            .extend(to.into_iter().map(index));
    }
    edges.retain(|_, to| !to.is_empty());
    edges
}

/// The pool core with its submitters, chain script, verifier and observer.
pub struct PoolCoreHarness {
    evidence: SharedEvidence,
    /// Held for the whole run, so the core never sees its handles or the
    /// chain go away.
    _handle: PoolHandle,
    _chain: Rc<RefCell<ChainTransitionSender>>,
    _active: watch::Sender<bool>,
}

impl HasView for PoolCoreHarness {
    type View = PoolView;

    /// The shared evidence.
    fn view(&self) -> PoolView {
        self.evidence.clone()
    }

    /// Records how the run ended, for [`HasView::end_violations`].
    fn ended(&self, end: &RunEnd) {
        properties::ended(&self.evidence, end);
    }

    /// See [`properties::end_violations`].
    fn end_violations(&self, _log: &ObservationLog) -> Vec<EndViolation> {
        properties::end_violations(&self.evidence)
    }
}

impl Subsystem for PoolCoreHarness {
    const NAME: &'static str = "pool/core";
    const GENERATOR: u32 = 1;
    const SCENARIO_BYTES: usize = 2;
    type Scenario = PoolScenario;
    type Prepared = SharedEvidence;

    /// Two workers, a budget past the scripts, and a timer cap past the
    /// core's longest deadline, so a parked transaction's wait can end in
    /// the drain.
    fn config(_: &PoolScenario) -> RunConfig {
        RunConfig {
            scheduler: SchedulerConfig {
                workers: 2,
                ..SchedulerConfig::default()
            },
            budget: 60_000,
            drain_bound: 60_000,
            harness_timer_cap: Duration::from_secs(180),
            ..RunConfig::default()
        }
    }

    /// Empty evidence, which a caller may keep to read after the run.
    fn prepare(_: &PoolScenario) -> SharedEvidence {
        SharedEvidence::default()
    }

    /// Starts the core, the submitters, the chain script and the observer.
    fn build(sim: &mut Sim, scenario: &PoolScenario, evidence: SharedEvidence) -> Self {
        let universe = Arc::new(Universe::build());
        {
            let mut evidence = lock(&evidence);
            evidence.conflict_pairs = vec![(T3, T4)];
            for (index, planned) in UNIVERSE.iter().enumerate() {
                if let Some(after) = planned.expires_after {
                    evidence.expiries.insert(index, START + after);
                }
            }
            evidence.tip = START;
        }
        let sender = chain_transitions(Some(tip(START, 0)));
        let transitions = sender.transitions();
        let (start, events) = transitions
            .subscribe()
            .expect("the harness is the queue's only consumer");
        let generation = transitions.generation();
        let sender = Rc::new(RefCell::new(sender));
        lock(&evidence)
            .mined_at
            .insert(tip(START, 0).hash.0, Default::default());
        let (handle, mut inbox) = pool_port();
        {
            let universe = Arc::clone(&universe);
            let evidence = Arc::clone(&evidence);
            let replay = std::sync::Mutex::new((Replay::default(), ChangeSeq(0)));
            inbox.observe_publications(Box::new(move |batches, snapshot| {
                let mut replay = replay
                    .lock()
                    .expect("no publication check panics while it holds the replay");
                let (replay, last) = &mut *replay;
                check_publication(&universe, &evidence, replay, last, batches, snapshot);
            }));
        }
        let (active, active_rx) = watch::channel(true);
        let verifier = SimVerifier {
            pool: handle.reader(),
            universe: Arc::clone(&universe),
            evidence: Arc::clone(&evidence),
            delay: Duration::from_millis(u64::from(scenario.verify_millis)),
        };
        sim.scheduler().spawn_root("pool-core", async move {
            let core = PoolCore::with_limits(
                &Config::default(),
                start,
                events,
                generation,
                verifier,
                inbox,
                active_rx,
                Limits {
                    pending: usize::try_from(LIMITS.pending).expect("a small limit"),
                    revalidating: 3,
                },
            );
            core.run().await;
        });
        for submitter in submitters() {
            let (handle, universe, evidence) =
                (handle.clone(), Arc::clone(&universe), Arc::clone(&evidence));
            sim.scheduler().spawn_root(
                submitter.label,
                submit_all(submitter, handle, universe, evidence),
            );
        }
        {
            let handle = handle.clone();
            let outpoint = OutPoint {
                hash: universe.id(T8).mined_id(),
                index: 0,
            };
            sim.scheduler().spawn_root("output-waiter", async move {
                let _output = handle.await_output(outpoint).await;
            });
        }
        let chain = Chain {
            sender: Rc::clone(&sender),
            blocks: Vec::new(),
            fork: 0,
        };
        sim.scheduler().spawn_root(
            "chain",
            run_chain(
                chain,
                scenario.clone(),
                handle.clone(),
                Arc::clone(&universe),
                Arc::clone(&evidence),
            ),
        );
        sim.scheduler().spawn_root(
            "observer",
            observe(handle.clone(), Arc::clone(&universe), Arc::clone(&evidence)),
        );
        PoolCoreHarness {
            evidence,
            _handle: handle,
            _chain: sender,
            _active: active,
        }
    }

    /// [`crate::domain::pool::properties`].
    fn properties() -> Vec<Property<Self>> {
        properties::properties()
    }

    /// The schedule, and the evidence the tasks record.
    fn sources() -> Sources {
        Sources::observed(&[Source::Schedule, Source::View])
    }

    /// The first byte is the verifier's delay in units of 4 ms. The second
    /// byte's low bits pick an upgrade, a reorg and a resync.
    fn draw(prefix: &[u8]) -> PoolScenario {
        let delay = prefix.first().copied().unwrap_or(5);
        let flags = prefix.get(1).copied().unwrap_or(0b110);
        PoolScenario {
            verify_millis: u16::from(delay) * 4,
            rules_change: flags & 0b001 != 0,
            reorg: flags & 0b010 != 0,
            resync: flags & 0b100 != 0,
        }
    }
}
