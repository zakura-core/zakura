//! BPv1 IO layer: [`Bpv1Service`], [`Bpv1Handle`], peer routines, node driver,
//! proposer driver, `zakura-quic` `PathSource`, and metrics/trace translation.
//!
//! IO makes no protocol decisions: every rule lives in [`PeerState`],
//! [`BlockTable`], [`PeerPaths`], and [`ProposerState`]. Shared state sits
//! behind short, non-awaited [`Mutex`] locks.

use std::{collections::VecDeque, fmt, sync::Arc};

use futures::future::BoxFuture;
use rand_10::RngExt;

use zakura_chain::{
    block::{self, Block, Header, Height},
    parameters::Network,
    serialization::ZcashSerialize,
};
use zakura_header_chain::{EngineSnapshot, FrontierSet};
use zakura_quic::{ConnLoad, NodeId};
use zakura_rt::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc, watch, CancellationToken, Mutex, MutexGuard, PoisonError, Semaphore,
    },
    task,
    time::{self, Instant},
};

use super::{
    assemble::assemble_block,
    blocks::{
        BlockEvent, BlockInput, BlockOutcome, BlockOutput, BlockStep, BlockTable, FallbackReason,
        MetaEnvelope, PartSource, PullPeerState, StripeJob, VerifierVerdict,
    },
    close::{Bpv1CloseCause, Bpv1CloseRow},
    codec::{decode_and_check_stripe, encode_body},
    commitment::{
        coinbase_paths, commitment_payload, extract_single_commitment, verify_commitment,
        CommitmentFields, COMMITMENT_PAYLOAD_LEN,
    },
    config::{HavePacing, ZakuraBpv1Config, PATH_POLL_INTERVAL},
    paths::{Arrival, PathEstimate},
    peer::{
        stall_timeout, DropReason, PartAuth, PartToken, PeerEvent, PeerInput, PeerOutput,
        PeerState, PeerStep, RecvHave, RecvPartPrefix, RecvWant, RecvWantEnd, Seed, Seeds,
        ServeDone, ServePartQueued, PULL_WINDOW,
    },
    ports::{BlockFallbackPort, BlockVerifierPort, BodyStorePort, HeaderAdmissionPort},
    profile::{Coding, Profile, W2_PROFILE_ID},
    proposer::{
        compute_body_identity, PreparedCommitment, ProposerConfig, ProposerEvent, ProposerInput,
        ProposerOutput, ProposerState, SeedDeal, TemplateId,
    },
    pull_trace::{is_sampled_part, PullOutcome, PullTraceRecord},
    route::{
        charge_part, held_parts, pull_state, refused_grant, replay_admitted, route_block_output,
        route_peer_output, BlockRoute, PeerRoute,
    },
    tree::{verify_part_proof, VerifiedPart},
    unknown_parent::{
        check_binding_and_height, check_proof_of_work, check_target_bound, target_bound_applies,
        UnknownParentVerdict,
    },
    wire::{
        message_type, read_part_prefix, Bpv1FifoQueue, Bpv1Message, DataQueueItem, HaveFlags,
        WantEndReason, BPV1_CONTROL_KIND, BPV1_DATA_KIND, BPV1_STREAMS,
    },
};
use crate::zakura::{
    wire_codec::{decode_frame, encode_frame, BlockHash, BoundedReader, LeU32, WireError},
    CustomService, Frame, FramedRecv, FramedSend, Peer, Service, ServicePeerDirection,
    SessionOpening, SessionPolicy, Stream, ZakuraConnId, ZakuraPeerId, ZakuraServiceId,
};

/// Converts a `zakura-quic` [`ConnLoad`] snapshot into a routing [`PathEstimate`].
fn conn_load_to_path_estimate(load: &ConnLoad) -> PathEstimate {
    PathEstimate {
        min_rtt: load.min_rtt,
        rate_in: load.rate_in,
        rate_out: load.rate_out,
    }
}

/// Bundle of the node-service ports injected into [`Bpv1Service`].
pub struct Bpv1Ports {
    /// Exact-header headerchain admission port.
    pub header_admission: Arc<dyn HeaderAdmissionPort>,
    /// Retained part & decoded block memory store.
    pub body_store: Arc<dyn BodyStorePort>,
    /// Consensus block verifier.
    pub verifier: Arc<dyn BlockVerifierPort>,
    /// Shared body-claim port coordinating with native block sync.
    pub fallback: Arc<dyn BlockFallbackPort>,
    /// Durable header-chain snapshots. The driver feeds their frontiers to
    /// `BlockTable`.
    pub committed_snapshots: watch::Receiver<Option<EngineSnapshot>>,
}

/// The commitments one encode produced for the templates that joined it.
/// The encoding caller holds the lock for the whole encode, so a joining
/// caller's `lock` returns once the commitments are in.
type EncodeResults = Arc<Mutex<HashMap<TemplateId, PreparedCommitment>>>;

/// One peer's current session as the node driver sees it. The driver sends
/// [`PeerInput`]s straight to the session's routine.
#[derive(Clone)]
struct LiveSession {
    conn_id: ZakuraConnId,
    session_gen: u64,
    inputs: mpsc::UnboundedSender<PeerInput>,
    /// Ends this session only.
    cancel: CancellationToken,
    /// Closes the connection.
    connection: CancellationToken,
    /// The connection's path load, when the transport reports one.
    load: Option<watch::Receiver<ConnLoad>>,
    snapshot: Arc<Mutex<PullPeerState>>,
}

/// Async completion returned to the BPv1 driver task.
enum DriverMsg {
    BlockInput(BlockInput),
    ReplayActiveToPeer {
        peer_node: NodeId,
        session_gen: u64,
    },
    /// A session left `sessions`; the driver ends it in the table.
    SessionEnded {
        peer_node: NodeId,
        session_gen: u64,
    },
    WakeTick,
}

/// Active pull timing state for `arrival_error.seconds` and [`PullTraceRecord`].
#[derive(Copy, Clone, Debug)]
struct ActivePullTelemetry {
    block_id: block::Hash,
    stripe: u32,
    bytes: u64,
    issued_at: Instant,
    estimated_arrival: Instant,
    first_part_at: Option<Instant>,
    rtt_ms: Option<u64>,
    rate_bps: Option<u64>,
    outcome: PullOutcome,
}

/// Shared node state accessed by peer routines, driver task, and proposer driver
/// strictly under short, non-awaited locks (`opt-6`).
struct SharedNode {
    config: ZakuraBpv1Config,
    profile: Profile,
    network: Network,
    chain_id: [u8; 32],
    local_node: NodeId,
    next_session_gen: AtomicU64,
    table: Mutex<BlockTable>,
    proposer: Mutex<ProposerState>,
    sessions: Mutex<HashMap<NodeId, LiveSession>>,
    header_admission: Arc<dyn HeaderAdmissionPort>,
    body_store: Arc<dyn BodyStorePort>,
    verifier: Arc<dyn BlockVerifierPort>,
    fallback: Arc<dyn BlockFallbackPort>,
    verify_sem: Arc<Semaphore>,
    decode_sem: Arc<Semaphore>,
    driver_tx: mpsc::UnboundedSender<DriverMsg>,
    shutdown: CancellationToken,
    /// Monotonic template id generator for [`Bpv1ProposerHook`].
    next_template_id: AtomicU64,
    /// In-flight encodes that other templates can join, keyed by
    /// body identity.
    encodes: Mutex<HashMap<[u8; 32], EncodeResults>>,
}

impl fmt::Debug for SharedNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SharedNode")
            .field("local_node", &self.local_node)
            .field("profile", &self.profile)
            .finish_non_exhaustive()
    }
}

/// Locks `mutex`, taking the value even if a panicking holder poisoned it.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl SharedNode {
    fn lock_table(&self) -> MutexGuard<'_, BlockTable> {
        lock(&self.table)
    }

    fn lock_proposer(&self) -> MutexGuard<'_, ProposerState> {
        lock(&self.proposer)
    }

    fn lock_sessions(&self) -> MutexGuard<'_, HashMap<NodeId, LiveSession>> {
        lock(&self.sessions)
    }

    fn record_pull_trace(&self, rec: PullTraceRecord) {
        let err_secs = (rec.observed_last_ms as f64 - rec.estimated_ms as f64) / 1_000.0;
        metrics::histogram!("zakura.p2p.bpv1.arrival_error.seconds").record(err_secs);
        tracing::debug!(
            peer = ?rec.peer,
            block_id = ?rec.block_id,
            stripe = rec.stripe,
            bytes = rec.bytes,
            rtt_ms = ?rec.rtt_ms,
            rate_bps = ?rec.rate_bps,
            estimated_ms = rec.estimated_ms,
            observed_first_ms = ?rec.observed_first_ms,
            observed_last_ms = rec.observed_last_ms,
            outcome = ?rec.outcome,
            "bpv1 pull completed"
        );
    }

    /// Sends `input` to `peer`'s session. With `session_gen`, only that
    /// session generation receives it. Returns `input` if no session took it.
    fn send_to(
        &self,
        peer: &NodeId,
        session_gen: Option<u64>,
        input: PeerInput,
    ) -> Result<(), PeerInput> {
        match self
            .lock_sessions()
            .get(peer)
            .filter(|sess| session_gen.is_none_or(|gen| gen == sess.session_gen))
        {
            Some(sess) => sess.inputs.send(input).map_err(|refused| refused.0),
            None => Err(input),
        }
    }

    /// Sends the input `make` builds for each live session.
    fn broadcast(&self, mut make: impl FnMut(&NodeId) -> PeerInput) {
        for (peer, sess) in self.lock_sessions().iter() {
            let _ = sess.inputs.send(make(peer));
        }
    }

    /// Closes `peer`'s connection for a protocol violation on `stream_kind`
    /// if `session_gen` still owns it (`bpv1-61`).
    fn protocol_close(&self, peer: &NodeId, session_gen: u64, stream_kind: u16, row: Bpv1CloseRow) {
        let connection = self
            .lock_sessions()
            .get(peer)
            .filter(|sess| sess.session_gen == session_gen)
            .map(|sess| sess.connection.clone());
        if let Some(connection) = connection {
            record_close_metric(row.cause(stream_kind));
            connection.cancel();
        }
    }

    /// Queues `input` for the driver, the sole ordered executor of
    /// `BlockTable` transitions.
    fn drive(&self, input: BlockInput) {
        let _ = self.driver_tx.send(DriverMsg::BlockInput(input));
    }

    fn collect_peer_states(&self) -> Vec<PullPeerState> {
        let snaps: Vec<Arc<Mutex<PullPeerState>>> = self
            .lock_sessions()
            .values()
            .map(|s| s.snapshot.clone())
            .collect();
        snaps.into_iter().map(|s| *lock(&s)).collect()
    }

    /// Executes `outputs` of a proposer step in order.
    fn run_proposer_outputs(self: &Arc<Self>, outputs: Vec<ProposerOutput>) {
        for out in outputs {
            match out {
                ProposerOutput::AdmitSolvedLocal {
                    height,
                    envelope,
                    parts,
                } => {
                    self.apply_block_input(BlockInput::BlockAdmittedLocal {
                        height,
                        envelope,
                        parts,
                    });
                }
                ProposerOutput::QueueSeeds {
                    peer,
                    block_id,
                    seeds,
                    copies,
                } => {
                    for seed in &seeds {
                        let deal = match seed.deal {
                            SeedDeal::One => "1",
                            SeedDeal::Two => "2",
                            SeedDeal::Direct => "direct",
                        };
                        // Widening usize to u64 is lossless on supported 64-bit targets.
                        metrics::counter!("zakura.p2p.bpv1.seed.bytes", "deal" => deal)
                            .increment(seed.part.wire_payload().len() as u64);
                        tracing::debug!(target: "bpv1_stage", %block_id, ?peer,
                            index = seed.part.index(), deal, "seed_queued");
                    }
                    let seeds = seeds
                        .into_iter()
                        .map(|seed| Seed {
                            part: seed.part,
                            relay: seed.relay,
                        })
                        .collect();
                    let _ = self.send_to(
                        &peer,
                        None,
                        PeerInput::Seeds(Seeds {
                            block_id,
                            seeds,
                            copies,
                        }),
                    );
                }
                ProposerOutput::StartEncode { .. } | ProposerOutput::SetCommitment { .. } => {}
            }
        }
    }

    fn apply_block_input(self: &Arc<Self>, input: BlockInput) {
        let step = {
            let mut table = self.lock_table();
            let step = table.step(Instant::now(), input);
            metrics::gauge!("zakura.p2p.bpv1.pending_meta.count")
                .set(table.pending_meta_len() as f64);
            metrics::gauge!("zakura.p2p.bpv1.path.entries").set(table.paths().len() as f64);
            metrics::gauge!("zakura.p2p.bpv1.path.unmeasured_peers")
                .set(table.paths().unmeasured_pull_peers() as f64);
            metrics::gauge!("zakura.p2p.bpv1.part_buffer.bytes")
                .set(table.part_buffer_bytes() as f64);
            step
        };
        self.dispatch_block_step(step);
    }

    fn dispatch_block_step(self: &Arc<Self>, step: BlockStep) {
        for event in step.events {
            record_block_event(&event);
        }
        for output in step.outputs {
            self.execute_block_output(output);
        }
    }

    fn execute_block_output(self: &Arc<Self>, output: BlockOutput) {
        if let BlockOutput::MetaRejected {
            block_id,
            peer,
            session_gen,
        } = &output
        {
            tracing::debug!(target: "bpv1_stage", %block_id, ?peer, session_gen,
                "header_meta_rejected");
        }
        match route_block_output(output) {
            BlockRoute::Peer {
                peer,
                session_gen,
                input,
            } => {
                if let Err(input) = self.send_to(&peer, session_gen, input) {
                    let states = || self.collect_peer_states();
                    if let Some(release) = refused_grant(peer, session_gen, input, states) {
                        self.drive(release);
                    }
                }
            }
            BlockRoute::Broadcast(broadcast) => {
                self.broadcast(|peer| broadcast.input_for(peer));
                if broadcast.admits() {
                    let _ = self.driver_tx.send(DriverMsg::WakeTick);
                }
            }
            BlockRoute::Shell(output) => self.run_shell_output(output),
        }
    }

    /// Runs one [`BlockRoute::Shell`] output.
    fn run_shell_output(self: &Arc<Self>, output: BlockOutput) {
        match output {
            BlockOutput::AdmitHeader {
                work_id,
                block_id,
                header_bytes,
                from_peer,
                session_gen,
            } => {
                tracing::debug!(target: "bpv1_stage", %block_id, peer = ?from_peer,
                    session_gen, work_id = work_id.0, "header_admission_queued");
                let Some(header) = parse_header(&header_bytes) else {
                    self.drive(BlockInput::HeaderAdmitted {
                        work_id,
                        result: super::blocks::HeaderAdmissionResult::PeerInvalid,
                    });
                    return;
                };
                let node = Arc::clone(self);
                let header = Arc::new(header);
                drop(task::spawn_named("bpv1-admit-header", async move {
                    tracing::debug!(target: "bpv1_stage", %block_id, peer = ?from_peer,
                        session_gen, work_id = work_id.0, "header_admission_started");
                    let result = node
                        .header_admission
                        .admit(header, from_peer, session_gen, work_id)
                        .await;
                    tracing::debug!(target: "bpv1_stage", %block_id, peer = ?from_peer,
                        session_gen, work_id = work_id.0, ?result, "header_admission_result");
                    node.drive(BlockInput::HeaderAdmitted { work_id, result });
                }));
            }
            BlockOutput::CheckUnknownParent {
                work_id,
                block_id,
                envelope,
                top,
            } => {
                let node = Arc::clone(self);
                drop(task::spawn_named("bpv1-check-unknown-parent", async move {
                    let verdict = check_unknown_parent(&node, envelope, top).await;
                    tracing::debug!(target: "bpv1_stage", %block_id,
                        work_id = work_id.0, ?verdict, "unknown_parent_checked");
                    node.drive(BlockInput::UnknownParentChecked { work_id, verdict });
                }));
            }
            BlockOutput::VerifyCommitment {
                work_id,
                block_id,
                height,
                envelope,
            } => {
                let node = Arc::clone(self);
                drop(task::spawn_named("bpv1-verify-commit", async move {
                    tracing::debug!(target: "bpv1_stage", %block_id,
                        work_id = work_id.0, "commitment_check_started");
                    let ok = verify_envelope_commitment(
                        &node.network,
                        Height(height),
                        node.chain_id,
                        &envelope,
                    );
                    tracing::debug!(target: "bpv1_stage", %block_id,
                        work_id = work_id.0, ok, "commitment_check_finished");
                    // decision-26: admission picks the `Have` initial group
                    // among the live peers.
                    let peers: Vec<NodeId> = node.lock_sessions().keys().copied().collect();
                    let rng_seed = zakura_rt::rand::rng().random::<u64>();
                    node.drive(BlockInput::CommitmentChecked {
                        work_id,
                        ok,
                        peers,
                        rng_seed,
                    });
                }));
            }
            BlockOutput::ClosePeer {
                peer,
                session_gen,
                row,
            } => {
                // Blocks close only for `HeaderMeta` rows 3 and 4, read on control.
                self.protocol_close(&peer, session_gen, BPV1_CONTROL_KIND, row);
            }
            BlockOutput::ClaimBody { block_id, deadline } => {
                let installed = self.fallback.claim(block_id, deadline);
                tracing::debug!(target: "bpv1_stage", %block_id, installed, "body_claim_installed");
            }
            BlockOutput::ExtendBodyClaim { block_id, deadline } => {
                let extended = self.fallback.extend(block_id, deadline);
                tracing::debug!(target: "bpv1_stage", %block_id, extended, "body_claim_extended");
            }
            BlockOutput::ReleaseBodyClaim { block_id, reason } => {
                let released = self.fallback.release(block_id, reason);
                tracing::debug!(target: "bpv1_stage", %block_id, ?reason, released,
                    "body_claim_released");
            }
            BlockOutput::StartStripeJob {
                work_id,
                kind,
                block_id,
                stripe,
                coding,
                enc_id,
                held,
            } => {
                let node = Arc::clone(self);
                let label = match kind {
                    StripeJob::Decode => "decode",
                    StripeJob::Check => "check",
                };
                drop(task::spawn_named("bpv1-stripe-job", async move {
                    let Ok(_permit) = node.decode_sem.clone().acquire_owned().await else {
                        return;
                    };
                    metrics::gauge!("zakura.p2p.bpv1.work.leases", "kind" => label).increment(1.0);
                    let res = task::spawn_blocking(move || {
                        decode_and_check_stripe(&coding, &enc_id, stripe, &held).map_err(|_| ())
                    })
                    .await
                    .unwrap_or(Err(()));
                    metrics::gauge!("zakura.p2p.bpv1.work.leases", "kind" => label).decrement(1.0);
                    node.drive(BlockInput::StripeJobDone {
                        work_id,
                        block_id,
                        stripe,
                        result: res,
                    });
                }));
            }
            BlockOutput::SubmitVerifier {
                work_id,
                block_id,
                header_bytes,
                coinbase_bytes,
                coding,
                stripe_sources,
            } => {
                let node = Arc::clone(self);
                drop(task::spawn_named("bpv1-assemble-verify", async move {
                    let assembled = parse_header(&header_bytes).and_then(|header| {
                        assemble_block(
                            &node.profile,
                            &header,
                            &coding,
                            &coinbase_bytes,
                            &stripe_sources,
                        )
                        .ok()
                    });
                    let verdict = match assembled {
                        Some((_raw, block)) => {
                            tracing::info!(target: "bpv1_measure", %block_id, "bpv1_body_assembled");
                            // Notify BlockTable ONLY after canonical
                            // `assemble_block` succeeds so `BlockDone` is never sent
                            // for a block that failed assembly/padding validation.
                            node.drive(BlockInput::BlockAssembled { work_id, block_id });
                            match node.body_store.admit_assembled(block) {
                                Ok(owned) => {
                                    tracing::info!(target: "bpv1_measure", %block_id, "bpv1_verifier_intake");
                                    node.verifier.submit(owned, work_id).await
                                }
                                // Distinct local decoded-pool capacity outcome.
                                Err(_) => VerifierVerdict::LocalCapacity,
                            }
                        }
                        None => VerifierVerdict::RootMismatch,
                    };
                    tracing::info!(target: "bpv1_measure", %block_id, ?verdict, "bpv1_verifier_result");
                    node.drive(BlockInput::VerifierVerdict {
                        work_id,
                        block_id,
                        verdict,
                    });
                }));
            }
            routed => unreachable!("route_block_output keeps {routed:?} off the shell"),
        }
    }
}

/// Whether `envelope`'s coinbase commits its layout under `chain_id` at
/// `admitted_height` (bpv1-2, check-3).
pub(super) fn verify_envelope_commitment(
    network: &Network,
    admitted_height: Height,
    chain_id: [u8; 32],
    envelope: &MetaEnvelope,
) -> bool {
    let Some(header) = parse_header(&envelope.header_bytes) else {
        return false;
    };
    let fields = CommitmentFields {
        chain_id,
        profile_id: W2_PROFILE_ID,
        coding: envelope.coding,
        part_root: envelope.part_root,
        proposer_node: envelope.proposer_node,
    };
    verify_commitment(
        network,
        admitted_height,
        &header,
        &envelope.binding,
        &fields,
    )
    .is_ok()
}

/// Runs the checks on `envelope`, whose parent header is unknown, in their
/// order: the binding, height, and proof-of-work checks on a blocking thread,
/// because they decode and hash an untrusted coinbase before any proof of
/// work, then the target bound.
async fn check_unknown_parent(
    node: &SharedNode,
    envelope: Box<MetaEnvelope>,
    top: u32,
) -> UnknownParentVerdict {
    let network = node.network.clone();
    let checked = task::spawn_blocking(move || {
        let header = parse_header(&envelope.header_bytes).ok_or(UnknownParentVerdict::Invalid(
            Bpv1CloseRow::Row3InvalidHeader,
        ))?;
        let height = check_binding_and_height(&header, &envelope, top)?;
        let target = check_proof_of_work(&network, &header)?;
        Ok::<_, UnknownParentVerdict>((height, target))
    })
    .await;
    let (height, target) = match checked {
        Ok(Ok(checked)) => checked,
        Ok(Err(verdict)) => return verdict,
        // A check that never finished faults no peer: block sync owns the block.
        Err(_) => return UnknownParentVerdict::Drop,
    };
    let bound = if target_bound_applies(&node.network, height) {
        node.header_admission.target_bound().await
    } else {
        None
    };
    check_target_bound(&node.network, target, height, bound)
}

/// Cloneable control & proposer handle for a running [`Bpv1Service`].
#[derive(Clone, Debug)]
pub struct Bpv1Handle {
    node: Arc<SharedNode>,
}

impl Bpv1Handle {
    /// Publishes a solved block over BPv1 if its v6 coinbase holds a valid
    /// prepared commitment.
    ///
    /// Returns `true` when BPv1 publication started; `false` when the block has
    /// no matching commitment (e.g., pool-built coinbase -> existing relay).
    pub fn publish_solved_block(&self, block: &Block, height: u32) -> bool {
        // O(1) coinbase commitment pre-check BEFORE building
        // Merkle sibling paths or serializing block transactions so pool-built
        // coinbases and non-BPv1 solves incur virtually zero overhead.
        let Some(coinbase) = block.transactions.first() else {
            return false;
        };
        if extract_single_commitment(coinbase).is_err() {
            return false;
        }
        let Ok(cb_paths) = coinbase_paths(block) else {
            return false;
        };
        let Ok(header_bytes) = block.header.zcash_serialize_to_vec() else {
            return false;
        };
        let mut body_bytes = Vec::new();
        for tx in block.transactions.iter().skip(1) {
            if tx.zcash_serialize(&mut body_bytes).is_err() {
                return false;
            }
        }
        let Ok(tx_count) = u32::try_from(block.transactions.len()) else {
            return false;
        };
        let body_identity = compute_body_identity(tx_count, &body_bytes);
        let block_id = block.hash();
        let peers: Vec<NodeId> = self.node.lock_sessions().keys().copied().collect();
        let paths = self.node.lock_table().paths().clone();

        let step = self.node.lock_proposer().step(ProposerInput::Solved {
            block_id,
            height,
            header_bytes,
            coinbase_bytes: cb_paths.coinbase,
            txid_siblings: cb_paths.txid_siblings,
            auth_siblings: cb_paths.auth_siblings,
            body_identity,
            peers,
            paths: Box::new(paths),
        });
        for ev in &step.events {
            record_proposer_event(ev);
        }
        if step.outputs.is_empty() {
            return false;
        }

        // Execute outputs in order: `AdmitSolvedLocal` publishes `HeaderMeta` and
        // `BlockDone` to every peer before any `QueueSeeds`.
        self.node.run_proposer_outputs(step.outputs);
        true
    }

    /// Retires `block_id` if this node proposed it, because its local commit
    /// failed after [`Self::publish_solved_block`] started relaying it.
    ///
    /// Retirement drops the parts still queued to peers, ends their `Want`s,
    /// and keeps the block out of the replay to sessions that connect later.
    pub fn retire_failed_block(&self, block_id: block::Hash) {
        self.node
            .apply_block_input(BlockInput::ProposedCommitFailed { block_id });
    }

    /// Number of currently live BPv1 peer sessions.
    #[cfg(any(test, zakura_sim))]
    pub fn live_sessions(&self) -> usize {
        self.node.lock_sessions().len()
    }

    /// Current lifecycle phase of `block_id` in `BlockTable`, if known.
    #[cfg(any(test, zakura_sim))]
    pub fn block_phase(&self, block_id: &block::Hash) -> Option<super::blocks::BlockPhase> {
        self.node.lock_table().block_phase(block_id)
    }

    /// Current number of path entries tracked in `PeerPaths` (`bpv1-65`).
    #[cfg(any(test, zakura_sim))]
    pub fn path_entries(&self) -> usize {
        self.node.lock_table().paths().len()
    }

    /// Peer part bytes `BlockTable` charges against its part buffer budget.
    #[cfg(any(test, zakura_sim))]
    pub fn retained_part_bytes(&self) -> u64 {
        self.node.lock_table().part_buffer_bytes()
    }

    /// Prepares the commitment for a post-NU7 mining template (`decision-1`).
    /// A template whose body is already encoding
    /// waits for that encode instead of starting another.
    pub(crate) fn prepare_commitment(
        &self,
        light_client_root: [u8; 32],
        height: Height,
        tx_count: u32,
        body_after_coinbase: &[u8],
    ) -> Option<PreparedCommitment> {
        use zakura_chain::parameters::NetworkUpgrade;

        // decision-9: post-NU7 (v6 coinbase) templates only.
        if NetworkUpgrade::current(&self.node.network, height) < NetworkUpgrade::Nu7 {
            return None;
        }
        if self.node.lock_sessions().is_empty() {
            return None;
        }

        let template_id = TemplateId(self.node.next_template_id.fetch_add(1, Ordering::AcqRel));
        let body_identity = compute_body_identity(tx_count, body_after_coinbase);

        // Lock this call's results before the proposer step, so a template
        // that joins its encode waits until the encode ends.
        let results = EncodeResults::default();
        let mut own_results = lock(&results);
        let (step, joined) = {
            let mut proposer = self.node.lock_proposer();
            let step = proposer.step(ProposerInput::TemplateBuilt {
                template_id,
                body_identity,
                light_client_root,
            });
            let mut encodes = lock(&self.node.encodes);
            if step
                .outputs
                .iter()
                .any(|out| matches!(out, ProposerOutput::StartEncode { .. }))
            {
                encodes.insert(body_identity, Arc::clone(&results));
            }
            // A template that joined an in-flight encode gets no output now.
            let joined = if step.outputs.is_empty() {
                encodes.get(&body_identity).cloned()
            } else {
                None
            };
            (step, joined)
        };
        for ev in &step.events {
            record_proposer_event(ev);
        }
        if let Some(joined) = joined {
            drop(own_results);
            // The lock returns once the encoding caller has finished.
            return lock(&joined).remove(&template_id);
        }

        let mut own = None;
        for out in step.outputs {
            match out {
                ProposerOutput::SetCommitment { commitment, .. } => own = Some(commitment),
                ProposerOutput::StartEncode { work_id, .. } => {
                    let encoded = encode_body(&self.node.profile, body_after_coinbase, tx_count);
                    let done_step = encoded.ok().map(|encoded| {
                        self.node
                            .lock_proposer()
                            .step(ProposerInput::EncodeDone { work_id, encoded })
                    });
                    lock(&self.node.encodes).remove(&body_identity);
                    // A failed encode leaves the results empty, so joined
                    // templates get `None`.
                    let done_step = done_step?;
                    for ev in &done_step.events {
                        record_proposer_event(ev);
                    }
                    for out in done_step.outputs {
                        if let ProposerOutput::SetCommitment {
                            template_id: id,
                            commitment,
                        } = out
                        {
                            if id == template_id {
                                own = Some(commitment);
                            } else {
                                own_results.insert(id, commitment);
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        own
    }
}

impl zakura_node_services::bpv1_proposer::Bpv1ProposerHook for Bpv1Handle {
    fn should_prepare_template(&self, height: Height) -> bool {
        use zakura_chain::parameters::NetworkUpgrade;

        NetworkUpgrade::current(&self.node.network, height) >= NetworkUpgrade::Nu7
            && !self.node.lock_sessions().is_empty()
    }

    fn prepare_template_commitment(
        &self,
        light_client_root: [u8; 32],
        height: Height,
        tx_count: u32,
        body_after_coinbase: &[u8],
    ) -> Option<[u8; COMMITMENT_PAYLOAD_LEN]> {
        self.prepare_commitment(light_client_root, height, tx_count, body_after_coinbase)
            .map(|commitment| commitment_payload(commitment.commitment_hash))
    }

    fn empty_template_commitment(&self, height: Height) -> Option<[u8; COMMITMENT_PAYLOAD_LEN]> {
        if !self.should_prepare_template(height) {
            return None;
        }
        // An empty body has one transaction, the coinbase, and no bytes after it.
        let encoded = encode_body(&self.node.profile, &[], 1).ok()?;
        let commitment_hash = self
            .node
            .lock_proposer()
            .commitment_hash(encoded.coding, encoded.part_root);
        Some(commitment_payload(commitment_hash))
    }

    fn publish_solved_block(&self, block: Arc<Block>, height: Height) -> bool {
        Bpv1Handle::publish_solved_block(self, &block, height.0)
    }

    fn retire_failed_block(&self, hash: block::Hash) {
        Bpv1Handle::retire_failed_block(self, hash);
    }
}

/// Zakura v2 [`Service`] implementing BPv1 block propagation.
pub struct Bpv1Service {
    node: Arc<SharedNode>,
}

impl fmt::Debug for Bpv1Service {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Bpv1Service")
            .field("local_node", &self.node.local_node)
            .finish()
    }
}

impl Bpv1Service {
    /// Creates a new [`Bpv1Service`] and its control [`Bpv1Handle`], spawning the
    /// node-wide BPv1 driver task.
    pub fn start(
        config: ZakuraBpv1Config,
        have_pacing: HavePacing,
        profile: Profile,
        network: Network,
        local_node: NodeId,
        ports: Bpv1Ports,
        shutdown: CancellationToken,
    ) -> (Arc<Self>, Bpv1Handle) {
        let chain_id = network.genesis_hash().0;
        let (driver_tx, driver_rx) = mpsc::unbounded_channel();
        let node = Arc::new(SharedNode {
            table: Mutex::new(BlockTable::new(config.block_config(profile, have_pacing))),
            proposer: Mutex::new(ProposerState::new(ProposerConfig {
                direct_copies: config.proposer_direct_copies,
                ..ProposerConfig::new(chain_id, local_node)
            })),
            sessions: Mutex::new(HashMap::new()),
            header_admission: ports.header_admission,
            body_store: ports.body_store,
            verifier: ports.verifier,
            fallback: ports.fallback,
            verify_sem: Arc::new(Semaphore::new(config.verify_concurrency.max(1))),
            decode_sem: Arc::new(Semaphore::new(config.decode_concurrency.max(1))),
            config,
            profile,
            network,
            chain_id,
            local_node,
            next_session_gen: AtomicU64::new(1),
            driver_tx,
            shutdown,
            next_template_id: AtomicU64::new(1),
            encodes: Mutex::new(HashMap::new()),
        });

        let driver_node = Arc::clone(&node);
        drop(task::spawn_named(
            "bpv1-driver",
            run_bpv1_driver(driver_node, driver_rx, ports.committed_snapshots),
        ));

        let handle = Bpv1Handle {
            node: Arc::clone(&node),
        };
        (Arc::new(Self { node }), handle)
    }
}

/// Service identifier advertised in Zakura discovery records for BPv1.
pub fn bpv1_service_id() -> ZakuraServiceId {
    ZakuraServiceId::try_from("bpv1").expect("the bpv1 service id is non-empty bounded ASCII")
}

/// Starts a [`Bpv1Service`] for the endpoint whose node id is `local_node`
/// and returns it as a [`CustomService`] with its [`Bpv1Handle`]. Unflagged
/// `Have`s follow `have_pacing` (decision-26).
pub fn bpv1_custom_service(
    config: ZakuraBpv1Config,
    have_pacing: HavePacing,
    profile: Profile,
    network: Network,
    local_node: NodeId,
    ports: Bpv1Ports,
    shutdown: CancellationToken,
) -> (CustomService, Bpv1Handle) {
    let (service, handle) = Bpv1Service::start(
        config,
        have_pacing,
        profile,
        network,
        local_node,
        ports,
        shutdown,
    );
    let custom = CustomService {
        service,
        provides: vec![bpv1_service_id()],
        seeks: vec![bpv1_service_id()],
        tx_relay_sessions: None,
    };
    (custom, handle)
}

impl Service for Bpv1Service {
    fn name(&self) -> &'static str {
        "bpv1"
    }

    fn streams(&self) -> &[Stream] {
        &BPV1_STREAMS
    }

    fn session_policy(&self) -> SessionPolicy {
        SessionPolicy {
            opening: SessionOpening::EitherSide,
            reopen: true,
        }
    }

    fn wants_peer(
        &self,
        _peer: &ZakuraPeerId,
        _negotiated: u64,
        _direction: ServicePeerDirection,
    ) -> bool {
        self.node.lock_sessions().len() < self.node.config.max_sessions
    }

    fn owns_connection_for_peer(&self, peer: &ZakuraPeerId, conn_id: ZakuraConnId) -> bool {
        let Some(peer_node) = peer.node_id() else {
            return false;
        };
        self.node
            .lock_sessions()
            .get(&peer_node)
            .is_some_and(|s| s.conn_id == conn_id)
    }

    fn add_peer(&self, mut peer: Peer) {
        let Some(peer_node) = peer.id.node_id() else {
            return;
        };
        let Some((ctrl_recv, ctrl_send)) = peer.take_stream(BPV1_CONTROL_KIND) else {
            return;
        };
        let Some((data_recv, data_send)) = peer.take_stream(BPV1_DATA_KIND) else {
            return;
        };

        let conn_id = peer.conn_id;
        let peer_cancel = peer.cancel_token();
        let session_cancel = peer_cancel.child_token();
        let (inputs, inputs_rx) = mpsc::unbounded_channel();

        // The generation is taken under the sessions lock, so a peer's
        // sessions enter the map in generation order.
        let (session_gen, snapshot) = {
            let mut sessions = self.node.lock_sessions();
            if sessions.len() >= self.node.config.max_sessions && !sessions.contains_key(&peer_node)
            {
                session_cancel.cancel();
                return;
            }
            let session_gen = self.node.next_session_gen.fetch_add(1, Ordering::AcqRel);
            let snapshot = Arc::new(Mutex::new(PullPeerState {
                peer: peer_node,
                session_gen,
                ahead_bytes: 0,
                can_pull: true,
                window_remaining: PULL_WINDOW,
            }));
            let live = LiveSession {
                conn_id,
                session_gen,
                inputs,
                cancel: session_cancel.clone(),
                connection: peer_cancel.clone(),
                load: peer.load().cloned(),
                snapshot: snapshot.clone(),
            };
            if let Some(prev) = sessions.insert(peer_node, live) {
                prev.cancel.cancel();
            }
            (session_gen, snapshot)
        };

        let node = Arc::clone(&self.node);
        // Audit crossing #5: replay currently active admitted blocks so a newly joined
        // or reconnected session learns in-flight blocks immediately.
        let _ = self.node.driver_tx.send(DriverMsg::ReplayActiveToPeer {
            peer_node,
            session_gen,
        });
        drop(task::spawn_named(
            "bpv1-peer-routine",
            run_peer_routine(
                node,
                conn_id,
                peer_node,
                session_gen,
                ctrl_recv,
                ctrl_send,
                data_recv,
                data_send,
                inputs_rx,
                snapshot,
                session_cancel,
                peer_cancel,
            ),
        ));
    }

    fn remove_peer(&self, peer: &ZakuraPeerId, conn_id: ZakuraConnId) {
        let Some(peer_node) = peer.node_id() else {
            return;
        };
        let removed = {
            let mut sessions = self.node.lock_sessions();
            if sessions
                .get(&peer_node)
                .is_some_and(|s| s.conn_id == conn_id)
            {
                sessions.remove(&peer_node)
            } else {
                None
            }
        };
        if let Some(sess) = removed {
            sess.cancel.cancel();
        }
    }
}

// ============================================================================
// Node-wide BPv1 Driver Task
// ============================================================================

/// Converts a change of committed header-chain frontiers into `BlockTable`
/// inputs.
///
/// A new verified tip moves the window and ends a fallback block that
/// native block sync committed. A new best header wakes the metadata
/// pending on it, and the metadata for the header itself when the snapshot
/// skipped its parent. The watch coalesces, so other metadata whose
/// parent a snapshot skipped ages out instead.
pub(crate) fn frontier_inputs(last: Option<FrontierSet>, next: FrontierSet) -> Vec<BlockInput> {
    let mut inputs = Vec::new();
    if last.map(|last| last.verified_best) != Some(next.verified_best) {
        inputs.push(BlockInput::TipHeight {
            height: next.verified_best.height.0,
            hash: next.verified_best.hash,
        });
        inputs.push(BlockInput::FallbackDone {
            block_id: next.verified_best.hash,
        });
    }
    if last.map(|last| last.header_best) != Some(next.header_best) {
        inputs.push(BlockInput::ParentAdmitted {
            hash: next.header_best.hash,
            height: next.header_best.height.0,
        });
    }
    inputs
}

/// Waits for the next committed snapshot's frontiers. Pends forever once the
/// snapshot sender is gone.
async fn next_frontiers(
    snapshots: &mut watch::Receiver<Option<EngineSnapshot>>,
) -> Option<FrontierSet> {
    if snapshots.changed().await.is_err() {
        return std::future::pending().await;
    }
    snapshots
        .borrow_and_update()
        .as_ref()
        .map(|snapshot| snapshot.frontiers)
}

async fn run_bpv1_driver(
    node: Arc<SharedNode>,
    mut rx: mpsc::UnboundedReceiver<DriverMsg>,
    mut snapshots: watch::Receiver<Option<EngineSnapshot>>,
) {
    let mut path_interval = time::interval(PATH_POLL_INTERVAL);
    let mut frontiers = None;
    let mut apply_frontiers = |next: Option<FrontierSet>| {
        let Some(next) = next else { return };
        for input in frontier_inputs(frontiers.replace(next), next) {
            node.apply_block_input(input);
        }
    };
    apply_frontiers(
        snapshots
            .borrow_and_update()
            .as_ref()
            .map(|snapshot| snapshot.frontiers),
    );

    loop {
        let next_tick_at = node.lock_table().next_deadline();
        let tick_sleep = async move {
            match next_tick_at {
                Some(deadline) => time::sleep_until(deadline).await,
                None => std::future::pending().await,
            }
        };
        zakura_rt::pin!(tick_sleep);

        zakura_rt::select! {
            _ = node.shutdown.cancelled() => break,
            msg = rx.recv() => {
                let Some(msg) = msg else { break };
                match msg {
                    DriverMsg::BlockInput(input) => node.apply_block_input(input),
                    DriverMsg::ReplayActiveToPeer {
                        peer_node,
                        session_gen,
                    } => {
                        let inputs = replay_admitted(&node.lock_table());
                        for input in inputs {
                            let _ = node.send_to(&peer_node, Some(session_gen), input);
                        }
                    }
                    DriverMsg::SessionEnded {
                        peer_node,
                        session_gen,
                    } => {
                        // Hold `sessions` while the table ends the session.
                        // Another session of the peer that is already live
                        // shows in `peer_states` and keeps the node's facts
                        // (bpv1-89). One that opens later registers after
                        // this end, so the end never clears what it rebuilt.
                        let step = {
                            let sessions = node.lock_sessions();
                            let peer_states =
                                sessions.values().map(|s| *lock(&s.snapshot)).collect();
                            node.lock_table().step(
                                Instant::now(),
                                BlockInput::SessionEnded {
                                    peer: peer_node,
                                    session_gen,
                                    peer_states,
                                },
                            )
                        };
                        node.dispatch_block_step(step);
                    }
                    DriverMsg::WakeTick => {}
                }
            }
            next = next_frontiers(&mut snapshots) => apply_frontiers(next),
            _ = &mut tick_sleep => {
                let states = node.collect_peer_states();
                node.apply_block_input(BlockInput::Tick { peer_states: states });
            }
            _ = path_interval.tick() => {
                // Hold `sessions` while updating `table` so `PathObserved` can never
                // race past a peer's `SessionEnded` cleanup (`bpv1-65`).
                let steps: Vec<BlockStep> = {
                    let sessions = node.lock_sessions();
                    let mut table = node.lock_table();
                    let now = Instant::now();
                    let mut out = Vec::new();
                    for (peer, live) in sessions.iter() {
                        if let Some(load) = &live.load {
                            let estimate = conn_load_to_path_estimate(&load.borrow());
                            out.push(table.step(
                                now,
                                BlockInput::PathObserved {
                                    peer: *peer,
                                    estimate,
                                },
                            ));
                        }
                    }
                    metrics::gauge!("zakura.p2p.bpv1.path.entries")
                        .set(table.paths().len() as f64);
                    metrics::gauge!("zakura.p2p.bpv1.path.unmeasured_peers")
                        .set(table.paths().unmeasured_pull_peers() as f64);
                    out
                };
                for step in steps {
                    node.dispatch_block_step(step);
                }
            }
        }
    }
}

// ============================================================================
// Per-Session Peer Routine Task
// ============================================================================

/// One live BPv1 session: its [`PeerState`], its two stream writers, and the
/// IO that runs the machine's outputs. Every input goes through
/// [`Session::apply`].
struct Session {
    node: Arc<SharedNode>,
    state: PeerState,
    peer_node: NodeId,
    ctrl: ControlWriter,
    data: DataWriter,
    verify_tx: mpsc::UnboundedSender<(PartToken, Option<VerifiedPart>)>,
    pulls: PullTelemetry,
    pushes: PushTelemetry,
}

/// When this node wrote its pushed parts to the session's peer, per block
///: a holder's push, or the proposer's flagged
/// seeds. A row at the first pushed part written and a row when the push
/// ends give the observed push finish.
#[derive(Default)]
struct PushTelemetry {
    blocks: HashMap<block::Hash, PushWrites>,
}

/// One block's pushed-part writes to one peer.
struct PushWrites {
    first: Instant,
    last: Instant,
    parts: u32,
}

impl PushTelemetry {
    fn written(&mut self, block_id: block::Hash, peer: &NodeId, now: Instant) {
        let writes = self.blocks.entry(block_id).or_insert_with(|| {
            tracing::debug!(?block_id, ?peer, "bpv1 push first part written");
            PushWrites {
                first: now,
                last: now,
                parts: 0,
            }
        });
        writes.last = now;
        writes.parts = writes.parts.saturating_add(1);
    }

    /// Logs the trace row of this node's push of `block_id` to `peer`:
    /// the parts it wrote and the last write after the first.
    fn finish(&mut self, block_id: block::Hash, peer: &NodeId) {
        if let Some(writes) = self.blocks.remove(&block_id) {
            tracing::debug!(
                ?block_id,
                ?peer,
                parts = writes.parts,
                last_after_first_ms = writes
                    .last
                    .saturating_duration_since(writes.first)
                    .as_millis(),
                "bpv1 push last part written"
            );
        }
    }
}

/// Open pull timings of one session, for `arrival_error.seconds` and the pull trace.
#[derive(Default)]
struct PullTelemetry {
    active: HashMap<u32, ActivePullTelemetry>,
    /// Arrival estimate of the grant whose `Want` the machine sends next.
    granted_arrival: Option<Arrival>,
}

impl Session {
    /// Steps the machine with `input` and runs its outputs. Returns `false`
    /// once the session must end.
    async fn apply(&mut self, now: Instant, input: PeerInput) -> bool {
        if let PeerInput::PullGranted(grant) = &input {
            self.pulls.granted_arrival = Some(grant.arrival);
        }
        let step = self.state.step(now, input);
        self.execute(now, step).await
    }

    /// Records `step`'s events and runs its outputs in order, including the
    /// outputs of the serve steps that a `ServeWant` takes.
    async fn execute(&mut self, now: Instant, step: PeerStep) -> bool {
        self.observe(now, &step);
        let mut work: VecDeque<PeerOutput> = step.outputs.into();
        while let Some(output) = work.pop_front() {
            match output {
                PeerOutput::ServeWant {
                    want_id,
                    block_id,
                    stripe,
                    eligible_slots,
                } => {
                    let sent = self.serve_want(want_id, block_id, stripe, eligible_slots);
                    let done = self.state.step(
                        Instant::now(),
                        PeerInput::ServeDone(ServeDone {
                            want_id,
                            sent,
                            reason: WantEndReason::Served,
                        }),
                    );
                    self.observe(now, &done);
                    work.extend(done.outputs);
                }
                other => {
                    if !self.run(other).await {
                        return false;
                    }
                }
            }
        }
        true
    }

    /// Queues the held parts of `eligible_slots` for `want_id` and returns how
    /// many it queued.
    fn serve_want(
        &mut self,
        want_id: u32,
        block_id: block::Hash,
        stripe: u32,
        eligible_slots: Vec<u32>,
    ) -> u16 {
        let parts = held_parts(&self.node.lock_table(), &block_id, stripe, &eligible_slots);
        // A `Want` names at most `n` slots of one stripe, so the count fits.
        let sent = parts.len() as u16;
        for part in parts {
            let index = part.index();
            self.data.push(DataQueueItem::Part {
                block_id,
                stripe,
                part,
                push: false,
            });
            // A queued part only marks the send-once record; it emits nothing.
            let _ = self.state.step(
                Instant::now(),
                PeerInput::ServePartQueued(ServePartQueued {
                    want_id,
                    block_id,
                    index,
                }),
            );
        }
        sent
    }

    /// Runs one machine output other than `ServeWant`. Returns `false` once
    /// the session must end.
    async fn run(&mut self, output: PeerOutput) -> bool {
        let node = &self.node;
        let peer = self.peer_node;
        let session_gen = self.state.session_gen();
        let output =
            match route_peer_output(peer, session_gen, output, || node.collect_peer_states()) {
                // The driver is the sole ordered executor of `BlockTable`
                // transitions and their effects.
                PeerRoute::Table(input) => {
                    node.drive(input);
                    return true;
                }
                PeerRoute::Session(output) => output,
            };
        match output {
            PeerOutput::SendControl(msg) => {
                if let Some(block_id) = header_meta_trace_id(&msg) {
                    tracing::debug!(target: "bpv1_stage", %block_id, ?peer,
                        session_gen = self.state.session_gen(), "header_meta_send_started");
                }
                self.ctrl.push(msg);
            }
            PeerOutput::SendHave {
                block_id,
                flags,
                indices,
            } => {
                self.data.push(DataQueueItem::Have {
                    block_id,
                    flags,
                    indices,
                });
            }
            PeerOutput::PushPart { block_id, index } => {
                // The flagged `Have` is already queued, so the part follows it
                // (bpv1-72).
                let held = {
                    let table = node.lock_table();
                    table
                        .admitted_descriptor(&block_id)
                        .and_then(|(c, _)| c.split_index(index))
                        .zip(table.get_part(&block_id, index).cloned())
                };
                if let Some(((stripe, _), part)) = held {
                    if tracing::enabled!(target: "bpv1_stage", tracing::Level::DEBUG) {
                        let path = node
                            .lock_sessions()
                            .get(&peer)
                            .filter(|live| live.session_gen == self.state.session_gen())
                            .and_then(|live| live.load.as_ref())
                            .map(|load| conn_load_to_path_estimate(&load.borrow()));
                        tracing::debug!(target: "bpv1_stage", %block_id, ?peer, index, ?path,
                            "holder_push_queued");
                    }
                    self.data.push(DataQueueItem::Part {
                        block_id,
                        stripe,
                        part,
                        push: true,
                    });
                }
            }
            PeerOutput::SendSeedPart { block_id, part } => {
                let stripe = node
                    .lock_table()
                    .admitted_descriptor(&block_id)
                    .and_then(|(c, _)| c.split_index(part.index()))
                    .map(|(s, _)| s)
                    .unwrap_or(0);
                // The flagged `Have` is already queued, so the seed follows
                // it (bpv1-72).
                self.data.push(DataQueueItem::Part {
                    block_id,
                    stripe,
                    part,
                    push: true,
                });
            }
            PeerOutput::SendWantEnd {
                want_id,
                sent,
                reason,
            } => {
                // `WantEnd` stays behind the parts it answered (bpv1-29).
                self.data.push(DataQueueItem::WantEnd {
                    want_id,
                    sent,
                    reason,
                });
            }
            PeerOutput::ServeWant { .. } => unreachable!("Session::execute serves wants"),
            PeerOutput::SubmitHeaderMeta { block_id, message } => {
                if let Some(env) = decode_meta_envelope(block_id, *message) {
                    // The table checks its metadata caps before this session
                    // reads its next frame, so a flood waits in the sender's
                    // stream window instead of the driver's queue.
                    node.apply_block_input(BlockInput::MetaReceived {
                        envelope: env,
                        from_peer: peer,
                        session_gen: self.state.session_gen(),
                    });
                } else {
                    tracing::debug!(target: "bpv1_stage", %block_id, ?peer,
                        session_gen = self.state.session_gen(), "header_meta_envelope_rejected");
                }
            }
            PeerOutput::VerifyPart { token, payload } => {
                // Charge the part buffer before proof and take
                // `(coding, part_root)` from the block table's admitted descriptor.
                // usize -> u64 widens on every supported target.
                let bytes = payload.len() as u64;
                let charge =
                    charge_part(&mut node.lock_table(), Instant::now(), peer, token, bytes);
                let (coding, part_root) = match charge {
                    Ok(descriptor) => descriptor,
                    // A refusal only clears the attempt and records the drop.
                    Err(refused) => {
                        let now = Instant::now();
                        let step = self.state.step(now, refused);
                        self.observe(now, &step);
                        return true;
                    }
                };
                let sem = node.verify_sem.clone();
                let tx = self.verify_tx.clone();
                let release_tx = node.driver_tx.clone();
                drop(task::spawn_named("bpv1-verify-part", async move {
                    let Ok(_permit) = sem.acquire_owned().await else {
                        return;
                    };
                    let verified =
                        parse_and_verify_part_frame(&payload, &coding, part_root, token.index);
                    tracing::debug!(target: "bpv1_stage", block_id = %token.block_id, ?peer,
                        index = token.index, session_gen = token.session_gen,
                        ok = verified.is_some(), "part_proof_finished");
                    finish_part_proof(&tx, &release_tx, peer, token, verified);
                }));
            }
            PeerOutput::CancelQueuedPush { block_id, index } => {
                let cancelled = self.data.queue.cancel_push(block_id, index);
                tracing::debug!(target: "bpv1_stage", %block_id, ?peer, index, cancelled,
                    "queued_push_cancelled");
            }
            PeerOutput::CancelQueuedStripe { block_id, stripe } => {
                let cancelled = self.data.queue.cancel_stripe(block_id, stripe);
                tracing::debug!(target: "bpv1_stage", %block_id, ?peer, stripe, cancelled,
                    "queued_stripe_cancelled");
            }
            PeerOutput::PurgeQueuedBlock {
                block_id,
                include_haves,
            } => {
                self.data.queue.purge_block(block_id, include_haves);
                // The peer finished the block or it retired: this holder's
                // push to the peer is over.
                self.pushes.finish(block_id, &self.peer_node);
            }
            PeerOutput::PauseDataReads => {
                metrics::gauge!("zakura.p2p.bpv1.early.paused_peers").increment(1.0);
            }
            PeerOutput::ResumeDataReads => {
                metrics::gauge!("zakura.p2p.bpv1.early.paused_peers").decrement(1.0);
            }
            PeerOutput::CloseProtocol(cause) => {
                // Loop teardown cancels the connection uniformly.
                record_close_metric(cause);
                return false;
            }
            PeerOutput::CloseNeutralStall => {
                metrics::counter!("zakura.p2p.bpv1.session.stall_closed").increment(1);
                return false;
            }
            routed => unreachable!("route_peer_output sends {routed:?} to the table"),
        }
        true
    }

    /// Records the frame the data writer just handed to the transport and
    /// starts the next one.
    async fn written(&mut self, written: Written) -> bool {
        let conn_id = self.state.session_gen();
        match written {
            Written::Part {
                block_id,
                index,
                bytes,
                push,
            } => {
                tracing::debug!(target: "bpv1_stage", %block_id, peer = ?self.peer_node,
                    index, bytes, push, session_gen = conn_id, "part_written");
                self.node
                    .drive(BlockInput::PartUploaded { block_id, bytes });
                if push {
                    self.pushes
                        .written(block_id, &self.peer_node, Instant::now());
                }
            }
            Written::Have {
                block_id,
                flags,
                indices,
            } => {
                tracing::debug!(target: "bpv1_stage", %block_id, peer = ?self.peer_node,
                    push = flags.push(), relay = flags.relay(), ?indices,
                    session_gen = conn_id, "have_written");
            }
            Written::WantEnd => {
                if !self.apply(Instant::now(), PeerInput::WantEndWritten).await {
                    return false;
                }
            }
        }
        self.data.start();
        true
    }
}

/// What the data writer just handed to the transport.
#[derive(Clone, Debug)]
pub(super) enum Written {
    Part {
        block_id: block::Hash,
        index: u32,
        bytes: u64,
        /// A part behind a flagged `Have`: a holder push or a proposer seed
        /// (decision-24).
        push: bool,
    },
    Have {
        block_id: block::Hash,
        flags: HaveFlags,
        indices: Vec<u32>,
    },
    WantEnd,
}

impl Written {
    fn of(item: DataQueueItem) -> Self {
        match item {
            DataQueueItem::Part {
                block_id,
                part,
                push,
                ..
            } => Written::Part {
                block_id,
                index: part.index(),
                // usize -> u64 widens on every supported target.
                bytes: part.wire_payload().len() as u64,
                push,
            },
            DataQueueItem::Have {
                block_id,
                flags,
                indices,
            } => Written::Have {
                block_id,
                flags,
                indices,
            },
            DataQueueItem::WantEnd { .. } => Written::WantEnd,
        }
    }
}

/// The data stream's one writer (decision-7).
///
/// Items wait unencoded in a FIFO. The writer encodes the head only after the
/// previous frame entered the transport's queue, so the transport's output
/// gate bounds this session's encoded data bytes to the one frame waiting for
/// its grant. A full gate makes the queue wait; nothing is dropped.
/// The session's routine drives the write in its own `select!`, so a blocked
/// write never stalls control reads, timers, or shutdown (`bpv1-54`).
/// The `bpv1-header-meta-want` stream's FIFO writer (decision-7). The session
/// loop polls it, so a peer that stops reading control stops only this
/// writer. A frame that waits [`stall_timeout`] for its bytes ends the session.
struct ControlWriter {
    send: FramedSend,
    queue: VecDeque<Box<Bpv1Message>>,
    in_flight: Option<ControlWrite>,
}

struct ControlWrite {
    msg: Box<Bpv1Message>,
    stall_at: Instant,
    write: BoxFuture<'static, bool>,
}

impl ControlWriter {
    fn new(send: FramedSend) -> Self {
        Self {
            send,
            queue: VecDeque::new(),
            in_flight: None,
        }
    }

    /// Queues `msg` behind every queued message and starts writing if idle.
    fn push(&mut self, msg: Box<Bpv1Message>) {
        self.queue.push_back(msg);
        self.start();
    }

    /// Starts writing the head of the queue unless a frame is in flight.
    fn start(&mut self) {
        if self.in_flight.is_some() {
            return;
        }
        while let Some(msg) = self.queue.pop_front() {
            let Ok(frame) = encode_frame(msg.as_ref()) else {
                continue;
            };
            // Widening usize to u64 is lossless on supported targets.
            let stall_at = Instant::now() + stall_timeout(frame.payload.len() as u64);
            let send = self.send.clone();
            let write = Box::pin(async move { send.send(frame).await.is_ok() });
            self.in_flight = Some(ControlWrite {
                msg,
                stall_at,
                write,
            });
            return;
        }
    }

    fn is_writing(&self) -> bool {
        self.in_flight.is_some()
    }

    /// When the frame in flight stalls, if one is in flight.
    fn stall_at(&self) -> Option<Instant> {
        self.in_flight.as_ref().map(|write| write.stall_at)
    }

    /// Waits until the frame in flight enters the transport's queue. Returns
    /// its message, or `None` once the stream closed. Cancel-safe: the frame
    /// stays in flight until this completes.
    async fn written(&mut self) -> Option<Box<Bpv1Message>> {
        let ok = (&mut self.in_flight.as_mut()?.write).await;
        let write = self.in_flight.take()?;
        ok.then_some(write.msg)
    }
}

/// Hands a proof result to its session. A failed proof, or a result its ended
/// session never reads, releases the part's charge; a delivered part's insert
/// takes the charge instead.
fn finish_part_proof(
    session: &mpsc::UnboundedSender<(PartToken, Option<VerifiedPart>)>,
    driver: &mpsc::UnboundedSender<DriverMsg>,
    peer: NodeId,
    token: PartToken,
    verified: Option<VerifiedPart>,
) {
    let failed = verified.is_none();
    if session.send((token, verified)).is_err() || failed {
        let _ = driver.send(DriverMsg::BlockInput(BlockInput::ReleasePartCharge {
            block_id: token.block_id,
            index: token.index,
            from_peer: peer,
            session_gen: token.session_gen,
        }));
    }
}

/// The block a `HeaderMeta` names, when stage tracing is on.
fn header_meta_trace_id(msg: &Bpv1Message) -> Option<block::Hash> {
    match msg {
        Bpv1Message::HeaderMeta { header, .. } if tracing::enabled!(target: "bpv1_stage", tracing::Level::DEBUG) => {
            parse_header(header).map(|parsed| parsed.hash())
        }
        _ => None,
    }
}

/// Traces a control message the transport just queued.
fn trace_control_written(msg: &Bpv1Message, peer: NodeId, session_gen: u64) {
    match msg {
        Bpv1Message::HeaderMeta { .. } => {
            if let Some(block_id) = header_meta_trace_id(msg) {
                tracing::debug!(target: "bpv1_stage", %block_id, ?peer, session_gen,
                    "header_meta_written");
            }
        }
        Bpv1Message::Want {
            block_id,
            want_id,
            stripe,
            requested,
        } => {
            tracing::debug!(target: "bpv1_stage", %block_id, ?peer, want_id, stripe,
                ?requested, session_gen, "want_written");
        }
        _ => {}
    }
}

pub(super) struct DataWriter {
    send: FramedSend,
    pub(super) queue: Bpv1FifoQueue,
    in_flight: Option<(Written, BoxFuture<'static, bool>)>,
}

impl DataWriter {
    pub(super) fn new(send: FramedSend) -> Self {
        Self {
            send,
            queue: Bpv1FifoQueue::default(),
            in_flight: None,
        }
    }

    /// Queues `item` behind every queued item and starts writing if idle.
    pub(super) fn push(&mut self, item: DataQueueItem) {
        self.queue.push_back(item);
        self.start();
    }

    /// Starts writing the head of the queue unless a frame is in flight.
    pub(super) fn start(&mut self) {
        if self.in_flight.is_some() {
            return;
        }
        while let Some(item) = self.queue.pop_front() {
            let Some(frame) = item.encode() else {
                continue;
            };
            let written = Written::of(item);
            let send = self.send.clone();
            let write = async move {
                match send.try_send(frame) {
                    Ok(()) => true,
                    Err(mpsc::error::TrySendError::Closed(_)) => false,
                    Err(mpsc::error::TrySendError::Full(frame)) => {
                        // The output gate is full: wait for it, never drop.
                        metrics::counter!("zakura.p2p.bpv1.data.write.waits").increment(1);
                        tracing::trace!("bpv1 data writer waits for the output gate");
                        send.send(frame).await.is_ok()
                    }
                }
            };
            self.in_flight = Some((written, Box::pin(write)));
            return;
        }
    }

    pub(super) fn is_writing(&self) -> bool {
        self.in_flight.is_some()
    }

    /// Waits until the frame in flight enters the transport's queue. Returns
    /// what it wrote, or `None` once the stream closed. Cancel-safe: the
    /// frame stays in flight until this completes.
    pub(super) async fn written(&mut self) -> Option<Written> {
        let (_, write) = self.in_flight.as_mut()?;
        let ok = write.await;
        let (written, _) = self.in_flight.take()?;
        ok.then_some(written)
    }
}

/// Decodes one control-stream frame into its machine input, or the close row
/// it violates (`bpv1-61`).
pub(super) fn control_input(frame: &Frame) -> Result<PeerInput, Bpv1CloseRow> {
    let msg = decode_frame::<Bpv1Message>(frame).map_err(|_| Bpv1CloseRow::Row2Decode)?;
    Ok(match msg {
        Bpv1Message::HeaderMeta { ref header, .. } => {
            let block_id = parse_header(header).ok_or(Bpv1CloseRow::Row2Decode)?.hash();
            PeerInput::RecvHeaderMeta {
                block_id,
                message: Box::new(msg),
            }
        }
        Bpv1Message::Want {
            want_id,
            block_id,
            stripe,
            requested,
        } => PeerInput::RecvWant(RecvWant {
            want_id,
            block_id,
            stripe,
            requested,
        }),
        Bpv1Message::StripeDone { block_id, stripe } => {
            PeerInput::RecvStripeDone { block_id, stripe }
        }
        Bpv1Message::BlockDone { block_id } => PeerInput::RecvBlockDone { block_id },
        _ => return Err(Bpv1CloseRow::Row1FrameHeader),
    })
}

/// Decodes one data-stream frame into its machine input, or the close row it
/// violates (`bpv1-61`). A `Part` keeps the received payload; only proof
/// verification copies it into a verified part.
pub(super) fn data_input(frame: Frame) -> Result<PeerInput, Bpv1CloseRow> {
    Ok(match frame.message_type {
        message_type::PART => {
            // bpv1-13: inspect only the 36-byte `(block_id, index)` prefix first.
            let prefix = read_part_prefix(&frame.payload).map_err(|_| Bpv1CloseRow::Row2Decode)?;
            PeerInput::RecvPartPrefix(RecvPartPrefix {
                block_id: prefix.block_id,
                index: prefix.index,
                payload: frame.payload,
            })
        }
        message_type::HAVE | message_type::WANT_END => match decode_frame::<Bpv1Message>(&frame) {
            Ok(Bpv1Message::Have {
                block_id,
                flags,
                indices,
            }) => PeerInput::RecvHave(RecvHave {
                block_id,
                flags,
                indices,
                frame_bytes: frame.payload.len(),
            }),
            Ok(Bpv1Message::WantEnd { want_id, sent, .. }) => {
                PeerInput::RecvWantEnd(RecvWantEnd { want_id, sent })
            }
            // A `Have` flags byte other than none, push, or push and relay is
            // a reserved flag (decision-24).
            Err(WireError::ReservedFlags(_)) => return Err(Bpv1CloseRow::Row1FrameHeader),
            _ => return Err(Bpv1CloseRow::Row2Decode),
        },
        _ => return Err(Bpv1CloseRow::Row1FrameHeader),
    })
}

#[allow(clippy::too_many_arguments)]
async fn run_peer_routine(
    node: Arc<SharedNode>,
    conn_id: ZakuraConnId,
    peer_node: NodeId,
    session_gen: u64,
    mut ctrl_recv: FramedRecv,
    ctrl_send: FramedSend,
    mut data_recv: FramedRecv,
    data_send: FramedSend,
    mut inputs: mpsc::UnboundedReceiver<PeerInput>,
    snapshot: Arc<Mutex<PullPeerState>>,
    session_cancel: CancellationToken,
    peer_cancel: CancellationToken,
) {
    let peer_cfg = node.config.peer_config(node.profile);
    let (verify_tx, mut verify_rx) = mpsc::unbounded_channel::<(PartToken, Option<VerifiedPart>)>();
    let mut session = Session {
        state: PeerState::new(peer_cfg, peer_node, session_gen),
        node: Arc::clone(&node),
        peer_node,
        ctrl: ControlWriter::new(ctrl_send),
        data: DataWriter::new(data_send),
        verify_tx,
        pulls: PullTelemetry::default(),
        pushes: PushTelemetry::default(),
    };

    loop {
        // Update the pull-capacity snapshot `BlockTable::plan_stripe` reads.
        *lock(&snapshot) = pull_state(&session.state, Instant::now());
        if session.state.is_closed() || session_cancel.is_cancelled() {
            break;
        }
        // The proposer's direct copies keep an otherwise idle upload busy
        // (decision-19). The writer admits each batch only once everything
        // queued before it is in the transport's queue.
        if !session.data.is_writing() && session.data.queue.is_empty() && session.state.has_copies()
        {
            if !session.apply(Instant::now(), PeerInput::DataIdle).await {
                break;
            }
            continue;
        }

        let data_paused = session.state.is_data_paused();
        let next_tick_at = session.state.next_deadline();
        let tick_sleep = async move {
            match next_tick_at {
                Some(deadline) => time::sleep_until(deadline).await,
                None => std::future::pending().await,
            }
        };
        zakura_rt::pin!(tick_sleep);
        let ctrl_stall_at = session.ctrl.stall_at();
        let ctrl_stall = async move {
            match ctrl_stall_at {
                Some(at) => time::sleep_until(at).await,
                None => std::future::pending().await,
            }
        };
        zakura_rt::pin!(ctrl_stall);

        let input = zakura_rt::select! {
            _ = node.shutdown.cancelled() => break,
            _ = session_cancel.cancelled() => break,
            Some(input) = inputs.recv() => input,
            Some((token, verified)) = verify_rx.recv() => match verified {
                Some(part) => PeerInput::PartVerified { token, part },
                None => PeerInput::PartFailed { token },
            },
            frame = ctrl_recv.recv() => {
                let Some(frame) = frame else { break };
                match control_input(&frame) {
                    Ok(input @ PeerInput::RecvHeaderMeta { block_id, .. }) => {
                        tracing::debug!(target: "bpv1_stage", %block_id, peer = ?peer_node,
                            session_gen, "header_meta_read");
                        input
                    }
                    Ok(PeerInput::RecvWant(want)) => {
                        tracing::debug!(target: "bpv1_stage", block_id = %want.block_id,
                            peer = ?peer_node, want_id = want.want_id, stripe = want.stripe,
                            requested = ?want.requested, session_gen, "want_read");
                        PeerInput::RecvWant(want)
                    }
                    Ok(input) => input,
                    Err(row) => {
                        record_close_metric(row.cause(BPV1_CONTROL_KIND));
                        peer_cancel.cancel();
                        break;
                    }
                }
            }
            frame = data_recv.recv(), if !data_paused => {
                let Some(frame) = frame else { break };
                match data_input(frame) {
                    Ok(PeerInput::RecvPartPrefix(part)) => {
                        tracing::debug!(target: "bpv1_stage", block_id = %part.block_id,
                            peer = ?peer_node, index = part.index, session_gen, "part_frame_read");
                        PeerInput::RecvPartPrefix(part)
                    }
                    Ok(PeerInput::RecvHave(have)) => {
                        tracing::debug!(target: "bpv1_stage", block_id = %have.block_id,
                            peer = ?peer_node, push = have.flags.push(), relay = have.flags.relay(),
                            indices = ?have.indices,
                            session_gen, "have_read");
                        PeerInput::RecvHave(have)
                    }
                    Ok(input) => input,
                    Err(row) => {
                        record_close_metric(row.cause(BPV1_DATA_KIND));
                        peer_cancel.cancel();
                        break;
                    }
                }
            }
            written = session.ctrl.written(), if session.ctrl.is_writing() => {
                let Some(msg) = written else { break };
                trace_control_written(&msg, peer_node, session_gen);
                session.ctrl.start();
                continue;
            }
            _ = &mut ctrl_stall => PeerInput::ControlStalled,
            written = session.data.written(), if session.data.is_writing() => {
                let Some(written) = written else { break };
                if !session.written(written).await {
                    break;
                }
                continue;
            }
            _ = &mut tick_sleep => PeerInput::Tick,
        };
        if !session.apply(Instant::now(), input).await {
            break;
        }
    }

    // Unified close & teardown (`bpv1-62`):
    // cancel peer on protocol close or repeat neutral stall, stop & join data writer,
    // and emit `SessionEnded`.
    if session.state.close_cause().is_some() || session.state.is_closed_neutral() {
        peer_cancel.cancel();
    }
    session_cancel.cancel();

    // Proof results this session will never insert release their part charges.
    // Closing first sends every later result back to its worker, which
    // releases its own charge.
    verify_rx.close();
    while let Ok((token, verified)) = verify_rx.try_recv() {
        if verified.is_some() {
            node.drive(BlockInput::ReleasePartCharge {
                block_id: token.block_id,
                index: token.index,
                from_peer: peer_node,
                session_gen: token.session_gen,
            });
        }
    }

    let end_step = session.state.step(Instant::now(), PeerInput::SessionEnd);
    for ev in &end_step.events {
        record_peer_event(ev, &peer_node);
    }
    // `remove_peer` may have taken this session's entry first, as the handler's
    // disconnect does. Its state in the table is still this session's to end.
    node.drive(BlockInput::MetaSessionEnded {
        peer: peer_node,
        session_gen,
    });
    {
        let mut sessions = node.lock_sessions();
        if sessions
            .get(&peer_node)
            .is_some_and(|s| s.conn_id == conn_id && s.session_gen == session_gen)
        {
            sessions.remove(&peer_node);
        }
    }
    // `SessionEnded` releases every claim and pending push of this session,
    // including the claims of grants still queued in its inbox.
    let _ = node.driver_tx.send(DriverMsg::SessionEnded {
        peer_node,
        session_gen,
    });
}

/// Parses a `Part` frame payload and verifies its proof for `expected_index`
/// under `part_root` (bpv1-12). Returns `None` for any malformed or unproven
/// frame.
pub(super) fn parse_and_verify_part_frame(
    payload: &[u8],
    coding: &Coding,
    part_root: [u8; 32],
    expected_index: u32,
) -> Option<VerifiedPart> {
    // Format: block_id(32) || index(4) || depth(1) || siblings(32*depth) || payload(wire_len(index))
    let mut reader = BoundedReader::new(payload);
    let (_block_id, index) = reader.read::<(BlockHash, LeU32)>().ok()?;
    if index != expected_index {
        return None;
    }
    let depth = usize::from(reader.read::<crate::zakura::wire_codec::U8>().ok()?);
    if depth != coding.tree_depth() || depth.saturating_mul(32) > reader.remaining() {
        return None;
    }
    let mut siblings = Vec::with_capacity(depth);
    for _ in 0..depth {
        siblings.push(reader.array::<32>("sibling").ok()?);
    }
    // The proof check pads the payload to `S` after checking its exact length.
    let wire_payload = reader.bytes(reader.remaining(), "part_payload").ok()?;
    let enc_id = super::tree::encoding_id(coding);
    verify_part_proof(coding, &enc_id, &part_root, index, siblings, wire_payload).ok()
}

/// Parses `bytes` as exactly one block header, with no trailing bytes.
fn parse_header(bytes: &[u8]) -> Option<Header> {
    let mut slice = bytes;
    let header =
        <Header as zakura_chain::serialization::ZcashDeserialize>::zcash_deserialize(&mut slice)
            .ok()?;
    slice.is_empty().then_some(header)
}

/// The table's envelope of a decoded `HeaderMeta`, or `None` for another
/// message or a header that does not parse.
pub(super) fn decode_meta_envelope(
    block_id: block::Hash,
    message: Bpv1Message,
) -> Option<Box<MetaEnvelope>> {
    let Bpv1Message::HeaderMeta {
        header,
        binding,
        coding,
        part_root,
        proposer_node,
        ..
    } = message
    else {
        return None;
    };
    parse_header(&header)?;
    Some(Box::new(MetaEnvelope {
        block_id,
        header_bytes: header,
        binding,
        coding,
        part_root,
        proposer_node,
    }))
}

// ============================================================================
// Metrics & Tracing Translation
// ============================================================================

impl Session {
    /// Records `step`'s events as metrics and traces, and tracks pull timings.
    fn observe(&mut self, now: Instant, step: &PeerStep) {
        let node = &self.node;
        let peer_node = self.peer_node;
        let active_pulls = &mut self.pulls.active;
        let pending_pull_est = &mut self.pulls.granted_arrival;
        for ev in &step.events {
            record_peer_event(ev, &peer_node);
        }
        for output in &step.outputs {
            match output {
                PeerOutput::InsertPart { block_id, part, .. } => {
                    let index = part.index();
                    if is_sampled_part(block_id, index) {
                        tracing::debug!(?peer_node, ?block_id, index, "bpv1 sampled part verified");
                    }
                }
                PeerOutput::SendControl(msg) => {
                    if let Bpv1Message::Want {
                        want_id,
                        block_id,
                        stripe,
                        requested,
                    } = msg.as_ref()
                    {
                        // The trace keeps the RTT and rate the planner used.
                        let arrival = pending_pull_est.take();
                        active_pulls.insert(
                            *want_id,
                            ActivePullTelemetry {
                                block_id: *block_id,
                                stripe: *stripe,
                                bytes: u64::from(requested.count())
                                    .saturating_mul(self.state.part_bytes_of(block_id)),
                                issued_at: now,
                                estimated_arrival: arrival.map_or(now, |a| a.at),
                                first_part_at: None,
                                // A pull RTT is far below u64::MAX milliseconds.
                                rtt_ms: arrival.map(|a| a.rtt.as_millis() as u64),
                                rate_bps: arrival.map(|a| a.rate_bps),
                                outcome: PullOutcome::OnTime,
                            },
                        );
                    }
                }
                _ => {}
            }
        }
        for ev in &step.events {
            match ev {
                PeerEvent::PartPrefix {
                    block_id,
                    index,
                    auth,
                } => {
                    if let PartAuth::Want(want_id) = auth {
                        if let Some(pull) = active_pulls.get_mut(want_id) {
                            pull.first_part_at.get_or_insert(now);
                        }
                    }
                    if is_sampled_part(block_id, *index) {
                        tracing::debug!(
                            ?peer_node,
                            ?block_id,
                            index,
                            ?auth,
                            "bpv1 sampled part arrived"
                        );
                    }
                }
                PeerEvent::PartDropped {
                    reason: DropReason::AfterStop,
                    ..
                } => {
                    metrics::counter!("zakura.p2p.bpv1.part.surplus_after_k").increment(1);
                }
                PeerEvent::WantLate { want_id, .. } => {
                    if let Some(pull) = active_pulls.get_mut(want_id) {
                        pull.outcome = PullOutcome::Late;
                    }
                }
                PeerEvent::WantDeadline { want_id, .. } => {
                    if let Some(pull) = active_pulls.get_mut(want_id) {
                        pull.outcome = PullOutcome::Deadline;
                    }
                }
                PeerEvent::WantEnded { want_id, .. } => {
                    if let Some(pull) = active_pulls.remove(want_id) {
                        let estimated_ms = pull
                            .estimated_arrival
                            .saturating_duration_since(pull.issued_at)
                            .as_millis() as u64;
                        let observed_first_ms = pull.first_part_at.map(|t| {
                            t.saturating_duration_since(pull.issued_at).as_millis() as u64
                        });
                        let observed_last_ms =
                            now.saturating_duration_since(pull.issued_at).as_millis() as u64;
                        node.record_pull_trace(PullTraceRecord {
                            peer: peer_node,
                            block_id: pull.block_id,
                            stripe: pull.stripe,
                            bytes: pull.bytes,
                            rtt_ms: pull.rtt_ms,
                            rate_bps: pull.rate_bps,
                            estimated_ms,
                            observed_first_ms,
                            observed_last_ms,
                            outcome: pull.outcome,
                        });
                    }
                }
                _ => {}
            }
        }
    }
}

fn record_close_metric(cause: Bpv1CloseCause) {
    metrics::counter!("zakura.p2p.bpv1.close", "row" => cause.row.label()).increment(1);
    tracing::debug!(
        row = cause.row.label(),
        stream_kind = cause.stream_kind,
        stream_version = cause.stream_version,
        "bpv1 protocol close"
    );
}

fn record_peer_event(event: &PeerEvent, peer: &NodeId) {
    match event {
        PeerEvent::PartDropped { reason, .. } => {
            let label = match reason {
                DropReason::AfterStop => "after_stop",
                DropReason::RetiredLate => "retired_late",
                DropReason::Unwanted => "unwanted",
                DropReason::EarlyAged => "early_aged",
                DropReason::EarlyFull => "early_full",
                DropReason::EarlyOversize => "early_oversize",
                DropReason::LocalCapacity => "local_capacity",
                DropReason::NotAdmitted => "not_admitted",
            };
            metrics::counter!("zakura.p2p.bpv1.part.dropped", "reason" => label).increment(1);
        }
        PeerEvent::WantLate { released, .. } => {
            metrics::counter!("zakura.p2p.bpv1.pull.late").increment(1);
            // Widening usize to u64 is lossless on supported 64-bit targets.
            metrics::counter!("zakura.p2p.bpv1.pull.late_released_parts")
                .increment(*released as u64);
        }
        PeerEvent::WantDeadline { .. } => {
            metrics::counter!("zakura.p2p.bpv1.want.deadline_released").increment(1);
        }
        PeerEvent::Parked => {
            metrics::counter!("zakura.p2p.bpv1.session.parked").increment(1);
        }
        PeerEvent::HaveSent {
            block_id,
            count,
            delayed,
        } => {
            let group = if *delayed { "delayed" } else { "initial" };
            tracing::debug!(target: "bpv1_stage", %block_id, ?peer, count, group,
                "have_queued");
            // Widening usize to u64 is lossless on supported 64-bit targets.
            metrics::counter!("zakura.p2p.bpv1.have.indices", "group" => group)
                .increment(*count as u64);
        }
        PeerEvent::HaveSentToProposer { block_id, count } => {
            tracing::debug!(target: "bpv1_stage", %block_id, ?peer, count, "have_to_proposer");
            // Widening usize to u64 is lossless on supported 64-bit targets.
            metrics::counter!("zakura.p2p.bpv1.have.indices", "group" => "proposer")
                .increment(*count as u64);
        }
        PeerEvent::ProposerHeardHave { block_id, indices } => {
            tracing::debug!(target: "bpv1_stage", %block_id, ?peer, ?indices,
                "proposer_heard_have");
        }
        PeerEvent::CopiesQueued { bytes, .. } => {
            metrics::counter!("zakura.p2p.bpv1.seed.bytes", "deal" => "direct").increment(*bytes);
        }
        PeerEvent::PushFlagged { count, .. } => {
            // Widening usize to u64 is lossless on supported 64-bit targets.
            metrics::counter!("zakura.p2p.bpv1.push.flagged").increment(*count as u64);
        }
        PeerEvent::HaveSkippedAtDeadline { count, .. } => {
            // Widening usize to u64 is lossless on supported 64-bit targets.
            metrics::counter!("zakura.p2p.bpv1.have.skipped_at_deadline").increment(*count as u64);
        }
        _ => {}
    }
}

fn record_block_event(event: &BlockEvent) {
    match event {
        BlockEvent::PushLate { .. } => {
            metrics::counter!("zakura.p2p.bpv1.push.late_released").increment(1);
        }
        BlockEvent::PartDuplicates { count, .. } => {
            metrics::histogram!("zakura.p2p.bpv1.part.duplicates").record(f64::from(*count));
        }
        BlockEvent::MetaPendingAged { .. } => {
            metrics::counter!("zakura.p2p.bpv1.pending_meta.aged").increment(1);
        }
        BlockEvent::MetaPendingDropped { .. } => {
            metrics::counter!("zakura.p2p.bpv1.pending_meta.dropped").increment(1);
        }
        BlockEvent::BlockAdmitted { block_id } => {
            tracing::debug!(target: "bpv1_stage", %block_id, "block_admitted");
        }
        BlockEvent::CommitmentVerified { block_id } => {
            tracing::info!(target: "bpv1_stage", %block_id, "commitment_verified");
        }
        BlockEvent::StripeReachedK { block_id, stripe } => {
            tracing::info!(target: "bpv1_stage", %block_id, stripe, "stripe_received");
        }
        BlockEvent::Verdict { verdict, .. } => {
            let label = match verdict {
                VerifierVerdict::Valid => "valid",
                VerifierVerdict::RootMismatch => "root_mismatch",
                VerifierVerdict::LocalCapacity => "local_capacity",
                VerifierVerdict::Invalid => "invalid",
                VerifierVerdict::Duplicate => "duplicate",
            };
            metrics::counter!("zakura.p2p.bpv1.verdict", "kind" => label).increment(1);
        }
        BlockEvent::FallbackStarted { block_id, reason } => {
            let label = match reason {
                FallbackReason::Deadline => "deadline",
                FallbackReason::ReencodeFailed => "re_encode",
                FallbackReason::RootMismatch => "root_mismatch",
                FallbackReason::LocalCapacity => "local_capacity",
                FallbackReason::VerifyDeadline => "verify_deadline",
            };
            tracing::debug!(target: "bpv1_stage", %block_id, reason = label, "fallback_started");
            metrics::counter!("zakura.p2p.bpv1.fallback.reason", "reason" => label).increment(1);
        }
        BlockEvent::Parked { block_id } => {
            tracing::debug!(target: "bpv1_stage", %block_id, "verify_parked");
            metrics::counter!("zakura.p2p.bpv1.verify.parked").increment(1);
        }
        BlockEvent::Assembled { since_meta, .. } => {
            metrics::histogram!("zakura.p2p.bpv1.assembly.seconds")
                .record(since_meta.as_secs_f64());
        }
        BlockEvent::PartInserted {
            block_id,
            index,
            first,
            from_peer,
            source,
        } => {
            let label = match source {
                PartSource::Seed => "seed",
                PartSource::Direct => "direct",
                PartSource::Push => "push",
                PartSource::Pull => "pull",
                PartSource::Repair => "repair",
            };
            metrics::counter!("zakura.p2p.bpv1.part.received", "source" => label).increment(1);
            tracing::debug!(target: "bpv1_stage", %block_id, index, first, ?from_peer,
                source = label, "part_inserted");
        }
        BlockEvent::FirstPartStored {
            meta_to_first_part,
            first_part_to_stored,
            ..
        } => {
            metrics::histogram!("zakura.p2p.bpv1.meta_to_first_part.seconds")
                .record(meta_to_first_part.as_secs_f64());
            metrics::histogram!("zakura.p2p.bpv1.first_part_to_verified.seconds")
                .record(first_part_to_stored.as_secs_f64());
        }
        BlockEvent::Outcome { outcome, .. } => {
            let label = match outcome {
                BlockOutcome::Normal => "normal",
                BlockOutcome::Repair => "repair",
                BlockOutcome::Fallback => "fallback",
            };
            metrics::counter!("zakura.p2p.bpv1.ladder.outcome", "outcome" => label).increment(1);
        }
        BlockEvent::Uploaded {
            uploaded_bytes,
            body_bytes,
            ..
        } if *body_bytes > 0 => {
            // Byte counts stay far below 2^53, so the f64 ratio is exact enough.
            let ratio = *uploaded_bytes as f64 / *body_bytes as f64;
            metrics::histogram!("zakura.p2p.bpv1.upload.bytes_over_body").record(ratio);
        }
        _ => {}
    }
}

fn record_proposer_event(event: &ProposerEvent) {
    match event {
        ProposerEvent::Solved { block_id } => {
            tracing::debug!(target: "bpv1_stage", %block_id, "origin_publish");
        }
        // 04 §2: each holder's pushed seeds, push rate, and predicted finish.
        ProposerEvent::SeedPlanned {
            block_id,
            peer,
            pushed,
            other,
            push_rate,
            finish,
        } => {
            tracing::debug!(
                target: "bpv1_stage",
                %block_id,
                ?peer,
                pushed,
                other,
                push_rate,
                finish_ms = finish.as_millis(),
                "seed_planned"
            );
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn custom_service_starts_with_the_endpoint_node_id() {
        // The service exists only once the endpoint's node id does,
        // so its first session never runs under a placeholder id.
        let runtime = tokio::runtime::Runtime::new().expect("test runtime starts");
        let _guard = runtime.enter();
        let endpoint_node = zakura_quic::NodeSecretKey::from_bytes(&[7; 32]).public();
        let (_svc, handle) = bpv1_custom_service(
            ZakuraBpv1Config::default(),
            HavePacing::default(),
            Profile::TINY,
            zakura_chain::parameters::Network::new_regtest(Default::default()),
            endpoint_node,
            Bpv1Ports::default(),
            CancellationToken::new(),
        );
        assert_eq!(handle.node.local_node, endpoint_node);
    }

    /// A peer that stops reading control stops only the control writer: later
    /// messages queue behind the frame in flight, which reports when it stalls.
    #[tokio::test]
    async fn control_writer_queues_behind_an_unread_frame_and_reports_its_stall() {
        use super::super::peer::REQUEST_BASE_TIMEOUT;
        use std::time::Duration;
        let (send, _unread) = crate::zakura::transport::framed_channel(1);
        let mut ctrl = ControlWriter::new(send);
        let done = |b| {
            Box::new(Bpv1Message::BlockDone {
                block_id: block::Hash([b; 32]),
            })
        };
        ctrl.push(done(1));
        assert!(
            ctrl.written().await.is_some(),
            "the first frame fills the queue"
        );
        ctrl.start();
        let before = Instant::now();
        ctrl.push(done(2));
        ctrl.push(done(3));
        let stall_at = ctrl.stall_at().expect("the second frame is in flight");
        assert!(stall_at >= before + REQUEST_BASE_TIMEOUT * 4);
        assert_eq!(ctrl.queue.len(), 1, "the third waits behind it");
        let blocked = tokio::time::timeout(Duration::from_millis(50), ctrl.written()).await;
        assert!(blocked.is_err(), "the unread stream holds the second frame");
        assert!(
            ctrl.is_writing(),
            "a timed-out wait keeps the frame in flight"
        );
    }

    /// A verified part whose session already ended releases its charge, as a
    /// failed proof does; a delivered part keeps it for its insert.
    #[test]
    fn a_proof_result_its_session_never_reads_releases_the_charge() {
        let encoded =
            super::super::codec::encode_body(&Profile::TINY, &[0x11; 64], 2).expect("encodes");
        let part = encoded.parts[0].clone();
        let token = PartToken {
            block_id: block::Hash([4; 32]),
            index: part.index(),
            session_gen: 1,
            attempt: 1,
        };
        let peer = NodeId::from_bytes(&[5; 32]);
        let (driver, mut released) = mpsc::unbounded_channel();
        let releases = |released: &mut mpsc::UnboundedReceiver<DriverMsg>| {
            std::iter::from_fn(|| released.try_recv().ok()).count()
        };

        let (session, mut results) = mpsc::unbounded_channel();
        finish_part_proof(&session, &driver, peer, token, Some(part.clone()));
        assert!(results.try_recv().is_ok());
        assert_eq!(releases(&mut released), 0, "the insert takes the charge");

        finish_part_proof(&session, &driver, peer, token, None);
        assert_eq!(releases(&mut released), 1, "a failed proof releases it");

        drop(results);
        finish_part_proof(&session, &driver, peer, token, Some(part));
        assert_eq!(releases(&mut released), 1, "an unread result releases it");
    }

    #[test]
    fn part_frame_payload_moves_into_the_prefix_without_a_copy() {
        // A received part costs no allocation until the block admits
        // it, so a dropped part costs none.
        let mut payload = vec![0u8; 36 + 64];
        payload[..32].copy_from_slice(&[9; 32]);
        let frame = Frame {
            message_type: message_type::PART,
            flags: 0,
            payload,
        };
        let received = frame.payload.as_ptr();
        let Ok(PeerInput::RecvPartPrefix(prefix)) = data_input(frame) else {
            panic!("a well-formed part prefix decodes");
        };
        assert_eq!(prefix.block_id, block::Hash([9; 32]));
        assert_eq!(prefix.payload.as_ptr(), received);
    }

    #[test]
    fn templates_joining_an_encode_each_get_their_commitment() {
        // A template for a body that is already encoding waits for
        // that encode and gets its own commitment instead of `None`.
        let runtime = tokio::runtime::Runtime::new().expect("test runtime starts");
        let _guard = runtime.enter();
        let network = zakura_chain::parameters::Network::new_regtest(
            zakura_chain::parameters::testnet::ConfiguredActivationHeights {
                nu7: Some(1),
                ..Default::default()
            }
            .into(),
        );
        let local = zakura_quic::NodeSecretKey::from_bytes(&[1; 32]).public();
        let peer = zakura_quic::NodeSecretKey::from_bytes(&[2; 32]).public();
        let (_svc, handle) = Bpv1Service::start(
            ZakuraBpv1Config::default(),
            HavePacing::default(),
            Profile::TINY,
            network,
            local,
            Bpv1Ports::default(),
            CancellationToken::new(),
        );
        let (inputs, _inputs_rx) = mpsc::unbounded_channel();
        handle.node.lock_sessions().insert(
            peer,
            LiveSession {
                conn_id: 1,
                session_gen: 1,
                inputs,
                cancel: CancellationToken::new(),
                connection: CancellationToken::new(),
                load: None,
                snapshot: Arc::new(Mutex::new(PullPeerState {
                    peer,
                    session_gen: 1,
                    ahead_bytes: 0,
                    can_pull: true,
                    window_remaining: PULL_WINDOW,
                })),
            },
        );
        let body = vec![7u8; 32 * 1024];

        // Holding the proposer lock queues three callers behind it. The first
        // to step starts the encode; the other two join it.
        let callers = {
            let _proposer = handle.node.lock_proposer();
            let callers: Vec<_> = (0..3u8)
                .map(|i| {
                    let handle = handle.clone();
                    let body = body.clone();
                    std::thread::spawn(move || {
                        handle.prepare_commitment([i; 32], Height(5), 2, &body)
                    })
                })
                .collect();
            std::thread::sleep(std::time::Duration::from_millis(200));
            callers
        };
        let commitments: Vec<PreparedCommitment> = callers
            .into_iter()
            .map(|caller| {
                caller
                    .join()
                    .expect("caller finishes")
                    .expect("every template gets a commitment")
            })
            .collect();
        let roots: zakura_rt::collections::HashSet<[u8; 32]> =
            commitments.iter().map(|c| c.light_client_root).collect();
        assert_eq!(roots.len(), 3, "each commitment keeps its template's root");
        assert!(commitments
            .iter()
            .all(|c| c.part_root == commitments[0].part_root));
    }
}
