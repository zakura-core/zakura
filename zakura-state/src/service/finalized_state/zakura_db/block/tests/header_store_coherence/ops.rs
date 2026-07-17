//! The op alphabet and the harness that drives the real store writers.
//!
//! Each op maps to one production write-batch shape:
//!
//! - [`Op::CommitHeaderRange`] → `prepare_header_range_batch_with_roots`
//!   (the header-sync range commit);
//! - [`Op::CommitBody`] → `prepare_block_header_and_transaction_data_batch`
//!   plus the finalization roots delete (internally runs the release path);
//! - [`Op::Seed`] → `ZakuraDb::seed_zakura_header_from_committed_block`
//!   (the non-finalized best-chain commit hook);
//! - [`Op::Finalize`] → sequential body commits along the expected canonical
//!   chain;
//! - [`Op::Reopen`] → close and reopen the database (restart survival).
//!
//! After every op the harness cross-checks the oracle's prediction against the
//! store's response (rejections must be side-effect free), then runs the full
//! audit. Any violation or prediction mismatch fails the sequence with a
//! transcribable [`FailureReport`].

use std::sync::{Arc, LazyLock};

use zakura_chain::block;

use super::super::common::{
    persistent_config, persistent_state, write_full_block_header_and_transactions,
};
use super::{
    audit::{audit_against_expected_chain, audit_store, dump_store, Violation},
    fabricate::{fabricate_body, FabHeader, Universe},
    oracle::{Oracle, Prediction, ResolvedRange},
};
use crate::{
    error::{CommitCheckpointVerifiedError, CommitHeaderRangeError},
    request::{FinalizedBlock, Treestate},
    service::finalized_state::{
        disk_db::{DiskWriteBatch, WriteDisk},
        ZakuraDb, COMMITMENT_ROOTS_BY_HEIGHT,
    },
    CheckpointVerifiedBlock, Config,
};

/// Which fabricated chain an op draws rows from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Source {
    Trunk,
    Branch(usize),
}

/// How a header range names its anchor.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Anchor {
    /// The natural anchor: the row below `offset` (the branch fork parent or
    /// genesis for `offset == 0`).
    Auto,
    /// The genesis hash, regardless of offset.
    Genesis,
    /// The trunk header at this height.
    TrunkAt(u32),
    /// `universe.branches[i].headers[j]`'s hash.
    BranchAt(usize, usize),
}

/// One mutation of the store.
#[derive(Clone, Debug)]
pub(crate) enum Op {
    CommitHeaderRange {
        source: Source,
        offset: usize,
        len: usize,
        anchor: Anchor,
    },
    CommitBody {
        source: Source,
        index: usize,
    },
    Seed {
        source: Source,
        index: usize,
    },
    Finalize {
        count: usize,
    },
    Reopen,
}

/// How the store responded to an executed op.
#[derive(Debug)]
pub(crate) enum OpOutcome {
    Accepted,
    Rejected(RejectError),
    /// The op does not apply in the current state and was not executed.
    Skipped(#[allow(dead_code)] &'static str),
}

#[derive(Debug)]
pub(crate) enum RejectError {
    HeaderRange(CommitHeaderRangeError),
    /// Diagnostic payload rendered through `Debug` in failure reports.
    Body(#[allow(dead_code)] Box<CommitCheckpointVerifiedError>),
}

impl OpOutcome {
    pub fn header_range_error(&self) -> &CommitHeaderRangeError {
        match self {
            OpOutcome::Rejected(RejectError::HeaderRange(error)) => error,
            other => panic!("expected a header-range rejection, got {other:?}"),
        }
    }
}

/// Why a sequence failed: the executed op prefix (last op is the failing one)
/// plus everything found after it. All fields are diagnostic payloads rendered
/// through `Debug` in test failure output.
#[derive(Debug)]
pub(crate) struct FailureReport {
    /// The transcribable op sequence.
    #[allow(dead_code)]
    pub executed: Vec<Op>,
    #[allow(dead_code)]
    pub violations: Vec<Violation>,
    #[allow(dead_code)]
    pub mismatches: Vec<String>,
}

/// The shared fabricated universe. Built once — it is deterministic and
/// immutable, and fabrication is the slowest part of a harness construction.
pub(crate) fn universe() -> &'static Universe {
    static UNIVERSE: LazyLock<Universe> = LazyLock::new(Universe::new);
    &UNIVERSE
}

pub(crate) struct Harness {
    pub universe: &'static Universe,
    pub oracle: Oracle,
    config: Config,
    state: Option<ZakuraDb>,
    executed: Vec<Op>,
    _tempdir: tempfile::TempDir,
}

impl Harness {
    /// A harness over a fresh persistent store holding only genesis.
    ///
    /// The store is always persistent (on a tempdir) so `Reopen` behaves the
    /// same wherever it appears in a sequence.
    pub fn new() -> Self {
        let universe = universe();
        let tempdir = tempfile::tempdir().expect("test tempdir is available");
        let config = persistent_config(tempdir.path());
        let state = persistent_state(&config, &universe.network);
        write_full_block_header_and_transactions(&state, universe.genesis.clone());

        Harness {
            universe,
            oracle: Oracle::new(universe),
            config,
            state: Some(state),
            executed: Vec::new(),
            _tempdir: tempdir,
        }
    }

    pub fn state(&self) -> &ZakuraDb {
        self.state
            .as_ref()
            .expect("state is only vacated inside Reopen")
    }

    fn rows_of(&self, source: Source) -> &[FabHeader] {
        match source {
            Source::Trunk => &self.universe.trunk,
            Source::Branch(index) => &self.universe.branches[index].headers,
        }
    }

    fn resolve_anchor(&self, source: Source, offset: usize, anchor: Anchor) -> block::Hash {
        match anchor {
            Anchor::Auto => {
                if offset == 0 {
                    match source {
                        Source::Trunk => self.universe.genesis.hash(),
                        Source::Branch(index) => self.universe.branches[index].fork_parent.1,
                    }
                } else {
                    self.rows_of(source)[offset - 1].hash
                }
            }
            Anchor::Genesis => self.universe.genesis.hash(),
            Anchor::TrunkAt(height) => {
                // Clamp so randomly generated anchors always resolve.
                let height = height.clamp(1, self.universe.trunk.len() as u32);
                self.universe.trunk_at(height).hash
            }
            Anchor::BranchAt(branch, index) => {
                let headers =
                    &self.universe.branches[branch % self.universe.branches.len()].headers;
                headers[index % headers.len()].hash
            }
        }
    }

    fn resolve_range(
        &self,
        source: Source,
        offset: usize,
        len: usize,
        anchor: Anchor,
    ) -> Option<ResolvedRange> {
        let rows = self.rows_of(source);
        if offset >= rows.len() || len == 0 {
            return None;
        }
        let end = (offset + len).min(rows.len());
        Some(ResolvedRange {
            anchor: self.resolve_anchor(source, offset, anchor),
            rows: rows[offset..end].to_vec(),
        })
    }

    /// Runs one op: execute, cross-check the oracle, audit the store.
    pub fn run(&mut self, op: &Op) -> Result<OpOutcome, FailureReport> {
        self.executed.push(op.clone());
        let mut mismatches = Vec::new();

        let outcome = match op {
            Op::CommitHeaderRange {
                source,
                offset,
                len,
                anchor,
            } => {
                let Some(range) = self.resolve_range(*source, *offset, *len, *anchor) else {
                    return self.finish(OpOutcome::Skipped("range out of bounds"), mismatches);
                };
                match self.oracle.predict_header_range(&range) {
                    Prediction::Skip(reason) => {
                        return self.finish(OpOutcome::Skipped(reason), mismatches)
                    }
                    prediction => {
                        let dump_before = dump_store(self.state());
                        let result = execute_header_range(self.state(), &range);
                        match (&prediction, &result) {
                            (Prediction::Accept, Ok(())) => {
                                self.oracle.apply_header_range(&range);
                            }
                            (Prediction::Reject(_), Err(_)) => {
                                if dump_store(self.state()) != dump_before {
                                    mismatches.push(format!(
                                        "rejected header range mutated the store: {op:?}"
                                    ));
                                }
                            }
                            (Prediction::Accept, Err(error)) => {
                                mismatches.push(format!(
                                    "oracle predicted accept, store rejected with {error:?}: {op:?}"
                                ));
                            }
                            (Prediction::Reject(kind), Ok(())) => {
                                mismatches.push(format!(
                                    "oracle predicted rejection ({kind:?}), store accepted: {op:?}"
                                ));
                                // Keep A4 meaningful for the rest of the report.
                                self.oracle.apply_header_range(&range);
                            }
                            (Prediction::Skip(_), _) => unreachable!("skips return early"),
                        }
                        match result {
                            Ok(()) => OpOutcome::Accepted,
                            Err(error) => OpOutcome::Rejected(RejectError::HeaderRange(error)),
                        }
                    }
                }
            }

            Op::CommitBody { source, index } => {
                let rows = self.rows_of(*source);
                let Some(fab) = rows.get(*index).cloned() else {
                    return self.finish(OpOutcome::Skipped("body index out of bounds"), mismatches);
                };
                match self.oracle.predict_body(&fab) {
                    Prediction::Skip(reason) => {
                        return self.finish(OpOutcome::Skipped(reason), mismatches)
                    }
                    _ => match execute_body(self.state(), &fab) {
                        Ok(()) => {
                            self.oracle.apply_body(&fab);
                            OpOutcome::Accepted
                        }
                        Err(error) => {
                            mismatches.push(format!(
                                "oracle predicted accept, body commit rejected with {error:?}: {op:?}"
                            ));
                            OpOutcome::Rejected(RejectError::Body(Box::new(error)))
                        }
                    },
                }
            }

            Op::Seed { source, index } => {
                let rows = self.rows_of(*source);
                let Some(fab) = rows.get(*index).cloned() else {
                    return self.finish(OpOutcome::Skipped("seed index out of bounds"), mismatches);
                };
                match self.oracle.predict_seed(&fab) {
                    Prediction::Skip(reason) => {
                        return self.finish(OpOutcome::Skipped(reason), mismatches)
                    }
                    _ => {
                        // A seed whose parent is not the stored row below it is
                        // rejected without mutating the store. Reporting success
                        // would let the caller publish a missing durable anchor.
                        let parent_linked = self.oracle.seed_is_parent_linked(&fab);
                        let dump_before = (!parent_linked).then(|| dump_store(self.state()));
                        let block = fabricate_body(&fab);
                        match self
                            .state()
                            .seed_zakura_header_from_committed_block(fab.height, &block)
                        {
                            Ok(()) => {
                                if parent_linked {
                                    self.oracle.apply_seed(&fab);
                                } else {
                                    mismatches.push(format!(
                                        "unlinked seed unexpectedly succeeded: {op:?}"
                                    ));
                                }
                                OpOutcome::Accepted
                            }
                            Err(error) => {
                                if parent_linked {
                                    mismatches.push(format!(
                                        "oracle predicted accept, seed rejected with {error:?}: {op:?}"
                                    ));
                                } else if dump_store(self.state())
                                    != dump_before.expect("dumped before an unlinked seed")
                                {
                                    mismatches.push(format!(
                                        "rejected unlinked seed mutated the store: {op:?}"
                                    ));
                                }
                                OpOutcome::Rejected(RejectError::HeaderRange(error))
                            }
                        }
                    }
                }
            }

            Op::Finalize { count } => {
                let mut finalized_any = false;
                for _ in 0..*count {
                    let next = self.oracle.next_body_height();
                    let Some(&hash) = self.oracle.canonical_chain().get(&next) else {
                        break;
                    };
                    let fab = self
                        .oracle
                        .fab_for(hash)
                        .expect("canonical hashes come from the universe")
                        .clone();
                    match execute_body(self.state(), &fab) {
                        Ok(()) => {
                            self.oracle.apply_body(&fab);
                            finalized_any = true;
                        }
                        Err(error) => {
                            mismatches.push(format!(
                                "finalizing canonical height {next:?} rejected with {error:?}"
                            ));
                            break;
                        }
                    }
                }
                if finalized_any || !mismatches.is_empty() {
                    OpOutcome::Accepted
                } else {
                    return self.finish(
                        OpOutcome::Skipped("no canonical row above the body tip"),
                        mismatches,
                    );
                }
            }

            Op::Reopen => {
                let dump_before = dump_store(self.state());
                let mut state = self.state.take().expect("state is present before Reopen");
                state.shutdown(true);
                drop(state);
                let state = persistent_state(&self.config, &self.universe.network);
                self.state = Some(state);
                if dump_store(self.state()) != dump_before {
                    mismatches.push("store changed across a reopen".to_string());
                }
                OpOutcome::Accepted
            }
        };

        self.finish(outcome, mismatches)
    }

    /// Audits the store and closes out an op.
    fn finish(
        &mut self,
        outcome: OpOutcome,
        mismatches: Vec<String>,
    ) -> Result<OpOutcome, FailureReport> {
        let mut violations = audit_store(self.state());
        violations.extend(audit_against_expected_chain(
            self.state(),
            self.oracle.canonical_chain(),
        ));

        if violations.is_empty() && mismatches.is_empty() {
            Ok(outcome)
        } else {
            Err(FailureReport {
                executed: self.executed.clone(),
                violations,
                mismatches,
            })
        }
    }

    /// Runs a whole sequence, stopping at the first violation.
    pub fn run_all(&mut self, ops: &[Op]) -> Result<Vec<OpOutcome>, FailureReport> {
        ops.iter().map(|op| self.run(op)).collect()
    }
}

fn execute_header_range(
    state: &ZakuraDb,
    range: &ResolvedRange,
) -> Result<(), CommitHeaderRangeError> {
    let headers: Vec<Arc<block::Header>> =
        range.rows.iter().map(|fab| fab.header.clone()).collect();
    let body_sizes: Vec<u32> = range.rows.iter().map(|fab| fab.body_size).collect();
    let roots: Vec<_> = range.rows.iter().map(|fab| fab.roots.clone()).collect();

    let mut batch = DiskWriteBatch::new();
    batch.prepare_header_range_batch_with_roots(
        state,
        range.anchor,
        &headers,
        &body_sizes,
        &roots,
    )?;
    state
        .write_batch(batch)
        .expect("header range batch writes successfully");
    Ok(())
}

/// The body-commit batch shape: header and transaction data, the internal
/// zakura release, and the finalization delete of the provisional roots row
/// (`prepare_block_batch`). The verified-roots write from the trees batch is
/// deliberately absent — it needs treestates the harness does not model, and
/// the audit treats a missing verified-roots row as acceptable.
fn execute_body(state: &ZakuraDb, fab: &FabHeader) -> Result<(), CommitCheckpointVerifiedError> {
    let block = fabricate_body(fab);
    let checkpoint_verified = CheckpointVerifiedBlock::from(block);
    let finalized =
        FinalizedBlock::from_checkpoint_verified(checkpoint_verified, Treestate::default());

    let mut batch = DiskWriteBatch::new();
    batch.prepare_block_header_and_transaction_data_batch(state, &finalized, true, None)?;
    let roots_cf = state.db.cf_handle(COMMITMENT_ROOTS_BY_HEIGHT).unwrap();
    batch.zs_delete(&roots_cf, fab.height);
    state
        .db
        .write(batch)
        .expect("body batch writes successfully");
    Ok(())
}
