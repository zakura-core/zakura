//! In-memory tracking for completely stored canonical checkpoint brackets.

use std::{collections::BTreeMap, sync::Arc};

use thiserror::Error;
use tokio::sync::watch;
use zakura_chain::block::{self, Height};

use super::ZakuraDb;

/// The highest configured checkpoint whose complete canonical bracket is durable.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct HighestCompletedCheckpoint {
    /// The completed configured checkpoint height.
    pub height: Height,
    /// The configured checkpoint hash stored canonically at `height`.
    pub hash: block::Hash,
}

/// Errors restoring or advancing the highest completed checkpoint.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum HighestCompletedCheckpointError {
    /// The configured checkpoint list does not contain genesis.
    #[error("the configured checkpoint list does not contain genesis")]
    MissingGenesisCheckpoint,

    /// A canonical header required to establish the trusted body base is missing.
    #[error("missing canonical header at trusted body height {height:?}")]
    MissingCanonicalHeader {
        /// Missing header height.
        height: Height,
    },

    /// A configured checkpoint does not match the canonical header store.
    #[error(
        "canonical header at configured checkpoint {height:?} has hash {actual:?}, expected {expected}"
    )]
    CheckpointMismatch {
        /// Configured checkpoint height.
        height: Height,
        /// Configured checkpoint hash.
        expected: block::Hash,
        /// Canonical hash, if one is stored.
        actual: Option<block::Hash>,
    },

    /// A header height operation overflowed.
    #[error("highest completed checkpoint height overflow")]
    HeightOverflow,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct TrackerState {
    current: Option<HighestCompletedCheckpoint>,
    /// The first configured checkpoint strictly above `current`.
    next_checkpoint: Option<(Height, block::Hash)>,
    cursor: Option<(Height, block::Hash)>,
}

/// A candidate state computed against a header batch before it commits.
///
/// Install this candidate only after the corresponding RocksDB write succeeds.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct ProposedHighestCompletedCheckpoint(TrackerState);

/// The process-local highest completed checkpoint and its publication channel.
///
/// Canonical headers are the durable source of truth. This cache is reconstructed on open
/// and replaced only after the database write that justifies an advance succeeds.
#[derive(Debug)]
pub struct HighestCompletedCheckpointTracker {
    state: TrackerState,
    sender: watch::Sender<Option<HighestCompletedCheckpoint>>,
}

impl HighestCompletedCheckpointTracker {
    /// Reconstructs checkpoint progress from durable canonical headers.
    pub fn open(
        db: &ZakuraDb,
    ) -> Result<
        (Self, watch::Receiver<Option<HighestCompletedCheckpoint>>),
        HighestCompletedCheckpointError,
    > {
        let state = TrackerState::reconstruct(db, &[], None)?;
        let (sender, receiver) = watch::channel(state.current);
        Ok((Self { state, sender }, receiver))
    }

    /// Returns the latest checkpoint made durable by a successful write.
    pub fn current(&self) -> Option<HighestCompletedCheckpoint> {
        self.state.current
    }

    /// Computes the post-commit tracker state using pending headers.
    ///
    /// This method does not mutate the tracker.
    pub fn propose_after_headers(
        &self,
        db: &ZakuraDb,
        anchor: block::Hash,
        headers: &[Arc<block::Header>],
    ) -> Result<ProposedHighestCompletedCheckpoint, HighestCompletedCheckpointError> {
        if headers.is_empty() {
            return Ok(ProposedHighestCompletedCheckpoint(self.state));
        }

        let Some(anchor_height) = db
            .header_height(anchor)
            .or_else(|| (anchor == db.network().genesis_hash()).then_some(Height::MIN))
        else {
            // Header-range validation reports the unknown anchor before this proposal is used.
            return Ok(ProposedHighestCompletedCheckpoint(self.state));
        };

        let mut pending = Vec::with_capacity(headers.len());
        for (index, header) in headers.iter().enumerate() {
            let offset = u32::try_from(index + 1)
                .map_err(|_| HighestCompletedCheckpointError::HeightOverflow)?;
            let height = (anchor_height + i64::from(offset))
                .ok_or(HighestCompletedCheckpointError::HeightOverflow)?;
            pending.push((
                height,
                block::Hash::from(header.as_ref()),
                Arc::clone(header),
            ));
        }

        let first_conflict = pending.iter().find_map(|(height, hash, _)| {
            db.header_hash(*height)
                .is_some_and(|stored| stored != *hash)
                .then_some(*height)
        });
        let post_tip = if first_conflict.is_some() {
            pending.last().map(|(height, _, _)| *height)
        } else {
            db.best_header_tip()
                .map(|(height, _)| height)
                .into_iter()
                .chain(pending.last().map(|(height, _, _)| *height))
                .max()
        };

        let mut start_hint = self.state;
        if first_conflict.is_some_and(|height| {
            start_hint
                .cursor
                .is_some_and(|(cursor, _)| height <= cursor)
        }) {
            start_hint.cursor = start_hint
                .current
                .map(|checkpoint| (checkpoint.height, checkpoint.hash));
        }

        Ok(ProposedHighestCompletedCheckpoint(
            TrackerState::reconstruct(db, &pending, Some((start_hint, post_tip)))?,
        ))
    }

    /// Rejects a reorg that would replace a completed checkpoint or any of its ancestors.
    pub fn check_immutable_conflicts(
        &self,
        db: &ZakuraDb,
        anchor: block::Hash,
        headers: &[Arc<block::Header>],
    ) -> Result<(), Height> {
        let Some(completed) = self.current() else {
            return Ok(());
        };
        let Some(anchor_height) = db
            .header_height(anchor)
            .or_else(|| (anchor == db.network().genesis_hash()).then_some(Height::MIN))
        else {
            return Ok(());
        };

        for (index, header) in headers.iter().enumerate() {
            let Ok(offset) = u32::try_from(index + 1) else {
                break;
            };
            let Some(height) = anchor_height + i64::from(offset) else {
                break;
            };
            if height > completed.height {
                break;
            }
            let hash = block::Hash::from(header.as_ref());
            if db.header_hash(height).is_some_and(|stored| stored != hash) {
                return Err(height);
            }
        }

        Ok(())
    }

    /// Installs a proposal after its corresponding header batch succeeds.
    pub fn commit_success(&mut self, proposed: ProposedHighestCompletedCheckpoint) {
        self.replace_state(proposed.0);
    }

    /// Reconstructs the tracker after a successful body write, rollback, or repair.
    pub fn rebind_from_db(&mut self, db: &ZakuraDb) -> Result<(), HighestCompletedCheckpointError> {
        let state = TrackerState::reconstruct(db, &[], Some((self.state, None)))?;
        self.replace_state(state);
        Ok(())
    }

    fn replace_state(&mut self, state: TrackerState) {
        let changed = self.state.current != state.current;
        self.state = state;
        if changed {
            let _ = self.sender.send(state.current);
        }
    }
}

impl TrackerState {
    fn reconstruct(
        db: &ZakuraDb,
        pending: &[(Height, block::Hash, Arc<block::Header>)],
        start_hint: Option<(TrackerState, Option<Height>)>,
    ) -> Result<Self, HighestCompletedCheckpointError> {
        let disk_tip = db.best_header_tip().map(|(height, _)| height);
        let canonical_tip = start_hint.and_then(|(_, tip)| tip).or(disk_tip);
        let Some(canonical_tip) = canonical_tip else {
            return Ok(Self {
                current: None,
                next_checkpoint: None,
                cursor: None,
            });
        };

        let checkpoints = db.network().checkpoint_list();
        let genesis_hash = checkpoints
            .hash(Height::MIN)
            .ok_or(HighestCompletedCheckpointError::MissingGenesisCheckpoint)?;

        let body_tip = db
            .finalized_tip_height()
            .filter(|height| *height <= canonical_tip);
        let hinted_state = start_hint.map(|(state, _)| state);
        let mut state = match hinted_state {
            Some(state)
                if state.current.is_some_and(|current| {
                    current.height <= canonical_tip
                        && Self::validate_checkpoint(db, current).is_ok()
                }) =>
            {
                state
            }
            _ => Self::trusted_body_base(db, canonical_tip, genesis_hash)?,
        };

        if let Some((cursor_height, cursor_hash)) = state.cursor {
            if cursor_height > canonical_tip
                || Self::header_hash(db, pending, cursor_height) != Some(cursor_hash)
            {
                state.cursor = state
                    .current
                    .map(|checkpoint| (checkpoint.height, checkpoint.hash));
            }
        }

        if let Some(body_height) = body_tip {
            if state
                .next_checkpoint
                .is_some_and(|(height, _)| height <= body_height)
            {
                let (height, hash) = checkpoints
                    .checkpoint_at_or_before(body_height)
                    .ok_or(HighestCompletedCheckpointError::MissingGenesisCheckpoint)?;
                let completed = HighestCompletedCheckpoint { height, hash };
                Self::validate_checkpoint(db, completed)?;
                state.current = Some(completed);
                state.next_checkpoint = checkpoints.checkpoint_after(height);
            }

            if state
                .cursor
                .is_none_or(|(cursor_height, _)| cursor_height < body_height)
            {
                let body_hash = db.header_hash(body_height).ok_or(
                    HighestCompletedCheckpointError::MissingCanonicalHeader {
                        height: body_height,
                    },
                )?;
                state.cursor = Some((body_height, body_hash));
            }
        }

        let pending: BTreeMap<_, _> = pending
            .iter()
            .map(|(height, hash, header)| (*height, (*hash, header)))
            .collect();
        let (mut cursor_height, mut cursor_hash) =
            state.cursor.unwrap_or((Height::MIN, genesis_hash));

        while cursor_height < canonical_tip {
            let next_height = cursor_height
                .next()
                .map_err(|_| HighestCompletedCheckpointError::HeightOverflow)?;
            let Some((hash, header)) = pending
                .get(&next_height)
                .map(|(hash, header)| (*hash, Arc::clone(header)))
                .or_else(|| db.header_by_height(next_height))
            else {
                break;
            };
            if block::Hash::from(header.as_ref()) != hash
                || header.previous_block_hash != cursor_hash
            {
                break;
            }
            if let Some((_, expected)) = state
                .next_checkpoint
                .filter(|(height, _)| *height == next_height)
            {
                if hash != expected {
                    return Err(HighestCompletedCheckpointError::CheckpointMismatch {
                        height: next_height,
                        expected,
                        actual: Some(hash),
                    });
                }
                state.current = Some(HighestCompletedCheckpoint {
                    height: next_height,
                    hash,
                });
                state.next_checkpoint = checkpoints.checkpoint_after(next_height);
            }
            cursor_height = next_height;
            cursor_hash = hash;
        }
        state.cursor = Some((cursor_height, cursor_hash));

        Ok(state)
    }

    fn trusted_body_base(
        db: &ZakuraDb,
        canonical_tip: Height,
        genesis_hash: block::Hash,
    ) -> Result<Self, HighestCompletedCheckpointError> {
        let checkpoints = db.network().checkpoint_list();
        let (completed, cursor) = if let Some(body_height) = db
            .finalized_tip_height()
            .filter(|height| *height <= canonical_tip)
        {
            let body_hash = db.header_hash(body_height).ok_or(
                HighestCompletedCheckpointError::MissingCanonicalHeader {
                    height: body_height,
                },
            )?;
            let (height, hash) = checkpoints
                .checkpoint_at_or_before(body_height)
                .ok_or(HighestCompletedCheckpointError::MissingGenesisCheckpoint)?;
            (
                HighestCompletedCheckpoint { height, hash },
                (body_height, body_hash),
            )
        } else {
            (
                HighestCompletedCheckpoint {
                    height: Height::MIN,
                    hash: genesis_hash,
                },
                (Height::MIN, genesis_hash),
            )
        };
        Self::validate_checkpoint(db, completed)?;

        Ok(Self {
            current: Some(completed),
            next_checkpoint: checkpoints.checkpoint_after(completed.height),
            cursor: Some(cursor),
        })
    }

    fn validate_checkpoint(
        db: &ZakuraDb,
        checkpoint: HighestCompletedCheckpoint,
    ) -> Result<(), HighestCompletedCheckpointError> {
        let expected = db
            .network()
            .checkpoint_list()
            .hash(checkpoint.height)
            .ok_or(HighestCompletedCheckpointError::CheckpointMismatch {
                height: checkpoint.height,
                expected: checkpoint.hash,
                actual: None,
            })?;
        let actual = db.header_hash(checkpoint.height);
        if checkpoint.hash != expected || actual != Some(expected) {
            return Err(HighestCompletedCheckpointError::CheckpointMismatch {
                height: checkpoint.height,
                expected,
                actual,
            });
        }
        Ok(())
    }

    fn header_hash(
        db: &ZakuraDb,
        pending: &[(Height, block::Hash, Arc<block::Header>)],
        height: Height,
    ) -> Option<block::Hash> {
        pending
            .iter()
            .find_map(|(pending_height, hash, _)| (*pending_height == height).then_some(*hash))
            .or_else(|| db.header_hash(height))
    }
}
