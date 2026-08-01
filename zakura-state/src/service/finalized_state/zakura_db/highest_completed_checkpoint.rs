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

    /// A canonical header required by `from_finalized_body` is missing.
    #[error("missing canonical header at finalized body height {height:?}")]
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

#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
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

#[cfg(test)]
impl ProposedHighestCompletedCheckpoint {
    /// Returns the checkpoint that would become current if this proposal commits.
    pub fn current(&self) -> Option<HighestCompletedCheckpoint> {
        self.0.current
    }
}

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
    ///
    /// Cold open: no `start_hint`, so reconstruct uses `from_finalized_body` (path B)
    /// then walks any headers above the body tip (path D).
    ///
    /// If durable headers are inconsistent, logs the error and clears progress so startup
    /// remains recoverable. No checkpoint is published until a later successful write
    /// reconstructs progress from the durable canonical headers.
    pub fn open(db: &ZakuraDb) -> (Self, watch::Receiver<Option<HighestCompletedCheckpoint>>) {
        let state = match TrackerState::reconstruct(db, &[], None) {
            Ok(state) => state,
            Err(error) => {
                tracing::error!(
                    ?error,
                    "could not reconstruct the highest completed checkpoint; clearing checkpoint progress"
                );
                TrackerState::default()
            }
        };
        let (sender, receiver) = watch::channel(state.current);
        (Self { state, sender }, receiver)
    }

    /// Returns the latest checkpoint made durable by a successful write.
    pub fn current(&self) -> Option<HighestCompletedCheckpoint> {
        self.state.current
    }

    /// Computes the post-commit tracker state using pending headers.
    ///
    /// Passes the current tracker as `start_hint` so reconstruct resumes (path A) and
    /// only walks from the cursor through the new pending range (path D).
    /// Empty batches and batches with an unknown anchor leave the proposed state unchanged.
    /// A conflicting batch uses its final height as the proposed tip and re-walks from the
    /// completed checkpoint when the conflict reaches the current cursor.
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

        let Some(anchor_height) = Self::anchor_height(db, anchor) else {
            // Header-range validation reports the unknown anchor before this proposal is used.
            return Ok(ProposedHighestCompletedCheckpoint(self.state));
        };

        let mut pending_headers = Vec::with_capacity(headers.len());
        for (index, header) in headers.iter().enumerate() {
            let offset = u32::try_from(index + 1)
                .map_err(|_| HighestCompletedCheckpointError::HeightOverflow)?;
            let height = (anchor_height + i64::from(offset))
                .ok_or(HighestCompletedCheckpointError::HeightOverflow)?;
            pending_headers.push((
                height,
                block::Hash::from(header.as_ref()),
                Arc::clone(header),
            ));
        }

        // First height where pending disagrees with durable headers (reorg into the range).
        let first_conflict = pending_headers.iter().find_map(|(height, hash, _)| {
            db.header_hash(*height)
                .is_some_and(|stored| stored != *hash)
                .then_some(*height)
        });
        // After a conflict, tip is the end of the pending batch; otherwise max(disk, pending).
        let post_tip = if first_conflict.is_some() {
            pending_headers.last().map(|(height, _, _)| *height)
        } else {
            db.best_header_tip()
                .map(|(height, _)| height)
                .into_iter()
                .chain(pending_headers.last().map(|(height, _, _)| *height))
                .max()
        };

        let mut start_hint = self.state;
        // Reorg at or below the cursor: rewind the cursor to `current` so path D
        // re-walks the disputed range instead of trusting the stale cursor.
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
            TrackerState::reconstruct(db, &pending_headers, Some((start_hint, post_tip)))?,
        ))
    }

    /// Rejects a reorg that would replace a completed checkpoint or any of its ancestors.
    ///
    /// Conflicts above the completed checkpoint are mutable and therefore allowed. An unknown
    /// anchor is also allowed because header-range validation rejects it before commit.
    pub fn check_immutable_conflicts(
        &self,
        db: &ZakuraDb,
        anchor: block::Hash,
        headers: &[Arc<block::Header>],
    ) -> Result<(), Height> {
        let Some(completed) = self.current() else {
            return Ok(());
        };
        let Some(anchor_height) = Self::anchor_height(db, anchor) else {
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
    ///
    /// Hints with the prior state (path A) so body progress can fast-forward via
    /// path C without rescanning checkpoints from genesis.
    ///
    /// Clears the published checkpoint before returning a reconstruction error.
    pub fn rebind_from_db(&mut self, db: &ZakuraDb) -> Result<(), HighestCompletedCheckpointError> {
        match TrackerState::reconstruct(db, &[], Some((self.state, None))) {
            Ok(state) => {
                self.replace_state(state);
                Ok(())
            }
            Err(error) => {
                // A stale completed checkpoint could authorize data that the durable
                // header store no longer justifies, so reconstruction errors fail closed.
                self.replace_state(TrackerState::default());
                Err(error)
            }
        }
    }

    /// Test-only: clear published progress the same way reconstruction failure does.
    #[cfg(test)]
    pub(crate) fn clear_published_for_test(&mut self) {
        self.replace_state(TrackerState::default());
    }

    /// Returns a sender clone that keeps subscriptions open without retaining the tracker.
    pub(crate) fn keepalive_sender(&self) -> watch::Sender<Option<HighestCompletedCheckpoint>> {
        self.sender.clone()
    }

    fn replace_state(&mut self, state: TrackerState) {
        let changed = self.state.current != state.current;
        self.state = state;
        if changed {
            let _ = self.sender.send(state.current);
        }
    }

    fn anchor_height(db: &ZakuraDb, anchor: block::Hash) -> Option<Height> {
        db.header_height(anchor)
            .or_else(|| (anchor == db.network().genesis_hash()).then_some(Height::MIN))
    }
}

impl TrackerState {
    /// Rebuilds tracker state for `[genesis, canonical_tip]`.
    ///
    /// Call paths:
    /// - startup / cold open: `start_hint = None` → `from_finalized_body`, then walk headers
    /// - post-commit proposal / rebind: `start_hint = Some(prior state)` → resume from
    ///   `current` / `next_checkpoint` / `cursor` instead of rescanning all checkpoints
    fn reconstruct(
        db: &ZakuraDb,
        pending: &[(Height, block::Hash, Arc<block::Header>)],
        start_hint: Option<(TrackerState, Option<Height>)>,
    ) -> Result<Self, HighestCompletedCheckpointError> {
        // Tip is either the proposed post-commit height or the durable header tip.
        let disk_best_header_tip = db.best_header_tip().map(|(height, _)| height);
        let canonical_tip = start_hint.and_then(|(_, tip)| tip).or(disk_best_header_tip);
        let Some(canonical_tip) = canonical_tip else {
            // Empty header store: nothing completed yet.
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

        // Path A — resume: reuse prior in-memory progress when `current` still matches disk.
        // Path B — cold start / invalid hint: jump to body tip via checkpoint_at_or_before
        // (no full checkpoint-list iteration).
        let mut state = match hinted_state {
            Some(state)
                if state.current.is_some_and(|current| {
                    current.height <= canonical_tip
                        && Self::validate_checkpoint(db, current).is_ok()
                }) =>
            {
                state
            }
            _ => Self::from_finalized_body(db, canonical_tip, genesis_hash)?,
        };

        // Drop a stale cursor (past tip, or hash no longer canonical / pending) back to
        // the last completed checkpoint so the walk below re-validates from a known point.
        if let Some((cursor_height, cursor_hash)) = state.cursor {
            if cursor_height > canonical_tip
                || Self::header_hash(db, pending, cursor_height) != Some(cursor_hash)
            {
                state.cursor = state
                    .current
                    .map(|checkpoint| (checkpoint.height, checkpoint.hash));
            }
        }

        // Path C — body fast-forward: finalized bodies cover whole brackets, so advance
        // `current` / `next_checkpoint` / `cursor` with a single lookup instead of walking
        // every intermediate header or checkpoint.
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

        // Path D — incremental header walk from cursor → tip only. Completes a checkpoint
        // when the walk hits `next_checkpoint`; otherwise just advances the cursor.
        while cursor_height < canonical_tip {
            let next_height = cursor_height
                .next()
                .map_err(|_| HighestCompletedCheckpointError::HeightOverflow)?;
            let Some((hash, header)) = pending
                .get(&next_height)
                .map(|(hash, header)| (*hash, Arc::clone(header)))
                .or_else(|| db.header_by_height(next_height))
            else {
                // Gap in the header chain: stop; do not invent progress past the hole.
                break;
            };
            if block::Hash::from(header.as_ref()) != hash
                || header.previous_block_hash != cursor_hash
            {
                // Non-canonical or broken parent link: stop at the last continuous height.
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

    /// Builds the cold-start tracker base when no valid in-memory hint is available (path B in reconstruct()).
    ///
    /// `current` is the highest configured checkpoint covered by the durable finalized
    /// body tip at or below `canonical_tip`, or genesis when no such body exists.
    /// `cursor` is that body tip (or genesis) so a later header walk only covers
    /// body→tip. Does not scan the full checkpoint list or walk headers.
    ///
    /// # Errors
    ///
    /// Returns if the body-tip header is missing, the checkpoint list lacks genesis,
    /// or the completed checkpoint does not match the canonical header store.
    fn from_finalized_body(
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
