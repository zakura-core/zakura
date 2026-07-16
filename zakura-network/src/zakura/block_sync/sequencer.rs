//! The serial commit pipeline for Zakura block sync.
//!
//! The [`Sequencer`] owns the consensus-critical reorder → applying →
//! `SubmitBlock` → apply-finished machinery and nothing else. It deliberately
//! never touches download-side state — the byte budget, the work scheduler,
//! peers, emitted actions, or state queries. Two rules keep that boundary clean:
//!
//! - every method that drops held bodies *returns* the freed byte count. Held
//!   bodies carry no wire-budget charge (retention is bounded by the resident
//!   look-ahead gate), so the count is accounting for callers and tests, not a
//!   budget-release obligation, and
//! - every download-side consequence (mark a height covered, clear covered,
//!   re-query, attribute misbehavior) is expressed as a value the reactor acts
//!   on, not performed here.
//!
//! Keeping the Sequencer free of download-side state is what lets it run on its
//! own serial task ([`super::sequencer_task`]), off the reactor's thread.

use super::{events::BlockApplyToken, reorder::*, retained_memory::RetainedCharge, state::*, *};

/// A received body draining contiguously toward the verified tip, awaiting (or
/// undergoing) verifier submission.
///
/// Bodies beyond the submission window are held in their serialized wire form
/// (`BufferedBlockBody::RawFramePayload`) and only decoded at `prepare_submit`,
/// so the decoded heap footprint of the apply backlog is bounded by the
/// submission window instead of the whole backlog (the incident mode was tens
/// of thousands of decoded bodies resident at once).
#[derive(Debug)]
pub(super) struct ApplyingBlock {
    pub(super) token: BlockApplyToken,
    pub(super) hash: block::Hash,
    /// `previous_block_hash` captured at receipt, so reset-conflict checks never
    /// need to decode a backlog body.
    pub(super) previous_block_hash: block::Hash,
    pub(super) body: BufferedBlockBody,
    pub(super) bytes: u64,
    pub(super) submitted: bool,
    /// The peer that delivered this body, used to attribute an apply rejection
    /// for misbehavior scoring.
    pub(super) source_peer: ZakuraPeerId,
}

/// Outcome of offering a received body to the commit pipeline.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum AcceptOutcome {
    /// The body was buffered. The reactor must mark `covered` covered in the
    /// download scheduler so the retry path stops re-requesting it.
    Buffered { covered: block::Height },
    /// The body was not buffered (already at/below the floor, held elsewhere in
    /// the commit pipeline, or a duplicate). `release_bytes` is the dropped
    /// body's size; held bodies carry no wire-budget charge, so it is
    /// informational.
    Redundant { release_bytes: u64 },
}

/// A body the Sequencer has assigned a token and marked submitted; the reactor
/// dispatches the matching `SubmitBlock` action.
#[derive(Clone, Debug)]
pub(super) struct SubmitItem {
    pub(super) height: block::Height,
    pub(super) hash: block::Hash,
    pub(super) token: BlockApplyToken,
    pub(super) block: Arc<block::Block>,
}

/// Accounting identity for a decoded body handed to the driver.
///
/// The driver can retain the matching `Arc<Block>` after the body is detached
/// from `applying`, so this record lives until the exact completion arrives.
#[derive(Clone, Debug)]
struct InFlightSubmission {
    height: block::Height,
    hash: block::Hash,
    /// Serialized block size received from the wire.
    bytes: u64,
    /// Deterministic size attributed to the decoded block's Rust object graph.
    decoded_attributed_memory_bytes: u64,
    /// Keeps the decoded body charged while the driver can retain its `Arc<Block>`.
    _retained_charge: RetainedCharge,
    /// The block left `applying`, but the driver can still retain its decoded `Arc<Block>`.
    detached: bool,
}

/// Sequencer half of a verified-tip advance (frontier growth/commit).
#[derive(Copy, Clone, Debug)]
pub(super) struct AdvanceOutcome {
    /// Bytes freed from the reorder/applying buffers (informational; held bodies
    /// carry no wire-budget charge).
    #[cfg_attr(not(test), allow(dead_code))] // asserted by sequencer unit tests
    pub(super) release_bytes: u64,
    /// Whether the verified tip actually moved. The reactor drops download state
    /// (scheduler/outstanding) and re-drains only when it did.
    pub(super) changed: bool,
}

/// The reorder → applying → submit → apply-finished commit pipeline.
#[derive(Debug)]
pub(super) struct Sequencer {
    reorder: ReorderBuffer,
    applying: BTreeMap<block::Height, ApplyingBlock>,
    submitted_applies: BTreeMap<block::Height, Vec<(block::Hash, usize)>>,
    in_flight_submissions: BTreeMap<BlockApplyToken, InFlightSubmission>,
    next_apply_token: BlockApplyToken,

    /// Running totals maintained incrementally so `publish_view` stays O(1).
    ///
    /// In-flight-submission totals include detached submissions still retained
    /// by the driver. `attached_submission_count` is only the submitted subset
    /// still in `applying`, used to derive the unsubmitted applying count.
    applying_buffered_bytes: u64,
    applying_decoded_attributed_memory_bytes: u64,
    detached_submission_decoded_attributed_memory_bytes: u64,
    attached_submission_count: usize,
    in_flight_submission_count: usize,
    in_flight_submission_bytes: u64,

    // The highest block height whose body has already been accepted into the contiguous
    // download-apply pipeline.
    body_download_floor: block::Height,
    verified_block_tip: block::Height,
    submitted_apply_limit: usize,
}

impl Sequencer {
    pub(super) fn new(verified_block_tip: block::Height, submitted_apply_limit: usize) -> Self {
        Self {
            reorder: ReorderBuffer::new(),
            applying: BTreeMap::new(),
            submitted_applies: BTreeMap::new(),
            in_flight_submissions: BTreeMap::new(),
            next_apply_token: 1,
            applying_buffered_bytes: 0,
            applying_decoded_attributed_memory_bytes: 0,
            detached_submission_decoded_attributed_memory_bytes: 0,
            attached_submission_count: 0,
            in_flight_submission_count: 0,
            in_flight_submission_bytes: 0,
            body_download_floor: verified_block_tip,
            verified_block_tip,
            submitted_apply_limit,
        }
    }

    // ---- reads (download side queries the commit pipeline through these) ----

    pub(super) fn floor(&self) -> block::Height {
        self.body_download_floor
    }

    pub(super) fn verified_tip(&self) -> block::Height {
        self.verified_block_tip
    }

    #[cfg(test)]
    pub(super) fn reorder_contains(&self, height: block::Height) -> bool {
        self.reorder.contains(height)
    }

    #[cfg(test)]
    pub(super) fn applying_contains(&self, height: block::Height) -> bool {
        self.applying.contains_key(&height)
    }

    #[cfg(test)]
    pub(super) fn submitted_contains(&self, height: block::Height) -> bool {
        self.submitted_applies.contains_key(&height)
    }

    pub(super) fn reorder_len(&self) -> usize {
        self.reorder.len()
    }

    pub(super) fn applying_len(&self) -> usize {
        self.applying.len()
    }

    pub(super) fn applying_buffered_bytes(&self) -> u64 {
        self.applying_buffered_bytes
    }

    pub(super) fn applying_decoded_attributed_memory_bytes(&self) -> u64 {
        self.applying_decoded_attributed_memory_bytes
            .saturating_add(self.detached_submission_decoded_attributed_memory_bytes)
    }

    #[cfg(test)]
    pub(super) fn applying_decoded_attributed_memory_bytes_scanned(&self) -> u64 {
        let attached = self
            .applying
            .values()
            .map(|applying| applying.body.decoded_attributed_memory_size_bytes())
            .fold(0u64, u64::saturating_add);
        let detached = self
            .in_flight_submissions
            .values()
            .filter(|submission| submission.detached)
            .map(|submission| submission.decoded_attributed_memory_bytes)
            .fold(0u64, u64::saturating_add);
        attached.saturating_add(detached)
    }

    /// Number of `applying` bodies currently holding a decoded block, which the
    /// bounded decode window keeps near `submitted_apply_limit`.
    #[cfg(test)]
    pub(super) fn decoded_applying_count(&self) -> usize {
        self.applying
            .values()
            .filter(|applying| applying.body.is_decoded())
            .count()
    }

    /// Ground-truth recomputation of [`applying_buffered_bytes`], used by tests to
    /// assert the maintained counter never drifts.
    #[cfg(test)]
    pub(super) fn applying_buffered_bytes_scanned(&self) -> u64 {
        self.applying
            .values()
            .map(|applying| applying.bytes)
            .fold(0u64, u64::saturating_add)
    }

    pub(super) fn reorder_buffered_bytes(&self) -> u64 {
        self.reorder.buffered_bytes()
    }

    pub(super) fn reorder_decoded_attributed_memory_bytes(&self) -> u64 {
        self.reorder.decoded_attributed_memory_bytes()
    }

    #[cfg(test)]
    pub(super) fn reorder_decoded_attributed_memory_bytes_scanned(&self) -> u64 {
        self.reorder.decoded_attributed_memory_bytes_scanned()
    }

    pub(super) fn unsubmitted_applying_count(&self) -> usize {
        // Derived: every applying body is either submitted or not.
        self.applying
            .len()
            .saturating_sub(self.attached_submission_count)
    }

    /// Wire bytes of all decoded submissions the driver can still retain,
    /// including submissions detached from `applying`.
    pub(super) fn in_flight_submission_bytes(&self) -> u64 {
        self.in_flight_submission_bytes
    }

    /// Number of decoded submissions the driver can still retain.
    pub(super) fn in_flight_submission_count(&self) -> usize {
        self.in_flight_submission_count
    }

    /// Ground-truth recomputations of the in-flight-submission counters, for tests.
    #[cfg(test)]
    pub(super) fn in_flight_submission_count_scanned(&self) -> usize {
        self.in_flight_submissions.len()
    }

    #[cfg(test)]
    pub(super) fn in_flight_submission_bytes_scanned(&self) -> u64 {
        self.in_flight_submissions
            .values()
            .map(|submission| submission.bytes)
            .fold(0u64, u64::saturating_add)
    }

    #[cfg(test)]
    pub(super) fn attached_submission_count_scanned(&self) -> usize {
        self.applying
            .values()
            .filter(|applying| applying.submitted)
            .count()
    }

    pub(super) fn has_submitted_apply(&self, height: block::Height, hash: block::Hash) -> bool {
        self.submitted_applies
            .get(&height)
            .is_some_and(|entries| entries.iter().any(|(entry_hash, _)| *entry_hash == hash))
    }

    /// Whether any reorder/applying/submitted body sits at or above `height`,
    /// used by the reactor to decide whether a reset is anchored by active
    /// successor work.
    pub(super) fn has_buffered_at_or_above(&self, height: block::Height) -> bool {
        self.reorder.contains_at_or_above(height)
            || self.applying.range(height..).next().is_some()
            || self.submitted_applies.range(height..).next().is_some()
    }

    /// `previous_block_hash` of a held `applying` body, for deciding whether a
    /// reset orphans an already-submitted successor.
    pub(super) fn applying_previous_block_hash(
        &self,
        height: block::Height,
    ) -> Option<block::Hash> {
        self.applying
            .get(&height)
            .map(|applying| applying.previous_block_hash)
    }

    pub(super) fn reorder_hash(&self, height: block::Height) -> Option<block::Hash> {
        self.reorder.hash(height)
    }

    pub(super) fn applying_hash(&self, height: block::Height) -> Option<block::Hash> {
        self.applying.get(&height).map(|applying| applying.hash)
    }

    /// True when `height` has submitted applies and *none* of them is `hash`
    /// (a reset to `hash` would conflict with our submitted work).
    pub(super) fn submitted_has_only_other_hashes(
        &self,
        height: block::Height,
        hash: block::Hash,
    ) -> bool {
        self.submitted_applies
            .get(&height)
            .is_some_and(|entries| entries.iter().all(|(entry_hash, _)| *entry_hash != hash))
    }

    // ---- body acceptance ----

    /// Offer a received body to the commit pipeline. Runs the redundancy checks
    /// and (when not redundant) buffers it in the reorder buffer with its wire
    /// `bytes` for the retained-size accounting.
    #[cfg(test)]
    pub(super) fn accept_body(
        &mut self,
        height: block::Height,
        hash: block::Hash,
        block: Arc<block::Block>,
        bytes: u64,
        source_peer: ZakuraPeerId,
    ) -> AcceptOutcome {
        let previous_block_hash = block.header.previous_block_hash;
        self.accept_buffered_body(
            height,
            hash,
            previous_block_hash,
            BufferedBlockBody::from_decoded_block(block, None),
            bytes,
            source_peer,
        )
    }

    pub(super) fn accept_buffered_body(
        &mut self,
        height: block::Height,
        hash: block::Hash,
        previous_block_hash: block::Hash,
        body: BufferedBlockBody,
        bytes: u64,
        source_peer: ZakuraPeerId,
    ) -> AcceptOutcome {
        if height <= self.body_download_floor
            || self.reorder.contains(height)
            || self.applying.contains_key(&height)
            || self.has_submitted_apply(height, hash)
        {
            return AcceptOutcome::Redundant {
                release_bytes: bytes,
            };
        }

        // Decide how much of the received body to keep before putting it in the reorder
        // buffer.
        // If height is the next block in the sequence, we can keep the whole body.
        // Otherwise, we need to retain the body for the backlog.
        let body = if next_height(self.body_download_floor) == Some(height) {
            body
        } else {
            body.retain_for_backlog()
        };

        match self
            .reorder
            .insert_body(height, hash, previous_block_hash, body, bytes, source_peer)
        {
            ReorderInsertResult::Inserted => AcceptOutcome::Buffered { covered: height },
            ReorderInsertResult::Duplicate => AcceptOutcome::Redundant {
                release_bytes: bytes,
            },
        }
    }

    // ---- drain reorder → applying ----

    /// Drain the contiguous reorder prefix above the floor into `applying`,
    /// advancing the floor. Returns the newly-covered heights so the reactor
    /// marks them covered in the download scheduler.
    ///
    /// Bodies stay in their buffered (usually serialized) form. The only
    /// exception is the front of the drain that will be submitted immediately:
    /// those keep an already-present decoded copy so the common steady-state
    /// path (receive next block → drain → submit) never decodes twice. Bodies
    /// past the free submission slots are demoted to raw bytes, so the decoded
    /// backlog stays bounded by the submission window.
    pub(super) fn drain_ready_into_applying(&mut self) -> Vec<block::Height> {
        let released = self
            .reorder
            .drain_contiguous_prefix(self.body_download_floor);
        let mut free_submit_slots = self
            .submitted_apply_limit
            .saturating_sub(self.applying.len());
        let mut covered = Vec::with_capacity(released.len());
        for drained in released {
            let mut body = drained.body;
            if free_submit_slots > 0 {
                free_submit_slots -= 1;
            } else {
                body.retain_for_backlog_in_place();
            }
            self.body_download_floor = drained.height;
            covered.push(drained.height);
            let decoded_attributed_memory_size_bytes = body.decoded_attributed_memory_size_bytes();
            self.applying.insert(
                drained.height,
                ApplyingBlock {
                    token: 0,
                    hash: drained.hash,
                    previous_block_hash: drained.previous_block_hash,
                    body,
                    bytes: drained.bytes,
                    submitted: false,
                    source_peer: drained.source_peer,
                },
            );
            // New bodies enter `applying` unsubmitted, so only the total grows.
            self.applying_buffered_bytes =
                self.applying_buffered_bytes.saturating_add(drained.bytes);
            self.applying_decoded_attributed_memory_bytes = self
                .applying_decoded_attributed_memory_bytes
                .saturating_add(decoded_attributed_memory_size_bytes);
        }
        covered
    }

    // ---- submission ----

    /// The unsubmitted `applying` heights eligible for verifier submission,
    /// bounded by the remaining submission window.
    pub(super) fn submittable_heights(&self) -> Vec<block::Height> {
        let available = self
            .submitted_apply_limit
            .saturating_sub(self.in_flight_submission_count());
        if available == 0 {
            return Vec::new();
        }
        self.applying
            .iter()
            .filter_map(|(height, applying)| (!applying.submitted).then_some(*height))
            .take(available)
            .collect()
    }

    /// Assign a token to `height`, mark it submitted, and return the dispatch
    /// item. `None` if the height is no longer applying (the token counter is
    /// not consumed in that case).
    ///
    /// Submission is the decode point: a body still held as raw bytes is decoded
    /// here (and cached on the entry until apply-finished or unsubmit), so at
    /// most `submitted_apply_limit` decoded bodies are resident at once.
    pub(super) fn prepare_submit(&mut self, height: block::Height) -> Option<SubmitItem> {
        if self
            .applying
            .get(&height)
            .is_none_or(|applying| applying.submitted)
        {
            return None;
        }
        let token = self.next_apply_token();
        let (
            hash,
            bytes,
            block,
            retained_charge,
            decoded_attributed_memory_size_bytes,
            newly_decoded_attributed_memory_bytes,
        ) = {
            let applying = self.applying.get_mut(&height)?;
            let decoded_before = applying.body.decoded_attributed_memory_size_bytes();
            let block = applying.body.decoded_block();
            let decoded_after = applying.body.decoded_attributed_memory_size_bytes();
            applying.token = token;
            applying.submitted = true;
            (
                applying.hash,
                applying.bytes,
                block,
                applying.body.retained_charge(),
                decoded_after,
                decoded_after.saturating_sub(decoded_before),
            )
        };
        self.applying_decoded_attributed_memory_bytes = self
            .applying_decoded_attributed_memory_bytes
            .saturating_add(newly_decoded_attributed_memory_bytes);
        self.attached_submission_count = self.attached_submission_count.saturating_add(1);
        self.in_flight_submissions.insert(
            token,
            InFlightSubmission {
                height,
                hash,
                bytes,
                decoded_attributed_memory_bytes: decoded_attributed_memory_size_bytes,
                _retained_charge: retained_charge,
                detached: false,
            },
        );
        self.in_flight_submission_count = self.in_flight_submission_count.saturating_add(1);
        self.in_flight_submission_bytes = self.in_flight_submission_bytes.saturating_add(bytes);
        Some(SubmitItem {
            height,
            hash,
            token,
            block,
        })
    }

    /// Roll back a submit whose dispatch failed (only if the token still matches,
    /// so a stale rollback cannot clobber a newer submission).
    pub(super) fn unsubmit(&mut self, height: block::Height, token: BlockApplyToken) {
        let unsubmitted = {
            let Some(applying) = self.applying.get_mut(&height) else {
                return;
            };
            if applying.token != token {
                return;
            }
            // Only a currently-submitted body affects the submitted counters; if the
            // matched token was already rolled back, just clear it.
            let was_submitted = applying.submitted;
            applying.token = 0;
            applying.submitted = false;
            // The body leaves the decode window: drop the decoded copy so a
            // rolled-back submission does not grow the decoded backlog.
            let decoded_before = applying.body.decoded_attributed_memory_size_bytes();
            applying.body.retain_for_backlog_in_place();
            let decoded_after = applying.body.decoded_attributed_memory_size_bytes();
            was_submitted.then_some((
                applying.hash,
                applying.bytes,
                decoded_before.saturating_sub(decoded_after),
            ))
        };
        if let Some((hash, bytes, decoded_attributed_memory_size_bytes)) = unsubmitted {
            self.attached_submission_count = self.attached_submission_count.saturating_sub(1);
            self.applying_decoded_attributed_memory_bytes = self
                .applying_decoded_attributed_memory_bytes
                .saturating_sub(decoded_attributed_memory_size_bytes);
            self.release_in_flight_submission(token, height, hash, bytes);
        }
    }

    fn next_apply_token(&mut self) -> BlockApplyToken {
        let token = self.next_apply_token;
        self.next_apply_token = self.next_apply_token.checked_add(1).unwrap_or(1);
        token
    }

    pub(super) fn record_submitted_apply(&mut self, height: block::Height, hash: block::Hash) {
        let entries = self.submitted_applies.entry(height).or_default();
        if let Some((_, count)) = entries
            .iter_mut()
            .find(|(entry_hash, _)| *entry_hash == hash)
        {
            *count = count.saturating_add(1);
        } else {
            entries.push((hash, 1));
        }
    }

    fn decrement_submitted_apply(&mut self, height: block::Height, hash: block::Hash) {
        let Some(entries) = self.submitted_applies.get_mut(&height) else {
            return;
        };
        if let Some(index) = entries
            .iter()
            .position(|(entry_hash, _)| *entry_hash == hash)
        {
            let (_, count) = &mut entries[index];
            *count = count.saturating_sub(1);
            if *count == 0 {
                entries.remove(index);
            }
        }
        if entries.is_empty() {
            self.submitted_applies.remove(&height);
        }
    }

    /// Release one driver-retained decoded submission only when its completion
    /// exactly matches the token identity assigned at dispatch.
    pub(super) fn finish_submission(
        &mut self,
        token: BlockApplyToken,
        height: block::Height,
        hash: block::Hash,
    ) -> bool {
        let Some(submission) = self.in_flight_submissions.get(&token) else {
            return false;
        };
        if submission.height != height || submission.hash != hash {
            return false;
        }
        let submission = self
            .in_flight_submissions
            .remove(&token)
            .expect("submission exists because it matched above");
        self.in_flight_submission_count = self.in_flight_submission_count.saturating_sub(1);
        self.in_flight_submission_bytes = self
            .in_flight_submission_bytes
            .saturating_sub(submission.bytes);
        if submission.detached {
            self.detached_submission_decoded_attributed_memory_bytes = self
                .detached_submission_decoded_attributed_memory_bytes
                .saturating_sub(submission.decoded_attributed_memory_bytes);
        }
        self.decrement_submitted_apply(height, hash);
        true
    }

    /// Finish a submission whose body must remain attached until a later
    /// frontier update removes it.
    ///
    /// The driver has released its decoded `Arc<Block>`, so downgrade the
    /// sequencer's copy to raw bytes before releasing the decode-window charge.
    /// Keep the applying entry marked submitted so it is not dispatched again.
    pub(super) fn finish_attached_submission(
        &mut self,
        token: BlockApplyToken,
        height: block::Height,
        hash: block::Hash,
    ) -> bool {
        let submission_matches = self
            .in_flight_submissions
            .get(&token)
            .is_some_and(|submission| submission.height == height && submission.hash == hash);
        if !submission_matches {
            return false;
        }
        let Some(applying) = self.applying.get_mut(&height) else {
            return false;
        };
        if applying.token != token || applying.hash != hash || !applying.submitted {
            return false;
        }
        let decoded_before = applying.body.decoded_attributed_memory_size_bytes();
        applying.body.retain_for_backlog_in_place();
        let decoded_after = applying.body.decoded_attributed_memory_size_bytes();
        self.applying_decoded_attributed_memory_bytes = self
            .applying_decoded_attributed_memory_bytes
            .saturating_sub(decoded_before.saturating_sub(decoded_after));
        self.finish_submission(token, height, hash)
    }

    fn release_in_flight_submission(
        &mut self,
        token: BlockApplyToken,
        height: block::Height,
        hash: block::Hash,
        bytes: u64,
    ) {
        let matches = self
            .in_flight_submissions
            .get(&token)
            .is_some_and(|submission| {
                submission.height == height && submission.hash == hash && submission.bytes == bytes
            });
        if matches {
            let submission = self
                .in_flight_submissions
                .remove(&token)
                .expect("submission exists because it matched above");
            self.in_flight_submission_count = self.in_flight_submission_count.saturating_sub(1);
            self.in_flight_submission_bytes = self.in_flight_submission_bytes.saturating_sub(bytes);
            if submission.detached {
                self.detached_submission_decoded_attributed_memory_bytes = self
                    .detached_submission_decoded_attributed_memory_bytes
                    .saturating_sub(submission.decoded_attributed_memory_bytes);
            }
        }
    }

    fn clear_submitted_applies_from(&mut self, from: block::Height) {
        let heights: Vec<_> = self
            .submitted_applies
            .range(from..)
            .map(|(height, _)| *height)
            .collect();
        for height in heights {
            self.submitted_applies.remove(&height);
        }
    }

    fn clear_submitted_applies_through(&mut self, tip: block::Height) {
        let heights: Vec<_> = self
            .submitted_applies
            .range(..=tip)
            .map(|(height, _)| *height)
            .collect();
        for height in heights {
            self.submitted_applies.remove(&height);
        }
    }

    // ---- apply finished ----

    /// The `(token, hash)` of the body currently applying at `height`, for
    /// validating an apply-finished completion against the in-flight submission.
    pub(super) fn applying_token_hash(
        &self,
        height: block::Height,
    ) -> Option<(BlockApplyToken, block::Hash)> {
        self.applying
            .get(&height)
            .map(|applying| (applying.token, applying.hash))
    }

    pub(super) fn remove_applying(&mut self, height: block::Height) -> Option<ApplyingBlock> {
        let mut removed = self.applying.remove(&height)?;
        let decoded_attributed_memory_bytes = removed.body.decoded_attributed_memory_size_bytes();
        self.applying_buffered_bytes = self.applying_buffered_bytes.saturating_sub(removed.bytes);
        self.applying_decoded_attributed_memory_bytes = self
            .applying_decoded_attributed_memory_bytes
            .saturating_sub(decoded_attributed_memory_bytes);
        if removed.submitted {
            removed.body.retain_for_driver_in_place();
            self.attached_submission_count = self.attached_submission_count.saturating_sub(1);
            if let Some(submission) = self.in_flight_submissions.get_mut(&removed.token) {
                if !submission.detached {
                    submission.detached = true;
                    self.detached_submission_decoded_attributed_memory_bytes = self
                        .detached_submission_decoded_attributed_memory_bytes
                        .saturating_add(submission.decoded_attributed_memory_bytes);
                }
            }
        }
        Some(removed)
    }

    /// After a rejected/timed-out apply at `height`, roll the download floor back
    /// below it — never below the verified tip — so the height is re-requestable.
    pub(super) fn reset_floor_below(&mut self, height: block::Height) {
        self.body_download_floor = previous_height(height)
            .unwrap_or(block::Height::MIN)
            .max(self.verified_block_tip);
    }

    /// Drop buffered reorder bodies at or above `from`; returns the freed bytes.
    pub(super) fn drop_reorder_from(&mut self, from: block::Height) -> u64 {
        self.reorder.drop_from(from)
    }

    /// Remove `applying` bodies at or above `from` and clear their duplicate
    /// suppression records; returns the freed bytes.
    ///
    /// In-flight submission charges remain until their exact completions arrive,
    /// because the driver can still retain the detached decoded blocks.
    pub(super) fn release_applying_blocks_from(&mut self, from: block::Height) -> u64 {
        let heights: Vec<_> = self
            .applying
            .range(from..)
            .map(|(height, _)| *height)
            .collect();
        let mut released = 0u64;
        for height in heights {
            if let Some(applying) = self.remove_applying(height) {
                released = released.saturating_add(applying.bytes);
            }
        }
        self.clear_submitted_applies_from(from);
        released
    }

    /// Remove committed `applying` bodies at or below `tip`; returns freed bytes.
    /// Detached in-flight submissions remain charged through completion.
    pub(super) fn release_applied_through(&mut self, tip: block::Height) -> u64 {
        let applied: Vec<_> = self
            .applying
            .range(..=tip)
            .map(|(height, _)| *height)
            .collect();
        self.clear_submitted_applies_through(tip);
        let mut released = 0u64;
        for height in applied {
            if let Some(applying) = self.remove_applying(height) {
                released = released.saturating_add(applying.bytes);
            }
        }
        released
    }

    // ---- frontier advance / reset ----

    /// Advance the verified tip to `new_tip` (frontier growth/commit). Bumps the
    /// floor unconditionally, drops superseded reorder bodies (and, when
    /// `release_applied`, committed applying bodies), and moves the verified tip.
    /// Returns the freed bytes and whether the tip moved.
    pub(super) fn advance_verified_tip(
        &mut self,
        new_tip: block::Height,
        release_applied: bool,
    ) -> AdvanceOutcome {
        self.body_download_floor = self.body_download_floor.max(new_tip);
        if new_tip == self.verified_block_tip {
            return AdvanceOutcome {
                release_bytes: 0,
                changed: false,
            };
        }
        let mut released = self.reorder.drop_through(new_tip);
        if release_applied {
            released = released.saturating_add(self.release_applied_through(new_tip));
        }
        self.verified_block_tip = new_tip;
        AdvanceOutcome {
            release_bytes: released,
            changed: true,
        }
    }

    /// Destructively reset the commit pipeline to `new_tip` (reorg/rollback):
    /// clear the reorder buffer and all applying bodies (optionally preserving
    /// duplicate-suppression records), and pin the floor and verified tip to
    /// `new_tip`. Driver-retained submissions remain charged through their exact
    /// completions. Returns the freed bytes.
    pub(super) fn reset_to(&mut self, new_tip: block::Height, keep_submitted_applies: bool) -> u64 {
        self.verified_block_tip = new_tip;
        self.body_download_floor = new_tip;
        let mut released = self.reorder.clear();
        released =
            released.saturating_add(self.release_all_applying_for_reset(keep_submitted_applies));
        released
    }

    fn release_all_applying_for_reset(&mut self, keep_submitted_applies: bool) -> u64 {
        let released = self.applying.values().map(|applying| applying.bytes).sum();
        for applying in self
            .applying
            .values_mut()
            .filter(|applying| applying.submitted)
        {
            applying.body.retain_for_driver_in_place();
            if let Some(submission) = self.in_flight_submissions.get_mut(&applying.token) {
                if !submission.detached {
                    submission.detached = true;
                    self.detached_submission_decoded_attributed_memory_bytes = self
                        .detached_submission_decoded_attributed_memory_bytes
                        .saturating_add(submission.decoded_attributed_memory_bytes);
                }
            }
        }
        if !keep_submitted_applies {
            self.submitted_applies.clear();
        }
        self.applying.clear();
        self.applying_buffered_bytes = 0;
        self.applying_decoded_attributed_memory_bytes = 0;
        self.attached_submission_count = 0;
        released
    }
}
