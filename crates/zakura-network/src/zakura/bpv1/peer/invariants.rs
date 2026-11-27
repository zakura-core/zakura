//! `PeerState` Watch invariants, checked by the
//! tests and the fuzz replay after every step.

use zakura_rt::collections::HashSet;

use super::*;

/// The fuzz property that `bpv1_peer` registers. Every failed invariant names it,
/// so a campaign reports which property broke.
const PEER_REPLAY: &str = "bpv1/peer-replay";

impl PeerState {
    /// Checks every `PeerState` Watch invariant.
    pub fn assert_invariants(&self) {
        let hard_wants_in = MAX_WANTS_INFLIGHT.saturating_mul(2) as usize;
        assert!(
            self.wants_in.len() + self.unwritten_ends.len() <= hard_wants_in,
            "{PEER_REPLAY}: bpv1-49 live wants_in {} plus unwritten ends {} exceeded 2x limit {}",
            self.wants_in.len(),
            self.unwritten_ends.len(),
            hard_wants_in
        );
        assert!(
            self.wants_out.len() <= MAX_WANTS_INFLIGHT as usize,
            "{PEER_REPLAY}: live wants_out {} exceeded max {}",
            self.wants_out.len(),
            MAX_WANTS_INFLIGHT
        );
        assert!(
            self.early_data_bytes <= self.config.early_peer_bytes_cap,
            "{PEER_REPLAY}: bpv1-48 early_data_bytes {} exceeded cap {}",
            self.early_data_bytes,
            self.config.early_peer_bytes_cap
        );
        let recomputed_early: usize = self.early_data.iter().map(|e| e.kind.held_bytes()).sum();
        assert_eq!(
            self.early_data_bytes, recomputed_early,
            "{PEER_REPLAY}: early_data_bytes accounting mismatch"
        );
        assert!(
            self.flags_lost.len() <= MAX_PENDING_META_COUNT,
            "{PEER_REPLAY}: bpv1-60 lost-flag marks {} exceeded {MAX_PENDING_META_COUNT}",
            self.flags_lost.len()
        );
        assert!(
            self.meta_dropped.len() <= MAX_PENDING_META_COUNT,
            "{PEER_REPLAY}: bpv1-14 dropped-metadata marks {} exceeded {MAX_PENDING_META_COUNT}",
            self.meta_dropped.len()
        );
        // bpv1-14: the early record holds only traffic behind the peer's
        // `HeaderMeta` while local admission runs.
        assert!(
            self.early_data
                .iter()
                .all(|e| self.meta_received_pending.contains(&e.kind.block_id())),
            "{PEER_REPLAY}: bpv1-14 the early record holds traffic with no pending HeaderMeta"
        );
        assert!(
            self.early_control.len() <= EARLY_CONTROL_CAP,
            "{PEER_REPLAY}: early_control len {} exceeded cap {}",
            self.early_control.len(),
            EARLY_CONTROL_CAP
        );
        // Outgoing live Want sets must be pairwise disjoint per (block_id, stripe) (bpv1-28).
        let out_vec: Vec<&OutgoingWant> = self.wants_out.values().collect();
        for i in 0..out_vec.len() {
            let w = out_vec[i];
            assert!(
                w.received.iter().all(|slot| w.requested.contains(slot)),
                "{PEER_REPLAY}: received slots outside the requested set"
            );
            for other in &out_vec[(i + 1)..] {
                if w.block_id == other.block_id && w.stripe == other.stripe {
                    assert!(
                        !w.requested.intersects(&other.requested),
                        "{PEER_REPLAY}: bpv1-28 overlapping live wants_out"
                    );
                }
            }
        }
        // The peer's live Wants, including those whose end is unwritten, have
        // distinct ids and pairwise disjoint sets per (block_id, stripe) (bpv1-28).
        let in_vec: Vec<(u32, &IncomingWant)> = self.live_wants_in_iter().collect();
        for i in 0..in_vec.len() {
            for other in &in_vec[(i + 1)..] {
                assert_ne!(
                    in_vec[i].0, other.0,
                    "{PEER_REPLAY}: bpv1-28 two live wants_in share an id"
                );
                if in_vec[i].1.block_id == other.1.block_id && in_vec[i].1.stripe == other.1.stripe
                {
                    assert!(
                        !in_vec[i].1.requested.intersects(&other.1.requested),
                        "{PEER_REPLAY}: bpv1-28 overlapping live wants_in"
                    );
                }
            }
        }
        for (block_id, block) in &self.blocks {
            let relay = block.relay_bound();
            // The proposer flags up to `n` of its seeds; a holder up to `n − k`.
            let local_bound = if block.as_proposer {
                block.coding.n
            } else {
                relay
            };
            for stripe in 0..block.coding.stripes {
                // bpv1-69: per stripe, flags stay within each sender's push
                // bound, and relay flags within `n − k`.
                assert!(
                    block.stripe_count(&block.peer_flagged, stripe)
                        <= block.push_bound(self.remote_node)
                        && block.stripe_count(&block.local_flagged, stripe)
                            <= local_bound
                        && block.stripe_count(&block.peer_relayed, stripe) <= relay,
                    "{PEER_REPLAY}: bpv1-69 push flags past the bound in stripe {stripe} of {block_id:?}"
                );
            }
            // Widening u32 to usize is lossless on supported targets.
            for i in 0..block.coding.total_parts().unwrap_or(0) as usize {
                // bpv1-72: a flagged index was announced, and its part sent.
                assert!(
                    !block.local_flagged.get(i)
                        || (block.local_have_sent.get(i) && block.send_mark.get(i)),
                    "{PEER_REPLAY}: bpv1-72 pushed index {i} without its flag"
                );
                // bpv1-71: the proposer is never pushed to.
                assert!(
                    !block.local_flagged.get(i) || self.remote_node != block.proposer_node,
                    "{PEER_REPLAY}: bpv1-71 pushed to the proposer"
                );
                // bpv1-73: before the advertisement deadline, only flagged
                // `Have`s go out, except to the proposer (decision-32).
                assert!(
                    block.have_held_until.is_none()
                        || !block.local_have_sent.get(i)
                        || block.local_flagged.get(i)
                        || self.remote_node == block.proposer_node,
                    "{PEER_REPLAY}: bpv1-73 plain Have for index {i} of {block_id:?} before the deadline"
                );
            }
            // bpv1-8: HeaderMeta crossed from admission on, so no Have,
            // Part, or Done goes out before it.
            assert!(
                block.meta_sent || block.meta_received,
                "{PEER_REPLAY}: bpv1-8 admitted {block_id:?} before HeaderMeta crossed"
            );
        }
        // bpv1-50: proof work runs only for an accepted part, once per part.
        let mut verifying = HashSet::new();
        for job in self.inflight_verify.values() {
            assert!(
                self.blocks
                    .get(&job.block_id)
                    .is_some_and(|b| b.recv_record.get(job.index as usize)),
                "{PEER_REPLAY}: bpv1-50 proof job for a part the session never accepted"
            );
            assert!(
                verifying.insert((job.block_id, job.index)),
                "{PEER_REPLAY}: bpv1-50 two proof jobs for one part"
            );
        }
    }

    /// Checks that `SessionEnd` returned every per-peer structure (bpv1-62).
    pub fn assert_drained(&self) {
        assert!(
            self.early_data_bytes == 0
                && self.early_data.is_empty()
                && self.early_control.is_empty()
                && self.flags_lost.is_empty()
                && self.wants_out.is_empty()
                && self.wants_in.is_empty()
                && self.blocks.is_empty()
                && self.retired.is_empty()
                && self.meta_received_pending.is_empty()
                && self.meta_dropped.is_empty()
                && self.inflight_verify.is_empty()
                && self.unwritten_ends.is_empty(),
            "{PEER_REPLAY}: bpv1-62 SessionEnd left per-peer state behind"
        );
    }
}
