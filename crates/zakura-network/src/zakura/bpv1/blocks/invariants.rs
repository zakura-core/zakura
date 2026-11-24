//! `BlockTable` Watch invariants, checked by the
//! tests and the fuzz replay after every step.

use super::*;

/// The fuzz property that `bpv1_block` registers. Every failed invariant names it,
/// so a campaign reports which property broke.
const BLOCK_REPLAY: &str = "bpv1/block-replay";

impl BlockTable {
    /// Checks every `BlockTable` Watch invariant.
    pub fn assert_invariants(&self) {
        assert!(
            self.pending.len() <= MAX_PENDING_META_COUNT,
            "{BLOCK_REPLAY}: bpv1-11 pending meta count {} > cap {}",
            self.pending.len(),
            MAX_PENDING_META_COUNT
        );
        assert!(
            self.pending_bytes <= MAX_PENDING_META_BYTES,
            "{BLOCK_REPLAY}: bpv1-11 pending meta bytes {} > cap {}",
            self.pending_bytes,
            MAX_PENDING_META_BYTES
        );
        let recomputed_pending: usize = self
            .pending
            .values()
            .map(|e| e.envelope.estimated_bytes())
            .sum();
        assert_eq!(
            self.pending_bytes, recomputed_pending,
            "{BLOCK_REPLAY}: pending_bytes accounting mismatch"
        );

        // Every admitting or pending entry counts against the metadata caps.
        assert!(
            self.admitting.len() + self.pending.len() <= MAX_PENDING_META_COUNT,
            "{BLOCK_REPLAY}: bpv1-11 admitting {} + pending {} > cap {}",
            self.admitting.len(),
            self.pending.len(),
            MAX_PENDING_META_COUNT
        );
        // bpv1-52: charged part bytes stay within the part buffer budget.
        assert!(
            self.part_buffer_bytes() <= self.config.part_buffer_bytes,
            "{BLOCK_REPLAY}: bpv1-52 part buffer {} > budget {}",
            self.part_buffer_bytes(),
            self.config.part_buffer_bytes
        );

        // The `Propagation` lease the IO shell holds covers every retained
        // byte, and the reserve sized from `retained_bound` covers the lease.
        assert!(
            self.retained_bytes() <= self.lease_bytes()
                && self.lease_bytes() <= Self::retained_bound(&self.config),
            "{BLOCK_REPLAY}: retained bytes {} > lease {} or lease > bound {}",
            self.retained_bytes(),
            self.lease_bytes(),
            Self::retained_bound(&self.config)
        );

        // bpv1-50: every indexed header or commitment job belongs to its
        // admitting entry, so finished or abandoned work leaves no lease.
        for (work_id, block_id) in &self.work_to_block {
            assert!(
                self.admitting
                    .get(block_id)
                    .is_some_and(|entry| entry.work_id == *work_id),
                "{BLOCK_REPLAY}: bpv1-50 work index holds a job no admitting entry owns"
            );
        }

        let non_retired = self
            .blocks
            .values()
            .filter(|b| b.phase != BlockPhase::Retired)
            .count();
        assert!(
            non_retired <= self.config.max_concurrent_blocks,
            "{BLOCK_REPLAY}: concurrent non-retired blocks {} > cap {}",
            non_retired,
            self.config.max_concurrent_blocks
        );

        // mem-5: the reserve funds one assembled block per concurrent block
        // until its verdict.
        let verdicts = self
            .blocks
            .values()
            .filter(|b| b.verify_work_id.is_some())
            .count()
            .saturating_add(self.owed_verdicts.len());
        assert!(
            verdicts <= self.config.max_concurrent_blocks,
            "{BLOCK_REPLAY}: owed verdicts {} > cap {}",
            verdicts,
            self.config.max_concurrent_blocks
        );

        for block in self.blocks.values() {
            if block.phase == BlockPhase::Retired {
                assert_eq!(
                    block.retained_part_count(),
                    0,
                    "{BLOCK_REPLAY}: bpv1-46 retired block must hold zero part buffers"
                );
                assert!(
                    !block.body_claimed,
                    "{BLOCK_REPLAY}: retired block must not hold body claim"
                );
                assert_eq!(
                    block.charged_bytes(),
                    0,
                    "{BLOCK_REPLAY}: retired block must hold no part charges"
                );
                // bpv1-50: retirement returns every worker lease.
                assert!(
                    block.verify_work_id.is_none()
                        && block.stripes.iter().all(|st| st.job.is_none()),
                    "{BLOCK_REPLAY}: bpv1-50 retired block still holds worker leases"
                );
                continue;
            }
            if block.phase == BlockPhase::Fallback {
                assert_eq!(block.repair_round, 0);
                assert!(!block.body_claimed);
            }
            for st in &block.stripes {
                if !st.assemblable && !st.decoding() {
                    assert!(
                        st.held_count.saturating_add(st.claims.len() as u32) <= block.meta.coding.k,
                        "{BLOCK_REPLAY}: bpv1-28 held ({}) + pending ({}) > k ({})",
                        st.held_count,
                        st.claims.len(),
                        block.meta.coding.k
                    );
                }
                // Padding slots (decision-18) count as held without a stored part.
                let k = block.meta.coding.k;
                let actual_held = (0..block.meta.coding.n)
                    .filter(|&slot| st.has(slot, k))
                    .count() as u32;
                assert_eq!(
                    actual_held, st.held_count,
                    "{BLOCK_REPLAY}: held_count accounting mismatch"
                );
                assert!(
                    (st.padding_from..k).all(|slot| st.held[slot as usize].is_none()),
                    "{BLOCK_REPLAY}: a padding slot never stores a part"
                );
                // bpv1-64 / bpv1-69: a pending seed or push names an unheld,
                // unclaimed slot.
                for &slot in st.incoming.keys() {
                    assert!(
                        !st.has(slot, k) && !st.claims.contains_key(&slot),
                        "{BLOCK_REPLAY}: bpv1-64 pending push on a held or claimed slot"
                    );
                }
            }
        }
    }
}
