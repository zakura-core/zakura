//! The store audit: checks the header-store invariants I1–I3 (as audit checks
//! A1–A3) after every mutation, plus the oracle comparison (A4).
//!
//! The audit window is `finalized_tip ..= last row in any header CF`. All five
//! column families are scanned in full — the chains in these tests are tiny.

use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use zakura_chain::block::{self, Height};

use super::super::super::{
    AdvertisedBodySize, ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT, ZAKURA_HEADER_BY_HEIGHT,
    ZAKURA_HEADER_HASH_BY_HEIGHT, ZAKURA_HEADER_HEIGHT_BY_HASH,
};
use crate::service::finalized_state::{
    disk_format::shielded::CommitmentRootsByHeight, ZakuraDb, COMMITMENT_ROOTS_BY_HEIGHT,
};

/// A single store-invariant violation found by [`audit_store`].
///
/// Tests match on the variants; the fields are diagnostic payload rendered
/// through `Debug` in failure reports.
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(crate) enum Violation {
    /// A1: a `hash_by_height` row's hash has no `height_by_hash` entry.
    MissingHeightByHash { height: Height, hash: block::Hash },
    /// A1: a `hash_by_height` row's hash maps back to a different height.
    WrongHeightByHash {
        hash: block::Hash,
        expected: Height,
        actual: Height,
    },
    /// A1: a `height_by_hash` entry whose target height row is missing or holds
    /// a different hash — a stranded reverse-index row.
    OrphanHeightByHash {
        hash: block::Hash,
        points_at: Height,
        height_row: Option<block::Hash>,
    },
    /// A1: the header CF and hash CF disagree at a height: one row is missing,
    /// or the stored header does not hash to the stored hash.
    HeaderHashRowMismatch {
        height: Height,
        header_row_hash: Option<block::Hash>,
        hash_row: Option<block::Hash>,
    },
    /// A2: the header at `height` does not link to the row below it.
    BrokenLinkage {
        height: Height,
        prev_in_header: block::Hash,
        hash_below: block::Hash,
    },
    /// A3: a zakura header CF holds a row at a height with a committed body
    /// (the frontier-overlay rule: zakura rows live only above the body store).
    ZakuraRowAtBodyHeight { cf: &'static str, height: Height },
    /// A3: a header CF holds a row above the last linked height.
    RowAboveLastLinked { cf: &'static str, height: Height },
    /// A3: the linked chain stops at a gap, with rows stranded above it.
    GapBelowTip { missing_height: Height },
    /// A3: `best_header_tip()` is not the tip of the linked chain.
    BestHeaderTipMismatch {
        reported: Option<(Height, block::Hash)>,
        last_linked: (Height, block::Hash),
    },
    /// A3: a `body_size` or `roots` row at a height without a backing header row.
    AuxRowWithoutHeader { cf: &'static str, height: Height },
    /// A4: the linked on-disk chain disagrees with the expected canonical chain.
    CanonicalMismatch {
        height: Height,
        on_disk: Option<block::Hash>,
        expected: Option<block::Hash>,
    },
}

/// A full, comparable snapshot of the five header-store column families.
///
/// Used for "rejections are side-effect free" and reopen-survival assertions.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct StoreDump {
    pub headers: BTreeMap<Height, Arc<block::Header>>,
    pub hashes: BTreeMap<Height, block::Hash>,
    /// `height_by_hash` rows in RocksDB key order (hash has no `Ord`).
    pub heights_by_hash: Vec<(block::Hash, Height)>,
    pub body_sizes: BTreeMap<Height, u32>,
    pub roots: BTreeMap<Height, CommitmentRootsByHeight>,
}

pub(crate) fn dump_store(state: &ZakuraDb) -> StoreDump {
    let header_cf = state.db.cf_handle(ZAKURA_HEADER_BY_HEIGHT).unwrap();
    let hash_cf = state.db.cf_handle(ZAKURA_HEADER_HASH_BY_HEIGHT).unwrap();
    let height_cf = state.db.cf_handle(ZAKURA_HEADER_HEIGHT_BY_HASH).unwrap();
    let body_size_cf = state
        .db
        .cf_handle(ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT)
        .unwrap();
    let roots_cf = state.db.cf_handle(COMMITMENT_ROOTS_BY_HEIGHT).unwrap();

    StoreDump {
        headers: state
            .db
            .zs_forward_range_iter::<_, Height, Arc<block::Header>, _>(&header_cf, ..)
            .collect(),
        hashes: state
            .db
            .zs_forward_range_iter::<_, Height, block::Hash, _>(&hash_cf, ..)
            .collect(),
        heights_by_hash: state
            .db
            .zs_forward_range_iter::<_, block::Hash, Height, _>(&height_cf, ..)
            .collect(),
        body_sizes: state
            .db
            .zs_forward_range_iter::<_, Height, AdvertisedBodySize, _>(&body_size_cf, ..)
            .map(|(height, size)| (height, size.get()))
            .collect(),
        roots: state
            .db
            .zs_forward_range_iter::<_, Height, CommitmentRootsByHeight, _>(&roots_cf, ..)
            .collect(),
    }
}

/// Checks A1 (bijection), A2 (linkage), and A3 (tip integrity) on the store.
///
/// Returns every violation found; an empty vec means the store is coherent.
pub(crate) fn audit_store(state: &ZakuraDb) -> Vec<Violation> {
    let dump = dump_store(state);
    let mut violations = Vec::new();

    // A1: hash_by_height -> height_by_hash roundtrip.
    let heights_by_hash: HashMap<block::Hash, Height> =
        dump.heights_by_hash.iter().copied().collect();
    for (&height, &hash) in &dump.hashes {
        match heights_by_hash.get(&hash) {
            Some(&actual) if actual == height => {}
            Some(&actual) => violations.push(Violation::WrongHeightByHash {
                hash,
                expected: height,
                actual,
            }),
            None => violations.push(Violation::MissingHeightByHash { height, hash }),
        }
    }

    // A1 reverse: every height_by_hash entry points at a matching height row.
    for &(hash, points_at) in &dump.heights_by_hash {
        if dump.hashes.get(&points_at) != Some(&hash) {
            violations.push(Violation::OrphanHeightByHash {
                hash,
                points_at,
                height_row: dump.hashes.get(&points_at).copied(),
            });
        }
    }

    // A1: header rows and hash rows agree, both ways.
    for height in dump
        .headers
        .keys()
        .chain(dump.hashes.keys())
        .copied()
        .collect::<std::collections::BTreeSet<_>>()
    {
        let header_row_hash = dump
            .headers
            .get(&height)
            .map(|header| block::Hash::from(&**header));
        let hash_row = dump.hashes.get(&height).copied();
        if header_row_hash != hash_row {
            violations.push(Violation::HeaderHashRowMismatch {
                height,
                header_row_hash,
                hash_row,
            });
        }
    }

    // A2: walk the merged header view up from the finalized (body) tip,
    // verifying linkage at each step.
    let (body_tip_height, body_tip_hash) = state
        .tip()
        .expect("harness states always have a committed genesis");
    let mut last_linked = (body_tip_height, body_tip_hash);
    let mut linkage_broken = false;
    let mut next_height = body_tip_height.next().ok();
    while let Some(height) = next_height {
        let Some((hash, header)) = state.header_by_height(height) else {
            break;
        };
        if header.previous_block_hash != last_linked.1 {
            violations.push(Violation::BrokenLinkage {
                height,
                prev_in_header: header.previous_block_hash,
                hash_below: last_linked.1,
            });
            linkage_broken = true;
            break;
        }
        last_linked = (height, hash);
        next_height = height.next().ok();
    }

    // A3: no zakura rows at body-backed heights (the roots CF is excluded — it
    // legitimately holds verified rows at body heights).
    let zakura_height_keyed: [(&'static str, Vec<Height>); 4] = [
        (
            ZAKURA_HEADER_BY_HEIGHT,
            dump.headers.keys().copied().collect(),
        ),
        (
            ZAKURA_HEADER_HASH_BY_HEIGHT,
            dump.hashes.keys().copied().collect(),
        ),
        (
            ZAKURA_HEADER_HEIGHT_BY_HASH,
            dump.heights_by_hash
                .iter()
                .map(|&(_hash, height)| height)
                .collect(),
        ),
        (
            ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT,
            dump.body_sizes.keys().copied().collect(),
        ),
    ];
    for (cf, heights) in &zakura_height_keyed {
        for &height in heights {
            if state.contains_body_at_height(height) {
                violations.push(Violation::ZakuraRowAtBodyHeight { cf, height });
            }
        }
    }

    // A3: no rows above the last linked height, in any header CF.
    let mut rows_above = false;
    for (cf, heights) in &zakura_height_keyed {
        for &height in heights {
            if height > last_linked.0 {
                violations.push(Violation::RowAboveLastLinked { cf, height });
                rows_above = true;
            }
        }
    }
    for &height in dump.roots.keys() {
        if height > last_linked.0 {
            violations.push(Violation::RowAboveLastLinked {
                cf: COMMITMENT_ROOTS_BY_HEIGHT,
                height,
            });
            rows_above = true;
        }
    }
    if rows_above && !linkage_broken {
        // The walk stopped at a missing height with rows stranded above it.
        violations.push(Violation::GapBelowTip {
            missing_height: last_linked
                .0
                .next()
                .expect("linked tip is far below the max height"),
        });
    }

    // A3: best_header_tip() is the tip of the linked chain.
    let reported = state.best_header_tip();
    if reported != Some(last_linked) {
        violations.push(Violation::BestHeaderTipMismatch {
            reported,
            last_linked,
        });
    }

    // A3: body-size rows require a backing zakura header row; roots rows
    // require a zakura header row or a committed body.
    for &height in dump.body_sizes.keys() {
        if !dump.hashes.contains_key(&height) {
            violations.push(Violation::AuxRowWithoutHeader {
                cf: ZAKURA_HEADER_BODY_SIZE_BY_HEIGHT,
                height,
            });
        }
    }
    for &height in dump.roots.keys() {
        if !dump.hashes.contains_key(&height) && !state.contains_body_at_height(height) {
            violations.push(Violation::AuxRowWithoutHeader {
                cf: COMMITMENT_ROOTS_BY_HEIGHT,
                height,
            });
        }
    }

    violations
}

/// Checks A4: the merged on-disk chain above genesis equals `expected`
/// (the oracle's canonical chain), over the union of both domains.
pub(crate) fn audit_against_expected_chain(
    state: &ZakuraDb,
    expected: &BTreeMap<Height, block::Hash>,
) -> Vec<Violation> {
    let mut violations = Vec::new();

    let last_expected = expected.keys().next_back().copied().unwrap_or(Height(0));
    let last_on_disk = state.best_header_tip().map_or(Height(0), |(h, _)| h);
    let last = last_expected.max(last_on_disk);

    for height in 1..=last.0 {
        let height = Height(height);
        let on_disk = state.header_hash(height);
        let expected_hash = expected.get(&height).copied();
        if on_disk != expected_hash {
            violations.push(Violation::CanonicalMismatch {
                height,
                on_disk,
                expected: expected_hash,
            });
        }
    }

    violations
}

#[cfg(test)]
mod tests {
    use super::super::super::common::{commit_header_range, state_with_genesis_config};
    use super::super::fabricate::{Universe, BRANCH_A, FORK_HEIGHT};
    use super::*;
    use crate::{
        service::finalized_state::disk_db::{DiskWriteBatch, WriteDisk},
        Config,
    };

    fn assert_clean(state: &ZakuraDb) {
        let violations = audit_store(state);
        assert!(
            violations.is_empty(),
            "unexpected violations: {violations:?}"
        );
    }

    /// The audit is green on stores produced by clean write sequences.
    #[test]
    fn audit_is_green_on_coherent_stores() {
        let _init_guard = zakura_test::init();
        let universe = Universe::new();

        // Genesis-only store.
        let state = state_with_genesis_config(
            &universe.network,
            universe.genesis.clone(),
            Config::ephemeral(),
        );
        assert_clean(&state);

        // Trunk committed up to the fork, then a branch on top.
        let trunk_headers: Vec<_> = universe.trunk[..FORK_HEIGHT as usize]
            .iter()
            .map(|fab| fab.header.clone())
            .collect();
        commit_header_range(&state, universe.genesis.hash(), &trunk_headers);
        assert_clean(&state);

        let branch = &universe.branches[BRANCH_A];
        let branch_headers: Vec<_> = branch
            .headers
            .iter()
            .map(|fab| fab.header.clone())
            .collect();
        commit_header_range(&state, branch.fork_parent.1, &branch_headers);
        assert_clean(&state);
    }

    /// Each hand-made corruption fires the right violation variant.
    #[test]
    fn audit_detects_hand_made_corruption() {
        let _init_guard = zakura_test::init();
        let universe = Universe::new();

        let corrupted_state = || {
            let state = state_with_genesis_config(
                &universe.network,
                universe.genesis.clone(),
                Config::ephemeral(),
            );
            let trunk_headers: Vec<_> = universe.trunk[..FORK_HEIGHT as usize]
                .iter()
                .map(|fab| fab.header.clone())
                .collect();
            commit_header_range(&state, universe.genesis.hash(), &trunk_headers);
            state
        };

        // Delete one height_by_hash row: forward bijection breaks.
        let state = corrupted_state();
        let height_cf = state.db.cf_handle(ZAKURA_HEADER_HEIGHT_BY_HASH).unwrap();
        let mut batch = DiskWriteBatch::new();
        batch.zs_delete(&height_cf, universe.trunk_at(10).hash);
        state.db.write(batch).expect("raw delete writes");
        let violations = audit_store(&state);
        assert!(
            violations
                .iter()
                .any(|v| matches!(v, Violation::MissingHeightByHash { height, .. } if *height == Height(10))),
            "expected MissingHeightByHash: {violations:?}"
        );

        // Insert a stray height_by_hash row: reverse bijection breaks.
        let state = corrupted_state();
        let height_cf = state.db.cf_handle(ZAKURA_HEADER_HEIGHT_BY_HASH).unwrap();
        let stray_hash = universe.branches[BRANCH_A].headers[0].hash;
        let mut batch = DiskWriteBatch::new();
        batch.zs_insert(&height_cf, stray_hash, Height(12));
        state.db.write(batch).expect("raw insert writes");
        let violations = audit_store(&state);
        assert!(
            violations.iter().any(
                |v| matches!(v, Violation::OrphanHeightByHash { hash, .. } if *hash == stray_hash)
            ),
            "expected OrphanHeightByHash: {violations:?}"
        );

        // Replace a mid-chain header row with a foreign header: the header/hash
        // rows disagree and linkage breaks above the corrupted height.
        let state = corrupted_state();
        let header_cf = state.db.cf_handle(ZAKURA_HEADER_BY_HEIGHT).unwrap();
        let foreign_header = universe.branches[BRANCH_A].headers[3].header.clone();
        let mut batch = DiskWriteBatch::new();
        batch.zs_insert(&header_cf, Height(20), foreign_header);
        state.db.write(batch).expect("raw insert writes");
        let violations = audit_store(&state);
        assert!(
            violations
                .iter()
                .any(|v| matches!(v, Violation::HeaderHashRowMismatch { height, .. } if *height == Height(20))),
            "expected HeaderHashRowMismatch: {violations:?}"
        );

        // Delete a mid-chain hash row: the linked walk stops below it and every
        // surviving row above is stranded.
        let state = corrupted_state();
        let hash_cf = state.db.cf_handle(ZAKURA_HEADER_HASH_BY_HEIGHT).unwrap();
        let mut batch = DiskWriteBatch::new();
        batch.zs_delete(&hash_cf, Height(15));
        state.db.write(batch).expect("raw delete writes");
        let violations = audit_store(&state);
        assert!(
            violations
                .iter()
                .any(|v| matches!(v, Violation::RowAboveLastLinked { height, .. } if *height > Height(15))),
            "expected RowAboveLastLinked: {violations:?}"
        );
        assert!(
            violations
                .iter()
                .any(|v| matches!(v, Violation::GapBelowTip { missing_height } if *missing_height == Height(15))),
            "expected GapBelowTip: {violations:?}"
        );
        assert!(
            violations
                .iter()
                .any(|v| matches!(v, Violation::BestHeaderTipMismatch { .. })),
            "expected BestHeaderTipMismatch: {violations:?}"
        );
    }

    /// A4 reports disagreements with an expected canonical chain.
    #[test]
    fn audit_detects_canonical_mismatch() {
        let _init_guard = zakura_test::init();
        let universe = Universe::new();

        let state = state_with_genesis_config(
            &universe.network,
            universe.genesis.clone(),
            Config::ephemeral(),
        );
        let trunk_headers: Vec<_> = universe.trunk[..FORK_HEIGHT as usize]
            .iter()
            .map(|fab| fab.header.clone())
            .collect();
        commit_header_range(&state, universe.genesis.hash(), &trunk_headers);

        let mut expected: BTreeMap<Height, block::Hash> = universe.trunk[..FORK_HEIGHT as usize]
            .iter()
            .map(|fab| (fab.height, fab.hash))
            .collect();
        assert!(audit_against_expected_chain(&state, &expected).is_empty());

        // Claim one extra expected height: the store must be reported behind.
        let next = &universe.branches[BRANCH_A].headers[0];
        expected.insert(next.height, next.hash);
        let violations = audit_against_expected_chain(&state, &expected);
        assert!(
            violations
                .iter()
                .any(|v| matches!(v, Violation::CanonicalMismatch { height, on_disk: None, .. } if *height == next.height)),
            "expected CanonicalMismatch: {violations:?}"
        );
    }
}
