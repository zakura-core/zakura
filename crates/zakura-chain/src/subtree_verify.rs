//! Checking completed subtree roots against a note commitment tree frontier.
//!
//! Subtree roots are interior nodes, so a treestate's per-height root check does not test them:
//! a wrong subtree root and a right one produce the same tree root as long as the leaves agree.
//! A frontier does pin them, though. Its ommers at levels at or above
//! [`TRACKED_SUBTREE_HEIGHT`] are the pairwise hashes of the subtrees that are already complete,
//! so folding a candidate set of subtree roots must reproduce them exactly.
//!
//! That makes a frontier and a set of subtree roots mutually checkable offline, with no database
//! and no block bodies, which is what the embedded Mainnet subtree-root artifact relies on.

use incrementalmerkletree::{frontier::NonEmptyFrontier, Hashable, Level, Source};
use thiserror::Error;

use crate::subtree::TRACKED_SUBTREE_HEIGHT;

/// An error checking completed subtree roots against a frontier.
#[derive(Clone, Debug, Eq, PartialEq, Error)]
pub enum SubtreeRootsError {
    /// The number of supplied roots does not match the number of subtrees the frontier completed.
    #[error(
        "expected {expected} completed subtree roots for a frontier with {leaves} leaves, \
         found {found}"
    )]
    CountMismatch {
        /// How many completed subtrees the frontier has.
        expected: u64,
        /// How many roots were supplied.
        found: usize,
        /// The frontier's leaf count.
        leaves: u64,
    },

    /// A supplied root disagrees with the frontier's interior node covering it.
    #[error(
        "subtree roots {first_index}..{end_index} do not hash to the frontier node at level {level}"
    )]
    RootMismatch {
        /// The level of the frontier node that did not match.
        level: u8,
        /// The first subtree index that node covers.
        first_index: u64,
        /// The end of the subtree index range that node covers, exclusive.
        end_index: u64,
    },

    /// The frontier does not pin every completed subtree.
    ///
    /// Verifying part of a set while reporting success would overstate what was proven, so a
    /// shortfall is an error rather than a smaller success.
    #[error("frontier pins only {covered} of {expected} completed subtree roots")]
    IncompleteCoverage {
        /// How many completed subtrees the frontier's ommers cover.
        covered: u64,
        /// How many completed subtrees the frontier has.
        expected: u64,
    },

    /// The frontier is malformed: it indexes an ommer it does not have.
    #[error("frontier is missing the ommer at index {index}")]
    MissingOmmer {
        /// The missing ommer index.
        index: usize,
    },
}

/// Checks `roots`, the completed subtree roots in index order, against `frontier`.
///
/// `depth` is the tree's Merkle depth. Returns how many roots were checked.
///
/// Every completed subtree falls inside exactly one frontier ommer's span, so a single wrong root
/// changes exactly one ommer and is rejected. An empty frontier accepts only an empty root set.
pub fn verify_completed_subtree_roots<H: Hashable + Clone + PartialEq>(
    frontier: Option<&NonEmptyFrontier<H>>,
    roots: &[H],
    depth: u8,
) -> Result<usize, SubtreeRootsError> {
    let Some(frontier) = frontier else {
        return if roots.is_empty() {
            Ok(0)
        } else {
            Err(SubtreeRootsError::CountMismatch {
                expected: 0,
                found: roots.len(),
                leaves: 0,
            })
        };
    };

    // A frontier's position is the index of its most recently appended leaf, so the tree holds one
    // more leaf than that. Positions are bounded by the tree depth, well below `u64::MAX`.
    let position = frontier.position();
    let leaves = u64::from(position) + 1;
    let expected = leaves >> TRACKED_SUBTREE_HEIGHT;

    // Comparing as u64 avoids a usize cast on a caller-supplied length.
    if roots.len() as u64 != expected {
        return Err(SubtreeRootsError::CountMismatch {
            expected,
            found: roots.len(),
            leaves,
        });
    }

    let ommers = frontier.ommers();
    let mut covered = 0;

    // The same walk `NonEmptyFrontier::root` uses: each `Past` entry is a left sibling held in the
    // frontier, and the yielded address is that sibling's own address.
    for (address, source) in position.witness_addrs(Level::from(depth)) {
        let Source::Past(ommer_index) = source else {
            continue;
        };

        let level = u8::from(address.level());
        if level < TRACKED_SUBTREE_HEIGHT {
            continue;
        }

        // A node at `level` spans `2^(level - TRACKED_SUBTREE_HEIGHT)` complete subtrees, and its
        // index at that level is the same span's offset. Both fit in u64 for any supported depth.
        let span = 1u64 << (level - TRACKED_SUBTREE_HEIGHT);
        let first_index = address.index() * span;
        let end_index = first_index + span;

        let block = usize::try_from(first_index)
            .ok()
            .zip(usize::try_from(end_index).ok())
            .and_then(|(first, end)| roots.get(first..end))
            .ok_or(SubtreeRootsError::IncompleteCoverage { covered, expected })?;

        let ommer =
            ommers
                .get(usize::from(ommer_index))
                .ok_or(SubtreeRootsError::MissingOmmer {
                    index: usize::from(ommer_index),
                })?;

        if fold_to_level(block, level) != *ommer {
            return Err(SubtreeRootsError::RootMismatch {
                level,
                first_index,
                end_index,
            });
        }

        covered += span;
    }

    // A frontier sitting exactly on a subtree boundary has just completed a subtree, and holds
    // that subtree's nodes below level `TRACKED_SUBTREE_HEIGHT` rather than as an ommer above it.
    // Its own root taken at that level is precisely the completed root, the same way
    // `completed_subtree_index_and_root` reads it, so the last root is checkable too.
    if covered + 1 == expected && leaves % (1 << TRACKED_SUBTREE_HEIGHT) == 0 {
        let last = roots
            .last()
            .expect("expected is non-zero, so roots is non-empty");

        if *last != frontier.root(Some(Level::from(TRACKED_SUBTREE_HEIGHT))) {
            return Err(SubtreeRootsError::RootMismatch {
                level: TRACKED_SUBTREE_HEIGHT,
                first_index: covered,
                end_index: expected,
            });
        }

        covered += 1;
    }

    if covered != expected {
        return Err(SubtreeRootsError::IncompleteCoverage { covered, expected });
    }

    Ok(roots.len())
}

/// Hashes `block`, a full aligned run of subtree roots, up to a single node at `level`.
///
/// `block` always holds a power-of-two number of complete subtrees, so every pair is populated and
/// no empty-node padding is involved.
fn fold_to_level<H: Hashable + Clone>(block: &[H], level: u8) -> H {
    let mut nodes = block.to_vec();

    for current in TRACKED_SUBTREE_HEIGHT..level {
        nodes = nodes
            .as_chunks::<2>()
            .0
            .iter()
            .map(|pair| H::combine(Level::from(current), &pair[0], &pair[1]))
            .collect();
    }

    nodes
        .pop()
        .expect("a non-empty power-of-two block folds to exactly one node")
}

#[cfg(test)]
mod tests {
    use super::*;

    const SUBTREE_LEAVES: u64 = 1 << TRACKED_SUBTREE_HEIGHT;
    const DEPTH: u8 = 32;

    /// A cheap stand-in for a pool's node type.
    ///
    /// The real Pedersen and Sinsemilla hashes are far too slow to append the hundreds of
    /// thousands of leaves these tests need. Only the tree walk is under test here, so all
    /// `combine` has to be is sensitive to its inputs, their order, and the level.
    #[derive(Clone, Debug, Eq, PartialEq)]
    struct TestNode(u64);

    impl Hashable for TestNode {
        fn empty_leaf() -> Self {
            TestNode(u64::MAX)
        }

        fn combine(level: Level, a: &Self, b: &Self) -> Self {
            TestNode(
                a.0.wrapping_mul(0x9e37_79b9_7f4a_7c15)
                    .wrapping_add(b.0.wrapping_mul(0xc2b2_ae3d_27d4_eb4f))
                    .wrapping_add(u64::from(u8::from(level)) + 1),
            )
        }
    }

    /// Builds a frontier over `leaves` leaves, with the root of every subtree it completed.
    fn build(leaves: u64) -> (NonEmptyFrontier<TestNode>, Vec<TestNode>) {
        let mut frontier = NonEmptyFrontier::new(TestNode(0));
        let mut roots = Vec::new();

        for leaf in 1..leaves {
            frontier.append(TestNode(leaf));

            if (u64::from(frontier.position()) + 1) % SUBTREE_LEAVES == 0 {
                roots.push(frontier.root(Some(Level::from(TRACKED_SUBTREE_HEIGHT))));
            }
        }

        (frontier, roots)
    }

    #[test]
    fn verifies_every_completed_subtree_root() {
        // Two subtrees, which the frontier covers with a single level-17 ommer.
        let (frontier, roots) = build(2 * SUBTREE_LEAVES + 5);
        assert_eq!(roots.len(), 2);
        assert_eq!(
            verify_completed_subtree_roots(Some(&frontier), &roots, DEPTH),
            Ok(2)
        );

        // Three subtrees, which take two ommers: level 17 for the first two, level 16 for the
        // third. Covering more than one block is the case a single-ommer test would miss.
        let (frontier, roots) = build(3 * SUBTREE_LEAVES + 1);
        assert_eq!(roots.len(), 3);
        assert_eq!(
            verify_completed_subtree_roots(Some(&frontier), &roots, DEPTH),
            Ok(3)
        );
    }

    #[test]
    fn rejects_a_single_wrong_root() {
        let (frontier, roots) = build(3 * SUBTREE_LEAVES + 1);

        for index in 0..roots.len() {
            let mut tampered = roots.clone();
            tampered[index] = TestNode(0xdead_beef);

            assert!(
                verify_completed_subtree_roots(Some(&frontier), &tampered, DEPTH).is_err(),
                "a wrong root at index {index} must be rejected"
            );
        }
    }

    #[test]
    fn rejects_a_root_count_that_does_not_match_the_frontier() {
        let (frontier, roots) = build(2 * SUBTREE_LEAVES + 5);

        assert_eq!(
            verify_completed_subtree_roots(Some(&frontier), &roots[..1], DEPTH),
            Err(SubtreeRootsError::CountMismatch {
                expected: 2,
                found: 1,
                leaves: 2 * SUBTREE_LEAVES + 5,
            })
        );

        // The empty artifact this check exists to catch.
        assert!(matches!(
            verify_completed_subtree_roots(Some(&frontier), &[], DEPTH),
            Err(SubtreeRootsError::CountMismatch { found: 0, .. })
        ));
    }

    #[test]
    fn verifies_a_frontier_sitting_exactly_on_a_subtree_boundary() {
        // The subtree that just completed is not an ommer yet, so it is checked against the
        // frontier's own root at the subtree level instead.
        let (frontier, roots) = build(2 * SUBTREE_LEAVES);
        assert_eq!(roots.len(), 2);
        assert_eq!(
            verify_completed_subtree_roots(Some(&frontier), &roots, DEPTH),
            Ok(2)
        );

        // That last root is really checked, not waved through.
        let mut tampered = roots.clone();
        tampered[1] = TestNode(0xdead_beef);
        assert_eq!(
            verify_completed_subtree_roots(Some(&frontier), &tampered, DEPTH),
            Err(SubtreeRootsError::RootMismatch {
                level: TRACKED_SUBTREE_HEIGHT,
                first_index: 1,
                end_index: 2,
            })
        );

        // And so is the one the ommer covers.
        let mut tampered = roots;
        tampered[0] = TestNode(0xdead_beef);
        assert!(verify_completed_subtree_roots(Some(&frontier), &tampered, DEPTH).is_err());
    }

    #[test]
    fn an_empty_frontier_accepts_only_an_empty_root_set() {
        assert_eq!(
            verify_completed_subtree_roots(None::<&NonEmptyFrontier<TestNode>>, &[], DEPTH),
            Ok(0)
        );
        assert_eq!(
            verify_completed_subtree_roots(None, &[TestNode(1)], DEPTH),
            Err(SubtreeRootsError::CountMismatch {
                expected: 0,
                found: 1,
                leaves: 0,
            })
        );
    }

    #[test]
    fn a_frontier_below_one_subtree_has_no_roots_to_check() {
        let (frontier, roots) = build(SUBTREE_LEAVES - 1);
        assert!(roots.is_empty());
        assert_eq!(
            verify_completed_subtree_roots(Some(&frontier), &[], DEPTH),
            Ok(0)
        );
    }
}
