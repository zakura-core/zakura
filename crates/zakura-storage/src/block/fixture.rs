//! Fixture blocks for the block service.
//!
//! A fixture block is a [`HeaderFixture`] header with a body. The body is
//! one coinbase transaction that encodes the block's height and is padded to
//! a chosen size, and the header commits to that transaction's merkle root.
//! In every other field the header is the one [`HeaderFixture::extend`]
//! builds, so the engine accepts it. So a body decodes, hashes to its
//! header, and passes the block-sync routine's own checks: the coinbase
//! height matches the requested height, and the transactions match the
//! header's merkle root. A body never passes consensus. The block service
//! models the verifier, so nothing checks it.
//!
//! [`BlockFixture::committable`] builds blocks that real zakura-state also
//! commits through its semantic path, for tests that compare the block
//! service with it. Their coinbase is a v4 transaction, since zakura-state's
//! non-finalized chain takes no older version, and each header commits to
//! the chain history root of its ancestors, as ZIP 221 requires from
//! Heartwood. They still fail the verifier's consensus checks.
//!
//! Each block also carries a size hint, separate from its real size. Header
//! insertion advertises it as the header's `TargetEntry::body_size`, the
//! engine keeps it as that header's auxiliary delivery, and the
//! missing-body query reads it back as the block's size estimate, as
//! zakura-state does. A hint far below the real size is how a scenario
//! serves bodies larger than block sync expects.

use std::sync::Arc;

use zakura_chain::{
    amount::Amount,
    block::{self, merkle},
    history_tree::HistoryTree,
    ironwood, orchard, sapling,
    serialization::ZcashSerialize,
    transaction::{LockTime, Transaction},
    transparent,
};
use zakura_header_chain::{EngineSnapshot, Frontier};
use zakura_node_services::header_chain::PrepareHeaderTarget;

use crate::header::fixture::HeaderFixture;

/// The coinbase data every fixture body carries after its height. A
/// coinbase script must be at least two bytes, and heights 1 to 16 encode in
/// one.
const COINBASE_DATA: [u8; 2] = [0x7a, 0x6b];

/// The size and size hint of one fixture body.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct BodySpec {
    /// The body's serialized size in bytes. At least
    /// [`BlockFixture::min_body_size`] for its height.
    pub size: u32,
    /// The size its header advertises. 0 advertises none, which block sync
    /// reads as an unknown size.
    pub hint: u32,
}

impl BodySpec {
    /// A body of `size` bytes whose hint is exact.
    pub const fn exact(size: u32) -> Self {
        BodySpec { size, hint: size }
    }
}

/// One fixture block and its size hint.
#[derive(Clone, Debug)]
pub struct FixtureBlock {
    /// The block.
    pub block: Arc<block::Block>,
    /// Its height, which its coinbase encodes.
    pub height: block::Height,
    /// The size its header advertises.
    pub hint: u32,
}

impl FixtureBlock {
    /// The block's hash.
    pub fn hash(&self) -> block::Hash {
        self.block.hash()
    }

    /// The block's height and hash.
    pub fn frontier(&self) -> Frontier {
        Frontier::new(self.height, self.hash())
    }

    /// The block's consensus serialization, which serving sends unchanged.
    ///
    /// # Panics
    ///
    /// Never: a fixture block always serializes.
    pub fn bytes(&self) -> Vec<u8> {
        self.block
            .zcash_serialize_to_vec()
            .expect("a fixture block serializes")
    }

    /// The block's serialized size in bytes.
    pub fn size(&self) -> u32 {
        u32::try_from(self.block.zcash_serialized_size()).expect("a fixture block fits u32")
    }
}

/// Builds fixture blocks on the Regtest genesis block.
#[derive(Clone, Debug)]
pub struct BlockFixture {
    headers: HeaderFixture,
}

impl BlockFixture {
    /// The default Regtest network, anchored at its genesis block.
    pub fn regtest() -> Self {
        BlockFixture {
            headers: HeaderFixture::regtest(),
        }
    }

    /// The header fixture underneath.
    pub fn headers(&self) -> &HeaderFixture {
        &self.headers
    }

    /// The anchor, genesis at height 0.
    pub fn anchor(&self) -> Frontier {
        self.headers.anchor()
    }

    /// The smallest body the fixture builds at `height`: a coinbase with an
    /// empty padding script.
    pub fn min_body_size(height: block::Height) -> u32 {
        let coinbase = coinbase(Version::V1, height, 0, 0);
        block_size(&coinbase)
    }

    /// The smallest body [`Self::committable`] builds at `height`.
    pub fn min_committable_body_size(height: block::Height) -> u32 {
        let coinbase = coinbase(Version::V4, height, 0, 0);
        block_size(&coinbase)
    }

    /// The body at height `parent + 1` on `branch`, extending `parent`.
    ///
    /// # Panics
    ///
    /// If `spec.size` is below [`Self::min_body_size`] or above
    /// `MAX_BLOCK_BYTES`, or the height overflows.
    pub fn block(&self, parent: Frontier, branch: u8, spec: BodySpec) -> FixtureBlock {
        self.block_with(Version::V1, parent, branch, spec, None)
    }

    /// The body at height `parent + 1` on `branch`, with a `version`
    /// coinbase. Its header commits to `commitment` if given, and otherwise
    /// keeps the header fixture's.
    fn block_with(
        &self,
        version: Version,
        parent: Frontier,
        branch: u8,
        spec: BodySpec,
        commitment: Option<[u8; 32]>,
    ) -> FixtureBlock {
        let height = parent.height.next().expect("fixture heights stay small");
        assert!(
            u64::from(spec.size) <= block::MAX_BLOCK_BYTES,
            "a fixture body fits a block"
        );
        let coinbase = padded_coinbase(version, height, spec.size);
        let root: merkle::Root = std::iter::once(coinbase.hash()).collect();
        let mut header = self.headers.header_at(parent.hash, height.0, branch, root);
        if let Some(commitment) = commitment {
            header.commitment_bytes = commitment.into();
        }
        let block = Arc::new(block::Block {
            header: Arc::new(header),
            transactions: vec![Arc::new(coinbase)],
        });
        let fixture = FixtureBlock {
            block,
            height,
            hint: spec.hint,
        };
        assert_eq!(fixture.size(), spec.size, "padding reaches the exact size");
        fixture
    }

    /// One block per spec on `branch`, extending the last block of `prefix`,
    /// or genesis if `prefix` is empty, that real zakura-state commits.
    /// `prefix` is that block's chain from height 1, whose history each
    /// header commits to.
    ///
    /// # Panics
    ///
    /// If `prefix` isn't a chain from height 1, a spec is below
    /// [`Self::min_committable_body_size`], or as [`Self::block`] does.
    pub fn committable(
        &self,
        prefix: &[FixtureBlock],
        specs: &[BodySpec],
        branch: u8,
    ) -> Vec<FixtureBlock> {
        let network = self.headers.network();
        let push = |history: &mut HistoryTree, block: &FixtureBlock| {
            // Fixture bodies have no shielded data, so every note commitment
            // tree stays empty.
            history
                .push(
                    network,
                    block.block.clone(),
                    &sapling::tree::NoteCommitmentTree::default().root(),
                    &orchard::tree::NoteCommitmentTree::default().root(),
                    &ironwood::tree::NoteCommitmentTree::default().root(),
                )
                .expect("a fixture block extends its history");
        };
        let mut history = HistoryTree::default();
        let mut parent = self.anchor();
        for block in prefix {
            assert_eq!(
                block.block.header.previous_block_hash, parent.hash,
                "a prefix is a chain from height 1"
            );
            push(&mut history, block);
            parent = block.frontier();
        }
        specs
            .iter()
            .map(|spec| {
                // The Heartwood activation block commits to zeros.
                let commitment = history.hash().map_or([0; 32], <[u8; 32]>::from);
                let block = self.block_with(Version::V4, parent, branch, *spec, Some(commitment));
                push(&mut history, &block);
                parent = block.frontier();
                block
            })
            .collect()
    }

    /// One block per spec extending `parent`, parent first, on `branch`.
    /// Two branches from the same parent never share a block.
    ///
    /// # Panics
    ///
    /// As [`Self::block`] does.
    pub fn extend(&self, parent: Frontier, specs: &[BodySpec], branch: u8) -> Vec<FixtureBlock> {
        let mut parent = parent;
        specs
            .iter()
            .map(|spec| {
                let block = self.block(parent, branch, *spec);
                parent = block.frontier();
                block
            })
            .collect()
    }

    /// `count` blocks of the same spec extending `parent` on `branch`.
    ///
    /// # Panics
    ///
    /// As [`Self::block`] does.
    pub fn uniform(
        &self,
        parent: Frontier,
        count: u32,
        spec: BodySpec,
        branch: u8,
    ) -> Vec<FixtureBlock> {
        let specs: Vec<_> = (0..count).map(|_| spec).collect();
        self.extend(parent, &specs, branch)
    }

    /// The request that prepares `blocks`' headers, which extend `parent`,
    /// as complete work toward the last one, each advertising its block's
    /// hint. See [`HeaderFixture::target_request`].
    ///
    /// # Panics
    ///
    /// As [`HeaderFixture::target_request`] does.
    pub fn target_request(
        &self,
        snapshot: &EngineSnapshot,
        parent: Frontier,
        blocks: &[FixtureBlock],
        request_id: u64,
    ) -> PrepareHeaderTarget {
        let headers: Vec<_> = blocks
            .iter()
            .map(|block| (block.block.header.clone(), block.hint))
            .collect();
        self.headers
            .sized_target_request(snapshot, parent, &headers, request_id)
    }
}

/// The coinbase for `height` whose block serializes to exactly `size` bytes.
///
/// The padding script's length prefix grows by 2 bytes at 253 bytes and by
/// 2 more at 65,536, so padding alone skips some sizes. Up to 2 extra bytes
/// of coinbase data fill those gaps.
///
/// # Panics
///
/// If `size` is below the minimum.
fn padded_coinbase(version: Version, height: block::Height, size: u32) -> Transaction {
    let base = block_size(&coinbase(version, height, 0, 0));
    assert!(
        size >= base,
        "a fixture body at {height:?} is at least {base} bytes"
    );
    let gap = size - base;
    for extra in 0..=2 {
        // The prefix adds at most 4 bytes over the raw padding length.
        let most = gap.saturating_sub(extra);
        for padding in (most.saturating_sub(4)..=most).rev() {
            let candidate = coinbase(version, height, extra, padding);
            if block_size(&candidate) == size {
                return candidate;
            }
        }
    }
    unreachable!("2 extra data bytes cover every length-prefix step")
}

/// A fixture coinbase's transaction version.
#[derive(Copy, Clone, Debug)]
enum Version {
    /// The smallest, for blocks only the block service commits.
    V1,
    /// The oldest that zakura-state's non-finalized chain takes.
    V4,
}

/// The `version` coinbase for `height` with `extra` more data bytes and a
/// `padding`-byte output script.
fn coinbase(version: Version, height: block::Height, extra: u32, padding: u32) -> Transaction {
    let mut data = COINBASE_DATA.to_vec();
    // Widening u32 to usize is lossless on supported targets.
    data.resize(data.len() + extra as usize, 0);
    let inputs = vec![transparent::Input::Coinbase {
        height,
        data,
        sequence: u32::MAX,
    }];
    let outputs = vec![transparent::Output::new(
        Amount::zero(),
        // Widening u32 to usize is lossless on supported targets.
        transparent::Script::new(&vec![0; padding as usize]),
    )];
    let lock_time = LockTime::unlocked();
    match version {
        Version::V1 => Transaction::V1 {
            inputs,
            outputs,
            lock_time,
        },
        Version::V4 => Transaction::V4 {
            inputs,
            outputs,
            lock_time,
            expiry_height: height,
            joinsplit_data: None,
            sapling_shielded_data: None,
        },
    }
}

/// The serialized size of a block whose only transaction is `coinbase`.
fn block_size(coinbase: &Transaction) -> u32 {
    let header_size = HeaderFixture::regtest()
        .genesis_header()
        .zcash_serialized_size();
    // A block is its header, the transaction count, then each transaction.
    let size = header_size + 1 + coinbase.zcash_serialized_size();
    u32::try_from(size).expect("a fixture body fits u32")
}
