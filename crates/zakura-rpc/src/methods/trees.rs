//! Types and functions for note commitment tree RPCs.

use derive_getters::Getters;
use derive_new::new;
use zakura_chain::{
    block::Hash,
    block::Height,
    subtree::{NoteCommitmentSubtreeData, NoteCommitmentSubtreeIndex},
};

/// A subtree data type that can hold Sapling, Orchard, or Ironwood subtree roots.
pub type SubtreeRpcData = NoteCommitmentSubtreeData<String>;

/// Response to a `z_getsubtreesbyindex` RPC request.
///
/// Contains the shielded pool label, the index of the first subtree in the
/// list, and a list of subtree roots and end heights.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize, Getters, new)]
pub struct GetSubtreesByIndexResponse {
    /// The shielded pool to which the subtrees belong.
    //
    // TODO: consider an enum with a string conversion?
    pub(crate) pool: String,

    /// The index of the first subtree.
    #[getter(copy)]
    pub(crate) start_index: NoteCommitmentSubtreeIndex,

    /// A sequential list of complete subtrees, in `index` order.
    ///
    /// The generic subtree root type is a hex-encoded shielded subtree root
    /// string.
    //
    // TODO: is this needed?
    //#[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) subtrees: Vec<SubtreeRpcData>,
}

impl Default for GetSubtreesByIndexResponse {
    fn default() -> Self {
        Self {
            pool: "sapling | orchard | ironwood".to_string(),
            start_index: NoteCommitmentSubtreeIndex(u16::default()),
            subtrees: vec![],
        }
    }
}

/// Response to a `z_gettreestate` RPC request.
///
/// Contains hex-encoded shielded note commitment trees and their corresponding
/// [`struct@Hash`], [`Height`], and block time.
///
/// The format of the serialized trees represents `CommitmentTree`s from the crate
/// `incrementalmerkletree` and not `Frontier`s from the same crate, even though `zakurad`'s
/// `NoteCommitmentTree`s are implemented using `Frontier`s. Zebra follows the former format to stay
/// consistent with `zcashd`'s RPCs.
///
/// The formats are semantically equivalent. The difference is that in `Frontier`s, the vector of
/// ommers is dense (we know where the gaps are from the position of the leaf in the overall tree);
/// whereas in `CommitmentTree`, the vector of ommers is sparse with [`None`] values in the gaps.
///
/// The dense format might be used in future RPCs.
///
/// When Ironwood tree state is not available for the requested block, the
/// `ironwood` field contains empty commitments, matching the Sapling and
/// Orchard fields before their activation heights.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize, Getters, new)]
pub struct GetTreestateResponse {
    /// The block hash corresponding to the treestate, hex-encoded.
    #[serde(with = "hex")]
    #[getter(copy)]
    hash: Hash,

    /// The block height corresponding to the treestate, numeric.
    #[getter(copy)]
    height: Height,

    /// Unix time when the block corresponding to the treestate was mined,
    /// numeric.
    ///
    /// UTC seconds since the Unix 1970-01-01 epoch.
    time: u32,

    /// A treestate containing a Sprout note commitment tree, hex-encoded. Zebra
    /// does not support returning it; but the field is here to enable parsing
    /// responses from other implementations.
    #[serde(skip_serializing_if = "Option::is_none")]
    sprout: Option<Treestate>,

    /// A treestate containing a Sapling note commitment tree, hex-encoded.
    sapling: Treestate,

    /// A treestate containing an Orchard note commitment tree, hex-encoded.
    orchard: Treestate,

    /// A treestate containing an Ironwood note commitment tree, hex-encoded.
    /// Contains empty commitments unless Ironwood tree state is available.
    #[serde(default)]
    ironwood: Treestate,
}

impl GetTreestateResponse {
    /// Constructs [`Treestate`] from its constituent parts.
    #[deprecated(note = "Use `new` instead.")]
    pub fn from_parts(
        hash: Hash,
        height: Height,
        time: u32,
        sapling: Option<Vec<u8>>,
        orchard: Option<Vec<u8>>,
    ) -> Self {
        let sapling = Treestate {
            commitments: Commitments {
                final_state: sapling,
                final_root: None,
            },
        };
        let orchard = Treestate {
            commitments: Commitments {
                final_state: orchard,
                final_root: None,
            },
        };
        Self {
            hash,
            height,
            time,
            sprout: None,
            sapling,
            orchard,
            // This deprecated compatibility helper only accepts Sapling and
            // Orchard tree bytes, so it cannot synthesize an Ironwood tree.
            ironwood: Default::default(),
        }
    }

    /// Returns the contents of ['GetTreeState'].
    #[deprecated(note = "Use getters instead.")]
    pub fn into_parts(self) -> (Hash, Height, u32, Option<Vec<u8>>, Option<Vec<u8>>) {
        (
            self.hash,
            self.height,
            self.time,
            self.sapling.commitments.final_state,
            self.orchard.commitments.final_state,
        )
    }
}

impl Default for GetTreestateResponse {
    fn default() -> Self {
        Self {
            hash: Hash([0; 32]),
            height: Height::MIN,
            time: Default::default(),
            sprout: Default::default(),
            sapling: Default::default(),
            orchard: Default::default(),
            ironwood: Default::default(),
        }
    }
}

/// Response to a `z_gettreestateanchor` RPC request.
///
/// A Zakura-specific companion to `z_gettreestate`, for the band where a fast-synced node has no
/// stored per-height tree. Instead of the treestate itself it returns what a client needs to
/// rebuild one: the authenticated roots at the target, and the highest frontier at or below it
/// that this node has already checked against those roots.
///
/// The client replays canonical blocks from the anchor and accepts its result only if all three
/// target roots match. That check is the whole trust story — a frontier that reproduces an
/// authenticated root *is* the frontier — so this hands out no authority the node did not already
/// have over the client's view of the chain.
///
/// `z_gettreestate`'s request and response are untouched. A client tries it first and only falls
/// back here when it reports the historical tree unavailable.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize, Getters, new)]
pub struct GetTreestateAnchorResponse {
    /// The canonical target block and the roots the client's replay must reproduce.
    target: TreestateTarget,

    /// The verified frontier to replay forward from, or `null` to replay from genesis.
    #[serde(skip_serializing_if = "Option::is_none")]
    anchor: Option<TreestateAnchorFrontiers>,

    /// The first height the client has to replay.
    #[serde(rename = "replayFrom")]
    #[getter(copy)]
    replay_from: Height,
}

/// The target block of a `z_gettreestateanchor` response: what the client is reconstructing, and
/// the authenticated roots that prove it got it right.
///
/// The per-pool fields carry `finalRoot` only — the `finalState` is exactly what the client is
/// asking the node to help it derive — and use the same byte order and encoding as
/// `z_gettreestate`, so the two responses are directly comparable.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize, Getters, new)]
pub struct TreestateTarget {
    /// The canonical block hash at `height`, hex-encoded.
    #[serde(with = "hex")]
    #[getter(copy)]
    hash: Hash,

    /// The block height, numeric.
    #[getter(copy)]
    height: Height,

    /// Unix time when the block was mined, so the client can build the same treestate the exact
    /// path would have returned.
    time: u32,

    /// The authenticated Sapling root at this block.
    sapling: Treestate,

    /// The authenticated Orchard root at this block.
    orchard: Treestate,

    /// The authenticated Ironwood root at this block.
    ironwood: Treestate,
}

/// The anchor of a `z_gettreestateanchor` response: a frontier this node has already verified,
/// bound to the canonical block it is the state at the end of.
///
/// Both `finalRoot` and `finalState` are present for each pool, in `z_gettreestate`'s encoding, so
/// a client can load the frontier with the same parser it already uses.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize, Getters, new)]
pub struct TreestateAnchorFrontiers {
    /// The canonical block hash at `height`, hex-encoded.
    #[serde(with = "hex")]
    #[getter(copy)]
    hash: Hash,

    /// The height the frontiers are the state at the end of, numeric.
    #[getter(copy)]
    height: Height,

    /// The Sapling frontier and its root.
    sapling: Treestate,

    /// The Orchard frontier and its root.
    orchard: Treestate,

    /// The Ironwood frontier and its root.
    ironwood: Treestate,
}

/// A treestate that is included in the [`z_gettreestate`][1] RPC response.
///
/// [1]: https://zcash.github.io/rpc/z_gettreestate.html
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize, Getters, new)]
pub struct Treestate {
    /// Contains a shielded serialized note commitment tree,
    /// hex-encoded.
    commitments: Commitments,
}

impl Treestate {
    /// Returns a reference to the commitments.
    #[deprecated(note = "Use `commitments()` instead.")]
    pub fn inner(&self) -> &Commitments {
        self.commitments()
    }
}

impl Default for Treestate {
    fn default() -> Self {
        Self {
            commitments: Commitments {
                final_root: None,
                final_state: None,
            },
        }
    }
}

/// A wrapper that contains a shielded note commitment tree.
///
/// `finalRoot` and `finalState` are omitted when a specific tree state is not
/// available.
///
#[serde_with::serde_as]
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize, Getters, new)]
pub struct Commitments {
    /// Shielded serialized note commitment tree root, hex-encoded.
    #[serde_as(as = "Option<serde_with::hex::Hex>")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "finalRoot")]
    final_root: Option<Vec<u8>>,
    /// Shielded serialized note commitment tree, hex-encoded.
    #[serde_as(as = "Option<serde_with::hex::Hex>")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "finalState")]
    final_state: Option<Vec<u8>>,
}

impl Commitments {
    /// Returns a reference to the optional `final_state`.
    #[deprecated(note = "Use `final_state()` instead.")]
    pub fn inner(&self) -> &Option<Vec<u8>> {
        &self.final_state
    }
}
