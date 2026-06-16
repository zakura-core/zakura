//! Types and functions for note commitment tree RPCs.

use derive_getters::Getters;
use derive_new::new;
use zebra_chain::{
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
/// `incrementalmerkletree` and not `Frontier`s from the same crate, even though `zebrad`'s
/// `NoteCommitmentTree`s are implemented using `Frontier`s. Zebra follows the former format to stay
/// consistent with `zcashd`'s RPCs.
///
/// The formats are semantically equivalent. The difference is that in `Frontier`s, the vector of
/// ommers is dense (we know where the gaps are from the position of the leaf in the overall tree);
/// whereas in `CommitmentTree`, the vector of ommers is sparse with [`None`] values in the gaps.
///
/// The dense format might be used in future RPCs.
///
/// The serialized response omits `ironwood` unless Ironwood tree state is
/// available for the requested block.
#[derive(Clone, Debug, Eq, PartialEq, Getters)]
pub struct GetTreestateResponse {
    /// The block hash corresponding to the treestate, hex-encoded.
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
    sprout: Option<Treestate>,

    /// A treestate containing a Sapling note commitment tree, hex-encoded.
    sapling: Treestate,

    /// A treestate containing an Orchard note commitment tree, hex-encoded.
    orchard: Treestate,

    /// A treestate containing an Ironwood note commitment tree, hex-encoded.
    /// Serialized only when [`Self::has_ironwood`] returns true.
    #[getter(skip)]
    ironwood: Treestate,

    /// Whether the serialized RPC response should include Ironwood tree state.
    #[getter(skip)]
    has_ironwood: bool,
}

impl GetTreestateResponse {
    /// Constructs a new [`GetTreestateResponse`] with Ironwood tree state.
    pub fn new(
        hash: Hash,
        height: Height,
        time: u32,
        sprout: Option<Treestate>,
        sapling: Treestate,
        orchard: Treestate,
        ironwood: Treestate,
    ) -> Self {
        Self {
            hash,
            height,
            time,
            sprout,
            sapling,
            orchard,
            ironwood,
            has_ironwood: true,
        }
    }

    /// Constructs a new [`GetTreestateResponse`] that may omit Ironwood tree state.
    pub fn new_with_optional_ironwood(
        hash: Hash,
        height: Height,
        time: u32,
        sprout: Option<Treestate>,
        sapling: Treestate,
        orchard: Treestate,
        ironwood: Option<Treestate>,
    ) -> Self {
        let has_ironwood = ironwood.is_some();

        Self {
            hash,
            height,
            time,
            sprout,
            sapling,
            orchard,
            ironwood: ironwood.unwrap_or_default(),
            has_ironwood,
        }
    }

    /// Returns the Ironwood treestate.
    pub fn ironwood(&self) -> &Treestate {
        &self.ironwood
    }

    /// Returns whether the Ironwood treestate was present in the RPC response.
    pub fn has_ironwood(&self) -> bool {
        self.has_ironwood
    }

    /// Returns the Ironwood treestate if it was present in the RPC response.
    pub fn optional_ironwood(&self) -> Option<&Treestate> {
        self.has_ironwood.then_some(&self.ironwood)
    }

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
            ironwood: Treestate::default(),
            has_ironwood: false,
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
            has_ironwood: false,
        }
    }
}

impl serde::Serialize for GetTreestateResponse {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(serde::Serialize)]
        struct GetTreestateResponseRef<'a> {
            #[serde(with = "hex")]
            hash: Hash,
            height: Height,
            time: u32,
            #[serde(skip_serializing_if = "Option::is_none")]
            sprout: Option<&'a Treestate>,
            sapling: &'a Treestate,
            orchard: &'a Treestate,
            #[serde(skip_serializing_if = "Option::is_none")]
            ironwood: Option<&'a Treestate>,
        }

        let response = GetTreestateResponseRef {
            hash: self.hash,
            height: self.height,
            time: self.time,
            sprout: self.sprout.as_ref(),
            sapling: &self.sapling,
            orchard: &self.orchard,
            ironwood: self.optional_ironwood(),
        };

        serde::Serialize::serialize(&response, serializer)
    }
}

impl<'de> serde::Deserialize<'de> for GetTreestateResponse {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(serde::Deserialize)]
        struct GetTreestateResponseHelper {
            #[serde(with = "hex")]
            hash: Hash,
            height: Height,
            time: u32,
            #[serde(default)]
            sprout: Option<Treestate>,
            sapling: Treestate,
            orchard: Treestate,
            #[serde(default)]
            ironwood: Option<Treestate>,
        }

        let response =
            <GetTreestateResponseHelper as serde::Deserialize>::deserialize(deserializer)?;
        let has_ironwood = response.ironwood.is_some();

        Ok(Self {
            hash: response.hash,
            height: response.height,
            time: response.time,
            sprout: response.sprout,
            sapling: response.sapling,
            orchard: response.orchard,
            ironwood: response.ironwood.unwrap_or_default(),
            has_ironwood,
        })
    }
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
