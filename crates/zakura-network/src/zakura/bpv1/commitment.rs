//! Post-NU7 v6 coinbase commitment construction and verification (decision-1, decision-9).
//!
//! The proposer adds one zero-value transparent `OP_RETURN` output to its
//! coinbase:
//! ```text
//! script = 6a 23 || ASCII("BP") || 01 || C
//! C      = T("commit", chain_id || u16(profile) || coding || part_root
//!                      [|| proposer_node])
//! ```
//!
//! `C` covers `proposer_node` only when the coinbase holds no single miner
//! mesh output. With exactly one, the mesh output's gateway ID names the
//! proposer, and `HeaderMeta` and `C` leave `proposer_node` out
//! (decision-34).
//!
//! Verification checks (in order, after headerchain admits the header):
//! - **check-2**: v6 coinbase txid + index-0 Merkle path against `header.merkle_root`
//! - **check-3**: v6 coinbase auth digest + index-0 auth-data path against
//!   `hashAuthDataRoot`, then `hashBlockCommitments` against `header.commitment_bytes`
//! - **check-4**: exactly one version-01 commitment output (bpv1-70)
//! - **check-5**: `HeaderMeta` names the proposer in the form the coinbase
//!   asks for (decision-34), and the recomputed `C` matches the output's
//!   32-byte commitment

use thiserror::Error;
use zakura_chain::{
    block::{
        merkle::{auth_digest_or_placeholder, hash_auth_data_pair, hash_txid_pair, AuthDataRoot},
        Block, ChainHistoryBlockTxAuthCommitmentHash, ChainHistoryMmrRootHash, Header, Height,
    },
    miner_mesh::{GatewayId, MinerMeshCommitment},
    parameters::{Network, NetworkUpgrade},
    serialization::ZcashSerialize,
    transaction::Transaction,
    transparent::Output,
};
use zakura_quic::NodeId;

use super::{
    profile::{Coding, W2_MAX_COINBASE_BYTES},
    tree::COMMIT,
};

/// Exact 5-byte prefix of a version-01 commitment script:
/// `OP_RETURN (0x6a) || PUSH_35 (0x23) || "BP" || 0x01`.
///
/// The byte after the tag is the commitment format version (decision-25).
/// The tag differs from the miner mesh tag `MR`, so one coinbase can carry
/// both outputs.
pub const COMMITMENT_SCRIPT_PREFIX: [u8; 5] = [0x6a, 0x23, b'B', b'P', 0x01];

/// Byte length of the commitment output's null-data push
/// (`"BP" || 0x01 || C`, `3 + 32 = 35`).
pub const COMMITMENT_PAYLOAD_LEN: usize =
    zakura_node_services::bpv1_proposer::BPV1_COMMITMENT_NULL_DATA_LEN;

/// Exact byte length of a version-01 commitment script: `OP_RETURN`, `PUSH_35`,
/// and the payload (`2 + 35 = 37`).
pub const COMMITMENT_SCRIPT_LEN: usize = 2 + COMMITMENT_PAYLOAD_LEN;

const _: () = assert!(COMMITMENT_SCRIPT_PREFIX[1] as usize == COMMITMENT_PAYLOAD_LEN);
const _: () = assert!(COMMITMENT_SCRIPT_LEN == COMMITMENT_SCRIPT_PREFIX.len() + 32);

/// Metadata fields committed by `C`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommitmentFields {
    /// Consensus genesis block hash (raw 32 bytes).
    pub chain_id: [u8; 32],
    /// Negotiated profile identifier (W2: `2`).
    pub profile_id: u16,
    /// Block body coding tuple.
    pub coding: Coding,
    /// Root of the BPv1 part Merkle tree.
    pub part_root: [u8; 32],
    /// Transport identity of the proposer, when `HeaderMeta` carries it.
    /// `None` when the coinbase's miner mesh output names the proposer
    /// (decision-34).
    pub proposer_node: Option<NodeId>,
}

/// The node a block names as its proposer (decision-34).
///
/// The proposer's connection may flag up to `n` indices per stripe and set
/// relay flags (decision-27). Naming a node grants it only that, on its own
/// connections. It never silences the node.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum Proposer {
    /// `HeaderMeta` names the node, because the coinbase holds no single
    /// miner mesh output.
    Node(NodeId),
    /// The coinbase's miner mesh output names the node by its gateway ID, the
    /// first 20 bytes of its node ID, and `HeaderMeta` names no node.
    Gateway(GatewayId),
}

impl Proposer {
    /// Returns how a block whose coinbase holds `mesh` names `local_node`,
    /// the node that builds the block.
    ///
    /// Returns `None` when the mesh output names another node's gateway:
    /// receivers would then treat this node's seeds as an ordinary peer's
    /// pushes, so the block cannot use BPv1 (decision-34).
    pub fn local(local_node: NodeId, mesh: Option<GatewayId>) -> Option<Self> {
        match mesh {
            None => Some(Self::Node(local_node)),
            Some(gateway) => gateway
                .is_prefix_of(local_node.as_bytes())
                .then_some(Self::Gateway(gateway)),
        }
    }

    /// Returns the proposer a block names: the coinbase's miner mesh output
    /// if it holds exactly one, else `proposer_node` from `HeaderMeta`.
    ///
    /// `HeaderMeta` must carry `proposer_node` exactly when the coinbase holds
    /// no single miner mesh output (decision-34). Two or more mesh outputs
    /// name no gateway.
    pub fn named(
        coinbase: &Transaction,
        proposer_node: Option<NodeId>,
    ) -> Result<Self, CommitmentError> {
        let mesh = MinerMeshCommitment::from_coinbase_outputs(coinbase.outputs());
        match (mesh, proposer_node) {
            (None, Some(node)) => Ok(Self::Node(node)),
            (Some(mesh), None) => Ok(Self::Gateway(mesh.gateway_id())),
            (None, None) => Err(CommitmentError::MissingProposer),
            (Some(_), Some(_)) => Err(CommitmentError::ProposerNamedTwice),
        }
    }

    /// Whether `node` is this proposer: the named node, or a node whose ID
    /// starts with the gateway ID.
    ///
    /// Finding a node key whose ID starts with a given gateway ID takes
    /// about 2^160 work.
    pub fn is(&self, node: &NodeId) -> bool {
        match self {
            Self::Node(proposer) => proposer == node,
            Self::Gateway(gateway) => gateway.is_prefix_of(node.as_bytes()),
        }
    }

    /// The `proposer_node` that `HeaderMeta` and `C` carry: the node, or
    /// `None` when the miner mesh output names the proposer.
    pub fn proposer_node(&self) -> Option<NodeId> {
        match self {
            Self::Node(node) => Some(*node),
            Self::Gateway(_) => None,
        }
    }
}

/// Wire binding carried inside `HeaderMeta` to prove the coinbase commitment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoinbaseBinding {
    /// Canonical serialized coinbase transaction bytes.
    pub coinbase: Vec<u8>,
    /// Index-0 txid Merkle path siblings (leaf-to-root, `0..=32`).
    pub txid_siblings: Vec<[u8; 32]>,
    /// Index-0 auth-data Merkle path siblings (leaf-to-root, `0..=32`).
    pub auth_siblings: Vec<[u8; 32]>,
    /// `hashLightClientRoot` combined with `hashAuthDataRoot` to recompute
    /// `header.commitment_bytes`.
    pub light_client_root: [u8; 32],
}

/// Computes `C = T("commit", chain_id || u16(profile) || coding || part_root [|| proposer_node])`.
///
/// `C` covers `proposer_node` only when `fields` carries it (decision-34).
/// The coinbase decides which form a block uses, and the two forms hash
/// preimages of different lengths.
///
/// `C` names no peer: the proposer picks its seed recipients at solve time
/// (decision-19).
pub fn commit_hash(fields: &CommitmentFields) -> [u8; 32] {
    let profile_bytes = fields.profile_id.to_le_bytes();
    let coding_bytes = fields.coding.to_bytes();
    let proposer_node: &[u8] = match &fields.proposer_node {
        Some(node) => node.as_bytes(),
        None => &[],
    };

    COMMIT.hash(&[
        &fields.chain_id,
        &profile_bytes,
        &coding_bytes,
        &fields.part_root,
        proposer_node,
    ])
}

/// Builds the 35-byte null-data push committing `c`: `"BP" || 0x01 || C`.
pub fn commitment_payload(c: [u8; 32]) -> [u8; COMMITMENT_PAYLOAD_LEN] {
    let mut payload = [0u8; COMMITMENT_PAYLOAD_LEN];
    payload.copy_from_slice(&commitment_script(c)[2..]);
    payload
}

/// Builds the exact 37-byte `OP_RETURN` locking script committing `c`.
pub fn commitment_script(c: [u8; 32]) -> [u8; COMMITMENT_SCRIPT_LEN] {
    let mut script = [0u8; COMMITMENT_SCRIPT_LEN];
    script[..COMMITMENT_SCRIPT_PREFIX.len()].copy_from_slice(&COMMITMENT_SCRIPT_PREFIX);
    script[COMMITMENT_SCRIPT_PREFIX.len()..].copy_from_slice(&c);
    script
}

/// Returns the expected index-0 Merkle tree path depth for `tx_count` transactions:
/// `0` when `tx_count <= 1`, else `ceil(log2(tx_count))`.
///
/// Uses `u64` arithmetic so the function is total across the full `u32` domain (`0..=u32::MAX`).
pub const fn expected_tx_path_depth(tx_count: u32) -> usize {
    if tx_count <= 1 {
        0
    } else {
        (tx_count as u64).next_power_of_two().trailing_zeros() as usize
    }
}

/// Verifies a `HeaderMeta`'s coinbase commitment binding against an already
/// headerchain-admitted header (bpv1-2, decision-9), and returns the
/// proposer the block names (decision-34).
pub fn verify_commitment(
    network: &Network,
    admitted_height: Height,
    header: &Header,
    binding: &CoinbaseBinding,
    fields: &CommitmentFields,
) -> Result<Proposer, CommitmentError> {
    // decision-9: post-NU7 blocks only (v6 coinbases under NU5+ hashBlockCommitments).
    if NetworkUpgrade::current(network, admitted_height) < NetworkUpgrade::Nu7 {
        return Err(CommitmentError::UnsupportedNetworkUpgrade);
    }

    let coinbase = verify_txid_path(header, binding, fields.coding.tx_count)?;

    check_block_commitments(
        header,
        &coinbase,
        &binding.auth_siblings,
        binding.light_client_root,
    )?;

    // check-4: exactly one version-01 commitment output (bpv1-70).
    let found_c = extract_single_commitment(&coinbase)?;

    // check-5: the coinbase decides whether `HeaderMeta` names the proposer
    // (decision-34); then recompute `C` and compare.
    let proposer = Proposer::named(&coinbase, fields.proposer_node)?;
    let expected_c = commit_hash(fields);
    if found_c != expected_c {
        return Err(CommitmentError::CommitmentHashMismatch);
    }

    Ok(proposer)
}

/// check-2: decodes the binding's coinbase and checks that its txid at index 0
/// reaches `header.merkle_root` through `txid_siblings`.
///
/// Both sibling paths must have the depth that `tx_count` transactions give.
/// The check needs no chain context.
pub fn verify_txid_path(
    header: &Header,
    binding: &CoinbaseBinding,
    tx_count: u32,
) -> Result<Transaction, CommitmentError> {
    // The exact path depth is at most 32 for any `u32` count.
    let expected_depth = expected_tx_path_depth(tx_count);
    if binding.txid_siblings.len() != expected_depth
        || binding.auth_siblings.len() != expected_depth
    {
        return Err(CommitmentError::WrongPathDepth {
            expected: expected_depth,
            txid: binding.txid_siblings.len(),
            auth: binding.auth_siblings.len(),
        });
    }

    let coinbase = decode_coinbase(&binding.coinbase)?;

    let txid = coinbase.hash().0;
    let computed_merkle_root = fold_index0(txid, &binding.txid_siblings, hash_txid_pair);
    if computed_merkle_root != header.merkle_root.0 {
        return Err(CommitmentError::TxidPathMismatch);
    }
    Ok(coinbase)
}

/// Decodes the coinbase a `HeaderMeta` binds: a canonical v6 coinbase
/// transaction of at most `W2_MAX_COINBASE_BYTES` (decision-9).
///
/// The proposer runs this check before it announces a block, so a coinbase
/// that receivers refuse never leaves through BPv1.
pub fn decode_coinbase(bytes: &[u8]) -> Result<Transaction, CommitmentError> {
    // The byte bound also bounds the parsed coinbase's outputs and script bytes.
    if bytes.is_empty() || bytes.len() > W2_MAX_COINBASE_BYTES {
        return Err(CommitmentError::CoinbaseOutOfBounds);
    }
    let mut cursor = bytes;
    let coinbase = Transaction::zcash_deserialize_bounded(&mut cursor)
        .map_err(|_| CommitmentError::InvalidCoinbaseEncoding)?;
    if !cursor.is_empty() {
        return Err(CommitmentError::TrailingCoinbaseBytes);
    }
    if !matches!(coinbase, Transaction::V6 { .. }) {
        return Err(CommitmentError::UnsupportedCoinbaseVersion);
    }
    if !coinbase.is_coinbase() {
        return Err(CommitmentError::NotCoinbase);
    }
    Ok(coinbase)
}

/// check-3: the coinbase auth digest at index 0 reaches `hashAuthDataRoot`
/// through `auth_siblings`, and `hashBlockCommitments(light_client_root,
/// hashAuthDataRoot)` equals `header.commitment_bytes`.
pub fn check_block_commitments(
    header: &Header,
    coinbase: &Transaction,
    auth_siblings: &[[u8; 32]],
    light_client_root: [u8; 32],
) -> Result<(), CommitmentError> {
    let auth_digest = coinbase
        .auth_digest()
        .ok_or(CommitmentError::UnsupportedCoinbaseVersion)?
        .0;
    let auth_data_root =
        AuthDataRoot::from(fold_index0(auth_digest, auth_siblings, hash_auth_data_pair));
    let expected_commitment = ChainHistoryBlockTxAuthCommitmentHash::from_commitments(
        &ChainHistoryMmrRootHash::from(light_client_root),
        &auth_data_root,
    );
    if <[u8; 32]>::from(expected_commitment) != *header.commitment_bytes {
        return Err(CommitmentError::BlockCommitmentMismatch);
    }
    Ok(())
}

/// Returns `C` from the coinbase's only version-01 commitment output (check-4,
/// bpv1-70).
///
/// Outputs that are not version-01 commitments are ignored. With no
/// commitment, or with two or more, the coinbase has no binding.
pub fn extract_single_commitment(coinbase: &Transaction) -> Result<[u8; 32], CommitmentError> {
    let mut commitments = coinbase.outputs().iter().filter_map(commitment_from_output);
    let c = commitments
        .next()
        .ok_or(CommitmentError::MissingCommitmentOutput)?;
    if commitments.next().is_some() {
        return Err(CommitmentError::MultipleCommitmentOutputs);
    }
    Ok(c)
}

/// Returns `C` if `output` is a version-01 commitment: value zero and a script
/// of exactly `6a 23 || "BP" || 01 || C` (decision-25).
///
/// Every other output is not a commitment, including another version, length,
/// push encoding, or value.
fn commitment_from_output(output: &Output) -> Option<[u8; 32]> {
    if !output.value.is_zero() {
        return None;
    }
    output
        .lock_script
        .as_raw_bytes()
        .strip_prefix(&COMMITMENT_SCRIPT_PREFIX)?
        .try_into()
        .ok()
}

/// A block's serialized coinbase and its index-0 txid and auth-data Merkle paths.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoinbasePaths {
    /// Serialized coinbase transaction.
    pub coinbase: Vec<u8>,
    /// Txid Merkle siblings for index 0.
    pub txid_siblings: Vec<[u8; 32]>,
    /// ZIP 244 auth-data Merkle siblings for index 0.
    pub auth_siblings: Vec<[u8; 32]>,
}

/// Serializes `block`'s coinbase and builds its index-0 Merkle paths.
pub fn coinbase_paths(block: &Block) -> Result<CoinbasePaths, CommitmentError> {
    let coinbase = block
        .transactions
        .first()
        .ok_or(CommitmentError::NotCoinbase)?;
    let coinbase_bytes = coinbase
        .zcash_serialize_to_vec()
        .map_err(|_| CommitmentError::InvalidCoinbaseEncoding)?;

    let txids = block.transactions.iter().map(|tx| tx.hash().0).collect();
    let txid_siblings = index0_path(txids, hash_txid_pair);

    // ZIP 244 pads the auth-data leaves with zeros to a power of two.
    let mut auth_digests: Vec<[u8; 32]> = block
        .transactions
        .iter()
        .map(|tx| auth_digest_or_placeholder(tx).0)
        .collect();
    auth_digests.resize(auth_digests.len().next_power_of_two(), [0; 32]);
    let auth_siblings = index0_path(auth_digests, hash_auth_data_pair);

    Ok(CoinbasePaths {
        coinbase: coinbase_bytes,
        txid_siblings,
        auth_siblings,
    })
}

/// Folds leaf 0 up its sibling path, joining nodes with `node`.
fn fold_index0(leaf: [u8; 32], siblings: &[[u8; 32]], node: MerkleNode) -> [u8; 32] {
    siblings
        .iter()
        .fold(leaf, |cur, sibling| node(&cur, sibling))
}

/// The sibling path from leaf 0 to the root of the Merkle tree over `level`.
/// Nodes join with `node`, and an odd last node joins with itself, as in the
/// transaction tree.
fn index0_path(mut level: Vec<[u8; 32]>, node: MerkleNode) -> Vec<[u8; 32]> {
    let mut siblings = Vec::new();
    while level.len() > 1 {
        siblings.push(level[1]);
        level = level
            .chunks(2)
            .map(|pair| node(&pair[0], pair.last().expect("chunks(2) are never empty")))
            .collect();
    }
    siblings
}

/// Joins two Merkle tree nodes into their parent.
type MerkleNode = fn(&[u8; 32], &[u8; 32]) -> [u8; 32];

/// Errors produced when building or verifying a BPv1 coinbase commitment.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Error)]
pub enum CommitmentError {
    /// Block height is below NU7 (decision-9).
    #[error("BPv1 commitment binding supports post-NU7 blocks only")]
    UnsupportedNetworkUpgrade,
    /// The coinbase is empty or longer than the W2 coinbase bound.
    #[error("coinbase exceeds profile bounds")]
    CoinbaseOutOfBounds,
    /// Merkle path depth did not match `ceil(log2(tx_count))`.
    #[error("expected Merkle path depth {expected}, got txid={txid} auth={auth}")]
    WrongPathDepth {
        /// Expected depth from `coding.tx_count`.
        expected: usize,
        /// Supplied txid sibling count.
        txid: usize,
        /// Supplied auth-data sibling count.
        auth: usize,
    },
    /// Coinbase transaction failed bounded canonical deserialization.
    #[error("coinbase transaction failed bounded deserialization")]
    InvalidCoinbaseEncoding,
    /// Trailing bytes remained after parsing the coinbase transaction.
    #[error("trailing bytes after coinbase transaction")]
    TrailingCoinbaseBytes,
    /// Coinbase is not a v6 transaction (decision-9).
    #[error("coinbase must be a v6 transaction")]
    UnsupportedCoinbaseVersion,
    /// Supplied transaction is not a coinbase transaction.
    #[error("transaction is not a coinbase")]
    NotCoinbase,
    /// Index-0 txid Merkle path did not reproduce `header.merkle_root`.
    #[error("coinbase txid Merkle path mismatch")]
    TxidPathMismatch,
    /// Recomputed `hashBlockCommitments` did not match `header.commitment_bytes`.
    #[error("header block commitment mismatch")]
    BlockCommitmentMismatch,
    /// The coinbase holds no version-01 commitment output.
    #[error("coinbase has no BPv1 version-01 commitment output")]
    MissingCommitmentOutput,
    /// The coinbase holds two or more version-01 commitment outputs.
    #[error("coinbase has multiple BPv1 version-01 commitment outputs")]
    MultipleCommitmentOutputs,
    /// The coinbase holds no single miner mesh output, and `HeaderMeta`
    /// names no proposer (decision-34).
    #[error("HeaderMeta names no proposer and the coinbase has no miner mesh output")]
    MissingProposer,
    /// The coinbase's miner mesh output names the proposer, and `HeaderMeta`
    /// names one too (decision-34).
    #[error("HeaderMeta names a proposer that the coinbase's miner mesh output already names")]
    ProposerNamedTwice,
    /// Recomputed commitment hash `C` did not equal the coinbase output's `C`.
    #[error("recomputed commitment hash C mismatch")]
    CommitmentHashMismatch,
}
