//! BPv1: block propagation for Zakura v2 P2P.
//!
//! Organized along the clean separation of pure functions, pure state machines,
//! ports, and runtime IO:
//! - [`profile`]: `Profile` (`W2`, `TINY`), coding layout derivation and checked arithmetic
//! - [`tree`]: BLAKE3 KDF part Merkle tree and proof-gated [`VerifiedPart`]
//! - [`codec`]: systematic Reed–Solomon GF(2^16) stripe encoding, decoding, and re-encode check
//! - [`commitment`]: post-NU7 v6 coinbase commitment construction and verification
//! - [`assemble`]: canonical block assembly with zero-padding and size checks
//! - [`close`]: versioned `bpv1-close-N` close-cause enum
//! - [`wire`]: seven-message wire grammar, message tables, `WireMessage`, and FIFO data writer

pub mod assemble;
pub mod close;
pub mod codec;
pub mod commitment;
pub mod profile;
pub mod tree;
pub mod wire;

pub use assemble::{assemble_block, AssembleError};
pub use close::{Bpv1CloseCause, Bpv1CloseRow};
pub use codec::{decode_and_check_stripe, encode_body, CodecError, EncodedBody};
pub use commitment::{
    coinbase_paths, commit_hash, commitment_payload, commitment_script, expected_tx_path_depth,
    extract_single_commitment, verify_commitment, CoinbaseBinding, CommitmentError,
    CommitmentFields, COMMITMENT_PAYLOAD_LEN, COMMITMENT_SCRIPT_LEN, COMMITMENT_SCRIPT_PREFIX,
};
pub use profile::{Coding, LayoutError, Profile, W2_PROFILE_ID};
pub use tree::{
    build_part_tree, encoding_id, pad_hash, verify_part_proof, Bpv1Hash, ProofError, VerifiedPart,
};
pub use wire::{
    encode_part_frame, read_part_prefix, Bpv1FifoQueue, Bpv1Message, DataQueueItem, HaveFlags,
    PartPrefix, WantBitmap, WantEndReason, BPV1_CONTROL_BUDGET, BPV1_CONTROL_KIND,
    BPV1_DATA_BUDGET, BPV1_DATA_KIND, BPV1_STREAMS, BPV1_STREAM_VERSION, MAX_WANTS_INFLIGHT,
    PROPAGATION_DEPTH, ZAKURA_CAP_BPV1,
};
pub use zakura_quic::NodeId;
