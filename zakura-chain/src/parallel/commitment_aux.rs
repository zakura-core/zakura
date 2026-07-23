//! Cross-client commitment-auxiliary payload types for Zakura header sync.
//!
//! These travel over the Zakura `tree_aux` stream (increment 6) and are also produced
//! and consumed locally by `zakura-state`. They live here in `zakura-chain` so both
//! `zakura-network` and `zakura-state` can use them without a dependency cycle.
//!
//! The final-frontier handoff payload (§5.2) is *not* here: it is embedded in the
//! binary, not carried on the wire, so `tree_aux` is a roots-only stream.

use std::io;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use crate::{
    block::{self, merkle::AuthDataRoot},
    ironwood, orchard, sapling,
    serialization::{SerializationError, ZcashDeserialize, ZcashSerialize},
};

/// Per-block commitment roots carried with Zakura header-sync ranges.
///
/// One entry per height; each root is the note-commitment treestate root as of
/// end-of-block-`height`. `orchard_root` is the empty/default root below NU5.
///
/// This payload carries no trust. Authentication is per field and network-upgrade epoch:
///
/// - each note-commitment root is either pinned to its pre-activation value or folded into
///   the applicable ZIP-221 history leaf;
/// - Orchard and Ironwood transaction counts are pinned to zero before activation and folded
///   into their applicable history leaves afterward;
/// - the auth-data root is body-verified-only below NU5 and committed by the current header
///   from NU5 onward;
/// - the Sapling transaction count is folded into the V1 history leaf from Heartwood onward,
///   but is body-verified-only below Heartwood.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BlockCommitmentRoots {
    /// The block height these roots are for.
    pub height: block::Height,
    /// The Sapling note-commitment tree root as of the end of this block.
    pub sapling_root: sapling::tree::Root,
    /// The Orchard note-commitment tree root as of the end of this block (empty below NU5).
    pub orchard_root: orchard::tree::Root,
    /// The Ironwood note-commitment tree root as of the end of this block (empty below NU6.3).
    ///
    /// Carried alongside the Sapling/Orchard roots because the ZIP-221 V3 history leaf
    /// (NU6.3+) folds the Ironwood root into the chain-history MMR; a recipient rebuilding
    /// the leaf to verify against header commitments (design §6) needs it, without the body.
    pub ironwood_root: ironwood::tree::Root,
    /// Number of this block's transactions carrying Sapling shielded data
    /// (`Block::sapling_transactions_count`).
    ///
    /// The per-block shielded transaction counts are the *only* ZIP-221 history-leaf inputs
    /// the header and roots don't already provide (everything else — hash, time,
    /// difficulty/work, height — is header-derived). From Heartwood onward a wrong count
    /// changes the leaf and fails the header commitment check. Below Heartwood ZIP-221 does
    /// not exist, so this field is body-verified-only.
    pub sapling_tx: u64,
    /// Number of this block's transactions carrying Orchard shielded data
    /// (`Block::orchard_transactions_count`); pinned to zero below NU5 and authenticated by
    /// the Orchard V2 leaf from NU5 onward.
    pub orchard_tx: u64,
    /// Number of this block's transactions carrying Ironwood shielded data; the Ironwood
    /// pinned to zero below NU6.3 and authenticated by the V3 leaf from NU6.3 onward.
    pub ironwood_tx: u64,
    /// The authorizing-data root (ZIP-244 `hashAuthDataRoot`) of *this* block's own
    /// transactions. Serialized last.
    ///
    /// Carried so a recipient can authenticate the *predecessor's* note-commitment
    /// roots against this block's NU5+ header commitment
    /// (`hashBlockCommitments = BLAKE2b(chainHistoryRoot ‖ authDataRoot ‖ 0)`) without
    /// downloading this block's body. A wrong value fails verification against the current
    /// header from NU5 onward. Below NU5 the header commits the chain-history root directly,
    /// so this field is body-verified-only.
    pub auth_data_root: AuthDataRoot,
}

impl ZcashSerialize for BlockCommitmentRoots {
    fn zcash_serialize<W: io::Write>(&self, mut writer: W) -> Result<(), io::Error> {
        writer.write_u32::<LittleEndian>(self.height.0)?;
        self.sapling_root.zcash_serialize(&mut writer)?;
        self.orchard_root.zcash_serialize(&mut writer)?;
        self.ironwood_root.zcash_serialize(&mut writer)?;
        writer.write_u64::<LittleEndian>(self.sapling_tx)?;
        writer.write_u64::<LittleEndian>(self.orchard_tx)?;
        writer.write_u64::<LittleEndian>(self.ironwood_tx)?;
        writer.write_all(&<[u8; 32]>::from(self.auth_data_root))?;
        Ok(())
    }
}

impl ZcashDeserialize for BlockCommitmentRoots {
    fn zcash_deserialize<R: io::Read>(mut reader: R) -> Result<Self, SerializationError> {
        // The height is an unvalidated `u32` here; an out-of-range or wrong height simply
        // fails to match any local header during verification (design §6), so it is
        // harmless. The Sapling/Orchard root parsers reject malformed root bytes.
        let height = block::Height(reader.read_u32::<LittleEndian>()?);
        let sapling_root = sapling::tree::Root::zcash_deserialize(&mut reader)?;
        let orchard_root = orchard::tree::Root::zcash_deserialize(&mut reader)?;
        let ironwood_root = ironwood::tree::Root::zcash_deserialize(&mut reader)?;
        let sapling_tx = reader.read_u64::<LittleEndian>()?;
        let orchard_tx = reader.read_u64::<LittleEndian>()?;
        let ironwood_tx = reader.read_u64::<LittleEndian>()?;
        let mut auth_data_root = [0u8; 32];
        reader.read_exact(&mut auth_data_root)?;
        let auth_data_root = AuthDataRoot::from(auth_data_root);
        Ok(BlockCommitmentRoots {
            height,
            sapling_root,
            orchard_root,
            ironwood_root,
            sapling_tx,
            orchard_tx,
            ironwood_tx,
            auth_data_root,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serialization::ZcashDeserializeInto;

    #[test]
    fn block_commitment_roots_round_trip() {
        let roots = BlockCommitmentRoots {
            height: block::Height(1_687_200),
            sapling_root: sapling::tree::NoteCommitmentTree::default().root(),
            orchard_root: orchard::tree::NoteCommitmentTree::default().root(),
            auth_data_root: AuthDataRoot::from([7u8; 32]),
            ironwood_root: ironwood::tree::NoteCommitmentTree::default().root(),
            sapling_tx: 3,
            orchard_tx: 5,
            ironwood_tx: 7,
        };

        let bytes = roots
            .zcash_serialize_to_vec()
            .expect("serialization to a vec does not fail");
        let parsed: BlockCommitmentRoots = bytes
            .zcash_deserialize_into()
            .expect("round-trips back to the original");

        assert_eq!(
            parsed, roots,
            "BlockCommitmentRoots round-trips on the wire"
        );
    }
}
