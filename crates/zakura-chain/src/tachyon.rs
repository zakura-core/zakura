//! Tachyon shielded-pool chain-state types.

use std::{fmt, io};

use serde::{Deserialize, Serialize};

use crate::{
    block::Block,
    parameters::{Network, NetworkUpgrade},
    serialization::{ReadZcashExt, SerializationError, ZcashDeserialize, ZcashSerialize},
};

/// The number of blocks in a Tachyon epoch.
pub const EPOCH_LENGTH: u32 = zcash_tachyon::constants::EPOCH_SIZE;

/// Returns the zero-based Tachyon pool height at `height`.
pub fn pool_height(network: &Network, height: crate::block::Height) -> Option<u32> {
    let activation_height = NetworkUpgrade::NuTachyon.activation_height(network)?;
    height.0.checked_sub(activation_height.0)
}

/// Returns the Tachyon epoch containing `pool_height`.
pub fn epoch_of_pool_height(pool_height: u32) -> u32 {
    pool_height / EPOCH_LENGTH
}

/// Returns `true` if `pool_height` is the first block in its epoch.
pub fn is_epoch_first(pool_height: u32) -> bool {
    pool_height.is_multiple_of(EPOCH_LENGTH)
}

/// Returns the Tachyon epoch containing `height`, if the pool is active.
pub fn epoch(network: &Network, height: crate::block::Height) -> Option<u32> {
    pool_height(network, height).map(epoch_of_pool_height)
}

/// Returns `true` if `earlier` is in `later`'s two-epoch consensus scan window.
pub fn within_scan_window(
    network: &Network,
    earlier: crate::block::Height,
    later: crate::block::Height,
) -> bool {
    match (epoch(network, earlier), epoch(network, later)) {
        (Some(earlier_epoch), Some(later_epoch)) => earlier_epoch + 1 >= later_epoch,
        _ => false,
    }
}

/// The running Tachyon pool anchor after a block.
#[derive(Clone, Copy, Default, Eq, PartialEq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Anchor(pub [u8; 32]);

impl fmt::Debug for Anchor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("tachyon::Anchor")
            .field(&hex::encode(self.0))
            .finish()
    }
}

impl From<[u8; 32]> for Anchor {
    fn from(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}

impl From<Anchor> for [u8; 32] {
    fn from(anchor: Anchor) -> Self {
        anchor.0
    }
}

impl From<&Anchor> for [u8; 32] {
    fn from(anchor: &Anchor) -> Self {
        anchor.0
    }
}

impl ZcashSerialize for Anchor {
    fn zcash_serialize<W: io::Write>(&self, mut writer: W) -> Result<(), io::Error> {
        writer.write_all(&self.0)
    }
}

impl ZcashDeserialize for Anchor {
    fn zcash_deserialize<R: io::Read>(mut reader: R) -> Result<Self, SerializationError> {
        Ok(Self(reader.read_32_bytes()?))
    }
}

impl From<zcash_tachyon::Anchor> for Anchor {
    fn from(anchor: zcash_tachyon::Anchor) -> Self {
        let mut bytes = Vec::with_capacity(32);
        anchor
            .write(&mut bytes)
            .expect("serializing a Tachyon anchor into a Vec is infallible");
        Self(
            bytes
                .try_into()
                .expect("Tachyon anchors always encode as 32 bytes"),
        )
    }
}

/// A nullifier or note commitment revealed by a Tachyon proof stamp.
#[derive(Clone, Copy, Eq, PartialEq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Tachygram(pub [u8; 32]);

impl fmt::Debug for Tachygram {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("tachyon::Tachygram")
            .field(&hex::encode(self.0))
            .finish()
    }
}

impl From<[u8; 32]> for Tachygram {
    fn from(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}

impl From<Tachygram> for [u8; 32] {
    fn from(tachygram: Tachygram) -> Self {
        tachygram.0
    }
}

impl From<&Tachygram> for [u8; 32] {
    fn from(tachygram: &Tachygram) -> Self {
        tachygram.0
    }
}

impl From<zcash_tachyon::Tachygram> for Tachygram {
    fn from(tachygram: zcash_tachyon::Tachygram) -> Self {
        let field: halo2::pasta::pallas::Base = tachygram.into();
        Self(field.into())
    }
}

impl Anchor {
    fn to_tachyon(self) -> zcash_tachyon::Anchor {
        zcash_tachyon::Anchor::read(&self.0[..])
            .expect("stored Tachyon anchors are canonical field-element encodings")
    }

    /// Computes the Tachyon pool anchor after `block`.
    pub fn advance_with_block(
        &self,
        pool_height: u32,
        block: &Block,
    ) -> Result<AnchorAdvance, zcash_tachyon::AnchorError> {
        use zcash_tachyon::TachyonBundle;

        let epoch = zcash_tachyon::EpochIndex(epoch_of_pool_height(pool_height));
        let mut anchor = if pool_height == 0 {
            zcash_tachyon::Anchor::default()
        } else {
            self.to_tachyon()
        };
        let epoch_boundary = if pool_height == 0 {
            Some(Anchor::from(anchor))
        } else if is_epoch_first(pool_height) {
            anchor = anchor.next_epoch(epoch)?;
            Some(Anchor::from(anchor))
        } else {
            None
        };

        for transaction in &block.transactions {
            let Some(shielded_data) = transaction.tachyon_shielded_data() else {
                continue;
            };
            if let TachyonBundle::Proven(bundle) = &shielded_data.0 {
                anchor = anchor.next_stamp(epoch, &bundle.stamp.tachygram_set)?;
            }
        }

        Ok(AnchorAdvance {
            post_block: Anchor::from(anchor),
            epoch_boundary,
        })
    }
}

/// The result of advancing the Tachyon anchor through a block.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AnchorAdvance {
    /// The anchor after the block.
    pub post_block: Anchor,
    /// The anchor immediately after an epoch lift, if this block starts an epoch.
    pub epoch_boundary: Option<Anchor>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn anchor_serialization_round_trip() {
        let anchor = Anchor([42; 32]);
        let mut bytes = Vec::new();
        anchor.zcash_serialize(&mut bytes).unwrap();
        assert_eq!(Anchor::zcash_deserialize(&bytes[..]).unwrap(), anchor);
    }

    #[test]
    fn epoch_helpers_follow_epoch_length() {
        assert_eq!(epoch_of_pool_height(0), 0);
        assert_eq!(epoch_of_pool_height(EPOCH_LENGTH), 1);
        assert!(is_epoch_first(0));
        assert!(is_epoch_first(EPOCH_LENGTH));
    }
}
