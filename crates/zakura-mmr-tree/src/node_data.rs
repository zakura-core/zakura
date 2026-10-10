use crate::U256;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use crate::Version;

/// Maximum serialized size of the node metadata.
pub const MAX_NODE_DATA_SIZE: usize = 32 + // subtree commitment
    4 +  // start time
    4 +  // end time
    4 +  // start target
    4 +  // end target
    32 + // start sapling tree root
    32 + // end sapling tree root
    32 + // subtree total work
    9 +  // start height (compact uint)
    9 +  // end height (compact uint)
    9 + // Sapling tx count (compact uint)
    32 + // start Orchard tree root
    32 + // end Orchard tree root
    9 + // Orchard tx count (compact uint)
    32 + // start Ironwood tree root
    32 + // end Ironwood tree root
    9 + // Ironwood tx count (compact uint)
    if cfg!(zcash_unstable = "nutachyon") {
        32 + // start Tachyon anchor
        32 + // end Tachyon anchor
        9 // Tachyon tx count (compact uint)
    } else {
        0
    };

/// V1 node metadata.
#[repr(C)]
#[derive(Debug, Clone, Default)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub struct NodeData {
    /// Consensus branch id, should be provided by deserializing node.
    pub consensus_branch_id: u32,
    /// Subtree commitment - either block hash for leaves or hashsum of children
    /// for nodes.
    pub subtree_commitment: [u8; 32],
    /// Start time.
    pub start_time: u32,
    /// End time.
    pub end_time: u32,
    /// Start target.
    pub start_target: u32,
    /// End target.
    pub end_target: u32,
    /// Start sapling tree root.
    pub start_sapling_root: [u8; 32],
    /// End sapling tree root.
    pub end_sapling_root: [u8; 32],
    /// Part of tree total work.
    pub subtree_total_work: U256,
    /// Start height.
    pub start_height: u64,
    /// End height
    pub end_height: u64,
    /// Number of Sapling transactions.
    pub sapling_tx: u64,
}

impl NodeData {
    /// Combine two nodes metadata.
    pub fn combine(left: &NodeData, right: &NodeData) -> NodeData {
        crate::V1::combine(left, right)
    }

    pub(crate) fn combine_inner(
        subtree_commitment: [u8; 32],
        left: &NodeData,
        right: &NodeData,
    ) -> NodeData {
        NodeData {
            consensus_branch_id: left.consensus_branch_id,
            subtree_commitment,
            start_time: left.start_time,
            end_time: right.end_time,
            start_target: left.start_target,
            end_target: right.end_target,
            start_sapling_root: left.start_sapling_root,
            end_sapling_root: right.end_sapling_root,
            subtree_total_work: left.subtree_total_work + right.subtree_total_work,
            start_height: left.start_height,
            end_height: right.end_height,
            sapling_tx: left.sapling_tx + right.sapling_tx,
        }
    }

    fn write_compact<W: std::io::Write>(w: &mut W, compact: u64) -> std::io::Result<()> {
        match compact {
            // The match arm bounds compact below u8::MAX.
            0..=0xfc => w.write_all(&[compact as u8])?,
            0xfd..=0xffff => {
                w.write_all(&[0xfd])?;
                // The match arm bounds compact at u16::MAX.
                w.write_u16::<LittleEndian>(compact as u16)?;
            }
            0x10000..=0xffff_ffff => {
                w.write_all(&[0xfe])?;
                // The match arm bounds compact at u32::MAX.
                w.write_u32::<LittleEndian>(compact as u32)?;
            }
            _ => {
                w.write_all(&[0xff])?;
                w.write_u64::<LittleEndian>(compact)?;
            }
        }
        Ok(())
    }

    fn read_compact<R: std::io::Read>(reader: &mut R) -> std::io::Result<u64> {
        let result = match reader.read_u8()? {
            i @ 0..=0xfc => i.into(),
            0xfd => reader.read_u16::<LittleEndian>()?.into(),
            0xfe => reader.read_u32::<LittleEndian>()?.into(),
            _ => reader.read_u64::<LittleEndian>()?,
        };

        Ok(result)
    }

    /// Write to the byte representation.
    pub fn write<W: std::io::Write>(&self, w: &mut W) -> std::io::Result<()> {
        w.write_all(&self.subtree_commitment)?;
        w.write_u32::<LittleEndian>(self.start_time)?;
        w.write_u32::<LittleEndian>(self.end_time)?;
        w.write_u32::<LittleEndian>(self.start_target)?;
        w.write_u32::<LittleEndian>(self.end_target)?;
        w.write_all(&self.start_sapling_root)?;
        w.write_all(&self.end_sapling_root)?;

        let work_buf = self.subtree_total_work.to_little_endian();
        w.write_all(&work_buf)?;

        Self::write_compact(w, self.start_height)?;
        Self::write_compact(w, self.end_height)?;
        Self::write_compact(w, self.sapling_tx)?;
        Ok(())
    }

    /// Read from the byte representation.
    pub fn read<R: std::io::Read>(consensus_branch_id: u32, r: &mut R) -> std::io::Result<Self> {
        let mut data = NodeData {
            consensus_branch_id,
            ..Default::default()
        };
        r.read_exact(&mut data.subtree_commitment)?;
        data.start_time = r.read_u32::<LittleEndian>()?;
        data.end_time = r.read_u32::<LittleEndian>()?;
        data.start_target = r.read_u32::<LittleEndian>()?;
        data.end_target = r.read_u32::<LittleEndian>()?;
        r.read_exact(&mut data.start_sapling_root)?;
        r.read_exact(&mut data.end_sapling_root)?;

        let mut work_buf = [0u8; 32];
        r.read_exact(&mut work_buf)?;
        data.subtree_total_work = U256::from_little_endian(&work_buf);

        data.start_height = Self::read_compact(r)?;
        data.end_height = Self::read_compact(r)?;
        data.sapling_tx = Self::read_compact(r)?;

        Ok(data)
    }

    /// Convert to byte representation.
    pub fn to_bytes(&self) -> Vec<u8> {
        crate::V1::to_bytes(self)
    }

    /// Convert from byte representation.
    pub fn from_bytes<T: AsRef<[u8]>>(consensus_branch_id: u32, buf: T) -> std::io::Result<Self> {
        crate::V1::from_bytes(consensus_branch_id, buf)
    }

    /// Hash node metadata
    pub fn hash(&self) -> [u8; 32] {
        crate::V1::hash(self)
    }
}

/// V2 node metadata.
#[derive(Debug, Clone, Default)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub struct V2 {
    /// The V1 node data retained in V2.
    pub v1: NodeData,
    /// Start Orchard tree root.
    pub start_orchard_root: [u8; 32],
    /// End Orchard tree root.
    pub end_orchard_root: [u8; 32],
    /// Number of Orchard transactions.
    pub orchard_tx: u64,
}

impl V2 {
    pub(crate) fn combine_inner(subtree_commitment: [u8; 32], left: &V2, right: &V2) -> V2 {
        V2 {
            v1: NodeData::combine_inner(subtree_commitment, &left.v1, &right.v1),
            start_orchard_root: left.start_orchard_root,
            end_orchard_root: right.end_orchard_root,
            orchard_tx: left.orchard_tx + right.orchard_tx,
        }
    }

    /// Write to the byte representation.
    pub fn write<W: std::io::Write>(&self, w: &mut W) -> std::io::Result<()> {
        self.v1.write(w)?;
        w.write_all(&self.start_orchard_root)?;
        w.write_all(&self.end_orchard_root)?;
        NodeData::write_compact(w, self.orchard_tx)?;
        Ok(())
    }

    /// Read from the byte representation.
    pub fn read<R: std::io::Read>(consensus_branch_id: u32, r: &mut R) -> std::io::Result<Self> {
        let mut data = V2 {
            v1: NodeData::read(consensus_branch_id, r)?,
            ..Default::default()
        };
        r.read_exact(&mut data.start_orchard_root)?;
        r.read_exact(&mut data.end_orchard_root)?;
        data.orchard_tx = NodeData::read_compact(r)?;

        Ok(data)
    }
}

/// V3 node metadata.
///
/// This extends the NU5 history node format with metadata for the Ironwood
/// shielded pool. Ironwood uses an Orchard-shaped note commitment tree, but is
/// represented as a distinct pool in chain history.
#[derive(Debug, Clone, Default)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub struct V3 {
    /// The V2 node data retained in V3.
    pub v2: V2,
    /// Ironwood tree root at the start of this node's interval.
    ///
    /// Leaf nodes represent a single block, so their start and end roots are
    /// both the final Ironwood note commitment tree root after the
    /// corresponding block. Internal nodes carry the start root from their
    /// leftmost leaf.
    pub start_ironwood_root: [u8; 32],
    /// Ironwood tree root at the end of this node's interval.
    ///
    /// Leaf nodes represent a single block, so their start and end roots are
    /// both the final Ironwood note commitment tree root after the
    /// corresponding block. Internal nodes carry the end root from their
    /// rightmost leaf.
    pub end_ironwood_root: [u8; 32],
    /// Number of transactions containing an Ironwood bundle.
    pub ironwood_tx: u64,
}

impl V3 {
    pub(crate) fn combine_inner(subtree_commitment: [u8; 32], left: &V3, right: &V3) -> V3 {
        V3 {
            v2: V2::combine_inner(subtree_commitment, &left.v2, &right.v2),
            start_ironwood_root: left.start_ironwood_root,
            end_ironwood_root: right.end_ironwood_root,
            ironwood_tx: left.ironwood_tx + right.ironwood_tx,
        }
    }

    /// Write to the byte representation.
    pub fn write<W: std::io::Write>(&self, w: &mut W) -> std::io::Result<()> {
        self.v2.write(w)?;
        w.write_all(&self.start_ironwood_root)?;
        w.write_all(&self.end_ironwood_root)?;
        NodeData::write_compact(w, self.ironwood_tx)?;
        Ok(())
    }

    /// Read from the byte representation.
    pub fn read<R: std::io::Read>(consensus_branch_id: u32, r: &mut R) -> std::io::Result<Self> {
        let mut data = V3 {
            v2: V2::read(consensus_branch_id, r)?,
            ..Default::default()
        };
        r.read_exact(&mut data.start_ironwood_root)?;
        r.read_exact(&mut data.end_ironwood_root)?;
        data.ironwood_tx = NodeData::read_compact(r)?;

        Ok(data)
    }
}

/// V4 node metadata.
///
/// This extends the NU6.3 history node format with metadata for the Tachyon
/// shielded pool. Tachyon has no note commitment tree; its pool state is a
/// running anchor, so the anchor takes the place a tree root has for the other
/// pools.
#[cfg(zcash_unstable = "nutachyon")]
#[derive(Debug, Clone, Default)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub struct V4 {
    /// The V3 node data retained in V4.
    pub v3: V3,
    /// Tachyon anchor at the start of this node's interval.
    pub start_tachyon_anchor: [u8; 32],
    /// Tachyon anchor at the end of this node's interval.
    pub end_tachyon_anchor: [u8; 32],
    /// Number of transactions containing a Tachyon bundle.
    pub tachyon_tx: u64,
}

#[cfg(zcash_unstable = "nutachyon")]
impl V4 {
    pub(crate) fn combine_inner(subtree_commitment: [u8; 32], left: &V4, right: &V4) -> V4 {
        V4 {
            v3: V3::combine_inner(subtree_commitment, &left.v3, &right.v3),
            start_tachyon_anchor: left.start_tachyon_anchor,
            end_tachyon_anchor: right.end_tachyon_anchor,
            tachyon_tx: left.tachyon_tx + right.tachyon_tx,
        }
    }

    /// Write to the byte representation.
    pub fn write<W: std::io::Write>(&self, w: &mut W) -> std::io::Result<()> {
        self.v3.write(w)?;
        w.write_all(&self.start_tachyon_anchor)?;
        w.write_all(&self.end_tachyon_anchor)?;
        NodeData::write_compact(w, self.tachyon_tx)?;
        Ok(())
    }

    /// Read from the byte representation.
    pub fn read<R: std::io::Read>(consensus_branch_id: u32, r: &mut R) -> std::io::Result<Self> {
        let mut data = V4 {
            v3: V3::read(consensus_branch_id, r)?,
            ..Default::default()
        };
        r.read_exact(&mut data.start_tachyon_anchor)?;
        r.read_exact(&mut data.end_tachyon_anchor)?;
        data.tachyon_tx = NodeData::read_compact(r)?;

        Ok(data)
    }
}

#[cfg(test)]
pub(crate) mod testing {
    use crate::U256;
    use proptest::array::uniform32;
    use proptest::prelude::{any, prop_compose};

    use super::NodeData;

    prop_compose! {
        pub(crate) fn arb_node_data()(
            subtree_commitment in uniform32(any::<u8>()),
            start_time in any::<u32>(),
            end_time in any::<u32>(),
            start_target in any::<u32>(),
            end_target in any::<u32>(),
            start_sapling_root in uniform32(any::<u8>()),
            end_sapling_root in uniform32(any::<u8>()),
            subtree_total_work in uniform32(any::<u8>()),
            start_height in any::<u64>(),
            end_height in any::<u64>(),
            sapling_tx in any::<u64>(),
        ) -> NodeData {
            NodeData {
                consensus_branch_id: 0,
                subtree_commitment,
                start_time,
                end_time,
                start_target,
                end_target,
                start_sapling_root,
                end_sapling_root,
                subtree_total_work: U256::from_little_endian(&subtree_total_work[..]),
                start_height,
                end_height,
                sapling_tx
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::testing::arb_node_data;
    use proptest::prelude::*;

    use crate::U256;

    #[cfg(zcash_unstable = "nutachyon")]
    use crate::V4 as HistoryV4;
    use crate::{
        Entry, EntryLink, Version, MAX_ENTRY_SIZE, V1 as HistoryV1, V2 as HistoryV2,
        V3 as HistoryV3,
    };

    #[cfg(zcash_unstable = "nutachyon")]
    use super::V4;
    use super::{NodeData, MAX_NODE_DATA_SIZE, V2, V3};

    fn node_data(start_height: u64, end_height: u64) -> NodeData {
        NodeData {
            consensus_branch_id: 1,
            subtree_commitment: [1; 32],
            start_time: 2,
            end_time: 3,
            start_target: 4,
            end_target: 5,
            start_sapling_root: [6; 32],
            end_sapling_root: [7; 32],
            subtree_total_work: U256::from(8u64),
            start_height,
            end_height,
            sapling_tx: 9,
        }
    }

    fn node_data_v2(start_height: u64, end_height: u64) -> V2 {
        V2 {
            v1: node_data(start_height, end_height),
            start_orchard_root: [10; 32],
            end_orchard_root: [11; 32],
            orchard_tx: 12,
        }
    }

    fn node_data_v3(start_height: u64, end_height: u64) -> V3 {
        V3 {
            v2: node_data_v2(start_height, end_height),
            start_ironwood_root: [13; 32],
            end_ironwood_root: [14; 32],
            ironwood_tx: 15,
        }
    }

    #[cfg(zcash_unstable = "nutachyon")]
    fn node_data_v4(start_height: u64, end_height: u64) -> V4 {
        V4 {
            v3: node_data_v3(start_height, end_height),
            start_tachyon_anchor: [16; 32],
            end_tachyon_anchor: [17; 32],
            tachyon_tx: 18,
        }
    }

    fn max_node_data() -> NodeData {
        NodeData {
            consensus_branch_id: u32::MAX,
            subtree_commitment: [1; 32],
            start_time: u32::MAX,
            end_time: u32::MAX,
            start_target: u32::MAX,
            end_target: u32::MAX,
            start_sapling_root: [2; 32],
            end_sapling_root: [3; 32],
            subtree_total_work: U256::MAX,
            start_height: u64::MAX,
            end_height: u64::MAX,
            sapling_tx: u64::MAX,
        }
    }

    fn max_node_data_v2() -> V2 {
        V2 {
            v1: max_node_data(),
            start_orchard_root: [4; 32],
            end_orchard_root: [5; 32],
            orchard_tx: u64::MAX,
        }
    }

    fn max_node_data_v3() -> V3 {
        V3 {
            v2: max_node_data_v2(),
            start_ironwood_root: [6; 32],
            end_ironwood_root: [7; 32],
            ironwood_tx: u64::MAX,
        }
    }

    #[cfg(zcash_unstable = "nutachyon")]
    fn max_node_data_v4() -> V4 {
        V4 {
            v3: max_node_data_v3(),
            start_tachyon_anchor: [8; 32],
            end_tachyon_anchor: [9; 32],
            tachyon_tx: u64::MAX,
        }
    }

    fn v1_fixture_bytes() -> Vec<u8> {
        let mut expected = vec![];
        expected.extend_from_slice(&[1; 32]);
        expected.extend_from_slice(&2u32.to_le_bytes());
        expected.extend_from_slice(&3u32.to_le_bytes());
        expected.extend_from_slice(&4u32.to_le_bytes());
        expected.extend_from_slice(&5u32.to_le_bytes());
        expected.extend_from_slice(&[6; 32]);
        expected.extend_from_slice(&[7; 32]);
        expected.extend_from_slice(&8u64.to_le_bytes());
        expected.extend_from_slice(&[0; 24]);
        expected.push(1);
        expected.push(2);
        expected.push(9);
        expected
    }

    fn v2_fixture_bytes() -> Vec<u8> {
        let mut expected = v1_fixture_bytes();
        expected.extend_from_slice(&[10; 32]);
        expected.extend_from_slice(&[11; 32]);
        expected.push(12);
        expected
    }

    fn v3_fixture_bytes() -> Vec<u8> {
        let mut expected = v2_fixture_bytes();
        expected.extend_from_slice(&[13; 32]);
        expected.extend_from_slice(&[14; 32]);
        expected.push(15);
        expected
    }

    #[cfg(zcash_unstable = "nutachyon")]
    fn v4_fixture_bytes() -> Vec<u8> {
        let mut expected = v3_fixture_bytes();
        expected.extend_from_slice(&[16; 32]);
        expected.extend_from_slice(&[17; 32]);
        expected.push(18);
        expected
    }

    proptest! {
        #[test]
        fn serialization_round_trip(node_data in arb_node_data()) {
            assert_eq!(NodeData::from_bytes(0, node_data.to_bytes()).unwrap(), node_data);
        }
    }

    #[test]
    fn full_width_work_serialization() {
        let mut work_bytes = [0; 32];
        for (byte, value) in work_bytes.iter_mut().zip(1u8..=32) {
            *byte = value;
        }
        let mut data = node_data(1, 2);
        data.subtree_total_work = U256::from_little_endian(&work_bytes);
        let mut expected = v1_fixture_bytes();
        // Commitment, four header fields, and two Sapling roots precede work.
        let work_offset = 32 + 4 * 4 + 2 * 32;
        expected[work_offset..work_offset + work_bytes.len()].copy_from_slice(&work_bytes);
        assert_eq!(data.to_bytes(), expected);
        assert_eq!(NodeData::from_bytes(1, &expected).unwrap(), data);
    }

    #[test]
    fn combining_work_carries_across_limbs() {
        let mut left = node_data(1, 1);
        let mut right = node_data(2, 2);
        left.subtree_total_work = U256([u64::MAX, u64::MAX, u64::MAX, 0]);
        right.subtree_total_work = U256::one();
        let combined = HistoryV1::combine(&left, &right);
        let mut expected_work = [0; 32];
        expected_work[24] = 1;
        assert_eq!(
            combined.subtree_total_work.to_little_endian(),
            expected_work
        );
    }

    #[test]
    fn v1_and_v2_serialization_fixtures_are_stable() {
        assert_eq!(HistoryV1::to_bytes(&node_data(1, 2)), v1_fixture_bytes());
        assert_eq!(HistoryV2::to_bytes(&node_data_v2(1, 2)), v2_fixture_bytes());
    }

    #[test]
    fn v3_serialization_round_trip() {
        let node_data = node_data_v3(1, 2);

        assert_eq!(
            HistoryV3::from_bytes(1, v3_fixture_bytes()).unwrap(),
            node_data
        );
        assert_eq!(HistoryV3::to_bytes(&node_data), v3_fixture_bytes());
    }

    #[cfg(zcash_unstable = "nutachyon")]
    #[test]
    fn v4_serialization_round_trip() {
        let node_data = node_data_v4(1, 2);

        assert_eq!(
            HistoryV4::from_bytes(1, v4_fixture_bytes()).unwrap(),
            node_data
        );
        assert_eq!(HistoryV4::to_bytes(&node_data), v4_fixture_bytes());
    }

    #[test]
    fn max_serialized_sizes_cover_all_versions() {
        assert_eq!(HistoryV1::to_bytes(&max_node_data()).len(), 171);
        assert_eq!(HistoryV2::to_bytes(&max_node_data_v2()).len(), 244);
        let max_v3_bytes = HistoryV3::to_bytes(&max_node_data_v3());
        assert_eq!(max_v3_bytes.len(), 317);
        assert_eq!(
            HistoryV3::from_bytes(u32::MAX, max_v3_bytes).unwrap(),
            max_node_data_v3()
        );
        #[cfg(not(zcash_unstable = "nutachyon"))]
        let entry = {
            assert_eq!(MAX_NODE_DATA_SIZE, 317);
            Entry::<HistoryV3>::new(
                max_node_data_v3(),
                EntryLink::Stored(u32::MAX),
                EntryLink::Stored(u32::MAX),
            )
        };

        #[cfg(zcash_unstable = "nutachyon")]
        let entry = {
            let max_v4_bytes = HistoryV4::to_bytes(&max_node_data_v4());
            assert_eq!(max_v4_bytes.len(), MAX_NODE_DATA_SIZE);
            assert_eq!(
                HistoryV4::from_bytes(u32::MAX, &max_v4_bytes).unwrap(),
                max_node_data_v4()
            );
            assert_eq!(MAX_NODE_DATA_SIZE, 390);
            Entry::<HistoryV4>::new(
                max_node_data_v4(),
                EntryLink::Stored(u32::MAX),
                EntryLink::Stored(u32::MAX),
            )
        };
        let mut encoded = vec![];
        entry.write(&mut encoded).unwrap();
        assert_eq!(encoded.len(), MAX_ENTRY_SIZE);
    }

    #[test]
    fn v3_combine_tracks_ironwood_fields() {
        let mut left = node_data_v3(1, 1);
        left.start_ironwood_root = [16; 32];
        left.end_ironwood_root = [17; 32];
        left.ironwood_tx = 18;

        let mut right = node_data_v3(2, 2);
        right.start_ironwood_root = [19; 32];
        right.end_ironwood_root = [20; 32];
        right.ironwood_tx = 21;

        let combined = HistoryV3::combine(&left, &right);

        assert_eq!(combined.v2.v1.start_height, 1);
        assert_eq!(combined.v2.v1.end_height, 2);
        assert_eq!(combined.start_ironwood_root, [16; 32]);
        assert_eq!(combined.end_ironwood_root, [20; 32]);
        assert_eq!(combined.ironwood_tx, 39);
    }

    #[cfg(zcash_unstable = "nutachyon")]
    #[test]
    fn v4_combine_tracks_tachyon_fields() {
        let mut left = node_data_v4(1, 1);
        left.start_tachyon_anchor = [22; 32];
        left.end_tachyon_anchor = [23; 32];
        left.tachyon_tx = 24;

        let mut right = node_data_v4(2, 2);
        right.start_tachyon_anchor = [25; 32];
        right.end_tachyon_anchor = [26; 32];
        right.tachyon_tx = 27;

        let combined = HistoryV4::combine(&left, &right);

        assert_eq!(combined.v3.v2.v1.start_height, 1);
        assert_eq!(combined.v3.v2.v1.end_height, 2);
        assert_eq!(combined.start_tachyon_anchor, [22; 32]);
        assert_eq!(combined.end_tachyon_anchor, [26; 32]);
        assert_eq!(combined.tachyon_tx, 51);
    }

    #[test]
    fn v3_combine_hash_commits_to_ironwood_fields() {
        let left = node_data_v3(1, 1);
        let right = node_data_v3(2, 2);
        let base_hash = HistoryV3::combine(&left, &right).v2.v1.subtree_commitment;

        let mut changed_start_root = left.clone();
        changed_start_root.start_ironwood_root[0] ^= 1;
        assert_ne!(
            HistoryV3::combine(&changed_start_root, &right)
                .v2
                .v1
                .subtree_commitment,
            base_hash
        );

        let mut changed_left_end_root = left.clone();
        changed_left_end_root.end_ironwood_root[0] ^= 1;
        assert_ne!(
            HistoryV3::combine(&changed_left_end_root, &right)
                .v2
                .v1
                .subtree_commitment,
            base_hash
        );

        let mut changed_right_start_root = right.clone();
        changed_right_start_root.start_ironwood_root[0] ^= 1;
        assert_ne!(
            HistoryV3::combine(&left, &changed_right_start_root)
                .v2
                .v1
                .subtree_commitment,
            base_hash
        );

        let mut changed_end_root = right.clone();
        changed_end_root.end_ironwood_root[0] ^= 1;
        assert_ne!(
            HistoryV3::combine(&left, &changed_end_root)
                .v2
                .v1
                .subtree_commitment,
            base_hash
        );

        let mut changed_tx_count = left.clone();
        changed_tx_count.ironwood_tx += 1;
        assert_ne!(
            HistoryV3::combine(&changed_tx_count, &right)
                .v2
                .v1
                .subtree_commitment,
            base_hash
        );

        let mut redistributed_left_tx = left.clone();
        let mut redistributed_right_tx = right;
        redistributed_left_tx.ironwood_tx += 1;
        redistributed_right_tx.ironwood_tx -= 1;
        assert_ne!(
            HistoryV3::combine(&redistributed_left_tx, &redistributed_right_tx)
                .v2
                .v1
                .subtree_commitment,
            base_hash
        );
    }
}
