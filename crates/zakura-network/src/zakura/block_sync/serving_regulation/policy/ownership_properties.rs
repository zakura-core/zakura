//! Exercise shared admission with GetBlocks' real request and response limits.
//! The same checkers run with GetPeers in `regulation::request::properties`.

use super::*;
use proptest::prelude::*;

proptest! {
    #[test]
    fn get_blocks_request_owners_match_shared_model(
        node_capacity in 1usize..=2,
        choices in prop::collection::vec((any::<u8>(), any::<u8>()), 1..160),
    ) {
        crate::zakura::regulation::check_request_owners(
            GetBlocksPolicy::new(&ZakuraBlockSyncConfig::default()),
            tests::frame(block::Height(42), 1),
            node_capacity,
            &choices,
        )?;
    }

    #[test]
    fn get_blocks_admission_waiters_match_shared_model(
        node_capacity in 1usize..=3,
        choices in prop::collection::vec((any::<u8>(), any::<u8>()), 1..160),
    ) {
        crate::zakura::regulation::check_admission_waiters(
            GetBlocksPolicy::new(&ZakuraBlockSyncConfig::default()),
            tests::frame(block::Height(42), 1),
            node_capacity,
            &choices,
        )?;
    }

}
