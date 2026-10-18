//! Validates that the database has the genesis Ironwood note commitment tree, and an Ironwood
//! anchor for the finalized tip's tree.
//!
//! The genesis block commit writes the empty Ironwood tree and its anchor. The 28.0.0 format
//! upgrade backfilled them for older databases. Startup refuses databases older than 28.0.0, so
//! only this check remains.

use zakura_chain::block::Height;

use crate::service::finalized_state::ZakuraDb;

/// Checks the genesis Ironwood tree and the finalized tip's Ironwood anchor.
pub(super) fn detailed_check(db: &ZakuraDb) -> Result<(), String> {
    if db.finalized_tip_height().is_none() {
        return Ok(());
    }

    if db
        .ironwood_tree_by_height_range(..=Height::MIN)
        .next()
        .is_none()
    {
        return Err("missing Ironwood note commitment tree for the genesis height".to_string());
    }

    let ironwood_tree = db.ironwood_tree_for_tip();
    if !db.contains_ironwood_anchor(&ironwood_tree.root()) {
        return Err("missing Ironwood anchor for the finalized tip's Ironwood tree".to_string());
    }

    Ok(())
}
