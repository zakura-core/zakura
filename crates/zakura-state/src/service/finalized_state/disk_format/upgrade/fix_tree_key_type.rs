//! Validates the current Sprout and history tree key formats.

use std::sync::Arc;

use crossbeam_channel::Receiver;
use zakura_chain::{history_tree::HistoryTree, sprout};

use crate::service::finalized_state::ZakuraDb;

use super::CancelFormatChange;

/// Quickly check that the sprout and history tip trees have updated key formats.
///
/// # Panics
///
/// If the state is empty.
pub(super) fn quick_check(db: &ZakuraDb) -> Result<(), String> {
    // Check the entire format before returning any errors.
    let mut result = Ok(());

    let mut prev_key = None;
    let mut prev_tree: Option<Arc<sprout::tree::NoteCommitmentTree>> = None;

    for (key, tree) in db.sprout_trees_full_tip() {
        // The tip tree should be indexed by `()` (which serializes to an empty array).
        if !key.raw_bytes().is_empty() {
            result = Err(format!(
                "found incorrect sprout tree key format after running key format upgrade \
                 key: {key:?}, tree: {:?}",
                tree.root()
            ));
            error!(?result);
        }

        // There should only be one tip tree in this column family.
        if let Some(prev_tree) = prev_tree {
            result = Err(format!(
                "found duplicate sprout trees after running key format upgrade\n\
                 key: {key:?}, tree: {:?}\n\
                 prev key: {prev_key:?}, prev_tree: {:?}\n\
                 ",
                tree.root(),
                prev_tree.root(),
            ));
            error!(?result);
        }

        prev_key = Some(key);
        prev_tree = Some(tree);
    }

    let mut prev_key = None;
    let mut prev_tree: Option<Arc<HistoryTree>> = None;

    for (key, tree) in db
        .try_history_trees_full_tip()
        .map_err(|error| format!("cannot load stored history tree snapshots: {error}"))?
    {
        // The tip tree should be indexed by `()` (which serializes to an empty array).
        if !key.raw_bytes().is_empty() {
            result = Err(format!(
                "found incorrect history tree key format after running key format upgrade \
                 key: {key:?}, tree: {:?}",
                tree.hash()
            ));
            error!(?result);
        }

        // There should only be one tip tree in this column family.
        if let Some(prev_tree) = prev_tree {
            result = Err(format!(
                "found duplicate history trees after running key format upgrade\n\
                 key: {key:?}, tree: {:?}\n\
                 prev key: {prev_key:?}, prev_tree: {:?}\n\
                 ",
                tree.hash(),
                prev_tree.hash(),
            ));
            error!(?result);
        }

        prev_key = Some(key);
        prev_tree = Some(tree);
    }

    result
}

/// Detailed check that the sprout and history tip trees have updated key formats.
/// This is currently the same as the quick check.
///
/// # Panics
///
/// If the state is empty.
pub(super) fn detailed_check(
    db: &ZakuraDb,
    _cancel_receiver: &Receiver<CancelFormatChange>,
) -> Result<Result<(), String>, CancelFormatChange> {
    // This upgrade only changes two key-value pairs, so checking it is always quick.
    Ok(quick_check(db))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        service::finalized_state::{disk_format::RawBytes, FinalizedState},
        Config,
    };
    use zakura_chain::parameters::Network;

    #[test]
    fn quick_check_reports_history_snapshot_decode_errors() {
        let _init_guard = zakura_test::init();
        let state = FinalizedState::new(&Config::ephemeral(), &Network::Mainnet)
            .expect("the ephemeral state opens");
        state
            .db
            .raw_history_tree_cf()
            .new_batch_for_writing()
            .zs_insert(
                &RawBytes::new_raw_bytes(Vec::new()),
                &RawBytes::new_raw_bytes(vec![0xff]),
            )
            .write_batch()
            .expect("the malformed snapshot writes");
        let error = quick_check(&state.db).expect_err("snapshot decoding must return an error");
        assert!(error.contains("cannot load stored history tree snapshots"));
    }
}
