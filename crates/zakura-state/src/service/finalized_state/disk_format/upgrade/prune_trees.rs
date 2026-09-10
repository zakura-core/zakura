//! Validates that duplicate Sapling and Orchard note commitment trees are
//! absent from the database.

use crossbeam_channel::{Receiver, TryRecvError};

use crate::service::finalized_state::ZakuraDb;

use super::{CancelFormatChange, FormatChangeError};

/// Checks that note commitment trees use the deduplicated current format.
pub(super) fn detailed_check(
    db: &ZakuraDb,
    cancel_receiver: &Receiver<CancelFormatChange>,
) -> Result<Result<(), String>, FormatChangeError> {
    let mut result = Ok(());

    let mut previous_height = None;
    let mut previous_tree = None;
    for (height, tree) in db.sapling_tree_by_height_range(..) {
        if !matches!(cancel_receiver.try_recv(), Err(TryRecvError::Empty)) {
            return Err(CancelFormatChange.into());
        }

        if previous_tree == Some(tree.clone()) {
            result = Err(format!(
                "found duplicate sapling trees: height: {height:?}, previous height: {:?}, \
                 tree root: {:?}",
                previous_height.expect("a duplicate tree has a previous height"),
                tree.root()
            ));
            error!(?result);
        }

        previous_height = Some(height);
        previous_tree = Some(tree);
    }

    let mut previous_height = None;
    let mut previous_tree = None;
    for (height, tree) in db.orchard_tree_by_height_range(..) {
        if !matches!(cancel_receiver.try_recv(), Err(TryRecvError::Empty)) {
            return Err(CancelFormatChange.into());
        }

        if previous_tree == Some(tree.clone()) {
            result = Err(format!(
                "found duplicate orchard trees: height: {height:?}, previous height: {:?}, \
                 tree root: {:?}",
                previous_height.expect("a duplicate tree has a previous height"),
                tree.root()
            ));
            error!(?result);
        }

        previous_height = Some(height);
        previous_tree = Some(tree);
    }

    Ok(result)
}
