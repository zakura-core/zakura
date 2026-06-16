//! Backfill Ironwood tree data and rebuild history tree entries for existing databases.

use crossbeam_channel::{Receiver, TryRecvError};
use semver::Version;
use zebra_chain::{block::Height, history_tree::HistoryTree, ironwood};

use crate::service::finalized_state::{DiskWriteBatch, ZebraDb};

use super::{CancelFormatChange, DiskFormatUpgrade};

/// Implements [`DiskFormatUpgrade`] for adding Ironwood tree and history data.
pub struct Upgrade;

impl DiskFormatUpgrade for Upgrade {
    fn version(&self) -> Version {
        Version::new(28, 0, 0)
    }

    fn description(&self) -> &'static str {
        "add ironwood value pool, indexes, and history tree metadata upgrade"
    }

    #[allow(clippy::unwrap_in_result)]
    fn run(
        &self,
        _initial_tip_height: Height,
        db: &ZebraDb,
        cancel_receiver: &Receiver<CancelFormatChange>,
    ) -> Result<(), CancelFormatChange> {
        loop {
            check_cancelled(cancel_receiver)?;

            let Some((tip, history_tree)) =
                db.cached_rebuild_history_tree_to_tip(|| check_cancelled(cancel_receiver))?
            else {
                let mut batch = DiskWriteBatch::new();
                let needs_ironwood_backfill = backfill_ironwood_tree(db, &mut batch);

                if needs_ironwood_backfill {
                    check_cancelled(cancel_receiver)?;
                    db.write_batch(batch)
                        .expect("backfilling Ironwood tree data should always succeed");
                }

                return Ok(());
            };

            check_cancelled(cancel_receiver)?;

            let mut batch = DiskWriteBatch::new();
            backfill_ironwood_tree(db, &mut batch);
            batch.update_history_tree(db, &history_tree);

            let wrote_tree = db
                .write_batch_if_finalized_tip(batch, tip)
                .expect("rewriting Ironwood history tree data should always succeed");

            if wrote_tree {
                return Ok(());
            }
        }
    }

    fn validate(
        &self,
        db: &ZebraDb,
        cancel_receiver: &Receiver<CancelFormatChange>,
    ) -> Result<Result<(), String>, CancelFormatChange> {
        let Some(tip_height) = db.finalized_tip_height() else {
            return Ok(Ok(()));
        };

        if !has_ironwood_tree_at_or_before(db, Height::MIN) {
            return Ok(Err(
                "missing Ironwood note commitment tree for the first finalized height".to_string(),
            ));
        }

        let Some((_height, ironwood_tree)) = db.ironwood_tree_by_height_range(..=tip_height).last()
        else {
            return Ok(Err(format!(
                "missing Ironwood note commitment tree for finalized tip {tip_height:?}"
            )));
        };

        if !db.contains_ironwood_anchor(&ironwood_tree.root()) {
            return Ok(Err(format!(
                "missing Ironwood anchor for finalized tip {tip_height:?}"
            )));
        }

        loop {
            check_cancelled(cancel_receiver)?;

            let Some(tip @ (tip_height, _)) = db.tip() else {
                return Ok(Ok(()));
            };

            let expected_history_tree =
                db.rebuild_history_tree_to_height(tip_height, || check_cancelled(cancel_receiver))?;
            let history_tree = db.history_tree_from_disk();

            if db.tip() != Some(tip) {
                continue;
            }

            let expected_hash = expected_history_tree.hash();
            let actual_hash = history_tree.hash();

            if actual_hash != expected_hash {
                return Ok(Err(format!(
                    "history tree hash mismatch at finalized tip {tip_height:?}: \
                     expected {expected_hash:?}, found {actual_hash:?}"
                )));
            }

            let expected_height = history_tree_height(&expected_history_tree);
            let actual_height = history_tree_height(&history_tree);

            if actual_height != expected_height {
                return Ok(Err(format!(
                    "history tree height mismatch at finalized tip {tip_height:?}: \
                     expected {expected_height:?}, found {actual_height:?}"
                )));
            }

            if db.tip() == Some(tip) {
                return Ok(Ok(()));
            }
        }
    }
}

fn backfill_ironwood_tree(db: &ZebraDb, batch: &mut DiskWriteBatch) -> bool {
    if has_ironwood_tree_at_or_before(db, Height::MIN) {
        return false;
    }

    let ironwood_tree = ironwood::tree::NoteCommitmentTree::default();
    batch.create_ironwood_tree(db, &Height::MIN, &ironwood_tree);

    true
}

fn has_ironwood_tree_at_or_before(db: &ZebraDb, height: Height) -> bool {
    db.ironwood_tree_by_height_range(..=height).next().is_some()
}

fn check_cancelled(
    cancel_receiver: &Receiver<CancelFormatChange>,
) -> Result<(), CancelFormatChange> {
    match cancel_receiver.try_recv() {
        Err(TryRecvError::Empty) => Ok(()),
        _ => Err(CancelFormatChange),
    }
}

fn history_tree_height(history_tree: &HistoryTree) -> Option<Height> {
    history_tree.as_ref().map(|tree| tree.current_height())
}
