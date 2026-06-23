//! Rebuild the finalized tip history tree in the current on-disk format.
//!
//! # Why this upgrade exists
//!
//! The history-tree node [`Entry`](zebra_chain::primitives::zcash_history::Entry) is a fixed-size
//! buffer whose length is `zcash_history::MAX_ENTRY_SIZE`. Adding Ironwood (`V3`) to the
//! `zcash_history` dependency grew `MAX_ENTRY_SIZE` (V3 node data carries the extra Ironwood tree
//! roots and tx count), so the buffer went from 253 to 326 bytes.
//!
//! [`HistoryTreeParts`](crate::service::finalized_state::disk_format::chain::HistoryTreeParts)
//! bincode-serializes the tip tree's `peaks: BTreeMap<u32, Entry>`. Databases written before the
//! Ironwood `MAX_ENTRY_SIZE` bump stored each `Entry` at the *smaller* size. The new code reads the
//! *larger* fixed array per entry, overrunning the bincode stream and panicking with
//! `Io(UnexpectedEof)` the first time anything deserializes the history-tree column family (for
//! example in `history_tree()` during backup restore, in the block-write task, or in the
//! `z_gettreestate` RPC).
//!
//! Because bincode is not self-describing and uses varint encoding here, there is no clean way to
//! detect-and-read the old layout in place. Instead, this upgrade *rebuilds* the single tip tree
//! from data that is still readable — the finalized blocks and the per-height Sapling/Orchard/
//! Ironwood note commitment tree roots — and writes it back, which re-serializes it in the current
//! `Entry` format. The MMR root is a pure function of that node data, so the rebuilt tree is
//! byte-for-byte equivalent in consensus terms (same `peaks`, same `size`, same root) to the tree a
//! fresh sync would produce.
//!
//! # When the rebuild runs
//!
//! The rebuild MUST complete before any reader deserializes the history-tree column family.
//! [`run`](Upgrade::run) is invoked from the *background* format-upgrade thread, which races
//! synchronous readers that run during state open (backup restore, the block-write task, the
//! `z_gettreestate` RPC). So the rebuild is actually performed *synchronously* while the database is
//! being opened, by [`rebuild_tip_history_tree_if_needed`], before the background thread is spawned
//! and before any reader runs. By the time [`run`](Upgrade::run) executes in the background,
//! [`needs_rebuild`] is already `false`, so [`run`](Upgrade::run) is a no-op that only participates
//! in version-marking and validation in the normal upgrade loop.

use std::sync::Arc;

use bincode::Options as _;
use crossbeam_channel::Receiver;
use semver::Version;
use thiserror::Error;

use zebra_chain::{
    block::{Block, Height},
    history_tree::HistoryTree,
    ironwood, orchard,
    parameters::{Network, NetworkUpgrade},
    sapling,
};

use crate::service::finalized_state::{
    disk_format::chain::HistoryTreeParts, DiskWriteBatch, ZebraDb,
};

use super::{CancelFormatChange, DiskFormatUpgrade};

/// An error that prevents the tip history tree from being rebuilt.
#[derive(Debug, Error)]
pub enum RebuildError {
    /// A block, or a note commitment tree, required to rebuild the history tree is missing from the
    /// database.
    ///
    /// This happens when a database has an old-format (pre-Ironwood) history-tree entry — so it
    /// needs a rebuild — but was *pruned* before the Ironwood bump, dropping the historical blocks
    /// or trees the rebuild reads. Such a database cannot be repaired in place.
    #[error(
        "cannot rebuild the tip history tree: the data at height {height:?} needed for the rebuild \
         is missing, which happens on a database that was pruned before the Ironwood upgrade. \
         Delete the cache directory and re-sync from genesis to recover."
    )]
    MissingData {
        /// The height whose block or note commitment tree could not be found.
        height: Height,
    },
}

/// Implements [`DiskFormatUpgrade`] for rebuilding the tip history tree in the current `Entry`
/// format.
///
/// This upgrade is the capstone of the bump to database format major version 28: the Ironwood tree,
/// value pool, and index data are backfilled by earlier upgrades, and this upgrade rebuilds the
/// stored history tree entry so it uses the Ironwood-capable entry size. Its [`version`] is
/// therefore the in-code format version, so an upgraded database ends at the running version and the
/// standalone rollback/prune tools (which require an exact version match) accept it.
///
/// [`version`]: Upgrade::version
pub struct Upgrade;

impl DiskFormatUpgrade for Upgrade {
    fn version(&self) -> Version {
        // The capstone of the bump to format major version 28: this is the in-code format version,
        // so an upgraded database lands at the running version. See the type-level docs.
        //
        // We take only the major.minor.patch and drop any build metadata (the `indexer` build tag),
        // matching every other `version()` here. Build metadata on the on-disk version is managed
        // separately by `apply_format_upgrade` (it is added/removed depending on the `indexer`
        // feature), and excluding it keeps this upgrade's declared version exactly equal to the
        // value the version-ordering checks and the standalone tools compare against.
        let in_code = crate::constants::state_database_format_version_in_code();
        Version::new(in_code.major, in_code.minor, in_code.patch)
    }

    fn description(&self) -> &'static str {
        "rebuild tip history tree in current entry format"
    }

    #[allow(clippy::unwrap_in_result)]
    fn run(
        &self,
        initial_tip_height: Height,
        db: &ZebraDb,
        cancel_receiver: &Receiver<CancelFormatChange>,
    ) -> Result<(), CancelFormatChange> {
        // Return early if the upgrade is cancelled.
        if cancel_receiver.try_recv().is_ok() {
            return Err(CancelFormatChange);
        }

        // The tip tree is rebuilt synchronously while the database is opened (see
        // `rebuild_tip_history_tree_if_needed`), so by the time this runs in the background upgrade
        // thread there is normally nothing to do. This call is kept for idempotency and to handle
        // the (unreachable in production) case where the synchronous rebuild was skipped.
        if let Err(err @ RebuildError::MissingData { .. }) =
            rebuild_tip_history_tree_if_needed(db, initial_tip_height)
        {
            // A pruned old-format database can't be rebuilt. Surface it as a loud, explained panic
            // rather than marking the database as upgraded with an unreadable entry. (The
            // synchronous open path returns this same error before this point in production.)
            panic!("{err}");
        }

        Ok(())
    }

    #[allow(clippy::unwrap_in_result)]
    fn validate(
        &self,
        db: &ZebraDb,
        _cancel_receiver: &Receiver<CancelFormatChange>,
    ) -> Result<Result<(), String>, CancelFormatChange> {
        Ok(quick_check(db))
    }
}

/// Rebuilds the tip history tree in the current `Entry` format if the stored entry is in an older,
/// unreadable format, writing the rebuilt tree back under the same `()` key.
///
/// This is called *synchronously* while the database is being opened, before any code path
/// deserializes the history-tree column family, so the unreadable entry is replaced before it can
/// trigger a panic. It is a no-op for databases that are already in the current format (including
/// newly created and freshly synced databases), so it is safe to call unconditionally on the node's
/// open path.
///
/// # Errors
///
/// Returns [`RebuildError::MissingData`] if a block or note commitment tree the rebuild needs is
/// absent (a database pruned before the Ironwood bump). The caller should treat this as fatal and
/// ask the operator to delete and re-sync, because the database has an unreadable history-tree entry
/// that cannot be repaired.
#[allow(clippy::unwrap_in_result)]
pub(crate) fn rebuild_tip_history_tree_if_needed(
    db: &ZebraDb,
    tip_height: Height,
) -> Result<(), RebuildError> {
    // Nothing to rebuild if the tip tree is already readable in the current format. This is the
    // case for databases that were created or last written by code with the current
    // `MAX_ENTRY_SIZE`, including freshly synced databases and pruned databases written after the
    // Ironwood bump.
    if !needs_rebuild(db) {
        return Ok(());
    }

    let network = db.network();

    let Some(history_tree) = rebuild_tip_history_tree(db, &network, tip_height)? else {
        // Pre-Heartwood tips have no history tree, so there is nothing to rebuild. (Any stale entry
        // would be deleted rather than rewritten, but pre-Heartwood databases never wrote one.)
        return Ok(());
    };

    // Writing the tree back to the database re-serializes it in the current `Entry` format,
    // overwriting the unreadable old-format entry under the same `()` key.
    let mut batch = DiskWriteBatch::new();
    batch.update_history_tree(db, &history_tree);
    db.write_batch(batch)
        .expect("rewriting the tip history tree in the current format should always succeed");

    Ok(())
}

/// Returns `true` if the tip history tree entry exists but cannot be deserialized in the current
/// format, and therefore needs to be rebuilt.
///
/// Reads the entry as raw bytes and attempts a non-panicking deserialization in the current format.
/// An entry that fails this check was written with a smaller `Entry` buffer by an older Zebra
/// version, which is exactly the case this upgrade repairs.
pub(crate) fn needs_rebuild(db: &ZebraDb) -> bool {
    let Some(raw_entry) = db.raw_history_tree_value_cf().zs_get(&()) else {
        // No tip tree stored (empty/pre-Heartwood database), so there is nothing to rebuild.
        return false;
    };

    bincode::DefaultOptions::new()
        .deserialize::<HistoryTreeParts>(raw_entry.raw_bytes())
        .is_err()
}

/// Rebuilds the finalized tip history tree from finalized blocks and the per-height note commitment
/// tree roots.
///
/// Returns `Ok(None)` if the tip is pre-Heartwood, where no history tree exists.
///
/// The history tree resets at every network upgrade boundary, so the tip tree only contains blocks
/// from the current network upgrade's activation height up to the tip. Rebuilding from that
/// activation height reproduces the identical tree.
///
/// This mirrors the rollback tool's `rebuild_history_tree_from_upgrade_activation`, so the rebuilt
/// tree matches the tree a fresh sync produces.
///
/// # Errors
///
/// Returns [`RebuildError::MissingData`] if a block or note commitment tree needed for the rebuild
/// is missing (a database pruned before the Ironwood bump).
//
// The `.expect()`s below are on genuine invariants of a finalized, already-validated chain (the
// activation height exists, and the history tree always accepts a finalized block), not on the
// missing-data condition, which is reported via `RebuildError`.
#[allow(clippy::unwrap_in_result)]
pub(crate) fn rebuild_tip_history_tree(
    db: &ZebraDb,
    network: &Network,
    tip_height: Height,
) -> Result<Option<HistoryTree>, RebuildError> {
    let network_upgrade = NetworkUpgrade::current(network, tip_height);

    if network_upgrade < NetworkUpgrade::Heartwood {
        return Ok(None);
    }

    let start_height = network_upgrade
        .activation_height(network)
        .expect("network upgrades at or after Heartwood have an activation height");

    let (block, sapling_root, orchard_root, ironwood_root) =
        history_rebuild_inputs_at_height(db, start_height)?;
    let mut history_tree =
        HistoryTree::from_block(network, block, &sapling_root, &orchard_root, &ironwood_root)
            .expect("rebuilding the tip history tree from a finalized block should always succeed");

    for height in ((start_height.0 + 1)..=tip_height.0).map(Height) {
        let (block, sapling_root, orchard_root, ironwood_root) =
            history_rebuild_inputs_at_height(db, height)?;

        history_tree
            .push(network, block, &sapling_root, &orchard_root, &ironwood_root)
            .expect("pushing a finalized block onto the tip history tree should always succeed");
    }

    Ok(Some(history_tree))
}

/// Loads the block and the Sapling, Orchard, and Ironwood note commitment tree roots at `height`,
/// which are the inputs needed to add a block to the history tree.
///
/// This reads only column families that are unaffected by the `Entry` format change, so it works on
/// a database whose history-tree column family is in the old format.
///
/// # Errors
///
/// Returns [`RebuildError::MissingData`] if the block or a note commitment tree at `height` is
/// absent. This happens on a database that was pruned before the Ironwood bump: it still has an
/// old-format history-tree entry (so a rebuild is needed) but no longer has the historical data the
/// rebuild requires.
fn history_rebuild_inputs_at_height(
    db: &ZebraDb,
    height: Height,
) -> Result<
    (
        Arc<Block>,
        sapling::tree::Root,
        orchard::tree::Root,
        ironwood::tree::Root,
    ),
    RebuildError,
> {
    let block = db
        .block(height.into())
        .ok_or(RebuildError::MissingData { height })?;
    let sapling_root = db
        .sapling_tree_by_height(&height)
        .ok_or(RebuildError::MissingData { height })?
        .root();
    let orchard_root = db
        .orchard_tree_by_height(&height)
        .ok_or(RebuildError::MissingData { height })?
        .root();
    // Ironwood trees are only stored from the Ironwood activation height onwards, and are
    // de-duplicated, so search backwards for the most recent one. Before Ironwood activation the
    // root is the empty-tree root, which the pre-Ironwood history tree versions ignore.
    let ironwood_root = match db.ironwood_tree_by_height_range(..=height).last() {
        Some((_height, tree)) => tree.root(),
        None => Default::default(),
    };

    Ok((block, sapling_root, orchard_root, ironwood_root))
}

/// Quickly checks that the tip history tree can be read in the current format.
///
/// After this upgrade runs, the entry (if any) must deserialize cleanly in the current `Entry`
/// format. An entry that still fails means the rebuild did not complete.
pub fn quick_check(db: &ZebraDb) -> Result<(), String> {
    if needs_rebuild(db) {
        let err = Err(
            "tip history tree could not be read in the current format after the history tree \
             rebuild upgrade"
                .to_string(),
        );
        error!(?err);
        return err;
    }

    Ok(())
}
