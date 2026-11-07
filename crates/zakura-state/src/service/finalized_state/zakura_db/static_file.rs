//! Moving finalized transaction bodies from `tx_by_loc` into static files.
//!
//! Reth writes bodies straight into static files when it persists a batch of
//! about 50 blocks, and syncs them once per batch. Zakura commits one finalized
//! block per database write, and block times keep falling, so syncing static
//! files on every commit would cost several fsyncs per block. Instead, each block
//! still lands in `tx_by_loc`, and the committer moves batches of finalized
//! blocks into static files, like reth's static file producer did before reth
//! batched its persistence.
//!
//! A move appends and syncs the static files first. Then one database batch
//! deletes the moved `tx_by_loc` rows and advances
//! [`ZakuraDb::static_file_transactions_tip`]. So the database never points past
//! durable static files, and bodies are always in at least one of the two
//! stores. The moved rows usually sit in a memtable, and RocksDB drops rows
//! covered by a range delete when it flushes, so they never reach compaction.

use std::ops::RangeInclusive;

use zakura_chain::block::Height;

use crate::{
    service::finalized_state::{
        disk_db::{DiskWriteBatch, ReadDisk, WriteDisk},
        disk_format::block::TransactionLocation,
        static_file::StaticFileError,
        zakura_db::ZakuraDb,
        IntoDisk, RawBytes, PRUNING_METADATA,
    },
    StateInitError,
};

/// Near the chain tip, moves start once this many finalized blocks are waiting,
/// reth's default persistence threshold. At 25 second blocks that is about 21
/// minutes of bodies.
pub(crate) const STATIC_FILE_MOVE_THRESHOLD: u32 = 50;

/// The most blocks one move takes, which bounds the work added to one commit
/// while an existing database migrates its history.
///
/// While the node is at least this far behind its best header, it is syncing,
/// and moves wait for this many blocks. Each move costs a few fsyncs, so at
/// initial sync speeds 50-block moves would spend a large share of the commit
/// thread on them.
const MAX_STATIC_FILE_MOVE_BLOCKS: u32 = 1_000;

/// The most body bytes one move takes, for the same reason.
const MAX_STATIC_FILE_MOVE_BYTES: usize = 64 * 1024 * 1024;

/// The key of the static file tip in the [`PRUNING_METADATA`] column family.
///
/// Its bytes are distinct from the other keys in that column family.
#[derive(Debug)]
struct StaticFileTransactionsTipKey;

impl IntoDisk for StaticFileTransactionsTipKey {
    type Bytes = &'static [u8];

    fn as_bytes(&self) -> Self::Bytes {
        b"static_file_transactions_tip"
    }
}

impl ZakuraDb {
    /// Returns the highest block whose transactions were moved to static files
    /// and deleted from `tx_by_loc`.
    ///
    /// This is the database's record of the static files. Static file blocks
    /// above it are uncommitted moves and are truncated on open.
    pub fn static_file_transactions_tip(&self) -> Option<Height> {
        let pruning_metadata = self.db.cf_handle(PRUNING_METADATA)?;
        self.db
            .zs_get(&pruning_metadata, &StaticFileTransactionsTipKey)
    }

    /// Makes the static files match the database when a writable database opens.
    ///
    /// Like reth, the database wins when static files are ahead: those blocks are
    /// still in `tx_by_loc`, so they are truncated and moved again later. Static
    /// files behind the database lost committed bodies, which only happens when
    /// files are damaged or deleted, so the open fails.
    pub(crate) fn heal_static_files(&self) -> Result<(), StateInitError> {
        let recorded = self.static_file_transactions_tip();
        let on_disk = self.static_files.tip();
        let path = self.static_files.dir().to_owned();

        if on_disk > recorded {
            tracing::info!(
                ?recorded,
                ?on_disk,
                "truncating static file transactions the database did not commit"
            );
            self.static_files
                .truncate_above(recorded)
                .map_err(|source| StateInitError::StaticFiles {
                    path: path.clone(),
                    source: Box::new(source),
                })?;
        } else if on_disk < recorded
            // Pruning deletes whole static files, so they can all be gone when
            // the recorded tip is below the pruning marker.
            && self
                .lowest_retained_height()
                .is_none_or(|lowest_retained| recorded >= Some(lowest_retained))
        {
            return Err(StateInitError::StaticFilesBehindDatabase {
                path,
                static_file_tip: on_disk,
                database_tip: recorded,
            });
        }

        self.delete_pruned_static_files()
            .map_err(|source| StateInitError::StaticFiles {
                path,
                source: Box::new(source),
            })
    }

    /// Deletes whole static files below the pruning marker in pruned mode.
    pub(crate) fn delete_pruned_static_files(&self) -> Result<(), StaticFileError> {
        match self.lowest_retained_height() {
            Some(lowest_retained) => self.static_files.delete_below(lowest_retained),
            None => Ok(()),
        }
    }

    /// Moves a batch of finalized transaction bodies from `tx_by_loc` into
    /// static files, if at least [`STATIC_FILE_MOVE_THRESHOLD`] are waiting, or
    /// [`MAX_STATIC_FILE_MOVE_BLOCKS`] while the node is syncing.
    ///
    /// Only the block committer calls this, after a successful commit, so no
    /// other writer changes `tx_by_loc` or the static files meanwhile.
    pub(crate) fn move_transactions_to_static_files(&self) -> Result<(), StaticFileError> {
        let Some(finalized_tip) = self.finalized_tip_height() else {
            return Ok(());
        };
        let recorded = self.static_file_transactions_tip();

        // A failed earlier move can leave uncommitted static file blocks.
        if self.static_files.tip() > recorded {
            self.static_files.truncate_above(recorded)?;
        }

        // Pruned heights have no bodies to move. The genesis body is never pruned,
        // and it stays in `tx_by_loc` on pruned nodes.
        let next = recorded.map_or(0, |tip| tip.0 + 1);
        let start = self
            .lowest_retained_height()
            .map_or(next, |lowest_retained| next.max(lowest_retained.0));
        let Some(waiting) = finalized_tip.0.checked_sub(start).map(|n| n + 1) else {
            return Ok(());
        };
        if waiting < STATIC_FILE_MOVE_THRESHOLD {
            return Ok(());
        }
        let syncing = self.best_header_tip().is_some_and(|(header_tip, _)| {
            header_tip.0.saturating_sub(finalized_tip.0) >= MAX_STATIC_FILE_MOVE_BLOCKS
        });
        if syncing && waiting < MAX_STATIC_FILE_MOVE_BLOCKS {
            return Ok(());
        }
        let end = finalized_tip
            .0
            .min(start.saturating_add(MAX_STATIC_FILE_MOVE_BLOCKS - 1));

        let blocks = self.blocks_to_move(start..=end);
        let Some((last, _)) = blocks.last() else {
            tracing::warn!(
                height = start,
                "finalized block has no transaction rows to move to static files"
            );
            return Ok(());
        };
        let last = *last;

        let move_start = std::time::Instant::now();
        self.static_files.append_blocks(&blocks)?;

        let mut batch = DiskWriteBatch::new();
        batch.prepare_static_file_move_batch(self, Height(start), last);
        self.db.write(batch)?;

        metrics::histogram!("zakura.state.static_files.move.duration_seconds")
            .record(move_start.elapsed().as_secs_f64());
        metrics::counter!("zakura.state.static_files.moved.blocks").increment(blocks.len() as u64);
        metrics::gauge!("zakura.state.static_files.tip").set(f64::from(last.0));

        Ok(())
    }

    /// Reads the `tx_by_loc` rows of `heights` to move, stopping before the
    /// first height without rows or once the batch reaches its byte limit.
    fn blocks_to_move(&self, heights: RangeInclusive<u32>) -> Vec<(Height, Vec<RawBytes>)> {
        let tx_by_loc = self.db.cf_handle("tx_by_loc").unwrap();
        let mut blocks = Vec::new();
        let mut bytes = 0;

        for height in heights {
            let height = Height(height);
            let transactions: Vec<RawBytes> = self
                .db
                .zs_forward_range_iter(
                    tx_by_loc,
                    TransactionLocation::min_for_height(height)
                        ..=TransactionLocation::max_for_height(height),
                )
                .map(|(_, raw): (TransactionLocation, RawBytes)| raw)
                .collect();
            if transactions.is_empty() {
                break;
            }
            bytes += transactions
                .iter()
                .map(|raw| raw.raw_bytes().len())
                .sum::<usize>();
            blocks.push((height, transactions));
            if bytes >= MAX_STATIC_FILE_MOVE_BYTES {
                break;
            }
        }

        blocks
    }

    /// Returns `true` if `height` is below the pruning marker, so its body must
    /// not be served even if a partly pruned static file still holds it.
    fn is_pruned_height(&self, height: Height) -> bool {
        height.0 != 0
            && self
                .lowest_retained_height()
                .is_some_and(|lowest_retained| height < lowest_retained)
    }

    /// Returns the raw transactions of block `height` from static files.
    pub(crate) fn static_file_block_transactions(&self, height: Height) -> Option<Vec<RawBytes>> {
        if self.is_pruned_height(height) {
            return None;
        }
        self.static_files
            .block_transactions(height)
            .expect("static file reads check offsets against configs written by this node; an error means the files were damaged")
    }

    /// Returns the raw transaction at `location` from static files.
    pub(crate) fn static_file_transaction(
        &self,
        location: TransactionLocation,
    ) -> Option<RawBytes> {
        if self.is_pruned_height(location.height) {
            return None;
        }
        self.static_files
            .transaction(location.height, location.index.as_usize())
            .expect("static file reads check offsets against configs written by this node; an error means the files were damaged")
    }

    /// Returns `true` if the body of block `height` is in static files.
    pub(crate) fn static_file_contains(&self, height: Height) -> bool {
        !self.is_pruned_height(height)
            && self
                .static_files
                .contains(height)
                .expect("static file reads check offsets against configs written by this node; an error means the files were damaged")
    }

    /// Returns the static file block ranges within `heights`, without pruned heights.
    pub(crate) fn static_file_ranges_within(
        &self,
        heights: RangeInclusive<Height>,
    ) -> Vec<RangeInclusive<Height>> {
        let floor = self
            .lowest_retained_height()
            .filter(|_| *heights.start() != Height(0));
        let start = floor.map_or(*heights.start(), |floor| floor.max(*heights.start()));
        self.static_files.ranges_within(start..=*heights.end())
    }

    /// Removes static file blocks above `height` after an offline rollback
    /// committed the database rollback to `height`.
    pub(crate) fn truncate_static_files_above(
        &self,
        height: Height,
    ) -> Result<(), StaticFileError> {
        self.static_files.truncate_above(Some(height))
    }
}

impl DiskWriteBatch {
    /// Adds the database half of a move: deleting the moved `tx_by_loc` rows for
    /// `start..=last` and recording `last` as the static file tip.
    fn prepare_static_file_move_batch(
        &mut self,
        zakura_db: &ZakuraDb,
        start: Height,
        last: Height,
    ) {
        let tx_by_loc = zakura_db.db.cf_handle("tx_by_loc").unwrap();
        let pruning_metadata = zakura_db.db.cf_handle(PRUNING_METADATA).unwrap();

        let next = Height(last.0 + 1);
        self.zs_delete_range(
            &tx_by_loc,
            TransactionLocation::min_for_height(start),
            TransactionLocation::min_for_height(next),
        );
        self.zs_insert(&pruning_metadata, StaticFileTransactionsTipKey, last);
    }

    /// Lowers the recorded static file tip to `height` in an offline rollback batch.
    ///
    /// The rollback truncates the static files after this batch commits. If it
    /// crashes first, the next open truncates them, because the database wins.
    pub(crate) fn prepare_static_file_rollback(&mut self, zakura_db: &ZakuraDb, height: Height) {
        let Some(recorded) = zakura_db.static_file_transactions_tip() else {
            return;
        };
        if recorded <= height {
            return;
        }
        let pruning_metadata = zakura_db.db.cf_handle(PRUNING_METADATA).unwrap();
        self.zs_insert(&pruning_metadata, StaticFileTransactionsTipKey, height);
    }
}
