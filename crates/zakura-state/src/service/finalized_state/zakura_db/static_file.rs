//! Moving finalized transaction bodies from `tx_by_loc` into static files.
//!
//! Reth writes bodies straight into static files when it persists a batch of
//! about 50 blocks, and syncs them once per batch. Zakura commits one finalized
//! block per database write, and block times keep falling, so syncing static
//! files on every commit would cost several fsyncs per block. Instead, each block
//! still lands in `tx_by_loc`, and a [`StaticFileMover`] thread moves batches
//! of finalized blocks into static files, like reth's static file producer did
//! before reth batched its persistence.
//!
//! A move appends and syncs the static files first. Then one database batch
//! deletes the moved `tx_by_loc` rows and advances
//! [`ZakuraDb::static_file_transactions_tip`]. So the database never points past
//! durable static files, and bodies are always in at least one of the two
//! stores. Near the tip the moved rows usually sit in a memtable, and RocksDB
//! drops rows covered by a range delete when it flushes. Moves of older rows,
//! such as an upgraded database's history, also delete the RocksDB files that
//! held them, so an upgrade never keeps two copies of its bodies on disk.

use std::{
    collections::VecDeque,
    ops::RangeInclusive,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{sync_channel, Receiver, RecvTimeoutError, SyncSender, TrySendError},
        Arc, Mutex,
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

use zakura_chain::block::Height;

use crate::{
    service::finalized_state::{
        disk_db::{DiskWriteBatch, ReadDisk, WriteDisk},
        disk_format::block::TransactionLocation,
        static_file::StaticFileError,
        zakura_db::ZakuraDb,
        FromDisk, IntoDisk, RawBytes, PRUNING_METADATA,
    },
    StateInitError,
};

/// Near the chain tip, moves start once this many finalized blocks are waiting,
/// reth's default persistence threshold. At 25 second blocks that is about 21
/// minutes of bodies.
pub(crate) const STATIC_FILE_MOVE_THRESHOLD: u32 = 50;

/// The most blocks one move takes, which bounds how long the mover holds its
/// batch in memory and how long a stopping mover takes to exit.
///
/// A move takes every waiting block up to this limit, so when commits outpace
/// the mover, its batches grow and each fsync covers more blocks.
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
    /// Static files missing a retained block the database moved lost committed
    /// bodies, which only happens when files are damaged or deleted, so the open
    /// fails before changing any file. Otherwise, like reth, the database wins
    /// when static files are ahead: those blocks are still in `tx_by_loc`, so
    /// they are truncated and moved again later. Then crash leftovers are
    /// repaired and pruned files deleted.
    pub(crate) fn heal_static_files(&self) -> Result<(), StateInitError> {
        let recorded = self.static_file_transactions_tip();
        let on_disk = self.static_files.tip();
        let static_file_error = |source| StateInitError::StaticFiles {
            path: self.static_files.dir().to_owned(),
            source: Box::new(source),
        };

        // Every moved height from the oldest retained one up to the recorded tip
        // must be in some static file. Moves start at height 1, and pruning
        // deletes whole files below its marker, so only those gaps are allowed.
        if let Some(recorded) = recorded {
            let first = self
                .lowest_retained_height()
                .map_or(Height(1), |lowest_retained| lowest_retained.max(Height(1)));
            let mut next = first;
            for range in self.static_files.ranges_within(first..=recorded) {
                if *range.start() != next {
                    break;
                }
                next = Height(range.end().0 + 1);
            }
            if next <= recorded {
                return Err(StateInitError::StaticFilesMissingBlocks {
                    path: self.static_files.dir().to_owned(),
                    first_missing: next,
                    database_tip: recorded,
                });
            }
        }

        if on_disk > recorded {
            tracing::info!(
                ?recorded,
                ?on_disk,
                "truncating static file transactions the database did not commit"
            );
            self.static_files
                .truncate_above(recorded)
                .map_err(static_file_error)?;
        }
        self.static_files.repair().map_err(static_file_error)?;

        self.delete_pruned_static_files().map_err(static_file_error)
    }

    /// Deletes whole static files below the pruning marker in pruned mode.
    ///
    /// Does nothing once a static file write failed, see
    /// [`StaticFileError::Failed`], so pruning doesn't log on every commit.
    pub(crate) fn delete_pruned_static_files(&self) -> Result<(), StaticFileError> {
        let Some(lowest_retained) = self.lowest_retained_height() else {
            return Ok(());
        };
        if self.static_files.has_failed() {
            return Ok(());
        }
        if !self.static_files.has_files_below(lowest_retained) {
            return Ok(());
        }
        // A marker lost to a power cut would claim the deleted blocks are retained.
        self.db.sync_wal()?;
        self.static_files.delete_below(lowest_retained)
    }

    /// Moves a batch of up to [`MAX_STATIC_FILE_MOVE_BLOCKS`] finalized
    /// transaction bodies from `tx_by_loc` into static files, if at least
    /// [`STATIC_FILE_MOVE_THRESHOLD`] are waiting. Returns `true` if it moved any.
    ///
    /// Only the [`StaticFileMover`] thread calls this, so moves never overlap.
    /// The committer only adds blocks above the finalized tip, and pruning
    /// only removes rows below the pruning marker, which this skips.
    fn move_transactions_to_static_files(&self) -> Result<bool, StaticFileError> {
        let Some(finalized_tip) = self.finalized_tip_height() else {
            return Ok(false);
        };
        // The store already logged the failure. Bodies stay readable in
        // `tx_by_loc` until a restart reconciles the files.
        if self.static_files.has_failed() {
            metrics::gauge!("zakura.state.static_files.failed").set(1.0);
            return Ok(false);
        }
        let recorded = self.static_file_transactions_tip();

        // Pruned heights have no bodies to move. The genesis body never moves, so
        // deleting pruned static files can't take it with them.
        let next = recorded.map_or(1, |tip| tip.0 + 1);
        let start = self
            .lowest_retained_height()
            .map_or(next, |lowest_retained| next.max(lowest_retained.0));

        // A failed earlier move can leave static file blocks the database didn't
        // record. They copy rows still in `tx_by_loc`, so record them rather than
        // truncate files that concurrent readers may be using.
        if let Some(appended) = self.static_files.tip().filter(|tip| Some(*tip) > recorded) {
            self.commit_static_file_move(Height(start.min(appended.0)), appended)?;
            return Ok(true);
        }
        let Some(waiting) = finalized_tip.0.checked_sub(start).map(|n| n + 1) else {
            return Ok(false);
        };
        if waiting < STATIC_FILE_MOVE_THRESHOLD {
            return Ok(false);
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
            return Ok(false);
        };
        let last = *last;

        let move_start = std::time::Instant::now();
        self.static_files.append_blocks(&blocks)?;
        self.commit_static_file_move(Height(start), last)?;

        metrics::histogram!("zakura.state.static_files.move.duration_seconds")
            .record(move_start.elapsed().as_secs_f64());
        metrics::counter!("zakura.state.static_files.moved.blocks").increment(blocks.len() as u64);

        Ok(true)
    }

    /// Records static file blocks up to `last` in the database and deletes their
    /// `tx_by_loc` rows from `start`. The mover frees their SST files later,
    /// see [`MOVED_FILE_DELETE_DELAY`].
    fn commit_static_file_move(&self, start: Height, last: Height) -> Result<(), StaticFileError> {
        let mut batch = DiskWriteBatch::new();
        batch.prepare_static_file_move_batch(self, start, last);
        self.db.write(batch)?;
        // Deleting SST files later removes the only RocksDB copy of these rows,
        // so the move must be durable first. Otherwise a power cut could lose
        // the move record, and the next open would truncate the static file
        // copy too.
        self.db.sync_wal()?;

        metrics::gauge!("zakura.state.static_files.tip").set(f64::from(last.0));
        Ok(())
    }

    /// Deletes the `tx_by_loc` SST files that only hold rows at or below `last`.
    ///
    /// Every height from 1 to `last` is in static files or below the pruning
    /// marker, so every row in those files is deleted. The range starts at
    /// height 1, because a file can straddle an earlier move's boundary, and
    /// stops short of genesis, which never moves.
    fn delete_moved_transaction_files(&self, last: Height) -> Result<(), rocksdb::Error> {
        let tx_by_loc = self.db.cf_handle("tx_by_loc").unwrap();
        self.db.delete_files_in_range_cf(
            &tx_by_loc,
            TransactionLocation::min_for_height(Height(1))
                .as_bytes()
                .as_ref(),
            TransactionLocation::max_for_height(last)
                .as_bytes()
                .as_ref(),
        )
    }

    /// Reads the `tx_by_loc` rows of `heights` to move, stopping before the
    /// first height without rows or once the batch reaches its byte limit.
    ///
    /// Moves mostly read cold history, so this reads the whole range with one
    /// sequential scan, see `DiskDb::raw_sequential_scan_cf`.
    fn blocks_to_move(&self, heights: RangeInclusive<u32>) -> Vec<(Height, Vec<RawBytes>)> {
        let tx_by_loc = self.db.cf_handle("tx_by_loc").unwrap();
        let lower = TransactionLocation::min_for_height(Height(*heights.start())).as_bytes();
        let upper = TransactionLocation::min_for_height(Height(heights.end() + 1)).as_bytes();
        let mut blocks: Vec<(Height, Vec<RawBytes>)> = Vec::new();
        let mut bytes = 0;

        for (key, value) in self.db.raw_sequential_scan_cf(&tx_by_loc, &lower, &upper) {
            let height = TransactionLocation::from_bytes(key).height;
            let raw = RawBytes::from_bytes(value);
            let raw_len = raw.raw_bytes().len();

            match blocks.last_mut() {
                Some((last, transactions)) if *last == height => transactions.push(raw),
                last => {
                    let next = last.map_or(*heights.start(), |(last, _)| last.0 + 1);
                    if height.0 != next || bytes >= MAX_STATIC_FILE_MOVE_BYTES {
                        break;
                    }
                    blocks.push((height, vec![raw]));
                }
            }
            bytes += raw_len;
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

    /// Returns the part of `heights` a lookup may read from static files.
    ///
    /// The primary reads every static file block. A secondary only reads blocks
    /// its RocksDB view recorded as moved: blocks above that are still in its
    /// `tx_by_loc`, and the primary may truncate them when it reopens. When its
    /// static file list doesn't reach those blocks yet, because the primary moved
    /// them after this instance last looked, it refreshes the list first.
    fn readable_static_file_heights(
        &self,
        heights: RangeInclusive<Height>,
    ) -> Option<RangeInclusive<Height>> {
        if !self.db.is_read_only_secondary() {
            return Some(heights);
        }
        let recorded = self.static_file_transactions_tip()?;
        let end = (*heights.end()).min(recorded);
        if *heights.start() > end {
            return None;
        }
        if self
            .static_files
            .refreshed_to()
            .is_none_or(|refreshed| refreshed < end)
        {
            if let Err(error) = self
                .static_files
                .refresh_to(recorded, self.secondary_keep_from())
            {
                // A secondary reports a body it can't read as missing, not wrong.
                tracing::warn!(?error, "failed to refresh secondary static files");
                return None;
            }
        }
        Some(*heights.start()..=end)
    }

    /// Returns the lowest height a secondary's own view retains, so a refresh
    /// keeps jars the primary pruned past it, see `StaticFiles::rescan`.
    fn secondary_keep_from(&self) -> Height {
        self.lowest_retained_height().unwrap_or(Height(0))
    }

    /// Runs a static file `read`. The primary panics on an error, because only
    /// damaged files cause one. A secondary can also race the primary's prunes,
    /// truncations and crash recovery, so it rescans once and retries, then
    /// reports the body as missing, never wrong.
    fn read_static_file<T>(
        &self,
        read: impl Fn() -> Result<Option<T>, StaticFileError>,
    ) -> Option<T> {
        match read() {
            Ok(found) => found,
            Err(error) if self.db.is_read_only_secondary() => {
                tracing::debug!(
                    ?error,
                    "rescanning secondary static files after a failed read"
                );
                let retried = self
                    .static_files
                    .rescan(self.secondary_keep_from())
                    .and_then(|()| read());
                retried.unwrap_or_else(|error| {
                    tracing::warn!(?error, "a secondary static file read failed after a rescan");
                    None
                })
            }
            Err(error) => panic!(
                "static file reads check offsets against configs written by this node; an \
                 error means the files were damaged: {error}"
            ),
        }
    }

    /// Returns `true` if `height`'s body was moved to static files, by the
    /// recorded static file tip in this instance's database view, even if
    /// pruning later deleted it. `tx_by_loc` rows at these heights are never
    /// read: deleting moved rows' SST files can bring an older row back, even
    /// one a rollback replaced. Genesis never moves.
    pub(crate) fn is_moved_height(&self, height: Height) -> bool {
        if height == Height(0) {
            return false;
        }
        // On the primary, the recorded tip only passes the static file tip when
        // pruning deleted every jar, so heights above that tip skip the read.
        if !self.db.is_read_only_secondary()
            && self.static_files.tip().is_some_and(|tip| height > tip)
        {
            return false;
        }
        self.static_file_transactions_tip()
            .is_some_and(|recorded| height <= recorded)
    }

    /// Returns the raw transactions of block `height` from static files.
    pub(crate) fn static_file_block_transactions(&self, height: Height) -> Option<Vec<RawBytes>> {
        if self.is_pruned_height(height) {
            return None;
        }
        self.readable_static_file_heights(height..=height)?;
        self.read_static_file(|| self.static_files.block_transactions(height))
    }

    /// Returns the raw transaction at `location` from static files.
    pub(crate) fn static_file_transaction(
        &self,
        location: TransactionLocation,
    ) -> Option<RawBytes> {
        if self.is_pruned_height(location.height) {
            return None;
        }
        self.readable_static_file_heights(location.height..=location.height)?;
        self.read_static_file(|| {
            self.static_files
                .transaction(location.height, location.index.as_usize())
        })
    }

    /// Returns `true` if the body of block `height` is in static files.
    pub(crate) fn static_file_contains(&self, height: Height) -> bool {
        // On a secondary, the recorded tip check comes first, so reads of
        // recent blocks still in `tx_by_loc` stop there.
        self.readable_static_file_heights(height..=height).is_some()
            && !self.is_pruned_height(height)
            && self.static_files.contains(height)
    }

    /// Returns the static file block ranges within `heights`, without pruned heights.
    ///
    /// Callers take their RocksDB snapshot first. On a secondary, that snapshot
    /// can miss rows the primary moved after this instance last listed its
    /// static files. Those moves are recorded at or below the recorded tip read
    /// here, so [`Self::readable_static_file_heights`] lists every missing block.
    pub(crate) fn static_file_ranges_within(
        &self,
        heights: RangeInclusive<Height>,
    ) -> Vec<RangeInclusive<Height>> {
        let floor = self
            .lowest_retained_height()
            .filter(|_| *heights.start() != Height(0));
        let start = floor.map_or(*heights.start(), |floor| floor.max(*heights.start()));
        match self.readable_static_file_heights(start..=*heights.end()) {
            Some(heights) => self.static_files.ranges_within(heights),
            None => Vec::new(),
        }
    }

    /// Removes static file blocks above `height` after an offline rollback
    /// committed the database rollback to `height`.
    pub(crate) fn truncate_static_files_above(
        &self,
        height: Height,
    ) -> Result<(), StaticFileError> {
        // A rollback lost to a power cut would leave the database recording
        // static file blocks that this removed.
        self.db.sync_wal()?;
        self.static_files.truncate_above(Some(height))
    }
}

/// How long the mover waits after a move before deleting the RocksDB files that
/// only held its rows. RPC secondaries catch up with the primary about once per
/// block, and until then their older view can still read those files. A full
/// archive upgrade moves about 7 GB a minute, so a longer delay holds that much
/// more disk at the upgrade's peak.
const MOVED_FILE_DELETE_DELAY: Duration = Duration::from_secs(60);

/// Moves waiting a delay, [`MOVED_FILE_DELETE_DELAY`] in production, before
/// their RocksDB files are deleted.
#[derive(Debug)]
struct DelayedFileDeletes {
    /// How long each move waits.
    delay: Duration,
    /// Static file tips recorded by each move, with when it was recorded, oldest first.
    moves: VecDeque<(Height, Instant)>,
}

impl DelayedFileDeletes {
    /// Returns an empty queue whose moves wait `delay`.
    fn new(delay: Duration) -> Self {
        Self {
            delay,
            moves: VecDeque::new(),
        }
    }

    /// Records that a move recorded static file tip `tip` at `now`.
    fn push(&mut self, tip: Height, now: Instant) {
        self.moves.push_back((tip, now));
    }

    /// Returns when the oldest waiting move's files come due, if any are waiting.
    fn next_due(&self) -> Option<Instant> {
        self.moves
            .front()
            .map(|(_, recorded_at)| *recorded_at + self.delay)
    }

    /// Removes the moves recorded at least the delay before `now`, and returns
    /// the highest tip among them.
    fn take_due(&mut self, now: Instant) -> Option<Height> {
        let mut due = None;
        while let Some(&(tip, recorded_at)) = self.moves.front() {
            if now.saturating_duration_since(recorded_at) < self.delay {
                break;
            }
            due = Some(tip);
            self.moves.pop_front();
        }
        due
    }
}

/// Runs static file moves on a dedicated thread, off the block commit path.
///
/// A move reads, appends and syncs up to [`MAX_STATIC_FILE_MOVE_BYTES`] of
/// bodies, which would otherwise delay the next block commit. Reth runs its
/// static file writes on its persistence thread for the same reason. Readers
/// already tolerate a concurrent move, see [`ZakuraDb::contains_body_at_height`].
///
/// The thread starts on the first [`Self::notify`], so read-only states never
/// start one. Dropping the mover stops the thread after its current move. An
/// interrupted move is safe, because the database wins on open.
#[derive(Debug)]
pub(crate) struct StaticFileMover {
    worker: Mutex<Option<MoverWorker>>,
    /// How long moved rows' RocksDB files wait before they're deleted.
    file_delete_delay: Duration,
}

impl Default for StaticFileMover {
    fn default() -> Self {
        Self::with_file_delete_delay(MOVED_FILE_DELETE_DELAY)
    }
}

/// The running mover thread and the handles that control it.
#[derive(Debug)]
struct MoverWorker {
    requests: SyncSender<MoveRequest>,
    stopping: Arc<AtomicBool>,
    handle: JoinHandle<()>,
}

/// A request to move every waiting batch, then reply on `done` if it is set.
#[derive(Debug)]
struct MoveRequest {
    done: Option<SyncSender<()>>,
}

impl StaticFileMover {
    /// Returns a mover that deletes moved rows' RocksDB files `delay` after
    /// each move. Production uses [`MOVED_FILE_DELETE_DELAY`].
    pub(crate) fn with_file_delete_delay(delay: Duration) -> Self {
        Self {
            worker: Mutex::new(None),
            file_delete_delay: delay,
        }
    }

    /// Asks the mover to move every waiting batch of `db`, after a block commit.
    ///
    /// This never waits for a move. Requests coalesce, because a queued request
    /// already sees every block committed before it runs.
    ///
    /// # Panics
    ///
    /// If the mover thread panicked, which only happens on damaged static files.
    pub(crate) fn notify(&self, db: &ZakuraDb) {
        let mut worker = self
            .worker
            .lock()
            .expect("the mover lock is only held to send requests, which can't panic");
        let running =
            worker.get_or_insert_with(|| MoverWorker::spawn(db.clone(), self.file_delete_delay));

        match running.requests.try_send(MoveRequest { done: None }) {
            Ok(()) | Err(TrySendError::Full(_)) => {}
            Err(TrySendError::Disconnected(_)) => {
                let running = worker.take().expect("the worker was just used");
                if let Err(panic) = running.handle.join() {
                    std::panic::resume_unwind(panic);
                }
                unreachable!("the mover thread only exits early by panicking");
            }
        }
    }

    /// Blocks until the mover has moved every batch waiting when this was called.
    #[cfg(test)]
    pub(crate) fn wait(&self) {
        let requests = match self.worker.lock().expect("mover lock").as_ref() {
            Some(worker) => worker.requests.clone(),
            None => return,
        };
        let (done, finished) = sync_channel(1);
        requests
            .send(MoveRequest { done: Some(done) })
            .expect("the mover thread is running");
        finished.recv().expect("the mover thread replies");
    }
}

impl Drop for StaticFileMover {
    fn drop(&mut self) {
        let Some(worker) = self.worker.get_mut().ok().and_then(Option::take) else {
            return;
        };
        worker.stopping.store(true, Ordering::Relaxed);
        drop(worker.requests);
        if worker.handle.join().is_err() {
            tracing::error!("the static file mover thread panicked");
        }
    }
}

impl MoverWorker {
    /// Starts the mover thread for `db`, deleting moved rows' RocksDB files
    /// `file_delete_delay` after each move.
    fn spawn(db: ZakuraDb, file_delete_delay: Duration) -> Self {
        // One queued request is enough, see `StaticFileMover::notify`.
        let (requests, receiver) = sync_channel(1);
        let stopping = Arc::new(AtomicBool::new(false));
        let thread_stopping = stopping.clone();
        let handle = std::thread::Builder::new()
            .name("static-file-mover".to_owned())
            .spawn(move || Self::run(&db, &receiver, &thread_stopping, file_delete_delay))
            .expect("the OS can start one more thread at state startup");

        Self {
            requests,
            stopping,
            handle,
        }
    }

    /// Handles requests until every sender is dropped. Each request moves every
    /// waiting batch and deletes pruned static files. Between batches, and on a
    /// timed wakeup when idle, it frees the RocksDB files of moves that are old
    /// enough, so a long migration frees space as it goes.
    fn run(
        db: &ZakuraDb,
        requests: &Receiver<MoveRequest>,
        stopping: &AtomicBool,
        file_delete_delay: Duration,
    ) {
        let mut file_deletes = DelayedFileDeletes::new(file_delete_delay);
        // Moves recorded before a restart can still have RocksDB files to free.
        if let Some(tip) = db.static_file_transactions_tip() {
            file_deletes.push(tip, Instant::now());
        }

        loop {
            let request = match file_deletes.next_due() {
                Some(due) => {
                    match requests.recv_timeout(due.saturating_duration_since(Instant::now())) {
                        Ok(request) => Some(request),
                        Err(RecvTimeoutError::Timeout) => None,
                        Err(RecvTimeoutError::Disconnected) => return,
                    }
                }
                None => match requests.recv() {
                    Ok(request) => Some(request),
                    Err(_) => return,
                },
            };

            if let Some(request) = request {
                loop {
                    if stopping.load(Ordering::Relaxed) {
                        return;
                    }
                    match db.move_transactions_to_static_files() {
                        Ok(true) => {
                            if let Some(tip) = db.static_file_transactions_tip() {
                                file_deletes.push(tip, Instant::now());
                            }
                            Self::free_due_files(db, &mut file_deletes);
                        }
                        Ok(false) => break,
                        // The bodies are still in `tx_by_loc`, so the next commit retries.
                        Err(error) => {
                            tracing::error!(
                                ?error,
                                "failed to move finalized transactions to static files, will retry"
                            );
                            break;
                        }
                    }
                }

                if let Err(error) = db.delete_pruned_static_files() {
                    tracing::error!(?error, "failed to delete pruned static files, will retry");
                }
                if let Some(done) = request.done {
                    // The waiter can't stop waiting, so the reply can't fail.
                    let _ = done.send(());
                }
            }
            Self::free_due_files(db, &mut file_deletes);
        }
    }

    /// Frees the RocksDB files of moves at least [`MOVED_FILE_DELETE_DELAY`] old.
    fn free_due_files(db: &ZakuraDb, file_deletes: &mut DelayedFileDeletes) {
        // The moves are committed, so a failure here only delays freeing space.
        if let Some(tip) = file_deletes.take_due(Instant::now()) {
            if let Err(error) = db.delete_moved_transaction_files(tip) {
                tracing::warn!(
                    ?error,
                    "failed to free the RocksDB files of moved transactions"
                );
            }
        }
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
        // Lowering the tip lets these heights read `tx_by_loc` again, which can
        // hold rows a deleted SST file brought back, at indexes the rolled back
        // block didn't have. Deleting the whole range keeps them out.
        let tx_by_loc = zakura_db.db.cf_handle("tx_by_loc").unwrap();
        self.zs_delete_range(
            &tx_by_loc,
            TransactionLocation::min_for_height(Height(height.0 + 1)),
            TransactionLocation::min_for_height(Height(recorded.0 + 1)),
        );
        let pruning_metadata = zakura_db.db.cf_handle(PRUNING_METADATA).unwrap();
        // Genesis never moves, so a rollback to it leaves no static file blocks.
        if height == Height(0) {
            self.zs_delete(&pruning_metadata, StaticFileTransactionsTipKey);
        } else {
            self.zs_insert(&pruning_metadata, StaticFileTransactionsTipKey, height);
        }
    }
}
