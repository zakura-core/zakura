//! Crash-recoverable summaries and immutable compressed timeline chunks.

use anyhow::{ensure, Context as _, Result};
use fs2::FileExt;
use profiles::{Block, Event, Frame, SCHEMA_VERSION};
use rusqlite::{params, Connection, OpenFlags, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use zakura_jsonl_trace::block_profile as profiles;

pub(crate) const MAX_INGEST_BATCH: usize = 256;
const MAX_CHUNK_EVENTS: usize = 4096;
const MAX_DECODE_BYTES: u64 = 4 * 1024 * 1024;
const MAX_DETAIL_CHUNKS: usize = 64;
const DAY_MS: u64 = 86_400_000;

#[derive(Serialize, Deserialize)]
struct Detail {
    run: String,
    data: Event,
}

pub(crate) struct Store {
    db: Connection,
    path: PathBuf,
    _lock: File,
    budget: u64,
    used: u64,
    pending: Vec<Detail>,
    discarded: u64,
}

pub(crate) fn now_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}

fn valid_id(id: &str) -> bool {
    id.len() == 32 && id.bytes().all(|c| c.is_ascii_hexdigit())
}
fn integer(value: u64) -> Result<i64> {
    Ok(i64::try_from(value)?)
}
fn name<T: Serialize>(value: &T) -> Result<String> {
    Ok(serde_json::to_value(value)?
        .as_str()
        .context("enum string")?
        .into())
}
fn attempt_id(data: &Event) -> u64 {
    match data {
        Event::Start { attempt, .. }
        | Event::Span { attempt, .. }
        | Event::Finish { attempt, .. }
        | Event::Seal { attempt, .. } => *attempt,
    }
}

impl Store {
    pub(crate) fn open(path: &Path, budget: u64) -> Result<Self> {
        ensure!(
            (16_000_000..=100_000_000_000).contains(&budget),
            "budget must be 16 MB to 100 GB"
        );
        fs::create_dir_all(path.join("chunks"))?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path.join("collector.lock"))?;
        lock.try_lock_exclusive()
            .context("another collector owns this store")?;
        let db = Connection::open(path.join("index.sqlite"))?;
        let version: u32 = db.pragma_query_value(None, "user_version", |r| r.get(0))?;
        ensure!(
            version <= SCHEMA_VERSION,
            "newer store schema cannot be opened"
        );
        db.busy_timeout(Duration::from_millis(100))?;
        db.execute_batch(include_str!("schema.sql"))?;
        let mut store = Self {
            db,
            path: path.into(),
            _lock: lock,
            budget,
            used: 0,
            pending: Vec::with_capacity(MAX_CHUNK_EVENTS),
            discarded: 0,
        };
        store.recover()?;
        crate::cpu::recover(&store.db, &store.path)?;
        store.prune()?;
        Ok(store)
    }

    fn recover(&mut self) -> Result<()> {
        // Files become visible before their catalog transaction. Uncatalogued files are orphaned.
        for entry in fs::read_dir(self.path.join("chunks"))? {
            let entry = entry?;
            ensure!(
                entry.file_type()?.is_file(),
                "unexpected entry in private chunk directory"
            );
            let file = entry.file_name().to_string_lossy().into_owned();
            let id = file.strip_suffix(".zst").unwrap_or("");
            let state = self
                .db
                .query_row("SELECT deleting FROM chunks WHERE id=?", [id], |r| {
                    r.get::<_, bool>(0)
                })
                .optional()?;
            if state != Some(false) {
                fs::remove_file(entry.path())?;
            }
        }
        let ids: Vec<String> = self
            .db
            .prepare("SELECT id FROM chunks")?
            .query_map([], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?;
        for id in ids {
            if !self.path.join("chunks").join(format!("{id}.zst")).exists() {
                self.remove_chunk(&id)?;
            }
        }
        Ok(())
    }

    /// Commit a bounded receive batch with one durable SQLite sync. Each event keeps its
    /// own savepoint, so malformed events cannot advance the sequence. A batch commit
    /// failure is fatal to the collector; reopening recovers any unpublished chunks.
    pub(crate) fn ingest_batch(&mut self, frames: Vec<Frame>) -> Result<u64> {
        ensure!(
            frames.len() <= MAX_INGEST_BATCH,
            "receive batch exceeds bound"
        );
        self.db.execute_batch("SAVEPOINT profile_ingest")?;
        let mut errors = 0u64;
        for frame in frames {
            if self.ingest(frame).is_err() {
                errors = errors.saturating_add(1);
            }
        }
        self.db.execute_batch("RELEASE profile_ingest")?;
        Ok(errors)
    }

    pub(crate) fn ingest(&mut self, frame: Frame) -> Result<()> {
        match frame {
            Frame::Run { schema, run } => {
                ensure!(
                    schema == SCHEMA_VERSION && valid_id(&run.id),
                    "unsupported run"
                );
                ensure!(
                    run.node.len() <= 128
                        && run.session.len() <= 128
                        && run.build.len() <= 256
                        && run.storage.len() <= 256
                        && run.network.len() <= 128,
                    "metadata too long"
                );
                let metadata = serde_json::to_string(&run)?;
                self.db.execute("INSERT INTO runs(id,metadata,utc_ms,seen_ms) VALUES(?,?,?,?) ON CONFLICT(id) DO UPDATE SET seen_ms=excluded.seen_ms WHERE metadata=excluded.metadata", params![run.id,metadata,integer(run.utc_start_ms)?,integer(now_ms())?])?;
            }
            Frame::Health {
                schema,
                run_id,
                at_us: _,
                attempts,
                dropped,
                transport_dropped,
            } => {
                ensure!(
                    schema == SCHEMA_VERSION && valid_id(&run_id),
                    "unsupported health"
                );
                self.db.execute(
                    "UPDATE runs SET seen_ms=?,attempts=?,dropped=?,transport_dropped=? WHERE id=?",
                    params![
                        integer(now_ms())?,
                        integer(attempts)?,
                        integer(dropped)?,
                        integer(transport_dropped)?,
                        run_id
                    ],
                )?;
            }
            Frame::Event {
                schema,
                run_id,
                sequence,
                data,
            } => {
                ensure!(
                    schema == SCHEMA_VERSION && valid_id(&run_id),
                    "unsupported event"
                );
                let sequence = integer(sequence)?;
                let previous: Option<i64> = self
                    .db
                    .query_row("SELECT sequence FROM runs WHERE id=?", [&run_id], |r| {
                        r.get(0)
                    })
                    .optional()?;
                let previous = previous.context("run metadata missing")?;
                if sequence <= previous {
                    return Ok(());
                }
                let attempt = integer(attempt_id(&data))?;
                ensure!(attempt > 0, "invalid attempt");
                // A transaction keeps sequence deduplication consistent with summary writes.
                let tx = self.db.savepoint()?;
                tx.execute(
                    "UPDATE runs SET sequence=?,gaps=gaps+?,seen_ms=? WHERE id=?",
                    params![
                        sequence,
                        sequence - previous - 1,
                        integer(now_ms())?,
                        run_id
                    ],
                )?;
                tx.execute(
                    "INSERT OR IGNORE INTO attempts(run,attempt) VALUES(?,?)",
                    params![run_id, attempt],
                )?;
                match data {
                    Event::Start {
                        block, start_us, ..
                    } => Self::summary(&tx, &run_id, attempt, block, start_us, None, None, 0)?,
                    Event::Finish {
                        block,
                        start_us,
                        end_us,
                        outcome,
                        dropped,
                        ..
                    } => {
                        ensure!(end_us >= start_us, "backwards interval");
                        Self::summary(
                            &tx,
                            &run_id,
                            attempt,
                            block,
                            start_us,
                            Some(end_us),
                            Some(name(&outcome)?),
                            dropped,
                        )?;
                    }
                    Event::Seal { spans, dropped, .. } => {
                        tx.execute("UPDATE attempts SET expected_spans=?,dropped=MAX(dropped,?) WHERE run=? AND attempt=?", params![integer(spans)?,integer(dropped)?,run_id,attempt])?;
                    }
                    Event::Span {
                        span,
                        start_us,
                        end_us,
                        parent,
                        ..
                    } => {
                        ensure!(
                            span > 0 && span <= 256 && parent < span && end_us >= start_us,
                            "invalid span"
                        );
                    }
                }
                tx.commit()?;
                if matches!(data, Event::Span { .. }) {
                    if self.used < self.budget * 9 / 10 {
                        self.pending.push(Detail { run: run_id, data });
                        if self.pending.len() >= MAX_CHUNK_EVENTS {
                            self.flush()?;
                        }
                    } else {
                        self.discarded = self.discarded.saturating_add(1);
                    }
                }
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn summary(
        db: &Connection,
        run: &str,
        attempt: i64,
        block: Block,
        start: u64,
        end: Option<u64>,
        outcome: Option<String>,
        dropped: u64,
    ) -> Result<()> {
        // Native hash bytes are reversed for the conventional block explorer representation.
        let mut hash = block.hash;
        hash.reverse();
        db.execute("UPDATE attempts SET hash=?,height=?,mode=?,transactions=?,start_us=?,end_us=COALESCE(?,end_us),outcome=COALESCE(?,outcome),dropped=MAX(dropped,?),utc_ms=(SELECT utc_ms FROM runs WHERE id=?)+?/1000 WHERE run=? AND attempt=?",
            params![hex::encode(hash),block.height,name(&block.mode)?,block.transactions,integer(start)?,end.map(integer).transpose()?,outcome,integer(dropped)?,run,integer(start)?,run,attempt])?;
        Ok(())
    }

    pub(crate) fn flush(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        // Take first so a disk failure cannot leave a queue growing without bound.
        let pending = std::mem::replace(&mut self.pending, Vec::with_capacity(MAX_CHUNK_EVENTS));
        let result = self.write_chunk(&pending);
        if result.is_err() {
            self.discarded = self.discarded.saturating_add(u64::try_from(pending.len())?);
        }
        result
    }

    fn write_chunk(&mut self, pending: &[Detail]) -> Result<()> {
        let json = serde_json::to_vec(pending)?;
        ensure!(
            u64::try_from(json.len())? <= MAX_DECODE_BYTES,
            "chunk exceeds decode bound"
        );
        let compressed = zstd::stream::encode_all(json.as_slice(), 1)?;
        let size = u64::try_from(compressed.len())?;
        ensure!(
            self.used.saturating_add(size * 2) < self.budget * 9 / 10,
            "profile storage pressure"
        );
        let id = format!("{:032x}", rand::random::<u128>());
        let temp = self.path.join("chunks").join(format!("{id}.tmp"));
        let final_path = temp.with_extension("zst");
        let size = write_payload(&temp, &final_path, &compressed, |size| {
            let tx = self.db.savepoint()?;
            tx.execute(
                "INSERT INTO chunks VALUES(?,?,?,0)",
                params![id, integer(size)?, integer(now_ms())?],
            )?;
            for detail in pending {
                let attempt = integer(attempt_id(&detail.data))?;
                tx.execute(
                    "INSERT OR IGNORE INTO details VALUES(?,?,?)",
                    params![detail.run, attempt, id],
                )?;
                tx.execute(
                    "UPDATE attempts SET received_spans=received_spans+1 WHERE run=? AND attempt=?",
                    params![detail.run, attempt],
                )?;
            }
            tx.commit()?;
            Ok(())
        })?;
        self.used = self.used.saturating_add(size);
        Ok(())
    }

    fn remove_chunk(&mut self, id: &str) -> Result<()> {
        self.db
            .execute("UPDATE chunks SET deleting=1 WHERE id=?", [id])?;
        // Readers load and close bounded chunks immediately. They report expiry on a race.
        let file = self.path.join("chunks").join(format!("{id}.zst"));
        match fs::remove_file(file) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        let tx = self.db.unchecked_transaction()?;
        tx.execute("UPDATE attempts SET expired=1 WHERE (run,attempt) IN (SELECT run,attempt FROM details WHERE chunk=?)", [id])?;
        tx.execute("DELETE FROM chunks WHERE id=?", [id])?;
        tx.commit()?;
        Ok(())
    }

    pub(crate) fn prune(&mut self) -> Result<()> {
        self.used = self.storage_bytes()?;
        if self.used < self.budget * 85 / 100 {
            let ready = fs::read_dir(self.path.join("inbox"))?.filter(|entry| {
                entry.as_ref().map_or(true, |entry| {
                    entry.path().extension().is_some_and(|e| e == "json")
                })
            });
            for entry in ready.take(8) {
                let entry = entry?;
                if crate::cpu::import(&self.db, &self.path, &entry.path()).is_err() {
                    self.discarded = self.discarded.saturating_add(1);
                    fs::remove_file(entry.path())?;
                }
            }
        }
        self.used = self.storage_bytes()?;
        let cpu_bytes: i64 =
            self.db
                .query_row("SELECT coalesce(sum(bytes),0) FROM cpu", [], |r| r.get(0))?;
        let excess = u64::try_from(cpu_bytes)?.saturating_sub(self.budget / 5);
        if excess > 0 {
            self.used = self
                .used
                .saturating_sub(crate::cpu::prune(&self.db, &self.path, excess)?);
        }

        if self.used >= self.budget * 85 / 100 {
            let mut deleted = 0u64;
            // Prefer routine detail. A chunk containing a recent outlier is protected until later.
            let chunks: Vec<(String,i64)> = self.db.prepare("SELECT c.id,c.bytes FROM chunks c ORDER BY EXISTS(SELECT 1 FROM details d JOIN attempts a USING(run,attempt) WHERE d.chunk=c.id AND a.utc_ms>? AND (a.end_us-a.start_us>500000 OR a.outcome IN ('failed','abandoned'))) ASC,c.created_ms ASC LIMIT 4096")?
                .query_map([integer(now_ms().saturating_sub(DAY_MS))?], |r| Ok((r.get(0)?,r.get(1)?)))?.collect::<rusqlite::Result<_>>()?;
            for (id, size) in chunks {
                self.remove_chunk(&id)?;
                deleted = deleted.saturating_add(u64::try_from(size)?);
                if self.used.saturating_sub(deleted) <= self.budget * 75 / 100 {
                    break;
                }
            }
            self.used = self.used.saturating_sub(deleted);
        }
        // Summary retention is independently bounded. Chunk expiry is visible until this bound.
        // Limit each maintenance transaction, keeping ingestion responsive during catch-up sync.
        let count: i64 = self
            .db
            .query_row("SELECT count(*) FROM attempts", [], |r| r.get(0))?;
        if count > 2_000_000 || self.used > self.budget * 85 / 100 {
            self.db.execute("DELETE FROM attempts WHERE rowid IN (SELECT rowid FROM attempts ORDER BY utc_ms LIMIT 10000)", [])?;
            self.db.execute_batch("PRAGMA incremental_vacuum(1024)")?;
        }
        // Remove disconnected run rows once no summaries refer to them.
        self.db.execute("DELETE FROM runs WHERE seen_ms<? AND NOT EXISTS(SELECT 1 FROM attempts WHERE run=runs.id) AND NOT EXISTS(SELECT 1 FROM cpu WHERE run=runs.id)", [integer(now_ms().saturating_sub(DAY_MS))?])?;
        self.checkpoint()?;
        Ok(())
    }

    fn storage_bytes(&self) -> Result<u64> {
        let payloads: i64 = self.db.query_row(
            "SELECT (SELECT coalesce(sum(bytes),0) FROM chunks) + (SELECT coalesce(sum(bytes),0) FROM cpu)",
            [], |r| r.get(0),
        )?;
        let mut total = u64::try_from(payloads)?;
        for entry in fs::read_dir(&self.path)? {
            let entry = entry?;
            let metadata = fs::symlink_metadata(entry.path())?;
            total = total.saturating_add(allocated_bytes(&metadata));
            if metadata.is_dir() && entry.file_name() != "chunks" && entry.file_name() != "cpu" {
                total = total.saturating_add(directory_bytes(&entry.path())?);
            }
        }
        Ok(total)
    }

    pub(crate) fn checkpoint(&self) -> Result<()> {
        self.db.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")?;
        Ok(())
    }
    pub(crate) fn health(&self, errors: u64) -> Result<()> {
        self.db.execute(
            "INSERT OR REPLACE INTO status VALUES(1,?,?,?,?,?)",
            params![
                integer(now_ms())?,
                integer(errors)?,
                integer(self.budget)?,
                integer(self.used)?,
                integer(self.discarded)?
            ],
        )?;
        Ok(())
    }
}

fn allocated_bytes(metadata: &fs::Metadata) -> u64 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        metadata.blocks().saturating_mul(512)
    }
    #[cfg(not(unix))]
    metadata.len()
}

/// Publish the payload before its catalog reference. Remove failed writes immediately so
/// byte accounting can use cataloged allocation without repeatedly scanning every payload.
pub(crate) fn write_payload(
    temp: &Path,
    target: &Path,
    bytes: &[u8],
    commit: impl FnOnce(u64) -> Result<()>,
) -> Result<u64> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(temp)?;
    let result = (|| {
        file.write_all(bytes)?;
        file.sync_all()?;
        let size = allocated_bytes(&file.metadata()?);
        fs::rename(temp, target)?;
        File::open(target.parent().context("payload directory")?)?.sync_all()?;
        commit(size)?;
        Ok(size)
    })();
    if result.is_err() {
        // A crash before cleanup is handled by startup recovery.
        for path in [temp, target] {
            match fs::remove_file(path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e).context("remove uncatalogued profile payload"),
            }
        }
    }
    result
}

fn directory_bytes(path: &Path) -> Result<u64> {
    let mut total = 0u64;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let metadata = fs::symlink_metadata(entry.path())?;
        total = total.saturating_add(allocated_bytes(&metadata));
        if metadata.is_dir() {
            total = total.saturating_add(directory_bytes(&entry.path())?);
        }
    }
    Ok(total)
}

pub(crate) struct Reader {
    db: Connection,
    path: PathBuf,
}
impl Reader {
    pub(crate) fn open(path: &Path) -> Result<Self> {
        let db = Connection::open_with_flags(
            path.join("index.sqlite"),
            OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        db.busy_timeout(Duration::from_millis(100))?;
        let started = Instant::now();
        db.progress_handler(
            1000,
            Some(move || started.elapsed() > Duration::from_secs(4)),
        )?;
        Ok(Self {
            db,
            path: path.into(),
        })
    }
    fn rows(&self, sql: &str, run: &str, mode: &str, since: u64) -> Result<Vec<Value>> {
        Ok(self
            .db
            .prepare(sql)?
            .query_map(params![run, mode, integer(since)?], row)?
            .collect::<rusqlite::Result<_>>()?)
    }
    pub(crate) fn home(&self, selected: Option<&str>, mode: &str) -> Result<Value> {
        ensure!(
            ["semantic", "checkpoint", "proposal", "preparation"].contains(&mode),
            "unknown mode"
        );
        let runs: Vec<Value> = self.db.prepare("SELECT metadata,seen_ms,attempts,dropped,transport_dropped,gaps FROM runs ORDER BY utc_ms DESC LIMIT 100")?.query_map([], |r| {
            Ok(json!({"metadata":serde_json::from_str::<Value>(&r.get::<_,String>(0)?).unwrap_or(Value::Null),"seen_ms":r.get::<_,i64>(1)?,"attempts":r.get::<_,i64>(2)?,"dropped":r.get::<_,i64>(3)?,"transport_dropped":r.get::<_,i64>(4)?,"sequence_gaps":r.get::<_,i64>(5)?}))
        })?.collect::<rusqlite::Result<_>>()?;
        let run = selected.unwrap_or_else(|| {
            runs.first()
                .and_then(|v| v["metadata"]["id"].as_str())
                .unwrap_or("")
        });
        let since = now_ms().saturating_sub(DAY_MS);
        let latest = self.rows(&format!("{COLUMNS} WHERE run=?1 AND mode=?2 AND outcome='success' AND utc_ms>=?3 AND NOT EXISTS (SELECT 1 FROM attempts b WHERE b.run=a.run AND b.hash=a.hash AND b.mode=a.mode AND b.outcome='success' AND b.attempt<a.attempt) ORDER BY utc_ms DESC LIMIT 10"),run,mode,0)?;
        let outliers = self.rows(&format!("{COLUMNS} WHERE run=?1 AND mode=?2 AND outcome='success' AND utc_ms>=?3 AND end_us-start_us>=500000 ORDER BY end_us-start_us DESC LIMIT 20"),run,mode,since)?;
        let failures = self.rows(&format!("{COLUMNS} WHERE run=?1 AND mode=?2 AND utc_ms>=?3 AND (outcome IS NULL OR outcome IN ('failed','abandoned')) ORDER BY utc_ms DESC LIMIT 20"),run,mode,since)?;
        let counts: Value = self.db.query_row("SELECT count(*),coalesce(sum(outcome='success'),0),coalesce(sum(expected_spans IS NOT NULL AND expected_spans=received_spans AND dropped=0 AND expired=0),0),min(utc_ms),max(utc_ms) FROM attempts WHERE run=? AND mode=? AND utc_ms>=?", params![run,mode,integer(since)?], |r| Ok(json!({"captured":r.get::<_,i64>(0)?,"success":r.get::<_,i64>(1)?,"sealed_detail":r.get::<_,i64>(2)?,"first_ms":r.get::<_,Option<i64>>(3)?,"last_ms":r.get::<_,Option<i64>>(4)?})))?;
        let accepted = counts["success"].as_i64().unwrap_or(0);
        let mut latency = serde_json::Map::new();
        for (label, percent) in [("p50_us", 50), ("p95_us", 95), ("p99_us", 99)] {
            let value: Option<i64> = if accepted > 0 {
                self.db.query_row("SELECT end_us-start_us FROM attempts WHERE run=? AND mode=? AND outcome='success' AND utc_ms>=? ORDER BY end_us-start_us LIMIT 1 OFFSET ?",params![run,mode,integer(since)?,(accepted-1)*percent/100],|r|r.get(0)).optional()?
            } else {
                None
            };
            latency.insert(label.into(), json!(value));
        }
        let cpu:Value = self.db.query_row("SELECT count(*),coalesce(sum(samples),0),min(start_us),max(end_us) FROM cpu WHERE run=? AND end_us>=coalesce((SELECT (? - utc_ms)*1000 FROM runs WHERE id=?),0)",params![run,integer(since)?,run],|r|Ok(json!({"captures":r.get::<_,i64>(0)?,"samples":r.get::<_,i64>(1)?,"first_us":r.get::<_,Option<i64>>(2)?,"last_us":r.get::<_,Option<i64>>(3)?,"scope":"process"})))?;
        let health = self.db.query_row("SELECT updated_ms,errors,budget,used,discarded_spans FROM status WHERE id=1", [], |r| Ok(json!({"updated_ms":r.get::<_,i64>(0)?,"errors":r.get::<_,i64>(1)?,"budget":r.get::<_,i64>(2)?,"used":r.get::<_,i64>(3)?,"discarded_spans":r.get::<_,i64>(4)?}))).optional()?;
        Ok(
            json!({"generated_ms":now_ms(),"run":run,"mode":mode,"runs":runs,"latest":latest,"outliers":outliers,"failures":failures,"counts":counts,"health":health,"cpu":cpu,"latency":latency}),
        )
    }
    pub(crate) fn detail(&self, run: &str, attempt: u64) -> Result<Value> {
        ensure!(valid_id(run), "invalid run");
        let summary = self.db.query_row(
            &format!("{COLUMNS} WHERE run=? AND attempt=?"),
            params![run, integer(attempt)?],
            row,
        )?;
        let files: Vec<String> = self.db.prepare("SELECT chunk FROM details JOIN chunks ON chunk=id WHERE run=? AND attempt=? AND deleting=0 LIMIT 65")?.query_map(params![run,integer(attempt)?], |r| r.get(0))?.collect::<rusqlite::Result<_>>()?;
        ensure!(
            files.len() <= MAX_DETAIL_CHUNKS,
            "detail exceeds query budget"
        );
        let mut spans = Vec::new();
        let mut ids = BTreeSet::new();
        let mut missing = false;
        for id in files {
            ensure!(valid_id(&id), "invalid chunk identifier");
            let file = match File::open(self.path.join("chunks").join(format!("{id}.zst"))) {
                Ok(f) => f,
                Err(_) => {
                    missing = true;
                    continue;
                }
            };
            ensure!(
                file.metadata()?.len() <= MAX_DECODE_BYTES,
                "oversized chunk"
            );
            let mut decoded = Vec::new();
            zstd::stream::read::Decoder::new(file)?
                .take(MAX_DECODE_BYTES + 1)
                .read_to_end(&mut decoded)?;
            ensure!(
                u64::try_from(decoded.len())? <= MAX_DECODE_BYTES,
                "decompression limit"
            );
            let details: Vec<Detail> = serde_json::from_slice(&decoded)?;
            for detail in details {
                if detail.run == run && attempt_id(&detail.data) == attempt {
                    if let Event::Span { span, .. } = detail.data {
                        if ids.insert(span) {
                            spans.push(detail.data);
                        }
                    }
                }
            }
        }
        let cpu = crate::cpu::window(
            &self.db,
            &self.path,
            run,
            summary["start_us"].as_u64().unwrap_or(0),
            summary["end_us"].as_u64().unwrap_or(0),
        )
        .unwrap_or_else(|error| json!({"status":"unavailable","reason":error.to_string()}));
        let complete = summary["end_us"].is_number()
            && !missing
            && summary["expired"] == false
            && summary["dropped"] == 0
            && summary["expected_spans"].as_u64() == Some(u64::try_from(spans.len())?);
        Ok(
            json!({"summary":summary,"spans":spans,"complete":complete,"missing_chunks":missing,"cpu":cpu,"boundary":"router entry to caller result; caller readiness, network and ingress are outside this interval"}),
        )
    }
    pub(crate) fn search(&self, query: &str) -> Result<Value> {
        ensure!(query.len() <= 64, "query too long");
        let height = query.parse::<u32>().ok();
        let hash = if query.len() == 64 && query.bytes().all(|b| b.is_ascii_hexdigit()) {
            query.to_ascii_lowercase()
        } else {
            String::new()
        };
        let rows: Vec<Value> = self
            .db
            .prepare(&format!(
                "{COLUMNS} WHERE hash=? OR height=? ORDER BY utc_ms DESC LIMIT 50"
            ))?
            .query_map(params![hash, height], row)?
            .collect::<rusqlite::Result<_>>()?;
        Ok(json!(rows))
    }
}

const COLUMNS: &str = "SELECT run,attempt,hash,height,mode,transactions,start_us,end_us,utc_ms,outcome,dropped,expected_spans,received_spans,expired FROM attempts a";
fn row(r: &rusqlite::Row<'_>) -> rusqlite::Result<Value> {
    Ok(
        json!({"run":r.get::<_,String>(0)?,"attempt":r.get::<_,i64>(1)?,"hash":r.get::<_,Option<String>>(2)?,"height":r.get::<_,Option<u32>>(3)?,"mode":r.get::<_,Option<String>>(4)?,"transactions":r.get::<_,Option<u32>>(5)?,"start_us":r.get::<_,Option<i64>>(6)?,"end_us":r.get::<_,Option<i64>>(7)?,"utc_ms":r.get::<_,Option<i64>>(8)?,"outcome":r.get::<_,Option<String>>(9)?,"dropped":r.get::<_,i64>(10)?,"expected_spans":r.get::<_,Option<i64>>(11)?,"received_spans":r.get::<_,i64>(12)?,"expired":r.get::<_,bool>(13)?}),
    )
}

#[cfg(test)]
mod tests;
