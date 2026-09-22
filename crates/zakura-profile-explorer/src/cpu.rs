//! Timestamped process samples. No inferred per-block CPU ownership.
use anyhow::{ensure, Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::Read,
    path::Path,
};
use zakura_jsonl_trace::block_profile as profiles;

const MAX_BYTES: u64 = 16 * 1024 * 1024;
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Capture {
    pub run: String,
    pub pid: u32,
    pub frequency: u32,
    pub clock: String,
    pub start_mono_us: u64,
    pub end_mono_us: u64,
    pub process_start_ticks: u64,
    pub executable_sha256: String,
    pub build_ids: Vec<String>,
    pub decode_errors: u64,
    pub truncated: bool,
    pub samples: Vec<Sample>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Sample {
    pub mono_us: u64,
    pub tid: u32,
    pub frames: Vec<String>,
}

pub(crate) fn import(db: &Connection, path: &Path, source: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(source)?;
    ensure!(
        metadata.is_file() && metadata.len() <= MAX_BYTES,
        "capture input limit"
    );
    let capture: Capture = serde_json::from_slice(&fs::read(source)?)?;
    ensure!(
        capture.run.len() == 32 && capture.run.bytes().all(|b| b.is_ascii_hexdigit()),
        "run identity"
    );
    ensure!(
        [19, 49].contains(&capture.frequency) && capture.clock == "monotonic",
        "capture settings"
    );
    ensure!(
        capture.end_mono_us >= capture.start_mono_us
            && capture.end_mono_us - capture.start_mono_us <= 120_000_000,
        "capture duration"
    );
    ensure!(
        capture.samples.len() <= 50_000
            && capture.build_ids.len() <= 512
            && capture.build_ids.iter().all(|id| id.len() <= 512),
        "capture record bound"
    );
    ensure!(
        capture.executable_sha256.len() == 64
            && capture
                .executable_sha256
                .bytes()
                .all(|b| b.is_ascii_hexdigit()),
        "executable identity"
    );
    let run: String = db.query_row(
        "SELECT metadata FROM runs WHERE id=?",
        [&capture.run],
        |r| r.get(0),
    )?;
    let run: profiles::Run = serde_json::from_str(&run)?;
    let anchor = run
        .monotonic_start_us
        .context("run has no Linux monotonic anchor")?;
    ensure!(
        run.pid == capture.pid && capture.start_mono_us >= anchor,
        "capture belongs to a different process"
    );
    for sample in &capture.samples {
        ensure!(
            sample.mono_us >= capture.start_mono_us && sample.mono_us <= capture.end_mono_us,
            "sample outside capture"
        );
        ensure!(
            sample.frames.len() <= 128 && sample.frames.iter().all(|f| f.len() <= 1024),
            "stack bound"
        );
    }
    let id = source
        .file_stem()
        .and_then(|s| s.to_str())
        .context("capture name")?;
    ensure!(
        id.len() == 32 && id.bytes().all(|b| b.is_ascii_hexdigit()),
        "capture name"
    );
    if db
        .query_row("SELECT 1 FROM cpu WHERE id=?", [id], |r| r.get::<_, i64>(0))
        .optional()?
        .is_some()
    {
        fs::remove_file(source)?;
        return Ok(());
    }
    let compressed = zstd::stream::encode_all(serde_json::to_vec(&capture)?.as_slice(), 1)?;
    let target = path.join("cpu").join(format!("{id}.zst"));
    let temp = target.with_extension("tmp");
    let summary = json!({"frequency":capture.frequency,"scope":"process","decode_errors":capture.decode_errors,"truncated":capture.truncated,"executable_sha256":capture.executable_sha256,"build_ids":capture.build_ids,"process_start_ticks":capture.process_start_ticks});
    crate::store::write_payload(&temp, &target, &compressed, |size| {
        db.execute(
            "INSERT INTO cpu VALUES(?,?,?,?,?,?,?,0)",
            params![
                id,
                capture.run,
                i64::try_from(capture.start_mono_us - anchor)?,
                i64::try_from(capture.end_mono_us - anchor)?,
                i64::try_from(capture.samples.len())?,
                i64::try_from(size)?,
                summary.to_string()
            ],
        )?;
        Ok(())
    })?;
    fs::remove_file(source)?;
    Ok(())
}

pub(crate) fn window(
    db: &Connection,
    path: &Path,
    run: &str,
    start: u64,
    end: u64,
) -> Result<Value> {
    let metadata: String =
        db.query_row("SELECT metadata FROM runs WHERE id=?", [run], |r| r.get(0))?;
    let run_metadata: profiles::Run = serde_json::from_str(&metadata)?;
    let Some(anchor) = run_metadata.monotonic_start_us else {
        return Ok(json!({"status":"unavailable","reason":"No Linux clock anchor"}));
    };
    let files:Vec<(String,String,i64,i64)>=db.prepare("SELECT id,metadata,start_us,end_us FROM cpu WHERE run=? AND end_us>=? AND start_us<=? AND deleting=0 ORDER BY start_us LIMIT 9")?
        .query_map(params![run,i64::try_from(start)?,i64::try_from(end)?],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)))?.collect::<rusqlite::Result<_>>()?;
    if files.is_empty() {
        return Ok(
            json!({"status":"unavailable","reason":"No retained samples covering this interval"}),
        );
    }
    ensure!(
        files.len() <= 8,
        "CPU interval too large for interactive decoding"
    );
    let mut stacks = BTreeMap::<Vec<String>, u64>::new();
    let mut samples = 0u64;
    let mut unknown = 0u64;
    let mut captures = Vec::new();
    let mut covered = Vec::new();
    let min = anchor
        .saturating_add(start)
        .saturating_add(run_metadata.clock_error_us);
    let max = anchor
        .saturating_add(end)
        .saturating_sub(run_metadata.clock_error_us);
    for (id, meta, from, to) in files {
        ensure!(
            id.len() == 32 && id.bytes().all(|b| b.is_ascii_hexdigit()),
            "invalid capture path"
        );
        let file = File::open(path.join("cpu").join(format!("{id}.zst")))
            .context("CPU profile expired")?;
        ensure!(file.metadata()?.len() <= MAX_BYTES, "CPU file too large");
        let mut bytes = Vec::new();
        zstd::stream::read::Decoder::new(file)?
            .take(MAX_BYTES + 1)
            .read_to_end(&mut bytes)?;
        ensure!(
            u64::try_from(bytes.len())? <= MAX_BYTES,
            "CPU decompression bound"
        );
        let capture: Capture = serde_json::from_slice(&bytes)?;
        captures.push(serde_json::from_str::<Value>(&meta)?);
        covered.push((from, to));
        for sample in capture.samples {
            if sample.mono_us >= min && sample.mono_us <= max {
                samples += 1;
                if sample.frames.is_empty() || sample.frames.iter().any(|f| f.contains("[unknown]"))
                {
                    unknown += 1;
                }
                *stacks.entry(sample.frames).or_default() += 1;
            }
        }
    }
    let mut stacks: Vec<_> = stacks.into_iter().collect();
    stacks.sort_by_key(|entry| std::cmp::Reverse(entry.1));
    stacks.truncate(200);
    Ok(
        json!({"status":"available","scope":"process samples during the request interval; includes other blocks and background work","samples":samples,"unknown_samples":unknown,"sparse":samples<100,"clock_error_us":run_metadata.clock_error_us,"captures":captures,"covered_intervals_us":covered,"stacks":stacks.into_iter().map(|(frames,count)|json!({"frames":frames,"samples":count})).collect::<Vec<_>>() }),
    )
}

pub(crate) fn recover(db: &Connection, path: &Path) -> Result<()> {
    fs::create_dir_all(path.join("cpu"))?;
    fs::create_dir_all(path.join("inbox"))?;
    for entry in fs::read_dir(path.join("cpu"))? {
        let entry = entry?;
        ensure!(entry.file_type()?.is_file(), "unexpected CPU entry");
        let name = entry.file_name();
        let id = Path::new(&name)
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("");
        let retained = db
            .query_row("SELECT deleting FROM cpu WHERE id=?", [id], |r| {
                r.get::<_, bool>(0)
            })
            .optional()?;
        if retained != Some(false) || entry.path().extension().is_none_or(|e| e != "zst") {
            fs::remove_file(entry.path())?;
        }
    }
    db.execute("DELETE FROM cpu WHERE deleting=1", [])?;
    Ok(())
}

pub(crate) fn prune(db: &Connection, path: &Path, mut bytes: u64) -> Result<u64> {
    let files: Vec<(String, i64)> = db
        .prepare("SELECT id,bytes FROM cpu ORDER BY start_us LIMIT 4096")?
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect::<rusqlite::Result<_>>()?;
    let initial = bytes;
    for (id, size) in files {
        if bytes == 0 {
            break;
        }
        db.execute("UPDATE cpu SET deleting=1 WHERE id=?", [&id])?;
        match fs::remove_file(path.join("cpu").join(format!("{id}.zst"))) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        db.execute("DELETE FROM cpu WHERE id=?", [&id])?;
        bytes = bytes.saturating_sub(u64::try_from(size)?);
    }
    Ok(initial - bytes)
}
