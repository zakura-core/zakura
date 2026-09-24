//! Bounded timestamped process samples. Sample counts are never elapsed CPU ownership.
use anyhow::{ensure, Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    fmt::{self, Write},
    fs::{self, File},
    io::Read,
    path::Path,
};
use zakura_jsonl_trace::block_profile as profiles;

const MAX_BYTES: u64 = 16 * 1024 * 1024;
const MAX_QUERY_SAMPLES: usize = 20_000;
const MAX_QUERY_REFERENCES: usize = 500_000;
const MAX_SYMBOL_BYTES: usize = 8 * 1024 * 1024;
const MAX_SYMBOL: usize = 65_536;
const MAX_CAPTURES: usize = 32;

#[derive(Default, Serialize, Deserialize)]
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
    #[serde(default)]
    pub schema_version: u32,
    #[serde(default)]
    pub session: String,
    #[serde(default)]
    pub sequence: u64,
    #[serde(default)]
    pub uncertainty_us: u64,
    #[serde(default)]
    pub lost_samples: Option<u64>,
    #[serde(default)]
    pub omitted_samples: u64,
    #[serde(default)]
    pub omitted_frames: u64,
    #[serde(default)]
    pub symbol_truncations: u64,
    #[serde(default)]
    pub coverage_proven: bool,
    #[serde(default)]
    pub frames: Vec<CpuFrame>,
    #[serde(default)]
    pub stacks: Vec<Vec<u32>>,
}
#[derive(Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Sample {
    pub mono_us: u64,
    pub tid: u32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub frames: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stack: Option<u32>,
}
#[derive(Clone, Default, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CpuFrame {
    pub ip: String,
    pub symbol: String,
    pub dso: String,
}

fn validate(c: &Capture) -> Result<()> {
    ensure!(
        c.run.len() == 32 && c.run.bytes().all(|b| b.is_ascii_hexdigit()),
        "run identity"
    );
    ensure!(
        [19, 49, 99].contains(&c.frequency) && c.clock == "monotonic",
        "capture settings"
    );
    ensure!(matches!(c.schema_version, 0..=2), "capture schema");
    ensure!(
        c.end_mono_us >= c.start_mono_us && c.end_mono_us - c.start_mono_us <= 120_000_000,
        "capture duration"
    );
    ensure!(
        c.samples.len() <= 50_000 && c.frames.len() <= 50_000 && c.stacks.len() <= 50_000,
        "capture record bound"
    );
    ensure!(
        c.session.len() <= 128
            && c.build_ids.len() <= 512
            && c.build_ids.iter().all(|id| id.len() <= 512),
        "capture metadata bound"
    );
    ensure!(
        c.executable_sha256.len() == 64
            && c.executable_sha256.bytes().all(|b| b.is_ascii_hexdigit()),
        "executable identity"
    );
    for frame in &c.frames {
        ensure!(
            frame.symbol.len() <= MAX_SYMBOL && frame.ip.len() <= 128 && frame.dso.len() <= 4096,
            "symbol bound"
        );
    }
    for stack in &c.stacks {
        ensure!(
            stack.len() <= 128
                && stack
                    .iter()
                    .all(|id| usize::try_from(*id).is_ok_and(|id| id < c.frames.len())),
            "stack dictionary"
        );
    }
    for sample in &c.samples {
        ensure!(
            sample.mono_us >= c.start_mono_us && sample.mono_us <= c.end_mono_us,
            "sample outside capture"
        );
        ensure!(
            sample.frames.len() <= 128 && sample.frames.iter().all(|f| f.len() <= MAX_SYMBOL),
            "stack bound"
        );
        if c.schema_version == 2 {
            ensure!(
                sample.frames.is_empty()
                    && sample
                        .stack
                        .is_some_and(|id| usize::try_from(id).is_ok_and(|id| id < c.stacks.len())),
                "sample dictionary"
            );
        } else {
            ensure!(sample.stack.is_none(), "legacy stack encoding");
        }
    }
    Ok(())
}

pub(crate) fn import(db: &Connection, path: &Path, source: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(source)?;
    ensure!(
        metadata.is_file() && metadata.len() <= MAX_BYTES,
        "capture input limit"
    );
    let bytes = fs::read(source)?;
    let c: Capture = serde_json::from_slice(&bytes)?;
    validate(&c)?;
    let run: String = db.query_row("SELECT metadata FROM runs WHERE id=?", [&c.run], |r| {
        r.get(0)
    })?;
    let run: profiles::Run = serde_json::from_str(&run)?;
    let anchor = run
        .monotonic_start_us
        .context("run has no Linux monotonic anchor")?;
    ensure!(
        run.pid == c.pid && c.start_mono_us >= anchor,
        "capture belongs to a different process"
    );
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
    let compressed = zstd::stream::encode_all(bytes.as_slice(), 1)?;
    let target = path.join("cpu").join(format!("{id}.zst"));
    let temp = target.with_extension("tmp");
    let summary = json!({"schema_version":c.schema_version,"frequency":c.frequency,"scope":"process","decode_errors":c.decode_errors,"truncated":c.truncated,"executable_sha256":c.executable_sha256,"build_ids":c.build_ids,"process_start_ticks":c.process_start_ticks,"session":c.session,"sequence":c.sequence,"uncertainty_us":c.uncertainty_us,"lost_samples":c.lost_samples,"omitted_samples":c.omitted_samples,"omitted_frames":c.omitted_frames,"symbol_truncations":c.symbol_truncations,"coverage_proven":c.coverage_proven,"decoded_bytes":bytes.len()});
    crate::store::write_payload(&temp, &target, &compressed, |size| {
        db.execute(
            "INSERT INTO cpu VALUES(?,?,?,?,?,?,?,0)",
            params![
                id,
                c.run,
                i64::try_from(c.start_mono_us - anchor)?,
                i64::try_from(c.end_mono_us - anchor)?,
                i64::try_from(c.samples.len())?,
                i64::try_from(size)?,
                summary.to_string()
            ],
        )?;
        Ok(())
    })?;
    fs::remove_file(source)?;
    Ok(())
}

fn pending(db: &Connection, path: &Path, run: &str, anchor: u64, end: u64) -> bool {
    // A seal's observed end can precede publication of neighboring samples inside this
    // window. Wait for an imported segment whose sample-adjusted start is beyond it.
    let imported_start: Option<i64> = db
        .query_row(
            "SELECT max(start_us) FROM cpu WHERE run=? AND deleting=0",
            [run],
            |r| r.get(0),
        )
        .ok()
        .flatten();
    if imported_start
        .and_then(|n| u64::try_from(n).ok())
        .is_some_and(|n| end < n)
    {
        return false;
    }
    let end_mono = anchor.saturating_add(end);
    let path = path.join("cpu-status.json");
    let status = (|| -> Result<Value> {
        ensure!(fs::metadata(&path)?.len() <= 16_384, "sampler status bound");
        Ok(serde_json::from_slice(&fs::read(path)?)?)
    })()
    .ok();
    status.is_some_and(|s| {
        s["run"] == run
            && s["state"] == "recording"
            && s["updated_ms"]
                .as_u64()
                .is_some_and(|updated| crate::store::now_ms().saturating_sub(updated) < 30_000)
            && s["started_mono_us"]
                .as_u64()
                .is_some_and(|start| end_mono >= start)
    })
}
/// Indexed availability only. Ordinary block pages never decompress CPU captures.
pub(crate) fn availability(
    db: &Connection,
    path: &Path,
    run: &str,
    start: u64,
    end: u64,
) -> Result<Value> {
    let count: i64 = db.query_row(
        "SELECT count(*) FROM cpu WHERE run=? AND start_us>=? AND end_us>=? AND start_us<=? AND deleting=0",
        params![run, i64::try_from(start.saturating_sub(120_000_000))?, i64::try_from(start)?, i64::try_from(end)?],
        |r| r.get(0),
    )?;
    let metadata: String =
        db.query_row("SELECT metadata FROM runs WHERE id=?", [run], |r| r.get(0))?;
    let run_metadata: profiles::Run = serde_json::from_str(&metadata)?;
    let waiting = run_metadata
        .monotonic_start_us
        .is_some_and(|anchor| pending(db, path, run, anchor, end));
    Ok(
        json!({"status":if count>0{"available"}else if waiting{"pending"}else{"unavailable"},"captures":count,"pending":waiting,"scope":"process"}),
    )
}

fn gaps(start: u64, end: u64, intervals: &mut [(u64, u64)]) -> Vec<(u64, u64)> {
    intervals.sort_unstable();
    let mut missing = Vec::new();
    let mut cursor = start;
    for &(a, b) in intervals.iter() {
        let a = a.max(start);
        let b = b.min(end);
        if a > cursor {
            missing.push((cursor, a));
        }
        cursor = cursor.max(b);
    }
    if cursor < end {
        missing.push((cursor, end));
    }
    missing
}

/// Canonical point samples retain timestamps and TIDs. No sample is expanded across a wait.
pub(crate) fn window(
    db: &Connection,
    path: &Path,
    run: &str,
    start: u64,
    end: u64,
) -> Result<Value> {
    ensure!(end >= start, "CPU interval order");
    let metadata: String =
        db.query_row("SELECT metadata FROM runs WHERE id=?", [run], |r| r.get(0))?;
    let run_metadata: profiles::Run = serde_json::from_str(&metadata)?;
    let Some(anchor) = run_metadata.monotonic_start_us else {
        return Ok(
            json!({"status":"unavailable","coverage":{"state":"unavailable","reason":"No Linux clock anchor"},"samples":[],"frames":[],"stacks":[]}),
        );
    };
    let files:Vec<(String,String,i64,i64,i64)>=db.prepare("SELECT id,metadata,start_us,end_us,samples FROM cpu WHERE run=? AND start_us>=? AND end_us>=? AND start_us<=? AND deleting=0 ORDER BY start_us,id LIMIT 33")?
        .query_map(params![run,i64::try_from(start.saturating_sub(120_000_000))?,i64::try_from(start)?,i64::try_from(end)?],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?)))?.collect::<rusqlite::Result<_>>()?;
    let waiting = pending(db, path, run, anchor, end);
    let mut limited = files.len() > MAX_CAPTURES;
    let mut skipped = false;
    let mut decoded = 0u64;
    let mut sample_refs = 0usize;
    let mut symbol_bytes = 0usize;
    let mut stop_samples = false;
    let mut frame_ids = BTreeMap::<CpuFrame, u32>::new();
    let mut frames = Vec::new();
    let mut stack_ids = BTreeMap::<Vec<u32>, u32>::new();
    let mut stacks = Vec::new();
    let mut samples = Vec::<(u64, u32, u32)>::new();
    let mut matching = 0u64;
    let mut boundary_samples = 0u64;
    let mut omitted = 0u64;
    let mut capture_omitted = 0u64;
    let mut unknown = 0u64;
    let mut lost = 0u64;
    let mut loss_unknown = false;
    let mut decode_errors = 0u64;
    let mut omitted_frames = 0u64;
    let mut symbol_truncations = 0u64;
    let mut truncated = false;
    let mut proven = true;
    let mut captures = Vec::new();
    let mut intervals = Vec::new();
    let min = anchor
        .saturating_add(start)
        .saturating_add(run_metadata.clock_error_us);
    let max = anchor
        .saturating_add(end)
        .saturating_sub(run_metadata.clock_error_us);
    for (id, meta, from, to, _) in files.into_iter().take(MAX_CAPTURES) {
        ensure!(
            id.len() == 32 && id.bytes().all(|b| b.is_ascii_hexdigit()),
            "invalid capture path"
        );
        let meta: Value = serde_json::from_str(&meta)?;
        if meta["decoded_bytes"]
            .as_u64()
            .is_some_and(|n| decoded.saturating_add(n) > MAX_BYTES)
        {
            limited = true;
            skipped = true;
            continue;
        }
        let bytes = (|| -> Result<Vec<u8>> {
            let file = File::open(path.join("cpu").join(format!("{id}.zst")))?;
            ensure!(file.metadata()?.len() <= MAX_BYTES, "CPU compressed bound");
            let mut bytes = Vec::new();
            zstd::stream::read::Decoder::new(file)?
                .take(MAX_BYTES.saturating_sub(decoded) + 1)
                .read_to_end(&mut bytes)?;
            ensure!(
                u64::try_from(bytes.len())? <= MAX_BYTES.saturating_sub(decoded),
                "CPU query decode bound"
            );
            Ok(bytes)
        })();
        let Ok(bytes) = bytes else {
            skipped = true;
            continue;
        };
        decoded = decoded.saturating_add(u64::try_from(bytes.len())?);
        let capture = (|| -> Result<Capture> {
            let c: Capture = serde_json::from_slice(&bytes)?;
            validate(&c)?;
            ensure!(
                c.run == run && c.pid == run_metadata.pid,
                "capture identity"
            );
            Ok(c)
        })();
        let Ok(c) = capture else {
            skipped = true;
            continue;
        };
        proven &= c.coverage_proven;
        truncated |= c.truncated;
        decode_errors = decode_errors.saturating_add(c.decode_errors);
        capture_omitted = capture_omitted.saturating_add(c.omitted_samples);
        omitted_frames = omitted_frames.saturating_add(c.omitted_frames);
        symbol_truncations = symbol_truncations.saturating_add(c.symbol_truncations);
        match c.lost_samples {
            Some(n) => lost = lost.saturating_add(n),
            None => loss_unknown = true,
        }
        intervals.push((u64::try_from(from)?, u64::try_from(to)?));
        captures.push(json!({"id":id,"start_us":from,"end_us":to,"metadata":meta}));
        let mut local_frames = vec![None; c.frames.len()];
        let mut local_stacks = vec![None; c.stacks.len()];
        let frame_unknown: Vec<bool> = c
            .frames
            .iter()
            .map(|f| f.symbol.contains("[unknown]"))
            .collect();
        for sample in &c.samples {
            if sample.mono_us < anchor.saturating_add(start)
                || sample.mono_us >= anchor.saturating_add(end)
            {
                continue;
            }
            if sample.mono_us < min || sample.mono_us >= max {
                boundary_samples += 1;
                continue;
            }
            matching += 1;
            let depth = sample.stack.map_or(sample.frames.len(), |id| {
                c.stacks[usize::try_from(id).expect("validated stack")].len()
            });
            if stop_samples
                || samples.len() >= MAX_QUERY_SAMPLES
                || sample_refs.saturating_add(depth) > MAX_QUERY_REFERENCES
            {
                stop_samples = true;
                limited = true;
                omitted = omitted.saturating_add(1);
                continue;
            }
            if let Some((stack, is_unknown)) = sample
                .stack
                .and_then(|id| local_stacks[usize::try_from(id).expect("validated stack")])
            {
                sample_refs += depth;
                unknown += u64::from(is_unknown);
                samples.push((sample.mono_us, sample.tid, stack));
                continue;
            }
            let legacy;
            let source: Vec<(Option<usize>, &CpuFrame)> = if let Some(stack) = sample.stack {
                c.stacks[usize::try_from(stack)?]
                    .iter()
                    .rev()
                    .map(|id| {
                        let index = usize::try_from(*id).expect("validated frame");
                        (Some(index), &c.frames[index])
                    })
                    .collect()
            } else {
                legacy = sample
                    .frames
                    .iter()
                    .rev()
                    .map(|symbol| CpuFrame {
                        symbol: symbol.clone(),
                        ..Default::default()
                    })
                    .collect::<Vec<_>>();
                legacy.iter().map(|frame| (None, frame)).collect()
            };
            let is_unknown = source.is_empty()
                || source.iter().any(|(index, f)| {
                    index.map_or_else(|| f.symbol.contains("[unknown]"), |id| frame_unknown[id])
                });
            let mut stack = Vec::new();
            for (local, frame) in source {
                if let Some(id) = local.and_then(|index| local_frames[index]) {
                    stack.push(id);
                    continue;
                }
                let id = if let Some(id) = frame_ids.get(frame) {
                    *id
                } else {
                    let demangled = readable_frame(&frame.symbol);
                    let name = if frame.dso.is_empty() {
                        demangled
                    } else {
                        format!("{} ({})", demangled, frame.dso)
                    };
                    let bytes = frame
                        .symbol
                        .len()
                        .saturating_add(frame.dso.len())
                        .saturating_add(frame.ip.len())
                        .saturating_add(name.len());
                    if symbol_bytes.saturating_add(bytes) > MAX_SYMBOL_BYTES {
                        stop_samples = true;
                        limited = true;
                        break;
                    }
                    symbol_bytes += bytes;
                    let id = u32::try_from(frames.len())?;
                    frames.push(
                        json!({"name":name,"symbol":frame.symbol,"ip":frame.ip,"dso":frame.dso}),
                    );
                    frame_ids.insert(frame.clone(), id);
                    id
                };
                if let Some(index) = local {
                    local_frames[index] = Some(id);
                }
                stack.push(id);
            }
            if stop_samples {
                omitted = omitted.saturating_add(1);
                continue;
            }
            unknown += u64::from(is_unknown);
            sample_refs += depth;
            let next = u32::try_from(stacks.len())?;
            let stack = *stack_ids.entry(stack.clone()).or_insert_with(|| {
                stacks.push(stack);
                next
            });
            if let Some(id) = sample.stack {
                local_stacks[usize::try_from(id)?] = Some((stack, is_unknown));
            }
            samples.push((sample.mono_us, sample.tid, stack));
        }
    }
    samples.sort_unstable();
    let missing = gaps(start, end, &mut intervals);
    let partial = boundary_samples > 0
        || limited
        || skipped
        || truncated
        || decode_errors > 0
        || lost > 0
        || omitted > 0
        || capture_omitted > 0
        || omitted_frames > 0
        || symbol_truncations > 0
        || !missing.is_empty()
        || !proven
        || loss_unknown;
    let state = if waiting {
        "pending"
    } else if captures.is_empty() {
        "unavailable"
    } else if partial {
        "partial"
    } else {
        "complete"
    };
    let reason = if waiting {
        "Recent samples are still being sealed or imported"
    } else if captures.is_empty() {
        "No retained CPU capture covers this interval"
    } else if limited || skipped {
        "Some capture data exceeded query limits or expired"
    } else if !proven {
        "Capture boundaries are approximate; complete acquisition is not proven"
    } else if partial {
        "Capture contains gaps, loss, or incomplete stacks"
    } else {
        "Acquisition coverage verified; sample counts remain statistical"
    };
    Ok(
        json!({"status":if samples.is_empty(){state}else{"available"},"scope":"User-space process CPU samples in this block window, including other blocks and background work; not exclusive block CPU ownership","frequency_hz":captures.first().and_then(|c|c["metadata"]["frequency"].as_u64()),"sparse":samples.len()<100,
        "counts":{"returned_samples":samples.len(),"matching_samples":matching,"omitted_samples":omitted,"capture_omitted_samples":capture_omitted,"unknown_samples":unknown,"boundary_excluded_samples":boundary_samples,"matching_count_complete":!skipped&&!limited},
        "coverage":{"state":state,"reason":reason,"capture_intervals_us":intervals,"gaps_us":missing,"clock_error_us":run_metadata.clock_error_us,"acquisition_uncertain":!proven,"query_limited":limited,"expired_or_unreadable":skipped,"lost_samples":if loss_unknown{None}else{Some(lost)},"known_lost_samples":lost,"capture_omitted_samples":capture_omitted,"decode_errors":decode_errors,"omitted_frames":omitted_frames,"symbol_truncations":symbol_truncations,"truncated":truncated,"captures":captures},
        "samples":samples.into_iter().map(|(mono_us,tid,stack)|json!({"at_us":mono_us.saturating_sub(anchor).saturating_sub(start),"mono_us":mono_us,"tid":tid,"stack":stack})).collect::<Vec<_>>(),"frames":frames,"stacks":stacks}),
    )
}

/// Speedscope widths are sample counts. Its schema has no per-sample timestamp field.
pub(crate) fn speedscope(data: &Value) -> Result<Value> {
    // A function's sampled instruction addresses should not split its hot-path count.
    // Raw symbols preserve monomorph identity; unresolved addresses stay distinct.
    let mut identities = BTreeMap::new();
    let mut frames = Vec::new();
    let mut mapping = Vec::new();
    for frame in data["frames"].as_array().context("CPU frames")? {
        let symbol = frame["symbol"].as_str().context("CPU raw symbol")?;
        let dso = frame["dso"].as_str().context("CPU module")?;
        let address = if symbol.is_empty() || symbol.contains("[unknown]") {
            Some(frame["ip"].as_str().context("CPU unresolved address")?)
        } else {
            None
        };
        let next = frames.len();
        let id = *identities.entry((symbol, dso, address)).or_insert_with(|| {
            frames.push(json!({"name":frame["name"]}));
            next
        });
        mapping.push(id);
    }
    let stacks = data["stacks"]
        .as_array()
        .context("CPU stacks")?
        .iter()
        .map(|stack| {
            stack
                .as_array()
                .context("CPU stack")?
                .iter()
                .map(|id| {
                    id.as_u64()
                        .and_then(|id| usize::try_from(id).ok())
                        .and_then(|id| mapping.get(id))
                        .copied()
                        .context("CPU frame reference")
                })
                .collect::<Result<Vec<_>>>()
                .map(|stack| json!(stack))
        })
        .collect::<Result<Vec<_>>>()?;
    let samples = data["samples"].as_array().context("CPU samples")?;
    let mut threads = BTreeMap::<u64, Vec<Value>>::new();
    let mut all = Vec::new();
    for sample in samples {
        let stack = sample["stack"]
            .as_u64()
            .and_then(|id| usize::try_from(id).ok())
            .and_then(|id| stacks.get(id))
            .context("CPU stack reference")?
            .clone();
        all.push(stack.clone());
        threads
            .entry(sample["tid"].as_u64().context("CPU TID")?)
            .or_default()
            .push(stack);
    }
    let profile = |name: String, samples: Vec<Value>| json!({"type":"sampled","unit":"none","name":name,"startValue":0,"endValue":samples.len(),"weights":vec![1;samples.len()],"samples":samples});
    let mut profiles = vec![profile(
        "All threads: sample order, not elapsed time".into(),
        all,
    )];
    profiles.extend(threads.into_iter().map(|(tid, samples)| {
        profile(
            format!("TID {tid}: sample order, not elapsed time"),
            samples,
        )
    }));
    let coverage = data["coverage"]["state"].as_str().unwrap_or("unknown");
    Ok(
        json!({"$schema":"https://www.speedscope.app/file-format-schema.json","name":format!("User-space process CPU samples ({coverage} capture); widths are counts"),"activeProfileIndex":0,"exporter":"Zakura profiler","shared":{"frames":frames},"profiles":profiles,
            "zakura":{"frame_aggregation":"Exact raw function symbol and module; unresolved addresses remain distinct","window":data["window"],"counts":data["counts"],"coverage":data["coverage"],"run":data["summary"]["run"],"attempt":data["summary"]["attempt"],"block_height":data["summary"]["height"]}}),
    )
}

fn readable_frame(frame: &str) -> String {
    let (symbol, suffix) = frame
        .rsplit_once(" (")
        .map_or((frame, ""), |(symbol, _)| (symbol, &frame[symbol.len()..]));
    let Ok(symbol) = rustc_demangle::try_demangle(symbol) else {
        return frame.to_owned();
    };
    struct Label(String);
    impl Write for Label {
        fn write_str(&mut self, text: &str) -> fmt::Result {
            if self.0.len().saturating_add(text.len()) > MAX_SYMBOL {
                return Err(fmt::Error);
            }
            self.0.push_str(text);
            Ok(())
        }
    }
    let mut label = Label(String::new());
    if write!(&mut label, "{symbol:#}{suffix}").is_ok() {
        label.0
    } else {
        frame.to_owned()
    }
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

#[cfg(test)]
mod export_tests {
    use super::*;

    #[test]
    fn function_export_aggregates_instruction_addresses_without_merging_raw_symbols() -> Result<()>
    {
        let frame = |symbol: &str, ip: &str, dso: &str| json!({"name":"same display label","symbol":symbol,"ip":ip,"dso":dso});
        let data = json!({
            "frames":[frame("function<T>","100","node"),frame("function<T>","104","node"),
                frame("function<U>","108","node"),frame("function<T>","100","other.so"),
                frame("[unknown]","200","node"),frame("[unknown]","204","node")],
            "stacks":[[0],[1],[2],[3],[4],[5]],
            "samples":(0..6).map(|id|json!({"tid":1,"stack":id})).collect::<Vec<_>>(),
            "coverage":{"state":"partial"}
        });
        let export = speedscope(&data)?;
        assert_eq!(export["shared"]["frames"].as_array().unwrap().len(), 5);
        assert_eq!(
            export["profiles"][0]["samples"],
            json!([[0], [0], [1], [2], [3], [4]])
        );
        assert_eq!(export["profiles"][0]["weights"], json!([1, 1, 1, 1, 1, 1]));
        assert_eq!(
            data["frames"].as_array().unwrap().len(),
            6,
            "canonical addresses remain intact"
        );
        Ok(())
    }
}
