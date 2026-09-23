use super::*;
use profiles::{Mode, Outcome, Run, Stage};
use zakura_jsonl_trace::block_profile as profiles;

const RUN: &str = "11111111111111111111111111111111";
fn metadata() -> Frame {
    Frame::Run {
        schema: SCHEMA_VERSION,
        run: Run {
            id: RUN.into(),
            node: "test".into(),
            session: "test".into(),
            network: "regtest".into(),
            build: "fixture".into(),
            storage: "pruned".into(),
            pid: 1,
            utc_start_ms: now_ms(),
            monotonic_start_us: None,
            clock_error_us: 1,
            startup_gate: false,
        },
    }
}
fn block() -> Block {
    Block {
        hash: [1; 32],
        parent: [0; 32],
        height: Some(42),
        transactions: 1,
        mode: Mode::Semantic,
    }
}
fn event(sequence: u64, data: Event) -> Frame {
    Frame::Event {
        schema: SCHEMA_VERSION,
        run_id: RUN.into(),
        sequence,
        data,
    }
}
fn finish() -> Event {
    Event::Finish {
        attempt: 1,
        start_us: 100,
        end_us: 700100,
        block: block(),
        outcome: Outcome::Success,
        dropped: 0,
    }
}
fn span() -> Event {
    Event::Span {
        attempt: 1,
        span: 1,
        parent: 0,
        stage: Stage::WriterOccupied,
        transaction_index: None,
        start_us: 600000,
        end_us: 800000,
        completion_thread: None,
    }
}

#[test]
fn large_transaction_profile_survives_chunking_and_restart() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let mut store = Store::open(temp.path(), 16_000_000)?;
    store.ingest(metadata())?;
    store.ingest(event(1, finish()))?;
    let count = u64::try_from(MAX_CHUNK_EVENTS + 100)?;
    for first in (1..=count).step_by(MAX_INGEST_BATCH) {
        let frames = (first..=(first + u64::try_from(MAX_INGEST_BATCH)? - 1).min(count))
            .map(|id| {
                event(
                    id + 1,
                    Event::Span {
                        attempt: 1,
                        span: id,
                        parent: if id == 1 { 0 } else { 1 },
                        stage: if id == 1 {
                            Stage::Transaction
                        } else {
                            Stage::TransactionChecks
                        },
                        transaction_index: Some(7),
                        start_us: 200,
                        end_us: 600000,
                        completion_thread: None,
                    },
                )
            })
            .collect();
        assert_eq!(store.ingest_batch(frames)?, 0);
    }
    store.ingest(event(
        count + 2,
        Event::Seal {
            attempt: 1,
            spans: count,
            dropped: 0,
        },
    ))?;
    store.flush()?;
    drop(store);
    let _reopened = Store::open(temp.path(), 16_000_000)?;
    let detail = Reader::open(temp.path())?.detail(RUN, 1)?;
    assert_eq!(detail["complete"], true);
    let spans = detail["spans"].as_array().unwrap();
    assert_eq!(spans.len(), usize::try_from(count)?);
    assert!(spans.iter().all(|span| span["transaction_index"] == 7));
    Ok(())
}

#[test]
fn startup_boundary_uses_entry_time_and_survives_heartbeat_and_collector_restarts() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let mut store = Store::open(temp.path(), 16_000_000)?;
    let mut run = metadata();
    if let Frame::Run { run, .. } = &mut run {
        run.startup_gate = true;
    }
    store.ingest(run)?;
    store.ingest(event(1, finish()))?;
    let health = |ready_us| Frame::Health {
        schema: SCHEMA_VERSION,
        run_id: RUN.into(),
        at_us: 1_000_000,
        attempts: 2,
        dropped: 0,
        transport_dropped: 0,
        ready_us,
    };
    store.ingest(health(None))?;
    let home = Reader::open(temp.path())?.home(Some(RUN), "semantic")?;
    assert_eq!(home["startup_pending"], true);
    assert_eq!(home["timing_blocks"], 0);
    assert_eq!(home["outliers"], json!([]));
    assert_eq!(home["latest"][0]["startup"], true);

    store.ingest(health(Some(200_000)))?;
    // Finishing after the boundary must not promote a request that started during startup.
    let detail = Reader::open(temp.path())?.detail(RUN, 1)?;
    assert_eq!(detail["summary"]["startup"], true);
    assert_eq!(detail["timing"]["verifier_elapsed_us"], 700_000);
    assert_eq!(detail["timing"]["eligible_for_statistics"], false);
    let mut next = block();
    next.hash = [2; 32];
    next.height = Some(43);
    store.ingest(event(
        2,
        Event::Finish {
            attempt: 2,
            start_us: 200_000,
            end_us: 1_000_000,
            block: next,
            outcome: Outcome::Success,
            dropped: 0,
        },
    ))?;
    store.ingest(health(None))?;
    drop(store);
    let _store = Store::open(temp.path(), 16_000_000)?;
    let reader = Reader::open(temp.path())?;
    let home = reader.home(Some(RUN), "semantic")?;
    assert_eq!(home["startup_pending"], false);
    assert_eq!(home["startup_timings"], 1);
    assert_eq!(home["timing_blocks"], 1);
    assert_eq!(home["outliers"].as_array().unwrap().len(), 1);
    assert_eq!(home["outliers"][0]["attempt"], 2);
    assert_eq!(home["latency"]["p99_us"], 800_000);
    assert_eq!(reader.search("42")?[0]["startup"], true);
    Ok(())
}

#[test]
fn legacy_startup_boundary_preserves_raw_timings_and_cannot_move() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let mut store = Store::open(temp.path(), 16_000_000)?;
    store.ingest(metadata())?;
    store.ingest(event(1, finish()))?;
    assert_eq!(
        Reader::open(temp.path())?.home(Some(RUN), "semantic")?["timing_blocks"],
        1
    );
    startup_boundary(temp.path(), RUN, 200_000)?;
    startup_boundary(temp.path(), RUN, 200_000)?;
    assert!(startup_boundary(temp.path(), RUN, 0).is_err());
    let reader = Reader::open(temp.path())?;
    assert_eq!(reader.home(Some(RUN), "semantic")?["outliers"], json!([]));
    assert_eq!(reader.detail(RUN, 1)?["summary"]["end_us"], 700_100);
    Ok(())
}

#[test]
fn late_detail_seals_after_response_and_survives_restart() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let mut store = Store::open(temp.path(), 16_000_000)?;
    store.ingest(metadata())?;
    store.ingest(event(1, finish()))?;
    store.ingest(event(
        2,
        Event::Seal {
            attempt: 1,
            spans: 1,
            dropped: 0,
        },
    ))?;
    assert_eq!(
        Reader::open(temp.path())?.detail(RUN, 1)?["complete"],
        false
    );
    store.ingest(event(3, span()))?;
    store.flush()?;
    let result = Reader::open(temp.path())?.detail(RUN, 1)?;
    assert_eq!(result["complete"], true);
    assert_eq!(result["spans"][0]["end_us"], 800000);
    assert_eq!(result["timing"]["recorded_elapsed_us"], 799900);
    assert_eq!(result["timing"]["verifier_elapsed_us"], 700000);
    assert_eq!(result["timing"]["after_response_us"], 99900);
    assert_eq!(result["timing"]["valid"], true);
    drop(store);
    let _reopened = Store::open(temp.path(), 16_000_000)?;
    assert_eq!(Reader::open(temp.path())?.detail(RUN, 1)?["complete"], true);
    Ok(())
}

#[test]
fn transport_gap_and_duplicate_do_not_fabricate_complete_detail() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let mut store = Store::open(temp.path(), 16_000_000)?;
    store.ingest(metadata())?;
    store.ingest(event(1, finish()))?;
    store.ingest(event(
        3,
        Event::Seal {
            attempt: 1,
            spans: 2,
            dropped: 0,
        },
    ))?;
    store.ingest(event(4, span()))?;
    store.ingest(event(4, span()))?;
    store.flush()?;
    let reader = Reader::open(temp.path())?;
    assert_eq!(reader.detail(RUN, 1)?["complete"], false);
    assert_eq!(reader.detail(RUN, 1)?["spans"].as_array().unwrap().len(), 1);
    assert_eq!(
        reader.home(Some(RUN), "semantic")?["runs"][0]["sequence_gaps"],
        1
    );
    Ok(())
}

#[test]
fn pruning_keeps_summary_and_marks_expired() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let mut store = Store::open(temp.path(), 16_000_000)?;
    store.ingest(metadata())?;
    store.ingest(event(1, finish()))?;
    store.ingest(event(2, span()))?;
    store.flush()?;
    let id: String = store
        .db
        .query_row("SELECT id FROM chunks", [], |r| r.get(0))?;
    store.remove_chunk(&id)?;
    let result = Reader::open(temp.path())?.detail(RUN, 1)?;
    assert_eq!(result["summary"]["expired"], true);
    assert_eq!(result["summary"]["end_us"], 700100);
    assert_eq!(result["complete"], false);
    Ok(())
}

#[test]
fn crash_recovery_removes_orphans_and_recovers_deletion() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let mut store = Store::open(temp.path(), 16_000_000)?;
    store.ingest(metadata())?;
    store.ingest(event(1, finish()))?;
    store.ingest(event(2, span()))?;
    store.flush()?;
    store.db.execute("UPDATE chunks SET deleting=1", [])?;
    fs::write(temp.path().join("chunks/orphan.tmp"), b"interrupted write")?;
    drop(store);
    let store = Store::open(temp.path(), 16_000_000)?;
    assert_eq!(fs::read_dir(temp.path().join("chunks"))?.count(), 0);
    let count: i64 = store
        .db
        .query_row("SELECT count(*) FROM chunks", [], |r| r.get(0))?;
    assert_eq!(count, 0);
    assert_eq!(
        Reader::open(temp.path())?.detail(RUN, 1)?["summary"]["expired"],
        true
    );
    Ok(())
}

#[test]
fn malformed_events_cannot_advance_durable_sequence() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let mut store = Store::open(temp.path(), 16_000_000)?;
    store.ingest(metadata())?;
    let mut invalid = span();
    if let Event::Span { end_us, .. } = &mut invalid {
        *end_us = 0;
    }
    assert!(store.ingest(event(1, invalid)).is_err());
    store.ingest(event(1, finish()))?;
    assert_eq!(
        Reader::open(temp.path())?.detail(RUN, 1)?["summary"]["outcome"],
        "success"
    );
    assert!(Store::open(temp.path(), 16_000_000).is_err());
    Ok(())
}

#[test]
fn cpu_samples_use_process_scope_and_exclude_clock_boundaries() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let mut store = Store::open(temp.path(), 16_000_000)?;
    let mut frame = metadata();
    if let Frame::Run { run, .. } = &mut frame {
        run.monotonic_start_us = Some(1_000_000);
        run.clock_error_us = 2;
    }
    store.ingest(frame)?;
    store.ingest(event(1, finish()))?;
    let capture = crate::cpu::Capture {
        run: RUN.into(),
        pid: 1,
        frequency: 19,
        clock: "monotonic".into(),
        start_mono_us: 1_000_000,
        end_mono_us: 2_000_000,
        process_start_ticks: 1,
        executable_sha256: "a".repeat(64),
        build_ids: vec!["abc node".into()],
        decode_errors: 0,
        truncated: false,
        samples: vec![100, 200, 700099, 800000]
            .into_iter()
            .map(|offset| crate::cpu::Sample {
                mono_us: 1_000_000 + offset,
                tid: 1,
                frames: vec![
                    "_RNvC6_123foo3bar (zakurad)".into(),
                    "native function (libc.so.6)".into(),
                ],
            })
            .collect(),
    };
    let path = temp
        .path()
        .join("inbox")
        .join(format!("{}.json", "a".repeat(32)));
    fs::write(&path, serde_json::to_vec(&capture)?)?;
    crate::cpu::import(&store.db, temp.path(), &path)?;
    let result = Reader::open(temp.path())?.detail(RUN, 1)?;
    assert_eq!(result["cpu"]["samples"], 1);
    assert_eq!(result["cpu"]["sparse"], true);
    assert_eq!(
        result["cpu"]["stacks"][0]["frames"][0],
        "123foo::bar (zakurad)"
    );
    assert_eq!(
        result["cpu"]["stacks"][0]["frames"][1],
        "native function (libc.so.6)"
    );
    assert!(result["cpu"]["scope"]
        .as_str()
        .unwrap()
        .contains("other blocks"));
    assert!(!path.exists());
    Ok(())
}

#[test]
fn failed_catalog_commit_removes_payload_and_preserves_budget() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let store = Store::open(temp.path(), 16_000_000)?;
    let staged = temp.path().join("chunks/pending.tmp");
    let published = staged.with_extension("zst");
    let result = write_payload(&staged, &published, b"profile data", |_| {
        anyhow::bail!("injected catalog failure")
    });
    assert!(result.is_err());
    assert!(!staged.exists());
    assert!(!published.exists());
    assert_eq!(fs::read_dir(temp.path().join("chunks"))?.count(), 0);
    assert!(store.storage_bytes()? < 16_000_000);
    Ok(())
}

#[test]
fn receive_batch_syncs_once_and_isolates_invalid_events() -> Result<()> {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    let temp = tempfile::tempdir()?;
    let mut store = Store::open(temp.path(), 16_000_000)?;
    let commits = Arc::new(AtomicUsize::new(0));
    let count = commits.clone();
    store.db.commit_hook(Some(move || {
        count.fetch_add(1, Ordering::Relaxed);
        false
    }))?;
    let mut invalid = span();
    if let Event::Span { end_us, .. } = &mut invalid {
        *end_us = 0;
    }
    assert_eq!(
        store.ingest_batch(vec![
            metadata(),
            event(1, finish()),
            event(2, invalid),
            event(2, span()),
            event(
                3,
                Event::Seal {
                    attempt: 1,
                    spans: 1,
                    dropped: 0
                }
            ),
        ])?,
        1
    );
    assert_eq!(commits.load(Ordering::Relaxed), 1);
    assert_eq!(
        store
            .db
            .pragma_query_value::<u32, _>(None, "synchronous", |r| r.get(0))?,
        2
    );
    store.flush()?;
    drop(store);
    let _reopened = Store::open(temp.path(), 16_000_000)?;
    assert_eq!(Reader::open(temp.path())?.detail(RUN, 1)?["complete"], true);
    Ok(())
}

#[test]
fn receive_batch_can_publish_a_full_chunk_and_recover_a_failed_commit() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let mut store = Store::open(temp.path(), 16_000_000)?;
    store.ingest(metadata())?;
    let mut sequence = 0u64;
    for batch in 0..(MAX_CHUNK_EVENTS / MAX_INGEST_BATCH) {
        let frames = (0..MAX_INGEST_BATCH)
            .map(|index| {
                sequence += 1;
                let mut data = span();
                if let Event::Span { attempt, span, .. } = &mut data {
                    *attempt = u64::try_from(batch + 1).unwrap();
                    *span = u64::try_from(index + 1).unwrap();
                }
                event(sequence, data)
            })
            .collect();
        assert_eq!(store.ingest_batch(frames)?, 0);
    }
    assert!(store.pending.is_empty());
    assert_eq!(
        Reader::open(temp.path())?.detail(RUN, 1)?["spans"]
            .as_array()
            .unwrap()
            .len(),
        MAX_INGEST_BATCH
    );
    // Force the next chunk to publish its file, then reject the outer catalog commit.
    for _ in 0..(MAX_CHUNK_EVENTS - 1) {
        store.pending.push(Detail {
            run: RUN.into(),
            data: span(),
        });
    }
    store.db.commit_hook(Some(|| true))?;
    assert!(store
        .ingest_batch(vec![event(sequence + 1, span())])
        .is_err());
    store.db.commit_hook(None::<fn() -> bool>)?;
    drop(store);
    let reopened = Store::open(temp.path(), 16_000_000)?;
    assert_eq!(fs::read_dir(temp.path().join("chunks"))?.count(), 1);
    assert_eq!(
        reopened
            .db
            .query_row("SELECT sequence FROM runs WHERE id=?", [RUN], |r| r
                .get::<_, i64>(0))?,
        i64::try_from(sequence)?
    );
    Ok(())
}

fn recording(attempt: u64, hash: u8, start_us: u64, duration_us: u64) -> Event {
    Event::Finish {
        attempt,
        start_us,
        end_us: start_us + duration_us,
        block: Block {
            hash: [hash; 32],
            ..block()
        },
        outcome: Outcome::Success,
        dropped: 0,
    }
}

#[test]
fn search_returns_latest_across_runs_without_merging_forks() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let mut store = Store::open(temp.path(), 16_000_000)?;
    let old = metadata();
    let mut new = metadata();
    let next_run = "22222222222222222222222222222222";
    if let Frame::Run { run, .. } = &mut new {
        run.id = next_run.into();
        run.utc_start_ms += 10_000;
    }
    store.ingest(old)?;
    store.ingest(event(1, recording(100, 1, 100, 900_000)))?;
    store.ingest(new)?;
    for (sequence, data) in [
        (1, recording(1, 1, 100, 500_000)),
        // The same millisecond still has a deterministic latest request.
        (2, recording(2, 1, 101, 100_000)),
        (3, recording(3, 2, 102, 600_000)),
    ] {
        store.ingest(Frame::Event {
            schema: SCHEMA_VERSION,
            run_id: next_run.into(),
            sequence,
            data,
        })?;
    }
    let reader = Reader::open(temp.path())?;
    let search = reader.search("42")?;
    assert_eq!(search.as_array().unwrap().len(), 2);
    assert!(search
        .as_array()
        .unwrap()
        .iter()
        .all(|r| r["run"] == next_run));
    let hash_search = reader.search(&"01".repeat(32))?;
    assert_eq!(hash_search.as_array().unwrap().len(), 1);
    assert_eq!(hash_search[0]["attempt"], 2);
    let home = reader.home(Some(next_run), "semantic")?;
    assert_eq!(home["latest"].as_array().unwrap().len(), 2);
    assert_eq!(home["outliers"].as_array().unwrap().len(), 1);
    assert_eq!(home["outliers"][0]["attempt"], 3);
    assert_eq!(home["timing_blocks"], 2);
    assert_eq!(home["latency"]["p50_us"], 100_000);
    // Original evidence remains addressable even though searches prefer the newer run.
    assert_eq!(reader.detail(RUN, 100)?["summary"]["end_us"], 900_100);
    Ok(())
}

#[test]
fn excluded_timing_preserves_raw_evidence_and_cannot_resurface_old_outlier() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let mut store = Store::open(temp.path(), 16_000_000)?;
    store.ingest(metadata())?;
    store.ingest(event(1, recording(1, 1, 100, 900_000)))?;
    store.ingest(event(2, recording(2, 1, 200, 332_618_525)))?;
    store.ingest(event(3, recording(3, 2, 300, 600_000)))?;
    // Annotation works with the collector open. Missing records and empty reasons fail.
    assert!(exclude_timing(temp.path(), RUN, 99, "test").is_err());
    assert!(exclude_timing(temp.path(), RUN, 2, " ").is_err());
    exclude_timing(temp.path(), RUN, 2, "Operator paused the node")?;
    drop(store);
    let store = Store::open(temp.path(), 16_000_000)?;
    let reader = Reader::open(temp.path())?;
    let home = reader.home(Some(RUN), "semantic")?;
    assert_eq!(home["outliers"].as_array().unwrap().len(), 1);
    assert_eq!(home["outliers"][0]["attempt"], 3);
    assert_eq!(home["timing_blocks"], 1);
    assert_eq!(home["excluded_timings"], 1);
    assert_eq!(home["latency"]["p99_us"], 600_000);
    let search = reader.search(&"01".repeat(32))?;
    assert_eq!(search.as_array().unwrap().len(), 1);
    assert_eq!(search[0]["attempt"], 2);
    assert_eq!(search[0]["exclusion_reason"], "Operator paused the node");
    let detail = reader.detail(RUN, 2)?;
    assert_eq!(detail["timing"]["valid"], false);
    assert_eq!(detail["timing"]["recorded_elapsed_us"], 332_618_525);
    assert_eq!(detail["summary"]["end_us"], 332_618_725);
    store
        .db
        .execute("DELETE FROM attempts WHERE run=? AND attempt=2", [RUN])?;
    assert_eq!(
        store
            .db
            .query_row("SELECT count(*) FROM timing_exclusions", [], |r| r
                .get::<_, i64>(0))?,
        0
    );
    Ok(())
}
