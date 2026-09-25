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
            source: None,
            storage: "pruned".into(),
            pid: 1,
            utc_start_ms: now_ms(),
            monotonic_start_us: None,
            clock_error_us: 1,
            startup_gate: false,
            verification_detail_version: 0,
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
        transaction_hash: None,
        start_us: 600000,
        end_us: 800000,
        completion_thread: None,
        verification: None,
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
                        transaction_hash: if id == 1 { Some([3; 32]) } else { None },
                        start_us: 200,
                        end_us: 600000,
                        completion_thread: None,
                        verification: None,
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
    let root = spans.iter().find(|span| span["span"] == 1).unwrap();
    assert_eq!(root["transaction_hash"], json!([3; 32].to_vec()));
    assert!(spans
        .iter()
        .filter(|span| span["span"] != 1)
        .all(|span| span.get("transaction_hash").is_none()));
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
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    let path = temp
        .path()
        .join("inbox")
        .join(format!("{}.json", "a".repeat(32)));
    fs::write(&path, serde_json::to_vec(&capture)?)?;
    crate::cpu::import(&store.db, temp.path(), &path)?;
    let reader = Reader::open(temp.path())?;
    assert_eq!(reader.detail(RUN, 1)?["cpu"]["status"], "available");
    let result = reader.cpu(RUN, 1, "recorded")?;
    assert_eq!(result["counts"]["returned_samples"], 1);
    assert_eq!(result["counts"]["boundary_excluded_samples"], 2);
    assert_eq!(result["sparse"], true);
    assert_eq!(result["frames"][1]["name"], "123foo::bar (zakurad)");
    assert_eq!(result["frames"][0]["name"], "native function (libc.so.6)");
    assert!(result["scope"].as_str().unwrap().contains("other blocks"));
    assert_eq!(result["coverage"]["state"], "partial");
    let export = reader.cpu_speedscope(RUN, 1, "recorded")?;
    assert_eq!(export["profiles"][0]["unit"], "none");
    assert_eq!(export["profiles"][0]["weights"], json!([1]));
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

#[test]
fn source_provenance_survives_collection_and_legacy_annotation() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let mut store = Store::open(temp.path(), 16_000_000)?;
    let legacy = metadata();
    let mut encoded = serde_json::to_value(&legacy)?;
    assert!(encoded["run"].get("source").is_none());
    let source_info = profiles::Source {
        base_commit: "a".repeat(40),
        commit: "b".repeat(40),
    };
    store.ingest(legacy)?;
    source(temp.path(), RUN, "fixture", source_info.clone())?;
    source(temp.path(), RUN, "fixture", source_info.clone())?;
    assert!(source(temp.path(), RUN, "wrong build", source_info.clone()).is_err());
    assert!(source(
        temp.path(),
        RUN,
        "fixture",
        profiles::Source {
            base_commit: "c".repeat(40),
            ..source_info.clone()
        }
    )
    .is_err());
    let home = Reader::open(temp.path())?.home(Some(RUN), "semantic")?;
    assert_eq!(
        home["runs"][0]["metadata"]["source"],
        serde_json::to_value(&source_info)?
    );
    // New run frames retain both revisions and reject malformed provenance.
    encoded["run"]["id"] = json!("22222222222222222222222222222222");
    encoded["run"]["source"] = serde_json::to_value(&source_info)?;
    store.ingest(serde_json::from_value(encoded.clone())?)?;
    encoded["run"]["source"]["base_commit"] = json!("main");
    assert!(store.ingest(serde_json::from_value(encoded)?).is_err());
    Ok(())
}

#[test]
fn shared_verification_work_preserves_block_totals_and_survives_restart() -> Result<()> {
    use profiles::verification::{Detail, Status, Workload};
    let temp = tempfile::tempdir()?;
    let mut store = Store::open(temp.path(), 16_000_000)?;
    store.ingest(metadata())?;
    store.ingest(event(1, finish()))?;
    let mut batch = span();
    if let Event::Span {
        stage,
        start_us,
        end_us,
        verification,
        ..
    } = &mut batch
    {
        *stage = Stage::VerificationBatch;
        *start_us = 10;
        *end_us = 900000;
        *verification = Some(Detail::Batch {
            id: 7,
            workload: Workload {
                spends: 0,
                outputs: 0,
                actions: 4,
            },
            members: 2,
            profiled: 1,
            unprofiled: 1,
            sapling: 0,
            orchard: 2,
            ironwood: 0,
            flush_us: Some(15),
            dispatch_us: Some(20),
            worker_start_us: Some(30),
            setup_end_us: Some(40),
            execution_end_us: Some(899000),
            published_us: Some(900000),
            status: Status::Success,
            partial: false,
        });
    }
    store.ingest(event(2, batch))?;
    store.ingest(event(
        3,
        Event::Seal {
            attempt: 1,
            spans: 1,
            dropped: 0,
        },
    ))?;
    store.flush()?;
    drop(store);
    let _store = Store::open(temp.path(), 16_000_000)?;
    let detail = Reader::open(temp.path())?.detail(RUN, 1)?;
    assert_eq!(detail["complete"], true);
    assert_eq!(detail["timing"]["recorded_elapsed_us"], 700000);
    assert_eq!(detail["timing"]["after_response_us"], 0);
    assert_eq!(detail["spans"][0]["verification"]["id"], 7);
    assert_eq!(detail["spans"][0]["start_us"], 10);
    Ok(())
}

#[test]
fn malformed_verification_evidence_is_rejected_without_losing_following_frames() -> Result<()> {
    use profiles::verification::{Cache, Detail, Pool, Status, Workload};
    let temp = tempfile::tempdir()?;
    let mut store = Store::open(temp.path(), 16_000_000)?;
    store.ingest(metadata())?;
    let mut invalid = span();
    if let Event::Span {
        stage,
        verification,
        ..
    } = &mut invalid
    {
        *stage = Stage::VerificationRequest;
        *verification = Some(Detail::Request {
            pool: Pool::Sapling,
            workload: Workload::default(),
            cache: Cache::Miss,
            status: Status::Success,
            primary_batch: Some(0),
            fallback_batch: None,
            fallback: false,
            partial: false,
        });
    }
    assert_eq!(
        store.ingest_batch(vec![event(1, invalid), event(2, finish())])?,
        1
    );
    assert_eq!(
        Reader::open(temp.path())?.detail(RUN, 1)?["summary"]["outcome"],
        "success"
    );
    Ok(())
}

#[test]
fn verification_metadata_fits_chunk_and_attempt_query_budgets() -> Result<()> {
    use profiles::verification::{Detail as VerificationDetail, Status, Workload};
    let verification = VerificationDetail::Batch {
        id: u64::MAX,
        workload: Workload {
            spends: u32::MAX,
            outputs: u32::MAX,
            actions: u32::MAX,
        },
        members: u32::MAX,
        profiled: u32::MAX,
        unprofiled: u32::MAX,
        sapling: u32::MAX,
        orchard: u32::MAX,
        ironwood: u32::MAX,
        flush_us: Some(u64::MAX),
        dispatch_us: Some(u64::MAX),
        worker_start_us: Some(u64::MAX),
        setup_end_us: Some(u64::MAX),
        execution_end_us: Some(u64::MAX),
        published_us: Some(u64::MAX),
        status: Status::Abandoned,
        partial: false,
    };
    // Use maximum-width values even where collector validation would reject them.
    let detail = Detail {
        run: RUN.into(),
        data: Event::Span {
            attempt: u64::MAX,
            span: u64::MAX,
            parent: u64::MAX,
            stage: Stage::VerificationBatch,
            transaction_index: Some(u32::MAX),
            transaction_hash: Some([255; 32]),
            start_us: u64::MAX,
            end_us: u64::MAX,
            completion_thread: Some(u64::MAX),
            verification: Some(verification),
        },
    };
    let encoded_size = serde_json::to_vec(&detail)?.len();
    assert!(u64::try_from((encoded_size + 1) * MAX_CHUNK_EVENTS + 2)? <= MAX_DECODE_BYTES);
    assert!(
        profiles::MAX_SPANS.div_ceil(u64::try_from(MAX_CHUNK_EVENTS)?)
            <= u64::try_from(MAX_DETAIL_CHUNKS)?
    );
    Ok(())
}

fn cpu_v2(samples: Vec<(u64, u32)>) -> crate::cpu::Capture {
    crate::cpu::Capture {
        run: RUN.into(),
        pid: 1,
        frequency: 99,
        clock: "monotonic".into(),
        schema_version: 2,
        session: "session".into(),
        start_mono_us: 1_000_000,
        end_mono_us: 2_000_000,
        executable_sha256: "a".repeat(64),
        lost_samples: Some(0),
        frames: vec![
            crate::cpu::CpuFrame {
                ip: "1234".into(),
                symbol: "long_symbol".repeat(500),
                dso: "zakurad".into(),
            },
            crate::cpu::CpuFrame {
                symbol: "root".into(),
                ..Default::default()
            },
        ],
        stacks: vec![vec![0, 1]],
        samples: samples
            .into_iter()
            .map(|(offset, tid)| crate::cpu::Sample {
                mono_us: 1_000_000 + offset,
                tid,
                stack: Some(0),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}
fn cpu_test_store(path: &Path) -> Result<Store> {
    let mut store = Store::open(path, 16_000_000)?;
    let mut frame = metadata();
    if let Frame::Run { run, .. } = &mut frame {
        run.monotonic_start_us = Some(1_000_000);
        run.clock_error_us = 2;
    }
    store.ingest(frame)?;
    store.ingest(event(1, finish()))?;
    store.ingest(event(2, span()))?;
    store.ingest(event(
        3,
        Event::Seal {
            attempt: 1,
            spans: 1,
            dropped: 0,
        },
    ))?;
    store.flush()?;
    Ok(store)
}
fn import_cpu(store: &Store, capture: &crate::cpu::Capture, id: &str) -> Result<()> {
    let path = store.path.join("inbox").join(format!("{id}.json"));
    fs::write(&path, serde_json::to_vec(capture)?)?;
    crate::cpu::import(&store.db, &store.path, &path)
}
#[test]
fn cpu_import_keeps_up_with_one_second_segments_and_a_short_backlog() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let mut store = cpu_test_store(temp.path())?;
    let mut capture = cpu_v2(vec![(200, 11)]);
    capture.schema_version = 3;
    capture.frequency = 999;
    capture.samples[0].cpu_period_ns = Some(1_001_001);
    // Ten fresh seconds plus ten seconds accumulated during a collector restart.
    for second in 0..20u64 {
        let mut segment = serde_json::to_value(&capture)?;
        segment["start_mono_us"] = json!(1_000_000 + second * 1_000_000);
        segment["end_mono_us"] = json!(2_000_000 + second * 1_000_000);
        segment["samples"][0]["mono_us"] = json!(1_000_200 + second * 1_000_000);
        fs::write(
            temp.path()
                .join("inbox")
                .join(format!("{second:032x}.json")),
            serde_json::to_vec(&segment)?,
        )?;
    }
    store.prune()?;
    let imported: i64 = store
        .db
        .query_row("SELECT count(*) FROM cpu", [], |r| r.get(0))?;
    assert_eq!(imported, 20);
    assert_eq!(fs::read_dir(temp.path().join("inbox"))?.count(), 0);
    Ok(())
}

#[test]
fn cpu_v3_preserves_periods_through_import_query_and_export() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let store = cpu_test_store(temp.path())?;
    let mut capture = cpu_v2(vec![(200, 11), (600000, 12), (750000, 11)]);
    capture.schema_version = 3;
    capture.frequency = 999;
    for (sample, ns) in capture
        .samples
        .iter_mut()
        .zip([1_000_000, 2_000_000, 4_000_000])
    {
        sample.cpu_period_ns = Some(ns);
    }
    import_cpu(&store, &capture, &"a".repeat(32))?;
    let reader = Reader::open(temp.path())?;
    let data = reader.cpu(RUN, 1, "recorded")?;
    assert_eq!(data["weight"]["estimated_cpu_ns"], 7_000_000);
    assert_eq!(data["samples"][2]["cpu_period_ns"], 4_000_000);
    assert_eq!(
        crate::cpu::speedscope(&data)?["profiles"][0]["endValue"],
        7.0
    );
    let verifier = reader.cpu(RUN, 1, "verifier")?;
    assert_eq!(verifier["weight"]["estimated_cpu_ns"], 3_000_000);
    capture.samples[0].cpu_period_ns = None;
    assert!(import_cpu(&store, &capture, &"b".repeat(32)).is_err());
    capture.samples[0].cpu_period_ns = Some(1_000_000_001);
    assert!(import_cpu(&store, &capture, &"c".repeat(32)).is_err());
    Ok(())
}

#[test]
fn cpu_v2_preserves_timestamps_tids_full_symbols_and_finalization_window() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let store = cpu_test_store(temp.path())?;
    import_cpu(
        &store,
        &cpu_v2(vec![(200, 11), (600000, 12), (750000, 11)]),
        &"a".repeat(32),
    )?;
    let reader = Reader::open(temp.path())?;
    let data = reader.cpu(RUN, 1, "recorded")?;
    assert_eq!(data["window"]["end_us"], 800000);
    assert_eq!(data["window"]["boundary_complete"], true);
    assert_eq!(data["samples"].as_array().unwrap().len(), 3);
    assert_eq!(data["samples"][2]["mono_us"], 1_750_000);
    assert_eq!(data["samples"][2]["at_us"], 749900);
    assert_eq!(data["samples"][1]["tid"], 12);
    assert_eq!(data["frames"][1]["symbol"], "long_symbol".repeat(500));
    assert_eq!(data["stacks"][0], json!([0, 1]));
    assert_eq!(
        data["coverage"]["state"], "partial",
        "acquisition bounds are not proof of gap-free sampling"
    );
    let verifier = reader.cpu(RUN, 1, "verifier")?;
    assert_eq!(verifier["window"]["end_us"], 700100);
    assert_eq!(verifier["counts"]["returned_samples"], 2);
    let export = reader.cpu_speedscope(RUN, 1, "recorded")?;
    assert_eq!(
        export["profiles"][0]["weights"],
        json!([1, 1, 1]),
        "long waits must never become sample CPU duration"
    );
    assert_eq!(export["profiles"].as_array().unwrap().len(), 3);
    assert_eq!(export["profiles"][0]["endValue"], 3);
    assert!(export["name"].as_str().unwrap().contains("partial capture"));
    assert_eq!(export["zakura"]["window"], data["window"]);
    assert_eq!(export["zakura"]["counts"], data["counts"]);
    assert_eq!(export["zakura"]["coverage"], data["coverage"]);
    Ok(())
}
#[test]
fn cpu_coverage_distinguishes_pending_gaps_empty_and_loss() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let store = cpu_test_store(temp.path())?;
    fs::write(
        temp.path().join("cpu-status.json"),
        serde_json::to_vec(
            &json!({"run":RUN,"state":"recording","updated_ms":now_ms(),"started_mono_us":1_000_000,"published_through_mono_us":1_000_000}),
        )?,
    )?;
    let reader = Reader::open(temp.path())?;
    assert_eq!(reader.detail(RUN, 1)?["cpu"]["status"], "pending");
    assert_eq!(
        reader.cpu(RUN, 1, "recorded")?["coverage"]["state"],
        "pending"
    );
    fs::remove_file(temp.path().join("cpu-status.json"))?;
    assert_eq!(
        reader.cpu(RUN, 1, "recorded")?["coverage"]["state"],
        "unavailable"
    );
    let mut empty = cpu_v2(vec![]);
    empty.end_mono_us = 1_100_000;
    empty.coverage_proven = true;
    import_cpu(&store, &empty, &"b".repeat(32))?;
    let data = reader.cpu(RUN, 1, "recorded")?;
    assert_eq!(data["coverage"]["state"], "partial");
    assert_eq!(data["counts"]["returned_samples"], 0);
    assert_eq!(data["sparse"], true);
    assert_eq!(data["coverage"]["gaps_us"], json!([[100000, 800000]]));
    let mut lost = cpu_v2(vec![]);
    lost.start_mono_us = 1_100_000;
    lost.truncated = true;
    lost.lost_samples = None;
    import_cpu(&store, &lost, &"c".repeat(32))?;
    let data = reader.cpu(RUN, 1, "recorded")?;
    assert_eq!(data["coverage"]["lost_samples"], Value::Null);
    assert_eq!(data["coverage"]["truncated"], true);
    assert_eq!(data["coverage"]["state"], "partial");
    Ok(())
}
#[test]
fn cpu_limits_are_explicit_and_more_than_two_hundred_stacks_survive() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let store = cpu_test_store(temp.path())?;
    let mut capture = cpu_v2(vec![]);
    capture.frames = (0..250)
        .map(|n| crate::cpu::CpuFrame {
            symbol: format!("function{n}"),
            ..Default::default()
        })
        .collect();
    capture.stacks = (0..250).map(|n| vec![n]).collect();
    capture.samples = (0..20_005)
        .map(|n| crate::cpu::Sample {
            mono_us: 1_001_000 + n,
            tid: 11,
            stack: Some(u32::try_from(n % 250).unwrap()),
            ..Default::default()
        })
        .collect();
    import_cpu(&store, &capture, &"d".repeat(32))?;
    let data = Reader::open(temp.path())?.cpu(RUN, 1, "recorded")?;
    assert_eq!(data["stacks"].as_array().unwrap().len(), 250);
    assert_eq!(data["counts"]["returned_samples"], 20000);
    assert_eq!(data["counts"]["omitted_samples"], 5);
    assert_eq!(data["counts"]["matching_samples"], 20005);
    assert_eq!(data["coverage"]["query_limited"], true);
    Ok(())
}

#[test]
fn cpu_repeated_long_frames_are_interned_and_expansion_is_bounded() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let store = cpu_test_store(temp.path())?;
    let mut capture = cpu_v2(vec![]);
    capture.frames = vec![crate::cpu::CpuFrame {
        symbol: "x".repeat(65_536),
        ..Default::default()
    }];
    capture.stacks = vec![vec![0; 128]];
    capture.samples = (0..50_000)
        .map(|n| crate::cpu::Sample {
            mono_us: 1_001_000 + n,
            tid: 1,
            stack: Some(0),
            ..Default::default()
        })
        .collect();
    import_cpu(&store, &capture, &"e".repeat(32))?;
    let data = Reader::open(temp.path())?.cpu(RUN, 1, "recorded")?;
    assert_eq!(data["frames"].as_array().unwrap().len(), 1);
    assert_eq!(data["frames"][0]["symbol"].as_str().unwrap().len(), 65_536);
    assert_eq!(data["stacks"].as_array().unwrap().len(), 1);
    assert_eq!(data["counts"]["returned_samples"], 3906);
    assert_eq!(data["counts"]["omitted_samples"], 46094);
    assert_eq!(data["counts"]["matching_samples"], 50000);
    assert_eq!(data["coverage"]["query_limited"], true);
    Ok(())
}

#[test]
fn cpu_late_window_retains_exact_maximum_duration_overlap() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let store = cpu_test_store(temp.path())?;
    let mut old = cpu_v2(vec![]);
    old.start_mono_us = 10_999_999;
    old.end_mono_us = 130_999_999;
    import_cpu(&store, &old, &"7".repeat(32))?;
    let mut edge = cpu_v2(vec![]);
    edge.start_mono_us = 11_000_000;
    edge.end_mono_us = 131_000_000;
    import_cpu(&store, &edge, &"8".repeat(32))?;
    let mut recent = cpu_v2(vec![(130_000_010, 7)]);
    recent.start_mono_us = 130_000_000;
    recent.end_mono_us = 131_000_100;
    import_cpu(&store, &recent, &"9".repeat(32))?;
    let availability =
        crate::cpu::availability(&store.db, temp.path(), RUN, 130_000_000, 130_000_100)?;
    assert_eq!(
        availability["captures"], 2,
        "exact 120-second lookback boundary remains inclusive"
    );
    let data = crate::cpu::window(&store.db, temp.path(), RUN, 130_000_000, 130_000_100)?;
    assert_eq!(data["coverage"]["captures"].as_array().unwrap().len(), 2);
    assert_eq!(data["counts"]["returned_samples"], 1);
    assert_eq!(data["samples"][0]["at_us"], 10);
    Ok(())
}

#[test]
fn cpu_pending_waits_for_boundary_neighbor_with_late_samples() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let store = cpu_test_store(temp.path())?;
    fs::write(
        temp.path().join("cpu-status.json"),
        serde_json::to_vec(&json!({
            "run":RUN,"state":"recording","updated_ms":now_ms(),"started_mono_us":1_000_000,
            "published_through_mono_us":1_900_000
        }))?,
    )?;
    let mut first = cpu_v2(vec![(200, 1)]);
    first.end_mono_us = 1_900_000;
    import_cpu(&store, &first, &"a".repeat(32))?;
    let reader = Reader::open(temp.path())?;
    let initial = reader.cpu(RUN, 1, "recorded")?;
    assert_eq!(initial["window"]["end_us"], 800000);
    assert_eq!(initial["counts"]["returned_samples"], 1);
    assert_eq!(
        initial["coverage"]["state"], "pending",
        "observed sealing end does not prove the neighboring segment was imported"
    );

    let mut neighbor = cpu_v2(vec![(770000, 2)]);
    neighbor.start_mono_us = 1_750_000;
    neighbor.end_mono_us = 2_000_000;
    import_cpu(&store, &neighbor, &"b".repeat(32))?;
    let updated = reader.cpu(RUN, 1, "recorded")?;
    assert_eq!(updated["counts"]["returned_samples"], 2);
    assert_eq!(updated["coverage"]["state"], "pending");

    let mut beyond = cpu_v2(vec![]);
    beyond.start_mono_us = 1_900_000;
    beyond.end_mono_us = 2_100_000;
    import_cpu(&store, &beyond, &"c".repeat(32))?;
    let settled = reader.cpu(RUN, 1, "recorded")?;
    assert_eq!(settled["counts"]["returned_samples"], 2);
    assert_eq!(settled["coverage"]["state"], "partial");
    assert_eq!(reader.detail(RUN, 1)?["cpu"]["pending"], false);
    Ok(())
}

#[test]
fn accounting_tolerates_published_or_pruned_entries_but_preserves_other_errors() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let directory = temp.path().join("raw");
    fs::create_dir(&directory)?;
    let temporary = directory.join("segment.json.tmp");
    fs::write(&temporary, b"capture metadata")?;
    let listed = fs::read_dir(&directory)?.next().unwrap()?;
    fs::rename(&temporary, directory.join("segment.json"))?;
    assert!(
        accounting_metadata(&listed.path())?.is_none(),
        "an atomically published temporary file no longer exists at its enumerated name"
    );
    assert!(directory_bytes(&directory)? > 0);

    let nested = directory.join("expired-session");
    fs::create_dir(&nested)?;
    assert!(accounting_metadata(&nested)?.unwrap().is_dir());
    fs::remove_dir(&nested)?;
    assert_eq!(
        directory_bytes(&nested)?,
        0,
        "pruning can remove a directory after its parent entry was examined"
    );

    let ordinary_file = directory.join("segment.json");
    assert!(
        directory_bytes(&ordinary_file).is_err(),
        "not-a-directory must not be treated as ordinary disappearance"
    );
    assert!(
        accounting_metadata(&ordinary_file.join("child")).is_err(),
        "other stat failures still surface"
    );
    Ok(())
}

#[test]
fn collector_accounting_survives_sampler_atomic_publication() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let mut store = Store::open(temp.path(), 16_000_000)?;
    let root = temp.path().to_path_buf();
    let worker = std::thread::spawn(move || -> std::io::Result<()> {
        for _ in 0..500 {
            let staged = root.join("cpu-status.json.tmp");
            let published = root.join("cpu-status.json");
            fs::write(&staged, b"{}")?;
            fs::rename(&staged, &published)?;
            fs::remove_file(&published)?;
        }
        Ok(())
    });
    for _ in 0..100 {
        store.prune()?;
    }
    worker
        .join()
        .map_err(|_| anyhow::anyhow!("sampler fixture panicked"))??;
    store.prune()?;
    Ok(())
}

#[test]
fn home_keeps_history_across_restarts_and_uses_latest_block_result() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let mut store = Store::open(temp.path(), 16_000_000)?;
    store.ingest(metadata())?;
    store.ingest(event(1, recording(1, 1, 100, 900_000)))?;
    store.ingest(event(2, recording(2, 2, 200, 600_000)))?;
    store.ingest(event(3, recording(3, 3, 300, 800_000)))?;
    store.db.execute(
        "UPDATE attempts SET utc_ms=? WHERE attempt=3",
        [integer(now_ms().saturating_sub(DAY_MS + 1000))?],
    )?;
    let next_run = "22222222222222222222222222222222";
    let mut next = metadata();
    if let Frame::Run { run, .. } = &mut next {
        run.id = next_run.into();
        run.utc_start_ms += 10_000;
    }
    store.ingest(next)?;
    for (sequence, data) in [
        (1, recording(1, 1, 100, 50_000)),
        (2, recording(2, 4, 200, 60_000)),
    ] {
        store.ingest(Frame::Event {
            schema: SCHEMA_VERSION,
            run_id: next_run.into(),
            sequence,
            data,
        })?;
    }
    let reader = Reader::open(temp.path())?;
    let home = reader.home(None, "semantic")?;
    assert_eq!(home["run"], next_run);
    let latest = home["latest"].as_array().context("latest rows")?;
    assert_eq!(latest.len(), 4);
    assert_eq!(latest[0]["run"], next_run);
    assert_eq!(
        latest
            .iter()
            .filter(|r| r["hash"] == "01".repeat(32))
            .count(),
        1
    );
    assert!(latest.iter().any(|r| r["run"] == RUN && r["attempt"] == 2));
    assert_eq!(home["outliers"].as_array().context("outliers")?.len(), 1);
    assert_eq!(home["outliers"][0]["run"], RUN);
    assert_eq!(home["outliers"][0]["attempt"], 2);
    assert_eq!(home["timing_blocks"], 3);
    assert_eq!(
        reader.home(Some(next_run), "semantic")?["latest"]
            .as_array()
            .context("run rows")?
            .len(),
        2
    );
    assert_eq!(reader.detail(RUN, 1)?["summary"]["end_us"], 900100);
    Ok(())
}
