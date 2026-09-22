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
        start_us: 600000,
        end_us: 800000,
        completion_thread: None,
    }
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
                frames: vec!["synthetic function".into()],
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
