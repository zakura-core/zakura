//! Storage bounds exercised through the same batch writer used by live peer connections.

use super::*;

const CURRENT: &str = "legacy_gossip.jsonl";
const PREVIOUS: &str = "legacy_gossip.prev.jsonl";

fn writer(path: &std::path::Path, limit: u64) -> TraceWriter {
    TraceWriter::with_legacy_gossip_limit(path.to_owned(), JsonlTraceConfig::default(), limit)
}

fn gossip(line: &[u8]) -> JsonlWriteEvent {
    JsonlWriteEvent {
        table: "legacy_gossip",
        file_name: CURRENT,
        line: line.to_vec(),
    }
}

async fn contents(path: &std::path::Path, name: &str) -> String {
    tokio::fs::read_to_string(path.join(name))
        .await
        .expect("retained trace is readable")
}

#[tokio::test]
async fn buffered_rows_and_newlines_rotate_at_the_limit() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = writer(dir.path(), 4);
    // These two JSON values fill the current file exactly, even before flushing.
    writer
        .write_batch(vec![gossip(b"1"), gossip(b"2")], false)
        .await;
    assert!(!dir.path().join(PREVIOUS).exists());
    assert_eq!(contents(dir.path(), CURRENT).await, "");
    writer.write_batch(vec![gossip(b"3")], true).await;
    assert_eq!(contents(dir.path(), PREVIOUS).await, "1\n2\n");
    assert_eq!(contents(dir.path(), CURRENT).await, "3\n");
}

#[tokio::test]
async fn one_batch_can_rotate_repeatedly_without_retaining_a_third_file() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = writer(dir.path(), 4);
    writer
        .write_batch(
            (1..=7).map(|n| gossip(n.to_string().as_bytes())).collect(),
            true,
        )
        .await;
    assert_eq!(contents(dir.path(), PREVIOUS).await, "5\n6\n");
    assert_eq!(contents(dir.path(), CURRENT).await, "7\n");
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 2);
}

#[tokio::test]
async fn reconnects_share_the_same_files_and_oversized_rows_are_dropped() {
    let dir = tempfile::tempdir().unwrap();
    let first = br#"{"connection_id":1}"#;
    let second = br#"{"connection_id":2}"#;
    let limit = u64::try_from(first.len() + 1).unwrap();
    let mut writer = writer(dir.path(), limit);
    writer
        .write_batch(
            vec![
                gossip(first),
                gossip(&vec![b'x'; first.len() + 1]),
                gossip(second),
            ],
            true,
        )
        .await;
    assert_eq!(
        contents(dir.path(), PREVIOUS).await,
        "{\"connection_id\":1}\n"
    );
    assert_eq!(
        contents(dir.path(), CURRENT).await,
        "{\"connection_id\":2}\n"
    );
}

#[tokio::test]
async fn restart_counts_existing_bytes_and_preserves_bounded_history() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join(PREVIOUS), "0\n").unwrap();
    std::fs::write(dir.path().join(CURRENT), "1\n").unwrap();
    let mut first = writer(dir.path(), 4);
    first.write_batch(vec![gossip(b"2")], true).await;
    assert_eq!(contents(dir.path(), PREVIOUS).await, "0\n");
    assert_eq!(contents(dir.path(), CURRENT).await, "1\n2\n");
    drop(first);
    let mut restarted = writer(dir.path(), 4);
    restarted.write_batch(vec![gossip(b"3")], true).await;
    assert_eq!(contents(dir.path(), PREVIOUS).await, "1\n2\n");
    assert_eq!(contents(dir.path(), CURRENT).await, "3\n");
}

#[tokio::test]
async fn startup_discards_oversized_files_before_accepting_rows() {
    let dir = tempfile::tempdir().unwrap();
    for name in [CURRENT, PREVIOUS] {
        std::fs::write(dir.path().join(name), "12345\n").unwrap();
    }
    let mut writer = writer(dir.path(), 4);
    writer.write_batch(vec![gossip(b"1")], true).await;
    assert_eq!(contents(dir.path(), CURRENT).await, "1\n");
    assert_eq!(contents(dir.path(), PREVIOUS).await, "");
}

#[tokio::test]
async fn external_truncation_can_only_make_rotation_happen_earlier() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = writer(dir.path(), 4);
    writer.write_batch(vec![gossip(b"1")], true).await;
    std::fs::OpenOptions::new()
        .write(true)
        .open(dir.path().join(CURRENT))
        .unwrap()
        .set_len(0)
        .unwrap();
    writer
        .write_batch(vec![gossip(b"2"), gossip(b"3")], true)
        .await;
    assert_eq!(contents(dir.path(), PREVIOUS).await, "2\n");
    assert_eq!(contents(dir.path(), CURRENT).await, "3\n");
}

fn other_table() -> JsonlWriteEvent {
    JsonlWriteEvent {
        table: "other",
        file_name: "other.jsonl",
        line: b"12345".to_vec(),
    }
}

#[tokio::test]
async fn failed_rotation_disables_only_gossip_and_discards_later_rows() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = writer(dir.path(), 2);
    writer.write_batch(vec![gossip(b"1")], false).await;
    // Replacing a directory with a file must fail on the real filesystem.
    std::fs::create_dir(dir.path().join(PREVIOUS)).unwrap();
    writer
        .write_batch(vec![gossip(b"2"), other_table()], true)
        .await;
    assert!(writer.disabled_tables.contains("legacy_gossip"));
    assert!(!writer.tables.contains_key("legacy_gossip"));
    writer.write_batch(vec![gossip(b"3")], true).await;
    assert_eq!(contents(dir.path(), CURRENT).await, "1\n");
    assert_eq!(contents(dir.path(), "other.jsonl").await, "12345\n");
}

#[tokio::test]
async fn failed_open_disables_only_gossip() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join(CURRENT)).unwrap();
    let mut writer = writer(dir.path(), 4);
    writer
        .write_batch(vec![gossip(b"1"), other_table()], true)
        .await;
    assert!(writer.disabled_tables.contains("legacy_gossip"));
    assert_eq!(contents(dir.path(), "other.jsonl").await, "12345\n");
}

#[tokio::test]
async fn failed_flush_disables_gossip_without_accepting_more_rows() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = writer(dir.path(), 2);
    writer.write_batch(vec![gossip(b"1")], false).await;
    // A real read-only handle injects a write failure without changing the write path.
    let readonly = std::fs::File::open(dir.path().join(CURRENT)).unwrap();
    writer.tables.get_mut("legacy_gossip").unwrap().file =
        Some(tokio::fs::File::from_std(readonly));
    writer
        .write_batch(vec![gossip(b"2"), other_table()], true)
        .await;
    writer.write_batch(vec![gossip(b"3")], true).await;
    assert!(writer.disabled_tables.contains("legacy_gossip"));
    assert_eq!(contents(dir.path(), CURRENT).await, "");
    assert_eq!(contents(dir.path(), "other.jsonl").await, "12345\n");
}

#[cfg(unix)]
#[tokio::test]
async fn failed_startup_metadata_disables_gossip() {
    let dir = tempfile::tempdir().unwrap();
    // A symlink loop gives a deterministic metadata error, including when tests run as root.
    std::os::unix::fs::symlink(PREVIOUS, dir.path().join(PREVIOUS)).unwrap();
    let mut writer = writer(dir.path(), 4);
    writer
        .write_batch(vec![gossip(b"1"), other_table()], true)
        .await;
    assert!(writer.disabled_tables.contains("legacy_gossip"));
    assert_eq!(contents(dir.path(), CURRENT).await, "");
    assert_eq!(contents(dir.path(), "other.jsonl").await, "12345\n");
}
