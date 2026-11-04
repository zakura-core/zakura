//! A bounded, nonblocking observation queue and a small rotating JSONL sink.

use std::{
    fs::{self, OpenOptions},
    io::{self, Write},
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc::{sync_channel, SyncSender},
        LazyLock,
    },
    time::{SystemTime, UNIX_EPOCH},
};

const SEGMENT_BYTES: u64 = 128 * 1024 * 1024;
const SEGMENTS: usize = 4;
static DROPPED: AtomicU64 = AtomicU64::new(0);
static SEQUENCE: AtomicU64 = AtomicU64::new(0);
static QUEUE: LazyLock<SyncSender<String>> = LazyLock::new(|| {
    let (sender, receiver) = sync_channel::<String>(4096);
    let _ = std::thread::Builder::new()
        .name("digest-perf-jsonl".into())
        .spawn(move || {
            let directory = std::env::var_os("ZAKURA_DIGEST_PERF_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("/var/log/zakura-digest-perf"));
            let mut writer = RotatingWriter::new(directory, SEGMENT_BYTES, SEGMENTS);
            for row in receiver {
                if writer.append(row.as_bytes()).is_err() {
                    DROPPED.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
    sender
});

pub(super) fn emit(event: &str, payload: String) {
    let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let at_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX))
        .unwrap_or(0);
    let row = format!("{{\"event\":\"{event}\",\"at_ns\":{at_ns},\"pid\":{},\"sequence\":{sequence},\"trace_drops\":{},\"payload\":{payload}}}\n", std::process::id(), DROPPED.load(Ordering::Relaxed));
    if QUEUE.try_send(row).is_err() {
        DROPPED.fetch_add(1, Ordering::Relaxed);
    }
}

struct RotatingWriter {
    directory: PathBuf,
    limit: u64,
    segments: usize,
}

impl RotatingWriter {
    fn new(directory: PathBuf, limit: u64, segments: usize) -> Self {
        Self {
            directory,
            limit,
            segments,
        }
    }

    fn append(&mut self, row: &[u8]) -> io::Result<()> {
        let bytes = u64::try_from(row.len()).unwrap_or(u64::MAX);
        if bytes > self.limit {
            return Err(io::Error::other("observation exceeds segment budget"));
        }
        fs::create_dir_all(&self.directory)?;
        let current = self.directory.join("events.jsonl");
        let size = fs::metadata(&current)
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        if size.saturating_add(bytes) > self.limit {
            let last = self
                .directory
                .join(format!("events.jsonl.{}", self.segments - 1));
            if last.exists() {
                fs::remove_file(last)?;
            }
            for index in (1..self.segments - 1).rev() {
                let source = self.directory.join(format!("events.jsonl.{index}"));
                if source.exists() {
                    fs::rename(
                        source,
                        self.directory.join(format!("events.jsonl.{}", index + 1)),
                    )?;
                }
            }
            if current.exists() {
                fs::rename(&current, self.directory.join("events.jsonl.1"))?;
            }
        }
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(current)?
            .write_all(row)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rotations_retain_complete_rows_within_the_trace_budget() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = RotatingWriter::new(dir.path().into(), 8, 3);
        for _ in 0..10 {
            writer.append(b"123\n").unwrap();
        }
        let files: Vec<_> = fs::read_dir(dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect();
        assert_eq!(files.len(), 3);
        assert!(
            files
                .iter()
                .map(|path| fs::metadata(path).unwrap().len())
                .sum::<u64>()
                <= 24
        );
        for file in files {
            assert!(fs::read(file).unwrap().ends_with(b"\n"));
        }
        assert!(writer.append(b"too large\n").is_err());
    }

    #[test]
    fn unavailable_sink_returns_an_error_instead_of_panicking() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("file");
        fs::write(&path, b"not a directory").unwrap();
        assert!(RotatingWriter::new(path, 8, 3).append(b"row\n").is_err());
    }
}
