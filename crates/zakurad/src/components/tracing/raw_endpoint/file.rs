//! The filesystem side of the endpoint: trace file names, identity and reads.
//! Every function here blocks; callers run it on [`super::fs_threads::FsThreads`].

use std::{
    fs::{File, Metadata, OpenOptions},
    io::{self, Read, Seek, SeekFrom},
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::Path,
    time::UNIX_EPOCH,
};

use bytes::Bytes;
use serde::Serialize;
use tokio::sync::mpsc;

/// Bytes per body chunk. With a one-slot channel, a transfer holds at most
/// three chunks: one queued, one being read and one being written.
pub(super) const CHUNK_BYTES: usize = 256 * 1024;

/// The index lists at most this many tables, so its size stays bounded.
const MAX_LISTED_TABLES: usize = 1024;

const SUFFIX: &str = ".jsonl";

/// Whether `name` can name a table: 1 to 64 lowercase ASCII letters, digits or
/// underscores. This rule alone keeps request paths inside the trace directory.
pub(super) fn is_table_name(name: &str) -> bool {
    (1..=64).contains(&name.len())
        && name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

/// Why a table could not be opened.
#[derive(Debug)]
pub(super) enum OpenError {
    NotFound,
    /// A symlink, directory, FIFO, socket or device.
    NotRegular,
    Io(io::Error),
}

/// One table's JSONL file, open for reading.
pub(super) struct TraceFile {
    file: File,
    len: u64,
    etag: String,
}

impl TraceFile {
    /// Open `<directory>/<table>.jsonl`. The open never follows a symlink and
    /// never blocks on a FIFO.
    pub(super) fn open(directory: &Path, table: &str) -> Result<Self, OpenError> {
        debug_assert!(is_table_name(table), "callers validate table names");
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(directory.join(format!("{table}{SUFFIX}")))
            .map_err(|error| match error.kind() {
                io::ErrorKind::NotFound => OpenError::NotFound,
                // ELOOP: a symlink under O_NOFOLLOW. ENXIO: a Unix socket.
                _ if matches!(error.raw_os_error(), Some(libc::ELOOP | libc::ENXIO)) => {
                    OpenError::NotRegular
                }
                _ => OpenError::Io(error),
            })?;
        let metadata = file.metadata().map_err(OpenError::Io)?;
        if !metadata.is_file() {
            return Err(OpenError::NotRegular);
        }
        Ok(Self {
            file,
            len: metadata.len(),
            etag: etag(&metadata),
        })
    }

    /// The file's length when it was opened. Responses never extend past it.
    pub(super) fn len(&self) -> u64 {
        self.len
    }

    /// A strong entity tag naming this file, not its current length. The trace
    /// writer only appends, so bytes already read under one tag never change.
    pub(super) fn etag(&self) -> &str {
        &self.etag
    }

    /// Send bytes `start..end` as chunks. Stops early when the receiver is gone.
    /// Sends an error, which aborts the response, when the file ends before
    /// `end` because something truncated it.
    pub(super) fn send_range(
        mut self,
        start: u64,
        end: u64,
        chunks: &mpsc::Sender<io::Result<Bytes>>,
    ) {
        if let Err(error) = self.read_range(start, end, chunks) {
            let _ = chunks.blocking_send(Err(error));
        }
    }

    fn read_range(
        &mut self,
        start: u64,
        end: u64,
        chunks: &mpsc::Sender<io::Result<Bytes>>,
    ) -> io::Result<()> {
        self.file.seek(SeekFrom::Start(start))?;
        let mut remaining = end - start;
        while remaining > 0 {
            // The minimum is at most CHUNK_BYTES, so it fits in usize.
            let mut buffer = vec![0; remaining.min(CHUNK_BYTES as u64) as usize];
            let read = self.file.read(&mut buffer)?;
            if read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "trace file truncated during the response",
                ));
            }
            buffer.truncate(read);
            remaining -= read as u64; // `read` is at most the buffer length.
            if chunks.blocking_send(Ok(Bytes::from(buffer))).is_err() {
                return Ok(());
            }
        }
        Ok(())
    }
}

/// One entry of the table index.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub(super) struct TableEntry {
    pub(super) name: String,
    pub(super) bytes: u64,
    pub(super) etag: String,
}

/// List the regular `*.jsonl` files with table names, sorted by name. Symlinks
/// are skipped, and the list stops at [`MAX_LISTED_TABLES`] entries.
pub(super) fn list_tables(directory: &Path) -> io::Result<Vec<TableEntry>> {
    let mut tables = Vec::new();
    for entry in std::fs::read_dir(directory)? {
        let entry = entry?;
        let Some(name) = entry
            .file_name()
            .to_str()
            .and_then(|file_name| file_name.strip_suffix(SUFFIX))
            .filter(|name| is_table_name(name))
            .map(str::to_owned)
        else {
            continue;
        };
        // `DirEntry::metadata` does not follow symlinks.
        let metadata = entry.metadata()?;
        if !metadata.is_file() {
            continue;
        }
        tables.push(TableEntry {
            name,
            bytes: metadata.len(),
            etag: etag(&metadata),
        });
        if tables.len() == MAX_LISTED_TABLES {
            break;
        }
    }
    tables.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(tables)
}

/// Device, inode and, where the filesystem records it, birth time. The birth
/// time tells a recreated file apart from an old one that reused its inode.
fn etag(metadata: &Metadata) -> String {
    let born = metadata
        .created()
        .ok()
        .and_then(|created| created.duration_since(UNIX_EPOCH).ok())
        .map_or(0, |since| since.as_nanos());
    format!("\"{:x}-{:x}-{born:x}\"", metadata.dev(), metadata.ino())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read_all(file: TraceFile, start: u64, end: u64) -> io::Result<Vec<u8>> {
        let (chunks, mut receiver) = mpsc::channel(1);
        // The reader drops its sender when it returns, which ends the loop.
        std::thread::spawn(move || file.send_range(start, end, &chunks));
        let mut body = Vec::new();
        while let Some(chunk) = receiver.blocking_recv() {
            body.extend_from_slice(&chunk?);
        }
        Ok(body)
    }

    #[test]
    fn table_names_cannot_leave_the_directory() {
        for name in ["conn", "block_sync", "legacy_peer_request", "a1"] {
            assert!(is_table_name(name), "{name}");
        }
        for name in [
            "",
            "..",
            "../conn",
            "conn/x",
            "Conn",
            "conn.jsonl",
            "c-d",
            &"x".repeat(65),
        ] {
            assert!(!is_table_name(name), "{name}");
        }
    }

    #[test]
    fn ranges_preserve_exact_bytes_across_chunks() {
        let dir = tempfile::tempdir().unwrap();
        let data: Vec<u8> = (0..3 * CHUNK_BYTES + 7).map(|i| (i % 251) as u8).collect();
        std::fs::write(dir.path().join("conn.jsonl"), &data).unwrap();
        let open = || TraceFile::open(dir.path(), "conn").unwrap();
        assert_eq!(open().len(), data.len() as u64);
        assert_eq!(read_all(open(), 0, data.len() as u64).unwrap(), data);
        let (start, end) = (CHUNK_BYTES as u64 - 3, 2 * CHUNK_BYTES as u64 + 5);
        assert_eq!(
            read_all(open(), start, end).unwrap(),
            data[start as usize..end as usize]
        );
    }

    #[test]
    fn appends_keep_the_tag_and_replacement_changes_it() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("conn.jsonl");
        std::fs::write(&path, b"first\n").unwrap();
        let before = TraceFile::open(dir.path(), "conn").unwrap();
        let mut writer = OpenOptions::new().append(true).open(&path).unwrap();
        writer.write_all(b"second\n").unwrap();
        let after = TraceFile::open(dir.path(), "conn").unwrap();
        assert_eq!(before.etag(), after.etag());
        assert_eq!((before.len(), after.len()), (6, 13));
        // A file opened before an append still ends at its opening length.
        assert_eq!(read_all(before, 0, 6).unwrap(), b"first\n");

        std::fs::rename(&path, dir.path().join("old.jsonl")).unwrap();
        std::fs::write(&path, b"first\n").unwrap();
        assert_ne!(
            TraceFile::open(dir.path(), "conn").unwrap().etag(),
            after.etag()
        );
    }

    #[test]
    fn truncation_during_a_read_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("conn.jsonl");
        std::fs::write(&path, vec![b'x'; 2 * CHUNK_BYTES]).unwrap();
        let file = TraceFile::open(dir.path(), "conn").unwrap();
        File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(10)
            .unwrap();
        assert_eq!(
            read_all(file, 0, 2 * CHUNK_BYTES as u64)
                .unwrap_err()
                .kind(),
            io::ErrorKind::UnexpectedEof
        );
    }

    #[test]
    fn only_regular_files_open() {
        let dir = tempfile::tempdir().unwrap();
        assert!(matches!(
            TraceFile::open(dir.path(), "conn"),
            Err(OpenError::NotFound)
        ));
        std::fs::write(dir.path().join("secret"), b"secret\n").unwrap();
        std::os::unix::fs::symlink(dir.path().join("secret"), dir.path().join("link.jsonl"))
            .unwrap();
        std::fs::create_dir(dir.path().join("dir.jsonl")).unwrap();
        let _socket =
            std::os::unix::net::UnixListener::bind(dir.path().join("socket.jsonl")).unwrap();
        let made_fifo = std::process::Command::new("mkfifo")
            .arg(dir.path().join("fifo.jsonl"))
            .status()
            .unwrap();
        assert!(made_fifo.success());
        for table in ["link", "dir", "socket", "fifo"] {
            assert!(
                matches!(
                    TraceFile::open(dir.path(), table),
                    Err(OpenError::NotRegular)
                ),
                "{table}"
            );
        }
    }

    #[test]
    fn the_index_lists_only_regular_table_files() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("conn.jsonl"), b"row\n").unwrap();
        std::fs::write(dir.path().join("block_sync.jsonl"), b"").unwrap();
        std::fs::write(dir.path().join("Bad.jsonl"), b"row\n").unwrap();
        std::fs::write(dir.path().join("notes.txt"), b"row\n").unwrap();
        std::fs::create_dir(dir.path().join("dir.jsonl")).unwrap();
        std::os::unix::fs::symlink(dir.path().join("conn.jsonl"), dir.path().join("link.jsonl"))
            .unwrap();
        let tables = list_tables(dir.path()).unwrap();
        let names: Vec<_> = tables
            .iter()
            .map(|table| (table.name.as_str(), table.bytes))
            .collect();
        assert_eq!(names, [("block_sync", 0), ("conn", 4)]);
        assert_eq!(
            tables[1].etag,
            TraceFile::open(dir.path(), "conn").unwrap().etag()
        );
    }
}
