//! Durable storage for the last self-record sequence this node authored.
//!
//! Peers keep the record with the highest sequence. A node that seeds its
//! sequence only from the wall clock can author a lower sequence after a
//! backward clock step, and peers then keep its old record until it expires.
//! Storing each authored sequence before publishing it lets a restart always
//! author a newer one.

use std::{io, path::PathBuf};

use tracing::warn;
use zakura_chain::common::atomic_write;

/// The file that holds this node's last authored self-record sequence.
#[derive(Clone, Debug)]
pub(crate) struct AuthoredSequenceFile {
    path: PathBuf,
}

impl AuthoredSequenceFile {
    pub(crate) fn new(path: PathBuf) -> Self {
        Self { path }
    }

    /// The stored sequence, or `None` when no sequence was stored. An
    /// unreadable or unparsable file is logged and reads as `None`, so
    /// authoring falls back to the wall clock.
    pub(crate) fn load(&self) -> Option<u64> {
        let contents = match std::fs::read_to_string(&self.path) {
            Ok(contents) => contents,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return None,
            Err(error) => {
                warn!(
                    ?error,
                    path = ?self.path,
                    "could not read the discovery self-record sequence; seeding from the clock"
                );
                return None;
            }
        };
        match contents.trim().parse() {
            Ok(sequence) => Some(sequence),
            Err(error) => {
                warn!(
                    ?error,
                    path = ?self.path,
                    "ignoring an unparsable discovery self-record sequence; seeding from the clock"
                );
                None
            }
        }
    }

    /// Atomically replace the stored sequence with `sequence`.
    pub(crate) fn store(&self, sequence: u64) -> io::Result<()> {
        atomic_write(self.path.clone(), sequence.to_string().as_bytes())?.map(|_| ())
    }

    /// Store `sequence`, logging a failure. Authoring continues without a
    /// stored sequence, as it did before this file existed.
    pub(crate) fn store_or_warn(&self, sequence: u64) {
        if let Err(error) = self.store(sequence) {
            warn!(
                ?error,
                path = ?self.path,
                sequence,
                "could not store the discovery self-record sequence; a restart after a \
                 backward clock step may author an older record"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_stored_sequence_loads_back() {
        let dir = tempfile::tempdir().unwrap();
        let file = AuthoredSequenceFile::new(dir.path().join("sequence"));
        assert_eq!(file.load(), None, "a missing file stores no sequence");
        file.store(u64::MAX - 1).unwrap();
        assert_eq!(file.load(), Some(u64::MAX - 1));
        file.store(7).unwrap();
        assert_eq!(file.load(), Some(7));
    }

    #[test]
    fn an_unparsable_file_reads_as_no_sequence() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sequence");
        std::fs::write(&path, "not a sequence").unwrap();
        assert_eq!(AuthoredSequenceFile::new(path).load(), None);
    }
}
