//! Immutable corpus snapshots, named by the digest of their index.
//!
//! A snapshot is a directory `snapshots/<digest>/` with `snapshot.json`, its
//! index, and `inputs/<sha256>`, one file per input. The index lists every
//! input's digest and length in digest order, and `<digest>` is the SHA-256
//! of the index's bytes. So two snapshots of the same inputs share one name,
//! and [`Store::verify_snapshot`] can check a snapshot with nothing but its
//! name.

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use super::store::{
    partial_path, read_record, record_bytes, sync_dir, write_new_synced, CampaignError, Digest,
    Store, STORE_VERSION,
};

/// A snapshot's index file.
const INDEX_FILE: &str = "snapshot.json";

/// A snapshot's directory of inputs.
const INPUTS_DIR: &str = "inputs";

/// A snapshot's index: every input, in digest order.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotIndex {
    /// [`STORE_VERSION`].
    pub version: u32,
    /// Every input, by digest, in digest order, each once.
    pub inputs: Vec<SnapshotEntry>,
}

/// One input in a [`SnapshotIndex`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotEntry {
    /// The input's SHA-256, which names its file.
    pub sha256: Digest,
    /// Its length in bytes.
    pub len: u64,
}

/// A saved snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Snapshot {
    /// The digest of its index, which names it.
    pub digest: Digest,
    /// Its index.
    pub index: SnapshotIndex,
}

/// Where a new snapshot's inputs come from.
#[derive(Clone, Copy, Debug)]
pub enum Source<'a> {
    /// Every regular file in a directory, such as a libFuzzer corpus or a
    /// snapshot's inputs. A directory that doesn't exist holds nothing.
    /// Names that start with `.` are skipped, since they're temporary files.
    Dir(&'a Path),
    /// One input.
    Input(&'a [u8]),
}

impl Store {
    /// The directory that holds snapshot `digest`'s inputs, each named by its
    /// SHA-256, which a fuzzer can read as a corpus.
    pub fn snapshot_inputs(&self, digest: &Digest) -> PathBuf {
        self.snapshot_dir(digest).join(INPUTS_DIR)
    }

    /// Saves the union of `sources` as a snapshot, and returns it.
    ///
    /// Each input is read once, hashed, and written under its digest in a
    /// temporary directory, which is synced and then renamed to the
    /// snapshot's name. An interrupted save leaves only a temporary
    /// directory, which every reader skips, and no source changes. If a
    /// snapshot of the same inputs exists, it's kept and the copy dropped.
    pub fn save_snapshot(&self, sources: &[Source<'_>]) -> Result<Snapshot, CampaignError> {
        let snapshots = self.snapshots_dir();
        let partial = partial_path(&snapshots);
        let saved = self.save_into(&partial, sources);
        if saved.is_err() {
            // The save's error matters more than a failed cleanup.
            let _ = fs::remove_dir_all(&partial);
        }
        saved
    }

    /// [`Store::save_snapshot`], writing into the temporary directory
    /// `partial`.
    fn save_into(&self, partial: &Path, sources: &[Source<'_>]) -> Result<Snapshot, CampaignError> {
        let inputs = partial.join(INPUTS_DIR);
        fs::create_dir_all(&inputs).map_err(CampaignError::io(format!(
            "creating the snapshot directory {}",
            partial.display()
        )))?;
        let mut entries: BTreeMap<Digest, u64> = BTreeMap::new();
        let mut add = |bytes: &[u8]| -> Result<(), CampaignError> {
            let digest = Digest::of(bytes);
            if entries.contains_key(&digest) {
                return Ok(());
            }
            let path = inputs.join(digest.as_str());
            write_new_synced(&path, bytes)
                .map_err(CampaignError::io(format!("writing {}", path.display())))?;
            // A usize always fits in a u64 on supported targets.
            entries.insert(digest, bytes.len() as u64);
            Ok(())
        };
        for source in sources {
            match source {
                Source::Input(bytes) => add(bytes)?,
                Source::Dir(dir) => {
                    for path in files(dir)? {
                        let bytes = fs::read(&path)
                            .map_err(CampaignError::io(format!("reading {}", path.display())))?;
                        add(&bytes)?;
                    }
                }
            }
        }
        let index = SnapshotIndex {
            version: STORE_VERSION,
            inputs: entries
                .into_iter()
                .map(|(sha256, len)| SnapshotEntry { sha256, len })
                .collect(),
        };
        let bytes = record_bytes(&index);
        let digest = Digest::of(&bytes);
        let io = |what: &str| CampaignError::io(format!("{what} {}", partial.display()));
        write_new_synced(&partial.join(INDEX_FILE), &bytes).map_err(io("writing the index of"))?;
        sync_dir(&inputs).map_err(io("syncing"))?;
        sync_dir(partial).map_err(io("syncing"))?;
        let dir = self.snapshot_dir(&digest);
        if dir.exists() {
            // The same inputs are already saved under this name.
            fs::remove_dir_all(partial).map_err(io("removing"))?;
            return Ok(Snapshot { digest, index });
        }
        fs::rename(partial, &dir).map_err(io("renaming"))?;
        sync_dir(&self.snapshots_dir()).map_err(io("syncing the snapshots of"))?;
        Ok(Snapshot { digest, index })
    }

    /// Reads snapshot `digest` and checks it: the index's digest is the
    /// snapshot's name, the index is in digest order with each input once,
    /// every input file is there with the index's digest and length, and no
    /// other file is.
    pub fn verify_snapshot(&self, digest: &Digest) -> Result<Snapshot, CampaignError> {
        let dir = self.snapshot_dir(digest);
        let index_path = dir.join(INDEX_FILE);
        let index: SnapshotIndex = read_record(&index_path, Some(digest))?;
        if index.version != STORE_VERSION {
            return Err(CampaignError::corrupt(
                &index_path,
                format!(
                    "it's at version {}, and this build reads {STORE_VERSION}",
                    index.version
                ),
            ));
        }
        if !index
            .inputs
            .windows(2)
            .all(|pair| pair[0].sha256 < pair[1].sha256)
        {
            return Err(CampaignError::corrupt(
                &index_path,
                "its inputs aren't in digest order, each once",
            ));
        }
        let inputs = dir.join(INPUTS_DIR);
        let on_disk = files(&inputs)?;
        if on_disk.len() != index.inputs.len() {
            return Err(CampaignError::corrupt(
                &inputs,
                format!(
                    "it holds {} files, and the index lists {}",
                    on_disk.len(),
                    index.inputs.len()
                ),
            ));
        }
        for entry in &index.inputs {
            let path = inputs.join(entry.sha256.as_str());
            let bytes = fs::read(&path)
                .map_err(CampaignError::io(format!("reading {}", path.display())))?;
            let actual = Digest::of(&bytes);
            // A usize always fits in a u64 on supported targets.
            if actual != entry.sha256 || bytes.len() as u64 != entry.len {
                return Err(CampaignError::corrupt(
                    &path,
                    format!(
                        "the index lists {} bytes with digest {}, and the file has {} bytes \
                         with digest {actual}",
                        entry.len,
                        entry.sha256,
                        bytes.len()
                    ),
                ));
            }
        }
        Ok(Snapshot {
            digest: digest.clone(),
            index,
        })
    }
}

/// The regular files in `dir`, sorted, skipping names that start with `.`.
/// A directory that doesn't exist holds none.
fn files(dir: &Path) -> Result<Vec<PathBuf>, CampaignError> {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => {
            return Err(CampaignError::Io {
                context: format!("listing {}", dir.display()),
                error,
            })
        }
    };
    let mut paths = Vec::new();
    for entry in entries {
        let entry = entry.map_err(CampaignError::io(format!("listing {}", dir.display())))?;
        let hidden = entry.file_name().to_string_lossy().starts_with('.');
        let file = entry
            .file_type()
            .map_err(CampaignError::io(format!("listing {}", dir.display())))?
            .is_file();
        if file && !hidden {
            paths.push(entry.path());
        }
    }
    paths.sort();
    Ok(paths)
}
