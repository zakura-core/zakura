//! Checking a whole store before trusting it, as after a copy to another
//! host.

use std::{collections::BTreeSet, fs};

use super::{
    run::{read_end, verify_end, Compaction},
    store::{CampaignError, Digest, RunState, Store},
};

/// What [`verify`] checked.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Verified {
    /// Manifests, each matching its digest.
    pub manifests: usize,
    /// Runs with an end record.
    pub ended: usize,
    /// Runs without an end record that no supervisor holds.
    pub open: usize,
    /// Runs a supervisor holds.
    pub running: usize,
    /// Snapshots, each with every input.
    pub snapshots: usize,
    /// Fuzzer builds, each binary matching its digest.
    pub builds: usize,
    /// Failure files the end records list, each matching its digest.
    pub failures: usize,
}

/// Checks every record in `store`: each manifest, snapshot index, input,
/// fuzzer binary and listed failure file matches its digest, each end
/// record matches its run's files, each run an end record names is in the
/// store, and each manifest's parent and build are too. Returns the first
/// problem it finds.
pub fn verify(store: &Store) -> Result<Verified, CampaignError> {
    let mut verified = Verified::default();
    let manifests = store.manifests()?;
    verified.manifests = manifests.len();
    let mut builds = BTreeSet::new();
    let mut named: BTreeSet<Digest> = BTreeSet::new();
    for (digest, manifest) in &manifests {
        for parent in [&manifest.resume_parent, &manifest.previous_segment]
            .into_iter()
            .flatten()
        {
            if !manifests.contains_key(parent) {
                return Err(CampaignError::corrupt(
                    &store.manifest_path(digest),
                    format!("it continues run {parent}, which the store doesn't hold"),
                ));
            }
        }
        builds.insert(manifest.build.binary.clone());
        named.insert(manifest.initial_snapshot.clone());
        match store.run_state(digest)? {
            RunState::Running => verified.running += 1,
            RunState::Open => verified.open += 1,
            RunState::Ended => {
                verified.ended += 1;
                let end = read_end(store, digest)?;
                named.insert(end.snapshot.clone());
                if let Some(Compaction::Compacted { snapshot, .. }) = &end.compaction {
                    named.insert(snapshot.clone());
                }
                verify_end(store, digest, manifest, &end)?;
                verified.failures += end.failures.len();
            }
        }
    }
    for binary in &builds {
        store.read_build(binary)?;
    }
    verified.builds = builds.len();
    let dir = store.snapshots_dir();
    let entries =
        fs::read_dir(&dir).map_err(CampaignError::io(format!("listing {}", dir.display())))?;
    let mut on_disk = BTreeSet::new();
    for entry in entries {
        let entry = entry.map_err(CampaignError::io(format!("listing {}", dir.display())))?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with('.') {
            continue;
        }
        let digest = name.parse::<Digest>().map_err(|_| {
            CampaignError::corrupt(&entry.path(), "it isn't named by a snapshot digest")
        })?;
        store.verify_snapshot(&digest)?;
        on_disk.insert(digest);
    }
    if let Some(missing) = named.difference(&on_disk).next() {
        return Err(CampaignError::corrupt(
            &store.snapshot_dir(missing),
            "a record names this snapshot, and the store doesn't hold it",
        ));
    }
    verified.snapshots = on_disk.len();
    Ok(verified)
}
