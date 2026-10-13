---
status: proposed
date: 2026-10-13
story: State database migrations keep breaking in ways that each fix only locally (#1366, #1384, #1385, #710, #715, #736, #1015, #1075, #1079, #681)
---

# Freeze the state directory at v30 and record database facts in the database

## Context and Problem Statement

Zakura inherited Zebra's state versioning. Each state database lives in `state/v{major}/{network}` (`config.rs` `db_path`), next to a `version` file that holds one semver string. The upgrade registry in `disk_format/upgrade.rs` lists 13 steps from 27.0.0 to 30.0.0. Five of the steps change rows, and eight are `NoMigration` entries. A major bump renames the previous directory into the new one on a read-write open (`DiskDb::try_reusing_previous_db_after_major_upgrade`).

Since #240, startup upgrades finish before the node serves requests, which closed one source of bugs. The remaining incidents come from two causes.

**The database records too little about itself.** The single version string stands in for five separate facts:

- The schema version.
- Which migrations ran. A downgrade rewrites the version file to the running version, so every later upgrade has to be safe to re-run.
- Whether older binaries may open the database. #1366 (v30) and #1062 (header-chain v5) bumped a version only to keep older releases out. The fence leaks: an older binary that finds no matching directory creates an empty database and resyncs (#1015).
- Which optional indexes exist. `+indexer` build metadata carries this, and upgrades have overwritten it (#623, #681).
- Which repairs happened. Startup infers missing VCT Sprout history from version 28.0.1 (`is_unrepairable_vct_database`), and the registry keeps tombstone entries for deleted steps (#533).

**The on-disk layout couples the directory, the major version, and compatibility.** Each major bump moves the database to a new directory, so every code path that locates the database must repeat the rename-or-search logic. Seven code paths locate a database or read its version:

1. The read-write open in `ZakuraDb::new`.
2. The read-only open in `ZakuraDb::new`, which `tip-height`, `audit-historical-treestates`, offline `zakura-checkpoints`, and the `zakura-rpc` follower use.
3. The panic-report metadata in `zakurad/src/application.rs`.
4. The version reads during format changes and shutdown.
5. `delete_old_databases`.
6. The header-chain `is_initialized` probe.
7. The header-chain `migrate_to_current` migration.

Each of these code paths has failed or still fails on its own:

- #1366 bumped the major version to 30. Read-only opens then failed on every un-upgraded v29 cache, which broke the Mainnet handoff canary. #1384 worked around the failure in CI, and #1385 fixed the read-only open.
- The panic metadata still reads only the current-major path, so it reports a new database for an un-upgraded cache.
- The rename runs before the RocksDB lock, so it can move a database that an older node still has open. #1075 and #1079 tried to fix this and were closed unmerged.
- `delete_old_databases` never deletes `v(r-1)` for any reusable major `r`, so it keeps `v26`–`v29` forever.
- The header-chain store has its own version number (`HeaderChainDiskVersion::CURRENT = 5`) and its own decoders. Its startup probe only understood the current format, so the existing migrator never ran on older stores. Mainnet nodes crash-looped three times as a result (#710, #715, #736).
- Major numbers are a global namespace, so feature branches collide on them (#1028, #795).

Zakura has bumped the major version three times in about three months (28, 29, 30). Every Zakura release wrote format 28.0.0 or later: v1.0.0 wrote 28.0.0, and v1.6.0 writes 29.1.0. No release has shipped format 30 yet. Zakura does not import Zebra databases (#720).

## Priorities & Constraints

- Mainnet nodes must keep their synced databases. A plan that forces every node to resync is out of scope.
- Format changes must not need a new directory, a rename, or per-call-site handling.
- An incompatible change must stop older binaries reliably, instead of letting them resync silently.
- The database must say what it contains, so startup does not infer facts from version numbers.
- Each step must ship on its own and leave main releasable.

## Considered Options

- Option 1: Keep the current scheme, and fix each call site as it fails.
- Option 2: Freeze the directory at `v30`, record database facts in a manifest stored in the database, and delete the inherited machinery that no Zakura database needs.
- Option 3: Move to a version-free directory such as `state/{network}` with a manifest.

### Pros and Cons of the Options

#### Option 1: Keep the current scheme

- Good, because it needs no design work now.
- Bad, because every major bump repeats the #1385 class of failure at whichever of the seven code paths missed the rename.
- Bad, because it keeps the leaky fence, the `+indexer` metadata, and the version-number inference.

#### Option 2: Freeze at v30 and add a manifest

- Good, because v29→v30 becomes the last directory move. Nodes that already run v30 never move again.
- Good, because one manifest replaces the fence bumps, `+indexer`, tombstones, and version-number inference with explicit facts.
- Good, because each phase below removes code.
- Bad, because binaries released before the manifest cannot read it. The fence therefore only holds if the first v30 release already checks the manifest.

#### Option 3: Version-free directory

- Good, because the path no longer contains a version.
- Bad, because it costs one more rename on every node and gives nothing beyond Option 2.

## Decision Outcome

Chosen option: Option 2.

The work ships in phases. Phases 1 and 2 should land before the first release with format 30. If they do, every binary that can open `v30` also enforces the manifest, and the `v29` directory boundary fences off every older release.

### Phase 1: one resolver, frozen directory

- Add one resolver that returns the database path, the directory major version, the on-disk version, and whether a reuse move is pending. Route all seven code paths through it.
- Take the previous database's RocksDB lock before the v29→v30 rename, so a running older node keeps its directory (#1075).
- Add a test that fails if the registry gains an `x.0.0` step above 30. From now on, every format change is a minor or patch step.
- Make `delete_old_databases` delete stale majors below `v30` once the current database exists.

### Phase 2: database manifest

Store a manifest row in the state database, written in the same RocksDB batch as each migration's final write. The manifest records:

- the schema version and the IDs of applied migrations;
- `min_reader_version`: an older binary refuses to open the database instead of resyncing, which replaces fence bumps;
- the optional indexes that exist, which replaces `+indexer`;
- the storage mode and pruning facts;
- the VCT and Sprout-history facts, which replace the version-28.0.1 inference;
- the binary that last wrote the database (#415).

The `version` file stays for one release so that older tooling keeps working. After that release, the code derives the file from the manifest.

### Phase 3: upgrade floor and dead-code removal

- Delete registry steps at or below 28.0.0: `block_info_and_address_received`, `add_ironwood_tree`, and the 27.x `NoMigration` entries. No Zakura database is older than 28.0.0, so no node loses anything.
- Delete the Zebra version-file fallbacks: "minor.patch" files, and a missing file treated as `major.0.0`.
- Limit the reuse search to majors at or above the floor.
- Reject databases below the floor with `FormatChangeError::ResyncRequired`.
- Raise the floor to 29.0.0 once supported releases no longer write 28.x. That raise deletes the 28.x steps and tombstones.

### Phase 4: downgrades and the header-chain version

- Replace the `Downgrade` arm with a refusal unless the manifest's `min_reader_version` allows the running binary (#1015). Upgrades then no longer need to be safe to re-run.
- Track the header-chain format in the same registry and manifest. Classify it once in `ZakuraDb::new`, before any reader decodes it. This removes the probe-before-migrate failure behind #710, #715, and #736.

### Phase 5: smaller cleanups

- Spawn the periodic format-check thread only when `debug_validity_check_interval` is set.
- Measure the detailed validity check that every startup and shutdown runs on Mainnet. Then decide whether it must block both.

### Expected Consequences

- A format change becomes a minor step plus, when needed, a manifest flag or a higher `min_reader_version`. No directory moves, no renames, and no per-call-site handling.
- An incompatible change stops older binaries with an explicit error instead of a silent resync.
- Nodes more than the floor behind must restore a snapshot or resync.
- Ad-hoc rollbacks within a format family work only when the manifest allows the older binary. Today they mostly work by luck.
- The upgrade machinery shrinks. Today it is about 3,370 non-test lines in `upgrade.rs` and `upgrade/`, plus about 500 lines of directory and version-file code and the 972-line header-chain migrator.

## More Information

- #1385 fixes read-only opens of un-upgraded previous-major caches. That fix stays in place until Phase 1 replaces it with the shared resolver.
- Upstream Zebra keeps the directory-per-major layout and runs format upgrades on a background thread. Zakura changed the threading in #240, and this plan changes the layout.
- Open questions for review:
  - Which column family holds the manifest?
  - Should the floor rise to 29.0.0 in the same release as Phase 3?
  - Must the detailed validity check block every startup?
