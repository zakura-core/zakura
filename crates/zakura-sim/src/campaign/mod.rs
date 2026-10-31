//! Durable fuzz campaigns: versioned manifests, corpus snapshots, and the
//! start, resume and close commands (explorer implementation plan, E3a-2).
//!
//! A campaign lives in one store directory. The store is the campaign's
//! only authoritative copy, so the operator keeps it on durable storage and
//! copies it whole to move the campaign to another host. A CI cache is an
//! optional accelerator and never holds the only copy.
//!
//! ```text
//! <store>/
//!   store.lock                     held while `start` or `resume` writes a manifest
//!   builds/<binary sha256>/        a fuzzer binary and its build record
//!     fuzzer
//!     build.json
//!   snapshots/<index sha256>/      an immutable corpus snapshot
//!     snapshot.json                the index: every input's digest and length
//!     inputs/<sha256>              each input, named by its digest
//!   manifests/<sha256>.json        an immutable run manifest
//!   runs/<manifest sha256>/        one run's work
//!     lock                         held while a supervisor runs the run
//!     workers/<n>/corpus/          libFuzzer's corpus additions
//!     workers/<n>/artifacts/       libFuzzer's crash, timeout and OOM inputs
//!     workers/<n>/sim/             the target's saved inputs and traces
//!     workers/<n>/stats/           each fuzzer process's counts, `stats-<pid>.json`
//!     workers/<n>/attempts.jsonl   each fuzzer process the worker ran
//!     workers/<n>/outcome.json     why the worker stopped
//!     compact-artifacts/           the inputs the compaction's merge crashed on
//!     compact-sim/                 the compaction's saved inputs and traces
//!     end.json                     the run's one end record
//! ```
//!
//! Every file under a digest is written to a temporary name, synced, and
//! then linked or renamed into place, so an interrupted write never leaves
//! a partial file under a final name. No build, snapshot, manifest or end
//! record changes once written, and nothing deletes a worker's files: a run
//! reads its initial snapshot and writes a new one, so a failed or
//! interrupted run, merge or compaction leaves every earlier snapshot and
//! every failure intact.
//!
//! A run is one manifest. Its supervisor runs each worker's fuzzer
//! processes until the worker's budget is spent, restarting a process that
//! fails or exits before its time limit. When the workers stop, it saves the union of the initial snapshot
//! and every worker's corpus as a new snapshot, optionally compacts that
//! snapshot with libFuzzer's merge into another, and writes the end record.
//! Every run ends with exactly one end record, whose [`Status`] means one
//! thing:
//!
//! - [`Status::Complete`]: the supervisor ran every worker to the end of its
//!   budget.
//! - [`Status::WorkerStopped`]: the supervisor ran every worker until it
//!   stopped, and at least one stopped at its restart limit before its
//!   budget was spent.
//! - [`Status::Interrupted`]: the supervisor stopped before it wrote the end
//!   record. `close` or `resume` wrote it later, from what the workers left.
//!
//! The end record names the new snapshot and lists every failure file the
//! workers and the compaction left, by digest. `resume` checks both before
//! it trusts them, then writes a new manifest whose initial snapshot is the
//! end record's, and whose `resume_parent` is the run it continues. A run of another build,
//! generator, format or contract version can't resume: it starts a new
//! campaign segment with an explicit corpus migration, and the previous
//! segment's manifests and snapshots stay as they are.
//!
//! Each worker's fuzzer processes count their runs in the worker's
//! `stats/` directory. [`evidence()`] sums those counts per worker and for
//! the run, and lists every worker, process and file whose counts fall
//! short.

mod build;
mod evidence;
mod manifest;
mod run;
mod snapshot;
mod store;
mod verify;

pub use build::{build_fuzzer, BuildIdentity, FuzzBuild, FUZZ_PACKAGE, NIGHTLY};
pub use evidence::{evidence, Evidence, Incomplete, WorkerEvidence};
pub use manifest::{
    prepare_resume, prepare_start, Budget, FeedbackRecord, HarnessRecord, LimitsRecord, Manifest,
    Migration, ResumeOptions, SeedsRecord, StartOptions, WorkerPlan, SANITIZER_VARS,
};
pub use run::{
    close, read_end, Attempt, AttemptEnd, Compaction, End, FileRecord, Prepared, Recorder, Status,
    WorkerEnd, WorkerOutcome, ATTEMPTS_FILE, OUTCOME_FILE,
};
pub use snapshot::{Snapshot, SnapshotEntry, SnapshotIndex, Source};
pub use store::{CampaignError, Digest, RunState, Store, STORE_VERSION};
pub use verify::{verify, Verified};
