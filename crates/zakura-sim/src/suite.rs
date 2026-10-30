//! The full-suite manifest (campaign.md, C1): every row the full property
//! suite runs, with its command, timeout and one expected outcome.
//!
//! `qa/suite.toml` names what the harness registry can't: which packages'
//! tests the suite runs, which ignored tests it runs on demand, which fuzz
//! targets it runs, and which rows are planned but unimplemented. Each
//! simulator fuzz target names its harness in its package's
//! `[package.metadata.zakura-sim.targets]` table. [`manifest`] joins
//! `qa/suite.toml` with [`HARNESSES`], each harness's properties, the
//! coverage manifest, the retained planted cases, the committed seeds, and
//! the test and fuzz targets `cargo metadata` reports, with their sources.
//! It returns the manifest, or every problem that keeps the manifest from
//! covering the suite:
//!
//! - a harness that no target runs and no `missing-target` row names;
//! - a fuzz target that `qa/suite.toml` neither maps nor excludes;
//! - a simulator fuzz target that its package's table maps to no harness;
//! - a simulator-only test target that a package's `tests` list leaves out;
//! - a simulator package whose library row runs and has no unit tests;
//! - an ignored test that `qa/suite.toml` doesn't list, or an attribute
//!   that may ignore a test in a form the scan can't classify, such as
//!   `#[cfg_attr(zakura_sim, ignore)]`;
//! - a target that covers a harness its own harness doesn't compose;
//! - a reported property without a coverage row, or an on-demand witness
//!   that the suite doesn't run;
//! - a planted case whose property no harness reports;
//! - a required seed that fails validation;
//! - a pending row that a harness now registers;
//! - a row that names a package, test, target or harness that doesn't
//!   exist.
//!
//! A new harness, property, fuzz target, ignored test, planted case or seed
//! joins the manifest by itself or fails the check, so the suite can't
//! shrink without an edit to `qa/suite.toml`.
//!
//! Each row's `expect` is its one expected outcome. A command's
//! `{evidence}` stands for the row's own evidence directory, which the
//! runner creates. Commands run from the repository's root.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt, fs, io,
    path::{Path, PathBuf},
    process::Command,
};

use serde::{Deserialize, Serialize};

use crate::{
    harness::{
        coverage::{Bypass, Ci, Witness, COVERAGE},
        HarnessEntry, HARNESSES,
    },
    property::Status,
    record::{CONTRACT_VERSION, FORMAT_VERSION},
    seed::{self, sha256},
    trace::BuildInfo,
};

/// The suite definition, relative to the repository's root.
pub const SUITE_FILE: &str = "qa/suite.toml";

/// The suite definition's schema, and the manifest's.
pub const SCHEMA: u32 = 1;

/// [`NIGHTLY`], as a literal for `concat!`.
macro_rules! nightly {
    () => {
        "nightly-2026-07-15"
    };
}

/// The nightly toolchain that builds the fuzz targets and the ASan lane,
/// as `header-fuzz.yml` and `tests-unit.yml` pin it.
pub const NIGHTLY: &str = nightly!();

/// Where the fuzz packages live, relative to the repository's root.
pub const FUZZ_DIR: &str = "qa/fuzz";

/// The fuzz package under [`FUZZ_DIR`] whose targets run harnesses in the
/// simulator.
pub const SIM_FUZZ_PACKAGE: &str = "sim";

/// Where the planted cases live, relative to the repository's root.
pub const PLANTED_DIR: &str = "qa/planted";

/// Where the committed seeds live, relative to the repository's root.
pub const FIXTURES_DIR: &str = "crates/zakura-sim/tests/fixtures";

/// What a command's `{evidence}` stands for.
pub const EVIDENCE: &str = "{evidence}";

/// `qa/suite.toml`.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct SuiteFile {
    /// [`SCHEMA`].
    pub schema: u32,
    /// The timeout of each planted case's row.
    pub planted_timeout_secs: u64,
    /// The timeout of each seed set's and seed witness's row.
    pub seed_timeout_secs: u64,
    /// The packages whose tests the simulator build runs.
    #[serde(default)]
    pub sim_package: Vec<SimPackage>,
    /// The packages whose ordinary-build property suites run.
    #[serde(default)]
    pub property_package: Vec<PropertyPackage>,
    /// Every ignored test in the scanned sources: run on demand, or
    /// excluded with a reason.
    #[serde(default)]
    pub ignored: Vec<Ignored>,
    /// Commands the package rows don't cover, such as doc tests and the
    /// sanitizer builds.
    #[serde(default)]
    pub lane: Vec<Lane>,
    /// The fuzz targets the suite runs, and the harnesses each one runs.
    #[serde(default)]
    pub target: Vec<Target>,
    /// The fuzz targets the suite leaves out, each with its reason.
    #[serde(default)]
    pub excluded_target: Vec<ExcludedTarget>,
    /// The harnesses no target runs yet.
    #[serde(default)]
    pub missing_target: Vec<MissingTarget>,
    /// The planned rows nothing implements yet.
    #[serde(default)]
    pub pending: Vec<Pending>,
}

/// A package whose tests the simulator build runs.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct SimPackage {
    /// The package, such as `zakura-sim`. Its directory is
    /// `crates/<name>`.
    pub name: String,
    /// Whether its library's unit tests run. `true` by default.
    #[serde(default = "yes")]
    pub lib: bool,
    /// The test targets that run. Every test target `cargo metadata`
    /// reports when absent. When present, it must list every test target
    /// whose root file starts with `#![cfg(zakura_sim)]`.
    pub tests: Option<Vec<String>>,
    /// The timeout of each of its rows.
    pub timeout_secs: u64,
}

/// A package whose ordinary-build property suites run: each source file
/// with a `proptest!` block, or a call of one of `suite_macros`, becomes
/// one row ([`is_property_suite`]).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct PropertyPackage {
    /// The package. Its directory is `crates/<name>`.
    pub name: String,
    /// Macros that expand to a property suite, such as
    /// `stream_conformance_suite`. The file that defines one isn't a suite
    /// itself.
    #[serde(default)]
    pub suite_macros: Vec<String>,
    /// The package features its suites need, such as `zakura-testkit`.
    #[serde(default)]
    pub features: Vec<String>,
    /// The timeout of each of its rows.
    pub timeout_secs: u64,
}

/// An ignored test.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct Ignored {
    /// The test's file, relative to the repository's root.
    pub file: String,
    /// The test's name.
    pub test: String,
    /// Whether the suite runs it, with `--ignored` and the `planted`
    /// profile.
    pub run: bool,
    /// The timeout of its row, if it runs.
    pub timeout_secs: Option<u64>,
    /// Why the suite leaves it out, if it doesn't run.
    pub reason: Option<String>,
}

/// A command the package rows don't cover.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct Lane {
    /// The row's ID, after `lane/`.
    pub id: String,
    /// The build it runs in.
    pub build: Build,
    /// The arguments after `cargo`.
    pub args: String,
    /// The timeout of its row.
    pub timeout_secs: u64,
}

/// A fuzz target the suite runs. A simulator target's harness comes from
/// its package's `[package.metadata.zakura-sim.targets]` table.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct Target {
    /// The fuzz package's directory under `qa/fuzz/`, such as `sim`.
    pub package: String,
    /// The target's `[[bin]]` name.
    pub name: String,
    /// Harnesses whose runs it covers through its harness, such as the race
    /// fixtures through `fixtures/all`. Its harness must compose each
    /// covered harness ([`HarnessEntry::composes`]) and check its
    /// properties, and its report must account for each.
    #[serde(default)]
    pub covers: Vec<String>,
    /// The smoke run's length.
    pub smoke_secs: u64,
    /// More libFuzzer arguments, such as `-max_len=512`.
    #[serde(default)]
    pub args: Vec<String>,
    /// Environment variables the run needs, as `NAME=value`, such as
    /// `ASAN_OPTIONS=detect_leaks=0`.
    #[serde(default)]
    pub env: Vec<String>,
    /// The timeout of its row, build included.
    pub timeout_secs: u64,
}

/// A fuzz target the suite leaves out.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct ExcludedTarget {
    /// The fuzz package's directory under `qa/fuzz/`.
    pub package: String,
    /// The target's `[[bin]]` name.
    pub name: String,
    /// Why.
    pub reason: String,
}

/// A harness no target runs yet.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct MissingTarget {
    /// The harness.
    pub harness: String,
    /// The chunk that adds its target.
    pub owner: String,
    /// What the target waits for.
    pub reason: String,
}

/// A planned row nothing implements yet.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct Pending {
    /// The planned property, harness or feature ID.
    pub id: String,
    /// What it is.
    pub kind: PendingKind,
    /// The chunk that implements it, such as `H1`.
    pub owner: String,
    /// The plan that defines it.
    pub plan: String,
}

/// What a [`Pending`] row plans.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PendingKind {
    /// A property ID, which no harness may register yet.
    Property,
    /// A harness name, which [`HARNESSES`] may not hold yet.
    Harness,
    /// A protocol-state feature, which a generator or collector adds.
    Feature,
}

/// The build a row runs in. Each has its own target directory.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Build {
    /// The ordinary build.
    Ordinary,
    /// The simulator build, `--cfg zakura_sim`, as CI's `sim-build` job.
    Sim,
    /// The simulator build's doc tests, which need the flag in
    /// `RUSTDOCFLAGS` too.
    SimDoc,
    /// The simulator build with the `planted` profile, for slow runs.
    SimPlanted,
    /// The simulator build in release mode.
    SimRelease,
    /// The simulator build under AddressSanitizer, on [`NIGHTLY`].
    SimAsan,
    /// `cargo xtask`, with the simulator flag, as CI runs `planted`.
    Xtask,
    /// A fuzz package outside the simulator, on [`NIGHTLY`].
    Fuzz,
    /// The simulator fuzz package, on [`NIGHTLY`].
    FuzzSim,
}

impl Build {
    /// The command line up to and including `cargo` and its toolchain.
    pub fn cargo(self) -> &'static str {
        match self {
            Build::Ordinary => "CARGO_TARGET_DIR=target/suite cargo",
            Build::Sim | Build::SimPlanted => {
                "RUSTFLAGS=\"--cfg zakura_sim\" CARGO_TARGET_DIR=target/sim cargo"
            }
            Build::SimDoc => {
                "RUSTFLAGS=\"--cfg zakura_sim\" RUSTDOCFLAGS=\"--cfg zakura_sim\" \
                 CARGO_TARGET_DIR=target/sim cargo"
            }
            Build::SimRelease => {
                "RUSTFLAGS=\"--cfg zakura_sim\" CARGO_TARGET_DIR=target/sim-release cargo"
            }
            Build::SimAsan => concat!(
                "RUSTFLAGS=\"--cfg zakura_sim -Zsanitizer=address\" ",
                "CARGO_TARGET_DIR=target/sim-asan ",
                "LSAN_OPTIONS=suppressions=.github/lsan/zakura-sim.supp ",
                "cargo +",
                nightly!()
            ),
            Build::Xtask => "RUSTFLAGS=\"--cfg zakura_sim\" cargo",
            Build::Fuzz => concat!("cargo +", nightly!()),
            Build::FuzzSim => concat!("RUSTFLAGS=\"--cfg zakura_sim\" cargo +", nightly!()),
        }
    }

    /// `cargo <subcommand> <args>` in this build: the `planted` profile
    /// goes after the subcommand.
    fn command(self, subcommand: &str, args: &str) -> String {
        let profile = match self {
            Build::SimPlanted => " --profile planted",
            _ => "",
        };
        format!("{} {subcommand}{profile} {args}", self.cargo())
    }
}

/// `true`, for serde defaults.
fn yes() -> bool {
    true
}

/// A row's one expected outcome. The C4 report gives each row its actual
/// outcome beside it, and lists every row that didn't reach it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Expect {
    /// The command exits 0 within the timeout. A test command runs at least
    /// one test and fails none. A row with `decided_by` passes when that
    /// row's run reports its test as passed.
    Pass,
    /// No run in the suite violates the `Active` property, and each of its
    /// witness rows passes.
    Holds,
    /// The harness plants a bug that breaks the `Active` property
    /// ([`HarnessEntry::planted`]), so some run in the suite violates it,
    /// and each of its witness rows passes. A suite with no violation
    /// missed the planted bug.
    Violated,
    /// The suite archives and reports each violation of the `Known` or
    /// `Draft` property without failing, and each of its witness rows
    /// passes.
    Reported,
    /// Every row of the harness reaches its own expected outcome.
    Complete,
    /// Nothing implements the row yet. The report lists it as
    /// unimplemented.
    Unimplemented,
    /// The suite doesn't run it, for the row's reason. The report lists it
    /// as excluded.
    Excluded,
}

/// The manifest: provenance, then every row by kind.
#[derive(Clone, Debug, Serialize)]
pub struct Manifest {
    /// [`SCHEMA`].
    pub schema: u32,
    /// The generating binary's provenance: commit, dirty flag, source and
    /// lockfile digests, compiler and flags.
    pub build: ManifestBuild,
    /// The SHA-256 of `qa/suite.toml`.
    pub suite_sha256: String,
    /// The SHA-256 of each fuzz package's `Cargo.lock`, by package.
    pub fuzz_lockfiles: BTreeMap<String, String>,
    /// The trace format version.
    pub format: u32,
    /// The trace contract version.
    pub contract: u32,
    /// [`NIGHTLY`].
    pub nightly: &'static str,
    /// How many rows of each kind, and properties by status.
    pub counts: Counts,
    /// One row per registered harness.
    pub harnesses: Vec<HarnessRow>,
    /// One row per property a harness reports.
    pub properties: Vec<PropertyRow>,
    /// One row per test command.
    pub tests: Vec<TestRow>,
    /// One row per coverage witness: each property's passing test and its
    /// bypass.
    pub witnesses: Vec<WitnessRow>,
    /// One row per property with retained planted cases.
    pub planted: Vec<PlantedRow>,
    /// One row per harness's committed seed set.
    pub seed_sets: Vec<SeedSetRow>,
    /// One row per witness a committed seed carries.
    pub seed_witnesses: Vec<SeedWitnessRow>,
    /// One row per fuzz target the suite runs.
    pub targets: Vec<TargetRow>,
    /// One row per missing target and planned property, harness or
    /// feature.
    pub pending: Vec<PendingRow>,
    /// One row per ignored test or fuzz target the suite leaves out.
    pub excluded: Vec<ExcludedRow>,
    /// `zakura-sim seeds --report`'s JSON for every committed seed.
    pub seed_report: seed::Report,
}

/// [`BuildInfo`] without `binary`, the generating executable's digest. That
/// digest changes with the checkout's path and with the build that runs
/// `suite`, so two runs at one commit would write different manifests.
#[derive(Clone, Debug, Serialize)]
pub struct ManifestBuild {
    /// [`BuildInfo::git`].
    pub git: String,
    /// [`BuildInfo::dirty`].
    pub dirty: bool,
    /// [`BuildInfo::source`].
    pub source: String,
    /// [`BuildInfo::lockfile`].
    pub lockfile: String,
    /// [`BuildInfo::rustc`].
    pub rustc: String,
    /// [`BuildInfo::target`].
    pub target: String,
    /// [`BuildInfo::cfg`].
    pub cfg: Vec<String>,
    /// [`BuildInfo::features`].
    pub features: Vec<String>,
    /// [`BuildInfo::opt_level`].
    pub opt_level: String,
    /// [`BuildInfo::sanitizer`].
    pub sanitizer: Option<String>,
}

impl From<BuildInfo> for ManifestBuild {
    fn from(build: BuildInfo) -> Self {
        ManifestBuild {
            git: build.git,
            dirty: build.dirty,
            source: build.source,
            lockfile: build.lockfile,
            rustc: build.rustc,
            target: build.target,
            cfg: build.cfg,
            features: build.features,
            opt_level: build.opt_level,
            sanitizer: build.sanitizer,
        }
    }
}

/// How many rows of each kind.
#[derive(Clone, Debug, Default, Serialize)]
pub struct Counts {
    /// Harness rows.
    pub harnesses: usize,
    /// Distinct property IDs, by status name.
    pub property_ids: BTreeMap<String, usize>,
    /// Property rows: one per harness and property.
    pub properties: usize,
    /// Test rows, by kind name.
    pub tests: BTreeMap<String, usize>,
    /// Witness rows.
    pub witnesses: usize,
    /// Planted rows, and the patches they apply.
    pub planted: usize,
    /// Patches the planted rows apply.
    pub planted_patches: usize,
    /// Committed seeds the seed sets admit.
    pub seeds: usize,
    /// Seed witness rows.
    pub seed_witnesses: usize,
    /// Target rows.
    pub targets: usize,
    /// Pending rows, by kind name.
    pub pending: BTreeMap<String, usize>,
    /// Excluded rows.
    pub excluded: usize,
}

/// A registered harness.
#[derive(Clone, Debug, Serialize)]
pub struct HarnessRow {
    /// `harness/<name>`.
    pub id: String,
    /// [`Expect::Complete`].
    pub expect: Expect,
    /// The harness's name.
    pub harness: String,
    /// What fixes its fuzz input's meaning: [`seed::versions`].
    pub versions: BTreeMap<&'static str, String>,
    /// Its property rows.
    pub properties: Vec<String>,
    /// The target rows that run it, directly or through an aggregate.
    pub targets: Vec<String>,
    /// Its seed set's row.
    pub seed_set: String,
    /// Its missing-target row, if no target runs it.
    pub missing_target: Option<String>,
}

/// A property one harness reports.
#[derive(Clone, Debug, Serialize)]
pub struct PropertyRow {
    /// `property/<harness>/<property>`.
    pub id: String,
    /// [`Expect::Holds`] if `Active`, [`Expect::Violated`] if `Active` in a
    /// harness that plants its bugs, and [`Expect::Reported`] if not
    /// `Active`.
    pub expect: Expect,
    /// The harness.
    pub harness: String,
    /// The property's ID.
    pub property: String,
    /// Its status in the harness.
    pub status: Status,
    /// The rows whose runs decide it: its witnesses, the harness's seed
    /// witnesses of it, and the targets that run the harness.
    pub decided_by: Vec<String>,
}

/// What a [`TestRow`] runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum TestKind {
    /// A simulator package's library unit tests.
    SimLib,
    /// One of a simulator package's test binaries.
    SimTest,
    /// One ordinary-build source file's property suite.
    PropertySuite,
    /// One ignored test, run on demand.
    OnDemand,
    /// A `[[lane]]` command.
    Lane,
}

/// A test command.
#[derive(Clone, Debug, Serialize)]
pub struct TestRow {
    /// `test/<package>/<binary or lib>`, `property-suite/<package>/<module>`,
    /// `on-demand/<file>/<test>` or `lane/<id>`.
    pub id: String,
    /// [`Expect::Pass`].
    pub expect: Expect,
    /// What it runs.
    pub kind: TestKind,
    /// The build it runs in.
    pub build: Build,
    /// The command.
    pub command: String,
    /// Its timeout.
    pub timeout_secs: u64,
    /// The source file it runs, if one.
    pub file: Option<String>,
}

/// A coverage witness.
#[derive(Clone, Debug, Serialize)]
pub struct WitnessRow {
    /// `witness/<property>/passing` or `witness/<property>/bypass`.
    pub id: String,
    /// [`Expect::Pass`], or [`Expect::Unimplemented`] for a pending
    /// bypass.
    pub expect: Expect,
    /// The property.
    pub property: String,
    /// `passing` or `bypass`.
    pub role: &'static str,
    /// The test, as `<file>::<name>` relative to the repository's root, if
    /// one.
    pub test: Option<String>,
    /// For a passing witness no scenario reaches, why.
    pub unreachable: Option<&'static str>,
    /// For a planted bypass, the patch's name.
    pub patch: Option<&'static str>,
    /// For a pending bypass, what its check is missing.
    pub pending: Option<&'static str>,
    /// The row whose run decides it: the test binary, the on-demand test
    /// or the planted cases.
    pub decided_by: Option<String>,
}

/// A property's retained planted cases.
#[derive(Clone, Debug, Serialize)]
pub struct PlantedRow {
    /// `planted/<property>`.
    pub id: String,
    /// [`Expect::Pass`]: `cargo xtask planted` catches every patch.
    pub expect: Expect,
    /// The property.
    pub property: String,
    /// Its patches' names.
    pub patches: Vec<String>,
    /// The command.
    pub command: String,
    /// Its timeout.
    pub timeout_secs: u64,
}

/// A harness's committed seeds.
#[derive(Clone, Debug, Serialize)]
pub struct SeedSetRow {
    /// `seeds/<harness>`.
    pub id: String,
    /// [`Expect::Pass`]: `cargo xtask sim-seeds` admits every required
    /// seed.
    pub expect: Expect,
    /// The harness.
    pub harness: String,
    /// The admitted seeds' files, relative to the repository's root.
    pub seeds: Vec<String>,
    /// The command, which writes the full inputs to `{evidence}/corpus`.
    pub command: String,
    /// Its timeout.
    pub timeout_secs: u64,
}

/// A witness a committed seed carries.
#[derive(Clone, Debug, Serialize)]
pub struct SeedWitnessRow {
    /// `seed-witness/<seed>/<property>`.
    pub id: String,
    /// [`Expect::Pass`]: the witness test passes.
    pub expect: Expect,
    /// The seed's file, relative to the repository's root.
    pub seed: String,
    /// The harness.
    pub harness: String,
    /// The property.
    pub property: String,
    /// What the witness test expects: `holds`, `violated` or `applicable`.
    pub witness: String,
    /// The command.
    pub command: String,
    /// Its timeout.
    pub timeout_secs: u64,
}

/// A fuzz target's smoke run over its seeds.
#[derive(Clone, Debug, Serialize)]
pub struct TargetRow {
    /// `target/<package>/<name>`.
    pub id: String,
    /// [`Expect::Pass`]: libFuzzer runs the seeds, then the smoke budget,
    /// and exits 0.
    pub expect: Expect,
    /// The fuzz package's directory under `qa/fuzz/`.
    pub package: String,
    /// The target.
    pub name: String,
    /// The harness it runs, from its package's
    /// `[package.metadata.zakura-sim.targets]` table.
    pub harness: Option<String>,
    /// The harnesses it covers through `harness`.
    pub covers: Vec<String>,
    /// The smoke run's length.
    pub smoke_secs: u64,
    /// The command.
    pub command: String,
    /// Its timeout.
    pub timeout_secs: u64,
}

/// A planned row.
#[derive(Clone, Debug, Serialize)]
pub struct PendingRow {
    /// `missing-target/<harness>` or `pending/<id>`.
    pub id: String,
    /// [`Expect::Unimplemented`].
    pub expect: Expect,
    /// `target`, or the [`PendingKind`].
    pub kind: String,
    /// The chunk that implements it.
    pub owner: String,
    /// What it waits for, or the plan that defines it.
    pub detail: String,
}

/// An ignored test or fuzz target the suite leaves out.
#[derive(Clone, Debug, Serialize)]
pub struct ExcludedRow {
    /// `excluded/<file>/<test>` or `excluded/target/<package>/<name>`.
    pub id: String,
    /// [`Expect::Excluded`].
    pub expect: Expect,
    /// Why.
    pub reason: String,
}

/// Why the manifest doesn't cover the suite: every problem found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Problems(pub Vec<String>);

impl fmt::Display for Problems {
    /// One problem per line.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for problem in &self.0 {
            writeln!(f, "{problem}")?;
        }
        Ok(())
    }
}

impl std::error::Error for Problems {}

/// The manifest of the checkout at `root`, from its `qa/suite.toml`.
pub fn manifest(root: &Path) -> Result<Manifest, Problems> {
    let path = root.join(SUITE_FILE);
    let text = fs::read_to_string(&path)
        .map_err(|error| Problems(vec![format!("reading {}: {error}", path.display())]))?;
    manifest_from(root, &text)
}

/// The manifest of the checkout at `root`, with `text` as its
/// `qa/suite.toml`.
pub fn manifest_from(root: &Path, text: &str) -> Result<Manifest, Problems> {
    let suite: SuiteFile =
        toml::from_str(text).map_err(|error| Problems(vec![format!("{SUITE_FILE}: {error}")]))?;
    manifest_of(root, &suite, sha256(text.as_bytes()))
}

/// The manifest of the checkout at `root`, with `suite` as its parsed
/// `qa/suite.toml`, whose SHA-256 is `suite_sha256`.
pub fn manifest_of(
    root: &Path,
    suite: &SuiteFile,
    suite_sha256: String,
) -> Result<Manifest, Problems> {
    let mut builder = Builder {
        root,
        canonical_root: root.canonicalize().unwrap_or_else(|_| root.to_owned()),
        suite,
        problems: Vec::new(),
        ids: BTreeSet::new(),
        owners: BTreeMap::new(),
    };
    let manifest = builder.build(suite_sha256);
    match builder.problems.is_empty() {
        true => Ok(manifest),
        false => Err(Problems(builder.problems)),
    }
}

/// The state of one [`manifest_from`] call.
struct Builder<'a> {
    root: &'a Path,
    /// `root` with symbolic links resolved, as `cargo metadata` reports
    /// paths.
    canonical_root: PathBuf,
    suite: &'a SuiteFile,
    problems: Vec<String>,
    ids: BTreeSet<String>,
    /// The test targets and libraries that compile each scanned source
    /// file, by file.
    owners: BTreeMap<String, Vec<Owner>>,
}

/// A test target or library that compiles a source file.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Owner {
    /// Its cargo selector, such as `-p zakura-sim --test harness`.
    selector: String,
    /// The build it runs in.
    build: Build,
    /// What the full names of the tests at the file's top level start
    /// with: the file's module path and `::`, or nothing in the crate's
    /// root.
    prefix: String,
}

impl Builder<'_> {
    /// Records a problem.
    fn problem(&mut self, problem: String) {
        self.problems.push(problem);
    }

    /// Claims row ID `id`, recording a problem if another row has it.
    fn id(&mut self, id: String) -> String {
        if !self.ids.insert(id.clone()) {
            self.problem(format!("two rows have the ID `{id}`"));
        }
        id
    }

    /// Every row, and the problems on the way.
    fn build(&mut self, suite_sha256: String) -> Manifest {
        if self.suite.schema != SCHEMA {
            self.problem(format!(
                "{SUITE_FILE} has schema {}, and this build reads schema {SCHEMA}",
                self.suite.schema
            ));
        }
        let (tests, excluded_tests) = self.tests();
        let planted = self.planted();
        let (seed_sets, seed_witnesses, seed_report) = self.seeds();
        let (targets, excluded_targets, fuzz_lockfiles) = self.targets();
        let witnesses = self.witnesses(&tests, &planted);
        let pending = self.pending();
        let (harnesses, properties) =
            self.harnesses(&witnesses, &seed_witnesses, &targets, &pending);
        let excluded: Vec<ExcludedRow> =
            excluded_tests.into_iter().chain(excluded_targets).collect();
        let mut counts = Counts {
            harnesses: harnesses.len(),
            properties: properties.len(),
            witnesses: witnesses.len(),
            planted: planted.len(),
            planted_patches: planted.iter().map(|row| row.patches.len()).sum(),
            seeds: seed_sets.iter().map(|row| row.seeds.len()).sum(),
            seed_witnesses: seed_witnesses.len(),
            targets: targets.len(),
            excluded: excluded.len(),
            ..Counts::default()
        };
        let mut statuses = BTreeMap::new();
        for row in &properties {
            statuses.insert(row.property.clone(), status_name(row.status));
        }
        for status in statuses.into_values() {
            *counts.property_ids.entry(status.to_owned()).or_default() += 1;
        }
        for row in &tests {
            let kind = serde_json::to_value(row.kind)
                .ok()
                .and_then(|kind| kind.as_str().map(str::to_owned))
                .unwrap_or_default();
            *counts.tests.entry(kind).or_default() += 1;
        }
        for row in &pending {
            *counts.pending.entry(row.kind.clone()).or_default() += 1;
        }
        Manifest {
            schema: SCHEMA,
            build: BuildInfo::current().into(),
            suite_sha256,
            fuzz_lockfiles,
            format: FORMAT_VERSION,
            contract: CONTRACT_VERSION,
            nightly: NIGHTLY,
            counts,
            harnesses,
            properties,
            tests,
            witnesses,
            planted,
            seed_sets,
            seed_witnesses,
            targets,
            pending,
            excluded,
            seed_report,
        }
    }

    /// The test rows: each simulator package's library and test binaries,
    /// each ordinary-build property suite, each ignored test the suite
    /// runs, and each lane. Also the excluded rows of the ignored tests it
    /// leaves out.
    fn tests(&mut self) -> (Vec<TestRow>, Vec<ExcludedRow>) {
        let mut rows = Vec::new();
        // Every ignored test in the sources the suite runs, as (file, test),
        // with whether it sits at its file's top level.
        let mut ignored = BTreeMap::new();
        if let Some(workspace) = self.cargo_packages("Cargo.toml") {
            for package in &self.suite.sim_package {
                self.sim_package(package, &workspace, &mut rows, &mut ignored);
            }
        }
        for package in &self.suite.property_package {
            let dir = format!("crates/{}", package.name);
            let files: Vec<String> = files(self.root, &format!("{dir}/src"), "rs")
                .into_iter()
                .filter(|file| {
                    fs::read_to_string(self.root.join(file))
                        .is_ok_and(|source| is_property_suite(&source, &package.suite_macros))
                })
                .collect();
            if files.is_empty() {
                self.problem(format!(
                    "property package `{}` has no property suite",
                    package.name
                ));
            }
            for file in files {
                // The row's filter selects the file's child modules too.
                let children = match file.strip_suffix("/mod.rs") {
                    Some(module_dir) => module_dir.to_owned(),
                    None => file.trim_end_matches(".rs").to_owned(),
                };
                let features = features(&package.features);
                for source in
                    std::iter::once(file.clone()).chain(self::files(self.root, &children, "rs"))
                {
                    ignored.extend(self.ignored_in(&source));
                    self.own(
                        source.clone(),
                        Owner {
                            selector: format!("-p {}{features} --lib", package.name),
                            build: Build::Ordinary,
                            prefix: test_prefix(&source, &dir),
                        },
                    );
                }
                let module = module_path(&file, &dir);
                let id = self.id(format!("property-suite/{}/{module}", package.name));
                rows.push(TestRow {
                    id,
                    expect: Expect::Pass,
                    kind: TestKind::PropertySuite,
                    build: Build::Ordinary,
                    command: Build::Ordinary.command(
                        "test",
                        &format!("--locked -p {}{features} --lib -- {module}::", package.name),
                    ),
                    timeout_secs: package.timeout_secs,
                    file: Some(file),
                });
            }
        }
        let mut excluded = Vec::new();
        let mut listed = BTreeSet::new();
        for entry in &self.suite.ignored {
            let key = (entry.file.clone(), entry.test.clone());
            if !listed.insert(key.clone()) {
                self.problem(format!(
                    "{SUITE_FILE} lists {}::{} twice",
                    entry.file, entry.test
                ));
            }
            let Some(&top_level) = ignored.get(&key) else {
                self.problem(format!(
                    "{SUITE_FILE} lists {}::{}, which isn't an ignored test in the suite's sources",
                    entry.file, entry.test
                ));
                continue;
            };
            match (entry.run, entry.timeout_secs, &entry.reason) {
                (true, Some(timeout_secs), None) => {
                    if !top_level {
                        self.problem(format!(
                            "{SUITE_FILE} runs {}::{}, which sits inside a module of its file, \
                             so the suite can't name its full path",
                            entry.file, entry.test
                        ));
                        continue;
                    }
                    let Some(owner) = self.owner(&entry.file) else {
                        continue;
                    };
                    let build = match owner.build {
                        Build::Ordinary => Build::Ordinary,
                        _ => Build::SimPlanted,
                    };
                    let id = self.id(format!("on-demand/{}/{}", entry.file, entry.test));
                    rows.push(TestRow {
                        id,
                        expect: Expect::Pass,
                        kind: TestKind::OnDemand,
                        build,
                        command: build.command(
                            "test",
                            &format!(
                                "--locked {} -- --ignored --exact {}{}",
                                owner.selector, owner.prefix, entry.test
                            ),
                        ),
                        timeout_secs,
                        file: Some(entry.file.clone()),
                    });
                }
                (false, None, Some(reason)) => {
                    let id = self.id(format!("excluded/{}/{}", entry.file, entry.test));
                    excluded.push(ExcludedRow {
                        id,
                        expect: Expect::Excluded,
                        reason: reason.clone(),
                    });
                }
                _ => self.problem(format!(
                    "{SUITE_FILE}: {}::{} needs either `run = true` and `timeout-secs`, or \
                     `run = false` and `reason`",
                    entry.file, entry.test
                )),
            }
        }
        for (file, test) in ignored.keys().filter(|key| !listed.contains(*key)) {
            self.problem(format!(
                "{file}::{test} is ignored, and {SUITE_FILE} neither runs nor excludes it"
            ));
        }
        for lane in &self.suite.lane {
            let id = self.id(format!("lane/{}", lane.id));
            let (subcommand, args) = lane.args.split_once(' ').unwrap_or((&lane.args, ""));
            rows.push(TestRow {
                id,
                expect: Expect::Pass,
                kind: TestKind::Lane,
                build: lane.build,
                command: lane.build.command(subcommand, args),
                timeout_secs: lane.timeout_secs,
                file: None,
            });
        }
        (rows, excluded)
    }

    /// The ignored tests `file` defines, as (file, test), each with whether
    /// it sits at the file's top level.
    fn ignored_in(&mut self, file: &str) -> Vec<((String, String), bool)> {
        self.scan(file)
            .ignored
            .into_iter()
            .map(|(test, top_level)| ((file.to_owned(), test), top_level))
            .collect()
    }

    /// [`scan_tests`] on `file`. A failed read, and each attribute the scan
    /// can't classify, is a problem.
    fn scan(&mut self, file: &str) -> TestScan {
        let source = match fs::read_to_string(self.root.join(file)) {
            Ok(source) => source,
            Err(error) => {
                self.problem(format!("reading {file}: {error}"));
                return TestScan::default();
            }
        };
        let scan = scan_tests(&source);
        for (line, attribute) in &scan.unclassified {
            self.problem(format!(
                "{file}:{line}: the suite can't tell which test `{attribute}` ignores, or when. \
                 Put a plain `#[ignore]` or `#[ignore = \"...\"]` on the test itself"
            ));
        }
        scan
    }

    /// The rows of the simulator package `package`: its library's unit
    /// tests and its test targets, as `workspace`, the workspace's packages,
    /// reports them. Adds the ignored tests in the sources they compile to
    /// `ignored`.
    fn sim_package(
        &mut self,
        package: &SimPackage,
        workspace: &[CargoPackage],
        rows: &mut Vec<TestRow>,
        ignored: &mut BTreeMap<(String, String), bool>,
    ) {
        let Some(cargo) = workspace.iter().find(|cargo| cargo.name == package.name) else {
            self.problem(format!(
                "sim package `{}` isn't a package of the workspace",
                package.name
            ));
            return;
        };
        let dir = self.cargo_path(cargo.manifest_path.parent().unwrap_or(Path::new("")));
        if package.lib {
            match cargo.targets.iter().find(|target| target.is("lib")) {
                Some(lib) => {
                    let id = self.id(format!("test/{}/lib", package.name));
                    rows.push(TestRow {
                        id,
                        expect: Expect::Pass,
                        kind: TestKind::SimLib,
                        build: Build::Sim,
                        command: Build::Sim
                            .command("test", &format!("--locked -p {} --lib", package.name)),
                        timeout_secs: package.timeout_secs,
                        file: None,
                    });
                    let src = self.cargo_path(lib.src_path.parent().unwrap_or(Path::new("")));
                    let mut tests = 0;
                    for file in files(self.root, &src, "rs") {
                        let scan = self.scan(&file);
                        tests += scan.tests;
                        ignored.extend(
                            scan.ignored
                                .into_iter()
                                .map(|(test, top_level)| ((file.clone(), test), top_level)),
                        );
                        self.own(
                            file.clone(),
                            Owner {
                                selector: format!("-p {} --lib", package.name),
                                build: Build::Sim,
                                prefix: test_prefix(&file, &dir),
                            },
                        );
                    }
                    // `--lib` with no unit test runs nothing, so its row can't
                    // pass.
                    if tests == 0 {
                        self.problem(format!(
                            "sim package `{}`'s library has no unit tests, so its `--lib` row \
                             can't pass: set `lib = false`",
                            package.name
                        ));
                    }
                }
                None => self.problem(format!("sim package `{}` has no library", package.name)),
            }
        }
        // Each test target's root file, by name.
        let found: BTreeMap<String, String> = cargo
            .targets
            .iter()
            .filter(|target| target.is("test"))
            .map(|target| (target.name.clone(), self.cargo_path(&target.src_path)))
            .collect();
        let names: Vec<String> = match &package.tests {
            None => found.keys().cloned().collect(),
            Some(listed) => {
                for (name, file) in &found {
                    if !listed.contains(name) && self.sim_only(file) {
                        self.problem(format!(
                            "{file} builds only under `--cfg zakura_sim`, and {SUITE_FILE} \
                             doesn't list its test target `{name}` under `{}`",
                            package.name
                        ));
                    }
                }
                listed.clone()
            }
        };
        for name in names {
            let Some(file) = found.get(&name) else {
                self.problem(format!(
                    "sim package `{}` has no test target `{name}`",
                    package.name
                ));
                continue;
            };
            let (sources, problems) = crate_files(self.root, file);
            self.problems.extend(problems);
            for (source, prefix) in sources {
                ignored.extend(self.ignored_in(&source));
                self.own(
                    source,
                    Owner {
                        selector: format!("-p {} --test {name}", package.name),
                        build: Build::Sim,
                        prefix,
                    },
                );
            }
            let id = self.id(format!("test/{}/{name}", package.name));
            rows.push(TestRow {
                id,
                expect: Expect::Pass,
                kind: TestKind::SimTest,
                build: Build::Sim,
                command: Build::Sim.command(
                    "test",
                    &format!("--locked -p {} --test {name}", package.name),
                ),
                timeout_secs: package.timeout_secs,
                file: Some(file.clone()),
            });
        }
    }

    /// Whether `file`'s first inner attribute is `#![cfg(zakura_sim)]`.
    fn sim_only(&self, file: &str) -> bool {
        fs::read_to_string(self.root.join(file)).is_ok_and(|source| {
            source
                .lines()
                .map(str::trim)
                .find(|line| line.starts_with("#!["))
                .is_some_and(|line| line == "#![cfg(zakura_sim)]")
        })
    }

    /// Records that `owner` compiles `file`.
    fn own(&mut self, file: String, owner: Owner) {
        let owners = self.owners.entry(file).or_default();
        if !owners.contains(&owner) {
            owners.push(owner);
        }
    }

    /// The one test target or library that compiles `file`. No owner, or
    /// more than one, is a problem: the suite can't pick the command that
    /// runs the file's tests.
    fn owner(&mut self, file: &str) -> Option<Owner> {
        match self.owners.get(file).map(Vec::as_slice) {
            Some([owner]) => Some(owner.clone()),
            Some(owners) if !owners.is_empty() => {
                let selectors: Vec<&str> =
                    owners.iter().map(|owner| owner.selector.as_str()).collect();
                self.problem(format!(
                    "{file} compiles into more than one test target ({}), so the suite can't \
                     pick the one that runs its ignored tests",
                    selectors.join(", ")
                ));
                None
            }
            _ => {
                self.problem(format!("no package row runs {file}"));
                None
            }
        }
    }

    /// The packages of the Cargo workspace whose manifest is `manifest`,
    /// relative to the root, as `cargo metadata --no-deps` reports them.
    /// A failure is a problem.
    fn cargo_packages(&mut self, manifest: &str) -> Option<Vec<CargoPackage>> {
        let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
        let output = Command::new(cargo)
            .args([
                "metadata",
                "--no-deps",
                "--format-version",
                "1",
                "--offline",
                "--manifest-path",
            ])
            .arg(self.root.join(manifest))
            .current_dir(self.root)
            .output();
        let packages = match output {
            Ok(output) if output.status.success() => {
                serde_json::from_slice::<CargoMetadata>(&output.stdout)
                    .map(|metadata| metadata.packages)
                    .map_err(|error| error.to_string())
            }
            Ok(output) => Err(String::from_utf8_lossy(&output.stderr).trim().to_owned()),
            Err(error) => Err(error.to_string()),
        };
        packages
            .map_err(|error| self.problem(format!("cargo metadata of {manifest}: {error}")))
            .ok()
    }

    /// `path`, as `cargo metadata` reports it, relative to the root.
    fn cargo_path(&self, path: &Path) -> String {
        let path = path.canonicalize().unwrap_or_else(|_| path.to_owned());
        relative(&self.canonical_root, &path)
    }

    /// One row per property directory under `qa/planted/`, each running
    /// its patches.
    fn planted(&mut self) -> Vec<PlantedRow> {
        let mut by_property: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for file in files(self.root, PLANTED_DIR, "patch") {
            let path = Path::new(&file);
            let property = path
                .parent()
                .and_then(|parent| parent.strip_prefix(PLANTED_DIR).ok())
                .map(|parent| parent.to_string_lossy().into_owned())
                .unwrap_or_default();
            let name = path
                .file_stem()
                .map(|stem| stem.to_string_lossy().into_owned())
                .unwrap_or_default();
            by_property.entry(property).or_default().push(name);
        }
        let reported: BTreeSet<String> = reported_properties();
        let mut rows = Vec::new();
        for (property, patches) in by_property {
            if !reported.contains(&property) {
                self.problem(format!(
                    "{PLANTED_DIR}/{property} holds planted cases, and no harness reports \
                     `{property}`"
                ));
            }
            let id = self.id(format!("planted/{property}"));
            rows.push(PlantedRow {
                id,
                expect: Expect::Pass,
                command: Build::Xtask.command("xtask", &format!("planted {property}")),
                property,
                patches,
                timeout_secs: self.suite.planted_timeout_secs,
            });
        }
        rows
    }

    /// One seed-set row per harness, one row per witness an admitted seed
    /// carries, and the seed report. A rejected seed is a problem.
    fn seeds(&mut self) -> (Vec<SeedSetRow>, Vec<SeedWitnessRow>, seed::Report) {
        let fixtures = vec![self.root.join(FIXTURES_DIR)];
        let report = match seed::load(&fixtures, None) {
            Ok(report) => report,
            Err(error) => {
                self.problem(format!("listing {FIXTURES_DIR}: {error}"));
                return (
                    Vec::new(),
                    Vec::new(),
                    seed::SeedReport::default().rows(None, &[]),
                );
            }
        };
        for failed in &report.rejected {
            self.problem(format!(
                "the required seed {} fails validation: {}",
                relative(self.root, &failed.path),
                failed.error
            ));
        }
        let mut sets = Vec::new();
        for entry in HARNESSES {
            let seeds: Vec<String> = report
                .admitted
                .iter()
                .filter(|seed| seed.sidecar.harness == entry.name)
                .map(|seed| relative(self.root, &seed.path))
                .collect();
            let id = self.id(format!("seeds/{}", entry.name));
            sets.push(SeedSetRow {
                id,
                expect: Expect::Pass,
                harness: entry.name.to_owned(),
                seeds,
                command: seed_corpus(entry.name),
                timeout_secs: self.suite.seed_timeout_secs,
            });
        }
        let mut witnesses = Vec::new();
        for seed in &report.admitted {
            let file = relative(self.root, &seed.path);
            if !self.root.join(&seed.sidecar.source).exists() {
                self.problem(format!(
                    "the seed {file} names the source `{}`, which doesn't exist",
                    seed.sidecar.source
                ));
            }
            let stem = seed
                .path
                .file_stem()
                .map(|stem| stem.to_string_lossy().into_owned())
                .unwrap_or_default();
            for witness in &seed.sidecar.witness {
                let id = self.id(format!("seed-witness/{stem}/{}", witness.property));
                let record = Build::SimPlanted.command(
                    "run",
                    &format!(
                        "--locked -p zakura-sim --bin zakura-sim -- record --sim {} \
                         --input {EVIDENCE}/corpus/{} --out {EVIDENCE}/trace --witness {} \
                         --expect {}",
                        seed.sidecar.harness,
                        seed.input_sha256(),
                        witness.property,
                        witness.expect
                    ),
                );
                witnesses.push(SeedWitnessRow {
                    id,
                    expect: Expect::Pass,
                    seed: file.clone(),
                    harness: seed.sidecar.harness.clone(),
                    property: witness.property.clone(),
                    witness: witness.expect.clone(),
                    command: format!("{} && {record}", seed_corpus(&seed.sidecar.harness)),
                    timeout_secs: self.suite.seed_timeout_secs,
                });
            }
        }
        let rows = report.rows(None, &[PathBuf::from(FIXTURES_DIR)]);
        (sets, witnesses, rows)
    }

    /// The target rows, the excluded targets' rows, and each fuzz
    /// package's lockfile digest. Every binary target of every fuzz
    /// package must be mapped or excluded, and each mapped one of
    /// [`SIM_FUZZ_PACKAGE`] must have a harness in its package's
    /// `[package.metadata.zakura-sim.targets]` table.
    fn targets(&mut self) -> (Vec<TargetRow>, Vec<ExcludedRow>, BTreeMap<String, String>) {
        let mut bins = BTreeSet::new();
        // The harness each target runs, by (package, target).
        let mut harnesses = BTreeMap::new();
        let mut lockfiles = BTreeMap::new();
        let dir = self.root.join(FUZZ_DIR);
        let packages = match sorted_dirs(&dir) {
            Ok(packages) => packages,
            Err(error) => {
                self.problem(format!("listing {FUZZ_DIR}: {error}"));
                Vec::new()
            }
        };
        for package in packages {
            let manifest = format!("{FUZZ_DIR}/{package}/Cargo.toml");
            if !self.root.join(&manifest).is_file() {
                continue;
            }
            // A fuzz package is a workspace of its own.
            for cargo in self.cargo_packages(&manifest).unwrap_or_default() {
                for target in cargo.targets.iter().filter(|target| target.is("bin")) {
                    bins.insert((package.clone(), target.name.clone()));
                }
                for (name, harness) in cargo.harnesses() {
                    harnesses.insert((package.clone(), name), harness);
                }
            }
            let lockfile = dir.join(&package).join("Cargo.lock");
            lockfiles.insert(
                package.clone(),
                fs::read(&lockfile).map_or_else(|_| "none".to_owned(), |bytes| sha256(&bytes)),
            );
        }
        for (package, name) in harnesses.keys() {
            if !bins.contains(&(package.clone(), name.clone())) {
                self.problem(format!(
                    "{FUZZ_DIR}/{package}/Cargo.toml maps `{name}` to a harness, and defines no \
                     such target"
                ));
            }
        }
        let excluded: BTreeSet<(String, String)> = self
            .suite
            .excluded_target
            .iter()
            .map(|target| (target.package.clone(), target.name.clone()))
            .collect();
        for key in &bins {
            if key.0 == SIM_FUZZ_PACKAGE && !harnesses.contains_key(key) && !excluded.contains(key)
            {
                self.problem(format!(
                    "{FUZZ_DIR}/{} defines the target `{}`, and its \
                     `[package.metadata.zakura-sim.targets]` table maps it to no harness",
                    key.0, key.1
                ));
            }
        }
        let mut mapped = BTreeSet::new();
        let mut rows = Vec::new();
        for target in &self.suite.target {
            let key = (target.package.clone(), target.name.clone());
            let harness: Option<String> = harnesses.get(&key).cloned();
            if !bins.contains(&key) {
                self.problem(format!(
                    "{SUITE_FILE} maps {FUZZ_DIR}/{}'s target `{}`, which its Cargo.toml \
                     doesn't define",
                    target.package, target.name
                ));
            }
            if !mapped.insert(key) {
                self.problem(format!(
                    "{SUITE_FILE} names {FUZZ_DIR}/{}'s target `{}` twice",
                    target.package, target.name
                ));
            }
            for harness in harness.iter().chain(&target.covers) {
                if !HARNESSES.iter().any(|entry| entry.name == harness) {
                    self.problem(format!(
                        "the target `{}` names the harness `{harness}`, which isn't registered",
                        target.name
                    ));
                }
            }
            if harness.is_none() && !target.covers.is_empty() {
                self.problem(format!(
                    "the target `{}` covers harnesses without running one",
                    target.name
                ));
            }
            if let Some(runs) = HARNESSES
                .iter()
                .find(|entry| Some(entry.name) == harness.as_deref())
            {
                self.covered_by(&target.name, runs, &target.covers);
            }
            let id = self.id(format!("target/{}/{}", target.package, target.name));
            let build = match target.package == SIM_FUZZ_PACKAGE {
                true => Build::FuzzSim,
                false => Build::Fuzz,
            };
            let corpus = match &harness {
                Some(harness) => seed_corpus(harness),
                None => format!("mkdir -p {EVIDENCE}/corpus"),
            };
            let mut args = format!("-max_total_time={}", target.smoke_secs);
            for arg in &target.args {
                args.push(' ');
                args.push_str(arg);
            }
            let mut fuzz = build.command(
                "fuzz",
                &format!(
                    "run --fuzz-dir {FUZZ_DIR}/{} {} {EVIDENCE}/corpus -- {args}",
                    target.package, target.name
                ),
            );
            if !target.env.is_empty() {
                fuzz = format!("{} {fuzz}", target.env.join(" "));
            }
            rows.push(TargetRow {
                id,
                expect: Expect::Pass,
                package: target.package.clone(),
                name: target.name.clone(),
                harness,
                covers: target.covers.clone(),
                smoke_secs: target.smoke_secs,
                command: format!("{corpus} && {fuzz}"),
                timeout_secs: target.timeout_secs,
            });
        }
        let mut excluded = Vec::new();
        for target in &self.suite.excluded_target {
            let key = (target.package.clone(), target.name.clone());
            if !bins.contains(&key) {
                self.problem(format!(
                    "{SUITE_FILE} excludes {FUZZ_DIR}/{}'s target `{}`, which its Cargo.toml \
                     doesn't define",
                    target.package, target.name
                ));
            }
            if !mapped.insert(key) {
                self.problem(format!(
                    "{SUITE_FILE} names {FUZZ_DIR}/{}'s target `{}` twice",
                    target.package, target.name
                ));
            }
            let id = self.id(format!(
                "excluded/target/{}/{}",
                target.package, target.name
            ));
            excluded.push(ExcludedRow {
                id,
                expect: Expect::Excluded,
                reason: target.reason.clone(),
            });
        }
        for (package, name) in bins.difference(&mapped) {
            self.problem(format!(
                "{FUZZ_DIR}/{package} defines the target `{name}`, and {SUITE_FILE} neither \
                 maps nor excludes it"
            ));
        }
        (rows, excluded, lockfiles)
    }

    /// Records a problem for each harness in `covers` that `runs`, the
    /// harness of the target `target`, doesn't compose, or whose properties
    /// it doesn't check. A shared property ID doesn't make one harness's
    /// runs another's, so the code-owned [`HarnessEntry::composes`] decides
    /// what the target's runs include. Its runs can decide only the
    /// properties its harness checks.
    fn covered_by(&mut self, target: &str, runs: &HarnessEntry, covers: &[String]) {
        let checks: BTreeSet<String> = (runs.properties)()
            .into_iter()
            .map(|property| property.id)
            .collect();
        for name in covers {
            let Some(covered) = HARNESSES.iter().find(|entry| entry.name == name) else {
                continue;
            };
            if !runs.composes.contains(&covered.name) {
                self.problem(format!(
                    "the target `{target}` covers the harness `{name}`, and its harness `{}` \
                     doesn't compose `{name}`",
                    runs.name
                ));
                continue;
            }
            let unchecked: Vec<String> = (covered.properties)()
                .into_iter()
                .filter(|property| !checks.contains(&property.id))
                .map(|property| format!("`{}`", property.id))
                .collect();
            if !unchecked.is_empty() {
                self.problem(format!(
                    "the target `{target}` covers the harness `{name}`, and its harness `{}` \
                     doesn't check `{name}`'s properties {}",
                    runs.name,
                    unchecked.join(", ")
                ));
            }
        }
    }

    /// One passing and one bypass row per coverage row. A reported
    /// property without a coverage row, or a witness the suite doesn't
    /// run, is a problem.
    fn witnesses(&mut self, tests: &[TestRow], planted: &[PlantedRow]) -> Vec<WitnessRow> {
        let reported = reported_properties();
        let listed: BTreeSet<String> = COVERAGE.iter().map(|row| row.property.to_owned()).collect();
        for property in reported.difference(&listed) {
            self.problem(format!(
                "a harness reports `{property}`, and the coverage manifest has no row for it"
            ));
        }
        let mut rows = Vec::new();
        for row in COVERAGE {
            let passing = row.passing.witness();
            let unreachable = match row.passing {
                crate::harness::coverage::Passing::Unreachable { reason, .. } => Some(reason),
                crate::harness::coverage::Passing::Test(_) => None,
            };
            let decided_by = self.witness_row(row.property, passing, tests);
            let id = self.id(format!("witness/{}/passing", row.property));
            rows.push(WitnessRow {
                id,
                expect: Expect::Pass,
                property: row.property.to_owned(),
                role: "passing",
                test: Some(witness_test(passing)),
                unreachable,
                patch: None,
                pending: None,
                decided_by,
            });
            let id = self.id(format!("witness/{}/bypass", row.property));
            let bypass = match row.bypass {
                Bypass::Test(witness) => WitnessRow {
                    id,
                    expect: Expect::Pass,
                    property: row.property.to_owned(),
                    role: "bypass",
                    test: Some(witness_test(witness)),
                    unreachable: None,
                    patch: None,
                    pending: None,
                    decided_by: self.witness_row(row.property, witness, tests),
                },
                Bypass::Planted(name) => {
                    let case = planted
                        .iter()
                        .find(|case| case.property == row.property)
                        .filter(|case| case.patches.iter().any(|patch| patch == name));
                    if case.is_none() {
                        self.problem(format!(
                            "`{}`'s bypass is {PLANTED_DIR}/{}/{name}.patch, which doesn't exist",
                            row.property, row.property
                        ));
                    }
                    WitnessRow {
                        id,
                        expect: Expect::Pass,
                        property: row.property.to_owned(),
                        role: "bypass",
                        test: None,
                        unreachable: None,
                        patch: Some(name),
                        pending: None,
                        decided_by: case.map(|case| case.id.clone()),
                    }
                }
                Bypass::Pending(reason) => WitnessRow {
                    id,
                    expect: Expect::Unimplemented,
                    property: row.property.to_owned(),
                    role: "bypass",
                    test: None,
                    unreachable: None,
                    patch: None,
                    pending: Some(reason),
                    decided_by: None,
                },
            };
            rows.push(bypass);
        }
        rows
    }

    /// The test row that runs `witness`: its test binary's row, or its
    /// on-demand row. A witness no row runs is a problem.
    fn witness_row(
        &mut self,
        property: &str,
        witness: Witness,
        tests: &[TestRow],
    ) -> Option<String> {
        let file = format!("crates/zakura-sim/{}", witness.file);
        let found = match witness.ci {
            Ci::Job(_) => tests
                .iter()
                .find(|row| row.kind == TestKind::SimTest && row.file.as_deref() == Some(&file)),
            Ci::OnDemand => {
                let id = format!("on-demand/{file}/{}", witness.test);
                tests.iter().find(|row| row.id == id)
            }
        };
        if found.is_none() {
            self.problem(format!(
                "`{property}`'s witness {file}::{} runs in no row of the suite",
                witness.test
            ));
        }
        found.map(|row| row.id.clone())
    }

    /// The missing-target and pending rows. A pending property or harness
    /// that is now registered is a problem.
    fn pending(&mut self) -> Vec<PendingRow> {
        let reported = reported_properties();
        let mut rows = Vec::new();
        for missing in &self.suite.missing_target {
            if !HARNESSES.iter().any(|entry| entry.name == missing.harness) {
                self.problem(format!(
                    "{SUITE_FILE} names a missing target for `{}`, which isn't registered",
                    missing.harness
                ));
            }
            let id = self.id(format!("missing-target/{}", missing.harness));
            rows.push(PendingRow {
                id,
                expect: Expect::Unimplemented,
                kind: "target".to_owned(),
                owner: missing.owner.clone(),
                detail: missing.reason.clone(),
            });
        }
        for pending in &self.suite.pending {
            let registered = match pending.kind {
                PendingKind::Property => reported.contains(&pending.id),
                PendingKind::Harness => HARNESSES.iter().any(|entry| entry.name == pending.id),
                PendingKind::Feature => false,
            };
            if registered {
                self.problem(format!(
                    "{SUITE_FILE} lists `{}` as pending, and it's registered: remove its \
                     `[[pending]]` row",
                    pending.id
                ));
            }
            let id = self.id(format!("pending/{}", pending.id));
            let kind = serde_json::to_value(pending.kind)
                .ok()
                .and_then(|kind| kind.as_str().map(str::to_owned))
                .unwrap_or_default();
            rows.push(PendingRow {
                id,
                expect: Expect::Unimplemented,
                kind,
                owner: pending.owner.clone(),
                detail: pending.plan.clone(),
            });
        }
        rows
    }

    /// One row per harness and one per property it reports. A harness that
    /// no target runs and no missing-target row names, or both, is a
    /// problem.
    fn harnesses(
        &mut self,
        witnesses: &[WitnessRow],
        seed_witnesses: &[SeedWitnessRow],
        targets: &[TargetRow],
        pending: &[PendingRow],
    ) -> (Vec<HarnessRow>, Vec<PropertyRow>) {
        let mut harnesses = Vec::new();
        let mut properties = Vec::new();
        for entry in HARNESSES {
            let running: Vec<String> = targets
                .iter()
                .filter(|row| {
                    row.harness.as_deref() == Some(entry.name)
                        || row.covers.iter().any(|name| name == entry.name)
                })
                .map(|row| row.id.clone())
                .collect();
            let missing_id = format!("missing-target/{}", entry.name);
            let missing = pending
                .iter()
                .any(|row| row.id == missing_id)
                .then_some(missing_id);
            match (running.is_empty(), &missing) {
                (true, None) => self.problem(format!(
                    "no target runs the harness `{}`, and {SUITE_FILE} has no \
                     `[[missing-target]]` row for it",
                    entry.name
                )),
                (false, Some(_)) => self.problem(format!(
                    "a target runs the harness `{}`, and {SUITE_FILE} still lists it as a \
                     missing target",
                    entry.name
                )),
                _ => {}
            }
            let seed_set = format!("seeds/{}", entry.name);
            let mut ids = Vec::new();
            for property in (entry.properties)() {
                let mut decided_by: Vec<String> = witnesses
                    .iter()
                    .filter(|row| row.property == property.id)
                    .map(|row| row.id.clone())
                    .collect();
                decided_by.extend(
                    seed_witnesses
                        .iter()
                        .filter(|row| row.harness == entry.name && row.property == property.id)
                        .map(|row| row.id.clone()),
                );
                decided_by.extend(running.iter().cloned());
                let id = self.id(format!("property/{}/{}", entry.name, property.id));
                ids.push(id.clone());
                properties.push(PropertyRow {
                    id,
                    expect: match property.status {
                        Status::Active if entry.planted => Expect::Violated,
                        Status::Active => Expect::Holds,
                        Status::Known { .. } | Status::Draft => Expect::Reported,
                    },
                    harness: entry.name.to_owned(),
                    property: property.id,
                    status: property.status,
                    decided_by,
                });
            }
            let id = self.id(format!("harness/{}", entry.name));
            harnesses.push(HarnessRow {
                id,
                expect: Expect::Complete,
                harness: entry.name.to_owned(),
                versions: seed::versions(entry).into_iter().collect(),
                properties: ids,
                targets: running,
                seed_set,
                missing_target: missing,
            });
        }
        (harnesses, properties)
    }
}

/// Why `suite --frozen` refuses the manifest of the checkout at `root`
/// that `build` made, if it does. The build's own dirty flag covers only
/// its source paths, and the manifest reads `qa/` and the test sources as
/// they are on disk, so the checkout itself must be clean and at the
/// build's commit.
pub fn unfrozen(root: &Path, build: &ManifestBuild) -> Option<String> {
    let head = git(root, &["rev-parse", "HEAD"]);
    let status = git(root, &["status", "--porcelain", "--untracked-files=all"]);
    unfrozen_by(build, head.as_deref(), status.as_deref())
}

/// Why `suite` refuses to run in `dir`, if it does. The binary reads the
/// checkout at `root`, the one it was built in, so `dir` must lie in that
/// checkout's worktree. Otherwise a binary built in another checkout would
/// describe that checkout from this one.
pub fn other_checkout(root: &Path, dir: &Path) -> Option<String> {
    let canonical = |path: &Path| path.canonicalize().unwrap_or_else(|_| path.to_owned());
    let root = canonical(root);
    let dir = canonical(dir);
    let inside = match git(&dir, &["rev-parse", "--show-toplevel"]) {
        Some(top) => canonical(Path::new(top.trim_end())) == root,
        None => dir.starts_with(&root),
    };
    (!inside).then(|| {
        format!(
            "this zakura-sim was built in the checkout {}, and runs in {}",
            root.display(),
            dir.display()
        )
    })
}

/// `git -C dir <args>`'s output, or `None` if git couldn't say.
fn git(dir: &Path, args: &[&str]) -> Option<String> {
    Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
}

/// [`unfrozen`], given `HEAD`'s commit and `git status --porcelain`'s
/// output, each `None` if git couldn't say.
pub fn unfrozen_by(
    build: &ManifestBuild,
    head: Option<&str>,
    status: Option<&str>,
) -> Option<String> {
    if build.dirty {
        return Some(format!("the build of {} was dirty", build.git));
    }
    match (head.map(str::trim), status.map(str::trim_end)) {
        (Some(head), _) if head != build.git => Some(format!(
            "the checkout is at {head}, and the build is of {}",
            build.git
        )),
        (Some(_), Some("")) => None,
        (Some(_), Some(status)) => Some(format!("the checkout has changes:\n{status}")),
        _ => Some("git can't say whether the checkout is clean".to_owned()),
    }
}

/// `cargo metadata --no-deps`'s output, as far as the manifest reads it.
#[derive(Deserialize)]
struct CargoMetadata {
    /// The workspace's packages.
    packages: Vec<CargoPackage>,
}

/// One package `cargo metadata` reports.
#[derive(Deserialize)]
struct CargoPackage {
    /// Its name.
    name: String,
    /// Its `Cargo.toml`.
    manifest_path: PathBuf,
    /// Every target Cargo builds from it, declared or discovered.
    targets: Vec<CargoTarget>,
    /// Its `[package.metadata]` table.
    metadata: Option<PackageMetadata>,
}

impl CargoPackage {
    /// Its `[package.metadata.zakura-sim.targets]` table: the harness each
    /// target runs, by target.
    fn harnesses(self) -> BTreeMap<String, String> {
        self.metadata
            .and_then(|metadata| metadata.zakura_sim)
            .map(|sim| sim.targets)
            .unwrap_or_default()
    }
}

/// The part of a package's `[package.metadata]` the manifest reads.
#[derive(Deserialize)]
struct PackageMetadata {
    /// `[package.metadata.zakura-sim]`.
    #[serde(rename = "zakura-sim")]
    zakura_sim: Option<SimMetadata>,
}

/// `[package.metadata.zakura-sim]`.
#[derive(Deserialize)]
struct SimMetadata {
    /// The harness each fuzz target runs, by target.
    #[serde(default)]
    targets: BTreeMap<String, String>,
}

/// One target `cargo metadata` reports.
#[derive(Deserialize)]
struct CargoTarget {
    /// Its name, as `--test` or `cargo fuzz run` takes it.
    name: String,
    /// Its kinds, such as `lib`, `bin` or `test`.
    kind: Vec<String>,
    /// Its root source file.
    src_path: PathBuf,
}

impl CargoTarget {
    /// Whether it is of `kind`.
    fn is(&self, kind: &str) -> bool {
        self.kind.iter().any(|own| own == kind)
    }
}

/// Every property ID any harness reports.
fn reported_properties() -> BTreeSet<String> {
    HARNESSES
        .iter()
        .flat_map(|entry| (entry.properties)())
        .map(|property| property.id)
        .collect()
}

/// `status`'s name in counts: `active`, `known` or `draft`.
fn status_name(status: Status) -> &'static str {
    match status {
        Status::Active => "active",
        Status::Known { .. } => "known",
        Status::Draft => "draft",
    }
}

/// The command that writes `harness`'s admitted seeds, as full fuzz
/// inputs, to `{evidence}/corpus`.
fn seed_corpus(harness: &str) -> String {
    format!("cargo xtask sim-seeds --sim {harness} --out {EVIDENCE}/corpus")
}

/// `witness` as `<file>::<test>`, with the file relative to the
/// repository's root.
fn witness_test(witness: Witness) -> String {
    format!("crates/zakura-sim/{}::{}", witness.file, witness.test)
}

/// ` --features <features>`, or nothing if `features` is empty.
fn features(features: &[String]) -> String {
    match features.is_empty() {
        true => String::new(),
        false => format!(" --features {}", features.join(",")),
    }
}

/// Whether `source` is a property suite: outside comments, it has a
/// `proptest!` block or calls one of `macros`, and it defines none of
/// `macros`.
pub fn is_property_suite(source: &str, macros: &[String]) -> bool {
    let code: Vec<&str> = source
        .lines()
        .map(str::trim)
        .filter(|line| !line.starts_with("//"))
        .collect();
    let defines = code.iter().any(|line| {
        macros
            .iter()
            .any(|name| line.starts_with(&format!("macro_rules! {name}")))
    });
    !defines
        && code.iter().any(|line| {
            line.contains("proptest!")
                || macros
                    .iter()
                    .any(|name| line.contains(&format!("{name}!(")))
        })
}

/// What [`scan_tests`] finds in a source file.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TestScan {
    /// How many outer attributes mark a test, such as `#[test]` or
    /// `#[tokio::test]`.
    pub tests: usize,
    /// Each test a plain `#[ignore]` or `#[ignore = "..."]` marks, with
    /// whether the attribute's line is unindented, so the test sits at the
    /// file's top level.
    pub ignored: Vec<(String, bool)>,
    /// Each attribute that may ignore a test in a form the scan can't
    /// classify, with its line, from 1: an `ignore` inside another
    /// attribute, such as `#[cfg_attr(zakura_sim, ignore)]`, or an
    /// `#[ignore]` that no function follows.
    pub unclassified: Vec<(usize, String)>,
}

/// What an outer attribute is, to [`scan_tests`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Attribute {
    /// A path ending in `test`, such as `test` or `tokio::test`.
    Test,
    /// `ignore`, or `ignore = "..."`.
    Ignore,
    /// Any other attribute that names `ignore`.
    Unclassified,
    /// An attribute that names no `ignore`.
    Other,
}

/// The tests and ignored tests in `source`, from its outer attributes
/// outside comments and literals. The scan doesn't evaluate `cfg`: a test
/// that a build leaves out still counts, and its on-demand row then runs
/// no test and fails [`Expect::Pass`].
pub fn scan_tests(source: &str) -> TestScan {
    let code = mask(source);
    let bytes = code.as_bytes();
    let mut scan = TestScan::default();
    let mut at = 0;
    while let Some(offset) = code[at..].find('#') {
        let hash = at + offset;
        // `#!` is an inner attribute, and `r#` a raw identifier.
        let open = hash + 1 + leading_whitespace(&code[hash + 1..]);
        if bytes.get(open) != Some(&b'[') {
            at = hash + 1;
            continue;
        }
        let Some(close) = closing_bracket(bytes, open) else {
            break;
        };
        let line = code[..hash].matches('\n').count() + 1;
        let attribute = || source[hash..=close].to_owned();
        match classify(&code[open + 1..close]) {
            Attribute::Test => scan.tests += 1,
            Attribute::Ignore => {
                let line_start = code[..hash].rfind('\n').map_or(0, |at| at + 1);
                let top_level = !matches!(bytes[line_start], b' ' | b'\t');
                match test_name(&code[close + 1..]) {
                    Some(name) => scan.ignored.push((name, top_level)),
                    None => scan.unclassified.push((line, attribute())),
                }
            }
            Attribute::Unclassified => scan.unclassified.push((line, attribute())),
            Attribute::Other => {}
        }
        at = close + 1;
    }
    scan
}

/// What the attribute with `content` between its brackets is.
fn classify(content: &str) -> Attribute {
    let content = content.trim();
    let path_end = content
        .find(|c: char| !(c.is_alphanumeric() || c == '_' || c == ':'))
        .unwrap_or(content.len());
    let (path, rest) = content.split_at(path_end);
    let rest = rest.trim_start();
    let names_ignore = content
        .split(|c: char| !(c.is_alphanumeric() || c == '_'))
        .any(|word| word == "ignore");
    match path {
        "ignore" if rest.is_empty() || rest.starts_with('=') => Attribute::Ignore,
        _ if names_ignore => Attribute::Unclassified,
        _ if path.rsplit("::").next() == Some("test") => Attribute::Test,
        _ => Attribute::Other,
    }
}

/// The name of the function that `code`, the masked text after an
/// attribute, starts with, past further attributes. `None` if no function
/// follows.
fn test_name(mut code: &str) -> Option<String> {
    loop {
        code = code.trim_start();
        let Some(rest) = code.strip_prefix('#') else {
            break;
        };
        let rest = rest.trim_start();
        let close = closing_bracket(rest.as_bytes(), 0)?;
        code = &rest[close + 1..];
    }
    let mut words = code
        .split(|c: char| !(c.is_alphanumeric() || c == '_'))
        .filter(|word| !word.is_empty());
    for word in words.by_ref() {
        match word {
            "fn" => return words.next().map(str::to_owned),
            "pub" | "crate" | "super" | "self" | "in" | "const" | "async" | "unsafe" | "extern" => {
            }
            _ => return None,
        }
    }
    None
}

/// The length of `text`'s leading whitespace, in bytes.
fn leading_whitespace(text: &str) -> usize {
    text.len() - text.trim_start().len()
}

/// The index of the `]` that closes the `[` at `open` in `bytes`, if
/// `bytes` has one there.
fn closing_bracket(bytes: &[u8], open: usize) -> Option<usize> {
    if bytes.get(open) != Some(&b'[') {
        return None;
    }
    let mut depth = 0usize;
    for (at, byte) in bytes.iter().enumerate().skip(open) {
        match byte {
            b'[' => depth += 1,
            b']' => {
                depth -= 1;
                if depth == 0 {
                    return Some(at);
                }
            }
            _ => {}
        }
    }
    None
}

/// `source` with its comments and the contents of its string, byte string,
/// raw string and character literals replaced by spaces, newlines kept, so
/// each byte keeps its offset.
fn mask(source: &str) -> String {
    let src = source.as_bytes();
    let mut out = src.to_vec();
    let blank = |out: &mut Vec<u8>, from: usize, to: usize| {
        let to = to.min(out.len());
        for byte in out.iter_mut().take(to).skip(from) {
            if *byte != b'\n' {
                *byte = b' ';
            }
        }
    };
    let ident = |byte: u8| byte.is_ascii_alphanumeric() || byte == b'_';
    let mut at = 0;
    while at < src.len() {
        let rest = &src[at..];
        if rest.starts_with(b"//") {
            let end = rest
                .iter()
                .position(|&byte| byte == b'\n')
                .map_or(src.len(), |end| at + end);
            blank(&mut out, at, end);
            at = end;
        } else if rest.starts_with(b"/*") {
            let mut depth = 0usize;
            let mut end = at;
            while end < src.len() {
                if src[end..].starts_with(b"/*") {
                    depth += 1;
                    end += 2;
                } else if src[end..].starts_with(b"*/") {
                    depth -= 1;
                    end += 2;
                    if depth == 0 {
                        break;
                    }
                } else {
                    end += 1;
                }
            }
            blank(&mut out, at, end);
            at = end;
        } else if src[at] == b'r'
            && (at == 0
                || !ident(src[at - 1])
                || (matches!(src[at - 1], b'b' | b'c') && (at < 2 || !ident(src[at - 2]))))
            && {
                let hashes = rest[1..].iter().take_while(|&&byte| byte == b'#').count();
                rest.get(1 + hashes) == Some(&b'"')
            }
        {
            let hashes = rest[1..].iter().take_while(|&&byte| byte == b'#').count();
            let body = at + 2 + hashes;
            let mut closing = vec![b'"'];
            closing.extend(std::iter::repeat_n(b'#', hashes));
            let end = src[body..]
                .windows(closing.len())
                .position(|window| window == closing.as_slice())
                .map_or(src.len(), |end| body + end);
            blank(&mut out, body, end);
            at = end + closing.len();
        } else if src[at] == b'"' {
            let mut end = at + 1;
            while end < src.len() && src[end] != b'"' {
                if src[end] == b'\\' {
                    end += 1;
                }
                end += 1;
            }
            blank(&mut out, at + 1, end);
            at = end + 1;
        } else if src[at] == b'\'' {
            // A character literal, or else a lifetime or a label.
            let end = match rest.get(1) {
                Some(b'\\') => rest
                    .iter()
                    .skip(3)
                    .position(|&byte| byte == b'\'')
                    .map(|end| at + 3 + end),
                Some(_) => source[at + 1..]
                    .chars()
                    .next()
                    .map(|c| at + 1 + c.len_utf8())
                    .filter(|&end| src.get(end) == Some(&b'\'')),
                None => None,
            };
            match end {
                Some(end) => {
                    blank(&mut out, at + 1, end);
                    at = end + 1;
                }
                None => at += 1,
            }
        } else {
            at += 1;
        }
    }
    String::from_utf8(out).expect("masking replaces whole characters with ASCII spaces")
}

/// What the full name of a test at the top level of `file`, in the library
/// at `dir`, starts with: its module path and `::`, or nothing in the
/// crate's root.
fn test_prefix(file: &str, dir: &str) -> String {
    match module_path(file, dir) {
        module if module.is_empty() => module,
        module => format!("{module}::"),
    }
}

/// The module path of `file` in the library at `dir`, such as
/// `zakura::wire_codec::tests` for `<dir>/src/zakura/wire_codec/tests.rs`.
pub fn module_path(file: &str, dir: &str) -> String {
    let relative = file
        .strip_prefix(dir)
        .and_then(|rest| rest.strip_prefix("/src/"))
        .unwrap_or(file);
    let relative = relative.strip_suffix(".rs").unwrap_or(relative);
    if relative == "lib" {
        return String::new();
    }
    relative
        .strip_suffix("/mod")
        .unwrap_or(relative)
        .replace('/', "::")
}

/// The source files of the crate whose root file is `root_file`, both
/// relative to `root`, each with what the full names of the tests at its
/// top level start with: nothing for `root_file`, and the module path and
/// `::` for each file a `mod name;` declaration reaches, as rustc resolves
/// it, `#[path]` included. Also the problems: each declared module whose
/// file doesn't exist. The walk reads declarations line by line, so a
/// declaration inside an inline module resolves as if it were outside, and
/// fails if that file doesn't exist.
pub fn crate_files(root: &Path, root_file: &str) -> (Vec<(String, String)>, Vec<String>) {
    let mut files = Vec::new();
    let mut problems = Vec::new();
    let mut seen = BTreeSet::new();
    // (file, prefix, whether its child modules' files sit beside it, as a
    // crate root's, a `mod.rs`'s and a `#[path]` file's do).
    let mut stack = vec![(PathBuf::from(root_file), String::new(), true)];
    while let Some((file, prefix, beside)) = stack.pop() {
        if !seen.insert(file.clone()) {
            continue;
        }
        let source = match fs::read_to_string(root.join(&file)) {
            Ok(source) => source,
            Err(error) => {
                problems.push(format!("reading {}: {error}", file.display()));
                continue;
            }
        };
        let parent = file.parent().unwrap_or(Path::new("")).to_path_buf();
        let children = match beside || file.file_name().is_some_and(|name| name == "mod.rs") {
            true => parent.clone(),
            false => parent.join(file.file_stem().unwrap_or_default()),
        };
        for (name, path) in module_declarations(&source) {
            let candidates = match path {
                Some(path) => vec![(parent.join(path), true)],
                None => vec![
                    (children.join(format!("{name}.rs")), false),
                    (children.join(&name).join("mod.rs"), false),
                ],
            };
            match candidates
                .iter()
                .find(|(candidate, _)| root.join(candidate).is_file())
            {
                Some((found, beside)) => {
                    stack.push((found.clone(), format!("{prefix}{name}::"), *beside));
                }
                None => problems.push(format!(
                    "{} declares `mod {name};`, and {} doesn't exist",
                    file.display(),
                    candidates
                        .iter()
                        .map(|(candidate, _)| candidate.display().to_string())
                        .collect::<Vec<_>>()
                        .join(" or ")
                )),
            }
        }
        files.push((file.to_string_lossy().into_owned(), prefix));
    }
    files.sort();
    (files, problems)
}

/// The `mod name;` declarations in `source`, outside comments, each with
/// its `#[path = "..."]` attribute's value, if it has one.
fn module_declarations(source: &str) -> Vec<(String, Option<String>)> {
    let mut declarations = Vec::new();
    let mut path = None;
    for line in source.lines().map(str::trim) {
        if line.is_empty() || line.starts_with("//") {
            continue;
        }
        // Outer attributes, on their own lines or before the item.
        let mut rest = line;
        while let Some((attribute, after)) = rest
            .strip_prefix("#[")
            .and_then(|attribute| attribute.split_once(']'))
        {
            if attribute.starts_with("path") {
                path = attribute.split('"').nth(1).map(str::to_owned);
            }
            rest = after.trim_start();
        }
        if rest.is_empty() {
            continue;
        }
        if let Some(name) = module_declaration(rest) {
            declarations.push((name.to_owned(), path.clone()));
        }
        path = None;
    }
    declarations
}

/// The module `line` declares, if it is `mod name;` with any visibility.
fn module_declaration(line: &str) -> Option<&str> {
    let line = match line.strip_prefix("pub") {
        Some(rest) if rest.starts_with('(') => rest.split_once(')')?.1,
        Some(rest) => rest,
        None => line,
    };
    let (name, _) = line.trim_start().strip_prefix("mod ")?.split_once(';')?;
    let name = name.trim();
    (!name.is_empty() && name.chars().all(|c| c.is_alphanumeric() || c == '_')).then_some(name)
}

/// Every file with `extension` under `root`'s `dir`, relative to `root`,
/// sorted.
fn files(root: &Path, dir: &str, extension: &str) -> Vec<String> {
    let mut files = Vec::new();
    let mut stack = vec![root.join(dir)];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().and_then(|e| e.to_str()) == Some(extension) {
                files.push(relative(root, &path));
            }
        }
    }
    files.sort();
    files
}

/// `path` relative to `root`, with `/` separators, or as given if it isn't
/// under `root`.
fn relative(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .into_owned()
}

/// The directories in `dir`, by name, sorted.
fn sorted_dirs(dir: &Path) -> io::Result<Vec<String>> {
    let mut dirs = Vec::new();
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_dir() {
            if let Some(name) = path.file_name() {
                dirs.push(name.to_string_lossy().into_owned());
            }
        }
    }
    dirs.sort();
    Ok(dirs)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Each ignored test is the next function after its attribute.
    #[test]
    fn ignored_tests_take_the_next_function() {
        let source = "#[test]\nfn plain() {}\n\n#[test]\n#[ignore = \"slow\"]\n\
                      /// Docs.\nfn slow() {}\n#[ignore]\n#[tokio::test]\nasync fn \
                      later() {}\nlet s = \"#[ignore]\";\n";
        let scan = scan_tests(source);
        assert_eq!(
            scan.ignored,
            [("slow".to_owned(), true), ("later".to_owned(), true)]
        );
        assert_eq!(scan.tests, 3);
        assert!(scan.unclassified.is_empty());
        let nested = "mod tests {\n    #[test]\n    #[ignore]\n    fn inner() {}\n}\n";
        assert_eq!(scan_tests(nested).ignored, [("inner".to_owned(), false)]);
    }

    /// Every form of `ignore` either marks the next test or fails the
    /// scan, and literals and comments never do either.
    #[test]
    fn ignore_attributes_the_scan_cant_classify_fail() {
        let source = "#[test] #[ignore]\nfn same_line() {}\n\
                      #[cfg(feature = \"slow\")]\n#[test]\n#[ignore = \"a ] in the reason\"]\n\
                      pub(crate) async fn gated() {}\n\
                      #[cfg_attr(zakura_sim, ignore)]\n#[test]\nfn sim_only() {}\n\
                      #[test]\n#[cfg_attr(\n    not(zakura_sim),\n    ignore = \"slow\"\n)]\n\
                      fn split() {}\n\
                      #[ignore]\nstruct NotATest;\n\
                      /* #[cfg_attr(x, ignore)] /* nested */ */\n\
                      const C: char = '\"';\nconst Q: char = '\\'';\n\
                      fn f<'a>(s: &'a str) {}\n\
                      const R: &str = r#\"#[cfg_attr(x, ignore)]\"#;\n\
                      #[allow(clippy::ignored_unit_patterns)]\nfn g() {}\n";
        let scan = scan_tests(source);
        assert_eq!(
            scan.ignored,
            [("same_line".to_owned(), true), ("gated".to_owned(), true)]
        );
        assert_eq!(scan.tests, 4);
        assert_eq!(
            scan.unclassified,
            [
                (7, "#[cfg_attr(zakura_sim, ignore)]".to_owned()),
                (
                    11,
                    "#[cfg_attr(\n    not(zakura_sim),\n    ignore = \"slow\"\n)]".to_owned()
                ),
                (16, "#[ignore]".to_owned()),
            ]
        );
    }

    /// A target can cover only a harness its harness composes, and only if
    /// its harness checks the covered harness's properties.
    #[test]
    fn a_composed_harness_must_be_checked() {
        use crate::harness::{entry, fixtures::All};
        let suite: SuiteFile =
            toml::from_str("schema = 1\nplanted-timeout-secs = 1\nseed-timeout-secs = 1\n")
                .unwrap();
        let mut builder = Builder {
            root: Path::new("."),
            canonical_root: PathBuf::from("."),
            suite: &suite,
            problems: Vec::new(),
            ids: BTreeSet::new(),
            owners: BTreeMap::new(),
        };
        // A wrong code-owned list: `fixtures/all` checks none of
        // `header-sync/wire`'s properties.
        let runs = HarnessEntry {
            composes: &["header-sync/wire"],
            ..entry::<All>()
        };
        builder.covered_by("t", &runs, &["header-sync/wire".to_owned()]);
        assert_eq!(builder.problems.len(), 1);
        assert!(
            builder.problems[0].contains("doesn't check `header-sync/wire`'s properties"),
            "{:?}",
            builder.problems
        );
    }

    /// A suite is a `proptest!` block or a suite macro's call, in code.
    #[test]
    fn property_suites_are_code_that_calls_a_suite() {
        let macros = ["message_suite".to_owned()];
        assert!(is_property_suite("proptest! {\n}", &macros));
        assert!(is_property_suite(
            "message_suite!(family, Message);",
            &macros
        ));
        assert!(!is_property_suite(
            "//! message_suite!(family, Message);",
            &macros
        ));
        assert!(!is_property_suite(
            "macro_rules! message_suite {\n proptest! {}\n}",
            &macros
        ));
        assert!(!is_property_suite("#[test]\nfn plain() {}", &macros));
    }

    /// `--frozen` needs a clean build, and a clean checkout at its commit.
    #[test]
    fn frozen_needs_a_clean_checkout_at_the_builds_commit() {
        let mut build = ManifestBuild::from(BuildInfo::current());
        build.git = "c0ffee".to_owned();
        build.dirty = false;
        assert_eq!(unfrozen_by(&build, Some("c0ffee\n"), Some("")), None);
        for (head, status, why) in [
            (Some("c0ffef"), Some(""), "the checkout is at c0ffef"),
            (
                Some("c0ffee"),
                Some(" M qa/suite.toml\n"),
                "M qa/suite.toml",
            ),
            (
                Some("c0ffee"),
                Some("?? qa/planted/x/y.patch\n"),
                "?? qa/planted",
            ),
            (None, Some(""), "git can't say"),
            (Some("c0ffee"), None, "git can't say"),
        ] {
            let problem = unfrozen_by(&build, head, status).unwrap_or_default();
            assert!(problem.contains(why), "{problem}");
        }
        build.dirty = true;
        assert!(unfrozen_by(&build, Some("c0ffee"), Some("")).is_some());
    }

    /// A crate's files are its root and the files its `mod name;`
    /// declarations reach, as rustc resolves them.
    #[test]
    fn crate_files_follow_module_declarations() {
        let dir = tempfile::TempDir::new().unwrap();
        for (file, text) in [
            (
                "tests/flat.rs",
                "//! mod commented;\nmod support;\n#[cfg(zakura_sim)] pub(crate) mod gated;\n",
            ),
            ("tests/support/mod.rs", "pub mod feed;\n"),
            ("tests/support/feed.rs", ""),
            (
                "tests/gated.rs",
                "#[path = \"elsewhere/x.rs\"]\nmod renamed;\n",
            ),
            ("tests/elsewhere/x.rs", ""),
            ("tests/nested/main.rs", "mod helpers;\nmod missing;\n"),
            ("tests/nested/helpers.rs", "mod deeper;\n"),
            ("tests/nested/helpers/deeper.rs", ""),
        ] {
            let path = dir.path().join(file);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, text).unwrap();
        }
        let owned = |files: &[(&str, &str)]| -> Vec<(String, String)> {
            files
                .iter()
                .map(|(file, prefix)| ((*file).to_owned(), (*prefix).to_owned()))
                .collect()
        };
        assert_eq!(
            crate_files(dir.path(), "tests/flat.rs"),
            (
                owned(&[
                    ("tests/elsewhere/x.rs", "gated::renamed::"),
                    ("tests/flat.rs", ""),
                    ("tests/gated.rs", "gated::"),
                    ("tests/support/feed.rs", "support::feed::"),
                    ("tests/support/mod.rs", "support::"),
                ]),
                Vec::new()
            )
        );
        let (files, problems) = crate_files(dir.path(), "tests/nested/main.rs");
        assert_eq!(
            files,
            owned(&[
                ("tests/nested/helpers.rs", "helpers::"),
                ("tests/nested/helpers/deeper.rs", "helpers::deeper::"),
                ("tests/nested/main.rs", ""),
            ])
        );
        assert_eq!(
            problems,
            [
                "tests/nested/main.rs declares `mod missing;`, and tests/nested/missing.rs or \
              tests/nested/missing/mod.rs doesn't exist"
            ]
        );
    }

    /// File paths become module paths.
    #[test]
    fn module_paths_follow_the_file_layout() {
        let dir = "crates/zakura-network";
        for (file, module) in [
            (
                "crates/zakura-network/src/zakura/wire_codec/tests.rs",
                "zakura::wire_codec::tests",
            ),
            (
                "crates/zakura-network/src/zakura/handshake.rs",
                "zakura::handshake",
            ),
            (
                "crates/zakura-network/src/zakura/discovery/mod.rs",
                "zakura::discovery",
            ),
        ] {
            assert_eq!(module_path(file, dir), module, "{file}");
        }
    }
}
