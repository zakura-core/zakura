//! The full-suite manifest (campaign.md, C1): every row the full property
//! suite runs, with its command, timeout and one expected outcome.
//!
//! `qa/suite.toml` names what the harness registry can't: which packages'
//! tests the suite runs, which ignored tests it runs on demand, which fuzz
//! target runs each harness, and which rows are planned but unimplemented.
//! [`manifest`] joins it with [`HARNESSES`], each harness's properties, the
//! coverage manifest, the retained planted cases, the committed seeds, the
//! fuzz packages' targets and the test sources. It returns the manifest, or
//! every problem that keeps the manifest from covering the suite:
//!
//! - a harness that no target runs and no `missing-target` row names;
//! - a fuzz target that `qa/suite.toml` neither maps nor excludes;
//! - an ignored test that `qa/suite.toml` doesn't list;
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
        HARNESSES,
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
    /// The test binaries that run. All of `tests/*.rs` when absent. When
    /// present, it must list every file in `tests/` that starts with
    /// `#![cfg(zakura_sim)]`.
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

/// A fuzz target the suite runs.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
pub struct Target {
    /// The fuzz package's directory under `qa/fuzz/`, such as `sim`.
    pub package: String,
    /// The target's `[[bin]]` name.
    pub name: String,
    /// The harness it runs, for a `sim` target.
    pub harness: Option<String>,
    /// Harnesses whose runs it covers through `harness`, such as the race
    /// fixtures through `fixtures/all`. Its report must account for each.
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
    /// [`Expect::Holds`] if `Active`, [`Expect::Reported`] if not.
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
    /// The harness it runs, for a `sim` target.
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
        suite,
        problems: Vec::new(),
        ids: BTreeSet::new(),
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
    suite: &'a SuiteFile,
    problems: Vec<String>,
    ids: BTreeSet<String>,
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
        for package in &self.suite.sim_package {
            let dir = format!("crates/{}", package.name);
            if !self.root.join(&dir).join("Cargo.toml").is_file() {
                self.problem(format!("sim package `{}` has no {dir}", package.name));
                continue;
            }
            if package.lib {
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
                for file in files(self.root, &format!("{dir}/src"), "rs") {
                    ignored.extend(self.ignored_in(&file));
                }
            }
            let found = test_binaries(&self.root.join(&dir).join("tests"));
            let binaries: Vec<String> = match &package.tests {
                None => found.keys().cloned().collect(),
                Some(listed) => {
                    for (name, sim_only) in &found {
                        if *sim_only && !listed.contains(name) {
                            self.problem(format!(
                                "{dir}/tests/{name}.rs builds only under `--cfg zakura_sim`, \
                                 and {SUITE_FILE} doesn't list it under `{}`",
                                package.name
                            ));
                        }
                    }
                    listed.clone()
                }
            };
            for name in binaries {
                let file = format!("{dir}/tests/{name}.rs");
                if !found.contains_key(&name) {
                    self.problem(format!("sim package `{}` has no {file}", package.name));
                    continue;
                }
                ignored.extend(self.ignored_in(&file));
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
                    file: Some(file),
                });
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
                ignored.extend(self.ignored_in(&file));
                for child in self::files(self.root, &children, "rs") {
                    ignored.extend(self.ignored_in(&child));
                }
                let module = module_path(&file, &dir);
                let features = features(&package.features);
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
                    let Some(selector) = self.test_selector(&entry.file) else {
                        continue;
                    };
                    let build = match selector.1 {
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
                                selector.0, selector.2, entry.test
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
        match fs::read_to_string(self.root.join(file)) {
            Ok(source) => ignored_tests(&source)
                .into_iter()
                .map(|(test, top_level)| ((file.to_owned(), test), top_level))
                .collect(),
            Err(error) => {
                self.problem(format!("reading {file}: {error}"));
                Vec::new()
            }
        }
    }

    /// The cargo selector of the test binary or library that holds `file`,
    /// the build it needs, and the module path its top-level tests' names
    /// start with: nothing in a simulator package's test binary, and the
    /// file's module in a library.
    fn test_selector(&mut self, file: &str) -> Option<(String, Build, String)> {
        for package in &self.suite.sim_package {
            let dir = format!("crates/{}/", package.name);
            let Some(rest) = file.strip_prefix(&dir) else {
                continue;
            };
            if let Some(name) = rest
                .strip_prefix("tests/")
                .and_then(|name| name.strip_suffix(".rs"))
                .filter(|name| !name.contains('/'))
            {
                return Some((
                    format!("-p {} --test {name}", package.name),
                    Build::Sim,
                    String::new(),
                ));
            }
            // A package whose library doesn't build in the simulator may
            // still be a property package.
            if rest.starts_with("src/") && package.lib {
                return Some((
                    format!("-p {} --lib", package.name),
                    Build::Sim,
                    test_prefix(file, &format!("crates/{}", package.name)),
                ));
            }
        }
        for package in &self.suite.property_package {
            if file.starts_with(&format!("crates/{}/src/", package.name)) {
                return Some((
                    format!("-p {}{} --lib", package.name, features(&package.features)),
                    Build::Ordinary,
                    test_prefix(file, &format!("crates/{}", package.name)),
                ));
            }
        }
        self.problem(format!("no package row runs {file}"));
        None
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
    /// package's lockfile digest. Every `[[bin]]` of every fuzz package
    /// must be mapped or excluded.
    fn targets(&mut self) -> (Vec<TargetRow>, Vec<ExcludedRow>, BTreeMap<String, String>) {
        let mut bins = BTreeSet::new();
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
            let manifest = dir.join(&package).join("Cargo.toml");
            let Ok(text) = fs::read_to_string(&manifest) else {
                continue;
            };
            match toml::from_str::<FuzzManifest>(&text) {
                Ok(parsed) => {
                    for bin in parsed.bin {
                        bins.insert((package.clone(), bin.name));
                    }
                }
                Err(error) => self.problem(format!("{FUZZ_DIR}/{package}/Cargo.toml: {error}")),
            }
            let lockfile = dir.join(&package).join("Cargo.lock");
            lockfiles.insert(
                package.clone(),
                fs::read(&lockfile).map_or_else(|_| "none".to_owned(), |bytes| sha256(&bytes)),
            );
        }
        let mut mapped = BTreeSet::new();
        let mut rows = Vec::new();
        for target in &self.suite.target {
            let key = (target.package.clone(), target.name.clone());
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
            for harness in target.harness.iter().chain(&target.covers) {
                if !HARNESSES.iter().any(|entry| entry.name == harness) {
                    self.problem(format!(
                        "the target `{}` names the harness `{harness}`, which isn't registered",
                        target.name
                    ));
                }
            }
            if target.harness.is_none() && !target.covers.is_empty() {
                self.problem(format!(
                    "the target `{}` covers harnesses without running one",
                    target.name
                ));
            }
            let id = self.id(format!("target/{}/{}", target.package, target.name));
            let build = match target.package.as_str() {
                "sim" => Build::FuzzSim,
                _ => Build::Fuzz,
            };
            let corpus = match &target.harness {
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
                harness: target.harness.clone(),
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
    let git = |args: &[&str]| {
        Command::new("git")
            .arg("-C")
            .arg(root)
            .args(args)
            .output()
            .ok()
            .filter(|output| output.status.success())
            .and_then(|output| String::from_utf8(output.stdout).ok())
    };
    let head = git(&["rev-parse", "HEAD"]);
    let status = git(&["status", "--porcelain", "--untracked-files=all"]);
    unfrozen_by(build, head.as_deref(), status.as_deref())
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

/// A fuzz package's `Cargo.toml`, as far as the manifest reads it.
#[derive(Deserialize)]
struct FuzzManifest {
    /// Its targets.
    #[serde(default)]
    bin: Vec<FuzzBin>,
}

/// One `[[bin]]` of a fuzz package.
#[derive(Deserialize)]
struct FuzzBin {
    /// The target's name.
    name: String,
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

/// The tests in `source` that carry an `#[ignore` attribute: for each
/// line that starts with one, the next function's name, and whether the
/// attribute is unindented, so the test sits at the file's top level.
pub fn ignored_tests(source: &str) -> Vec<(String, bool)> {
    let raw: Vec<&str> = source.lines().collect();
    let lines: Vec<&str> = raw.iter().map(|line| line.trim()).collect();
    let mut tests = Vec::new();
    for (at, line) in lines.iter().enumerate() {
        if !line.starts_with("#[ignore") {
            continue;
        }
        let top_level = raw[at].starts_with("#[ignore");
        let name = lines[at + 1..]
            .iter()
            .find_map(|line| {
                ["fn ", "async fn ", "pub fn ", "pub async fn "]
                    .iter()
                    .find_map(|prefix| line.strip_prefix(prefix))
            })
            .and_then(|rest| rest.split(['(', '<']).next())
            .map(str::to_owned);
        if let Some(name) = name {
            tests.push((name, top_level));
        }
    }
    tests
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

/// The test binaries in `dir`, `tests/*.rs`, by name, each with whether it
/// starts with `#![cfg(zakura_sim)]`.
fn test_binaries(dir: &Path) -> BTreeMap<String, bool> {
    let mut binaries = BTreeMap::new();
    let Ok(entries) = fs::read_dir(dir) else {
        return binaries;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_file() || path.extension().and_then(|e| e.to_str()) != Some("rs") {
            continue;
        }
        let Some(name) = path.file_stem().map(|s| s.to_string_lossy().into_owned()) else {
            continue;
        };
        let sim_only = fs::read_to_string(&path).is_ok_and(|source| {
            source
                .lines()
                .map(str::trim)
                .find(|line| line.starts_with("#!["))
                .is_some_and(|line| line == "#![cfg(zakura_sim)]")
        });
        binaries.insert(name, sim_only);
    }
    binaries
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
        assert_eq!(
            ignored_tests(source),
            [("slow".to_owned(), true), ("later".to_owned(), true)]
        );
        let nested = "mod tests {\n    #[test]\n    #[ignore]\n    fn inner() {}\n}\n";
        assert_eq!(ignored_tests(nested), [("inner".to_owned(), false)]);
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
