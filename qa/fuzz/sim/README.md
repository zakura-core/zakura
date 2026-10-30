# Simulation fuzzing

This isolated `cargo-fuzz` package fuzzes `zakura-sim` harnesses. It isn't a
workspace member. Each input is a harness's scenario prefix, then the bytes
the simulator's `Plan` decodes into scheduling choices.

The package needs `RUSTFLAGS="--cfg zakura_sim"`. Without it, the build fails
with a `compile_error!`, rather than compiling a target that runs nothing.
Run a target from this directory with the pinned nightly toolchain, and pass
`--fuzz-dir .`:

```console
RUSTFLAGS="--cfg zakura_sim" cargo +nightly-2026-07-15 fuzz run --fuzz-dir . discovery_wait_complete
```

The default build uses AddressSanitizer with `zakura-scheduler`'s `sanitizer`
feature, which tells ASan about each coroutine stack switch. A build without
a sanitizer must turn the feature off:

```console
RUSTFLAGS="--cfg zakura_sim" cargo +nightly-2026-07-15 fuzz run --fuzz-dir . -s none --no-default-features race_fixtures
```

## Targets

- `discovery_wait_complete` runs discovery's `wait_complete` harness.
- `header_sync_storage` runs `header-sync/storage`. Its 344-byte prefix
  encodes the scenario in the action grammar of
  `zakura_sim::harness::header_sync::grammar`. A prefix whose first byte
  isn't the harness's generator, 2, runs the default scenario. Pass
  `-max_len=4096`, the input size the grammar is built for.
- `race_fixtures` runs the six race fixtures through `fixtures::All`, whose
  three-byte prefix picks the fixture, the variant and the worker count.
- `feedback_probe` runs `explore::feedback::Probe`, whose runs differ only
  in data, with a collector that turns each run's end time into a feature.
  It checks the feedback transport and searches for nothing.

## Semantic feedback

A target with a collector, `sim_target!(Harness, collector)`, adds abstract
protocol states to libFuzzer's code edges. The collector writes one counter
per feature into the `__libfuzzer_extra_counters` section. libFuzzer clears
the section before each input and reads each nonzero counter as a feature,
so an input that reaches a new state through covered code still joins the
corpus. The section works on Linux only.

`ZAKURA_SIM_FEEDBACK=off` moves the collector's writes to counters libFuzzer
doesn't read. The collector still runs, so `on` and `off` runs take the same
code path and differ only in what libFuzzer reads. Each target with a
collector logs its backend, version, feature count and mode at startup.

`cargo xtask sim-feedback` builds `feedback_probe` and runs libFuzzer's
corpus merge with feedback `off` and `on`. With feedback off, an input whose
code edges another input already covered adds nothing. With feedback on,
libFuzzer adds it for its one new feature.

`[package.metadata.zakura-sim.targets]` in `Cargo.toml` maps each target to
the registered harness it runs. Each entry there needs a `[[bin]]`.
`cargo xtask sim-minimize` minimizes only a target with an entry.
`feedback_probe` runs no registered harness, so it has no entry.

## What a target does with a run

- An `Active` property violation saves the input under
  `inputs/active/<property-id>/<sha256>.bin` before anything reruns it, then
  records the run and panics with the property ID and the trace's directory.
  A rerun that disagrees with the run panics with `nondeterministic:`
  instead, keeps the compared runs' records and property reports and their
  first difference under `nondeterministic/<property-id>/<12 hex>/`, and
  moves the input to `inputs/nondeterministic/<property-id>/`.
- A harness failure panics with `harness:`. If the run also violated an
  `Active` property, the target first saves that violation's input and
  trace as it does for a `Known` one, under `inputs/active/` and `traces/`.
- A task's panic violates `sim/no-task-panic`, which is `Active`. Each target
  swaps libFuzzer's panic hook, which aborts at any panic, for the default one
  at startup, so the task's panic reaches the scheduler.
- Each `Known` and `Draft` violation saves one trace per property under
  `known/` or `draft/`, keeps each distinct input under
  `inputs/<bucket>/<property-id>/`, up to 256 per property per process, and
  the target goes on.
- A run whose shutdown abandoned tasks saves its input under
  `inputs/abandoned/`, whatever else it found, and retires the thread it ran
  on. Each retired thread keeps what its leaked tasks hold, so once a process
  has abandoned `ZAKURA_SIM_ABANDON_LIMIT` runs, 1 by default, the target
  panics with `abandoned:`, and the process's supervisor restarts it.

Traces and inputs go under `$ZAKURA_SIM_ARTIFACTS`, or under
`$TMPDIR/zakura-sim/` when the variable is unset. Traces go to
`traces/<property-id>/<12 hex>/`.

`race_fixtures` expects failures, so it acts differently. A planted variant's
violation of its own fixture's property is expected, and the target logs the
first find of each fixture's `Base` variant. A `Fixed` variant's failure, or a
failure of another property, panics. Once a fixture's limit in
`fixtures::EXPECTED` passes without its `Base` find, the target panics with
`missed:`.

`ZAKURA_SIM_EXPECT=<property-id>` makes a target panic only on that
property's `Active` violation and record nothing, for `cargo fuzz tmin`.
Before it panics, the target prints `zakura-sim-fuzz harness: <name>`. An
abort, a stack overflow, a sanitizer report or a timeout still crashes the
process, so `tmin`'s output is only a candidate.

## Minimizing a crash

`cargo xtask sim-minimize` runs `tmin` with that variable set, and keeps a
smaller input only if fresh processes confirm it. Each budget is required:

```console
cargo xtask sim-minimize race_fixtures artifacts/race_fixtures/crash-… \
    --property race/lost-update --out <dir> --runs 64 --wall-secs 300 --child-secs 30
```

The wrapper copies the original into `<dir>/original/` and checks its
SHA-256 first. It builds `zakura-sim` and the target once, and pins both in
`<dir>/bin/`. `zakura-sim properties` must list `--property` for the
harness, or the run ends with `error` before any check. The original and
each smaller candidate then pass three fresh processes, or the input is
rejected:

1. the pinned target must crash with the property's panic alone, not an
   abort, a sanitizer report, a timeout or another panic, and must name the
   harness the manifest maps it to;
2. `zakura-sim confirm` must find the property violated alone, through its
   trigger history, with the original's harness, generator, build,
   assumptions and cleanup;
3. `zakura-sim replay` must replay the recorded trace Exact.

Candidates run smallest first, and the first that passes moves to
`<dir>/minimized/` with its trace. `<dir>/report.json` gives the status
(`minimized`, `original-kept`, `original-rejected`, `interrupted`,
`build-failed` or `error`), each candidate's rejection reason, the counts,
the budgets, the elapsed time and whether `tmin` reached its fixed point.
`error` means the wrapper itself failed, and its reason is the error. The
report keeps what ran before the failure. The command exits 0 only for
`minimized`. Ctrl-C, SIGTERM and SIGHUP stop the run
and still write the report.

Reproduce a crash input as a trace with the `zakura-sim` binary:

```console
RUSTFLAGS="--cfg zakura_sim" cargo run -p zakura-sim -- record --sim discovery/wait-complete --input artifacts/discovery_wait_complete/crash-… --out <dir>
```

## Triage

`cargo xtask sim-triage` confirms each saved input's failure in fresh
processes, and archives the input with its evidence:

```console
cargo xtask sim-triage --sim discovery/wait-complete --out <archive> [--child-secs 60] artifacts/discovery_wait_complete/crash-…
cargo xtask sim-verify-archive <archive>
```

For each input, triage copies the input into a staging bundle before any
child starts, and never changes the input itself. It then records the input
twice, each time in a fresh `zakura-sim record` process that it kills after
`--child-secs`, and classifies each recording from its exit, its standard
error and its trace. It compares the two traces if either wrote its `end`
record, and counts a trace that doesn't read as a difference. If both recordings filed the input alike, their traces match, and both
ended without failing, a third process replays the first trace Exact. Each
input gets one status:

- `confirmed`: both recordings filed the input under the same buckets and
  signature, their traces match, and that is a failure.
- `not-reproduced`: both recordings ran clean, and the replay matched the
  first one Exact.
- `nondeterministic`: the two recordings filed the input differently, or
  their traces differ. `triage.json` names the first record where they
  differ, or the recording whose trace doesn't read.
- `replay-failed`: the recordings agreed, but the replay didn't match the
  first one Exact.

Each bundle goes under `<harness>/<provenance>/`, where `<harness>` is the
harness's name and `<provenance>` is a hash of its generator and the
`zakura-sim` build that triaged it. A clean build's hash leaves out its
executable's hash, so equivalent clean builds of one source share it. Below
that, a `confirmed` bundle goes to `<bucket>/<signature hash>/<input sha256>/`,
and a bundle with any other status goes to `<status>/<input sha256>/`. So the
same bytes on another harness, generator or build keep their own bundle. A
bundle holds `input.bin`, `scenario.json`, each child's output and trace
under `runs/`, `triage.json` and `SHA256SUMS`. `triage.json` names the
harness, the generator, the provenance hash and the build. Triage writes the
bundle under `.staging/` and renames it into place, and never changes it
after that. A later triage of the same input on the same harness, generator
and build files it in the same place. It keeps the bundle that's there if that
bundle matches its `SHA256SUMS` and its `triage.json` names the same result.
Otherwise, as with a stale or misplaced copy, it keeps both, and names its own
bundle `<input sha256>-<n>/` with the first free `n`.
`sim-verify-archive` checks every bundle against its `SHA256SUMS`, and counts
the bundles under each harness, provenance and signature.

`cargo-fuzz` creates each target's `corpus/` and `artifacts/` directories,
which Git ignores. Don't commit corpus files.

## Seeds

Curated seeds live in `crates/zakura-sim/tests/fixtures/` as `<name>.bin`
with a `<name>.toml` sidecar. The sidecar records the harness, the generator
version, the file's SHA-256, the SHA-256 of the scenario the seed decodes to,
the test or plan the seed comes from, and the property witnesses it carries.
A `kind = "plan"` seed holds Plan bytes alone, and its `prefix` gives the
scenario prefix, such as a saved Plan's tag, that encodes its scenario.

`cargo xtask sim-seeds` validates every seed against the current generators.
A required seed that fails, such as one built for an older generator or one
whose prefix now decodes to another scenario, fails the command with the
reason. A seed with `required = false` is quarantined instead. With `--sim`
and `--out`, it writes one harness's admitted seeds into a corpus, each named
by its SHA-256:

```console
cargo xtask sim-seeds --sim discovery/wait-complete --out qa/fuzz/sim/corpus/discovery_wait_complete
```

`cargo xtask sim-versions --sim <name>` prints the versions that fix what the
harness's fuzz input means, as `KEY=VALUE` lines. A corpus keyed on all of
them starts fresh when any of them changes.
