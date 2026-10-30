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
- `race_fixtures` runs the six race fixtures through `fixtures::All`, whose
  three-byte prefix picks the fixture, the variant and the worker count.

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
property's `Active` violation and record nothing, for `cargo fuzz tmin`. An
abort, a stack overflow, a sanitizer report or a timeout still crashes the
process, so `tmin`'s output is only a candidate.

Reproduce a crash input as a trace with the `zakura-sim` binary:

```console
RUSTFLAGS="--cfg zakura_sim" cargo run -p zakura-sim -- record --sim discovery/wait-complete --input artifacts/discovery_wait_complete/crash-… --out <dir>
```

`cargo-fuzz` creates each target's `corpus/` and `artifacts/` directories,
which Git ignores. Don't commit corpus files.
