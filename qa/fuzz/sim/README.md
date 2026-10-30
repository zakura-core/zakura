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

`[package.metadata.zakura-sim.targets]` in `Cargo.toml` maps each target to
the registered harness it runs. A new target needs both its `[[bin]]` and its
entry there.

## What a target does with a run

- An `Active` property violation records the run, then panics with the
  property ID and the trace's directory. A rerun that disagrees with the run
  panics with `nondeterministic:` instead.
- A harness failure panics with `harness:`.
- A task's panic violates `sim/no-task-panic`, which is `Active`. Each target
  swaps libFuzzer's panic hook, which aborts at any panic, for the default one
  at startup, so the task's panic reaches the scheduler.
- Each `Known` and `Draft` violation saves one trace per property under
  `known/` or `draft/`, and the target goes on.
- A run whose shutdown abandoned tasks retires the thread it ran on.

Traces go to `$ZAKURA_SIM_ARTIFACTS/traces/<property-id>/<12 hex>/`, or under
`$TMPDIR/zakura-sim/` when the variable is unset.

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

`cargo-fuzz` creates each target's `corpus/` and `artifacts/` directories,
which Git ignores. Don't commit corpus files.
