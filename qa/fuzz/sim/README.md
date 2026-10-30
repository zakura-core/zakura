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
  instead, keeps both compared records and their first difference under
  `nondeterministic/<property-id>/<12 hex>/`, and moves the input to
  `inputs/nondeterministic/<property-id>/`.
- A harness failure panics with `harness:`.
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

## Campaigns

A campaign runs one target for a long time, on a host that may stop. Its
state lives in one store directory, on durable storage. The store is the only
authoritative copy; a CI cache can speed a run up but never holds the only
copy. To move a campaign to another host, copy the whole store there. Main
holds the tooling and this documentation. The store, host addresses,
credentials, worker counts, durations, logs and failure bundles stay outside
main.

`cargo xtask sim-campaign` runs `zakura-sim campaign`. Its subcommands:

- `build` builds a target with the pinned nightly and cargo-fuzz, and saves
  the binary in the store with a build record: the commit, whether the tree
  was dirty, the source and lockfile digests, `rustc`, cargo-fuzz and
  `libfuzzer-sys` versions, `RUSTFLAGS` and the sanitizer. The build is named
  by the binary's SHA-256.
- `start` writes a run manifest and runs it. The manifest records the build,
  the `zakura-sim` binary that wrote it, the harness's versions, properties
  and simulator limits, the feedback line the target logs at startup (none
  for a target without a collector), the suite manifest's digest, each
  worker's libFuzzer seed and CPUs, the budget and libFuzzer flags, and the
  initial snapshot, which holds the harness's curated seeds. Each worker runs
  libFuzzer on its own corpus directory plus the initial snapshot, and
  restarts a process that ends early, such as at a crash or the abandonment
  limit, up to `--max-restarts` times. When every worker stops, the run saves
  the union of the initial snapshot and the workers' corpora as a new
  snapshot, compacts it with libFuzzer's merge if `--compact-secs` is set,
  and writes the end record.
- `resume` continues the last run. If that run has no end record, because
  its supervisor or host stopped, `resume` first closes it. It checks the
  run's snapshot and failure files against their digests, then runs a new
  manifest from that snapshot, with `resume_parent` naming the run. It
  refuses a build with another identity, another harness version, or other
  feedback.
- `close` ends a run whose supervisor stopped: it saves the snapshot from
  what the workers left and records the run as `interrupted`.
- `status` lists the runs, and `verify` checks every record in the store.

Every run ends with exactly one end record, `runs/<manifest>/end.json`, with
one of three statuses:

- `complete`: the supervisor ran every worker to the end of its budget.
- `worker-stopped`: at least one worker stopped at its restart limit.
- `interrupted`: the supervisor stopped first, and `close` wrote the record.

Findings don't change the status. The end record lists every file the workers
left in their libFuzzer artifact directories and their `ZAKURA_SIM_ARTIFACTS`
directories, by SHA-256, for triage.

Snapshots, manifests and builds are written to temporary names, synced, then
renamed or linked under their digests, and never change. A run never deletes
its initial snapshot or a worker's files, so an interrupted run, merge or
compaction loses nothing that a snapshot already held.

A new commit, generator, format, contract or feedback version starts a new
campaign segment. `start --parent <run> --corpus carry` carries the last
snapshot over when the harness's versions didn't change.
`--corpus seeds` starts from the curated seeds alone. Either way the previous
segment's manifests and snapshots stay in the store.

```console
# Build the target and start a campaign of 4 workers for 12 hours each.
cargo xtask sim-campaign build --store /mnt/campaign --target discovery_wait_complete
cargo xtask sim-campaign start --store /mnt/campaign --campaign discovery-2026-11 \
    --sim discovery/wait-complete --build <sha256> --workers 4 --seconds 43200 \
    --compact-secs 600 --artifacts <uri>

# After the host or the supervisor stopped: check the store and continue.
cargo xtask sim-campaign verify --store /mnt/campaign
cargo xtask sim-campaign resume --store /mnt/campaign

# On a new host, rebuild the same commit and resume with that build.
cargo xtask sim-campaign build --store /mnt/campaign --target discovery_wait_complete
cargo xtask sim-campaign resume --store /mnt/campaign --build <sha256>

# Record a saved failure as a trace, and replay it Exact.
cargo xtask record --sim discovery/wait-complete \
    --input /mnt/campaign/runs/<run>/workers/0/artifacts/crash-… --out <dir>
cargo xtask replay <dir>
```

Minimization (`cargo xtask sim-minimize`, T2b) and archive bundles
(`cargo xtask sim-triage`, E4a) are proposed in their own PRs, and aren't part
of this tooling yet.
