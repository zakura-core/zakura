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

### Minimized bundles

With `--target`, triage also minimizes each `confirmed` failure in the
`active` bucket with `cargo xtask sim-minimize`, as
[Minimizing a crash](#minimizing-a-crash) describes. It needs
`--runs` and `--wall-secs`, takes `--sanitizer`, and passes `--child-secs`
on:

```console
cargo xtask sim-triage --sim fixtures/all --out <archive> --target race_fixtures \
    --runs 64 --wall-secs 300 --child-secs 30 artifacts/race_fixtures/crash-…
```

The failure's bundle is in place before the minimizer starts, and the
minimizer reads that bundle's `input.bin`. So a minimizer that fails or is
killed never costs the failure its bundle, and the failure's bundle never
changes. The minimizer writes under `.staging/`, with no time limit from
triage: its own budgets bound `tmin` and each check. The minimizer rebuilds
`zakura-sim` in the target directory triage runs from, so before the first
input triage copies its own binary under `.staging/`, and every input's
`record` and `replay` children run that copy. Triage refuses to start if the
copy's SHA-256 isn't the running build's, and removes the copy when it exits.
If its report says
`minimized`, triage copies its output, without the pinned binaries in
`bin/`, into a new bundle, and holds it to the failure's bundle:

- the report must name the bundle's harness, property and input;
- the original's and the candidate's `zakura-sim confirm` records must be
  `confirmed`, on the bundle's harness, generator and property, each on its
  own input, and on a build equivalent to the bundle's;
- held to the original's confirmation again, the candidate's must stay
  `confirmed`: the same property, assumptions with their parameters, and
  cleanup;
- the candidate's `input.bin` must hash to its digest and be smaller than
  the original, and its trace must read with that input;
- a fresh `zakura-sim replay` must replay the candidate's trace Exact.

If any of that fails, or the minimizer kept no candidate, triage prints why
and leaves the minimizer's output under `.staging/`, without the pinned
binaries, whose SHA-256 its report keeps. Otherwise the new
bundle goes beside the failure's bundle as `<bundle name>.minimized/`, or
`<bundle name>.minimized-<n>/` with the first free `n`. It holds the
candidate as `input.bin`, the minimizer's output under `minimizer/`, the
minimizer's and the replay's output under `runs/`, `minimized.json` and
`SHA256SUMS`. `minimized.json` names the failure's bundle, its input and the
SHA-256 of its `SHA256SUMS`. A minimized bundle never changes after its
rename. A later triage of the same input keeps the first minimized bundle
that verifies, and doesn't run the minimizer. If none verifies, it minimizes
again, and puts its own beside the others. Only a confirmed `active` failure
is minimized, since the fuzz target's filter selects only an `Active`
property.

`sim-verify-archive` also holds each minimized bundle to its failure's
bundle: its name must be that bundle's name with `.minimized`, that bundle's
`SHA256SUMS` must be the one it was minimized from, and its files must pass
every check above except the replay. Every field of `minimized.json` but
`child_timeout_ms` and `build` must match what those files and the failure's
bundle give. It counts minimized bundles on their own line.

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
  the binary in the store with a build record: the harness the target runs,
  from `[package.metadata.zakura-sim.targets]` in `Cargo.toml`, the commit,
  whether the tree was dirty, the source and lockfile digests, `rustc`, cargo-fuzz and
  `libfuzzer-sys` versions, `RUSTFLAGS` and the sanitizer. The build is named
  by the binary's SHA-256.
- `start` writes a run manifest and runs it. It refuses a build whose target
  runs another harness than `--sim`. The manifest records the build,
  the `zakura-sim` binary that wrote it, the harness's versions, properties
  and simulator limits, the feedback line the target logs at startup (none
  for a target without a collector), the suite manifest's digest, each
  worker's libFuzzer seed and CPUs, the budget, libFuzzer flags and sanitizer
  variables such as `ASAN_OPTIONS`, and the
  initial snapshot, which holds the harness's curated seeds. Each worker runs
  libFuzzer on its own corpus directory plus the initial snapshot, and
  restarts a process that ends early, such as at a crash or the abandonment
  limit, up to `--max-restarts` times. Each process's log keeps the first and
  last MiB of its output. When every worker stops, the run saves
  the union of the initial snapshot and the workers' corpora as a new
  snapshot, compacts it with libFuzzer's merge if `--compact-secs` is set,
  and writes the end record.
- `resume` continues the last run. If that run has no end record, because
  its supervisor or host stopped, `resume` first closes it. It checks the
  run's snapshot and failure files against their digests, and checks that
  the end record lists every failure file in the run's directory and that
  its snapshot holds every input the workers added, then runs a new
  manifest from that snapshot, with `resume_parent` naming the run. It
  refuses a build with another identity, another harness version, or other
  feedback.
- `close` ends a run whose supervisor stopped: it saves the snapshot from
  what the workers left and records the run as `interrupted`. It refuses
  while a worker's fuzzer or the compaction's merge still runs. The merge
  runs in its own process group, which the supervisor kills at the
  compaction's time limit. A supervisor killed first leaves the merge
  running; stop it with `kill -- -<pid>`.
- `status` lists the runs, and `verify` checks every record in the store,
  and each end record against its run's files as `resume` does.
- `evidence` sums what a run's fuzzer processes counted, per worker and for
  the run, and `--out <file>` also writes it as JSON. See
  [Campaign evidence](#campaign-evidence).

Every run ends with exactly one end record, `runs/<manifest>/end.json`, with
one of three statuses:

- `complete`: the supervisor ran every worker to the end of its budget.
- `worker-stopped`: at least one worker stopped at its restart limit.
- `interrupted`: the supervisor stopped first, and `close` wrote the record.

Findings don't change the status. The end record lists every file the workers
and the compaction left in their libFuzzer artifact directories and their
`ZAKURA_SIM_ARTIFACTS` directories, by SHA-256, for triage.

Snapshots, manifests and builds are written to temporary names, synced, then
renamed or linked under their digests, and never change. A run never deletes
its initial snapshot or a worker's files, so an interrupted run, merge or
compaction loses nothing that a snapshot already held.

A new commit, generator, format, contract or feedback version starts a new
campaign segment. `start --parent <run> --corpus carry` carries the last
snapshot over when the harness's versions didn't change and both segments'
fuzzer builds are clean, since a dirty build can change the generator without
changing a version.
`--corpus seeds` starts from the curated seeds alone. Either way the previous
segment's manifests and snapshots stay in the store.

```console
# Build the target and start a campaign of 4 workers for 12 hours each.
cargo xtask sim-campaign build --store /mnt/campaign --target discovery_wait_complete
cargo xtask sim-campaign start --store /mnt/campaign --campaign discovery-2026-11 \
    --sim discovery/wait-complete --build <sha256> --workers 4 --seconds 43200 \
    --compact-secs 600 --artifacts <uri>

# What the last run's processes counted, and where the counts fall short.
cargo xtask sim-campaign evidence --store /mnt/campaign --out evidence.json

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

### Campaign evidence

The supervisor sets `ZAKURA_SIM_STATS` to each worker's
`workers/<n>/stats/` directory, and each fuzzer process writes its counts to
`stats-<pid>.json` there: every 10 seconds, when the target stops the
process on a run it counted (a failure it panics on, or the abandonment
limit), and at a clean exit. The counts are runs by what the target did
(`clean`, `fail`, `harness-failure`, `abandoned`), runs by how they ended
(`violation`, `budget`, `idle/<status>`, `harness`, `not-run`), drains that
stopped at their bound, cleanup (abandoned runs and their leaked tasks, and
runs with unwound, panicked or unwind-failed tasks), each property's runs by
outcome with its applicable evaluations, and the runs that reached each
feature. Only the clean exit marks a file `complete`, since libFuzzer exits
there with `exit` between runs and the target's `atexit` handler runs. An
`exit` from inside a run leaves the file incomplete. A crash, a timeout
or a kill leaves the counts of the last write, and the file stays incomplete.
The compaction's merge writes no stats.

`evidence` matches each file to the attempt in the worker's attempts log
whose process has the file's process ID and started while the attempt ran.
It reports each worker's outcome, its processes by how they ended, and its
counts; the run's totals; the totals of the complete files that logged
processes wrote and whose attempts exited cleanly, with code 0 and no
failure in their output; each property's assumption losses
(`assumptions-never-held` and `assumption-lost`); the features reached, by
family; and the properties, the harness's and any file's, that none of
those complete files saw apply. It lists every gap: a worker that didn't
spend its budget, a process that left no file, a file its process never
marked complete, a complete file whose attempt didn't exit cleanly, a file
no logged process wrote (a reused process ID or a
stale copy among them), and a file that can't be read, that another
harness wrote, or that isn't named `stats-<pid>.json`. Feature counts come
only from a target with a collector.

`cargo xtask sim-minimize` minimizes a saved failure, as
[Minimizing a crash](#minimizing-a-crash) describes, and
`cargo xtask sim-triage` archives it, as [Triage](#triage) describes.
