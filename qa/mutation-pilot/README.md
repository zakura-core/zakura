# mut-2: ten-case pilot catalogue

`catalogue.json` selects ten existing production mutations across six fault
classes and five transport-free simulator targets. It pins patches, source
files, checker and generator files, sensitivity inputs and the initial
discovery corpus. Each case names its causal trigger and the observations
that expose the changed behavior. The catalogue changes no production code,
property, generator, witness input or mutation patch.

This package prepares the pilot. No mutant and no scored discovery campaign
has run. The full retained-suite readiness gate remains open.
The [mutation plan](https://valargroup.getoutline.com/doc/6af54d3b-7ca5-4f05-975d-77893109e7ad)
defines the broader acceptance criteria.

## Pinned build inputs

| Build | Revision | Toolchain |
| --- | --- | --- |
| Dev/test witness simulator | `debba4e31dde0c2d5a01b7988fcf07902bd1a538` | Rust 1.99.0, `--cfg zakura_sim`, no debug information or incremental compilation |
| Fuzz targets | `debba4e31dde0c2d5a01b7988fcf07902bd1a538` | `nightly-2026-07-15`, cargo-fuzz 0.13.2, libfuzzer-sys 0.4.13, `--sanitizer none` |

Both builds use one `main` commit. #2083 refreshed `qa/fuzz/sim/Cargo.lock`
on `main`, so the fuzz build needs no commit of its own.
`scripts/check_mutation_pilot.py` checks the pilot's own inputs under `qa/`
on any tree. `--at-pin` also requires the tree's production, oracle and lock
files to equal both revisions, which a pilot run needs. `crates/xtask` holds
the pilot tooling, so it stays outside the pin. Run the pilot from a tree
where `--at-pin` passes, or re-pin the catalogue first.

## Case matrix

Paths are relative to `qa/planted/`. The JSON file holds their hashes and
complete trigger descriptions. The step identifies the causal operation in
the baseline trace. Reviewers must inspect that operation; the checker only
verifies that the step exists.

| Mutation | Fault class | Trigger step | Intended check |
| --- | --- | ---: | --- |
| `block/reactor-policy/lost-forced-retry.patch` | Queue refusal | 4 | `block/reactor-policy` |
| `block/reactor-policy/refusal-consumes-query-id.patch` | Queue refusal | 2 | `block/reactor-policy` |
| `block/reactor-policy/stale-scope-completes-query.patch` | Stale result | 2 | `block/reactor-policy` |
| `block/decoded-retention/missing-admission.patch` | Boundary accounting | 1 | `block/decoded-retention` |
| `block/decoded-retention/double-release.patch` | Resource lifetime | 12 | `block/decoded-retention` |
| `block/decoded-retention/raw-header-escape.patch` | Resource lifetime | 4 | `block/decoded-retention` |
| `discovery/record-book/keep-old-relay-attribution.patch` | Ownership | 14 | `discovery/record-book` |
| `discovery/record-book/wrong-eviction-victim.patch` | Eviction policy | 20 | `discovery/record-book` |
| `header/reactor-policy/locator-call-settles-without-removal.patch` | Resource lifetime | 7 | `header/reactor-policy` |
| `header/publisher-policy/lost-credit.patch` | Boundary accounting | 3 | `header/publisher-policy` |

The reactor model owns separate query identities and retry state. The decoded
retention model owns a separate handle graph. The record-book model computes
entry selection and relay attribution. The header models compute locator and
publisher state. Each driver compares production observations with its model.
The JSON file names and hashes the driver, harness and shared checker sources.
Each mutation changes production behavior, not these observations or models.

## Discovery inputs and bounds

Discovery starts from an empty corpus. This measures cold-start discovery,
not discovery from the existing regression corpus. `discovery-empty/`
contains no `.bin` inputs or `.toml` sidecars, which are the only files the
seed loader reads. Every discovery invocation must pass
`--fixtures qa/mutation-pilot/discovery-empty`; omitting it loads the curated
sensitivity witnesses. Start every original, mutant and seed run in a fresh
store. Never pass `--parent`, carry a corpus, reuse a calibration corpus, or
import a witness or derived input. The paired runner must also verify the
campaign's saved initial snapshot.

The allocation is five seeds (`11`, `23`, `47`, `89`, `179`) and 600
seconds per seed for each original and mutant. Each run uses one worker,
512-byte inputs, a 10-second input limit, 2048 MiB RSS, no restarts,
semantic feedback and no sanitizer. Each machine decoder bounds its
operations to the 512-byte scenario prefix. Its harness bounds simulator
work to 128 steps, or 129 for decoded retention including drain.
Confirmation gets 60 seconds per child. Shrinking gets 600 seconds and 2000
candidates. The scored campaign runs on a remote droplet, never locally or
in CI.

`STORE` names a fresh disk-backed directory for one case, lane and seed.
`BUILD` names the store's verified build digest for that case's target.

```sh
"$SIM" campaign start --store "$STORE" --campaign "$CASE-$LANE-$SEED" \
  --sim "$HARNESS" --build "$BUILD" --seconds 600 --workers 1 --seed "$SEED" \
  --fixtures qa/mutation-pilot/discovery-empty --max-len 512 --timeout 10 \
  --rss-limit-mb 2048 --max-restarts 0 --abandon-limit 1 --feedback on
```

## Verify

```sh
python3 scripts/check_mutation_pilot.py
python3 scripts/check_mutation_pilot.py --at-pin
python3 scripts/check_mutation_pilot.py --evidence "$BUNDLE_DIR"
python3 -m unittest discover -s scripts/tests -p test_check_mutation_pilot.py -v
```

By default, the checker verifies the repository inputs under `qa/`. It checks
the case count and fault classes, patch, sidecar and witness hashes, patch
bindings, target bindings and the `holds` witness for each intended property.
It checks the empty discovery corpus.
With `--at-pin`, it also checks the production and checker hashes and the lock
hashes, and it rejects a tree whose build inputs differ from either pinned
revision. A pilot run needs `--at-pin`; later commits on `main` fail it.
With `--evidence`, it also checks the extracted bundle against the
catalogue. Each baseline must have nonzero intended applicability, a passing
witness, no other property outcome than held or not applicable, a quiescent
end and zero cleanup failures. Its record and replay exit codes come from the
bundle's `baseline/results.json`, whose hash the catalogue pins. Each calibration run must be complete and
clean, with matching build identity and applicability label.
The checker reruns no replay, proves no causal trigger semantics and runs no
mutation. The tests build a small synthetic checkout and bundle.

## Paired runner

`cargo xtask planted pilot` runs the paired experiments from this
catalogue (mut-3). It checks out each pinned revision in fresh worktrees,
so it runs from a later tree such as `main`. There the Python checker's
`--at-pin` check fails, so the runner checks the pins itself: each patch, witness
input and production file must match its catalogue hash, and each trace or
fuzz build must pair with the catalogue's build identity. The runner builds
the original and every mutant in separate worktrees and target directories
under `--work`. `--out` must be missing or empty, so a run never replaces an
earlier result.

```sh
cargo xtask planted pilot sensitivity --out "$OUT" [--only <case-id>]...
cargo xtask planted pilot discovery --out "$OUT" [--only <case-id>]... [--seed <n>]...
```

`sensitivity` builds the witness revision with Rust 1.99.0 and the dev
profile without debug information or incremental compilation. It writes
`sensitivity.json`. `discovery` builds the fuzz revision and runs each
original and mutant campaign with the bounds above. It writes
`discovery.json`. Run `discovery` only on the remote campaign host. It runs
one 600-second original campaign per target and seed, and one 600-second
mutant campaign per case and seed. `--only` and `--seed` split the work
across hosts.

Each result is exactly one class of the plan's table: intended violation,
other violation, survived budget, not applicable, baseline failure, invalid
mutant, harness failure or equivalent. Only review metadata marks a case
equivalent, through an `equivalent` argument on its catalogue entry. An
intended violation also needs a fresh `zakura-sim confirm` and an Exact
replay of the confirmed trace. Each report counts results by class and fault
class. The rate's denominator leaves out baseline failures, invalid mutants
and equivalent mutants, and keeps not-applicable results. `discovery` skips
each case whose catalogue entry sets `discovery_excluded`, and lists it with
its reason outside the counts. `sensitivity` still runs it.

The catalogue first pinned production at `870678be1` and the fuzz inputs
at `b2e0456474`, which existed only on the stack branches that merged as
squashes. The re-pin moved both onto `main` at `debba4e31` and recorded
fresh baselines and calibration on the campaign host. Between the two pins,
`main` changed three pinned files: `discovery/protocol.rs`,
`header_sync/reactor/state.rs` and `header_sync/reactor/machine_tests.rs`.
Every patch still applies. Every baseline trace matches the first pin's
record for record after its `meta` record, which names the new build.

## Reproduce the baselines and calibration

Build the witness simulator as in `qa/mutation-readiness/README.md`
(mut-1.1). Set `SIM` to that executable, `INPUT` and `HARNESS` from a
catalogue case, and `OUT` to a fresh disk-backed directory.

```sh
timeout 60 "$SIM" record --sim "$HARNESS" --input "$INPUT" --out "$OUT" \
  --witness "$HARNESS" --expect holds
timeout 60 "$SIM" replay "$OUT"
"$SIM" seeds --sim "$HARNESS" \
  --fixtures qa/mutation-pilot/discovery-empty --report "$OUT/empty-seeds.json"
```

Calibration builds each target as a discovery lane does, with
`"$SIM" campaign build --store "$STORE" --target "$TARGET" --sanitizer none`,
and runs the store's fuzzer from a new empty corpus with seed 11. Each run
record in the bundle holds its exact command, environment and build
identity. Remove `ZAKURA_SIM_EXPECT` and the sanitizer
option variables, as the record lists. Never reuse a calibration corpus for
scoring.

## Expected outcome

Six retained inputs cover the ten cases. Each passes its `holds` witness and
Exact replay, ends quiescent and has zero cleanup failures. The `seeds`
command admits zero inputs from `discovery-empty/` for each of the five
harnesses.

| Input stem | Steps | Intended applicability |
| --- | ---: | ---: |
| `block-decoded-retention` | 21 | 1 |
| `block-reactor-identity` | 4 | 4 |
| `block-reactor-policy` | 15 | 15 |
| `discovery-record-book` | 20 | 13 |
| `header-publisher-policy` | 8 | 8 |
| `header-reactor-policy` | 26 | 23 |

Six unmodified calibration runs exit zero with complete stats, no
violations, no abandoned runs and no cleanup failures. Each run's measured
duration, input count and applicability are in the bundle under
`calibration/<target>/`. Throughput is host-specific.

**Record-book applicability remains open.** `discovery_record_book` applied
to zero of 17,071 cold-start inputs at the re-pin (3,617 in 5 s and 13,454
in 30 s), and to zero of 27,183 at the first pin. Its `Holds` gate requires
an installation, replacement, eviction and sample within one input. Zero
applicability is not a pass and not coverage. No generator or input repair
followed. A target that applied to zero inputs in calibration shows a
generator gap, not a result. So both record-book cases count for sensitivity
only. Each catalogue entry sets `discovery_excluded` with that reason. The
paired runner then runs no campaign for them, and its report lists them
outside its counts. The Python checker requires the exclusion for every case
whose target never applied in calibration. Their committed sensitivity
fixture still applies 13 times, and their curated target readiness passed in
mut-1.3.

The 600-second allocation keeps the plan's initial proposal. It does not
promise that any target reaches its trigger within that budget.

## Evidence bundle

`2063-mut-3-repin-2026-11-06.tar.zst`, SHA-256
`c4fc16e6c00f8394e7bb926f57442b4989d559b1626690f1a2948075e0f0935d`. It is an
Outline attachment, listed in the mutation plan's evidence table. Extract it
and pass `2063-mut-3-repin-2026-11-06/` as `--evidence`.
`baseline.directory` and `calibration.runs[].directory` in the catalogue
resolve inside that directory. `SHA256SUMS` inside the bundle covers every
other file.

```text
2063-mut-3-repin-2026-11-06/SHA256SUMS
2063-mut-3-repin-2026-11-06/baseline/results.json
2063-mut-3-repin-2026-11-06/baseline/{block-decoded-retention,block-reactor-identity,block-reactor-policy}/{input.bin,trace.jsonl,witness.json}
2063-mut-3-repin-2026-11-06/baseline/{discovery-record-book,header-publisher-policy,header-reactor-policy}/{input.bin,trace.jsonl,witness.json}
2063-mut-3-repin-2026-11-06/baseline/<stem>.log, one per input stem above
2063-mut-3-repin-2026-11-06/baseline/<stem>-discovery-seeds.json for block-decoded-retention, block-reactor-policy, discovery-record-book, header-publisher-policy, header-reactor-policy
2063-mut-3-repin-2026-11-06/calibration/{block_decoded_retention,block_reactor_policy,discovery_record_book,header_publisher_policy,header_reactor_policy}/{run.json,run.log,stats/stats-<pid>.json}
2063-mut-3-repin-2026-11-06/calibration/discovery_record_book-30s/{run.json,run.log,stats/stats-<pid>.json}
```

## Exclusions and remaining work

The full inventory keeps all 56 retained patches visible in mut-1.2. This
pilot selects ten of them. It excludes the other 46, including all 22 native
cases, because this discovery experiment uses registered simulator machine
targets. Exclusion establishes neither equivalence nor coverage.

The pilot also excludes integrated block storage, header wire, discovery
worker lifetime, held-credit runtime coverage and all-reactor composition.
The discovery lost-trigger failures, the header-wire end-record mismatch and
the unresolved capacity-release timeout from mut-1.1 do not count as passing
baselines. sim-9 stays deferred.

The catalogue reuses existing regression mutations and their sensitivity
fixtures. Discovery receives none of those inputs, but this remains a
regression catalogue rather than unseen-mutant evaluation. Later generator
repairs need fresh mutants to measure generalization.

Before scoring, mut-3 must verify patch application, compilation, the
reviewed behavior change, passing paired baselines and required
observations. It must enforce paired build identity. The dev/test witness
results above cannot substitute for a paired fuzz baseline. Sensitivity then
records the intended violation or a valid survivor; a catch is not an entry
requirement. The runner must preserve an initial miss before any test or
generator repair. It must keep wrong-ID failures, zero applicability,
baseline failures and incomplete observations separate from intended
detection. A timeout, compiler failure or unrelated panic never counts as
intended detection.
