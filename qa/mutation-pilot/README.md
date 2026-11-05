# mut-2: ten-case pilot catalogue

`catalogue.json` selects ten existing production mutations across six fault
classes and five transport-free simulator targets. It pins patches, source
files, checker/generator files, sensitivity inputs and the initial discovery
corpus. Each case names its causal trigger and the observations that expose
the changed behavior. The catalogue changes no production code, property,
generator, witness input or mutation patch.

This package prepares the pilot. It records baseline calibration and no
mutant or scored discovery campaign. The full retained-suite readiness gate
remains open.
The [mutation plan](https://valargroup.getoutline.com/doc/6af54d3b-7ca5-4f05-975d-77893109e7ad)
defines the broader acceptance criteria.

## Case matrix

Paths below are relative to `qa/planted/`. The JSON file contains their hashes
and complete trigger descriptions. The step identifies the causal operation
in the corresponding baseline trace. Reviewers must inspect that operation;
the checker only verifies that the step exists.

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
The mutations change production behavior rather than these observations or
models. Compilation and intended mutant failure remain separate checks.

## Baseline evidence

Six distinct retained inputs cover the ten cases. Every input passed its
`holds` witness and Exact replay in separate processes. All traces ended
quiescent with zero cleanup failures. These runs used the saved unmodified
simulator from mut-1.1, built at
`870678be1739ea799e59b4e0bdfe6cf512792710` with Rust 1.99.0 on Linux x86_64.
The catalogue records that executable's build provenance and SHA-256.

| Input stem | Steps | Intended applicability |
| --- | ---: | ---: |
| `block-decoded-retention` | 21 | 1 |
| `block-reactor-identity` | 4 | 4 |
| `block-reactor-policy` | 15 | 15 |
| `discovery-record-book` | 20 | 13 |
| `header-publisher-policy` | 8 | 8 |
| `header-reactor-policy` | 26 | 23 |

`baseline/` retains inputs, full traces, witness results, replay logs and
process outcomes. `catalogue_base_revision` identifies the metadata starting
point. It does not replace the executable's source revision. The source paths
used by the simulator provenance were unchanged between those revisions.

The dev/test simulator used `RUSTFLAGS='--cfg zakura_sim'`, no debug information
and no incremental compilation. It retained debug assertions and overflow
checks. A nightly fuzz executable has a different build contract. Before
scoring, the paired runner must run the original and mutant with identical
fuzz toolchain, dependency lockfile, profile, flags and bounds. These dev/test
witness results cannot substitute for that paired fuzz baseline.

## Discovery inputs and bounds

The selected experiment starts from an empty corpus. This measures cold-start
discovery. It does not measure discovery from the existing regression corpus.
`discovery-empty/` contains no `.bin` inputs or `.toml` sidecars. The canonical
corpus is the sorted input-digest list encoded as UTF-8 `[]` plus LF. The JSON
file records its SHA-256. The `seeds` command admitted zero inputs for each of
the five harnesses when given this explicit fixture directory.

Every discovery invocation must pass `--fixtures qa/mutation-pilot/discovery-empty`.
Omitting that flag loads the repository's curated sensitivity witnesses.
Start every original/mutant/seed run in a fresh store. Do not pass `--parent`,
carry a corpus, reuse a calibration corpus, or import a witness or derived
input. A fresh empty corpus prevents those inputs from entering by accident.
The shared `seeds` loader currently accepts an explicit empty fixture directory.
The paired runner must also verify the campaign's saved initial snapshot.

The allocation is five seeds (`11`, `23`, `47`, `89`, `179`) and 600
seconds per seed for each original/mutant pair. Each run uses one worker,
512-byte inputs, a 10-second input limit, 2048 MiB RSS, no restarts, semantic
feedback and no sanitizer. Each machine decoder bounds its operations to the
512-byte scenario prefix. Its harness bounds simulator work to 128 steps, or
129 steps for decoded retention including drain. Confirmation gets 60 seconds
per child. Shrinking gets 600 seconds and 2000 candidates.

## Baseline calibration

Six unmodified fuzz runs calibrated the bounds above. Each run started a
fresh empty corpus with seed 11. The first five requested five seconds each.
The record-book target observed no applicable input, so a separate run used
the same seed, bounds and generator for 30 seconds. Both results remain below.
No generator or input repair followed either run.

| Target | Requested seconds | Elapsed seconds | Inputs | Applicable inputs |
| --- | ---: | ---: | ---: | ---: |
| `block_decoded_retention` | 5 | 6.032 | 41,685 | 32,496 |
| `block_reactor_policy` | 5 | 6.026 | 56,272 | 32,760 |
| `discovery_record_book` | 5 | 6.027 | 5,484 | 0 |
| `discovery_record_book` | 30 | 31.024 | 21,699 | 0 |
| `header_publisher_policy` | 5 | 6.026 | 87,963 | 66,117 |
| `header_reactor_policy` | 5 | 6.027 | 85,923 | 8,057 |

Every process exited zero with complete stats, no violations, no abandoned
runs and no cleanup failures. Peak RSS ranged from 32 to 33 MiB. The runs
support the selected resource limits. They do not establish a discovery rate.
Other builds ran during calibration, so throughput is host-specific. The
600-second allocation retains the plan's initial proposal; it does not promise
that any target will reach its trigger within that budget.

**Record-book applicability remains open.** Its `Holds` gate requires an
installation, replacement, eviction and sample within the same input. Neither
calibration run satisfied that whole-history gate. Zero applicability does
not mean that the driver executed no operations. The two record-book cases
remain in the future discovery denominator. A scored run with zero
applicability must report that outcome, not coverage or intended detection.
Their committed sensitivity fixture still passes, and their curated target
readiness result remains separate from cold-start discovery.

The clean fuzz binaries came from mut-1.3 at
`b2e0456474e47e6264c912b2f6b50d09c8603115`. Their build identities pin
`nightly-2026-07-15`, cargo-fuzz 0.13.2, libfuzzer-sys 0.4.13, and the refreshed
simulator fuzz lockfile. `fuzz_build_inputs` records these separately from the
dev/test witness build. Each `calibration/*/run.json` records its binary hash,
build identity, command, environment, elapsed time and initial empty corpus.
The catalogue hashes the run records, stats and log copies. Log copies omit
trailing whitespace; each run record also retains the raw log's hash.
Generated calibration corpora remain outside this catalogue and all scored
inputs. Their counts and manifest hashes provide an audit record.

This command shape supplies the explicit discovery configuration after the
paired runner has prepared and verified the selected build. `STORE` names a
fresh disk-backed directory for exactly one case, lane and seed. `BUILD` names
the store's verified build digest. The build must match that case's target.

```sh
"$SIM" campaign start --store "$STORE" --campaign "$CASE-$LANE-$SEED" \
  --sim "$HARNESS" --build "$BUILD" --seconds 600 --workers 1 --seed "$SEED" \
  --fixtures qa/mutation-pilot/discovery-empty --max-len 512 --timeout 10 \
  --rss-limit-mb 2048 --max-restarts 0 --abandon-limit 1 --feedback on
```

## Exclusions and remaining work

The full inventory keeps all 56 retained patches visible in mut-1.2. This
pilot selects ten of them. It excludes the other 46, including all 22 native
cases, because this first discovery experiment uses registered simulator
machine targets. Their exclusion does not establish equivalence or coverage.

The pilot also excludes integrated block storage, header wire, discovery
worker lifetime, held-credit runtime coverage and all-reactor composition.
mut-1.1 retained the discovery lost-trigger failures, header-wire end-record
mismatch and unresolved capacity-release timeout. None of those failures
counts as a passing baseline here. mut-1.3 records the missing-witness and
readiness dispositions. sim-9 remains deferred.

The catalogue uses existing regression mutations and known sensitivity
fixtures. Discovery receives none of those inputs, but this remains a
regression catalogue rather than unseen-mutant evaluation. Later generator
repairs need fresh mutants to measure generalization.

Before scoring, mut-3 must verify patch application, compilation, the reviewed
behavior change, passing paired baselines and required observations. It must
enforce paired build identity. Sensitivity then records the intended violation
or a valid survivor. A catch is not an entry requirement. The runner must
preserve an initial miss before any test or generator repair. It must keep
wrong-ID failures, zero applicability, baseline failures and incomplete
observations separate from intended detection. A timeout, compiler failure
or unrelated panic never counts as intended detection.

## Verify or reproduce

```sh
python3 scripts/check_mutation_pilot.py
python3 -m unittest discover -s scripts -p test_check_mutation_pilot.py -v
```

The checker verifies hashes and bindings, nonzero sensitivity applicability,
passing witnesses, clean outcomes, recorded replay exits and empty discovery
inputs. It checks calibration build identities, complete input counts and
applicability labels. It rejects drift in source and fuzz build inputs. It
does not rerun replay, prove causal trigger semantics or execute a mutation.
The ten tests reject wrong-ID witnesses, zero sensitivity applicability,
another property's failure, copied sensitivity inputs, missing corpus
directories, source/checker drift, misreported calibration applicability and
calibration cleanup failures.

To repeat one baseline, build the pinned source using the mut-1.1 instructions.
Set `SIM` to that executable, `INPUT` and `HARNESS` from a catalogue case, and
`OUT` to a fresh disk-backed directory. Each process has a 60-second limit.

```sh
timeout 60 "$SIM" record --sim "$HARNESS" --input "$INPUT" --out "$OUT" \
  --witness "$HARNESS" --expect holds
timeout 60 "$SIM" replay "$OUT"
"$SIM" seeds --sim "$HARNESS" \
  --fixtures qa/mutation-pilot/discovery-empty --report "$OUT/empty-seeds.json"
```

To repeat calibration, build the selected target with mut-1.3's toolchain and
flags. Start a new empty directory for each run and use its recorded command
and environment. `-max_total_time` requests five seconds, or 30 for the second
record-book run. An outer watchdog allows 15 extra seconds. Remove
`ZAKURA_SIM_EXPECT` and sanitizer-option variables as the run record specifies.
Preserve raw process stats and never reuse the generated corpus for scoring.
