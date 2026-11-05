# mut-2: ten-case pilot catalogue

`catalogue.json` selects ten existing production mutations across six fault
classes and five transport-free simulator targets. It pins patches, source
files, checker/generator files, sensitivity inputs and the initial discovery
corpus. Each case names its causal trigger and the observations that expose
the changed behavior. The catalogue changes no production code, property,
generator, witness input or mutation patch.

This package prepares the pilot. It records no new mutant score or discovery
result. The full retained-suite readiness gate remains open.
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

The proposed allocation is five seeds (`11`, `23`, `47`, `89`, `179`) and 600
seconds per seed for each original/mutant pair. Each run uses one worker,
512-byte inputs, a 10-second input limit, 2048 MiB RSS, no restarts, semantic
feedback and no sanitizer. Each machine decoder bounds its operations to the
512-byte scenario prefix. Its harness bounds simulator work to 128 steps, or
129 steps for decoded retention including drain. Confirmation gets 60 seconds
per child. Shrinking gets 600 seconds and 2000 candidates. Calibration must
verify these bounds before scoring.

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

The checker verifies hashes and bindings, nonzero intended applicability,
passing witnesses, clean outcomes, recorded replay exits and empty discovery
inputs. It rejects drift in the source tree. It does not rerun replay, prove
causal trigger semantics or execute a mutation. The tests reject wrong-ID
witnesses, zero applicability, another property's failure, copied sensitivity
inputs and checker drift.

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
