# Joint mutation runs

The droplet timer for mutation plan mut-4.6 and run-2 to run-4. Once a day it
runs `scripts/mutation_joint_run.py routine`, which:

1. fetches `origin/main` into its own checkout and detaches it there;
2. writes the frozen suite manifest, `suite-manifest.json`, with
   `cargo xtask sim-suite --frozen`;
3. runs the full planted sim suite (`cargo test -p zakura-sim --profile
   planted`);
4. runs `cargo xtask planted loop` with generator lanes on the cases a change
   since the last pin affects, or on every case the first time;
5. with `--libfuzzer-seconds`, runs the libFuzzer lane on the same cases;
6. with `--suite-fuzz` or `--fuzz-seconds`, runs `scripts/suite_fuzz_run.py
   run` (run-3), then `scripts/suite_fuzz_run.py archive` (run-4);
7. diffs each new report against its cumulative report into
   `joint-diff.json`, then merges it in. `<state>/cumulative.json` holds
   every case's latest generator-lane result and every property finding so
   far, and `<state>/cumulative-libfuzzer.json` holds the libFuzzer lane's.
   A `--changed-since` report holds only the cases it reran, so diffing it
   against the last report alone would list every unchanged survivor again.
   `findings.json` holds every diff of the run;
8. runs `scripts/suite_fuzz_run.py completeness` (run-4), which writes
   `completeness.json`.

Each run writes under `<state>/runs/<UTC time>-<commit>/`: one log per step,
the loop's report under `loop/`, the diff beside it, `findings.json`,
`completeness.json`, and `routine.json` with each step's exit status.
`<state>/latest.json` names the last pin. The unit fails when a step fails or
when either diff lists a new survivor, a newly failing baseline or a new
property finding. `untriaged` in the diff lists the lanes that still need a
record in `qa/native-mutations/triage.toml`.

## Suite fuzz

The suite fuzz is off by default. `--suite-fuzz` turns it on, and fuzzes each
target of the frozen manifest for its budget in `qa/suite.toml`:
`[campaign.sim]`, or the row's own. `--fuzz-seconds <s>` sets every target's
budget instead, and also turns the suite fuzz on. `--fuzz-workers` sets each
campaign run's workers, and `--fuzz-jobs` sets how many campaigns run at once.
The host then needs the nightly toolchain and cargo-fuzz that qa/fuzz/sim's
README names.

For each target, `run` runs `cargo xtask sim-targets --target <name>` as a
smoke, builds the target with `sim-campaign build`, and runs one campaign
run in `<state>/campaigns/<target>/`. The first run starts the campaign. A
later run resumes it if the build has the last run's identity and the
harness's versions didn't change, as the campaign tool requires. Otherwise,
such as after a new commit or a new generator, it starts a new segment after
the last run. The segment carries the last snapshot only if both builds are
clean and the harness's versions match, and else starts from the curated
seeds. `run` then runs `sim-campaign verify` and `sim-campaign evidence`,
and writes `suite-fuzz.json`. `sim-campaign build` builds only qa/fuzz/sim's
targets, so `suite-fuzz.json` lists each p2p and header-chain target as not
run, with that reason.

`archive` triages each campaign run's failure inputs into `<state>/archive/`
with `cargo xtask sim-triage`, which also minimizes each confirmed `active`
failure. It then checks the archive with `cargo xtask sim-verify-archive`,
and writes `archive.json`: each bundle's path, status and verdict.

`completeness.json` lists each manifest row the routine ran, each row it
didn't run with the reason, each ran row whose evidence is inconclusive, and
each unimplemented row: the `missing-target` and `pending` rows, and each
pending bypass. It reports coverage only: no row it lists fails the unit.
`findings.json` holds the correctness findings.

Each target's smoke and build take minutes. Size `--fuzz-seconds` and
`--fuzz-jobs` so that a run fits the unit's 36-hour timeout.

## Install

Install it as root on the host with `deploy/mutation-joint/install.sh`, then
set the budget in `/etc/zakura-mutation-joint.env`. Check a configuration
without running anything:

```sh
python3 scripts/mutation_joint_run.py routine --checkout <dir> --state <dir> --dry-run
```

No GitHub Actions job runs the routine.
