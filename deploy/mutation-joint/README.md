# Joint mutation runs

The droplet timer for mutation plan mut-4.6 and run-2 to run-4. Once a day it
runs `scripts/mutation_joint_run.py routine`, which, in order:

1. fetches `origin/main` into its own checkout and detaches it there;
2. `suite-manifest`: writes the frozen suite manifest, `suite-manifest.json`,
   with `cargo xtask sim-suite --frozen`;
3. `suite`: runs the full planted sim suite (`cargo test -p zakura-sim
   --profile planted`);
4. `loop`: runs `cargo xtask planted loop` with generator lanes on the cases
   a change since the last pin affects, or on every case the first time;
5. `libfuzzer`: with `--libfuzzer-seconds`, runs the libFuzzer lane on the
   same cases;
6. `suite-fuzz` and `archive`: with `--suite-fuzz` or `--fuzz-seconds`, runs
   `scripts/suite_fuzz_run.py run` (run-3), then `scripts/suite_fuzz_run.py
   archive` (run-4). See [Suite fuzz](#suite-fuzz);
7. `cases`: lists the loop's cases at the checkout with `cargo xtask planted
   loop --list`;
8. `triage-<lane>`: merges each new report into its cumulative report.
   `<state>/cumulative.json` holds every case's latest generator-lane result,
   every property finding so far and each generator search's latest baseline
   failure, and `<state>/cumulative-libfuzzer.json` holds the libFuzzer
   lane's. A `--changed-since` report holds only the cases it reran, so
   diffing it against the last report alone would list every unchanged
   survivor again. Each cumulative report keeps only the cases that `cases`
   listed, so a removed case, or one a witness change re-keyed, leaves it,
   and the diff lists it under `removed_cases`. The step then runs `cargo
   xtask planted triage` on the cumulative report, which applies the
   checkout's `qa/native-mutations/triage.toml` afresh. A triage-only commit
   selects no case, so this step is what records its triage. Last, the
   routine diffs each new report against the previous cumulative report into
   `joint-diff.json`, with the cumulative report's untriaged lanes, and
   writes every lane's diff to `findings.json`;
9. `completeness`: runs `scripts/suite_fuzz_run.py completeness` (run-4),
   which writes `completeness.json`.

Each run writes under `<state>/runs/<UTC time>-<commit>/`: one log per step,
the loop's report under `loop/`, the diff beside it, `findings.json`,
`completeness.json`, and `routine.json` with each step's exit status.
`<state>/latest.json` names the last pin. The libFuzzer lane keeps its own
pin in `<state>/latest-libfuzzer.json`, which advances only when its step
succeeds, so the next run covers a failed run's changes again. Before it has
one, the lane uses the loop's pin. The unit fails when a step fails or when
either diff lists a new survivor, a newly failing baseline, a new property
finding or a new generator baseline failure. `untriaged` in the diff lists
the cumulative report's lanes that still need a record in
`qa/native-mutations/triage.toml`.

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
