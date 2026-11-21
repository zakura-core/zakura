# Joint mutation runs

The droplet timer for mutation plan mut-4.6 and run-2 to run-4. Once a day it
runs `scripts/mutation_joint_run.py routine`, which, in order:

1. fetches `origin/main` into its own checkout and detaches it there;
2. `qa-store-pull`: mirrors the QA store's Space into `--store`
   (`<state>/qa-store` by default) with `scripts/qa_store.py pull`. See
   [QA store](#qa-store);
3. `suite-manifest`: writes the frozen suite manifest, `suite-manifest.json`,
   with `cargo xtask sim-suite --frozen`;
4. `suite`: runs the full planted sim suite (`cargo test -p zakura-sim
   --profile planted`);
5. `planted`, `planted-targets` and `sim-targets`: with `--planted-shards
   <n>`, runs the day's shard of the planted cases with `cargo xtask planted
   --shard`, so each case runs once every `n` days. `--planted-targets` also
   confirms the shard through its fuzz targets. `--sim-targets` checks each
   fuzz target on the store's seeds with `cargo xtask sim-targets`. These
   steps replaced the nightly `sim-planted`, `sim-planted-targets` and
   `sim-targets` CI jobs, which had no store to read;
6. `loop`: runs `cargo xtask planted loop` with generator lanes on the cases
   a change since the last pin affects, in the repository or the store, or
   on every case the first time;
7. `libfuzzer`: with `--libfuzzer-seconds`, runs the libFuzzer lane on the
   same cases;
8. `suite-fuzz` and `archive`: with `--suite-fuzz` or `--fuzz-seconds`, runs
   `scripts/suite_fuzz_run.py run` (run-3), then `scripts/suite_fuzz_run.py
   archive` (run-4). See [Suite fuzz](#suite-fuzz);
9. `cases`: lists the loop's cases at the checkout with `cargo xtask planted
   loop --list`;
10. `triage-<lane>`: merges each new report into its cumulative report.
   `<state>/cumulative.json` holds every case's latest generator-lane result,
   every property finding so far and each generator search's latest baseline
   failure, and `<state>/cumulative-libfuzzer.json` holds the libFuzzer
   lane's. A `--changed-since` report holds only the cases it reran, so
   diffing it against the last report alone would list every unchanged
   survivor again. Each cumulative report keeps only the cases that `cases`
   listed, so a removed case, or one a witness change re-keyed, leaves it,
   and the diff lists it under `removed_cases`. The step then runs `cargo
   xtask planted triage` on the cumulative report, which applies the
   store's `qa/native-mutations/triage.toml` afresh. A triage-only commit
   selects no case, so this step is what records its triage. Last, the
   routine diffs each new report against the previous cumulative report into
   `joint-diff.json`, with the cumulative report's untriaged lanes, and
   writes every lane's diff to `findings.json`;
11. `completeness`: runs `scripts/suite_fuzz_run.py completeness` (run-4),
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
the cumulative report's lanes that still need a record in the store's
`qa/native-mutations/triage.toml`.

## QA store

The repository holds no test artifact. The planted cases, the native
mutations and their triage, the scenario fixtures and the seeds live in the
DigitalOcean Space `zakura-sim-corpus` (nyc3). The QA store is a local mirror
of it, and every tool reads it through `ZAKURA_QA_STORE`.
`scripts/qa_store.py` documents its layout.

The routine's first step pulls the Space and writes the store's manifest,
`qa-store-manifest.json`, into the run's directory. Each pin records its
run's manifest. The routine diffs the new manifest against each lane's pin
and passes the changed paths to the loop's `--store-changed`, so a changed
case or seed reruns as a changed commit would. A pin without a manifest
counts every store path as changed. A failed pull ends the run before any
other step, and no pin advances.

The pull reads the bucket, endpoint and bucket-scoped key from
`/etc/zakura-mutation-joint.env`. To add or change a case, edit a pulled
store and push it:

```sh
python3 scripts/qa_store.py --env-file /etc/zakura-mutation-joint.env pull --dest ~/qa-store
# edit ~/qa-store/qa/planted/...
python3 scripts/qa_store.py --env-file /etc/zakura-mutation-joint.env push --src ~/qa-store
```

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
clean and the harness's versions match, and else starts from the store's
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

## Cleanup

A second timer, `zakura-mutation-joint-cleanup.timer`, runs
`scripts/mutation_joint_cleanup.py` daily at 03:00 UTC, before the routine.
Under each `--root` it deletes three kinds of directory:

- cargo target directories;
- the routine's runs under `<state>/runs/`, other than the runs a
  `latest*.json` names;
- the `builds/` directory of each campaign store outside `<state>/campaigns/`.

It deletes each one that didn't change for `--max-age-days` (3). If the disk
is then over `--disk-limit` (80%), it deletes the rest, least recently
changed first, until the disk is below the limit. It never deletes what
changed in the last 6 hours, which a running job may still use. It keeps
every `--keep` path, the cumulative reports, `latest*.json`, `<state>/archive/`
and the live campaign stores under `<state>/campaigns/`. A live store stays
whole, because `sim-campaign verify` reads the build of every run the store
records. `ZAKURA_CLEANUP_ARGS` in `/etc/zakura-mutation-joint.env` sets the
roots, keeps and limits. Preview a cleanup with `--dry-run`:

```sh
python3 scripts/mutation_joint_cleanup.py --state <dir> --root /root --dry-run
```

## Install

Install both timers as root on the host with
`deploy/mutation-joint/install.sh`, then set the budget in
`/etc/zakura-mutation-joint.env`. Check a configuration without running
anything:

```sh
python3 scripts/mutation_joint_run.py routine --checkout <dir> --state <dir> --dry-run
```

No GitHub Actions job runs the routine.
