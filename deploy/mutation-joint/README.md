# Joint mutation runs

The droplet timer for mutation plan mut-4.6. Once a day it runs
`scripts/mutation_joint_run.py routine`, which:

1. fetches `origin/main` into its own checkout and detaches it there;
2. runs the full planted sim suite (`cargo test -p zakura-sim --profile
   planted`);
3. runs `cargo xtask planted loop` with generator lanes on the cases a change
   since the last pin affects, or on every case the first time;
4. with `--libfuzzer-seconds`, runs the libFuzzer lane on the same cases;
5. merges each new report into its cumulative report.
   `<state>/cumulative.json` holds every case's latest generator-lane result
   and every property finding so far, and
   `<state>/cumulative-libfuzzer.json` holds the libFuzzer lane's. A
   `--changed-since` report holds only the cases it reran, so diffing it
   against the last report alone would list every unchanged survivor again.
   Each cumulative report keeps only the cases that `cargo xtask planted
   loop --list` names at the checkout, so a removed case, or one a witness
   change re-keyed, leaves it, and the diff lists it under `removed_cases`;
6. runs `cargo xtask planted triage` on each cumulative report, which
   applies the checkout's `qa/native-mutations/triage.toml` afresh. A
   triage-only commit selects no case, so this step is what records its
   triage;
7. diffs each new report against the previous cumulative report into
   `joint-diff.json`, with the cumulative report's untriaged lanes.

Each run writes under `<state>/runs/<UTC time>-<commit>/`: one log per step,
the loop's report under `loop/`, the diff beside it, and `routine.json` with
each step's exit status. `<state>/latest.json` names the last pin. The
unit fails when a step fails or when either diff lists a new survivor, a
newly failing baseline or a new property finding. `untriaged` in the diff lists
the cumulative report's lanes that still need a record in
`qa/native-mutations/triage.toml`.

Install it as root on the host with `deploy/mutation-joint/install.sh`, then
set the budget in `/etc/zakura-mutation-joint.env`. Check a configuration
without running anything:

```sh
python3 scripts/mutation_joint_run.py routine --checkout <dir> --state <dir> --dry-run
```

No GitHub Actions job runs the routine.
