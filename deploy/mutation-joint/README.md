# Joint mutation runs

The droplet timer for mutation plan mut-4.6. Once a day it runs
`scripts/mutation_joint_run.py routine`, which:

1. fetches `origin/main` into its own checkout and detaches it there;
2. runs the full planted sim suite (`cargo test -p zakura-sim --profile
   planted`);
3. runs `cargo xtask planted loop` with generator lanes on the cases a change
   since the last report's pin affects, or on every case the first time;
4. with `--libfuzzer-seconds`, runs the libFuzzer lane on the same cases;
5. diffs the new report against the last one into `joint-diff.json`.

Each run writes under `<state>/runs/<UTC time>-<commit>/`: one log per step,
the loop's report under `loop/`, the diff beside it, and `routine.json` with
each step's exit status. `<state>/latest.json` names the last report. The
unit fails when a step fails or when the diff lists a new survivor, a newly
failing baseline or a new property finding. `untriaged` in the diff lists
the lanes that still need a record in `qa/native-mutations/triage.toml`.

Install it as root on the host with `deploy/mutation-joint/install.sh`, then
set the budget in `/etc/zakura-mutation-joint.env`. Check a configuration
without running anything:

```sh
python3 scripts/mutation_joint_run.py routine --checkout <dir> --state <dir> --dry-run
```

No GitHub Actions job runs the routine.
