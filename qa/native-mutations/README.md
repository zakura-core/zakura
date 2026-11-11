# Native mutations

Each patch here plants a bug that a named test detects outside the planted
runner. The README beside each patch names that test and how to run it.

`cargo xtask planted` reads a case file for every patch under `qa/planted`,
and the case file names either a committed fuzz input or a typed case: a
committed scenario and the Plan bytes it runs under, for a scenario the fuzz
grammar can't encode. These patches have no case file, so they live here.

`cases.json` maps every patch to its package, exact test selector, source,
and intended assertion. Most cases use library tests. The handoff case uses
the `zakura-sim` integration test named in its `test_target` field and needs
`RUSTFLAGS="--cfg zakura_sim"`.

The mutation loop's patches live in `<entry>/<name>/`, one directory per
patch, where `<entry>` is a `work-list.toml` entry. A generator writes them
from `PROMPT.md`. Their `cases.json` entries name a property and its witness
instead of a native test. `cargo xtask planted loop` runs them on the remote
host. `python3 scripts/mutation_inventory.py --loop` checks them.

With `--generate-runs <n>`, the loop also gives each case generator lanes:
`zakura-sim generate` searches `n` inputs drawn from each seed of
`--generate-seeds` for a violation of the case's property, skipping every
committed input here and under `qa/planted`. Each input starts with one of
the harness's scenario families' tags, or none, picked uniformly.
`--generate-harness` picks the harness, `auto` by default.
`--generate-harness-runs <harness>=<n>` gives one harness its own count, for
a harness whose runs take seconds.
`--generate-mode libfuzzer` runs one campaign per seed for
`--generate-seconds` instead, from an empty corpus and with no exclusions.
A kill must confirm in a fresh process, and the loop shrinks its trace. The
loop then replays the shrunk trace on the unpatched build. If the unpatched
build violates the same ID, the lane is `baseline-fails` and the report lists
a property finding, because the bug is in the pinned revision. One
search per harness and seed runs on the unpatched build first, and one that
a run failure ended reruns alone. A confirmed violation there is a property
finding in the report. Any other search that isn't clean is a generator
baseline failure. Either way that lane is `baseline-fails`. A property that
no generator harness reaches has a `[[no-generator]]` record in
`work-list.toml` with the reason.

Every lane that survived, or that another rule killed, needs a triage record
in `triage.toml`: the case's patch and property, the lane (`witness` or the
generator harness), the causal-chain stage where the bug stopped
(`generator`, `schedule`, `observation`, `property` or `unsupported`), the
reason, and an optional tracking issue. The loop copies each record onto the
lanes it names, lists records that name no such lane as stale, and prints how
many lanes still need one.

`python3 scripts/mutation_joint_run.py run --previous <report.json> --out
<dir>` runs the loop with generator lanes on the cases changed since the
previous report's pin, and lists new survivors, new baseline failures and
new property findings. `diff <old> <new>` lists them for two reports.

Run `python3 scripts/mutation_inventory.py --output "${TMPDIR:-/tmp}/mutations.json"`
from the repository root to check patch application and binding metadata.
The command does not apply patches or compile tests. A matching source symbol
does not prove that Cargo selects a test. Before scoring a native case, verify
its exact selector with `--list`, require one executed test, and compare the
unchanged, patched, and restored runs. Only the declared assertion counts as
detection. Native assertions do not acquire simulator property IDs through
this mapping.
