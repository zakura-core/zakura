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

Run `python3 scripts/mutation_inventory.py --output "${TMPDIR:-/tmp}/mutations.json"`
from the repository root to check patch application and binding metadata.
The command does not apply patches or compile tests. A matching source symbol
does not prove that Cargo selects a test. Before scoring a native case, verify
its exact selector with `--list`, require one executed test, and compare the
unchanged, patched, and restored runs. Only the declared assertion counts as
detection. Native assertions do not acquire simulator property IDs through
this mapping.

## Generator lanes

The handshake PR includes the shared mut-4 generator and mutation-runner
infrastructure. It keeps the existing harness decoders and registers their
scenario-family selectors. `handshake/wire` lists fifteen families;
`handshake/counts` draws machine operations directly.

Run searches and mutation campaigns on the droplet. For example:

```sh
RUSTFLAGS='--cfg zakura_sim' cargo run -p zakura-sim --bin zakura-sim -- generators
RUSTFLAGS='--cfg zakura_sim' cargo run -p zakura-sim --bin zakura-sim -- generate \
  --sim handshake/counts --seed 11 --runs 1000 --out results/handshake-counts
cargo xtask planted loop --only session/capacity-conserved \
  --generate-runs 100 --generate-seeds 11 --generate-harness handshake/wire \
  --out results/handshake-capacity-loop
```

The loop checks its unpatched baseline first. It excludes committed inputs from
in-process generator searches, confirms a discovered failure in a fresh process,
and requires exact replay before classifying the mutation. The report records
which family found the failure and whether the property applied. A baseline
failure remains a separate finding; it never counts as a killed mutation.

`scripts/mutation_joint_run.py` compares reports and reruns lanes affected by
changes since the previous report's pinned commit. A newly surviving mutation,
failing baseline, or property finding makes the comparison fail.
