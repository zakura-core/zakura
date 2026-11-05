# Native mutations

Each patch here plants a bug that a named test detects outside the planted
runner. The README beside each patch names that test and how to run it.

`cargo xtask planted` reads a case file and a committed fuzz input for every
patch under `qa/planted`. These patches have neither, so they live here.

`cases.json` maps every patch to its package, exact test selector, source,
and intended assertion. Most cases use library tests. The handoff case uses
the `zakura-sim` integration test named in its `test_target` field and needs
`RUSTFLAGS="--cfg zakura_sim"`.

Run `python3 scripts/mutation_inventory.py --output "$TMPDIR/mutations.json"`
from the repository root to check patch application and binding metadata.
The command does not apply patches or compile tests. A matching source symbol
does not prove that Cargo selects a test. Before scoring a native case, verify
its exact selector with `--list`, require one executed test, and compare the
unchanged, patched, and restored runs. Only the declared assertion counts as
detection. Native assertions do not acquire simulator property IDs through
this mapping.
