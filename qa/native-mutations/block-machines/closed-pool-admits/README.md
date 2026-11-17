# A closed pool generation admits blocks

The pool's three closure checks are gone: before the pipeline charge, in
the retention loop, and after the charge. A closed generation keeps
admitting blocks, so a backend that closed a pool to drain it never drains.

The patch removes all three checks, because each one alone refuses the
admission. The witness's history closes pool 2 and then admits to it
(step 14). The model refuses the admission, and the patched pool admits
it.

## Witness

`qa/planted/block/decoded-retention/missing-admission.toml`, the work list's witness for `block/decoded-retention`. It holds on `main`, and
its committed input reaches the patched code.

## Reproduce

The loop applies the patch in its own worktree. Run it over this
property's cases:

```sh
cargo xtask planted loop --only block/decoded-retention --out <report dir>
```

Required failure: the run ends at `block/decoded-retention`.
