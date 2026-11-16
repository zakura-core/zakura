# Full verification slots let workers start without one

`ZakuraDiscoveryHandle::verify_off_lock` tries to reserve a verification
slot and starts the worker without one when none is free. Unsettled
verification results then pass the slot count.

The witness holds the state lock once the readers fill the single slot. The
other readers' workers start without a slot, so three workers run while no
record installs.

## Witness

`qa/planted/discovery/verification-capacity/release-before-install.toml`, the work list's witness for `discovery/verification-capacity`. It holds on `main`, and
its committed input reaches the patched code.

## Reproduce

The loop applies the patch in its own worktree. Run it over this
property's cases:

```sh
cargo xtask planted loop --only discovery/verification-capacity --out <report dir>
```

Required failure: the run ends at `discovery/verification-capacity`.
