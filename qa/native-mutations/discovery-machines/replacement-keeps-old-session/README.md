# A replacement session keeps the old session admitted

`DiscoveryState::admit_session` reports a same-direction replacement as
admitted but keeps the old session's entry. The old session stays current,
so its pending `Hello` installs and the replacement's results are
superseded.

The witness's history admits peer 0's connection 1 session 1 over its
connection 0 session 0 (step 4). The model makes the new session current,
and the patched state keeps the old one.

## Witness

`qa/planted/discovery/session-installation/install-superseded-result.toml`, the work list's witness for `discovery/session-installation`. It holds on `main`, and
its committed input reaches the patched code.

## Reproduce

The loop applies the patch in its own worktree. Run it over this
property's cases:

```sh
cargo xtask planted loop --only discovery/session-installation --out <report dir>
```

Required failure: the run ends at `discovery/session-installation`.
