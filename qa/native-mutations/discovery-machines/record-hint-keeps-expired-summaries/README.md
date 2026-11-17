# A record-backed hint keeps expired live summaries

`DiscoveryState::sync_active_services` prunes expired live summaries only
from hints without a record. A connected peer whose own record backs its
hint keeps serving live summaries past their expiry.

The witness's history installs peer 0's record, receives its live summaries
with a two-second expiry, waits 40 seconds and reads the hints (step 10).
The model reports no live summary, and the patched state still serves
one.

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
