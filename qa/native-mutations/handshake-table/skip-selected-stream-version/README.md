# Skip selected stream version

The handler bypasses the unselected-version admission guard while retaining stream setup and service execution.

The patch changes production behavior only. Honest streams still pass the remaining guards.

## Reproduce

Run on the droplet:

```sh
cargo xtask planted loop --only handshake/prelude/unselected-version-resets --generate-runs 100 --generate-harness handshake/wire
```

Witness: `crates/zakura-sim/tests/handshake.rs::prelude_guards_apply_each_declared_consequence`.

Required failure: the run ends at `handshake/prelude/unselected-version-resets`.

## Other-rule triage

The `d23f46bbb1` campaign first failed `session/installed-services-authorized`.
The prelude rule waited for a reset after production had accepted the malformed
prelude. The rule now compares each accepted prelude with the peer's label and
fails at acceptance. A new droplet classification must verify the named rule.
