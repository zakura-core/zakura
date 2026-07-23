# Header-chain fuzzing

This isolated `cargo-fuzz` package is intentionally not a workspace member.

Run a target from this directory with a pinned nightly toolchain:

```console
cargo +nightly fuzz run header_codec -- -dict=fuzz_dicts/header_sync.dict
```

When libFuzzer reports a failure, preserve the artifact bytes and exact command,
then reproduce it without mutation:

```console
cargo +nightly fuzz run header_codec path/to/crash-artifact
```

Minimized artifacts belong in the matching checked-in `corpus/<target>/`
directory and must also receive a deterministic regression test in the owning
crate. Record the target, artifact SHA-256, decoded input or operation list,
first divergent snapshots, and transition receipt in the regression comment.

`fork_transitions` consumes at most 512 operation bytes. Bits 3–5 select linear
or fork insertion, stale-version work, operator invalidation/reconsideration,
or crash/reopen; bits 0–2 bound insertion length to 1–8 headers. Invalid
references are explicit refused operations. The shared feature-gated replay
function runs the production planner, verifies generation deltas and committed
snapshots after every operation, and is also used by deterministic corpus tests.

`header_pursuit` consumes at most 512 bytes as four-byte operations over twenty
logical peers. It drives the production peer work queue, response-page and
outcome predicates, and completion ownership gate against an independent
bounded model. Checked-in seeds cover exact completion, wrong targets, wrong
ancestry, explicit outcomes, stale generations, and the 17th-pursuit capacity
boundary.
