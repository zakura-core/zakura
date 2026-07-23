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
For a standard `artifacts/<target>/crash-*` path, run:

```console
cargo xtask minimize-header-fuzz fuzz/header-chain/artifacts/<target>/crash-…
```

The command invokes `cargo fuzz tmin` with the CI-pinned nightly and prints the
minimized SHA-256, bounded operation bytes, and a target-specific Rust
regression template.

`fork_transitions` consumes at most 512 operation bytes. Bits 3–6 select linear
or fork insertion, stale-version work, operator invalidation/reconsideration,
body mismatch/invalid/unavailable/verified evidence, deferred
insertion/reevaluation, clock advancement, full-state verified-path
replacement/finality, or crash/reopen; bits 0–2 bound insertion length to 1–8
headers. Invalid references are explicit refused operations; valid
informational/idempotent no-effects are counted separately. The shared
feature-gated replay function runs the production planner and independently
rebuilds retained indexes, eligibility, work ordering, and projections after
every operation. It is also used by deterministic corpus tests.

`header_pursuit` consumes at most 512 bytes as four-byte operations over twenty
logical peers. It drives the production peer work queue, response-page and
outcome predicates, and completion ownership gate against an independent
bounded model. Checked-in seeds cover exact completion, wrong targets, wrong
ancestry, explicit outcomes, stale generations, and the 17th-pursuit capacity
boundary.

`recovery_rows` snapshots all twelve header-engine column families as a raw
logical dump, applies at most 64 bounded row/key/value mutations, and installs
the dump in one RocksDB batch. Startup must either perform only a named
source-derived repair and reopen cleanly at the authenticated frontier, or fail
without changing any logical row and before constructing a publisher.
