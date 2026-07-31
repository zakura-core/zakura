# Header-chain fuzzing

This isolated `cargo-fuzz` package is intentionally not a workspace member.

Run a target from this directory with a pinned nightly toolchain:

```console
cargo +nightly fuzz run header_codec -- -dict=fuzz_dicts/header_sync.dict
```

The available targets are `header_codec`, `fork_transitions`, `header_pursuit`,
and `recovery_rows`. `cargo-fuzz` creates and updates each target's local
`corpus/` and `artifacts/` directories; both are intentionally ignored by Git.

When libFuzzer reports a failure, reproduce and minimize the artifact:

```console
cargo +nightly fuzz run header_codec path/to/crash-artifact
cargo xtask minimize-header-fuzz fuzz/header-chain/artifacts/<target>/crash-…
```

Turn every confirmed bug into a normal deterministic test in the owning crate.
Do not commit raw or minimized corpus files.
