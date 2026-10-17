# Header-chain fuzzing

This isolated `cargo-fuzz` package is intentionally not a workspace member.

Run a target from this directory with a pinned nightly toolchain. Pass
`--fuzz-dir .`, because `cargo-fuzz` otherwise looks for `fuzz/` under the
workspace root:

```console
cargo +nightly-2026-07-15 fuzz run --fuzz-dir . header_codec -- -dict=fuzz_dicts/header_sync.dict
```

The available targets are `header_codec`, `header_codec_v9`, `fork_transitions`,
`header_pursuit`, and `recovery_rows`. `header_codec_v9` fuzzes the header sync
version 9 codec, which nothing negotiates yet. Its first input byte selects the
header size and message type, and the rest is the payload.
`cargo-fuzz` creates and updates each target's local `corpus/` and `artifacts/`
directories; both are intentionally ignored by Git.
The RocksDB-backed `recovery_rows` target must use
`ASAN_OPTIONS=detect_leaks=0`: RocksDB retains process-global allocations after
temporary databases close, so its exit-time LeakSanitizer reports are not
actionable. AddressSanitizer remains enabled for the target.

When libFuzzer reports a failure, reproduce and minimize the artifact:

```console
cargo +nightly-2026-07-15 fuzz run --fuzz-dir . header_codec path/to/crash-artifact
cargo xtask minimize-header-fuzz qa/fuzz/header-chain/artifacts/<target>/crash-…
```

Turn every confirmed bug into a normal deterministic test in the owning crate.
Do not commit raw or minimized corpus files.
