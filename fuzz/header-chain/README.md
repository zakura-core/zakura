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
