# RPC fuzzing

This isolated `cargo-fuzz` package is intentionally not a workspace member.
It has no committed `Cargo.lock`: Cargo resolves its dependencies when it builds.

Run a target from this directory with a pinned nightly toolchain. Pass
`--fuzz-dir .`, because `cargo-fuzz` otherwise looks for `fuzz/` under the
workspace root:

```console
cargo +nightly-2026-07-15 fuzz run --fuzz-dir . json_rpc_request
```

`json_rpc_request` sends its input through the RPC server's request handling,
using `zakura_rpc::server::fuzz::json_rpc_request`. That covers the body reader,
the JSON-RPC request parser, batch handling, each method's parameter decoding,
and response encoding. Methods are not run. The target fails on a panic or on a
response that isn't valid JSON. Its first input byte picks the maximum response
size, its second byte splits the body into two chunks, and the rest is the
request body.

`cargo-fuzz` creates and updates the target's local `corpus/` and `artifacts/`
directories; both are intentionally ignored by Git.

When libFuzzer reports a failure, reproduce it with the artifact:

```console
cargo +nightly-2026-07-15 fuzz run --fuzz-dir . json_rpc_request path/to/crash-artifact
```

Turn every confirmed bug into a normal deterministic test in `zakura-rpc`.
Do not commit raw or minimized corpus files.
