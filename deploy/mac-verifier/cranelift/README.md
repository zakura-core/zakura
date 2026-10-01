# Native Apple Silicon Cranelift candidate

The deployment candidate workflow builds Rust code, including its static standard
library, with a patched Cranelift backend on an isolated ARM64 Mac. Zakura source
comes from the requested deployment `ref` (branch, tag or SHA); PR builds use
the workflow commit. The compiler remains pinned to backend
`05409775adc5f87a3aae12184486301f70ca519d` and `nightly-2026-09-30`.

`macos-unwind.patch` corrects ARM64 Mach-O personality encoding and pointer-to-GOT
relocation. It keeps exception tables in a shared read-only section while each
frame references its own exception-table symbol. The compiler profile uses
Apple's classic linker, `panic=unwind`, disabled LTO and one Cargo build job.
Compiler bootstrap, native C dependencies and system libraries use their normal
toolchains; this does not establish an entirely LLVM-free build.

## Compiler acceptance

`probes/` is one small release-mode crate. It checks nested destructor order,
panic payload preservation, cleanup across an async suspension point, async lock
release, healthy-task survival and Tokio panic reporting. A separate invocation
must abort with `SIGABRT` when cleanup itself panics. These checks exercise the
unwinding metadata modified by the backend patch; compilation alone cannot do so.

`qualify.py` builds the node and executes all eight existing consensus cases in
`../corpus.json`, plus these actual network panic-containment tests:

- `zakura::transport::pipe::tests::supervised_pipe_runs_teardown_on_panic`
- `zakura::transport::pipe::tests::supervised_peer_task_runs_teardown_and_disconnect_on_panic`

Each invocation must report exactly one passed test, zero failed and zero ignored.
The driver checks the clean checkout of the requested source commit and the complete tracked
backend contents against the pinned backend plus accepted patch. Git diff
formatting does not affect that comparison. It refuses a build host with a
running `zakurad` and requires fresh output outside both source checkouts.

## Candidate CI and runtime boundary

The `Build Cranelift Mac verifier deployment candidate` workflow runs in PR CI
and supports manual dispatch with a source `ref`. It prepares the pinned backend and static standard
library, runs the acceptance driver, verifies native ARM64 output and uploads
only the binary and receipt after every gate passes. Receipts record source,
lockfile, compiler patch, backend, binary, SDK and configuration provenance.
Failed builds publish no deployment candidate.

Installation uses the mainnet CI workflow's explicit `zakura-mac-os` target and
the requested source `ref`, as described in the [runtime README](../README.md).
CI builds a candidate automatically; a matching successful candidate run ID can
optionally be reused.
It preserves comparison cursors, incident history and queued alerts across binary
changes. Compiler acceptance does not establish live health, sustained runtime
qualification or permission to enable alerts. Independent compiler review and
operational qualification remain required.
