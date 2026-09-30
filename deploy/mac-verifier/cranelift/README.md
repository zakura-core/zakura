# Native Apple Silicon Cranelift build

This optional compiler profile uses Cranelift for Rust machine-code generation,
including the static standard library, with panic unwinding enabled. Zakura's
consensus source stays at `af944f5194ef2e9921bc96af017629450375013c`.
The default verifier builder continues to use Rust 1.97.1.

Build on an isolated Apple Silicon development host with no running verifier.
The deployed Mac is not a compiler experiment host or a general CI runner.
The recipe does not provision hosts, change live databases or enable alerts.

## Compiler provenance

Pin [rustc_codegen_cranelift](https://github.com/rust-lang/rustc_codegen_cranelift/tree/05409775adc5f87a3aae12184486301f70ca519d)
to `05409775adc5f87a3aae12184486301f70ca519d` and Rust to
`nightly-2026-09-30`. Apply `macos-unwind.patch` before building the backend.
It corrects the ARM64 Mach-O personality encoding and pointer-to-GOT relocation.
Mach-O exception tables use a shared read-only section: each frame description
still references its own exception-table symbol. Allocating a custom section per
function produced more than 72,000 sections in one network test object and
exhausted the classic linker worker stack.

The accepted profile uses Apple's classic linker, static Cranelift standard
library, `panic=unwind`, disabled LTO and one Cargo build job. The compiler and
build tooling use their normal bootstrap compiler. Native C dependencies,
system libraries and the runtime unwinder retain their normal toolchain.
This profile does not establish an entirely LLVM-free toolchain.

## Rebuild the backend and standard library

Install Xcode command-line tools, Python 3, Git, Rustup, CMake and Protobuf.
Choose a fresh build directory outside the repository. In the commands below,
`recipe_dir` is this directory and `backend_dir` is the new backend checkout.
Keep build logs and receipts private; never put host configuration in them.

```sh
rustup toolchain install nightly-2026-09-30 --profile minimal --component rust-src
git clone https://github.com/rust-lang/rustc_codegen_cranelift.git "$backend_dir"
git -C "$backend_dir" checkout --detach 05409775adc5f87a3aae12184486301f70ca519d
git -C "$backend_dir" apply "$recipe_dir/macos-unwind.patch"
python3 "$recipe_dir/prepare_std.py" --backend "$backend_dir"
(cd "$backend_dir" && CARGO_BUILD_JOBS=1 ./y.sh build --panic-unwind-support --keep-sysroot)
```

`prepare_std.py` copies the pinned Rust source, applies the upstream standard
library patches and selects an rlib-only standard library. It refuses an existing
staging directory and does not edit Rustup's installed source. The shared
standard-library build is outside this qualified profile.

## Acceptance and deployment

Require the four standalone probe configurations in `probes/` (debug and
optimized basic/extended unwinding), double-panic abort, and the async probe's
100 Tokio task panics. Build every Rust dependency with this backend and static
sysroot. Use `-Cpanic=unwind -Clink-arg=-Wl,-ld_classic`, locked dependencies,
`CARGO_BUILD_JOBS=1` and `CARGO_PROFILE_RELEASE_LTO=false`.

Build `zakurad` from the clean pinned source and execute all eight cases in
`../corpus.json` by exact name. Also execute these exact network library tests,
requiring one passed, zero failed and zero ignored for each invocation:

- `zakura::transport::pipe::tests::supervised_pipe_runs_teardown_on_panic`
- `zakura::transport::pipe::tests::supervised_peer_task_runs_teardown_and_disconnect_on_panic`

The broad `supervised_` filter also selects a normal-exit case, so a fixed
expectation of two selected tests is incorrect. Never accept a zero-test run.
Record source, lockfile, patch, backend, binary, SDK and configuration digests.
No acceptance gate authorizes notification enablement.

Run the probes, node build and exact-case gates with a fresh private output
directory outside both checkouts:

```sh
python3 "$recipe_dir/qualify.py" --backend "$backend_dir" \
  --source "$source_dir" --output "$qualification_dir"
```

The driver refuses a host with a running `zakurad`, source revision drift,
modified consensus source, a different backend patch and zero-test successes.
It records the candidate binary digest and individual gate results. Double panic
must abort with `SIGABRT`; each exact corpus and containment case must execute.
The driver does not deploy the binary.

For a binary change, preserve and verify the current rollback binary and receipts,
stop the comparator, and coordinate the Mac binary/receipt, adapter restart and
Linux receipt identity. Preserve the bootstrap anchor, comparison cursor, retained
history, incidents and outbox. Reset qualification counters for the new compiler.
The monitor deliberately rejects an unexpected receipt change; do not erase its
state to get past that guard. Configuration, consensus source or bootstrap changes
require their own reviewed coverage boundary.

After switching, verify the actual running native binary, startup replay, new
mainnet advancement, hashes and decoded Sapling/Orchard/Ironwood state against the
Linux reference, resource headroom and recovery. Historical finalized state stays
trusted. Count new compiler coverage conservatively after the post-switch
reference baseline. Short startup checks do not establish sustained qualification.

The initial deployed candidate passed the full build, all eight exact consensus
cases, both exact containment cases and five unwind probe configurations. Its
initial live check passed 14 consecutive healthy samples and three new compared
blocks. Full compiler conformance, independent review and sustained operational
qualification remain separate gates. The compiler patch has not been submitted
upstream. The verifier PR remains draft until its review and qualification gates
are complete.
