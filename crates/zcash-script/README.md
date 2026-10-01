# Zakura-owned Zcash script implementation

This crate implements Zcash transparent scripts in Rust.
Zakura vendors the 0.4.5 API and dependency line.
[UPSTREAM.md](UPSTREAM.md) records the source revision and local changes.
The crate keeps the upstream Apache-2.0 license.

The workspace patch makes all script consumers use this source.
Raw counting skips complete pushes of any encoded size.
Execution retains the 520-byte push limit.
The isolated [C++ oracle](../../qa/script-oracle/README.md) checks compatibility.

Zakura does not publish this crate under the upstream package name.
The migration must resolve package delivery before merge.
