# Orchard Merkle fuzzing

This isolated `cargo-fuzz` package checks the consensus-relevant weighted
Orchard Merkle hash against the direct Sinsemilla implementation.

Run the target with the pinned nightly toolchain:

```console
cargo +nightly-2026-07-15 fuzz run --fuzz-dir . orchard_merkle_equivalence -- -max_len=65
```

The target interprets each 65-byte input as one tree level and two full-width
Pallas field elements. It compares both hashes byte-for-byte. The fuzzer can
detect implementation, bit-packing, and level-mapping regressions. It cannot
test the discrete-logarithm assumption that makes Sinsemilla exceptional cases
infeasible.

`cargo-fuzz` creates local `corpus/` and `artifacts/` directories. Git ignores
both directories. Turn every confirmed failure into a deterministic test in
`zakura-chain`.
