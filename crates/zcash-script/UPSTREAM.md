# Upstream provenance

Zakura imported the Rust crate from ZcashFoundation/zcash_script 0.4.5 at
`cc4ec7ee8b96103586588e51e0f7cb3ebad4d944`.
The source, tests, and license retain their upstream attribution.
The Cargo manifest preserves the published 0.4.5 dependency requirements.
Zakura keeps the package name and version so the workspace patch unifies all
`^0.4` consumers, including zakura-primitives and zakura-transparent.

Zakura changes raw script sigop counting to skip complete pushes without
applying execution limits. Script execution still rejects pushes above 520 bytes.
The isolated `qa/script-oracle` workspace pins the original C++ implementation.

Publication requires a separate package and library dependency decision.
Cargo does not propagate this workspace's patch to crates.io consumers.
Do not publish this migration until that delivery path uses the owned crate.

Zakura also updates one `map_or` expression for the workspace Clippy version
and forbids unsafe code in the owned interpreter.

Zakura also adds raw P2SH redeem extraction. The counter accepts complete
oversized pushes and clears pushed data after small-integer opcodes, matching
C++ GetOp. Execution still applies the original push limit.

Zakura omits the upstream release changelog because this private crate has no
independent publication flow. CI preserves spelling in the pinned source.
