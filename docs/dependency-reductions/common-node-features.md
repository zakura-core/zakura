# Common node feature reduction proof

This demo is stacked on [the separate wallet-feature integration](https://github.com/zakura-core/zakura/pull/1436).
That baseline integrates Common's merged BIP 32 and PCZT separation. Its
five-package reduction on x86-64 and six-package reduction on ARM64 are not
attributed to this stack.

All pins use the canonical Common repository. This demo selects standard I/O,
omits wallet derivation and memory accounting, and constructs coinbase rewards
through the key-free entry point. Transparent validation, shielded validation,
and coinbase proving stay enabled.

Normal and build dependencies of the default release node were resolved with
Cargo 1.97.1, identical features and targets, and unchanged unrelated dependency
versions. Counts deduplicate package names and versions across feature
instances. The exact source revisions and package deltas are recorded in
[common-node-features.json](common-node-features.json).

| Target | Wallet baseline | Standard I/O | No ZIP 32 | No memory accounting |
| --- | ---: | ---: | ---: | ---: |
| Linux x86-64 | 328 | 327 | 326 | 325 |
| Linux ARM64 | 325 | 324 | 323 | 322 |
| macOS ARM64 | 326 | 325 | 324 | 323 |

Each layer removes one external package: `corez` 0.1.1, `zip32` 0.2.1, and
`memuse` 0.2.2, respectively. No external packages are added. ZIP 32 itself
retains memory accounting, so the last removal is cumulative. Whole-workspace
and development graphs may still contain packages omitted from production.

Common stack:

- [Standard I/O](https://github.com/zakura-core/common/pull/586)
- [Optional wallet derivation and coinbase entry point](https://github.com/zakura-core/common/pull/587)
- [Optional memory accounting](https://github.com/zakura-core/common/pull/588)

Reproduce a row with the target replaced as needed:

```sh
cargo +1.97.1 tree --locked -p zakura --features default-release-binaries \
  --edges normal,build --target x86_64-unknown-linux-gnu \
  --prefix none --format '{p}|{f}'
```

These are graph measurements. They do not establish compilation on all three
targets, runtime behavior, vulnerability remediation, or a speedup. RPC library
checks have passed through each source layer. Sixty-four focused node tests passed, including transparent coinbase generation,
Sapling and Orchard proof validation, and a real Ironwood coinbase proof.
Full CI and independent review remain acceptance gates.

Exact-revision cargo-vet exemptions carry the existing Common audit backlog and
the unpublished changes under review. They are not audit certifications. This
is a draft integration proof; release use requires reviewed Common releases
and replacing the temporary source pins.
