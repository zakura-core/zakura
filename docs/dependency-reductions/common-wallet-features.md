# Common transparent wallet feature boundary

This is the baseline integration of Common's merged BIP 32 and PCZT feature
boundary. It contains no standard-I/O, ZIP 32, or memory-accounting changes.

All Common packages use the canonical Common repository at the same pinned
revision. Normal and build dependencies of the default release node were
compared using Cargo 1.97.1 and unchanged unrelated dependency versions.
Feature instances are deduplicated by package name and version. The machine-
readable receipt is [common-wallet-features.json](common-wallet-features.json).

| Target | Unchanged node | Wallet features omitted | Removed | Added |
| --- | ---: | ---: | ---: | ---: |
| Linux x86-64 | 333 | 328 | 5 | 0 |
| Linux ARM64 | 331 | 325 | 6 | 0 |
| macOS ARM64 | 332 | 326 | 6 | 0 |

The five common removals are `bip32` 0.6.0, `hmac` 0.13.0, `sha2` 0.11.1,
`ripemd` 0.2.0, and `digest` 0.11.3. `cpufeatures` 0.3.0 also disappears on
ARM64, but remains on x86-64 through note encryption. The Common source change
adds no package names or unrelated version upgrades to the lockfile.

Reproduce each graph with the target replaced by the desired row:

```sh
cargo +1.97.1 tree --locked -p zakura --features default-release-binaries \
  --edges normal,build --target x86_64-unknown-linux-gnu \
  --prefix none --format '{p}|{f}'
```

These are resolved dependency graph results. They do not by themselves prove
compilation, runtime behavior, vulnerability remediation, or a speedup. The
focused RPC library and chain test targets compiled against this revision;
full CI, runtime regression tests, and policy checks remain acceptance gates.

The draft uses exact-revision cargo-vet exemptions for the Common pins. The
existing v3.0.0 audit backlog remains, and changes since publication require
review before deployment. Passing the policy check with these exemptions is
not an audit certification.
