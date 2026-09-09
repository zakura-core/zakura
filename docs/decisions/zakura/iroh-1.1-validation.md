# Iroh 1.1 preparation evidence

## Source and package boundary

The integration consumes compatibility commit
`9e6cbf251d2edd6304c2d9799fc29bf39788d305` from the networking fork. Compared
with upstream Iroh 1.1.0, all 115 packaged Rust files are unchanged. Three
requirements retain published ed25519-dalek 2.2.0 and curve25519-dalek 4.1.3.
BIP32 and the selected Zcash cryptographic package versions and sources remain
unchanged in both the integration and the prepared registry-consumer graph.

The fork's `scripts/prepare-zakura-packages.py` prepares four names at
`1.1.0-rc.1`: `zakura-iroh-base`, `zakura-iroh-dns`, `zakura-iroh-relay`, and
`zakura-iroh`. Sibling dependencies use exact registry versions and package
aliases. Library names remain unchanged. DNS server and benchmark packages
are excluded from publication. The script only writes a new output directory,
records its source commit and hashes Rust files; it has no publication command.

All four local package archives build with Cargo's verification enabled.
A separate copy of the full Zakura workspace also passes `cargo check --workspace
--locked` using the prepared package aliases and temporary local source patches.
Those local patches stand in for packages that have not been published; this is
not a claim that crates.io can resolve the proposed names today. The fork's
`PUBLICATION.md` and generated `zakura-consumer.toml` define the eventual switch.
No registry names were reserved and no packages were uploaded.

The prepared archives were generated from fork commit
`e1f60bfeac7c314b943654658c11a4e81b929aab`. That commit adds tests and preparation
tooling to the consumed compatibility source; production Rust remains unchanged.

## Authentication review

The TLS verifier requires TLS 1.3 and raw public keys. The server's presented
Ed25519 key must match the endpoint identity requested by the client. Both
certificate-verifier roles delegate transcript signatures to Iroh's public-key
verification method, which calls Dalek `verify_strict`. The retained version
rejects small-order keys and signature R points and checks canonical scalars.
The resolved feature set does not enable `legacy_compatibility`.

The added authentication tests cover an RFC8032 known-answer signature, wrong
keys, changed messages, a bit mutation in every signature byte, weak-key forgery,
noncanonical scalars and invalid key/signature lengths. They pass with both the
compatibility fork and unmodified upstream Iroh 1.1. Existing key and serialization
tests also pass. These checks support the narrow dependency delta; they do not
constitute a new audit of every dependency or all upstream transport code.

## Supply-chain coverage

Explicit `audit-as-crates-io = true` policies preserve the registry baseline
requirement and add review of each Git delta. The four recorded deltas cover
only the compared source and manifest changes. They do not exempt the upstream
baseline or trust all future publications from any maintainer.

Cargo-vet still reports 69 versions missing `safe-to-deploy` coverage. This is
missing audit evidence, not 69 identified vulnerabilities. The remaining work
includes Iroh/noq, their networking and platform dependencies, and new versions
of smaller supporting packages. The exact versions and suggested review bases
are recorded in `qa/supply-chain/iroh-1.1-audit-backlog.json`. Refresh that file
from `cargo vet check --output-format json` after dependency or audit changes.

Publication under the proposed names does not erase this review requirement.
The new registry packages will need audit records tied to their published
contents. The existing cargo-vet CI gate remains enabled and fails until the
remaining coverage is supplied; no blanket exemption has been added.

## Network validation

The regtest fixture now assigns node2 and node4 distinct native UDP ports,
18334 and 18534. The previous host-network collision prevented node4 startup.
The full Linux regtest PR gate passes with those explicit ports.

Same-cohort native process tests transfer 1 MiB, while mixed native cohorts
fail within the bounded operation deadline. Mixed dual-stack process tests
complete Ping/Pong both ways without a native handoff. The production-limit
transfer test sends 64 MiB in each direction, and the exact noq 1.2 tail-loss
regression passes on macOS and Linux.
The complete noq-proto 1.2 library suite also passes on both platforms: 421
tests each, including simulated loss, reordering, congestion and MTU changes.
Those protocol simulations do not substitute for full-node Linux impairment
or resource measurements. Cargo-deny advisories, bans, licenses and sources pass.

`scripts/test_iroh_full_nodes.py` runs independently pinned old and new local
binaries with fresh regtest state and explicit loopback ports. It mines blocks
on each peer, requires propagation to the other, restarts the client with empty
state, and requires catch-up while the seed remains idle. It also checks that
both peers retain TCP and that native connection and upgrade metrics remain zero.

The full-node comparison uses old source
`44f5afa38272932271789e0a854979887d23a0d5` and new source
`90bb8cd16111d1a8dc7794f279ed18402e22cea7`. Both mixed directions pass block
propagation and fresh-client recovery at a 66-block gap, recovering in 30.9
seconds with two retained legacy connections on each peer.

After building separate binaries from those revisions, run:

```sh
python3 scripts/test_iroh_full_nodes.py --old-bin /path/to/old-zakurad \
  --new-bin /path/to/new-zakurad --output-dir /tmp/iroh-small-gap
python3 scripts/test_iroh_full_nodes.py --old-bin /path/to/old-zakurad \
  --new-bin /path/to/new-zakurad --output-dir /tmp/iroh-large-gap \
  --blocks-per-peer 33 --catchup-timeout 180
```

Each output directory must be new. The harness records configurations, node
logs, metrics and JSON results, and terminates only its own processes.

The small-gap restart check must allow the existing ten-minute fallback window.
The faster legacy probe requires a gap of at least 64 blocks; a six-block gap
does not meet it. A two-minute catch-up deadline failed despite working TCP and
block propagation. Do not infer fast restart recovery from the Ping/Pong check.
With the full fallback window, both six-block runs pass: the new client recovers
in 603.58 seconds and the old client in 603.26 seconds. Both peers reach the same
tip, retain two legacy connections each and report no native handoff. This
existing recovery delay remains an operational limitation for a mixed cohort.

Long-duration deployment soak and broader Linux loss, delay, reordering and MTU
comparisons remain separate release gates; no deployment has been performed.
