# Threat model for the OSS Scanner

Zakura is a Zcash full node in Rust: a consensus validator that downloads the
chain from peers, verifies every block and transaction, stores the result in
RocksDB, and serves the chain to local clients over JSON-RPC. It is a fork of
the Zcash Foundation's Zebra, with its own peer protocol, QUIC transport,
header-chain engine, and many changes to the inherited code. Most nodes on the
Zcash network now run Zakura, and the mining pools behind more than 80% of the
network's hashrate build their blocks on it, so a bug here is a bug in Zcash.
Zakura's security policy is [`docs/security.md`](../docs/security.md).

## Where untrusted input enters

- **Peers on the Internet are the main attack surface.** Any host can connect
  on the listening port (8233 on mainnet) or be connected to. Everything a
  peer sends is attacker-controlled: handshake and version messages, address
  gossip, inventory, headers, blocks, transactions, and every message of
  Zakura's own peer protocol (header sync, block sync, and the QUIC transport
  in `zakura-quic`). Zakura keeps many peer connections open and must stay
  healthy while some of them are hostile.
- **Blocks and transactions are untrusted even from honest peers.** Their
  content comes from whoever mined or broadcast them. Deserialization in
  `zakura-chain`, verification in `zakura-consensus` and `zakura-script`, and
  storage in `zakura-state` all run on attacker-chosen bytes.
- **JSON-RPC is semi-trusted.** The server is off by default. When enabled it
  is meant for the operator's own wallets, lightwalletd, and mining software,
  on loopback or a private network. A listener without cookie authentication
  is restricted to a classified method set; the admin listener is loopback
  only. Treat a malicious RPC client as a less likely attacker than a peer,
  but a bug that lets an RPC request corrupt consensus state or stall the
  node still matters. The indexer gRPC server is optional and requires
  mutual TLS off loopback.
- **Trusted:** the configuration file, the data directory on disk, environment
  variables, and the command line. The operator controls these.

## What matters most

In descending order:

1. `zakura-consensus`, `zakura-script`, `zakura-chain` and `zakura-state`:
   consensus. A block or transaction that Zakura accepts but `zcashd`/Zebra
   reject, or rejects when they accept, splits the chain. Missing checks,
   wrong arithmetic, serialization that is not canonical or not round-trip
   safe, sigop or size accounting errors, and state transitions that do not
   match the protocol all belong here. `TrustedPreallocate` and bounded
   deserialization are the defense against memory exhaustion from oversized
   collections.
2. `zakura-network` and `zakura-quic`: denial of service and eclipse
   resistance. One hostile peer, or a handful of Sybil identities, must not be
   able to crash the node, stall sync, exhaust memory, disk or file
   descriptors, get honest peers banned, or keep the node on a stale tip.
   Panics in a peer task are contained (they disconnect only that peer; see
   `panic = "unwind"` in `Cargo.toml`), so a panic is a finding when it
   reaches shared or consensus state, not when it only drops the offender.
3. `zakura-header-chain` and `zakura-mmr-tree`: header sync and the
   commitments it trusts. Peer-supplied roots and proofs must be
   authenticated before anything is built on them.
4. `zakura-rpc`: `getblocktemplate` and `submitblock` correctness (an invalid
   template costs a miner a block), `zcashd` compatibility, and the method
   classification of the unauthenticated listener.
5. `zakurad`: startup, configuration handling, and the wiring between the
   components.

Less important or out of scope: `crates/xtask`, `crates/zakura-test`,
`crates/zakura-utils`, `crates/zakura-jsonl-trace`, `crates/zakura-scheduler`
(test-only), `crates/zakura-assets`
(a publish-only packaging crate), `deploy/`, `docker/`, `qa/`, `scripts/`,
and the CI under `.github/`. The `zcashd` compatibility mode drives an
external `zcashd` binary that is not in this repository and not in the image;
bugs in `zcashd` itself are out of scope. Code that only runs in tests is out
of scope.

## How to exercise it

- The image was built with the `ci-tests` Cargo profile and the features
  `default-release-binaries,portable`, so use those on every `cargo` command,
  or it recompiles the workspace. `SKIP_NETWORK_TESTS=1` is set, which makes
  the tests that need Internet peers skip themselves, and `CARGO_NET_OFFLINE`
  keeps Cargo off the network.
- Run one crate's tests with a nextest filterset, which picks tests without
  changing the set of packages Cargo builds, so nothing is recompiled:
  `cargo nextest run --cargo-profile ci-tests --features default-release-binaries,portable -E 'package(zakura-network)'`.
  (`-p zakura-network` would fail: the features belong to the `zakura`
  package.) The profiles in `.config/nextest.toml` select larger or slower
  sets. Unit and property tests live in `src/**/tests/` (`prop.rs`,
  `vectors.rs`, `preallocate.rs`); acceptance tests that start a node are in
  `crates/zakurad/tests/`.
- The node binary is `target/ci-tests/zakurad`. `zakurad generate` prints a
  default configuration, and `docker/default-zakura-config.toml` is an
  annotated one. Set `network = "Regtest"` under `[network]` to run a node
  that needs no peers; mainnet and testnet need the Internet. The end-to-end
  test in `crates/zakurad/tests/zakura_regtest_e2e.rs` shows how regtest
  nodes are driven, though it needs Docker, which the image does not have.
- `qa/fuzz/` holds `cargo-fuzz` targets for the header chain and the RPC
  server. They need a nightly toolchain, which the image does not have, but
  they show which parsers are worth attacking.
- The specifications in `docs/specs/` and the design notes in `docs/design/`
  say how the peer protocol, block sync, header sync and QUIC transport are
  meant to behave. A behavior that contradicts them is a bug even before it
  is an exploit.

## How we rate severity

- **Critical:** a chain split (accepting a block or transaction that the
  Zcash protocol rejects, or rejecting one it accepts); anything that lets
  money be created or spent against the protocol; remote code execution;
  memory corruption reachable from a peer.
- **High:** a single unauthenticated peer, or a small number of them, can
  crash the node, stall or permanently freeze sync, exhaust memory, disk or
  file descriptors, get honest peers banned, or eclipse the node; a mining
  template that is invalid or leaves out valid transactions; corruption of
  the on-disk state; a panic that unwinds through shared or consensus state.
- **Medium:** denial of service that needs many Sybil identities, sustained
  bandwidth, or a large fraction of the node's peers; resource exhaustion
  that a documented configuration limit bounds; a bug that needs a malicious
  RPC client on an unauthenticated listener; leaks of the node's own view of
  the network or chain.
- **Low:** bugs that need the operator's configuration, data directory or
  command line to be malicious; panics or errors that only disconnect the
  offending peer; issues in test-only code, examples, tooling and
  documentation.

Zakura shares much code with upstream Zebra. If a finding is also present
upstream, say so: we coordinate disclosure with the Zcash Foundation as
`docs/security.md` describes.

## Reports and patches

A report is most useful with a reproducer we can run offline: a Rust test in
the affected crate, or a script that drives `zakurad` on regtest or sends
crafted messages to it. Say which crate and function is at fault and which
consensus rule, specification clause or resource bound is violated. A patch
should be a diff against `main` that keeps the existing tests passing and adds
one for the bug; prefer the smallest change that closes the hole over a
refactor.

## Leave alone

- Unauthenticated RPC exposed to the Internet against the documentation, and
  any issue that needs cookie credentials to have been stolen.
- The theoretical cost of a syncing node downloading the whole chain, and
  resource use that a documented configuration limit already bounds.
- Timing side channels: the node holds no wallet or signing keys, and the
  RPC cookie is compared in constant time.
- Dependencies that are not in this repository, including the `zakura-*`
  cryptography and protocol crates from
  [`zakura-core/common`](https://github.com/zakura-core/common), which is
  enrolled separately. Zakura's use of them is in scope; their internals are
  not.
