<p align="center">
  <img src="book/theme/zakura-flower-v1.svg" alt="Zakura logo" width="160">
</p>

---

[![Unit Tests](https://github.com/zakura-core/zakura/actions/workflows/tests-unit.yml/badge.svg)](https://github.com/zakura-core/zakura/actions/workflows/tests-unit.yml)
[![Lint](https://github.com/zakura-core/zakura/actions/workflows/lint.yml/badge.svg)](https://github.com/zakura-core/zakura/actions/workflows/lint.yml)
[![codecov](https://codecov.io/gh/zakura-core/zakura/branch/main/graph/badge.svg)](https://codecov.io/gh/zakura-core/zakura)
[![Build docs](https://github.com/zakura-core/zakura/actions/workflows/book.yml/badge.svg)](https://github.com/zakura-core/zakura/actions/workflows/book.yml)
![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)

- [Getting Started](#getting-started)
  - [Installer](#installer)
  - [Docker](#docker)
  - [Manual Install](#manual-install)
- [Documentation](#documentation)
- [User support](#user-support)
- [Security](#security)
- [License](#license)

[Zakura](https://github.com/zakura-core/zakura/) is a consensus-compatible Zcash full node written in Rust, built for scale. Supporting payments at global scale requires on the order of 50k TPS, which translates to consensus processing at least 100 MB/s of block data. Today's chain peaks at 28 KB/s. With cryptographic optimizations to the Zcash protocol underway in [Project Tachyon](https://tachyon.z.cash/) and at [Valargroup](https://valargroup.dev), Zakura is the node implementation built to close that gap.

Zakura is forked from [Zebra](https://github.com/ZcashFoundation/zebra). This first release brings major improvements over existing Zcash node software:

- Performance: Blockchain sync is nearly 5× faster than Zebra. Block execution is notably faster than Zebra, especially on worst-case sandblast attacks.
- Pruning and snapshots: Native block pruning with configurable retention lets you operate a full node with substantially less disk space. We also publish snapshots (~11 GB pruned) that let you bootstrap a node 680× faster than syncing over the standard P2P network. See the [snapshots page](https://zakura.com/snapshots/).
- [zcashd compatibility](book/src/user/zcashd-compat.md): A compatibility mode
  reproduces the legacy zcashd RPC interface, so existing wallets and
  integrations keep working.
- Experimental P2P v2: We are building a new P2P transport layer for Zakura nodes, currently off by default on Mainnet. The goals are sub-500ms worst-case block propagation, mempool aggregation (used in Tachyon), sync at the speed of your bandwidth, and a future-proofed gossip protocol. The v2 stack has known DoS risks and is not yet production-hardened; see its [current tradeoffs and production readiness criteria](book/src/user/p2p.md).

## Getting Started

There are several ways to install the node software: an interactive installer that downloads the binary, Docker, or building from source or crates.io.

### Installer

The simplest way to install Zakura on a new machine is using the interactive installer:

```console
curl -fsSL https://raw.githubusercontent.com/zakura-core/zakura/main/scripts/install-zakura.sh | bash
```

The installer can set up either standard Zakura or its
[zcashd-compatible variant](book/src/user/zcashd-compat.md).

### Docker

You can run Zakura using our [Docker
image](https://hub.docker.com/r/zakuracore/zakura/tags) or install it manually.
The zcashd-compatible split-container mode uses the
[zakuracore/zcashd:v1.0.0 image](https://hub.docker.com/r/zakuracore/zcashd/tags).

This command will run our latest release and sync it to the tip:

```sh
docker run -d \
  --name zakura \
  -p 8233:8233 \
  -v zakurad-cache:/home/zakura/.cache/zakura \
  zakuracore/zakura:latest
```

The `-p 8233:8233` flag exposes the P2P port so other Zcash nodes can connect to
yours, and `-v` persists the chain state across restarts (use port `18233` for
Testnet). For more information, read our [Docker
documentation](book/src/user/docker.md).

### Manual Install

Building Zakura requires [Rust](https://www.rust-lang.org/tools/install),
[libclang](https://clang.llvm.org/doxygen/group__CINDEX.html), and a C++
compiler. Below are quick summaries for installing these dependencies.

[//]: # "The empty lines in the `summary` tag below are required for correct Markdown rendering."

<details><summary>

#### General Instructions for Installing Dependencies

</summary>

1. Install [`cargo` and `rustc`](https://www.rust-lang.org/tools/install).
2. Install Zakura's build dependencies:
   - **libclang**, which is a library that comes under various names, typically
     `libclang`, `libclang-dev`, `llvm`, or `llvm-dev`;
   - **clang** or another C++ compiler (`g++`, which is for all platforms, or
     `Xcode`, which is for macOS);
   - **[`protoc`](https://grpc.io/docs/protoc-installation/)** (optional).

</details>

[//]: # "The empty lines in the `summary` tag below are required for correct Markdown rendering."

<details><summary>

#### Dependencies on Arch Linux

</summary>

```sh
sudo pacman -S rust clang protobuf
```

Note that the package `clang` includes `libclang` as well. The GCC version on
Arch Linux has a broken build script in a `rocksdb` dependency. A workaround is:

```sh
export CXXFLAGS="$CXXFLAGS -include cstdint"
```

</details>

Once you have the dependencies in place, you can install Zakura with:

```sh
cargo install --locked zakura
```

Alternatively, you can install it from GitHub:

```sh
cargo install --git https://github.com/zakura-core/zakura --tag v1.0.0-rc6 zakura
```

You can start Zakura by running

```sh
zakurad start
```

Refer to the [Building and Installing
Zakura](book/src/user/install.md) and [Running
Zakura](book/src/user/run.md) sections in the book for enabling
optional features, detailed configuration, and further details.

## Documentation

The Zakura maintainers provide the following resources:

- The [documentation of the public
  APIs](https://docs.rs/zakura/latest/zakurad/#zakura-crates) for the latest
  releases of the individual Zakura crates.

- The [documentation of the internal APIs](https://zakura-core.github.io/zakura/internal)
  for the `main` branch of the whole Zakura monorepo.

## User support

If Zakura doesn't behave the way you expected, [open an
issue](https://github.com/zakura-core/zakura/issues/new/choose). We regularly
triage new issues and we will respond. We maintain a list of known issues in the
[Troubleshooting](book/src/user/troubleshooting.md) section of
the book.

If you want to chat with us, use the GitHub issues for now.

## Security

Zakura has a [responsible disclosure
policy](https://github.com/zakura-core/zakura/blob/main/SECURITY.md), which
we encourage security researchers to follow.

## License

Zakura is distributed under the terms of both the MIT license and the Apache
License (Version 2.0). Some Zakura crates are distributed under the [MIT license
only](LICENSE-MIT), because some of their code was originally from MIT-licensed
projects. See each crate's directory for details.

See [LICENSE-APACHE](LICENSE-APACHE) and [LICENSE-MIT](LICENSE-MIT).
