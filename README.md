![Zakura logotype](book/theme/zakura-flower-v1.svg)

---

[![Unit Tests](https://github.com/zakura-core/zakura/actions/workflows/tests-unit.yml/badge.svg)](https://github.com/zakura-core/zakura/actions/workflows/tests-unit.yml)
[![Lint](https://github.com/zakura-core/zakura/actions/workflows/lint.yml/badge.svg)](https://github.com/zakura-core/zakura/actions/workflows/lint.yml)
[![codecov](https://codecov.io/gh/zakura-core/zakura/branch/main/graph/badge.svg)](https://codecov.io/gh/zakura-core/zakura)
[![Build docs](https://github.com/zakura-core/zakura/actions/workflows/book.yml/badge.svg)](https://github.com/zakura-core/zakura/actions/workflows/book.yml)
![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)

- [Getting Started](#getting-started)
  - [Docker](#docker)
  - [Manual Install](#manual-install)
- [Documentation](#documentation)
- [User support](#user-support)
- [Security](#security)
- [License](#license)

[Zakura](https://github.com/zakura-core/zakura/) is a fully consensus compatible Zcash full node written in Rust, built for scale. We dream of the future where Zcash can power the worlds payments. Mastercard and Visa give a lower bound, we have to first hit 50k TPS of capacity. With ongoing cryptographic optimizations to the Zcash protocol, from [Project Tachyon](https://tachyon.z.cash/) and [Valargroup](https://valargroup.dev), this implies consensus must be capable of at least 100MB/s of block data. The starting point today is 28 KB/s. Zakura builds for this future.

Zakura is forked off of [Zebra](https://github.com/ZcashFoundation/zebra). This first release brings major improvements over existing Zcash node software:

- Performance: Blockchain sync is nearly 5× faster than Zebra. Block execution is notably faster than Zebra on worst case sandblast attacks as well.
- Pruning and snapshots: Native block pruning with configurable retention cuts disk usage substantially. We also publish snapshots (~11 GB pruned) that let you bootstrap a node 680× faster than syncing over the standard P2P network. See [here](https://zakura.com/snapshots/)
- zcashd compatibility: A compatibility mode reproduces the legacy zcashd RPC interface, so existing wallets and integrations keep working.
- Experimental P2P v2: We are building a new P2P transport layer for Zakura nodes, currently off by default on Mainnet. The goals are sub-500ms worst-case block propagation, mempool aggregation (used in Tachyon), sync at the speed of your bandwidth, and a future-proofed gossip protocol. The native stack has known DoS risks and is not yet production-hardened; see its [current tradeoffs and exit criteria](book/src/user/p2p.md).

## Getting Started

You can run Zakura using our [Docker
image](https://hub.docker.com/r/valargroup/zakura/tags) or you can install it manually.

### Docker

This command will run our latest release, and sync it to the tip:

```sh
docker run -d \
  --name zakura \
  -p 8233:8233 \
  -v zakurad-cache:/home/zakura/.cache/zakura \
  valargroup/zakura:latest
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
   - **clang** or another C++ compiler (`g++,` which is for all platforms or
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
cargo install --git https://github.com/zakura-core/zakura --tag v1.0.0 zakura
```

You can start Zakura by running

```sh
zakurad start
```

Refer to the [Building and Installing
Zakura](book/src/user/install.md) and [Running
Zakura](book/src/user/run.md) sections in the book for enabling
optional features, detailed configuration and further details.

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

If you want to chat with us, use the project discussion channels linked from the Zakura repository.

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
