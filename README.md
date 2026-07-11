![Zakura logotype](book/theme/favicon.png)

---

[![Unit Tests](https://github.com/zakura-core/zakura/actions/workflows/tests-unit.yml/badge.svg)](https://github.com/zakura-core/zakura/actions/workflows/tests-unit.yml)
[![Lint](https://github.com/zakura-core/zakura/actions/workflows/lint.yml/badge.svg)](https://github.com/zakura-core/zakura/actions/workflows/lint.yml)
[![Integration Tests (GCP)](https://github.com/zakura-core/zakura/actions/workflows/zfnd-ci-integration-tests-gcp.yml/badge.svg)](https://github.com/zakura-core/zakura/actions/workflows/zfnd-ci-integration-tests-gcp.yml)
[![codecov](https://codecov.io/gh/zakura-core/zakura/branch/main/graph/badge.svg)](https://codecov.io/gh/zakura-core/zakura)
[![Build docs](https://github.com/zakura-core/zakura/actions/workflows/book.yml/badge.svg)](https://github.com/zakura-core/zakura/actions/workflows/book.yml)
[![Deploy Nodes (GCP)](https://github.com/zakura-core/zakura/actions/workflows/zfnd-deploy-nodes-gcp.yml/badge.svg)](https://github.com/zakura-core/zakura/actions/workflows/zfnd-deploy-nodes-gcp.yml)
![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)

- [Getting Started](#getting-started)
  - [Docker](#docker)
  - [Manual Install](#manual-install)
- [CI/CD Architecture](#cicd-architecture)
- [Documentation](#documentation)
- [User support](#user-support)
- [Security](#security)
- [License](#license)

[Zakura](https://github.com/zakura-core/zakura/) is a Zcash full node written in Rust.

## Getting Started

You can run Zakura using our [Docker
image](https://hub.docker.com/r/zakura-core/zakura/tags) or you can install it manually.

### Docker

This command will run our latest release, and sync it to the tip:

```sh
docker run -d \
  --name zakura \
  -p 8233:8233 \
  -v zakurad-cache:/home/zakura/.cache/zakura \
  zakura-core/zakura:latest
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
cargo install --git https://github.com/zakura-core/zakura --tag v1.0.0-rc2 zakura
```

You can start Zakura by running

```sh
zakurad start
```

Refer to the [Building and Installing
Zakura](book/src/user/install.md) and [Running
Zakura](book/src/user/run.md) sections in the book for enabling
optional features, detailed configuration and further details.

## CI/CD Architecture

Zakura uses a comprehensive CI/CD system built on GitHub Actions to ensure code
quality, maintain stability, and automate routine tasks. Our CI/CD
infrastructure:

- Runs automated tests on every PR and commit.
- Manages deployments to various environments.
- Handles cross-platform compatibility checks.
- Automates release processes.

For a detailed understanding of our CI/CD system, including workflow diagrams,
infrastructure details, and best practices, see our [CI/CD Architecture
Documentation](.github/workflows/README.md).

## Documentation

The Zakura maintainers provide the following resources:

- The Zakura Book:
  - [General Introduction](README.md),
  - [User Documentation](book/src/user.md),
  - [Developer Documentation](book/src/dev.md).

  - User guides of note:
    - [Zakura Health Endpoints](book/src/user/health.md) — liveness/readiness checks for Kubernetes and load balancers

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
