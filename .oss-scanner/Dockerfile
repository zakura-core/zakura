# Image for Anthropic's OSS Scanner (https://red.anthropic.com/oss-scanner/), which
# enrolls this repository as projects/zakura in https://github.com/anthropics/oss-scanner.
# It is not a production image; docker/Dockerfile builds those.
#
# The scanner builds this file with the repository checkout as the build context and
# with network access, then audits the result with no network at all. So this build
# installs the toolchain, fetches every crate, and compiles zakurad and every test
# binary, leaving `cargo` able to work offline. .oss-scanner/threat_model.md says what
# matters and how to run things.
#
# Dockerfile.dockerignore beside this file replaces the repository's .dockerignore for
# this build, so docs/ and the other files the production images leave out are in /src.
# x86-64 only: the cargo-nextest download below is the Linux x86-64 release.

# The same Rust release docker/Dockerfile builds with.
FROM rust:1.97.1-trixie
ENV DEBIAN_FRONTEND=noninteractive

# libclang-dev is for bindgen, which the RocksDB bindings run at build time.
RUN apt-get update && apt-get install -y --no-install-recommends \
      libclang-dev clang cmake pkg-config python3 \
    && rm -rf /var/lib/apt/lists/*

# cargo-nextest, the test runner CI uses.
RUN curl -LsSf https://get.nexte.st/latest/linux | tar zxf - -C "${CARGO_HOME}/bin" \
    && cargo nextest --version

# scanner contract: the checkout lives inside the image, at /src
COPY . /src
WORKDIR /src

# rust-toolchain.toml asks for "stable". The image's toolchain is the release CI and the
# release binaries build with, so pin to it instead of downloading another one. The
# ci-tests profile is release code generation without LTO; this adds line-level debug
# info and frame pointers for debuggers and profilers. Acceptance tests that need
# Internet peers skip themselves when SKIP_NETWORK_TESTS is set (see zakura-test).
ENV RUSTUP_TOOLCHAIN=1.97.1 \
    CARGO_INCREMENTAL=0 \
    CARGO_PROFILE_CI_TESTS_DEBUG=limited \
    RUSTFLAGS="-C force-frame-pointers=yes" \
    SKIP_NETWORK_TESTS=1
RUN rustup component add clippy rustfmt

# Every crate in Cargo.lock, for every target and feature, so that builds with other
# features than the ones below also work offline.
RUN cargo fetch --locked

# Build as CI does: the ci-tests profile and the release feature set. `portable` selects
# Pasta arithmetic that runs on any x86-64 CPU, not only the one that built it.
RUN cargo build --locked --profile ci-tests --features default-release-binaries,portable \
      --package zakura --bins \
    && ls -l target/ci-tests/zakurad

# Compile every test binary without running it, so the whole suite is runnable offline.
RUN cargo nextest run --locked --cargo-profile ci-tests \
      --features default-release-binaries,portable --no-run

# Run the cheap tests so the image is known to work, but do not let a failing test abort
# the build. The filterset picks the tests without changing which packages Cargo builds.
RUN (timeout 900 cargo nextest run --locked --cargo-profile ci-tests \
      --features default-release-binaries,portable -E 'package(zakura-chain)' \
     || echo "WARNING: zakura-chain tests reported failures")

# Everything is fetched: from here on cargo never tries the network.
ENV CARGO_NET_OFFLINE=true
