#!/usr/bin/env bash
set -euo pipefail
# Keep Ubuntu's signed package metadata, using a mirror outside Canonical's DNS.
sed -i -E 's#https?://[^ ]*(archive.ubuntu.com|security.ubuntu.com|mirrors.digitalocean.com)/ubuntu/?#https://mirror.math.princeton.edu/pub/ubuntu/#g' /etc/apt/sources.list.d/ubuntu.sources
apt-get -o Acquire::Retries=1 -o Acquire::https::Timeout=20 update
DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends build-essential pkg-config librocksdb-dev clang libclang-dev protobuf-compiler shellcheck git curl ca-certificates unzip zip jq python3 python3-venv python3-pip iptables libicu-dev libssl-dev zstd xz-utils nodejs npm
if ! id zakura-ci >/dev/null 2>&1; then
  useradd --system --user-group --create-home --home-dir /var/lib/zakura-ci/home --shell /bin/bash zakura-ci
fi
install -d -m 755 /opt/zakura-ci /opt/zakura-ci/cargo /opt/zakura-ci/rustup
export CARGO_HOME=/opt/zakura-ci/cargo RUSTUP_HOME=/opt/zakura-ci/rustup
curl --max-time 60 --proto '=https' --tlsv1.2 -fsSL https://sh.rustup.rs -o /tmp/zakura-rustup.sh
bash /tmp/zakura-rustup.sh -y --profile minimal --default-toolchain stable --no-modify-path
export PATH="$CARGO_HOME/bin:$PATH"
rustup component add clippy rustfmt
rustup toolchain install nightly --profile minimal --component rustfmt
# System libraries and toolchains remain read-only to the job account.
chmod -R a+rX /opt/zakura-ci
if [[ ! -f /swapfile ]]; then
  fallocate -l 8G /swapfile
  chmod 600 /swapfile
  mkswap /swapfile
  swapon /swapfile
  echo '/swapfile none swap sw 0 0' >> /etc/fstab
fi
nproc
free -m
pkg-config --modversion rocksdb
rustc --version
