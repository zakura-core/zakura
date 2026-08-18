#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
# shellcheck source=scripts/lib/release-packages.sh
. "${repo_root}/scripts/lib/release-packages.sh"

fixture="$(mktemp -d)"
trap 'rm -rf "$fixture"' EXIT
cd "$fixture"

git init --quiet
git config user.name "Release Test"
git config user.email "release-test@example.com"
mkdir -p old/demo
cat > old/demo/Cargo.toml <<'EOF'
[package]
name = "demo-crate"
version = "1.2.3"
edition = "2021"
EOF
git add .
git commit --quiet -m "add package"
git tag base

mkdir crates
git mv old/demo crates/demo
git commit --quiet -m "move package"

expected=$'demo-crate\t1.2.3\told/demo/Cargo.toml'
actual="$(list_release_packages_at base)"
if [ "$actual" != "$expected" ]; then
  printf 'expected:\n%s\nactual:\n%s\n' "$expected" "$actual" >&2
  exit 1
fi

cat > dependent.toml <<'EOF'
[dependencies]
zakura-node-services = { path = "../zakura-node-services", version = "3.2.0" }
zakura-rpc = { path = "../zakura-rpc", version = "7.0.0" }
zakura-node-services-legacy = { package = "zakura-node-services", path = "../zakura-node-services", version = "3.1.0" }

[dev-dependencies]
zakura-node-services = { path = "../zakura-node-services", version = "3.2.1", features = [
    "rpc-client",
] }

[target.'cfg(unix)'.dependencies]
zakura-node-services = "3.2.0"
EOF
cat > expected-dependent.toml <<'EOF'
[dependencies]
zakura-node-services = { path = "../zakura-node-services", version = "3.2.1-rc0" }
zakura-rpc = { path = "../zakura-rpc", version = "7.0.0" }
zakura-node-services-legacy = { package = "zakura-node-services", path = "../zakura-node-services", version = "3.2.1-rc0" }

[dev-dependencies]
zakura-node-services = { path = "../zakura-node-services", version = "3.2.1-rc0", features = [
    "rpc-client",
] }

[target.'cfg(unix)'.dependencies]
zakura-node-services = "3.2.1-rc0"
EOF

rewrite_prerelease_dependency_requirements \
  dependent.toml zakura-node-services 3.2.1-rc0
if ! diff -u expected-dependent.toml dependent.toml; then
  exit 1
fi

printf 'release package identity survives manifest moves\n'
printf 'prerelease dependency rewrites include older compatible requirements\n'
printf 'prerelease dependency rewrites cover renamed and bare-string requirements\n'
