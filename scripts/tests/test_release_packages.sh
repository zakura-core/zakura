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

printf 'release package identity survives manifest moves\n'
