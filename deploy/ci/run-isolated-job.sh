#!/usr/bin/env bash
# Run one Actions job, then discard every file its unprivileged account can write.
# systemd kills the service's remaining processes before restarting this script.
set -euo pipefail
readonly state=/var/lib/zakura-ci
readonly job="$state/job"
readonly registration=/opt/zakura-ci/registration

rm -rf "$job"
install -d -o zakura-ci -g zakura-ci -m 700 "$job"
cp -a "$registration/." "$job/runner"
mkdir -p "$job/home" "$job/cargo/bin" "$job/rustup" "$job/toolcache"
cp -a /opt/zakura-ci/cargo/bin/. "$job/cargo/bin/"
# Each job can update its own toolchains without changing the prepared copies.
cp -a /opt/zakura-ci/rustup/. "$job/rustup/"
python3 -m venv "$job/venv"
chown -R zakura-ci:zakura-ci "$job"
cd "$job/runner"
exec runuser -u zakura-ci -- env \
  HOME="$job/home" CARGO_HOME="$job/cargo" RUSTUP_HOME="$job/rustup" \
  RUNNER_TOOL_CACHE="$job/toolcache" AGENT_TOOLSDIRECTORY="$job/toolcache" \
  PATH="$job/venv/bin:$job/cargo/bin:/usr/local/bin:/usr/bin:/bin" \
  CARGO_BUILD_JOBS=12 ROCKSDB_LIB_DIR=/usr/lib/x86_64-linux-gnu \
  ./run.sh --once
