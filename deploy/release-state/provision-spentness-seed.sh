#!/usr/bin/env bash
# Copy a reviewed artifact to one seed and verify it with that seed's compiled pins.
set -euo pipefail

ARTIFACT=${1:?usage: provision-spentness-seed.sh <artifact> <ssh-host> <absolute-cache-dir>}
SEED=${2:?supply the seed SSH host}
CACHE=${3:?supply the seed cache directory}
if ! [[ "$SEED" =~ ^[a-zA-Z0-9@._-]+$ && "$SEED" != -* && "$CACHE" =~ ^/[a-zA-Z0-9/._-]+$ ]]; then
    echo "seed host and cache path contain unsupported characters" >&2
    exit 1
fi
DIGEST=$(sha256sum "$ARTIFACT" | cut -d' ' -f1)
ssh "$SEED" "mkdir -p '$CACHE'"
scp "$ARTIFACT" "$SEED:$CACHE/$DIGEST.incoming"
ssh "$SEED" "zakura-spentness install --artifact '$CACHE/$DIGEST.incoming' --cache '$CACHE'"
ssh "$SEED" "rm -- '$CACHE/$DIGEST.incoming'"
echo "Provisioned $DIGEST on $SEED. Restart the configured seed to load and advertise it."
