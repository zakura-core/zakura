#!/bin/bash
set -euo pipefail

/app/docker-entrypoint.sh true

sed -i 's/}$/,"switching":{}}/g' /app/config.json
node <<'NODE'
const fs = require("fs");

const configPath = "/app/pool_configs/zcash.json";
const config = JSON.parse(fs.readFileSync(configPath));
const asicPort = process.env.STRATUM_PORT || "3333";
const cpuPort = process.env.CPU_STRATUM_PORT || "3334";
const minimumDifficulty = 0.000125;

function parseDifficulty(name, fallback) {
    const value = Number(process.env[name] || fallback);
    if (!Number.isFinite(value) || value < minimumDifficulty) {
        throw new Error(`${name} must be a number greater than or equal to ${minimumDifficulty}`);
    }
    return value;
}

const asicDifficulty = parseDifficulty("ASIC_DIFFICULTY", "64");
const cpuDifficulty = parseDifficulty("CPU_DIFFICULTY", "0.000125");

config.ports = {
    [asicPort]: {
        tls: false,
        diff: asicDifficulty,
    },
    [cpuPort]: {
        tls: false,
        diff: cpuDifficulty,
    },
};

fs.writeFileSync(configPath, JSON.stringify(config, null, 2));
NODE

if [ "$NETWORK" = "Mainnet" ]; then
    sed -i 's|tmRGc4CD1UyUdbSJmTUzcB6oDqk4qUaHnnh|t1Hsc1LR8yKnbbe3twRp88p6vFfC5t7DLbs|g' /app/pool_configs/zcash.json
    sed -i 's|blockRefreshInterval": 500|blockRefreshInterval": 2000|g' /app/config.json
fi

exec node init.js
