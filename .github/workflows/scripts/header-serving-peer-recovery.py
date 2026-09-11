"""Restore public bootstrap peers on the exact disposable test seed only."""
import json
import subprocess
from pathlib import Path

expected = "ab4a68521f1a3161537c1f3003c1bc2cce1f05ef"
actual = subprocess.check_output(["git", "-C", "/root/zakura", "rev-parse", "HEAD"], text=True).strip()
assert actual == expected, actual
assert not Path("/root/out/paired/downloader.toml").exists(), "paired serving test already started"
old = 'initial_mainnet_peers = ["104.131.174.28:8233"]\npeerset_initial_target_size = 1'
new = 'initial_mainnet_peers = ["138.197.11.145:8233", "209.38.85.70:8233", "159.65.183.89:8233", "104.131.184.123:8233", "dnsseed.z.cash:8233", "dnsseed.str4d.xyz:8233"]\npeerset_initial_target_size = 25'
paths = [Path("/etc/zakura/zakura.toml"), Path("/root/zakura/deploy/deployer/templates/zakura.toml")]
texts = [p.read_text() for p in paths]
assert 'p2p_stack = "legacy"' in texts[0]
for text in texts:
    assert text.count(old) == 1
subprocess.run(["systemctl", "stop", "zakurad"], check=True, timeout=90)
for path, text in zip(paths, texts):
    path.write_text(text.replace(old, new))
with Path("/root/out/notes.md").open("a") as notes:
    notes.write("- The pinned legacy bootstrap peer gave no connection. Restored public bootstrap peers and restarted only the seed during preparation, before the paired test. Preserved the state and candidate binary.\n")
subprocess.run(["systemctl", "start", "zakurad"], check=True, timeout=90)
print(json.dumps({"action": "test_seed_peer_recovery", "sha": actual, "state": "preserved", "paired_test_started": False}))
