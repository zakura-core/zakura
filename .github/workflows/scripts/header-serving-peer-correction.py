import json
import subprocess
from pathlib import Path
sha=subprocess.check_output(['git','-C','/root/zakura','rev-parse','HEAD'],text=True).strip()
assert sha in {'a30202f2f1c47e08e9c2f8510392353c37d24dfd','71e93477b2e9cd240c5181e5bf3ef281227967c4'}
assert not Path('/root/out/paired/events.jsonl').exists(), 'native phase already started'
old='initial_mainnet_peers = ["104.131.174.28:8233"]\npeerset_initial_target_size = 1'
new='initial_mainnet_peers = ["138.197.11.145:8233", "209.38.85.70:8233", "159.65.183.89:8233", "104.131.184.123:8233", "dnsseed.z.cash:8233", "dnsseed.str4d.xyz:8233"]\npeerset_initial_target_size = 25'
paths=[Path('/root/zakura/deploy/deployer/templates/zakura.toml'),Path('/etc/zakura/zakura.toml')]
contents=[p.read_text() for p in paths]
assert all(old in s for s in contents), 'configuration differs from expected fixture'
for p,s in zip(paths,contents): p.write_text(s.replace(old,new))
subprocess.run(['systemctl','restart','zakurad'],check=True,timeout=100)
print(json.dumps({'action':'test_seed_peer_correction','sha':sha,'peers':new,'database':'preserved'}))
