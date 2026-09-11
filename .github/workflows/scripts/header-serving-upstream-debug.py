import json,subprocess,urllib.request
from pathlib import Path
sha=subprocess.check_output(['git','-C','/root/zakura','rev-parse','HEAD'],text=True).strip()
assert sha in {'a30202f2f1c47e08e9c2f8510392353c37d24dfd','71e93477b2e9cd240c5181e5bf3ef281227967c4'}
try:
 urllib.request.urlopen('http://127.0.0.1:18232',timeout=2)
except urllib.error.URLError:
 pass
else:
 raise RuntimeError('client is running; do not interrupt its supplier')
p=Path('/etc/zakura/zakura.toml');s=p.read_text()
assert 'p2p_stack = "dual"' in s
assert '\nfilter =' not in s
s=s.replace('[network]','[network]\nexpose_peer_addresses = true')
s=s.replace('[tracing]','[tracing]\nfilter = "info,zakura_network::peer::handshake=debug,zakura_network::peer_set::initialize=debug"')
p.write_text(s)
subprocess.run(['systemctl','restart','zakurad'],check=True,timeout=100)
print(json.dumps({'action':'enable_test_seed_handshake_diagnostics','sha':sha}))
