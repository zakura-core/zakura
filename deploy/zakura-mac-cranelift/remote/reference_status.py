"""Inspect private monitoring permissions and the published dashboard handoff."""
import json, pathlib, pwd, subprocess, sys, time, urllib.request
result = {}
for name in ['zakura-fleet-watchdog', 'zakura-mainnet-dashboard']:
    check = subprocess.run(['systemctl', 'is-active', name], capture_output=True, text=True, timeout=10)
    result[name] = check.stdout.strip()
ssh = pathlib.Path('/etc/zakura-mac-verifier/ssh')
if ssh.exists():
    account = pwd.getpwnam('zakura-mac-verifier')
    paths = [ssh, *(ssh / name for name in ['config', 'id_ed25519', 'known_hosts'])]
    result['monitoring_config_private'] = all(path.stat().st_uid == account.pw_uid
        and not path.stat().st_mode & 0o077 for path in paths)
    check = subprocess.run(['sudo', '-n', '-u', 'zakura-mac-verifier', 'ssh', '-F', str(ssh / 'config'),
                            '-T', 'mac-verifier', 'id'], input=b'', capture_output=True, timeout=8)
    result['monitoring_key_restricted'] = check.returncode == 0 and check.stdout == b''
guard = pathlib.Path('/etc/zakura-mainnet-dashboard/private/addresses.json')
result['private_address_config_private'] = guard.is_file() and all(
    path.stat().st_uid == 0 and not path.stat().st_mode & 0o077
    for path in [guard.parent, guard])
public = pathlib.Path('/var/lib/zakura-mac-cranelift-public/status.json')
result['dashboard_file_present'] = public.is_file()
try:
    sample = json.loads(public.read_text())
    result['status'] = {key: sample.get(key) for key in ('sample_time', 'condition')}
    stamp = sample.get('sample_time')
    result['dashboard_file_fresh'] = type(stamp) in (int, float) and 0 <= time.time() - stamp <= 90
    result['dashboard_file_healthy'] = sample.get('condition') == 'matching'
    identity = json.loads(pathlib.Path('/etc/zakura-mac-verifier/dashboard.json').read_text())
    result['dashboard_identity_matches'] = sample.get('verifier_id') == identity['verifier_id'] == sys.argv[1]
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open('http://127.0.0.1:8090/data', timeout=10) as response:
        rows = json.load(response)['rows']
    result['dashboard_row_healthy'] = any(row.get('name') == 'mac-os-cranelift'
        and row.get('healthy') is True and type(row.get('height')) is int and row['height'] > 0 for row in rows)
except (OSError, ValueError, KeyError, TypeError):
    result['dashboard_row_healthy'] = False
    result['status'] = None
script = pathlib.Path('/opt/zakura-mainnet-dashboard/zakura-cluster-status.py')
result['dashboard_supports_mac'] = 'ZAKURA_MAC_CRANELIFT_STATUS' in script.read_text()
check = subprocess.run(['systemctl', 'show', 'zakura-mainnet-dashboard', '-p', 'Environment', '--value'],
                       capture_output=True, text=True, timeout=10)
result['dashboard_mac_enabled'] = 'ZAKURA_MAC_CRANELIFT_STATUS=1' in check.stdout
print(json.dumps(result))
