#!/usr/bin/env python3
"""Deploy an accepted CI artifact to the existing private Mac verifier."""
import argparse
import ipaddress
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import tempfile

PACKAGE = Path(__file__).resolve().parents[1] / 'mac-verifier'
sys.path.insert(0, str(PACKAGE))
from status_bridge import public_status

BASE = '/Library/Application Support/ZakuraVerifier'
MAC_PYTHON = '/opt/homebrew/opt/python@3.12/bin/python3.12'


class SSH:
    """Keep private endpoints and remote output out of public CI logs."""
    def __init__(self, prefix, directory):
        host = str(ipaddress.ip_address(os.environ[prefix + 'HOST']))
        user = os.environ[prefix + 'USER']
        if not re.fullmatch(r'[a-zA-Z_][a-zA-Z0-9_-]{0,31}', user):
            raise ValueError('invalid SSH user')
        port = os.environ.get(prefix + 'SSH_PORT') or '22'
        if not port.isdecimal() or not 1 <= int(port) <= 65535:
            raise ValueError('invalid SSH port')
        paths = []
        for name in ['SSH_KEY', 'KNOWN_HOSTS']:
            value = os.environ[prefix + name]
            if not value.strip():
                raise ValueError('missing deployment secret')
            path = Path(directory) / (prefix + name)
            path.write_text(value.rstrip('\n') + '\n')
            path.chmod(0o600)
            paths.append(str(path))
        self.command = ['ssh', '-i', paths[0], '-p', port, '-o', 'BatchMode=yes',
                        '-o', 'IdentitiesOnly=yes', '-o', 'StrictHostKeyChecking=yes',
                        '-o', 'UserKnownHostsFile=' + paths[1], '-o', 'ConnectTimeout=10',
                        '-o', 'ServerAliveInterval=15', '-o', 'ServerAliveCountMax=3',
                        user + '@' + host]

    def run(self, script, timeout=60):
        result = subprocess.run(self.command + ['bash -s'], input=script,
                                capture_output=True, text=True, timeout=timeout)
        if result.returncode:
            raise RuntimeError('remote operation failed (private output withheld)')
        return result.stdout


def status(mac, linux, identifier):
    mac_info = json.loads(mac.run(f'''sudo -n {MAC_PYTHON} - <<'REMOTE'
import json, pathlib, platform, subprocess
base = pathlib.Path({BASE!r})
result = {{'architecture': platform.machine(), 'receipt_present': (base / 'receipt.json').is_file()}}
for name in ['node', 'adapter', 'tunnel']:
    check = subprocess.run(['launchctl', 'print', 'system/dev.valargroup.zakura-verifier-' + name], capture_output=True, text=True, timeout=10)
    result[name + '_running'] = check.returncode == 0 and 'state = running' in check.stdout
print(json.dumps(result))
REMOTE
'''))
    info = json.loads(linux.run('''sudo -n python3 - <<'REMOTE'
import json, pathlib, subprocess
result = {}
for name in ['zakura-mac-verifier', 'zakura-mac-verifier-dashboard', 'zakura-mainnet-dashboard']:
    check = subprocess.run(['systemctl', 'is-active', name], capture_output=True, text=True, timeout=10)
    result[name] = check.stdout.strip()
path = pathlib.Path('/var/lib/zakura-mac-verifier/status.json')
result['status'] = json.loads(path.read_text()) if path.exists() else None
script = pathlib.Path('/opt/zakura-mainnet-dashboard/zakura-cluster-status.py')
result['dashboard_supports_mac'] = 'ZAKURA_PRIVATE_VERIFIER_STATUS' in script.read_text()
check = subprocess.run(['systemctl', 'show', 'zakura-mainnet-dashboard', '-p', 'Environment', '--value'], capture_output=True, text=True, timeout=10)
result['dashboard_mac_enabled'] = 'ZAKURA_PRIVATE_VERIFIER_STATUS=1' in check.stdout
print(json.dumps(result))
REMOTE
'''))
    raw = info.pop('status')
    if raw:
        info['status'] = public_status(raw, identifier)
    print(json.dumps({'mac': mac_info, 'reference': info}, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('operation', choices=['status'])
    args = parser.parse_args()
    identifier = os.environ['MAC_VERIFIER_ID']
    if not re.fullmatch(r'verifier-[a-f0-9]{32}', identifier):
        raise ValueError('invalid verifier identity')
    with tempfile.TemporaryDirectory(prefix='mac-verifier-') as directory:
        mac = SSH('MAC_VERIFIER_', directory)
        linux = SSH('MAC_VERIFIER_REFERENCE_', directory)
        status(mac, linux, identifier)


if __name__ == '__main__':
    try:
        main()
    except Exception:
        raise SystemExit('Mac verifier operation failed; private remote output withheld') from None
