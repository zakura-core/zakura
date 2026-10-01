#!/usr/bin/env python3
"""Deploy an accepted CI artifact to the existing private zakura-mac-cranelift."""
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
import time
import traceback

PACKAGE = Path(__file__).resolve().parents[1] / 'zakura-mac-cranelift'
sys.path.insert(0, str(PACKAGE))
sys.path.insert(0, str(PACKAGE.parent / "runner"))
from mac_cranelift_status import public_status

# Installed paths, launchd labels and service accounts are persistent deployment
# bindings. Keep them stable across the repository/CI naming change.
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
            hints = [hint for hint in ['Bootstrap failed', 'Boot-out failed', 'Permission denied',
                      'FileNotFoundError', 'SyntaxError', 'ModuleNotFoundError'] if hint in result.stderr]
            print('Remote exit code:', result.returncode, 'diagnostic categories:', hints, flush=True)
            raise RuntimeError('remote operation failed (private output withheld)')
        return result.stdout

    def put(self, data, path):
        command = ('set -eu; umask 077; staged=$(mktemp); '
                   'trap \'rm -f "$staged"\' EXIT; cat > "$staged"; '
                   'sudo -n install -m 644 "$staged" ' + shlex.quote(path))
        result = subprocess.run(self.command + [command], input=data,
                                capture_output=True, timeout=120)
        if result.returncode:
            raise RuntimeError('private transfer failed')


def probe_program():
    source = (PACKAGE / 'ssh_probe.py').read_text()
    shared_import = 'from common import RPC, Transport, Unavailable, MAX_JSON, digest, integer, read_json, hex_bytes\n'
    if source.count(shared_import) != 1:
        raise ValueError('unexpected probe imports')
    return ((PACKAGE / 'common.py').read_text() + '\n' + source.replace(shared_import, '')).encode()


def dashboard(mac, linux):
    """Deploy the watchdog's sanitized file handoff and retire the bridge service."""
    stage = linux.run('mktemp -d /var/tmp/zakura-dashboard-ci.XXXXXX').strip()
    if not re.fullmatch(r'/var/tmp/zakura-dashboard-ci\.[A-Za-z0-9]+', stage):
        raise ValueError('unexpected dashboard staging path')
    files = {
        '/opt/zakura-fleet-watchdog/zakura-cluster-watchdog.py': PACKAGE.parent / 'runner/zakura-cluster-watchdog.py',
        '/opt/zakura-fleet-watchdog/mac_cranelift_status.py': PACKAGE.parent / 'runner/mac_cranelift_status.py',
        '/opt/zakura-mainnet-dashboard/zakura-cluster-status.py': PACKAGE.parent / 'runner/zakura-cluster-status.py',
        '/opt/zakura-mac-verifier/comparison.py': PACKAGE / 'comparison.py',
        '/opt/zakura-mac-verifier/common.py': PACKAGE / 'common.py',
    }
    dropin = '/etc/systemd/system/zakura-mainnet-dashboard.service.d/70-private-verifier.conf'
    targets = [*files, dropin]
    for index, target in enumerate(targets):
        linux.run(f'''set -eu
if sudo -n test -f {target}; then
  sudo -n cp -p {target} {stage}/{index}.previous
else
  sudo -n touch {stage}/{index}.absent
fi
''')
    mac_stage = mac.run('mktemp -d /var/tmp/zakura-tools-ci.XXXXXX').strip()
    if not re.fullmatch(r'/var/tmp/zakura-tools-ci\.[A-Za-z0-9]+', mac_stage):
        raise ValueError('unexpected Mac tooling staging path')
    mac_files = {'ssh_probe.py': probe_program(), 'rotate_logs.py': (PACKAGE / 'rotate_logs.py').read_bytes()}
    for name, contents in mac_files.items():
        mac.run(f"if sudo -n test -f '{BASE}/{name}'; then sudo -n cp -p '{BASE}/{name}' {mac_stage}/{name}.previous; else touch {mac_stage}/{name}.absent; fi")
        mac.put(contents, mac_stage + '/' + name)
    bridge_active = linux.run('systemctl is-active zakura-mac-verifier-dashboard || true').strip() == 'active'
    started = time.time()
    try:
        linux.run('sudo -n systemctl stop zakura-fleet-watchdog')
        for target, source in files.items():
            linux.put(source.read_bytes(), target)
        for name in mac_files:
            mac.run(f"sudo -n mv {mac_stage}/{name} '{BASE}/{name}'")
        linux.run('''set -eu
sudo -n install -d -m 755 /var/lib/zakura-mac-cranelift-public
sudo -n install -d -m 755 /etc/systemd/system/zakura-mainnet-dashboard.service.d
printf '[Service]\\nEnvironment=ZAKURA_MAC_CRANELIFT_STATUS=1\\n' | sudo -n tee /etc/systemd/system/zakura-mainnet-dashboard.service.d/70-private-verifier.conf >/dev/null
sudo -n systemctl daemon-reload
sudo -n systemctl start zakura-fleet-watchdog
sudo -n systemctl restart zakura-mainnet-dashboard
''')
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            result = linux.run(f'''python3 - <<'REMOTE'
import json, pathlib, time, urllib.request
try:
    sample = json.loads(pathlib.Path('/var/lib/zakura-mac-cranelift-public/status.json').read_text())
    assert sample['sample_time'] >= {started!r} and time.time() - sample['sample_time'] <= 90
    assert sample['comparison_healthy'] and sample['condition'] == 'matching'
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({{}}))
    with opener.open('http://127.0.0.1:8090/data', timeout=10) as response:
        rows = json.load(response)['rows']
    print(json.dumps(any(row['name'] == 'zakura-mac-os' and row.get('height') and row.get('healthy') for row in rows)))
except Exception:
    print('false')
REMOTE
''')
            if json.loads(result):
                linux.run('''set -eu
sudo -n systemctl disable --now zakura-mac-verifier-dashboard
sudo -n python3 - <<'REMOTE'
import socket
try:
    connection = socket.create_connection(('127.0.0.1', 28236), timeout=2)
except OSError:
    pass
else:
    connection.close()
    raise RuntimeError('retired bridge is still listening')
REMOTE
''')
                print('Dashboard verified: fresh matching comparison via sanitized file; bridge disabled')
                return
            time.sleep(5)
        raise RuntimeError('dashboard did not report a fresh healthy comparison')
    except Exception:
        linux.run('sudo -n systemctl stop zakura-fleet-watchdog')
        for name in mac_files:
            mac.run(f"if test -f {mac_stage}/{name}.absent; then sudo -n rm -f '{BASE}/{name}'; else sudo -n cp -p {mac_stage}/{name}.previous '{BASE}/{name}'; fi")
        for index, target in enumerate(targets):
            linux.run(f'''set -eu
if sudo -n test -f {stage}/{index}.absent; then
  sudo -n rm -f {target}
else
  sudo -n cp -p {stage}/{index}.previous {target}
fi
''')
        if bridge_active:
            linux.run('sudo -n systemctl enable --now zakura-mac-verifier-dashboard')
        linux.run('sudo -n systemctl daemon-reload\nsudo -n systemctl start zakura-fleet-watchdog\nsudo -n systemctl restart zakura-mainnet-dashboard')
        raise


def public_report(value):
    payload = json.dumps(value, indent=2)
    private = ipaddress.ip_address(os.environ['ZAKURA_MAC_CRANELIFT_HOST'])
    if any(address in payload for address in (str(private), private.exploded)):
        raise ValueError('refusing a status report containing the private Mac address')
    print(payload)


def check_address_history():
    """Check the secret's exact value without ever echoing it or matching lines."""
    private = ipaddress.ip_address(os.environ['ZAKURA_MAC_CRANELIFT_HOST'])
    for address in {str(private), private.exploded}:
        for selector in [['-S', address], ['--fixed-strings', '--grep', address]]:
            result = subprocess.run(['git', 'log', '--all', '--format=%H', *selector, '--'],
                                    capture_output=True, timeout=120)
            if result.returncode or result.stdout.strip():
                raise ValueError('private Mac address history audit failed; no content published')
    print('Private Mac address absent from tracked content and commit messages', flush=True)


def status(mac, linux, identifier):
    mac_info = json.loads(mac.run(f'''sudo -n {MAC_PYTHON} - <<'REMOTE'
import hashlib, json, pathlib, platform, socket, subprocess, tomllib
base = pathlib.Path({BASE!r})
result = {{'architecture': platform.machine(), 'receipt_present': (base / 'receipt.json').is_file()}}
receipt = json.loads((base / 'receipt.json').read_text())
with (base / 'bin/zakurad').open('rb') as stream:
    actual = hashlib.file_digest(stream, 'sha256').hexdigest()
result['binary_matches_receipt'] = actual == receipt.get('binary_sha256')
result['binary_sha256'] = actual
result['receipt_fields'] = sorted(receipt)
compiler = receipt.get('compiler')
result['compiler_metadata'] = compiler
config = tomllib.loads((base / 'zakurad.toml').read_text())
result['full_verification_enabled'] = (config.get('consensus', {{}}).get('checkpoint_sync') is False
    and config.get('consensus', {{}}).get('vct_fast_sync') is False)
result['compiler_evidence_files'] = sorted(str(p.relative_to(base / 'evidence')) for p in (base / 'evidence').rglob('*.json'))
result['compiler_acceptance'] = []
for path in (base / 'evidence').rglob('*.json'):
    evidence = json.loads(path.read_text())
    if evidence.get('binary_sha256') == actual:
        result['compiler_acceptance'].append({{key: evidence.get(key) for key in
            ['passed', 'source_sha', 'patch_sha256', 'binary_architecture', 'configuration', 'checks']}})
for name in ['node', 'adapter', 'tunnel']:
    check = subprocess.run(['launchctl', 'print', 'system/dev.valargroup.zakura-verifier-' + name], capture_output=True, text=True, timeout=10)
    result[name + '_running'] = check.returncode == 0 and 'state = running' in check.stdout
try:
    with socket.create_connection(('127.0.0.1', 28233), timeout=2):
        result['adapter_listener_closed'] = False
except OSError:
    result['adapter_listener_closed'] = True
print(json.dumps(result))
REMOTE
'''))
    info = json.loads(linux.run('''sudo -n python3 - <<'REMOTE'
import json, pathlib, pwd, socket, subprocess, time, urllib.request
result = {}
for name in ['zakura-mac-verifier', 'zakura-fleet-watchdog', 'zakura-mac-verifier-dashboard', 'zakura-mainnet-dashboard']:
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
try:
    with socket.create_connection(('127.0.0.1', 28233), timeout=2):
        result['reverse_listener_closed'] = False
except OSError:
    result['reverse_listener_closed'] = True
try:
    with socket.create_connection(('127.0.0.1', 28236), timeout=2):
        result['dashboard_bridge_closed'] = False
except OSError:
    result['dashboard_bridge_closed'] = True
public = pathlib.Path('/var/lib/zakura-mac-cranelift-public/status.json')
result['dashboard_file_present'] = public.is_file()
try:
    sample = json.loads(public.read_text())
    stamp = sample.get('sample_time')
    result['dashboard_file_fresh'] = type(stamp) in (int, float) and 0 <= time.time() - stamp <= 90
    result['dashboard_file_healthy'] = sample.get('comparison_healthy') is True
    identity = json.loads(pathlib.Path('/etc/zakura-mac-verifier/dashboard.json').read_text())
    result['dashboard_identity_matches'] = sample.get('verifier_id') == identity['verifier_id']
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open('http://127.0.0.1:8090/data', timeout=10) as response:
        rows = json.load(response)['rows']
    result['dashboard_row_healthy'] = any(row.get('name') == 'zakura-mac-os'
        and row.get('healthy') is True and type(row.get('height')) is int and row['height'] > 0 for row in rows)
except (OSError, ValueError, KeyError, TypeError):
    result['dashboard_row_healthy'] = False
path = pathlib.Path('/var/lib/zakura-mac-verifier/status.json')
result['status'] = json.loads(path.read_text()) if path.exists() else None
script = pathlib.Path('/opt/zakura-mainnet-dashboard/zakura-cluster-status.py')
result['dashboard_supports_mac'] = any(name in script.read_text() for name in
    ['ZAKURA_MAC_CRANELIFT_STATUS', 'ZAKURA_PRIVATE_VERIFIER_STATUS'])
check = subprocess.run(['systemctl', 'show', 'zakura-mainnet-dashboard', '-p', 'Environment', '--value'], capture_output=True, text=True, timeout=10)
result['dashboard_mac_enabled'] = any(name in check.stdout for name in
    ['ZAKURA_MAC_CRANELIFT_STATUS=1', 'ZAKURA_PRIVATE_VERIFIER_STATUS=1'])
print(json.dumps(result))
REMOTE
'''))
    raw = info.pop('status')
    # Compiler metadata is private input. Publish only recognized profile fields.
    compiler = mac_info.pop('compiler_metadata')
    if isinstance(compiler, dict):
        mac_info['compiler_metadata_fields'] = sorted(compiler)
        mac_info['compiler_profile'] = {key: compiler.get(key) for key in
            ['backend', 'upstream_sha', 'patch_sha256', 'backend_sha256', 'panic',
             'standard_library', 'linker', 'acceptance_passed', 'acceptance_receipt_sha256']}
    else:
        mac_info['compiler_metadata_type'] = type(compiler).__name__
    if raw:
        info['status'] = public_status(raw, identifier)
    public_report({'mac': mac_info, 'reference': info})
    require_healthy(mac_info, info)


def require_healthy(mac, reference):
    """A successful CI status check must establish current end-to-end health."""
    required_mac = ['receipt_present', 'binary_matches_receipt', 'full_verification_enabled',
                    'node_running', 'adapter_listener_closed']
    required_reference = ['monitoring_config_private', 'monitoring_key_restricted',
        'reverse_listener_closed', 'dashboard_bridge_closed', 'dashboard_file_present',
        'dashboard_file_fresh', 'dashboard_file_healthy', 'dashboard_identity_matches',
        'dashboard_row_healthy', 'dashboard_mac_enabled', 'dashboard_supports_mac']
    sample = reference.get('status') or {}
    stamp = sample.get('sample_time')
    checks = [*(mac.get(key) is True for key in required_mac),
              *(reference.get(key) is True for key in required_reference),
              mac.get('architecture') in ('arm64', 'aarch64'),
              mac.get('adapter_running') is False, mac.get('tunnel_running') is False,
              reference.get('zakura-fleet-watchdog') == 'active',
              reference.get('zakura-mainnet-dashboard') == 'active',
              reference.get('zakura-mac-verifier') == 'inactive',
              reference.get('zakura-mac-verifier-dashboard') == 'inactive',
              any(item.get('passed') is True for item in mac.get('compiler_acceptance', [])),
              sample.get('comparison_healthy') is True, sample.get('condition') == 'matching',
              type(stamp) in (int, float) and 0 <= time.time() - stamp <= 90]
    if not all(checks):
        raise RuntimeError('end-to-end health checks failed; inspect the sanitized status report')


def validate_candidate(directory, source_sha, lock_sha256):
    """Require the complete compiler acceptance receipt for these exact bytes."""
    from common import digest
    receipt = json.loads((directory / 'receipt.json').read_text())
    manifest = json.loads((PACKAGE / 'corpus.json').read_text())
    expected = {'unwind-probe-build', 'unwind-probe', 'double-panic', 'native-node-build'}
    expected.update('zakura-consensus-' + str(i) for i in range(len(manifest['tests'])))
    expected.update('zakura-network-' + str(i) for i in range(2))
    checks = receipt.get('checks', [])
    configuration = dict(panic='unwind', lto=False, build_jobs=1,
                         linker='apple-classic', standard_library='cranelift-static')
    if (not re.fullmatch(r'[a-f0-9]{40}', source_sha)
            or not re.fullmatch(r'[a-f0-9]{64}', lock_sha256)
            or receipt.get('source_sha') != source_sha
            or receipt.get('cargo_lock_sha256') != lock_sha256
            or receipt.get('passed') is not True
            or receipt.get('configuration') != configuration
            or receipt.get('patch_sha256') != digest(PACKAGE / 'cranelift/macos-unwind.patch')
            or receipt.get('binary_sha256') != digest(directory / 'zakurad')
            or 'arm64' not in receipt.get('binary_architecture', '')
            or len(checks) != len(expected)
            or {c.get('name') for c in checks} != expected
            or any(c.get('passed') is not True for c in checks)):
        raise ValueError('candidate does not satisfy the pinned Cranelift acceptance profile')
    return receipt


def download_candidate(run_id, directory):
    if not re.fullmatch(r'[0-9]+', run_id):
        raise ValueError('a successful candidate run ID is required')
    repository = 'zakura-core/zakura'
    def api(suffix):
        return json.loads(subprocess.check_output(
            ['gh', 'api', f'repos/{repository}/actions/runs/{run_id}' + suffix], timeout=30))
    run = api('')
    # Keep the former builder path valid for artifacts from historical runs.
    if (run.get('head_repository', {}).get('full_name') != repository
            or run.get('head_branch') not in ['main', os.environ.get('ZAKURA_MAC_CRANELIFT_DEPLOY_BRANCH')]
            or run.get('path') not in ['.github/workflows/zakura-mac-cranelift.yml', '.github/workflows/build-zakura-mac-cranelift.yml',
                                       '.github/workflows/deploy-mac-verifier.yml',
                                       '.github/workflows/mac-verifier.yml', '.github/workflows/build-mac-verifier.yml',
                                       '.github/workflows/zakura-mainnet-deploy.yml']):
        raise ValueError('candidate must come from successful trusted zakura-mac-cranelift CI')
    if run.get('status') != 'completed':
        print('Waiting for the selected Cranelift candidate to finish acceptance', flush=True)
        subprocess.run(['gh', 'run', 'watch', run_id, '--repo', repository,
                        '--exit-status', '--interval', '30'],
                       check=True, capture_output=True, timeout=2 * 3600)
        run = api('')
    if run.get('conclusion') != 'success':
        raise ValueError('candidate CI did not pass; live binary unchanged')
    artifacts = [a for a in api('/artifacts')['artifacts']
                 if a['name'].startswith(('zakura-mac-cranelift-candidate-', 'mac-verifier-cranelift-')) and not a['expired']]
    if len(artifacts) != 1:
        raise ValueError('exactly one accepted candidate artifact required')
    subprocess.run(['gh', 'run', 'download', run_id, '--repo', repository,
                    '--name', artifacts[0]['name'], '--dir', str(directory)],
                   check=True, capture_output=True, timeout=180)
    # upload-artifact preserves the common ancestor of the binary and receipt.
    binary = directory / 'target/release/zakurad'
    if binary.is_file():
        binary.rename(directory / 'zakurad')


def transitioned_receipt(old, candidate, now):
    new = dict(old, source_sha=candidate['source_sha'],
               cargo_lock_sha256=candidate['cargo_lock_sha256'],
               binary_sha256=candidate['binary_sha256'], deployed_at=now,
               compiler=candidate, toolchain=candidate['toolchain'])
    return new


def deploy_candidate(mac, linux, candidate_dir, candidate):
    """Coordinate binary/receipt replacement while preserving comparison history."""
    from common import digest
    linux.run('sudo -n test -f /etc/systemd/system/zakura-fleet-watchdog.service.d/70-mac-comparison.conf')
    print('Checking live receipts and staging the accepted binary', flush=True)
    old_mac = json.loads(mac.run(f'sudo -n cat {shlex.quote(BASE + "/receipt.json")}'))
    old_linux = json.loads(linux.run('sudo -n cat /etc/zakura-mac-verifier/receipt.json'))
    if old_mac != old_linux:
        raise ValueError('live receipts disagree; deployment refused')
    new = transitioned_receipt(old_mac, candidate, time.time())
    stage = mac.run('mktemp -d /var/tmp/zakura-verifier-ci.XXXXXX').strip()
    if not re.fullmatch(r'/var/tmp/zakura-verifier-ci\.[A-Za-z0-9]+', stage):
        raise ValueError('unexpected staging path')
    binary = candidate_dir / 'zakurad'
    mac.put(binary.read_bytes(), stage + '/zakurad')
    mac.put((json.dumps(new) + '\n').encode(), stage + '/receipt.json')
    mac.put((json.dumps(candidate) + '\n').encode(), stage + '/acceptance.json')
    # Execute on the destination before touching its running node. This also
    # catches unavailable dylibs / an incompatible deployment target.
    mac.run(f'''set -eu
sudo -n chmod 755 {stage}/zakurad
sudo -n {MAC_PYTHON} - <<'REMOTE'
import hashlib, pathlib
with pathlib.Path('{stage}/zakurad').open('rb') as stream:
    assert hashlib.file_digest(stream, 'sha256').hexdigest() == '{digest(binary)}'
REMOTE
sudo -n {stage}/zakurad --version >/dev/null
''')
    linux.run('set -eu\nsudo -n cp -p /etc/zakura-mac-verifier/receipt.json /etc/zakura-mac-verifier/receipt.json.previous\n'
              'sudo -n systemctl stop zakura-fleet-watchdog')
    try:
        print('Replacing the Mac binary and restarting its node', flush=True)
        mac.run(f'''set -eu
sudo -n cp -p '{BASE}/bin/zakurad' '{BASE}/bin/zakurad.previous'
sudo -n cp -p '{BASE}/receipt.json' '{BASE}/receipt.json.previous'
sudo -n install -m 755 {stage}/zakurad '{BASE}/bin/zakurad.next'
sudo -n mv -f '{BASE}/bin/zakurad.next' '{BASE}/bin/zakurad'
sudo -n cp {stage}/receipt.json '{BASE}/receipt.json'
sudo -n install -m 644 {stage}/acceptance.json '{BASE}/evidence/ci-acceptance.json'
sudo -n launchctl kickstart -k system/dev.valargroup.zakura-verifier-node
''', timeout=300)
        print('Updating the Linux receipt while retaining comparison state', flush=True)
        linux.put((json.dumps(new) + '\n').encode(), '/etc/zakura-mac-verifier/receipt.json')
        linux.run('''set -eu
sudo -n python3 - <<'REMOTE'
import hashlib, json, os, pathlib, sys
sys.path.insert(0, '/opt/zakura-mac-verifier')
from common import atomic_json
path = pathlib.Path('/var/lib/zakura-mac-verifier/cursor.json')
stat = path.stat()
state = json.loads(path.read_text())
receipt = json.loads(pathlib.Path('/etc/zakura-mac-verifier/receipt.json').read_text())
state.update(receipt_digest=hashlib.sha256(json.dumps(receipt, sort_keys=True).encode()).hexdigest())
atomic_json(path, state)
os.chown(path, stat.st_uid, stat.st_gid)
REMOTE
sudo -n systemctl start zakura-fleet-watchdog
''')
        deadline = time.monotonic() + 600
        print('Waiting for new mainnet progress and matching state comparison', flush=True)
        previous_height = None
        while time.monotonic() < deadline:
            raw = json.loads(linux.run('sudo -n cat /var/lib/zakura-mac-verifier/status.json'))
            verifier = raw.get('verifier') or {}
            tip = (verifier.get('tip') or {}).get('height')
            if (raw.get('sample_time', 0) >= new['deployed_at']
                    and verifier.get('binary_sha256') == new['binary_sha256']
                    and verifier.get('receipt') == new and raw.get('caught_up')
                    and raw.get('error') is None
                    and not raw.get('incidents', {})):
                if previous_height is not None and tip > previous_height:
                    print('Accepted Cranelift binary installed; mainnet height advanced and comparison caught up')
                    return
                if previous_height is None:
                    previous_height = tip
            time.sleep(15)
        raise RuntimeError('deployed node did not advance with healthy comparison')
    except Exception:
        print('Restoring the previous binary and receipt identity', flush=True)
        linux.run('sudo -n systemctl stop zakura-fleet-watchdog')
        mac.run(f'''set -eu
sudo -n cp -p '{BASE}/bin/zakurad.previous' '{BASE}/bin/zakurad.next'
sudo -n mv -f '{BASE}/bin/zakurad.next' '{BASE}/bin/zakurad'
sudo -n cp -p '{BASE}/receipt.json.previous' '{BASE}/receipt.json'
sudo -n launchctl kickstart -k system/dev.valargroup.zakura-verifier-node
''', timeout=300)
        linux.run('''set -eu
sudo -n cp -p /etc/zakura-mac-verifier/receipt.json.previous /etc/zakura-mac-verifier/receipt.json
sudo -n python3 - <<'REMOTE'
import hashlib, json, os, pathlib, sys
sys.path.insert(0, '/opt/zakura-mac-verifier')
from common import atomic_json
path = pathlib.Path('/var/lib/zakura-mac-verifier/cursor.json')
stat = path.stat()
state = json.loads(path.read_text())
receipt = json.loads(pathlib.Path('/etc/zakura-mac-verifier/receipt.json').read_text())
state.update(receipt_digest=hashlib.sha256(json.dumps(receipt, sort_keys=True).encode()).hexdigest())
atomic_json(path, state)
os.chown(path, stat.st_uid, stat.st_gid)
REMOTE
sudo -n systemctl start zakura-fleet-watchdog
''')
        raise
    finally:
        mac.run('rm -rf -- ' + shlex.quote(stage))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('operation', choices=['status', 'dashboard', 'deploy'])
    args = parser.parse_args()
    check_address_history()
    if os.environ.get('NO_RESTART') == 'true' or (
            os.environ.get('FORCE_REBUILD') == 'true' and os.environ.get('MAC_CANDIDATE_RUN_ID')):
        raise ValueError('Mac deployment requires a restart; force_rebuild cannot reuse a candidate')
    identifier = os.environ['ZAKURA_MAC_CRANELIFT_ID']
    if not re.fullmatch(r'verifier-[a-f0-9]{32}', identifier):
        raise ValueError('invalid verifier identity')
    with tempfile.TemporaryDirectory(prefix='zakura-mac-cranelift-') as directory:
        mac = SSH('ZAKURA_MAC_CRANELIFT_', directory)
        linux = SSH('ZAKURA_MAC_CRANELIFT_REFERENCE_', directory)
        if args.operation == 'deploy':
            if os.environ.get('MAC_CANDIDATE_RUN_ID'):
                candidate_dir = Path(directory) / 'candidate'
                download_candidate(os.environ['MAC_CANDIDATE_RUN_ID'], candidate_dir)
            else:
                candidate_dir = Path(os.environ['MAC_CANDIDATE_DIR'])
                (candidate_dir / 'target/release/zakurad').rename(candidate_dir / 'zakurad')
            candidate = validate_candidate(candidate_dir, os.environ['MAC_SOURCE_SHA'],
                                           os.environ['MAC_SOURCE_LOCK_SHA256'])
            deploy_candidate(mac, linux, candidate_dir, candidate)
        if args.operation in ('dashboard', 'deploy'):
            dashboard(mac, linux)
        status(mac, linux, identifier)


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        frames = ' -> '.join(f'{Path(frame.filename).name}:{frame.lineno}'
                             for frame in traceback.extract_tb(error.__traceback__))
        raise SystemExit(f'zakura-mac-cranelift operation failed ({type(error).__name__}, {frames}); '
                         'private remote output withheld') from None
