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
import time
import traceback

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


def dashboard(linux):
    """Restore the integration that a regular fleet deployment can overwrite."""
    target = '/opt/zakura-mainnet-dashboard/zakura-cluster-status.py'
    bridge = '/opt/zakura-mac-verifier/status_bridge.py'
    linux.run('set -eu\nsudo -n test -f ' + target + '\nsudo -n cp -p ' + target + ' ' + target + '.previous')
    linux.run('set -eu\nsudo -n cp -p ' + bridge + ' ' + bridge + '.previous')
    try:
        linux.put((PACKAGE / 'status_bridge.py').read_bytes(), bridge)
        linux.put((PACKAGE.parent / 'runner/zakura-cluster-status.py').read_bytes(), target)
        linux.run('''set -eu
sudo -n install -d -m 755 /etc/systemd/system/zakura-mainnet-dashboard.service.d
printf '[Service]\\nEnvironment=ZAKURA_PRIVATE_VERIFIER_STATUS=1\\n' | sudo -n tee /etc/systemd/system/zakura-mainnet-dashboard.service.d/70-private-verifier.conf >/dev/null
sudo -n systemctl daemon-reload
sudo -n systemctl restart zakura-mac-verifier-dashboard
sudo -n systemctl restart zakura-mainnet-dashboard
''')
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            result = linux.run('''python3 - <<'REMOTE'
import json, urllib.request
try:
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open('http://127.0.0.1:8090/data', timeout=10) as response:
        rows = json.load(response)['rows']
    print(json.dumps(any(row['name'] == 'zakura-mac-os' and row.get('height') for row in rows)))
except Exception:
    print('false')
REMOTE
''')
            if json.loads(result):
                print('Dashboard deployment verified: Mac row has a live height')
                return
            time.sleep(5)
        raise RuntimeError('Mac row did not become available')
    except Exception:
        linux.run('set -eu\nsudo -n cp -p ' + bridge + '.previous ' + bridge + '\nsudo -n systemctl restart zakura-mac-verifier-dashboard')
        linux.run('set -eu\nsudo -n cp -p ' + target + '.previous ' + target + '\nsudo -n systemctl restart zakura-mainnet-dashboard')
        raise



def migrate_watchdog(linux):
    """Shadow-check the new comparator, then transfer ownership with rollback."""
    linux.run('sudo -n test ! -f /etc/systemd/system/zakura-fleet-watchdog.service.d/70-mac-comparison.conf')
    runner = PACKAGE.parent / 'runner'
    stage = linux.run('mktemp -d /var/tmp/zakura-comparison-ci.XXXXXX').strip()
    if not re.fullmatch(r'/var/tmp/zakura-comparison-ci\.[A-Za-z0-9]+', stage):
        raise ValueError('unexpected migration staging path')
    for name, source in [('comparison.py', PACKAGE / 'comparison.py'),
                         ('common.py', PACKAGE / 'common.py'),
                         ('status_bridge.py', PACKAGE / 'status_bridge.py'),
                         ('watchdog.py', runner / 'zakura-cluster-watchdog.py')]:
        linux.put(source.read_bytes(), stage + '/' + name)
    # No host output or private evidence is returned. Shadow work uses a copy.
    print('Checking comparison against a copy of the current cursor', flush=True)
    linux.run(f"""set -eu
sudo -n chmod 755 {stage}
sudo -n mkdir {stage}/shadow
sudo -n cp /var/lib/zakura-mac-verifier/cursor.json {stage}/shadow/cursor.json
sudo -n python3 {stage}/comparison.py migrate --directory {stage}/shadow
sudo -n timeout 20 python3 {stage}/comparison.py once --directory {stage}/shadow
sudo -n python3 - <<'REMOTE'
import json, pathlib
sample = json.loads(pathlib.Path('{stage}/shadow/status.json').read_text())
assert sample['condition'] in ('matching', 'catching_up')
REMOTE
""", timeout=60)
    print('Transferring comparison ownership to the fleet watchdog', flush=True)
    # Back up at rest after stopping both possible writers. Keep these files for
    # operator recovery; the old notification queue is never delivered.
    linux.run(f"""set -eu
trap 'sudo -n systemctl start zakura-mac-verifier zakura-fleet-watchdog' ERR
sudo -n systemctl stop zakura-mac-verifier zakura-fleet-watchdog
sudo -n mkdir {stage}/backup
sudo -n cp -a /var/lib/zakura-mac-verifier/cursor.json {stage}/backup/cursor.json
sudo -n cp -a /var/lib/zakura-mac-verifier/status.json {stage}/backup/status.json
sudo -n cp -a /opt/zakura-fleet-watchdog/zakura-cluster-watchdog.py {stage}/backup/watchdog.py
sudo -n cp -a /opt/zakura-mac-verifier/common.py {stage}/backup/common.py
sudo -n cp -a /opt/zakura-mac-verifier/status_bridge.py {stage}/backup/status_bridge.py
sudo -n systemctl is-enabled zakura-mac-verifier > {stage}/backup/old-enabled || true
""")
    try:
        linux.run(f"""set -eu
sudo -n install -m 644 {stage}/comparison.py /opt/zakura-mac-verifier/comparison.py
sudo -n install -m 644 {stage}/common.py /opt/zakura-mac-verifier/common.py
sudo -n install -m 644 {stage}/status_bridge.py /opt/zakura-mac-verifier/status_bridge.py
sudo -n install -m 755 {stage}/watchdog.py /opt/zakura-fleet-watchdog/zakura-cluster-watchdog.py
sudo -n -u zakura-mac-verifier python3 /opt/zakura-mac-verifier/comparison.py migrate
sudo -n install -d -m 755 /etc/systemd/system/zakura-fleet-watchdog.service.d
printf '[Service]\\nEnvironment=ZAKURA_MAC_COMPARISON=1\\nEnvironment=ZAKURA_MAC_COMPARISON_ALERTS=0\\n' | sudo -n tee /etc/systemd/system/zakura-fleet-watchdog.service.d/70-mac-comparison.conf >/dev/null
sudo -n systemctl daemon-reload
sudo -n systemctl disable zakura-mac-verifier
sudo -n systemctl restart zakura-mac-verifier-dashboard
sudo -n systemctl start zakura-fleet-watchdog
""")
        deadline = time.monotonic() + 600
        baseline = None
        while time.monotonic() < deadline:
            sample = json.loads(linux.run("sudo -n cat /var/lib/zakura-mac-verifier/status.json"))
            if sample.get('condition') == 'matching' and sample.get('alerts_muted') is True:
                height = sample['compared_through']
                if baseline is not None and height > baseline:
                    linux.run('set -eu\nsudo -n systemctl is-active --quiet zakura-fleet-watchdog\n'
                              '! sudo -n systemctl is-active --quiet zakura-mac-verifier')
                    print('Fleet watchdog owns comparison; new blocks match; alerts remain muted')
                    return
                baseline = height if baseline is None else baseline
            time.sleep(10)
        raise RuntimeError('watchdog comparison did not advance')
    except Exception:
        print('Restoring previous comparison owner; retained migration evidence privately', flush=True)
        linux.run(f"""set -eu
sudo -n systemctl stop zakura-fleet-watchdog
sudo -n cp -a {stage}/backup/watchdog.py /opt/zakura-fleet-watchdog/zakura-cluster-watchdog.py
sudo -n cp -a {stage}/backup/common.py /opt/zakura-mac-verifier/common.py
sudo -n cp -a {stage}/backup/status_bridge.py /opt/zakura-mac-verifier/status_bridge.py
sudo -n cp -a /var/lib/zakura-mac-verifier/cursor.json {stage}/backup/failed-cursor.json
sudo -n cp -a {stage}/backup/cursor.json /var/lib/zakura-mac-verifier/cursor.json
sudo -n cp -a {stage}/backup/status.json /var/lib/zakura-mac-verifier/status.json
if sudo -n test -f /var/lib/zakura-mac-verifier/legacy-cursor.json; then
  sudo -n mv /var/lib/zakura-mac-verifier/legacy-cursor.json {stage}/backup/legacy-cursor.json
fi
sudo -n rm -f /etc/systemd/system/zakura-fleet-watchdog.service.d/70-mac-comparison.conf
sudo -n systemctl daemon-reload
if grep -qx enabled {stage}/backup/old-enabled; then sudo -n systemctl enable zakura-mac-verifier; fi
sudo -n systemctl start zakura-mac-verifier zakura-fleet-watchdog
sudo -n systemctl restart zakura-mac-verifier-dashboard
""")
        raise


def public_report(value):
    payload = json.dumps(value, indent=2)
    private = ipaddress.ip_address(os.environ['MAC_VERIFIER_HOST'])
    if any(address in payload for address in (str(private), private.exploded)):
        raise ValueError('refusing a status report containing the private Mac address')
    print(payload)


def check_address_history():
    """Check the secret's exact value without ever echoing it or matching lines."""
    private = ipaddress.ip_address(os.environ['MAC_VERIFIER_HOST'])
    for address in {str(private), private.exploded}:
        for selector in [['-S', address], ['--fixed-strings', '--grep', address]]:
            result = subprocess.run(['git', 'log', '--all', '--format=%H', *selector, '--'],
                                    capture_output=True, timeout=120)
            if result.returncode or result.stdout.strip():
                raise ValueError('private Mac address history audit failed; no content published')
    print('Private Mac address absent from tracked content and commit messages', flush=True)


def status(mac, linux, identifier):
    mac_info = json.loads(mac.run(f'''sudo -n {MAC_PYTHON} - <<'REMOTE'
import hashlib, json, pathlib, platform, subprocess, tomllib
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
    if (run.get('head_repository', {}).get('full_name') != repository
            or run.get('head_branch') not in ['main', os.environ.get('MAC_VERIFIER_DEPLOY_BRANCH')]
            or run.get('path') not in ['.github/workflows/mac-verifier.yml', '.github/workflows/deploy-mac-verifier.yml',
                                       '.github/workflows/zakura-mainnet-deploy.yml']):
        raise ValueError('candidate must come from successful trusted Mac verifier CI')
    if run.get('status') != 'completed':
        print('Waiting for the selected Cranelift candidate to finish acceptance', flush=True)
        subprocess.run(['gh', 'run', 'watch', run_id, '--repo', repository,
                        '--exit-status', '--interval', '30'],
                       check=True, capture_output=True, timeout=2 * 3600)
        run = api('')
    if run.get('conclusion') != 'success':
        raise ValueError('candidate CI did not pass; live binary unchanged')
    artifacts = [a for a in api('/artifacts')['artifacts']
                 if a['name'].startswith('mac-verifier-cranelift-') and not a['expired']]
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
        print('Replacing the Mac binary and restarting its node and adapter', flush=True)
        mac.run(f'''set -eu
sudo -n cp -p '{BASE}/bin/zakurad' '{BASE}/bin/zakurad.previous'
sudo -n cp -p '{BASE}/receipt.json' '{BASE}/receipt.json.previous'
sudo -n install -m 755 {stage}/zakurad '{BASE}/bin/zakurad.next'
sudo -n mv -f '{BASE}/bin/zakurad.next' '{BASE}/bin/zakurad'
sudo -n cp {stage}/receipt.json '{BASE}/receipt.json'
sudo -n install -m 644 {stage}/acceptance.json '{BASE}/evidence/ci-acceptance.json'
sudo -n launchctl kickstart -k system/dev.valargroup.zakura-verifier-node
sudo -n launchctl kickstart -k system/dev.valargroup.zakura-verifier-adapter
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
                    and not (set(raw.get('incidents', {})) - {'alert delivery unavailable'})):
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
sudo -n launchctl kickstart -k system/dev.valargroup.zakura-verifier-adapter
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
    parser.add_argument('operation', choices=['status', 'dashboard', 'deploy', 'migrate'])
    args = parser.parse_args()
    check_address_history()
    if os.environ.get('NO_RESTART') == 'true' or (
            os.environ.get('FORCE_REBUILD') == 'true' and os.environ.get('MAC_CANDIDATE_RUN_ID')):
        raise ValueError('Mac deployment requires a restart; force_rebuild cannot reuse a candidate')
    identifier = os.environ['MAC_VERIFIER_ID']
    if not re.fullmatch(r'verifier-[a-f0-9]{32}', identifier):
        raise ValueError('invalid verifier identity')
    with tempfile.TemporaryDirectory(prefix='mac-verifier-') as directory:
        mac = SSH('MAC_VERIFIER_', directory)
        linux = SSH('MAC_VERIFIER_REFERENCE_', directory)
        if args.operation == 'migrate':
            migrate_watchdog(linux)
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
        if args.operation in ('dashboard', 'deploy', 'migrate'):
            dashboard(linux)
        status(mac, linux, identifier)


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        frames = ' -> '.join(f'{Path(frame.filename).name}:{frame.lineno}'
                             for frame in traceback.extract_tb(error.__traceback__))
        raise SystemExit(f'Mac verifier operation failed ({type(error).__name__}, {frames}); '
                         'private remote output withheld') from None
