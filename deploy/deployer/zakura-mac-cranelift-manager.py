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


def rotate_program():
    source = (PACKAGE / 'rotate_logs.py').read_text()
    shared_import = 'from common import rotate as rotate_file\n'
    if source.count(shared_import) != 1:
        raise ValueError('unexpected rotator imports')
    return ((PACKAGE / 'common.py').read_text() + '\n'
            + source.replace(shared_import, 'rotate_file = rotate\n')).encode()


def remote_program(name, *arguments, interpreter='python3'):
    """Run a checked-in remote program with shell-quoted positional arguments."""
    command = ['sudo', '-n', interpreter, '-', *map(str, arguments)]
    return shlex.join(command) + " <<'REMOTE'\n" + (PACKAGE / 'remote' / name).read_text() + "\nREMOTE\n"


def rebind_cursor(linux):
    linux.run('set -eu\n' + remote_program('rebind_cursor.py')
              + 'sudo -n systemctl start zakura-fleet-watchdog\n')


def dashboard(mac, linux):
    """Update Mac helpers; canonical mainnet deployment owns all Linux scripts."""
    stage = mac.run('mktemp -d /var/tmp/zakura-tools-ci.XXXXXX').strip()
    if not re.fullmatch(r'/var/tmp/zakura-tools-ci\.[A-Za-z0-9]+', stage):
        raise ValueError('unexpected Mac tooling staging path')
    files = {'ssh_probe.py': probe_program(), 'rotate_logs.py': rotate_program()}
    try:
        for name, contents in files.items():
            mac.run(f"if sudo -n test -f '{BASE}/{name}'; then sudo -n cp -p '{BASE}/{name}' {stage}/{name}.previous; else touch {stage}/{name}.absent; fi")
            mac.put(contents, stage + '/' + name)
        started = time.time()
        for name in files:
            mac.run(f"sudo -n mv {stage}/{name} '{BASE}/{name}'")
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            if json.loads(linux.run(remote_program('check_dashboard.py', started))):
                print('Mac helpers verified: fresh matching comparison and healthy dashboard row')
                return
            time.sleep(5)
        raise RuntimeError('dashboard did not report a fresh healthy comparison')
    except Exception:
        for name in files:
            mac.run(f"if test -f {stage}/{name}.absent; then sudo -n rm -f '{BASE}/{name}'; elif sudo -n test -f {stage}/{name}.previous; then sudo -n cp -p {stage}/{name}.previous '{BASE}/{name}'; fi")
        raise
    finally:
        mac.run('rm -rf -- ' + shlex.quote(stage))


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
    mac_info = json.loads(mac.run(remote_program('mac_status.py', BASE, interpreter=MAC_PYTHON)))
    info = json.loads(linux.run(remote_program('reference_status.py', identifier)))
    # Compiler metadata is private input. Publish only recognized profile fields.
    compiler = mac_info.pop('compiler_metadata')
    if isinstance(compiler, dict):
        mac_info['compiler_metadata_fields'] = sorted(compiler)
        mac_info['compiler_profile'] = {key: compiler.get(key) for key in
            ['backend', 'upstream_sha', 'patch_sha256', 'backend_sha256', 'panic',
             'standard_library', 'linker', 'acceptance_passed', 'acceptance_receipt_sha256']}
    else:
        mac_info['compiler_metadata_type'] = type(compiler).__name__
    public_report({'mac': mac_info, 'reference': info})
    require_healthy(mac_info, info)
    configured = json.loads(linux.run('sudo -n cat /etc/zakura-mainnet-dashboard/private/addresses.json'))
    expected_address = ipaddress.ip_address(os.environ['ZAKURA_MAC_CRANELIFT_HOST'])
    if not isinstance(configured, list) or expected_address not in [ipaddress.ip_address(value) for value in configured]:
        raise ValueError('private address protection does not match deployment secret')
    audit_public_privacy()


def audit_public_privacy():
    import urllib.request
    import urllib.parse
    from html.parser import HTMLParser
    private = ipaddress.ip_address(os.environ['ZAKURA_MAC_CRANELIFT_HOST'])
    variants = {str(private), private.exploded}
    if isinstance(private, ipaddress.IPv4Address):
        mapped = ipaddress.ip_address('::ffff:' + str(private))
        variants.update([str(mapped), mapped.exploded, '::ffff:' + str(private)])
    class Scripts(HTMLParser):
        def __init__(self):
            super().__init__()
            self.sources = []
        def handle_starttag(self, tag, attrs):
            if tag == 'script' and dict(attrs).get('src'):
                self.sources.append(dict(attrs)['src'])
    base = 'https://status.mainnet.zakura.valargroup.dev'
    routes = ['/', '/node/mac-os-cranelift', '/data', '/data/node/mac-os-cranelift', '/ironwood-status.json']
    for route in routes:
        url = urllib.parse.urljoin(base, route)
        with urllib.request.urlopen(url, timeout=15) as response:
            payload = response.read(8 * 1024 * 1024 + 1)
        if len(payload) > 8 * 1024 * 1024:
            raise ValueError('public privacy audit response too large')
        text = payload.decode()
        try:
            text = json.dumps(json.loads(text), ensure_ascii=False)
        except ValueError:
            pass
        text = urllib.parse.unquote(text)
        if any(value.lower() in text.lower() for value in variants):
            raise ValueError('private Mac address found in public response; content withheld')
        if route == '/data':
            snapshot = json.loads(text)
            for row in snapshot.get('rows', []):
                name = row.get('name') if isinstance(row, dict) else None
                if isinstance(name, str) and re.fullmatch(r'[a-zA-Z0-9_-]{1,64}', name):
                    routes.extend(['/node/' + name, '/data/node/' + name])
        if route == '/':
            parser = Scripts()
            parser.feed(text)
            if len(parser.sources) > 16:
                raise ValueError('unexpected number of dashboard scripts')
            routes.extend(parser.sources)
    print('Exact private Mac address absent from public HTML, JavaScript and JSON', flush=True)


def require_healthy(mac, reference):
    """A successful CI status check must establish current end-to-end health."""
    required_mac = ['receipt_present', 'binary_matches_receipt', 'full_verification_enabled',
                    'node_running']
    required_reference = ['private_address_config_private', 'monitoring_config_private', 'monitoring_key_restricted',
        'dashboard_file_present',
        'dashboard_file_fresh', 'dashboard_file_healthy', 'dashboard_identity_matches',
        'dashboard_row_healthy', 'dashboard_mac_enabled', 'dashboard_supports_mac']
    sample = reference.get('status') or {}
    stamp = sample.get('sample_time')
    checks = [*(mac.get(key) is True for key in required_mac),
              *(reference.get(key) is True for key in required_reference),
              mac.get('architecture') in ('arm64', 'aarch64'),
              reference.get('zakura-fleet-watchdog') == 'active',
              reference.get('zakura-mainnet-dashboard') == 'active',
              any(item.get('passed') is True for item in mac.get('compiler_acceptance', [])),
              sample.get('condition') == 'matching',
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
    if (run.get('head_repository', {}).get('full_name') != repository
            or run.get('head_branch') not in ['main', os.environ.get('ZAKURA_MAC_CRANELIFT_DEPLOY_BRANCH')]
            or run.get('path') not in ['.github/workflows/zakura-mac-cranelift.yml', '.github/workflows/build-zakura-mac-cranelift.yml',
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
                 if a['name'].startswith('zakura-mac-cranelift-candidate-') and not a['expired']]
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
    mac.run(f'set -eu\nsudo -n chmod 755 {stage}/zakurad\n'
            + remote_program('binary_digest.py', stage + '/zakurad', digest(binary), interpreter=MAC_PYTHON)
            + f'sudo -n {stage}/zakurad --version >/dev/null\n')
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
        rebind_cursor(linux)
        deadline = time.monotonic() + 600
        print('Waiting for new mainnet progress and matching state comparison', flush=True)
        previous_height = None
        while time.monotonic() < deadline:
            raw = json.loads(linux.run('sudo -n cat /var/lib/zakura-mac-verifier/status.json'))
            verifier = raw.get('verifier') or {}
            tip = (verifier.get('tip') or {}).get('height')
            if (raw.get('sample_time', 0) >= new['deployed_at']
                    and verifier.get('binary_sha256') == new['binary_sha256']
                    and verifier.get('receipt') == new and raw.get('condition') == 'matching'):
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
        linux.run('sudo -n cp -p /etc/zakura-mac-verifier/receipt.json.previous /etc/zakura-mac-verifier/receipt.json')
        rebind_cursor(linux)
        raise
    finally:
        mac.run('rm -rf -- ' + shlex.quote(stage))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('operation', choices=['status', 'dashboard', 'deploy'])
    args = parser.parse_args()
    if args.operation == 'deploy':
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
