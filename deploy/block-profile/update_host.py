#!/usr/bin/env python3
"""Build a pinned profiling revision and update the dedicated systemd host."""
import argparse
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import socket
import subprocess
import time
import urllib.request

SERVICES = ['zakurad', 'zakura-profile-collector', 'zakura-profile-web']
BINARIES = ['zakurad', 'zakura-profile-explorer']


def command(args, **kwargs):
    return subprocess.run(args, check=True, text=True, timeout=5400, **kwargs)


def git(*args):
    return command(['git', *args], capture_output=True).stdout.strip()


def home():
    with urllib.request.urlopen('http://127.0.0.1:8787/api/home', timeout=5) as response:
        return json.load(response)


def old_sources(runs, revision):
    """Only backfill clean Git-described builds that belong to this profiling history."""
    result = []
    for row in runs:
        run = row['metadata']
        match = re.search(r'-g([0-9a-f]{7,40})$', run['build'])
        if run.get('source') or not match:
            continue
        try:
            commit = git('rev-parse', '--verify', match[1] + '^{commit}')
            git('merge-base', '--is-ancestor', commit, revision)
            base = git('merge-base', '--all', commit, 'origin/main')
            if re.fullmatch('[0-9a-f]{40}', base):
                result.append((run, base, commit))
        except subprocess.CalledProcessError:
            continue
    return result


def deploy(args, progress):
    if os.geteuid() != 0 or socket.gethostname() != args.hostname:
        raise RuntimeError('Expected root on the configured profiling host.')
    # The public web service never exposes this operator command.
    with open('/run/zakura-profile-update.lock', 'w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        os.chdir(args.repo)
        if git('status', '--porcelain'):
            raise RuntimeError('Remote checkout has local changes.')
        for service in SERVICES:
            command(['systemctl', 'is-active', '--quiet', service])
        if command(['systemctl', 'show', 'zakura-profile-sampler', '-p', 'MainPID', '--value'], capture_output=True).stdout.strip() != '0':
            raise RuntimeError('Stop the explicit CPU capture before upgrading.')
        # Fail before building if this is not the expected installation layout.
        for service, binary in [('zakurad', 'zakurad'), ('zakura-profile-collector', 'zakura-profile-explorer'), ('zakura-profile-web', 'zakura-profile-explorer')]:
            start = command(['systemctl', 'show', service, '-p', 'ExecStart', '--value'], capture_output=True).stdout
            if f'path=/usr/local/bin/{binary} ;' not in start:
                raise RuntimeError(f'Unexpected executable for {service}.')
        progress('fetching')
        git('fetch', '--no-tags', 'origin', 'main:refs/remotes/origin/main',
            f'{args.branch}:refs/remotes/origin/{args.branch}')
        git('merge-base', '--is-ancestor', args.revision, f'origin/{args.branch}')
        git('checkout', '--detach', args.revision)
        if git('merge-base', '--all', 'HEAD', 'origin/main') != args.base:
            raise RuntimeError('Main base changed during update. No services were stopped.')
        prior = old_sources(home()['runs'], args.revision)
        os.environ['PATH'] = str(Path.home() / '.cargo/bin') + ':' + os.environ['PATH']
        os.environ['CARGO_TARGET_DIR'] = args.target
        os.environ['CARGO_BUILD_JOBS'] = '4'
        progress('testing')
        command(['cargo', 'test', '--release', '--locked', '-p', 'zakura-jsonl-trace', '-p', 'zakura-profile-explorer'])
        progress('building')
        command(['cargo', 'build', '--release', '--locked', '-p', 'zakura', '-p', 'zakura-profile-explorer',
                 '--features', 'zakura/prometheus,zakura/commit-metrics'])
        version = command([str(Path(args.target) / 'release/zakurad'), '--version'], capture_output=True).stdout
        if args.revision[:9] not in version or '-dirty' in version:
            raise RuntimeError('Built executable does not identify the pinned revision.')
        backup = Path(__file__).parent / 'previous'
        backup.mkdir(exist_ok=True)
        for name in BINARIES:
            shutil.copy2(f'/usr/local/bin/{name}', backup / name)
            command(['install', '-m0755', str(Path(args.target) / 'release' / name), f'/usr/local/bin/{name}.next'])
        progress('deploying')
        # Seal the old node's last work, then drain the collector before replacing it.
        for service in SERVICES:
            command(['systemctl', 'stop', service])
        for name in BINARIES:
            os.replace(f'/usr/local/bin/{name}.next', f'/usr/local/bin/{name}')
        for service in ['zakura-profile-collector', 'zakura-profile-web', 'zakurad']:
            command(['systemctl', 'start', service])
        new_pid = int(command(['systemctl', 'show', 'zakurad', '-p', 'MainPID', '--value'], capture_output=True).stdout)
        progress('verifying')
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            try:
                data = home()
                run = next(r for r in data['runs'] if r['metadata']['id'] == data['run'])
                metadata = run['metadata']
                if metadata['pid'] == new_pid and metadata.get('source') == {'base_commit': args.base, 'commit': args.revision}:
                    if time.time() * 1000 - run['seen_ms'] < 10000 and time.time() * 1000 - data['health']['updated_ms'] < 15000:
                        break
            except (OSError, ValueError, StopIteration):
                pass
            time.sleep(2)
        else:
            raise RuntimeError('New run provenance was not observed. Inspect services before retrying.')
        for service in SERVICES:
            command(['systemctl', 'is-active', '--quiet', service])
        annotations = 0
        for legacy, base, commit in prior:
            try:
                command(['runuser', '-u', 'zakura-profile', '--', '/usr/local/bin/zakura-profile-explorer', 'source',
                         '--store', '/srv/zakura-profile/data', '--run', legacy['id'], '--expected-build', legacy['build'],
                         '--base-commit', base, '--commit', commit])
                annotations += 1
            except subprocess.CalledProcessError as error:
                print(f'Could not annotate legacy run {legacy["id"]}: {error}', flush=True)
        return {'run': metadata['id'], 'pid': metadata['pid'], 'legacy_runs_annotated': annotations,
                'binaries': {name: hashlib.file_digest(open(f'/usr/local/bin/{name}', 'rb'), 'sha256').hexdigest() for name in BINARIES}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ['hostname', 'revision', 'base', 'branch', 'repo', 'target']:
        parser.add_argument('--' + name, required=True)
    args = parser.parse_args()
    for sha in [args.revision, args.base]:
        if not re.fullmatch('[0-9a-f]{40}', sha):
            parser.error('Revision and base must be full Git object IDs.')
    directory = Path(__file__).resolve().parent
    with (directory / 'update.log').open('a', buffering=1) as log:
        os.dup2(log.fileno(), 1)
        os.dup2(log.fileno(), 2)
        result = {'status': 'running', 'base_commit': args.base, 'commit': args.revision}
        def save():
            result['updated_at'] = datetime.now(timezone.utc).isoformat()
            temp = directory / 'result.next.json'
            temp.write_text(json.dumps(result, indent=2) + '\n')
            temp.replace(directory / 'result.json')
        def progress(stage):
            result['stage'] = stage
            save()
        try:
            progress('checking host')
            result.update(deploy(args, progress), status='complete')
            save()
        except Exception as error:
            result.update(status='failed', error=str(error))
            save()
            raise


if __name__ == '__main__':
    main()
