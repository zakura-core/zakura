#!/usr/bin/env python3
"""Merge a main revision into the profiling branch, then build and deploy one host."""
import argparse
import json
from pathlib import Path
import shlex
import subprocess
import sys
import time


def command(args, **kwargs):
    return subprocess.run(args, check=True, text=True, **kwargs)


def git(*args):
    return command(['git', *args], capture_output=True).stdout.strip()


def prepare(branch, base, keep_base=False):
    requested = None
    if git('status', '--porcelain'):
        raise RuntimeError('Commit or stash local changes before updating the profiler.')
    if git('branch', '--show-current') != branch:
        raise RuntimeError(f'Run this from the {branch} checkout.')
    git('fetch', '--no-tags', 'origin', f'main:refs/remotes/origin/main',
        f'{branch}:refs/remotes/origin/{branch}')
    # Include other operators' changes without rewriting or overwriting their work.
    git('merge', '--ff-only', f'origin/{branch}')
    if not keep_base:
        base = requested = git('rev-parse', '--verify', f'{base}^{{commit}}')
        git('merge-base', '--is-ancestor', base, 'origin/main')
        try:
            git('merge', '--no-edit', base)
        except subprocess.CalledProcessError:
            # The checkout was clean. Abort only the merge started by this invocation.
            if Path(git('rev-parse', '--git-path', 'MERGE_HEAD')).exists():
                git('merge', '--abort')
            raise RuntimeError('Main conflicts with profiling changes. Resolve locally before deployment.')
    revision = git('rev-parse', 'HEAD')
    base = git('merge-base', '--all', revision, 'origin/main')
    if len(base) != 40:
        raise RuntimeError('Expected one unambiguous main base.')
    if requested is not None and base != requested:
        raise RuntimeError('This branch already includes newer main code. Updating cannot downgrade it.')
    git('push', 'origin', f'HEAD:refs/heads/{branch}')
    return revision, base


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--base', default='origin/main', help='Main commit to include (default: latest main).')
    parser.add_argument('--keep-base', action='store_true', help='Deploy profiling changes without updating main.')
    parser.add_argument('--prepare-only', action='store_true', help='Merge and push without touching the host.')
    args = parser.parse_args()
    config = json.loads(args.config.read_text())
    root = Path(git('rev-parse', '--show-toplevel'))
    branch = config.get('branch', 'adam/block-profile-operations')
    revision, base = prepare(branch, args.base, args.keep_base)
    print(f'Main base {base}\nInstrumented commit {revision}', flush=True)
    if args.prepare_only:
        return
    command(['node', '--test', str(root / 'crates/zakura-profile-explorer/web/app.test.cjs'), str(root / 'crates/zakura-profile-explorer/web/cpu.test.cjs')])
    ssh = ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=15']
    if config.get('ssh_config'):
        ssh += ['-F', str(Path(config['ssh_config']).expanduser())]
    ssh += [config['host']]
    actual_host = command(ssh + ['hostname'], capture_output=True).stdout.strip()
    if actual_host != config['hostname']:
        raise RuntimeError('SSH destination does not match the configured hostname.')
    directory = f'/var/lib/zakura-profile-updates/{revision}'
    payload = (root / 'deploy/block-profile/update_host.py').read_text()
    # Upload to a commit-specific operator directory. Builds survive a disconnected terminal.
    command(ssh + [f'umask 077; mkdir -p {directory}; cat > {directory}/update.py'], input=payload)
    remote_args = ['systemd-run', '--unit=zakura-profile-update', '--uid=root',
                   '--property=Nice=10', '--property=CPUQuota=400%', '--property=MemoryMax=20G',
                   'python3', '-u', f'{directory}/update.py',
                   '--hostname', config['hostname'], '--revision', revision, '--base', base,
                   '--branch', branch, '--repo', config.get('repo', '/root/zakura'),
                   '--target', config.get('target', '/root/cargo-target')]
    # A still-running unit rejects a duplicate invocation, before the checkout can change.
    command(ssh + ['systemctl reset-failed zakura-profile-update.service 2>/dev/null || true'])
    command(ssh + [shlex.join(remote_args)])
    print(f'Build and deployment started. Logs: {directory}/update.log', flush=True)
    status_path = f'{directory}/result.json'
    previous = None
    for _ in range(720):
        time.sleep(5)
        response = command(ssh + [f'cat {status_path} 2>/dev/null || true'], capture_output=True).stdout
        if not response:
            continue
        status = json.loads(response)
        if status != previous:
            print(json.dumps(status), flush=True)
            previous = status
        if status['status'] == 'complete':
            return
        if status['status'] == 'failed':
            raise RuntimeError(f"Update stopped: {status['error']}. Read {directory}/update.log.")
    raise RuntimeError('Update is still running. Inspect the remote unit and log before retrying.')


if __name__ == '__main__':
    try:
        main()
    except (RuntimeError, subprocess.CalledProcessError) as error:
        print(str(error), file=sys.stderr)
        sys.exit(1)
