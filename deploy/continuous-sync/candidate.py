#!/usr/bin/env python3
"""Launch or collect an isolated, two-host genesis comparison without addressing the fleet."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import shlex
import subprocess
import tarfile
import time

ROOT = Path(__file__).resolve().parent
LEGS = ('baseline', 'candidate')
TAG = 'zakura-pr-node'

# Read-only observations run from collection, outside the host's sync controller.
LIVE_OBSERVATION = '''
import json, time, urllib.request
from pathlib import Path
result = {'observed_at_epoch': time.time(), 'errors': {}}
for name, path in {
    'environment': '/root/genesis-candidate/environment.json',
    'node_config': '/etc/zakura/node.toml',
    'cpu_counters': '/proc/stat',
    'memory': '/proc/meminfo',
    'disk_counters': '/proc/diskstats',
    'uptime': '/proc/uptime',
}.items():
    try:
        result[name] = Path(path).read_text()
    except OSError as error:
        result['errors'][name] = str(error)
try:
    with urllib.request.urlopen('http://127.0.0.1:9999/metrics', timeout=5) as response:
        limit = 4 * 1024 * 1024
        data = response.read(limit + 1)
        result['metrics_truncated'] = len(data) > limit
        result['metrics'] = data[:limit].decode('utf-8', errors='replace')
except OSError as error:
    result['errors']['metrics'] = str(error)
print(json.dumps(result))
'''


def run(args, **kwargs):
    return subprocess.run(args, check=True, text=True, capture_output=True, timeout=kwargs.pop('timeout', 120), **kwargs).stdout


def do(*args):
    return json.loads(run(['doctl', 'compute', *args, '--output', 'json']))


def names(run_id, leg):
    if not re.fullmatch(r'[1-9][0-9]{0,19}', run_id) or leg not in LEGS:
        raise ValueError('invalid launch run ID or comparison leg')
    return f'zakura-genesis-{run_id}-{leg}', f'zakura-pr-genesis-{run_id}-{leg}'


def owned(run_id):
    droplets = do('droplet', 'list')
    volumes = do('volume', 'list')
    result = {}
    for leg in LEGS:
        node_name, volume_name = names(run_id, leg)
        nodes = [n for n in droplets if n['name'] == node_name]
        disks = [v for v in volumes if v['name'] == volume_name]
        if len(nodes) > 1 or len(disks) > 1:
            raise ValueError('ambiguous experiment ownership')
        node = nodes[0] if nodes else None
        disk = disks[0] if disks else None
        if node and (TAG not in node['tags'] or not disk or node['volume_ids'] != [disk['id']]):
            raise ValueError('experiment droplet tag or attached volume does not match')
        if disk and TAG not in disk.get('tags', []):
            raise ValueError('experiment volume tag does not match')
        if disk and any(i != (node or {}).get('id') for i in disk.get('droplet_ids', [])):
            raise ValueError('experiment volume is attached to another host')
        result[leg] = (node, disk)
    return result


def public_ip(node):
    return next(n['ip_address'] for n in node['networks']['v4'] if n['type'] == 'public')


def ssh_options(key):
    return ['-o', 'BatchMode=yes', '-o', 'ConnectTimeout=15', '-o', 'StrictHostKeyChecking=accept-new', '-i', str(key)]


def remote(node, key, args, **kwargs):
    return run(['ssh', *ssh_options(key), 'root@' + public_ip(node), shlex.join(args)], **kwargs)


def upload(node, key, local, path):
    run(['scp', '-C', *ssh_options(key), str(local), 'root@' + public_ip(node) + ':' + path])


def launch(args, out):
    if any(n or v for n, v in owned(args.run_id).values()):
        raise ValueError('this run already owns resources; collect it instead of relaunching')
    active = [n for n in do('droplet', 'list') if n['name'].startswith(('zakura-genesis-', 'zakura-sync-fixture-'))]
    if len(active) + 2 > 4:
        raise ValueError('launch would exceed four task droplets; collect/clean an earlier comparison first')
    for sha in (args.baseline_sha, args.candidate_sha, args.harness_sha):
        if not sha or not re.fullmatch(r'[0-9a-f]{40}', sha):
            raise ValueError('full pinned source SHAs are required')
    if not args.fingerprint:
        raise ValueError('DO SSH key fingerprint is required')
    # Resolve once: both legs get the same concrete image, even if its slug changes.
    image = do('image', 'get', 'ubuntu-24-04-x64')[0]
    image_id = str(image['id'])
    manifest = dict(launch_run_id=args.run_id, mode=args.mode, image_id=image_id,
                    size='g-8vcpu-32gb', region='sfo2', volume_gib=200, legs={})
    (out/'launch.json').write_text(json.dumps(manifest, indent=2)+'\n')
    bundle = out/'host-tools.tar'
    with tarfile.open(bundle, 'w') as tar:
        for name in ('candidate-host.py', 'candidate-start.sh', 'continuous-sync.py', 'nodes.toml', 'templates'):
            tar.add(ROOT/name, arcname=name)
    for leg, sha in zip(LEGS, (args.baseline_sha, args.candidate_sha)):
        name, volume_name = names(args.run_id, leg)
        volume = do('volume', 'create', volume_name, '--region', 'sfo2', '--size', '200GiB', '--fs-type', 'ext4', '--tag', TAG)[0]
        node = do('droplet', 'create', name, '--region', 'sfo2', '--size', 'g-8vcpu-32gb', '--image', image_id,
                  '--ssh-keys', args.fingerprint, '--volumes', volume['id'], '--tag-names', TAG, '--wait')[0]
        node = do('droplet', 'get', str(node['id']))[0]
        if node['size_slug'] != 'g-8vcpu-32gb' or node['region']['slug'] != 'sfo2' or node['volume_ids'] != [volume['id']]:
            raise ValueError('provisioned hardware or volume differs from the comparison plan')
        owner = dict(launch_run_id=args.run_id, leg=leg, source_sha=sha, harness_sha=args.harness_sha,
                     mode=args.mode, droplet_id=node['id'], volume_id=volume['id'], volume_name=volume_name,
                     ip=public_ip(node), image_id=image_id, size=node['size_slug'], region=node['region']['slug'])
        manifest['legs'][leg] = owner
        (out/'launch.json').write_text(json.dumps(manifest, indent=2)+'\n')
        owner_path = out/(leg+'-owner.json'); owner_path.write_text(json.dumps(owner, indent=2)+'\n')
        deadline = time.monotonic()+600
        while True:
            try:
                remote(node, args.key, ['true']); break
            except subprocess.CalledProcessError:
                if time.monotonic() >= deadline: raise
                time.sleep(5)
        remote(node, args.key, ['mkdir', '-p', '/root/genesis-candidate'])
        upload(node, args.key, bundle, '/root/genesis-candidate/host-tools.tar')
        upload(node, args.key, owner_path, '/root/genesis-candidate/owner.json')
        remote(node, args.key, ['tar', '-xf', '/root/genesis-candidate/host-tools.tar', '-C', '/root/genesis-candidate'])
        # First-boot package upgrades can restart transient services. Finish them
        # before starting the non-restartable build/sync controller.
        remote(node, args.key, ['cloud-init', 'status', '--wait'], timeout=900)
        remote(node, args.key, ['systemd-run', '--unit=zakura-genesis-candidate', '--property=RuntimeMaxSec=22h',
                               '--property=ExecStopPost=/usr/bin/systemctl stop zakura.service',
                               '/bin/bash', '/root/genesis-candidate/candidate-start.sh'])
    return {'status': 'launched', 'launch_run_id': args.run_id, 'legs': manifest['legs'],
            'next': 'Collect using this launch run ID before the 24-hour reaper deadline.'}


def rate(state):
    height, duration = state.get('last_success_end_height'), state.get('last_success_duration_seconds')
    if state.get('phase') != 'complete' or state.get('failed') or type(height) is not int or not 0 <= height <= 0xFFFFFFFF or type(duration) is not int or duration <= 0:
        return None
    return (height+1)/duration


def collect(args, out):
    found = owned(args.run_id)
    results = {}
    for leg, (node, volume) in found.items():
        if not node:
            results[leg] = {'status': 'missing', 'volume_id': (volume or {}).get('id')}; continue
        owner = json.loads(remote(node, args.key, ['cat', '/root/genesis-candidate/owner.json']))
        if owner['launch_run_id'] != args.run_id or owner['leg'] != leg or owner['droplet_id'] != node['id']:
            raise ValueError('host owner record does not match requested experiment')
        state_text = remote(node, args.key, ['python3', '-c', "from pathlib import Path; p=Path('/var/lib/zakura-continuous-sync/state.json'); print(p.read_text() if p.exists() else '{}')"])
        state = json.loads(state_text)
        unit = remote(node, args.key, ['systemctl', 'show', 'zakura-genesis-candidate', '-p', 'ActiveState', '-p', 'Result'])
        journal = remote(node, args.key, ['journalctl', '--no-pager', '-n', '300',
                         '-u', 'zakura-genesis-candidate', '-u', 'cloud-final',
                         '-u', 'apt-daily-upgrade', '-u', 'unattended-upgrades'])
        (out/(leg+'-bootstrap-journal.log')).write_text(journal)
        observation = remote(node, args.key, ['python3', '-c', LIVE_OBSERVATION])
        (out/(leg+'-live-observation.json')).write_text(observation)
        result = {'owner': owner, 'state': state, 'unit': unit, 'blocks_per_second': rate(state)}
        results[leg] = result
        # A complete/failed controller has stopped the node. Preserve its full evidence.
        if state.get('phase') in ('complete', 'failed') or 'ActiveState=failed' in unit:
            remote(node, args.key, ['bash', '-c', '''set -euo pipefail
journalctl --no-pager -u zakura-genesis-candidate -u zakura.service > /root/genesis-candidate/journal.log
paths=(root/genesis-candidate)
for path in etc/zakura var/log/zakura var/lib/zakura-continuous-sync/state.json; do
  if [[ -e "/$path" ]]; then paths+=("$path"); fi
done
tar -C / -czf /root/genesis-candidate-result.tar.gz "${paths[@]}"
'''], timeout=600)
            archive = out/(leg+'.tar.gz')
            run(['scp', '-C', *ssh_options(args.key), 'root@'+public_ip(node)+':/root/genesis-candidate-result.tar.gz', str(archive)], timeout=600)
            with tarfile.open(archive) as tar:
                members = tar.getnames()
                if 'root/genesis-candidate/owner.json' not in members: raise ValueError('missing ownership evidence in archive')
            with archive.open('rb') as archive_file:
                result['artifact_sha256'] = hashlib.file_digest(archive_file, 'sha256').hexdigest()
    return {'launch_run_id': args.run_id, 'legs': results,
            'note': 'BPS uses confirmed completion height plus genesis, divided by the full sync duration. Keep incomplete/failed rates unavailable.'}


def cleanup(args):
    resources = owned(args.run_id)
    node_ids = {n['id'] for n, _ in resources.values() if n}
    volume_ids = {v['id'] for _, v in resources.values() if v}
    for node, volume in resources.values():
        if node:
            run(['doctl', 'compute', 'droplet', 'delete', str(node['id']), '--force'])
        if volume:
            deadline = time.monotonic()+120
            while do('volume', 'get', volume['id'])[0].get('droplet_ids'):
                if time.monotonic() >= deadline: raise RuntimeError('volume did not detach')
                time.sleep(3)
            run(['doctl', 'compute', 'volume', 'delete', volume['id'], '--force'])
    # Deletion can clear a volume's tags before removing its inventory row.
    # Ownership was checked before deletion; now wait for those exact IDs to vanish.
    deadline = time.monotonic()+120
    while (node_ids & {n['id'] for n in do('droplet', 'list')}
           or volume_ids & {v['id'] for v in do('volume', 'list')}):
        if time.monotonic() >= deadline: raise RuntimeError('cleanup verification timed out')
        time.sleep(3)
    return {'launch_run_id': args.run_id, 'status': 'cleanup_verified'}


def summary(result):
    lines = [f"## Genesis comparison {result['launch_run_id']}", '']
    legs = result.get('legs', {})
    if any('state' in leg for leg in legs.values()):
        lines += ['| Leg | Commit | Phase | End height | Seconds | Blocks/sec |',
                  '| --- | --- | --- | ---: | ---: | ---: |']
        for name in LEGS:
            leg = legs.get(name, {})
            state = leg.get('state', {})
            bps = leg.get('blocks_per_second')
            lines.append('| ' + ' | '.join(map(str, [name,
                leg.get('owner', {}).get('source_sha', 'unavailable')[:12],
                state.get('phase', leg.get('status', 'unavailable')),
                state.get('last_success_end_height', '—'),
                state.get('last_success_duration_seconds', '—'),
                f'{bps:.2f}' if bps is not None else 'unavailable'])) + ' |')
        baseline, candidate = [legs.get(name, {}).get('blocks_per_second') for name in LEGS]
        if baseline is not None and candidate is not None:
            lines += ['', f'Candidate throughput change: {(candidate/baseline-1)*100:+.2f}%.',
                      'This is one public-network pair. Repeat before attributing the difference to the change.']
    else:
        lines += [result.get('status', 'No hosts found for this launch ID.')]
    lines += ['', 'Download the workflow artifact for source pins, host identities, and available run evidence.']
    return '\n'.join(lines)+'\n'


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('action', choices=['start', 'collect', 'cleanup'])
    p.add_argument('--run-id', required=True)
    p.add_argument('--mode', choices=['dual', 'zakura'], default='dual')
    p.add_argument('--baseline-sha'); p.add_argument('--candidate-sha'); p.add_argument('--harness-sha')
    p.add_argument('--fingerprint'); p.add_argument('--key', type=Path, default=Path('/tmp/do_ssh'))
    p.add_argument('--out', type=Path, default=Path('candidate-results'))
    args = p.parse_args(); names(args.run_id, 'baseline'); args.out.mkdir(parents=True, exist_ok=True)
    if args.action == 'start': result = launch(args, args.out)
    elif args.action == 'collect': result = collect(args, args.out)
    else: result = cleanup(args)
    (args.out/'result.json').write_text(json.dumps(result, indent=2)+'\n')
    (args.out/'summary.md').write_text(summary(result))
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
