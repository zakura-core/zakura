#!/usr/bin/env python3
"""Restore isolated Testnet runs, supervise their countdown, and serve public status.

The operator config names a preserved seed and a deployment root. Reset replaces
only databases below that root. run.json records a generation and its scheduled
mining start; serve recovers that schedule after a controller restart.
"""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import statistics
import subprocess
import tarfile
import threading
import time
import tomllib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.request import Request, urlopen


def command(*args, timeout=120):
    return subprocess.run(args, check=True, capture_output=True, text=True, timeout=timeout).stdout


def atomic_json(path, value):
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value, indent=2) + '\n')
    temporary.replace(path)


def atomic_text(path, value):
    temporary = path.with_suffix('.tmp')
    temporary.write_text(value)
    temporary.replace(path)


def load_config(path):
    config = json.loads(path.read_text())
    root = Path(config['root'])
    if not root.is_absolute() or len(root.parts) < 4 or root.is_symlink():
        raise ValueError('deployment root must be an absolute dedicated directory')
    if not re.fullmatch(r'[a-z][a-z0-9-]*', config['service_prefix']):
        raise ValueError('invalid service prefix')
    if not re.fullmatch(r'[0-9a-f]{40}', config['revision']):
        raise ValueError('revision must identify the deployed source commit')
    if config['seed_height'] >= 4465026 or config['seed_height'] < 4134000:
        raise ValueError('seed must be after NU6.3 and before public Testnet NU7')
    if not 0 < config['seed_finalized_height'] < config['seed_height']:
        raise ValueError('finalized checkpoint must precede the full seed tip so its last block is replayed')
    for field in ('seed_hash', 'seed_finalized_hash', 'seed_sha256', 'binary_sha256'):
        if not re.fullmatch(r'[0-9a-f]{64}', config[field]):
            raise ValueError('seed and binary hashes must be full lowercase hex values')
    return config


def rpc(port, method, params=None):
    request = Request(f'http://127.0.0.1:{port}/', json.dumps({
        'jsonrpc': '2.0', 'id': 1, 'method': method, 'params': params or []
    }).encode(), {'Content-Type': 'application/json'})
    with urlopen(request, timeout=5) as response:
        result = json.load(response)
    if result.get('error'):
        raise RuntimeError(str(result['error']))
    return result['result']


def verify_archive(config):
    with Path(config['seed_archive']).open('rb') as archive:
        digest = hashlib.file_digest(archive, 'sha256').hexdigest()
    if digest != config['seed_sha256']:
        raise ValueError('seed archive checksum differs from the pinned checksum')
    root = Path(config['root'])
    reclaimable = 0
    for index in range(2):
        node = root / f'node{index}'
        if node.is_symlink():
            raise ValueError('refusing a symlinked disposable node directory')
        if node.exists():
            reclaimable += sum(p.stat().st_size for p in node.rglob('*') if p.is_file() and not p.is_symlink())
    if shutil.disk_usage(root).free + reclaimable < config.get('minimum_free_bytes', 45 * 1024**3):
        raise ValueError('insufficient free disk space for two restored databases')


def restore_archive(archive, destination, network_name):
    """Extract state files without trusting archive paths, links, or special files."""
    destination.mkdir()
    with subprocess.Popen(['zstd', '-dc', str(archive)], stdout=subprocess.PIPE) as decompressor:
        with tarfile.open(fileobj=decompressor.stdout, mode='r|') as stream:
            for member in stream:
                parts = Path(member.name).parts
                if member.name.startswith('/') or '..' in parts:
                    raise ValueError('unsafe seed archive path')
                if not parts or parts[0] not in ('state', 'non_finalized_state'):
                    raise ValueError('seed archive contains files outside chain state')
                if not member.isdir() and not member.isfile():
                    raise ValueError('seed archive contains a link or special file')
                target = destination.joinpath(*parts)
                if member.isdir():
                    target.mkdir(parents=True, exist_ok=True)
                else:
                    target.parent.mkdir(parents=True, exist_ok=True)
                    with stream.extractfile(member) as source, target.open('wb') as output:
                        shutil.copyfileobj(source, output)
        if decompressor.wait(timeout=30):
            raise RuntimeError('seed decompression failed')
    finalized = destination / 'state'
    if not finalized.exists():
        raise ValueError('seed has no finalized state')
    for version in finalized.iterdir():
        if not version.is_dir() or not re.fullmatch(r'v[0-9]+', version.name):
            raise ValueError('unexpected finalized state layout')
        networks = list(version.iterdir())
        if len(networks) != 1 or not networks[0].is_dir():
            raise ValueError('seed must contain one network per state version')
        networks[0].rename(version / network_name.lower())
    backup = destination / 'non_finalized_state'
    if backup.exists():
        # Non-finalized backups are grouped by network, without a version directory.
        networks = list(backup.iterdir())
        if len(networks) != 1 or not networks[0].is_dir():
            raise ValueError('seed must contain one non-finalized network backup')
        networks[0].rename(backup / network_name.lower())


def node_config(config, run, index, mining=False):
    root = Path(config['root'])
    name = run['network']['name']
    peer = 18333 if index == 0 else 18233
    port = 18233 if index == 0 else 18333
    rpc_port = 18232 if index == 0 else 18332
    magic = json.dumps(run['magic_bytes'])
    address = json.dumps(config['miner_address'])
    checkpoints = json.dumps([[0, list(bytes.fromhex('05a60a92d99d85997cce3b87616c089f6124d7342af37106edc76126334a2c38')[::-1])], [config['seed_finalized_height'], list(bytes.fromhex(config['seed_finalized_hash'])[::-1])]])
    return f'''[network]
listen_addr = "0.0.0.0:{port}"
initial_testnet_peers = ["127.0.0.1:{peer}"]
p2p_stack = "legacy"
cache_dir = "{root}/node{index}/network"
identity_dir = "{root}/node{index}/identity"
[network.network]
network_name = "{name}"
network_magic = {magic}
checkpoints = {checkpoints}
inherit_activation_heights = true
pq_p2sh_activation_height = {run['activation_height']}
max_block_time_start_height = {config['seed_height'] + 7}
[network.network.activation_heights]
NU7 = {run['activation_height']}
[state]
cache_dir = "{root}/node{index}/cache"
storage_mode = "pruned"
[rpc]
listen_addr = "127.0.0.1:{rpc_port}"
enable_cookie_auth = false
[mining]
miner_address = {address}
internal_miner = {str(mining).lower()}
extra_coinbase_data = "{run['id']}-node{index}"
'''


def participant_config(config, run):
    source = tomllib.loads(node_config(config, run, 1))
    # Retain exactly the trusted consensus table; local service paths are not public config.
    params = source['network']['network']
    lines = ['[network]', 'listen_addr = "0.0.0.0:18233"',
             f'initial_testnet_peers = ["{config["public_peer"]}:18233"]',
             'p2p_stack = "legacy"', '[network.network]']
    for key, value in params.items():
        if key != 'activation_heights':
            lines.append(f'{key} = {json.dumps(value)}')
    lines += ['[network.network.activation_heights]', f'NU7 = {run["activation_height"]}',
              '[state]', f'cache_dir = "./{run["id"]}-state"', 'storage_mode = "pruned"',
              '[rpc]', 'listen_addr = "127.0.0.1:18232"']
    return '\n'.join(lines) + '\n'


def new_run(config):
    identity = secrets.token_hex(4)
    magic = list(secrets.token_bytes(4))
    while magic in ([250, 26, 249, 191], [36, 233, 39, 100], [170, 232, 63, 245]):
        magic = list(secrets.token_bytes(4))
    return {'id': identity, 'phase': 'preparing', 'magic_bytes': magic,
            'activation_height': config['seed_height'] + 1,
            'network': {'name': 'PqNu7' + identity, 'magic': bytes(magic).hex()},
            'revision': config['revision'], 'seed_height': config['seed_height'],
            'seed_hash': config['seed_hash']}


def verify_nodes(config, run, require_seed=False):
    rows = []
    for index, port in enumerate((18232, 18332)):
        params = rpc(port, 'getnetworkparameters')
        if (params['network'] != run['network']['name']
                or params['pqP2shActivationHeight'] != run['activation_height']
                or params['nu7ActivationHeight'] != run['activation_height']):
            raise ValueError('running node activation parameters do not match this run')
        info = rpc(port, 'getblockchaininfo')
        height = info['blocks']
        block_hash = rpc(port, 'getblockhash', [height])
        if require_seed and (height != config['seed_height'] or block_hash != config['seed_hash']):
            raise ValueError('restored node tip does not match the preserved seed')
        rows.append({'name': f'node{index}', 'height': height, 'hash': block_hash})
    common = min(row['height'] for row in rows)
    if rpc(18232, 'getblockhash', [common]) != rpc(18332, 'getblockhash', [common]):
        raise ValueError('validators disagree at their common height')
    return rows


def reset(config, delay):
    root = Path(config['root'])
    owner = json.loads((root / 'owner.json').read_text())
    if owner != {'service_prefix': config['service_prefix'], 'root': str(root)}:
        raise ValueError('deployment ownership does not match the reset configuration')
    verify_archive(config)
    with Path(config['binary']).open('rb') as binary:
        if hashlib.file_digest(binary, 'sha256').hexdigest() != config['binary_sha256']:
            raise ValueError('installed binary differs from deployment provenance')
    command(config['binary'], '--version')
    previous = root / 'run.json'
    if previous.exists():
        history = root / 'history'
        history.mkdir(exist_ok=True)
        old = json.loads(previous.read_text())
        atomic_json(history / f'{old["id"]}.json', old)
        for expired in sorted(history.glob('*.json'), key=lambda p: p.stat().st_mtime)[:-20]:
            expired.unlink()
    run = new_run(config)
    command('systemctl', 'stop', f'{config["service_prefix"]}-controller.service')
    atomic_json(root / 'run.json', run)
    try:
        for suffix in ('node0', 'node1'):
            command('systemctl', 'stop', f'{config["service_prefix"]}-{suffix}.service')
        command('systemctl', 'start', f'{config["service_prefix"]}-controller.service')
        for index in range(2):
            node = root / f'node{index}'
            if node.is_symlink():
                raise ValueError('refusing to replace a symlinked node directory')
            if node.exists():
                shutil.rmtree(node)
            node.mkdir()
            restore_archive(config['seed_archive'], node / 'cache', run['network']['name'])
            if config.get('node_user'):
                for path in [node, *node.rglob('*')]:
                    shutil.chown(path, user=config['node_user'], group=config['node_user'])
            atomic_text(root / f'node{index}.toml', node_config(config, run, index))
        for index in range(2):
            command('systemctl', 'start', f'{config["service_prefix"]}-node{index}.service')
        deadline = time.monotonic() + 240
        while True:
            try:
                verify_nodes(config, run, require_seed=True)
                break
            except (OSError, RuntimeError, ValueError):
                if time.monotonic() >= deadline:
                    raise RuntimeError('restored nodes did not become ready within four minutes')
                time.sleep(2)
        run.update(phase='countdown', ready_at=int(time.time()), mining_at=int(time.time()) + delay)
        atomic_json(root / 'run.json', run)
        (root / 'participant.toml').write_text(participant_config(config, run))
        command('systemctl', 'start', f'{config["service_prefix"]}-controller.service')
    except Exception as error:
        run.update(phase='failed', error=str(error))
        atomic_json(root / 'run.json', run)
        for index in range(2):
            command('systemctl', 'stop', f'{config["service_prefix"]}-node{index}.service')
        command('systemctl', 'start', f'{config["service_prefix"]}-controller.service')
        raise
    return run


class Controller:
    def __init__(self, config):
        self.config = config
        self.root = Path(config['root'])
        self.lock = threading.Lock()
        self.recent_tip = None
        self.recent = []

    def recent_blocks(self, tip, activation):
        key = (tip['height'], tip['hash'])
        if key != self.recent_tip:
            blocks = []
            for height in range(max(0, tip['height'] - 5), tip['height'] + 1):
                block_hash = rpc(18232, 'getblockhash', [height])
                header = rpc(18232, 'getblockheader', [block_hash, True])
                blocks.append({'height': height, 'hash': block_hash, 'time': header['time']})
            self.recent = blocks
            self.recent_tip = key
        intervals = [right['time'] - left['time'] for left, right in zip(self.recent, self.recent[1:]) if left['height'] >= activation]
        return self.recent, statistics.median(intervals) if intervals else None

    def sample(self, advance=False):
        with self.lock:
            run = json.loads((self.root / 'run.json').read_text())
            result = dict(run, observed_at=int(time.time()), nodes=[])
            result.pop('activation_hash', None)
            try:
                if run['phase'] not in ('preparing', 'failed'):
                    result['nodes'] = verify_nodes(self.config, run)
                if advance and run['phase'] == 'countdown' and time.time() >= run['mining_at']:
                    # Repeating this step after a crash only restarts the same named service.
                    atomic_text(self.root / 'node0.toml', node_config(self.config, run, 0, mining=True))
                    command('systemctl', 'restart', f'{self.config["service_prefix"]}-node0.service')
                    run['phase'] = 'mining'
                    atomic_json(self.root / 'run.json', run)
                if result['nodes'] and run['phase'] in ('mining', 'active'):
                    if min(n['height'] for n in result['nodes']) >= run['activation_height']:
                        run['phase'] = 'active'
                        run['activation_hash'] = rpc(18232, 'getblockhash', [run['activation_height']])
                    else:
                        run['phase'] = 'mining'
                        run.pop('activation_hash', None)
                    if advance:
                        atomic_json(self.root / 'run.json', run)
                result.update(run)
                result['healthy'] = bool(result['nodes'])
            except Exception as error:
                result.update(healthy=False, error=str(error))
            result['recent_blocks'] = []
            result['median_interval_seconds'] = None
            result['mining_enabled'] = run['phase'] in ('mining', 'active')
            if result['nodes']:
                try:
                    result['recent_blocks'], result['median_interval_seconds'] = self.recent_blocks(result['nodes'][0], run['activation_height'])
                except Exception:
                    result['recent_blocks_available'] = False
            result['countdown_seconds'] = max(0, run.get('mining_at', 0) - int(time.time()))
            result['config'] = participant_config(self.config, run)
            result['build_features'] = ['pq', 'internal-miner']
            result['snapshot'] = {'url': '/snapshots/seed.tar.zst', 'sha256': self.config['seed_sha256'],
                                  'height': self.config['seed_height'], 'network_directory': self.config['seed_network_directory']}
            return result


def serve(config):
    controller = Controller(config)
    status = controller.sample(advance=True)
    status_lock = threading.Lock()

    def refresh():
        nonlocal status
        while True:
            sample = controller.sample(advance=True)
            with status_lock:
                status = sample
            time.sleep(2)

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path in ('/v1/network', '/v1/status', '/healthz'):
                with status_lock:
                    body = json.dumps(status).encode()
                self.send_response(200 if status.get('healthy') else 503)
                self.send_header('Content-Type', 'application/json')
            elif self.path == '/v1/config':
                with status_lock:
                    body = status['config'].encode()
                self.send_response(200)
                self.send_header('Content-Type', 'text/plain')
            elif self.path == '/':
                body = Path(__file__).with_name('dashboard.html').read_bytes()
                self.send_response(200)
                self.send_header('Content-Type', 'text/html; charset=utf-8')
            else:
                self.send_error(404)
                return
            self.send_header('Cache-Control', 'no-store')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    threading.Thread(target=refresh, daemon=True).start()
    ThreadingHTTPServer(('127.0.0.1', 8093), Handler).serve_forever()


def check_node(config, index):
    """Fail closed on reboot if a node config belongs to an old or unfinished run."""
    root = Path(config['root'])
    run = json.loads((root / 'run.json').read_text())
    node = tomllib.loads((root / f'node{index}.toml').read_text())
    expected = tomllib.loads(node_config(config, run, index))
    if node['network'] != expected['network'] or node['state'] != expected['state']:
        raise ValueError('node configuration does not belong to the current run')
    if run['phase'] not in ('preparing', 'countdown', 'mining', 'active'):
        raise ValueError('this run is not permitted to start nodes')
    if node['mining'].get('internal_miner'):
        if index != 0 or run['phase'] not in ('countdown', 'mining', 'active') or time.time() < run['mining_at']:
            raise ValueError('mining is not permitted before this run countdown ends')


def activation_delay(value):
    match = re.fullmatch(r'([1-9][0-9]*)([sm])', value)
    if not match:
        raise argparse.ArgumentTypeError('use a positive delay such as 5m or 30s')
    seconds = int(match[1]) * (60 if match[2] == 'm' else 1)
    if seconds > 86400:
        raise argparse.ArgumentTypeError('activation delay must be at most one day')
    return seconds


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, default=Path('/etc/zakura-custom-testnet.json'))
    commands = parser.add_subparsers(dest='action', required=True)
    reset_parser = commands.add_parser('reset')
    reset_parser.add_argument('--activation-delay', type=activation_delay, default=300)
    commands.add_parser('serve')
    commands.add_parser('status')
    gate = commands.add_parser('check-node')
    gate.add_argument('--index', type=int, choices=(0, 1), required=True)
    args = parser.parse_args(argv)
    config = load_config(args.config)
    root = Path(config['root'])
    root.mkdir(parents=True, exist_ok=True)
    if args.action == 'serve':
        serve(config)
    elif args.action == 'check-node':
        check_node(config, args.index)
    elif args.action == 'status':
        print(json.dumps(Controller(config).sample(), indent=2))
    else:
        with (root / 'reset.lock').open('w') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            print(json.dumps(reset(config, args.activation_delay), indent=2))


if __name__ == '__main__':
    main()
