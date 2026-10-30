#!/usr/bin/env python3
"""Capture archive state and refresh an isolated deployment's reusable public seed.

The capture command briefly stops one source node while copying its state, then
compresses the copy after restarting that node. Refresh restores a verified seed,
syncs it on public Testnet, and opens it without public peers to record an exact,
stable fork tip. Only state and non-finalized backups enter the preserved archive.
"""
from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import secrets
import signal
import shutil
import subprocess
import time
from urllib.request import Request, urlopen


NU7_HEIGHT = 4465026


class OperationCancelled(Exception):
    """An operator cancellation, never a retryable network or filesystem error."""


def interrupt_operation(signum, frame):
    """Turn service cancellation into an exception so ordinary cleanup runs."""
    raise OperationCancelled('archive operation was cancelled')


def command(*args, timeout=120):
    return subprocess.run(args, check=True, capture_output=True, text=True, timeout=timeout).stdout


def rpc(port, method, params=None):
    request = Request(f'http://127.0.0.1:{port}/', json.dumps({
        'jsonrpc': '2.0', 'id': 1, 'method': method, 'params': params or []
    }).encode(), {'Content-Type': 'application/json'})
    with urlopen(request, timeout=5) as response:
        result = json.load(response)
    if result.get('error'):
        raise RuntimeError(str(result['error']))
    return result['result']


def checksum(path):
    with Path(path).open('rb') as source:
        return hashlib.file_digest(source, 'sha256').hexdigest()


def retire_old_seed(config, current):
    """Keep the new seed and its previous base; remove only a recorded older file."""
    if not config.get('previous_seed_archive'):
        return
    previous = Path(config['previous_seed_archive'])
    base = Path(config['seed_archive'])
    if previous in (base, current):
        return
    if previous.parent != base.parent or previous.is_symlink() or previous.suffixes != ['.tar', '.zst']:
        raise ValueError('refusing to retire a seed outside the recorded archive directory')
    previous.unlink(missing_ok=True)


def state_bytes(cache):
    return sum(p.stat().st_size for name in ('state', 'non_finalized_state')
               for p in (Path(cache) / name).rglob('*') if p.is_file())


def pack_state(cache, archive):
    """Write an atomic compressed snapshot of a stopped node's state."""
    cache, archive = Path(cache), Path(archive)
    if archive.exists():
        raise ValueError('refusing to replace an immutable archive')
    temporary = archive.with_suffix('.partial')
    names = [name for name in ('state', 'non_finalized_state') if (cache / name).is_dir()]
    if 'state' not in names:
        raise ValueError('archive source has no finalized state')
    try:
        with temporary.open('wb') as output:
            tar = subprocess.Popen(['tar', '--hard-dereference', '-C', str(cache), '-cf', '-', *names], stdout=subprocess.PIPE)
            try:
                compressor = subprocess.Popen(['zstd', '-T2', '-3', '-q'], stdin=tar.stdout, stdout=output)
                tar.stdout.close()
                try:
                    if compressor.wait(timeout=300) or tar.wait(timeout=30):
                        raise RuntimeError('state compression failed')
                finally:
                    if compressor.poll() is None:
                        compressor.kill()
                        compressor.wait()
            finally:
                if tar.poll() is None:
                    tar.kill()
                    tar.wait()
        command('zstd', '-tq', str(temporary), timeout=180)
        temporary.replace(archive)
    finally:
        temporary.unlink(missing_ok=True)
    return checksum(archive)


def public_reference(url):
    """Require two agreeing, fresh public validators from the trusted collector."""
    with urlopen(url, timeout=10) as response:
        document = json.load(response)
    status = document['status']
    params = status['network']
    if (document.get('selectedProfile') != 'public-testnet'
            or params['name'] != 'Testnet' or params['activationHeight'] != NU7_HEIGHT
            or not -5 <= time.time() - document['generatedAt'] <= 60):
        raise ValueError('public reference is stale or describes a different network')
    rows = {row['name']: row for row in status['nodes'] if row['healthy']}
    counts = Counter((row['height'], row['hash']) for row in rows.values())
    if not counts or counts.most_common(1)[0][1] < 2:
        raise ValueError('two public validators do not agree')
    height, block_hash = counts.most_common(1)[0][0]
    return {'height': height, 'hash': block_hash,
            'validators': [name for name, row in rows.items()
                           if (row['height'], row['hash']) == (height, block_hash)]}


def verify_archive_history(port):
    info = rpc(port, 'getblockchaininfo')
    if info.get('pruned') is not False:
        raise ValueError('node does not report archive storage')
    for height in (1, 1000000, info['blocks'] - 20000):
        block_hash = rpc(port, 'getblockhash', [height])
        if not rpc(port, 'getblock', [block_hash, 0]):
            raise ValueError('archive node cannot return a historical block body')
    return info


def capture(cache, output, service, reference_url):
    """Copy a source archive while stopped; always restart it before compression."""
    cache, output = Path(cache), Path(output)
    if output.exists() or output.with_suffix('.capture').exists():
        raise ValueError('capture output already exists')
    reference = public_reference(reference_url)
    if len([name for name in reference['validators'] if name != 'zakura-testnet-1']) < 2:
        raise ValueError('two other public validators must remain online during capture')
    info = verify_archive_history(18232)
    size = state_bytes(cache)
    if shutil.disk_usage(output.parent).free < size * 2 + 1024**3:
        raise ValueError('insufficient capture space')
    staging = output.with_suffix('.capture')
    staging.mkdir()
    try:
        try:
            command('systemctl', 'stop', service)
            for name in ('state', 'non_finalized_state'):
                if (cache / name).exists():
                    command('cp', '-a', '--reflink=auto', str(cache / name), str(staging / name), timeout=180)
        finally:
            command('systemctl', 'start', service)
        digest = pack_state(staging, output)
        metadata = {'storage_mode': 'archive', 'sha256': digest,
                    'unpacked_bytes': size, 'source_observed_height': info['blocks'],
                    'source_observed_hash': info['bestblockhash'],
                    'source_observed_finalized': info['header_chain']['finalized'],
                    'captured_at': int(time.time())}
        output.with_suffix('.json').write_text(json.dumps(metadata, indent=2) + '\n')
        return metadata
    finally:
        shutil.rmtree(staging)


def preparation_config(root, probe=None):
    """Public sync uses normal parameters; the probe cannot connect to public peers."""
    network = 'network = "Testnet"' if probe is None else f'''initial_testnet_peers = ["127.0.0.1:1"]
[network.network]
network_name = "{probe['name']}"
network_magic = {probe['magic']}
inherit_activation_heights = true
checkpoints = {probe['checkpoints']}'''
    return f'''[network]
listen_addr = "127.0.0.1:18433"
p2p_stack = "legacy"
cache_dir = "{root}/preparation/network"
identity_dir = "{root}/preparation/identity"
{network}
[state]
cache_dir = "{root}/preparation/cache"
storage_mode = "archive"
[consensus]
checkpoint_sync = false
vct_fast_sync = false
[rpc]
listen_addr = "127.0.0.1:18432"
enable_cookie_auth = false
[mining]
internal_miner = false
'''


def wait_for_rpc(network_name, timeout=240):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if rpc(18432, 'getnetworkparameters')['network'] == network_name:
                return
        except (OSError, RuntimeError):
            pass
        time.sleep(2)
    raise TimeoutError('archive preparation node did not become ready')


def wait_for_sync(reference_url, timeout=1800):
    deadline = time.monotonic() + timeout
    consecutive = 0
    while time.monotonic() < deadline:
        try:
            reference = public_reference(reference_url)
            info = rpc(18432, 'getblockchaininfo')
            ready = (info.get('pruned') is False
                     and reference['height'] <= info['blocks'] <= reference['height'] + 1
                     and info['estimatedheight'] - info['blocks'] <= 1
                     and rpc(18432, 'getblockhash', [reference['height']]) == reference['hash'])
            consecutive = consecutive + 1 if ready else 0
            if consecutive == 2:
                verify_archive_history(18432)
                return reference
        except (OSError, RuntimeError, ValueError):
            consecutive = 0
        time.sleep(2)
    raise TimeoutError('archive did not sync to the agreeing public validators')


def refresh_seed(config, restore_archive):
    """Sync, isolate, verify and atomically preserve a reusable archive seed.

    The private probe resolves blocks committed between the last public RPC read
    and shutdown. Its fresh peer cache and distinct magic prevent further imports.
    """
    root = Path(config['root'])
    progress = root / 'seed-progress.json'

    def stage(phase, **values):
        temporary = progress.with_suffix('.tmp')
        temporary.write_text(json.dumps(dict(phase=phase, observed_at=int(time.time()), **values)))
        temporary.replace(progress)

    archive = Path(config['seed_archive'])
    if not archive.is_file() or checksum(archive) != config['seed_sha256']:
        raise ValueError('refresh source archive checksum mismatch')
    preparation = root / 'preparation'
    service = f'{config["service_prefix"]}-preparation.service'
    command('systemctl', 'stop', service)
    if preparation.is_symlink():
        raise ValueError('refusing a symlinked preparation directory')
    if preparation.exists():
        shutil.rmtree(preparation)
    preparation.mkdir()
    stage('restoring')
    try:
        restore_archive(archive, preparation / 'cache', 'Testnet', config.get('seed_state_version'))
        command('chown', '-R', f'{config["node_user"]}:{config["node_user"]}', str(preparation))
        (root / 'preparation.toml').write_text(preparation_config(root))
        command('systemctl', 'start', service)
        stage('syncing')
        wait_for_rpc('Testnet')
        reference = wait_for_sync(config['reference_status_url'])
        info = rpc(18432, 'getblockchaininfo')
        finalized = info['header_chain']['finalized']
        command('systemctl', 'stop', service)
        stage('verifying', public_tip=reference)
        name = 'ArchiveProbe' + secrets.token_hex(4)
        for version in (preparation / 'cache/state').iterdir():
            (version / 'testnet').rename(version / name.lower())
        backups = preparation / 'cache/non_finalized_state/testnet'
        if backups.exists():
            backups.rename(backups.with_name(name.lower()))
        for directory in ('network', 'identity'):
            shutil.rmtree(preparation / directory, ignore_errors=True)
        probe = {'name': name, 'magic': list(secrets.token_bytes(4)),
                 'checkpoints': [[0, list(bytes.fromhex('05a60a92d99d85997cce3b87616c089f6124d7342af37106edc76126334a2c38')[::-1])],
                                 [finalized['height'], list(bytes.fromhex(finalized['hash'])[::-1])]]}
        (root / 'preparation.toml').write_text(preparation_config(root, probe))
        command('systemctl', 'start', service)
        wait_for_rpc(name)
        info = verify_archive_history(18432)
        if (not reference['height'] <= info['blocks'] <= reference['height'] + 10
                or rpc(18432, 'getblockhash', [reference['height']]) != reference['hash']):
            raise ValueError('isolated archive differs from the verified public tip')
        finalized = info['header_chain']['finalized']
        command('systemctl', 'stop', service)
        size = state_bytes(preparation / 'cache')
        # Preparation is removed before restoring the two validators. Reserving
        # twice its size covers both the compressed seed and those final copies.
        if shutil.disk_usage(root).free < size * 2 + 2 * 1024**3:
            raise ValueError('insufficient space for the preserved seed and two archive validators')
        stage('preserving', public_tip=reference, seed_height=info['blocks'])
        destination = archive.parent / f'archive-seed-{info["blocks"]}-{name.lower()}.tar.zst'
        digest = pack_state(preparation / 'cache', destination)
        updated = dict(config, seed_archive=str(destination), seed_sha256=digest,
                       previous_seed_archive=str(archive),
                       seed_height=info['blocks'], seed_hash=info['bestblockhash'],
                       seed_finalized_height=finalized['height'], seed_finalized_hash=finalized['hash'],
                       seed_network_directory=name.lower(), seed_storage_mode='archive',
                       seed_unpacked_bytes=size, seed_synced_at=int(time.time()))
        retire_old_seed(config, destination)
        stage('ready', public_tip=reference, seed_height=info['blocks'])
        return updated
    except Exception as error:
        stage('failed', error=str(error))
        raise
    finally:
        command('systemctl', 'stop', service)
        shutil.rmtree(preparation)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cache-dir', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--service', default='zakurad.service')
    parser.add_argument('--reference-url', default='https://api.nu7.valargroup.dev/v1/dashboard')
    args = parser.parse_args(argv)
    signal.signal(signal.SIGTERM, interrupt_operation)
    print(json.dumps(capture(args.cache_dir, args.output, args.service, args.reference_url), indent=2))


if __name__ == '__main__':
    main()
