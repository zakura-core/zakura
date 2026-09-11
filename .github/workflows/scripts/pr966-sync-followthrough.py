#!/usr/bin/env python3
"""Finish the task-owned Mainnet sync with native metrics and a public reference."""
import datetime
import hashlib
import json
import re
import runpy
import subprocess
import time
import tomllib
from pathlib import Path

helpers = runpy.run_path('/root/pr-node-paired-smoke.py')
rpc, metric, metrics, stop = (helpers[k] for k in ('rpc', 'metric', 'metrics', 'stop'))
sha = subprocess.check_output(['git', '-C', '/root/zakura', 'rev-parse', 'HEAD'], text=True).strip()
assert sha == helpers['EXPECTED_SHA'] == 'e3328340aa040971681ed222a5bfc63fe43d8991'
# Leave time for graceful shutdown and artifact collection before the parent teardown.
hard_stop = datetime.datetime(2026, 9, 11, 7, 36, 0, tzinfo=datetime.timezone.utc).timestamp()
prior_path = Path('/root/out/paired/summary.json')
while not prior_path.exists():
    if time.time() >= hard_stop - 120:
        raise RuntimeError('the first sync/restart phase did not finish in time')
    print('Waiting for the first sync/restart phase to finish.', flush=True)
    time.sleep(5)
prior = json.loads(prior_path.read_text())
assert prior['sha'] == sha
assert prior.get('error') == 'initial-sync did not verify 2150 new native blocks before its deadline', prior
assert prior['phases'] == [], prior
config_text = Path('/root/out/paired/downloader.toml').read_text()
config = tomllib.loads(config_text)
assert config['network']['p2p_stack'] == 'zakura'
assert config['consensus']['vct_fast_sync'] is True
try:
    rpc(helpers['CLIENT_RPC'], 'getblockcount')
except OSError:
    pass
else:
    raise RuntimeError('the first downloader must stop before followthrough')

out = Path('/root/out/native-followthrough')
out.mkdir(exist_ok=False)
config_text = config_text.replace('/root/out/paired/', str(out) + '/')
(out / 'downloader.toml').write_text(config_text)
result = {'sha': sha, 'pass': False, 'mode': 'restart diagnostic after checkpoint stall', 'prior': prior, 'samples': [],
          'peer_health_source': 'native block-sync connection gauge',
          'public_reference': 'http://159.65.183.89:8232'}
proc = None
console = (out / 'console.log').open('w')
try:
    binary = helpers['BINARY']
    with open(binary, 'rb') as f:
        result['binary_sha256'] = hashlib.file_digest(f, 'sha256').hexdigest()
    tip_output = subprocess.check_output([binary, '-c', str(out / 'downloader.toml'), 'tip-height',
        '--cache-dir', config['state']['cache_dir'], '--network', 'Mainnet'], text=True,
        stderr=subprocess.STDOUT, timeout=90)
    result['persisted_start_height'] = int(re.findall(r'^([0-9]+)$', tip_output, re.M)[-1])
    assert result['persisted_start_height'] == 3476010, 'expected the stalled checkpoint state'
    result['reference_start_height'] = rpc(result['public_reference'], 'getblockcount')
    proc = subprocess.Popen([binary, '-c', str(out / 'downloader.toml'), 'start'], stdout=console, stderr=console)
    caught_up_at = None
    probe_started = time.time()
    while time.time() < hard_stop:
        if proc.poll() is not None:
            raise RuntimeError(f'downloader exited unexpectedly: {proc.returncode}')
        sample = {'unix_time': time.time()}
        try:
            client = rpc(helpers['CLIENT_RPC'], 'getblockchaininfo')
            seed = rpc(helpers['SEED_RPC'], 'getblockchaininfo')
            text = metrics(19999)
            native_connections = sum(float(line.split()[-1]) for line in text.splitlines()
                if line.startswith('zakura_p2p_reactor_active_connections{') and 'reactor="block_sync"' in line)
            sample.update(height=client['blocks'], seed_height=seed['blocks'],
                pruneheight=client['pruneheight'], native_connections=native_connections,
                native_bodies=metric(text, 'sync_block_body_received'),
                native_requests=metric(text, 'sync_block_request_sent'),
                legacy_fallbacks=metric(text, 'sync_zakura_legacy_fallback_engaged'))
            assert sample['legacy_fallbacks'] == 0, 'native client used a legacy fallback'
            if native_connections > 0 and sample['height'] >= result['persisted_start_height'] + 32 and sample['native_bodies'] >= 32:
                if caught_up_at is None:
                    caught_up_at = time.time()
                if time.time() - probe_started >= 45:
                    result['final_client'] = client
                    result['final_seed'] = seed
                    result['restart_probe_seconds'] = time.time() - caught_up_at
                    result['final'] = sample
                    (out / 'final-metrics.txt').write_text(text)
                    break
            else:
                caught_up_at = None
        except (OSError, ValueError, RuntimeError) as exc:
            sample['sample_error'] = str(exc)
        result['samples'].append(sample)
        print(json.dumps(sample), flush=True)
        time.sleep(5)
    assert 'final' in result, 'native client did not resume verification after restart'
    height = result['final_client']['blocks']
    assert height > result['persisted_start_height'], 'no verified progress after restart'
    urls = {'client':helpers['CLIENT_RPC'], 'seed':helpers['SEED_RPC'], 'reference':result['public_reference']}
    result['hashes'] = {role:rpc(url, 'getblockhash', [height]) for role,url in urls.items()}
    assert len(set(result['hashes'].values())) == 1, 'block hashes disagree'
    result['trees'] = {role:rpc(url, 'z_gettreestate', [str(height)]) for role,url in urls.items()}
    result['tree_matches'] = {}
    for pool in ['sapling', 'orchard', 'ironwood']:
        assert all(pool in tree for tree in result['trees'].values()), f'missing {pool} tree'
        result['tree_matches'][pool] = len({json.dumps(tree[pool],sort_keys=True) for tree in result['trees'].values()}) == 1
    assert all(result['tree_matches'].values()), 'tree states disagree'
    stop(proc)
    proc = None
    console.flush()
    assert 'panicked at' not in (out / 'console.log').read_text(errors='replace')
    statuses = []
    for folder in [Path('/root/out/paired/traces'), out / 'traces']:
        for path in folder.rglob('block_sync*.jsonl'):
            for line in path.open():
                row = json.loads(line)
                if row.get('event') == 'block_status_sent':
                    statuses.append(row)
    assert statuses, 'missing native Status evidence'
    assert all((0 < row['range_start'] <= row['height']) or (row['range_start'] == row['height'] == 0) for row in statuses), 'invalid retained range advertised'
    floors = sorted({row['range_start'] for row in statuses if row['range_start'] > 0})
    assert len(floors) > 1, 'retained range did not advance'
    result['advertised_range'] = {'samples':len(statuses),'first_floor':floors[0],'last_floor':floors[-1]}
    result['pass'] = True
except Exception as exc:
    result['error'] = str(exc)
finally:
    try:
        stop(proc)
    except Exception as exc:
        result['cleanup_error'] = str(exc)
        result['pass'] = False
    console.close()
    (out / 'summary.json').write_text(json.dumps(result,indent=2) + '\n')
print(json.dumps({k:v for k,v in result.items() if k not in ('samples','prior','trees')}),flush=True)
raise SystemExit(0 if result['pass'] else 1)
