#!/usr/bin/env python3
"""Continue an already-passing disposable native smoke client for five minutes."""
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
prior = json.loads(Path('/root/out/paired/summary.json').read_text())
assert prior['pass'], 'the preceding sync/restart checks must pass first'
sha = subprocess.check_output(['git', '-C', '/root/zakura', 'rev-parse', 'HEAD'], text=True).strip()
assert sha == helpers['EXPECTED_SHA'] == prior['sha']
config_text = Path('/root/out/paired/downloader.toml').read_text()
config = tomllib.loads(config_text)
assert config['network']['p2p_stack'] == 'zakura'
assert config['consensus']['vct_fast_sync'] is True
try:
    rpc(helpers['CLIENT_RPC'], 'getblockcount')
except OSError:
    pass
else:
    raise RuntimeError('another downloader is already using the test endpoint')

out = Path('/root/out/native-followthrough')
out.mkdir(exist_ok=False)
config_text = config_text.replace('/root/out/paired/', str(out) + '/')
(out / 'downloader.toml').write_text(config_text)
checkpoint = int(Path('/root/zakura/crates/zakura-chain/src/parameters/checkpoint/main-checkpoints.txt').read_text().splitlines()[-1].split()[0])
result = {'sha': sha, 'seed_sha': prior.get('seed_sha', sha), 'pass': False,
          'vct_fast_sync': True, 'p2p_stack': 'zakura', 'required_crossing': checkpoint,
          'prior_verified_height': prior['phases'][-1]['height'], 'samples': []}
proc = None
console = (out / 'console.log').open('w')
started = time.monotonic()
try:
    binary = helpers['BINARY']
    result['binary_sha256'] = hashlib.file_digest(open(binary, 'rb'), 'sha256').hexdigest()
    proc = subprocess.Popen([binary, '-c', str(out / 'downloader.toml'), 'start'], stdout=console, stderr=console)
    while time.monotonic() - started < 300:
        if proc.poll() is not None:
            raise RuntimeError(f'downloader exited unexpectedly: {proc.returncode}')
        sample = {'elapsed_seconds': round(time.monotonic() - started, 1)}
        try:
            sample['height'] = rpc(helpers['CLIENT_RPC'], 'getblockcount')
            sample['seed_height'] = rpc(helpers['SEED_RPC'], 'getblockcount')
            text = metrics(19999)
            sample['native_bodies'] = metric(text, 'sync_block_body_received')
            sample['native_requests'] = metric(text, 'sync_block_request_sent')
            sample['legacy_fallbacks'] = metric(text, 'sync_zakura_legacy_fallback_engaged')
            assert sample['legacy_fallbacks'] == 0, 'native client used a legacy fallback'
            sample['rss_mib'] = int(re.search(r'^VmRSS:\s+(\d+)', Path(f'/proc/{proc.pid}/status').read_text(), re.M)[1]) / 1024
            (out / 'latest-metrics.txt').write_text(text)
        except (OSError, ValueError, RuntimeError) as exc:
            sample['sample_error'] = str(exc)
        result['samples'].append(sample)
        with (out / 'samples.jsonl').open('a') as f:
            f.write(json.dumps(sample) + '\n')
        print(json.dumps(sample), flush=True)
        time.sleep(10)
    valid = [s for s in result['samples'] if 'native_bodies' in s]
    if not valid:
        raise RuntimeError('no successful native samples')
    final = valid[-1]
    result['final'] = final
    result['peak_rss_mib'] = max(s.get('rss_mib', 0) for s in valid)
    assert final['height'] > checkpoint, 'native client did not cross the checkpoint'
    assert final['height'] >= result['prior_verified_height'] + 32, 'no continued verified progress'
    assert final['native_bodies'] >= 32 and final['native_requests'] > 0
    height = final['height']
    seed_hash = rpc(helpers['SEED_RPC'], 'getblockhash', [height])
    client_hash = rpc(helpers['CLIENT_RPC'], 'getblockhash', [height])
    assert seed_hash == client_hash, f'block hash mismatch at {height}'
    result['block_hash'] = client_hash
    stop(proc)
    proc = None
    console.flush()
    assert 'panicked at' not in (out / 'console.log').read_text(errors='replace')
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
    result['elapsed_seconds'] = round(time.monotonic() - started, 1)
    (out / 'summary.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps({k: v for k, v in result.items() if k != 'samples'}), flush=True)
raise SystemExit(0 if result['pass'] else 1)
