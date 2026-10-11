"""Require a fresh comparison and healthy dashboard row after probe replacement."""
import json
from pathlib import Path
import sys
import time
import urllib.request

try:
    sample = json.loads(Path('/var/lib/zakura-mac-cranelift-public/status.json').read_text())
    stamp = sample['sample_time']
    assert type(stamp) in (int, float) and float(sys.argv[1]) <= stamp <= time.time()
    assert time.time() - stamp <= 90 and sample['condition'] == 'matching'
    assert '10' in sample['ancestor_hashes']
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open('http://127.0.0.1:8090/data', timeout=10) as response:
        rows = json.load(response)['rows']
    print(json.dumps(any(row['name'] == 'mac-os-cranelift' and row.get('height')
                         and row.get('healthy') for row in rows)))
except Exception:
    print('false')
