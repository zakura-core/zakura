"""Retest the exact disposable candidate using compatible public peers."""
import json
import subprocess
from pathlib import Path

expected = "ab4a68521f1a3161537c1f3003c1bc2cce1f05ef"
actual = subprocess.check_output(["git", "-C", "/root/zakura", "rev-parse", "HEAD"], text=True).strip()
assert actual == expected
assert json.loads(Path('/root/out/paired/summary.json').read_text())['pass'] is False
assert not Path('/root/out/paired-public').exists()
config = Path('/etc/zakura/zakura.toml')
text = config.read_text()
old = 'dev_network = "header-serving-stability-20260911"'
assert text.count(old) == 1
assert 'p2p_stack = "dual"' in text
subprocess.run(['systemctl','stop','zakurad'],check=True,timeout=90)
config.write_text(text.replace(old, '# Public mainnet cohort for this wire-compatible candidate.'))
with Path('/root/out/notes.md').open('a') as notes:
    notes.write('- The isolated cohort could serve B but rejected public native peers during upgrade, so A stayed idle. Preserved that failed concurrency capture. Retested in the public cohort with B limited to one connection and gated on 100 new blocks at A.\n')
subprocess.run(['systemctl','start','zakurad'],check=True,timeout=90)
result = subprocess.run(['python3','-u','/root/pr-node-paired-public.py','--state','/mnt/snapshots/paired-client','--duration-minutes','6'],timeout=410)
path = Path('/root/out/paired-public/summary.json')
if path.exists():
    summary=json.loads(path.read_text())
    evidence=summary.get('serving_evidence',{})
    spanning=evidence.get('successful_responses_across_generation_changes',[])
    evidence['spanning_response_count']=len(spanning)
    evidence['successful_responses_across_generation_changes']=spanning[:5]
    print('PUBLIC_RETEST_SUMMARY',json.dumps(summary),flush=True)
raise SystemExit(result.returncode)
