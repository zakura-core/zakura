"""Keep an externally stimulated diagnostic distinct from a clean smoke pass."""
import json,shutil,time
from pathlib import Path
root=Path('/root/out/paired')
probe=json.loads((root/'handoff-probe.json').read_text())
assert probe['sha']=='0e6398c22f0705b9ccf7a071a5229e743dd1572b'
assert probe['test_only_intervention'] and probe['submitblock_response']=='duplicate'
path=root/'summary.json'
summary=json.loads(path.read_text())
copy=root/'summary-before-diagnostic-classification.json'
assert not copy.exists(), 'classify this run only once'
shutil.copyfile(path,copy)
summary['diagnostic_intervention']=probe
summary['pass']=False
summary['error']='Checkpoint handoff required a test-only duplicate-block stimulus; this is a diagnostic recovery, not an uninterrupted smoke pass'
path.write_text(json.dumps(summary,indent=2)+'\n')
with (root/'events.jsonl').open('a') as f:f.write(json.dumps({'event':'diagnostic_intervention_classified','unix_time':time.time(),'passed':False})+'\n')
print(json.dumps({'pass':False,'phases':summary.get('phases'),'error':summary['error']}))
