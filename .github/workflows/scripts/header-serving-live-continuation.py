"""Finish restart validation after the client crossed its final VCT checkpoint."""
import importlib.util
import json
import re
import subprocess
import time
from pathlib import Path

spec=importlib.util.spec_from_file_location('paired','/root/pr-node-paired-public.py')
test=importlib.util.module_from_spec(spec)
spec.loader.exec_module(test)
root=test.OUT
old=json.loads((root/'summary.json').read_text())
assert old['error']=='the downloader did not exercise VCT fast verification'
actual=subprocess.check_output(['git','-C','/root/zakura','rev-parse','HEAD'],text=True).strip()
assert actual==test.EXPECTED_SHA
events=[json.loads(line) for line in (root/'events.jsonl').read_text().splitlines()]
first=next(row for row in events if row['event']=='phase_pass')
checkpoint=int(Path('/root/zakura/crates/zakura-chain/src/parameters/checkpoint/main-checkpoints.txt').read_text().splitlines()[-1].split()[0])
required_fast=min(64,max(0,checkpoint-first['start_height']))
assert required_fast>0 and first['vct_fast_blocks']>=required_fast
assert first['height']>checkpoint and first['legacy_fallbacks']==0
result={**old,'phases':[first],'checkpoint':checkpoint,'required_vct_fast_blocks':required_fast,'pass':False}
result.pop('error')
proc=None
console=(root/'downloader-console.log').open('a')
try:
    config=root/'downloader.toml'
    initial=subprocess.check_output([test.BINARY,'-c',str(config),'tip-height','--cache-dir','/mnt/snapshots/paired-client','--network','Mainnet'],text=True,stderr=subprocess.STDOUT,timeout=90)
    start=int(re.findall(r'^([0-9]+)$',initial,re.MULTILINE)[-1])
    test.emit('restarting_after_checkpoint',persisted_height=start)
    proc=subprocess.Popen([test.BINARY,'-c',str(config),'start'],stdout=console,stderr=console)
    second=test.run_phase(proc,'restart-sync',start,64,time.monotonic()+140)
    assert test.check_hash(first['height'])==first['block_hash']
    result['phases'].append(second)
    server=[json.loads(line) for line in Path('/var/log/zakura/seed-traces/header_sync.jsonl').read_text().splitlines()]
    process=server[-1]['process_trace_id']
    server=[row for row in server if row['process_trace_id']==process]
    client=[json.loads(line) for line in (root/'traces/header_sync.jsonl').read_text().splitlines()]
    peers={row['peer'] for row in client if row['event']=='header_peer_connected'}
    assert len(peers)==1, peers
    received={(row['request_id'],row['target_hash']) for row in client if row['event']=='header_response_received'}
    latest=None; spanning=[]; updates=0
    for row in server:
        if row['event']=='header_snapshot_observed': latest=row; updates+=1
        if row['event']=='header_response_served' and latest and row['header_generation']<latest['header_generation'] and (row['request_id'],row['target_hash']) in received:
            spanning.append({'request_id':row['request_id'],'target_hash':row['target_hash'],'header_count':row['header_count'],'request_generation':row['header_generation'],'current_generation':latest['header_generation'],'head_height':latest['new_selected_height']})
    assert updates>=100 and spanning
    result['serving_evidence']={'snapshot_updates':updates,'spanning_responses_received_by_client':len(spanning),'examples':spanning[:5],'client_peer_count':len(peers),'busy_replies':sum(row['event']=='header_outcome' and row.get('outcome')=='busy' for row in server)}
    test.stop(proc); proc=None
    result['pass']=True
except Exception as exc:
    result['error']=str(exc)
finally:
    try: test.stop(proc)
    except Exception as exc: result['cleanup_error']=str(exc); result['pass']=False
    console.close()
    (root/'validation-summary.json').write_text(json.dumps(result,indent=2)+'\n')
    print('VALIDATION_SUMMARY',json.dumps(result),flush=True)
raise SystemExit(0 if result['pass'] else 1)
