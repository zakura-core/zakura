"""Probe the exact disposable B node with an already supplied mainnet block."""
import json,re,subprocess,time,urllib.request
from pathlib import Path
EXPECTED_SHA="0e6398c22f0705b9ccf7a071a5229e743dd1572b"
CHECKPOINT=3476010
FIRST_FULL_HASH="00000000001aeaaf7ec2c67bda8efa3065662a0a9e06831bca876557c9a77b2e"
def rpc(port,method,params=None):
    req=urllib.request.Request(f"http://127.0.0.1:{port}",data=json.dumps({"jsonrpc":"2.0","id":"handoff-probe","method":method,"params":params or []}).encode(),headers={"Content-Type":"application/json"})
    with urllib.request.urlopen(req,timeout=15) as res: data=json.load(res)
    if data.get('error'):raise RuntimeError(str(data['error']))
    return data['result']
def metric_snapshot():
    with urllib.request.urlopen('http://127.0.0.1:19999/metrics',timeout=5) as r:
        return [line for line in r.read().decode().splitlines() if line.startswith('state_requests{') or line.startswith('sync_zakura_apply_operations ')]
sha=subprocess.check_output(['git','-C','/root/zakura','rev-parse','HEAD'],text=True).strip()
assert sha==EXPECTED_SHA
out=Path('/root/out/paired/handoff-probe.json')
assert not out.exists(), 'a second probe must not be sent'
result={'sha':sha,'started_at_unix':time.time(),'test_only_intervention':True,'before_height':rpc(18232,'getblockcount'),'before_metrics':metric_snapshot(),'samples':[]}
assert result['before_height']==CHECKPOINT, 'B must still be stalled at the checkpoint'
assert any('type="commit_semantically_verified_block"} 20' in l for l in result['before_metrics']), 'all first 20 full blocks must have reached the writer'
assert rpc(8232,'getblockhash',[CHECKPOINT+1])==FIRST_FULL_HASH
block_hex=rpc(8232,'getblock',[FIRST_FULL_HASH,0])
assert isinstance(block_hex,str) and re.fullmatch('[0-9a-fA-F]+',block_hex)
result['stimulus_height']=CHECKPOINT+1
result['stimulus_hash']=FIRST_FULL_HASH
result['stimulus_at_unix']=time.time()
out.write_text(json.dumps(result,indent=2)+'\n')
try:result['submitblock_response']=rpc(18232,'submitblock',[block_hex])
except Exception as e:result['submitblock_error']=str(e)
for _ in range(30):
    sample={'unix_time':time.time(),'height':rpc(18232,'getblockcount')}
    result['samples'].append(sample)
    if sample['height']>=CHECKPOINT+20:break
    time.sleep(1)
result['after_metrics']=metric_snapshot()
result['finished_at_unix']=time.time()
out.write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result,indent=2))
