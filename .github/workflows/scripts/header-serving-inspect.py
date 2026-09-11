import json
import subprocess
import urllib.request
import tomllib
from collections import deque, Counter
from pathlib import Path


def emit(kind, **fields):
    print(json.dumps({'kind': kind, **fields}), flush=True)


emit('source', sha=subprocess.check_output(['git','-C','/root/zakura','rev-parse','HEAD'],text=True).strip())
for line in subprocess.check_output(['ps','-eo','pid,comm,pcpu,rss,etime'],text=True).splitlines():
    if any(word in line for word in ['cargo','rustc','zakurad',' cp ',' git ',' rsync ','mount','xfs']):
        emit('process', value=line)
        pid=line.split()[0]
        io=Path('/proc')/pid/'io'
        if io.exists(): emit('process_io',pid=pid,values=io.read_text())
cfg=tomllib.loads(Path('/etc/zakura/zakura.toml').read_text())
emit('seed_network_config', fields={k:cfg['network'].get(k) for k in ['initial_mainnet_peers','peerset_initial_target_size','p2p_stack','cache_dir']})
for port in [8232,18232]:
    try:
        req=urllib.request.Request(f'http://127.0.0.1:{port}',data=json.dumps({'jsonrpc':'2.0','id':'inspect','method':'getblockcount','params':[]}).encode(),headers={'Content-Type':'application/json'})
        with urllib.request.urlopen(req,timeout=3) as response:
            emit('height',port=port,response=json.load(response))
    except Exception as e:
        emit('rpc_unavailable',port=port,error=str(e))
for port in [9999,19999]:
    try:
        with urllib.request.urlopen(f'http://127.0.0.1:{port}/metrics',timeout=3) as response:
            metrics=response.read().decode()
        names=['state_vct_fast_block_count','sync_block_body_received','sync_block_request_sent','sync_zakura_legacy_fallback_engaged','pool_num_peers','peer_set','peer_connected','sync_zakura_watchdog']
        emit('metrics',port=port,values=[line for line in metrics.splitlines() if any(line.startswith(name) for name in names)])
    except Exception:
        pass
for name in ['/root/out/notes.md','/root/out/seed-priming/summary.json','/root/out/paired/summary.json','/root/out/paired/events.jsonl','/root/out/paired-continuation/events.jsonl','/root/out/paired-continuation/summary.json']:
    p=Path(name)
    if p.exists():
        emit('progress',path=name,tail=''.join(deque(p.open(errors='replace'),maxlen=12)))
for name in ['/var/log/zakura/seed-traces/header_sync.jsonl','/root/out/paired/traces/header_sync.jsonl']:
    p=Path(name)
    if not p.exists():
        continue
    refusals=[]
    requests=[]
    responses=[]
    snapshots=[]
    repairs=[]
    for line in p.open(errors='replace'):
        try: row=json.loads(line)
        except ValueError: continue
        if row.get('outcome')=='busy': refusals.append(row)
        if row.get('event')=='header_request_sent': requests.append(row)
        if row.get('event')=='header_response_received': responses.append(row)
        if row.get('event')=='header_snapshot_observed': snapshots.append(row)
        if row.get('event')=='header_vct_repair_state': repairs.append(row)
    current_process=requests[-1].get('process_trace_id') if requests else None
    correlated=[]
    for busy in refusals:
        if busy.get('process_trace_id') != current_process or busy.get('event') != 'header_request_terminal': continue
        original=next((r for r in requests if r.get('process_trace_id')==current_process and r.get('session_id')==busy.get('session_id') and r.get('request_id')==busy.get('request_id')),None)
        if not original or original.get('header_count')!=1: continue
        def matches(r):
            return r.get('process_trace_id')==current_process and r.get('session_id')==busy.get('session_id') and r.get('peer')==busy.get('peer') and r.get('target_hash')==busy.get('target_hash') and r['ts']>busy['ts']
        retry=next((r for r in requests if matches(r)),None)
        success=next((r for r in responses if matches(r)),None)
        correlated.append({'original':original,'busy':busy,'retry':retry,'success':success})
    emit('correlated_repair_refusals',path=name,count=len(correlated),first=correlated[:5])
    emit('first_busy',path=name,rows=refusals[:6])
    emit('header_trace',path=name,busy_count=len(refusals),last_busy=refusals[-8:],request_count=len(requests),last_requests=requests[-8:],response_count=len(responses),last_responses=responses[-8:],last_snapshots=snapshots[-4:],last_repairs=repairs[-4:])
p=Path('/var/log/zakura/zakura.log')
if p.exists():
    reasons=[]
    connection_failures=[]
    handshake_errors=Counter()
    tail=deque(maxlen=6)
    for line in p.open(errors='replace'):
        tail.append(line)
        if 'header serving reproduction:' in line: reasons.append(line)
        if 'error=' in line and ('handshake' in line.lower() or 'outbound connection' in line): handshake_errors[line.split('error=',1)[1].strip()] += 1
        if 'ZakuradConfig' not in line and ('failed to make outbound connection' in line or 'handshake' in line.lower()): connection_failures.append(line)
    emit('storage_refusals',count=len(reasons),last=reasons[-10:],handshake_errors=handshake_errors.most_common(12),connection_failures=connection_failures[-12:],log_tail=list(tail))
