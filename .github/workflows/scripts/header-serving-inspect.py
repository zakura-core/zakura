import json
import subprocess
import urllib.request
from collections import deque
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
for port in [8232,18232]:
    try:
        req=urllib.request.Request(f'http://127.0.0.1:{port}',data=json.dumps({'jsonrpc':'2.0','id':'inspect','method':'getblockcount','params':[]}).encode(),headers={'Content-Type':'application/json'})
        with urllib.request.urlopen(req,timeout=3) as response:
            emit('height',port=port,response=json.load(response))
    except Exception as e:
        emit('rpc_unavailable',port=port,error=str(e))
for name in ['/root/out/notes.md','/root/out/seed-priming/summary.json','/root/out/paired/summary.json','/root/out/paired/events.jsonl']:
    p=Path(name)
    if p.exists():
        emit('progress',path=name,tail=''.join(deque(p.open(errors='replace'),maxlen=12)))
for name in ['/var/log/zakura/seed-traces/header_sync.jsonl','/root/out/paired/traces/header_sync.jsonl']:
    p=Path(name)
    if not p.exists():
        continue
    refusals=[]
    requests=[]
    snapshots=[]
    repairs=[]
    for line in p.open(errors='replace'):
        try: row=json.loads(line)
        except ValueError: continue
        if row.get('outcome')=='busy': refusals.append(row)
        if row.get('event')=='header_request_sent': requests.append(row)
        if row.get('event')=='header_snapshot_observed': snapshots.append(row)
        if row.get('event')=='header_vct_repair_state': repairs.append(row)
    emit('header_trace',path=name,busy_count=len(refusals),last_busy=refusals[-8:],request_count=len(requests),last_requests=requests[-8:],last_snapshots=snapshots[-4:],last_repairs=repairs[-4:])
p=Path('/var/log/zakura/zakura.log')
if p.exists():
    reasons=[]
    tail=deque(maxlen=6)
    for line in p.open(errors='replace'):
        tail.append(line)
        if 'header serving reproduction:' in line: reasons.append(line)
    emit('storage_refusals',count=len(reasons),last=reasons[-10:],log_tail=list(tail))
