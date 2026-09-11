"""Read progress and traces only on the exact disposable serving test host."""
import json,re,subprocess,urllib.request
from pathlib import Path
from collections import Counter
processes=subprocess.check_output(["ps","-eo","pid,comm,etime,pcpu,rss"],text=True)
print("build_and_node_processes")
for line in processes.splitlines():
    if any(name in line for name in ["cargo", "rustc", "zakurad", "clang", "cc1", "lld", "cp ", "cloud-init", "git", "python3"]):
        print(line)
print("filesystems", subprocess.check_output(["df", "-h"], text=True))
for name in ["/root/out/notes.md","/root/out/seed-priming/summary.json","/root/out/paired/summary.json","/root/out/paired/downloader-console.log","/root/out/paired/downloader.log"]:
    p=Path(name)
    if p.exists():
        if name.endswith('/paired/summary.json'):
            summary=json.loads(p.read_text())
            evidence=summary.get('serving_evidence',{})
            spanning=evidence.get('successful_responses_across_generation_changes',[])
            evidence['spanning_response_count']=len(spanning)
            evidence['successful_responses_across_generation_changes']=spanning[:5]
            print(name,json.dumps(summary))
        else:
            print(name,p.read_text()[-12000:])
p=Path('/var/log/zakura/zakura.log')
if p.exists():
    print('seed_log_tail',subprocess.check_output(['tail','-n','60',str(p)],text=True)[-15000:])
config=Path('/etc/zakura/zakura.toml')
if config.exists():
    import tomllib
    conf=tomllib.loads(config.read_text())
    net=conf.get('network',{})
    print('public_network_config',json.dumps({k:net.get(k) for k in ['p2p_stack','initial_mainnet_peers','peerset_initial_target_size','cache_dir','zakura']}))
for port in [8232,18232]:
    try:
        req=urllib.request.Request(f"http://127.0.0.1:{port}",data=json.dumps({"jsonrpc":"2.0","id":"inspect","method":"getblockcount","params":[]}).encode(),headers={"Content-Type":"application/json"})
        with urllib.request.urlopen(req,timeout=3) as r: print("height",port,json.load(r))
    except Exception as e: print("rpc_not_ready",port,str(e))
p=Path("/var/log/zakura/seed-traces/header_sync.jsonl")
if p.exists():
    rows=[]
    for line in p.open():
        try:rows.append(json.loads(line))
        except ValueError:pass
    if rows:
        process=rows[-1]["process_trace_id"]
        rows=[r for r in rows if r.get("process_trace_id")==process]
        print("server_events",dict(Counter(r["event"] for r in rows)))
        latest=None; spanning=[]
        for row in rows:
            if row["event"]=="header_snapshot_observed":latest=row
            if row["event"]=="header_response_served" and latest and row["header_generation"]<latest["header_generation"]:
                spanning.append({"request":row["request_id"],"request_generation":row["header_generation"],"current_generation":latest["header_generation"],"headers":row["header_count"]})
        print("spanning_responses",len(spanning),spanning[:5])
for filename in ["events.jsonl", "traces/block_sync.jsonl", "traces/commit_state.jsonl", "traces/header_sync.jsonl"]:
    path=Path('/root/out/paired')/filename
    if path.exists():
        print('client_trace_tail',filename,subprocess.check_output(['tail','-n','12',str(path)],text=True))
try:
    with urllib.request.urlopen('http://127.0.0.1:19999/metrics',timeout=5) as r:
        for line in r.read().decode().splitlines():
            if not line.startswith('#') and any(s in line for s in ['state_vct','sync_block','sync_zakura','checkpoint_']):
                print('client_metric',line)
except Exception as e:
    print('client_metrics_unavailable',str(e))
path=Path('/root/out/paired/downloader.log')
if path.exists():
    stages=Counter()
    first_block=[]
    for line in path.open():
        for stage in ['performing block checks','built async tx checks','passed quick checks','got state UTXOs','awaiting async checks','finished async checks','got tx verify result','queueing block for contextual verification']:
            if stage in line:
                height=re.search(r'height=Some\(Height\((\d+)\)\)',line)
                stages[(height.group(1) if height else '?',stage)]+=1
                if height and height.group(1)=='3476011':first_block.append(line.strip()[:1600])
    print('client_verifier_stages',json.dumps([{'height':h,'stage':s,'count':n} for (h,s),n in stages.items()]))
    print('client_first_full_block_progress',json.dumps(first_block[-35:]))
