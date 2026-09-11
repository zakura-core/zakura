"""Read progress and traces only on the exact disposable serving test host."""
import json,subprocess,urllib.request
from pathlib import Path
from collections import Counter
processes=subprocess.check_output(["ps","-eo","pid,comm,etime,pcpu,rss"],text=True)
print("build_and_node_processes")
for line in processes.splitlines():
    if any(name in line for name in ["cargo", "rustc", "zakurad", "clang", "cc1", "lld"]):
        print(line)
for name in ["/root/out/notes.md","/root/out/seed-priming/summary.json","/root/out/paired/summary.json"]:
    p=Path(name)
    if p.exists(): print(name, p.read_text()[-12000:])
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
