import importlib.util,json,re,subprocess,time
from pathlib import Path
sha=subprocess.check_output(['git','-C','/root/zakura','rev-parse','HEAD'],text=True).strip()
assert sha in {'a30202f2f1c47e08e9c2f8510392353c37d24dfd','71e93477b2e9cd240c5181e5bf3ef281227967c4'}
original=Path('/root/out/paired')
summary=json.loads((original/'summary.json').read_text())
assert summary.get('error') == '<urlopen error [Errno 111] Connection refused>'
spec=importlib.util.spec_from_file_location('paired','/root/pr-node-paired-smoke.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
m.OUT=Path('/root/out/paired-continuation');m.OUT.mkdir(exist_ok=False)
config=original/'downloader.toml'
import tomllib
state=tomllib.loads(config.read_text())['state']['cache_dir']
tip=subprocess.check_output([m.BINARY,'-c',str(config),'tip-height','--cache-dir',state,'--network','Mainnet'],text=True,stderr=subprocess.STDOUT,timeout=90)
start=int(re.findall(r'^([0-9]+)$',tip,re.MULTILINE)[-1])
result={'sha':sha,'start_height':start,'resumed_after_fixture_rpc_interruption':True,'samples':[]}
console=(original/'downloader-console.log').open('a')
proc=subprocess.Popen([m.BINARY,'-c',str(config),'start'],stdout=console,stderr=console)
deadline=time.monotonic()+300
m.emit('continuation_started',sha=sha,height=start)
try:
 while time.monotonic()<deadline:
  assert proc.poll() is None, 'client exited'
  try:
   height=m.rpc(m.CLIENT_RPC,'getblockcount'); seed=m.rpc(m.SEED_RPC,'getblockcount'); metrics=m.metrics(19999)
   sample={'height':height,'seed_height':seed,'native_bodies':m.metric(metrics,'sync_block_body_received'),'native_requests':m.metric(metrics,'sync_block_request_sent'),'legacy_fallbacks':m.metric(metrics,'sync_zakura_legacy_fallback_engaged'),'vct_fast_blocks':m.metric(metrics,'state_vct_fast_block_count')}
   assert sample['legacy_fallbacks']==0, 'native client used legacy fallback'
   result['samples'].append(sample);m.emit('sample',**sample)
  except (OSError,RuntimeError,ValueError) as exc:m.emit('sample_error',error=str(exc))
  time.sleep(10)
 result['end_height']=m.rpc(m.CLIENT_RPC,'getblockcount')
 result['block_hash']=m.check_hash(result['end_height'])
 (m.OUT/'metrics.txt').write_text(m.metrics(19999))
 result['hash_matches']=True
except Exception as exc:result['error']=str(exc)
finally:
 m.stop(proc);console.close()
 (m.OUT/'summary.json').write_text(json.dumps(result,indent=2)+'\n')
 m.emit('continuation_complete',start_height=start,end_height=result.get('end_height'),error=result.get('error'))
