"""Capture the exact failed offline CLI read on the disposable downloader."""
import json,subprocess
from pathlib import Path
assert subprocess.check_output(['git','-C','/root/zakura','rev-parse','HEAD'],text=True).strip()=='0e6398c22f0705b9ccf7a071a5229e743dd1572b'
summary=json.loads(Path('/root/out/paired/summary.json').read_text())
assert not summary['pass'] and not summary['phases']
assert not Path('/root/out/paired/downloader.log').exists()
config=Path('/root/out/paired/downloader.toml')
print('DOWNLOADER_CONFIG',config.read_text(),flush=True)
r=subprocess.run(['/usr/local/bin/zakurad','-c',str(config),'tip-height','--cache-dir','/mnt/snapshots/paired-client','--network','Mainnet'],stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,timeout=60)
Path('/root/out/paired/offline-tip-error.log').write_text(r.stdout)
print('OFFLINE_TIP_RESULT',r.returncode,r.stdout,flush=True)
