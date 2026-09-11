"""Preserve this test build and correct only its disposable native bind address."""
import hashlib,json,subprocess,time
from pathlib import Path
expected='0e6398c22f0705b9ccf7a071a5229e743dd1572b'
assert subprocess.check_output(['git','-C','/root/zakura','rev-parse','HEAD'],text=True).strip()==expected
summary=json.loads(Path('/root/out/paired/summary.json').read_text())
assert summary['pass'] is False and not summary['phases']
assert not Path('/root/out/paired/downloader.toml').exists()
binary=Path('/usr/local/bin/zakurad')
out=Path('/root/out/tested-binary');out.mkdir()
subprocess.run(['zstd','-T2','-3','-q',str(binary),'-o',str(out/'zakurad.zst')],check=True,timeout=45)
metadata={'sha':expected,'binary_sha256':hashlib.sha256(binary.read_bytes()).hexdigest(),'source_run':34653015599}
(out/'metadata.json').write_text(json.dumps(metadata,indent=2)+'\n')
print('PRESERVED_BINARY',json.dumps(metadata),flush=True)
config=Path('/etc/zakura/zakura.toml');text=config.read_text()
assert text.count('listen_addr = "127.0.0.1:8234"')==1
(out/'original-network-config.toml').write_text(text)
subprocess.run(['systemctl','stop','zakurad'],check=True,timeout=90)
config.write_text(text.replace('listen_addr = "127.0.0.1:8234"','listen_addr = "0.0.0.0:8234"'))
subprocess.run(['systemctl','start','zakurad'],check=True,timeout=90)
print('CORRECTED_NATIVE_BIND','0.0.0.0:8234',flush=True)
