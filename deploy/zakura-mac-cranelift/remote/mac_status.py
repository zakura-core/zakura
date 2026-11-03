"""Inspect the installed native node and compiler receipt."""
import hashlib, json, pathlib, platform, subprocess, sys, tomllib
base = pathlib.Path(sys.argv[1])
result = {'architecture': platform.machine(), 'receipt_present': (base / 'receipt.json').is_file()}
receipt = json.loads((base / 'receipt.json').read_text())
with (base / 'bin/zakurad').open('rb') as stream:
    actual = hashlib.file_digest(stream, 'sha256').hexdigest()
result['binary_matches_receipt'] = actual == receipt.get('binary_sha256')
result['binary_sha256'] = actual
result['receipt_fields'] = sorted(receipt)
compiler = receipt.get('compiler')
result['compiler_metadata'] = compiler
config = tomllib.loads((base / 'zakurad.toml').read_text())
result['full_verification_enabled'] = (config.get('consensus', {}).get('checkpoint_sync') is False
    and config.get('consensus', {}).get('vct_fast_sync') is False)
result['compiler_evidence_files'] = sorted(str(p.relative_to(base / 'evidence')) for p in (base / 'evidence').rglob('*.json'))
result['compiler_acceptance'] = []
for path in (base / 'evidence').rglob('*.json'):
    evidence = json.loads(path.read_text())
    if evidence.get('binary_sha256') == actual:
        result['compiler_acceptance'].append({key: evidence.get(key) for key in
            ['passed', 'source_sha', 'patch_sha256', 'binary_architecture', 'configuration', 'checks']})
check = subprocess.run(['launchctl', 'print', 'system/dev.valargroup.zakura-verifier-node'],
                       capture_output=True, text=True, timeout=10)
result['node_running'] = check.returncode == 0 and 'state = running' in check.stdout
print(json.dumps(result))
