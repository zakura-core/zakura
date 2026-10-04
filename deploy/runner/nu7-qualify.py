"""Read-only readiness receipt; does not acquire or mutate selector state."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import sys
import time
sys.path.insert(0, '/opt/zakura-nu7-status')
from nu7_activation import ActivationFeed, rpc

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--expected-revision', help='approved candidate SHA to qualify before updating the runtime manifest')
args = parser.parse_args()
config = json.loads(Path('/etc/zakura/nu7-activation.json').read_text())
if args.expected_revision:
    if not re.fullmatch(r'[0-9a-f]{40}', args.expected_revision):
        parser.error('--expected-revision requires a full source SHA')
    config['public']['manifest']['nodeRevision'] = args.expected_revision
feed = object.__new__(ActivationFeed)
feed.config = config
feed.rpc = rpc
feed.clock = time.time
feed.progress = {}
reference = feed.observe(config['public']['reference'], time.time(), False)
nodes = [feed.observe(node, time.time()) for node in config['public']['nodes']]
receipt = {'checkedAt': time.time(), 'armed': config['armed'], 'dispatch': config['dispatch'],
           'referenceIdentityVerified': reference.get('fresh', False),
           'referenceHeight': reference.get('height'), 'referenceNu7Active': reference.get('active'),
           'nodeSources': [{k: n[k] for k in ('name', 'fresh', 'height', 'error', 'sourceRevision', 'buildVersion') if k in n} for n in nodes],
           'sourceRevision': config['public']['manifest']['nodeRevision']}
reference_source = config['public']['reference']
receipt['referenceNu7ScheduleVerified'] = bool(reference.get('fresh') and reference_source.get('adapter') != 'grpcurl')
receipt['referenceTransport'] = reference_source.get('adapter', 'json-rpc')
if reference_source.get('adapter') == 'grpcurl':
    receipt['grpcurlSha256'] = hashlib.sha256(Path(reference_source['grpcurlPath']).read_bytes()).hexdigest()
elif reference.get('fresh'):
    reference_info = rpc(reference_source['rpcUrl'], 'getinfo')
    receipt['referenceBuildVersion'] = reference_info.get('build', reference_info.get('subversion'))
    receipt['referenceProtocolVersion'] = reference_info.get('protocolversion')
    receipt['referenceNu7ScheduleVerified'] = bool(
        receipt['referenceNu7ScheduleVerified']
        and re.search(r'(?:^v|/Zebra:)7\.', str(receipt['referenceBuildVersion']))
        and isinstance(receipt['referenceProtocolVersion'], int)
        and receipt['referenceProtocolVersion'] >= 170180)
receipt['preparedChainAgreement'] = False
if reference.get('fresh') and all(n.get('fresh') for n in nodes):
    sources = nodes + [reference]
    common = min(n['height'] for n in sources)
    hashes = [feed.source_hash(n, common) for n in sources]
    receipt.update(commonHeight=common, commonHash=hashes[0], preparedChainAgreement=len(set(hashes)) == 1)
receipt['activationReferencePrepared'] = bool(receipt['preparedChainAgreement'] and receipt['referenceNu7ScheduleVerified'])
receipt['prepared'] = receipt['preparedChainAgreement']
print(json.dumps(receipt, indent=2))
sys.exit(0 if receipt['prepared'] else 1)
