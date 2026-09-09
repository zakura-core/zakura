#!/usr/bin/env python3
"""Configure one newly provisioned genesis comparison host, never a fleet node."""
import dataclasses
import importlib.util
import json
from pathlib import Path
import socket
import subprocess
import sys
import tomllib

ROOT = Path(__file__).resolve().parent
OWNER = Path('/root/genesis-candidate/owner.json')


def load_controller():
    spec = importlib.util.spec_from_file_location('candidate_controller', ROOT / 'continuous-sync.py')
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def configure(owner):
    expected = f"zakura-genesis-{owner['launch_run_id']}-{owner['leg']}"
    if owner['leg'] not in ('baseline', 'candidate') or socket.gethostname() != expected:
        raise ValueError('host does not match the isolated experiment identity')
    sync = load_controller()
    with (ROOT / 'nodes.toml').open('rb') as f:
        inventory = tomllib.load(f)
    raw = inventory['defaults'] | next(n for n in inventory['nodes'] if n['p2p_stack'] == owner['mode'])
    paths = dict(repo_dir='/root/zakura', state_dir='/var/lib/zakura-continuous-sync',
                 runs_dir='/var/log/zakura/runs', chain_state_dir='/var/lib/zakura',
                 wipe_sentinel='/var/lib/zakura/.continuous-sync-wipe-ok',
                 build_cache_dir='/var/lib/zakura-continuous-sync/build-cache',
                 config_template='/etc/zakura/template.toml', zakurad_config='/etc/zakura/node.toml',
                 bin_path='/usr/local/bin/zakurad', log_file='/var/log/zakura/zebrad.log',
                 monitor_log='/var/log/zakura/monitor.log', trace_link='/var/log/zakura/traces')
    policy = dataclasses.asdict(sync.Policy())
    policy.update({k: v for k, v in raw.items() if k in policy})
    policy.update(pinned_sha=owner['source_sha'], hostname=expected, alias=expected,
                  public_ip=owner['ip'], ssh_string='root@' + owner['ip'],
                  mode_label=owner['mode'], p2p_stack=owner['mode'], max_run_seconds=20*3600)
    for path in ('/etc/zakura', paths['state_dir'], paths['runs_dir'], paths['chain_state_dir']):
        Path(path).mkdir(parents=True, exist_ok=True)
    Path(paths['wipe_sentinel']).touch(exist_ok=False)
    config = ''.join('[' + section + ']\n' + ''.join(k + ' = ' + json.dumps(v) + '\n' for k, v in values.items())
                     for section, values in [('paths', paths), ('policy', policy)])
    (ROOT / 'controller.toml').write_text(config)
    template = (ROOT / 'templates/zakurad.toml.template').read_text()
    template = template.replace('{{PUBLIC_IP}}', owner['ip']).replace('{{HEALTH_MIN_CONNECTED_PEERS}}', str(raw['health_min_connected_peers']))
    Path(paths['config_template']).write_text(template)
    service = (ROOT / 'templates/zakura.service').read_text()
    for key, val in {'BIN_PATH': paths['bin_path'], 'CONFIG_PATH': paths['zakurad_config'], 'REPO_DIR': paths['repo_dir']}.items():
        service = service.replace('{{' + key + '}}', val)
    Path('/etc/systemd/system/zakura.service').write_text(service)
    subprocess.run(['systemctl', 'daemon-reload'], check=True)
    return sync.load_config(ROOT / 'controller.toml')


def main():
    owner = json.loads(OWNER.read_text())
    configure(owner)
    environment = {
        'owner': owner, 'kernel': subprocess.check_output(['uname', '-a'], text=True).strip(),
        'cpu': subprocess.check_output(['lscpu'], text=True),
        'rustc': subprocess.check_output(['rustc', '--version'], text=True).strip(),
        'packages': subprocess.check_output(['dpkg-query', '-W', '-f=${Package}\t${Version}\n'], text=True),
    }
    (ROOT / 'environment.json').write_text(json.dumps(environment, indent=2) + '\n')


if __name__ == '__main__':
    main()
