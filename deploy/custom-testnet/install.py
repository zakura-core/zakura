#!/usr/bin/env python3
"""Install an already built custom Testnet deployment; never start a new run."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess

import network


def install(config):
    if os.geteuid() != 0:
        raise PermissionError('installation needs root to install system services')
    root = Path(config['root'])
    root.mkdir(parents=True, exist_ok=True)
    owner = {'service_prefix': config['service_prefix'], 'root': str(root)}
    marker = root / 'owner.json'
    if marker.exists() and json.loads(marker.read_text()) != owner:
        raise ValueError('refusing to adopt another deployment directory')
    if not marker.exists() and any(root.iterdir()):
        raise ValueError('new deployment directory must be empty')
    network.verify_archive(config)
    with Path(config['binary']).open('rb') as binary:
        actual = hashlib.file_digest(binary, 'sha256').hexdigest()
    if actual != config['binary_sha256']:
        raise ValueError('binary checksum does not match deployment provenance')
    network.atomic_json(marker, owner)
    directory = Path('/opt/zakura-custom-testnet')
    directory.mkdir(exist_ok=True)
    for name in ('network.py', 'archive_seed.py', 'dashboard.html'):
        source = Path(__file__).with_name(name)
        if source.resolve() != (directory / name).resolve():
            shutil.copy2(source, directory / name)
    user = config.get('node_user', 'zakura-testnet')
    config['node_user'] = user
    import pwd
    try:
        pwd.getpwnam(user)
    except KeyError:
        network.command('useradd', '--system', '--create-home', '--home-dir', '/var/lib/zakura-testnet', '--shell', '/usr/sbin/nologin', user)
    Path('/usr/local/bin/zakura-testnet').write_text('#!/bin/sh\nexec python3 /opt/zakura-custom-testnet/network.py "$@"\n')
    Path('/usr/local/bin/zakura-testnet').chmod(0o755)
    network.atomic_json(Path('/etc/zakura-custom-testnet.json'), config)
    prefix = config['service_prefix']
    Path(f'/etc/systemd/system/{prefix}-preparation.service').write_text(f'''[Unit]
Description=Prepare an archive seed from public Testnet
After=network-online.target
RequiresMountsFor={root}
[Service]
User={user}
ExecStart={config['binary']} -c {root}/preparation.toml start
TimeoutStopSec=90
LimitNOFILE=65536
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths={root}/preparation /var/lib/zakura-testnet
''')
    Path(f'/etc/systemd/system/{prefix}-reset.service').write_text(f'''[Unit]
Description=Refresh archive state and regenerate the custom Testnet
After=network-online.target
RequiresMountsFor={root}
[Service]
Type=oneshot
ExecStart=/usr/local/bin/zakura-testnet reset --activation-delay 10m
TimeoutStartSec=3600
TimeoutStopSec=120
''')
    for index in range(2):
        unit = f'''[Unit]
Description=Zakura custom Testnet validator {index}
After=network-online.target
RequiresMountsFor={root}
[Service]
User={user}
ExecStartPre=/usr/local/bin/zakura-testnet check-node --index {index}
ExecStart={config['binary']} -c {root}/node{index}.toml start
Restart=on-failure
RestartSec=3
TimeoutStopSec=90
LimitNOFILE=65536
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths={root}/node{index} /var/lib/zakura-testnet
[Install]
WantedBy=multi-user.target
'''
        Path(f'/etc/systemd/system/{prefix}-node{index}.service').write_text(unit)
    Path(f'/etc/systemd/system/{prefix}-controller.service').write_text(f'''[Unit]
Description=Zakura custom Testnet countdown and public status
After=network-online.target
RequiresMountsFor={root}
[Service]
ExecStart=/usr/local/bin/zakura-testnet serve
Restart=on-failure
RestartSec=3
TimeoutStopSec=130
[Install]
WantedBy=multi-user.target
''')
    domain = config['public_peer']
    Path('/etc/caddy/Caddyfile').write_text(f'''{domain} {{
    encode gzip zstd
    handle /snapshots/seed.tar.zst {{
        reverse_proxy 127.0.0.1:8093
    }}
    handle_path /snapshots/* {{
        root * {Path(config['seed_archive']).parent}
        file_server
    }}
    handle {{
        reverse_proxy 127.0.0.1:8093
    }}
}}
''')
    network.command('caddy', 'validate', '--config', '/etc/caddy/Caddyfile')
    network.command('systemctl', 'daemon-reload')
    network.command('systemctl', 'enable', f'{prefix}-controller.service', f'{prefix}-node0.service', f'{prefix}-node1.service', 'caddy.service')
    network.command('systemctl', 'restart', 'caddy.service')
    print(f'Installed. Start a fresh run with: systemctl start --no-block {prefix}-reset.service')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True, type=Path)
    args = parser.parse_args(argv)
    install(network.load_config(args.config))


if __name__ == '__main__':
    main()
