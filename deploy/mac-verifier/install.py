#!/usr/bin/env python3
"""Install only POC-owned files/services; preserve existing mainnet services."""
import argparse
import grp
import json
import os
from pathlib import Path
import plistlib
import pwd
import re
import shutil
import subprocess
import sys
import tarfile

from common import atomic_json, read_json

BASE = Path("/Library/Application Support/ZakuraVerifier")
PACKAGE = Path(__file__).resolve().parent
MAC_USER = "_zakuraverifier"
LINUX_USER = "zakura-mac-verifier"
TUNNEL_USER = "zakura-mac-tunnel"
LINUX_HOME = Path("/var/lib/zakura-mac-verifier")
LINUX_CODE = Path("/opt/zakura-mac-verifier")
ETC = Path("/etc/zakura-mac-verifier")
LABELS = ("dev.valargroup.zakura-verifier-node", "dev.valargroup.zakura-verifier-adapter",
          "dev.valargroup.zakura-verifier-tunnel")


def call(*args):
    subprocess.run(list(args), check=True, timeout=60)


def write(path, text, mode=0o644):
    path = Path(path)
    if path.is_symlink():
        raise ValueError("refusing to replace symlink")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    path.chmod(mode)


def copy_package(target):
    target.mkdir(parents=True, exist_ok=True)
    for source in PACKAGE.glob("*.py"):
        shutil.copyfile(source, target / source.name)
        (target / source.name).chmod(0o644)


def mac_account():
    try:
        return pwd.getpwnam(MAC_USER)
    except KeyError:
        used = {entry.pw_uid for entry in pwd.getpwall()}
        uid = next(uid for uid in range(400, 500) if uid not in used)
        record = "/Users/" + MAC_USER
        for field, value in [("UniqueID", str(uid)), ("PrimaryGroupID", "20"),
                             ("UserShell", "/usr/bin/false"), ("NFSHomeDirectory", str(BASE / "home")),
                             ("IsHidden", "1"), ("RealName", "Zakura verifier service")]:
            call("dscl", ".", "-create", record, field, value)
        return pwd.getpwnam(MAC_USER)


def install_mac(args):
    if sys.platform != "darwin":
        raise ValueError("Mac installer requires macOS")
    account = mac_account()
    BASE.mkdir(parents=True, exist_ok=True)
    BASE.chmod(0o755)
    for directory in ("bin", "code"):
        (BASE / directory).mkdir(exist_ok=True)
    for directory in ("home", "run", "logs", "evidence", "peers", "ssh"):
        path = BASE / directory
        path.mkdir(exist_ok=True)
        os.chown(path, account.pw_uid, account.pw_gid)
        path.chmod(0o700)
    # Re-running preparation does not replace a running binary or state.
    binary = BASE / "bin/zakurad"
    if not binary.exists():
        if not args.binary:
            raise ValueError("initial preparation requires --binary")
        shutil.copyfile(args.binary, binary)
        binary.chmod(0o755)
    copy_package(BASE / "code")
    config = BASE / "zakurad.toml"
    expected = (PACKAGE / "templates/zakurad.toml").read_text()
    if config.exists() and config.read_text() != expected:
        raise ValueError("existing verifier configuration differs: explicit rollback/rebootstrap required")
    write(config, expected)
    write(BASE / "code/node.sh", '#!/bin/sh\nset -eu\n'
          'echo $$ > "/Library/Application Support/ZakuraVerifier/run/node.pid"\n'
          'exec "/Library/Application Support/ZakuraVerifier/bin/zakurad" '
          '-c "/Library/Application Support/ZakuraVerifier/zakurad.toml" start\n', 0o755)
    key = BASE / "ssh/tunnel"
    if not key.exists():
        call("ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(key))
        # This newly created workload key must be vaulted before activation.
        print("Tunnel key generated; store it in Infisical before activation.")
    for path in (key, Path(str(key) + ".pub")):
        os.chown(path, account.pw_uid, account.pw_gid)
        path.chmod(0o600)
    if not args.known_hosts:
        raise ValueError("--known-hosts must contain authenticated DO host keys")
    known_hosts = BASE / "ssh/known_hosts"
    shutil.copyfile(args.known_hosts, known_hosts)
    os.chown(known_hosts, account.pw_uid, account.pw_gid)
    known_hosts.chmod(0o600)
    python = str(Path(sys.executable).resolve())
    commands = [
        [str(BASE / "code/node.sh")],
        [python, str(BASE / "code/adapter.py"), "--base", str(BASE)],
        ["/usr/bin/ssh", "-NT", "-i", str(key), "-o", "IdentitiesOnly=yes",
         "-o", "BatchMode=yes", "-o", "ExitOnForwardFailure=yes", "-o", "ConnectTimeout=10",
         "-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=3",
         "-o", "StrictHostKeyChecking=yes", "-o", "UserKnownHostsFile=" + str(known_hosts),
         "-R", "127.0.0.1:28233:127.0.0.1:28233", TUNNEL_USER + "@159.65.183.89"],
    ]
    for label, command in zip(LABELS, commands):
        job = {"Label": label, "ProgramArguments": command, "UserName": MAC_USER,
               "RunAtLoad": True, "KeepAlive": True, "ThrottleInterval": 10,
               "WorkingDirectory": str(BASE),
               "EnvironmentVariables": {"HOME": str(BASE / "home"), "PYTHONDONTWRITEBYTECODE": "1"},
               "StandardOutPath": str(BASE / "logs" / (label + ".out.log")),
               "StandardErrorPath": str(BASE / "logs" / (label + ".err.log"))}
        path = Path("/Library/LaunchDaemons") / (label + ".plist")
        path.write_bytes(plistlib.dumps(job))
        path.chmod(0o644)
    rotation = '\n'.join(f'"{BASE}/logs/{label}.{suffix}.log" {MAC_USER}:staff 600 4 10240 * J'
                         for label in LABELS for suffix in ("out", "err"))
    write("/etc/newsyslog.d/zakura-verifier.conf", rotation + "\n")
    call("pmset", "-a", "sleep", "0", "autorestart", "1")
    print("Prepared Mac services; bootstrap and anchor before activation.")


def stop_mac():
    for label in LABELS:
        subprocess.run(["launchctl", "bootout", "system/" + label],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=60)


def linux_account(name, home):
    try:
        return pwd.getpwnam(name)
    except KeyError:
        call("useradd", "--system", "--home-dir", str(home), "--shell", "/usr/sbin/nologin", name)
        return pwd.getpwnam(name)


def install_linux(args):
    if sys.platform != "linux":
        raise ValueError("Linux installer requires Linux")
    linux_account(LINUX_USER, LINUX_HOME)
    linux_account(TUNNEL_USER, ETC / "tunnel")
    ETC.mkdir(parents=True, exist_ok=True)
    ETC.chmod(0o755)
    copy_package(LINUX_CODE)
    if not args.tunnel_public_key or not args.receipt:
        raise ValueError("Linux installation requires --tunnel-public-key and --receipt")
    key = Path(args.tunnel_public_key).read_text().strip()
    if not re.fullmatch(r"ssh-ed25519 [A-Za-z0-9+/=]+(?: [^\n]*)?", key):
        raise ValueError("invalid tunnel public key")
    write(ETC / "tunnel/authorized_keys", 'restrict,port-forwarding,permitlisten="127.0.0.1:28233" ' + key + "\n")
    match = f'''Match User {TUNNEL_USER}
    AuthorizedKeysFile {ETC}/tunnel/authorized_keys
    AllowTcpForwarding remote
    PermitListen 127.0.0.1:28233
    GatewayPorts no
    MaxSessions 0
    AllowAgentForwarding no
    X11Forwarding no
    PermitTTY no
    PasswordAuthentication no
    KbdInteractiveAuthentication no
Match all
'''
    write("/etc/ssh/sshd_config.d/70-zakura-mac-verifier.conf", match)
    try:
        call("sshd", "-t")
    except Exception:
        Path("/etc/ssh/sshd_config.d/70-zakura-mac-verifier.conf").unlink()
        raise
    call("systemctl", "reload", "ssh")
    shutil.copyfile(args.receipt, ETC / "receipt.json")
    (ETC / "receipt.json").chmod(0o644)
    if not args.infisical_project:
        raise ValueError("--infisical-project is required")
    atomic_json(ETC / "infisical.json", {"project_id": args.infisical_project})
    (ETC / "infisical.json").chmod(0o644)
    write("/etc/systemd/system/zakura-mac-verifier.service", f'''[Unit]
Description=Native Mac mainnet verifier comparison
After=network-online.target
Wants=network-online.target
StartLimitIntervalSec=0

[Service]
User={LINUX_USER}
Group={LINUX_USER}
StateDirectory={LINUX_USER}
StateDirectoryMode=0700
Environment=PYTHONDONTWRITEBYTECODE=1
LoadCredential=infisical-identity:{ETC}/identity.json
ExecStart=/usr/bin/python3 {LINUX_CODE}/secrets_runner.py
Restart=always
RestartSec=15
TimeoutStopSec=30
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6
UMask=0077

[Install]
WantedBy=multi-user.target
''')
    call("systemctl", "daemon-reload")
    print("Prepared Linux monitor; install scoped identity.json before activation.")


def export_evidence(args):
    dest = Path(args.output).resolve()
    if dest.exists():
        raise ValueError("use a new evidence export directory")
    dest.mkdir(mode=0o700, parents=True)
    if sys.platform == "darwin":
        for name in ("receipt.json", "snapshot-manifest.json"):
            shutil.copyfile(BASE / name, dest / name)
        shutil.copytree(BASE / "evidence", dest / "native")
        shutil.copytree(BASE / "logs", dest / "logs")
    else:
        shutil.copyfile(ETC / "receipt.json", dest / "receipt.json")
        for name in ("status.json", "cursor.json"):
            shutil.copyfile(LINUX_HOME / name, dest / name)
        for source in LINUX_HOME.glob("audit.jsonl*"):
            shutil.copyfile(source, dest / source.name)
    print("Exported evidence to", dest)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["mac-prepare", "mac-activate", "mac-stop", "mac-uninstall",
                                           "linux-prepare", "linux-activate", "linux-stop", "linux-uninstall", "export"])
    parser.add_argument("--binary")
    parser.add_argument("--known-hosts")
    parser.add_argument("--tunnel-public-key")
    parser.add_argument("--receipt")
    parser.add_argument("--infisical-project")
    parser.add_argument("--output")
    args = parser.parse_args()
    if os.geteuid() != 0:
        parser.error("installer requires root")
    if args.command.startswith("mac-") and sys.platform != "darwin":
        parser.error("Mac operation requires macOS")
    if args.command.startswith("linux-") and sys.platform != "linux":
        parser.error("Linux operation requires Linux")
    if args.command == "mac-prepare":
        install_mac(args)
    elif args.command == "mac-activate":
        read_json(BASE / "receipt.json")
        for label in LABELS:
            result = subprocess.run(["launchctl", "print", "system/" + label], capture_output=True, timeout=30)
            if result.returncode:
                call("launchctl", "bootstrap", "system", "/Library/LaunchDaemons/" + label + ".plist")
    elif args.command in ("mac-stop", "mac-uninstall"):
        stop_mac()
        if args.command == "mac-uninstall":
            for label in LABELS:
                Path("/Library/LaunchDaemons/" + label + ".plist").unlink(missing_ok=True)
            Path("/etc/newsyslog.d/zakura-verifier.conf").unlink(missing_ok=True)
            print("Services removed; state and evidence preserved for export.")
    elif args.command == "linux-prepare":
        install_linux(args)
    elif args.command == "linux-activate":
        identity = read_json(ETC / "identity.json")
        if set(identity) != {"client_id", "client_secret"}:
            raise ValueError("invalid Universal Auth identity file")
        if (ETC / "identity.json").stat().st_mode & 0o077:
            raise ValueError("identity.json must be root-owned mode 0600")
        call("systemctl", "enable", "--now", "zakura-mac-verifier")
    elif args.command in ("linux-stop", "linux-uninstall"):
        call("systemctl", "disable", "--now", "zakura-mac-verifier")
        if args.command == "linux-uninstall":
            Path("/etc/systemd/system/zakura-mac-verifier.service").unlink(missing_ok=True)
            Path("/etc/ssh/sshd_config.d/70-zakura-mac-verifier.conf").unlink(missing_ok=True)
            call("sshd", "-t")
            call("systemctl", "reload", "ssh")
            call("systemctl", "daemon-reload")
            print("POC services removed; evidence retained and mainnet services unchanged.")
    else:
        if not args.output:
            parser.error("export requires --output")
        export_evidence(args)


if __name__ == "__main__":
    main()
