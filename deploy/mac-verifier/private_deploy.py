#!/usr/bin/env python3
"""Secret-fed remote operations. Never relay SSH output to public Actions logs."""
import argparse
import ipaddress
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import tempfile

from common import Unavailable

SOURCE = "af944f5194ef2e9921bc96af017629450375013c"
REMOTE = "/var/tmp/zakura-verifier"
BASE = "/Library/Application Support/ZakuraVerifier"
MAC_PYTHON = "/opt/homebrew/opt/python@3.12/bin/python3.12"


def validate_host(value):
    # IP only: no option injection, credentials, whitespace or shell metacharacters.
    return str(ipaddress.ip_address(value))


def secret_file(directory, name, value):
    if not value.strip():
        raise Unavailable("required deployment secret missing")
    path = Path(directory) / name
    path.write_text(value + ("" if value.endswith("\n") else "\n"))
    path.chmod(0o600)
    return path


class SSH:
    def __init__(self, prefix, directory):
        self.host = validate_host(os.environ[prefix + "HOST"])
        user = os.environ[prefix + "USER"]
        if not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_-]{0,31}", user):
            raise Unavailable("invalid SSH user")
        port = os.environ.get(prefix + "SSH_PORT", "22")
        if not re.fullmatch(r"[0-9]{1,5}", port) or not 1 <= int(port) <= 65535:
            raise Unavailable("invalid SSH port")
        self.destination = user + "@" + self.host
        key = secret_file(directory, prefix + "key", os.environ[prefix + "SSH_KEY"])
        hosts = secret_file(directory, prefix + "hosts", os.environ[prefix + "KNOWN_HOSTS"])
        self.options = ["-i", str(key), "-p", port, "-o", "BatchMode=yes", "-o", "IdentitiesOnly=yes",
                        "-o", "StrictHostKeyChecking=yes", "-o", "UserKnownHostsFile=" + str(hosts),
                        "-o", "ConnectTimeout=10", "-o", "LogLevel=ERROR"]

    def run(self, script, timeout=60):
        result = subprocess.run(["ssh", *self.options, self.destination, "bash -s"],
                                input=script, capture_output=True, text=True, timeout=timeout)
        if result.returncode:
            raise Unavailable("remote operation failed; inspect privately over SSH")
        return result.stdout

    def put(self, data, path):
        # Content travels over stdin, never arguments, logs or uploaded artifacts.
        # Atomic replacement preserves a running reader's original inode.
        command = ("set -eu; umask 077; verifier_temp=$(mktemp " + shlex.quote(path + ".XXXXXX") + "); "
                   "trap 'rm -f \"$verifier_temp\"' EXIT; cat > \"$verifier_temp\"; "
                   "mv -f \"$verifier_temp\" " + shlex.quote(path))
        result = subprocess.run(["ssh", *self.options, self.destination, command],
                                input=data, capture_output=True, text=True, timeout=60)
        if result.returncode:
            raise Unavailable("private file transfer failed")

    def tunnel(self, forwarding):
        return subprocess.Popen(["ssh", *self.options, "-NT", "-o", "ExitOnForwardFailure=yes",
                                 "-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=3",
                                 *forwarding, self.destination], stdout=subprocess.DEVNULL,
                                stderr=subprocess.DEVNULL)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["preflight", "prepare", "bootstrap", "activate", "status", "stop"])
    args = parser.parse_args()
    identifier = os.environ["MAC_VERIFIER_ID"]
    if not re.fullmatch(r"verifier-[a-f0-9]{32}", identifier):
        raise Unavailable("invalid opaque verifier ID")
    revision = os.environ["MAC_VERIFIER_TOOLING_SHA"]
    if not re.fullmatch(r"[a-f0-9]{40}", revision):
        raise Unavailable("invalid tooling revision")
    with tempfile.TemporaryDirectory(prefix="verifier-private-") as directory:
        mac = SSH("MAC_VERIFIER_", directory)
        linux = SSH("MAC_VERIFIER_REFERENCE_", directory)
        if args.command == "preflight":
            mac.run('set -eu\nexport PATH="/opt/homebrew/bin:$HOME/.cargo/bin:$PATH"\n[ "$(uname -s)" = Darwin ]\n[ "$(uname -m)" = arm64 ]\n'
                    '[ "$(sysctl -n hw.memsize)" -ge 16000000000 ]\nsudo -n true\n'
                    'test -x /opt/homebrew/opt/python@3.12/bin/python3.12; command -v rustup; command -v protoc; command -v zstd\n'
                    '/opt/homebrew/opt/python@3.12/bin/python3.12 -c "import shutil; assert shutil.disk_usage(\'/var/tmp\').free >= 120*10**9"\n')
            linux.run('set -eu\nsudo -n true\ncommand -v python3; command -v systemctl\n')
        elif args.command == "prepare":
            mac.run(f"set -eu\numask 077\nmkdir -p {REMOTE}/tooling/templates\n")
            package = Path(__file__).resolve().parent
            for path in list(package.glob("*.py")) + [package / "build.sh", package / "corpus.json", package / "templates/zakurad.toml"]:
                relative = path.relative_to(package)
                mac.put(path.read_text(), REMOTE + "/tooling/" + str(relative))
            mac.put(os.environ["MAC_VERIFIER_REFERENCE_KNOWN_HOSTS"], REMOTE + "/reference-hosts")
            mac.put(os.environ["MAC_VERIFIER_TUNNEL_KEY"], REMOTE + "/tunnel")
            # Endpoints are root-private input data, not substituted into script text.
            mac.put(json.dumps({"reference_host": linux.host}), REMOTE + "/endpoints.json")
            mac.run(f'''set -eu
export PATH="/opt/homebrew/bin:$HOME/.cargo/bin:$PATH"
cd {REMOTE}
if [ ! -d source ]; then git clone https://github.com/zakura-core/zakura.git source; fi
git -C source checkout --detach {SOURCE}
bash tooling/build.sh source build
sudo -n mkdir -p "{BASE}/ssh"
sudo -n install -m 600 tunnel "{BASE}/ssh/tunnel"
sudo -n ssh-keygen -y -f "{BASE}/ssh/tunnel" | sudo -n tee "{BASE}/ssh/tunnel.pub" >/dev/null
sudo -n {MAC_PYTHON} - <<'PY'
import json,os,sys
os.environ['MAC_VERIFIER_REFERENCE_HOST']=json.load(open('{REMOTE}/endpoints.json'))['reference_host']
sys.path.insert(0,'{REMOTE}/tooling')
import install
sys.argv=['install.py','mac-prepare','--binary','{REMOTE}/build/zakurad','--known-hosts','{REMOTE}/reference-hosts']
install.main()
PY
sudo -n cp build/evidence/* "{BASE}/evidence/"
''', timeout=4 * 3600)
        elif args.command == "bootstrap":
            forwards = [linux.tunnel(["-L", "127.0.0.1:28235:127.0.0.1:8232"]),
                        mac.tunnel(["-R", "127.0.0.1:28235:127.0.0.1:28235"])]
            try:
                mac.run(f'''set -eu
export PATH="/opt/homebrew/bin:$HOME/.cargo/bin:$PATH"
sudo -n {MAC_PYTHON} {REMOTE}/tooling/bootstrap.py --source {REMOTE}/source --tooling-sha {revision}
''', timeout=3600)
                if any(p.poll() is not None for p in forwards):
                    raise Unavailable("bootstrap forwarding failed")
            finally:
                for process in forwards:
                    process.terminate()
                    try:
                        process.wait(timeout=15)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
        elif args.command == "activate":
            linux.run(f"set -eu\numask 077\nmkdir -p {REMOTE}/tooling\n")
            for path in Path(__file__).resolve().parent.glob("*.py"):
                linux.put(path.read_text(), REMOTE + "/tooling/" + path.name)
            fleet_script = Path(__file__).resolve().parents[1] / "runner/zakura-cluster-status.py"
            linux.put(fleet_script.read_text(), REMOTE + "/fleet-dashboard.py")
            linux.put(mac.run(f"sudo -n cat '{BASE}/receipt.json'"), REMOTE + "/receipt.json")
            linux.put(mac.run(f"sudo -n cat '{BASE}/ssh/tunnel.pub'"), REMOTE + "/tunnel.pub")
            credential = json.loads(os.environ["MAC_VERIFIER_MONITOR_IDENTITY_JSON"])
            if set(credential) != {"client_id", "client_secret"}:
                raise Unavailable("invalid monitor identity")
            linux.put(json.dumps(credential), REMOTE + "/identity.json")
            linux.put(json.dumps({"verifier_id": identifier}), REMOTE + "/dashboard.json")
            linux.run(f'''set -eu
sudo -n python3 {REMOTE}/tooling/install.py linux-prepare --receipt {REMOTE}/receipt.json --tunnel-public-key {REMOTE}/tunnel.pub --infisical-project c57a6889-6a7c-4d05-a54a-e4a4c0b14ee7 --fleet-dashboard-script {REMOTE}/fleet-dashboard.py
sudo -n install -m 600 {REMOTE}/identity.json /etc/zakura-mac-verifier/identity.json
sudo -n install -m 644 {REMOTE}/dashboard.json /etc/zakura-mac-verifier/dashboard.json
rm -f {REMOTE}/identity.json
sudo -n python3 {REMOTE}/tooling/install.py linux-activate
''')
            mac.run(f"sudo -n {MAC_PYTHON} {REMOTE}/tooling/install.py mac-activate")
        elif args.command == "stop":
            mac.run(f"sudo -n {MAC_PYTHON} {REMOTE}/tooling/install.py mac-stop")
            linux.run(f"sudo -n python3 {REMOTE}/tooling/install.py linux-stop")
        else:
            # Public Actions output uses the same allowlist as the dashboard.
            from dashboard import public_status
            raw = linux.run("sudo -n cat /var/lib/zakura-mac-verifier/status.json")
            print(json.dumps(public_status(json.loads(raw), identifier)))
        print(identifier + ": " + args.command + " completed")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        # Exception strings may contain an endpoint, key path or remote output.
        raise SystemExit("Private verifier operation failed; inspect privately, no endpoint details logged") from None
