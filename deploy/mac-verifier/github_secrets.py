#!/usr/bin/env python3
"""Mirror operator secrets from Infisical to a fixed GitHub environment."""
import argparse
import json
import subprocess
import tempfile
from pathlib import Path
import uuid

from common import Unavailable
from private_deploy import validate_host

PROJECT = "c57a6889-6a7c-4d05-a54a-e4a4c0b14ee7"
FOLDER = "/mac-verifier-poc/provisioner"
REPO = "zakura-core/zakura"
ENVIRONMENT = "mac-verifier-private"
KEYS = ("MAC_VERIFIER_HOST", "MAC_VERIFIER_USER", "MAC_VERIFIER_SSH_KEY", "MAC_VERIFIER_KNOWN_HOSTS",
        "MAC_VERIFIER_REFERENCE_HOST", "MAC_VERIFIER_REFERENCE_USER", "MAC_VERIFIER_REFERENCE_SSH_KEY",
        "MAC_VERIFIER_REFERENCE_KNOWN_HOSTS", "MAC_VERIFIER_TUNNEL_KEY", "MAC_VERIFIER_ID")


def get(key, path=FOLDER):
    result = subprocess.run(["infisical", "secrets", "get", key, "--plain", "--silent", "--env=prod",
                             "--expand=false", "--include-imports=false", "--secret-overriding=false",
                             "--projectId=" + PROJECT, "--path=" + path], capture_output=True, text=True, timeout=60)
    if result.returncode or not result.stdout.strip():
        raise Unavailable("required Infisical secret unavailable: " + key)
    return result.stdout.rstrip("\n")


def set_secret(key, value):
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "value"
        path.write_text(value)
        path.chmod(0o600)
        result = subprocess.run(["infisical", "secrets", "set", key + "=@" + str(path), "--silent",
                                 "--env=prod", "--projectId=" + PROJECT, "--path=" + FOLDER],
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=60)
        if result.returncode:
            raise Unavailable("could not vault configuration")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["init", "sync", "bind-host"])
    parser.add_argument("--host-file", help="private local file; never pass the IP as an argument")
    args = parser.parse_args()
    if args.command == "init":
        try:
            get("MAC_VERIFIER_ID")
        except Unavailable:
            set_secret("MAC_VERIFIER_ID", "verifier-" + uuid.uuid4().hex)
        return
    if args.command == "bind-host":
        if not args.host_file:
            parser.error("bind-host requires --host-file")
        set_secret("MAC_VERIFIER_HOST", validate_host(Path(args.host_file).read_text().strip()))
        return
    # Fetch all values before mutating GitHub; an incomplete vault fails closed.
    values = {key: get(key) for key in KEYS}
    values["MAC_VERIFIER_MONITOR_IDENTITY_JSON"] = get("MAC_VERIFIER_MONITOR_IDENTITY_JSON", "/mac-verifier-poc")
    result = subprocess.run(["gh", "api", "--method", "PUT", f"repos/{REPO}/environments/{ENVIRONMENT}"],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=60)
    if result.returncode:
        raise Unavailable("GitHub environment setup failed")
    for key, value in values.items():
        result = subprocess.run(["gh", "secret", "set", key, "--repo", REPO, "--env", ENVIRONMENT],
                                input=value, text=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=60)
        if result.returncode:
            raise Unavailable("GitHub secret propagation failed: " + key)
    print("Private deployment secrets propagated; no endpoint values emitted")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        raise SystemExit("Private secret configuration failed; inspect locally without printing values") from None
