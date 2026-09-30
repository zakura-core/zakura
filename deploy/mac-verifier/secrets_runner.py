#!/usr/bin/env python3
"""Fetch one runtime secret through a scoped Universal Auth identity, then exec."""
import os
from pathlib import Path
import sys
import urllib.parse

from common import Transport, read_json


def main():
    directory = Path(os.environ["CREDENTIALS_DIRECTORY"])
    identity = read_json(directory / "infisical-identity")
    metadata = read_json("/etc/zakura-mac-verifier/infisical.json")
    client = Transport()
    token = client.json("https://app.infisical.com/api/v1/auth/universal-auth/login",
                        {"clientId": identity["client_id"], "clientSecret": identity["client_secret"]})["accessToken"]
    query = urllib.parse.urlencode({"workspaceId": metadata["project_id"], "environment": "prod",
                                   "secretPath": "/mac-verifier/monitor", "include_imports": "false",
                                   "type": "shared"})
    result = client.json("https://app.infisical.com/api/v3/secrets/raw/MAC_VERIFIER_SLACK_BOT_TOKEN?" + query,
                         headers={"Authorization": "Bearer " + token})
    secret = result["secret"]
    if secret["secretKey"] != "MAC_VERIFIER_SLACK_BOT_TOKEN" or not secret["secretValue"]:
        raise SystemExit("Dedicated monitor Slack token unavailable")
    environment = {**os.environ, "MAC_VERIFIER_SLACK_BOT_TOKEN": secret["secretValue"]}
    os.execve(sys.executable, [sys.executable, str(Path(__file__).with_name("monitor.py")), "run"], environment)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        raise SystemExit("Infisical runtime secret retrieval failed") from None
