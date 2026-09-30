#!/usr/bin/env python3
"""Create a no-access identity plus one narrowly scoped monitor permission."""
import argparse
import json
from pathlib import Path
import re
import subprocess
import tempfile
import urllib.error
import uuid

from common import Transport, Unavailable, atomic_json, read_json

PROJECT = "c57a6889-6a7c-4d05-a54a-e4a4c0b14ee7"
ORGANIZATION = "a0ad6bcd-d39f-4295-bd0d-ee09c97a1ea8"
NAME = "zakura-mainnet-mac-verifier-poc-monitor"


def user_token():
    result = subprocess.run(["infisical", "user", "get", "token", "--silent"],
                            capture_output=True, text=True, timeout=30)
    token = re.search(r"eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+", result.stdout)
    if result.returncode or token is None:
        raise Unavailable("operator Infisical login unavailable")
    return token.group()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["create", "revoke"])
    parser.add_argument("--receipt", required=True)
    args = parser.parse_args()
    client, token = Transport(), user_token()
    def api(path, data=None, method=None):
        return client.json("https://app.infisical.com/api" + path, data,
                           {"Authorization": "Bearer " + token}, method)
    path = Path(args.receipt)
    state = read_json(path) if path.exists() else {"request_id": str(uuid.uuid4())}
    if args.command == "revoke":
        if not state.get("identity_id"):
            raise Unavailable("no recorded POC identity")
        api("/v1/identities/" + state["identity_id"], method="DELETE")
        state["revoked"] = True
        atomic_json(path, state)
        print("POC monitor identity revoked")
        return
    if state.get("complete") or state.get("revoked"):
        print("Recorded identity already configured or revoked; inspect its receipt")
        return
    if not state.get("identity_id"):
        existing = api("/v1/identities?orgId=" + ORGANIZATION)
        # Existing names must be reconciled explicitly; never grant an unrelated identity.
        identities = existing.get("identities", [])
        if any(x.get("name", x.get("identity", {}).get("name")) == NAME for x in identities):
            raise Unavailable("unrecorded monitor identity exists; reconcile before creating")
        if state.get("create_attempted"):
            raise Unavailable("identity creation outcome uncertain; reconcile before retrying")
        state["create_attempted"] = True
        atomic_json(path, state)
        try:
            result = api("/v1/identities", {"name": NAME, "organizationId": ORGANIZATION,
                                           "role": "no-access", "metadata": [{"key": "poc_request",
                                                                                 "value": state["request_id"]}]})
        except urllib.error.HTTPError as error:
            if error.code in (400, 401, 402, 403, 404, 405, 409, 422):
                state["create_attempted"] = False
                state["creation_rejected_http"] = error.code
                atomic_json(path, state)
            raise
        state["identity_id"] = result["identity"]["id"]
        atomic_json(path, state)
    identity = state["identity_id"]
    if not state.get("membership"):
        api(f"/v1/projects/{PROJECT}/identity-memberships/{identity}", {"role": "no-access"})
        state["membership"] = True
        atomic_json(path, state)
    if not state.get("permission"):
        api("/v2/identity-project-additional-privilege", {
            "identityId": identity, "projectId": PROJECT, "slug": "mac-verifier-monitor-read",
            "type": {"isTemporary": False}, "permissions": [
                {"subject": "secrets", "action": "read", "conditions": {
                    "environment": "prod", "secretPath": "/mac-verifier-poc/monitor",
                    "secretName": "MAC_VERIFIER_SLACK_BOT_TOKEN"}},
            ],
        })
        state["permission"] = True
        atomic_json(path, state)
    if not state.get("client_id"):
        result = api(f"/v1/auth/universal-auth/identities/{identity}", {
            "clientSecretTrustedIps": [{"ipAddress": "159.65.183.89/32"}],
            "accessTokenTrustedIps": [{"ipAddress": "159.65.183.89/32"}],
            "accessTokenTTL": 300, "accessTokenMaxTTL": 300,
        })
        state["client_id"] = result["identityUniversalAuth"]["clientId"]
        atomic_json(path, state)
    if state.get("secret_creation_attempted"):
        raise Unavailable("client-secret outcome uncertain: revoke unresolved secret before retry")
    state["secret_creation_attempted"] = True
    atomic_json(path, state)
    result = api(f"/v1/auth/universal-auth/identities/{identity}/client-secrets", {
        "description": "72-hour POC " + state["request_id"], "ttl": 96 * 3600, "numUsesLimit": 0,
    })
    try:
        with tempfile.TemporaryDirectory(prefix="zakura-monitor-identity-") as directory:
            credential = Path(directory) / "identity.json"
            atomic_json(credential, {"client_id": state["client_id"], "client_secret": result["clientSecret"]})
            command = ["infisical", "secrets", "set", "--env=prod", "--projectId=" + PROJECT,
                       "--path=/mac-verifier-poc", "--silent",
                       "MAC_VERIFIER_MONITOR_IDENTITY_JSON=@" + str(credential)]
            saved = subprocess.run(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=60)
            if saved.returncode:
                raise Unavailable("could not vault new monitor identity")
    except Exception:
        api(f"/v1/auth/universal-auth/identities/{identity}/client-secrets/"
            + result["clientSecretData"]["id"] + "/revoke", {}, "POST")
        state["secret_creation_attempted"] = False
        atomic_json(path, state)
        raise
    state["complete"] = True
    state["client_secret_id"] = result["clientSecretData"]["id"]
    atomic_json(path, state)
    print("Monitor identity configured and vaulted; install on DO through a private runtime fetch")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        raise SystemExit("Identity operation failed: " + type(error).__name__
                         + (f" HTTP {error.code}" if isinstance(error, urllib.error.HTTPError) else "")) from None
