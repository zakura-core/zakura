#!/usr/bin/env python3
"""Scaleway trial lifecycle. Never print raw provider responses (they contain secrets)."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import urllib.error
import uuid

from common import Transport, Unavailable, atomic_json, read_json

ZONE = "fr-par-1"
NAME = "zakura-mainnet-mac-verifier-poc"
KIND = "M2-M"


class Provider:
    def __init__(self, project, token, transport=None):
        uuid.UUID(project)
        self.project, self.token = project, token
        self.transport = transport or Transport(timeout=30)

    def call(self, path, payload=None, method=None):
        return self.transport.json(
            f"https://api.scaleway.com/apple-silicon/v1alpha1/zones/{ZONE}/{path}",
            payload, {"X-Auth-Token": self.token}, method,
        )

    def find(self):
        matches = []
        page = 1
        while True:
            result = self.call(f"servers?project_id={self.project}&page_size=100&page={page}")
            servers = result["servers"]
            matches += [s for s in servers if s["name"] == NAME]
            if page * 100 >= result["total_count"]:
                break
            page += 1
            if page > 100:
                raise Unavailable("unexpected provider inventory size")
        if len(matches) > 1:
            raise Unavailable("multiple POC servers: reconcile manually")
        return matches[0] if matches else None

    def get(self, server_id):
        uuid.UUID(server_id)
        server = self.call("servers/" + server_id)
        if server["project_id"] != self.project or server["name"] != NAME or server["type"] != KIND:
            raise Unavailable("resource ownership/type does not match POC")
        return server

    def verify_deleted(self, state_path):
        saved = read_json(state_path)
        if saved.get("project_id") != self.project or not saved.get("deletion_requested_at"):
            raise Unavailable("recorded deletion request required before confirming termination")
        try:
            self.get(saved["server_id"])
        except urllib.error.HTTPError as error:
            if error.code != 404:
                raise
            saved["deletion_confirmed_at"] = time.time()
            saved["billing_stopped"] = True
            atomic_json(state_path, saved)
            return {"server_id": saved["server_id"], "deletion_confirmed": True,
                    "billing_stopped": True}
        return {"server_id": saved["server_id"], "deletion_confirmed": False,
                "billing_stopped": False}

    def create(self, state_path, price_confirmed):
        path = Path(state_path)
        prior = read_json(path) if path.exists() else {}
        if prior.get("server_id"):
            return self.get(prior["server_id"])
        existing = self.find()
        if existing:
            if not prior.get("create_attempted_at"):
                raise Unavailable("unrecorded POC exists: inspect and explicitly adopt its ID")
            if existing["type"] != KIND:
                raise Unavailable("existing POC has wrong server type")
            created = datetime.fromisoformat(existing["created_at"].replace("Z", "+00:00")).timestamp()
            if created < prior["create_attempted_at"] - 10:
                raise Unavailable("existing POC predates this creation attempt")
            persist_inventory(path, existing)
            return existing
        if prior.get("create_attempted_at"):
            raise Unavailable("previous creation outcome uncertain: do not retry POST automatically")
        if price_confirmed != "0.17":
            raise Unavailable("current €0.17/hour pre-tax price must be verified before ordering")
        offer = self.call("server-type/M2-M")
        if offer.get("stock") not in ("high_stock", "low_stock"):
            raise Unavailable("M2-M capacity not confirmed")
        if offer.get("memory", {}).get("capacity") not in (16 * 1000**3, 16 * 1024**3):
            raise Unavailable("provider memory does not match 16 GiB offer")
        if offer.get("default_os", {}).get("is_beta") is not False:
            raise Unavailable("default OS is not confirmed stable")
        atomic_json(path, {"project_id": self.project, "create_attempted_at": time.time(),
                           "managed": True, "request_id": str(uuid.uuid4())})
        server = self.call("servers", {"project_id": self.project, "name": NAME, "type": KIND,
                                       "commitment_type": "duration_24h", "enable_vpc": False,
                                       "enable_kext": False})
        persist_inventory(path, server)
        return server


def public_inventory(server):
    created = datetime.fromisoformat(server["created_at"].replace("Z", "+00:00")).timestamp()
    return {"server_id": server["id"], "name": server["name"], "project_id": server["project_id"],
            "type": server["type"], "zone": server["zone"], "ip": server.get("ip"),
            "ssh_username": server.get("ssh_username"), "status": server["status"],
            "os": {k: server.get("os", {}).get(k) for k in ("id", "name", "version")},
            "created_at": server["created_at"], "deletable_at": server["deletable_at"],
            "trial_review_at": created + 60 * 3600, "trial_expires_at": created + 72 * 3600,
            "commitment": server.get("commitment"), "public_bandwidth_bps": server.get("public_bandwidth_bps")}


def persist_inventory(path, server):
    prior = read_json(path) if Path(path).exists() else {}
    inventory = {k: prior[k] for k in ("managed", "request_id", "create_attempted_at") if k in prior}
    inventory.update(public_inventory(server))
    atomic_json(path, inventory)


def store_credentials(server, project, secret_path):
    # CLI accepts file references, keeping secret values out of argv and all output.
    with tempfile.TemporaryDirectory(prefix="zakura-scw-secret-") as directory:
        os.chmod(directory, 0o700)
        args = ["infisical", "secrets", "set", "--env=prod", "--projectId=" + project,
                "--path=" + secret_path, "--silent"]
        for key, field in [("MAC_VERIFIER_SUDO_PASSWORD", "sudo_password"),
                           ("MAC_VERIFIER_VNC_URL", "vnc_url")]:
            value = server.get(field)
            if value:
                path = Path(directory) / key
                path.write_text(value)
                path.chmod(0o600)
                args.append(key + "=@" + str(path))
        if len(args) == 7:
            raise Unavailable("provider credentials unavailable")
        result = subprocess.run(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=60)
        if result.returncode:
            raise Unavailable("Infisical credential persistence failed; resource ID retained")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["inspect", "create", "adopt", "reboot", "destroy", "verify-deleted"])
    parser.add_argument("--inventory", required=True)
    parser.add_argument("--confirmed-hourly-eur")
    parser.add_argument("--server-id")
    parser.add_argument("--evidence-export", help="required durable evidence directory before deletion")
    args = parser.parse_args()
    provider = Provider(os.environ["SCW_PROJECT_ID"], os.environ["SCW_SECRET_KEY"])
    if args.command == "verify-deleted":
        print(json.dumps(provider.verify_deleted(args.inventory)))
        return
    if args.command == "create":
        server = provider.create(args.inventory, args.confirmed_hourly_eur)
        store_credentials(server, os.environ["INFISICAL_PROJECT_ID"], "/mac-verifier-poc")
    elif args.command == "adopt":
        if not args.server_id:
            parser.error("adopt requires an explicitly selected --server-id")
        server = provider.get(args.server_id)
        atomic_json(args.inventory, {"managed": False})
    else:
        server = provider.get(read_json(args.inventory)["server_id"])
        if args.command == "reboot":
            provider.call("servers/" + server["id"] + "/reboot", {}, "POST")
        elif args.command == "destroy":
            saved = read_json(args.inventory)
            if saved.get("managed") is not True or not saved.get("request_id"):
                raise Unavailable("only recorded resources created by this POC can be destroyed")
            evidence = Path(args.evidence_export or "")
            for name in ("receipt.json", "status.json", "cursor.json", "audit.jsonl"):
                if not args.evidence_export or not (evidence / name).is_file():
                    parser.error("export receipt/status/cursor/audit before destroying the trial")
            deadline = datetime.fromisoformat(server["deletable_at"].replace("Z", "+00:00"))
            if datetime.now(timezone.utc) < deadline:
                raise Unavailable("minimum lease has not elapsed")
            provider.call("servers/" + server["id"], method="DELETE")
            inventory = public_inventory(server)
            inventory["deletion_requested_at"] = time.time()
            atomic_json(args.inventory, inventory)
            # Deliberately do not report billing stopped before provider returns 404.
            print(json.dumps({"server_id": server["id"], "deletion_requested": True,
                              "billing_stopped": False}))
            return
    persist_inventory(args.inventory, server)
    print(json.dumps(public_inventory(server), indent=2))


if __name__ == "__main__":
    try:
        main()
    except (Unavailable, urllib.error.HTTPError, KeyError, ValueError) as error:
        # Avoid request/response bodies and credentials in diagnostics.
        raise SystemExit("Provider operation failed: " + type(error).__name__) from None
