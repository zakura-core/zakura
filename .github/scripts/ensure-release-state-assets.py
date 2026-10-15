#!/usr/bin/env python3
"""Dispatch the authorized publisher on main and verify its exact-run receipt."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import tempfile
import time
from pathlib import Path

from release_state_assets import AssetError, validate_receipt

WORKFLOW = "update-release-state.yml"
REPOSITORY = "zakura-core/zakura"


def gh(*args, timeout=30):
    result = subprocess.run(["gh", *args], capture_output=True, text=True, timeout=timeout)
    if result.returncode:
        raise AssetError("GitHub publisher request failed; inspect authentication and workflow access")
    return result.stdout


def validate_run(run: dict, workflow_id: int, title: str) -> None:
    if (run["workflow_id"] != workflow_id or run["event"] != "workflow_dispatch"
            or run["head_branch"] != "main" or run["display_title"] != title
            or run["repository"]["full_name"] != REPOSITORY
            or not re.fullmatch(r"[0-9a-f]{40}", run["head_sha"])):
        raise AssetError("publisher run identity does not match the requested main workflow")


def ensure(selected: dict, request_id: str, *, call=gh, clock=time.monotonic, sleep=time.sleep,
           timeout=5400) -> dict:
    if not re.fullmatch(r"prepare-[0-9]+-[0-9]+", request_id):
        raise AssetError("invalid release preparation request ID")
    deadline = clock() + timeout
    def invoke(*args):
        remaining = deadline - clock()
        if remaining <= 0:
            raise AssetError("timed out waiting for the asset publisher")
        return call(*args, timeout=min(30, remaining))
    prefix = f"repos/{REPOSITORY}/actions"
    workflow = json.loads(invoke("api", f"{prefix}/workflows/{WORKFLOW}"))
    if workflow["path"] != f".github/workflows/{WORKFLOW}" or workflow["state"] != "active":
        raise AssetError("unexpected or disabled asset publisher workflow")
    invoke("workflow", "run", WORKFLOW, "--repo", REPOSITORY, "--ref", "main",
           "-f", "mode=ensure-assets", "-f", f'meta_url={selected["meta_url"]}',
           "-f", f'meta_sha256={selected["meta_sha256"]}', "-f", f"request_id={request_id}")
    title = f"Ensure release-state assets {request_id}"
    run_id = None
    while True:
        if run_id is None:
            response = json.loads(invoke("api", f"{prefix}/workflows/{WORKFLOW}/runs?branch=main&event=workflow_dispatch&per_page=100"))
            matches = [run for run in response["workflow_runs"] if run["display_title"] == title]
            if len(matches) > 1:
                raise AssetError("multiple publisher runs claim the same request ID")
            if matches:
                run_id = matches[0]["id"]
                print(f"Waiting for https://github.com/{REPOSITORY}/actions/runs/{run_id}", flush=True)
        if run_id is not None:
            run = json.loads(invoke("api", f"{prefix}/runs/{run_id}"))
            validate_run(run, workflow["id"], title)
            if run["status"] == "completed":
                if run["conclusion"] != "success":
                    raise AssetError(f'asset publisher finished with {run["conclusion"]}: run {run_id}')
                with tempfile.TemporaryDirectory(prefix="zakura-publication-receipt-") as scratch:
                    invoke("run", "download", str(run_id), "--repo", REPOSITORY,
                           "--name", "release-state-assets", "--dir", scratch)
                    path = Path(scratch) / "receipt.json"
                    if not path.is_file() or path.stat().st_size > 64 * 1024:
                        raise AssetError("publisher receipt is missing or too large")
                    receipt = json.loads(path.read_text())
                current = json.loads(invoke("api", f"{prefix}/runs/{run_id}"))
                validate_run(current, workflow["id"], title)
                if (current["status"] != "completed" or current["conclusion"] != "success"
                        or current["run_attempt"] != run["run_attempt"] or current["head_sha"] != run["head_sha"]):
                    raise AssetError("publisher run changed while downloading its receipt")
                validate_receipt(receipt, selected, request_id, current)
                return receipt
        remaining = deadline - clock()
        if remaining <= 0:
            raise AssetError("timed out waiting for the asset publisher")
        sleep(min(15, remaining))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", type=Path, required=True)
    args = parser.parse_args()
    if (os.environ.get("GITHUB_REPOSITORY") != REPOSITORY
            or os.environ.get("GITHUB_REF") != "refs/heads/main"
            or os.environ.get("DRY_RUN") != "false"):
        raise AssetError("only a non-dry preparation run on canonical main may dispatch publication")
    selected = json.loads((args.state / "selection.json").read_text())
    if not selected["has_changes"] or os.environ.get("RELEASE_STATE_WAIVER"):
        raise AssetError("publication dispatch requires an advancing, unwaived import")
    request = f'prepare-{os.environ["GITHUB_RUN_ID"]}-{os.environ["GITHUB_RUN_ATTEMPT"]}'
    receipt = ensure(selected, request)
    (args.state / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")


if __name__ == "__main__":
    main()
