#!/usr/bin/env python3
"""Fetch a ready release-state set or promote a verified candidate."""

import argparse
import importlib.util
import json
import os
from pathlib import Path

from ready_release_state import ROOT, fresh, promote, production_store, resolve_ready, verify_ready_snapshot
from release_state_assets import AssetError, cargo_grid, registry_grid


def main(*, store_factory=production_store):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("fetch", "verify", "promote"))
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--committed", action="store_true")
    args = parser.parse_args()
    state = args.state.resolve()
    if args.operation == "promote":
        if (os.environ.get("GITHUB_REPOSITORY") != "zakura-core/zakura"
                or os.environ.get("GITHUB_REF") != "refs/heads/main"):
            raise AssetError("storage promotion is restricted to canonical main")
        manifest = promote(state, store_factory())
        (state / "promoted-ready.json").write_text(json.dumps(manifest, indent=2) + "\n")
    elif args.operation == "fetch":
        url = os.environ.get("READY_URL", "")
        if not url:
            raise AssetError("MAINNET_RELEASE_STATE_READY_URL is not configured; no candidate fallback is permitted")
        manifest = resolve_ready(url, state)
        spec = importlib.util.spec_from_file_location("release_asset_tools", Path(__file__).with_name("release-state-assets.py"))
        tools = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(tools)
        selection = tools.select(ROOT, state / "bundle", json.loads((state / "resolution.json").read_text()))
        if any(selection[key] != manifest["asset"][key] for key in ("name", "version", "grid_sha256")):
            raise AssetError("ready asset differs from the repository's selected asset family")
        (state / "selection.json").write_text(json.dumps(selection, indent=2) + "\n")
    else:
        manifest = verify_ready_snapshot(state)
        if args.committed:
            provenance = ROOT / "crates/zakura-state/src/service/finalized_state/vct/mainnet-vct-manifest.json"
            fresh(json.loads(provenance.read_text())["generated_at"])
        selected = json.loads((state / "selection.json").read_text())
        checksum = registry_grid(selected)
        if checksum != manifest["asset"]["checksum"]:
            raise AssetError("ready manifest differs from the registry archive")
        cargo_grid(selected)
        verify_ready_snapshot(state)


if __name__ == "__main__":
    main()
