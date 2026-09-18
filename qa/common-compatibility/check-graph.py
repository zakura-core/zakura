#!/usr/bin/env python3
"""Reject a mixed common dependency graph in the temporary integration checkout."""

import json
from pathlib import Path
import sys
import tomllib


def check_graph(manifest, metadata):
    patches = manifest["patch"]["crates-io"]
    if len(patches) != 17:
        raise ValueError("the integration patch must cover all 17 common crates")
    revisions = {(patch["git"], patch["rev"]) for patch in patches.values()}
    if len(revisions) != 1:
        raise ValueError("all common patches must use the same source and revision")
    repository, revision = revisions.pop()
    if repository != "https://github.com/zakura-core/common.git":
        raise ValueError("the integration patch must use the common repository")
    source = f"git+{repository}?rev={revision}#{revision}"
    for name in patches:
        packages = [p for p in metadata["packages"] if p["name"] == name]
        if len(packages) != 1:
            raise ValueError(f"expected one copy of {name}, found {len(packages)}")
        package = packages[0]
        if package["source"] != source or package["version"] != "1.2.0":
            raise ValueError(f"{name} must resolve to the pinned common 1.2.0 snapshot")
        if "zip-233" in package["features"]:
            raise ValueError(f"{name} still exposes the obsolete zip-233 feature")
    print(f"Verified all 17 common crates at 1.2.0, revision {revision}")


if __name__ == "__main__":
    manifest = tomllib.loads(Path(sys.argv[1]).read_text())
    metadata = json.loads(Path(sys.argv[2]).read_text())
    check_graph(manifest, metadata)
