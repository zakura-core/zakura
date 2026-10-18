#!/usr/bin/env python3
"""Check that every entry in hot-sources.txt is a RocksDB source in the locked librocksdb-sys.

`cxx.sh` builds every RocksDB source that is not listed with `-Os`. If a RocksDB
upgrade renames or removes a hot source, the list entry silently stops matching
and that code loses `-O3`, so a stale entry is an error.
"""

import json
import pathlib
import subprocess
import sys

HOT_LIST = pathlib.Path(__file__).with_name("hot-sources.txt")


def hot_sources(text):
    """Return the source paths listed in a hot-sources file, skipping comments and blanks."""
    return [
        line.strip()
        for line in text.splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]


def locked_rocksdb_sources():
    """Return the RocksDB sources that the locked librocksdb-sys compiles."""
    metadata = json.loads(
        subprocess.run(
            ["cargo", "metadata", "--format-version", "1", "--locked"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    )
    manifests = [
        package["manifest_path"]
        for package in metadata["packages"]
        if package["name"] == "librocksdb-sys"
    ]
    if len(manifests) != 1:
        raise SystemExit(f"expected one librocksdb-sys package, found {len(manifests)}")
    source_list = pathlib.Path(manifests[0]).with_name("rocksdb_lib_sources.txt")
    return set(source_list.read_text().split())


def main(argv=None):
    del argv
    listed = hot_sources(HOT_LIST.read_text())
    known = locked_rocksdb_sources()
    stale = [source for source in listed if source not in known]
    duplicates = sorted({source for source in listed if listed.count(source) > 1})
    for source in stale:
        print(f"{HOT_LIST.name}: {source} is not a RocksDB source in the locked librocksdb-sys")
    for source in duplicates:
        print(f"{HOT_LIST.name}: {source} is listed more than once")
    if stale or duplicates:
        return 1
    print(f"{HOT_LIST.name}: {len(listed)} hot sources match the locked librocksdb-sys")
    return 0


if __name__ == "__main__":
    sys.exit(main())
