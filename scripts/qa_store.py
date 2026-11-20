#!/usr/bin/env python3
"""Sync the QA store, the simulator's test artifacts, with its Space.

The repository holds no test artifact: no planted case, native mutation,
scenario fixture or seed. They live in the DigitalOcean Space
`zakura-sim-corpus` (nyc3), which mirrors one local directory, the QA store,
that the tooling reads through `ZAKURA_QA_STORE`:

    qa/planted/<property>/<case>.{patch,toml,scenario.json}
    qa/native-mutations/cases.json, work-list.toml, triage.toml, <entry>/...
    qa/mutations/<pilot>/...
    fixtures/<name>.json
    seeds/index.json, seeds/<name>.bin

`pull` mirrors the Space into `--dest`: it downloads each object whose local
copy differs, removes each local file under a store prefix that the Space
lacks, and with `--manifest` writes every file's SHA-256. `push` uploads each
file under `--src`'s store prefixes that the Space lacks or holds otherwise,
and with `--delete` removes each object the directory lacks. `changed` lists
the paths whose hash differs between two manifests, one per line, for
`cargo xtask planted --store-changed`.

The Space's bucket comes from `ZAKURA_CORPUS_BUCKET` and its endpoint from
`ZAKURA_CORPUS_ENDPOINT`. boto3 reads the key from `AWS_ACCESS_KEY_ID` and
`AWS_SECRET_ACCESS_KEY`. On the joint-run droplet these live in
`/etc/zakura-mutation-joint.env`, which `--env-file` reads. `pull` and
`push` need boto3, which the droplet's system Python carries. Repository
tests never reach the Space.

Trust boundary: the joint-run routine applies the store's patches and builds
and runs the patched code as root on the droplet, so a store writer can run
code there. The Space is private, and its only bucket-scoped key lives in
that droplet's root-only env file, so only someone who can already run code
on the droplet, or an account administrator, can write the store; no CI job
reads it.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys

PREFIXES = ("qa/planted/", "qa/native-mutations/", "qa/mutations/", "fixtures/", "seeds/")


def read_env_file(path):
    """Return the `KEY=VALUE` lines of a systemd environment file, without comments."""
    env = {}
    for line in Path(path).read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        env[key.strip()] = value.strip().strip('"')
    return env


def in_store(path):
    """True if the store-relative `path` lies under a store prefix."""
    return any(path.startswith(prefix) for prefix in PREFIXES)


def md5(path):
    return hashlib.md5(Path(path).read_bytes()).hexdigest()


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def local_files(root):
    """Return every file under `root`'s store prefixes, as sorted store-relative paths."""
    root = Path(root)
    files = []
    for prefix in PREFIXES:
        base = root / prefix
        if base.is_dir():
            files.extend(path.relative_to(root).as_posix() for path in base.rglob("*") if path.is_file())
    return sorted(files)


def manifest(root):
    """Return `{path: sha256}` for every file under `root`'s store prefixes."""
    return {path: sha256(Path(root) / path) for path in local_files(root)}


def changed(old, new):
    """Return the sorted paths that one manifest lacks or the two hash differently."""
    return sorted(path for path in set(old) | set(new) if old.get(path) != new.get(path))


def plain(path):
    """True if `path` is relative and names no `..` or `.` part, so it stays inside the store."""
    parts = Path(path).parts
    return bool(parts) and not Path(path).is_absolute() and all(part not in ("..", ".") for part in parts)


def client(env):
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=env["ZAKURA_CORPUS_ENDPOINT"],
        region_name=env.get("AWS_DEFAULT_REGION", "us-east-1"),
        aws_access_key_id=env.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=env.get("AWS_SECRET_ACCESS_KEY"),
    )


def remote_objects(s3, bucket):
    """Return `{key: etag}` for every object under a store prefix."""
    objects = {}
    for prefix in PREFIXES:
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
            for item in page.get("Contents", []):
                objects[item["Key"]] = item["ETag"].strip('"')
    return objects


def same(path, etag):
    """True if the local file at `path` holds what a single-part upload with `etag` holds."""
    return path.is_file() and "-" not in etag and md5(path) == etag


def pull(s3, bucket, dest):
    """Mirror the Space into `dest`, and return (downloaded, removed) counts."""
    dest = Path(dest)
    remote = remote_objects(s3, bucket)
    downloaded = 0
    for key, etag in sorted(remote.items()):
        if not plain(key):
            raise SystemExit(f"the Space holds {key!r}, which isn't a plain relative path")
        path = dest / key
        if same(path, etag):
            continue
        path.parent.mkdir(parents=True, exist_ok=True)
        s3.download_file(bucket, key, str(path))
        downloaded += 1
    removed = 0
    for path in local_files(dest):
        if path not in remote:
            (dest / path).unlink()
            removed += 1
    return downloaded, removed


def push(s3, bucket, src, delete):
    """Upload `src`'s store files the Space lacks or holds otherwise, and return (uploaded, deleted) counts."""
    src = Path(src)
    remote = remote_objects(s3, bucket)
    files = local_files(src)
    uploaded = 0
    for path in files:
        if path in remote and same(src / path, remote[path]):
            continue
        s3.upload_file(str(src / path), bucket, path)
        uploaded += 1
    deleted = 0
    if delete:
        for key in sorted(set(remote) - set(files)):
            s3.delete_object(Bucket=bucket, Key=key)
            deleted += 1
    return uploaded, deleted


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--env-file", help="a systemd environment file with the bucket, endpoint and key")
    commands = parser.add_subparsers(dest="command", required=True)
    get = commands.add_parser("pull", help="mirror the Space into a local store")
    get.add_argument("--dest", required=True, help="the local store")
    get.add_argument("--manifest", help="also write every local store file's SHA-256 here, as JSON")
    put = commands.add_parser("push", help="upload a local store's files to the Space")
    put.add_argument("--src", required=True, help="the local store")
    put.add_argument("--delete", action="store_true", help="also remove each object the local store lacks")
    diff = commands.add_parser("changed", help="list the paths two manifests hash differently")
    diff.add_argument("old")
    diff.add_argument("new")
    diff.add_argument("--out", help="write the list here instead of to standard output")
    args = parser.parse_args(argv)
    if args.command == "changed":
        paths = changed(json.loads(Path(args.old).read_text()), json.loads(Path(args.new).read_text()))
        text = "".join(f"{path}\n" for path in paths)
        if args.out:
            Path(args.out).write_text(text)
        else:
            sys.stdout.write(text)
        return 0
    env = {**os.environ, **(read_env_file(args.env_file) if args.env_file else {})}
    bucket = env.get("ZAKURA_CORPUS_BUCKET")
    if not bucket or not env.get("ZAKURA_CORPUS_ENDPOINT"):
        raise SystemExit("set ZAKURA_CORPUS_BUCKET and ZAKURA_CORPUS_ENDPOINT, or pass --env-file")
    s3 = client(env)
    if args.command == "pull":
        downloaded, removed = pull(s3, bucket, args.dest)
        if args.manifest:
            Path(args.manifest).write_text(json.dumps(manifest(args.dest), indent=1, sort_keys=True) + "\n")
        print(f"pulled {downloaded} files and removed {removed} into {args.dest}")
    else:
        uploaded, deleted = push(s3, bucket, args.src, args.delete)
        print(f"pushed {uploaded} files and deleted {deleted} from {bucket}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
