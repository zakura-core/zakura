#!/usr/bin/env python3
"""Verify and restore only finalized pruned state before starting launchd."""
import argparse
import hashlib
import os
from pathlib import Path, PurePosixPath
import platform
import pwd
import re
import shutil
import subprocess
import tarfile
import time
import urllib.parse
import urllib.request

from common import RPC, Transport, Unavailable, atomic_json, digest, read_json

SOURCE_SHA = "af944f5194ef2e9921bc96af017629450375013c"
MANIFEST = "https://zakura.valargroup.dev/mainnet-pruned/latest.json"


def verify_manifest(value):
    if (value.get("network") != "mainnet" or value.get("snapshot_kind") != "pruned"
            or value.get("db_major") != 29 or not re.fullmatch(r"29\.[0-9]+\.[0-9]+", value.get("db_format_version", ""))
            or not re.fullmatch(r"[0-9a-fA-F]{64}", value.get("sha256", ""))
            or type(value.get("size_bytes")) is not int or not 0 < value["size_bytes"] < 60 * 10**9):
        raise Unavailable("invalid or incompatible pruned snapshot manifest")
    url = urllib.parse.urlsplit(value.get("url", ""))
    if url.scheme != "https" or not url.hostname or url.username or url.password:
        raise Unavailable("snapshot URL must be HTTPS")
    return value


def finalized_member(member):
    path = PurePosixPath(member.name)
    if path.is_absolute() or ".." in path.parts or member.issym() or member.islnk():
        raise Unavailable("unsafe archive member")
    parts = list(path.parts)
    # Publishers can wrap their cache directory; extract only state/v29/mainnet.
    matches = [i for i in range(len(parts) - 2) if parts[i:i + 3] == ["state", "v29", "mainnet"]]
    if len(matches) != 1:
        return None
    if not (member.isfile() or member.isdir()):
        raise Unavailable("non-file archive member")
    suffix = parts[matches[0]:]
    if "non_finalized_state" in suffix or suffix[-1] == "LOCK":
        return None
    return PurePosixPath(*suffix)


def restore(archive, base):
    stage = base / "restore-stage"
    if stage.exists() or (base / "state").exists():
        raise Unavailable("bootstrap destination already exists; do not overwrite state")
    stage.mkdir(mode=0o700)
    budget = shutil.disk_usage(base).free - 50 * 10**9
    extracted = 0
    process = subprocess.Popen(["zstd", "-dc", str(archive)], stdout=subprocess.PIPE)
    try:
        with tarfile.open(fileobj=process.stdout, mode="r|") as stream:
            for member in stream:
                relative = finalized_member(member)
                if relative is None:
                    continue
                dest = stage / relative
                if member.isdir():
                    dest.mkdir(parents=True, exist_ok=True)
                else:
                    extracted += member.size
                    if extracted > budget or member.size < 0:
                        raise Unavailable("expanded snapshot exceeds disk budget")
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    if dest.exists():
                        raise Unavailable("duplicate archive file")
                    with stream.extractfile(member) as src, dest.open("xb") as target:
                        shutil.copyfileobj(src, target, length=1024 * 1024)
        if process.wait(timeout=30) != 0:
            raise Unavailable("zstd decompression failed")
        if not (stage / "state/v29/mainnet/CURRENT").is_file():
            raise Unavailable("archive has no finalized v29 mainnet database")
        os.rename(stage / "state", base / "state")
        stage.rmdir()
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()


def finalize_bootstrap(base, archive, receipt):
    """Publish activation evidence only after cleanup and ownership succeed."""
    archive.unlink()
    if shutil.disk_usage(base).free < 50 * 10**9:
        raise Unavailable("less than 50 GB free after cleanup")
    account = pwd.getpwnam("_zakuraverifier")
    for path in [base / "state", *(base / "state").rglob("*")]:
        os.chown(path, account.pw_uid, account.pw_gid)
        path.chmod(0o700 if path.is_dir() else 0o600)
    atomic_json(base / "receipt.json", receipt)
    (base / "receipt.json").chmod(0o644)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", default="/Library/Application Support/ZakuraVerifier")
    parser.add_argument("--source", required=True)
    parser.add_argument("--tooling-sha", required=True)
    parser.add_argument("--reference-rpc", default="http://127.0.0.1:28235")
    args = parser.parse_args()
    base, source = Path(args.base), Path(args.source)
    if (base / "receipt.json").exists():
        raise Unavailable("bootstrap already completed")
    if platform.system() != "Darwin" or platform.machine() != "arm64":
        raise Unavailable("bootstrap requires native Apple Silicon macOS")
    if subprocess.check_output(["git", "-C", str(source), "rev-parse", "HEAD"], text=True).strip() != SOURCE_SHA:
        raise Unavailable("wrong consensus source revision")
    if shutil.disk_usage(base).free < 120 * 10**9:
        raise Unavailable("less than 120 GB free before import")
    corpus = read_json(base / "evidence/corpus-receipt.json")
    if corpus.get("passed") is not True or corpus.get("source_sha") != SOURCE_SHA or corpus.get("architecture") != "arm64":
        raise Unavailable("native pinned corpus has not passed")
    pinned = base / "snapshot-manifest.json"
    manifest = verify_manifest(read_json(pinned) if pinned.exists() else Transport().json(
        MANIFEST, headers={"User-Agent": "zakura-mac-verifier/1"}))
    if not pinned.exists():
        atomic_json(pinned, manifest)
    archive = base / "snapshot.tar.zst"
    if not archive.exists():
        partial = base / "snapshot.partial"
        result = subprocess.run(
            ["curl", "--fail", "--location", "--proto", "=https", "--proto-redir", "=https",
             "--connect-timeout", "30", "--max-time", "1800", "--max-filesize",
             str(manifest["size_bytes"]), "--output", str(partial), manifest["url"]],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=1810,
        )
        if result.returncode:
            raise Unavailable("snapshot download failed or exceeded its 30-minute budget")
        os.rename(partial, archive)
    if archive.stat().st_size != manifest["size_bytes"] or digest(archive) != manifest["sha256"].lower():
        raise Unavailable("snapshot size/checksum mismatch")
    restore(archive, base)
    result = subprocess.check_output([str(base / "bin/zakurad"), "-c", str(base / "zakurad.toml"),
                                      "tip-height", "--cache-dir", str(base),
                                      "--network", "Mainnet"], text=True, timeout=120)
    numeric = [line for line in result.splitlines() if re.fullmatch(r"[0-9]+", line)]
    if len(numeric) != 1:
        raise Unavailable("offline restored state did not return exactly one height")
    height = int(numeric[0])
    checkpoint = int((source / "crates/zakura-chain/src/parameters/checkpoint/main-checkpoints.txt").read_text().splitlines()[-1].split()[0])
    if height <= checkpoint:
        raise Unavailable("snapshot does not exceed mandatory checkpoint range")
    # Start temporary node so imported anchor can be inspected; stop before supervision.
    with (base / "evidence/bootstrap-node.log").open("a") as log:
        node = subprocess.Popen([str(base / "bin/zakurad"), "-c", str(base / "zakurad.toml"), "start"],
                                stdout=log, stderr=subprocess.STDOUT)
        try:
            local = RPC("http://127.0.0.1:28232")
            reference = RPC(args.reference_rpc)
            deadline = time.monotonic() + 600
            while True:
                if node.poll() is not None:
                    raise Unavailable("bootstrap node exited")
                try:
                    anchor = local.block(height)
                    if anchor != reference.block(height):
                        raise Unavailable("bootstrap anchor differs from reference")
                    break
                except Unavailable:
                    if time.monotonic() >= deadline:
                        raise Unavailable("bootstrap anchor could not be established")
                    time.sleep(5)
            receipt = {"schema_version": 1, "source_sha": SOURCE_SHA, "tooling_sha": args.tooling_sha,
                       "binary_sha256": digest(base / "bin/zakurad"),
                       "config_sha256": digest(base / "zakurad.toml"),
                       "cargo_lock_sha256": digest(source / "Cargo.lock"),
                       "toolchain": "1.97.1", "features": "default", "os": platform.platform(),
                       "bootstrap_height": height, "bootstrap_record": anchor,
                       "checkpoint_max": checkpoint, "snapshot": manifest,
                       "corpus": corpus, "deployed_at": time.time()}
        finally:
            node.terminate()
            try:
                node.wait(timeout=60)
            except subprocess.TimeoutExpired:
                node.kill()
                node.wait()
    finalize_bootstrap(base, archive, receipt)
    print("Bootstrap anchored; continuous coverage starts at", height + 1)


if __name__ == "__main__":
    main()
