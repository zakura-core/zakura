#!/usr/bin/env python3
"""Copy a retained, frozen approach fixture into a new regional bake volume.

Only a disposable clone is read. The source snapshot and running nodes are
untouched. The destination is the empty approach volume of this bake.
"""

import argparse
import shlex
import subprocess
import time

import do_provision as provision


def ssh(ip, command, **kwargs):
    return [
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=10",
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "UserKnownHostsFile=/dev/null",
        "-o",
        "ServerAliveInterval=30",
        "-o",
        "ServerAliveCountMax=6",
        "-i",
        kwargs.get("key", "/tmp/do_ssh"),
        f"root@{ip}",
        command,
    ]


def remote(ip, command, timeout=120):
    return subprocess.run(
        ssh(ip, command), check=True, capture_output=True, text=True, timeout=timeout
    ).stdout.strip()


def source_plans(source_args, candidates, images, sizes):
    """Keep the closest usable fixture for each regional capacity candidate."""
    result, seen = [], set()
    for snapshot in sorted(
        candidates,
        key=lambda s: (provision.height(s), s.get("created_at", "")),
        reverse=True,
    ):
        request = argparse.Namespace(**vars(source_args))
        request.regions = ",".join(snapshot["regions"])
        request.snapshot_id = str(snapshot["id"])
        for plan in provision.plans(request, images, candidates, sizes):
            key = (plan["region"], plan["size"]["slug"])
            if key not in seen:
                result.append(plan)
                seen.add(key)
    return result


def seed(args):
    snapshots = provision.doctl("snapshot", "list", "--resource", "volume")
    # Only dedicated approach fixtures have a single tip/ tree, unlike the
    # much larger ordinary Mainnet snapshots containing tip/ and sandblast/.
    candidates = [
        s
        for s in snapshots
        if s["name"].startswith("zakura-vct-approach-mainnet-")
        and provision.height(s) is not None
        and provision.height(s) < args.checkpoint
    ]
    if not candidates:
        raise RuntimeError(
            "no retained approach fixture; run an explicit approach rebuild"
        )
    source_args = provision.parser().parse_args(
        [
            "--name",
            args.name,
            "--policy",
            "correctness",
            "--size",
            "c-8",
            "--volume-name",
            args.name + "-vol",
            "--ssh-fingerprint",
            args.ssh_fingerprint,
        ]
    )
    if any(d["name"] == source_args.name for d in provision.doctl("droplet", "list")):
        raise RuntimeError("seed host name already exists; refusing to adopt it")
    if any(
        v["name"].startswith(source_args.volume_name + "-")
        for v in provision.doctl("volume", "list")
    ):
        raise RuntimeError("seed volume name already exists; refusing to adopt it")
    plans = source_plans(
        source_args,
        candidates,
        provision.doctl("image", "list-user"),
        provision.doctl("size", "list"),
    )
    if not plans:
        raise RuntimeError("no compatible host can read the retained approach fixture")
    source_args.regions = ",".join(dict.fromkeys(plan["region"] for plan in plans))
    source = None
    try:
        source = provision.provision(source_args, plans)
        snapshot = next(
            s for s in candidates if str(s["id"]) == str(source["state_snapshot_id"])
        )
        for attempt in range(90):
            try:
                remote(source["ip"], "true", timeout=15)
                break
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
                if attempt == 89:
                    raise TimeoutError("approach source SSH did not become ready")
                time.sleep(5)
        device = shlex.quote("/dev/disk/by-id/scsi-0DO_Volume_" + source["volume_name"])
        target_device = shlex.quote(
            "/dev/disk/by-id/scsi-0DO_Volume_" + args.volume_name
        )
        remote(
            source["ip"],
            "mkdir -p /mnt/approach-source && "
            f"mount -o ro,noload {device} /mnt/approach-source",
        )
        remote(
            args.ip,
            f"mkdir -p /mnt/bake-approach && mount {target_device} /mnt/bake-approach "
            '&& test -z "$(find /mnt/bake-approach -mindepth 1 -maxdepth 1 '
            '! -name lost+found -print -quit)"',
        )
        sender = subprocess.Popen(
            ssh(
                source["ip"], "tar -C /mnt/approach-source --exclude=lost+found -cf - ."
            ),
            stdout=subprocess.PIPE,
        )
        receiver = subprocess.Popen(
            ssh(args.ip, "tar -C /mnt/bake-approach -xf -"), stdin=sender.stdout
        )
        sender.stdout.close()
        try:
            receiver.wait(timeout=3600)
            sender.wait(timeout=60)
            if receiver.returncode or sender.returncode:
                raise RuntimeError(
                    "approach copy failed; destination will not be published"
                )
        finally:
            for process in (receiver, sender):
                if process.poll() is None:
                    process.kill()
                    process.wait()
        # The new binary must actually reopen this frozen DB before publication.
        value = remote(
            args.ip,
            "printf '[state]\\nstorage_mode = \"pruned\"\\n' "
            "> /root/inspect-approach.toml && "
            "/root/cargo-target/release/zakurad -c /root/inspect-approach.toml "
            "tip-height --cache-dir /mnt/bake-approach/tip --network Mainnet",
            timeout=300,
        )
        heights = [int(line) for line in value.splitlines() if line.isdigit()]
        if not heights or heights[-1] != provision.height(snapshot):
            raise RuntimeError("copied fixture height disagrees with source metadata")
        remote(
            args.ip,
            f"sync && umount /mnt/bake-approach && "
            f"printf '%s\\n' {heights[-1]} > /root/mainnet-approach-height",
        )
    finally:
        if source:
            provision.cleanup([source["id"]], [source["volume_id"]])


if __name__ == "__main__":
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("--name", required=True)
    cli.add_argument("--ip", required=True)
    cli.add_argument("--volume-name", required=True)
    cli.add_argument("--checkpoint", type=int, required=True)
    cli.add_argument("--ssh-fingerprint", required=True)
    seed(cli.parse_args())
