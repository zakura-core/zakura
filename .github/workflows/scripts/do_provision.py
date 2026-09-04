#!/usr/bin/env python3
"""Plan and provision ephemeral CI hosts from compatible regional artifacts.

The default policy preserves the requested size. Correctness jobs opt into
resource-based selection; image builders additionally require a 100 GB disk.
All API calls use the caller's existing doctl authentication.
"""

import argparse
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

REGIONS = ("nyc1", "sfo3", "nyc3")
TAGS = {"zakura-pr-node", "zakura-image-bake", "zakura-mempool-load"}


def doctl(*args):
    """Run one bounded API operation; never retry an ambiguous mutation."""
    retries = "3" if args[1] in {"get", "list", "list-user"} else "0"
    result = subprocess.run(
        [
            "doctl",
            "compute",
            *map(str, args),
            "--output",
            "json",
            "--http-retry-max",
            retries,
        ],
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )
    if result.returncode:
        # doctl's JSON mode puts API errors on stdout, unlike text mode.
        message = result.stderr
        try:
            errors = json.loads(result.stdout).get("errors", [])
            message += "\n".join(error["detail"] for error in errors)
        except (ValueError, AttributeError, KeyError, TypeError):
            pass
        raise subprocess.CalledProcessError(
            result.returncode,
            result.args,
            stderr=message or "doctl failed without a structured error",
        )
    return json.loads(result.stdout) if result.stdout.strip() else None


def one(value):
    return value[0] if isinstance(value, list) else value


def output(**values):
    if os.environ.get("GITHUB_OUTPUT"):
        with open(os.environ["GITHUB_OUTPUT"], "a") as stream:
            for key, value in values.items():
                stream.write(f"{key}={value}\n")


def volume_names(args):
    """Exact names this invocation may allocate, including its region suffix."""
    if args.policy == "bake":
        return {
            f"{args.volume_name}-{n}-{r}"
            for r in args.regions.split(",")
            for n in ("mainnet", "testnet", "approach")
        }
    return {f"{args.volume_name}-{r}" for r in args.regions.split(",")}


def height(snapshot):
    match = re.search(r"-h(\d+)$", snapshot["name"])
    return int(match[1]) if match else None


def newest(items):
    return sorted(
        items, key=lambda item: (item.get("created_at", ""), item["name"]), reverse=True
    )


def select_state(snapshots, region, network, mode, checkpoint=None, snapshot_id=""):
    """Pick a regional fixture, preserving exact IDs and the handoff boundary."""
    regional = [s for s in snapshots if region in s["regions"]]
    if snapshot_id:
        selected = next((s for s in regional if str(s["id"]) == snapshot_id), None)
        if selected and mode == "pre-checkpoint":
            if (
                not checkpoint
                or height(selected) is None
                or height(selected) >= checkpoint
            ):
                return None
        return selected
    if not network:
        return None
    states = [
        s for s in regional if s["name"].startswith(f"zakura-pr-state-{network}-")
    ]
    if mode != "pre-checkpoint":
        return next(iter(newest(states)), None)
    if not checkpoint or checkpoint <= 0:
        raise ValueError("pre-checkpoint mode requires a positive checkpoint")
    if network == "mainnet":
        states += [
            s for s in regional if s["name"].startswith("zakura-vct-approach-mainnet-")
        ]
    candidates = [s for s in states if height(s) is not None and height(s) < checkpoint]
    # Unknown-height legacy fixtures remain usable for tip/sandblast runs only.
    return max(
        candidates, key=lambda s: (height(s), s.get("created_at", "")), default=None
    )


def eligible_sizes(sizes, region, requested, policy, disk, max_price):
    requested_size = next((s for s in sizes if s["slug"] == requested), None)
    if requested_size is None:
        raise ValueError(f"unknown requested size: {requested}")
    candidates = []
    for size in sizes:
        if (
            not size.get("available")
            or region not in (size.get("regions") or [])
            or size["disk"] < disk
        ):
            continue
        if policy == "fixed":
            if size["slug"] != requested:
                continue
        else:
            if (
                size["vcpus"] < requested_size["vcpus"]
                or size["memory"] < requested_size["memory"]
            ):
                continue
            if size["price_hourly"] > max_price:
                continue
            if policy == "bake" and size["disk"] != 100:
                continue
        candidates.append(size)
    return sorted(
        candidates, key=lambda s: (s["slug"] != requested, s["price_hourly"], s["slug"])
    )


def plans(args, images, snapshots, sizes):
    """Resolve complete region/image/state/size tuples before allocating anything."""
    result = []
    for region in args.regions.split(","):
        state = select_state(
            snapshots,
            region,
            args.network,
            args.mode,
            args.checkpoint,
            args.snapshot_id,
        )
        if (args.network or args.snapshot_id) and state is None:
            continue
        if args.policy == "bake":
            choices = [{"id": "ubuntu-24-04-x64", "min_disk_size": 100}]
        else:
            choices = newest(
                [
                    image
                    for image in images
                    if region in image["regions"]
                    and image.get("status") == "available"
                    and (
                        str(image["id"]) == args.image_id
                        if args.image_id
                        else image["name"].startswith("zakura-pr-node-")
                    )
                ]
            )
        planned_sizes = set()
        for image in choices:
            candidates = eligible_sizes(
                sizes,
                region,
                args.size,
                args.policy,
                image["min_disk_size"],
                args.max_price,
            )
            for size in candidates:
                if size["slug"] in planned_sizes:
                    continue
                # Keep the newest compatible image for each regional size.
                planned_sizes.add(size["slug"])
                result.append(
                    {"region": region, "image": image, "state": state, "size": size}
                )
    return result


def cleanup(droplets, volumes):
    """Delete only resources allocated by this invocation, with detach retries."""
    failed = []
    for kind, ids in (("droplet", droplets), ("volume", volumes)):
        for resource_id in ids:
            for attempt in range(12):
                try:
                    doctl(kind, "delete", resource_id, "--force")
                    break
                except subprocess.CalledProcessError as error:
                    if "404" in error.stderr:
                        break
                    if attempt == 11:
                        failed.append(str(resource_id))
                    else:
                        time.sleep(5)
    if failed:
        raise RuntimeError(
            f"cleanup incomplete; tagged resources require reaper: {failed}"
        )


def wait_droplet(resource_id):
    for _ in range(120):
        droplet = one(doctl("droplet", "get", resource_id))
        ips = [
            n["ip_address"]
            for n in droplet.get("networks", {}).get("v4", [])
            if n["type"] == "public"
        ]
        if droplet["status"] == "active" and ips:
            return ips[0]
        time.sleep(5)
    raise TimeoutError(f"droplet {resource_id} did not become active in 10 minutes")


def capacity_rejection(error):
    # Only a rejected create is safe to retry. A timeout can hide a live host.
    message = error.stderr.lower()
    return "422" in message and any(
        text in message for text in ("capacity", "size is not available")
    )


def provision(args, candidates):
    allocated_droplets, allocated_volumes = [], []
    success = False
    try:
        for plan in candidates:
            region, size, state = plan["region"], plan["size"]["slug"], plan["state"]
            print(
                f"Trying {region}/{size} with image {plan['image']['id']}", flush=True
            )
            volumes = []
            if args.policy == "bake":
                volume_specs = [
                    (network, f"{args.volume_name}-{network}-{region}", gb, None)
                    for network, gb in (
                        ("mainnet", 300),
                        ("testnet", 100),
                        ("approach", 100),
                    )
                ]
            elif state:
                volume_specs = [
                    (
                        "",
                        f"{args.volume_name}-{region}",
                        state["min_disk_size"],
                        state["id"],
                    )
                ]
            else:
                volume_specs = []
            for key, name, gb, snapshot_id in volume_specs:
                # Publish deterministic names before the API call, so workflow
                # teardown can recover IDs even if this process is killed.
                output(**{f"{key}_vol" if key else "volume_name": name})
                command = [
                    "volume",
                    "create",
                    name,
                    "--region",
                    region,
                    "--size",
                    f"{gb}GiB",
                ]
                if snapshot_id:
                    command += ["--snapshot", snapshot_id]
                volume = one(doctl(*command))
                allocated_volumes.append(volume["id"])
                volumes.append(volume)
                if key:
                    output(**{f"{key}_vol_id": volume["id"], f"{key}_vol": name})
                else:
                    output(volume_id=volume["id"], volume_name=name)
            command = [
                "droplet",
                "create",
                args.name,
                "--region",
                region,
                "--size",
                size,
                "--image",
                plan["image"]["id"],
                "--ssh-keys",
                args.ssh_fingerprint,
                "--tag-name",
                args.tag,
            ]
            for volume in volumes:
                command += ["--volumes", volume["id"]]
            try:
                droplet = one(doctl(*command))
            except subprocess.CalledProcessError as error:
                if not capacity_rejection(error):
                    raise
                # A known rejected request cannot have created a droplet.
                cleanup([], allocated_volumes)
                allocated_volumes.clear()
                print(
                    f"Capacity rejected in {region}/{size}; "
                    "trying the next compatible plan",
                    flush=True,
                )
                continue
            allocated_droplets.append(droplet["id"])
            output(id=droplet["id"])
            ip = wait_droplet(droplet["id"])
            metadata = dict(
                id=droplet["id"],
                ip=ip,
                region=region,
                size=size,
                image_id=plan["image"]["id"],
                state_snapshot_id=state["id"] if state else "",
                snapshot_height=height(state)
                if state and height(state) is not None
                else "",
            )
            if args.policy != "bake":
                metadata.update(
                    volume_id=volumes[0]["id"] if volumes else "",
                    volume_name=volumes[0]["name"] if volumes else "",
                )
            output(**metadata)
            if args.metadata:
                Path(args.metadata).write_text(json.dumps(metadata, indent=2) + "\n")
            if os.environ.get("GITHUB_STEP_SUMMARY"):
                with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as stream:
                    stream.write(
                        f"\nProvisioned `{region}/{size}`, "
                        f"image `{metadata['image_id']}`, "
                        f"state `{metadata['state_snapshot_id'] or 'none'}`.\n"
                    )
            success = True
            return metadata
        raise RuntimeError(
            "all compatible plans rejected for capacity; no node was started"
        )
    finally:
        if not success:
            # Recover IDs after an API timeout without reissuing the create.
            found = doctl("droplet", "list", "--tag-name", args.tag)
            allocated_droplets += [
                d["id"]
                for d in found
                if d["name"] == args.name and d["id"] not in allocated_droplets
            ]
            names = volume_names(args)
            found_volumes = doctl("volume", "list") if args.volume_name else []
            allocated_volumes += [
                v["id"]
                for v in found_volumes
                if v["name"] in names and v["id"] not in allocated_volumes
            ]
            cleanup(allocated_droplets, allocated_volumes)


def parser():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("--name", required=True)
    cli.add_argument("--size", default="c-8")
    cli.add_argument(
        "--policy", choices=("fixed", "correctness", "bake"), default="fixed"
    )
    cli.add_argument("--regions", default=",".join(REGIONS))
    cli.add_argument("--max-price", type=float, default=0.5)
    cli.add_argument("--ssh-fingerprint", default="")
    cli.add_argument("--tag", choices=sorted(TAGS), default="zakura-pr-node")
    cli.add_argument("--image-id", default="")
    cli.add_argument("--snapshot-id", default="")
    cli.add_argument("--network", choices=("", "mainnet", "testnet"), default="")
    cli.add_argument(
        "--mode",
        choices=("tip", "sandblast", "pre-checkpoint", "genesis"),
        default="tip",
    )
    cli.add_argument("--checkpoint", type=int)
    cli.add_argument("--volume-name", default="")
    cli.add_argument("--metadata", default="")
    cli.add_argument("--plan", action="store_true", help="read-only catalog validation")
    return cli


def main():
    args = parser().parse_args()
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,62}", args.name):
        raise ValueError("invalid droplet name")
    if (args.network or args.snapshot_id or args.policy == "bake") and not re.fullmatch(
        r"zakura-pr-[a-z0-9-]+", args.volume_name
    ):
        raise ValueError("state volume names must start with zakura-pr- for the reaper")
    if args.policy == "bake" and (args.network or args.snapshot_id or args.image_id):
        raise ValueError("bakes use stock Ubuntu and blank volumes")
    if not all(
        re.fullmatch(r"[a-z]+[0-9]+", region) for region in args.regions.split(",")
    ):
        raise ValueError("regions must be comma-separated DigitalOcean region slugs")
    sizes = doctl("size", "list")
    images = (
        doctl("image", "list-user")
        if not args.image_id
        else [one(doctl("image", "get", args.image_id))]
    )
    snapshots = doctl("snapshot", "list", "--resource", "volume")
    candidates = plans(args, images, snapshots, sizes)
    if not candidates:
        raise RuntimeError(
            "no region has a compatible image, state snapshot and size; "
            "provisioning did not start"
        )
    if args.plan:
        print(json.dumps(candidates, indent=2))
        return
    if not args.ssh_fingerprint:
        raise ValueError("an SSH key fingerprint is required to create a CI host")
    # Names include the workflow run/attempt; collisions are never adopted/deleted.
    if any(d["name"] == args.name for d in doctl("droplet", "list")):
        raise ValueError(f"droplet name already exists: {args.name}")
    if args.volume_name and any(
        v["name"] in volume_names(args) for v in doctl("volume", "list")
    ):
        raise ValueError(f"volume prefix already exists: {args.volume_name}")
    provision(args, candidates)


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as error:
        print(error.stderr, file=sys.stderr)
        sys.exit(1)
