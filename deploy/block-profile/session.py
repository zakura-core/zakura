#!/usr/bin/env python3
"""Create or cleanly stop a disposable profiling droplet, preserving its profile volume."""
import argparse
import ipaddress
import json
import re
import subprocess
import time

TAG = "zakura-profile-session"


def do(*args):
    result = subprocess.run(["doctl", *args, "--output", "json"], capture_output=True, text=True, check=True, timeout=120)
    data = json.loads(result.stdout) if result.stdout.strip() else None
    if isinstance(data, dict) and data.get("errors"):
        raise RuntimeError("DigitalOcean request failed. Check local doctl authentication.")
    return data


def resource(kind, identity):
    rows = do("compute", kind, "get", str(identity))
    if not isinstance(rows, list) or len(rows) != 1:
        raise RuntimeError("Expected one exact DigitalOcean resource")
    return rows[0]


def retained_volume(identity):
    volume = resource("volume", identity)
    if not volume["name"].startswith("zakura-profile-store-"):
        raise ValueError("Only a zakura-profile-store-* volume may be attached or detached")
    return volume


def stop(droplet_id, volume_id):
    droplet = resource("droplet", droplet_id)
    volume = retained_volume(volume_id)
    if TAG not in droplet.get("tags", []) or not droplet["name"].startswith("zakura-profile-"):
        raise ValueError("Refusing to stop a droplet outside the disposable profiling fleet")
    if volume_id not in droplet.get("volume_ids", []) or droplet_id not in volume.get("droplet_ids", []):
        raise ValueError("The exact volume is not attached to the exact droplet")
    addresses = [item["ip_address"] for item in droplet["networks"]["v4"] if item["type"] == "public"]
    if len(addresses) != 1:
        raise ValueError("No unique public management address")
    ip = str(ipaddress.IPv4Address(addresses[0]))
    # No acceptance of a new SSH host key, no unbounded wait, and no forced unmount.
    command = "systemctl stop zakura-profile-sampler.service && systemctl stop zakurad.service && systemctl stop zakura-profile-report.timer zakura-profile-web.service && systemctl stop zakura-profile-collector.service && systemctl start zakura-profile-report.service && sync && umount /srv/zakura-profile"
    subprocess.run(["ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes", "-o", "ConnectTimeout=15", f"root@{ip}", command], check=True, timeout=240)
    do("compute", "volume-action", "detach", volume_id, str(droplet_id), "--wait")
    if retained_volume(volume_id).get("droplet_ids"):
        raise RuntimeError("Volume remains attached. Droplet was not deleted.")
    do("compute", "droplet", "delete", str(droplet_id), "--force")
    # There is deliberately no volume-delete path in this controller.
    print(json.dumps({"deleted_droplet": droplet_id, "retained_volume": volume_id}))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    create = sub.add_parser("create")
    create.add_argument("--image", required=True, help="Exact approved dedicated pruned-node image ID")
    create.add_argument("--volume-id", required=True)
    create.add_argument("--ssh-key", required=True)
    create.add_argument("--name", required=True)
    create.add_argument("--size", default="c-16")
    create.add_argument("--region", default="nyc1")
    create.add_argument("--hours", type=int, default=24)
    delete = sub.add_parser("stop")
    delete.add_argument("--droplet-id", required=True, type=int)
    delete.add_argument("--volume-id", required=True)
    reap = sub.add_parser("reap")
    args = parser.parse_args()
    if args.command == "create":
        if not re.fullmatch(r"zakura-profile-[a-z0-9-]{1,40}", args.name) or not 1 <= args.hours <= 24:
            parser.error("Use a zakura-profile-* name and a 1–24 hour session")
        volume = retained_volume(args.volume_id)
        if volume.get("droplet_ids") or volume["region"]["slug"] != args.region:
            raise ValueError("Retained volume must be detached and in the selected region")
        end = int(time.time()) + args.hours * 3600
        result = do("compute", "droplet", "create", args.name, "--image", args.image, "--size", args.size, "--region", args.region, "--ssh-keys", args.ssh_key, "--volumes", args.volume_id, "--tag-names", f"{TAG},zakura-profile-expires-{end}", "--wait")
        print(json.dumps(result, indent=2))
    elif args.command == "stop":
        stop(args.droplet_id, args.volume_id)
    else:
        for droplet in do("compute", "droplet", "list", "--tag-name", TAG):
            ends = [int(tag.removeprefix("zakura-profile-expires-")) for tag in droplet["tags"] if re.fullmatch(r"zakura-profile-expires-\d+", tag)]
            if len(ends) == 1 and ends[0] <= time.time():
                volumes = [identity for identity in droplet.get("volume_ids", []) if resource("volume", identity)["name"].startswith("zakura-profile-store-")]
                if len(volumes) != 1:
                    raise ValueError("Expired session has no unique retained volume. Manual inspection required.")
                stop(droplet["id"], volumes[0])


if __name__ == "__main__":
    main()
