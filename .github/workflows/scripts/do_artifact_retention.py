#!/usr/bin/env python3
"""Retain usable CI artifacts independently in each region."""

import argparse
from datetime import datetime, timedelta, timezone

from do_provision import REGIONS, doctl, handoff_height, height, newest, output

VALIDATION_PREFIX = "zakura-pr-validation-"
VALIDATION_TTL = timedelta(days=7)


def expired_validation_ids(items, now):
    """Return IDs of branch-bake validation artifacts older than `VALIDATION_TTL`.

    Scheduled jobs never select these. Only PR-node runs given their exact IDs
    use them, and a validation pass finishes well inside the TTL.
    """
    return {
        str(i["id"])
        for i in items
        if i["name"].startswith(VALIDATION_PREFIX)
        and now - datetime.fromisoformat(i["created_at"].replace("Z", "+00:00"))
        > VALIDATION_TTL
    }


def retained_ids(images, snapshots, checkpoint):
    keep = set()
    regions = {region for item in images + snapshots for region in item["regions"]}
    for region in regions:
        regional_images = [
            i
            for i in images
            if region in i["regions"] and i.get("status") == "available"
        ]
        keep.update(str(i["id"]) for i in newest(regional_images)[:2])
        for prefix, count in (
            ("zakura-pr-state-mainnet-", 6),
            ("zakura-pr-state-testnet-", 3),
            ("zakura-vct-approach-mainnet-", 2),
        ):
            items = [
                s
                for s in snapshots
                if region in s["regions"] and s["name"].startswith(prefix)
            ]
            keep.update(str(s["id"]) for s in newest(items)[:count])
        # A recent fixture may already be beyond C; retain the closest usable
        # ordinary AND dedicated approach fixture, independently of their ages.
        for prefix in ("zakura-pr-state-mainnet-", "zakura-vct-approach-mainnet-"):
            items = [
                s
                for s in snapshots
                if region in s["regions"] and s["name"].startswith(prefix)
            ]
            before = [
                s
                for s in items
                if handoff_height(s) is not None and handoff_height(s) < checkpoint
            ]
            if before:
                keep.add(
                    str(
                        max(before, key=lambda s: (height(s), s.get("created_at", "")))[
                            "id"
                        ]
                    )
                )
            elif prefix == "zakura-pr-state-mainnet-":
                # Retain the historical date-only transition asset too.
                legacy = newest([s for s in items if height(s) is None])
                if legacy:
                    keep.add(str(legacy[-1]["id"]))
    return keep


def stale_regions(images, now):
    stale = []
    for region in REGIONS[:2]:
        regional = newest(
            [
                i
                for i in images
                if region in i["regions"] and i.get("status") == "available"
            ]
        )
        if not regional or now - datetime.fromisoformat(
            regional[0]["created_at"].replace("Z", "+00:00")
        ) > timedelta(days=14):
            stale.append(region)
    return stale


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("--checkpoint", type=int, required=True)
    cli.add_argument("--apply", action="store_true")
    args = cli.parse_args()
    if args.checkpoint <= 0:
        raise ValueError("checkpoint must be positive before pruning")
    all_images = doctl("image", "list-user")
    all_snapshots = doctl("snapshot", "list", "--resource", "volume")
    images = [i for i in all_images if i["name"].startswith("zakura-pr-node-")]
    snapshots = [
        s
        for s in all_snapshots
        if s["name"].startswith(
            (
                "zakura-pr-state-mainnet-",
                "zakura-pr-state-testnet-",
                "zakura-vct-approach-mainnet-",
            )
        )
    ]
    keep = retained_ids(images, snapshots, args.checkpoint)
    now = datetime.now(timezone.utc)
    stale = stale_regions(images, now)
    output(stale_regions=",".join(stale), notify_stale=str(now.hour == 12).lower())
    for region in stale:
        print(
            f"::warning::CI image missing or older than 14 days in {region}; "
            "repair the regional bake"
        )
    for kind, items in (("image", images), ("snapshot", snapshots)):
        for item in items:
            # Never race an in-progress bake or remove unpublished images.
            recent = now - datetime.fromisoformat(
                item["created_at"].replace("Z", "+00:00")
            ) < timedelta(days=1)
            if (
                str(item["id"]) in keep
                or recent
                or (kind == "image" and item.get("status") != "available")
            ):
                continue
            print(f"Prune {kind} {item['id']} ({item['name']})")
            if args.apply:
                doctl(kind, "delete", item["id"], "--force")
    expired = expired_validation_ids(all_images + all_snapshots, now)
    for kind, items in (("image", all_images), ("snapshot", all_snapshots)):
        for item in items:
            if str(item["id"]) not in expired or (
                kind == "image" and item.get("status") != "available"
            ):
                continue
            print(f"Prune expired validation {kind} {item['id']} ({item['name']})")
            if args.apply:
                doctl(kind, "delete", item["id"], "--force")


if __name__ == "__main__":
    main()
