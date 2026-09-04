#!/usr/bin/env python3
"""Capacity, artifact compatibility, and partial provisioning regressions."""

import argparse
import json
import subprocess
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

import do_artifact_retention as retention
import do_provision as p
import do_seed_approach as seed


def size(slug, regions=("nyc1",), disk=100, price=0.25, cpu=8, memory=16384):
    return dict(
        slug=slug,
        regions=list(regions),
        available=True,
        disk=disk,
        price_hourly=price,
        vcpus=cpu,
        memory=memory,
    )


def image(region="nyc1", disk=100, id="image", created="2026-09-01T00:00:00Z"):
    return dict(
        id=id,
        name=f"zakura-pr-node-{id}",
        regions=[region],
        min_disk_size=disk,
        created_at=created,
        status="available",
    )


def snapshot(
    region="nyc1",
    height=90,
    id="state",
    prefix="zakura-vct-approach-mainnet-",
    created="2026-09-01T00:00:00Z",
):
    return dict(
        id=id,
        name=f"{prefix}{id}-h{height}",
        regions=[region],
        min_disk_size=100,
        created_at=created,
    )


def args(*extra):
    return p.parser().parse_args(
        ["--name", "zakura-pr-test", "--volume-name", "zakura-pr-vol-test", *extra]
    )


class Selection(unittest.TestCase):
    def test_nyc3_incident_uses_shared_cpu_only_with_explicit_policy(self):
        sizes = [
            size("c-8"),
            size("c-8-intel"),
            size("s-8vcpu-16gb", ["nyc3"], disk=320),
        ]
        request = args("--regions", "nyc3", "--policy", "correctness")
        plans = p.plans(request, [image("nyc3")], [], sizes)
        self.assertEqual([v["size"]["slug"] for v in plans], ["s-8vcpu-16gb"])
        request.policy = "fixed"
        self.assertEqual(p.plans(request, [image("nyc3")], [], sizes), [])

    def test_region_requires_both_image_and_precheckpoint_fixture(self):
        request = args(
            "--network", "mainnet", "--mode", "pre-checkpoint", "--checkpoint", "100"
        )
        images = [image("nyc1"), image("sfo3", id="second")]
        snapshots = [snapshot("nyc1", 100), snapshot("sfo3", 90)]
        plans = p.plans(request, images, snapshots, [size("c-8", ["nyc1", "sfo3"])])
        self.assertEqual([plan["region"] for plan in plans], ["sfo3"])

    def test_exact_artifacts_never_substituted(self):
        request = args("--image-id", "exact", "--snapshot-id", "chosen")
        self.assertEqual(
            p.plans(
                request, [image(id="wrong")], [snapshot(id="chosen")], [size("c-8")]
            ),
            [],
        )
        self.assertEqual(
            p.plans(
                request, [image(id="exact")], [snapshot(id="wrong")], [size("c-8")]
            ),
            [],
        )

    def test_exact_snapshot_still_must_be_below_handoff(self):
        self.assertIsNone(
            p.select_state(
                [snapshot(height=100)],
                "nyc1",
                "mainnet",
                "pre-checkpoint",
                100,
                "state",
            )
        )

    def test_highest_height_below_checkpoint_wins_even_if_older(self):
        states = [
            snapshot(height=95, id="older", created="2026-08-01T00:00:00Z"),
            snapshot(height=90),
        ]
        self.assertEqual(
            p.select_state(states, "nyc1", "mainnet", "pre-checkpoint", 100)["id"],
            "older",
        )

    def test_legacy_unknown_height_is_not_a_verified_handoff_fixture(self):
        state = snapshot(prefix="zakura-pr-state-mainnet-")
        state["name"] = "zakura-pr-state-mainnet-20260727-0814"
        self.assertIsNone(
            p.select_state([state], "nyc1", "mainnet", "pre-checkpoint", 100)
        )
        self.assertEqual(p.select_state([state], "nyc1", "mainnet", "tip"), state)

    def test_bake_cannot_inflate_root_disk(self):
        sizes = [
            size("c-8"),
            size("shared", disk=320, price=0.1),
            size("large", cpu=16, memory=32768, disk=100, price=0.6),
        ]
        self.assertEqual(
            [
                s["slug"]
                for s in p.eligible_sizes(sizes, "nyc1", "c-8", "bake", 100, 0.5)
            ],
            ["c-8"],
        )

    def test_catalog_entries_without_regions_are_ineligible(self):
        omitted = size("not-offered")
        del omitted["regions"]
        self.assertEqual(
            p.eligible_sizes([omitted, size("c-8")], "nyc1", "c-8", "fixed", 100, 0.5),
            [size("c-8")],
        )

    def test_correctness_enforces_memory_cpu_disk_and_price(self):
        sizes = [
            size("c-8"),
            size("small-cpu", cpu=4),
            size("small-ram", memory=8192),
            size("small-disk", disk=80),
            size("expensive", price=0.51),
        ]
        self.assertEqual(
            [
                s["slug"]
                for s in p.eligible_sizes(sizes, "nyc1", "c-8", "correctness", 100, 0.5)
            ],
            ["c-8"],
        )

    def test_older_compatible_image_survives_oversized_latest(self):
        images = [image(disk=320, created="2026-09-02T00:00:00Z"), image(id="older")]
        result = p.plans(args(), images, [], [size("c-8")])
        self.assertEqual(result[0]["image"]["id"], "older")

    def test_unavailable_image_is_not_selected(self):
        candidate = image()
        candidate["status"] = "pending"
        self.assertEqual(p.plans(args(), [candidate], [], [size("c-8")]), [])

    def test_older_image_adds_sizes_without_repeating_capacity_pools(self):
        images = [
            image(disk=320, created="2026-09-02T00:00:00Z"),
            image(id="older"),
            image(id="oldest", created="2026-08-01T00:00:00Z"),
        ]
        sizes = [size("c-8"), size("shared", disk=320)]
        request = args("--policy", "correctness")
        result = p.plans(request, images, [], sizes)
        self.assertEqual(
            [(plan["image"]["id"], plan["size"]["slug"]) for plan in result],
            [("image", "shared"), ("older", "c-8")],
        )
        request.image_id = "image"
        result = p.plans(request, images, [], sizes)
        self.assertEqual(
            [(plan["image"]["id"], plan["size"]["slug"]) for plan in result],
            [("image", "shared")],
        )


class Lifecycle(unittest.TestCase):
    @patch.object(p, "output")
    @patch.object(p, "wait_droplet", return_value="192.0.2.1")
    @patch.object(p, "doctl")
    def test_capacity_rejection_falls_back_to_size_enabled_by_older_image(
        self, api, wait, output
    ):
        request = args("--policy", "correctness", "--regions", "nyc1")
        plans = p.plans(
            request,
            [image(disk=320, created="2026-09-02T00:00:00Z"), image(id="older")],
            [],
            [size("c-8"), size("shared", disk=320)],
        )
        api.side_effect = [
            subprocess.CalledProcessError(1, "doctl", stderr="422 capacity exhausted"),
            [{"id": 2}],
        ]
        result = p.provision(request, plans)
        self.assertEqual(result["image_id"], "older")
        self.assertEqual(result["size"], "c-8")
        creates = [call.args for call in api.call_args_list]
        self.assertEqual(
            [
                (call[call.index("--image") + 1], call[call.index("--size") + 1])
                for call in creates
            ],
            [("image", "shared"), ("older", "c-8")],
        )

    @patch.object(p.subprocess, "run")
    def test_json_stdout_capacity_error_is_classified(self, run):
        run.return_value = subprocess.CompletedProcess(
            ["doctl"],
            1,
            stdout=json.dumps(
                {
                    "errors": [
                        {
                            "detail": "POST https://api.digitalocean.com/v2/droplets: "
                            "422 Size is not available in this region"
                        }
                    ]
                }
            ),
            stderr="",
        )
        with self.assertRaises(subprocess.CalledProcessError) as caught:
            p.doctl("droplet", "create", "zakura-pr-test")
        self.assertTrue(p.capacity_rejection(caught.exception))
        self.assertEqual(run.call_args.args[0][-1], "0")

    def plans(self):
        request = args(
            "--network", "mainnet", "--policy", "correctness", "--regions", "nyc1,sfo3"
        )
        snapshots = [
            snapshot(r, prefix="zakura-pr-state-mainnet-") for r in ("nyc1", "sfo3")
        ]
        plans = p.plans(
            request,
            [image(r) for r in ("nyc1", "sfo3")],
            snapshots,
            [size("c-8", ["nyc1", "sfo3"])],
        )
        return request, plans

    @patch.object(p, "output")
    @patch.object(p, "wait_droplet", return_value="192.0.2.1")
    @patch.object(p, "doctl")
    def test_capacity_rejection_deletes_clone_before_next_region(
        self, api, wait, output
    ):
        request, plans = self.plans()
        api.side_effect = [
            [{"id": "v1", "name": "vol1"}],
            subprocess.CalledProcessError(
                1, "doctl", stderr="422 Size is not available in this region"
            ),
            None,
            [{"id": "v2", "name": "vol2"}],
            [{"id": 2}],
        ]
        result = p.provision(request, plans)
        self.assertEqual(result["region"], "sfo3")
        self.assertEqual(
            api.call_args_list[2].args, ("volume", "delete", "v1", "--force")
        )
        self.assertEqual(result["volume_id"], "v2")

    @patch.object(p, "output")
    @patch.object(p, "doctl")
    def test_ambiguous_timeout_recovers_and_deletes_host_without_retry(
        self, api, output
    ):
        request, plans = self.plans()
        api.side_effect = [
            [{"id": "v1", "name": "vol1"}],
            subprocess.TimeoutExpired("doctl", 180),
            [{"id": 4, "name": request.name}],
            [],
            None,
            None,
        ]
        with self.assertRaises(subprocess.TimeoutExpired):
            p.provision(request, plans)
        creates = [c for c in api.call_args_list if c.args[:2] == ("droplet", "create")]
        self.assertEqual(len(creates), 1)
        self.assertIn(
            unittest.mock.call("droplet", "delete", 4, "--force"), api.call_args_list
        )

    @patch.object(p, "output")
    @patch.object(p, "doctl")
    def test_authentication_error_does_not_try_more_sizes(self, api, output):
        request, plans = self.plans()
        api.side_effect = [
            [{"id": "v1", "name": "vol1"}],
            subprocess.CalledProcessError(1, "doctl", stderr="401 unauthorized"),
            [],
            [],
            None,
        ]
        with self.assertRaises(subprocess.CalledProcessError):
            p.provision(request, plans)
        self.assertEqual(
            sum(c.args[:2] == ("droplet", "create") for c in api.call_args_list), 1
        )

    def test_bake_names_do_not_collide_between_matrix_regions(self):
        first, second = (
            args("--policy", "bake", "--regions", "nyc1"),
            args("--policy", "bake", "--regions", "sfo3"),
        )
        self.assertFalse(p.volume_names(first) & p.volume_names(second))


class Retention(unittest.TestCase):
    def test_keep_two_images_in_each_region(self):
        images = [
            image(r, id=f"{r}-{n}", created=f"2026-09-0{n}T00:00:00Z")
            for r in ("nyc1", "sfo3", "nyc3")
            for n in (1, 2, 3)
        ]
        kept = retention.retained_ids(images, [], 100)
        self.assertEqual(
            kept, {f"{r}-{n}" for r in ("nyc1", "sfo3", "nyc3") for n in (2, 3)}
        )

    def test_pin_older_precheckpoint_snapshot_in_each_region(self):
        states = [
            snapshot(r, n, id=f"{r}-{n}", created=f"2026-09-0{n // 50}T00:00:00Z")
            for r in ("nyc1", "sfo3")
            for n in (50, 100, 150, 200)
        ]
        kept = retention.retained_ids([], states, 100)
        self.assertEqual(
            kept, {f"{r}-{n}" for r in ("nyc1", "sfo3") for n in (50, 150, 200)}
        )

    def test_stale_or_missing_regions_are_visible(self):
        now = datetime(2026, 9, 4, tzinfo=timezone.utc)
        self.assertEqual(retention.stale_regions([image("nyc1")], now), ["sfo3"])
        self.assertEqual(
            retention.stale_regions(
                [image("nyc1", created="2026-07-01T00:00:00Z")], now
            ),
            ["nyc1", "sfo3"],
        )


class ApproachCopy(unittest.TestCase):
    def test_seed_can_use_another_region_when_newest_source_is_unavailable(self):
        request = args("--policy", "correctness")
        states = [snapshot("nyc1", 95, id="new"), snapshot("sfo3", 90, id="old")]
        plans = seed.source_plans(
            request, states, [image("sfo3")], [size("c-8", ["nyc1", "sfo3"])]
        )
        self.assertEqual([plan["state"]["id"] for plan in plans], ["old"])

    def test_seed_does_not_retry_one_capacity_pool_for_multiple_snapshots(self):
        request = args("--policy", "correctness")
        states = [snapshot(height=95, id="new"), snapshot(height=90, id="old")]
        plans = seed.source_plans(request, states, [image()], [size("c-8")])
        self.assertEqual([plan["state"]["id"] for plan in plans], ["new"])

    @patch.object(seed.provision, "cleanup")
    @patch.object(seed.provision, "provision")
    @patch.object(seed, "source_plans", return_value=[{"region": "nyc1"}])
    @patch.object(seed.provision, "doctl")
    @patch.object(seed, "remote")
    @patch.object(seed.subprocess, "Popen")
    def test_wrong_height_never_publishes_and_cleans_source(
        self, popen, remote, api, plans, provision, cleanup
    ):
        request = argparse.Namespace(
            name="zakura-pr-seed-test",
            ip="192.0.2.2",
            volume_name="zakura-pr-bake-approach",
            checkpoint=100,
            ssh_fingerprint="test-key",
        )
        api.side_effect = [[snapshot()], [], [], [image()], [size("c-8")]]
        provision.return_value = dict(
            id=1,
            ip="192.0.2.1",
            volume_id="v1",
            volume_name="source",
            state_snapshot_id="state",
        )
        process = unittest.mock.MagicMock()
        process.returncode = 0
        popen.return_value = process
        remote.side_effect = ["", "", "", "89"]
        with self.assertRaisesRegex(RuntimeError, "height disagrees"):
            seed.seed(request)
        cleanup.assert_called_once_with([1], ["v1"])
        self.assertFalse(
            any(
                "mainnet-approach-height" in call.args[1]
                for call in remote.call_args_list
            )
        )
        api.reset_mock(side_effect=True)
        api.side_effect = [[snapshot()], [], [], [image()], [size("c-8")]]
        remote.reset_mock(side_effect=True)
        remote.side_effect = ["", "", "", "90", ""]
        cleanup.reset_mock()
        seed.seed(request)
        cleanup.assert_called_once_with([1], ["v1"])
        self.assertIn("mainnet-approach-height", remote.call_args.args[1])


if __name__ == "__main__":
    unittest.main()
