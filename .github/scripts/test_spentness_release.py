"""Exercise spentness release verification before publisher/importer rollout."""

import copy
import hashlib
import importlib.util
import json
import os
import subprocess
import struct
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

import spentness_release as hints


def load_script(name):
    spec = importlib.util.spec_from_file_location(
        name, Path(__file__).with_name(name + ".py")
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class SpentnessReleaseTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.bundle = self.root / "bundle"
        self.bundle.mkdir()
        self.pin = {
            "chain_identity": list(range(32)),
            "terminal_height": 10,
            "terminal_block_hash": list(range(32, 64)),
            "format_version": hints.FORMAT_VERSION,
            "output_count": 9,
            "byte_len": hints.HEADER.size + 2,
        }
        data = (
            hints.HEADER.pack(
                hints.MAGIC,
                hints.FORMAT_VERSION,
                bytes(self.pin["chain_identity"]),
                10,
                bytes(self.pin["terminal_block_hash"]),
                9,
            )
            + b"\x06\x01"
        )
        self.pin["sha256"] = list(hashlib.sha256(data).digest())
        (self.bundle / hints.ARTIFACT).write_bytes(data)
        (self.bundle / hints.COMMITMENT).write_text(json.dumps(self.pin))
        (self.bundle / "main-checkpoints.txt").write_text(
            "0 " + bytes(self.pin["chain_identity"])[::-1].hex()
            + "\n10 " + bytes(self.pin["terminal_block_hash"])[::-1].hex() + "\n"
        )
        self.report = {
            "schema_version": hints.SCHEMA_VERSION,
            "commitment": self.pin,
            "survivor_count": 3,
            "oracle": hints.ORACLE,
            "complete_entries": True,
            "salted_multiset": True,
        }
        (self.bundle / hints.VERIFICATION).write_text(json.dumps(self.report))
        self.meta = {
            "schema_version": 2,
            "height": 10,
            "block_hash": bytes(self.pin["terminal_block_hash"])[::-1].hex(),
            "network": "Mainnet",
            "generated_at": "2026-09-10T00:00:00Z",
            "spentness": {
                "verification": self.report,
                "generator_revision": "a" * 40,
                "independent_source": "separate fully validated archive",
                "reproduced_sha256": bytes(self.pin["sha256"]).hex(),
            },
        }

    def test_valid_bundle_and_retained_commitments(self):
        self.assertEqual(
            hints.validate_bundle(self.bundle, self.meta)["commitment"], self.pin
        )
        manifest_path = self.root / hints.MANIFEST
        manifest_path.parent.mkdir(parents=True)
        old = copy.deepcopy(self.pin)
        old["terminal_height"] = 5
        manifest_path.write_text(
            json.dumps({"schema_version": 1, "artifacts": [{"commitment": old}]})
        )
        manifest, compiled = hints.prepare_import(self.root, self.bundle, self.meta)
        self.assertEqual(
            [entry["commitment"]["terminal_height"] for entry in manifest["artifacts"]],
            [5, 10],
        )
        self.assertIn("terminal_height: 5", compiled)
        self.assertIn("terminal_height: 10", compiled)
        self.assertNotIn("include_bytes", compiled)

    def test_resealed_genesis_and_padding_fail_the_named_checks(self):
        path = self.bundle / hints.ARTIFACT
        header = path.read_bytes()[:hints.HEADER.size]
        for bitmap, message in (
            (b"\x07\x01", "retains genesis"),
            (b"\x06\x81", "padding must be zero"),
        ):
            data = header + bitmap
            pin = {**self.pin, "sha256": list(hashlib.sha256(data).digest())}
            report = {**self.report, "commitment": pin,
                      "survivor_count": sum(byte.bit_count() for byte in bitmap)}
            meta = copy.deepcopy(self.meta)
            meta["spentness"]["verification"] = report
            meta["spentness"]["reproduced_sha256"] = bytes(pin["sha256"]).hex()
            path.write_bytes(data)
            (self.bundle / hints.COMMITMENT).write_text(json.dumps(pin))
            (self.bundle / hints.VERIFICATION).write_text(json.dumps(report))
            with self.subTest(message=message), self.assertRaisesRegex(ValueError, message):
                hints.validate_bundle(self.bundle, meta)

    def importer_fixture(self):
        importer = load_script("import-release-state")
        repo = self.root / "repo"
        for relative in (importer.CHECKPOINTS, importer.FRONTIER, importer.SUBTREES,
                         importer.PROVENANCE, importer.EOS_FILE):
            (repo / relative).parent.mkdir(parents=True, exist_ok=True)
        genesis = (self.bundle / "main-checkpoints.txt").read_text().splitlines()[0]
        (repo / importer.CHECKPOINTS).write_text(genesis + "\n")
        (repo / importer.FRONTIER).write_bytes(b"old frontier")
        (repo / importer.EOS_FILE).write_text("const ESTIMATED_RELEASE_HEIGHT: u32 = 1;\n")
        for height, path in (
            (0, repo / importer.SUBTREES),
            (10, self.bundle / importer.SUBTREE_BUNDLE_NAME),
        ):
            prefix = importer.SUBTREE_HEADER_PREFIX.pack(b"ZKVCTST1", 1, 1, height, 0, 0, 0)
            path.write_bytes(prefix + hashlib.sha256(prefix).digest())
        (self.bundle / importer.FRONTIER.name).write_bytes(b"new frontier")
        prefix = importer.FRONTIER_GRID_HEADER_PREFIX.pack(b"ZKVCTFR1", 1, 1, 1, 10, 1)
        payload = struct.pack("<IIII", 0, 0, 0, 0)
        (self.bundle / importer.FRONTIER_GRID_BUNDLE_NAME).write_bytes(
            prefix + hashlib.sha256(prefix + payload).digest() + payload
        )
        meta_bytes = json.dumps(self.meta).encode()
        (self.bundle / "meta.json").write_bytes(meta_bytes)
        resolution = {
            key: self.meta[key] for key in ("height", "block_hash", "generated_at")
        }
        resolution.update(meta_url="https://example.test/v2/10/meta.json",
                          meta_sha256=hashlib.sha256(meta_bytes).hexdigest())
        path = self.root / "resolution.json"
        path.write_text(json.dumps(resolution))
        return importer, repo, path

    def test_schema_two_import_writes_manifest_compiled_pin_and_provenance(self):
        importer, repo, resolution = self.importer_fixture()
        result = importer.import_bundle(repo, self.bundle, resolution)
        self.assertTrue(result["has_changes"])
        manifest = json.loads((repo / hints.MANIFEST).read_text())
        self.assertEqual(manifest["artifacts"][0]["commitment"], self.pin)
        self.assertEqual((repo / hints.COMPILED).read_text(), hints.render_commitments(manifest))
        provenance = json.loads((repo / importer.PROVENANCE).read_text())
        self.assertEqual(provenance["spentness_sha256"], bytes(self.pin["sha256"]).hex())
        self.assertFalse(importer.import_bundle(repo, self.bundle, resolution)["has_changes"])
        with self.assertRaisesRegex(ValueError, "advance in height"):
            hints.prepare_import(repo, self.bundle, self.meta)

    def test_schema_two_import_requires_metadata(self):
        importer, repo, resolution = self.importer_fixture()
        before = (repo / importer.CHECKPOINTS).read_bytes()
        (self.bundle / "meta.json").unlink()
        with self.assertRaisesRegex(importer.BundleImportError, "metadata is required"):
            importer.import_bundle(repo, self.bundle, resolution)
        self.assertEqual((repo / importer.CHECKPOINTS).read_bytes(), before)
        self.assertFalse((repo / hints.MANIFEST).exists())

    def test_artifact_mutation_truncation_and_trailing_bytes(self):
        path = self.bundle / hints.ARTIFACT
        original = path.read_bytes()
        for data in (
            original[:-1],
            original + b"\0",
            original[: hints.HEADER.size] + b"\x07\x01",
            original[: hints.HEADER.size] + b"\x06\x81",
        ):
            with self.subTest(data=data):
                path.write_bytes(data)
                with self.assertRaises(ValueError):
                    hints.validate_bundle(self.bundle, self.meta)

    def test_false_pin_requires_matching_generation_evidence(self):
        for field, value in (
            ("terminal_height", 11),
            ("chain_identity", [3] * 32),
            ("output_count", 2**64 - 1),
            ("byte_len", True),
            ("sha256", [True] * 32),
        ):
            with self.subTest(field=field):
                pin = {**self.pin, field: value}
                (self.bundle / hints.COMMITMENT).write_text(json.dumps(pin))
                with self.assertRaises(ValueError):
                    hints.validate_bundle(self.bundle, self.meta)

    def test_missing_or_mismatched_evidence(self):
        for key, value in (
            ("spentness", None),
            ("block_hash", "03" * 32),
            ("height", 9),
        ):
            with self.subTest(key=key), self.assertRaises(ValueError):
                hints.validate_bundle(self.bundle, {**self.meta, key: value})
        for key, value in (
            ("survivor_count", 2),
            ("complete_entries", False),
            ("salted_multiset", False),
        ):
            report = {**self.report, key: value}
            (self.bundle / hints.VERIFICATION).write_text(json.dumps(report))
            with self.assertRaises(ValueError):
                hints.validate_bundle(self.bundle, self.meta)

    def test_v2_fetch_requires_hint_and_shared_boundary(self):
        fetcher = load_script("fetch-release-state")
        for name in (
            "mainnet-frontier.bin",
            "mainnet-treestate-subtrees.bin",
            "mainnet-frontier-grid.bin",
        ):
            (self.bundle / name).write_bytes(b"fixture")
        self.meta["files"] = {
            path.name: {
                "size": path.stat().st_size,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
            for path in self.bundle.iterdir()
        }
        base = "https://zakura-release.valargroup.dev/release-state/"
        meta_bytes = json.dumps(self.meta).encode()
        latest = {
            key: self.meta[key]
            for key in (
                "schema_version",
                "network",
                "height",
                "block_hash",
                "generated_at",
            )
        }
        latest.update(
            meta_url=base + "v2/10/meta.json",
            meta_sha256=hashlib.sha256(meta_bytes).hexdigest(),
        )
        responses = {
            base + "latest.json": json.dumps(latest).encode(),
            latest["meta_url"]: meta_bytes,
        }
        responses.update(
            {
                base + "v2/10/" + path.name: path.read_bytes()
                for path in self.bundle.iterdir()
            }
        )

        def fetch(url, limit):
            self.assertLessEqual(len(responses[url]), limit)
            return responses[url]

        kwargs = {
            "latest_url": base + "latest.json",
            "output_dir": self.root / "fetched",
            "metadata_out": self.root / "resolution.json",
            "max_age_hours": 48,
            "fetch": fetch,
            "now": datetime(2026, 9, 10, tzinfo=timezone.utc),
        }
        fetcher.resolve_bundle(**kwargs)
        self.assertTrue((kwargs["output_dir"] / hints.ARTIFACT).exists())
        fetcher.resolve_pinned_bundle(
            latest["meta_url"],
            latest["meta_sha256"],
            self.root / "pinned",
            self.root / "pinned-resolution.json",
            fetch=fetch,
            now=kwargs["now"],
        )
        self.assertEqual(
            (self.root / "pinned" / hints.ARTIFACT).read_bytes(),
            (kwargs["output_dir"] / hints.ARTIFACT).read_bytes(),
        )
        del self.meta["files"][hints.ARTIFACT]
        meta_bytes = json.dumps(self.meta).encode()
        latest["meta_sha256"] = hashlib.sha256(meta_bytes).hexdigest()
        responses[latest["meta_url"]] = meta_bytes
        responses[base + "latest.json"] = json.dumps(latest).encode()
        kwargs["output_dir"] = self.root / "missing-hint"
        with self.assertRaises(fetcher.BundleError):
            fetcher.resolve_bundle(**kwargs)
        self.assertFalse(kwargs["output_dir"].exists())

    def publisher_environment(self):
        tools = self.root / "tools"
        tools.mkdir()
        mock = tools / "mock"
        mock.write_text("""#!/usr/bin/env python3
import json, os, shutil, sys
from pathlib import Path
tool = Path(sys.argv[0]).name
args = sys.argv[1:]
fixture = Path(os.environ["MOCK_FIXTURE"])
remote = Path(os.environ["MOCK_REMOTE"])
def option(flag):
    return Path(args[args.index(flag) + 1])
def resolve(value):
    return remote / value.split(":", 1)[1] if value.startswith("r2:") else Path(value)
if tool == "zakura-checkpoints":
    if os.environ.get("MOCK_FAIL_GENERATION"):
        sys.exit(9)
    output = option("--mainnet-spentness-output")
    for source, target in [("mainnet-spentness-hints.bin", output), ("mainnet-spentness-hints.commitment.json", output.with_suffix(".commitment.json")), ("mainnet-spentness-hints.verification.json", output.with_suffix(".verification.json"))]:
        shutil.copyfile(fixture / source, target)
    for flag in ("--mainnet-frontier-output", "--mainnet-subtree-output", "--mainnet-frontier-grid-output"):
        option(flag).write_bytes(b"treestate fixture")
    print((fixture / "main-checkpoints.txt").read_text(), end="")
elif tool == "zakura-spentness":
    if args[0] == "generate":
        shutil.copyfile(fixture / "mainnet-spentness-hints.bin", option("--output"))
        shutil.copyfile(fixture / "mainnet-spentness-hints.commitment.json", option("--commitment"))
    if args[0] == "verify" and os.environ.get("MOCK_FAIL_VERIFICATION"):
        sys.exit(10)
elif tool == "rclone":
    target = resolve(args[1])
    if args[0] == "lsf":
        if not target.exists(): sys.exit(3)
        if target.is_file(): print(target.name)
        else:
            for child in target.iterdir(): print(child.name + ("/" if child.is_dir() else ""))
    elif args[0] == "copyto":
        destination = resolve(args[2])
        if os.environ.get("MOCK_FAIL_UPLOAD") and args[2].startswith("r2:") and destination.name == "meta.json":
            sys.exit(11)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(target, destination)
    elif args[0] == "purge": shutil.rmtree(target)
else: sys.exit(12)
""")
        mock.chmod(0o755)
        for name in ("zakura-checkpoints", "zakura-spentness", "rclone"):
            (tools / name).symlink_to(mock)
        source = self.root / "source"
        oracle = self.root / "oracle"
        source.mkdir()
        oracle.mkdir()
        remote = self.root / "remote"
        pointer = remote / "bucket/release-state/latest.json"
        pointer.parent.mkdir(parents=True)
        pointer.write_text(json.dumps({"schema_version": 2, "height": 5}))
        old = pointer.parent / "v2/5/mainnet-spentness-hints.bin"
        old.parent.mkdir(parents=True)
        old.write_bytes(b"supported old artifact")
        env = {
            **os.environ,
            "PATH": str(tools) + os.pathsep + os.environ["PATH"],
            "MOCK_FIXTURE": str(self.bundle),
            "MOCK_REMOTE": str(remote),
            "RELEASE_STATE_R2_REMOTE": "r2:bucket",
            "RELEASE_STATE_PUBLIC_BASE": "https://zakura-release.valargroup.dev/release-state",
            "RELEASE_STATE_ORACLE_SOURCE": str(oracle),
            "RELEASE_STATE_ORACLE_ID": "independent validated fixture",
            "RELEASE_STATE_GENERATOR_REVISION": "a" * 40,
            "RELEASE_STATE_LOCK_FILE": str(self.root / "publisher.lock"),
            "RELEASE_STATE_DATA_DIR": str(self.root / "replay"),
        }
        return source, pointer, old, env

    def test_publisher_failure_preserves_pointer_and_old_artifacts(self):
        source, pointer, old, env = self.publisher_environment()
        original = pointer.read_bytes()
        script = (
            Path(__file__).resolve().parents[2]
            / "deploy/release-state/publish-release-state.sh"
        )
        for failure in (
            "MOCK_FAIL_GENERATION",
            "MOCK_FAIL_VERIFICATION",
            "MOCK_FAIL_UPLOAD",
        ):
            result = subprocess.run(
                ["bash", str(script), str(source)],
                env={**env, failure: "1"},
                capture_output=True,
                timeout=30,
                check=False,
            )
            self.assertNotEqual(result.returncode, 0, result.stderr.decode())
            self.assertEqual(pointer.read_bytes(), original)
            self.assertTrue(old.exists())
        result = subprocess.run(
            ["bash", str(script), str(source)],
            env=env,
            capture_output=True,
            timeout=30,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        self.assertEqual(json.loads(pointer.read_text())["schema_version"], 2)
        self.assertTrue(old.exists())
        published = pointer.parent / "v2/10"
        hints.validate_bundle(
            published, json.loads((published / "meta.json").read_text())
        )
        metadata = (published / "meta.json").read_bytes()
        result = subprocess.run(
            ["bash", str(script), str(source)],
            env=env,
            capture_output=True,
            timeout=30,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        self.assertEqual((published / "meta.json").read_bytes(), metadata)

    def test_publisher_rejects_invalid_revision_and_oversized_sidecars(self):
        source, pointer, old, env = self.publisher_environment()
        original = pointer.read_bytes()
        script = Path(__file__).resolve().parents[2] / "deploy/release-state/publish-release-state.sh"

        def reject(environment):
            result = subprocess.run(["bash", str(script), str(source)], env=environment,
                                    capture_output=True, timeout=30, check=False)
            self.assertNotEqual(result.returncode, 0, result.stderr.decode())
            self.assertEqual(pointer.read_bytes(), original)
            self.assertTrue(old.exists())
            self.assertFalse((pointer.parent / "v2/10").exists())
            return result.stderr.decode()

        self.assertIn("generator revision", reject({**env, "RELEASE_STATE_GENERATOR_REVISION": "not-a-revision"}))
        for name, limit in ((hints.COMMITMENT, 16 * 1024), (hints.VERIFICATION, 32 * 1024)):
            path = self.bundle / name
            contents = path.read_bytes()
            path.write_bytes(contents + b" " * limit)
            self.assertIn(name, reject(env))
            path.write_bytes(contents)
        self.assertIn("meta.json", reject({**env, "RELEASE_STATE_ORACLE_ID": "x" * (65 * 1024)}))


if __name__ == "__main__":
    unittest.main()
