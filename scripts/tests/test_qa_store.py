"""The QA store's sync: pull mirrors the Space, push uploads what differs, changed diffs two manifests."""

import hashlib
import importlib.util
import io
import contextlib
import json
from pathlib import Path
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / "qa_store.py"
SPEC = importlib.util.spec_from_file_location("qa_store", SCRIPT)
store = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(store)


class FakeSpace:
    """An in-memory bucket with the boto3 client calls the script makes."""

    def __init__(self, objects=None):
        self.objects = dict(objects or {})
        self.downloads = []

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        space = self

        class Paginator:
            def paginate(self, Bucket, Prefix):
                return [{"Contents": [
                    {"Key": key, "ETag": f'"{hashlib.md5(data).hexdigest()}"'}
                    for key, data in sorted(space.objects.items()) if key.startswith(Prefix)
                ]}]

        return Paginator()

    def download_file(self, bucket, key, path):
        self.downloads.append(key)
        Path(path).write_bytes(self.objects[key])

    def upload_file(self, path, bucket, key):
        self.objects[key] = Path(path).read_bytes()

    def delete_object(self, Bucket, Key):
        del self.objects[Key]


def write(root, files):
    for path, data in files.items():
        (Path(root) / path).parent.mkdir(parents=True, exist_ok=True)
        (Path(root) / path).write_bytes(data)


class StoreTests(unittest.TestCase):
    def test_pull_mirrors_the_space_and_skips_unchanged_files(self):
        space = FakeSpace({"qa/planted/p/a.toml": b"a", "seeds/s.bin": b"s", "other/x": b"x"})
        with tempfile.TemporaryDirectory() as dest:
            write(dest, {"seeds/s.bin": b"s", "fixtures/gone.json": b"g", "notes.txt": b"n"})
            self.assertEqual(store.pull(space, "b", dest), (1, 1))
            self.assertEqual(space.downloads, ["qa/planted/p/a.toml"], "an unchanged file isn't downloaded")
            self.assertEqual(store.local_files(dest), ["qa/planted/p/a.toml", "seeds/s.bin"])
            self.assertTrue((Path(dest) / "notes.txt").exists(), "a file outside the prefixes stays")
            self.assertEqual(store.manifest(dest), {
                "qa/planted/p/a.toml": hashlib.sha256(b"a").hexdigest(),
                "seeds/s.bin": hashlib.sha256(b"s").hexdigest(),
            })

    def test_pull_refuses_a_key_that_leaves_the_store(self):
        space = FakeSpace({"seeds/../../escape": b"x"})
        with tempfile.TemporaryDirectory() as dest, self.assertRaises(SystemExit):
            store.pull(space, "b", dest)

    def test_push_uploads_what_differs_and_deletes_only_on_request(self):
        space = FakeSpace({"seeds/s.bin": b"s", "seeds/old.bin": b"o"})
        with tempfile.TemporaryDirectory() as src:
            write(src, {"seeds/s.bin": b"s", "fixtures/f.json": b"f"})
            self.assertEqual(store.push(space, "b", src, delete=False), (1, 0))
            self.assertIn("seeds/old.bin", space.objects)
            self.assertEqual(store.push(space, "b", src, delete=True), (0, 1))
            self.assertEqual(sorted(space.objects), ["fixtures/f.json", "seeds/s.bin"])

    def test_changed_lists_paths_either_manifest_hashes_differently(self):
        old = {"a": "1", "b": "2", "c": "3"}
        new = {"a": "1", "b": "9", "d": "4"}
        self.assertEqual(store.changed(old, new), ["b", "c", "d"])
        with tempfile.TemporaryDirectory() as dir:
            Path(dir, "old.json").write_text(json.dumps(old))
            Path(dir, "new.json").write_text(json.dumps(new))
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                self.assertEqual(store.main(["changed", f"{dir}/old.json", f"{dir}/new.json"]), 0)
        self.assertEqual(out.getvalue(), "b\nc\nd\n")

    def test_the_env_file_reads_systemd_lines(self):
        with tempfile.TemporaryDirectory() as dir:
            path = Path(dir, "env")
            path.write_text("# comment\nCXXFLAGS=-include cstdint\nZAKURA_CORPUS_BUCKET=\"bucket\"\n\n")
            self.assertEqual(store.read_env_file(path),
                             {"CXXFLAGS": "-include cstdint", "ZAKURA_CORPUS_BUCKET": "bucket"})


if __name__ == "__main__":
    unittest.main()
