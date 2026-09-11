import contextlib
import importlib.util
import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from zipfile import ZipFile


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / ".agents/skills/zakura-trace-zip/scripts/zip_zakura_traces.py"
SPEC = importlib.util.spec_from_file_location("trace_archive", SCRIPT)
archive = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(archive)


class TraceArchiveTests(unittest.TestCase):
    def test_archive_includes_rotated_csv_and_excludes_lock_files(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            traces = root / "traces"
            node = traces / "node1"
            node.mkdir(parents=True)
            for name in ("block_sync.csv", "block_sync.csv.1", "legacy_sync.csv.2"):
                (node / name).write_text("event,extra\nexample,\n")
            (node / "block_sync.csv.lock").write_text("")
            output = root / "traces.zip"
            with patch("sys.argv", [str(SCRIPT), str(traces), "--output", str(output)]):
                with contextlib.redirect_stdout(io.StringIO()):
                    archive.main()
            with ZipFile(output) as saved:
                names = set(saved.namelist())
            self.assertTrue({
                "traces/node1/block_sync.csv",
                "traces/node1/block_sync.csv.1",
                "traces/node1/legacy_sync.csv.2",
            }.issubset(names))
            self.assertFalse(any(name.endswith(".lock") for name in names))


if __name__ == "__main__":
    unittest.main()
