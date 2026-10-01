import json
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import Unavailable
from comparison import Remote
from ssh_probe import reply


class SSHTests(unittest.TestCase):
    def client(self, source, seconds=1):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        script = Path(directory.name) / 'probe.py'
        script.write_text(source)
        popen = subprocess.Popen
        def launch(argv, **kwargs):
            self.assertEqual(argv, ['ssh', '-F', '/private/config', '-T', 'mac-verifier'])
            self.assertEqual(kwargs['stderr'], subprocess.DEVNULL)
            return popen([sys.executable, '-u', str(script)], **kwargs)
        with patch('comparison.subprocess.Popen', side_effect=launch):
            remote = Remote('/private/config', time.monotonic() + seconds)
        self.addCleanup(remote.close)
        return remote

    def test_one_session_serves_multiple_bounded_requests(self):
        remote = self.client('import sys, json\nfor line in sys.stdin:\n print(json.dumps({"request":json.loads(line)}), flush=True)\n')
        self.assertEqual(remote.status(), {'request': {'operation': 'status'}})
        self.assertEqual(remote.get({'operation': 'block', 'height': 12}),
                         {'request': {'operation': 'block', 'height': 12}})

    def test_malformed_oversized_and_private_remote_errors_are_not_exposed(self):
        for output in ['not json 198.51.100.42', json.dumps({'error': '198.51.100.42'}), 'x' * (256 * 1024 + 1)]:
            with self.subTest(length=len(output)):
                remote = self.client('import sys\nsys.stdin.readline()\nprint(' + repr(output) + ', flush=True)\n')
                with self.assertRaises(Unavailable) as raised:
                    remote.status()
                self.assertNotIn('198.51.100.42', str(raised.exception))

    def test_unresponsive_probe_exhausts_shared_deadline(self):
        remote = self.client('import time\ntime.sleep(30)\n', seconds=0.1)
        started = time.monotonic()
        with self.assertRaises(Unavailable):
            remote.status()
        self.assertLess(time.monotonic() - started, 1)

    def test_probe_rejects_non_read_only_operations_and_invalid_heights(self):
        for request in [None, [], {'operation': 'exec', 'command': 'whoami'},
                        {'operation': 'status', 'path': '/etc/passwd'},
                        {'operation': 'block', 'height': True},
                        {'operation': 'block', 'height': -1}]:
            with self.subTest(request=request), self.assertRaises(Unavailable):
                reply(request, Path('/unused'), None)
