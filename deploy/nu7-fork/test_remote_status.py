"""Mining observations must not mix retained histories across a reset."""
import runpy
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

module = runpy.run_path(str(Path(__file__).parent / 'miner/remote-status.py'))


class GenerationBoundaryTests(unittest.TestCase):
    def test_current_generation_excludes_previous_network_blocks(self):
        run = module['accepted_blocks_24h'].__globals__['subprocess']
        with patch('time.time', return_value=200_000), patch.object(
                run, 'run', return_value=SimpleNamespace(returncode=0, stdout='block accepted\n', stderr='')) as mocked:
            self.assertEqual(module['accepted_blocks_24h']('miner', since=199_000), 1)
            self.assertIn('@199000', mocked.call_args.args[0])

    def test_day_window_advances_beyond_generation_start(self):
        run = module['accepted_blocks_24h'].__globals__['subprocess']
        with patch('time.time', return_value=200_000), patch.object(
                run, 'run', return_value=SimpleNamespace(returncode=0, stdout='', stderr='')) as mocked:
            self.assertEqual(module['accepted_blocks_24h']('miner', since=100_000), 0)
            self.assertIn('@113600', mocked.call_args.args[0])
