import importlib.util
import json
import hashlib
import subprocess
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('sample', Path(__file__).with_name('sample.py'))
sample = importlib.util.module_from_spec(spec)
spec.loader.exec_module(sample)


class Parsing(unittest.TestCase):
    def test_exact_timestamp_stack_and_shared_dictionary(self):
        lines = [' 12/13 123.123456789: cpu-clock:u: abcd validate (/usr/bin/zakurad)\n',
                 ' abce caller (/usr/bin/zakurad)\n', '\n'] * 2
        result = sample.parse_perf(lines, 12, 123000000, 124000000)
        self.assertEqual(result['samples'][0], dict(mono_us=123123456, tid=13, stack=0))
        self.assertEqual(result['frames'][0], dict(ip='abcd', symbol='validate', dso='/usr/bin/zakurad'))
        self.assertEqual(result['stacks'], [[0, 1]])
        self.assertEqual(len(result['samples']), 2)
        self.assertEqual(result['decode_errors'], 0)
        self.assertFalse(result['truncated'])

    def test_cpu_period_survives_without_filling_idle_gaps(self):
        lines = ['12/13 1.001: 1001001 cpu-clock:u: abcd work (node)\n',
                 '12/14 8.001: 2000000 cpu-clock:u: abce work (node)\n']
        parsed = sample.parse_perf(lines, 12, 0, 9000000, require_period=True)
        self.assertEqual([s['cpu_period_ns'] for s in parsed['samples']], [1001001, 2000000])
        self.assertEqual([s['mono_us'] for s in parsed['samples']], [1001000, 8001000])
        self.assertEqual(parsed['decode_errors'], 0)

    def test_weighted_capture_rejects_missing_wrong_or_invalid_period(self):
        for header in ('cpu-clock:u:', '0 cpu-clock:u:', '1000000001 cpu-clock:u:', '1000 cycles:u:'):
            parsed = sample.parse_perf([f'12/13 1.001: {header} abcd work (node)\n'],
                                       12, 0, 2000000, require_period=True)
            self.assertEqual(parsed['samples'], [])
            self.assertEqual(parsed['decode_errors'], 1)
            self.assertEqual(parsed['omitted_samples'], 1)
            self.assertTrue(parsed['truncated'])

    def test_long_symbols_and_raw_addresses_do_not_merge(self):
        symbol = '_R' + 'long_symbol' * 500
        lines = [f'12/13 1.1: cpu-clock:u: {ip} {symbol} (/full/node)\n\n' for ip in ('abcd', 'abce')]
        result = sample.parse_perf(''.join(lines).splitlines(keepends=True), 12, 0, 2000000)
        self.assertEqual(len(result['frames']), 2)
        self.assertEqual(result['frames'][0]['symbol'], symbol)
        self.assertFalse(result['truncated'])

    def test_symbol_containing_lost_is_not_a_loss_record(self):
        result = sample.parse_perf(['12/13 1.1: cpu-clock:u: abcd lost_wakeup (node)\n'], 12, 0, 2000000)
        self.assertEqual(result['frames'][0]['symbol'], 'lost_wakeup')
        self.assertEqual(result['decode_errors'], 0)
        self.assertEqual(result['lost_samples'], 0)

    def test_loss_and_exceptional_symbol_limit_are_explicit(self):
        result = sample.parse_perf(['12/13 1.1: cpu-clock:u: abcd ' + 'x' * 70000 + ' (node)\n',
                                    'LOST 12 events\n'], 12, 0, 2000000)
        self.assertEqual(result['symbol_truncations'], 1)
        self.assertEqual(result['lost_samples'], 12)
        self.assertTrue(result['truncated'])
        self.assertEqual(len(result['frames'][0]['symbol']), sample.MAX_SYMBOL)

    def test_sample_budget_reports_omissions(self):
        with patch.object(sample, 'MAX_JSON', 200):
            result = sample.parse_perf(['12/13 1.1: cpu-clock:u: abcd ' + 'x' * 300 + ' (node)\n'], 12, 0, 2000000)
        self.assertEqual(result['samples'], [])
        self.assertEqual(result['omitted_samples'], 1)
        self.assertTrue(result['truncated'])

    def test_empty_capture_is_not_decoder_failure(self):
        result = sample.parse_perf([], 12, 0, 1)
        self.assertEqual(result['samples'], [])
        self.assertEqual(result['decode_errors'], 0)
        self.assertFalse(result['truncated'])

    def test_inode_seal_does_not_trust_rotated_filename(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            active = root / 'capture.data.123'
            active.write_bytes(b'active')
            (root / 'capture.data.123.done.json').write_text('{}')
            (root / 'capture.data.txt').write_text('decode')
            stat = active.stat()
            self.assertEqual(sample.sealed_files(root, {(stat.st_dev, stat.st_ino)}), [])
            self.assertEqual(sample.sealed_files(root, set()), [active])

    def test_pruning_preserves_queued_and_active_data(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            directory = root / 'session'
            directory.mkdir()
            done = directory / 'capture.data.1'
            done.write_bytes(b'1' * 50)
            active = directory / 'capture.data'
            active.write_bytes(b'2' * 50)
            queued = directory / 'capture.data.2'
            queued.write_bytes(b'3' * 50)
            (directory / 'capture.data.1.done.json').write_text(json.dumps({'raw': str(done)}))
            with patch.object(sample, 'RAW_BUDGET', 100), patch.object(sample, 'MAX_FILE', 1):
                sample.prune_raw(root)
            self.assertFalse(done.exists())
            self.assertTrue(active.exists())
            self.assertTrue(queued.exists())

    def test_symbol_generations_preserve_referenced_raw_work(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            (root / 'symbols').mkdir()
            (root / 'raw').mkdir()
            executable = root / 'node'
            executable.write_bytes(b'node')
            current, used, obsolete = ('a' * 64, 'b' * 64, 'c' * 64)
            for digest in (used, obsolete):
                directory = root / 'symbols' / digest
                directory.mkdir()
                (directory / 'elf').write_bytes(b'elf')
            (root / 'raw' / 'pending.json').write_text(json.dumps({'capture': {'executable_sha256': used}}))
            target = sample.symbol_cache(root, executable, current, prune=True)
            self.assertEqual(target.name, current)
            self.assertTrue((root / 'symbols' / used).exists())
            self.assertFalse((root / 'symbols' / obsolete).exists())

    def test_symbol_pressure_expires_decoded_raw_but_keeps_pending(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            (root / 'symbols').mkdir()
            (root / 'raw').mkdir()
            executable = root / 'node'
            executable.write_bytes(b'node' * 10)
            old, pending = 'b' * 64, 'c' * 64
            for digest in (old, pending):
                generation = root / 'symbols' / digest
                generation.mkdir()
                (generation / 'elf').write_bytes(b'elf' * 10)
            payload = root / 'raw' / 'capture.data.123'
            payload.write_bytes(b'raw')
            marker = root / 'raw' / 'capture.data.123.done.json'
            marker.write_text(json.dumps({'raw': str(payload), 'capture': {'executable_sha256': old}}))
            (root / 'raw' / 'segment-000001.json').write_text(json.dumps({'capture': {'executable_sha256': pending}}))
            with patch.object(sample, 'SYMBOL_BUDGET', 80):
                sample.symbol_cache(root, executable, 'a' * 64, prune=True)
            self.assertFalse(payload.exists())
            self.assertFalse(marker.exists())
            self.assertFalse((root / 'symbols' / old).exists())
            self.assertTrue((root / 'symbols' / pending).exists())

    def test_cgroup_must_be_exact_supervised_non_root_group(self):
        with patch.object(sample.subprocess, 'check_output', return_value='/system.slice/zakurad.service\n'), \
             patch.object(Path, 'read_text', return_value='0::/system.slice/zakurad.service\n'):
            self.assertEqual(sample.node_cgroup(123, 'zakurad'), '/system.slice/zakurad.service')
        with patch.object(sample.subprocess, 'check_output', return_value='/\n'), \
             patch.object(Path, 'read_text', return_value='0::/\n'):
            with self.assertRaises(RuntimeError):
                sample.node_cgroup(123, 'zakurad')

    def test_incomplete_symbol_copy_is_rebuilt_and_published_atomically(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            executable = root / 'node'
            executable.write_bytes(b'complete node executable')
            digest = hashlib.sha256(executable.read_bytes()).hexdigest()
            target = root / digest
            target.mkdir()
            (target / 'elf').write_bytes(b'truncated')
            def build(argv, **kwargs):
                self.assertEqual(kwargs['preexec_fn'], sample.symbol_limits)
                self.assertFalse(target.exists())
                staging = Path(argv[2])
                (staging / 'elf').write_bytes(executable.read_bytes())
            with patch.object(sample.subprocess, 'run', side_effect=build):
                sample.prepare_symbols(target, executable, digest)
            self.assertEqual((target / 'elf').read_bytes(), executable.read_bytes())
            self.assertTrue((target / '.complete').exists())
            self.assertFalse(target.with_name(target.name + '.building').exists())

    def test_failed_symbol_copy_cannot_be_reused_on_retry(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            executable = root / 'node'
            executable.write_bytes(b'complete')
            digest = hashlib.sha256(executable.read_bytes()).hexdigest()
            target = root / digest
            def fail(argv, **kwargs):
                (Path(argv[2]) / 'elf').write_bytes(b'partial')
                raise subprocess.CalledProcessError(-25, argv)
            with patch.object(sample.subprocess, 'run', side_effect=fail):
                with self.assertRaises(subprocess.CalledProcessError):
                    sample.prepare_symbols(target, executable, digest)
            self.assertFalse(target.exists())
            self.assertFalse(target.with_name(target.name + '.building').exists())

    def test_symbol_preflight_does_not_create_root_owned_cache(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            executable = root / 'node'
            executable.write_bytes(b'node')
            sample.symbol_cache(root, executable, 'a' * 64)
            self.assertFalse((root / 'symbols').exists())

    def test_loss_marker_keeps_run_interval_without_invented_samples(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            (root / 'inbox').mkdir()
            raw = root / 'capture.data.123'
            manifest = dict(key='a' * 32, raw=str(raw), capture=dict(run='b' * 32, start_mono_us=1, end_mono_us=2))
            sample.publish_loss(root, manifest, 'decoder overloaded')
            value = json.loads(next((root / 'inbox').glob('*.json')).read_text())
            self.assertEqual(value['samples'], [])
            self.assertTrue(value['truncated'])
            self.assertIsNone(value['lost_samples'])
            self.assertTrue(root.joinpath('capture.data.123.done.json').exists())


if __name__ == '__main__':
    unittest.main()
