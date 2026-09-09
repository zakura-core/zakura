import importlib.util
import io
from pathlib import Path
import tempfile
import unittest
from unittest import mock

SPEC = importlib.util.spec_from_file_location(
    'sync_sample', Path(__file__).parents[1] / 'zakura-sync-profile-sample.py')
sample = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(sample)


def proc_stat():
    # Linux stat fields 3..42, including a command with spaces and parentheses.
    fields = ['0'] * 40
    for i, value in {0: 'S', 11: '23', 12: '7', 19: '1234', 21: '99',
                     36: '3', 39: '5'}.items():
        fields[i] = value
    return '42 (node (worker)) ' + ' '.join(fields)


class SamplingTests(unittest.TestCase):
    def test_stat_parsing_preserves_units_and_complex_command(self):
        row = sample.thread_stat(proc_stat())
        self.assertEqual(row, dict(comm='node (worker)', state='S', utime_ticks=23,
                                  stime_ticks=7, nice=0, start_ticks=1234, rss_pages=99,
                                  processor=3, blkio_delay_ticks=5))

    def test_lightweight_preserves_process_io_and_metrics_without_thread_scan(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            task = root / '42'
            thread = task / 'task' / '42'
            thread.mkdir(parents=True)
            (task / 'stat').write_text(proc_stat())
            (thread / 'stat').write_text(proc_stat())
            (task / 'io').write_text('read_bytes: 1024\nwrite_bytes: 2048\n')
            (task / 'schedstat').write_text('100 200 3\n')
            (task / 'status').write_text('VmRSS:\t396 kB\n')
            with mock.patch.object(sample.urllib.request, 'urlopen',
                                   side_effect=lambda *a, **k: io.BytesIO(b'height 123\n')):
                full = sample.capture(42, 'http://unused', proc=root)
                with mock.patch.object(Path, 'glob', side_effect=AssertionError('thread scan')):
                    light = sample.capture(42, 'http://unused', include_threads=False, proc=root)
            self.assertEqual(set(full['threads']), {'42'})
            self.assertEqual(light['threads'], {})
            for key in ('process', 'process_status', 'process_schedstat', 'io', 'metrics'):
                self.assertEqual(light[key], full[key])
            self.assertIn('1024', light['io'])
            self.assertEqual(light['metrics'], 'height 123\n')
            self.assertGreaterEqual(light['finished_monotonic_ns'], light['monotonic_ns'])

    def test_exited_process_stops_capture(self):
        with tempfile.TemporaryDirectory() as temp:
            self.assertIsNone(sample.capture(42, 'http://unused', proc=Path(temp)))

    def test_host_process_accounting_skips_exits_and_omits_arguments(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            for pid in ('42', '73', '99'):
                (root / pid).mkdir()
            (root / '42' / 'stat').write_text(proc_stat())
            fields = proc_stat().split(') ')[-1].split()
            fields[16] = '19'
            (root / '73' / 'stat').write_text('73 (maintenance) ' + ' '.join(fields))
            (root / '73' / 'cmdline').write_text('must not collect arguments')
            with mock.patch.object(sample.urllib.request, 'urlopen',
                                   return_value=io.BytesIO(b'height 123\n')):
                row = sample.capture(42, 'http://unused', include_threads=False,
                                     include_host_processes=True, proc=root)
            self.assertEqual(set(row['host_processes']), {'42', '73'})
            process = row['host_processes']['73']
            self.assertEqual(process['comm'], 'maintenance')
            self.assertEqual(process['nice'], 19)
            self.assertEqual(process['utime_ticks'], 23)
            self.assertNotIn('cmdline', process)


if __name__ == '__main__':
    unittest.main()
