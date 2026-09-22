import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location("sample", Path(__file__).with_name("sample.py"))
sample = importlib.util.module_from_spec(spec)
spec.loader.exec_module(sample)


class Parsing(unittest.TestCase):
    def test_exact_timestamp_and_stack(self):
        lines = [" 12/13 123.123456789: cpu-clock:u: abcd validate (/usr/bin/zakurad)\n", " abce caller (/usr/bin/zakurad)\n", "\n"]
        rows, errors, truncated = sample.parse_perf(lines, 12, 123000000, 124000000)
        self.assertEqual(rows[0]["mono_us"], 123123456)
        self.assertEqual(rows[0]["tid"], 13)
        self.assertEqual(rows[0]["frames"], ["validate (zakurad)", "caller (zakurad)"])
        self.assertEqual(errors, 0)
        self.assertFalse(truncated)

    def test_wrong_process_and_malformed_input_visible(self):
        rows, errors, truncated = sample.parse_perf([" 99 13 123.5: cpu-clock:u: abcd function (node)\n", "unparsed\n"], 12, 123000000, 124000000)
        self.assertEqual(rows, [])
        self.assertEqual(errors, 2)
        self.assertFalse(truncated)

    def test_unbounded_symbols_stop_decoding(self):
        rows, errors, truncated = sample.parse_perf(["x" * 5000], 12, 0, 1)
        self.assertEqual(rows, [])
        self.assertEqual(errors, 1)
        self.assertTrue(truncated)


if __name__ == "__main__":
    unittest.main()
