"""Regression checks for exact-case qualification acceptance."""
import unittest
from qualify import qualifies


class AcceptanceTests(unittest.TestCase):
    def test_exact_case_requires_one_executed_success(self):
        self.assertTrue(qualifies(0, 'test result: ok. 1 passed; 0 failed; 0 ignored;', True))
        for output in [
            'test result: ok. 0 passed; 0 failed; 0 ignored;',
            'test result: ok. 2 passed; 0 failed; 0 ignored;',
            'test result: ok. 1 passed; 0 failed; 1 ignored;',
            'test result: FAILED. 0 passed; 1 failed; 0 ignored;',
            '',
        ]:
            self.assertFalse(qualifies(0, output, True))

    def test_process_failure_cannot_be_accepted(self):
        self.assertFalse(qualifies(1, 'test result: ok. 1 passed; 0 failed; 0 ignored;', True))
        self.assertFalse(qualifies(1, '', False))


if __name__ == '__main__':
    unittest.main()
