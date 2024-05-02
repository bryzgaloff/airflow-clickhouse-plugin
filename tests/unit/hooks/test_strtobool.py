import unittest

from airflow_clickhouse_plugin.hooks.clickhouse import strtobool


class StrToBoolTestCase(unittest.TestCase):
    def test_correct_true(self):
        self.assertTrue(strtobool('true'))

    def test_correct_one(self):
        self.assertTrue(strtobool('1'))

    def test_correct_false(self):
        self.assertFalse(strtobool('false'))

    def test_correct_zero(self):
        self.assertFalse(strtobool('0'))

    def test_unknown_throws(self):
        with self.assertRaises(ValueError):
            strtobool('unknown')


if __name__ == '__main__':
    unittest.main()
