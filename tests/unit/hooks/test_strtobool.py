import unittest

from airflow_clickhouse_plugin.hooks.clickhouse import strtobool


class StrToBoolTestCase(unittest.TestCase):
    def test_correct_true(self):
        for v in ['true', 'True']:
            with self.subTest('Testing %s value' % v, v=v):
                self.assertTrue(strtobool(v))

    def test_correct_false(self):
        for v in ['false', 'False']:
            with self.subTest('Testing %s value' % v, v=v):
                self.assertFalse(strtobool(v))

    def test_unknown_throws(self):
        with self.assertRaises(ValueError):
            strtobool('unknown')


if __name__ == '__main__':
    unittest.main()
