import unittest

from airflow_clickhouse_plugin.hooks.clickhouse import strtobool


class StrToBoolTestCase(unittest.TestCase):
    def test_correct_true(self):
        self.assertTrue(strtobool('true'))

    def test_correct_true_capital(self):
        self.assertTrue(strtobool('True'))

    def test_correct_false(self):
        self.assertFalse(strtobool('false'))

    def test_correct_false_capital(self):
        self.assertFalse(strtobool('False'))

    def test_unknown_throws(self):
        with self.assertRaises(ValueError):
            strtobool('unknown')


if __name__ == '__main__':
    unittest.main()
