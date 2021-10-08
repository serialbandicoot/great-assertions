import unittest

from pandas._testing.asserters import assert_equal
from great_assertions import GreatAssertionResult
import pandas as pd
from pandas.testing import assert_frame_equal


class DisplayTest(unittest.TestCase):

    __test__ = False

    def test_pass1(self):
        assert True is True

    def test_pass2(self):
        assert True is True

    def test_pass3(self):
        assert True is True

    def test_pass4(self):
        assert True is True

    def test_fail1(self):
        assert False is True

    def test_fail2(self):
        assert "Hello" is "World"

    def test_error1(self):
        self.no_method_here()

    def test_error2(self):
        self.no_method_here()

    @unittest.skip("demonstrating skipping")
    def test_skip(self):
        pass

    def test_fail3(self):
        assert False is True


def _run_tests(test_class):
    suite = unittest.TestLoader().loadTestsFromTestCase(test_class)
    testRunner = unittest.runner.TextTestRunner(resultclass=GreatAssertionResult)
    return testRunner.run(suite)


class GreatAssertionDisplayTests(unittest.TestCase):
    def test_to_results_table(self):

        col = ["type", "quantity"]
        data = [
            ["succeeded", 4],
            ["errors", 2],
            ["fails", 3],
            ["skipped", 1],
        ]
        expected = pd.DataFrame(data, columns=col)

        actual = _run_tests(DisplayTest).to_results_table()
        assert_frame_equal(expected, actual)

    def test_to_pie(self):
        actual = _run_tests(DisplayTest).to_pie(title="My Test Result")
        self.assertTrue(actual.get_title(), "My Test Result")

    def test_to_full_results_table(self):
        import numpy as np

        col = ["Test Method", "Test Information", "Test Status"]
        data = [
            ["test_fail1", "Stack trace of Fail", "Fail"],
            ["test_fail2", "Stack trace of Fail", "Fail"],
            ["test_fail3", "Stack trace of Fail", "Fail"],
            ["test_error1", "Stack trace of error", "Error"],
            ["test_error2", "Stack trace of error", "Error"],
            ["test_skip", np.nan, "Skip"],
            ["test_pass1", np.nan, "Pass"],
            ["test_pass2", np.nan, "Pass"],
            ["test_pass3", np.nan, "Pass"],
            ["test_pass4", np.nan, "Pass"],
        ]
        expected = pd.DataFrame(data, columns=col)

        actual = _run_tests(DisplayTest).to_full_results_table()

        # Can't check full frame because of Test Information is too random
        self.assertEqual(len(actual), len(expected))
        test_cols = ["Test Method", "Test Status"]
        assert_frame_equal(expected[test_cols], actual[test_cols])
        assert_equal(actual.columns, expected.columns)
        
        # Check order
        self.assertEqual(expected.iloc[0]["Test Status"], "Fail")
        self.assertEqual(expected.iloc[3]["Test Status"], "Error")
        self.assertEqual(expected.iloc[5]["Test Status"], "Skip")
        self.assertEqual(expected.iloc[6]["Test Status"], "Pass")

        # Check Test Info
        self.assertAlmostEqual(expected.iloc[0]["Test Information"], "Stack trace of Fail")