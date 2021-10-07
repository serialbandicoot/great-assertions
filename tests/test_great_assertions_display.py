import unittest
from great_assertions import GreatAssertionResult
import pandas as pd
from pandas.testing import assert_frame_equal
import pytest


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
        assert False is True

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

    def test_to_result_table(self):

        col = ["type", "quantity"]
        data = [
            ["succeeded", 4],
            ["errors", 2],
            ["fails", 3],
            ["skipped", 1],
        ]
        expected = pd.DataFrame(data, columns=col)

        actual = _run_tests(DisplayTest).to_result_table()
        assert_frame_equal(expected, actual)

    def test_to_pie(self):
        actual = _run_tests(DisplayTest).to_pie("My Test Result")
        self.assertTrue(actual.get_ylabel(), "My Test Result")  

