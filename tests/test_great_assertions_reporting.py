import unittest
import sys
from great_assertions import GreatAssertionResult, GreatAssertions
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


class SaveTest(GreatAssertions):

    __test__ = False

    @classmethod
    def setUpClass(cls):
        import os
        import shutil

        dirpath = os.path.join("spark-warehouse")
        shutil.rmtree(dirpath, ignore_errors=True)

    def test_fail1(self):
        df = spark.createDataFrame([{"col_1": 100}])
        self.expect_table_row_count_to_equal(df, 3)

    def test_fail2(self):
        df = spark.createDataFrame([{"col_1": 100}, {"col_1": 300}])
        self.expect_column_values_to_be_between(df, "col_1", 101, 301)

    def test_fail3(self):
        self.assertFalse(True)

    def test_pass(self):
        df = spark.createDataFrame([{"col_1": 100}])
        self.expect_table_row_count_to_equal(df, 1)

    # Removing as code coverage fails!
    # Add back in once todo completed
    # @unittest.skip("demonstrating skipping")
    # def test_skip(self):
    #     pass

    def test_error(self):
        self.no_method_here()


def _run_tests(test_class):
    suite = unittest.TestLoader().loadTestsFromTestCase(test_class)
    testRunner = unittest.runner.TextTestRunner(resultclass=GreatAssertionResult)
    return testRunner.run(suite)


class GreatAssertionSaveTests(unittest.TestCase):
    def test_to_results_table(self):
        # todo: extend this out to cover more of the different
        # extended data created by the asserts GA and Non-GA
        if not sys.platform.startswith("win32"):
            _run_tests(SaveTest).save("databricks", spark=spark)
            df = spark.table("ga_result")
            self.assertEqual(df.count(), 5)
            self.assertEqual(df.filter(df.status == "Fail").count(), 3)
            self.assertEqual(df.filter(df.status == "Pass").count(), 1)
            # self.assertEqual(df.filter(df.status == "Skip").count(), 1)
            self.assertEqual(df.filter(df.status == "Error").count(), 1)
