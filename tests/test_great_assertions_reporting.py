import unittest
import sys
from great_assertions import GreatAssertionResult, GreatAssertions
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


class SaveTest(GreatAssertions):

    __test__ = False

    def test_pass1(self):
        df = spark.createDataFrame(
            [
                {"col_1": 100, "col_2": 10},
            ]
        )
        self.expect_table_row_count_to_equal(df, 20)


def _run_tests(test_class):
    suite = unittest.TestLoader().loadTestsFromTestCase(test_class)
    testRunner = unittest.runner.TextTestRunner(resultclass=GreatAssertionResult)
    return testRunner.run(suite)


class GreatAssertionSaveTests(unittest.TestCase):

    def test_to_results_table(self):
        if not sys.platform.startswith('win32'):
            _run_tests(SaveTest).save("databricks", spark=spark)
            self.assertEqual(spark.table("ga_result").count(), 1)        
