import unittest
import sys
from great_assertions import GreatAssertionResult, GreatAssertions
from pyspark.sql import SparkSession
from pandas.testing import assert_frame_equal
import pandas as pd
import pytest

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

    def test_pass1(self):
        df = spark.createDataFrame([{"col_1": 100}])
        self.expect_table_row_count_to_equal(df, 1)

    def test_pass2(self):
        df = spark.createDataFrame([{"col_1": ""}])
        self.expect_date_range_to_be_more_than(df, "col_1", "1899-12-31")

    @unittest.skip("demonstrating skipping")
    def test_skip(self):
        pass

    def test_error(self):
        self.no_method_here()


def _run_tests(test_class):
    suite = unittest.TestLoader().loadTestsFromTestCase(test_class)
    testRunner = unittest.runner.TextTestRunner(resultclass=GreatAssertionResult)
    return testRunner.run(suite)


class GreatAssertionSaveTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if sys.platform.startswith("win"):
            pytest.skip("Skipping test for windows", allow_module_level=True)

        _run_tests(SaveTest).save("databricks", spark=spark)
        cls.df = spark.table("ga_result")

    def test_status(self):
        self.assertEqual(self.df.count(), 7)
        self.assertEqual(self.df.filter(self.df.status == "Fail").count(), 3)
        self.assertEqual(self.df.filter(self.df.status == "Pass").count(), 2)
        self.assertEqual(self.df.filter(self.df.status == "Skip").count(), 1)
        self.assertEqual(self.df.filter(self.df.status == "Error").count(), 1)

    def test_extended_fail(self):
        df = self.df.filter((self.df.status == "Fail") & (self.df.test_id == 7))

        col = ["test_id", "status", "extended"]
        data = [
            [
                7,
                "Fail",
                '{"id": 7, "name": "expect_column_values_to_be_between", "values": {"column": "col_1", "exp_min_value": 101, "exp_max_value": 301, "act_min_value": 100.0}}',
            ]
        ]
        expected = pd.DataFrame(data, columns=col)
        actual = df.toPandas()

        assert_frame_equal(expected, actual[col])

    def test_extended_pass(self):
        df = self.df.filter((self.df.status == "Pass")).sort(self.df.test_id.desc())

        col = ["test_id", "status", "extended"]
        data = [
            [
                14,
                "Pass",
                '{"id": 14, "name": "expect_date_range_to_be_more_than", "values": {"expected_min_date": "1899-12-31"}}',
            ],
            [
                1,
                "Pass",
                '{"id": 1, "name": "expect_table_row_count_to_equal", "values": {"exp_count": 1, "act_count": 1, "tolerance": 0}}',
            ],
        ]
        expected = pd.DataFrame(data, columns=col)
        actual = df.toPandas()

        assert_frame_equal(expected, actual[col])

    def test_extended_skip(self):
        df = self.df.filter((self.df.status == "Skip"))

        col = ["test_id", "status", "extended"]
        data = [
            [
                -1,
                "Skip",
                "{}",
            ]
        ]
        expected = pd.DataFrame(data, columns=col)
        actual = df.toPandas()

        assert_frame_equal(expected, actual[col])

    def test_extended_error(self):
        df = self.df.filter((self.df.status == "Error"))

        col = ["test_id", "status", "extended"]
        data = [
            [
                -1,
                "Error",
                "{}",
            ]
        ]
        expected = pd.DataFrame(data, columns=col)
        actual = df.toPandas()

        assert_frame_equal(expected, actual[col])

    def test_run_id(self):
        cnt = self.df.groupBy("run_id").count().select("count").collect()[0][0]
        self.assertEqual(cnt, 7)

    def test_created_at(self):
        cnt = self.df.groupBy("created_at").count().select("count").collect()[0][0]
        self.assertEqual(cnt, 7)

    def test_information(self):
        df = self.df.filter(self.df.information.contains("Traceback"))
        self.assertEqual(df.count(), 4)

    def test_method(self):
        df = self.df.filter(self.df.method.contains("test_fail"))
        self.assertEqual(df.count(), 3)
