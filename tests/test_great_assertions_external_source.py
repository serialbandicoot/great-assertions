from great_assertions.great_assertions import GreatAssertions
from pyspark.sql import SparkSession
import pandas as pd
import pytest
import os


class GreatAssertionExternalSourceTests(GreatAssertions):
    def setUp(self):
        self.data = os.path.join("tests", "data", "external_data_source.csv")

    def test_external_source_pandas_expect_table_row_count_to_equal(self):
        df = pd.read_csv(self.data)
        self.assertExpectTableRowCountToEqual(df, 4)

    def test_external_source_pandas_expect_table_row_count_to_equal_fail(
        self,
    ):
        df = pd.read_csv(self.data)

        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectTableRowCountToEqual(df, 1)

        assert "expected row count is 1 the actual was 4" in str(excinfo.value)

    def test_external_source_pyspark_expect_table_row_count_to_equal(self):
        spark = SparkSession.builder.getOrCreate()
        df = spark.read.csv(self.data)
        self.assertExpectTableRowCountToEqual(df, 5)

    def test_external_source_pyspark_expect_table_row_count_to_equal_fail(
        self,
    ):
        spark = SparkSession.builder.getOrCreate()
        df = spark.read.csv(self.data)

        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectTableRowCountToEqual(df, 1)

        assert "expected row count is 1 the actual was 5" in str(excinfo.value)

    def test_external_source_with_delimited_text(self):
        filepath = os.path.join("tests", "data", "external_delimited_source.txt")
        df = pd.read_csv(filepath, sep="|")

        self.assertExpectTableRowCountToEqual(df, 4)

    def test_external_source_with_delimited_text_int_check(self):
        filepath = os.path.join("tests", "data", "external_delimited_source.txt")
        df = pd.read_csv(filepath, sep="|")

        self.expect_column_values_to_match_regex(df, "col2", "^[0-9]{2}$")
