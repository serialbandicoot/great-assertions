from great_assertions.src.great_assertions import GreatAssertions
from pyspark.sql import SparkSession
import pytest


class GreatAssertionPySparkTests(GreatAssertions):
    def setUp(self):
        self.spark = SparkSession.builder.getOrCreate()

    def test_pyspark_expect_table_row_count_to_equal(self):
        df = self.spark.createDataFrame(
            [
                {"col_1": 100, "col_2": 10},
                {"col_1": 200, "col_2": 20},
                {"col_1": 300, "col_2": 30},
            ]
        )
        self.assertExpectTableRowCountToEqual(df, 3)

    def test_pyspark_expect_table_row_count_to_equal_raises_type_error(self):
        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectTableRowCountToEqual(1, 1)

        assert "Not a valid pandas/pyspark DataFrame" in str(excinfo.value)

    def test_pyspark_expect_table_row_count_to_equal_fails(self):
        df = self.spark.createDataFrame(
            [
                {"col_1": 100, "col_2": 10},
                {"col_1": 200, "col_2": 20},
                {"col_1": 300, "col_2": 30},
            ]
        )
        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectTableRowCountToEqual(df, 4)

        assert "expected row count is 4 the actual was 3" in str(excinfo.value)

    def test_pyspark_assert_expect_column_values_to_be_between(self):
        # int
        df = self.spark.createDataFrame(
            [{"col_1": 100}, {"col_1": 200}, {"col_1": 300}]
        )
        self.assertExpectColumnValuesToBeBetween(
            df, "col_1", min_value=99, max_value=301
        )

        # float
        df = self.spark.createDataFrame(
            [{"col_1": 100.02}, {"col_1": 200.01}, {"col_1": 300.01}]
        )
        self.assertExpectColumnValuesToBeBetween(df, "col_1", 100.01, 300.02)

        # Same float
        df = self.spark.createDataFrame(
            [{"col_1": 100.05}, {"col_1": 200.01}, {"col_1": 300.05}]
        )
        self.assertExpectColumnValuesToBeBetween(df, "col_1", 100.05, 300.05)

    def test_pyspark_assert_expect_column_values_to_be_between_min_fail(self):
        df = self.spark.createDataFrame(
            [{"col_1": 100}, {"col_1": 200}, {"col_1": 300}]
        )
        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectColumnValuesToBeBetween(df, "col_1", 101, 301)

        assert (
            "Min value provided (101) must be less than column col_1 value of 100"
            in str(excinfo.value)
        )

    def test_pyspark_assert_expect_column_values_to_be_between_min_fail_bad_syntax(
        self,
    ):
        df = self.spark.createDataFrame(
            [{"col_1": 100}, {"col_1": 200}, {"col_1": 300}]
        )

        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectColumnValuesToBeBetween(df, "col_1", 100, 50)

        assert "Max value must be greater than min value" in str(excinfo.value)

    def test_pyspark_assert_expect_column_values_to_be_between_max_fail(self):
        df = self.spark.createDataFrame(
            [{"col_1": 100}, {"col_1": 200}, {"col_1": 300}]
        )

        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectColumnValuesToBeBetween(df, "col_1", 100, 299)

        assert (
            "Max value provided (299) must be greater than column col_1 value of 300"
            in str(excinfo.value)
        )

    def test_pyspark_assert_expect_column_values_to_match_regex(self):
        df = self.spark.createDataFrame(
            [{"col_1": "BA2"}, {"col_1": "BA15"}, {"col_1": "SW1"}]
        )
        self.assertExpectColumnValuesToMatchRegex(
            df, "col_1", "^[a-zA-Z]{2}[0-9]{1,2}$"
        )

    def test_pyspark_assert_expect_column_values_to_match_regex_fail(self):
        df = self.spark.createDataFrame(
            [{"col_1": "BA2"}, {"col_1": "BA151"}, {"col_1": "AAA13"}]
        )
        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectColumnValuesToMatchRegex(
                df, "col_1", "^[a-zA-Z]{2}[0-9]{1,2}$"
            )

        assert "Column col_1 did not match regular expression, found BA151" in str(
            excinfo.value
        )

    def test_pyspark_assert_expect_column_values_to_be_in_set(self):
        df_fruits = [
            {"col_1": "Apple"},
            {"col_1": "Orange"},
            {"col_1": "Cherry"},
            {"col_1": "Apricot(Summer)"},
        ]
        fruits = set(("Apple", "Orange", "Pear", "Cherry", "Apricot(Summer)"))
        df = self.spark.createDataFrame(df_fruits)

        self.assertExpectColumnValuesToBeInSet(df, "col_1", fruits)

    def test_pyspark_assert_expect_column_values_to_be_in_set_fail(self):
        df_fruits = [{"col_1": "Tomato"}, {"col_1": "Cherry"}, {"col_1": "Apple"}]
        fruits = set(("Apple", "Orange", "Pear", "Cherry"))
        df = self.spark.createDataFrame(df_fruits)

        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectColumnValuesToBeInSet(df, "col_1", fruits)

        assert "Column col_1 provided set was not in Apple, Cherry, Tomato" in str(
            excinfo.value
        )

    def test_pyspark_expect_column_values_to_be_of_type(self):
        df = self.spark.createDataFrame(
            [
                {"col_1": "BA2", "col_2": 10, "col_3": 10.45},
                {"col_1": "BA15", "col_2": 20, "col_3": 10.45},
                {"col_1": "SW1", "col_2": 30, "col_3": 10.45},
            ]
        )
        self.assertExpectColumnValuesToBeOfType(df, "col_1", str)
        self.assertExpectColumnValuesToBeOfType(df, "col_2", int)
        self.assertExpectColumnValuesToBeOfType(df, "col_3", float)

    def test_pyspark_expect_column_values_to_be_of_type_fail(self):
        df = self.spark.createDataFrame(
            [
                {"col_1": "BA2", "col_2": 10, "col_3": 10.45},
                {"col_1": "BA15", "col_2": 20, "col_3": 10.45},
                {"col_1": "SW1", "col_2": 30, "col_3": 10.45},
            ]
        )

        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectColumnValuesToBeOfType(df, "col_1", int)

        assert "Column col_1 was not type <class 'int'>" in str(excinfo.value)

        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectColumnValuesToBeOfType(df, "col_2", float)

        assert "Column col_2 was not type <class 'float'>" in str(excinfo.value)

        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectColumnValuesToBeOfType(df, "col_3", str)

        assert "Column col_3 was not type <class 'str'>" in str(excinfo.value)

    def test_assert_pyspark_expect_table_columns_to_match_ordered_list(self):
        df = self.spark.createDataFrame(
            [
                {"col_1": 100, "col_2": "a", "col_3": 1.01},
            ]
        )
        self.assertExpectTableColumnsToMatchOrderedList(
            df, list(("col_1", "col_2", "col_3"))
        )

    def test_assert_pyspark_expect_table_columns_to_match_ordered_list_fail(self):
        df = self.spark.createDataFrame(
            [
                {"col_1": 100, "col_2": "a", "col_3": 1.01},
            ]
        )

        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectTableColumnsToMatchOrderedList(
                df, list(("col_2", "col_1", "col_3"))
            )

        assert "Ordered columns did not match" in str(excinfo.value)

    def test_assert_pyspark_expect_table_columns_to_match_set(self):
        df = self.spark.createDataFrame(
            [
                {"col_1": 100, "col_2": "a", "col_3": 1.01},
            ]
        )
        self.assertExpectTableColumnsToMatchSet(df, set(("col_1", "col_2", "col_3")))
        self.assertExpectTableColumnsToMatchSet(df, set(("col_2", "col_1", "col_3")))
        self.assertExpectTableColumnsToMatchSet(df, list(("col_1", "col_2", "col_3")))
        self.assertExpectTableColumnsToMatchSet(df, list(("col_2", "col_1", "col_3")))

    def test_assert_pyspark_expect_table_columns_to_match_set_fail(self):
        df = self.spark.createDataFrame(
            [
                {"col_1": 100, "col_2": "a", "col_3": 1.01},
            ]
        )

        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectTableColumnsToMatchSet(df, set(("col_2", "col_1")))

        assert "Columns did not match set" in str(excinfo.value)

        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectTableColumnsToMatchSet(df, list(("col_2", "col_1")))

        assert "Columns did not match set" in str(excinfo.value)

    def test_assert_expect_date_range_to_be_less_than(self):
        df = self.spark.createDataFrame(
            [
                {"col_1": "2019-05-13"},
                {"col_1": "2018-12-12"},
                {"col_1": "2015-10-01"},
            ]
        )
        self.assertExpectDateRangeToBeLessThan(df, "col_1", "2019-05-14")

    def test_assert_expect_date_range_to_be_less_than_formatted(self):
        df = self.spark.createDataFrame(
            [
                {"col_1": "2019/05/13"},
                {"col_1": "2018/12/12"},
                {"col_1": "2015/10/01"},
            ]
        )
        self.assertExpectDateRangeToBeLessThan(
            df, "col_1", "2019/05/14", format="%Y/%m/%d"
        )

    def test_assert_expect_date_range_to_be_less_than_default(self):
        df = self.spark.createDataFrame([{"col_1": ""}])

        self.assertExpectDateRangeToBeLessThan(df, "col_1", "1900-01-02")

    def test_assert_expect_date_range_to_be_less_than_fail(self):
        df = self.spark.createDataFrame(
            [
                {"col_1": "2019-05-13"},
                {"col_1": "2018-12-12"},
                {"col_1": "2015-10-01"},
            ]
        )
        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectDateRangeToBeLessThan(df, "col_1", "2019-05-13")

        assert (
            "Column col_1 date is greater or equal than 2019-05-13 found 2019-05-13"
            in str(excinfo.value)
        )

    def test_assert_expect_date_range_to_be_more_than(self):
        df = self.spark.createDataFrame(
            [
                {"col_1": "2019-05-13"},
                {"col_1": "2018-12-12"},
                {"col_1": "2015-10-01"},
            ]
        )
        self.assertExpectDateRangeToBeMoreThan(df, "col_1", "2015-09-30")

    def test_assert_expect_date_range_to_be_more_than_default(self):
        df = self.spark.createDataFrame([{"col_1": ""}])

        self.assertExpectDateRangeToBeMoreThan(df, "col_1", "1899-12-31")

    def test_assert_expect_date_range_to_be_more_than_fail(self):
        df = self.spark.createDataFrame(
            [
                {"col_1": "2019-05-13"},
                {"col_1": "2018-12-12"},
                {"col_1": "2015-10-01"},
            ]
        )

        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectDateRangeToBeMoreThan(df, "col_1", "2015-10-01")

        assert "Column col_1 is less or equal than 2015-10-01 found 2015-10-01" in str(
            excinfo.value
        )

    def test_assert_expect_date_range_to_be_between(self):
        df = self.spark.createDataFrame(
            [
                {"col_1": "2010-01-02"},
                {"col_1": "2025-01-01"},
            ]
        )

        self.assertExpectDateRangeToBeBetween(
            df, "col_1", date_start="2010-01-01", date_end="2025-01-02"
        )

    def test_assert_expect_date_range_to_be_between_start_date_greater_than_end(self):
        df = self.spark.createDataFrame(
            [
                {"col_1": "1975-01-01"},
            ]
        )

        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectDateRangeToBeBetween(
                df, "col_1", date_start="1950-01-02", date_end="1950-01-01"
            )

        assert (
            "Column col_1 start date 1950-01-02 cannot be greater than end_date 1950-01-01"
            in str(excinfo.value)
        )

    def test_assert_expect_date_range_to_be_between_fail(self):
        df = self.spark.createDataFrame(
            [
                {"col_1": "2010-01-02"},
                {"col_1": "2025-01-02"},
            ]
        )

        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectDateRangeToBeBetween(
                df, "col_1", date_start="2010-01-03", date_end="2025-01-03"
            )

        assert (
            "Column col_1 is not between 2010-01-03 and 2025-01-03 found 2010-01-02"
            in str(excinfo.value)
        )

        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectDateRangeToBeBetween(
                df, "col_1", date_start="2010-01-01", date_end="2025-01-01"
            )

        assert (
            "Column col_1 is not between 2010-01-01 and 2025-01-01 found 2025-01-02"
            in str(excinfo.value)
        )

    def test_assert_expect_date_range_to_be_between_fail_equal(self):
        df = self.spark.createDataFrame(
            [
                {"col_1": "2010-01-02"},
                {"col_1": "2025-01-02"},
            ]
        )

        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectDateRangeToBeBetween(
                df, "col_1", date_start="2010-01-02", date_end="2025-01-03"
            )

        assert (
            "Column col_1 is not between 2010-01-02 and 2025-01-03 found 2010-01-02"
            in str(excinfo.value)
        )

        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectDateRangeToBeBetween(
                df, "col_1", date_start="2010-01-01", date_end="2025-01-02"
            )

        assert (
            "Column col_1 is not between 2010-01-01 and 2025-01-02 found 2025-01-02"
            in str(excinfo.value)
        )
