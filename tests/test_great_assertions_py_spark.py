from great_assertions import GreatAssertions
from pyspark.sql import SparkSession, types as T
from pyspark.sql.functions import col, regexp_extract
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
        self.expect_table_row_count_to_equal(df, 3)

    def test_pyspark_expect_table_row_count_to_equal_raises_type_error(
        self,
    ):
        with pytest.raises(AssertionError) as excinfo:
            self.expect_table_row_count_to_equal(1, 1)

        assert "Not a valid pandas/pyspark DataFrame" == str(excinfo.value)

    def test_pyspark_expect_table_row_count_to_equal_fails(self):
        df = self.spark.createDataFrame(
            [
                {"col_1": 100, "col_2": 10},
                {"col_1": 200, "col_2": 20},
                {"col_1": 300, "col_2": 30},
            ]
        )
        with pytest.raises(AssertionError) as excinfo:
            self.expect_table_row_count_to_equal(df, 4)

        assert "expected row count is 4 the actual was 3 : " == str(excinfo.value)

    def test_pyspark_expect_table_row_count_to_be_greater_than(self):
        df = self.spark.createDataFrame(
            [
                {"col_1": 100, "col_2": 10},
                {"col_1": 200, "col_2": 20},
                {"col_1": 300, "col_2": 30},
            ]
        )
        self.expect_table_row_count_to_be_greater_than(df, 2)

    def test_pyspark_expect_table_row_count_to_be_greater_than_raises_type_error(
        self,
    ):
        with pytest.raises(AssertionError) as excinfo:
            self.expect_table_row_count_to_be_greater_than(1, 1)

        assert "Not a valid pandas/pyspark DataFrame" == str(excinfo.value)

    def test_pyspark_expect_table_row_count_to_be_greater_than_fails(self):
        df = self.spark.createDataFrame(
            [
                {"col_1": 100, "col_2": 10},
                {"col_1": 200, "col_2": 20},
                {"col_1": 300, "col_2": 30},
            ]
        )
        with pytest.raises(AssertionError) as excinfo:
            self.expect_table_row_count_to_be_greater_than(df, 4)

        assert "expected row count of at least 4 but the actual was 3 : " == str(
            excinfo.value
        )

    def test_pyspark_expect_table_has_no_duplicate_rows(self):
        df = self.spark.createDataFrame(
            [
                {"col_1": 100, "col_2": 10},
                {"col_1": 100, "col_2": 20},
                {"col_1": 300, "col_2": 30},
            ]
        )
        self.expect_table_has_no_duplicate_rows(df)

    def test_pyspark_expect_table_has_no_duplicate_rows_fail(self):
        df = self.spark.createDataFrame(
            [
                {"col_1": 100, "col_2": 10},
                {"col_1": 100, "col_2": 10},
                {"col_1": 300, "col_2": 30},
            ]
        )

        with pytest.raises(AssertionError) as excinfo:
            self.expect_table_has_no_duplicate_rows(df)

        assert "Table contains duplicate rows : " == str(excinfo.value)

    def test_pyspark_assert_expect_column_values_to_be_between(self):
        # int
        df = self.spark.createDataFrame(
            [{"col_1": 100}, {"col_1": 200}, {"col_1": 300}]
        )
        self.expect_column_values_to_be_between(
            df, "col_1", min_value=99, max_value=301
        )

        # float
        df = self.spark.createDataFrame(
            [{"col_1": 100.02}, {"col_1": 200.01}, {"col_1": 300.01}]
        )
        self.expect_column_values_to_be_between(df, "col_1", 100.01, 300.02)

        # Same float
        df = self.spark.createDataFrame(
            [{"col_1": 100.05}, {"col_1": 200.01}, {"col_1": 300.05}]
        )
        self.expect_column_values_to_be_between(df, "col_1", 100.05, 300.05)

    def test_pyspark_assert_expect_column_values_to_be_between_min_fail(
        self,
    ):
        df = self.spark.createDataFrame(
            [{"col_1": 100}, {"col_1": 200}, {"col_1": 300}]
        )
        with pytest.raises(AssertionError) as excinfo:
            self.expect_column_values_to_be_between(df, "col_1", 101, 301)

        assert (
            "Min value provided (101) must be less than column col_1 value of 100 : "
            == str(excinfo.value)
        )

    def test_pyspark_assert_expect_column_values_to_be_between_min_fail_bad_syntax(
        self,
    ):
        df = self.spark.createDataFrame(
            [{"col_1": 100}, {"col_1": 200}, {"col_1": 300}]
        )

        with pytest.raises(AssertionError) as excinfo:
            self.expect_column_values_to_be_between(df, "col_1", 100, 50)

        assert "Max value must be greater than min value : " == str(excinfo.value)

    def test_pyspark_assert_expect_column_values_to_be_between_max_fail(
        self,
    ):
        df = self.spark.createDataFrame(
            [{"col_1": 100}, {"col_1": 200}, {"col_1": 300}]
        )

        with pytest.raises(AssertionError) as excinfo:
            self.expect_column_values_to_be_between(df, "col_1", 100, 299)

        assert (
            "Max value provided (299) must be greater than column col_1 value of 300 : "
            == str(excinfo.value)
        )

    def test_pyspark_assert_expect_column_values_to_match_regex(self):
        df = self.spark.createDataFrame(
            [{"col_1": "BA2"}, {"col_1": "BA15"}, {"col_1": "SW1"}]
        )
        self.expect_column_values_to_match_regex(df, "col_1", "^[a-zA-Z]{2}[0-9]{1,2}$")

    def test_pyspark_assert_expect_column_values_to_match_regex_fail(self):
        import sys

        print(sys.executable)

        df = self.spark.createDataFrame(
            [{"col_1": "BA2"}, {"col_1": "BA151"}, {"col_1": "AAA13"}]
        )
        with pytest.raises(AssertionError) as excinfo:
            self.expect_column_values_to_match_regex(
                df, "col_1", "^[a-zA-Z]{2}[0-9]{1,2}$"
            )

        assert "Column col_1 did not match regular expression, found BA151 : " == str(
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

        self.expect_column_values_to_be_in_set(df, "col_1", fruits)

    def test_pyspark_assert_expect_column_values_to_be_in_set_case(self):
        df_fruits = [
            {"col_1": "Apple"},
            {"col_1": "Orange and Apple"},
            {"col_1": "Cherry"},
        ]
        fruits = set(("apple", "Orange And Apple", "Cherry"))
        df = self.spark.createDataFrame(df_fruits)

        self.expect_column_values_to_be_in_set(df, "col_1", fruits, ignore_case=True)

    def test_pyspark_assert_expect_column_values_to_be_in_set_fail(self):
        df_fruits = [
            {"col_1": "Tomato"},
            {"col_1": "Cherry"},
            {"col_1": "Apple"},
        ]
        fruits = set(("Apple", "Orange", "Pear", "Cherry"))
        df = self.spark.createDataFrame(df_fruits)

        with pytest.raises(AssertionError) as excinfo:
            self.expect_column_values_to_be_in_set(df, "col_1", fruits)

        assert (
            "Column col_1 provided set was not in actaul set of Apple, Cherry, Tomato : "
            == str(excinfo.value)
        )

    def test_pyspark_expect_column_values_to_be_of_type(self):
        df = self.spark.createDataFrame(
            [
                {"col_1": "BA2", "col_2": 10, "col_3": 10.45},
                {"col_1": "BA15", "col_2": 20, "col_3": 10.45},
                {"col_1": "SW1", "col_2": 30, "col_3": 10.45},
            ]
        )
        self.expect_column_values_to_be_of_type(df, "col_1", str)
        self.expect_column_values_to_be_of_type(df, "col_2", int)
        self.expect_column_values_to_be_of_type(df, "col_3", float)

    def test_pyspark_expect_column_values_to_be_of_type_fail(self):
        df = self.spark.createDataFrame(
            [
                {"col_1": "BA2", "col_2": 10, "col_3": 10.45},
                {"col_1": "BA15", "col_2": 20, "col_3": 10.45},
                {"col_1": "SW1", "col_2": 30, "col_3": 10.45},
            ]
        )

        with pytest.raises(AssertionError) as excinfo:
            self.expect_column_values_to_be_of_type(df, "col_1", int)

        assert "Column col_1 was not type <class 'int'> : " == str(excinfo.value)

        with pytest.raises(AssertionError) as excinfo:
            self.expect_column_values_to_be_of_type(df, "col_2", float)

        assert "Column col_2 was not type <class 'float'> : " == str(excinfo.value)

        with pytest.raises(AssertionError) as excinfo:
            self.expect_column_values_to_be_of_type(df, "col_3", str)

        assert "Column col_3 was not type <class 'str'> : " == str(excinfo.value)

    def test_assert_pyspark_expect_table_columns_to_match_ordered_list(
        self,
    ):
        df = self.spark.createDataFrame(
            [
                {"col_1": 100, "col_2": "a", "col_3": 1.01},
            ]
        )
        self.expect_table_columns_to_match_ordered_list(
            df, list(("col_1", "col_2", "col_3"))
        )

    def test_assert_pyspark_expect_table_columns_to_match_ordered_list_fail(
        self,
    ):
        df = self.spark.createDataFrame(
            [
                {"col_1": 100, "col_2": "a", "col_3": 1.01},
            ]
        )

        with pytest.raises(AssertionError) as excinfo:
            self.expect_table_columns_to_match_ordered_list(
                df, list(("col_2", "col_1", "col_3"))
            )

        assert (
            "Ordered columns did not match ordered columns col_1, col_2, col_3 : "
            == str(excinfo.value)
        )

    def test_assert_pyspark_expect_table_columns_to_match_set(self):
        df = self.spark.createDataFrame(
            [
                {"col_1": 100, "col_2": "a", "col_3": 1.01},
            ]
        )
        self.expect_table_columns_to_match_set(df, set(("col_1", "col_2", "col_3")))
        self.expect_table_columns_to_match_set(df, set(("col_2", "col_1", "col_3")))
        self.expect_table_columns_to_match_set(df, list(("col_1", "col_2", "col_3")))
        self.expect_table_columns_to_match_set(df, list(("col_2", "col_1", "col_3")))

    def test_assert_pyspark_expect_table_columns_to_match_set_fail(self):
        df = self.spark.createDataFrame(
            [
                {"col_1": 100, "col_2": "a", "col_3": 1.01},
            ]
        )

        with pytest.raises(AssertionError) as excinfo:
            self.expect_table_columns_to_match_set(df, set(("col_2", "col_1")))

        assert "Columns did not match set found col_1, col_2, col_3 : " == str(
            excinfo.value
        )

        with pytest.raises(AssertionError) as excinfo:
            self.expect_table_columns_to_match_set(df, list(("col_2", "col_1")))

        assert "Columns did not match set found col_1, col_2, col_3 : " == str(
            excinfo.value
        )

    def test_assert_expect_date_range_to_be_less_than(self):
        df = self.spark.createDataFrame(
            [
                {"col_1": "2019-05-13"},
                {"col_1": "2018-12-12"},
                {"col_1": "2015-10-01"},
            ]
        )
        self.expect_date_range_to_be_less_than(df, "col_1", "2019-05-14")

    def test_assert_expect_date_range_to_be_less_than_formatted(self):
        df = self.spark.createDataFrame(
            [
                {"col_1": "2019/05/13"},
                {"col_1": "2018/12/12"},
                {"col_1": "2015/10/01"},
            ]
        )
        self.expect_date_range_to_be_less_than(
            df, "col_1", "2019/05/14", date_format="%Y/%m/%d"
        )

    def test_assert_expect_date_range_to_be_less_than_default(self):
        df = self.spark.createDataFrame([{"col_1": ""}])

        self.expect_date_range_to_be_less_than(df, "col_1", "1900-01-02")

    def test_assert_expect_date_range_to_be_less_than_fail(self):
        df = self.spark.createDataFrame(
            [
                {"col_1": "2019-05-13"},
                {"col_1": "2018-12-12"},
                {"col_1": "2015-10-01"},
            ]
        )
        with pytest.raises(AssertionError) as excinfo:
            self.expect_date_range_to_be_less_than(df, "col_1", "2019-05-13")

        assert (
            "Column col_1 date is greater or equal than 2019-05-13 found 2019-05-13 : "
            == str(excinfo.value)
        )

    def test_assert_expect_date_range_to_be_more_than(self):
        df = self.spark.createDataFrame(
            [
                {"col_1": "2019-05-13"},
                {"col_1": "2018-12-12"},
                {"col_1": "2015-10-01"},
            ]
        )
        self.expect_date_range_to_be_more_than(df, "col_1", "2015-09-30")

    def test_assert_expect_date_range_to_be_more_than_default(self):
        df = self.spark.createDataFrame([{"col_1": ""}])

        self.expect_date_range_to_be_more_than(df, "col_1", "1899-12-31")

    def test_assert_expect_date_range_to_be_more_than_fail(self):
        df = self.spark.createDataFrame(
            [
                {"col_1": "2019-05-13"},
                {"col_1": "2018-12-12"},
                {"col_1": "2015-10-01"},
            ]
        )

        with pytest.raises(AssertionError) as excinfo:
            self.expect_date_range_to_be_more_than(df, "col_1", "2015-10-01")

        assert (
            "Column col_1 is less or equal than 2015-10-01 found 2015-10-01 : "
            == str(excinfo.value)
        )

    def test_assert_expect_date_range_to_be_between(self):
        df = self.spark.createDataFrame(
            [
                {"col_1": "2010-01-02"},
                {"col_1": "2025-01-01"},
            ]
        )

        self.expect_date_range_to_be_between(
            df, "col_1", date_start="2010-01-01", date_end="2025-01-02"
        )

    def test_assert_expect_date_range_to_be_between_start_date_greater_than_end(
        self,
    ):
        df = self.spark.createDataFrame(
            [
                {"col_1": "1975-01-01"},
            ]
        )

        with pytest.raises(AssertionError) as excinfo:
            self.expect_date_range_to_be_between(
                df, "col_1", date_start="1950-01-02", date_end="1950-01-01"
            )

        assert (
            "Column col_1 start date 1950-01-02 cannot be greater than end_date 1950-01-01 : "
            == str(excinfo.value)
        )

    def test_assert_expect_date_range_to_be_between_fail(self):
        df = self.spark.createDataFrame(
            [
                {"col_1": "2010-01-02"},
                {"col_1": "2025-01-02"},
            ]
        )

        with pytest.raises(AssertionError) as excinfo:
            self.expect_date_range_to_be_between(
                df, "col_1", date_start="2010-01-03", date_end="2025-01-03"
            )

        assert (
            "Column col_1 is not between 2010-01-03 and 2025-01-03 found 2010-01-02 : "
            == str(excinfo.value)
        )

        with pytest.raises(AssertionError) as excinfo:
            self.expect_date_range_to_be_between(
                df, "col_1", date_start="2010-01-01", date_end="2025-01-01"
            )

        assert (
            "Column col_1 is not between 2010-01-01 and 2025-01-01 found 2025-01-02 : "
            == str(excinfo.value)
        )

    def test_assert_expect_date_range_to_be_between_fail_equal(self):
        df = self.spark.createDataFrame(
            [
                {"col_1": "2010-01-02"},
                {"col_1": "2025-01-02"},
            ]
        )

        with pytest.raises(AssertionError) as excinfo:
            self.expect_date_range_to_be_between(
                df, "col_1", date_start="2010-01-02", date_end="2025-01-03"
            )

        assert (
            "Column col_1 is not between 2010-01-02 and 2025-01-03 found 2010-01-02 : "
            == str(excinfo.value)
        )

        with pytest.raises(AssertionError) as excinfo:
            self.expect_date_range_to_be_between(
                df, "col_1", date_start="2010-01-01", date_end="2025-01-02"
            )

        assert (
            "Column col_1 is not between 2010-01-01 and 2025-01-02 found 2025-01-02 : "
            == str(excinfo.value)
        )

    def test_expect_column_mean_to_be_between(self):
        df = self.spark.createDataFrame(
            [
                {"col_1": 100.05},
                {"col_1": 200.01},
                {"col_1": 300.05},
            ]
        )

        self.expect_column_mean_to_be_between(df, "col_1", 100.0, 400.0)

    def test_expect_column_mean_to_be_between_min_greater_than_max_fail(
        self,
    ):
        df = self.spark.createDataFrame(
            [
                {"col_1": 100.05},
                {"col_1": 200.01},
                {"col_1": 300.05},
            ]
        )

        with pytest.raises(AssertionError) as excinfo:
            self.expect_column_mean_to_be_between(df, "col_1", 200.0, 100.0)

        assert (
            "Column col_1 min_value 200.0 cannot be greater than max_value 100.0 : "
            == str(excinfo.value)
        )

    def test_expect_column_mean_to_be_between_fail_min_value(self):
        df = self.spark.createDataFrame(
            [
                {"col_1": 100.05},
                {"col_1": 200.01},
                {"col_1": 300.05},
            ]
        )

        with pytest.raises(AssertionError) as excinfo:
            self.expect_column_mean_to_be_between(df, "col_1", 300.0, 400.0)

        assert "Column col_1 mean 200.03667 is less than min_value 300.0 : " == str(
            excinfo.value
        )

    def test_expect_column_mean_to_be_between_fail_max_value(self):
        df = self.spark.createDataFrame(
            [
                {"col_1": 100.05},
                {"col_1": 200.01},
                {"col_1": 300.05},
            ]
        )

        with pytest.raises(AssertionError) as excinfo:
            self.expect_column_mean_to_be_between(df, "col_1", 100.0, 200.0)

        assert "Column col_1 mean 200.03667 is greater than max_value 200.0 : " == str(
            excinfo.value
        )

    def test_expect_column_value_counts_percent_to_be_between(self):
        df = self.spark.createDataFrame(
            [
                {"col_1": "Y"},
                {"col_1": "Y"},
                {"col_1": "N"},
                {"col_1": "Y"},
                {"col_1": "Y"},
                {"col_1": "N"},
                {"col_1": "N"},
                {"col_1": "Y"},
                {"col_1": "N"},
                {"col_1": ""},
            ]
        )
        value_counts = {
            "Y": {"min": 45, "max": 55},
            "N": {"min": 35, "max": 45},
            "": {"min": 5, "max": 15},
        }

        self.expect_column_value_counts_percent_to_be_between(df, "col_1", value_counts)

    def test_expect_column_value_counts_percent_to_be_between_fail_min(self):
        df = self.spark.createDataFrame(
            [
                {"col_1": "Y"},
                {"col_1": "Y"},
                {"col_1": "N"},
                {"col_1": "Y"},
                {"col_1": "Y"},
                {"col_1": "N"},
                {"col_1": "N"},
                {"col_1": "Y"},
                {"col_1": "N"},
                {"col_1": ""},
            ]
        )
        value_counts = {"Y": {"min": 55, "max": 65}}

        with pytest.raises(AssertionError) as excinfo:
            self.expect_column_value_counts_percent_to_be_between(
                df, "col_1", value_counts
            )

        assert (
            "Column col_1 the actual value count of (Y) is 50.00000% is less than the min allowed of 55% : "
            == str(excinfo.value)
        )

    def test_expect_column_value_counts_percent_to_be_between_fail_max(self):
        df = self.spark.createDataFrame(
            [
                {"col_1": "Y"},
                {"col_1": "Y"},
                {"col_1": "N"},
                {"col_1": "Y"},
                {"col_1": "Y"},
                {"col_1": "N"},
                {"col_1": "N"},
                {"col_1": "Y"},
                {"col_1": "N"},
                {"col_1": ""},
            ]
        )
        value_counts = {"Y": {"min": 35, "max": 40}}

        with pytest.raises(AssertionError) as excinfo:
            self.expect_column_value_counts_percent_to_be_between(
                df, "col_1", value_counts
            )

        assert (
            "Column col_1 the actual value count of (Y) is 50.00000% is more than the max allowed of 40% : "
            == str(excinfo.value)
        )

    def test_expect_column_value_counts_percent_to_be_between_fail_key_error(self):
        df = self.spark.createDataFrame(
            [
                {"col_1": "Y"},
                {"col_1": "N"},
                {"col_1": "Maybe"},
            ]
        )
        value_counts = {"Yes": {"min": 0, "max": 0}}

        with pytest.raises(AssertionError) as excinfo:
            self.expect_column_value_counts_percent_to_be_between(
                df, "col_1", value_counts
            )

        assert (
            "Check the key 'Yes' is not in the available value counts names Maybe, N, Y : "
            == str(excinfo.value)
        )

    def test_expect_column_value_counts_percent_to_be_between_fail_min_max_key_error(
        self,
    ):
        df = self.spark.createDataFrame(
            [
                {"col_1": "Y"},
            ]
        )

        # Assert verify min
        value_counts = {"Y": {"minimum": 0, "max": 0}}
        with pytest.raises(AssertionError) as excinfo:
            self.expect_column_value_counts_percent_to_be_between(
                df, "col_1", value_counts
            )

        assert "Value count for key 'Y' not contain 'min' : " == str(excinfo.value)

        # Assert verify max
        value_counts = {"Y": {"min": 0, "maximum": 0}}
        with pytest.raises(AssertionError) as excinfo:
            self.expect_column_value_counts_percent_to_be_between(
                df, "col_1", value_counts
            )

        assert "Value count for key 'Y' not contain 'max' : " == str(excinfo.value)

    def test_pyspark_expect_assert_frame_equal(self):
        left = self.spark.createDataFrame([{"col_1": 100}])
        right = self.spark.createDataFrame([{"col_1": 100}])

        self.expect_frames_equal(left, right)

    def test_pyspark_expect_assert_frame_equal_with_null(self):
        def ps(column: col):
            return regexp_extract(column, "^([A-Z])+", idx=0)

        df = self.spark.createDataFrame([{"col_1": "123"}])
        right = df.withColumn("col_2", ps(col("col_1")))
        left = self.spark.createDataFrame([{"col_1": "123", "col_2": ""}]).select(
            "col_1", "col_2"
        )

        self.expect_frames_equal(left, right)

    def test_pyspark_expect_assert_frame_equal_fail(self):
        left = self.spark.createDataFrame([{"col_1": 100}])
        right = self.spark.createDataFrame([{"col_2": 100}])

        with pytest.raises(AssertionError) as excinfo:
            self.expect_frames_equal(left, right)

        assert "DataFrames are different with Schemas are different Left Schema" in str(
            excinfo.value
        )

        left = self.spark.createDataFrame([{"col_1": 100}])
        right = self.spark.createDataFrame([{"col_1": 200}])

        with pytest.raises(AssertionError) as excinfo:
            self.expect_frames_equal(left, right)

        assert "DataFrames are different with Data is different" in str(excinfo.value)

    def test_pyspark_expect_assert_frame_equal_index_ignore(self):
        df = self.spark.createDataFrame([{"col_1": 200}, {"col_1": 100}])
        left = df.filter(df["col_1"] == 100)
        right = self.spark.createDataFrame([{"col_1": 100}])

        self.expect_frames_equal(left, right, check_index=False)

    def test_pyspark_expect_assert_frame_equal_dtype(self):
        left = self.spark.createDataFrame(
            [
                (100, "a"),
            ],
            T.StructType(
                [
                    T.StructField("col_1", T.IntegerType()),
                    T.StructField("col_1", T.StringType()),
                ]
            ),
        )
        right = self.spark.createDataFrame([{"col_1": 100, "col_2": "a"}])

        self.expect_frames_equal(left, right, check_dtype=False)

    def test_pyspark_expect_column_value_to_equal(self):
        df = self.spark.createDataFrame([{"col_1": 200}, {"col_1": 200}])
        self.expect_column_value_to_equal(df, "col_1", 200)

        df = self.spark.createDataFrame([{"col_1": "ABC"}, {"col_1": "ABC"}])
        self.expect_column_value_to_equal(df, "col_1", 200)

        df = self.spark.createDataFrame([{"col_1": 12.72}, {"col_1": 12.72}])
        self.expect_column_value_to_equal(df, "col_1", 12.72)

    def test_expect_column_value_to_equal_fails(self):
        df1 = self.spark.createDataFrame([{"col_1": 200}, {"col_1": 100}])
        with pytest.raises(AssertionError) as excinfo:
            self.expect_column_value_to_equal(df1, "col_1", 200)

        assert "Column col_1 was not equal, found 100 : " == str(excinfo.value)

        df2 = self.spark.createDataFrame(
            [{"col_1": "zSu"}, {"col_1": "zSu2"}, {"col_1": "zSu"}]
        )
        with pytest.raises(AssertionError) as excinfo:
            self.expect_column_value_to_equal(df2, "col_1", "zSu")

        assert "Column col_1 was not equal, found zSu2 : " == str(excinfo.value)

    def test_pyspark_expect_column_has_no_duplicate_rows_all(self):
        df = self.spark.createDataFrame(
            [
                {"col_1": 1, "col_2": "a"},
                {"col_1": 2, "col_2": "b"},
                {"col_1": 3, "col_2": "c"},
            ]
        )

        self.expect_column_has_no_duplicate_rows(df)

    def test_pyspark_expect_column_has_no_duplicate_rows_single(self):
        df = self.spark.createDataFrame(
            [
                {"col_1": 1, "col_2": "a"},
                {"col_1": 2, "col_2": "b"},
                {"col_1": 3, "col_2": "c"},
            ]
        )
        self.expect_column_has_no_duplicate_rows(df, "col_1")

    def test_pyspark_expect_column_has_no_duplicate_rows_list(self):
        df = self.spark.createDataFrame(
            [
                {"col_1": 1, "col_2": "a"},
                {"col_1": 2, "col_2": "b"},
                {"col_1": 3, "col_2": "c"},
            ]
        )
        self.expect_column_has_no_duplicate_rows(df, ["col_1", "col_2"])

    def test_pyspark_expect_column_has_no_duplicate_rows_all_fail(self):
        df = self.spark.createDataFrame(
            [
                {"col_1": 1, "col_2": "a"},
                {"col_1": 1, "col_2": "b"},
                {"col_1": 3, "col_2": "c"},
            ]
        )

        with pytest.raises(AssertionError) as excinfo:
            self.expect_column_has_no_duplicate_rows(df)

        assert "Column col_1 contains a duplicate value : " == str(excinfo.value)
