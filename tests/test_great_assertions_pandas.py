from pandas.core.indexing import check_bool_indexer
from great_assertions import GreatAssertions
import pandas as pd
import pytest


class GreatAssertionPandasTests(GreatAssertions):
    def test_pandas_incorrect_dataframe_type_raises_type_error(self):
        with pytest.raises(AssertionError) as excinfo:
            self.expect_column_values_to_be_of_type(1, "col_1", str)

        assert "Not a valid pandas/pyspark DataFrame" == str(excinfo.value)

        with pytest.raises(AssertionError) as excinfo:
            self.expect_column_values_to_be_in_set(1, "col_1", set(("Apple")))

        assert "Not a valid pandas/pyspark DataFrame" == str(excinfo.value)

    def test_pandas_expect_table_row_count_to_equal(self):
        df = pd.DataFrame({"col_1": [100, 200, 300], "col_2": [10, 20, 30]})
        self.expect_table_row_count_to_equal(df, 3)

    def test_pandas_expect_table_row_count_to_equal_fails(self):
        df = pd.DataFrame({"col_1": [100, 200, 300], "col_2": [10, 20, 30]})
        with pytest.raises(AssertionError) as excinfo:
            self.expect_table_row_count_to_equal(df, 4)

        assert "expected row count is 4 the actual was 3 : " == str(excinfo.value)

    def test_pandas_expect_table_row_count_to_be_greater_than(self):
        df = pd.DataFrame({"col_1": [100, 200, 300], "col_2": [10, 20, 30]})
        self.expect_table_row_count_to_be_greater_than(df, 2)

    def test_pandas_expect_table_row_count_to_be_greater_than_fails(self):
        df = pd.DataFrame({"col_1": [100, 200, 300], "col_2": [10, 20, 30]})
        with pytest.raises(AssertionError) as excinfo:
            self.expect_table_row_count_to_be_greater_than(df, 4)

        assert "expected row count of at least 4 but the actual was 3 : " == str(
            excinfo.value
        )

    def test_pandas_expect_table_row_count_to_be_less_than(self):
        df = pd.DataFrame({"col_1": [100, 200, 300], "col_2": [10, 20, 30]})
        self.expect_table_row_count_to_be_less_than(df, 4)

    def test_pandas_expect_table_row_count_to_be_less_fails(self):
        df = pd.DataFrame({"col_1": [100, 200, 300], "col_2": [10, 20, 30]})
        with pytest.raises(AssertionError) as excinfo:
            self.expect_table_row_count_to_be_less_than(df, 2)

        assert "expected row count of maximum 2 but the actual was 3 : " == str(
            excinfo.value
        )

    def test_pandas_expect_table_has_no_duplicate_rows(self):
        df = pd.DataFrame({"col_1": [100, 100, 300], "col_2": [10, 11, 12]})
        self.expect_table_has_no_duplicate_rows(df)

    def test_pandas_expect_table_has_no_duplicate_rows_fail(self):
        df = pd.DataFrame({"col_1": [100, 100, 300], "col_2": [10, 10, 12]})

        with pytest.raises(AssertionError) as excinfo:
            self.expect_table_has_no_duplicate_rows(df)

        assert "Table contains duplicate rows : " == str(excinfo.value)

    def test_pandas_assert_expect_column_values_to_be_between(self):
        # int
        df = pd.DataFrame({"col_1": [100, 200, 300], "col_2": [10, 20, 30]})
        self.expect_column_values_to_be_between(
            df, "col_1", min_value=99, max_value=301
        )
        self.expect_column_values_to_be_between(df, "col_1", 100, 300)

        # float
        df = pd.DataFrame({"col_1": [100.02, 200.01, 300.01], "col_2": [10, 20, 30]})
        self.expect_column_values_to_be_between(df, "col_1", 100.01, 300.02)

        # Equality float
        df = pd.DataFrame({"col_1": [100.05, 200.01, 300.05], "col_2": [10, 20, 30]})
        self.expect_column_values_to_be_between(df, "col_1", 100.05, 300.05)

    def test_pandas_assert_expect_column_values_to_be_between_min_fail(
        self,
    ):
        df = pd.DataFrame({"col_1": [100, 200, 300], "col_2": [10, 20, 30]})
        with pytest.raises(AssertionError) as excinfo:
            self.expect_column_values_to_be_between(df, "col_1", 101, 301)

        assert (
            "Min value provided (101) must be less than column col_1 value of 100 : "
            == str(excinfo.value)
        )

    def test_pandas_assert_expect_column_values_to_be_between_min_fail_bad_syntax(
        self,
    ):
        df = pd.DataFrame({"col_1": [100, 200, 300], "col_2": [10, 20, 30]})
        with pytest.raises(AssertionError) as excinfo:
            self.expect_column_values_to_be_between(df, "col_1", 100, 50)

        assert "Max value must be greater than min value : " == str(excinfo.value)

    def test_pandas_assert_expect_column_values_to_be_between_max_fail(
        self,
    ):
        df = pd.DataFrame({"col_1": [100, 200, 300], "col_2": [10, 20, 30]})
        with pytest.raises(AssertionError) as excinfo:
            self.expect_column_values_to_be_between(df, "col_1", 99, 299)

        assert (
            "Max value provided (299) must be greater than column col_1 value of 300 : "
            == str(excinfo.value)
        )

    def test_pandas_assert_expect_column_values_to_match_regex(self):
        df = pd.DataFrame({"col_1": ["BA2", "BA15", "Sw1"]})
        self.expect_column_values_to_match_regex(df, "col_1", "^[a-zA-Z]{2}[0-9]{1,2}$")
        self.expect_column_values_to_match_regex(df, "col_1", "^[a-zA-Z]{2}[0-9]{1,2}$")

    def test_pandas_assert_expect_column_values_to_match_regex_fail(self):
        df = pd.DataFrame({"col_1": ["bA2", "BA151", "SW1", "AAA13"]})
        with pytest.raises(AssertionError) as excinfo:
            self.expect_column_values_to_match_regex(
                df, "col_1", "^[a-zA-Z]{2}[0-9]{1,2}$"
            )

        assert "Column col_1 did not match regular expression, found BA151 : " == str(
            excinfo.value
        )

    def test_pandas_assert_expect_column_values_to_be_in_set(self):
        fruits = ["Apple", "Orange", "Pear", "Cherry", "Apricot(Summer)"]
        fruits_set = set(("Apple", "Orange", "Pear", "Cherry", "Apricot(Summer)"))
        df = pd.DataFrame({"col_1": fruits})

        self.expect_column_values_to_be_in_set(df, "col_1", fruits_set)

    def test_pandas_assert_expect_column_values_to_be_in_set_case(self):
        fruits = ["Apple", "Orange and Apples", "Pear", "Cherry"]
        fruits_set = set(("apple", "Orange And Apples", "pear", "cherry"))
        df = pd.DataFrame({"col_1": fruits})

        self.expect_column_values_to_be_in_set(
            df, "col_1", fruits_set, ignore_case=True
        )

    def test_pandas_assert_expect_column_values_to_be_in_set_fail(self):
        fruits = set(("Apple", "Orange", "Pear", "Cherry"))
        df = pd.DataFrame({"col_1": ["Tomato", "Cherry", "Apple"]})

        with pytest.raises(AssertionError) as excinfo:
            self.expect_column_values_to_be_in_set(df, "col_1", fruits)

        assert (
            "Column col_1 provided set was not in actaul set of Apple, Cherry, Tomato : "
            == str(excinfo.value)
        )

    def test_pandas_assert_expect_column_values_to_be_in_set_fail_with_type(
        self,
    ):
        fruits = set(("Apple", "Orange", "Pear", "Cherry"))
        df = pd.DataFrame({"col_1": ["Tomato", 1.0, "Apple"]})

        with pytest.raises(AssertionError) as excinfo:
            self.expect_column_values_to_be_in_set(df, "col_1", fruits)

        assert (
            "Column col_1 provided set was not in actaul set of Tomato, 1.0, Apple : "
            == str(excinfo.value)
        )

    def test_pandas_expect_column_values_to_be_of_type(self):
        df = pd.DataFrame(
            {
                "col_1": ["BA2", "BA15", "SW1"],
                "col_2": [10, 20, 30],
                "col_3": [10.45, 20.32, 30.23],
            }
        )
        self.expect_column_values_to_be_of_type(df, "col_1", str)
        self.expect_column_values_to_be_of_type(df, "col_2", int)
        self.expect_column_values_to_be_of_type(df, "col_3", float)

    def test_pandas_expect_column_values_to_be_of_type_fail_type(self):
        df = pd.DataFrame(
            {
                "col_1": ["BA2"],
            }
        )
        with pytest.raises(AssertionError) as excinfo:
            self.expect_column_values_to_be_of_type(df, "col_1", object)

        assert "Please check available types; str, float, int : " == str(excinfo.value)

    def test_pandas_expect_column_values_to_be_of_type_fail(self):
        df = pd.DataFrame(
            {
                "col_1": ["BA2", "BA15", "SW1"],
                "col_2": [10, 20, 30],
                "col_3": [10.45, 20.32, 30.23],
            }
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

    def test_assert_pandas_expect_table_columns_to_match_ordered_list(
        self,
    ):
        df = pd.DataFrame({"col_1": [100], "col_2": ["a"], "col_3": [1.01]})
        self.expect_table_columns_to_match_ordered_list(
            df, list(("col_1", "col_2", "col_3"))
        )

    def test_assert_pandas_expect_table_columns_to_match_ordered_list_fail(
        self,
    ):
        df = pd.DataFrame({"col_1": [100], "col_2": ["a"], "col_3": [1.01]})

        with pytest.raises(AssertionError) as excinfo:
            self.expect_table_columns_to_match_ordered_list(
                df, list(("col_2", "col_1", "col_3"))
            )

        assert (
            "Ordered columns did not match ordered columns col_1, col_2, col_3 : "
            == str(excinfo.value)
        )

    def test_assert_pandas_expect_table_columns_to_match_set(self):
        df = pd.DataFrame({"col_1": [100], "col_2": ["a"], "col_3": [1.01]})
        self.expect_table_columns_to_match_set(df, set(("col_1", "col_2", "col_3")))
        self.expect_table_columns_to_match_set(df, set(("col_2", "col_1", "col_3")))
        self.expect_table_columns_to_match_set(df, list(("col_1", "col_2", "col_3")))
        self.expect_table_columns_to_match_set(df, list(("col_2", "col_1", "col_3")))

    def test_assert_pandas_expect_table_columns_to_match_set_fail(self):
        df = pd.DataFrame({"col_1": [100], "col_2": ["a"], "col_3": [1.01]})

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
        df = pd.DataFrame({"col_1": ["2019-05-13", "2018-12-12", "2015-10-01"]})

        self.expect_date_range_to_be_less_than(df, "col_1", "2019-05-14")

    def test_assert_expect_date_range_to_be_less_than_default(self):
        df = pd.DataFrame({"col_1": [""]})

        self.expect_date_range_to_be_less_than(df, "col_1", "1900-01-02")

    def test_assert_expect_date_range_to_be_less_than_formatted(self):
        df = pd.DataFrame({"col_1": ["2019/05/13", "2018/12/12", "2015/10/01"]})

        self.expect_date_range_to_be_less_than(
            df, "col_1", "2019/05/14", date_format="%Y/%m/%d"
        )

    def test_assert_expect_date_range_to_be_less_than_fail(self):
        df = pd.DataFrame({"col_1": ["2019-05-13", "2018-12-12", "2015-10-01"]})

        with pytest.raises(AssertionError) as excinfo:
            self.expect_date_range_to_be_less_than(df, "col_1", "2019-05-13")

        assert (
            "Column col_1 date is greater or equal than 2019-05-13 found 2019-05-13 : "
            == str(excinfo.value)
        )

    def test_assert_expect_date_range_to_be_more_than(self):
        df = pd.DataFrame({"col_1": ["2019-05-13", "2018-12-12", "2015-10-01"]})

        self.expect_date_range_to_be_more_than(df, "col_1", "2015-09-30")

    def test_assert_expect_date_range_to_be_more_than_default(self):
        df = pd.DataFrame({"col_1": [""]})

        self.expect_date_range_to_be_more_than(df, "col_1", "1899-12-31")

    def test_assert_expect_date_range_to_be_more_than_fail(self):
        df = pd.DataFrame({"col_1": ["2019-05-13", "2018-12-12", "2015-10-01"]})

        with pytest.raises(AssertionError) as excinfo:
            self.expect_date_range_to_be_more_than(df, "col_1", "2015-10-01")

        assert (
            "Column col_1 is less or equal than 2015-10-01 found 2015-10-01 : "
            == str(excinfo.value)
        )

    def test_assert_expect_date_range_to_be_between(self):
        df = pd.DataFrame({"col_1": ["2010-01-02", "2025-01-01"]})

        self.expect_date_range_to_be_between(
            df, "col_1", date_start="2010-01-01", date_end="2025-01-02"
        )

    def test_assert_expect_date_range_to_be_between_start_date_greater_than_end(
        self,
    ):
        df = pd.DataFrame({"col_1": ["1975-01-01"]})

        with pytest.raises(AssertionError) as excinfo:
            self.expect_date_range_to_be_between(
                df, "col_1", date_start="1950-01-02", date_end="1950-01-01"
            )

        assert (
            "Column col_1 start date 1950-01-02 cannot be greater than end_date 1950-01-01 : "
            == str(excinfo.value)
        )

    def test_assert_expect_date_range_to_be_between_fail(self):
        df = pd.DataFrame({"col_1": ["2010-01-02", "2025-01-02"]})

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
        df = pd.DataFrame({"col_1": ["2010-01-02", "2025-01-02"]})

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
        df = pd.DataFrame({"col_1": [100.05, 200.01, 300.05]})

        self.expect_column_mean_to_be_between(df, "col_1", 100.0, 400.0)

    def test_expect_column_mean_to_be_between_min_greater_than_max_fail(
        self,
    ):
        df = pd.DataFrame({"col_1": [100.05, 200.01, 300.05]})

        with pytest.raises(AssertionError) as excinfo:
            self.expect_column_mean_to_be_between(df, "col_1", 200.0, 100.0)

        assert (
            "Column col_1 min_value 200.0 cannot be greater than max_value 100.0 : "
            == str(excinfo.value)
        )

    def test_expect_column_mean_to_be_between_fail_min_value(self):
        df = pd.DataFrame({"col_1": [100.05, 200.01, 300.05]})

        with pytest.raises(AssertionError) as excinfo:
            self.expect_column_mean_to_be_between(df, "col_1", 300.0, 400.0)

        assert "Column col_1 mean 200.03667 is less than min_value 300.0 : " == str(
            excinfo.value
        )

    def test_expect_column_mean_to_be_between_fail_max_value(self):
        df = pd.DataFrame({"col_1": [100.05, 200.01, 300.05]})

        with pytest.raises(AssertionError) as excinfo:
            self.expect_column_mean_to_be_between(df, "col_1", 100.0, 200.0)

        assert "Column col_1 mean 200.03667 is greater than max_value 200.0 : " == str(
            excinfo.value
        )

    def test_expect_column_value_counts_percent_to_be_between(self):
        df = pd.DataFrame(
            {
                "col_1": ["Y", "Y", "N", "Y", "Y", "N", "N", "Y", "N", ""],
            }
        )
        value_counts = {
            "Y": {"min": 45, "max": 55},
            "N": {"min": 35, "max": 45},
            "": {"min": 5, "max": 15},
        }

        self.expect_column_value_counts_percent_to_be_between(df, "col_1", value_counts)

    def test_expect_column_value_counts_percent_to_be_between_fail_min(self):
        df = pd.DataFrame(
            {
                "col_1": ["Y", "Y", "N", "Y", "Y", "N", "N", "Y", "Y", "", "N", ""],
            }
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
        df = pd.DataFrame(
            {
                "col_1": ["Y", "Y", "N", "Y", "Y", "N", "N", "Y", "Y", "", "N", ""],
            }
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
        df = pd.DataFrame(
            {
                "col_1": ["Y", "N", "Maybe"],
            }
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
        df = pd.DataFrame(
            {
                "col_1": ["Y"],
            }
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

    def test_expect_assert_frame_equal(self):
        left = pd.DataFrame({"col_1": [1]})
        right = pd.DataFrame({"col_1": [1]})
        self.expect_frames_equal(left, right)

    def test_expect_assert_frame_equal_dtype(self):
        left = pd.DataFrame({"col_1": [1, 2]})
        right = pd.DataFrame({"col_1": [1, 2]})
        left = left.astype({"col_1": "int32"})
        right = left.astype({"col_1": "int64"})

        self.expect_frames_equal(left, right, check_dtype=False)

    def test_expect_assert_frame_equal_ignore_index(self):
        df = pd.DataFrame({"col_1": [2, 1]})
        left = df[df["col_1"] == 1]
        right = pd.DataFrame({"col_1": [1]})

        self.expect_frames_equal(left, right, check_index=False)

    def test_expect_assert_frame_equal_bad_type(self):
        from pyspark.sql import SparkSession

        left = pd.DataFrame({"col_1": [1]})
        spark = SparkSession.builder.getOrCreate()
        right = spark.createDataFrame([{"col_1": 100}])

        with pytest.raises(AssertionError) as excinfo:
            self.expect_frames_equal(left, right)

        assert "Different DataFrame types : " == str(excinfo.value)

    def test_expect_assert_frame_equal_fail(self):
        left = pd.DataFrame({"col_1": [1]})
        right = pd.DataFrame({"col_1": [2]})

        with pytest.raises(AssertionError) as excinfo:
            self.expect_frames_equal(left, right)

        # Just check the GA code, the fail is returned
        # with panda exception or the GASpark code
        assert "DataFrames are different" in str(excinfo.value)

    def test_expect_column_value_to_equal(self):
        df = pd.DataFrame({"col_1": [1, 1, 1, 1]})
        self.expect_column_value_to_equal(df, "col_1", 1)

        df = pd.DataFrame({"col_1": ["h", "h", "h", "h"]})
        self.expect_column_value_to_equal(df, "col_1", "h")

        df = pd.DataFrame({"col_1": [1.1, 1.1, 1.1, 1.1]})
        self.expect_column_value_to_equal(df, "col_1", 1.1)

    def test_expect_column_value_to_equal_fails(self):
        df1 = pd.DataFrame({"col_1": [2, 2, 5, 2]})
        with pytest.raises(AssertionError) as excinfo:
            self.expect_column_value_to_equal(df1, "col_1", 2)

        assert "Column col_1 was not equal, found 5 : " == str(excinfo.value)

        df2 = pd.DataFrame({"col_1": ["d", "d", "e", "d"]})
        with pytest.raises(AssertionError) as excinfo:
            self.expect_column_value_to_equal(df2, "col_1", "d")

        assert "Column col_1 was not equal, found e : " == str(excinfo.value)

    def test_expect_column_has_no_duplicate_rows_all(self):
        df = pd.DataFrame({"col_1": [1, 2, 3], "col_2": ["a", "b", "c"]})
        self.expect_column_has_no_duplicate_rows(df)

    def test_expect_column_has_no_duplicate_rows_single(self):
        df = pd.DataFrame({"col_1": [1, 2, 3], "col_2": ["a", "b", "c"]})
        self.expect_column_has_no_duplicate_rows(df, "col_1")

    def test_expect_column_has_no_duplicate_rows_list(self):
        df = pd.DataFrame({"col_1": [1, 2, 3], "col_2": ["a", "b", "c"]})
        self.expect_column_has_no_duplicate_rows(df, ["col_1", "col_2"])

    def test_expect_column_has_no_duplicate_rows_all_fail(self):
        df = pd.DataFrame({"col_1": [1, 2, 1, 4]})

        with pytest.raises(AssertionError) as excinfo:
            self.expect_column_has_no_duplicate_rows(df)

        assert "Column col_1 contains a duplicate value : " == str(excinfo.value)

    def test_expect_column_has_no_duplicate_rows_all_fail_multi_cols(self):
        df = pd.DataFrame({"col_1": [1, 2, 3], "col_2": ["a", "a", "c"]})

        with pytest.raises(AssertionError) as excinfo:
            self.expect_column_has_no_duplicate_rows(df)

        assert "Column col_2 contains a duplicate value : " == str(excinfo.value)

    def test_expect_column_has_no_duplicate_rows_single_fail(self):
        df = pd.DataFrame({"col_1": [1, 2, 3], "col_2": ["a", "c", "c"]})

        with pytest.raises(AssertionError) as excinfo:
            self.expect_column_has_no_duplicate_rows(df, "col_2")

        assert "Column col_2 contains a duplicate value : " == str(excinfo.value)

    def test_expect_column_has_no_duplicate_rows_single_fail_incorrect_col_name(self):
        df = pd.DataFrame({"col_1": [1]})

        with pytest.raises(AssertionError) as excinfo:
            self.expect_column_has_no_duplicate_rows(df, "bla")

        assert "Column bla is not valid : " == str(excinfo.value)

        with pytest.raises(AssertionError) as excinfo:
            self.expect_column_has_no_duplicate_rows(df, ["bla2"])

        assert "Column bla2 is not valid : " == str(excinfo.value)

    def test_expect_column_has_no_duplicate_rows_list_fail(self):
        df = pd.DataFrame({"col_1": [1, 2, 3], "col_2": ["a", "c", "c"]})

        with pytest.raises(AssertionError) as excinfo:
            self.expect_column_has_no_duplicate_rows(df, ["col_2"])

        assert "Column col_2 contains a duplicate value : " == str(excinfo.value)

    def test_expect_column_has_no_duplicate_rows_type_unknown(self):
        df = pd.DataFrame({"col_1": [1]})

        with pytest.raises(AssertionError) as excinfo:
            self.expect_column_has_no_duplicate_rows(df, 1)

        assert "Check help for method usage : " == str(excinfo.value)
