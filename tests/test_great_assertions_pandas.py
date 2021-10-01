from great_assertions.great_assertions import GreatAssertions
import pandas as pd
import pytest


class GreatAssertionPandasTests(GreatAssertions):
    def test_pandas_incorrect_dataframe_type_raises_type_error(self):
        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectTableRowCountToEqual(1, 1)

        assert "Not a valid pandas/pyspark DataFrame" == str(excinfo.value)

    def test_pandas_expect_table_row_count_to_equal(self):
        df = pd.DataFrame({"col_1": [100, 200, 300], "col_2": [10, 20, 30]})
        self.assertExpectTableRowCountToEqual(df, 3)
        self.expect_table_row_count_to_equal(df, 3)

    def test_pandas_expect_table_row_count_to_equal_fails(self):
        df = pd.DataFrame({"col_1": [100, 200, 300], "col_2": [10, 20, 30]})
        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectTableRowCountToEqual(df, 4)

        assert "expected row count is 4 the actual was 3 : " == str(excinfo.value)

    # assertExpectColumnValueLengthsToEqual
    def test_pandas_assert_expect_column_values_to_be_between(self):
        # int
        df = pd.DataFrame({"col_1": [100, 200, 300], "col_2": [10, 20, 30]})
        self.assertExpectColumnValuesToBeBetween(
            df, "col_1", min_value=99, max_value=301
        )
        self.expect_column_values_to_be_between(df, "col_1", 100, 300)

        # float
        df = pd.DataFrame({"col_1": [100.02, 200.01, 300.01], "col_2": [10, 20, 30]})
        self.assertExpectColumnValuesToBeBetween(df, "col_1", 100.01, 300.02)

        # Same float
        df = pd.DataFrame({"col_1": [100.05, 200.01, 300.05], "col_2": [10, 20, 30]})
        self.assertExpectColumnValuesToBeBetween(df, "col_1", 100.05, 300.05)

    def test_pandas_assert_expect_column_values_to_be_between_min_fail(
        self,
    ):
        df = pd.DataFrame({"col_1": [100, 200, 300], "col_2": [10, 20, 30]})
        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectColumnValuesToBeBetween(df, "col_1", 101, 301)

        assert (
            "Min value provided (101) must be less than column col_1 value of 100 : "
            == str(excinfo.value)
        )

    def test_pandas_assert_expect_column_values_to_be_between_min_fail_bad_syntax(
        self,
    ):
        df = pd.DataFrame({"col_1": [100, 200, 300], "col_2": [10, 20, 30]})
        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectColumnValuesToBeBetween(df, "col_1", 100, 50)

        assert "Max value must be greater than min value : " == str(excinfo.value)

    def test_pandas_assert_expect_column_values_to_be_between_max_fail(
        self,
    ):
        df = pd.DataFrame({"col_1": [100, 200, 300], "col_2": [10, 20, 30]})
        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectColumnValuesToBeBetween(df, "col_1", 99, 299)

        assert (
            "Max value provided (299) must be greater than column col_1 value of 300 : "
            == str(excinfo.value)
        )

    def test_pandas_assert_expect_column_values_to_match_regex(self):
        df = pd.DataFrame({"col_1": ["BA2", "BA15", "Sw1"]})
        self.assertExpectColumnValuesToMatchRegex(
            df, "col_1", "^[a-zA-Z]{2}[0-9]{1,2}$"
        )
        self.expect_column_values_to_match_regex(df, "col_1", "^[a-zA-Z]{2}[0-9]{1,2}$")

    def test_pandas_assert_expect_column_values_to_match_regex_fail(self):
        df = pd.DataFrame({"col_1": ["bA2", "BA151", "SW1", "AAA13"]})
        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectColumnValuesToMatchRegex(
                df, "col_1", "^[a-zA-Z]{2}[0-9]{1,2}$"
            )

        assert "Column col_1 did not match regular expression, found BA151 : " == str(
            excinfo.value
        )

    def test_pandas_assert_expect_column_values_to_be_in_set(self):
        fruits = ["Apple", "Orange", "Pear", "Cherry", "Apricot(Summer)"]
        fruits_set = set(("Apple", "Orange", "Pear", "Cherry", "Apricot(Summer)"))
        df = pd.DataFrame({"col_1": fruits})

        self.assertExpectColumnValuesToBeInSet(df, "col_1", fruits_set)
        self.expect_column_values_to_be_in_set(df, "col_1", fruits_set)

    def test_pandas_assert_expect_column_values_to_be_in_set_fail(self):
        fruits = set(("Apple", "Orange", "Pear", "Cherry"))
        df = pd.DataFrame({"col_1": ["Tomato", "Cherry", "Apple"]})

        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectColumnValuesToBeInSet(df, "col_1", fruits)

        assert "Column col_1 provided set was not in Apple, Cherry, Tomato : " == str(
            excinfo.value
        )

    def test_pandas_assert_expect_column_values_to_be_in_set_fail_with_type(
        self,
    ):
        fruits = set(("Apple", "Orange", "Pear", "Cherry"))
        df = pd.DataFrame({"col_1": ["Tomato", 1.0, "Apple"]})

        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectColumnValuesToBeInSet(df, "col_1", fruits)

        assert "Column col_1 provided set was not in Tomato, 1.0, Apple : " == str(
            excinfo.value
        )

    def test_pandas_expect_column_values_to_be_of_type(self):
        df = pd.DataFrame(
            {
                "col_1": ["BA2", "BA15", "SW1"],
                "col_2": [10, 20, 30],
                "col_3": [10.45, 20.32, 30.23],
            }
        )
        self.assertExpectColumnValuesToBeOfType(df, "col_1", str)
        self.assertExpectColumnValuesToBeOfType(df, "col_2", int)
        self.assertExpectColumnValuesToBeOfType(df, "col_3", float)
        self.expect_column_values_to_be_of_type(df, "col_3", float)

    def test_pandas_expect_column_values_to_be_of_type_fail_type(self):
        df = pd.DataFrame(
            {
                "col_1": ["BA2"],
            }
        )
        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectColumnValuesToBeOfType(df, "col_1", object)

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
            self.assertExpectColumnValuesToBeOfType(df, "col_1", int)

        assert "Column col_1 was not type <class 'int'> : " == str(excinfo.value)

        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectColumnValuesToBeOfType(df, "col_2", float)

        assert "Column col_2 was not type <class 'float'> : " == str(excinfo.value)

        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectColumnValuesToBeOfType(df, "col_3", str)

        assert "Column col_3 was not type <class 'str'> : " == str(excinfo.value)

    def test_assert_pandas_expect_table_columns_to_match_ordered_list(
        self,
    ):
        df = pd.DataFrame({"col_1": [100], "col_2": ["a"], "col_3": [1.01]})
        self.assertExpectTableColumnsToMatchOrderedList(
            df, list(("col_1", "col_2", "col_3"))
        )
        self.expect_table_columns_to_match_ordered_list(
            df, list(("col_1", "col_2", "col_3"))
        )

    def test_assert_pandas_expect_table_columns_to_match_ordered_list_fail(
        self,
    ):
        df = pd.DataFrame({"col_1": [100], "col_2": ["a"], "col_3": [1.01]})

        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectTableColumnsToMatchOrderedList(
                df, list(("col_2", "col_1", "col_3"))
            )

        assert (
            "Ordered columns did not match ordered columns col_1, col_2, col_3 : "
            == str(excinfo.value)
        )

    def test_assert_pandas_expect_table_columns_to_match_set(self):
        df = pd.DataFrame({"col_1": [100], "col_2": ["a"], "col_3": [1.01]})
        self.assertExpectTableColumnsToMatchSet(df, set(("col_1", "col_2", "col_3")))
        # self.assertExpectTableColumnsToMatchSet(df, set(("col_2", "col_1", "col_3")))
        # self.assertExpectTableColumnsToMatchSet(df, list(("col_1", "col_2", "col_3")))
        # self.assertExpectTableColumnsToMatchSet(df, list(("col_2", "col_1", "col_3")))
        # self.expect_table_columns_to_match_set(df, list(("col_2", "col_1", "col_3")))

    def test_assert_pandas_expect_table_columns_to_match_set_fail(self):
        df = pd.DataFrame({"col_1": [100], "col_2": ["a"], "col_3": [1.01]})

        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectTableColumnsToMatchSet(df, set(("col_2", "col_1")))

        assert "Columns did not match set found col_1, col_2, col_3 : " == str(
            excinfo.value
        )

        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectTableColumnsToMatchSet(df, list(("col_2", "col_1")))

        assert "Columns did not match set found col_1, col_2, col_3 : " == str(
            excinfo.value
        )

    def test_assert_expect_date_range_to_be_less_than(self):
        df = pd.DataFrame({"col_1": ["2019-05-13", "2018-12-12", "2015-10-01"]})

        self.assertExpectDateRangeToBeLessThan(df, "col_1", "2019-05-14")

    def test_assert_expect_date_range_to_be_less_than_default(self):
        df = pd.DataFrame({"col_1": [""]})

        self.assertExpectDateRangeToBeLessThan(df, "col_1", "1900-01-02")

    def test_assert_expect_date_range_to_be_less_than_formatted(self):
        df = pd.DataFrame({"col_1": ["2019/05/13", "2018/12/12", "2015/10/01"]})

        self.assertExpectDateRangeToBeLessThan(
            df, "col_1", "2019/05/14", format="%Y/%m/%d"
        )

    def test_assert_expect_date_range_to_be_less_than_fail(self):
        df = pd.DataFrame({"col_1": ["2019-05-13", "2018-12-12", "2015-10-01"]})

        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectDateRangeToBeLessThan(df, "col_1", "2019-05-13")

        assert (
            "Column col_1 date is greater or equal than 2019-05-13 found 2019-05-13 : "
            == str(excinfo.value)
        )

    def test_assert_expect_date_range_to_be_more_than(self):
        df = pd.DataFrame({"col_1": ["2019-05-13", "2018-12-12", "2015-10-01"]})

        self.assertExpectDateRangeToBeMoreThan(df, "col_1", "2015-09-30")

    def test_assert_expect_date_range_to_be_more_than_default(self):
        df = pd.DataFrame({"col_1": [""]})

        self.assertExpectDateRangeToBeMoreThan(df, "col_1", "1899-12-31")

    def test_assert_expect_date_range_to_be_more_than_fail(self):
        df = pd.DataFrame({"col_1": ["2019-05-13", "2018-12-12", "2015-10-01"]})

        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectDateRangeToBeMoreThan(df, "col_1", "2015-10-01")

        assert (
            "Column col_1 is less or equal than 2015-10-01 found 2015-10-01 : "
            == str(excinfo.value)
        )

    def test_assert_expect_date_range_to_be_between(self):
        df = pd.DataFrame({"col_1": ["2010-01-02", "2025-01-01"]})

        self.assertExpectDateRangeToBeBetween(
            df, "col_1", date_start="2010-01-01", date_end="2025-01-02"
        )

    def test_assert_expect_date_range_to_be_between_start_date_greater_than_end(
        self,
    ):
        df = pd.DataFrame({"col_1": ["1975-01-01"]})

        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectDateRangeToBeBetween(
                df, "col_1", date_start="1950-01-02", date_end="1950-01-01"
            )

        assert (
            "Column col_1 start date 1950-01-02 cannot be greater than end_date 1950-01-01 : "
            == str(excinfo.value)
        )

    def test_assert_expect_date_range_to_be_between_fail(self):
        df = pd.DataFrame({"col_1": ["2010-01-02", "2025-01-02"]})

        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectDateRangeToBeBetween(
                df, "col_1", date_start="2010-01-03", date_end="2025-01-03"
            )

        assert (
            "Column col_1 is not between 2010-01-03 and 2025-01-03 found 2010-01-02 : "
            == str(excinfo.value)
        )

        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectDateRangeToBeBetween(
                df, "col_1", date_start="2010-01-01", date_end="2025-01-01"
            )

        assert (
            "Column col_1 is not between 2010-01-01 and 2025-01-01 found 2025-01-02 : "
            == str(excinfo.value)
        )

    def test_assert_expect_date_range_to_be_between_fail_equal(self):
        df = pd.DataFrame({"col_1": ["2010-01-02", "2025-01-02"]})

        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectDateRangeToBeBetween(
                df, "col_1", date_start="2010-01-02", date_end="2025-01-03"
            )

        assert (
            "Column col_1 is not between 2010-01-02 and 2025-01-03 found 2010-01-02 : "
            == str(excinfo.value)
        )

        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectDateRangeToBeBetween(
                df, "col_1", date_start="2010-01-01", date_end="2025-01-02"
            )

        assert (
            "Column col_1 is not between 2010-01-01 and 2025-01-02 found 2025-01-02 : "
            == str(excinfo.value)
        )

    def test_expect_column_mean_to_be_between(self):
        df = pd.DataFrame({"col_1": [100.05, 200.01, 300.05]})

        self.assertExpectColumnMeanToBeBetween(df, "col_1", 100.0, 400.0)

    def test_expect_column_mean_to_be_between_min_greater_than_max_fail(
        self,
    ):
        df = pd.DataFrame({"col_1": [100.05, 200.01, 300.05]})

        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectColumnMeanToBeBetween(df, "col_1", 200.0, 100.0)

        assert (
            "Column col_1 min_value 200.0 cannot be greater than max_value 100.0 : "
            == str(excinfo.value)
        )

    def test_expect_column_mean_to_be_between_fail_min_value(self):
        df = pd.DataFrame({"col_1": [100.05, 200.01, 300.05]})

        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectColumnMeanToBeBetween(df, "col_1", 300.0, 400.0)

        assert "Column col_1 mean 200.03667 is less than min_value 300.0 : " == str(
            excinfo.value
        )

    def test_expect_column_mean_to_be_between_fail_max_value(self):
        df = pd.DataFrame({"col_1": [100.05, 200.01, 300.05]})

        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectColumnMeanToBeBetween(df, "col_1", 100.0, 200.0)

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

        self.assertExpectColumnValueCountsPercentToBeBetween(df, "col_1", value_counts)

    def test_expect_column_value_counts_percent_to_be_between_fail_min(self):
        df = pd.DataFrame(
            {
                "col_1": ["Y", "Y", "N", "Y", "Y", "N", "N", "Y", "Y", "", "N", ""],
            }
        )
        value_counts = {"Y": {"min": 55, "max": 65}}

        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectColumnValueCountsPercentToBeBetween(
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
            self.assertExpectColumnValueCountsPercentToBeBetween(
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
            self.assertExpectColumnValueCountsPercentToBeBetween(
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
            self.assertExpectColumnValueCountsPercentToBeBetween(
                df, "col_1", value_counts
            )

        assert "Value count for key 'Y' not contain 'min' : " == str(excinfo.value)

        # Assert verify max
        value_counts = {"Y": {"min": 0, "maximum": 0}}
        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectColumnValueCountsPercentToBeBetween(
                df, "col_1", value_counts
            )

        assert "Value count for key 'Y' not contain 'max' : " == str(excinfo.value)
