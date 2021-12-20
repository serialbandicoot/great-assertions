"""
Great Assertions.

This library is inspired by the Great Expectations library and has made various expectations
found in Great Expectations available when using the inbuilt python unittest assertions.

The library has also added in further expectations, which may be similar or new.
"""

import unittest
from datetime import datetime
from src.utils import (
    _get_dataframe_import_type,
    _default_null_dates,
    _get_dataframe_type,
)
from typing import Optional, Union, Set, List
from unittest.runner import TextTestResult


class GreatAssertionResult(TextTestResult):
    def __init__(self, *args, **kwargs):
        super(GreatAssertionResult, self).__init__(*args, **kwargs)
        self.successes = []
        self.extended = {}

    def addSuccess(self, test):
        super(GreatAssertionResult, self).addSuccess(test)
        self.successes.append(test)

    def _get_result_quantity(self, result_type: list) -> int:
        return list(filter(lambda x: x[0]._testMethodName, result_type))

    def to_results_table(self):
        import pandas as pd

        succeeded = list(
            filter(lambda success: success._testMethodName, self.successes)
        )
        errors = self._get_result_quantity(self.errors)
        failures = self._get_result_quantity(self.failures)
        skipped = self._get_result_quantity(self.skipped)

        data = [
            ["succeeded", len(succeeded)],
            ["errors", len(errors)],
            ["fails", len(failures)],
            ["skipped", len(skipped)],
        ]

        return pd.DataFrame(data, columns=["Type", "Quantity"])

    def __test_info(self, iter, status: str):
        return [iter[0]._testMethodName, str(iter[1]), status]

    def to_full_results_table(self):
        import pandas as pd

        data = []
        [data.append(self.__test_info(f, "Fail")) for f in self.failures]
        [data.append(self.__test_info(e, "Error")) for e in self.errors]
        [data.append(self.__test_info(s, "Skip")) for s in self.skipped]
        [
            data.append([success._testMethodName, "", "Pass"])
            for success in self.successes
        ]

        col = ["Test Method", "Test Information", "Test Status"]

        return pd.DataFrame(data, columns=col)

    @property
    def _get_grouped_results(self):
        return self.to_results_table().groupby(["Type"]).sum()

    def to_pie(self, title="Test Result", colors=["gray", "red", "blue", "green"]):
        return self._get_grouped_results.plot(
            kind="pie", y="Quantity", title=title, colors=colors
        )

    def to_barh(self, title="Test Result", color=["gray", "red", "blue", "green"]):
        return self._get_grouped_results.plot(
            kind="barh", y="Quantity", title=title, color=color
        )

    def __test_info_with_extended(self, iter, status: str):
        return [iter[0]._testMethodName, str(iter[1]), status, iter[0].extended]

    @property
    def _to_extended_info(self):
        import pandas as pd

        pd.set_option("display.width", 150)

        data = []
        [data.append(self.__test_info_with_extended(f, "Fail")) for f in self.failures]
        [data.append(self.__test_info_with_extended(e, "Error")) for e in self.errors]
        [data.append(self.__test_info_with_extended(s, "Skip")) for s in self.skipped]
        [
            data.append([success._testMethodName, "", "Pass", ""])
            for success in self.successes
        ]

        col = ["method", "information", "status", "extended"]

        return pd.DataFrame(data, columns=col)

    def save(self, format, **args):
        """This method will write the results to a chosen format."""

        extended_info = self._to_extended_info

        if format.lower() == "databricks" or format.lower() == "pyspark":
            spark = args["spark"]
            result_table = (
                args["result_table"] if "result_table" in args else "ga_result"
            )

            spark_df = spark.createDataFrame(extended_info)
            spark_df.write.mode("append").saveAsTable(result_table)


class GreatAssertions(unittest.TestCase):
    """
    GreatAssertions.

    A class which inherits unittest.TestCase and appends Great Expectation styled Assertions

    """

    def expect_table_row_count_to_equal(self, df, expected_count: int, msg=""):
        """Expect the number of rows to equal the count.

        Parameters
        ----------
            df (DataFrame)       : Pandas or PySpark DataFrame
            expected_count (int) : The expected row count of the DataFrame
            msg (str)            : Optional message if the assertion fails
        """

        df = _get_dataframe_import_type(df)
        actual_row_count = df.row_count

        if expected_count != actual_row_count:
            msg = self._formatMessage(
                msg,
                f"expected row count is {expected_count} the actual was {actual_row_count}",
            )
            self.extended = {
                "id": 1,
                "values": {"exp_count": expected_count, "act_count": actual_row_count},
            }
            raise self.failureException(msg)

        return

    def expect_table_row_count_to_be_less_than(
        self, df, expected_max_count: int, msg=""
    ):
        """Expect the number of rows to be less than the count.

        Parameters
        ----------
            df (DataFrame)       : Pandas or PySpark DataFrame
            expected_count (int) : The expected row count of the DataFrame
            msg (str)            : Optional message if the assertion fails
        """

        df = _get_dataframe_import_type(df)
        actual_row_count = df.row_count

        if actual_row_count >= expected_max_count:
            msg = self._formatMessage(
                msg,
                f"expected row count of maximum {expected_max_count} but the actual was {actual_row_count}",
            )
            raise self.failureException(msg)

        return

    def expect_table_row_count_to_be_greater_than(
        self, df, expected_min_count: int, msg=""
    ):
        """Expect the number of rows to be greater than the count.

        Parameters
        ----------
            df (DataFrame)       : Pandas or PySpark DataFrame
            expected_count (int) : The expected row count of the DataFrame
            msg (str)            : Optional message if the assertion fails
        """

        df = _get_dataframe_import_type(df)
        actual_row_count = df.row_count

        if actual_row_count <= expected_min_count:
            msg = self._formatMessage(
                msg,
                f"expected row count of at least {expected_min_count} but the actual was {actual_row_count}",
            )
            raise self.failureException(msg)

        return

    def expect_table_has_no_duplicate_rows(self, df, msg=""):
        """Expect the table to only have unique rows.

        Parameters
        ----------
            df (DataFrame) : Pandas or PySpark DataFrame
            msg (str)      : Optional message if the assertion fails
        """

        df = _get_dataframe_import_type(df)
        actual_row_count = df.row_count
        unique_rows = df.drop_duplicates().row_count

        if actual_row_count != unique_rows:
            msg = self._formatMessage(
                msg,
                "Table contains duplicate rows",
            )
            raise self.failureException(msg)

        return

    def expect_column_has_no_duplicate_rows(self, df, columns="*", msg=""):
        """Expect the column/s to only have unique rows.

        Parameters
        ----------
            df (DataFrame)     : Pandas or PySpark DataFrame
            columns (str/list) : Single, Array, Empty or * for all columns
            msg (str)          : Optional message if the assertion fails
        """

        def assert_column(column, msg):
            """Internal column assertion - uses df scope from outer method."""
            try:
                df_col = _get_dataframe_import_type(df).get_column(column)
                if df_col.row_count != df_col.drop_duplicates().row_count:
                    msg = self._formatMessage(
                        msg,
                        f"Column {column} contains a duplicate value",
                    )
                    raise self.failureException(msg)
            except KeyError:
                msg = self._formatMessage(
                    msg,
                    f"Column {column} is not valid",
                )
                raise self.failureException(msg)

        if columns == "*":
            [assert_column(column, msg) for column in list(df.columns)]
        elif type(columns) is list:
            [assert_column(column, msg) for column in columns]
        elif type(columns) is str:
            assert_column(columns, msg)
        else:
            msg = self._formatMessage(
                msg,
                "Check help for method usage",
            )
            raise self.failureException(msg)

        return

    def expect_column_value_to_equal(self, df, column: str, value: object, msg=""):
        """
        Expect the provided column and its value to equal.

        Parameters
        ----------
            df (DataFrame) : Pandas or PySpark DataFrame
            column (str)   : The name of the column to be examined
            value (float)  : The value of the column
            msg (str)      : Optional message if the assertion fails
        """

        df = _get_dataframe_import_type(df)
        results = df.filter(column, value)

        if results.row_count > 0:
            msg = self._formatMessage(
                msg,
                f"Column {column} was not equal, found {results.first(column)}",
            )
            raise self.failureException(msg)

        return

    def expect_column_values_to_be_between(
        self, df, column: str, min_value: float, max_value: float, msg=""
    ):
        """
        Expect column entries to be between a minimum value and a maximum value (inclusive).

        Parameters
        ----------
            df (DataFrame)    : Pandas or PySpark DataFrame
            column (str)      : The name of the column to be examined
            min_value (float) : Minimum value of the column
            max_value (float) : Maximum value of the column
            msg (str)         : Optional message if the assertion fails
        """

        if max_value < min_value:
            msg = self._formatMessage(
                msg,
                "Max value must be greater than min value",
            )
            raise self.failureException(msg)

        df = _get_dataframe_import_type(df)

        column_min = df.column_min(column)
        if float(column_min) < float(min_value):
            msg = self._formatMessage(
                msg,
                f"Min value provided ({min_value}) must be less than column {column} value of {column_min}",
            )
            raise self.failureException(msg)

        column_max = df.column_max(column)
        if float(max_value) < float(column_max):
            msg = self._formatMessage(
                msg,
                f"Max value provided ({max_value}) must be greater than column {column} value of {column_max}",
            )
            raise self.failureException(msg)

        return

    def expect_column_values_to_match_regex(self, df, column: str, regex: str, msg=""):
        """
        Expect column entries to be strings that do NOT match a given regular expression.

        The regex must not match any portion of the provided string. For example, “[at]+”
        would identify the following strings as expected: “fish”, “dog”, and the following
        as unexpected: “cat”, “hat”

        """

        df = _get_dataframe_import_type(df)

        results = df.check_regex(column, regex)
        if results.row_count > 0:
            msg = self._formatMessage(
                msg,
                f"Column {column} did not match regular expression, found {results.first(column)}",
            )
            raise self.failureException(msg)

        return

    def expect_column_values_to_be_in_set(
        self, df, column: str, value_set: set, ignore_case=False, msg=""
    ):
        """
        Expect each column value to be in a given set.

        Parameters
        ----------
            df (DataFrame)     : Pandas or PySpark DataFrame
            column (str)       : The name of the column to be examined
            value_set (str)    : Set of values found in the column
            ignore_case (bool) : Compare ignoring case sensitivity default False
            msg (str)          : Optional message if the assertion fails
        """

        df = _get_dataframe_import_type(df)

        results = df.is_in_set(column, value_set, ignore_case)
        if results.row_count > 0:
            # Sort if possible, otherwise just output
            column_unique_list = []
            try:
                column_unique_list = df.unique_list(column)
                column_unique_list = sorted(column_unique_list)
            except TypeError:
                pass

            msg = self._formatMessage(
                msg,
                f"Column {column} provided set was not in actaul set of {', '.join(map(str, column_unique_list))}",
            )
            raise self.failureException(msg)

        return

    def expect_column_values_to_be_of_type(
        self, df, column: str, type_: Union[str, float, int], msg=""
    ):
        """Expect a column to contain values of a specified data type."""

        df = _get_dataframe_type(df)

        df_type = df[column].dtypes
        fstr = f"Column {column} was not type {type_}"
        if type_ is str:
            # Consider object a string
            if df_type.num != 17:
                msg = self._formatMessage(msg, fstr)
                raise self.failureException(msg)

        if type_ is int:
            if df_type.num not in [7, 9]:
                msg = self._formatMessage(msg, fstr)
                raise self.failureException(msg)

        if type_ is float:
            if df_type.num != 12:
                msg = self._formatMessage(msg, fstr)
                raise self.failureException(msg)

        if type_ not in [str, int, float]:
            msg = self._formatMessage(
                msg, "Please check available types; str, float, int"
            )
            raise self.failureException(msg)

        return

    def expect_table_columns_to_match_ordered_list(
        self, df, column_list: List[str], msg=""
    ):
        """
        Expect the columns to exactly match a specified list.

        Parameters
        ----------
            df (DataFrame)    : Pandas or PySpark DataFrame
            column (str)      : The name of the column to be examined
            column_list (str) : List of column names in matched by order
            msg (str)         : Optional message if the assertion fails
        """

        df = _get_dataframe_import_type(df)

        if list(df.columns) != column_list:
            msg = self._formatMessage(
                msg,
                f"Ordered columns did not match ordered columns {', '.join(map(str, df.columns))}",
            )
            raise self.failureException(msg)

        return

    def expect_table_columns_to_match_set(
        self, df, column_set: Optional[Union[Set[str], List[str]]], msg=""
    ):
        """Expect the columns to match a specified set.

        Parameters
        ----------
            df (DataFrame) : Pandas or PySpark DataFrame
            column (str)   : The name of the column to be examined
            date (str)     : The date as a string, using the chosen format or default as %Y-%m-%d
            msg (str)      : Optional message if the assertion fails
        """
        df = _get_dataframe_import_type(df)

        column_set = set(column_set) if column_set is not None else set()
        if set(df.columns) != column_set:
            msg = self._formatMessage(
                msg,
                f"Columns did not match set found {', '.join(map(str, df.columns))}",
            )
            raise self.failureException(msg)

    def expect_date_range_to_be_less_than(
        self, df, column: str, date: str, date_format="%Y-%m-%d", msg=""
    ):
        """
        Expect the date columns to be less than date (Inclusive).

        Parameters
        ----------
            df (DataFrame)    : Pandas or PySpark DataFrame
            column (str)      : The name of the column to be examined
            date (str)        : The date as a string, using the chosen format or default as %Y-%m-%d
            date_format (str) : The format of the date defaulted to %Y-%m-%d
            msg (str)         : Optional message if the assertion fails

        Notes
        ----------
        Empty values are converted to 1900-01-01.
        """

        df = _get_dataframe_type(df)
        df[column] = df[column].apply(lambda dt: _default_null_dates(dt, date_format))

        results = df[df[column] >= datetime.strptime(date, date_format)]

        if len(results) > 0:
            dt = results[column].values[0].astype("datetime64[D]")
            msg = self._formatMessage(
                msg,
                f"Column {column} date is greater or equal than {date} found {dt}",
            )
            raise self.failureException(msg)

        return

    def expect_date_range_to_be_more_than(
        self, df, column: str, date: str, date_format="%Y-%m-%d", msg=""
    ):
        """
        Expect the date columns to be more than date (Inclusive).

        Parameters
        ----------
            df (DataFrame)    : Pandas or PySpark DataFrame
            column (str)      : The name of the column to be examined
            date (str)        : The date as a string, using the chosen format or default as %Y-%m-%d
            date_format (str) : The format of the date defaulted to %Y-%m-%d
            msg (str)         : Optional message if the assertion fails

        Notes
        ----------
        Empty values are converted to 1900-01-01.
        """

        df = _get_dataframe_type(df)
        df[column] = df[column].apply(lambda dt: _default_null_dates(dt, date_format))

        results = df[df[column] <= datetime.strptime(date, date_format)]

        if len(results) > 0:
            dt = results[column].values[0].astype("datetime64[D]")
            msg = self._formatMessage(
                msg,
                f"Column {column} is less or equal than {date} found {dt}",
            )
            raise self.failureException(msg)

        return

    def expect_date_range_to_be_between(
        self,
        df,
        column: str,
        date_start: str,
        date_end: str,
        date_format="%Y-%m-%d",
        msg="",
    ):
        """
        Expect the date columns to be between a start and end date.

        Parameters
        ----------
            df (DataFrame)    : Pandas or PySpark DataFrame
            column (str)      : The name of the column to be examined
            date_start (str)  : The start date as a string, using the chosen format or default as %Y-%m-%d
            date_end (str)    : The end date as a string, using the chosen format or default as %Y-%m-%d
            date_format (str) : The format of the date defaulted to %Y-%m-%d
            msg (str)         : Optional message if the assertion fails

        Notes
        ----------
        Empty values are converted to 1900-01-01.
        """

        df = _get_dataframe_type(df)
        df[column] = df[column].apply(lambda dt: _default_null_dates(dt, date_format))

        start_date = datetime.strptime(date_start, date_format)
        end_date = datetime.strptime(date_end, date_format)

        if start_date > end_date:
            msg = self._formatMessage(
                msg,
                f"Column {column} start date {date_start} cannot be greater than end_date {date_end}",
            )
            raise self.failureException(msg)

        mask = (df[column] <= start_date) | (df[column] >= end_date)
        results = df.loc[mask]

        if len(results) > 0:
            dt = results[column].values[0].astype("datetime64[D]")
            msg = self._formatMessage(
                msg,
                f"Column {column} is not between {date_start} and {date_end} found {dt}",
            )
            raise self.failureException(msg)

        return

    def expect_column_mean_to_be_between(
        self, df, column, min_value: float, max_value: float, msg=""
    ):
        """
        Expect the column mean to be between a minimum value and a maximum value (inclusive).

        Parameters
        ----------
            df (DataFrame)    : Pandas or PySpark DataFrame
            column (str)      : The name of the column to be examined
            min_value (float) : The minimum value for the column mean
            max_value (float) : The maximum value for the column mean
            msg (str)         : Optional message if the assertion fails
        """

        df = _get_dataframe_import_type(df)

        if min_value > max_value:
            msg = self._formatMessage(
                msg,
                f"Column {column} min_value {min_value} cannot be greater than max_value {max_value}",
            )
            raise self.failureException(msg)

        mean_value = df.column_mean(column)
        if min_value > mean_value:
            msg = self._formatMessage(
                msg,
                f"Column {column} mean {format(mean_value, '.5f')} is less than min_value {min_value}",
            )
            raise self.failureException(msg)

        if max_value < mean_value:
            msg = self._formatMessage(
                msg,
                f"Column {column} mean {format(mean_value, '.5f')} is greater than max_value {max_value}",
            )
            raise self.failureException(msg)

        return

    def expect_column_value_counts_percent_to_be_between(
        self, df, column: str, value_counts: dict, msg=""
    ):
        """
        Expect the value counts for each grouping to be a percentage between (Inclusive).

        Parameters
        ----------
            df (DataFrame)      : Pandas or PySpark DataFrame
            column (str)        : The name of the column to be examined
            value_counts (dict) : A dictionary of group names and thie associated min-max percentage values
            msg (str)           : Optional message if the assertion fails

        Example
        -------

        df = pd.DataFrame(
            {
                "col_1": ["Y", "Y", "N", "Y", "Y", "N", "N", "Y", "Y", "N"],
            }
        )

        value_counts = {"Y": {"min": 55, "max": 65}, "N": {"min": 35, "max": 45}}

        self.expect_column_value_counts_percent_to_be_between(
                df, "col_1", value_counts
        )

        Further
        -------

        col_1 actual percentages: Y - 60%, N - 40%

        The percent tolerance for Y is 55% to 65%, therefore 60% is valid
        and N is 35% to 45%, meaning 40% is valid
        """

        df = _get_dataframe_type(df)
        result = df[column].value_counts()

        for key in value_counts:

            # Verify keys from resulting DataFrame
            try:
                key_percent = (result[key] / len(df)) * 100
            except KeyError as e:
                sorted_results = ", ".join(sorted(result.index.tolist()))
                msg = self._formatMessage(
                    msg,
                    f"Check the key {str(e)} is not in the available value counts names {sorted_results}",
                )
                raise self.failureException(msg)

            # Verify min/max from resulting provided value counts
            try:
                column_min = value_counts[key]["min"]
                column_max = value_counts[key]["max"]
            except KeyError as e:
                msg = self._formatMessage(
                    msg,
                    f"Value count for key '{key}' not contain {str(e)}",
                )
                raise self.failureException(msg)

            if column_min > key_percent:
                msg = self._formatMessage(
                    msg,
                    f"Column {column} the actual value count of ({key}) is {format(key_percent, '.5f')}% "
                    f"is less than the min allowed of {value_counts[key]['min']}%",
                )
                raise self.failureException(msg)

            if column_max < key_percent:
                msg = self._formatMessage(
                    msg,
                    f"Column {column} the actual value count of ({key}) is {format(key_percent, '.5f')}% "
                    f"is more than the max allowed of {value_counts[key]['max']}%",
                )
                raise self.failureException(msg)

        return

    def expect_frames_equal(
        self, left, right, check_dtype=True, check_index=True, msg=""
    ):
        """
        Expect two DataFrames to compare.

        Parameters
        ----------
            left (DataFrame)   : Pandas or PySpark DataFrame
            right (DataFrame)  : Pandas or PySpark DataFrame
            check_dtype (bool) : Ignore Schema differences
            check_index (bool) : Ignore index differences
            msg (str)          : Optional message if the assertion fails

        """
        left = _get_dataframe_import_type(left)
        right = _get_dataframe_import_type(right)

        if type(left) != type(right):
            msg = self._formatMessage(
                msg,
                "Different DataFrame types",
            )
            raise self.failureException(msg)

        try:
            left.expect_frames_equal(right.df, check_dtype, check_index)
        except AssertionError as e:
            msg = self._formatMessage(
                msg,
                f"DataFrames are different with {e}",
            )
            raise self.failureException(msg)
