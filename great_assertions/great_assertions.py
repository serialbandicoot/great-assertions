"""
Great Assertions.

This library is inspired by the Great Expectations library and has made various expectations found in Great Expectations available when using the inbuilt python unittest assertions.
For example if you wanted to use expect_column_values_to_be_between then you can access assertExpectColumnValuesToBeBetween.

The library has also added in further expectations, which may be similar or new.
"""

import unittest
from datetime import datetime
from typing import Optional, Union, Set, List


def _get_dataframe_type(df):
    _type = str(type(df))
    if "pyspark" in _type:
        return df.toPandas().copy(deep=True)
    elif "pandas" in _type:
        return df.copy(deep=True)

    raise AssertionError("Not a valid pandas/pyspark DataFrame")


def _get_dataframe_import_type(data_frame):
    _type = str(type(data_frame))
    if "pyspark.sql.dataframe.DataFrame" in _type:
        from great_assertions.src.ga_spark import GASpark as df

        return df(data_frame)
    elif "pandas.core.frame.DataFrame" in _type:
        from great_assertions.src.ga_pandas import GAPandas as df

        return df(data_frame)

    raise AssertionError("Not a valid pandas/pyspark DataFrame")


def _default_null_dates(dt, format):
    if dt == "":
        return datetime.strptime("1900-01-01", "%Y-%m-%d")
    else:
        return datetime.strptime(dt, format)


class GreatAssertions(unittest.TestCase):
    """
    GreatAssertions.

    A class which inherits unittest.TestCase and appends Great Expectation styled assertions

    """

    def assertExpectTableRowCountToEqual(self, df, expected_count: int, msg=""):
        """Expect the number of rows to equal the count.

        Parameters
        ----------
            df (DataFrame)       : Pandas or PySpark DataFrame
            expected_count (int) : The expected row count of the DataFrame
            msg (str)            : Optional message if the assertion fails
        """

        df = _get_dataframe_import_type(df)
        actual_row_count = df.get_row_count

        if expected_count != actual_row_count:
            msg = self._formatMessage(
                msg,
                f"expected row count is {expected_count} the actual was {actual_row_count}",
            )
            raise self.failureException(msg)

        return

    expect_table_row_count_to_equal = assertExpectTableRowCountToEqual

    def assertExpectColumnValuesToBeBetween(
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

        df = _get_dataframe_type(df)

        column_min = df[column].min()
        if float(column_min) < float(min_value):
            msg = self._formatMessage(
                msg,
                f"Min value provided ({min_value}) must be less than column {column} value of {column_min}",
            )
            raise self.failureException(msg)

        column_max = df[column].max()
        if float(max_value) < float(column_max):
            msg = self._formatMessage(
                msg,
                f"Max value provided ({max_value}) must be greater than column {column} value of {column_max}",
            )
            raise self.failureException(msg)

        return

    expect_column_values_to_be_between = assertExpectColumnValuesToBeBetween

    def assertExpectColumnValuesToMatchRegex(self, df, column: str, regex: str, msg=""):
        """
        Expect column entries to be strings that do NOT match a given regular expression.

        The regex must not match any portion of the provided string. For example, “[at]+”
        would identify the following strings as expected: “fish”, “dog”, and the following
        as unexpected: “cat”, “hat”

        """

        df = _get_dataframe_type(df)

        results = df[df[column].astype(str).str.match(regex).eq(False)]
        if len(results) > 0:
            msg = self._formatMessage(
                msg,
                f"Column {column} did not match regular expression, found {results[column].values[0]}",
            )
            raise self.failureException(msg)

        return

    expect_column_values_to_match_regex = assertExpectColumnValuesToMatchRegex

    def assertExpectColumnValuesToBeInSet(
        self, df, column: str, value_set: set, msg=""
    ):
        """
        Expect each column value to be in a given set.

        Parameters
        ----------
            df (DataFrame)  : Pandas or PySpark DataFrame
            column (str)    : The name of the column to be examined
            value_set (str) : Set of values found in the column
            msg (str)       : Optional message if the assertion fails
        """

        df = _get_dataframe_type(df)

        results = df[~df[column].isin(value_set)].eq(False)
        if len(results) > 0:
            # Sort if possible, otherwise just output
            try:
                column_unique_list = df[column].unique().tolist()
                column_unique_list = sorted(column_unique_list)
            except TypeError:
                pass

            msg = self._formatMessage(
                msg,
                f"Column {column} provided set was not in {', '.join(map(str, column_unique_list))}",
            )
            raise self.failureException(msg)

        return

    expect_column_values_to_be_in_set = assertExpectColumnValuesToBeInSet

    def assertExpectColumnValuesToBeOfType(
        self, df, column: str, type_: Union[str, float, int], msg=""
    ):
        """Expect a column to contain values of a specified data type."""

        df = _get_dataframe_type(df)

        df_type = df[column].dtypes
        fstr = f"Column {column} was not type {type_}"
        if type_ is str:
            # Consider object a string
            if df_type.char != "O" and df_type.char != "S":
                msg = self._formatMessage(msg, fstr)
                raise self.failureException(msg)

        if type_ is int:
            if df_type.char != "l":
                msg = self._formatMessage(msg, fstr)
                raise self.failureException(msg)

        if type_ is float:
            if df_type.char != "d":
                msg = self._formatMessage(msg, fstr)
                raise self.failureException(msg)

        if type_ not in [str, int, float]:
            msg = self._formatMessage(
                msg, "Please check available types; str, float, int"
            )
            raise self.failureException(msg)

        return

    expect_column_values_to_be_of_type = assertExpectColumnValuesToBeOfType

    def assertExpectTableColumnsToMatchOrderedList(
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

    expect_table_columns_to_match_ordered_list = (
        assertExpectTableColumnsToMatchOrderedList
    )

    def assertExpectTableColumnsToMatchSet(
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

    expect_table_columns_to_match_set = assertExpectTableColumnsToMatchSet

    def assertExpectDateRangeToBeLessThan(
        self, df, column: str, date: str, format="%Y-%m-%d", msg=""
    ):
        """
        Expect the date columns to be less than date (Inclusive).

        Parameters
        ----------
            df (DataFrame) : Pandas or PySpark DataFrame
            column (str)   : The name of the column to be examined
            date (str)     : The date as a string, using the chosen format or default as %Y-%m-%d
            msg (str)      : Optional message if the assertion fails

        Notes
        ----------
        Empty values are converted to 1900-01-01.
        """

        df = _get_dataframe_type(df)
        df[column] = df[column].apply(lambda dt: _default_null_dates(dt, format))

        results = df[df[column] >= datetime.strptime(date, format)]

        if len(results) > 0:
            dt = results[column].values[0].astype("datetime64[D]")
            msg = self._formatMessage(
                msg,
                f"Column {column} date is greater or equal than {date} found {dt}",
            )
            raise self.failureException(msg)

        return

    expect_date_range_to_be_less_than = assertExpectDateRangeToBeLessThan

    def assertExpectDateRangeToBeMoreThan(
        self, df, column: str, date: str, format="%Y-%m-%d", msg=""
    ):
        """
        Expect the date columns to be more than date (Inclusive).

        Parameters
        ----------
            df (DataFrame) : Pandas or PySpark DataFrame
            column (str)   : The name of the column to be examined
            date (str)     : The date as a string, using the chosen format or default as %Y-%m-%d
            msg (str)      : Optional message if the assertion fails

        Notes
        ----------
        Empty values are converted to 1900-01-01.
        """

        df = _get_dataframe_type(df)
        df[column] = df[column].apply(lambda dt: _default_null_dates(dt, format))

        results = df[df[column] <= datetime.strptime(date, format)]

        if len(results) > 0:
            dt = results[column].values[0].astype("datetime64[D]")
            msg = self._formatMessage(
                msg,
                f"Column {column} is less or equal than {date} found {dt}",
            )
            raise self.failureException(msg)

        return

    assert_expect_date_range_to_be_more_than = assertExpectDateRangeToBeMoreThan

    def assertExpectDateRangeToBeBetween(
        self,
        df,
        column: str,
        date_start: str,
        date_end: str,
        format="%Y-%m-%d",
        msg="",
    ):
        """
        Expect the date columns to be between a start and end date.

        Parameters
        ----------
            df (DataFrame)   : Pandas or PySpark DataFrame
            column (str)     : The name of the column to be examined
            date_start (str) : The start date as a string, using the chosen format or default as %Y-%m-%d
            date_end (str)   : The end date as a string, using the chosen format or default as %Y-%m-%d
            msg (str)        : Optional message if the assertion fails

        Notes
        ----------
        Empty values are converted to 1900-01-01.
        """

        df = _get_dataframe_type(df)
        df[column] = df[column].apply(lambda dt: _default_null_dates(dt, format))

        start_date = datetime.strptime(date_start, format)
        end_date = datetime.strptime(date_end, format)

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

    expect_date_range_to_be_between = assertExpectDateRangeToBeBetween

    def assertExpectColumnMeanToBeBetween(
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

        df = _get_dataframe_type(df)

        if min_value > max_value:
            msg = self._formatMessage(
                msg,
                f"Column {column} min_value {min_value} cannot be greater than max_value {max_value}",
            )
            raise self.failureException(msg)

        mean_value = df[column].mean()
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

    expect_column_mean_to_be_between = assertExpectColumnMeanToBeBetween

    def assertExpectColumnValueCountsPercentToBeBetween(
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

        self.assertExpectColumnValueCountsPercentToBeBetween(
                df, "col_1", value_counts
        )

        BlHAA
        -----

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
                msg = self._formatMessage(
                    msg,
                    f"Check the key {str(e)} is not in the available value counts names {', '.join(sorted(result.index.tolist()))}",
                )
                raise self.failureException(msg)

            # Verify min/max from resulting provided value counts
            try:
                min = value_counts[key]["min"]
                max = value_counts[key]["max"]
            except KeyError as e:
                msg = self._formatMessage(
                    msg,
                    f"Value count for key '{key}' not contain {str(e)}",
                )
                raise self.failureException(msg)

            if min > key_percent:
                msg = self._formatMessage(
                    msg,
                    f"Column {column} the actual value count of ({key}) is {format(key_percent, '.5f')}% is less than the min allowed of {value_counts[key]['min']}%",
                )
                raise self.failureException(msg)

            if max < key_percent:
                msg = self._formatMessage(
                    msg,
                    f"Column {column} the actual value count of ({key}) is {format(key_percent, '.5f')}% is more than the max allowed of {value_counts[key]['max']}%",
                )
                raise self.failureException(msg)

        return

    expect_column_value_counts_percent_to_be_between = (
        assertExpectColumnValueCountsPercentToBeBetween
    )
