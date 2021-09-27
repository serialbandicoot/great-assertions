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


def _default_null_dates(dt, format):
    if dt == "":
        return datetime.strptime("1900-01-01", "%Y-%m-%d")
    else:
        return datetime.strptime(dt, format)


class GreatAssertions(unittest.TestCase):
    def assertExpectTableRowCountToEqual(self, df, expected_count: int, msg=""):
        """Expect the number of rows in this table to equal the number of rows in a different table."""

        df = _get_dataframe_type(df)
        actual_row_count = len(df)

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
        """Expect column entries to be between a minimum value and a maximum value (inclusive)."""

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
        """Expect column entries to be strings that do NOT match a given regular expression.
        The regex must not match any portion of the provided string. For example, “[at]+”
        would identify the following strings as expected: “fish”, “dog”, and the following
        as unexpected: “cat”, “hat”"""

        df = _get_dataframe_type(df)

        results = df[df[column].astype(str).str.match(regex) == False]
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
        """Expect each column value to be in a given set."""

        df = _get_dataframe_type(df)

        results = df[~df[column].isin(value_set)] == False
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
        """Expect the columns to exactly match a specified list"""
        df = _get_dataframe_type(df)

        if list(df.columns) != column_list:
            msg = self._formatMessage(msg, "Ordered columns did not match")
            raise self.failureException(msg)

        return

    expect_table_columns_to_match_ordered_list = (
        assertExpectTableColumnsToMatchOrderedList
    )

    def assertExpectTableColumnsToMatchSet(
        self, df, column_set: Optional[Union[Set[str], List[str]]], msg=""
    ):
        """Expect the columns to match a specified set."""

        df = _get_dataframe_type(df)

        column_set = set(column_set) if column_set is not None else set()
        if set(df.columns) != column_set:
            msg = self._formatMessage(msg, "Columns did not match set")
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

    def assertExpectDateRangeToBeBetween(
        self, df, column: str, date_start: str, date_end: str, format="%Y-%m-%d", msg=""
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
                f"Column {column} start date {date_start} cannot be greater than end_date {end_date}",
            )

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
        """

        df = _get_dataframe_type(df)

        if min_value > max_value:
            msg = self._formatMessage(
                msg,
                f"Column {column} min_value {min_value} cannot be greater than max_value {max_value}",
            )

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
