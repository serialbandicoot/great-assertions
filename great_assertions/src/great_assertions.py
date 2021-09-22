import unittest
from typing import Optional, Union, Set, List


def _get_dataframe_type(df):
    _type = str(type(df))
    if "pyspark" in _type:
        return df.toPandas()
    elif "pandas" in _type:
        return df

    raise AssertionError("Not a valid pandas/pyspark DataFrame")


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
        if float(column_min) > float(min_value):
            msg = self._formatMessage(
                msg,
                f"Min value provided ({min_value}) must be greater than column {column} value of {column_min}",
            )
            raise self.failureException(msg)

        column_max = df[column].max()
        if float(max_value) > float(column_max):
            msg = self._formatMessage(
                msg,
                f"Max value provided ({max_value}) must be less than column {column} value of {column_max}",
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

        result = df[df[column].str.match(regex) == False]
        if len(result) > 0:
            msg = self._formatMessage(
                msg,
                f"Column {column} did not match regular expression, found {result[column].values[0]}",
            )
            raise self.failureException(msg)

        return

    expect_column_values_to_match_regex = assertExpectColumnValuesToMatchRegex

    def assertExpectColumnValuesToBeInSet(
        self, df, column: str, value_set: set, msg=""
    ):
        """Expect each column value to be in a given set."""

        df = _get_dataframe_type(df)

        result = df[~df[column].isin(value_set)] == False
        if len(result) > 0:
            msg = self._formatMessage(
                msg, f"Column {column} was not in the provided set"
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
            if df_type.char != "O":
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
