import unittest


class GreatAssertions(unittest.TestCase):
    def assertExpectTableRowCountToEqual(self, df, expected_count: int, msg=""):
        """Expect the number of rows in this table to equal the number of rows in a different table."""

        try:
            actual_row_count = len(df)
        except TypeError:
            raise self.failureException("Object is not type DataFrame")

        if expected_count != actual_row_count:
            msg = self._formatMessage(
                msg,
                f"expected row count is {expected_count} the actual was {actual_row_count}",
            )
            raise self.failureException(msg)

        return

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

    def assertExpectColumnValuesToMatchRegex(self, df, column: str, regex: str, msg=""):
        """Expect column entries to be strings that do NOT match a given regular expression.
        The regex must not match any portion of the provided string. For example, “[at]+”
        would identify the following strings as expected: “fish”, “dog”, and the following
        as unexpected: “cat”, “hat”"""

        result = df[df[column].str.match(regex) == False]
        if len(result) > 0:
            msg = self._formatMessage(
                msg,
                f"Column {column} did not match regular expression, found {result[column].values[0]}",
            )
            raise self.failureException(msg)

        return

    def assertExpectColumnValuesToBeInSet(
        self, df, column: str, value_set: set, msg=""
    ):
        """Expect each column value to be in a given set."""

        result = df[~df[column].isin(value_set)] == False
        if len(result) > 0:
            msg = self._formatMessage(
                msg, f"Column {column} was not in the provided set"
            )
            raise self.failureException(msg)

        return

    def assertExpectColumnValuesToBeOfType(
        self, df, column: str, type_: object, msg=""
    ):
        """Expect a column to contain values of a specified data type."""

        df_type = df[column].dtypes
        if type_ in ["string", "char", str]:
            if df_type.char != "O":
                msg = self._formatMessage(msg, f"Column {column} was not type {type_}")
                raise self.failureException(msg)

        if type_ in ["int", int]:
            if df_type.char != "l":
                msg = self._formatMessage(msg, f"Column {column} was not type {type_}")
                raise self.failureException(msg)

        if type_ in ["float", float]:
            if df_type.char != "d":
                msg = self._formatMessage(msg, f"Column {column} was not type {type_}")
                raise self.failureException(msg)

        return
