from src.ga_dataframe import GADataFrame


class GAPandas(GADataFrame):
    """Great Assertions."""

    def __init__(self, df) -> None:
        """Great Assertions."""
        super().__init__(df.copy(deep=True))

    @property
    def row_count(self) -> int:
        """
        Calculate the row count.

        :returns: The row count value

        """
        return len(self.df)

    def column_mean(self, column: str) -> int:
        """
        Calculate the mean of a Column.

        :returns: The mean value of the column provided
        """

        return self.df[column].mean()

    def column_min(self, column: str) -> int:
        """
        Calculate the min of a Column.

        :returns: The min value of the column provided
        """

        return self.df[column].min()

    def column_max(self, column: str) -> int:
        """
        Calculate the max of a Column.

        :returns: The max value of the column provided
        """

        return self.df[column].max()

    def check_regex(self, column: str, regex: str):
        """
        Filter column values which DO NOT conform to a regex

        :returns: The dataframe of results
        """

        filtered_dataframe = self.df[
            self.df[column].astype(str).str.match(regex).eq(False)
        ]
        return GAPandas(filtered_dataframe)

    def first(self, column: str) -> str:
        """
        The first value found in the column

        :returns: The string of the value
        """

        return str(self.df[column].values[0])
