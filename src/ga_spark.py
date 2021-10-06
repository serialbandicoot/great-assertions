from src.ga_dataframe import GADataFrame


class GASpark(GADataFrame):
    """Great Assertions."""

    def __init__(self, df):
        """Great Assertions."""
        super().__init__(df)

    @property
    def row_count(self) -> int:
        """
        Calculate the row count.

        :returns: The row count value

        """
        return self.df.count()

    def column_mean(self, column: str) -> int:
        """
        Calculate the mean of a Column.

        :returns: The mean value of the column provided
        """

        return self.df.agg({column: "mean"}).first()[0]

    def column_min(self, column: str) -> int:
        """
        Calculate the min of a Column.

        :returns: The min value of the column provided
        """

        return self.df.agg({column: "min"}).first()[0]

    def column_max(self, column: str) -> int:
        """
        Calculate the max of a Column.

        :returns: The max value of the column provided
        """

        return self.df.agg({column: "max"}).first()[0]

    def check_regex(self, column: str, regex: str) -> GADataFrame:
        """
        Filter column values which DO NOT conform to a regex

        :returns: The dataframe of results
        """

        filtered_dataframe = self.df.filter(~self.df[column].rlike(regex))
        return GASpark(filtered_dataframe)

    def first(self, column: str) -> str:
        """
        The first value found in the column

        :returns: The string of the value
        """

        return str(self.df.collect()[0][column])
