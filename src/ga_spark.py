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
