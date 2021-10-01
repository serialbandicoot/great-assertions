"""Great Assertions."""

from src.ga_dataframe import GADataFrame


class GASpark(GADataFrame):
    """Great Assertions."""

    def __init__(self, df):
        """Great Assertions."""
        super().__init__(df)

    @property
    def get_row_count(self) -> int:
        """
        Calculate the row count.

        :returns: The row count value

        """
        return self.df.count()
