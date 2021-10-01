"""Great Assertions."""
from .ga_dataframe import GADataFrame


class GAPandas(GADataFrame):
    """Great Assertions."""

    def __init__(self, df) -> None:
        """Great Assertions."""
        super().__init__(df.copy(deep=True))

    @property
    def get_row_count(self) -> int:
        """
        Calculate the row count.

        :returns: The row count value

        """
        return len(self.df)
