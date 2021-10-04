class GADataFrame:
    """Great Assertions."""

    def __init__(self, df):
        """Great Assertions."""
        self.df = df

    @property
    def columns(self) -> list:
        """List of columns from Pandas or PySpark."""
        return self.df.columns
