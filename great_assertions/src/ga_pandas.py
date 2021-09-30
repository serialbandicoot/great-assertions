from .ga_dataframe import GADataFrame

class GAPandas(GADataFrame):
    def __init__(self, df) -> None:
        super().__init__(df.copy(deep=True))

    @property
    def get_row_count(self) -> int:
        return len(self.df)
