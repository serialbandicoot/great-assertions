from .ga_dataframe import GADataFrame

class GASpark(GADataFrame):
    def __init__(self, df) -> None:
        super().__init__(df)

    @property
    def get_row_count(self):
        return self.df.count()
 
