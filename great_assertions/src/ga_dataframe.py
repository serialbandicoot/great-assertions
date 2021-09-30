class GADataFrame:
    def __init__(self, df) -> None:
        self.df = df

    @property   
    def columns(self) -> list:
        return self.df.columns   