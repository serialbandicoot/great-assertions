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

    def is_in_set(self, column: str, value_set: set, ignore_case: bool):
        """
        Checks to see if the value_set is in the provided column.

        :returns: The dataframe of results
        """

        if ignore_case:
            value_set_lower = set(map(lambda x: x.lower(), value_set))
            filtered_dataframe = self.df[
                ~self.df[column].str.lower().isin(value_set_lower)
            ].eq(False)
        else:
            filtered_dataframe = self.df[~self.df[column].isin(value_set)].eq(False)

        return GAPandas(filtered_dataframe)

    def unique_list(self, column: str) -> list:
        """
        Gets the unique values from a provided column.

        :returns: The unique values in a list
        """

        return self.df[column].unique().tolist()

    def drop_duplicates(self):
        """
        Drops any duplicates in the DataFrame.

        :returns: The unique values in a list
        """

        self.df = self.df.drop_duplicates()
        return self

    def expect_frames_equal(self, right, check_dtype=True, check_index=True):
        """Compares two DataFrames."""
        from pandas.testing import assert_frame_equal

        if check_index is False:
            assert_frame_equal(
                self.df.reset_index(drop=True), right.reset_index(drop=True)
            )
        else:
            assert_frame_equal(
                self.df,
                right,
                check_dtype=check_dtype,
            )

    def filter(self, column: str, value: object):
        """Filters out if criteria not met."""
        self.df = self.df[self.df[column] != value]

        return self

    def get_column(self, column):
        """Returns Dataframe with only column."""
        return GAPandas(self.df[column])
