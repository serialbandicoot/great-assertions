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

    def is_in_set(self, column: str, value_set: set, ignore_case: bool):
        """
        Checks to see if the value_set is in the provided column.

        :returns: The dataframe of results
        """
        from pyspark.sql.functions import lower, col

        if ignore_case:
            value_set_lower = set(map(lambda x: x.lower(), value_set))
            filtered_dataframe = self.df.filter(
                ~lower(col(column)).isin(value_set_lower)
            )
        else:
            filtered_dataframe = self.df.filter(~col(column).isin(value_set))

        return GASpark(filtered_dataframe)

    def unique_list(self, column: str) -> list:
        """
        Gets the unique values from a provided column.

        :returns: The unique values in a list
        """

        colection = self.df.select(column).distinct().collect()
        values = [item.asDict()[column] for item in colection]

        return values

    def drop_duplicates(self):
        """
        Drops any duplicates in the DataFrame.

        :returns: The unique values in a list
        """

        return GASpark(self.df.drop_duplicates())

    def expect_frames_equal(self, right, check_dtype=True, ignore_index=True):
        """Compares two DataFrames"""
        if ignore_index is False:
            print("Spark DataFrames do not have native indexes")

        if check_dtype:
            if self.df.schema != right.schema:
                raise AssertionError(
                    f"Schemas are different Left Schema {self.df.schema} and Right Schema {right.schema}"
                )

        if self.df.collect() != right.collect():
            raise AssertionError(
                f"Data is different Left Data {self.df.collect()} and Right Data {right.collect()}"
            )

        return True

    def filter(self, column: str, value: object):
        """Filters out if criteria not met"""
        self.df = self.df.filter(self.df[column] != value)

        return self

    def get_column(self, column):
        """Returns Dataframe with only column."""
        return GASpark(self.df.select(column))
