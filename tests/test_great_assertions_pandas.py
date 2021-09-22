from great_assertions.src.great_assertions import GreatAssertions
import pandas as pd
import pytest


class GreatAssertionPandasTests(GreatAssertions):

    def test_pandas_incorrect_dataframe_type_raises_type_error(self):
        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectTableRowCountToEqual(1, 1)

        assert "Not a valid pandas/pyspark DataFrame" in str(excinfo.value)
    def test_pandas_expect_table_row_count_to_equal(self):
        df = pd.DataFrame({"col_1": [100, 200, 300], "col_2": [10, 20, 30]})
        self.assertExpectTableRowCountToEqual(df, 3)

    def test_pandas_expect_table_row_count_to_equal_fails(self):
        df = pd.DataFrame({"col_1": [100, 200, 300], "col_2": [10, 20, 30]})
        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectTableRowCountToEqual(df, 4)

        assert "expected row count is 4 the actual was 3" in str(excinfo.value)

    # assertExpectColumnValueLengthsToEqual
    def test_pandas_assert_expect_column_values_to_be_between(self):
        # int
        df = pd.DataFrame({"col_1": [100, 200, 300], "col_2": [10, 20, 30]})
        self.assertExpectColumnValuesToBeBetween(df, "col_1", 101, 299)

        # float
        df = pd.DataFrame({"col_1": [100.01, 200.01, 300.01], "col_2": [10, 20, 30]})
        self.assertExpectColumnValuesToBeBetween(df, "col_1", 100.02, 300)

        # Same float
        df = pd.DataFrame({"col_1": [100.05, 200.01, 300.05], "col_2": [10, 20, 30]})
        self.assertExpectColumnValuesToBeBetween(df, "col_1", 100.05, 300.05)

    def test_pandas_assert_expect_column_values_to_be_between_min_fail(self):
        df = pd.DataFrame({"col_1": [100, 200, 300], "col_2": [10, 20, 30]})
        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectColumnValuesToBeBetween(df, "col_1", 99, 299)

        assert (
            "Min value provided (99) must be greater than column col_1 value of 100"
            in str(excinfo.value)
        )

    def test_pandas_assert_expect_column_values_to_be_between_min_fail_bad_syntax(self):
        df = pd.DataFrame({"col_1": [100, 200, 300], "col_2": [10, 20, 30]})
        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectColumnValuesToBeBetween(df, "col_1", 100, 50)

        assert "Max value must be greater than min value" in str(excinfo.value)

    def test_pandas_assert_expect_column_values_to_be_between_max_fail(self):
        df = pd.DataFrame({"col_1": [100, 200, 300], "col_2": [10, 20, 30]})
        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectColumnValuesToBeBetween(df, "col_1", 101, 301)

        assert (
            "Max value provided (301) must be less than column col_1 value of 300"
            in str(excinfo.value)
        )

    def test_pandas_assert_expect_column_values_to_match_regex(self):
        df = pd.DataFrame({"col_1": ["BA2", "BA15", "SW1"]})
        self.assertExpectColumnValuesToMatchRegex(
            df, "col_1", "^[a-zA-Z]{2}[0-9]{1,2}$"
        )

    def test_pandas_assert_expect_column_values_to_match_regex_fail(self):
        df = pd.DataFrame({"col_1": ["BA2", "BA151", "SW1", "AAA13"]})
        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectColumnValuesToMatchRegex(
                df, "col_1", "^[a-zA-Z]{2}[0-9]{1,2}$"
            )

        assert "Column col_1 did not match regular expression, found BA151" in str(
            excinfo.value
        )

    def test_pandas_assert_expect_column_values_to_be_in_set(self):
        fruits = ["Apple", "Orage", "Pear", "Cherry", "Apricot(Summer)"]
        fruits_set = set(("Apple", "Orage", "Pear", "Cherry", "Apricot(Summer)"))
        df = pd.DataFrame({"col_1": fruits})

        self.assertExpectColumnValuesToBeInSet(df, "col_1", fruits_set)

    def test_pandas_assert_expect_column_values_to_be_in_set_fail(self):
        fruits = set(("Apple", "Orage", "Pear", "Cherry"))
        df = pd.DataFrame({"col_1": ["Tomato", "Cherry", "Apple"]})

        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectColumnValuesToBeInSet(df, "col_1", fruits)

        assert "Column col_1 was not in the provided set" in str(excinfo.value)

    def test_pandas_expect_column_values_to_be_of_type(self):
        df = pd.DataFrame(
            {
                "col_1": ["BA2", "BA15", "SW1"],
                "col_2": [10, 20, 30],
                "col_3": [10.45, 20.32, 30.23],
            }
        )
        self.assertExpectColumnValuesToBeOfType(df, "col_1", str)
        self.assertExpectColumnValuesToBeOfType(df, "col_2", int)
        self.assertExpectColumnValuesToBeOfType(df, "col_3", float)

    def test_pandas_expect_column_values_to_be_of_type_fail_type(self):
        df = pd.DataFrame(
            {
                "col_1": ["BA2"],
            }
        )
        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectColumnValuesToBeOfType(df, "col_1", object)
       
        assert "Please check available types; str, float, int" in str(excinfo.value)

    def test_pandas_expect_column_values_to_be_of_type_fail(self):
        df = pd.DataFrame(
            {
                "col_1": ["BA2", "BA15", "SW1"],
                "col_2": [10, 20, 30],
                "col_3": [10.45, 20.32, 30.23],
            }
        )
        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectColumnValuesToBeOfType(df, "col_1", int)

        assert "Column col_1 was not type <class 'int'>" in str(excinfo.value)

        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectColumnValuesToBeOfType(df, "col_2", float)

        assert "Column col_2 was not type <class 'float'>" in str(excinfo.value)

        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectColumnValuesToBeOfType(df, "col_3", str)

        assert "Column col_3 was not type <class 'str'>" in str(excinfo.value)

    def test_assert_pandas_expect_table_columns_to_match_ordered_list(self):
        df = pd.DataFrame({"col_1": [100], "col_2": ['a'], "col_3": [1.01]})
        self.assertExpectTableColumnsToMatchOrderedList(df, list(("col_1", "col_2", "col_3")))

    def test_assert_pandas_expect_table_columns_to_match_ordered_list_fail(self):
        df = pd.DataFrame({"col_1": [100], "col_2": ['a'], "col_3": [1.01]})
        
        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectTableColumnsToMatchOrderedList(df, list(("col_2", "col_1", "col_3")))
        
        assert "Ordered columns did not match" in str(excinfo.value)

    def test_assert_pandas_expect_table_columns_to_match_set(self):
        df = pd.DataFrame({"col_1": [100], "col_2": ['a'], "col_3": [1.01]})
        self.assertExpectTableColumnsToMatchSet(df, set(("col_1", "col_2", "col_3")))    
        self.assertExpectTableColumnsToMatchSet(df, set(("col_2", "col_1", "col_3")))    
        self.assertExpectTableColumnsToMatchSet(df, list(("col_1", "col_2", "col_3")))    
        self.assertExpectTableColumnsToMatchSet(df, list(("col_2", "col_1", "col_3")))    

    def test_assert_pandas_expect_table_columns_to_match_set_fail(self):
        df = pd.DataFrame({"col_1": [100], "col_2": ['a'], "col_3": [1.01]})
        
        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectTableColumnsToMatchSet(df, set(("col_2", "col_1")))
        
        assert "Columns did not match set" in str(excinfo.value)      

        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectTableColumnsToMatchSet(df, list(("col_2", "col_1")))
        
        assert "Columns did not match set" in str(excinfo.value)   