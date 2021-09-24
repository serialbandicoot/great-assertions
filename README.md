# Great Assertions

[![serialbandicoot](https://circleci.com/gh/serialbandicoot/great-assertions.svg?style=svg)](<LINK>)

This library is inspired by the Great Expectations library. The library has made the various expectations found in Great Expectations available when using the inbuilt python unittest assertions.

For example if you wanted to use `expect_column_values_to_be_between` then you can access `assertExpectColumnValuesToBeBetween`.

## Install
```bash
pip install great-assertions
```

## Code example Pandas
```python
from great_assertions import GreatAssertions
import pandas as pd

class GreatAssertionTests(GreatAssertions):
    def test_expect_table_row_count_to_equal(self):
        df = pd.DataFrame({"col_1": [100, 200, 300], "col_2": [10, 20, 30]})
        self.assertExpectTableRowCountToEqual(df, 3)
```

## Code example PySpark
```python
from great_assertions import GreatAssertions
from pyspark.sql import SparkSession

class GreatAssertionTests(GreatAssertions):

    def setUp(self):
        self.spark = SparkSession.builder.getOrCreate()

    def test_expect_table_row_count_to_equal(self):
        df = self.spark.createDataFrame(
            [
                {"col_1": 100, "col_2": 10},
                {"col_1": 200, "col_2": 20},
                {"col_1": 300, "col_2": 30},
            ]
        )
        self.assertExpectTableRowCountToEqual(df, 3)
```

## List of available assertions

|   | Pandas | PySpark |
| ------------- | ------------- | ------------- |
| assertExpectTableRowCountToEqual  | :white_check_mark: | :white_check_mark: |
| assertExpectColumnValuesToBeBetween  | :white_check_mark: | :white_check_mark: |
| assertExpectColumnValuesToMatchRegex  | :white_check_mark: | :white_check_mark: |
| assertExpectColumnValuesToBeInSet  | :white_check_mark: | :white_check_mark: |
| assertExpectColumnValuesToBeOfType  | :white_check_mark: | :white_check_mark: |
| assertExpectTableColumnsToMatchOrderedList  | :white_check_mark: | :white_check_mark: |
| assertExpectTableColumnsToMatchSet  | :white_check_mark: | :white_check_mark: |
| assertExpectDateRangeToBeMoreThan  | :white_check_mark: | :white_check_mark: |
| assertExpectDateRangeToBeLessThan  | :white_check_mark: | :white_check_mark: |

## Assertion Descriptions

For a description of the assertions see [Assertion Definitions](ASSERTION_DEFINITIONS.md)

## Running the tests

Executing the tests still require unittest, the following options have been tested with the examples provided.

### Option 1

```python
import unittest
suite = unittest.TestLoader().loadTestsFromTestCase(GreatAssertionTests)
runner = unittest.TextTestRunner(verbosity=2)
runner.run(suite) 
```
### Options 2

```python
if __name__ == '__main__':
    unittest.main()   
```

