# Great Assertions

[![serialbandicoot](https://circleci.com/gh/serialbandicoot/great-assertions.svg?style=svg)](<LINK>)

This library is inspired by the Great Expectations library. The library has made the various expectations found in Great Expectations available when using the inbuilt python unittest assertions.

For example if you wanted to use `expect_column_values_to_be_between` then you can access `assertExpectColumnValuesToBeBetween`.

## Install
```bash
pip install grea-assertions
```

## Coded example
```python
from great_assertions.src.great_assertions import GreatAssertions
import pandas as pd

class GreatAssertionTests(GreatAssertions):
    def test_expect_table_row_count_to_equal(self):
        df = pd.DataFrame({"col_1": [100, 200, 300], "col_2": [10, 20, 30]})
        self.assertExpectTableRowCountToEqual(df, 3)
```

## List of available assertions

|   | Pandas | PySpark |
| ------------- | ------------- | ------------- |
| assertExpectTableRowCountToEqual  | :white_check_mark: | TBA |
| assertExpectColumnValuesToBeBetween  | :white_check_mark: | TBA |
| assertExpectColumnValuesToMatchRegex  | :white_check_mark: | TBA |
| assertExpectColumnValuesToBeInSet  | :white_check_mark: | TBA |
| assertExpectColumnValuesToBeOfType  | :white_check_mark: | TBA |

## Assertion Descriptions

For a description of the assertions see [Assertion Definitions](ASSERTION_DEFINITIONS.md)