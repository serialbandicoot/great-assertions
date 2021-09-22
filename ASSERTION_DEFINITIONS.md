# Description of Assertions

|  Assertion | Description | 
| ------------- | ------------- | 
| assertExpectTableRowCountToEqual | Expect the number of rows in this table to equal the number of rows in a different table |
| assertExpectColumnValuesToBeBetween | Expect column entries to be between a minimum value and a maximum value (inclusive) |
| assertExpectColumnValuesToMatchRegex | Expect column entries to be strings that do NOT match a given regular expression |
| assertExpectColumnValuesToBeInSet | Expect each column value to be in a given set |
| assertExpectColumnValuesToBeOfType | Expect a column to contain values of a specified data type |
| assertExpectTableColumnsToMatchOrderedList | Expect the columns to exactly match a specified list |
| assertExpectTableColumnsToMatchSet | Expect the columns to match a specified set. |

## Assertions Params

Decription of the exceptions and their method attributes

### assertExpectTableRowCountToEqual

|  Assertion | Type | Description |
| ------------- | ------------- | ------------- |
| df| DataFrame | Pandas/PySpark |
| expected_count| int | Integer number of expected row count |

### assertExpectColumnValuesToBeBetween

|  Assertion | Type | Description |
| ------------- | ------------- | ------------- |
| df| DataFrame | Pandas/PySpark |
| column| str | Column name |
| min_value| float | The minimum value expected in the column |
| max_value| float | The maximum value expected in the column |

#### Additional Notes

The assertion is inclusive of the min and max value
