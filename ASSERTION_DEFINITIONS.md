# Description of Assertions

|  Assertion | Description | 
| ------------- | ------------- | 
| [assertExpectTableRowCountToEqual](#assertExpectTableRowCountToEqual) | Expect the number of rows in this table to equal the number of rows in a different table |
| [assertExpectColumnValuesToBeBetween](#assertExpectColumnValuesToBeBetween) | Expect column entries to be between a minimum value and a maximum value (inclusive) |
| [assertExpectColumnValuesToMatchRegex](#assertExpectColumnValuesToMatchRegex) | Expect column entries to be strings that do NOT match a given regular expression |
| [assertExpectColumnValuesToBeInSet](#assertExpectColumnValuesToBeInSet) | Expect each column value to be in a given set |
| [assertExpectColumnValuesToBeOfType](#assertExpectColumnValuesToBeOfType) | Expect a column to contain values of a specified data type |
| [assertExpectTableColumnsToMatchOrderedList](#assertExpectTableColumnsToMatchOrderedList)  | Expect the columns to exactly match a specified list |
| [assertExpectTableColumnsToMatchSet](#assertExpectTableColumnsToMatchSet) | Expect the columns to match a specified set. |
| [assertExpectDateRangeToBeMoreThan](#assertExpectDateRangeToBeMoreThan) | Expect the columns to be more than datetime. |
| [assertExpectDateRangeToBeLessThan](#assertExpectDateRangeToBeLessThan) | Expect the columns to be less than datetime. |

## Assertions Params

Decription of the exceptions and their method attributes

### assertExpectTableRowCountToEqual

|  Assertion | Type | Description |
| ------------- | ------------- | ------------- |
| df| DataFrame | Pandas/PySpark |
| expected_count| int | Integer number of expected row count |
| msg | str | Additional optional message information if exception is raised |

### assertExpectColumnValuesToBeBetween

|  Assertion | Type | Description |
| ------------- | ------------- | ------------- |
| df| DataFrame | Pandas/PySpark |
| column| str | Column name |
| min_value| float | The minimum value expected in the column |
| max_value| float | The maximum value expected in the column |
| msg | str | Additional optional message information if exception is raised |

#### Additional Notes

The assertion is inclusive of the min and max value

### assertExpectColumnValuesToMatchRegex

|  Assertion | Type | Description |
| ------------- | ------------- | ------------- |
| df| DataFrame | Pandas/PySpark |
| column| str | Column name |
| regex | str | If the regular expression fails this will cause the raised exception |
| msg | str | Additional optional message information if exception is raised |

### assertExpectColumnValuesToBeInSet

|  Assertion | Type | Description |
| ------------- | ------------- | ------------- |
| df| DataFrame | Pandas/PySpark |
| column| str | Column name |
| value_set | set | A set of objects used for comparison |
| msg | str | Additional optional message information if exception is raised |

### assertExpectColumnValuesToBeOfType

|  Assertion | Type | Description |
| ------------- | ------------- | ------------- |
| df| DataFrame | Pandas/PySpark |
| column| str | Column name |
| type_ | str, int, float | The type of field you expect the value to be |
| msg | str | Additional optional message information if exception is raised |

### assertExpectTableColumnsToMatchOrderedList

|  Assertion | Type | Description |
| ------------- | ------------- | ------------- |
| df| DataFrame | Pandas/PySpark |
| column| str | Column name |
| column_list | list | A list of strings |
| msg | str | Additional optional message information if exception is raised |

### assertExpectTableColumnsToMatchSet

|  Assertion | Type | Description |
| ------------- | ------------- | ------------- |
| df| DataFrame | Pandas/PySpark |
| column| str | Column name |
| column_list | list/set | A list of strings |
| msg | str | Additional optional message information if exception is raised |

### assertExpectDateRangeToBeMoreThan

|  Assertion | Type | Description |
| ------------- | ------------- | ------------- |
| df| DataFrame | Pandas/PySpark |
| column| str | Column name |
| date | str | The date of the string in the chosen format to compare, default is "%Y-%m-%d" |
| format | str | Provide the format, to convert from in the DataFrame |
| msg | str | Additional optional message information if exception is raised |

### assertExpectDateRangeToBeLessThan

|  Assertion | Type | Description |
| ------------- | ------------- | ------------- |
| df| DataFrame | Pandas/PySpark |
| column| str | Column name |
| date | str | The date of the string in the chosen format to compare, default is "%Y-%m-%d" |
| format | str | Provide the format, to convert from in the DataFrame |
| msg | str | Additional optional message information if exception is raised |