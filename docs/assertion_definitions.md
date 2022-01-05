# Description of Assertions

|  Assertion | Description | 
| ------------- | ------------- | 
| [expect_table_row_count_to_equal](#expect_table_row_count_to_equal) | Expect the number of rows in this table to equal the number of rows in a different table |
| [expect_table_row_count_to_be_less_than](#expect_table_row_count_to_be_less_than) | Expect the number of rows in this table to not exceed the number of rows in a table |
| [expect_table_row_count_to_be_greater_than](#expect_table_row_count_to_be_greater_than) | Expect the number of rows in this table to exceed the number of rows in a table |
| [expect_table_has_no_duplicate_rows](#expect_table_has_no_duplicate_rows) | Expect the table to only have unique rows |
| [expect_column_values_to_be_between](#expect_column_values_to_be_between) | Expect column entries to be between a minimum value and a maximum value (inclusive) |
| [expect_column_values_to_match_regex](#expect_column_values_to_match_regex) | Expect column entries to be strings that do NOT match a given regular expression |
| [expect_column_values_to_be_in_set](#expect_column_values_to_be_in_set) | Expect each column value to be in a given set |
| [expect_column_values_to_be_of_type](#expect_column_values_to_be_of_type) | Expect a column to contain values of a specified data type |
| [expect_table_columns_to_match_ordered_list](#expect_table_columns_to_match_ordered_list)  | Expect the columns to exactly match a specified list |
| [expect_table_columns_to_match_set](#expect_table_columns_to_match_set) | Expect the columns to match a specified set. |
| [expect_date_range_to_be_more_than](#expect_date_range_to_be_more_than) | Expect the columns to be more than date (Inclusive). |
| [expect_date_range_to_be_less_than](#expect_date_range_to_be_less_than) | Expect the columns to be less than date (Inclusive). |
| [expect_date_range_to_be_between](#expect_date_range_to_be_between) | Expect the columns to be between a start and end date (Not inclusive). |
| [expect_column_mean_to_be_between](#expect_column_mean_to_be_between) | Expect the column mean to be between a minimum value and a maximum value (inclusive). |
| [expect_column_value_counts_percent_to_be_between](#expect_column_value_counts_percent_to_be_between) | Expect the value counts for each grouping to be a percentage between (Inclusive). |
| [expect_frame_equal](#expect_frame_equal) | Expect the value counts for each grouping to be a percentage between (Inclusive). |
| [expect_column_value_to_equal](#expect_column_value_to_equal) | Expect the provided column and its value to equal. |
| [expect_column_has_no_duplicate_rows](#expect_column_has_no_duplicate_rows) | Expect the column/s to only have unique rows. |
| [expect_column_value_to_equal_if](#expect_column_value_to_equal_if) | xpect the provided column and its value to equal if filtered column. |

## Assertions Params

Decription of the exceptions and their method attributes

### expect_table_row_count_to_equal

|  Assertion | Type | Description |
| ------------- | ------------- | ------------- |
| df| DataFrame | Pandas/PySpark |
| expected_count| int | Integer number of expected row count |
| msg | str | Additional optional message information if exception is raised |

### expect_table_row_count_to_be_less_than

|  Assertion | Type | Description |
| ------------- | ------------- | ------------- |
| df| DataFrame | Pandas/PySpark |
| expected_max_count| int | Integer maxmium expected row count |
| msg | str | Additional optional message information if exception is raised |

### expect_table_row_count_to_be_greater_than

|  Assertion | Type | Description |
| ------------- | ------------- | ------------- |
| df| DataFrame | Pandas/PySpark |
| expected_min_count| int | Integer minimum expected row count |
| msg | str | Additional optional message information if exception is raised |

### expect_table_has_no_duplicate_rows

|  Assertion | Type | Description |
| ------------- | ------------- | ------------- |
| df| DataFrame | Pandas/PySpark |
| msg | str | Additional optional message information if exception is raised |

### expect_column_values_to_be_between

|  Assertion | Type | Description |
| ------------- | ------------- | ------------- |
| df| DataFrame | Pandas/PySpark |
| column| str | Column name |
| min_value| float | The minimum value expected in the column |
| max_value| float | The maximum value expected in the column |
| msg | str | Additional optional message information if exception is raised |

#### Additional Notes

The assertion is inclusive of the min and max value

### expect_column_values_to_match_regex

|  Assertion | Type | Description |
| ------------- | ------------- | ------------- |
| df| DataFrame | Pandas/PySpark |
| column| str | Column name |
| regex | str | If the regular expression fails this will cause the raised exception |
| msg | str | Additional optional message information if exception is raised |

### expect_column_values_to_be_in_set

|  Assertion | Type | Description |
| ------------- | ------------- | ------------- |
| df| DataFrame | Pandas/PySpark |
| column| str | Column name |
| value_set | set | A set of objects used for comparison |
| msg | str | Additional optional message information if exception is raised |

### expect_column_values_to_be_of_type

|  Assertion | Type | Description |
| ------------- | ------------- | ------------- |
| df| DataFrame | Pandas/PySpark |
| column| str | Column name |
| type_ | str, int, float | The type of field you expect the value to be |
| msg | str | Additional optional message information if exception is raised |

### expect_table_columns_to_match_ordered_list

|  Assertion | Type | Description |
| ------------- | ------------- | ------------- |
| df| DataFrame | Pandas/PySpark |
| column| str | Column name |
| column_list | list | A list of strings |
| msg | str | Additional optional message information if exception is raised |

### expect_table_columns_to_match_set

|  Assertion | Type | Description |
| ------------- | ------------- | ------------- |
| df| DataFrame | Pandas/PySpark |
| column| str | Column name |
| column_list | list/set | A list of strings |
| msg | str | Additional optional message information if exception is raised |

### expect_date_range_to_be_more_than

|  Assertion | Type | Description |
| ------------- | ------------- | ------------- |
| df| DataFrame | Pandas/PySpark |
| column| str | Column name |
| date | str | The date of the string in the chosen format to compare, default is "%Y-%m-%d" |
| format | str | Provide the format, to convert from in the DataFrame |
| msg | str | Additional optional message information if exception is raised |

### expect_date_range_to_be_less_than

|  Assertion | Type | Description |
| ------------- | ------------- | ------------- |
| df| DataFrame | Pandas/PySpark |
| column| str | Column name |
| date | str | The date of the string in the chosen format to compare, default is "%Y-%m-%d" |
| format | str | Provide the format, to convert from in the DataFrame |
| msg | str | Additional optional message information if exception is raised |

### expect_date_range_to_be_between

|  Assertion | Type | Description |
| ------------- | ------------- | ------------- |
| df| DataFrame | Pandas/PySpark |
| column| str | Column name |
| date_start | str | The start date of the string in the chosen format to compare, default is "%Y-%m-%d" |
| date_start | str | The end date of the string in the chosen format to compare, default is "%Y-%m-%d" |
| format | str | Provide the format, to convert from in the DataFrame |
| msg | str | Additional optional message information if exception is raised |

### expect_column_mean_to_be_between

|  Assertion | Type | Description |
| ------------- | ------------- | ------------- |
| df| DataFrame | Pandas/PySpark |
| column| str | Column name |
| min_value  | float | The minimum value for the column mean|
| max_value | float | The maximum value for the column mean|
| format | str | Provide the format, to convert from in the DataFrame |
| msg | str | Additional optional message information if exception is raised |

### expect_column_value_counts_percent_to_be_between

|  Assertion | Type | Description |
| ------------- | ------------- | ------------- |
| df| DataFrame | Pandas/PySpark |
| column| str | Column name |
| value_counts  | dict | A dictionary of group names and thie associated min-max percentage values |
| msg | str | Additional optional message information if exception is raised |

### expect_frame_equal

|  Assertion | Type | Description |
| ------------- | ------------- | ------------- |
| left| DataFrame | Pandas/PySpark |
| right| DataFrame | Pandas/PySpark |
| check_dtype  | bool | Ignore Schema differences |
| check_index  | bool | Ignore Indexes |
| msg | str | Additional optional message information if exception is raised |

### expect_column_value_to_equal

|  Assertion | Type | Description |
| ------------- | ------------- | ------------- |
| left| DataFrame | Pandas/PySpark |
| column| str | Column name |
| value | object | The value of the column |
| msg | str | Additional optional message information if exception is raised |

### expect_column_has_no_duplicate_rows

|  Assertion | Type | Description |
| ------------- | ------------- | ------------- |
| left| DataFrame | Pandas/PySpark |
| column| str | Single, Array, Empty or * for all columns |
| msg | str | Additional optional message information if exception is raised |

### expect_column_value_to_equal_if

|  Assertion | Type | Description |
| ------------- | ------------- | ------------- |
| df| DataFrame | Pandas/PySpark |
| filter_column| str | The column to be filtered |
| filter_value  | object | The value which the filtered_column should equal|
| column  | str | The name of the column to be examined|
| value | object | The value of the column|
| msg | str | Additional optional message information if exception is raised |