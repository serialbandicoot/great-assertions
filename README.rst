Great Assertions
================

|serialbandicoot| |flake8 Lint| |codecov| |CodeQL|

This library is inspired by the Great Expectations library. The library
has made the various expectations found in Great Expectations available
when using the inbuilt python unittest assertions.

For example if you wanted to use ``expect_column_values_to_be_between``
then you can access ``assertExpectColumnValuesToBeBetween``.

Install
-------

.. code:: bash

    pip install great-assertions

Code example Pandas
-------------------

.. code:: python

    from great_assertions.great_assertions import GreatAssertions
    import pandas as pd

    class GreatAssertionTests(GreatAssertions):
        def test_expect_table_row_count_to_equal(self):
            df = pd.DataFrame({"col_1": [100, 200, 300], "col_2": [10, 20, 30]})
            self.assertExpectTableRowCountToEqual(df, 3)

Code example PySpark
--------------------

.. code:: python

    from great_assertions.great_assertions import GreatAssertions
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

List of available assertions
----------------------------

+---------------------------------------------------+---------------------+---------------------+
|                                                   | Pandas              | PySpark             |
+===================================================+=====================+=====================+
| assertExpectTableRowCountToEqual                  | :white_check_mark:: | :white_check_mark:: |
+---------------------------------------------------+---------------------+---------------------+
| assertExpectColumnValuesToBeBetween               | :white_check_mark:: | :white_check_mark:: |
+---------------------------------------------------+---------------------+---------------------+
| assertExpectColumnValuesToMatchRegex              | :white_check_mark:: | :white_check_mark:: |
+---------------------------------------------------+---------------------+---------------------+
| assertExpectColumnValuesToBeInSet                 | :white_check_mark:: | :white_check_mark:: |
+---------------------------------------------------+---------------------+---------------------+
| assertExpectColumnValuesToBeOfType                | :white_check_mark:: | :white_check_mark:: |
+---------------------------------------------------+---------------------+---------------------+
| assertExpectTableColumnsToMatchOrderedList        | :white_check_mark:: | :white_check_mark:: |
+---------------------------------------------------+---------------------+---------------------+
| assertExpectTableColumnsToMatchSet                | :white_check_mark:: | :white_check_mark:: |  
+---------------------------------------------------+---------------------+---------------------+
| assertExpectDateRangeToBeMoreThan                 | :white_check_mark:: | :white_check_mark:: |
+---------------------------------------------------+---------------------+---------------------+
| assertExpectDateRangeToBeLessThan                 | :white_check_mark:: | :white_check_mark:: |
+---------------------------------------------------+---------------------+---------------------+
| assertExpectDateRangeToBeBetween                  | :white_check_mark:: | :white_check_mark:: |
+---------------------------------------------------+---------------------+---------------------+
| assertExpectColumnMeanToBeBetween                 | :white_check_mark:: | :white_check_mark:: |
+---------------------------------------------------+---------------------+---------------------+
| assertExpectColumnValueCountsPercentToBeBetween   | :white_check_mark:: | :white_check_mark:: |
+---------------------------------------------------+---------------------+---------------------+

Assertion Descriptions
----------------------

For a description of the assertions see `Assertion
Definitions <ASSERTION_DEFINITIONS.md>`__

Running the tests
-----------------

Executing the tests still require unittest, the following options have
been tested with the examples provided.

Option 1
~~~~~~~~

.. code:: python

    import unittest
    suite = unittest.TestLoader().loadTestsFromTestCase(GreatAssertionTests)
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite) 

Options 2
~~~~~~~~~

.. code:: python

    if __name__ == '__main__':
        unittest.main()   

Notes
-----

If you get an arrows function warning when running in Databricks, this
will happen becuase a toPandas() method is called. The plan is to remove
pandas conversion for Spark at a later date as use native PySpark code.
For make sure the datasets are not too big, to cause the driver to
crash.

Development
-----------

To create a development environment, create a virtualenv and make a
development installation

::

    virtualenv ve
    source ve/bin/activation

To run tests, just use pytest

::

    (ve) pytest     

.. |serialbandicoot| image:: https://circleci.com/gh/serialbandicoot/great-assertions.svg?style=svg
   :target: LINK
.. |flake8 Lint| image:: https://github.com/serialbandicoot/great-assertions/actions/workflows/flake8.yml/badge.svg
   :target: https://github.com/serialbandicoot/great-assertions/actions/workflows/flake8.yml
.. |codecov| image:: https://codecov.io/gh/serialbandicoot/great-assertions/branch/master/graph/badge.svg?token=OKBB0E5EUC
   :target: https://codecov.io/gh/serialbandicoot/great-assertions
.. |CodeQL| image:: https://github.com/serialbandicoot/great-assertions/workflows/CodeQL/badge.svg
   :target: https://github.com/serialbandicoot/great-assertions/actions?query=workflow%3ACodeQL
