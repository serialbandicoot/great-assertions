import unittest

class GreatAssertions(unittest.TestCase):
    
    def assertExpectTableRowCountToEqual(self, df, expected_count: int, msg=""):
        try:
            actual_row_count = len(df)
        except TypeError:
            raise self.failureException("Object is not type DataFrame")

        if expected_count != actual_row_count:
            msg = self._formatMessage(msg, f"expected row count is {expected_count} the actual was {actual_row_count}")
            raise self.failureException(msg)

        return       