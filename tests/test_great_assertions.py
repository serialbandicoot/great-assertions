from great_assertions import GreatAssertions
import pandas as pd
import pytest


class GreatAssertionTests(GreatAssertions):
    def test_expect_table_row_count_to_equal(self):
        df = pd.DataFrame({"calories": [420, 380, 390], "duration": [50, 40, 45]})
        self.assertExpectTableRowCountToEqual(df, 3)

    def test_raises_type_error(self):
        with pytest.raises(AssertionError) as excinfo:
            self.assertExpectTableRowCountToEqual(1, 1)

        assert "Object is not type DataFrame" in str(excinfo.value)
