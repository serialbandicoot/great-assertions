from datetime import datetime
from src.ga_spark import GASpark
from src.ga_pandas import GAPandas


def _get_dataframe_type(df):
    _type = str(type(df))
    if "pyspark" in _type:
        return df.toPandas().copy(deep=True)
    elif "pandas" in _type:
        return df.copy(deep=True)

    raise AssertionError("Not a valid pandas/pyspark DataFrame")


def _get_dataframe_import_type(data_frame):
    _type = str(type(data_frame))
    if "pyspark.sql.dataframe.DataFrame" in _type:
        return GASpark(data_frame)
    elif "pandas.core.frame.DataFrame" in _type:
        return GAPandas(data_frame)

    raise AssertionError("Not a valid pandas/pyspark DataFrame")


def _default_null_dates(dt, date_format):
    if dt == "":
        return datetime.strptime("1900-01-01", "%Y-%m-%d")
    else:
        return datetime.strptime(dt, date_format)
