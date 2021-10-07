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


def generate_pandas_results_display(test_results):
    import pandas as pd

    test_results.getDescription

    ts = []
    for suite in test_results:
        for test in suite:
            failures = [{k: v for k, v in failure.items()} for failure in test]
            if len(failures) > 0:
                for failure in failures:
                    attributes = {k: v for k, v in suite.attrib.items()}
                    attributes.update({f"test_{k}": v for k, v in test.attrib.items()})
                    attributes.update({f"failure_{k}": v for k, v in failure.items()})
                    ts.append(attributes)
            else:
                attributes = {k: v for k, v in suite.attrib.items()}
                attributes.update({f"test_{k}": v for k, v in test.attrib.items()})
                attributes.update({"failure_type": None, "failure_message": None})
                ts.append(attributes)

    df = pd.DataFrame(ts)
    df["tests"] = df["tests"].astype(int)
    df["errors"] = df["errors"].astype(int)
    df["failures"] = df["failures"].astype(int)
    df["skipped"] = df["skipped"].astype(int)
    df["succeeded"] = df["tests"] - (df["errors"] + df["failures"] + df["skipped"])
    df["name"] = df["name"].apply(lambda x: str.join("-", x.split("-")[:-1]))
    df = df.loc[
        :,
        [
            #   "timestamp",
            "name",
            #   "time",
            "tests",
            "succeeded",
            "errors",
            "failures",
            "skipped",
            "test_name",
            "test_time",
            "failure_type",
            "failure_message",
        ],
    ]
    df

    idx = (
        df.groupby(["name", "tests", "succeeded", "errors", "failures", "skipped"])
        .first()
        .index
    )

    gf = pd.DataFrame([[x for x in t] for t in idx], columns=idx.names)
    gf.index = gf["name"]
    gf = gf.iloc[:, 2:]
    gf.T.plot.pie(
        subplots=True,
        colors=["green", "orange", "red", "yellow"],
        labeldistance=None,
        figsize=(8, 8),
        legend=None,
    )

    from matplotlib import pyplot as plt

    plt.rcParams["figure.autolayout"] = True

    group = df.groupby(["name"]).first()

    group.loc[:, ["succeeded", "errors", "failures", "skipped"]].plot(
        kind="barh",
        stacked=True,
        color=["green", "orange", "red", "yellow"],
        xticks=[],
        legend=None,
        xlabel="",
    )

    return group, gf
