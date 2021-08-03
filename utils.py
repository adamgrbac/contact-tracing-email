import pandas as pd
from urllib.parse import unquote


def htmlify(df: pd.DataFrame) -> str:
    """
    Description:
        htmlify takes in a Pandas DataFrame and returns a prettified
        html version for insertion into an email.
    Arguments:
        df: pd.DataFrame - Pandas Dataframe to transform
    Returns:
        output: str - html string
    """

    output = "<ul>"
    for row in df.to_dict(orient="records"):
        output += f"<li>({row['severity']}) {row['data_location']}, {row['data_suburb']} on {row['data_datetext']} between {row['data_timetext']}</li>"
    output += "</ul>"
    return output


def clean_dataframe(df: pd.DataFrame, table_name: str) -> pd.DataFrame:
    """
    Description:
        clean_dataframe cleans a pandas DataFrame by correcting formatting
        issues and creating some desired columns.
    Arguments:
        df: pd.DataFrame - Pandas Dataframe to clean
        table_name: str - Name of contact tracing table (used as column value)
    Returns:
        df: pd.DataFrame - cleaned pandas Dataframe
    """

    col_names = list(df.columns)

    df["severity"] = table_name
    df["data_date"] = pd.to_datetime(df["data-date"])
    df["data_location"] = df["data-location"].apply(unquote)
    df["data_address"] = df["data-address"].apply(unquote)
    df["data_suburb"] = df["data-suburb"].apply(unquote)
    df["data_datetext"] = df["data-datetext"].apply(unquote)
    df["data_timetext"] = df["data-timetext"].apply(unquote)
    df["data_added"] = pd.to_datetime(df["data-added"])
    df["updated_flag"] = False if "class" not in df.columns else df["class"].apply(lambda x: False if type(x) != list else "qh-updated" in x)

    df = df.drop(col_names, axis=1)

    return df
