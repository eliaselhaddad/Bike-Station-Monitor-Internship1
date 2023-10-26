import pandas as pd


def get_min_and_max_dates_from_dataframe(df: pd.DataFrame) -> dict:
    if pd.api.types.is_datetime64_any_dtype(df["timestamp"]) == False:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    df = df.dropna(subset=["timestamp"])

    min_date = df["timestamp"].min().strftime("%Y-%m-%d")
    max_date = df["timestamp"].max().strftime("%Y-%m-%d")
    return {"min_date": min_date, "max_date": max_date}
