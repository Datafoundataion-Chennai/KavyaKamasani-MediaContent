import pandas as pd

def clean_data(df):
    df.drop_duplicates(inplace=True)
    df.dropna(inplace=True)
    df["publish_time"] = pd.to_datetime(df["publish_time"])
    return df

