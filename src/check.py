import pandas as pd

df = pd.read_parquet("data/raw/open_meteo")

print(df.head())