# %%
import pandas as pd

month = 12

df = pd.DataFrame({"day": [1, 2], "num_passengers": [3, 4]})
df["month"] = month
# %%
print(df.head())

# %%
df.to_parquet(f"outpu_{month}.parquet")
