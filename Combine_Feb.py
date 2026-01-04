import pandas as pd

files = [
    "events_20250223.csv",
    "events_20250224.csv",
    "events_20250225.csv",
    "events_20250226.csv",
    "events_20250227.csv",
    "events_20250228.csv"
]

df = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)
df.to_csv("combined_output_feb.csv", index=False)