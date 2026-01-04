import pandas as pd

files = [
    "events_20250301.csv",
    "events_20250302.csv",
    "events_20250303.csv",
    "events_20250304.csv",
    "events_20250305.csv",
    "events_20250306.csv",
    "events_20250307.csv",
    "events_20250308.csv"
]

df = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)
df.to_csv("combined_output_Mar.csv", index=False)
