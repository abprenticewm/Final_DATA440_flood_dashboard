"""
compare_p90_roc.py

produces a single csv that combines:
- current flow data
- rate of change (1h, 3h, 6h)
- comparison to 90th percentile historical values

output:
    data/high_flow_with_roc.csv
"""
# imports
import os
import pandas as pd
import numpy as np
from pathlib import Path

# directory and file paths
# set project root to the parent of the src folder
PROJECT_ROOT = Path(__file__).resolve().parent.parent
# data folder located outside src
DATA_DIR = PROJECT_ROOT / "data"

# current flow files for north and south regions
CURRENT_FILES = [
    DATA_DIR / "north_va.csv",
    DATA_DIR / "south_va.csv"
]
# historical 90th percentile flows
HISTORICAL_FILE = DATA_DIR / "historical_p90.csv"
# output file path
OUTPUT_FILE = DATA_DIR / "high_flow_with_roc.csv"

# rolling windows for rate of change calculations (5-minute interval data)
WINDOWS = {"1h": 12, "3h": 36, "6h": 72}  # 12 intervals per hour

# function to compute percent change over defined windows
def compute_rate_of_change(df):
    df = df.copy()
    # ensure timestamp is in datetime format
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"])
    # sort by site and timestamp
    df = df.sort_values(["site_no", "timestamp_utc"])

    # calculate percent change for each window
    for label, window in WINDOWS.items():
        df[f"pct_change_{label}"] = (
            df.groupby("site_no")["flow_cfs"]
              .transform(lambda x: (x - x.shift(window)) / x.shift(window) * 100)
        )

    return df

# function to load current data from csv files
def load_current_data():
    dfs = []
    for file_path in CURRENT_FILES:
        if file_path.exists():
            # determine region based on filename
            region = "north" if "north" in file_path.name else "south"
            df = pd.read_csv(file_path)
            df["region"] = region
            dfs.append(df)
        else:
            print(f"Warning: missing {file_path}")
    # if no files found, return empty dataframe
    if not dfs:
        print("no current data found. exiting.")
        return pd.DataFrame()

    # combine all region data into a single dataframe
    return pd.concat(dfs, ignore_index=True)

# function to prepare current data for comparison
def prepare_current_data(df):
    # convert timestamp to datetime in UTC
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True, errors="coerce")
    # extract day of year for matching with historical data
    df["day_of_year"] = df["timestamp_utc"].dt.dayofyear
    # drop rows missing essential columns
    return df.dropna(subset=["day_of_year", "flow_cfs", "site_no"])

# function to compare current data to historical p90
def compare_to_historical(df_current, df_hist):
    # merge current and historical data on site_no and day_of_year
    merged = pd.merge(
        df_current,
        df_hist,
        how="left",
        on=["site_no", "day_of_year"],
        suffixes=("_current", "_hist")
    )

    # determine site name from available columns
    if "site_name_current" in merged.columns:
        merged["site_name"] = merged["site_name_current"]
    elif "site_name_hist" in merged.columns:
        merged["site_name"] = merged["site_name_hist"]
    else:
        merged["site_name"] = "unknown"

    # create p90_flow_cfs column if missing
    if "p90_flow_cfs" not in merged.columns:
        merged["p90_flow_cfs"] = np.nan

    # calculate ratio of current flow to p90
    merged["ratio"] = merged["flow_cfs"] / merged["p90_flow_cfs"]
    # set ratio to nan if p90 is missing or zero
    merged.loc[merged["p90_flow_cfs"].isna() | (merged["p90_flow_cfs"] == 0), "ratio"] = np.nan
    # determine if current flow exceeds p90
    merged["high_flow"] = merged["ratio"] >= 1.0

    # define columns to include in output
    columns = [
        "site_no", "site_name", "timestamp_utc", "region",
        "lat", "lon",
        "flow_cfs", "p90_flow_cfs",
        "ratio", "high_flow",
        "pct_change_1h", "pct_change_3h", "pct_change_6h"
    ]

    # only include columns that exist in merged dataframe
    existing = [c for c in columns if c in merged.columns]
    return merged[existing]

# main execution function
def main():
    print("loading current data...")
    df_current = load_current_data()
    if df_current.empty:
        return

    print("computing rate of change...")
    df_current = compute_rate_of_change(df_current)

    print("preparing current data...")
    df_current = prepare_current_data(df_current)

    # check if historical file exists
    if not HISTORICAL_FILE.exists():
        print("historical file missing! run fetch_historical_data.py first.")
        return

    print("loading historical percentiles...")
    df_hist = pd.read_csv(HISTORICAL_FILE)

    print("joining with historical p90 data...")
    df_out = compare_to_historical(df_current, df_hist)

    print(f"saving → {OUTPUT_FILE}")
    df_out.to_csv(OUTPUT_FILE, index=False)

    # print summary of high flow sites
    high_flow_sites = df_out[df_out["high_flow"]].groupby("site_no")["site_name"].first()
    if len(high_flow_sites) > 0:
        print("\nhigh flow sites:")
        for site_no, site_name in high_flow_sites.items():
            print(f"  • {site_no}: {site_name}")
    else:
        print("\nno sites above 90th percentile.")

    print("done!")

# entry point
if __name__ == "__main__":
    main()