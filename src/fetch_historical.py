"""
fetch_historical.py - build 20-year Virginia streamflow reference dataset

Downloads daily discharge data (00060) for all Virginia stream gauges
from USGS NWIS Daily Values. Computes 90th percentile flow for each
day of year using all available data.

Fixes:
- Ensures a site NEVER produces NaN P90 unless it has zero valid data.
- Sites with no valid flow values are dropped to avoid NaN thresholds.
"""

import os
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

# data directory
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)

HISTORICAL_FILE = DATA_DIR / "historical_p90.csv"

# USGS NWIS DV parameters
NWIS_DV_URL = "https://waterservices.usgs.gov/nwis/dv/"
PARAMETER_CD = "00060"
YEARS_BACK = 20
CHUNK_YEARS = 5


# fetch daily values for a date range
def fetch_va_dv_chunk(start_date, end_date):
    """Fetch daily discharge data for all VA sites within a date range."""
    params = {
        "format": "json",
        "stateCd": "VA",
        "parameterCd": PARAMETER_CD,
        "siteType": "ST",
        "siteStatus": "active",
        "startDT": start_date.strftime("%Y-%m-%d"),
        "endDT": end_date.strftime("%Y-%m-%d"),
    }

    print(f"Fetching {start_date.date()} → {end_date.date()} ...")
    resp = requests.get(NWIS_DV_URL, params=params, timeout=60)
    resp.raise_for_status()
    j = resp.json()

    rows = []
    for ts in j.get("value", {}).get("timeSeries", []):
        site_no = ts["sourceInfo"]["siteCode"][0]["value"]
        site_name = ts["sourceInfo"]["siteName"]
        lat = ts["sourceInfo"]["geoLocation"]["geogLocation"].get("latitude", None)

        for v in ts["values"][0]["value"]:
            val_str = v.get("value")
            if val_str in (None, "", "Ice"):
                continue
            try:
                flow = float(val_str)
            except ValueError:
                continue

            timestamp_iso = v["dateTime"]

            rows.append(
                {
                    "site_no": site_no,
                    "site_name": site_name,
                    # store full timestamp string; compute DOY later with tz conversion
                    "date": timestamp_iso,
                    "flow_cfs": flow,
                    "lat": lat,
                }
            )

    return pd.DataFrame(rows)

# fetch historical data in chunks
def fetch_historical_data(years_back=YEARS_BACK, chunk_years=CHUNK_YEARS):
    """Fetch historical daily flow data in chunks."""
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=years_back * 365)

    all_dfs = []
    cur_start = start_date

    while cur_start < end_date:
        cur_end = min(cur_start + timedelta(days=chunk_years * 365), end_date)
        df_chunk = fetch_va_dv_chunk(cur_start, cur_end)

        if not df_chunk.empty:
            all_dfs.append(df_chunk)

        cur_start = cur_end + timedelta(days=1)
        time.sleep(1)

    if not all_dfs:
        return pd.DataFrame()

    return pd.concat(all_dfs, ignore_index=True)


# pompute p90 by day of year
def compute_p90_by_day(df):
    """
    Compute 90th percentile discharge for each site and each day-of-year.

    FIX:
    - Sites with zero valid lines of data are removed.
    - Ensures no NaN P90 if any data exists.
    - Day-of-year is computed in US/Eastern (local VA) time so it matches
      how real-time gauge data is assigned a day_of_year.
    """
    # parse full timestamp (assumed UTC)
    # keep original column name "date" to minimize changes elsewhere
    df["date"] = pd.to_datetime(df["date"], utc=True, errors="coerce")

    # real-time data after converting to US/Eastern.
    df["date_local"] = df["date"].dt.tz_convert("US/Eastern").dt.date
    df["day_of_year"] = pd.to_datetime(df["date_local"]).dt.dayofyear

    # identify sites that truly have *zero* flow data
    site_counts = df.groupby("site_no")["flow_cfs"].count()
    valid_sites = site_counts[site_counts > 0].index.tolist()
    df = df[df["site_no"].isin(valid_sites)]

    # Compute raw p90 (per site, per day_of_year)
    grouped = (
        df.groupby(["site_no", "site_name", "day_of_year"])["flow_cfs"]
        .quantile(0.9)
        .reset_index(name="p90_flow_cfs")
    )

    full_results = []

    for (site_no, site_name), site_df in grouped.groupby(["site_no", "site_name"]):

        full_idx = pd.DataFrame({"day_of_year": range(1, 366)})
        merged = full_idx.merge(site_df, on="day_of_year", how="left")

        # fill missing values
        merged["p90_flow_cfs"] = merged["p90_flow_cfs"].interpolate(method="linear")
        merged["p90_flow_cfs"] = merged["p90_flow_cfs"].bfill().ffill()

        if merged["p90_flow_cfs"].isna().all():
            merged["p90_flow_cfs"] = 0

        merged["site_no"] = site_no
        merged["site_name"] = site_name

        full_results.append(merged)

    if not full_results:
        # return empty DataFrame with expected columns to avoid downstream errors
        return pd.DataFrame(columns=["site_no", "site_name", "day_of_year", "p90_flow_cfs"])

    final = pd.concat(full_results, ignore_index=True)

    return final[["site_no", "site_name", "day_of_year", "p90_flow_cfs"]]


# main execution
def main():
    print(f"Building historical reference dataset ({YEARS_BACK} years)...")

    df = fetch_historical_data()
    print(f"Fetched {len(df)} daily flow records total.")

    if df.empty:
        print("No data fetched — exiting.")
        return

    print("Computing P90 flows per site/day-of-year...")
    df_p90 = compute_p90_by_day(df)

    df_p90.to_csv(HISTORICAL_FILE, index=False)

    print(f"Saved P90 dataset → {HISTORICAL_FILE}")
    print(f"{len(df_p90)} rows written.")

if __name__ == "__main__":
    main()