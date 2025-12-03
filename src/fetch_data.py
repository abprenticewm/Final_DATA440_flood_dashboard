"""
fetch_data.py - incremental real-time va discharge fetch

fetches only readings since the last timestamp
keeps a rolling 24-hour window
handles empty fetches safely
includes latitude and longitude columns
saves all data into a single csv
"""

import os
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
import numpy as np
from pathlib import Path

# config
# resolve project root → data folder is located outside the src directory
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)

# path to main gauge data file
DATA_FILE = DATA_DIR / "gauge_data.csv"

# usgs instantaneous values url
NWIS_IV_URL = "https://waterservices.usgs.gov/nwis/iv/"

# helper functions

def fetch_va_iv_since(start_time):
    """
    fetch all va gauge discharge readings since start_time
    """
    # define end time as now in utc
    end_time = datetime.now(timezone.utc)

    # request parameters for usgs service
    params = {
        "format": "json",
        "stateCd": "VA",
        "parameterCd": "00060",  # discharge
        "siteType": "ST",        # streamgage
        "siteStatus": "active",
        "startDT": start_time.strftime("%Y-%m-%dT%H:%M"),
        "endDT": end_time.strftime("%Y-%m-%dT%H:%M")
    }

    # perform http request
    resp = requests.get(NWIS_IV_URL, params=params, timeout=30)
    resp.raise_for_status()
    j = resp.json()

    # extract relevant data from json
    rows = []
    for ts in j.get("value", {}).get("timeSeries", []):
        site_no = ts["sourceInfo"]["siteCode"][0]["value"]
        site_name = ts["sourceInfo"]["siteName"]
        lat = ts["sourceInfo"]["geoLocation"]["geogLocation"]["latitude"]
        lon = ts["sourceInfo"]["geoLocation"]["geogLocation"]["longitude"]
        # iterate over recorded values
        for v in ts["values"][0]["value"]:
            try:
                flow = float(v["value"])
                if flow == -9999:   # convert usgs missing value code to nan
                    flow = np.nan
            except (TypeError, ValueError):
                flow = np.nan
            timestamp = v["dateTime"]
            rows.append({
                "site_no": site_no,
                "site_name": site_name,
                "timestamp_utc": timestamp,
                "flow_cfs": flow,
                "latitude": lat,
                "longitude": lon
            })
    # convert list of dictionaries to dataframe
    df = pd.DataFrame(rows)
    return df

def load_last_timestamp(file_path):
    """
    get the last timestamp from an existing csv or return 24 hours ago if file missing
    """
    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
        # ensure timestamps are in datetime format with utc
        df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
        last_time = df["timestamp_utc"].max()
        # add one second to avoid duplicate fetch
        return last_time + timedelta(seconds=1)
    else:
        # default to 24 hours ago if file does not exist
        return datetime.now(timezone.utc) - timedelta(hours=24)

def append_and_trim(df_new, file_path, hours=24):
    """
    append new data to csv and keep only last X hours
    """
    # define cutoff timestamp
    cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours)

    if os.path.exists(file_path):
        # read existing data
        df_old = pd.read_csv(file_path)
        # combine old and new data
        df_all = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df_all = df_new.copy()

    # ensure timestamp column is datetime with utc
    df_all["timestamp_utc"] = pd.to_datetime(df_all["timestamp_utc"], format="ISO8601", utc=True)
    # filter for only rows within last X hours
    df_all = df_all[df_all["timestamp_utc"] >= cutoff_time]
    # save updated data back to csv
    df_all.to_csv(file_path, index=False)
    print(f"saved {len(df_all)} rows to {file_path}")

# main function
def main():
    # determine last timestamp to fetch incremental data
    last_time = load_last_timestamp(DATA_FILE)

    # fetch new readings since last timestamp
    df = fetch_va_iv_since(last_time)
    print(f"fetched {len(df)} readings total.")

    if not df.empty:
        # append new data and trim old rows
        append_and_trim(df, DATA_FILE)
        print("update complete!")
    else:
        print("no new readings since last timestamp. nothing to update.")

# entry point
if __name__ == "__main__":
    main()