''' update_data.py: orchestrate data fetching, processing, and logging updates

This script ensures historical reference data is present, fetches the latest gauge data, processes it to compute rate of change against historical P90 values, and logs each update with a timestamp for tracking.
'''
import os
import pandas as pd
from datetime import datetime
from pathlib import Path

# resolve project root and data directory
# project root is one level above /src
ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"

# log file to record each update timestamp
LOG_FILE = ROOT / "update_log.csv"

# maximum number of log records to retain
MAX_LOG_RECORDS = 100

# import main functions from other scripts
# these functions handle fetching current data, computing ROC vs P90, and fetching historical reference data
from fetch_data import main as fetch_data_main
from compare_p90_roc import main as p90_rate_of_change_main
from fetch_historical import main as historical_main  # ensure the filename matches the actual script

# function to append a timestamp to the update log
def log_update():
    """append an update timestamp and keep only the last 100 records"""
    timestamp = datetime.utcnow()

    # read existing log if present, otherwise create empty DataFrame
    if LOG_FILE.exists():
        df_log = pd.read_csv(LOG_FILE)
    else:
        df_log = pd.DataFrame(columns=["timestamp_utc"])

    # append new timestamp
    df_log = pd.concat(
        [df_log, pd.DataFrame({"timestamp_utc": [timestamp]})],
        ignore_index=True
    )

    # retain only the most recent MAX_LOG_RECORDS entries
    df_log = df_log.tail(MAX_LOG_RECORDS)

    # save updated log
    df_log.to_csv(LOG_FILE, index=False)

    print(f"logged update at {timestamp}")

# function to check for historical data before updating
def historical_check():
    """check if historical data exists; if not, fetch it"""
    hist_file = DATA_DIR / "historical_p90.csv"

    # if historical data file missing or empty, fetch it
    if not hist_file.exists() or hist_file.stat().st_size == 0:
        print("historical data missing — fetching...")
        historical_main()
    else:
        print("historical data already exists.")

# function to fetch new data and process it
def update():
    """fetch latest data and run analyses"""
    print("starting update process...")

    # fetch latest gauge readings
    fetch_data_main()

    # compute rate of change and compare to historical P90 values
    p90_rate_of_change_main()

    # record the update in the log
    log_update()

    print("update process complete.\n")

# main function to check historical data and perform update
def main():
    historical_check()
    update()

# entry point
if __name__ == "__main__":
    main()