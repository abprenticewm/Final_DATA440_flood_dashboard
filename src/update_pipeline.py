''' update_pipeline.py: orchestrate data fetching and processing steps for gauge data 

This script runs the data fetching, historical data verification, and processing steps in sequence to ensure the gauge data is up-to-date and properly analyzed.
'''
import subprocess
import sys
import os
from pathlib import Path

# resolve project root and data directory
# project root is one level above /src
ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
HISTORICAL_FILE = DATA_DIR / "historical_p90.csv"

# function to run a script in /src
def run(script_name):
    """
    Run a Python script located inside the /src directory.
    """
    script_path = ROOT / "src" / script_name
    print(f"\nRunning {script_path}...")
    r = subprocess.run([sys.executable, str(script_path)])
    if r.returncode != 0:
        print(f"Error running {script_name}")
    else:
        print(f"Finished {script_name}")

def main():
    # fetch last 24h readings
    run("fetch_data.py")

    # ensure historical reference exists
    if not HISTORICAL_FILE.exists():
        run("fetch_historical.py")
    else:
        print(f"{HISTORICAL_FILE} already exists, skipping historical fetch.")

    # produce processed dataset
    run("process_gauge_data.py")

# entry
if __name__ == "__main__":
    main()