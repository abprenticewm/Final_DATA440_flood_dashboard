''' 
visualize.py - plot streamflow for a given site
generates and saves a time series plot of flow (cfs) for the specified site
saves plot to /plots directory
'''
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import sys

# resolve project root (one level above /src)
ROOT = Path(__file__).resolve().parent.parent

# directory containing input data files
DATA_DIR = ROOT / "data"

# input CSV files for north and south virginia sites
NORTH_FILE = DATA_DIR / "north_va.csv"
SOUTH_FILE = DATA_DIR / "south_va.csv"

# directory to save generated plots
PLOTS_DIR = ROOT / "plots"
PLOTS_DIR.mkdir(exist_ok=True, parents=True)

# function to load data for a specific site
def load_data(site_name_or_no):
    """load csv data for a given site and return dataframe and region"""

    # iterate through north and south files
    for file_path, region in [(NORTH_FILE, "north"), (SOUTH_FILE, "south")]:

        # check that file exists
        if file_path.exists():

            # read csv and convert timestamp column to datetime
            df = pd.read_csv(file_path)
            df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"])

            # filter for the site by name or site number
            mask = (df["site_name"] == site_name_or_no) | (df["site_no"] == site_name_or_no)
            df_site = df[mask]

            # if site data is found, return it sorted by timestamp
            if not df_site.empty:
                return df_site.sort_values("timestamp_utc"), region

    # return empty dataframe if site not found
    return pd.DataFrame(), None

def plot_site(df, site_name_or_no, region):
    """plot flow data for a site and save figure to plots folder"""

    # create figure
    plt.figure(figsize=(12, 6))

    # plot flow over time
    plt.plot(df["timestamp_utc"], df["flow_cfs"], marker='o', linestyle='-', label="Flow (cfs)")

    # add title and labels
    plt.title(f"Streamflow - {site_name_or_no} ({region})")
    plt.xlabel("Timestamp (UTC)")
    plt.ylabel("Flow (cfs)")

    # show legend and grid
    plt.legend()
    plt.grid(True)

    # save plot to plots directory
    plot_file = PLOTS_DIR / f"{site_name_or_no}_flow_plot.png"
    plt.tight_layout()
    plt.savefig(plot_file)
    plt.close()

    print(f"plot saved to {plot_file}")

# main function
def main():
    # require site identifier argument
    if len(sys.argv) < 2:
        print("usage: python visualize.py <site_name_or_no>")
        return

    # read site identifier from command line argument
    site_name_or_no = sys.argv[1]

    # load site data and determine region
    df_site, region = load_data(site_name_or_no)

    # check if site was found
    if df_site.empty:
        print(f"site '{site_name_or_no}' not found in north_va.csv or south_va.csv")
        return

    # generate and save plot
    plot_site(df_site, site_name_or_no, region)

# entry point
if __name__ == "__main__":
    main()