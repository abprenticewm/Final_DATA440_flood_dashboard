# VA Flood Dashboard

This project is an interactive, auto-updating dashboard built to visualize real-time **streamflow and flood-risk conditions** across Virginia using data from **USGS Water Services** website.
The dashboard provides a clearer, more accessible representation of short-term changes in flow, rate-of-change (ROC), and exceedance against historical norms, helping users quickly identify rapid rises or potential flooding conditions.

---

## Features

* **Live USGS streamflow data** updated automatically through a processing pipeline. More information found in [Pipeline Documentation](documentation.md)
* **Interactive Virginia map** showing flow status and 3-hour rate of change
* **Color-coded risk indicators**
  * Brown: stable/decreasing (≤ 0%)
  * Blue: moderately rising (0–25%)
  * Red: sharp rise (> 25%)
  * Gray: missing data
* **Gauge detail pages** with time-series graphs, ROC classification, and notes
* **Supports local time zones**

---

## Project Structure

Below are the key files you will interact with in this repository.

##### `main.py`:
Main dashboard application.
Loads and updates data, handles routing, map rendering, hover formatting, gauge detail page logic, and UI layout.

##### `fetch_data.py`:
Handles incremental downloading of USGS discharge readings and maintains a rolling 24-hour window.

##### `fetch_historical.py`:
Fetches historical data from the USGS website for comparison.

##### `process_gauge_data.py`:
Uses current and historical data to create calculations that will appear on the dashboard.

##### `update_pipeline.py`:
This script runs the data fetching, historical data verification, and processing steps in sequence to ensure the gauge data is up-to-date and properly analyzed.

---

## Quickstart Guide

### Running the Dashboard Locally With `uv`

This project uses **uv**, a fast Python package/dependency manager.

---

**Requirements:**
- Python 3.8+
- Libraries (use uv add):
  - `pandas`
  - `numpy`
  - `matplotlib`
  - `seaborn`
  - `plotly.express`
  - `dash-bootstrap-components`
  - `kaleido`
  - `tzlocal`

This project can be run on **Windows PowerShell**. This project is managed using **UV**. If you have not installed UV or are having issues related to UV, refer to their [documentation](https://docs.astral.sh/uv/guides/install-python/). Once UV is installed, clone the repository and then follow the steps below.  

1. **Navigate to the directory:**  

   `cd "path of directory"`
2. **Install UV dependencies:**

    `uv sync`
3. **You can then run the program:**

    ```bash
    uv run python main.py
    ```

    Your default browser will open automatically to:

    ```
    http://127.0.0.1:8050/
    ```

---


## Notes

* If you are having trouble, make sure you are in the `src` folder before running `main.py`!
    * Example: `cd "path of directory\src"`
* You must keep the **data/** folder structure intact.
* The dashboard will attempt to update USGS data **immediately on startup** and will continue running until closed in Powershell.
* If you run into timezone formatting issues, ensure the `tzlocal` package is installed.