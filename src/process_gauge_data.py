# src/process_gauge_data.py

import pandas as pd
from pathlib import Path
from datetime import timedelta

# -----------------------------
# Paths
# -----------------------------
ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"

GAUGE_FILE = DATA_DIR / "gauge_data.csv"
HISTORICAL_FILE = DATA_DIR / "historical_p90.csv"
OUTPUT_FILE = DATA_DIR / "gauge_data_processed.csv"

# -----------------------------
# Read data
# -----------------------------
df_gauge = pd.read_csv(GAUGE_FILE, parse_dates=['timestamp_utc'])
df_hist = pd.read_csv(HISTORICAL_FILE)

# Ensure proper sorting
df_gauge = df_gauge.sort_values(['site_no', 'timestamp_utc'])

# -----------------------------
# Compute pct_change_3h
# -----------------------------
def compute_pct_change_3h(df):
    pct_changes = []

    for site, group in df.groupby('site_no'):
        latest_row = group.iloc[-1]
        latest_time = latest_row['timestamp_utc']

        # Target time ~3 hours ago
        target_time = latest_time - pd.Timedelta(hours=3)

        # Find nearest timestamp within 30 minutes
        group['time_diff'] = (group['timestamp_utc'] - target_time).abs()
        mask = group['time_diff'] <= pd.Timedelta(minutes=30)

        if mask.any():
            flow_3h = group.loc[mask, 'flow_cfs'].iloc[group.loc[mask, 'time_diff'].argmin()]
            if flow_3h != 0:
                pct_change = (latest_row['flow_cfs'] - flow_3h) / flow_3h * 100
            else:
                pct_change = float('nan')  # avoid division by zero
        else:
            pct_change = float('nan')

        pct_changes.append(pct_change)

    return pct_changes


# -----------------------------
# Prepare output dataframe
# -----------------------------
df_latest = df_gauge.groupby('site_no').last().reset_index()  # latest row per gauge

# pct_change_3h
df_latest['pct_change_3h'] = compute_pct_change_3h(df_gauge)

# Add day_of_year for p90 lookup
df_latest['day_of_year'] = df_latest['timestamp_utc'].dt.dayofyear

# Merge p90
df_processed = pd.merge(
    df_latest,
    df_hist[['site_no', 'day_of_year', 'p90_flow_cfs']],
    on=['site_no', 'day_of_year'],
    how='left'
)

# Calculate ratio
df_processed['ratio'] = df_processed['flow_cfs'] / df_processed['p90_flow_cfs']

# Drop temporary column
df_processed = df_processed.drop(columns=['day_of_year'])

# Save CSV
df_processed.to_csv(OUTPUT_FILE, index=False)
print(f"Processed data saved to {OUTPUT_FILE}")
