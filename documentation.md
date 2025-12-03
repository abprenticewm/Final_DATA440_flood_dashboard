# **Data Fetching and Processing Pipeline Documentation**

## **Pipeline Execution**
Upon opening or refreshing the dashboard, the `update_pipeline.py` script is automatically executed. This pipeline performs these steps:

### **1. Fetch Current Gauge Data**
`fetch_data.py`  
Retrieves ~24 hours of readings for all selected USGS gauges and saves them to:

`data/gauge_data.csv`

This file contains raw flow values and timestamps.

---

### **2. Ensure Historical P90 Data Exists**
`fetch_historical.py`  
If `data/historical_p90.csv` does **not** exist, this script generates it by calculating the **90th percentile** flow rate for each day of the year from the past 20 years.

If the file already exists, it is reused without re-fetching.

---

### **3. Process Gauge Data**
`process_gauge_data.py`  
Produces the final dataset:

`data/gauge_data_processed.csv`

This output file contains:

- Only the **latest** reading per gauge  
- A computed **3-hour percent rate of change (ROC)**  
- Merged **historical P90 values**

---

## **Rate of Change (ROC) Calculation**
A **3-hour percent change** in flow is calculated for each gauge.

For each site:

1. The script finds the **most recent reading**.
2. Then it finds the reading **closest to 3 hours earlier**, within a **±30 minute tolerance window** (to handle irregular reporting).

If a suitable earlier reading is found, ROC is calculated as:

\[
\text{pct\_change\_3h} = \frac{\text{latest\_flow} - \text{flow\_3h\_ago}}{\text{flow\_3h\_ago}} \times 100
\]

If no earlier value exists, or the earlier flow is zero, the ROC is recorded as **missing**.

---

## **Use of Historical P90 Values**
- Each gauge’s latest timestamp is converted to a **day of year**.
- This day number is used to match and merge the correct value from:

`data/historical_p90.csv`

The merged historical P90 allows comparison of the current flow to long-term high-flow thresholds.

---
