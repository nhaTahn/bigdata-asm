# Gold Insight Summary

Date range: **2025-03-20 -> 2025-05-11**

## KPI Snapshot
- Total distance (km): **4224684.05**
- Estimated fuel (liter): **1217800.52**
- Estimated CO2 (kg): **3263705.4**
- Active buses: **528**
- Active routes: **20**
- Total trips: **1859**
- Missing route_id ratio (trip-level): **0.0%**

## Key Findings
1. **Route 32** is the highest-emission route in the current Gold snapshot, with **577541.64 kg CO2** across **769641.05 km** and **71 unique buses**.
2. The busiest emission window is **hour 17.0:00**, contributing **233651.22 kg CO2**. This indicates afternoon operations dominate system-wide emissions.
3. The highest single day in the current dataset is **2025-03-22**, reaching **123506.37 kg CO2**. Daily totals are therefore not flat and should be interpreted as a changing operating profile over time.
4. The highest-emission bus is **bf78beaa473f6dc9f2c33f4bb4b677da21c8e09bb108988ef3be45c7c2392985**, responsible for **17072.88 kg CO2**. This suggests a relatively small set of buses contributes disproportionately to fleet emissions.
5. **0 trips** still have missing `route_id` values, equivalent to **0.0%** of all trips. Route-level rankings are therefore directionally useful, but the missing share should be disclosed in the report.
6. `trip_outliers.csv` should be treated as a **review list of high-emission-intensity trips**, not confirmed anomalies. The current CO2 estimate is model-based, so these rows are best used for investigation and discussion rather than definitive fault attribution.

## Report Files
- `docs/analysis/kpi_summary.json`
- `docs/analysis/top_buses_by_co2.csv`
- `docs/analysis/top_routes_by_co2.csv`
- `docs/analysis/peak_hours_by_co2.csv`
- `docs/analysis/daily_totals.csv`
- `docs/analysis/trip_outliers.csv`
- `docs/dashboard/index.html`
- `docs/dashboard/map.html`
