# Gold Insight Summary

Date range: **2025-03-22 -> 2025-05-10**

## KPI Snapshot
- Total distance (km): **986.58**
- Estimated fuel (liter): **277.09**
- Estimated CO2 (kg): **742.59**
- Active buses: **511**
- Active routes: **20**
- Total trips: **651**
- Missing route_id ratio (trip-level): **0.0%**

## Key Findings
1. Highest-emission buses are concentrated in `top_buses_by_co2.csv`.
2. Route-time combinations with strongest contribution are in `top_routes_by_co2.csv` and `peak_hours_by_co2.csv`.
3. Trip-level anomalies by emission intensity (`co2_kg_per_km`) are listed in `trip_outliers.csv` for data quality review.

## Report Files
- `docs/analysis/kpi_summary.json`
- `docs/analysis/top_buses_by_co2.csv`
- `docs/analysis/top_routes_by_co2.csv`
- `docs/analysis/peak_hours_by_co2.csv`
- `docs/analysis/daily_totals.csv`
- `docs/analysis/trip_outliers.csv`
- `docs/dashboard/index.html`
