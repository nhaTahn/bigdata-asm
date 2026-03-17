from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd


def _read_gold_table(table_path: Path) -> pd.DataFrame:
    delta_log = table_path / "_delta_log"
    active_relative_paths: dict[str, bool] = {}

    if delta_log.exists():
        for log_file in sorted(delta_log.glob("*.json")):
            with log_file.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    obj = json.loads(line)
                    if "add" in obj and "path" in obj["add"]:
                        active_relative_paths[obj["add"]["path"]] = True
                    elif "remove" in obj and "path" in obj["remove"]:
                        active_relative_paths.pop(obj["remove"]["path"], None)

    if active_relative_paths:
        parquet_files = [table_path / rel for rel in active_relative_paths.keys()]
    else:
        parquet_files = sorted(table_path.glob("*.parquet"))

    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in: {table_path}")
    return pd.read_parquet(parquet_files)


def _safe_float(value: Any) -> float:
    if pd.isna(value):
        return 0.0
    return float(value)


def _build_dashboard_html(payload: dict[str, Any]) -> str:
    payload_json = json.dumps(payload, ensure_ascii=False)
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Bus Emission Dashboard</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <style>
    :root {{
      --bg: #f7f3ee;
      --ink: #1f2937;
      --muted: #5b667a;
      --card: #ffffff;
      --accent: #0f766e;
      --accent-soft: #a7f3d0;
      --warn: #be123c;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(1200px 700px at 100% -10%, #fde68a55, transparent),
        radial-gradient(1200px 700px at -20% 0%, #a7f3d055, transparent),
        var(--bg);
    }}
    .wrap {{
      max-width: 1200px;
      margin: 0 auto;
      padding: 28px 16px 36px;
    }}
    h1 {{
      margin: 0 0 6px;
      font-size: 28px;
      line-height: 1.2;
      letter-spacing: -0.02em;
    }}
    .sub {{
      color: var(--muted);
      margin-bottom: 18px;
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
      margin-bottom: 18px;
    }}
    .card {{
      background: var(--card);
      border-radius: 14px;
      padding: 14px;
      border: 1px solid #e5e7eb;
      box-shadow: 0 8px 24px rgba(15, 23, 42, 0.05);
    }}
    .kpi-label {{
      color: var(--muted);
      font-size: 12px;
      margin-bottom: 5px;
    }}
    .kpi-value {{
      font-size: 24px;
      font-weight: 700;
      color: var(--accent);
    }}
    .panels {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
      margin-bottom: 12px;
    }}
    .panel-title {{
      margin: 0 0 10px;
      font-size: 15px;
    }}
    .table-wrap {{
      overflow: auto;
      max-height: 340px;
    }}
    table {{
      border-collapse: collapse;
      width: 100%;
      font-size: 13px;
    }}
    th, td {{
      text-align: left;
      padding: 8px;
      border-bottom: 1px solid #eef2f7;
      white-space: nowrap;
    }}
    th {{
      color: var(--muted);
      font-weight: 600;
      position: sticky;
      top: 0;
      background: #fff;
    }}
    .warn {{
      color: var(--warn);
      font-weight: 600;
    }}
    @media (max-width: 900px) {{
      .grid {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
      .panels {{ grid-template-columns: 1fr; }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Bus Emission Dashboard</h1>
    <div class="sub">Generated from Gold layer (trip_summary, route_hourly_emissions, bus_daily_emissions)</div>

    <div class="grid">
      <div class="card"><div class="kpi-label">Total Distance (km)</div><div id="kpi-distance" class="kpi-value">-</div></div>
      <div class="card"><div class="kpi-label">Estimated Fuel (liter)</div><div id="kpi-fuel" class="kpi-value">-</div></div>
      <div class="card"><div class="kpi-label">Estimated CO2 (kg)</div><div id="kpi-co2" class="kpi-value">-</div></div>
      <div class="card"><div class="kpi-label">Active Buses</div><div id="kpi-buses" class="kpi-value">-</div></div>
    </div>

    <div class="panels">
      <div class="card">
        <h3 class="panel-title">Top 10 buses by CO2</h3>
        <canvas id="topBusesChart"></canvas>
      </div>
      <div class="card">
        <h3 class="panel-title">CO2 trend by day</h3>
        <canvas id="dailyTrendChart"></canvas>
      </div>
    </div>

    <div class="panels">
      <div class="card">
        <h3 class="panel-title">Peak emission hours</h3>
        <canvas id="peakHourChart"></canvas>
      </div>
      <div class="card">
        <h3 class="panel-title">Top routes by CO2</h3>
        <div class="table-wrap"><table id="routeTable"></table></div>
      </div>
    </div>

    <div class="card">
      <h3 class="panel-title">Top trips by CO2 per km <span class="warn">(outlier candidates)</span></h3>
      <div class="table-wrap"><table id="tripTable"></table></div>
    </div>
  </div>

  <script>
    const payload = {payload_json};
    const fmt = (v, digits=2) => Number(v || 0).toLocaleString(undefined, {{ maximumFractionDigits: digits }});

    document.getElementById("kpi-distance").textContent = fmt(payload.kpis.total_distance_km);
    document.getElementById("kpi-fuel").textContent = fmt(payload.kpis.total_fuel_liter);
    document.getElementById("kpi-co2").textContent = fmt(payload.kpis.total_co2_kg);
    document.getElementById("kpi-buses").textContent = fmt(payload.kpis.active_buses, 0);

    const axisColor = "#5b667a";
    const gridColor = "#e5e7eb";

    new Chart(document.getElementById("topBusesChart"), {{
      type: "bar",
      data: {{
        labels: payload.top_buses.labels,
        datasets: [{{
          label: "CO2 (kg)",
          data: payload.top_buses.values,
          backgroundColor: "#0f766e"
        }}]
      }},
      options: {{
        plugins: {{ legend: {{ display: false }} }},
        scales: {{
          x: {{ ticks: {{ color: axisColor }}, grid: {{ color: gridColor }} }},
          y: {{ ticks: {{ color: axisColor }}, grid: {{ color: gridColor }} }}
        }}
      }}
    }});

    new Chart(document.getElementById("dailyTrendChart"), {{
      type: "line",
      data: {{
        labels: payload.daily_trend.labels,
        datasets: [{{
          label: "CO2 (kg)",
          data: payload.daily_trend.values,
          borderColor: "#be123c",
          backgroundColor: "#fecdd3",
          fill: true,
          tension: 0.2
        }}]
      }},
      options: {{
        plugins: {{ legend: {{ display: false }} }},
        scales: {{
          x: {{ ticks: {{ color: axisColor }}, grid: {{ color: gridColor }} }},
          y: {{ ticks: {{ color: axisColor }}, grid: {{ color: gridColor }} }}
        }}
      }}
    }});

    new Chart(document.getElementById("peakHourChart"), {{
      type: "bar",
      data: {{
        labels: payload.peak_hours.labels,
        datasets: [{{
          label: "CO2 (kg)",
          data: payload.peak_hours.values,
          backgroundColor: "#334155"
        }}]
      }},
      options: {{
        plugins: {{ legend: {{ display: false }} }},
        scales: {{
          x: {{ ticks: {{ color: axisColor }}, grid: {{ color: gridColor }} }},
          y: {{ ticks: {{ color: axisColor }}, grid: {{ color: gridColor }} }}
        }}
      }}
    }});

    const renderTable = (id, rows) => {{
      const table = document.getElementById(id);
      if (!rows.length) {{
        table.innerHTML = "<tr><td>No data</td></tr>";
        return;
      }}
      const headers = Object.keys(rows[0]);
      const thead = "<tr>" + headers.map(h => `<th>${{h}}</th>`).join("") + "</tr>";
      const tbody = rows.map(r => "<tr>" + headers.map(h => `<td>${{r[h]}}</td>`).join("") + "</tr>").join("");
      table.innerHTML = `<thead>${{thead}}</thead><tbody>${{tbody}}</tbody>`;
    }};

    renderTable("routeTable", payload.top_routes_table);
    renderTable("tripTable", payload.trip_outliers_table);
  </script>
</body>
</html>
"""


def generate_assets(gold_root: Path, docs_dir: Path) -> None:
    bus_daily = _read_gold_table(gold_root / "bus_daily_emissions")
    route_hourly = _read_gold_table(gold_root / "route_hourly_emissions")
    trip_summary = _read_gold_table(gold_root / "trip_summary")

    bus_daily["event_date"] = pd.to_datetime(bus_daily["event_date"]).dt.date
    route_hourly["event_date"] = pd.to_datetime(route_hourly["event_date"]).dt.date
    route_hourly["event_hour"] = route_hourly["event_hour"].fillna(0).astype(int)
    route_hourly["route_id"] = route_hourly["route_id"].fillna("UNKNOWN").replace("", "UNKNOWN")
    trip_summary["route_id"] = trip_summary["route_id"].fillna("UNKNOWN").replace("", "UNKNOWN")

    analysis_dir = docs_dir / "analysis"
    dashboard_dir = docs_dir / "dashboard"
    analysis_dir.mkdir(parents=True, exist_ok=True)
    dashboard_dir.mkdir(parents=True, exist_ok=True)

    top_buses = (
        bus_daily.groupby("bus_id", as_index=False)[["distance_km", "fuel_liter_est", "co2_kg_est"]]
        .sum()
        .sort_values("co2_kg_est", ascending=False)
        .head(10)
    )
    top_routes = (
        route_hourly.groupby("route_id", as_index=False)[["distance_km", "fuel_liter_est", "co2_kg_est", "active_buses"]]
        .sum()
        .sort_values("co2_kg_est", ascending=False)
        .head(10)
    )
    peak_hours = (
        route_hourly.groupby("event_hour", as_index=False)[["distance_km", "co2_kg_est"]]
        .sum()
        .sort_values("co2_kg_est", ascending=False)
    )
    daily_totals = (
        bus_daily.groupby("event_date", as_index=False)[["distance_km", "fuel_liter_est", "co2_kg_est"]]
        .sum()
        .sort_values("event_date")
    )

    trip_outliers = trip_summary.copy()
    trip_outliers["co2_kg_per_km"] = trip_outliers["co2_kg_est"] / trip_outliers["distance_km"].replace({0: pd.NA})
    trip_outliers = (
        trip_outliers[trip_outliers["distance_km"] >= 1.0]
        .sort_values("co2_kg_per_km", ascending=False)
        .head(15)[["trip_id", "bus_id", "route_id", "distance_km", "co2_kg_est", "co2_kg_per_km"]]
    )

    kpis = {
        "total_distance_km": round(_safe_float(bus_daily["distance_km"].sum()), 2),
        "total_fuel_liter": round(_safe_float(bus_daily["fuel_liter_est"].sum()), 2),
        "total_co2_kg": round(_safe_float(bus_daily["co2_kg_est"].sum()), 2),
        "active_buses": int(bus_daily["bus_id"].nunique()),
        "active_routes": int(route_hourly["route_id"].nunique()),
        "total_trips": int(trip_summary["trip_id"].nunique()),
        "trip_route_missing_ratio": round(
            float((trip_summary["route_id"] == "UNKNOWN").sum()) / max(len(trip_summary), 1),
            4,
        ),
        "date_from": str(daily_totals["event_date"].min()) if not daily_totals.empty else None,
        "date_to": str(daily_totals["event_date"].max()) if not daily_totals.empty else None,
    }

    top_buses.to_csv(analysis_dir / "top_buses_by_co2.csv", index=False)
    top_routes.to_csv(analysis_dir / "top_routes_by_co2.csv", index=False)
    peak_hours.to_csv(analysis_dir / "peak_hours_by_co2.csv", index=False)
    daily_totals.to_csv(analysis_dir / "daily_totals.csv", index=False)
    trip_outliers.to_csv(analysis_dir / "trip_outliers.csv", index=False)

    with (analysis_dir / "kpi_summary.json").open("w", encoding="utf-8") as f:
        json.dump(kpis, f, ensure_ascii=False, indent=2)

    insight_md = f"""# Gold Insight Summary

Date range: **{kpis["date_from"]} -> {kpis["date_to"]}**

## KPI Snapshot
- Total distance (km): **{kpis["total_distance_km"]}**
- Estimated fuel (liter): **{kpis["total_fuel_liter"]}**
- Estimated CO2 (kg): **{kpis["total_co2_kg"]}**
- Active buses: **{kpis["active_buses"]}**
- Active routes: **{kpis["active_routes"]}**
- Total trips: **{kpis["total_trips"]}**
- Missing route_id ratio (trip-level): **{round(kpis["trip_route_missing_ratio"] * 100, 2)}%**

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
"""
    (analysis_dir / "insight_summary.md").write_text(insight_md, encoding="utf-8")

    dashboard_payload = {
        "kpis": kpis,
        "top_buses": {
            "labels": top_buses["bus_id"].astype(str).tolist(),
            "values": [round(_safe_float(v), 2) for v in top_buses["co2_kg_est"].tolist()],
        },
        "daily_trend": {
            "labels": [str(v) for v in daily_totals["event_date"].tolist()],
            "values": [round(_safe_float(v), 2) for v in daily_totals["co2_kg_est"].tolist()],
        },
        "peak_hours": {
            "labels": [str(v).zfill(2) for v in peak_hours.sort_values("event_hour")["event_hour"].tolist()],
            "values": [round(_safe_float(v), 2) for v in peak_hours.sort_values("event_hour")["co2_kg_est"].tolist()],
        },
        "top_routes_table": [
            {
                "route_id": str(row["route_id"]),
                "distance_km": round(_safe_float(row["distance_km"]), 2),
                "co2_kg_est": round(_safe_float(row["co2_kg_est"]), 2),
                "active_buses": int(_safe_float(row["active_buses"])),
            }
            for _, row in top_routes.iterrows()
        ],
        "trip_outliers_table": [
            {
                "trip_id": str(row["trip_id"]),
                "bus_id": str(row["bus_id"]),
                "route_id": str(row["route_id"]),
                "distance_km": round(_safe_float(row["distance_km"]), 2),
                "co2_kg_est": round(_safe_float(row["co2_kg_est"]), 2),
                "co2_kg_per_km": round(_safe_float(row["co2_kg_per_km"]), 3),
            }
            for _, row in trip_outliers.iterrows()
        ],
    }
    (dashboard_dir / "index.html").write_text(_build_dashboard_html(dashboard_payload), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate analysis and dashboard assets from Gold layer.")
    parser.add_argument("--gold-root", default="data/lakehouse/gold")
    parser.add_argument("--docs-dir", default="docs")
    args = parser.parse_args()

    generate_assets(gold_root=Path(args.gold_root), docs_dir=Path(args.docs_dir))
    print("Generated report assets under docs/analysis and docs/dashboard")


if __name__ == "__main__":
    main()
