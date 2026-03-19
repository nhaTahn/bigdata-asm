from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.dataset as ds

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from producer.config import load_config
from producer.data_loader import load_raw_files
from producer.schema_map import normalize_raw_schema


def _read_delta_table(
    table_path: Path,
    columns: list[str] | None = None,
    trip_ids: list[str] | None = None,
) -> pd.DataFrame:
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

    scan_columns = columns
    if trip_ids:
        scan_columns = list(dict.fromkeys((columns or []) + ["trip_id"]))

    if scan_columns or trip_ids:
        dataset = ds.dataset([str(path) for path in parquet_files], format="parquet")
        scan_filter = None
        if trip_ids:
            scan_filter = ds.field("trip_id").isin(trip_ids)
        table = dataset.to_table(columns=scan_columns, filter=scan_filter)
        return table.to_pandas()

    return pd.read_parquet(parquet_files)


def _read_gold_table(table_path: Path) -> pd.DataFrame:
    return _read_delta_table(table_path)


def _safe_float(value: Any) -> float:
    if pd.isna(value):
        return 0.0
    return float(value)


def _normalize_route_id(value: Any) -> str:
    if pd.isna(value):
        return "UNKNOWN"
    text = str(value).strip().upper()
    if not text:
        return "UNKNOWN"
    if text.isdigit():
        return str(int(text))
    return text


def _load_route_reference(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(
            columns=[
                "route_id",
                "route_name",
                "duration_min",
                "distance_km_ref",
                "stops_inbound_outbound",
                "connected_metro1",
            ]
        )

    ref = pd.read_csv(path)
    ref["route_id"] = ref["route_no"].map(_normalize_route_id)
    ref["route_name"] = ref["route_name"].fillna("").astype(str)
    ref["duration_min"] = ref["duration_min"].fillna("").astype(str)
    ref["distance_km_ref"] = pd.to_numeric(ref["distance_km"], errors="coerce")
    ref["stops_inbound_outbound"] = ref["stops_inbound_outbound"].fillna("").astype(str)
    ref["connected_metro1"] = ref["connected_metro1"].fillna("").astype(str)
    return ref[
        [
            "route_id",
            "route_name",
            "duration_min",
            "distance_km_ref",
            "stops_inbound_outbound",
            "connected_metro1",
        ]
    ].drop_duplicates(subset=["route_id"], keep="first")


def _downsample_indices(length: int, max_points: int) -> list[int]:
    if length <= max_points:
        return list(range(length))
    if max_points <= 2:
        return [0, length - 1]
    step = (length - 1) / float(max_points - 1)
    return sorted({min(int(round(i * step)), length - 1) for i in range(max_points)})


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    earth_radius_km = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)
    a = (
        math.sin(d_phi / 2.0) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2.0) ** 2
    )
    return 2.0 * earth_radius_km * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))


def _build_map_payload(
    silver_path: Path,
    trip_summary: pd.DataFrame,
    route_ref: pd.DataFrame,
    replay_cache_path: Path,
    max_trips: int = 80,
    max_points_per_trip: int = 180,
) -> dict[str, Any]:
    cfg = load_config("configs/pipeline.yaml")
    fuel_per_km = float(cfg["emission"]["fuel_liter_per_km"])
    idle_per_min = float(cfg["emission"]["idle_liter_per_minute"])
    co2_per_liter = float(cfg["emission"]["co2_kg_per_liter_fuel"])
    bucket_minutes = 30
    generated_from = "silver"
    route_names = route_ref[["route_id", "route_name"]].drop_duplicates(subset=["route_id"])

    def _build_from_raw_candidates() -> tuple[str, pd.DataFrame, pd.DataFrame]:
        raw_candidates = [Path("data/raw_report"), Path("data/raw_sample"), Path("data/raw")]
        chosen_raw = next((p for p in raw_candidates if p.exists() and any(p.rglob("*.json"))), None)
        if chosen_raw is None:
            return "raw_missing", pd.DataFrame(), pd.DataFrame()

        raw_df = load_raw_files(str(chosen_raw))
        if raw_df.empty:
            return chosen_raw.name, pd.DataFrame(), pd.DataFrame()

        norm = normalize_raw_schema(raw_df, cfg)
        norm = norm.dropna(subset=["event_time", "bus_id", "latitude", "longitude"]).copy()
        if norm.empty:
            return chosen_raw.name, pd.DataFrame(), pd.DataFrame()

        norm["event_time"] = pd.to_datetime(norm["event_time"], utc=True, errors="coerce")
        norm = norm.dropna(subset=["event_time"]).copy()
        norm["event_time_local"] = norm["event_time"].dt.tz_convert("Asia/Ho_Chi_Minh")
        norm["route_id"] = norm["route_id"].map(_normalize_route_id)
        norm = norm.sort_values(["bus_id", "event_time_local"]).reset_index(drop=True)

        prev_time = norm.groupby("bus_id")["event_time_local"].shift(1)
        prev_route = norm.groupby("bus_id")["route_id"].shift(1)
        is_new_trip = (
            prev_time.isna()
            | ((norm["event_time_local"] - prev_time).dt.total_seconds() > 15 * 60)
            | (norm["route_id"] != prev_route)
        )
        norm["trip_seq"] = is_new_trip.groupby(norm["bus_id"]).cumsum().astype(int)
        norm["trip_id"] = norm["bus_id"].astype(str) + "_" + norm["trip_seq"].astype(str)

        trip_rank = (
            norm.groupby(["trip_id", "bus_id", "route_id"], as_index=False)
            .agg(
                trip_start=("event_time_local", "min"),
                trip_end=("event_time_local", "max"),
                point_count=("event_time_local", "count"),
            )
            .sort_values(["point_count", "trip_start"], ascending=[False, False])
            .head(max_trips)
        )
        trip_rank["distance_km"] = 0.0
        trip_rank["co2_kg_est"] = 0.0
        trip_rank = trip_rank.merge(route_names, on="route_id", how="left")
        focus = norm.merge(trip_rank[["trip_id"]], on="trip_id", how="inner")
        focus = focus.sort_values(["trip_id", "event_time_local"])
        return chosen_raw.name, trip_rank, focus

    if silver_path.exists():
        try:
            top_trip_summary = (
                trip_summary[["trip_id", "bus_id", "route_id", "distance_km", "co2_kg_est", "trip_start", "trip_end"]]
                .copy()
                .sort_values(["distance_km", "co2_kg_est"], ascending=[False, False])
                .head(max_trips)
            )
            target_trip_ids = top_trip_summary["trip_id"].astype(str).tolist()
            silver = _read_delta_table(
                silver_path,
                columns=["event_time", "trip_id", "route_id", "latitude", "longitude", "speed_kmh"],
                trip_ids=target_trip_ids,
            )
        except FileNotFoundError:
            silver = pd.DataFrame()
    else:
        silver = pd.DataFrame()

    if not silver.empty:
        silver = silver.dropna(subset=["event_time", "trip_id", "latitude", "longitude"]).copy()
        silver["event_time"] = pd.to_datetime(silver["event_time"], utc=True, errors="coerce")
        silver = silver.dropna(subset=["event_time"]).copy()
        silver["event_time_local"] = silver["event_time"].dt.tz_convert("Asia/Ho_Chi_Minh")
        silver["route_id"] = silver["route_id"].map(_normalize_route_id)

        trip_rank = top_trip_summary.copy()
        trip_rank["route_id"] = trip_rank["route_id"].map(_normalize_route_id)
        trip_rank = trip_rank.merge(route_names, on="route_id", how="left")
        focus = silver.merge(trip_rank[["trip_id"]], on="trip_id", how="inner")
        focus = focus.sort_values(["trip_id", "event_time_local"])
    else:
        generated_from = "replay_cache"
        focus = pd.DataFrame()
        trip_rank = pd.DataFrame()
        if replay_cache_path.exists():
            replay = pd.read_json(replay_cache_path, lines=True)
            required_cols = {"event_time", "bus_id", "latitude", "longitude"}
            if not replay.empty and required_cols.issubset(set(replay.columns)):
                replay = replay.dropna(subset=["event_time", "bus_id", "latitude", "longitude"]).copy()
                if not replay.empty:
                    replay["event_time"] = pd.to_datetime(replay["event_time"], utc=True, errors="coerce")
                    replay = replay.dropna(subset=["event_time"]).copy()
                    replay["event_time_local"] = replay["event_time"].dt.tz_convert("Asia/Ho_Chi_Minh")
                    replay["route_id"] = replay["route_id"].map(_normalize_route_id)
                    replay = replay.sort_values(["bus_id", "event_time_local"]).reset_index(drop=True)

                    prev_time = replay.groupby("bus_id")["event_time_local"].shift(1)
                    prev_route = replay.groupby("bus_id")["route_id"].shift(1)
                    is_new_trip = (
                        prev_time.isna()
                        | ((replay["event_time_local"] - prev_time).dt.total_seconds() > 15 * 60)
                        | (replay["route_id"] != prev_route)
                    )
                    replay["trip_seq"] = is_new_trip.groupby(replay["bus_id"]).cumsum().astype(int)
                    replay["trip_id"] = replay["bus_id"].astype(str) + "_" + replay["trip_seq"].astype(str)

                    trip_rank = (
                        replay.groupby(["trip_id", "bus_id", "route_id"], as_index=False)
                        .agg(
                            trip_start=("event_time_local", "min"),
                            trip_end=("event_time_local", "max"),
                            point_count=("event_time_local", "count"),
                        )
                        .sort_values(["point_count", "trip_start"], ascending=[False, False])
                        .head(max_trips)
                    )
                    trip_rank["distance_km"] = 0.0
                    trip_rank["co2_kg_est"] = 0.0
                    trip_rank = trip_rank.merge(route_names, on="route_id", how="left")
                    focus = replay.merge(trip_rank[["trip_id"]], on="trip_id", how="inner")
                    focus = focus.sort_values(["trip_id", "event_time_local"])

        if focus.empty:
            generated_from, trip_rank, focus = _build_from_raw_candidates()
            if focus.empty:
                return {"generated_from": generated_from, "trip_count": 0, "trips": [], "time_slices": []}

    trips: list[dict[str, Any]] = []
    slice_accumulator: dict[tuple[str, str, float, float, float, float], dict[str, Any]] = {}
    slice_trip_sets: dict[str, set[str]] = {}
    slice_route_sets: dict[str, set[str]] = {}

    for trip_id, trip_df in focus.groupby("trip_id", sort=False):
        meta = trip_rank[trip_rank["trip_id"] == trip_id].iloc[0]
        trip_df = trip_df.drop_duplicates(subset=["event_time_local", "latitude", "longitude"]).reset_index(drop=True)
        if trip_df.empty:
            continue

        keep_idx = _downsample_indices(len(trip_df), max_points=max_points_per_trip)
        sampled = trip_df.iloc[keep_idx].copy()
        coordinates = [[round(float(row["latitude"]), 6), round(float(row["longitude"]), 6)] for _, row in sampled.iterrows()]
        timestamps = [row["event_time_local"].isoformat() for _, row in sampled.iterrows()]
        segment_co2: list[float] = []
        segment_distance_km: list[float] = []

        for idx in range(1, len(sampled)):
            prev_row = sampled.iloc[idx - 1]
            curr_row = sampled.iloc[idx]
            lat1 = float(prev_row["latitude"])
            lon1 = float(prev_row["longitude"])
            lat2 = float(curr_row["latitude"])
            lon2 = float(curr_row["longitude"])
            distance_km = _haversine_km(lat1, lon1, lat2, lon2)
            duration_min = max(
                (
                    pd.Timestamp(curr_row["event_time_local"]) - pd.Timestamp(prev_row["event_time_local"])
                ).total_seconds()
                / 60.0,
                0.0,
            )
            speed_value = curr_row["speed_kmh"] if "speed_kmh" in sampled.columns else None
            speed_kmh = float(speed_value) if speed_value is not None and not pd.isna(speed_value) else None
            idle_min = duration_min if speed_kmh is not None and speed_kmh <= 1.0 else 0.0
            fuel_liter = distance_km * fuel_per_km + idle_min * idle_per_min
            segment_co2_kg = fuel_liter * co2_per_liter

            segment_distance_km.append(round(distance_km, 4))
            segment_co2.append(round(segment_co2_kg, 4))

            bucket_key = pd.Timestamp(curr_row["event_time_local"]).floor(f"{bucket_minutes}min").isoformat()
            acc_key = (
                bucket_key,
                str(meta["route_id"]),
                round(lat1, 4),
                round(lon1, 4),
                round(lat2, 4),
                round(lon2, 4),
            )
            if acc_key not in slice_accumulator:
                slice_accumulator[acc_key] = {
                    "bucket_start": bucket_key,
                    "route_id": str(meta["route_id"]),
                    "route_name": str(meta.get("route_name", "")) if pd.notna(meta.get("route_name", "")) else "",
                    "start": [round(lat1, 5), round(lon1, 5)],
                    "end": [round(lat2, 5), round(lon2, 5)],
                    "midpoint": [round((lat1 + lat2) / 2.0, 5), round((lon1 + lon2) / 2.0, 5)],
                    "co2_kg_est": 0.0,
                    "distance_km": 0.0,
                    "segment_count": 0,
                }
            slice_accumulator[acc_key]["co2_kg_est"] += segment_co2_kg
            slice_accumulator[acc_key]["distance_km"] += distance_km
            slice_accumulator[acc_key]["segment_count"] += 1
            slice_trip_sets.setdefault(bucket_key, set()).add(str(trip_id))
            slice_route_sets.setdefault(bucket_key, set()).add(str(meta["route_id"]))

        trip_distance_km = round(_safe_float(meta["distance_km"]), 2)
        trip_co2_kg = round(_safe_float(meta["co2_kg_est"]), 2)
        if trip_distance_km <= 0:
            trip_distance_km = round(sum(segment_distance_km), 2)
        if trip_co2_kg <= 0:
            trip_co2_kg = round(sum(segment_co2), 2)

        trips.append(
            {
                "trip_id": str(trip_id),
                "bus_id": str(meta["bus_id"]),
                "route_id": str(meta["route_id"]),
                "route_name": str(meta.get("route_name", "")) if pd.notna(meta.get("route_name", "")) else "",
                "trip_start": pd.to_datetime(meta["trip_start"]).isoformat(),
                "trip_end": pd.to_datetime(meta["trip_end"]).isoformat(),
                "distance_km": trip_distance_km,
                "co2_kg_est": trip_co2_kg,
                "point_count_raw": int(len(trip_df)),
                "point_count_view": int(len(sampled)),
                "coordinates": coordinates,
                "timestamps": timestamps,
                "segment_co2_kg": segment_co2,
                "segment_distance_km": segment_distance_km,
            }
        )

    time_slices: list[dict[str, Any]] = []
    for bucket_key in sorted(slice_trip_sets.keys()):
        segments = []
        total_co2 = 0.0
        total_distance = 0.0
        for item in slice_accumulator.values():
            if item["bucket_start"] != bucket_key:
                continue
            total_co2 += item["co2_kg_est"]
            total_distance += item["distance_km"]
            segments.append(
                {
                    "route_id": item["route_id"],
                    "route_name": item["route_name"],
                    "start": item["start"],
                    "end": item["end"],
                    "midpoint": item["midpoint"],
                    "co2_kg_est": round(item["co2_kg_est"], 4),
                    "distance_km": round(item["distance_km"], 4),
                    "segment_count": int(item["segment_count"]),
                }
            )
        segments.sort(key=lambda row: row["co2_kg_est"], reverse=True)
        time_slices.append(
            {
                "bucket_start": bucket_key,
                "bucket_label": pd.Timestamp(bucket_key).strftime("%Y-%m-%d %H:%M"),
                "total_co2_kg": round(total_co2, 3),
                "total_distance_km": round(total_distance, 3),
                "active_trips": len(slice_trip_sets.get(bucket_key, set())),
                "active_routes": len(slice_route_sets.get(bucket_key, set())),
                "segments": segments,
            }
        )

    route_geometry_map: dict[str, dict[str, Any]] = {}
    for trip in trips:
        route_id = str(trip["route_id"])
        current = route_geometry_map.get(route_id)
        if current is None or int(trip["point_count_view"]) > int(current["point_count_view"]):
            route_geometry_map[route_id] = {
                "route_id": route_id,
                "route_name": str(trip.get("route_name", "")),
                "coordinates": trip["coordinates"],
                "point_count_view": int(trip["point_count_view"]),
            }

    route_bucket_map: dict[tuple[str, str], dict[str, Any]] = {}
    day_route_trip_sets: dict[tuple[str, str], set[str]] = {}
    for slice_item in time_slices:
        bucket_start = pd.Timestamp(slice_item["bucket_start"])
        day_key = bucket_start.strftime("%Y-%m-%d")
        for seg in slice_item["segments"]:
            key = (day_key, str(seg["route_id"]))
            if key not in route_bucket_map:
                route_bucket_map[key] = {
                    "route_id": str(seg["route_id"]),
                    "route_name": str(seg.get("route_name", "")),
                    "event_date": day_key,
                    "bucket_values": {},
                    "total_co2_kg": 0.0,
                    "total_distance_km": 0.0,
                }
            route_bucket_map[key]["bucket_values"][slice_item["bucket_start"]] = (
                route_bucket_map[key]["bucket_values"].get(slice_item["bucket_start"], 0.0)
                + float(seg["co2_kg_est"])
            )
            route_bucket_map[key]["total_co2_kg"] += float(seg["co2_kg_est"])
            route_bucket_map[key]["total_distance_km"] += float(seg["distance_km"])
        for route_id in slice_route_sets.get(slice_item["bucket_start"], set()):
            day_route_trip_sets.setdefault((day_key, route_id), set()).update(slice_trip_sets.get(slice_item["bucket_start"], set()))

    route_emission_days: list[dict[str, Any]] = []
    for key, item in sorted(route_bucket_map.items()):
        bucket_keys = sorted(item["bucket_values"].keys())
        cumulative = []
        running = 0.0
        exact = []
        for bucket_key in bucket_keys:
            value = float(item["bucket_values"][bucket_key])
            running += value
            exact.append(round(value, 4))
            cumulative.append(round(running, 4))
        route_emission_days.append(
            {
                "route_id": item["route_id"],
                "route_name": item["route_name"],
                "event_date": item["event_date"],
                "bucket_labels": [pd.Timestamp(bucket_key).strftime("%H:%M") for bucket_key in bucket_keys],
                "bucket_keys": bucket_keys,
                "co2_kg_exact": exact,
                "co2_kg_cumulative": cumulative,
                "total_co2_kg": round(item["total_co2_kg"], 3),
                "total_distance_km": round(item["total_distance_km"], 3),
                "active_trip_count": len(day_route_trip_sets.get((item["event_date"], item["route_id"]), set())),
            }
        )

    return {
        "generated_from": generated_from,
        "trip_count": len(trips),
        "max_points_per_trip": max_points_per_trip,
        "bucket_minutes": bucket_minutes,
        "trips": trips,
        "time_slices": time_slices,
        "route_geometries": list(route_geometry_map.values()),
        "route_emission_days": route_emission_days,
    }


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
      grid-template-columns: 1fr;
      gap: 12px;
      margin-bottom: 12px;
    }}
    .panel-title {{
      margin: 0 0 10px;
      font-size: 15px;
    }}
    .table-wrap {{
      overflow: visible;
      max-height: none;
    }}
    table {{
      border-collapse: collapse;
      width: 100%;
      font-size: 13px;
      table-layout: fixed;
    }}
    th, td {{
      text-align: left;
      padding: 8px;
      border-bottom: 1px solid #eef2f7;
      white-space: normal;
      word-break: break-word;
      vertical-align: top;
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

    <div class="card" style="margin-bottom: 12px;">
      <div class="kpi-label">Spatial View</div>
      <div style="display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap;">
        <div style="color:var(--muted);font-size:14px;">Open the trip map dashboard to explore GPS trajectories and trip timing on the Ho Chi Minh City map.</div>
        <a href="./map.html" style="text-decoration:none;background:#0f766e;color:#fff;padding:10px 14px;border-radius:10px;font-weight:600;">Open Trip Map</a>
      </div>
    </div>

    <div class="panels">
      <div class="card">
        <h3 class="panel-title">Top 10 buses by CO2</h3>
        <div class="table-wrap"><table id="topBusTable"></table></div>
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
      const labelize = (text) => text
        .replace(/_/g, " ")
        .replace(/\bkg\b/gi, "kg")
        .replace(/\bco2\b/gi, "CO2")
        .replace(/\bid\b/g, "ID")
        .replace(/\b\w/g, (ch) => ch.toUpperCase());
      const thead = "<tr>" + headers.map(h => `<th>${{labelize(h)}}</th>`).join("") + "</tr>";
      const tbody = rows.map(r => "<tr>" + headers.map(h => `<td>${{r[h]}}</td>`).join("") + "</tr>").join("");
      table.innerHTML = `<thead>${{thead}}</thead><tbody>${{tbody}}</tbody>`;
    }};

    renderTable("topBusTable", payload.top_buses_table);
    renderTable("routeTable", payload.top_routes_table);
    renderTable("tripTable", payload.trip_outliers_table);
  </script>
</body>
</html>
"""


def _build_map_html(payload: dict[str, Any]) -> str:
    payload_json = json.dumps(payload, ensure_ascii=False)
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>HCMC Bus CO2 Map</title>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
  <style>
    :root {
      --bg: #efe8dc;
      --ink: #1f2937;
      --muted: #5b667a;
      --card: rgba(255, 255, 255, 0.92);
      --accent: #c2410c;
      --accent-2: #0f766e;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(1000px 600px at 0% 0%, #fdba7455, transparent),
        radial-gradient(1000px 600px at 100% 100%, #5eead455, transparent),
        var(--bg);
    }
    .shell {
      display: grid;
      grid-template-columns: 400px minmax(0, 1fr);
      min-height: 100vh;
    }
    .side {
      padding: 18px;
      border-right: 1px solid #e5e7eb;
      background: var(--card);
      backdrop-filter: blur(10px);
      overflow: auto;
    }
    .map-wrap { min-height: 100vh; }
    #map { width: 100%; height: 100vh; }
    h1 {
      margin: 0 0 8px;
      font-size: 28px;
      line-height: 1.1;
      letter-spacing: -0.03em;
    }
    .sub {
      margin: 0 0 18px;
      color: var(--muted);
      font-size: 14px;
      line-height: 1.5;
    }
    .control { margin-bottom: 14px; }
    label {
      display: block;
      margin-bottom: 6px;
      font-size: 12px;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }
    select, input[type="range"], button { width: 100%; }
    select, button {
      border: 1px solid #d6d9e0;
      border-radius: 12px;
      padding: 10px 12px;
      background: #fff;
      color: var(--ink);
      font: inherit;
    }
    button {
      cursor: pointer;
      background: var(--accent);
      color: #fff;
      border: none;
      font-weight: 600;
    }
    .button-row {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 10px;
      margin-bottom: 14px;
    }
    .button-row button.secondary { background: var(--accent-2); }
    .stat {
      background: #fff;
      border: 1px solid #eceff4;
      border-radius: 14px;
      padding: 12px;
      margin-bottom: 10px;
    }
    .stat .k {
      font-size: 12px;
      color: var(--muted);
      margin-bottom: 4px;
    }
    .stat .v {
      font-size: 18px;
      font-weight: 700;
    }
    .legend {
      background: #fff;
      border: 1px solid #eceff4;
      border-radius: 14px;
      padding: 12px;
      margin-bottom: 10px;
    }
    .legend-title {
      font-size: 12px;
      color: var(--muted);
      margin-bottom: 8px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }
    .legend-scale {
      height: 12px;
      border-radius: 999px;
      background: linear-gradient(90deg, hsl(210, 82%, 45%), hsl(120, 82%, 48%), hsl(55, 82%, 52%), hsl(0, 82%, 51%));
      margin-bottom: 6px;
    }
    .legend-labels {
      display: flex;
      justify-content: space-between;
      font-size: 12px;
      color: var(--muted);
      margin-bottom: 10px;
    }
    .legend-weights {
      display: flex;
      align-items: flex-end;
      gap: 10px;
    }
    .legend-line {
      flex: 1;
      border-radius: 999px;
      background: #475569;
    }
    .legend-line.thin { height: 3px; }
    .legend-line.mid { height: 7px; }
    .legend-line.thick { height: 11px; }
    .legend-weight-labels {
      display: flex;
      justify-content: space-between;
      font-size: 12px;
      color: var(--muted);
      margin-top: 6px;
    }
    .hint {
      font-size: 13px;
      color: var(--muted);
      line-height: 1.5;
      margin-top: 12px;
    }
    .leaflet-popup-content { font-family: "IBM Plex Sans", "Segoe UI", sans-serif; }
    @media (max-width: 900px) {
      .shell { grid-template-columns: 1fr; }
      .side {
        border-right: none;
        border-bottom: 1px solid #e5e7eb;
      }
      #map { height: 70vh; }
    }
  </style>
</head>
<body>
  <div class="shell">
    <aside class="side">
      <h1>CO2 Map by Route</h1>
      <p class="sub">Each bus route is shown as a stable line on the map. Color and stroke weight indicate how much CO2 the full route emitted over the day or within the selected 30-minute window.</p>

      <div class="control">
        <label for="viewMode">View Mode</label>
        <select id="viewMode">
          <option value="route">Route CO2 Overview</option>
          <option value="trip">Single Trip View</option>
        </select>
      </div>
      <div class="control">
        <label for="valueMode">CO2 Metric</label>
        <select id="valueMode">
          <option value="cumulative">Cumulative up to selected time</option>
          <option value="exact">Selected 30-minute window only</option>
        </select>
      </div>
      <div class="control">
        <label for="dateFilter">Date</label>
        <select id="dateFilter"></select>
      </div>
      <div class="control">
        <label for="routeFilter">Route</label>
        <select id="routeFilter"></select>
      </div>
      <div class="control">
        <label for="snapshotSlider">Timeline Position</label>
        <input id="snapshotSlider" type="range" min="0" max="0" step="1" value="0" />
      </div>
      <div class="control">
        <label for="timeBucketSelect">Time Label</label>
        <select id="timeBucketSelect"></select>
      </div>
      <div class="control">
        <label for="tripFilter">Trip</label>
        <select id="tripFilter"></select>
      </div>
      <div class="control">
        <label for="timeSlider">Trip Timeline</label>
        <input id="timeSlider" type="range" min="0" max="0" step="1" value="0" />
      </div>

      <div class="button-row">
        <button id="playBtn">Play</button>
        <button id="fitBtn" class="secondary">Fit Map</button>
      </div>

      <div class="legend">
        <div class="legend-title">CO2 Color Scale</div>
        <div class="legend-scale"></div>
        <div class="legend-labels">
          <span>Low</span>
          <span>Medium</span>
          <span>High</span>
        </div>
        <div class="legend-title">Route Stroke Weight</div>
        <div class="legend-weights">
          <div class="legend-line thin"></div>
          <div class="legend-line mid"></div>
          <div class="legend-line thick"></div>
        </div>
        <div class="legend-weight-labels">
          <span>Lower CO2</span>
          <span>Mid</span>
          <span>Higher CO2</span>
        </div>
      </div>

      <div class="stat"><div class="k">Current Selection</div><div class="v" id="routeName">-</div></div>
      <div class="stat"><div class="k">Routes / Trips</div><div class="v" id="tripMeta">-</div></div>
      <div class="stat"><div class="k">Distance / CO2</div><div class="v" id="tripStats">-</div></div>
      <div class="stat"><div class="k">Time Context</div><div class="v" id="timeLabel">-</div></div>
      <div class="stat"><div class="k">Map Summary</div><div class="v" id="cityStats">-</div></div>
      <div class="stat"><div class="k">Top Route</div><div class="v" id="cityHotspot">-</div></div>

      <p class="hint">
        Use `Cumulative` to explain how city-wide emissions build over the day. Use `30-minute window only` to isolate the time slices that contribute the most pressure.
      </p>
    </aside>
    <main class="map-wrap"><div id="map"></div></main>
  </div>

  <script>
    const map = L.map("map", { zoomControl: true }).setView([10.7769, 106.7009], 11);
    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", { maxZoom: 18, attribution: "&copy; OpenStreetMap contributors" }).addTo(map);

    const payload = __MAP_PAYLOAD__;
    const viewMode = document.getElementById("viewMode");
    const valueMode = document.getElementById("valueMode");
    const dateFilter = document.getElementById("dateFilter");
    const routeFilter = document.getElementById("routeFilter");
    const snapshotSlider = document.getElementById("snapshotSlider");
    const timeBucketSelect = document.getElementById("timeBucketSelect");
    const tripFilter = document.getElementById("tripFilter");
    const timeSlider = document.getElementById("timeSlider");
    const playBtn = document.getElementById("playBtn");
    const fitBtn = document.getElementById("fitBtn");

    const routeNameEl = document.getElementById("routeName");
    const tripMetaEl = document.getElementById("tripMeta");
    const tripStatsEl = document.getElementById("tripStats");
    const timeLabelEl = document.getElementById("timeLabel");
    const cityStatsEl = document.getElementById("cityStats");
    const cityHotspotEl = document.getElementById("cityHotspot");

    let currentTrip = null;
    let polyline = null;
    let previewLine = null;
    let marker = null;
    let playTimer = null;
    const routeLayer = L.layerGroup().addTo(map);

    const fmt = (v, digits = 2) => Number(v || 0).toLocaleString(undefined, { maximumFractionDigits: digits });
    const routeHue = (routeId) => Array.from(String(routeId || "UNKNOWN")).reduce((acc, ch) => acc + ch.charCodeAt(0), 0) % 360;
    const routeBaseColor = (routeId) => `hsl(${routeHue(routeId)}, 70%, 38%)`;
    const emissionColor = (ratio) => {
      const bounded = Math.max(0, Math.min(1, ratio));
      const hue = 210 - bounded * 210;
      return `hsl(${hue}, 82%, ${45 + bounded * 6}%)`;
    };
    const routeGeometryMap = new Map(payload.route_geometries.map((item) => [item.route_id, item]));

    const clearTripLayers = () => {
      if (polyline) map.removeLayer(polyline);
      if (previewLine) map.removeLayer(previewLine);
      if (marker) map.removeLayer(marker);
      polyline = null;
      previewLine = null;
      marker = null;
    };
    const clearRouteLayers = () => routeLayer.clearLayers();

    const routeDayRows = () => {
      const day = dateFilter.value;
      const routeId = routeFilter.value;
      return payload.route_emission_days.filter((row) => (!day || row.event_date === day) && (!routeId || routeId === "ALL" || row.route_id === routeId));
    };

    const currentMetricLabel = () => valueMode.value === "exact" ? "30-minute CO2" : "Cumulative CO2";

    const updateTripStats = (trip, idx) => {
      routeNameEl.textContent = trip.route_name ? `${trip.route_id} - ${trip.route_name}` : trip.route_id;
      tripMetaEl.textContent = `${trip.bus_id.slice(0, 12)}... / ${trip.trip_id.slice(0, 16)}...`;
      tripStatsEl.textContent = `${fmt(trip.distance_km)} km / ${fmt(trip.co2_kg_est)} kg CO2`;
      timeLabelEl.textContent = trip.timestamps[idx] || "-";
      cityStatsEl.textContent = "-";
      cityHotspotEl.textContent = "-";
    };

    const drawTrip = (trip, idx = 0) => {
      clearRouteLayers();
      clearTripLayers();
      currentTrip = trip;
      polyline = L.polyline(trip.coordinates, { color: routeBaseColor(trip.route_id), weight: 4, opacity: 0.4 }).addTo(map);
      previewLine = L.polyline(trip.coordinates.slice(0, idx + 1), { color: "#c2410c", weight: 5, opacity: 0.95 }).addTo(map);
      marker = L.circleMarker(trip.coordinates[idx], { radius: 7, color: "#7c2d12", weight: 2, fillColor: "#fb923c", fillOpacity: 0.95 }).addTo(map);
      marker.bindPopup(`<strong>${trip.route_id}</strong><br/>${trip.route_name || "Unknown route"}<br/>${trip.timestamps[idx]}`);
      updateTripStats(trip, idx);
      map.fitBounds(polyline.getBounds(), { padding: [24, 24] });
    };

    const stepTrip = () => {
      if (!currentTrip) return;
      const idx = Number(timeSlider.value);
      previewLine.setLatLngs(currentTrip.coordinates.slice(0, idx + 1));
      marker.setLatLng(currentTrip.coordinates[idx]);
      marker.getPopup().setContent(`<strong>${currentTrip.route_id}</strong><br/>${currentTrip.route_name || "Unknown route"}<br/>${currentTrip.timestamps[idx]}`);
      updateTripStats(currentTrip, idx);
    };

    const drawRouteOverview = () => {
      clearTripLayers();
      clearRouteLayers();
      currentTrip = null;
      const rows = routeDayRows();
      const bucketIndex = Math.max(0, Math.min(Number(snapshotSlider.value), Math.max((rows[0]?.bucket_labels.length || 1) - 1, 0)));
      const useExact = valueMode.value === "exact";
      const rendered = rows.map((row) => {
        const geometry = routeGeometryMap.get(row.route_id);
        const co2 = Number((useExact ? row.co2_kg_exact : row.co2_kg_cumulative)[bucketIndex] || 0);
        const cumulative = Number(row.co2_kg_cumulative[bucketIndex] || 0);
        const exact = Number(row.co2_kg_exact[bucketIndex] || 0);
        return { ...row, geometry, co2, cumulative, exact };
      }).filter((row) => row.geometry && row.geometry.coordinates && row.geometry.coordinates.length > 1);
      rendered.sort((a, b) => b.co2 - a.co2);
      const maxCo2 = Math.max(...rendered.map((row) => row.co2), 0);
      let totalCo2 = 0;
      let totalDistance = 0;
      rendered.forEach((row) => {
        totalCo2 += row.co2;
        totalDistance += Number(row.total_distance_km || 0);
        const ratio = maxCo2 > 0 ? row.co2 / maxCo2 : 0;
        L.polyline(row.geometry.coordinates, {
          color: emissionColor(ratio),
          weight: 2 + ratio * 9,
          opacity: 0.88,
          lineCap: "round"
        }).bindPopup(
          `<strong>${row.route_id}</strong> ${row.route_name || ""}<br/>Cumulative CO2: ${fmt(row.cumulative, 3)} kg<br/>30-minute CO2: ${fmt(row.exact, 3)} kg<br/>Distance: ${fmt(row.total_distance_km, 2)} km<br/>Trips: ${row.active_trip_count}`
        ).addTo(routeLayer);
      });

      const label = rows[0] ? rows[0].bucket_labels[bucketIndex] : "-";
      const top = rendered[0];
      routeNameEl.textContent = routeFilter.value && routeFilter.value !== "ALL" ? routeFilter.value : "All routes";
      tripMetaEl.textContent = `${rendered.length} routes shown`;
      tripStatsEl.textContent = `${fmt(totalDistance)} km / ${fmt(totalCo2, 3)} kg CO2`;
      timeLabelEl.textContent = `${dateFilter.value || "-"} to ${label}`;
      cityStatsEl.textContent = `${currentMetricLabel()} | ${payload.bucket_minutes}-minute bucket`;
      cityHotspotEl.textContent = top ? `${top.route_id} · ${fmt(top.co2, 3)} kg` : "No route data";
      timeBucketSelect.innerHTML = rows[0] ? rows[0].bucket_labels.map((item, idx) => `<option value="${idx}">${item}</option>`).join("") : "";
      timeBucketSelect.value = String(bucketIndex);

      if (rendered.length) {
        const bounds = L.latLngBounds(rendered.flatMap((row) => row.geometry.coordinates));
        map.fitBounds(bounds, { padding: [24, 24] });
      }
    };

    const stopPlay = () => {
      if (playTimer) {
        clearInterval(playTimer);
        playTimer = null;
        playBtn.textContent = "Play";
      }
    };

    const startPlay = () => {
      if (viewMode.value === "trip") {
        if (!currentTrip) return;
        stopPlay();
        playBtn.textContent = "Pause";
        playTimer = setInterval(() => {
          const next = Number(timeSlider.value) + 1;
          if (next >= Number(timeSlider.max)) {
            timeSlider.value = timeSlider.max;
            stepTrip();
            stopPlay();
            return;
          }
          timeSlider.value = String(next);
          stepTrip();
        }, 250);
        return;
      }
      stopPlay();
      playBtn.textContent = "Pause";
      playTimer = setInterval(() => {
        const next = Number(snapshotSlider.value) + 1;
        if (next >= Number(snapshotSlider.max)) {
          snapshotSlider.value = snapshotSlider.max;
          drawRouteOverview();
          stopPlay();
          return;
        }
        snapshotSlider.value = String(next);
        drawRouteOverview();
      }, 500);
    };

    const refreshTripOptions = () => {
      const routeId = routeFilter.value;
      const trips = payload.trips.filter((trip) => !routeId || routeId === "ALL" || trip.route_id === routeId);
      tripFilter.innerHTML = trips.map((trip) => `<option value="${trip.trip_id}">${trip.route_id} | ${trip.route_name || "Unknown"} | ${trip.trip_start.slice(0, 16)}</option>`).join("");
      if (viewMode.value === "trip" && trips.length) {
        tripFilter.value = trips[0].trip_id;
        timeSlider.min = "0";
        timeSlider.max = String(Math.max(trips[0].coordinates.length - 1, 0));
        timeSlider.value = "0";
        drawTrip(trips[0], 0);
      }
    };

    const refreshRouteControls = () => {
      const rows = routeDayRows();
      const maxIndex = Math.max((rows[0]?.bucket_labels.length || 1) - 1, 0);
      snapshotSlider.min = "0";
      snapshotSlider.max = String(maxIndex);
      if (Number(snapshotSlider.value) > maxIndex) snapshotSlider.value = String(maxIndex);
      drawRouteOverview();
    };

    const applyViewMode = () => {
      const isTrip = viewMode.value === "trip";
      valueMode.disabled = isTrip;
      dateFilter.disabled = isTrip;
      snapshotSlider.disabled = isTrip;
      timeBucketSelect.disabled = isTrip;
      tripFilter.disabled = !isTrip;
      timeSlider.disabled = !isTrip;
      if (isTrip) refreshTripOptions();
      else refreshRouteControls();
    };

    valueMode.addEventListener("change", refreshRouteControls);
    dateFilter.addEventListener("change", refreshRouteControls);
    routeFilter.addEventListener("change", () => {
      refreshTripOptions();
      refreshRouteControls();
    });
    snapshotSlider.addEventListener("input", drawRouteOverview);
    timeBucketSelect.addEventListener("change", () => {
      snapshotSlider.value = timeBucketSelect.value;
      drawRouteOverview();
    });
    viewMode.addEventListener("change", applyViewMode);
    tripFilter.addEventListener("change", () => {
      const trip = payload.trips.find((item) => item.trip_id === tripFilter.value);
      if (!trip) return;
      timeSlider.max = String(Math.max(trip.coordinates.length - 1, 0));
      timeSlider.value = "0";
      drawTrip(trip, 0);
    });
    timeSlider.addEventListener("input", stepTrip);
    fitBtn.addEventListener("click", () => {
      if (viewMode.value === "trip" && polyline) map.fitBounds(polyline.getBounds(), { padding: [24, 24] });
      if (viewMode.value === "route") drawRouteOverview();
    });
    playBtn.addEventListener("click", () => {
      if (playTimer) stopPlay();
      else startPlay();
    });

    const dates = [...new Set(payload.route_emission_days.map((row) => row.event_date))].sort();
    dateFilter.innerHTML = dates.map((day) => `<option value="${day}">${day}</option>`).join("");
    const routes = [...new Set(payload.trips.map((trip) => trip.route_id))].sort();
    routeFilter.innerHTML = ['<option value="ALL">All routes</option>'].concat(routes.map((routeId) => `<option value="${routeId}">${routeId}</option>`)).join("");

    refreshTripOptions();
    refreshRouteControls();
    applyViewMode();
  </script>
</body>
</html>
""".replace("__MAP_PAYLOAD__", payload_json)


def generate_assets(gold_root: Path, docs_dir: Path) -> None:
    bus_daily = _read_gold_table(gold_root / "bus_daily_emissions")
    route_hourly = _read_gold_table(gold_root / "route_hourly_emissions")
    trip_summary = _read_gold_table(gold_root / "trip_summary")
    route_ref = _load_route_reference(Path("data/route_catalog_table2.csv"))
    silver_path = Path("data/lakehouse/silver/bus_gps")

    bus_daily["event_date"] = pd.to_datetime(bus_daily["event_date"]).dt.date
    route_hourly["event_date"] = pd.to_datetime(route_hourly["event_date"]).dt.date
    route_hourly["event_hour"] = route_hourly["event_hour"].fillna(0).astype(int)
    route_hourly["route_id"] = route_hourly["route_id"].map(_normalize_route_id)
    trip_summary["route_id"] = trip_summary["route_id"].map(_normalize_route_id)

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
    bus_route_pref = (
        trip_summary[trip_summary["route_id"] != "UNKNOWN"]
        .groupby(["bus_id", "route_id"], as_index=False)
        .size()
        .sort_values(["bus_id", "size"], ascending=[True, False])
        .drop_duplicates(subset=["bus_id"], keep="first")
        .rename(columns={"route_id": "dominant_route_id"})
    )
    top_buses = top_buses.merge(bus_route_pref[["bus_id", "dominant_route_id"]], on="bus_id", how="left")
    top_buses = top_buses.merge(
        route_ref[["route_id", "route_name"]].rename(columns={"route_id": "dominant_route_id", "route_name": "dominant_route_name"}),
        on="dominant_route_id",
        how="left",
    )
    top_routes = (
        route_hourly.groupby("route_id", as_index=False)[["distance_km", "fuel_liter_est", "co2_kg_est"]]
        .sum()
        .sort_values("co2_kg_est", ascending=False)
        .head(10)
    )
    route_unique_buses = (
        trip_summary[trip_summary["route_id"] != "UNKNOWN"]
        .groupby("route_id", as_index=False)["bus_id"]
        .nunique()
        .rename(columns={"bus_id": "unique_active_buses"})
    )
    top_routes = top_routes.merge(route_unique_buses, on="route_id", how="left")
    top_routes = top_routes.merge(route_ref, on="route_id", how="left")
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
    trip_outliers = trip_outliers.merge(route_ref[["route_id", "route_name"]], on="route_id", how="left")

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

    top_bus = top_buses.iloc[0] if not top_buses.empty else None
    top_route = top_routes.iloc[0] if not top_routes.empty else None
    peak_day = daily_totals.sort_values("co2_kg_est", ascending=False).iloc[0] if not daily_totals.empty else None
    peak_hour = peak_hours.iloc[0] if not peak_hours.empty else None
    route_missing_count = int((trip_summary["route_id"] == "UNKNOWN").sum())
    trip_outlier_count = int(len(trip_outliers))

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
1. **Route {str(top_route["route_id"]) if top_route is not None else "N/A"}** is the highest-emission route in the current Gold snapshot, with **{round(_safe_float(top_route["co2_kg_est"]), 2) if top_route is not None else 0} kg CO2** across **{round(_safe_float(top_route["distance_km"]), 2) if top_route is not None else 0} km** and **{int(_safe_float(top_route["unique_active_buses"])) if top_route is not None else 0} unique buses**.
2. The busiest emission window is **hour {str(peak_hour["event_hour"]).zfill(2) if peak_hour is not None else "N/A"}:00**, contributing **{round(_safe_float(peak_hour["co2_kg_est"]), 2) if peak_hour is not None else 0} kg CO2**. This indicates afternoon operations dominate system-wide emissions.
3. The highest single day in the current dataset is **{str(peak_day["event_date"]) if peak_day is not None else "N/A"}**, reaching **{round(_safe_float(peak_day["co2_kg_est"]), 2) if peak_day is not None else 0} kg CO2**. Daily totals are therefore not flat and should be interpreted as a changing operating profile over time.
4. The highest-emission bus is **{str(top_bus["bus_id"]) if top_bus is not None else "N/A"}**, responsible for **{round(_safe_float(top_bus["co2_kg_est"]), 2) if top_bus is not None else 0} kg CO2**. This suggests a relatively small set of buses contributes disproportionately to fleet emissions.
5. **{route_missing_count} trips** still have missing `route_id` values, equivalent to **{round(kpis["trip_route_missing_ratio"] * 100, 2)}%** of all trips. Route-level rankings are therefore directionally useful, but the missing share should be disclosed in the report.
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
"""
    (analysis_dir / "insight_summary.md").write_text(insight_md, encoding="utf-8")

    dashboard_payload = {
        "kpis": kpis,
        "top_buses_table": [
            {
                "bus_id": str(row["bus_id"]),
                "route_id": str(row.get("dominant_route_id", "")) if pd.notna(row.get("dominant_route_id", "")) else "",
                "route_name": str(row.get("dominant_route_name", "")) if pd.notna(row.get("dominant_route_name", "")) else "",
                "distance_km": round(_safe_float(row["distance_km"]), 2),
                "co2_kg_est": round(_safe_float(row["co2_kg_est"]), 2),
            }
            for _, row in top_buses.iterrows()
        ],
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
                "route_name": str(row.get("route_name", "")) if pd.notna(row.get("route_name", "")) else "",
                "duration_min": str(row.get("duration_min", "")) if pd.notna(row.get("duration_min", "")) else "",
                "distance_km_ref": round(_safe_float(row.get("distance_km_ref", 0)), 2),
                "stops_inbound_outbound": str(row.get("stops_inbound_outbound", "")) if pd.notna(row.get("stops_inbound_outbound", "")) else "",
                "connected_metro1": str(row.get("connected_metro1", "")) if pd.notna(row.get("connected_metro1", "")) else "",
                "distance_km": round(_safe_float(row["distance_km"]), 2),
                "co2_kg_est": round(_safe_float(row["co2_kg_est"]), 2),
                "unique_active_buses": int(_safe_float(row["unique_active_buses"])),
            }
            for _, row in top_routes.iterrows()
        ],
        "trip_outliers_table": [
            {
                "trip_id": str(row["trip_id"]),
                "bus_id": str(row["bus_id"]),
                "route_id": str(row["route_id"]),
                "route_name": str(row.get("route_name", "")) if pd.notna(row.get("route_name", "")) else "",
                "distance_km": round(_safe_float(row["distance_km"]), 2),
                "co2_kg_est": round(_safe_float(row["co2_kg_est"]), 2),
                "co2_kg_per_km": round(_safe_float(row["co2_kg_per_km"]), 3),
            }
            for _, row in trip_outliers.iterrows()
        ],
    }
    (dashboard_dir / "index.html").write_text(_build_dashboard_html(dashboard_payload), encoding="utf-8")
    map_payload = _build_map_payload(
        silver_path=silver_path,
        trip_summary=trip_summary,
        route_ref=route_ref,
        replay_cache_path=Path("data/replay/events.jsonl"),
    )
    with (dashboard_dir / "map_data.json").open("w", encoding="utf-8") as f:
        json.dump(map_payload, f, ensure_ascii=False)
    (dashboard_dir / "map.html").write_text(_build_map_html(map_payload), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate analysis and dashboard assets from Gold layer.")
    parser.add_argument("--gold-root", default="data/lakehouse/gold")
    parser.add_argument("--docs-dir", default="docs")
    args = parser.parse_args()

    generate_assets(gold_root=Path(args.gold_root), docs_dir=Path(args.docs_dir))
    print("Generated report assets under docs/analysis and docs/dashboard")


if __name__ == "__main__":
    main()
