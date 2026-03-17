from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


def _find_first(existing_columns: list[str], candidates: list[str]) -> str | None:
    lower_map = {c.lower(): c for c in existing_columns}
    for c in candidates:
        if c.lower() in lower_map:
            return lower_map[c.lower()]
    return None


def normalize_raw_schema(df: pd.DataFrame, cfg: dict[str, Any]) -> pd.DataFrame:
    if df.empty:
        return df

    schema_cfg = cfg["schema"]
    columns = list(df.columns)

    mapped = {
        "event_time": _find_first(columns, schema_cfg["event_time_candidates"]),
        "bus_id": _find_first(columns, schema_cfg["bus_id_candidates"]),
        "route_id": _find_first(columns, schema_cfg["route_id_candidates"]),
        "latitude": _find_first(columns, schema_cfg["latitude_candidates"]),
        "longitude": _find_first(columns, schema_cfg["longitude_candidates"]),
        "speed_kmh": _find_first(columns, schema_cfg["speed_candidates"]),
        "heading": _find_first(columns, schema_cfg["heading_candidates"]),
    }

    missing_critical = [k for k in ["event_time", "bus_id", "latitude", "longitude"] if mapped[k] is None]
    if missing_critical:
        raise ValueError(f"Cannot map required fields: {missing_critical}. Available columns: {columns}")

    result = pd.DataFrame()
    for target, src in mapped.items():
        if src is None:
            result[target] = None
        else:
            result[target] = df[src]

    # Handle epoch-second fields (common in telemetry dumps) and ISO timestamp strings.
    event_numeric = pd.to_numeric(result["event_time"], errors="coerce")
    numeric_ratio = event_numeric.notna().mean()
    if numeric_ratio > 0.8:
        result["event_time"] = pd.to_datetime(event_numeric, unit="s", errors="coerce", utc=True)
    else:
        result["event_time"] = pd.to_datetime(result["event_time"], errors="coerce", utc=True)
    result["latitude"] = pd.to_numeric(result["latitude"], errors="coerce")
    result["longitude"] = pd.to_numeric(result["longitude"], errors="coerce")
    result["speed_kmh"] = pd.to_numeric(result["speed_kmh"], errors="coerce")
    result["heading"] = pd.to_numeric(result["heading"], errors="coerce")

    mapping_path = cfg.get("data", {}).get("vehicle_route_mapping_path")
    if mapping_path:
        p = Path(mapping_path)
        if p.exists():
            mapping_df = pd.read_csv(p)
            if not mapping_df.empty and "vehicle" in mapping_df.columns:
                route_col = "route_no" if "route_no" in mapping_df.columns else ("route_id" if "route_id" in mapping_df.columns else None)
                if route_col is not None:
                    route_map = (
                        mapping_df[["vehicle", route_col]]
                        .dropna(subset=["vehicle", route_col])
                        .drop_duplicates(subset=["vehicle"])
                        .set_index("vehicle")[route_col]
                    )
                    mapped_routes = result["bus_id"].map(route_map)
                    result["route_id"] = result["route_id"].replace("", pd.NA).fillna(mapped_routes)

    if "source_file" in df.columns:
        result["source_file"] = df["source_file"]
    else:
        result["source_file"] = "unknown"

    result = result.sort_values("event_time").reset_index(drop=True)
    return result
