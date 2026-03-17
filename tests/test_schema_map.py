import pandas as pd

from producer.schema_map import normalize_raw_schema


def test_normalize_schema_basic() -> None:
    cfg = {
        "schema": {
            "event_time_candidates": ["timestamp"],
            "bus_id_candidates": ["vehicle_id"],
            "route_id_candidates": ["route"],
            "latitude_candidates": ["lat"],
            "longitude_candidates": ["lon"],
            "speed_candidates": ["speed"],
            "heading_candidates": ["heading"],
        }
    }
    raw = pd.DataFrame(
        [
            {
                "timestamp": "2024-01-01T10:00:00Z",
                "vehicle_id": "B01",
                "route": "R01",
                "lat": 10.1,
                "lon": 106.1,
                "speed": 30,
                "heading": 45,
            }
        ]
    )

    out = normalize_raw_schema(raw, cfg)
    assert list(out.columns) == ["event_time", "bus_id", "route_id", "latitude", "longitude", "speed_kmh", "heading", "source_file"]
    assert out.iloc[0]["bus_id"] == "B01"
