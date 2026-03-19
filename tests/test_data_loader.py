import json

from producer.data_loader import iter_raw_file_frames


def test_iter_raw_file_frames_streams_json_array(tmp_path) -> None:
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    payload = [
        {"msgBusWayPoint": {"vehicle": "B1", "datetime": 1, "x": 106.1, "y": 10.1}},
        {"msgBusWayPoint": {"vehicle": "B2", "datetime": 2, "x": 106.2, "y": 10.2}},
        {"msgBusWayPoint": {"vehicle": "B3", "datetime": 3, "x": 106.3, "y": 10.3}},
    ]
    (raw_dir / "sample.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")

    frames = list(iter_raw_file_frames(str(raw_dir), chunk_rows=2))

    assert len(frames) == 2
    assert sum(len(frame) for frame in frames) == 3
    assert all("source_file" in frame.columns for frame in frames)
    assert frames[0].iloc[0]["msgBusWayPoint.vehicle"] == "B1"
