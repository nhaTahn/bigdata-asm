from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


def _read_json_flexible(path: Path) -> pd.DataFrame:
    if path.stat().st_size == 0:
        return pd.DataFrame()

    with path.open("r", encoding="utf-8") as f:
        leading = f.read(1)
        while leading and leading.isspace():
            leading = f.read(1)

    if leading in {"[", "{"}:
        with path.open("r", encoding="utf-8") as f:
            obj = json.load(f)
        if isinstance(obj, list):
            return pd.json_normalize(obj)
        if isinstance(obj, dict):
            return pd.json_normalize([obj])
        return pd.DataFrame()

    rows: list[dict] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()
            if not stripped:
                continue
            try:
                item = json.loads(stripped)
            except json.JSONDecodeError:
                continue
            if isinstance(item, dict):
                rows.append(item)
    return pd.json_normalize(rows)


def load_raw_files(raw_path: str) -> pd.DataFrame:
    base = Path(raw_path)
    if not base.exists():
        raise FileNotFoundError(f"Raw data path does not exist: {raw_path}")

    files = [p for p in base.rglob("*") if p.is_file() and p.suffix.lower() in {".csv", ".json", ".parquet"}]
    if not files:
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []
    for f in files:
        ext = f.suffix.lower()
        if ext == ".csv":
            df = pd.read_csv(f)
        elif ext == ".json":
            df = _read_json_flexible(f)
        elif ext == ".parquet":
            df = pd.read_parquet(f)
        else:
            continue
        df["source_file"] = str(f)
        frames.append(df)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)
