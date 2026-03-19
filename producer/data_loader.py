from __future__ import annotations

import json
from pathlib import Path
from typing import Iterator

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


def _iter_json_array_items(path: Path, chunk_bytes: int = 1024 * 1024) -> Iterator[dict]:
    decoder = json.JSONDecoder()
    buffer = ""
    in_array = False
    reached_end = False

    with path.open("r", encoding="utf-8") as f:
        while True:
            if not reached_end and len(buffer) < chunk_bytes:
                piece = f.read(chunk_bytes)
                if piece:
                    buffer += piece
                else:
                    reached_end = True

            pos = 0
            made_progress = False
            while True:
                while pos < len(buffer) and buffer[pos].isspace():
                    pos += 1

                if pos >= len(buffer):
                    break

                current = buffer[pos]
                if not in_array:
                    if current == "[":
                        in_array = True
                        pos += 1
                        made_progress = True
                        continue
                    if current == "{":
                        try:
                            item, end = decoder.raw_decode(buffer, pos)
                        except json.JSONDecodeError:
                            break
                        if isinstance(item, dict):
                            yield item
                        pos = end
                        made_progress = True
                        continue
                    raise ValueError(f"Unsupported JSON structure in {path}")

                if current == ",":
                    pos += 1
                    made_progress = True
                    continue

                if current == "]":
                    pos += 1
                    made_progress = True
                    in_array = False
                    continue

                try:
                    item, end = decoder.raw_decode(buffer, pos)
                except json.JSONDecodeError:
                    break
                if isinstance(item, dict):
                    yield item
                pos = end
                made_progress = True

            if pos > 0:
                buffer = buffer[pos:]

            if reached_end:
                if buffer.strip():
                    raise ValueError(f"Trailing JSON content in {path}")
                return

            if not made_progress:
                continue


def iter_raw_file_frames(raw_path: str, chunk_rows: int = 5000) -> Iterator[pd.DataFrame]:
    base = Path(raw_path)
    if not base.exists():
        raise FileNotFoundError(f"Raw data path does not exist: {raw_path}")

    files = sorted(p for p in base.rglob("*") if p.is_file() and p.suffix.lower() in {".csv", ".json", ".parquet"})
    for f in files:
        ext = f.suffix.lower()
        if ext == ".csv":
            for df in pd.read_csv(f, chunksize=chunk_rows):
                if df.empty:
                    continue
                df["source_file"] = str(f)
                yield df
            continue

        if ext == ".json":
            rows: list[dict] = []
            for item in _iter_json_array_items(f):
                rows.append(item)
                if len(rows) >= chunk_rows:
                    df = pd.json_normalize(rows)
                    df["source_file"] = str(f)
                    yield df
                    rows = []
            if rows:
                df = pd.json_normalize(rows)
                df["source_file"] = str(f)
                yield df
            continue

        if ext == ".parquet":
            df = pd.read_parquet(f)
            if df.empty:
                continue
            df["source_file"] = str(f)
            yield df


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
