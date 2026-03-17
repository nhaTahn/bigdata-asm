from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path

import pandas as pd

SUPPORTED = {".csv", ".json", ".parquet"}
MAX_SAMPLE_ROWS = 2000


def _json_to_df(path: Path, max_rows: int) -> tuple[pd.DataFrame, list[str]]:
    warnings: list[str] = []
    if path.stat().st_size == 0:
        warnings.append("empty_file")
        return pd.DataFrame(), warnings

    with path.open("r", encoding="utf-8") as f:
        leading = f.read(1)
        while leading and leading.isspace():
            leading = f.read(1)

    # JSON array/object file
    if leading in {"[", "{"}:
        with path.open("r", encoding="utf-8") as f:
            obj = json.load(f)
        if isinstance(obj, list):
            sample = obj[:max_rows]
            return pd.json_normalize(sample), warnings
        if isinstance(obj, dict):
            return pd.json_normalize([obj]), warnings
        warnings.append("unsupported_json_root_type")
        return pd.DataFrame(), warnings

    # JSON-lines fallback
    rows: list[dict] = []
    bad_lines = 0
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()
            if not stripped:
                continue
            try:
                item = json.loads(stripped)
                if isinstance(item, dict):
                    rows.append(item)
                else:
                    bad_lines += 1
            except json.JSONDecodeError:
                bad_lines += 1
            if len(rows) >= max_rows:
                break
    if bad_lines > 0:
        warnings.append(f"bad_json_lines={bad_lines}")
    return pd.json_normalize(rows), warnings


def inspect_file(path: Path) -> dict:
    ext = path.suffix.lower()
    warnings: list[str] = []
    if ext == ".csv":
        df = pd.read_csv(path, nrows=MAX_SAMPLE_ROWS)
    elif ext == ".json":
        df, warnings = _json_to_df(path, MAX_SAMPLE_ROWS)
    elif ext == ".parquet":
        df = pd.read_parquet(path)
        if len(df) > MAX_SAMPLE_ROWS:
            df = df.head(MAX_SAMPLE_ROWS)
    else:
        return {}

    summary = {
        "file": str(path),
        "file_type": ext,
        "rows_sampled": int(len(df)),
        "columns": list(df.columns),
        "null_ratio": {c: float(df[c].isna().mean()) for c in df.columns[:30]},
    }
    if warnings:
        summary["warnings"] = warnings
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect dataset under data/raw")
    parser.add_argument("--data-path", default="data/raw")
    parser.add_argument("--output", default="docs/dataset_inspection.json")
    args = parser.parse_args()

    base = Path(args.data_path)
    files = [f for f in base.rglob("*") if f.is_file() and f.suffix.lower() in SUPPORTED]

    report = {
        "data_path": str(base),
        "file_count": len(files),
        "files": [],
        "column_frequency": {},
    }

    col_counter: Counter[str] = Counter()
    for f in files:
        try:
            summary = inspect_file(f)
        except Exception as exc:  # keep inspection resilient per-file
            report["files"].append(
                {
                    "file": str(f),
                    "file_type": f.suffix.lower(),
                    "error": str(exc),
                }
            )
            continue

        if summary:
            report["files"].append(summary)
            col_counter.update([c.lower() for c in summary["columns"]])

    report["column_frequency"] = dict(col_counter.most_common())

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
