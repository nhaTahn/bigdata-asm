from __future__ import annotations

import argparse
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from kafka import KafkaProducer
import pandas as pd

from producer.config import load_config
from producer.data_loader import iter_raw_file_frames
from producer.schema_map import normalize_raw_schema


logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Replay historical GPS records to Kafka.")
    parser.add_argument("--config", default="configs/pipeline.yaml", help="Path to pipeline config file")
    return parser


def _resolve_runtime_config(cfg: dict[str, Any]) -> dict[str, Any]:
    runtime = json.loads(json.dumps(cfg))
    runtime["kafka"]["bootstrap_servers"] = os.getenv("KAFKA_BOOTSTRAP_SERVERS", cfg["kafka"]["bootstrap_servers"])
    runtime["kafka"]["topic_raw_gps"] = os.getenv("KAFKA_TOPIC_RAW_GPS", cfg["kafka"]["topic_raw_gps"])

    runtime["replay"]["mode"] = os.getenv("REPLAY_MODE", str(cfg["replay"]["mode"]))
    runtime["replay"]["sample_size"] = int(os.getenv("REPLAY_SAMPLE_SIZE", str(cfg["replay"]["sample_size"])))
    runtime["replay"]["speed_multiplier"] = float(
        os.getenv("REPLAY_SPEED_MULTIPLIER", str(cfg["replay"]["speed_multiplier"]))
    )
    default_sleep = "0" if str(runtime["replay"]["mode"]).lower() == "full" else "1"
    runtime["replay"]["enable_sleep"] = os.getenv("REPLAY_ENABLE_SLEEP", default_sleep).lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    runtime["replay"]["flush_every_n"] = int(
        os.getenv("REPLAY_FLUSH_EVERY_N", os.getenv("REPLAY_TIMESTAMP_COLLECT_MS", str(cfg["replay"]["flush_every_n"])))
    )

    runtime["data"]["raw_path"] = os.getenv("DATA_RAW_PATH", cfg["data"]["raw_path"])
    runtime["data"]["vehicle_route_mapping_path"] = os.getenv(
        "VEHICLE_ROUTE_MAPPING_PATH",
        cfg["data"].get("vehicle_route_mapping_path", "./data/vehicle_route_mapping.csv"),
    )
    runtime["data"]["replay_cache_path"] = os.getenv("REPLAY_CACHE_PATH", cfg["data"]["replay_cache_path"])
    return runtime


def _iter_events(df, mode: str, sample_size: int):
    payload = df
    if mode.lower() == "sample":
        if sample_size > 0 and sample_size < len(df):
            # Take samples evenly across the sorted timeline instead of head(),
            # so sample mode still covers the full date range.
            step = len(df) / float(sample_size)
            idx = [min(int(i * step), len(df) - 1) for i in range(sample_size)]
            payload = df.iloc[idx]
        elif sample_size > 0:
            payload = df
        else:
            payload = df.iloc[0:0]
    for _, row in payload.iterrows():
        event = {
            "event_time": row["event_time"].isoformat() if row["event_time"] is not None else None,
            "bus_id": row["bus_id"],
            "route_id": row.get("route_id"),
            "latitude": row["latitude"],
            "longitude": row["longitude"],
            "speed_kmh": row.get("speed_kmh"),
            "heading": row.get("heading"),
            "source_file": row.get("source_file", "unknown"),
            "ingest_time": datetime.now(timezone.utc).isoformat(),
        }
        yield event


def _iter_normalized_chunks(cfg: dict[str, Any]) -> tuple[Iterator[pd.DataFrame], int]:
    chunk_rows = int(os.getenv("REPLAY_CHUNK_ROWS", "5000"))

    def generator() -> Iterator[pd.DataFrame]:
        for raw_chunk in iter_raw_file_frames(cfg["data"]["raw_path"], chunk_rows=chunk_rows):
            norm_df = normalize_raw_schema(raw_chunk, cfg)
            norm_df = norm_df.dropna(subset=["event_time", "bus_id", "latitude", "longitude"])
            if norm_df.empty:
                continue
            yield norm_df.sort_values("event_time").reset_index(drop=True)

    return generator(), chunk_rows


def _iter_replay_events(cfg: dict[str, Any]) -> Iterator[dict[str, Any]]:
    mode = str(cfg["replay"]["mode"]).lower()
    sample_size = int(cfg["replay"]["sample_size"])
    chunks, chunk_rows = _iter_normalized_chunks(cfg)

    if mode == "sample":
        seen = 0
        reservoir: list[dict[str, Any]] = []
        for norm_df in chunks:
            for event in _iter_events(norm_df, "full", sample_size):
                seen += 1
                if len(reservoir) < sample_size:
                    reservoir.append(event)
                else:
                    j = int.from_bytes(os.urandom(8), "big") % seen
                    if j < sample_size:
                        reservoir[j] = event
        reservoir.sort(key=lambda event: event["event_time"] or "")
        logger.info(
            "Prepared sample replay with %s events from streaming reservoir (chunk_rows=%s)",
            len(reservoir),
            chunk_rows,
        )
        yield from reservoir
        return

    for norm_df in chunks:
        yield from _iter_events(norm_df, "full", sample_size)


def main() -> None:
    args = build_parser().parse_args()
    cfg = _resolve_runtime_config(load_config(args.config))

    logger.info("Loading raw data from: %s", cfg["data"]["raw_path"])
    producer = KafkaProducer(
        bootstrap_servers=cfg["kafka"]["bootstrap_servers"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
        linger_ms=20,
    )

    replay_cache_path = Path(cfg["data"]["replay_cache_path"])
    replay_cache_path.parent.mkdir(parents=True, exist_ok=True)

    count = 0
    speed_multiplier = cfg["replay"]["speed_multiplier"]
    enable_sleep = bool(cfg["replay"].get("enable_sleep", True))
    prev_event_time = None
    emitted_any = False

    with replay_cache_path.open("w", encoding="utf-8") as replay_file:
        for event in _iter_replay_events(cfg):
            emitted_any = True
            event_time = datetime.fromisoformat(event["event_time"]) if event["event_time"] else None
            if enable_sleep and prev_event_time is not None and event_time is not None and speed_multiplier > 0:
                dt = (event_time - prev_event_time).total_seconds() / speed_multiplier
                if dt > 0:
                    time.sleep(min(dt, 1.0))
            prev_event_time = event_time

            producer.send(cfg["kafka"]["topic_raw_gps"], key=event["bus_id"], value=event)
            replay_file.write(json.dumps(event) + "\n")

            count += 1
            if count % cfg["replay"]["flush_every_n"] == 0:
                producer.flush()
                logger.info("Published %s events", count)

    if not emitted_any:
        logger.warning("No supported rows found in %s. Producer exits without publishing.", cfg["data"]["raw_path"])

    producer.flush()
    producer.close()
    logger.info("Replay completed. Total published events: %s", count)


if __name__ == "__main__":
    main()
