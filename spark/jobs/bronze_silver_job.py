from __future__ import annotations

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from spark.common.config import load_config
from spark.common.transform import enrich_segments, normalize_bronze


def _build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("bronze-silver-job")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.sql.streaming.schemaInference", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def run(config_path: str) -> None:
    cfg = load_config(config_path)
    spark = _build_spark()
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", cfg["kafka"]["bootstrap_servers"])

    starting_offsets = os.getenv("KAFKA_STARTING_OFFSETS", cfg["kafka"].get("starting_offsets", "earliest"))

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", cfg["kafka"]["topic_raw_gps"])
        .option("startingOffsets", starting_offsets)
        .option("maxOffsetsPerTrigger", str(cfg["kafka"].get("max_offsets_per_trigger", 2000)))
        .load()
    )

    raw_json = kafka_df.selectExpr("CAST(value AS STRING) AS json_payload", "timestamp AS kafka_ingest_ts")

    parsed = raw_json.select(
        F.from_json(
            F.col("json_payload"),
            "event_time string, bus_id string, route_id string, latitude double, longitude double, speed_kmh double, heading double, source_file string, ingest_time string",
        ).alias("x"),
        F.col("kafka_ingest_ts"),
        F.col("json_payload"),
    ).select("x.*", "kafka_ingest_ts", "json_payload")

    def process_batch(batch_df, batch_id: int) -> None:
        if batch_df.rdd.isEmpty():
            return

        batch_df.write.format("delta").mode("append").save(cfg["data"]["bronze_path"])

        silver_input = normalize_bronze(batch_df)
        silver_enriched = enrich_segments(
            silver_input,
            trip_gap_minutes=int(cfg["quality"]["trip_gap_minutes"]),
            max_speed_kmh=float(cfg["quality"]["max_speed_kmh"]),
            max_jump_meters=float(cfg["quality"]["max_jump_meters"]),
        )
        silver_clean = silver_enriched.filter(F.col("is_valid_gps") == True)  # noqa: E712
        silver_clean.write.format("delta").mode("append").save(cfg["data"]["silver_path"])

    query = (
        parsed.writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", f"{cfg['data']['checkpoint_root']}/bronze_silver")
        .start()
    )
    query.awaitTermination()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="configs/pipeline.yaml")
    args = parser.parse_args()
    run(args.config)


if __name__ == "__main__":
    main()
