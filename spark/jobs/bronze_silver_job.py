from __future__ import annotations

import argparse
import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from spark.common.config import load_config
from spark.common.transform import enrich_segments, normalize_bronze


logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


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
    spark.sparkContext.setLogLevel("WARN")
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", cfg["kafka"]["bootstrap_servers"])

    starting_offsets = os.getenv("KAFKA_STARTING_OFFSETS", cfg["kafka"].get("starting_offsets", "earliest"))
    max_offsets_per_trigger = os.getenv(
        "KAFKA_MAX_OFFSETS_PER_TRIGGER",
        str(cfg["kafka"].get("max_offsets_per_trigger", 2000)),
    )

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", cfg["kafka"]["topic_raw_gps"])
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", max_offsets_per_trigger)
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
            logger.info("bronze-silver batch=%s empty", batch_id)
            return

        row_count = batch_df.count()
        logger.info("bronze-silver batch=%s rows=%s writing Bronze/Silver", batch_id, row_count)
        batch_df.write.format("delta").mode("append").save(cfg["data"]["bronze_path"])

        silver_input = normalize_bronze(batch_df)
        silver_enriched = enrich_segments(
            silver_input,
            trip_gap_minutes=int(cfg["quality"]["trip_gap_minutes"]),
            max_speed_kmh=float(cfg["quality"]["max_speed_kmh"]),
            max_jump_meters=float(cfg["quality"]["max_jump_meters"]),
        )
        silver_clean = silver_enriched.filter(F.col("is_valid_gps") == True)  # noqa: E712
        valid_count = silver_clean.count()
        silver_clean.write.format("delta").mode("append").save(cfg["data"]["silver_path"])
        logger.info("bronze-silver batch=%s valid_rows=%s write_done", batch_id, valid_count)

    query = (
        parsed.writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", f"{cfg['data']['checkpoint_root']}/bronze_silver")
        .start()
    )
    logger.info("bronze-silver stream started")

    while query.isActive:
        query.awaitTermination(30)
        progress = query.lastProgress
        if progress:
            logger.info(
                "bronze-silver progress batch=%s input_rows=%s processed_rps=%s",
                progress.get("batchId"),
                progress.get("numInputRows"),
                progress.get("processedRowsPerSecond"),
            )
        else:
            logger.info("bronze-silver waiting for data")

    if query.exception() is not None:
        raise query.exception()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="configs/pipeline.yaml")
    args = parser.parse_args()
    run(args.config)


if __name__ == "__main__":
    main()
