from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from spark.common.config import load_config


def _build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("gold-job")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def run(config_path: str) -> None:
    cfg = load_config(config_path)
    spark = _build_spark()

    silver_path = Path(cfg["data"]["silver_path"])
    delta_log = silver_path / "_delta_log"
    has_commits = delta_log.exists() and any(delta_log.glob("*.json"))
    if not has_commits:
        raise RuntimeError(
            f"Silver Delta table not found at {silver_path}. "
            "Run bronze-silver first, then replay data, and wait for Silver writes."
        )

    silver_df = spark.read.format("delta").load(cfg["data"]["silver_path"])

    mapping_path = cfg["data"].get("vehicle_route_mapping_path")
    if mapping_path and Path(mapping_path).exists():
        mapping_df = (
            spark.read.option("header", True).csv(mapping_path)
            .select(
                F.col("vehicle").alias("map_vehicle"),
                F.coalesce(F.col("route_no"), F.col("route_id").cast("string")).alias("mapped_route_id"),
            )
            .dropna(subset=["map_vehicle", "mapped_route_id"])
            .dropDuplicates(["map_vehicle"])
        )
        silver_df = (
            silver_df.join(mapping_df, silver_df["bus_id"] == mapping_df["map_vehicle"], "left")
            .withColumn(
                "route_id",
                F.coalesce(
                    F.when(F.trim(F.col("route_id")) == "", None).otherwise(F.col("route_id").cast("string")),
                    F.col("mapped_route_id"),
                ),
            )
            .drop("map_vehicle", "mapped_route_id")
        )

    fuel_per_km = float(cfg["emission"]["fuel_liter_per_km"])
    idle_per_min = float(cfg["emission"]["idle_liter_per_minute"])
    co2_per_liter = float(cfg["emission"]["co2_kg_per_liter_fuel"])
    report_timezone = "Asia/Ho_Chi_Minh"

    enriched = (
        silver_df.withColumn("distance_km", F.coalesce(F.col("segment_distance_m"), F.lit(0.0)) / 1000.0)
        .withColumn("idle_min", F.when(F.coalesce(F.col("speed_kmh"), F.lit(0.0)) <= 1.0, F.col("segment_duration_s") / 60.0).otherwise(F.lit(0.0)))
        .withColumn("fuel_liter_est", F.col("distance_km") * F.lit(fuel_per_km) + F.col("idle_min") * F.lit(idle_per_min))
        .withColumn("co2_kg_est", F.col("fuel_liter_est") * F.lit(co2_per_liter))
        .withColumn("event_time_local", F.from_utc_timestamp("event_time", report_timezone))
        .withColumn("event_date", F.to_date("event_time_local"))
        .withColumn("event_hour", F.hour("event_time_local"))
    )

    trip_summary = (
        enriched.groupBy("trip_id", "bus_id", "route_id")
        .agg(
            F.min("event_time_local").alias("trip_start"),
            F.max("event_time_local").alias("trip_end"),
            F.sum("distance_km").alias("distance_km"),
            F.sum("fuel_liter_est").alias("fuel_liter_est"),
            F.sum("co2_kg_est").alias("co2_kg_est"),
        )
        .orderBy("trip_start")
    )

    route_hourly = (
        enriched.groupBy("route_id", "event_date", "event_hour")
        .agg(
            F.sum("distance_km").alias("distance_km"),
            F.sum("fuel_liter_est").alias("fuel_liter_est"),
            F.sum("co2_kg_est").alias("co2_kg_est"),
            F.countDistinct("bus_id").alias("active_buses"),
        )
        .orderBy("event_date", "event_hour")
    )

    bus_daily = (
        enriched.groupBy("bus_id", "event_date")
        .agg(
            F.sum("distance_km").alias("distance_km"),
            F.sum("fuel_liter_est").alias("fuel_liter_est"),
            F.sum("co2_kg_est").alias("co2_kg_est"),
        )
        .orderBy("event_date")
    )

    gold_root = cfg["data"]["gold_path"]
    trip_summary.write.format("delta").mode("overwrite").save(f"{gold_root}/trip_summary")
    route_hourly.write.format("delta").mode("overwrite").save(f"{gold_root}/route_hourly_emissions")
    bus_daily.write.format("delta").mode("overwrite").save(f"{gold_root}/bus_daily_emissions")

    spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="configs/pipeline.yaml")
    args = parser.parse_args()
    run(args.config)


if __name__ == "__main__":
    main()
