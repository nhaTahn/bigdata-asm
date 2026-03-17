from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from spark.common.geo import haversine_m


haversine_udf = F.udf(haversine_m, DoubleType())


def normalize_bronze(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("event_time", F.to_timestamp("event_time"))
        .withColumn("ingest_time", F.to_timestamp("ingest_time"))
        .withColumn("latitude", F.col("latitude").cast("double"))
        .withColumn("longitude", F.col("longitude").cast("double"))
        .withColumn("speed_kmh", F.col("speed_kmh").cast("double"))
        .withColumn("heading", F.col("heading").cast("double"))
    )


def enrich_segments(df: DataFrame, trip_gap_minutes: int, max_speed_kmh: float, max_jump_meters: float) -> DataFrame:
    w = Window.partitionBy("bus_id").orderBy("event_time")

    staged = (
        df.withColumn("prev_event_time", F.lag("event_time").over(w))
        .withColumn("prev_latitude", F.lag("latitude").over(w))
        .withColumn("prev_longitude", F.lag("longitude").over(w))
        .withColumn("segment_duration_s", F.col("event_time").cast("long") - F.col("prev_event_time").cast("long"))
        .withColumn(
            "segment_distance_m",
            haversine_udf("prev_latitude", "prev_longitude", "latitude", "longitude"),
        )
        .withColumn(
            "derived_speed_kmh",
            F.when(F.col("segment_duration_s") > 0, (F.col("segment_distance_m") / F.col("segment_duration_s")) * 3.6).otherwise(None),
        )
        .withColumn(
            "is_duplicate",
            (
                (F.col("event_time") == F.col("prev_event_time"))
                & (F.col("latitude") == F.col("prev_latitude"))
                & (F.col("longitude") == F.col("prev_longitude"))
            ).cast("boolean"),
        )
        .withColumn(
            "is_valid_gps",
            (
                F.col("latitude").between(-90.0, 90.0)
                & F.col("longitude").between(-180.0, 180.0)
                & F.col("event_time").isNotNull()
            ).cast("boolean"),
        )
        .withColumn(
            "is_outlier",
            (
                (F.col("segment_duration_s") <= 0)
                | (F.col("segment_distance_m") > F.lit(max_jump_meters))
                | (F.col("derived_speed_kmh") > F.lit(max_speed_kmh))
            ).cast("boolean"),
        )
    )

    is_new_trip = (
        F.col("prev_event_time").isNull()
        | (F.unix_timestamp("event_time") - F.unix_timestamp("prev_event_time") > trip_gap_minutes * 60)
        | (F.col("route_id") != F.lag("route_id").over(w))
    )

    return staged.withColumn("trip_seq", F.sum(F.when(is_new_trip, 1).otherwise(0)).over(w)).withColumn(
        "trip_id", F.concat_ws("_", F.col("bus_id"), F.col("trip_seq").cast("string"))
    )
