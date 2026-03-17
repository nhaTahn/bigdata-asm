# Architecture

## Overview
This MVP uses a local-first lakehouse pipeline:
1. Historical raw files under `data/raw/`
2. Replay producer publishes JSON events to Kafka in event-time order
3. Spark streaming ingests Kafka into Bronze Delta
4. Spark Silver transforms normalize + validate + enrich trajectories
5. Gold aggregations compute trip and emission analytics

## Bronze / Silver / Gold
- Bronze: immutable raw records + payload metadata
- Silver: normalized trajectory events with quality flags and segment features
- Gold: analytical aggregates (`trip_summary`, `route_hourly_emissions`, `bus_daily_emissions`)

## Core Rules
- Ordering by `bus_id` + `event_time`
- Haversine segment distance
- Segment duration from previous event timestamp
- Trip segmentation by configurable gap and route changes
- Dedup and outlier flags are explicit, not silent drops

## Emission Model (Estimated)
Inputs:
- `fuel_liter_per_km`
- `idle_liter_per_minute`
- `co2_kg_per_liter_fuel`

Equations:
- `fuel_liter_est = distance_km * fuel_liter_per_km + idle_min * idle_liter_per_minute`
- `co2_kg_est = fuel_liter_est * co2_kg_per_liter_fuel`

## Data Quality Flags
At minimum:
- invalid timestamp
- invalid coordinate range
- duplicate point
- non-positive duration
- high speed outlier
- abnormal jump distance

## Truths Preserved
- Source data is historical, not true real-time.
- Streaming is simulated via replay producer.
- Emissions are estimated unless direct sensor columns exist.
- GPS-only estimation has modeling uncertainty.
