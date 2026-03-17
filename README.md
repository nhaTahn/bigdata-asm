# bigdata-asm

Local runnable MVP for ingesting historical HCMC bus GPS data as simulated streaming input, then building Bronze/Silver/Gold outputs with estimated fuel and CO2 metrics.

## 1. Purpose
- Historical dataset replayed in event-time order (not live stream).
- Kafka ingestion + Spark Structured Streaming processing.
- Delta Lake outputs for Bronze/Silver/Gold.
- Emissions are **estimated**, not sensor-measured ground truth.

## 2. Repo Structure
```text
.
├── AGENT.md
├── README.md
├── .env.example
├── docker-compose.yml
├── Makefile
├── configs/
├── data/
│   └── raw/
├── docs/
├── producer/
├── scripts/
├── spark/
│   ├── common/
│   └── jobs/
└── tests/
```

## 3. Prerequisites
- Docker + Docker Compose
- Python 3.10+

## 4. Setup
```bash
cp .env.example .env
make setup
make up
```

## 5. Inspect Dataset (Phase 1)
Put dataset files into `data/raw/` first.

Supported raw file types:
- `.csv`
- `.json` (json-lines)
- `.parquet`

Run inspection:
```bash
make dataset-inspect
```

Output file:
- `docs/dataset_inspection.json`

## 6. Start End-to-End Flow
Create Kafka topic:
```bash
make create-topics
```

Run Bronze + Silver streaming job in background:
```bash
make bronze-silver-bg
```

Check stream logs:
```bash
make bronze-silver-logs
```

Run replay producer:
```bash
make replay
```

Run Gold aggregation job:
```bash
make gold
```

## 7. Sample Mode / Full Mode
Configured in `configs/pipeline.yaml` and override-able by `.env`:
- `REPLAY_MODE=sample|full`
- `REPLAY_SAMPLE_SIZE=10000`
- `REPLAY_SPEED_MULTIPLIER=10.0`
- `VEHICLE_ROUTE_MAPPING_PATH=./data/vehicle_route_mapping.csv` (optional route enrichment by vehicle)

## 8. Gold Outputs
- `trip_summary`
- `route_hourly_emissions`
- `bus_daily_emissions`

Written to:
- `data/lakehouse/gold/trip_summary`
- `data/lakehouse/gold/route_hourly_emissions`
- `data/lakehouse/gold/bus_daily_emissions`

## 9. Emission Formulas
Config location:
- `configs/pipeline.yaml` (`emission` block)
- `configs/emission.yaml`

Formula:
- `fuel_liter_est = distance_km * fuel_liter_per_km + idle_minutes * idle_liter_per_minute`
- `co2_kg_est = fuel_liter_est * co2_kg_per_liter_fuel`

## 10. Report Insights + Dashboard
Generate analysis artifacts and dashboard from Gold:
```bash
make report-assets
```

Generated outputs:
- `docs/analysis/insight_summary.md`
- `docs/analysis/kpi_summary.json`
- `docs/analysis/top_buses_by_co2.csv`
- `docs/analysis/top_routes_by_co2.csv`
- `docs/analysis/peak_hours_by_co2.csv`
- `docs/analysis/daily_totals.csv`
- `docs/analysis/trip_outliers.csv`
- `docs/dashboard/index.html`

Open dashboard in browser:
```bash
open docs/dashboard/index.html
```

## 11. Tests
```bash
make test
```

Current tests cover:
- Haversine helper
- Trip gap logic
- Schema normalization helper
- Emission helper

## 12. Known Limitations
- Current repo was initialized before real raw files were provided in `data/raw/`.
- Column mapping is candidate-based and should be adjusted after real inspection output.
- CO2 is estimated from assumptions and config coefficients.
- Historical replay simulates streaming behavior and timing.

## 13. Stop Services
```bash
make down
```
