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
- Podman Desktop (macOS) or Docker (with compose compatible CLI)
- Python 3.9+ (recommended: same version as `.venv` in this repo)
- `make`

## 4. Setup
```bash
cp .env.example .env
make setup
```

## 5. Data Placement
Put dataset files into `data/raw/` first.

Supported raw file types:
- `.csv`
- `.json` (json-lines)
- `.parquet`

## 6. Inspect Dataset
Run inspection:
```bash
make dataset-inspect
```

Output file:
- `docs/dataset_inspection.json`

## 7. End-to-End (Recommended First Run: Sample Mode)
Open 3 terminals in repo root.

Terminal A: start stream processor (Bronze/Silver), keep this running.
```bash
make create-topics
make bronze-silver
```

Terminal B: replay historical data to Kafka.
```bash
REPLAY_MODE=sample REPLAY_SAMPLE_SIZE=10000 make replay
```

Terminal C: after replay completes, run Gold and report/dashboard.
```bash
make gold
make report-assets
```

Open dashboard:
```bash
open docs/dashboard/index.html
```

Important:
- `make bronze-silver` is a streaming job and does not exit by itself.
- Consider the pipeline "done" when:
1. Replay finishes (`Replay completed ...`)
2. `make gold` succeeds
3. `make report-assets` generates files under `docs/analysis` and `docs/dashboard`

## 8. End-to-End (Full Mode)
Full mode reads entire `data/raw` and can take a long time before first publish log.

```bash
# Terminal A
make create-topics
make bronze-silver

# Terminal B
REPLAY_MODE=full make replay

# Terminal C (after replay completed)
make gold
make report-assets
```

If full mode looks "stuck", verify replay process is still alive:
```bash
ps aux | rg replay_producer
```

## 9. Runtime Config
Configured in `configs/pipeline.yaml` and override-able by `.env`:
- `REPLAY_MODE=sample|full`
- `REPLAY_SAMPLE_SIZE=10000`
- `REPLAY_SPEED_MULTIPLIER=10.0`
- `VEHICLE_ROUTE_MAPPING_PATH=./data/vehicle_route_mapping.csv` (optional route enrichment by vehicle)
- `data/route_catalog_table2.csv` (optional route metadata from PDF Table 2, used to enrich route report/dashboard)

## 10. Gold Outputs
- `trip_summary`
- `route_hourly_emissions`
- `bus_daily_emissions`

Written to:
- `data/lakehouse/gold/trip_summary`
- `data/lakehouse/gold/route_hourly_emissions`
- `data/lakehouse/gold/bus_daily_emissions`

## 11. Emission Formulas
Config location:
- `configs/pipeline.yaml` (`emission` block)
- `configs/emission.yaml`

Formula:
- `fuel_liter_est = distance_km * fuel_liter_per_km + idle_minutes * idle_liter_per_minute`
- `co2_kg_est = fuel_liter_est * co2_kg_per_liter_fuel`

## 12. Report Insights + Dashboard
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

## 13. How To Check Stream Is Healthy
Check process:
```bash
ps aux | rg bronze_silver_job.py
```

Check logs:
```bash
tail -f logs/bronze-silver.log
```

Check checkpoint growth:
```bash
find data/checkpoints/bronze_silver/offsets -type f | wc -l
```

Check Silver Delta commits:
```bash
ls data/lakehouse/silver/bus_gps/_delta_log | tail
```

## 14. Troubleshooting
1. `service "kafka" is not running` / `service "spark-master" is not running`
`make create-topics` first, then run `make bronze-silver`.

2. `NoBrokersAvailable` during replay
Kafka is not up or topic not created. Run:
```bash
make create-topics
```

3. Bronze/Silver crashes with offset reset error
If topic was deleted/recreated while old checkpoint exists, reset stream state:
```bash
make bronze-silver-stop
make reset-stream
make create-topics
```
Then start `make bronze-silver` again.

4. Full replay seems frozen
It is usually loading/normalizing very large raw files. Check CPU and replay process before stopping.

## 15. Tests
```bash
make test
```

Current tests cover:
- Haversine helper
- Trip gap logic
- Schema normalization helper
- Emission helper

## 16. Known Limitations
- Current repo was initialized before real raw files were provided in `data/raw/`.
- Column mapping is candidate-based and should be adjusted after real inspection output.
- CO2 is estimated from assumptions and config coefficients.
- Historical replay simulates streaming behavior and timing.

## 17. Stop Services
```bash
make down
```
