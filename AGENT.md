# AGENT.md

## 1. Project Overview

This repository is a **local runnable MVP** for a big-data pipeline that ingests and preprocesses **historical HCMC bus GPS data** as a **simulated stream**, then reconstructs journeys and estimates **fuel consumption** and **CO2 emissions**.

The original source dataset is historical, not a live feed.  
Therefore, this project must **replay historical records in event-time order** to emulate streaming ingestion.

This project is intended for:
- academic coursework
- technical demonstration
- local development and testing
- architecture prototyping for large-scale GPS telemetry analytics

---

## 2. Core Objective

Build a complete local pipeline that can:

1. Ingest large volumes of bus GPS records from files under `data/raw/`
2. Replay them as a pseudo-stream into Kafka
3. Persist raw data into a centralized storage layer
4. Clean and standardize the GPS stream
5. Reconstruct trip segments and journeys
6. Estimate fuel usage and CO2 emissions
7. Produce analytics-ready outputs for downstream dashboards or reports

---

## 3. Business Goals

### 3.1 Centralized Data Storage
The system must provide a centralized way to ingest, store, and reprocess raw and transformed GPS data.

### 3.2 Big Data Analytics
The system must process the data at scale and support near real-time analytics for:
- movement tracking
- trip reconstruction
- distance estimation
- idling estimation
- fuel estimation
- CO2 estimation

---

## 4. Project Constraints

### Mandatory constraints
- Must run **locally**
- Must use **Docker Compose**
- Must not require cloud-managed services
- Must not require Kubernetes
- Must use the dataset stored under `data/raw/`
- Must inspect the **actual dataset files first** before hardcoding schema assumptions
- Must keep the repository **simple, runnable, and well-documented**

### Preferred technology stack
- Python
- Kafka
- Spark Structured Streaming
- MinIO
- Delta Lake
- Optional lightweight orchestration only if it clearly adds value

### Non-goals
Do **not**:
- build a cloud-only solution
- introduce unnecessary microservices
- over-engineer the MVP
- optimize prematurely for enterprise production
- add tools that increase complexity without directly helping local execution

---

## 5. Architecture Principles

The architecture should follow a **Bronze / Silver / Gold** lakehouse pattern.

### Bronze
Raw immutable ingested events.

### Silver
Cleaned, normalized, validated, enriched trajectory records.

### Gold
Analytics-ready aggregates such as:
- trip summaries
- hourly route emissions
- daily bus emissions

### Streaming model
The source is historical, so the pipeline must:
1. read historical files
2. sort records by event time
3. replay them into Kafka
4. process them using streaming jobs

This is a **real-time capable architecture using simulated streaming input**.

---

## 6. Expected Repository Scope

The repository should include at least:

- `docker-compose.yml`
- `README.md`
- `.env.example`
- `Makefile` or runnable shell scripts
- `configs/`
- `data/raw/`
- `producer/`
- `spark/common/`
- `spark/jobs/`
- `scripts/`
- `tests/`
- `docs/architecture.md`

It is acceptable to add more files if they directly support the objective.

---

## 7. Required Functional Capabilities

### 7.1 Dataset inspection
Before implementing business logic, inspect files under `data/raw/` and summarize:
- file types
- column names
- candidate timestamp fields
- identifier fields
- route fields
- GPS fields
- likely data quality issues
- schema inconsistencies across files

### 7.2 Replay producer
Implement a replay producer that:
- reads the original dataset from `data/raw/`
- normalizes timestamps
- orders records by event time
- publishes rows to Kafka as JSON events
- supports:
  - sample mode
  - full mode
- allows configurable replay speed

### 7.3 Bronze layer
Store raw Kafka events as Bronze data in Delta format.

Bronze requirements:
- preserve raw payload
- preserve ingestion metadata
- support replay/reprocessing
- partition sensibly if needed

### 7.4 Silver layer
Silver must perform:
- schema normalization
- type casting
- timestamp standardization
- coordinate validation
- duplicate handling
- outlier detection or flagging
- previous-point enrichment
- segment distance computation
- segment duration computation
- trip segmentation using configurable time-gap logic

### 7.5 Gold layer
Gold must output at least:
- `trip_summary`
- `route_hourly_emissions`
- `bus_daily_emissions`

### 7.6 Emission estimation
Emission logic must be:
- configurable
- documented
- separated from ingestion logic

Minimum supported models:
1. distance-based estimation
2. optional idle-time penalty

---

## 8. Data Quality Expectations

The dataset may contain:
- irregular sampling intervals
- missing values
- duplicate records
- invalid timestamps
- GPS anomalies
- route deviations
- data drift across files

The pipeline must handle these gracefully.

At minimum, include validation or flags for:
- null timestamps
- null coordinates
- invalid latitude / longitude ranges
- duplicate events
- unrealistic movement jumps
- zero/negative duration between sequential points
- extremely high derived speed

Do not silently discard large classes of records without documenting the rule.

---

## 9. Normalized Schema Expectations

The final normalized schema should be derived from actual files, but the agent should aim to produce a cleaned schema similar to:

- `event_time`
- `bus_id`
- `route_id`
- `latitude`
- `longitude`
- `speed_kmh` (if available)
- `heading` (if available)
- `ingest_time`
- `source_file`
- `trip_id`
- `prev_event_time`
- `prev_latitude`
- `prev_longitude`
- `segment_distance_m`
- `segment_duration_s`
- `is_duplicate`
- `is_valid_gps`
- `is_outlier`

Do not assume these exact names exist in raw files.  
Map raw columns into a normalized schema after inspection.

---

## 10. Core Processing Rules

### 10.1 Ordering
Trajectory logic must operate per vehicle ordered by event time.

### 10.2 Distance
Use the **Haversine formula** to compute segment distance between consecutive GPS points.

### 10.3 Duration
Compute segment duration from consecutive event timestamps.

### 10.4 Trip segmentation
A new trip may begin when:
- route changes
- time gap exceeds a configurable threshold
- other reasonable heuristics derived from actual data indicate a new journey

Trip segmentation must be configurable, not hardcoded.

### 10.5 Deduplication
Deduplicate using a documented rule, for example:
- same bus_id
- same timestamp
- same coordinates

### 10.6 Outlier handling
Outlier records should be:
- flagged, or
- removed with clear documentation

Never hide the rule.

---

## 11. Emission Estimation Requirements

The project does **not** directly measure fuel from sensors unless such fields actually exist in the dataset.

Therefore, emission outputs must be framed as **estimated emissions**.

### Minimum model
Estimated fuel is derived from:
- travel distance
- optionally idle duration

Estimated CO2 is derived from:
- estimated fuel
- configurable emission factor

### Requirements
- all coefficients must live in config files
- formulas must be documented in `README.md` or `docs/architecture.md`
- business logic must not be deeply hardcoded inside unrelated modules

Example parameters:
- `fuel_liter_per_km`
- `idle_liter_per_minute`
- `co2_kg_per_liter_fuel`

These are examples only; use config-driven naming consistently.

---

## 12. Local-First Engineering Rules

This is an MVP for local execution.

Optimize for:
- simplicity
- repeatability
- clarity
- deterministic commands
- easy debugging

Avoid:
- brittle startup steps
- hidden dependencies
- manual edits in too many files
- absolute machine-specific paths

Use environment variables and config files instead.

---

## 13. Deliverables

The agent must produce:

1. Runnable repository structure
2. Source code for replay and processing
3. Docker Compose setup
4. README with exact run commands
5. Config files
6. Sample mode and full mode
7. Architecture document
8. At least a few tests for core utility logic

---

## 14. Working Style Required

Work incrementally in phases.  
At the end of each phase, provide:

- what was done
- files created or changed
- commands to run
- assumptions made
- known limitations

Do not stop at high-level design.  
Create actual files and runnable code.

If the repo becomes partially runnable in an early phase, preserve that runnable state.

---

## 15. Phase Plan

### Phase 1 — Inspect dataset
Tasks:
- inspect files under `data/raw/`
- summarize real schema candidates
- identify timestamp, ID, route, coordinate, and optional speed columns
- identify data quality risks
- propose normalized schema

Output:
- dataset inspection summary
- schema proposal
- documented assumptions

### Phase 2 — Scaffold repo
Tasks:
- create folder structure
- create `docker-compose.yml`
- create `.env.example`
- create config structure
- create `README.md` skeleton
- create base Python package structure

Output:
- coherent repository scaffold
- local services bootable or nearly bootable

### Phase 3 — Implement replay producer
Tasks:
- read dataset files
- normalize timestamp field(s)
- sort by event time
- emit Kafka JSON events
- add sample mode
- add logging and config

Output:
- runnable replay producer

### Phase 4 — Implement Bronze + Silver
Tasks:
- Kafka consumer stream in Spark
- write Bronze Delta table
- normalize schema
- validate GPS
- deduplicate
- compute lag features
- compute Haversine distance
- compute segment duration
- assign trip IDs

Output:
- runnable Bronze/Silver pipeline

### Phase 5 — Implement Gold
Tasks:
- aggregate trip summaries
- compute route hourly emissions
- compute bus daily emissions
- parameterize emission logic

Output:
- Gold analytical outputs

### Phase 6 — Finalize docs and usability
Tasks:
- finalize README
- add run scripts or Makefile
- add tests
- add architecture document
- verify sample run path from zero

Output:
- local runnable MVP with clear docs

---

## 16. Code Quality Standards

### General
- prefer readable code over clever code
- use type hints where practical
- keep functions focused
- separate config from business logic
- add comments only where they clarify non-obvious logic

### Logging
- add useful logging
- log startup parameters
- log record counts where relevant
- log warnings for dropped/flagged records
- do not flood logs unnecessarily

### Error handling
- fail loudly on critical configuration issues
- handle bad records explicitly
- document assumptions and fallbacks

### Testing
Add at least lightweight tests for:
- Haversine calculation
- trip segmentation logic
- schema normalization helpers
- emission estimation helpers if implemented as pure functions

---

## 17. File and Module Expectations

Suggested high-level structure:

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
│   └── architecture.md
├── producer/
├── scripts/
├── spark/
│   ├── common/
│   └── jobs/
└── tests/

18. Configuration Rules

All configurable settings should live in config or environment variables, for example:

Kafka bootstrap server

Kafka topic names

source data path

sample mode toggle

replay speed

Bronze/Silver/Gold output paths

trip gap threshold

coordinate validation thresholds

outlier thresholds

fuel/emission factors

Do not bury operational constants deep inside code.

19. Readme Requirements

The README.md must include:

project purpose

architecture summary

prerequisites

local setup

how to start services

how to create topics if needed

how to run producer

how to run Spark jobs

how to inspect outputs

sample mode instructions

known limitations

future improvements

Commands should be copy-paste friendly.

20. Definition of Done

The repository is considered done when:

services can be started locally

the dataset can be replayed into Kafka

Bronze data is written

Silver data is written with cleaned/enriched GPS records

Gold outputs are generated

CO2 estimation is configurable and documented

README explains how to run the end-to-end flow

at least sample mode works from scratch

21. Known Modeling Limitations to Preserve in Documentation

The agent must preserve the following truth in docs:

The dataset is historical, not a live stream

Streaming is simulated via replay

Emissions are estimated, not directly measured, unless sensor data actually exists

Accuracy depends on available fields and chosen assumptions

GPS-only trajectories may not perfectly capture true vehicle energy consumption

Do not present estimated CO2 as ground-truth CO2.

22. Prioritization Rules

When tradeoffs appear, prioritize in this order:

correctness of data flow

local runnability

simplicity

documentation clarity

extensibility

performance optimization

For the first working version, prefer a smaller complete pipeline over a larger unfinished design.

23. Agent Behavior Rules

You must:

inspect actual files before assuming schema

explain assumptions explicitly

keep the repository runnable at each major stage

create real code, not just pseudocode

avoid unnecessary tools and complexity

document every non-trivial processing rule

You must not:

invent schema fields without inspection

hardcode machine-specific absolute paths

introduce cloud dependencies without need

stop after design without implementing files

24. First-Step Instruction

When starting work in this repository, do the following in order:

inspect data/raw/

summarize actual dataset characteristics

propose normalized schema

show the folder tree you plan to create

show the phase implementation plan

start creating files