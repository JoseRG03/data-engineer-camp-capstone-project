# Brooklyn Citi Bike Real-Time Data Pipeline

A real-time and batch data engineering pipeline that ingests, transforms, and serves NYC Citi Bike station availability data, enabling operational insights into bike-share utilization across Brooklyn.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                          STREAMING PATH                              │
│                                                                       │
│  GBFS REST API  ──►  Python Producer  ──►  Confluent Kafka           │
│  (Citi Bike)          (Docker / ECS)        (Confluent Cloud)        │
│                                                    │                 │
│                                                    ▼                 │
│                                          Apache Flink SQL            │
│                                          (Stream Transformations)    │
│                                                    │                 │
│                                                    ▼                 │
│                                          Confluent Cloud Tables      │
│                                          (station_current_status)    │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                           BATCH PATH                                 │
│                                                                       │
│  PostgreSQL DB  ──►  Airbyte Cloud  ──►  Snowflake (RAW)            │
│                       (EL / Full Extract)                             │
│                                                    │                 │
│                                                    ▼                 │
│                                          dbt (Transformations)       │
│                                          (Staging → Marts)           │
│                                                    │                 │
│                                                    ▼                 │
│                                          Dagster (Orchestration)     │
│                                          (Schedules & Dependencies)  │
└─────────────────────────────────────────────────────────────────────┘
```

## Use Case

This pipeline answers questions like:
- Which Citi Bike stations are running low on bikes right now?
- What are the peak usage hours per station/region?
- How does bike availability change over 5-minute intervals across Brooklyn?

**Data consumers:** Operations teams, commuters, urban planners, and product managers who need real-time and historical bike-share availability insights.

## Tech Stack

| Component | Technology |
|---|---|
| Streaming Source | [GBFS](https://gbfs.lyft.com/gbfs/2.3/bkn/en/) (Lyft / Citi Bike) |
| Message Broker | Confluent Cloud (Kafka) |
| Stream Producer | Python + `confluent-kafka` |
| Stream Transformations | Apache Flink SQL (Confluent Cloud) |
| Batch Integration | Airbyte Cloud |
| Data Warehouse | Snowflake |
| Batch Transformations | dbt (dbt-snowflake) |
| Orchestration | Dagster |
| Containerisation | Docker |
| CI | GitHub Actions |
| Linting | Ruff |

## Project Structure

```
├── streams/
│   ├── event-producer/          # Kafka producer (Docker-ready)
│   │   ├── producer.py          # Live station status → Kafka topic
│   │   ├── station_data.py      # One-time station info seed producer
│   │   └── Dockerfile
│   └── transformations/         # Flink SQL transformation scripts
│       ├── 0.1_station_information.sql
│       ├── 0.2_station_current_status.sql
│       ├── 1_insert_current_station_status.sql
│       └── 2_bike_availability_tumbling_window.sql
│
├── batch/
│   ├── orchestrate/             # Dagster project
│   │   └── analytics/
│   │       ├── assets/          # dbt + Airbyte asset definitions
│   │       └── resources/       # PostgreSQL resource
│   └── transform/               # dbt project (Snowflake)
│       └── models/
│
├── .github/
│   └── workflows/
│       └── python_ci.yml        # CI: linting on pull requests
│
└── README.md
```

## Streaming Pipeline

### How it works

1. `producer.py` polls the Citi Bike GBFS station-status endpoint every 30 seconds.
2. Only stations whose status has **changed** (bikes available, docking capacity, operational flags) are published to the Kafka topic — reducing noise and downstream load.
3. A separate `station_data.py` seeds the `station_information` topic once with static station metadata (name, lat/lon, capacity).
4. Flink SQL joins the two streams and materialises a `station_current_status` table with enriched, typed records.
5. A tumbling-window aggregation rolls up per-station availability into 5-minute summaries.

### Running the producer locally

**Prerequisites:** `client.properties` (Confluent Cloud credentials) and `.env` file.

```bash
# Build the image
docker build -t event-producer streams/event-producer/

# Run with credentials mounted
docker run --rm \
  --env-file streams/event-producer/.env \
  -v "${PWD}/streams/event-producer/client.properties:/app/client.properties:ro" \
  -v "${PWD}/streams/event-producer/logs:/app/logs" \
  event-producer
```

**Environment variables (`.env`):**

| Variable | Description |
|---|---|
| `STATION_STATUS_URL` | GBFS station-status feed URL |
| `KAFKA_TOPIC` | Target Kafka topic name |
| `POLL_INTERVAL_SECONDS` | Polling frequency (default: 30) |

### Running the station info seed

```bash
cd streams/event-producer
pip install -r ../../requirements.txt
python station_data.py
```

## Batch Pipeline

### Airbyte → Snowflake

Airbyte Cloud is configured to extract from a PostgreSQL source into Snowflake (`STAGING` database, `TRIP_DATA` schema) on a daily schedule (07:00 UTC).

### dbt Transformations

```bash
cd batch/transform
pip install dbt-snowflake
dbt deps
dbt run
dbt test
```

### Dagster Orchestration

```bash
cd batch/orchestrate
pip install -e ".[dev]"
dagster dev
```

Navigate to `http://localhost:3000` to view the asset graph, materialise assets, and inspect run history.

## CI Pipeline

GitHub Actions runs on every pull request targeting `main`:

- **Trigger:** Changes to `streams/event-producer/**/*.py` or `batch/orchestrate/**/*.py`
- **Steps:** Checkout → Install dependencies → Run `ruff` linter

```bash
# Run linting locally
pip install ruff
ruff check streams/event-producer batch/
```

## Data Quality

dbt data tests are defined in `batch/transform/models/example/schema.yml`:

- `unique` — no duplicate primary keys
- `not_null` — required fields are populated
- `accepted_values` — status fields contain only valid values

```bash
cd batch/transform
dbt test
```

## Cloud Services

| Service | Provider | Purpose |
|---|---|---|
| Kafka Broker | Confluent Cloud | Message queue for station events |
| Stream Processing | Confluent Cloud (Flink) | Real-time SQL transformations |
| Data Warehouse | Snowflake | Storage and batch transformations |
| Data Integration | Airbyte Cloud | Extract & load from PostgreSQL |
| Container Registry | AWS ECR | Docker image hosting |
| Container Runtime | AWS ECS | Running the Kafka producer |

## Dataset

- **Source:** [Lyft GBFS](https://gbfs.lyft.com/gbfs/2.3/bkn/en/) — Brooklyn Citi Bike real-time station feed
- **Update frequency:** Every 30 seconds (station status), static (station information)
- **Coverage:** ~400+ Citi Bike stations across Brooklyn, NY
- **Key fields:** `num_bikes_available`, `num_docks_available`, `is_renting`, `is_returning`, `station_id`, `lat`, `lon`
