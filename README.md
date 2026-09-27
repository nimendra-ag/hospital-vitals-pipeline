# Hospital Patient Vital Signs Monitoring Pipeline

An end-to-end **Lambda architecture** data pipeline for near-real-time patient vitals monitoring, built for the Applied Big Data Engineering mini-project.

## Use Case

A hospital ward requires continuous monitoring of patient vital signs from bedside sensors, correlated daily with lab test results from pathology, to answer:

> **Which patients show concerning vital-sign trends right now, and how do yesterday's lab results change the risk picture for those patients going forward?**

## Architecture

**Lambda architecture** with separate speed and batch layers:

| Layer | Technology | Purpose |
|---|---|---|
| **Ingestion** | Apache Kafka | Durable, partitioned event ingestion |
| **Speed layer** | Spark Structured Streaming | Real-time windowed aggregation + alerting |
| **Batch layer** | Spark batch (via Airflow) | Daily vitals-labs reconciliation join |
| **Orchestration** | Apache Airflow | DAG-based batch pipeline scheduling |
| **Storage** | PostgreSQL (Neon) | Queryable relational store |
| **Serving** | FastAPI | REST API for ward monitoring + reports |
| **Observability** | structlog + metrics | Structured logging, health checks |

### Data Sources (Simulated)

- **Streaming**: Bedside monitors emitting vitals (HR, SpO2, BP, temperature) every 3 seconds for 12 patients via Kafka.
- **Batch**: Daily lab results file (WBC, hemoglobin, glucose, creatinine, CRP) dropped every simulated day.

### Simulated Time Compression

**1 simulated day = 5 minutes (300 seconds)**

- Vitals emit every 3 seconds (real time).
- Lab results file is generated every 5 minutes.
- Airflow DAG runs every 5 minutes.
- A 10-minute demo covers ~2 full daily cycles.

## Prerequisites

- Python 3.11+
- Docker & Docker Compose
- Java 11+ (for PySpark)
- A [Neon](https://neon.tech) PostgreSQL database

## Setup & Running

### 1. Clone and install dependencies

```bash
git clone <repo-url>
cd hospital-vitals-pipeline
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure environment

Copy `.env` and fill in your Neon connection string:

```bash
cp .env .env.local  # edit .env with your Neon credentials
```

### 3. Initialize the database

Run the schema against your Neon database:

```bash
python -c "from storage.db import init_schema; init_schema()"
```

Or connect via `psql` and run `storage/init.sql` directly.

### 4. Start Kafka & Zookeeper

```bash
docker-compose up -d
```

Wait for Kafka to be healthy:

```bash
docker-compose ps  # should show kafka and zookeeper as healthy
```

### 5. Create Kafka topics

```bash
python -m ingestion.kafka_config
```

### 6. Start the streaming producer

```bash
python -m data_sources.vitals_producer
```

### 7. Start the lab results generator (separate terminal)

```bash
python -m data_sources.lab_results_generator
```

### 8. Start the Spark stream processor (separate terminal)

```bash
python -m processing.speed_layer.vitals_stream_processor
```

> **Note**: On first run, PySpark will download the Kafka and PostgreSQL JDBC
> connector JARs. This may take a minute.

### 9. Start the FastAPI server (separate terminal)

```bash
python -m serving.api
```

The API is available at `http://localhost:8000`. Interactive docs at `/docs`.

### 10. Start Airflow (separate terminal)

```bash
export AIRFLOW_HOME=$(pwd)/airflow_home
airflow db init
airflow users create --role Admin --username admin --password admin \
    --email admin@example.com --firstname Admin --lastname User

# Copy the DAG
mkdir -p $AIRFLOW_HOME/dags
cp orchestration/dags/daily_pipeline_dag.py $AIRFLOW_HOME/dags/

# Start scheduler and webserver
airflow scheduler &
airflow webserver --port 8080 &
```

Access the Airflow UI at `http://localhost:8080`. Enable the
`daily_patient_risk_pipeline` DAG.

## API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| GET | `/api/ward/status` | Current ward overview with latest vitals per patient |
| GET | `/api/patients/{id}/vitals` | Recent vitals trend for one patient |
| GET | `/api/alerts/active` | All unacknowledged alerts |
| POST | `/api/alerts/{id}/acknowledge` | Acknowledge an alert |
| GET | `/api/reports/daily/{date}` | Daily risk report for a date |
| GET | `/api/reports/latest` | Most recent daily report |
| GET | `/api/health` | Pipeline health check + metrics |

## Project Structure

```
hospital-vitals-pipeline/
├── docker-compose.yml          # Kafka + Zookeeper
├── .env                        # Configuration
├── requirements.txt
├── storage/
│   ├── init.sql                # PostgreSQL schema
│   └── db.py                   # Connection pool + helpers
├── data_sources/
│   ├── vitals_producer.py      # Streaming: bedside monitors → Kafka
│   └── lab_results_generator.py # Batch: daily lab file generator
├── ingestion/
│   └── kafka_config.py         # Topic creation utility
├── processing/
│   ├── speed_layer/
│   │   └── vitals_stream_processor.py  # Spark Structured Streaming
│   └── batch_layer/
│       └── daily_risk_processor.py     # Daily risk reconciliation
├── serving/
│   ├── api.py                  # FastAPI endpoints
│   └── report_generator.py     # HTML/JSON report generator
├── orchestration/
│   └── dags/
│       └── daily_pipeline_dag.py # Airflow DAG
└── observability/
    ├── logging_config.py       # structlog configuration
    ├── metrics.py              # Pipeline metrics collection
    └── health_checks.py        # Health check rules + daemon
```

## Observability

### Structured Logging

All components use `structlog` with JSON output. Every log line includes:
- `component`: which pipeline stage emitted the log
- `timestamp`: ISO-8601 timestamp
- `level`: INFO / WARNING / ERROR

### Health Checks

- **Data staleness**: Alerts if no vitals received for a patient in 60 seconds.
- **Error rate**: Alerts if ingestion error rate exceeds 5%.
- Health check daemon runs in the API process every 30 seconds.
- Results exposed via `GET /api/health`.

## Assumptions & Simplifications

- Patient data is simulated (12 fixed patients).
- Risk scoring uses a simplified weighted model (not clinically validated).
- Single Kafka broker (no replication — suitable for demo only).
- PySpark runs in local mode (no standalone cluster).
- Airflow uses the default SQLite backend for simplicity.
