-- ============================================================
-- Hospital Vitals Pipeline — PostgreSQL Schema
-- Applied automatically by the `postgres` service in docker-compose.yml
-- (mounted into /docker-entrypoint-initdb.d) on first boot.
--
-- All TIMESTAMP columns hold naive UTC. Writers must never let a local
-- timezone leak in (the speed layer formats timestamps as UTC strings).
-- ============================================================

-- Patients reference table
CREATE TABLE IF NOT EXISTS patients (
    patient_id   VARCHAR(20) PRIMARY KEY,
    name         VARCHAR(100) NOT NULL,
    age          INT,
    ward         VARCHAR(50) DEFAULT 'General',
    bed_number   VARCHAR(10),
    admitted_at  TIMESTAMP DEFAULT NOW()
);

-- Real-time vitals aggregates (written by speed layer).
-- One row per patient per 30 s window; the speed layer UPSERTs on
-- (patient_id, window_start) because Spark's update output mode re-emits a
-- window every trigger while it is still open.
CREATE TABLE IF NOT EXISTS vitals_aggregates (
    id              SERIAL PRIMARY KEY,
    patient_id      VARCHAR(20) NOT NULL,
    window_start    TIMESTAMP NOT NULL,
    window_end      TIMESTAMP NOT NULL,
    avg_heart_rate  FLOAT,
    avg_spo2        FLOAT,
    avg_systolic_bp FLOAT,
    avg_diastolic_bp FLOAT,
    avg_temperature FLOAT,
    min_spo2        FLOAT,
    max_heart_rate  FLOAT,
    reading_count   INT,
    created_at      TIMESTAMP DEFAULT (NOW() AT TIME ZONE 'UTC'),
    CONSTRAINT uq_vitals_window UNIQUE (patient_id, window_start)
);

-- Lab results (ingested by batch layer): one daily report per patient, stored
-- as one row per test of the panel (WBC, Hemoglobin, Glucose, Creatinine, CRP)
CREATE TABLE IF NOT EXISTS lab_results (
    id               SERIAL PRIMARY KEY,
    patient_id       VARCHAR(20) NOT NULL,
    test_type        VARCHAR(50) NOT NULL,
    result_value     FLOAT NOT NULL,
    reference_min    FLOAT,
    reference_max    FLOAT,
    is_abnormal      BOOLEAN DEFAULT FALSE,
    collected_at     TIMESTAMP NOT NULL,
    simulated_day    INT NOT NULL,
    ingested_at      TIMESTAMP DEFAULT NOW(),
    -- One result per test per patient per day; also makes re-ingesting a
    -- day's file (Airflow retry / manual re-run) idempotent.
    CONSTRAINT uq_lab_result UNIQUE (patient_id, test_type, simulated_day)
);

-- Daily patient risk report (written by batch layer).
-- Keyed by SIMULATED day, not calendar date: with time compression many
-- simulated days fall on one real date and must not overwrite each other.
CREATE TABLE IF NOT EXISTS patient_risk (
    id                   SERIAL PRIMARY KEY,
    patient_id           VARCHAR(20) NOT NULL,
    report_date          DATE NOT NULL,
    simulated_day        INT NOT NULL,
    vitals_risk_score    FLOAT DEFAULT 0.0,
    lab_risk_score       FLOAT DEFAULT 0.0,
    combined_risk_score  FLOAT DEFAULT 0.0,
    risk_level           VARCHAR(20) DEFAULT 'LOW',
    abnormal_vitals_count INT DEFAULT 0,
    abnormal_labs_count  INT DEFAULT 0,
    risk_factors         TEXT,
    readings_analyzed    INT DEFAULT 0,
    heart_rate_trend     FLOAT,          -- bpm per minute over the day (least-squares slope)
    spo2_trend           FLOAT,          -- % per minute over the day
    period_start         TIMESTAMP,      -- vitals time range the batch job analysed
    period_end           TIMESTAMP,
    created_at           TIMESTAMP DEFAULT (NOW() AT TIME ZONE 'UTC'),
    CONSTRAINT uq_risk_report UNIQUE (patient_id, simulated_day)
);

-- Real-time alerts (written by speed layer)
CREATE TABLE IF NOT EXISTS alerts (
    id              SERIAL PRIMARY KEY,
    patient_id      VARCHAR(20) NOT NULL,
    alert_type      VARCHAR(50) NOT NULL,
    severity        VARCHAR(20) NOT NULL,
    message         TEXT,
    vital_name      VARCHAR(30),
    vital_value     FLOAT,
    threshold_value FLOAT,
    triggered_at    TIMESTAMP DEFAULT (NOW() AT TIME ZONE 'UTC'),
    acknowledged    BOOLEAN DEFAULT FALSE,
    -- Alarm de-duplication: repeat breaches of the same vital update one open
    -- alert (occurrences, last_seen_at) instead of inserting new rows.
    occurrences     INT DEFAULT 1,
    last_seen_at    TIMESTAMP,
    -- event_id of the bedside reading that caused the alert: lets an alert be
    -- traced back through the speed layer to the producer's log line.
    event_id        VARCHAR(64)
);

-- Pipeline health metrics (written by observability layer)
CREATE TABLE IF NOT EXISTS pipeline_metrics (
    id             SERIAL PRIMARY KEY,
    component      VARCHAR(50) NOT NULL,
    metric_name    VARCHAR(100) NOT NULL,
    metric_value   FLOAT NOT NULL,
    recorded_at    TIMESTAMP DEFAULT NOW()
);

-- Indexes for query performance
CREATE INDEX IF NOT EXISTS idx_vitals_patient_window ON vitals_aggregates(patient_id, window_start DESC);
CREATE INDEX IF NOT EXISTS idx_risk_patient_day ON patient_risk(patient_id, simulated_day DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_active ON alerts(acknowledged, triggered_at DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_patient ON alerts(patient_id, triggered_at DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_open_vital ON alerts(patient_id, vital_name, acknowledged);
CREATE INDEX IF NOT EXISTS idx_metrics_component ON pipeline_metrics(component, recorded_at DESC);

-- Seed patient data
INSERT INTO patients (patient_id, name, age, ward, bed_number) VALUES
    ('P001', 'Patient A', 45, 'Cardiology', 'B01'),
    ('P002', 'Patient B', 62, 'Cardiology', 'B02'),
    ('P003', 'Patient C', 33, 'General', 'B03'),
    ('P004', 'Patient D', 71, 'ICU', 'B04'),
    ('P005', 'Patient E', 28, 'General', 'B05'),
    ('P006', 'Patient F', 55, 'Respiratory', 'B06'),
    ('P007', 'Patient G', 48, 'ICU', 'B07'),
    ('P008', 'Patient H', 39, 'General', 'B08'),
    ('P009', 'Patient I', 67, 'Cardiology', 'B09'),
    ('P010', 'Patient J', 52, 'Respiratory', 'B10'),
    ('P011', 'Patient K', 41, 'General', 'B11'),
    ('P012', 'Patient L', 58, 'ICU', 'B12')
ON CONFLICT (patient_id) DO NOTHING;
