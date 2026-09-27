-- ============================================================
-- Hospital Vitals Pipeline — PostgreSQL Schema (Neon)
-- Run this once against your Neon database to bootstrap tables.
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

-- Real-time vitals aggregates (written by speed layer)
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
    created_at      TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_vitals_window UNIQUE (patient_id, window_start)
);

-- Lab results (ingested by batch layer)
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
    ingested_at      TIMESTAMP DEFAULT NOW()
);

-- Daily patient risk report (written by batch layer)
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
    created_at           TIMESTAMP DEFAULT NOW(),
    CONSTRAINT uq_risk_report UNIQUE (patient_id, report_date)
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
    triggered_at    TIMESTAMP DEFAULT NOW(),
    acknowledged    BOOLEAN DEFAULT FALSE
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
CREATE INDEX IF NOT EXISTS idx_lab_patient_day ON lab_results(patient_id, simulated_day);
CREATE INDEX IF NOT EXISTS idx_risk_patient_date ON patient_risk(patient_id, report_date DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_active ON alerts(acknowledged, triggered_at DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_patient ON alerts(patient_id, triggered_at DESC);
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
