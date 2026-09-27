"""
Serving layer — FastAPI REST API.

Exposes endpoints for:
  - Real-time ward status and per-patient vitals
  - Active alerts
  - Daily consolidated risk reports
  - Pipeline health check
"""

import os
import sys
from datetime import date, datetime
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from storage.db import execute_query
from observability.logging_config import get_logger
from observability.metrics import metrics
from observability.health_checks import HealthCheckDaemon, run_all_checks

logger = get_logger("serving.api")


# ── Application lifecycle ─────────────────────────────────────────

health_daemon: HealthCheckDaemon | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start health-check daemon on startup, stop on shutdown."""
    global health_daemon
    health_daemon = HealthCheckDaemon(interval=30)
    health_daemon.start()
    logger.info("api_started")
    yield
    health_daemon.stop()
    logger.info("api_stopped")


app = FastAPI(
    title="Hospital Vitals Pipeline API",
    description="Real-time ward monitoring and daily risk reporting",
    version="1.0.0",
    lifespan=lifespan,
)


# ── Ward status ───────────────────────────────────────────────────

@app.get("/api/ward/status")
def get_ward_status():
    """
    Current ward overview: total patients, active alerts,
    latest vitals summary per patient.
    """
    patients = execute_query("SELECT * FROM patients ORDER BY patient_id")

    active_alerts = execute_query(
        "SELECT COUNT(*) AS cnt FROM alerts WHERE acknowledged = FALSE"
    )

    latest_vitals = execute_query(
        """
        SELECT DISTINCT ON (patient_id)
            patient_id,
            avg_heart_rate,
            avg_spo2,
            avg_systolic_bp,
            avg_diastolic_bp,
            avg_temperature,
            window_end AS last_reading
        FROM vitals_aggregates
        ORDER BY patient_id, window_end DESC
        """
    )

    # Build a lookup for quick access
    vitals_map = {v["patient_id"]: v for v in latest_vitals}

    ward_data = []
    for p in patients:
        v = vitals_map.get(p["patient_id"], {})
        ward_data.append({
            "patient_id": p["patient_id"],
            "name": p["name"],
            "ward": p["ward"],
            "bed": p["bed_number"],
            "latest_vitals": {
                "heart_rate": v.get("avg_heart_rate"),
                "spo2": v.get("avg_spo2"),
                "systolic_bp": v.get("avg_systolic_bp"),
                "diastolic_bp": v.get("avg_diastolic_bp"),
                "temperature": v.get("avg_temperature"),
                "last_reading": str(v["last_reading"]) if v.get("last_reading") else None,
            },
        })

    return {
        "timestamp": datetime.utcnow().isoformat(),
        "total_patients": len(patients),
        "active_alerts": active_alerts[0]["cnt"] if active_alerts else 0,
        "patients": ward_data,
    }


# ── Per-patient vitals ────────────────────────────────────────────

@app.get("/api/patients/{patient_id}/vitals")
def get_patient_vitals(patient_id: str, limit: int = Query(20, ge=1, le=100)):
    """
    Recent vitals trend for a specific patient.

    Returns the last N windowed aggregations ordered by time.
    """
    rows = execute_query(
        """
        SELECT
            window_start, window_end,
            avg_heart_rate, avg_spo2,
            avg_systolic_bp, avg_diastolic_bp,
            avg_temperature, min_spo2, max_heart_rate,
            reading_count
        FROM vitals_aggregates
        WHERE patient_id = %s
        ORDER BY window_end DESC
        LIMIT %s
        """,
        (patient_id, limit),
    )

    if not rows:
        raise HTTPException(status_code=404, detail=f"No vitals found for {patient_id}")

    return {
        "patient_id": patient_id,
        "windows": [
            {
                "window_start": str(r["window_start"]),
                "window_end": str(r["window_end"]),
                "heart_rate": r["avg_heart_rate"],
                "spo2": r["avg_spo2"],
                "systolic_bp": r["avg_systolic_bp"],
                "diastolic_bp": r["avg_diastolic_bp"],
                "temperature": r["avg_temperature"],
                "min_spo2": r["min_spo2"],
                "max_heart_rate": r["max_heart_rate"],
                "readings": r["reading_count"],
            }
            for r in rows
        ],
    }


# ── Alerts ────────────────────────────────────────────────────────

@app.get("/api/alerts/active")
def get_active_alerts(limit: int = Query(50, ge=1, le=200)):
    """All unacknowledged alerts, newest first."""
    rows = execute_query(
        """
        SELECT id, patient_id, alert_type, severity, message,
               vital_name, vital_value, threshold_value, triggered_at
        FROM alerts
        WHERE acknowledged = FALSE
        ORDER BY triggered_at DESC
        LIMIT %s
        """,
        (limit,),
    )

    return {
        "total": len(rows),
        "alerts": [
            {
                "id": r["id"],
                "patient_id": r["patient_id"],
                "alert_type": r["alert_type"],
                "severity": r["severity"],
                "message": r["message"],
                "vital_name": r["vital_name"],
                "vital_value": r["vital_value"],
                "threshold": r["threshold_value"],
                "triggered_at": str(r["triggered_at"]),
            }
            for r in rows
        ],
    }


@app.post("/api/alerts/{alert_id}/acknowledge")
def acknowledge_alert(alert_id: int):
    """Mark an alert as acknowledged."""
    from storage.db import execute_write

    updated = execute_write(
        "UPDATE alerts SET acknowledged = TRUE WHERE id = %s", (alert_id,)
    )
    if updated == 0:
        raise HTTPException(status_code=404, detail="Alert not found")
    return {"status": "acknowledged", "alert_id": alert_id}


# ── Daily risk reports ────────────────────────────────────────────

@app.get("/api/reports/daily/{report_date}")
def get_daily_report(report_date: date):
    """
    Daily consolidated patient risk report for the given date.

    This is the key output of the batch layer — it joins vitals
    trends with lab results to produce a composite risk score.
    """
    rows = execute_query(
        """
        SELECT
            pr.patient_id, p.name, p.ward, p.bed_number,
            pr.vitals_risk_score, pr.lab_risk_score,
            pr.combined_risk_score, pr.risk_level,
            pr.abnormal_vitals_count, pr.abnormal_labs_count,
            pr.risk_factors, pr.created_at
        FROM patient_risk pr
        JOIN patients p ON pr.patient_id = p.patient_id
        WHERE pr.report_date = %s
        ORDER BY pr.combined_risk_score DESC
        """,
        (report_date,),
    )

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No risk report found for {report_date}",
        )

    return {
        "report_date": str(report_date),
        "generated_at": str(rows[0]["created_at"]) if rows else None,
        "total_patients": len(rows),
        "patients": [
            {
                "patient_id": r["patient_id"],
                "name": r["name"],
                "ward": r["ward"],
                "bed": r["bed_number"],
                "vitals_risk": r["vitals_risk_score"],
                "lab_risk": r["lab_risk_score"],
                "combined_risk": r["combined_risk_score"],
                "risk_level": r["risk_level"],
                "abnormal_vitals": r["abnormal_vitals_count"],
                "abnormal_labs": r["abnormal_labs_count"],
                "risk_factors": r["risk_factors"],
            }
            for r in rows
        ],
    }


@app.get("/api/reports/latest")
def get_latest_report():
    """Redirect to the most recent daily report."""
    rows = execute_query(
        "SELECT report_date FROM patient_risk ORDER BY report_date DESC LIMIT 1"
    )
    if not rows:
        raise HTTPException(status_code=404, detail="No reports generated yet")
    return get_daily_report(rows[0]["report_date"])


# ── Pipeline health ──────────────────────────────────────────────

@app.get("/api/health")
def health_check():
    """
    Pipeline health endpoint.

    Returns current metrics and any active health-check alerts.
    """
    try:
        alerts = run_all_checks()
        metric_snapshot = metrics.snapshot()

        return {
            "status": "degraded" if alerts else "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "checks": {
                "database": "connected",
                "alerts_fired": len(alerts),
                "alert_details": [
                    {"type": a["alert_type"], "severity": a["severity"], "message": a["message"]}
                    for a in alerts
                ],
            },
            "metrics": metric_snapshot,
        }
    except Exception as e:
        logger.exception("health_check_failed")
        return {
            "status": "unhealthy",
            "error": str(e),
        }


# ── Entrypoint ────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "serving.api:app",
        host=os.getenv("API_HOST", "0.0.0.0"),
        port=int(os.getenv("API_PORT", "8000")),
        reload=True,
    )
