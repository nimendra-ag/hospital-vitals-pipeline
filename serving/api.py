"""
Serving layer — FastAPI REST API.

Exposes endpoints for:
  - Real-time ward status and per-patient vitals      (speed-layer view)
  - Daily consolidated risk reports                   (batch-layer view)
  - /api/ward/risk: the Lambda serving-layer MERGE of both views, i.e. the
    current early warning score + short-term trend from the speed layer
    combined with the latest lab-driven risk from the batch layer
  - Active alerts, pipeline health, Prometheus /metrics
"""

import os
import sys
from datetime import date, datetime, timezone
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query, Response
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from prometheus_client import CONTENT_TYPE_LATEST

load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from storage.db import execute_query
from observability.logging_config import get_logger
from observability.metrics import metrics
from observability.health_checks import HealthCheckDaemon, run_all_checks
from observability.api_metrics import collect_live_metrics
from processing.clinical_rules import early_warning_score, outlook_level
from serving.report_generator import build_report, render_csv, render_html

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

# Allow the Next.js family dashboard (runs on a different origin/port) to
# call this API directly from the browser. Read-only endpoints, no cookies
# involved, so a permissive dev-friendly origin list is acceptable here.
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ALLOW_ORIGINS", "http://localhost:3001").split(","),
    allow_methods=["*"],
    allow_headers=["*"],
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
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "total_patients": len(patients),
        "active_alerts": active_alerts[0]["cnt"] if active_alerts else 0,
        "patients": ward_data,
    }


# ── Merged real-time + batch view (Lambda serving layer) ──────────

TREND_WINDOWS = 10  # last 10 x 30 s windows = 5 minutes of short-term trend


@app.get("/api/ward/risk")
def get_ward_risk():
    """
    Answers the business question in one call, per patient:
      "which patients show concerning vital-sign trends right now"
          -> early warning score of the latest window + 5-minute HR/SpO2 slope
             (speed-layer view, seconds old)
      "how do yesterday's lab results change the risk picture going forward"
          -> latest batch risk (lab risk, abnormal labs, change vs the day
             before) from the batch-layer view
    and merges them into one outlook level (worst of the two wins).
    """
    realtime = execute_query(
        """
        WITH recent AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY window_start DESC) AS rn
            FROM vitals_aggregates
        )
        SELECT patient_id,
               MAX(CASE WHEN rn = 1 THEN avg_heart_rate END)  AS heart_rate,
               MAX(CASE WHEN rn = 1 THEN avg_spo2 END)        AS spo2,
               MAX(CASE WHEN rn = 1 THEN avg_systolic_bp END) AS systolic_bp,
               MAX(CASE WHEN rn = 1 THEN avg_temperature END) AS temperature,
               MAX(CASE WHEN rn = 1 THEN window_end END)      AS last_window_end,
               -- least-squares slope per minute over the recent windows
               REGR_SLOPE(avg_heart_rate, EXTRACT(EPOCH FROM window_start) / 60.0) AS hr_trend,
               REGR_SLOPE(avg_spo2,       EXTRACT(EPOCH FROM window_start) / 60.0) AS spo2_trend
        FROM recent
        WHERE rn <= %s
        GROUP BY patient_id
        """,
        (TREND_WINDOWS,),
    )
    batch = execute_query(
        """
        SELECT DISTINCT ON (pr.patient_id)
               pr.patient_id, pr.simulated_day, pr.lab_risk_score, pr.vitals_risk_score,
               pr.combined_risk_score, pr.risk_level, pr.abnormal_labs_count, pr.risk_factors,
               pr.combined_risk_score - prev.combined_risk_score AS risk_change
        FROM patient_risk pr
        LEFT JOIN patient_risk prev
               ON prev.patient_id = pr.patient_id AND prev.simulated_day = pr.simulated_day - 1
        ORDER BY pr.patient_id, pr.simulated_day DESC
        """
    )
    patients = execute_query("SELECT patient_id, name, ward, bed_number FROM patients ORDER BY patient_id")
    rt_map = {r["patient_id"]: r for r in realtime}
    batch_map = {r["patient_id"]: r for r in batch}

    result = []
    for p in patients:
        rt = rt_map.get(p["patient_id"], {})
        b = batch_map.get(p["patient_id"])
        ews = early_warning_score(rt.get("heart_rate"), rt.get("spo2"), rt.get("systolic_bp"), rt.get("temperature"))

        concerns = []
        if ews["level"] != "LOW":
            concerns.append(f"Early warning score {ews['score']} ({ews['level']})")
        if rt.get("hr_trend") is not None and rt["hr_trend"] > 3.0:
            concerns.append(f"Heart rate rising {rt['hr_trend']:+.1f} bpm/min over last 5 min")
        if rt.get("spo2_trend") is not None and rt["spo2_trend"] < -0.5:
            concerns.append(f"SpO2 falling {rt['spo2_trend']:+.2f} %/min over last 5 min")
        if b and b["abnormal_labs_count"]:
            concerns.append(f"{b['abnormal_labs_count']} abnormal lab result(s) on day {b['simulated_day']}")

        result.append({
            "patient_id": p["patient_id"],
            "name": p["name"],
            "ward": p["ward"],
            "bed": p["bed_number"],
            "outlook": outlook_level(ews["level"], b["risk_level"] if b else None),
            "concerns": concerns,
            "realtime": {
                "early_warning_score": ews["score"],
                "early_warning_level": ews["level"],
                "ews_components": ews["components"],
                "heart_rate": rt.get("heart_rate"),
                "spo2": rt.get("spo2"),
                "systolic_bp": rt.get("systolic_bp"),
                "temperature": rt.get("temperature"),
                "hr_trend_per_min": rt.get("hr_trend"),
                "spo2_trend_per_min": rt.get("spo2_trend"),
                "last_window_end": str(rt["last_window_end"]) if rt.get("last_window_end") else None,
            },
            "batch": None if not b else {
                "simulated_day": b["simulated_day"],
                "risk_level": b["risk_level"],
                "combined_risk": b["combined_risk_score"],
                "lab_risk": b["lab_risk_score"],
                "vitals_risk": b["vitals_risk_score"],
                "risk_change_vs_previous_day": b["risk_change"],
                "risk_factors": b["risk_factors"],
            },
        })

    rank = {"CRITICAL": 3, "HIGH": 2, "MODERATE": 1, "LOW": 0}
    result.sort(key=lambda r: (rank[r["outlook"]], r["realtime"]["early_warning_score"]), reverse=True)
    return {"timestamp": datetime.now(timezone.utc).isoformat(), "patients": result}


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


# ── Per-patient labs & risk history ────────────────────────────────

@app.get("/api/patients/{patient_id}/labs")
def get_patient_labs(patient_id: str, limit: int = Query(20, ge=1, le=200)):
    """
    Most recent lab test results for a patient, newest first.

    Backs the "yesterday's lab results" half of the family-facing
    patient detail view — each row already carries the reference range
    and an is_abnormal flag so the frontend doesn't need clinical logic.
    """
    rows = execute_query(
        """
        SELECT test_type, result_value, reference_min, reference_max,
               is_abnormal, collected_at, simulated_day
        FROM lab_results
        WHERE patient_id = %s
        ORDER BY collected_at DESC
        LIMIT %s
        """,
        (patient_id, limit),
    )

    return {
        "patient_id": patient_id,
        "tests": [
            {
                "test_type": r["test_type"],
                "result_value": r["result_value"],
                "reference_min": r["reference_min"],
                "reference_max": r["reference_max"],
                "is_abnormal": r["is_abnormal"],
                "collected_at": str(r["collected_at"]),
                "simulated_day": r["simulated_day"],
            }
            for r in rows
        ],
    }


@app.get("/api/patients/{patient_id}/risk")
def get_patient_risk_history(patient_id: str, days: int = Query(14, ge=1, le=100)):
    """
    Risk score history for one patient across recent simulated days,
    plus the risk factors behind the most recent score — this is what
    answers "how do yesterday's lab results change the risk picture"
    for a single patient.
    """
    rows = execute_query(
        """
        SELECT report_date, simulated_day, vitals_risk_score, lab_risk_score,
               combined_risk_score, risk_level, abnormal_vitals_count,
               abnormal_labs_count, risk_factors, heart_rate_trend, spo2_trend, created_at
        FROM patient_risk
        WHERE patient_id = %s
        ORDER BY simulated_day DESC
        LIMIT %s
        """,
        (patient_id, days),
    )

    if not rows:
        raise HTTPException(status_code=404, detail=f"No risk history found for {patient_id}")

    return {
        "patient_id": patient_id,
        "latest": {
            "report_date": str(rows[0]["report_date"]),
            "simulated_day": rows[0]["simulated_day"],
            "risk_level": rows[0]["risk_level"],
            "combined_risk": rows[0]["combined_risk_score"],
            "vitals_risk": rows[0]["vitals_risk_score"],
            "lab_risk": rows[0]["lab_risk_score"],
            "risk_factors": rows[0]["risk_factors"],
            "heart_rate_trend": rows[0]["heart_rate_trend"],
            "spo2_trend": rows[0]["spo2_trend"],
        },
        "history": [
            {
                "report_date": str(r["report_date"]),
                "simulated_day": r["simulated_day"],
                "combined_risk": r["combined_risk_score"],
                "vitals_risk": r["vitals_risk_score"],
                "lab_risk": r["lab_risk_score"],
                "risk_level": r["risk_level"],
            }
            for r in reversed(rows)
        ],
    }


# ── Alerts ────────────────────────────────────────────────────────

@app.get("/api/alerts/active")
def get_active_alerts(limit: int = Query(50, ge=1, le=200)):
    """All unacknowledged alerts, newest first."""
    rows = execute_query(
        """
        SELECT id, patient_id, alert_type, severity, message,
               vital_name, vital_value, threshold_value, triggered_at, event_id,
               occurrences, last_seen_at
        FROM alerts
        WHERE acknowledged = FALSE
        ORDER BY (severity = 'CRITICAL') DESC, COALESCE(last_seen_at, triggered_at) DESC
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
                "event_id": r["event_id"],
                "occurrences": r["occurrences"] or 1,
                "last_seen_at": str(r["last_seen_at"]) if r["last_seen_at"] else None,
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

@app.get("/api/reports/day/{simulated_day}")
def get_report_for_day(simulated_day: int):
    """
    Consolidated patient risk report for one simulated day — the key
    batch-layer output: raw-vitals trends joined with that day's lab
    results, plus each patient's risk change versus the previous day.
    """
    report = build_report(simulated_day)
    if report is None:
        raise HTTPException(status_code=404, detail=f"No risk report for simulated day {simulated_day}")
    return report


@app.get("/api/reports/day/{simulated_day}/download")
def download_report(simulated_day: int, format: str = Query("csv", pattern="^(csv|html|json)$")):
    """Download one simulated day's consolidated risk report as CSV, HTML or JSON."""
    import json as _json

    report = build_report(simulated_day)
    if report is None:
        raise HTTPException(status_code=404, detail=f"No risk report for simulated day {simulated_day}")
    body, media = {
        "csv": (lambda: render_csv(report), "text/csv"),
        "html": (lambda: render_html(report), "text/html"),
        "json": (lambda: _json.dumps(report, indent=2, default=str), "application/json"),
    }[format]
    filename = f"patient_risk_report_day{simulated_day:03d}.{format}"
    return Response(
        content=body(),
        media_type=media,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/api/ward/risk-history")
def get_ward_risk_history(days: int = Query(10, ge=1, le=60)):
    """
    Risk score per patient per simulated day for the last N days, plus a
    ward-level summary per day. Backs the "is each patient getting better or
    worse" heatmap and trend charts on the dashboard.
    """
    rows = execute_query(
        """
        WITH recent AS (
            SELECT DISTINCT simulated_day FROM patient_risk
            ORDER BY simulated_day DESC LIMIT %s
        )
        SELECT pr.patient_id, p.name, pr.simulated_day, pr.combined_risk_score,
               pr.vitals_risk_score, pr.lab_risk_score, pr.risk_level
        FROM patient_risk pr
        JOIN recent r ON r.simulated_day = pr.simulated_day
        JOIN patients p ON p.patient_id = pr.patient_id
        ORDER BY pr.patient_id, pr.simulated_day
        """,
        (days,),
    )
    day_list = sorted({r["simulated_day"] for r in rows})
    patients: dict[str, dict] = {}
    for r in rows:
        entry = patients.setdefault(r["patient_id"], {"patient_id": r["patient_id"], "name": r["name"], "scores": []})
        entry["scores"].append({
            "simulated_day": r["simulated_day"],
            "combined_risk": r["combined_risk_score"],
            "vitals_risk": r["vitals_risk_score"],
            "lab_risk": r["lab_risk_score"],
            "risk_level": r["risk_level"],
        })

    for entry in patients.values():
        scores = entry["scores"]
        change = scores[-1]["combined_risk"] - scores[-2]["combined_risk"] if len(scores) >= 2 else None
        entry["latest_change"] = round(change, 2) if change is not None else None
        entry["direction"] = (
            "new" if change is None else "worsening" if change >= 5 else "improving" if change <= -5 else "stable"
        )

    ward = []
    for d in day_list:
        day_rows = [r for r in rows if r["simulated_day"] == d]
        levels = [r["risk_level"] for r in day_rows]
        ward.append({
            "simulated_day": d,
            "avg_risk": round(sum(r["combined_risk_score"] for r in day_rows) / len(day_rows), 2),
            "critical": levels.count("CRITICAL"),
            "high": levels.count("HIGH"),
            "moderate": levels.count("MODERATE"),
            "low": levels.count("LOW"),
        })

    return {"days": day_list, "patients": list(patients.values()), "ward": ward}


@app.get("/api/reports/days")
def list_report_days():
    """Simulated days that have a report, newest first."""
    rows = execute_query(
        "SELECT DISTINCT simulated_day, report_date FROM patient_risk ORDER BY simulated_day DESC"
    )
    return {"days": [{"simulated_day": r["simulated_day"], "report_date": str(r["report_date"])} for r in rows]}


@app.get("/api/reports/daily/{report_date}")
def get_daily_report(report_date: date):
    """Latest simulated day's report on a calendar date (kept for compatibility)."""
    rows = execute_query(
        "SELECT MAX(simulated_day) AS day FROM patient_risk WHERE report_date = %s", (report_date,)
    )
    if not rows or rows[0]["day"] is None:
        raise HTTPException(status_code=404, detail=f"No risk report found for {report_date}")
    return get_report_for_day(rows[0]["day"])


@app.get("/api/reports/latest")
def get_latest_report():
    """The most recent simulated day's report."""
    rows = execute_query("SELECT MAX(simulated_day) AS day FROM patient_risk")
    if not rows or rows[0]["day"] is None:
        raise HTTPException(status_code=404, detail="No reports generated yet")
    return get_report_for_day(rows[0]["day"])


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
            "timestamp": datetime.now(timezone.utc).isoformat(),
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


# ── Prometheus scrape endpoint ────────────────────────────────────

@app.get("/metrics")
def prometheus_metrics():
    """
    Prometheus scrape target.

    Recomputes business gauges (active patients, active alerts by
    severity, risk-level distribution, vitals staleness) directly from
    PostgreSQL on every scrape and renders them in Prometheus text
    exposition format.
    """
    payload = collect_live_metrics()
    return Response(content=payload, media_type=CONTENT_TYPE_LATEST)


# ── Entrypoint ────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "serving.api:app",
        host=os.getenv("API_HOST", "0.0.0.0"),
        port=int(os.getenv("API_PORT", "8000")),
        reload=True,
    )
