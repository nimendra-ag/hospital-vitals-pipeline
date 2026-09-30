"""
Daily consolidated patient risk report generator.

Produces both a JSON report file and a rendered HTML report
(via Jinja2) that can be served or emailed, one per simulated day.

Each patient row also carries the change in combined risk versus the
previous simulated day — i.e. how the newest lab results moved the
patient's risk picture.
"""

import os
import io
import csv
import json
import sys
from pathlib import Path

from jinja2 import Template
from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from storage.db import execute_query
from observability.logging_config import get_logger

logger = get_logger("serving.report_generator")

REPORT_OUTPUT_DIR = os.getenv("REPORT_OUTPUT_DIR", "data/reports")

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Patient Risk Report — Day {{ simulated_day }}</title>
  <style>
    body { font-family: system-ui, sans-serif; margin: 2rem; color: #1a1a1a; }
    h1 { font-size: 1.5rem; margin-bottom: 0.5rem; }
    .meta { color: #666; margin-bottom: 1.5rem; font-size: 0.9rem; }
    table { width: 100%; border-collapse: collapse; font-size: 0.85rem; }
    th, td { padding: 0.6rem 0.8rem; text-align: left; border-bottom: 1px solid #e0e0e0; }
    th { background: #f5f5f5; font-weight: 600; }
    .risk-critical { color: #a32d2d; font-weight: 600; }
    .risk-high { color: #ba7517; font-weight: 600; }
    .risk-moderate { color: #639922; }
    .risk-low { color: #5f5e5a; }
    .factors { font-size: 0.8rem; color: #555; max-width: 300px; }
    .summary { margin: 1.5rem 0; padding: 1rem; background: #f9f9f6; border-radius: 8px; }
  </style>
</head>
<body>
  <h1>Daily Patient Risk Report</h1>
  <div class="meta">
    Simulated day: {{ simulated_day }} | Report date: {{ report_date }} | Generated: {{ generated_at }} | Patients: {{ total_patients }}
  </div>

  <div class="summary">
    <strong>Summary:</strong>
    {{ critical_count }} critical, {{ high_count }} high,
    {{ moderate_count }} moderate, {{ low_count }} low risk patients.
  </div>

  <table>
    <thead>
      <tr>
        <th>Patient</th>
        <th>Ward / Bed</th>
        <th>Vitals risk</th>
        <th>Lab risk</th>
        <th>Combined</th>
        <th>vs prev day</th>
        <th>Level</th>
        <th>Risk factors</th>
      </tr>
    </thead>
    <tbody>
      {% for p in patients %}
      <tr>
        <td>{{ p.patient_id }} ({{ p.name }})</td>
        <td>{{ p.ward }} / {{ p.bed }}</td>
        <td>{{ "%.1f"|format(p.vitals_risk) }}%</td>
        <td>{{ "%.1f"|format(p.lab_risk) }}%</td>
        <td><strong>{{ "%.1f"|format(p.combined_risk) }}%</strong></td>
        <td>{% if p.risk_change is not none %}{{ "%+.1f"|format(p.risk_change) }}{% else %}new{% endif %}</td>
        <td class="risk-{{ p.risk_level|lower }}">{{ p.risk_level }}</td>
        <td class="factors">{{ p.risk_factors }}</td>
      </tr>
      {% endfor %}
    </tbody>
  </table>
</body>
</html>
"""


REPORT_QUERY = """
    SELECT
        pr.patient_id, p.name, p.ward, p.bed_number, pr.report_date,
        pr.simulated_day, pr.vitals_risk_score, pr.lab_risk_score,
        pr.combined_risk_score, pr.risk_level,
        pr.abnormal_vitals_count, pr.abnormal_labs_count,
        pr.risk_factors, pr.readings_analyzed, pr.heart_rate_trend,
        pr.spo2_trend, pr.created_at,
        pr.combined_risk_score - prev.combined_risk_score AS risk_change
    FROM patient_risk pr
    JOIN patients p ON pr.patient_id = p.patient_id
    LEFT JOIN patient_risk prev
        ON prev.patient_id = pr.patient_id
       AND prev.simulated_day = pr.simulated_day - 1
    WHERE pr.simulated_day = %s
    ORDER BY pr.combined_risk_score DESC
"""


def build_report(simulated_day: int) -> dict | None:
    """Assemble the consolidated report for one simulated day (None if absent)."""
    rows = execute_query(REPORT_QUERY, (simulated_day,))
    if not rows:
        return None

    patients = [
        {
            "patient_id": r["patient_id"],
            "name": r["name"],
            "ward": r["ward"],
            "bed": r["bed_number"],
            "vitals_risk": r["vitals_risk_score"],
            "lab_risk": r["lab_risk_score"],
            "combined_risk": r["combined_risk_score"],
            "risk_level": r["risk_level"],
            "risk_change": round(r["risk_change"], 2) if r["risk_change"] is not None else None,
            "abnormal_vitals": r["abnormal_vitals_count"],
            "abnormal_labs": r["abnormal_labs_count"],
            "readings_analyzed": r["readings_analyzed"],
            "heart_rate_trend": r["heart_rate_trend"],
            "spo2_trend": r["spo2_trend"],
            "risk_factors": r["risk_factors"],
        }
        for r in rows
    ]
    return {
        "simulated_day": simulated_day,
        "report_date": str(rows[0]["report_date"]),
        "generated_at": str(rows[0]["created_at"]),
        "total_patients": len(patients),
        "critical_count": sum(1 for p in patients if p["risk_level"] == "CRITICAL"),
        "high_count": sum(1 for p in patients if p["risk_level"] == "HIGH"),
        "moderate_count": sum(1 for p in patients if p["risk_level"] == "MODERATE"),
        "low_count": sum(1 for p in patients if p["risk_level"] == "LOW"),
        "patients": patients,
    }


CSV_COLUMNS = [
    "patient_id", "name", "ward", "bed", "risk_level", "combined_risk", "risk_change",
    "vitals_risk", "lab_risk", "abnormal_vitals", "abnormal_labs", "readings_analyzed",
    "heart_rate_trend", "spo2_trend", "risk_factors",
]


def render_html(report: dict) -> str:
    """Render the report dict as a standalone HTML page."""
    return Template(HTML_TEMPLATE).render(**report)


def render_csv(report: dict) -> str:
    """Render the report dict as CSV (one row per patient) for spreadsheets."""
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=["simulated_day"] + CSV_COLUMNS, extrasaction="ignore")
    writer.writeheader()
    for p in report["patients"]:
        writer.writerow({"simulated_day": report["simulated_day"], **p})
    return buf.getvalue()


def generate_report(simulated_day: int) -> dict:
    """
    Build the report for one simulated day and write JSON + HTML files.

    Returns the report data dict.
    """
    report = build_report(simulated_day)
    if report is None:
        logger.warning("no_risk_data_for_report", simulated_day=simulated_day)
        return {"error": "No data available for this simulated day"}

    # Write outputs
    output_dir = Path(REPORT_OUTPUT_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)

    stem = f"risk_report_day{str(simulated_day).zfill(3)}"

    # JSON report
    json_path = output_dir / f"{stem}.json"
    with open(json_path, "w") as f:
        json.dump(report, f, indent=2)

    # HTML report
    html_path = output_dir / f"{stem}.html"
    html_content = render_html(report)
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    logger.info(
        "report_generated",
        simulated_day=simulated_day,
        json_path=str(json_path),
        html_path=str(html_path),
        total_patients=report["total_patients"],
    )

    return report


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--day", type=int, required=True, help="Simulated day number")
    args = parser.parse_args()

    result = generate_report(simulated_day=args.day)
    print(json.dumps(result, indent=2))
