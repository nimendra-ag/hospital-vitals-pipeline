"""
Daily consolidated patient risk report generator.

Produces both a JSON report file and a rendered HTML report
(via Jinja2) that can be served or emailed.
"""

import os
import json
import sys
from datetime import date
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
  <title>Patient Risk Report — {{ report_date }}</title>
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
    Report date: {{ report_date }} | Generated: {{ generated_at }} | Patients: {{ total_patients }}
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
        <td class="risk-{{ p.risk_level|lower }}">{{ p.risk_level }}</td>
        <td class="factors">{{ p.risk_factors }}</td>
      </tr>
      {% endfor %}
    </tbody>
  </table>
</body>
</html>
"""


def generate_report(report_date: date = None, simulated_day: int = None) -> dict:
    """
    Query the patient_risk table and produce both JSON and HTML reports.

    Returns the report data dict.
    """
    if report_date is None:
        report_date = date.today()

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
        logger.warning("no_risk_data_for_report", report_date=str(report_date))
        return {"error": "No data available for this date"}

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
            "abnormal_vitals": r["abnormal_vitals_count"],
            "abnormal_labs": r["abnormal_labs_count"],
            "risk_factors": r["risk_factors"],
        }
        for r in rows
    ]

    report = {
        "report_date": str(report_date),
        "generated_at": str(rows[0]["created_at"]),
        "total_patients": len(patients),
        "critical_count": sum(1 for p in patients if p["risk_level"] == "CRITICAL"),
        "high_count": sum(1 for p in patients if p["risk_level"] == "HIGH"),
        "moderate_count": sum(1 for p in patients if p["risk_level"] == "MODERATE"),
        "low_count": sum(1 for p in patients if p["risk_level"] == "LOW"),
        "patients": patients,
    }

    # Write outputs
    output_dir = Path(REPORT_OUTPUT_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)

    day_suffix = f"_day{str(simulated_day).zfill(3)}" if simulated_day else ""

    # JSON report
    json_path = output_dir / f"risk_report_{report_date}{day_suffix}.json"
    with open(json_path, "w") as f:
        json.dump(report, f, indent=2)

    # HTML report
    html_path = output_dir / f"risk_report_{report_date}{day_suffix}.html"
    template = Template(HTML_TEMPLATE)
    html_content = template.render(**report)
    with open(html_path, "w") as f:
        f.write(html_content)

    logger.info(
        "report_generated",
        report_date=str(report_date),
        json_path=str(json_path),
        html_path=str(html_path),
        total_patients=len(patients),
    )

    return report


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default=None, help="Report date (YYYY-MM-DD)")
    parser.add_argument("--day", type=int, default=None, help="Simulated day number")
    args = parser.parse_args()

    rd = date.fromisoformat(args.date) if args.date else date.today()
    result = generate_report(report_date=rd, simulated_day=args.day)
    print(json.dumps(result, indent=2))
