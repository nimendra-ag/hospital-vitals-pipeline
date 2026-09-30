"""
Clinical rules shared by the batch layer, the serving layer and the tests.

Pure Python on purpose (no Spark, no DB): every scoring decision in the
pipeline can be unit-tested and explained line by line.

  - VITALS_NORMAL / ALERT_THRESHOLDS: reference ranges and alert limits.
  - early_warning_score(): simplified NEWS2 (UK Royal College of Physicians
    National Early Warning Score 2) over HR, SpO2, systolic BP, temperature.
    Used by the serving layer to score the *current* 30 s window.
  - vitals_risk_score() / lab_risk_score() / combine_risk(): the daily batch
    risk model that joins a day of raw vitals with that day's lab results.
"""

from __future__ import annotations

# Normal ranges (used for display / colouring; the simulator's random walk
# drifts slightly outside these routinely, so they are NOT used for scoring)
VITALS_NORMAL = {
    "heart_rate":   (60.0, 100.0),
    "spo2":         (95.0, 100.0),
    "systolic_bp":  (90.0, 140.0),
    "diastolic_bp": (60.0, 90.0),
    "temperature":  (36.1, 37.2),
}

# Speed-layer alert limits (mirrored in processing/speed_layer/vitals_stream_processor.py)
ALERT_THRESHOLDS = {
    "heart_rate_high":  120.0,
    "heart_rate_low":    50.0,
    "spo2_low":          90.0,
    "systolic_bp_high": 180.0,
    "systolic_bp_low":   80.0,
    "temperature_high":  38.5,
    "temperature_low":   35.0,
}

# Trend limits used by the daily batch model (per-minute least-squares slopes).
# Set above the simulator's random-walk noise: over a 5-minute simulated day
# they correspond to a sustained ~7 bpm rise / ~1.5 % SpO2 drop.
HR_RISING_BPM_PER_MIN = 1.5
SPO2_FALLING_PCT_PER_MIN = -0.3
TREND_PENALTY = 15.0

VITALS_WEIGHT = 0.6
LAB_WEIGHT = 0.4


# ── Early warning score (real-time view) ──────────────────────────────────

def _band(value: float, bands: list[tuple[float, int]], top: int) -> int:
    """Return the score of the first band whose upper bound >= value."""
    for upper, score in bands:
        if value <= upper:
            return score
    return top


def early_warning_score(heart_rate, spo2, systolic_bp, temperature) -> dict:
    """
    Simplified NEWS2 over the four vitals the bedside monitors provide
    (respiration rate and consciousness are not simulated).

    Returns {"score": int, "level": LOW|MEDIUM|HIGH, "components": {...}}.
    Missing values score 0 so a patient with no data is not flagged.
    """
    components = {
        "heart_rate": 0 if heart_rate is None else _band(
            heart_rate, [(40, 3), (50, 1), (90, 0), (110, 1), (130, 2)], 3),
        "spo2": 0 if spo2 is None else _band(
            spo2, [(91, 3), (93, 2), (95, 1)], 0),
        "systolic_bp": 0 if systolic_bp is None else _band(
            systolic_bp, [(90, 3), (100, 2), (110, 1), (219, 0)], 3),
        "temperature": 0 if temperature is None else _band(
            temperature, [(35.0, 3), (36.0, 1), (38.0, 0), (39.0, 1)], 2),
    }
    score = sum(components.values())
    if score >= 7:
        level = "HIGH"
    elif score >= 5 or any(v == 3 for v in components.values()):
        level = "MEDIUM"
    else:
        level = "LOW"
    return {"score": score, "level": level, "components": components}


# ── Daily batch risk model ────────────────────────────────────────────────

def classify_risk(score: float) -> str:
    """Map a 0-100 risk score to a categorical level."""
    if score >= 75:
        return "CRITICAL"
    if score >= 50:
        return "HIGH"
    if score >= 25:
        return "MODERATE"
    return "LOW"


def vitals_risk_score(total_readings: int, abnormal_readings: int,
                      hr_trend: float | None, spo2_trend: float | None) -> tuple[float, list[str]]:
    """
    Vitals half of the daily risk model.

    base  = % of the day's raw readings breaching a clinical ALERT_THRESHOLD
            (clinically significant, not marginal drift outside the normal band)
    +15   if heart rate is trending up faster than HR_RISING_BPM_PER_MIN
    +15   if SpO2 is trending down faster than SPO2_FALLING_PCT_PER_MIN
    """
    if total_readings <= 0:
        return 0.0, []
    score = 100.0 * abnormal_readings / total_readings
    factors = []
    if abnormal_readings:
        factors.append(f"{abnormal_readings}/{total_readings} vital readings breached alert thresholds")
    if hr_trend is not None and hr_trend > HR_RISING_BPM_PER_MIN:
        score += TREND_PENALTY
        factors.append(f"Heart rate rising ({hr_trend:+.2f} bpm/min)")
    if spo2_trend is not None and spo2_trend < SPO2_FALLING_PCT_PER_MIN:
        score += TREND_PENALTY
        factors.append(f"SpO2 falling ({spo2_trend:+.2f} %/min)")
    return round(min(score, 100.0), 2), factors


def lab_severity(result_value: float, reference_min: float, reference_max: float) -> float:
    """
    0-100 severity of one lab result.

    In range -> 0. Out of range -> 50 plus 50 x (distance outside the range /
    width of the range), capped at 100. So a result just outside the range
    scores ~50 and one a full range-width outside scores 100.
    """
    if reference_min <= result_value <= reference_max:
        return 0.0
    width = max(reference_max - reference_min, 1e-9)
    outside = (reference_min - result_value) if result_value < reference_min else (result_value - reference_max)
    return round(min(100.0, 50.0 + 50.0 * outside / width), 2)


EXTRA_ABNORMAL_LAB_POINTS = 10.0


def lab_risk_score(tests: list[dict]) -> tuple[float, list[str]]:
    """
    Lab half of the daily risk model, from the patient's daily panel:

        lab score = severity of the WORST test (see lab_severity)
                    + 10 points for every OTHER abnormal test, capped at 100

    So one mildly abnormal test gives ~50, one far-out result gives up to 100,
    and several abnormal tests together push the score up.
    """
    if not tests:
        return 0.0, []
    severities, factors = [], []
    for t in tests:
        severity = lab_severity(t["result_value"], t["reference_min"], t["reference_max"])
        if severity > 0:
            severities.append(severity)
            direction = "high" if t["result_value"] > t["reference_max"] else "low"
            factors.append(
                f"{t['test_type']} {direction} ({t['result_value']:.2f}, "
                f"ref: {t['reference_min']}-{t['reference_max']})"
            )
    if not severities:
        return 0.0, []
    score = max(severities) + EXTRA_ABNORMAL_LAB_POINTS * (len(severities) - 1)
    return round(min(score, 100.0), 2), factors


def combine_risk(vitals_score: float, lab_score: float) -> tuple[float, str]:
    """Weighted combination (vitals weigh more: they are more immediately actionable)."""
    combined = round(VITALS_WEIGHT * vitals_score + LAB_WEIGHT * lab_score, 2)
    return combined, classify_risk(combined)


def outlook_level(ews_level: str, batch_risk_level: str | None) -> str:
    """
    Serving-layer merge of the real-time view (early warning level from the
    latest window) and the batch view (yesterday's lab-driven risk level):
    the worse of the two wins, on a common LOW / MODERATE / HIGH / CRITICAL scale.
    """
    rank = {"LOW": 0, "MODERATE": 1, "HIGH": 2, "CRITICAL": 3}
    ews_as_risk = {"LOW": "LOW", "MEDIUM": "HIGH", "HIGH": "CRITICAL"}[ews_level]
    candidates = [ews_as_risk] + ([batch_risk_level] if batch_risk_level else [])
    return max(candidates, key=lambda lvl: rank.get(lvl, 0))
