// Clinical reference ranges mirrored from data_sources/vitals_producer.py
// (normal bands) and processing/speed_layer/vitals_stream_processor.py
// (breach thresholds that generate an alert). Kept here so the UI can
// color a single reading without waiting for the alert to round-trip
// through Kafka -> Spark -> Postgres.

import type { RiskLevel, Status } from "./types";

export type VitalKey = "heart_rate" | "spo2" | "systolic_bp" | "diastolic_bp" | "temperature";

const NORMAL_RANGE: Record<VitalKey, [number, number]> = {
  heart_rate: [60, 100],
  spo2: [95, 100],
  systolic_bp: [90, 140],
  diastolic_bp: [60, 90],
  temperature: [36.1, 37.2],
};

const CRITICAL_BREACH: Record<VitalKey, { low?: number; high?: number }> = {
  heart_rate: { low: 50, high: 120 },
  spo2: { low: 90 },
  systolic_bp: { low: 80, high: 180 },
  diastolic_bp: {},
  temperature: { low: 35.0, high: 38.5 },
};

export function classifyVital(key: VitalKey, value: number | null): Status {
  if (value === null || Number.isNaN(value)) return "watch";

  const breach = CRITICAL_BREACH[key];
  if ((breach.low !== undefined && value < breach.low) || (breach.high !== undefined && value > breach.high)) {
    return "critical";
  }

  const [low, high] = NORMAL_RANGE[key];
  if (value < low || value > high) return "watch";

  return "normal";
}

export const VITAL_LABELS: Record<VitalKey, string> = {
  heart_rate: "Heart Rate",
  spo2: "Blood Oxygen",
  systolic_bp: "Blood Pressure (Systolic)",
  diastolic_bp: "Blood Pressure (Diastolic)",
  temperature: "Temperature",
};

export const VITAL_UNITS: Record<VitalKey, string> = {
  heart_rate: "bpm",
  spo2: "%",
  systolic_bp: "mmHg",
  diastolic_bp: "mmHg",
  temperature: "°C",
};

export function riskLevelToStatus(level: RiskLevel | undefined): Status {
  if (level === "CRITICAL") return "critical";
  if (level === "HIGH") return "watch";
  return "normal";
}

/** Combine per-vital status, active alert severity, and the daily risk
 * level into one overall badge — whichever signal is worst wins. */
export function worstStatus(...statuses: Status[]): Status {
  if (statuses.includes("critical")) return "critical";
  if (statuses.includes("watch")) return "watch";
  return "normal";
}

export function secondsSince(isoTimestamp: string | null): number | null {
  if (!isoTimestamp) return null;
  const ts = new Date(isoTimestamp.endsWith("Z") ? isoTimestamp : `${isoTimestamp}Z`).getTime();
  if (Number.isNaN(ts)) return null;
  return Math.max(0, Math.floor((Date.now() - ts) / 1000));
}

export function formatAge(seconds: number | null): string {
  if (seconds === null) return "no data yet";
  if (seconds < 5) return "just now";
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  return `${hours}h ago`;
}
