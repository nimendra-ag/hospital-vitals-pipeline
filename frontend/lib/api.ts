import type {
  ActiveAlerts,
  DailyRiskReport,
  PatientLabs,
  PatientRiskHistory,
  PatientVitalsTrend,
  WardRisk,
  WardRiskHistory,
  WardStatus,
} from "./types";

const BASE_URL = process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://localhost:8000";

class ApiError extends Error {
  constructor(public status: number, message: string) {
    super(message);
    this.name = "ApiError";
  }
}

async function apiGet<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE_URL}${path}`, { cache: "no-store" });
  if (!res.ok) {
    throw new ApiError(res.status, `GET ${path} failed with ${res.status}`);
  }
  return res.json() as Promise<T>;
}

async function apiPost<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE_URL}${path}`, { method: "POST" });
  if (!res.ok) {
    throw new ApiError(res.status, `POST ${path} failed with ${res.status}`);
  }
  return res.json() as Promise<T>;
}

export const api = {
  wardStatus: () => apiGet<WardStatus>("/api/ward/status"),
  patientVitals: (patientId: string, limit = 20) =>
    apiGet<PatientVitalsTrend>(`/api/patients/${patientId}/vitals?limit=${limit}`),
  patientLabs: (patientId: string) =>
    apiGet<PatientLabs>(`/api/patients/${patientId}/labs`),
  patientRiskHistory: (patientId: string) =>
    apiGet<PatientRiskHistory>(`/api/patients/${patientId}/risk`),
  activeAlerts: () => apiGet<ActiveAlerts>("/api/alerts/active"),
  acknowledgeAlert: (alertId: number) =>
    apiPost<{ status: string; alert_id: number }>(`/api/alerts/${alertId}/acknowledge`),
  latestReport: () => apiGet<DailyRiskReport>("/api/reports/latest"),
  reportByDate: (date: string) => apiGet<DailyRiskReport>(`/api/reports/daily/${date}`),
  reportByDay: (day: number) => apiGet<DailyRiskReport>(`/api/reports/day/${day}`),
  reportDays: () =>
    apiGet<{ days: { simulated_day: number; report_date: string }[] }>("/api/reports/days"),
  wardRisk: () => apiGet<WardRisk>("/api/ward/risk"),
  wardRiskHistory: (days = 10) => apiGet<WardRiskHistory>(`/api/ward/risk-history?days=${days}`),
  /** Direct link (browser download) to one simulated day's report. */
  reportDownloadUrl: (day: number, format: "csv" | "html" | "json") =>
    `${BASE_URL}/api/reports/day/${day}/download?format=${format}`,
};

export { ApiError };
