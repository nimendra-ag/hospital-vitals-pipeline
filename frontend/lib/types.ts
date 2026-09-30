// Shapes returned by the FastAPI serving layer (serving/api.py).
// Kept intentionally close to the JSON the API actually returns.

export interface LatestVitals {
  heart_rate: number | null;
  spo2: number | null;
  systolic_bp: number | null;
  diastolic_bp: number | null;
  temperature: number | null;
  last_reading: string | null;
}

export interface WardPatient {
  patient_id: string;
  name: string;
  ward: string;
  bed: string;
  latest_vitals: LatestVitals;
}

export interface WardStatus {
  timestamp: string;
  total_patients: number;
  active_alerts: number;
  patients: WardPatient[];
}

export interface VitalsWindow {
  window_start: string;
  window_end: string;
  heart_rate: number | null;
  spo2: number | null;
  systolic_bp: number | null;
  diastolic_bp: number | null;
  temperature: number | null;
  min_spo2: number | null;
  max_heart_rate: number | null;
  readings: number;
}

export interface PatientVitalsTrend {
  patient_id: string;
  windows: VitalsWindow[];
}

export interface Alert {
  id: number;
  patient_id: string;
  alert_type: string;
  severity: "CRITICAL" | "WARNING";
  message: string;
  vital_name: string | null;
  vital_value: number | null;
  threshold: number | null;
  triggered_at: string;
}

export interface ActiveAlerts {
  total: number;
  alerts: Alert[];
}

export type RiskLevel = "LOW" | "MODERATE" | "HIGH" | "CRITICAL";

export interface RiskReportPatient {
  patient_id: string;
  name: string;
  ward: string;
  bed: string;
  vitals_risk: number;
  lab_risk: number;
  combined_risk: number;
  risk_level: RiskLevel;
  abnormal_vitals: number;
  abnormal_labs: number;
  risk_factors: string;
}

export interface DailyRiskReport {
  report_date: string;
  generated_at: string | null;
  total_patients: number;
  patients: RiskReportPatient[];
}

export interface LabTest {
  test_type: string;
  result_value: number;
  reference_min: number;
  reference_max: number;
  is_abnormal: boolean;
  collected_at: string;
  simulated_day: number;
}

export interface PatientLabs {
  patient_id: string;
  tests: LabTest[];
}

export interface RiskHistoryPoint {
  report_date: string;
  combined_risk: number;
  vitals_risk: number;
  lab_risk: number;
  risk_level: RiskLevel;
}

export interface PatientRiskHistory {
  patient_id: string;
  latest: {
    report_date: string;
    risk_level: RiskLevel;
    combined_risk: number;
    vitals_risk: number;
    lab_risk: number;
    risk_factors: string;
  };
  history: RiskHistoryPoint[];
}

export type Status = "normal" | "watch" | "critical";
