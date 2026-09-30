"use client";

import { useMemo, type ComponentType } from "react";
import { AlertTriangle, Users } from "lucide-react";
import { PatientCard } from "@/components/PatientCard";
import { LiveIndicator } from "@/components/LiveIndicator";
import { api } from "@/lib/api";
import { usePolling } from "@/lib/usePolling";
import { riskLevelToStatus } from "@/lib/clinical";

export default function WardOverviewPage() {
  const ward = usePolling(api.wardStatus, 5000);
  const alerts = usePolling(api.activeAlerts, 5000);
  const report = usePolling(api.latestReport, 30000);

  const alertCountByPatient = useMemo(() => {
    const counts = new Map<string, number>();
    for (const a of alerts.data?.alerts ?? []) {
      counts.set(a.patient_id, (counts.get(a.patient_id) ?? 0) + 1);
    }
    return counts;
  }, [alerts.data]);

  const riskByPatient = useMemo(() => {
    const map = new Map<string, ReturnType<typeof riskLevelToStatus>>();
    for (const p of report.data?.patients ?? []) {
      map.set(p.patient_id, riskLevelToStatus(p.risk_level));
    }
    return map;
  }, [report.data]);

  const criticalAlerts = (alerts.data?.alerts ?? []).filter((a) => a.severity === "CRITICAL");

  const patients = ward.data?.patients ?? [];
  const sorted = [...patients].sort((a, b) => {
    const rank = (id: string) => (alertCountByPatient.get(id) ?? 0) * 10 + (riskByPatient.get(id) === "critical" ? 5 : riskByPatient.get(id) === "watch" ? 2 : 0);
    return rank(b.patient_id) - rank(a.patient_id);
  });

  return (
    <div>
      <div className="mb-4 flex flex-wrap items-center justify-between gap-2">
        <div>
          <h1 className="text-xl font-bold text-slate-900">Ward Overview</h1>
          <p className="text-sm text-slate-500">Live vitals for every patient, updated every few seconds.</p>
        </div>
        <LiveIndicator live={ward.live} />
      </div>

      <div className="mb-4 grid grid-cols-2 gap-3 sm:grid-cols-4">
        <StatCard icon={Users} label="Patients" value={ward.data?.total_patients ?? "—"} />
        <StatCard
          icon={AlertTriangle}
          label="Active Alerts"
          value={ward.data?.active_alerts ?? "—"}
          tone={(ward.data?.active_alerts ?? 0) > 0 ? "critical" : "normal"}
        />
        <StatCard label="Critical Alerts" value={criticalAlerts.length} tone={criticalAlerts.length > 0 ? "critical" : "normal"} />
        <StatCard label="Report Date" value={report.data?.report_date ?? "—"} />
      </div>

      {criticalAlerts.length > 0 && (
        <div className="mb-4 rounded-xl border border-critical-border bg-critical-bg p-3">
          <p className="mb-1.5 flex items-center gap-1.5 text-sm font-semibold text-critical-text">
            <AlertTriangle className="h-4 w-4" /> Needs immediate attention
          </p>
          <ul className="space-y-1 text-sm text-critical-text">
            {criticalAlerts.slice(0, 5).map((a) => (
              <li key={a.id}>
                <span className="font-semibold">{a.patient_id}</span> — {a.message}
              </li>
            ))}
          </ul>
        </div>
      )}

      {ward.loading && !ward.data && <p className="text-sm text-slate-500">Loading ward status…</p>}
      {ward.error && !ward.data && (
        <p className="rounded-lg bg-critical-bg p-3 text-sm text-critical-text">
          Couldn&apos;t reach the monitoring API. Is the backend running at{" "}
          {process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://localhost:8000"}?
        </p>
      )}

      <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3">
        {sorted.map((patient) => (
          <PatientCard
            key={patient.patient_id}
            patient={patient}
            alertCount={alertCountByPatient.get(patient.patient_id) ?? 0}
            riskStatus={riskByPatient.get(patient.patient_id) ?? "normal"}
          />
        ))}
      </div>
    </div>
  );
}

function StatCard({
  icon: Icon,
  label,
  value,
  tone = "normal",
}: {
  icon?: ComponentType<{ className?: string }>;
  label: string;
  value: string | number;
  tone?: "normal" | "critical";
}) {
  return (
    <div className="rounded-xl border border-slate-200 bg-white p-3">
      <div className="flex items-center gap-1.5 text-slate-500">
        {Icon && <Icon className="h-3.5 w-3.5" />}
        <span className="text-xs font-medium uppercase tracking-wide">{label}</span>
      </div>
      <p className={`mt-1 text-2xl font-bold ${tone === "critical" ? "text-critical-text" : "text-slate-900"}`}>
        {value}
      </p>
    </div>
  );
}
