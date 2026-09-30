"use client";

import { useState } from "react";
import Link from "next/link";
import { ChevronDown, ChevronUp } from "lucide-react";
import { LiveIndicator } from "@/components/LiveIndicator";
import { StatusBadge } from "@/components/StatusBadge";
import { api } from "@/lib/api";
import { usePolling } from "@/lib/usePolling";
import { riskLevelToStatus } from "@/lib/clinical";
import type { RiskReportPatient } from "@/lib/types";

export default function DailyReportPage() {
  const report = usePolling(api.latestReport, 30000);
  const [expanded, setExpanded] = useState<Set<string>>(new Set());

  function toggle(id: string) {
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }

  const patients = [...(report.data?.patients ?? [])].sort((a, b) => b.combined_risk - a.combined_risk);

  return (
    <div>
      <div className="mb-4 flex items-center justify-between">
        <div>
          <h1 className="text-xl font-bold text-slate-900">Daily Risk Report</h1>
          <p className="text-sm text-slate-500">
            {report.data
              ? `For ${report.data.report_date} — combines today's vital-sign trends with yesterday's lab results.`
              : "Combines vital-sign trends with the latest lab results."}
          </p>
        </div>
        <LiveIndicator live={report.live} />
      </div>

      {report.error && !report.data && (
        <p className="rounded-lg bg-critical-bg p-3 text-sm text-critical-text">
          No daily report is available yet — it&apos;s generated once the batch pipeline (Airflow) has run at
          least once.
        </p>
      )}

      <div className="space-y-2">
        {patients.map((p) => (
          <ReportRow key={p.patient_id} patient={p} open={expanded.has(p.patient_id)} onToggle={() => toggle(p.patient_id)} />
        ))}
      </div>
    </div>
  );
}

function ReportRow({
  patient,
  open,
  onToggle,
}: {
  patient: RiskReportPatient;
  open: boolean;
  onToggle: () => void;
}) {
  const status = riskLevelToStatus(patient.risk_level);
  const factors = patient.risk_factors && patient.risk_factors !== "None" ? patient.risk_factors.split(";").map((f) => f.trim()) : [];

  return (
    <div className="rounded-xl border border-slate-200 bg-white">
      <button onClick={onToggle} className="flex w-full items-center justify-between gap-3 px-4 py-3 text-left">
        <div className="flex items-center gap-3">
          <div>
            <p className="font-semibold text-slate-900">
              <Link
                href={`/patients/${patient.patient_id}`}
                className="hover:underline"
                onClick={(e) => e.stopPropagation()}
              >
                {patient.name}
              </Link>
            </p>
            <p className="text-xs text-slate-500">
              {patient.ward} · Bed {patient.bed} · {patient.patient_id}
            </p>
          </div>
        </div>
        <div className="flex items-center gap-3">
          <div className="hidden text-right sm:block">
            <p className="text-xs text-slate-400">Combined Risk</p>
            <p className="font-bold text-slate-900">{patient.combined_risk.toFixed(0)}</p>
          </div>
          <StatusBadge status={status} />
          {open ? <ChevronUp className="h-4 w-4 text-slate-400" /> : <ChevronDown className="h-4 w-4 text-slate-400" />}
        </div>
      </button>

      {open && (
        <div className="border-t border-slate-100 px-4 py-3 text-sm">
          <div className="mb-2 grid grid-cols-2 gap-3 sm:grid-cols-4">
            <Metric label="Vitals Risk" value={patient.vitals_risk} />
            <Metric label="Lab Risk" value={patient.lab_risk} />
            <Metric label="Abnormal Vital Windows" value={patient.abnormal_vitals} />
            <Metric label="Abnormal Lab Tests" value={patient.abnormal_labs} />
          </div>
          {factors.length > 0 ? (
            <>
              <p className="mb-1 font-medium text-slate-700">Why this patient&apos;s risk changed:</p>
              <ul className="list-disc space-y-0.5 pl-5 text-slate-600">
                {factors.map((f, i) => (
                  <li key={i}>{f}</li>
                ))}
              </ul>
            </>
          ) : (
            <p className="text-slate-500">No abnormal vitals or lab results contributed to this score.</p>
          )}
        </div>
      )}
    </div>
  );
}

function Metric({ label, value }: { label: string; value: number }) {
  return (
    <div>
      <p className="text-xs text-slate-400">{label}</p>
      <p className="font-semibold text-slate-800">{value}</p>
    </div>
  );
}
