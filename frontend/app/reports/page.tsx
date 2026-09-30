"use client";

import { useState } from "react";
import Link from "next/link";
import { ArrowDownRight, ArrowRight, ArrowUpRight, ChevronDown, ChevronUp, Download } from "lucide-react";
import { LiveIndicator } from "@/components/LiveIndicator";
import { RiskBreakdown, RiskExplainer } from "@/components/RiskExplainer";
import { StatusBadge } from "@/components/StatusBadge";
import { api } from "@/lib/api";
import { usePolling } from "@/lib/usePolling";
import { riskLevelToStatus } from "@/lib/clinical";
import type { RiskReportPatient } from "@/lib/types";

export default function DailyReportPage() {
  const [day, setDay] = useState<number | null>(null); // null = latest
  const days = usePolling(api.reportDays, 30000);
  const report = usePolling(() => (day === null ? api.latestReport() : api.reportByDay(day)), 30000);
  const shownDay = report.data?.simulated_day;
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
              ? `Simulated day ${report.data.simulated_day} (${report.data.report_date}) — combines the day's vital-sign trends with that day's lab results.`
              : "Combines vital-sign trends with the latest lab results."}
          </p>
        </div>
        <LiveIndicator live={report.live} />
      </div>

      <div className="mb-4 flex flex-wrap items-center gap-2 rounded-xl border border-slate-200 bg-white p-3">
        <label className="text-sm font-medium text-slate-700" htmlFor="day">
          Report for
        </label>
        <select
          id="day"
          value={day ?? ""}
          onChange={(e) => setDay(e.target.value === "" ? null : Number(e.target.value))}
          className="rounded-lg border border-slate-300 px-2 py-1 text-sm"
        >
          <option value="">Latest day</option>
          {(days.data?.days ?? []).map((d) => (
            <option key={d.simulated_day} value={d.simulated_day}>
              Simulated day {d.simulated_day} ({d.report_date})
            </option>
          ))}
        </select>
        {shownDay !== undefined && (
          <div className="ml-auto flex flex-wrap gap-2">
            {(["csv", "html", "json"] as const).map((fmt) => (
              <a
                key={fmt}
                href={api.reportDownloadUrl(shownDay, fmt)}
                className="inline-flex items-center gap-1 rounded-lg bg-slate-900 px-3 py-1.5 text-xs font-semibold text-white hover:bg-slate-700"
              >
                <Download className="h-3.5 w-3.5" /> {fmt.toUpperCase()}
              </a>
            ))}
          </div>
        )}
      </div>

      <div className="mb-4">
        <RiskExplainer />
      </div>

      {report.data && (
        <div className="mb-4 grid grid-cols-2 gap-3 sm:grid-cols-4">
          {(["critical", "high", "moderate", "low"] as const).map((lvl) => (
            <div key={lvl} className="rounded-xl border border-slate-200 bg-white p-3">
              <p className="text-xs font-medium uppercase tracking-wide text-slate-500">{lvl} risk</p>
              <p className="mt-1 text-2xl font-bold text-slate-900">
                {report.data!.patients.filter((p) => p.risk_level === lvl.toUpperCase()).length}
              </p>
            </div>
          ))}
        </div>
      )}

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
          <ChangeBadge change={patient.risk_change} />
          <StatusBadge status={status} />
          {open ? <ChevronUp className="h-4 w-4 text-slate-400" /> : <ChevronDown className="h-4 w-4 text-slate-400" />}
        </div>
      </button>

      {open && (
        <div className="border-t border-slate-100 px-4 py-3 text-sm">
          <div className="mb-2">
            <RiskBreakdown
              vitals={patient.vitals_risk}
              lab={patient.lab_risk}
              combined={patient.combined_risk}
              level={patient.risk_level}
            />
          </div>
          <div className="mb-2 grid grid-cols-2 gap-3 sm:grid-cols-4">
            <Metric label="Vitals Risk" value={patient.vitals_risk} />
            <Metric label="Lab Risk" value={patient.lab_risk} />
            <Metric label="Readings Breaching Thresholds" value={patient.abnormal_vitals} />
            <Metric label="Abnormal Lab Tests (of 5)" value={patient.abnormal_labs} />
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

function ChangeBadge({ change }: { change: number | null }) {
  if (change === null || change === undefined) {
    return <span className="rounded-full bg-slate-100 px-2 py-0.5 text-xs text-slate-500">first day</span>;
  }
  const worse = change >= 5;
  const better = change <= -5;
  const Icon = worse ? ArrowUpRight : better ? ArrowDownRight : ArrowRight;
  return (
    <span
      title="Change in combined risk vs the previous simulated day"
      className={`inline-flex items-center gap-0.5 rounded-full px-2 py-0.5 text-xs font-semibold ${
        worse ? "bg-critical-bg text-critical-text" : better ? "bg-normal-bg text-normal-text" : "bg-slate-100 text-slate-600"
      }`}
    >
      <Icon className="h-3.5 w-3.5" />
      {change > 0 ? "+" : ""}
      {change.toFixed(0)}
    </span>
  );
}

function Metric({ label, value }: { label: string; value: number | string }) {
  return (
    <div>
      <p className="text-xs text-slate-400">{label}</p>
      <p className="font-semibold text-slate-800">{value}</p>
    </div>
  );
}
