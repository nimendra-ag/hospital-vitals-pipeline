"use client";

import Link from "next/link";
import clsx from "clsx";
import { ArrowDownRight, ArrowRight, ArrowUpRight, Sparkles } from "lucide-react";
import {
  Bar,
  CartesianGrid,
  ComposedChart,
  Legend,
  Line,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { LiveIndicator } from "@/components/LiveIndicator";
import { RiskExplainer } from "@/components/RiskExplainer";
import { api } from "@/lib/api";
import { usePolling } from "@/lib/usePolling";
import { RISK_COLORS } from "@/lib/clinical";
import type { Direction, PatientRiskTrend, RiskLevel, WardRiskPatient } from "@/lib/types";

const LEVEL_CELL: Record<RiskLevel, string> = {
  LOW: "bg-emerald-100 text-emerald-800",
  MODERATE: "bg-yellow-100 text-yellow-800",
  HIGH: "bg-amber-200 text-amber-900",
  CRITICAL: "bg-red-200 text-red-900",
};

export default function TrendsPage() {
  const history = usePolling(() => api.wardRiskHistory(10), 30000);
  const now = usePolling(api.wardRisk, 10000);

  const patients = [...(history.data?.patients ?? [])].sort(
    (a, b) => (b.scores.at(-1)?.combined_risk ?? 0) - (a.scores.at(-1)?.combined_risk ?? 0),
  );
  const count = (d: Direction) => patients.filter((p) => p.direction === d).length;
  const attention = (now.data?.patients ?? []).filter((p) => p.outlook === "HIGH" || p.outlook === "CRITICAL");

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div>
          <h1 className="text-xl font-bold text-slate-900">Is each patient getting better or worse?</h1>
          <p className="text-sm text-slate-500">
            Live vital signs (last 5 minutes) combined with each simulated day&apos;s lab-driven risk score.
          </p>
        </div>
        <LiveIndicator live={history.live && now.live} />
      </div>

      <RiskExplainer />

      <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
        <SummaryCard label="Need attention now" value={attention.length} tone="critical" />
        <SummaryCard label="Worsening since yesterday" value={count("worsening")} tone="critical" />
        <SummaryCard label="Improving since yesterday" value={count("improving")} tone="normal" />
        <SummaryCard label="Stable" value={count("stable")} tone="neutral" />
      </div>

      <section>
        <h2 className="mb-2 text-sm font-semibold text-slate-700">Needs attention right now</h2>
        {attention.length === 0 ? (
          <p className="rounded-xl border border-normal-border bg-normal-bg p-3 text-sm text-normal-text">
            No patient currently has a high combined outlook.
          </p>
        ) : (
          <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3">
            {attention.map((p) => (
              <AttentionCard key={p.patient_id} patient={p} />
            ))}
          </div>
        )}
      </section>

      <section className="rounded-xl border border-slate-200 bg-white p-4">
        <h2 className="text-sm font-semibold text-slate-700">Ward risk by simulated day</h2>
        <p className="mb-3 text-xs text-slate-500">
          Bars: how many patients are in each risk level. Line: average combined risk score (0-100).
        </p>
        <div className="h-64">
          <ResponsiveContainer width="100%" height="100%">
            <ComposedChart data={history.data?.ward ?? []} margin={{ top: 4, right: 8, left: -16, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
              <XAxis dataKey="simulated_day" tickFormatter={(d) => `Day ${d}`} tick={{ fontSize: 11 }} />
              <YAxis yAxisId="count" allowDecimals={false} tick={{ fontSize: 11 }} />
              <YAxis yAxisId="score" orientation="right" domain={[0, 100]} tick={{ fontSize: 11 }} />
              <Tooltip labelFormatter={(d) => `Simulated day ${d}`} />
              <Legend wrapperStyle={{ fontSize: 12 }} />
              <Bar yAxisId="count" dataKey="low" stackId="lvl" name="Low" fill={RISK_COLORS.LOW} />
              <Bar yAxisId="count" dataKey="moderate" stackId="lvl" name="Moderate" fill={RISK_COLORS.MODERATE} />
              <Bar yAxisId="count" dataKey="high" stackId="lvl" name="High" fill={RISK_COLORS.HIGH} />
              <Bar yAxisId="count" dataKey="critical" stackId="lvl" name="Critical" fill={RISK_COLORS.CRITICAL} />
              <Line yAxisId="score" type="monotone" dataKey="avg_risk" name="Average risk" stroke="#1e293b" strokeWidth={2} dot={{ r: 3 }} />
            </ComposedChart>
          </ResponsiveContainer>
        </div>
      </section>

      <section className="rounded-xl border border-slate-200 bg-white p-4">
        <h2 className="text-sm font-semibold text-slate-700">Each patient&apos;s risk, day by day</h2>
        <p className="mb-3 text-xs text-slate-500">
          Combined risk score per simulated day (vital signs 60 % + lab result 40 %). The last column compares the
          newest day with the day before.
        </p>
        <div className="overflow-x-auto">
          <table className="w-full border-separate border-spacing-1 text-sm">
            <thead>
              <tr className="text-xs text-slate-500">
                <th className="text-left font-medium">Patient</th>
                {(history.data?.days ?? []).map((d) => (
                  <th key={d} className="px-1 font-medium">
                    Day {d}
                  </th>
                ))}
                <th className="text-left font-medium">vs previous day</th>
              </tr>
            </thead>
            <tbody>
              {patients.map((p) => (
                <HeatmapRow key={p.patient_id} patient={p} days={history.data?.days ?? []} />
              ))}
            </tbody>
          </table>
        </div>
        <div className="mt-3 flex flex-wrap gap-2 text-xs">
          {(Object.keys(LEVEL_CELL) as RiskLevel[]).map((lvl) => (
            <span key={lvl} className={clsx("rounded px-2 py-0.5", LEVEL_CELL[lvl])}>
              {lvl}
            </span>
          ))}
        </div>
      </section>
    </div>
  );
}

function HeatmapRow({ patient, days }: { patient: PatientRiskTrend; days: number[] }) {
  const byDay = new Map(patient.scores.map((s) => [s.simulated_day, s]));
  return (
    <tr>
      <td className="whitespace-nowrap pr-2">
        <Link href={`/patients/${patient.patient_id}`} className="font-medium text-slate-800 hover:underline">
          {patient.name}
        </Link>
        <span className="ml-1 text-xs text-slate-400">{patient.patient_id}</span>
      </td>
      {days.map((d) => {
        const s = byDay.get(d);
        return (
          <td
            key={d}
            title={s ? `Vitals ${s.vitals_risk.toFixed(0)} · Lab ${s.lab_risk.toFixed(0)}` : "no report"}
            className={clsx(
              "min-w-[3rem] rounded text-center font-semibold",
              s ? LEVEL_CELL[s.risk_level] : "bg-slate-50 text-slate-300",
            )}
          >
            {s ? s.combined_risk.toFixed(0) : "–"}
          </td>
        );
      })}
      <td className="whitespace-nowrap pl-2">
        <DirectionPill direction={patient.direction} change={patient.latest_change} />
      </td>
    </tr>
  );
}

function DirectionPill({ direction, change }: { direction: Direction; change: number | null }) {
  const config = {
    worsening: { icon: ArrowUpRight, text: "Worsening", cls: "bg-critical-bg text-critical-text" },
    improving: { icon: ArrowDownRight, text: "Improving", cls: "bg-normal-bg text-normal-text" },
    stable: { icon: ArrowRight, text: "Stable", cls: "bg-slate-100 text-slate-600" },
    new: { icon: Sparkles, text: "First report", cls: "bg-slate-100 text-slate-600" },
  }[direction];
  const Icon = config.icon;
  return (
    <span className={clsx("inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs font-semibold", config.cls)}>
      <Icon className="h-3.5 w-3.5" />
      {config.text}
      {change !== null && ` (${change > 0 ? "+" : ""}${change.toFixed(0)})`}
    </span>
  );
}

function AttentionCard({ patient }: { patient: WardRiskPatient }) {
  const change = patient.batch?.risk_change_vs_previous_day;
  return (
    <Link
      href={`/patients/${patient.patient_id}`}
      className={clsx(
        "block rounded-xl border p-3 transition hover:shadow-sm",
        patient.outlook === "CRITICAL" ? "border-critical-border bg-critical-bg" : "border-watch-border bg-watch-bg",
      )}
    >
      <div className="flex items-center justify-between">
        <p className="font-semibold text-slate-900">{patient.name}</p>
        <span className={clsx("rounded px-2 py-0.5 text-xs font-bold", LEVEL_CELL[patient.outlook])}>
          {patient.outlook}
        </span>
      </div>
      <p className="text-xs text-slate-500">
        {patient.ward} · Bed {patient.bed} · early warning score {patient.realtime.early_warning_score}
        {change !== null && change !== undefined && ` · risk ${change > 0 ? "+" : ""}${change.toFixed(0)} vs yesterday`}
      </p>
      <ul className="mt-1.5 list-disc space-y-0.5 pl-4 text-xs text-slate-700">
        {patient.concerns.length ? patient.concerns.map((c, i) => <li key={i}>{c}</li>) : <li>Elevated daily risk score</li>}
      </ul>
    </Link>
  );
}

function SummaryCard({ label, value, tone }: { label: string; value: number; tone: "critical" | "normal" | "neutral" }) {
  return (
    <div className="rounded-xl border border-slate-200 bg-white p-3">
      <p className="text-xs font-medium uppercase tracking-wide text-slate-500">{label}</p>
      <p
        className={clsx("mt-1 text-2xl font-bold", {
          "text-critical-text": tone === "critical" && value > 0,
          "text-normal-text": tone === "normal" && value > 0,
          "text-slate-900": tone === "neutral" || value === 0,
        })}
      >
        {value}
      </p>
    </div>
  );
}
