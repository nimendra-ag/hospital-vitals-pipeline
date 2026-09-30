"use client";

import { useParams } from "next/navigation";
import Link from "next/link";
import { ArrowLeft, FlaskConical } from "lucide-react";
import { Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { LiveIndicator } from "@/components/LiveIndicator";
import { StatusBadge } from "@/components/StatusBadge";
import { VitalTile } from "@/components/VitalTile";
import { VitalsTrendChart } from "@/components/VitalsTrendChart";
import { api } from "@/lib/api";
import { usePolling } from "@/lib/usePolling";
import { riskLevelToStatus, worstStatus, type VitalKey } from "@/lib/clinical";

const VITAL_KEYS: VitalKey[] = ["heart_rate", "spo2", "systolic_bp", "temperature"];

export default function PatientDetailPage() {
  const params = useParams<{ id: string }>();
  const patientId = params.id;

  const ward = usePolling(api.wardStatus, 5000);
  const vitals = usePolling(() => api.patientVitals(patientId, 30), 5000);
  const labs = usePolling(() => api.patientLabs(patientId), 15000);
  const risk = usePolling(() => api.patientRiskHistory(patientId), 15000);

  const patient = ward.data?.patients.find((p) => p.patient_id === patientId);
  const riskStatus = riskLevelToStatus(risk.data?.latest.risk_level);
  const overall = worstStatus(riskStatus);

  const factors =
    risk.data?.latest.risk_factors && risk.data.latest.risk_factors !== "None"
      ? risk.data.latest.risk_factors.split(";").map((f) => f.trim())
      : [];

  return (
    <div>
      <Link href="/" className="mb-3 inline-flex items-center gap-1 text-sm text-slate-500 hover:text-slate-800">
        <ArrowLeft className="h-4 w-4" /> Back to ward overview
      </Link>

      <div className="mb-4 flex flex-wrap items-center justify-between gap-2">
        <div>
          <h1 className="text-xl font-bold text-slate-900">{patient?.name ?? patientId}</h1>
          <p className="text-sm text-slate-500">
            {patient ? `${patient.ward} · Bed ${patient.bed} · ${patient.patient_id}` : patientId}
          </p>
        </div>
        <div className="flex items-center gap-3">
          <StatusBadge status={overall} size="lg" />
          <LiveIndicator live={vitals.live} />
        </div>
      </div>

      <section className="mb-6">
        <h2 className="mb-2 text-sm font-semibold text-slate-700">Current Vitals</h2>
        <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
          {patient ? (
            VITAL_KEYS.map((key) => <VitalTile key={key} vitalKey={key} value={patient.latest_vitals[key]} />)
          ) : (
            <p className="col-span-4 text-sm text-slate-400">Loading…</p>
          )}
        </div>
      </section>

      <section className="mb-6">
        <h2 className="mb-2 text-sm font-semibold text-slate-700">Vital-Sign Trends</h2>
        <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
          {VITAL_KEYS.map((key) => (
            <VitalsTrendChart key={key} windows={vitals.data?.windows ?? []} vitalKey={key} />
          ))}
        </div>
      </section>

      <section className="mb-6">
        <h2 className="mb-2 text-sm font-semibold text-slate-700">Risk Picture</h2>
        <div className="rounded-xl border border-slate-200 bg-white p-4">
          {risk.data ? (
            <>
              <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
                <div>
                  <p className="text-xs text-slate-400">As of {risk.data.latest.report_date}</p>
                  <p className="text-2xl font-bold text-slate-900">
                    Combined risk: {risk.data.latest.combined_risk.toFixed(0)} / 100
                  </p>
                </div>
                <StatusBadge status={riskStatus} />
              </div>

              {factors.length > 0 ? (
                <>
                  <p className="mb-1 text-sm font-medium text-slate-700">
                    Yesterday&apos;s labs and today&apos;s vitals changed this patient&apos;s risk because:
                  </p>
                  <ul className="list-disc space-y-0.5 pl-5 text-sm text-slate-600">
                    {factors.map((f, i) => (
                      <li key={i}>{f}</li>
                    ))}
                  </ul>
                </>
              ) : (
                <p className="text-sm text-slate-500">No abnormal vitals or lab results right now.</p>
              )}

              {risk.data.history.length > 1 && (
                <div className="mt-4">
                  <p className="mb-1 text-xs font-medium uppercase tracking-wide text-slate-500">
                    Combined risk score, last {risk.data.history.length} days
                  </p>
                  <ResponsiveContainer width="100%" height={100}>
                    <LineChart data={risk.data.history} margin={{ top: 4, right: 12, left: -24, bottom: 0 }}>
                      <XAxis dataKey="report_date" tick={{ fontSize: 10 }} />
                      <YAxis tick={{ fontSize: 10 }} domain={[0, 100]} />
                      <Tooltip />
                      <Line type="monotone" dataKey="combined_risk" stroke="#ef4444" strokeWidth={2} dot={{ r: 2 }} />
                    </LineChart>
                  </ResponsiveContainer>
                </div>
              )}
            </>
          ) : (
            <p className="text-sm text-slate-400">
              No daily risk report yet for this patient — it&apos;s generated once the batch pipeline has run.
            </p>
          )}
        </div>
      </section>

      <section>
        <h2 className="mb-2 flex items-center gap-1.5 text-sm font-semibold text-slate-700">
          <FlaskConical className="h-4 w-4" /> Recent Lab Results
        </h2>
        <div className="overflow-hidden rounded-xl border border-slate-200 bg-white">
          <table className="w-full text-left text-sm">
            <thead className="bg-slate-50 text-xs uppercase tracking-wide text-slate-500">
              <tr>
                <th className="px-3 py-2">Test</th>
                <th className="px-3 py-2">Result</th>
                <th className="px-3 py-2">Reference Range</th>
                <th className="px-3 py-2">Collected</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100">
              {(labs.data?.tests ?? []).map((t, i) => (
                <tr key={i} className={t.is_abnormal ? "bg-critical-bg" : undefined}>
                  <td className="px-3 py-2 font-medium text-slate-800">{t.test_type}</td>
                  <td className={`px-3 py-2 font-semibold ${t.is_abnormal ? "text-critical-text" : "text-slate-800"}`}>
                    {t.result_value}
                  </td>
                  <td className="px-3 py-2 text-slate-500">
                    {t.reference_min} – {t.reference_max}
                  </td>
                  <td className="px-3 py-2 text-slate-500">{new Date(t.collected_at).toLocaleString()}</td>
                </tr>
              ))}
              {labs.data && labs.data.tests.length === 0 && (
                <tr>
                  <td colSpan={4} className="px-3 py-4 text-center text-slate-400">
                    No lab results yet for this patient.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}
