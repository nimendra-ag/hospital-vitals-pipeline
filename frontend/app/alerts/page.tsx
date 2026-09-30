"use client";

import { useState } from "react";
import Link from "next/link";
import { AlertTriangle, Check, ShieldCheck } from "lucide-react";
import { LiveIndicator } from "@/components/LiveIndicator";
import { api } from "@/lib/api";
import { formatLocalTime } from "@/lib/clinical";
import { usePolling } from "@/lib/usePolling";
import type { Alert } from "@/lib/types";

export default function AlertsPage() {
  const alerts = usePolling(api.activeAlerts, 5000);
  const [acking, setAcking] = useState<Set<number>>(new Set());

  async function acknowledge(id: number) {
    setAcking((prev) => new Set(prev).add(id));
    try {
      await api.acknowledgeAlert(id);
      alerts.refresh();
    } finally {
      setAcking((prev) => {
        const next = new Set(prev);
        next.delete(id);
        return next;
      });
    }
  }

  const list = alerts.data?.alerts ?? [];
  const critical = list.filter((a) => a.severity === "CRITICAL");
  const warning = list.filter((a) => a.severity === "WARNING");

  return (
    <div>
      <div className="mb-4 flex items-center justify-between">
        <div>
          <h1 className="text-xl font-bold text-slate-900">Active Alerts</h1>
          <p className="text-sm text-slate-500">Vital-sign threshold breaches that haven&apos;t been acknowledged yet.</p>
        </div>
        <LiveIndicator live={alerts.live} />
      </div>

      {list.length === 0 && !alerts.loading && (
        <div className="flex items-center gap-2 rounded-xl border border-normal-border bg-normal-bg p-4 text-normal-text">
          <ShieldCheck className="h-5 w-5" />
          No active alerts right now — every patient is within their acknowledged thresholds.
        </div>
      )}

      {critical.length > 0 && (
        <Section title="Critical" tone="critical" alerts={critical} acking={acking} onAck={acknowledge} />
      )}
      {warning.length > 0 && (
        <Section title="Warning" tone="watch" alerts={warning} acking={acking} onAck={acknowledge} />
      )}
    </div>
  );
}

function Section({
  title,
  tone,
  alerts,
  acking,
  onAck,
}: {
  title: string;
  tone: "critical" | "watch";
  alerts: Alert[];
  acking: Set<number>;
  onAck: (id: number) => void;
}) {
  const border = tone === "critical" ? "border-critical-border" : "border-watch-border";
  const bg = tone === "critical" ? "bg-critical-bg" : "bg-watch-bg";
  const text = tone === "critical" ? "text-critical-text" : "text-watch-text";

  return (
    <div className="mb-4">
      <h2 className={`mb-2 flex items-center gap-1.5 text-sm font-semibold ${text}`}>
        <AlertTriangle className="h-4 w-4" /> {title} ({alerts.length})
      </h2>
      <div className="space-y-2">
        {alerts.map((a) => (
          <div key={a.id} className={`flex items-center justify-between gap-3 rounded-xl border ${border} ${bg} p-3`}>
            <div>
              <p className={`text-sm font-semibold ${text}`}>
                <Link href={`/patients/${a.patient_id}`} className="underline decoration-dotted underline-offset-2">
                  {a.patient_id}
                </Link>{" "}
                · {a.alert_type.replace(/_/g, " ").toLowerCase()}
              </p>
              <p className="text-sm text-slate-700">{a.message}</p>
              <p className="mt-0.5 text-xs text-slate-500">
                First {formatLocalTime(a.triggered_at)}
                {a.occurrences > 1 && (
                  <>
                    {" "}· <span className="font-semibold">repeated {a.occurrences}×</span>, last{" "}
                    {formatLocalTime(a.last_seen_at)}
                  </>
                )}
              </p>
            </div>
            <button
              onClick={() => onAck(a.id)}
              disabled={acking.has(a.id)}
              className="flex shrink-0 items-center gap-1 rounded-lg bg-white px-3 py-1.5 text-xs font-semibold text-slate-700 shadow-sm ring-1 ring-slate-200 transition hover:bg-slate-50 disabled:opacity-50"
            >
              <Check className="h-3.5 w-3.5" />
              {acking.has(a.id) ? "Acknowledging…" : "Acknowledge"}
            </button>
          </div>
        ))}
      </div>
    </div>
  );
}
