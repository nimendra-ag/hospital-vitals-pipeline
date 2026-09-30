"use client";

import { Line, LineChart, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";
import { VITAL_LABELS, VITAL_UNITS, type VitalKey } from "@/lib/clinical";
import type { VitalsWindow } from "@/lib/types";

const NORMAL_RANGE: Record<VitalKey, [number, number]> = {
  heart_rate: [60, 100],
  spo2: [95, 100],
  systolic_bp: [90, 140],
  diastolic_bp: [60, 90],
  temperature: [36.1, 37.2],
};

export function VitalsTrendChart({ windows, vitalKey }: { windows: VitalsWindow[]; vitalKey: VitalKey }) {
  const data = [...windows]
    .reverse()
    .map((w) => ({
      time: new Date(w.window_end).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" }),
      value: w[vitalKey] as number | null,
    }));

  const [low, high] = NORMAL_RANGE[vitalKey];

  return (
    <div className="rounded-xl border border-slate-200 bg-white p-3">
      <p className="mb-1 text-xs font-medium uppercase tracking-wide text-slate-500">
        {VITAL_LABELS[vitalKey]} trend ({VITAL_UNITS[vitalKey]})
      </p>
      {data.length === 0 ? (
        <div className="flex h-40 items-center justify-center text-sm text-slate-400">No readings yet</div>
      ) : (
        <ResponsiveContainer width="100%" height={160}>
          <LineChart data={data} margin={{ top: 8, right: 12, left: -18, bottom: 0 }}>
            <XAxis dataKey="time" tick={{ fontSize: 10 }} interval="preserveStartEnd" />
            <YAxis tick={{ fontSize: 10 }} domain={["dataMin - 5", "dataMax + 5"]} />
            <Tooltip />
            <ReferenceLine y={low} stroke="#10b981" strokeDasharray="4 4" />
            <ReferenceLine y={high} stroke="#10b981" strokeDasharray="4 4" />
            <Line type="monotone" dataKey="value" stroke="#334155" strokeWidth={2} dot={{ r: 2 }} connectNulls />
          </LineChart>
        </ResponsiveContainer>
      )}
    </div>
  );
}
