import clsx from "clsx";
import type { ComponentType } from "react";
import { Heart, Wind, Activity, Thermometer } from "lucide-react";
import { VITAL_LABELS, VITAL_UNITS, classifyVital, type VitalKey } from "@/lib/clinical";

const ICONS: Record<VitalKey, ComponentType<{ className?: string }>> = {
  heart_rate: Heart,
  spo2: Wind,
  systolic_bp: Activity,
  diastolic_bp: Activity,
  temperature: Thermometer,
};

const RING: Record<"normal" | "watch" | "critical", string> = {
  normal: "border-slate-200",
  watch: "border-watch-border bg-watch-bg",
  critical: "border-critical-border bg-critical-bg",
};

export function VitalTile({ vitalKey, value, compact = false }: { vitalKey: VitalKey; value: number | null; compact?: boolean }) {
  const status = classifyVital(vitalKey, value);
  const Icon = ICONS[vitalKey];

  return (
    <div className={clsx("rounded-xl border px-3", compact ? "py-1.5" : "py-2", RING[status])}>
      <div className="flex items-center gap-1.5 text-slate-500">
        <Icon className="h-3.5 w-3.5" />
        <span className="text-[11px] font-medium uppercase tracking-wide">{VITAL_LABELS[vitalKey]}</span>
      </div>
      <div className="mt-0.5 flex items-baseline gap-1">
        <span
          className={clsx("font-bold tabular-nums", compact ? "text-lg" : "text-2xl", {
            "text-critical-text": status === "critical",
            "text-watch-text": status === "watch",
            "text-slate-900": status === "normal",
          })}
        >
          {value === null ? "—" : value.toFixed(vitalKey === "temperature" ? 1 : 0)}
        </span>
        <span className="text-xs text-slate-400">{VITAL_UNITS[vitalKey]}</span>
      </div>
    </div>
  );
}
