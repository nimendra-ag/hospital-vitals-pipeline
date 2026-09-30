import Link from "next/link";
import clsx from "clsx";
import { AlertTriangle } from "lucide-react";
import { StatusBadge } from "./StatusBadge";
import { VitalTile } from "./VitalTile";
import { classifyVital, formatAge, secondsSince, worstStatus } from "@/lib/clinical";
import type { Status, WardPatient } from "@/lib/types";

const BORDER: Record<Status, string> = {
  normal: "border-l-normal-border",
  watch: "border-l-watch-border",
  critical: "border-l-critical-border",
};

export function PatientCard({
  patient,
  alertCount,
  riskStatus,
}: {
  patient: WardPatient;
  alertCount: number;
  riskStatus: Status;
}) {
  const v = patient.latest_vitals;
  const vitalStatuses = [
    classifyVital("heart_rate", v.heart_rate),
    classifyVital("spo2", v.spo2),
    classifyVital("systolic_bp", v.systolic_bp),
    classifyVital("temperature", v.temperature),
  ];
  const alertStatus: Status = alertCount > 0 ? "critical" : "normal";
  const overall = worstStatus(...vitalStatuses, riskStatus, alertStatus);
  const age = secondsSince(v.last_reading);

  return (
    <Link
      href={`/patients/${patient.patient_id}`}
      className={clsx(
        "block rounded-2xl border border-slate-200 border-l-4 bg-white p-4 shadow-sm transition hover:shadow-md",
        BORDER[overall],
      )}
    >
      <div className="flex items-start justify-between gap-2">
        <div>
          <p className="font-semibold text-slate-900">{patient.name}</p>
          <p className="text-xs text-slate-500">
            {patient.ward} · Bed {patient.bed} · {patient.patient_id}
          </p>
        </div>
        <StatusBadge status={overall} size="sm" />
      </div>

      {alertCount > 0 && (
        <div className="mt-2 flex items-center gap-1.5 rounded-lg bg-critical-bg px-2 py-1 text-xs font-medium text-critical-text">
          <AlertTriangle className="h-3.5 w-3.5" />
          {alertCount} active alert{alertCount > 1 ? "s" : ""}
        </div>
      )}

      <div className="mt-3 grid grid-cols-2 gap-2">
        <VitalTile vitalKey="heart_rate" value={v.heart_rate} compact />
        <VitalTile vitalKey="spo2" value={v.spo2} compact />
        <VitalTile vitalKey="systolic_bp" value={v.systolic_bp} compact />
        <VitalTile vitalKey="temperature" value={v.temperature} compact />
      </div>

      <p className="mt-2 text-right text-[11px] text-slate-400">Updated {formatAge(age)}</p>
    </Link>
  );
}
