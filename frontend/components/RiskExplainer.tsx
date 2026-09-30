"use client";

import { useState } from "react";
import { ChevronDown, ChevronUp, Info } from "lucide-react";
import type { RiskLevel } from "@/lib/types";

/**
 * Plain-language explanation of the risk model in processing/clinical_rules.py.
 * Keep the numbers here in sync with that file.
 */
export function RiskExplainer() {
  const [open, setOpen] = useState(false);
  return (
    <div className="rounded-xl border border-sky-200 bg-sky-50 text-sm text-slate-700">
      <button onClick={() => setOpen(!open)} className="flex w-full items-center gap-2 px-4 py-2.5 text-left font-semibold text-sky-900">
        <Info className="h-4 w-4" /> How is the risk score calculated?
        {open ? <ChevronUp className="ml-auto h-4 w-4" /> : <ChevronDown className="ml-auto h-4 w-4" />}
      </button>
      {open && (
        <div className="space-y-3 border-t border-sky-200 px-4 py-3">
          <div>
            <p className="font-semibold text-slate-900">1. Vital-signs score (0–100), from the whole simulated day</p>
            <ul className="list-disc pl-5">
              <li>
                Percentage of the day&apos;s readings that crossed a clinical alarm limit (HR &gt;120 or &lt;50, SpO2 &lt;90,
                systolic BP &gt;180 or &lt;80, temperature &gt;38.5 or &lt;35).
              </li>
              <li>+15 if heart rate kept rising (faster than 1.5 bpm per minute over the day).</li>
              <li>+15 if SpO2 kept falling (faster than 0.3 % per minute over the day).</li>
            </ul>
          </div>
          <div>
            <p className="font-semibold text-slate-900">2. Lab score (0–100), from the day&apos;s lab report (5 tests)</p>
            <ul className="list-disc pl-5">
              <li>All 5 tests in range → 0.</li>
              <li>
                The most abnormal test sets the score: just outside its range ≈ 50, one full range-width outside = 100.
              </li>
              <li>+10 for every other abnormal test in the same report (max 100).</li>
            </ul>
          </div>
          <div>
            <p className="font-semibold text-slate-900">3. Combined daily risk</p>
            <p>
              <span className="font-mono">combined = 60 % × vitals score + 40 % × lab score</span> (vitals weigh more: they
              change fastest and need action soonest).
            </p>
            <p className="mt-1">
              LOW &lt; 25 ≤ MODERATE &lt; 50 ≤ HIGH &lt; 75 ≤ CRITICAL. “Improving / Worsening” compares with the previous day
              (a change of 5 points or more).
            </p>
          </div>
          <div>
            <p className="font-semibold text-slate-900">4. “Right now” (live, every few seconds)</p>
            <p>
              A simplified NEWS2 early-warning score from the latest 30-second vitals, plus the 5-minute heart-rate and SpO2
              trend. A patient&apos;s current outlook is the worse of this live picture and the latest daily risk.
            </p>
          </div>
        </div>
      )}
    </div>
  );
}

/** One-line worked calculation for a patient's combined risk. */
export function RiskBreakdown({
  vitals,
  lab,
  combined,
  level,
}: {
  vitals: number;
  lab: number;
  combined: number;
  level: RiskLevel;
}) {
  return (
    <p className="rounded-lg bg-slate-50 px-3 py-2 font-mono text-xs text-slate-700">
      vitals {vitals.toFixed(0)} × 60% = {(vitals * 0.6).toFixed(1)} &nbsp;+&nbsp; lab {lab.toFixed(0)} × 40% ={" "}
      {(lab * 0.4).toFixed(1)} &nbsp;=&nbsp; <span className="font-bold">{combined.toFixed(1)}</span> → {level}
    </p>
  );
}
