import clsx from "clsx";
import type { Status } from "@/lib/types";

const LABELS: Record<Status, string> = {
  normal: "Normal",
  watch: "Needs Watching",
  critical: "Critical",
};

const STYLES: Record<Status, string> = {
  normal: "bg-normal-bg text-normal-text border-normal-border",
  watch: "bg-watch-bg text-watch-text border-watch-border",
  critical: "bg-critical-bg text-critical-text border-critical-border",
};

export function StatusBadge({ status, size = "md" }: { status: Status; size?: "sm" | "md" | "lg" }) {
  const sizeClass =
    size === "lg" ? "text-sm px-3 py-1.5" : size === "sm" ? "text-[11px] px-2 py-0.5" : "text-xs px-2.5 py-1";

  return (
    <span
      className={clsx(
        "inline-flex items-center gap-1.5 rounded-full border font-semibold whitespace-nowrap",
        STYLES[status],
        sizeClass,
      )}
    >
      <span
        className={clsx("h-1.5 w-1.5 rounded-full", {
          "bg-normal-border": status === "normal",
          "bg-watch-border": status === "watch",
          "bg-critical-border animate-pulse": status === "critical",
        })}
      />
      {LABELS[status]}
    </span>
  );
}
