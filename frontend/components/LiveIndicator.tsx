import clsx from "clsx";

export function LiveIndicator({ live }: { live: boolean }) {
  return (
    <span className="inline-flex items-center gap-1.5 text-xs font-medium text-slate-500">
      <span
        className={clsx("h-2 w-2 rounded-full", live ? "bg-emerald-500 animate-pulse" : "bg-slate-300")}
      />
      {live ? "Live" : "Reconnecting…"}
    </span>
  );
}
