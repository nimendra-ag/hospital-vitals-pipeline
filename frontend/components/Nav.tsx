"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import clsx from "clsx";
import { HeartPulse } from "lucide-react";

const LINKS = [
  { href: "/", label: "Ward Overview" },
  { href: "/trends", label: "Trends" },
  { href: "/alerts", label: "Alerts" },
  { href: "/reports", label: "Daily Report" },
];

export function Nav() {
  const pathname = usePathname();

  return (
    <header className="border-b border-slate-200 bg-white">
      <div className="mx-auto flex max-w-6xl items-center justify-between px-4 py-3">
        <Link href="/" className="flex items-center gap-2 font-bold text-slate-900">
          <HeartPulse className="h-6 w-6 text-rose-500" />
          <span>Ward Patient Monitor</span>
        </Link>
        <nav className="flex gap-1">
          {LINKS.map((link) => {
            const active = link.href === "/" ? pathname === "/" : pathname.startsWith(link.href);
            return (
              <Link
                key={link.href}
                href={link.href}
                className={clsx(
                  "rounded-lg px-3 py-1.5 text-sm font-medium transition",
                  active ? "bg-slate-900 text-white" : "text-slate-600 hover:bg-slate-100",
                )}
              >
                {link.label}
              </Link>
            );
          })}
        </nav>
      </div>
    </header>
  );
}
