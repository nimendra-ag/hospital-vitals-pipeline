"use client";

import { useCallback, useEffect, useRef, useState } from "react";

interface PollingState<T> {
  data: T | null;
  error: Error | null;
  loading: boolean;
  /** true once the first successful response has arrived */
  live: boolean;
  refresh: () => void;
}

/**
 * Polls `fetcher` every `intervalMs` and exposes the latest result.
 * A failed poll keeps showing the last good `data` (so the dashboard
 * doesn't flash empty on a single dropped request) but flips `live`
 * to false so the UI can show a "reconnecting" indicator.
 */
export function usePolling<T>(fetcher: () => Promise<T>, intervalMs: number): PollingState<T> {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<Error | null>(null);
  const [loading, setLoading] = useState(true);
  const [live, setLive] = useState(false);
  const fetcherRef = useRef(fetcher);
  fetcherRef.current = fetcher;

  const run = useCallback(() => {
    fetcherRef
      .current()
      .then((result) => {
        setData(result);
        setError(null);
        setLive(true);
        setLoading(false);
      })
      .catch((err) => {
        setError(err instanceof Error ? err : new Error(String(err)));
        setLive(false);
        setLoading(false);
      });
  }, []);

  useEffect(() => {
    run();
    const id = setInterval(run, intervalMs);
    return () => clearInterval(id);
  }, [run, intervalMs]);

  return { data, error, loading, live, refresh: run };
}
