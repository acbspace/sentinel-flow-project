import { useCallback, useEffect, useRef, useState } from "react";

interface PollingState<T> {
  data: T | null;
  error: string | null;
  loading: boolean;
  refresh: () => void;
}

/**
 * usePolling re-fetches on an interval to keep a view live.
 *
 * Polling rather than websockets: the backend exposes a REST read API and no
 * push channel, so a socket would be a fiction layered over the same requests.
 * A few seconds of staleness is honest and completely adequate for an incident
 * list. If live-ness ever needs to be sub-second, that is a backend change
 * (server-sent events) and this hook is the seam where it would land.
 *
 * `loading` is only true for the first load: subsequent polls refresh in place
 * so the view never flickers back to a spinner while someone is reading it.
 */
export function usePolling<T>(
  fetcher: () => Promise<T>,
  intervalMs: number,
  deps: unknown[],
): PollingState<T> {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [tick, setTick] = useState(0);

  // Keep the latest fetcher without making it a dependency of the effect, which
  // would restart the interval on every render.
  const fetcherRef = useRef(fetcher);
  fetcherRef.current = fetcher;

  const refresh = useCallback(() => setTick((t) => t + 1), []);

  useEffect(() => {
    let cancelled = false;

    const run = async () => {
      try {
        const result = await fetcherRef.current();
        if (cancelled) return;
        setData(result);
        setError(null);
      } catch (err) {
        if (cancelled) return;
        setError(err instanceof Error ? err.message : String(err));
      } finally {
        if (!cancelled) setLoading(false);
      }
    };

    setLoading(true);
    void run();

    const id = window.setInterval(run, intervalMs);
    return () => {
      cancelled = true;
      window.clearInterval(id);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [intervalMs, tick, ...deps]);

  return { data, error, loading, refresh };
}
