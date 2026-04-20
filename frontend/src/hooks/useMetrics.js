import { useState, useEffect, useCallback } from "react";

/**
 * Polls GET /metrics every 60 s and returns live stats.
 * `refresh` can be called manually (e.g. after a query completes).
 */
export function useMetrics(apiBase) {
  const [metrics, setMetrics] = useState(null);

  const refresh = useCallback(async () => {
    try {
      const res = await fetch(`${apiBase}/metrics`);
      if (res.ok) setMetrics(await res.json());
    } catch {
      // non-fatal — metrics are cosmetic
    }
  }, [apiBase]);

  useEffect(() => {
    refresh();
    const id = setInterval(refresh, 60_000);
    return () => clearInterval(id);
  }, [refresh]);

  return { metrics, refresh };
}
