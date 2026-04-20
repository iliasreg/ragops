// ─── MetricsBadges ────────────────────────────────────────────────────────────
export default function MetricsBadges({ metrics }) {
  if (!metrics) return null;

  const faithScore = metrics.mean_faithfulness;
  const faithClass =
    faithScore === null ? "" :
    faithScore >= 0.85  ? "" :
    faithScore >= 0.70  ? "warn" : "danger";

  return (
    <div className="metrics-strip">
      <div className="metric-badge">
        <span className="metric-value">{metrics.total_queries}</span>
        <span className="metric-label">Queries / 24h</span>
      </div>
      <div className="metric-badge">
        <span className={`metric-value ${faithClass}`}>
          {faithScore !== null && faithScore !== undefined
            ? faithScore.toFixed(2)
            : "—"}
        </span>
        <span className="metric-label">Avg faithfulness</span>
      </div>
      <div className="metric-badge">
        <span className={`metric-value ${metrics.low_faithfulness_count > 0 ? "warn" : ""}`}>
          {metrics.low_faithfulness_count}
        </span>
        <span className="metric-label">Low faith alerts</span>
      </div>
      <div className="metric-badge">
        <span className="metric-value">{metrics.self_correction_count}</span>
        <span className="metric-label">Self-corrections</span>
      </div>
    </div>
  );
}
