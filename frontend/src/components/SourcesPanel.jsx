import { useState } from "react";

// ─── SourcesPanel ─────────────────────────────────────────────────────────────
export default function SourcesPanel({ sources }) {
  if (!sources.length) {
    return (
      <div className="sources-panel">
        <p className="sources-title">Retrieved context</p>
        <p style={{ fontFamily: "var(--mono)", fontSize: 11, color: "var(--text-muted)" }}>
          Sources will appear here after a query runs.
        </p>
      </div>
    );
  }

  return (
    <div className="sources-panel">
      <p className="sources-title">Retrieved context · {sources.length} chunks</p>
      {sources.map((s, i) => (
        <SourceCard key={s.chunk_id || i} source={s} rank={i + 1} />
      ))}
    </div>
  );
}

function SourceCard({ source, rank }) {
  const [open, setOpen] = useState(rank <= 2); // expand top 2 by default
  const level = source.log_level || "UNKNOWN";
  const score = typeof source.score === "number" ? source.score.toFixed(3) : "—";
  const ts = source.log_timestamp
    ? new Date(source.log_timestamp).toLocaleString(undefined, {
        month: "short", day: "numeric", hour: "2-digit", minute: "2-digit",
      })
    : "—";

  return (
    <div className="source-card">
      <div className="source-card-header" onClick={() => setOpen((o) => !o)}>
        <span className={`level-badge level-${level}`}>{level}</span>
        <span className="source-service">{source.service_name || "unknown"}</span>
        <span className="source-score">d={score}</span>
        {source.is_complete === 0 && (
          <span className="source-incomplete-flag" title={source.completeness_note}>⚠</span>
        )}
        <span style={{ color: "var(--text-muted)", fontSize: 11 }}>{open ? "▴" : "▾"}</span>
      </div>
      {open && (
        <div className="source-card-body">
          <div className="source-meta">
            {ts} · req: {source.request_id?.slice(0, 16) || "—"}
            {source.is_complete === 0 && (
              <span style={{ color: "var(--amber)", marginLeft: 8 }}>
                ⚠ {source.completeness_note}
              </span>
            )}
          </div>
          <pre className="source-text">{source.chunk_text}</pre>
        </div>
      )}
    </div>
  );
}
