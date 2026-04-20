// ─── QueryForm ───────────────────────────────────────────────────────────────
import { useState } from "react";

export default function QueryForm({ onSubmit, loading, onAbort }) {
  const now = new Date();
  const yesterday = new Date(now - 24 * 60 * 60 * 1000);

  const fmt = (d) => d.toISOString().slice(0, 16);

  const [form, setForm] = useState({
    question:         "",
    start_ts:         fmt(yesterday),
    end_ts:           fmt(now),
    log_level_filter: "",
    service_filter:   "",
    top_k:            8,
  });

  const set = (k) => (e) => setForm((prev) => ({ ...prev, [k]: e.target.value }));

  const handleSubmit = (e) => {
    e.preventDefault();
    if (!form.question.trim()) return;
    onSubmit({
      ...form,
      log_level_filter: form.log_level_filter || null,
      service_filter:   form.service_filter   || null,
      top_k:            Number(form.top_k),
    });
  };

  return (
    <form className="query-form" onSubmit={handleSubmit}>
      <label className="form-label">
        Engineer query
        <textarea
          className="query-textarea"
          value={form.question}
          onChange={set("question")}
          placeholder="e.g. Why are Lambda invocations timing out in payment-service? What is the root cause and suggested fix?"
          disabled={loading}
          rows={4}
        />
      </label>

      <div className="form-row">
        <label className="form-label" style={{ flex: 1 }}>
          Start (UTC)
          <input
            type="datetime-local"
            className="form-input"
            value={form.start_ts}
            onChange={set("start_ts")}
            disabled={loading}
          />
        </label>
        <label className="form-label" style={{ flex: 1 }}>
          End (UTC)
          <input
            type="datetime-local"
            className="form-input"
            value={form.end_ts}
            onChange={set("end_ts")}
            disabled={loading}
          />
        </label>
      </div>

      <div className="form-row">
        <label className="form-label" style={{ flex: 1 }}>
          Level filter
          <select className="form-select" value={form.log_level_filter} onChange={set("log_level_filter")} disabled={loading}>
            <option value="">All levels</option>
            <option value="ERROR">ERROR</option>
            <option value="WARNING">WARNING</option>
            <option value="INFO">INFO</option>
            <option value="DEBUG">DEBUG</option>
          </select>
        </label>
        <label className="form-label" style={{ flex: 1 }}>
          Service filter
          <input
            type="text"
            className="form-input"
            value={form.service_filter}
            onChange={set("service_filter")}
            placeholder="e.g. payment-service"
            disabled={loading}
          />
        </label>
        <label className="form-label" style={{ width: 100 }}>
          Top-K
          <select className="form-select" value={form.top_k} onChange={set("top_k")} disabled={loading}>
            {[4, 6, 8, 10, 15, 20].map((k) => (
              <option key={k} value={k}>{k}</option>
            ))}
          </select>
        </label>
      </div>

      <div className="form-actions">
        <button className="btn-submit" type="submit" disabled={loading || !form.question.trim()}>
          {loading ? "Streaming…" : "▶  Run query"}
        </button>
        {loading && (
          <button className="btn-abort" type="button" onClick={onAbort}>
            ◼  Abort
          </button>
        )}
      </div>
    </form>
  );
}
