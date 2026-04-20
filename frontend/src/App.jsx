import { useState, useRef, useCallback, useEffect } from "react";
import QueryForm from "./components/QueryForm";
import AnswerStream from "./components/AnswerStream";
import SourcesPanel from "./components/SourcesPanel";
import MetricsBadges from "./components/MetricsBadges";
import { useMetrics } from "./hooks/useMetrics";
import "./App.css";

const API_BASE = process.env.REACT_APP_API_URL || "http://localhost:8000";

export default function App() {
  const [streaming, setStreaming]       = useState(false);
  const [tokens, setTokens]             = useState("");
  const [sources, setSources]           = useState([]);
  const [queryId, setQueryId]           = useState(null);
  const [error, setError]               = useState(null);
  const [incomplete, setIncomplete]     = useState(false);
  const [corrected, setCorrected]       = useState(false);
  const eventSourceRef                  = useRef(null);
  const { metrics, refresh: refreshMetrics } = useMetrics(API_BASE);

  // Abort any in-flight stream on unmount
  useEffect(() => () => eventSourceRef.current?.close(), []);

  const handleQuery = useCallback(async (formData) => {
    // Reset state
    setTokens("");
    setSources([]);
    setQueryId(null);
    setError(null);
    setIncomplete(false);
    setCorrected(false);
    setStreaming(true);

    // POST to /query — the response is a text/event-stream
    const res = await fetch(`${API_BASE}/query`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(formData),
    });

    if (!res.ok) {
      setError(`API error: ${res.status} ${res.statusText}`);
      setStreaming(false);
      return;
    }

    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";

    const pump = async () => {
      while (true) {
        const { value, done } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split("\n");
        buffer = lines.pop(); // keep incomplete line in buffer

        for (const line of lines) {
          if (!line.startsWith("data:")) continue;
          const raw = line.slice(5).trim();
          if (!raw) continue;

          let event;
          try { event = JSON.parse(raw); } catch { continue; }

          switch (event.type) {
            case "token":
              setTokens(prev => prev + event.content);
              break;
            case "sources":
              setSources(event.content || []);
              setIncomplete(event.content?.some(s => s.is_complete === 0));
              break;
            case "done":
              setQueryId(event.query_id);
              setStreaming(false);
              refreshMetrics();
              break;
            case "error":
              setError(event.content);
              setStreaming(false);
              break;
            default:
              break;
          }
        }
      }
      setStreaming(false);
    };

    pump().catch(err => {
      setError(err.message);
      setStreaming(false);
    });
  }, [refreshMetrics]);

  const handleAbort = useCallback(() => {
    eventSourceRef.current?.close();
    setStreaming(false);
  }, []);

  return (
    <div className="app">
      <header className="app-header">
        <div className="header-inner">
          <div className="logo">
            <span className="logo-mark">◈</span>
            <span className="logo-text">RAG<span className="logo-ops">Ops</span></span>
          </div>
          <p className="tagline">CloudWatch error intelligence · grounded · evaluated</p>
          {metrics && <MetricsBadges metrics={metrics} />}
        </div>
      </header>

      <main className="app-main">
        <div className="query-col">
          <QueryForm onSubmit={handleQuery} loading={streaming} onAbort={handleAbort} />

          {error && (
            <div className="error-banner">
              <span className="error-icon">⚠</span> {error}
            </div>
          )}

          {incomplete && (
            <div className="warning-banner">
              <span>⚠</span> Some retrieved log chunks were <strong>incomplete</strong>.
              {corrected && " Self-correction was applied — confidence reduced."}
            </div>
          )}

          <AnswerStream tokens={tokens} streaming={streaming} queryId={queryId} />
        </div>

        <aside className="sources-col">
          <SourcesPanel sources={sources} />
        </aside>
      </main>
    </div>
  );
}
