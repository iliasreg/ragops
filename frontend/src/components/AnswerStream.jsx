// ─── AnswerStream ─────────────────────────────────────────────────────────────
export function AnswerStream({ tokens, streaming, queryId }) {
  if (!tokens && !streaming) return null;

  // Render markdown-style ## headings as styled elements
  const rendered = tokens.split(/^(## .+)$/m).map((part, i) => {
    if (part.startsWith("## ")) {
      return <h2 key={i}>{part.slice(3)}</h2>;
    }
    return <span key={i}>{part}</span>;
  });

  return (
    <div className="answer-wrap">
      <div className="answer-header">
        {streaming && <span className="pulse" />}
        <span>{streaming ? "Generating response…" : "Response"}</span>
      </div>
      <div className="answer-body">
        {rendered}
        {streaming && <span className="answer-cursor" />}
      </div>
      {queryId && (
        <div className="query-id">
          query_id: {queryId}
        </div>
      )}
    </div>
  );
}

export default AnswerStream;
