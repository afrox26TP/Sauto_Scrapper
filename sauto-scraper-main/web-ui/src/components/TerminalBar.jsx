import React from "react";
import { History } from "lucide-react";

export default function TerminalBar({
  scraperRunning,
  globalLogs,
  tickerStep,
  tickerPrefix,
  onShowHistory,
}) {
  const hasLogs = globalLogs.length > 0;
  const lastLog = hasLogs ? globalLogs[globalLogs.length - 1] : "";

  if (!scraperRunning && !hasLogs) return null;

  return (
    <div className="terminal-bar-wrap">
      <span
        className={`terminal-bar-dot${scraperRunning ? " active" : ""}`}
        title={scraperRunning ? "Scraper běží" : "Nečinný"}
      />
      <span
        className="terminal-bar-text"
        title={lastLog}
      >
        <span className="debug-prefix">[{tickerPrefix || (scraperRunning ? "Crawling" : "Poslední log")}]</span>{" "}
        {hasLogs ? lastLog : (
          <span className="terminal-bar-empty">Žádný log výstup.</span>
        )}
      </span>
      <button
        className="terminal-bar-history-btn"
        onClick={onShowHistory}
      >
        <History className="ui-icon" aria-hidden="true" /> Historie ({globalLogs.length})
      </button>
    </div>
  );
}