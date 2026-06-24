import React from "react";
import { History } from "lucide-react";

export default function TerminalBar({
  projectRunning,
  projectPaused,
  projectLogs,
  tickerPrefix,
  onShowHistory,
}) {
  const hasLogs = (projectLogs || []).length > 0;
  const lastLog = hasLogs ? projectLogs[projectLogs.length - 1] : "";

  if (!projectRunning) return null;

  return (
    <div className="terminal-bar-wrap">
      <span
        className={`terminal-bar-dot${projectRunning ? " active" : ""}${projectPaused ? " paused" : ""}`}
        title={projectPaused ? "Scraper je pozastaven" : projectRunning ? "Scraper běží" : "Nečinný"}
      />
      <span
        className="terminal-bar-text"
        title={lastLog}
      >
        <span className="debug-prefix">[{projectPaused ? "Pauza" : (tickerPrefix || (projectRunning ? "Crawling" : "Poslední log"))}]</span>{" "}
        {hasLogs ? lastLog : (
          <span className="terminal-bar-empty">Žádný log výstup.</span>
        )}
      </span>
      <button
        className="terminal-bar-history-btn"
        onClick={onShowHistory}
      >
        <History className="ui-icon" aria-hidden="true" /> Historie ({(projectLogs || []).length})
      </button>
    </div>
  );
}