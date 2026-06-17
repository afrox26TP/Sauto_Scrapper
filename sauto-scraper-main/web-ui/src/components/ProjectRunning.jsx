import React, { useRef, useEffect, useMemo } from "react";
import { LoaderCircle } from "lucide-react";

export default function ProjectRunning({ project, globalLogs }) {
  const scrollRef = useRef(null);

  // Combine project logs + global logs
  const allLogs = useMemo(() => {
    const projectLogs = project.logs || [];
    // Show last 200 log lines
    return [...projectLogs, ...(globalLogs || [])].slice(-200);
  }, [project.logs, globalLogs]);

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [allLogs]);

  return (
    <div className="project-running">
      <div className="running-header">
        <LoaderCircle className="ui-icon icon-spin" aria-hidden="true" />
        <span>
          Scraper běží — <strong>{project.name}</strong>
        </span>
        <span className="muted">
          {project.results.length > 0 && ` (${project.results.length} průběžných výsledků)`}
        </span>
      </div>

      <div className="terminal-log" ref={scrollRef}>
        {allLogs.length === 0 ? (
          <div className="terminal-empty">Čekám na výstup scraperu...</div>
        ) : (
          allLogs.map((line, i) => (
            <div
              key={`log-${i}`}
              className={`terminal-line${line.startsWith("[chyba]") || line.toLowerCase().includes("error") ? " error" : ""}${line.startsWith("[systém]") ? " system" : ""}`}
            >
              <span className="terminal-line-num">{i + 1}</span>
              <span className="terminal-line-text">{line}</span>
            </div>
          ))
        )}
      </div>
    </div>
  );
}