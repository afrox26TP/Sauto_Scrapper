import React, { useRef, useEffect, useMemo, useState } from "react";
import { LoaderCircle, Pause, Play, Square } from "lucide-react";

function fmtDuration(totalSec) {
  const sec = Math.max(0, Math.round(totalSec || 0));
  const h = Math.floor(sec / 3600);
  const m = Math.floor((sec % 3600) / 60);
  const s = sec % 60;
  if (h > 0) return `${h}h ${m}m ${s}s`;
  if (m > 0) return `${m}m ${s}s`;
  return `${s}s`;
}

function fmtCzk(value) {
  return `${Number(value || 0).toFixed(2)} Kč`;
}

export default function ProjectRunning({
  project,
  scraperPaused,
  scraperStartedAt,
  billingRates,
  estimatedTotalSec,
  onPause,
  onResume,
  onStop,
}) {
  const scrollRef = useRef(null);
  const pauseStartedAtRef = useRef(null);
  const [nowMs, setNowMs] = useState(Date.now());
  const [pausedAccumMs, setPausedAccumMs] = useState(0);

  useEffect(() => {
    setPausedAccumMs(0);
    pauseStartedAtRef.current = null;
    setNowMs(Date.now());
  }, [scraperStartedAt]);

  useEffect(() => {
    if (scraperPaused) {
      if (pauseStartedAtRef.current == null) {
        pauseStartedAtRef.current = Date.now();
      }
      return;
    }

    if (pauseStartedAtRef.current != null) {
      const pausedDelta = Date.now() - pauseStartedAtRef.current;
      setPausedAccumMs((prev) => prev + Math.max(0, pausedDelta));
      pauseStartedAtRef.current = null;
      setNowMs(Date.now());
    }
  }, [scraperPaused]);

  useEffect(() => {
    if (scraperPaused) {
      return undefined;
    }
    const t = setInterval(() => setNowMs(Date.now()), 1000);
    return () => clearInterval(t);
  }, [scraperPaused]);

  // Show only logs that belong to this project.
  const allLogs = useMemo(() => {
    const projectLogs = project.logs || [];
    const liveLogs = project.liveLogs || [];
    return [...projectLogs, ...liveLogs].slice(-200);
  }, [project.logs, project.liveLogs]);

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [allLogs]);

  const elapsedSec = useMemo(() => {
    if (typeof scraperStartedAt !== "number") return 0;
    const activeMs = Math.max(0, nowMs - (scraperStartedAt * 1000) - pausedAccumMs);
    return Math.max(0, Math.round(activeMs / 1000));
  }, [nowMs, scraperStartedAt, pausedAccumMs]);

  const totalEstimate = Math.max(30, Math.round(estimatedTotalSec || 180));
  const linearProgress = Math.max(0, Math.min(1, elapsedSec / totalEstimate));
  // Slightly front-load progress so short runs do not look stuck at low percentages.
  const easedProgress = 1 - ((1 - linearProgress) ** 1.7);
  const progressPct = Math.max(0, Math.min(99, Math.round(easedProgress * 100)));
  const etaSec = Math.max(0, totalEstimate - elapsedSec);

  const runBase = 5;
  const itemRate = Number(billingRates?.item_czk ?? 0.02);
  const proxyRate = Number(billingRates?.proxy_run_czk ?? 0.0);
  const runtimePerMin = Number(billingRates?.runtime_min_czk ?? 0.08);
  const itemsNow = Array.isArray(project.results) ? project.results.length : 0;
  const runtimeLiveCost = (elapsedSec / 60) * runtimePerMin;
  const estimatedCostNow = runBase + proxyRate + runtimeLiveCost + (itemsNow * itemRate);

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
        <div className="running-controls">
          {scraperPaused ? (
            <button className="btn-sm" onClick={onResume} title="Pokračovat">
              <Play className="ui-icon" aria-hidden="true" /> Pokračovat
            </button>
          ) : (
            <button className="btn-sm" onClick={onPause} title="Pozastavit">
              <Pause className="ui-icon" aria-hidden="true" /> Pause
            </button>
          )}
          <button className="btn-sm secondary" onClick={onStop} title="Ukončit scraper">
            <Square className="ui-icon" aria-hidden="true" /> Stop
          </button>
        </div>
      </div>

      <div className="running-stats">
        <div className="running-stat-card">
          <span className="running-stat-label">Běží</span>
          <strong>{fmtDuration(elapsedSec)}</strong>
        </div>
        <div className="running-stat-card">
          <span className="running-stat-label">Odhad zbývá</span>
          <strong>{fmtDuration(etaSec)}</strong>
        </div>
        <div className="running-stat-card">
          <span className="running-stat-label">Progress</span>
          <strong>{progressPct}%</strong>
        </div>
        <div className="running-stat-card running-stat-cost">
          <span className="running-stat-label">Odhad nákladů teď (proxy režim)</span>
          <strong>{fmtCzk(estimatedCostNow)}</strong>
        </div>
      </div>

      <div className="running-progress-wrap" aria-label="Odhad průběhu běhu">
        <div className="running-progress-bar" style={{ width: `${progressPct}%` }} />
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