import React from "react";
import { Plus, X, Settings, Play, Pause, CheckCircle, AlertCircle, LoaderCircle } from "lucide-react";

function PhaseIcon({ phase }) {
  const size = 14;
  switch (phase) {
    case "config": return <Settings size={size} className="tabbar-icon-config" />;
    case "running": return <LoaderCircle size={size} className="tabbar-icon-running icon-spin" />;
    case "queued": return <Pause size={size} className="tabbar-icon-queued" />;
    case "done": return <CheckCircle size={size} className="tabbar-icon-done" />;
    case "error": return <AlertCircle size={size} className="tabbar-icon-error" />;
    default: return <Settings size={size} />;
  }
}

function phaseTitle(phase, queuePosition) {
  switch (phase) {
    case "config": return "Konfigurace";
    case "running": return "Běží...";
    case "queued": return `Fronta #${queuePosition}`;
    case "done": return "Dokončeno";
    case "error": return "Chyba";
    default: return "";
  }
}

export default React.memo(function TabBar({
  projects,
  activeProjectId,
  onActivate,
  onRemove,
  onAdd,
  scraperRunning,
}) {
  return (
    <div className="tabbar">
      <div className="tabbar-tabs">
        {projects.map((proj) => {
          const isActive = proj.id === activeProjectId;
          return (
            <div
              key={proj.id}
              className={`tabbar-tab${isActive ? " active" : ""}${proj.phase === "running" ? " running" : ""}${proj.phase === "queued" ? " queued" : ""}`}
              onMouseDown={(e) => {
                if (e.button === 0) onActivate(proj.id);
              }}
              onClick={(e) => {
                // Keyboard-triggered click has detail === 0.
                if (e.detail === 0) onActivate(proj.id);
              }}
              title={phaseTitle(proj.phase, proj.queuePosition)}
            >
              <PhaseIcon phase={proj.phase} />
              <span className="tabbar-name">{proj.name}</span>
              {proj.phase !== "running" && proj.phase !== "queued" && (
                <button
                  className="tabbar-close"
                  onClick={(e) => {
                    e.stopPropagation();
                    if (window.confirm(`Opravdu zavřít projekt "${proj.name}"?`)) onRemove(proj.id);
                  }}
                  title="Zavřít projekt"
                >
                  <X size={12} />
                </button>
              )}
            </div>
          );
        })}
      </div>
      <button
        className="tabbar-add"
        onClick={onAdd}
        title="Nový projekt"
        disabled={scraperRunning}
      >
        <Plus size={16} />
        <span>Nový</span>
      </button>
    </div>
  );
});