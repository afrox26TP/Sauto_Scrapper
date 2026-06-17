import React from "react";
import { Clock } from "lucide-react";

export default function ProjectQueued({ project }) {
  return (
    <div className="project-queued">
      <div className="queued-card">
        <Clock className="ui-icon" style={{ width: 48, height: 48, opacity: 0.5 }} />
        <h2>
          {project.name}
        </h2>
        <p className="queued-message">
          Projekt čeká ve frontě na spuštění.
        </p>
        <p className="queued-position">
          Pozice ve frontě: <strong>#{project.queuePosition}</strong>
        </p>
        <p className="muted">
          Scraper se spustí automaticky po dokončení aktuálně běžícího projektu.
        </p>
      </div>
    </div>
  );
}