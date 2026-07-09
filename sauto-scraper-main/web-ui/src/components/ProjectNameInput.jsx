import React from "react";
import { Lock, Unlock } from "lucide-react";

export default function ProjectNameInput({ name, customName, onNameChange, onToggleCustom }) {
  return (
    <div className="project-name-input">
      <button
        type="button"
        className={`project-name-lock${customName ? " locked" : ""}`}
        onClick={onToggleCustom}
        title={customName ? "Odemknout — název se bude generovat automaticky" : "Zamknout — název se nebude měnit automaticky"}
      >
        {customName ? <Lock className="ui-icon" /> : <Unlock className="ui-icon" />}
      </button>
      <input
        type="text"
        className="project-name-field"
        value={name}
        onChange={(e) => onNameChange(e.target.value)}
        placeholder="Název projektu..."
        title={customName ? "Vlastní název (ručně zadaný)" : "Automatický název (mění se podle filtrů)"}
      />
    </div>
  );
}