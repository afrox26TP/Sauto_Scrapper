import React from "react";

export default function EquipmentSelector({
  equipment,
  selected,
  excluded,
  onToggle,
  filterText,
  onFilterChange,
}) {
  const filtered = equipment.filter(
    (e) =>
      e.label.toLowerCase().includes(filterText.toLowerCase()) ||
      e.value.toLowerCase().includes(filterText.toLowerCase())
  );

  return (
    <div className="catalog-block">
      <div className="catalog-title">Výbava</div>
      <input
        type="text"
        className="catalog-search"
        placeholder="Filtrovat výbavu..."
        value={filterText}
        onChange={(e) => onFilterChange(e.target.value)}
      />
      <div className="catalog-list">
        {equipment.length === 0 && (
          <div className="catalog-subhead">Načítám výbavu…</div>
        )}
        {filtered.map((e) => {
          const isSelected = selected.includes(e.value);
          const isExcluded = excluded.includes(e.value);
          return (
            <div key={e.value} className={`catalog-item${isExcluded ? " excluded" : ""}`}>
              <span
                className={`catalog-toggle-btn${isSelected ? " checked" : isExcluded ? " excluded" : ""}`}
                onClick={(ev) => {
                  ev.stopPropagation();
                  onToggle(e.value);
                }}
                title={
                  isSelected
                    ? "✓ Vyžadováno — klikni pro vyloučení"
                    : isExcluded
                    ? "✕ Vyloučeno — klikni pro zrušení"
                    : "Klikni pro vyžadování"
                }
              >
                {isSelected ? "✓" : isExcluded ? "✕" : ""}
              </span>
              <span>{e.label}</span>
            </div>
          );
        })}
      </div>
    </div>
  );
}