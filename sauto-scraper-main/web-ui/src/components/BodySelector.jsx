import React from "react";

export default function BodySelector({
  bodies,
  selected,
  excluded,
  onToggle,
  filterText,
  onFilterChange,
}) {
  const filtered = bodies.filter(
    (b) =>
      b.label.toLowerCase().includes(filterText.toLowerCase()) ||
      b.value.toLowerCase().includes(filterText.toLowerCase())
  );

  return (
    <div className="catalog-block">
      <div className="catalog-title">Karoserie</div>
      <input
        type="text"
        className="catalog-search"
        placeholder="Filtrovat karoserie..."
        value={filterText}
        onChange={(e) => onFilterChange(e.target.value)}
      />
      <div className="catalog-list">
        {bodies.length === 0 && (
          <div className="catalog-subhead">Načítám karoserie…</div>
        )}
        {filtered.map((b) => {
          const isSelected = selected.includes(b.value);
          const isExcluded = excluded.includes(b.value);
          return (
            <div key={b.value} className={`catalog-item${isExcluded ? " excluded" : ""}`}>
              <span
                className={`catalog-toggle-btn${isSelected ? " checked" : isExcluded ? " excluded" : ""}`}
                onClick={(e) => {
                  e.stopPropagation();
                  onToggle(b.value);
                }}
                title={
                  isSelected
                    ? "✓ Zahrnuto — klikni pro vyloučení"
                    : isExcluded
                    ? "✕ Vyloučeno — klikni pro zrušení"
                    : "Klikni pro zahrnutí"
                }
              >
                {isSelected ? "✓" : isExcluded ? "✕" : ""}
              </span>
              <span>{b.label}</span>
            </div>
          );
        })}
      </div>
    </div>
  );
}