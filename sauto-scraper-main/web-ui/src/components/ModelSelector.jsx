import React from "react";

export default function ModelSelector({
  selectedBrands,
  modelsByBrand,
  loadingModelsByBrand,
  selected,
  excluded,
  onToggle,
  filterText,
  onFilterChange,
}) {
  if (selectedBrands.length === 0) {
    return (
      <div className="catalog-block">
        <div className="catalog-title">Modely</div>
        <div className="catalog-list">
          <div className="catalog-placeholder">Nejprve vyberte značku</div>
        </div>
      </div>
    );
  }

  return (
    <div className="catalog-block models">
      <div className="catalog-title">Modely (pro vybrané značky)</div>
      <input
        type="text"
        className="catalog-search"
        placeholder="Filtrovat modely..."
        value={filterText}
        onChange={(e) => onFilterChange(e.target.value)}
      />
      <div className="catalog-list">
        {selectedBrands.flatMap((brand) => {
          const models = modelsByBrand[brand] || [];
          const loadingModels = loadingModelsByBrand[brand];
          if (loadingModels) {
            return [
              <div key={`loading-${brand}`} className="catalog-subhead">
                {brand} · načítám...
              </div>,
            ];
          }
          const filtered = models.filter(
            (m) =>
              m.label.toLowerCase().includes(filterText.toLowerCase()) ||
              m.value.toLowerCase().includes(filterText.toLowerCase())
          );
          return [
            <div key={`head-${brand}`} className="catalog-subhead">
              {brand}
            </div>,
            ...filtered.map((m) => {
              const isExcluded = excluded.includes(m.value);
              return (
                <div
                  key={`${brand}-${m.value}`}
                  className={`catalog-item model${isExcluded ? " excluded" : ""}`}
                >
                  <span
                    className={`catalog-toggle-btn${selected.includes(m.value) ? " checked" : isExcluded ? " excluded" : ""}`}
                    onClick={(e) => {
                      e.stopPropagation();
                      onToggle(m.value);
                    }}
                    title={
                      selected.includes(m.value)
                        ? "✓ Zahrnuto — klikni pro vyloučení"
                        : isExcluded
                        ? "✕ Vyloučeno — klikni pro zrušení"
                        : "Klikni pro zahrnutí"
                    }
                  >
                    {selected.includes(m.value) ? "✓" : isExcluded ? "✕" : ""}
                  </span>
                  <span>{m.label}</span>
                </div>
              );
            }),
          ];
        })}
      </div>
    </div>
  );
}