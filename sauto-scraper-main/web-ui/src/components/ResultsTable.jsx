import React, { memo } from "react";
import { CustomCheckbox } from "./index";
import { Star } from "lucide-react";

const ResultsTable = memo(function ResultsTable({
  visibleItems,
  scoreCache,
  selectedIds,
  markedIds,
  toggleSelected,
  markSelected,
  toggleSelectVisible,
  allVisibleSelected,
  getCachedScore,
  toggleSort,
  sortIndicator,
  resultKey,
}) {
  if (visibleItems.length === 0) {
    return <p className="empty">Žádné výsledky — spusť scraper a obnov.</p>;
  }

  return (
    <table>
      <thead>
        <tr>
          <th className="cell-check">
            <CustomCheckbox checked={allVisibleSelected} onChange={toggleSelectVisible} size="sm" />
          </th>
          <th className="cell-mark"></th>
          <th className="sortable-th" onClick={() => toggleSort("score")}>
            Skóre <span>{sortIndicator("score")}</span>
          </th>
          <th className="sortable-th" onClick={() => toggleSort("name")}>
            Název <span>{sortIndicator("name")}</span>
          </th>
          <th className="sortable-th" onClick={() => toggleSort("price")}>
            Cena (Kč) <span>{sortIndicator("price")}</span>
          </th>
          <th className="sortable-th" onClick={() => toggleSort("power_kw")}>
            kW <span>{sortIndicator("power_kw")}</span>
          </th>
          <th className="sortable-th" onClick={() => toggleSort("tachometer")}>
            Km <span>{sortIndicator("tachometer")}</span>
          </th>
          <th className="sortable-th" onClick={() => toggleSort("drive_type")}>
            Pohon <span>{sortIndicator("drive_type")}</span>
          </th>
          <th className="sortable-th" onClick={() => toggleSort("gearbox_type")}>
            Převod. <span>{sortIndicator("gearbox_type")}</span>
          </th>
          <th className="sortable-th" onClick={() => toggleSort("price_per_kw")}>
            Kč/kW <span>{sortIndicator("price_per_kw")}</span>
          </th>
          <th className="sortable-th" onClick={() => toggleSort("price_per_km")}>
            Kč/km <span>{sortIndicator("price_per_km")}</span>
          </th>
          <th className="sortable-th" onClick={() => toggleSort("km_per_year")}>
            Km/rok <span>{sortIndicator("km_per_year")}</span>
          </th>
          <th className="sortable-th" onClick={() => toggleSort("annual_total_cost")}>
            Náklady/rok <span>{sortIndicator("annual_total_cost")}</span>
          </th>
          <th></th>
        </tr>
      </thead>
      <tbody>
        {visibleItems.map((item, i) => {
          const key = resultKey(item);
          const selected = selectedIds.includes(key);
          const marked = markedIds.includes(String(item.ad_id));
          const cachedScore = getCachedScore(item);
          const scoreClass =
            cachedScore >= 80 ? "score-hi" : cachedScore >= 50 ? "score-mid" : "score-lo";
          const rowClass = `${selected ? "row-selected" : ""}${marked ? " row-marked" : ""} ${
            cachedScore >= 80 ? "row-score-hi" : cachedScore >= 50 ? "row-score-mid" : "row-score-lo"
          }`;
          return (
            <tr key={item.ad_id || i} className={rowClass}>
              <td className="cell-check">
                <CustomCheckbox checked={selected} onChange={() => toggleSelected(key)} size="sm" />
              </td>
              <td className="cell-mark">
                <button
                  className={`mark-chip${marked ? " marked" : ""}`}
                  onClick={() => markSelected(marked ? false : true)}
                  disabled={!selected && !marked}
                  title={marked ? "Odznačit" : "Označit"}
                >
                  <Star className="ui-icon" aria-hidden="true" />
                </button>
              </td>
              <td>
                <span className={`score ${scoreClass}`}>{cachedScore}</span>
                {item._suspicious && (
                  <span
                    className="suspicious-badge"
                    title="Podezřelý nájezd: auto starší 10 let s méně než 80 000 km, nebo průměr pod 3 000 km/rok"
                  >
                    ⚠️
                  </span>
                )}
              </td>
              <td className="name-cell">{item.name || "—"}</td>
              <td>{item._fmt_price ?? "—"}</td>
              <td>{item.power_kw ?? "—"}</td>
              <td>{item._fmt_tacho ?? "—"}</td>
              <td>{item.drive_type || "—"}</td>
              <td>{item.gearbox_type || "—"}</td>
              <td>{item._fmt_ppkw ?? "—"}</td>
              <td>{item._fmt_ppkm ?? "—"}</td>
              <td>{item._fmt_kpy ?? "—"}</td>
              <td>{item._fmt_atc ?? "—"}</td>
              <td>
                {item.url ? (
                  <a href={item.url} target="_blank" rel="noreferrer" className="link">
                    ↗
                  </a>
                ) : (
                  "—"
                )}
              </td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
});

export default ResultsTable;