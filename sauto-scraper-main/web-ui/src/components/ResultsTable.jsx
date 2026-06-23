import React, { memo } from "react";
import { TableVirtuoso } from "react-virtuoso";
import { CustomCheckbox } from "./index";
import { Star } from "lucide-react";

const COLUMN_WIDTHS = [
  "42px",  // select
  "42px",  // mark
  "82px",  // score
  "180px", // name
  "108px", // price
  "72px",  // power
  "100px", // km
  "92px",  // drive
  "92px",  // gearbox
  "84px",  // price/kw
  "84px",  // price/km
  "84px",  // km/year
  "100px", // annual cost
  "48px",  // link
];

const SCORE_GREEN_MIN = 620;
const SCORE_ORANGE_MIN = 540;

function getScoreTone(score) {
  if (score >= SCORE_GREEN_MIN) return "hi";
  if (score >= SCORE_ORANGE_MIN) return "mid";
  return "lo";
}

const virtuosoComponents = {
  Table: ({ children, ...props }) => (
    <table {...props} className="results-table">
      <colgroup>
        {COLUMN_WIDTHS.map((width, index) => (
          <col key={index} style={{ width }} />
        ))}
      </colgroup>
      {children}
    </table>
  ),
};

const ResultsTable = memo(function ResultsTable({
  visibleItems,
  selectedIdSet,
  markedIdSet,
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

  const header = (
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
  );

  const itemClassName = (_, item) => {
    const key = resultKey(item);
    const selected = selectedIdSet.has(key);
    const marked = markedIdSet.has(String(item.ad_id));
    const cachedScore = getCachedScore(item);
    const tone = getScoreTone(cachedScore);
    return `${selected ? "row-selected" : ""}${marked ? " row-marked" : ""} ${
      tone === "hi" ? "row-score-hi" : tone === "mid" ? "row-score-mid" : "row-score-lo"
    }`;
  };

  return (
    <div className="results-virtuoso-wrap">
      <TableVirtuoso
        components={virtuosoComponents}
        data={visibleItems}
        fixedHeaderContent={() => header}
        itemClassName={itemClassName}
        itemContent={(index, item) => {
          const key = resultKey(item);
          const selected = selectedIdSet.has(key);
          const marked = markedIdSet.has(String(item.ad_id));
          const cachedScore = getCachedScore(item);
          const tone = getScoreTone(cachedScore);
          const scoreClass = tone === "hi" ? "score-hi" : tone === "mid" ? "score-mid" : "score-lo";

          return (
            <>
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
                <span className={`score ${scoreClass}`} title={item._scoreTooltip || "Skore bez detailu"}>
                  {cachedScore}
                </span>
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
            </>
          );
        }}
      />
    </div>
  );
});

export default ResultsTable;