import React, { memo, useMemo } from "react";
import { TableVirtuoso } from "react-virtuoso";
import { CustomCheckbox } from "./index";
import { Star } from "lucide-react";

const LEADING_COLUMN_WIDTHS = ["42px", "42px"];
const SCORE_GREEN_MIN = 620;
const SCORE_ORANGE_MIN = 540;

const RESULT_COLUMNS = [
  {
    key: "score",
    label: "Skore",
    width: "82px",
    sortKey: "score",
    render: (item, cachedScore, scoreClass) => (
      <>
        <span className={`score ${scoreClass}`} title={item._scoreTooltip || "Skore bez detailu"}>
          {cachedScore}
        </span>
        {item._suspicious && (
          <span
            className="suspicious-badge"
            title="Podezrely najezd: auto starsi 10 let s mene nez 80 000 km, nebo prumer pod 3 000 km/rok"
          >
            ⚠️
          </span>
        )}
      </>
    ),
  },
  {
    key: "name",
    label: "Nazev",
    width: "180px",
    sortKey: "name",
    className: "name-cell",
    render: (item) => item.name || "-",
  },
  {
    key: "price",
    label: "Cena (Kc)",
    width: "108px",
    sortKey: "price",
    render: (item) => item._fmt_price ?? "-",
  },
  {
    key: "power_kw",
    label: "kW",
    width: "72px",
    sortKey: "power_kw",
    render: (item) => item.power_kw ?? "-",
  },
  {
    key: "tachometer",
    label: "Km",
    width: "100px",
    sortKey: "tachometer",
    render: (item) => item._fmt_tacho ?? "-",
  },
  {
    key: "age_years",
    label: "Stari",
    width: "72px",
    sortKey: "age_years",
    render: (item) => (item.age_years != null ? `${item.age_years} let` : "-"),
  },
  {
    key: "fuel_seo",
    label: "Palivo",
    width: "88px",
    sortKey: "fuel_seo",
    render: (item) => item.fuel_seo || "-",
  },
  {
    key: "body_seo",
    label: "Karoserie",
    width: "96px",
    sortKey: "body_seo",
    render: (item) => item.body_seo || "-",
  },
  {
    key: "drive_type",
    label: "Pohon",
    width: "92px",
    sortKey: "drive_type",
    render: (item) => item.drive_type || "-",
  },
  {
    key: "gearbox_type",
    label: "Prevod.",
    width: "92px",
    sortKey: "gearbox_type",
    render: (item) => item.gearbox_type || "-",
  },
  {
    key: "price_per_kw",
    label: "Kc/kW",
    width: "84px",
    sortKey: "price_per_kw",
    render: (item) => item._fmt_ppkw ?? "-",
  },
  {
    key: "price_per_km",
    label: "Kc/km",
    width: "84px",
    sortKey: "price_per_km",
    render: (item) => item._fmt_ppkm ?? "-",
  },
  {
    key: "km_per_year",
    label: "Km/rok",
    width: "84px",
    sortKey: "km_per_year",
    render: (item) => item._fmt_kpy ?? "-",
  },
  {
    key: "annual_total_cost",
    label: "Naklady/rok",
    width: "100px",
    sortKey: "annual_total_cost",
    render: (item) => item._fmt_atc ?? "-",
  },
  {
    key: "months_to_stk",
    label: "STK (mes.)",
    width: "90px",
    sortKey: "months_to_stk",
    render: (item) => (item.months_to_stk != null ? item.months_to_stk : "-"),
  },
  {
    key: "listing_age_days",
    label: "Inzerat (d)",
    width: "94px",
    sortKey: "listing_age_days",
    render: (item) => (item.listing_age_days != null ? item.listing_age_days : "-"),
  },
  {
    key: "url",
    label: "Odkaz",
    width: "48px",
    render: (item) =>
      item.url ? (
        <a href={item.url} target="_blank" rel="noreferrer" className="link">
          ↗
        </a>
      ) : (
        "-"
      ),
  },
];

export const RESULT_COLUMN_TOGGLE_OPTIONS = RESULT_COLUMNS.map(({ key, label }) => ({ key, label }));

function getScoreTone(score) {
  if (score >= SCORE_GREEN_MIN) return "hi";
  if (score >= SCORE_ORANGE_MIN) return "mid";
  return "lo";
}

const ResultsTable = memo(function ResultsTable({
  visibleItems,
  visibleColumnKeys,
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
  const activeColumns = useMemo(() => {
    const selected = new Set(
      Array.isArray(visibleColumnKeys) && visibleColumnKeys.length > 0
        ? visibleColumnKeys
        : RESULT_COLUMNS.map((col) => col.key)
    );
    return RESULT_COLUMNS.filter((col) => selected.has(col.key));
  }, [visibleColumnKeys]);

  const colWidths = useMemo(
    () => [...LEADING_COLUMN_WIDTHS, ...activeColumns.map((col) => col.width)],
    [activeColumns]
  );

  const virtuosoComponents = useMemo(
    () => ({
      Table: ({ children, ...props }) => (
        <table {...props} className="results-table">
          <colgroup>
            {colWidths.map((width, index) => (
              <col key={index} style={{ width }} />
            ))}
          </colgroup>
          {children}
        </table>
      ),
    }),
    [colWidths]
  );

  if (visibleItems.length === 0) {
    return <p className="empty">Žádné výsledky — spusť scraper a obnov.</p>;
  }

  const header = (
    <tr>
      <th className="cell-check">
        <CustomCheckbox checked={allVisibleSelected} onChange={toggleSelectVisible} size="sm" />
      </th>
      <th className="cell-mark"></th>
      {activeColumns.map((col) => (
        <th
          key={col.key}
          className={col.sortKey ? "sortable-th" : undefined}
          onClick={col.sortKey ? () => toggleSort(col.sortKey) : undefined}
        >
          {col.label}
          {col.sortKey && <span>{sortIndicator(col.sortKey)}</span>}
        </th>
      ))}
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
              {activeColumns.map((col) => (
                <td key={`${col.key}-${index}`} className={col.className}>
                  {col.render(item, cachedScore, scoreClass)}
                </td>
              ))}
            </>
          );
        }}
      />
    </div>
  );
});

export default ResultsTable;