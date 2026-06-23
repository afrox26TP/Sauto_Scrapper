import { getItemScoreDetails, isSuspiciousMileage } from "../utils/scoring";

const CS_COLLATOR = new Intl.Collator("cs", { sensitivity: "base", numeric: true });

function resultKey(item) {
  return String(item.ad_id || item.id || item.url || item.name || "");
}

function getSortKey(item, key) {
  if (key === "score") {
    return Number(item._score ?? 0);
  }

  switch (key) {
    case "price":
    case "power_kw":
    case "tachometer":
    case "annual_total_cost":
    case "price_per_kw":
    case "price_per_km":
    case "km_per_year":
      return Number(item?.[key] ?? NaN);
    case "name":
    case "drive_type":
    case "gearbox_type":
      return String(item?.[key] ?? "");
    default:
      return item?.[key];
  }
}

function compareSortKeys(a, b, direction) {
  const aMissing = a === "" || a === null || a === undefined || Number.isNaN(a);
  const bMissing = b === "" || b === null || b === undefined || Number.isNaN(b);
  if (aMissing && bMissing) return 0;
  if (aMissing) return 1;
  if (bMissing) return -1;

  if (typeof a === "number" && typeof b === "number") {
    return (a - b) * direction;
  }

  return CS_COLLATOR.compare(String(a), String(b)) * direction;
}

function formatItems(items, preset) {
  const fmt = (v, style) => {
    if (v == null || !Number.isFinite(v)) return null;
    return v.toLocaleString(
      "cs-CZ",
      style === "ppkw"
        ? { maximumFractionDigits: 2 }
        : style === "ppkm"
          ? { maximumFractionDigits: 4 }
          : undefined
    );
  };

  return (items || []).map((item) => {
    const scoreDetails = getItemScoreDetails(item, preset);
    return {
      ...item,
      _fmt_price: fmt(item.price),
      _fmt_tacho: fmt(item.tachometer),
      _fmt_ppkw: fmt(item.price_per_kw, "ppkw"),
      _fmt_ppkm: fmt(item.price_per_km, "ppkm"),
      _fmt_kpy: fmt(item.km_per_year),
      _fmt_atc: fmt(item.annual_total_cost),
      _suspicious: isSuspiciousMileage(item),
      _score: scoreDetails.score,
      _scoreTooltip: scoreDetails.tooltip,
      _resultKey: resultKey(item),
    };
  });
}

self.onmessage = (event) => {
  const {
    requestId,
    items,
    markedIds,
    sortConfig,
    preset,
  } = event.data || {};

  const formatted = formatItems(items || [], preset || null);

  let visibleItems = formatted;
  if (sortConfig?.key) {
    const direction = sortConfig.direction === "asc" ? 1 : -1;
    const decorated = formatted.map((item) => ({
      item,
      key: getSortKey(item, sortConfig.key),
    }));
    decorated.sort((a, b) => compareSortKeys(a.key, b.key, direction));
    visibleItems = decorated.map((entry) => entry.item);
  }

  self.postMessage({
    requestId,
    visibleItems,
    markedIds: markedIds || [],
  });
};
