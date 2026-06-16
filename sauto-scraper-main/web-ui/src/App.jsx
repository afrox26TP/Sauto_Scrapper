import React, { useCallback, useEffect, useMemo, useRef, useState, memo } from "react";
import {
  ArrowLeft,
  ChevronDown,
  ChevronUp,
  Download,
  History,
  LoaderCircle,
  Menu,
  Moon,
  Play,
  RefreshCw,
  Save,
  Star,
  Sun,
  Trash2,
  Upload,
  X,
} from "lucide-react";
import { CustomCheckbox, CustomSlider, CustomToggle } from "./components";

const API_BASE = "http://localhost:8000";

const LOCAL_SCORING_PRESETS = {
  value: {
    name: "Cena / výkon",
    description: "Nejlepší poměr ceny, výkonu a provozních nákladů.",
    weights: { age: 0.75, mileage: 1.1, price: 1.4, price_power: 1.85, cost: 1.45, consumption: 1.15, power: 0.85, equipment: 0.45, flags: 1.15, sport: 0.25, luxury: 0.15, power_weight: 0.2, sport_badge: 0.1, premium_equipment: 0.15, tco: 1.5 },
  },
  balanced: {
    name: "Balanced",
    description: "Univerzální hodnocení: stav, nájezd, cena, výkon, náklady i výbava.",
    weights: { age: 1, mileage: 1, price: 1, price_power: 1, cost: 1, consumption: 1, power: 0.75, equipment: 0.85, flags: 1, sport: 0.35, luxury: 0.35, power_weight: 0.4, sport_badge: 0.3, premium_equipment: 0.5, tco: 0.6 },
  },
  sport: {
    name: "Sport",
    description: "Priorita: výkon, dynamika, cena za kW, pohon a mladší kusy.",
    weights: { age: 1.05, mileage: 0.75, price: 0.55, price_power: 1.3, cost: 0.55, consumption: 0.35, power: 2.1, equipment: 0.45, flags: 0.8, sport: 1.45, luxury: 0.2, power_weight: 1.6, sport_badge: 1.4, premium_equipment: 0.25, tco: 0.3 },
  },
  luxury: {
    name: "Luxury",
    description: "Priorita: prémiová značka, výbava, komfort a kultivovaný výkon.",
    weights: { age: 1.35, mileage: 0.9, price: 0.25, price_power: 0.45, cost: 0.35, consumption: 0.25, power: 0.8, equipment: 2.1, flags: 0.9, sport: 0.25, luxury: 1.9, power_weight: 0.3, sport_badge: 0.2, premium_equipment: 1.7, tco: 0.3 },
  },
  daily: {
    name: "Daily Driver",
    description: "Spolehlivé auto na každý den s nízkými náklady a rozumným nájezdem.",
    weights: { age: 0.9, mileage: 1.3, price: 1.1, price_power: 0.5, cost: 1.5, consumption: 1.4, power: 0.6, equipment: 1.2, flags: 1.3, sport: 0.15, luxury: 0.25, power_weight: 0.15, sport_badge: 0.05, premium_equipment: 0.3, tco: 1.6 },
  },
  weekend: {
    name: "Weekend Toy",
    description: "Víkendová hračka – výkon, dynamika a radost z jízdy nad všechno.",
    weights: { age: 1.0, mileage: 0.4, price: 0.3, price_power: 1.6, cost: 0.2, consumption: 0.2, power: 2.0, equipment: 0.6, flags: 0.7, sport: 1.8, luxury: 0.4, power_weight: 2.0, sport_badge: 1.8, premium_equipment: 0.2, tco: 0.1 },
  },
  family: {
    name: "Family Hauler",
    description: "Rodinné auto – bezpečnost, prostor, výbava a přijatelné náklady.",
    weights: { age: 1.2, mileage: 1.1, price: 1.0, price_power: 0.4, cost: 1.3, consumption: 1.1, power: 0.5, equipment: 1.8, flags: 1.5, sport: 0.1, luxury: 0.6, power_weight: 0.1, sport_badge: 0.0, premium_equipment: 1.5, tco: 1.2 },
  },
  budget: {
    name: "Budget King",
    description: "Nejlepší poměr cena/užitná hodnota – co nejvíc auta za co nejmíň peněz.",
    weights: { age: 0.6, mileage: 0.8, price: 2.2, price_power: 2.0, cost: 1.8, consumption: 1.5, power: 0.4, equipment: 0.6, flags: 1.0, sport: 0.1, luxury: 0.1, power_weight: 0.1, sport_badge: 0.0, premium_equipment: 0.05, tco: 2.0 },
  },
  tech: {
    name: "Tech & Comfort",
    description: "Moderní technologické auto – výbava, asistenty a komfort na prvním místě.",
    weights: { age: 1.4, mileage: 0.8, price: 0.5, price_power: 0.3, cost: 0.5, consumption: 0.7, power: 0.6, equipment: 2.3, flags: 0.9, sport: 0.2, luxury: 1.4, power_weight: 0.2, sport_badge: 0.1, premium_equipment: 2.2, tco: 0.4 },
  },
};

const PARAM_GROUPS = [
  {
    label: "Hledání",
    fields: [
      { key: "seller_type", type: "select", label: "Prodejce", options: ["", "soukromy", "bazar"] },
      { key: "condition_seo", type: "text", label: "Stav (čárkou)" },
      { key: "operating_lease", type: "boolean", label: "Operativní leasing" },
    ],
  },
  {
    label: "Stránkování",
    fields: [
      { key: "category_id", type: "text", label: "Kategorie ID" },
      { key: "limit", type: "slider", label: "Limit výsledků", min: 1, max: 1000, step: 1 },
      { key: "offset", type: "number", label: "Offset" },
    ],
  },
  {
    label: "Cena (Kč)",
    fields: [
      { key: "price_from", type: "slider", label: "Cena od", min: 0, max: 10000000, step: 10000, fmt: "price" },
      { key: "price_to", type: "slider", label: "Cena do (0 = bez limitu)", min: 0, max: 10000000, step: 10000, fmt: "price" },
    ],
  },
  {
    label: "Technické filtry scraperu",
    fields: [
      { key: "year_from", type: "number", label: "Rok od" },
      { key: "year_to", type: "number", label: "Rok do" },
      { key: "tachometer_from", type: "number", label: "Nájezd od (km)" },
      { key: "tachometer_to", type: "number", label: "Nájezd do (km)" },
      { key: "power_from", type: "number", label: "Výkon od (kW)" },
      { key: "power_to", type: "number", label: "Výkon do (kW)" },
      { key: "fuel_seo", type: "text", label: "Palivo (např. benzin,nafta,hybrid,elektro)" },
      { key: "gearbox_filter", type: "select", label: "Převodovka scraper", options: ["", "manual", "automatic"] },
      { key: "drive_filter", type: "select", label: "Pohon scraper", options: ["", "fwd", "rwd", "awd"] },
    ],
  },
  {
    label: "Hodnocení",
    fields: [
      { key: "interesting_min_score", type: "slider", label: "Min. skóre", min: -1000, max: 300, step: 1 },
      { key: "interesting_top_n", type: "slider", label: "Top N", min: 1, max: 5000, step: 1 },
      { key: "interesting_min_price", type: "slider", label: "Min. cena pro hodnocení", min: 0, max: 500000, step: 5000, fmt: "price" },
    ],
  },
  {
    label: "Preference",
    fields: [
      { key: "allow_automatic", type: "boolean", label: "Povolit automat" },
      { key: "prefer_gearbox", type: "select", label: "Převodovka", options: ["any", "manual", "automatic"] },
      { key: "prefer_drive", type: "select", label: "Pohon", options: ["any", "fwd", "rwd", "awd"] },
      { key: "target_annual_km", type: "slider", label: "Cílových km/rok", min: 5000, max: 40000, step: 1000, fmt: "km" },
    ],
  },
  {
    label: "Tržní analýza",
    fields: [
      { key: "market_min_cohort_size", type: "slider", label: "Min. kohorta", min: 2, max: 50, step: 1 },
      { key: "market_expected_km_per_year", type: "slider", label: "Očekávaných km/rok", min: 5000, max: 40000, step: 1000, fmt: "km" },
      { key: "model_price_min_samples", type: "slider", label: "Min. vzorků modelu", min: 2, max: 30, step: 1 },
      { key: "undervalue_ratio_threshold", type: "slider", label: "Podhodnoceno ≤", min: 0.5, max: 0.99, step: 0.01, fmt: "ratio" },
      { key: "deep_undervalue_ratio_threshold", type: "slider", label: "Velmi podhodnoceno ≤", min: 0.4, max: 0.95, step: 0.01, fmt: "ratio" },
      { key: "overprice_ratio_threshold", type: "slider", label: "Předraženo ≥", min: 1.01, max: 2.0, step: 0.01, fmt: "ratio" },
    ],
  },
  {
    label: "Notifikace",
    fields: [
      { key: "discord_webhook_url", type: "text", label: "Discord webhook URL" },
      { key: "discord_notify_only_new", type: "boolean", label: "Pouze nové" },
    ],
  },
];

const BRAND_LOGOS = {
  skoda: "/logos/skoda.png",
  volkswagen: "/logos/volkswagen.png",
  vw: "/logos/volkswagen.png",
  audi: "/logos/audi.png",
  bmw: "/logos/bmw.png",
  mercedes: "/logos/mercedes-benz.png",
  "mercedes-benz": "/logos/mercedes-benz.png",
  benz: "/logos/mercedes-benz.png",
  ford: "/logos/ford.png",
  toyota: "/logos/toyota.png",
  honda: "/logos/honda.png",
  hyundai: "/logos/hyundai.png",
  kia: "/logos/kia.png",
  mazda: "/logos/mazda.png",
  nissan: "/logos/nissan.png",
  opel: "/logos/opel.png",
  peugeot: "/logos/peugeot.png",
  renault: "/logos/renault.png",
  seat: "/logos/seat.png",
  citroen: "/logos/citroen.png",
  "citroën": "/logos/citroen.png",
  fiat: "/logos/fiat.png",
  volvo: "/logos/volvo.png",
  dacia: "/logos/dacia.png",
  suzuki: "/logos/suzuki.png",
  mitsubishi: "/logos/mitsubishi.png",
  subaru: "/logos/subaru.png",
  porsche: "/logos/porsche.png",
  landrover: "/logos/land-rover.png",
  "land rover": "/logos/land-rover.png",
  jaguar: "/logos/jaguar.png",
  tesla: "/logos/tesla.png",
  mini: "/logos/mini.png",
  ferrari: "/logos/ferrari.png",
  lamborghini: "/logos/lamborghini.png",
  maserati: "/logos/maserati.png",
  alfaromeo: "/logos/alfa-romeo.png",
  "alfa romeo": "/logos/alfa-romeo.png",
  chevrolet: "/logos/chevrolet.png",
  lexus: "/logos/lexus.png",
  infiniti: "/logos/infiniti.png",
  acura: "/logos/acura.png",
  cadillac: "/logos/cadillac.png",
  chrysler: "/logos/chrysler.png",
  dodge: "/logos/dodge.png",
  jeep: "/logos/jeep.png",
  bentley: "/logos/bentley.png",
  astonmartin: "/logos/aston-martin.png",
  "aston martin": "/logos/aston-martin.png",
  mclaren: "/logos/mclaren.png",
  saab: "/logos/saab.png",
  genesis: "/logos/genesis.png",
  smart: "/logos/smart.png",
  lada: "/logos/lada.png",
  alpina: "/logos/alpina.png",
  byd: "/logos/byd.png",
  daewoo: "/logos/daewoo.png",
  daihatsu: "/logos/daihatsu.png",
  ds: "/logos/ds.png",
  hummer: "/logos/hummer.png",
  lancia: "/logos/lancia.png",
  mb: "/logos/mercedes-benz.png",
  polestar: "/logos/polestar.png",
  ram: "/logos/ram.png",
  ssangyong: "/logos/ssangyong.png",
};

const BRAND_FALLBACK_COLORS = {
  skoda: { bg: "#4ba82e", fg: "#fff" },
  volkswagen: { bg: "#1e3a5f", fg: "#fff" },
  vw: { bg: "#1e3a5f", fg: "#fff" },
  audi: { bg: "#000", fg: "#fff" },
  bmw: { bg: "#1c69d4", fg: "#fff" },
  mercedes: { bg: "#1a1a1a", fg: "#fff" },
  "mercedes-benz": { bg: "#1a1a1a", fg: "#fff" },
  benz: { bg: "#1a1a1a", fg: "#fff" },
  ford: { bg: "#003399", fg: "#fff" },
  toyota: { bg: "#eb0a1e", fg: "#fff" },
  honda: { bg: "#cc0000", fg: "#fff" },
  hyundai: { bg: "#003469", fg: "#fff" },
  kia: { bg: "#bb162b", fg: "#fff" },
  mazda: { bg: "#910a2d", fg: "#fff" },
  nissan: { bg: "#c3002f", fg: "#fff" },
  opel: { bg: "#ffcc00", fg: "#000" },
  peugeot: { bg: "#003355", fg: "#fff" },
  renault: { bg: "#ffcc00", fg: "#000" },
  seat: { bg: "#cc0000", fg: "#fff" },
  citroen: { bg: "#da291c", fg: "#fff" },
  "citroën": { bg: "#da291c", fg: "#fff" },
  fiat: { bg: "#c41230", fg: "#fff" },
  volvo: { bg: "#003057", fg: "#fff" },
  dacia: { bg: "#646b52", fg: "#fff" },
  suzuki: { bg: "#003366", fg: "#fff" },
  mitsubishi: { bg: "#e60012", fg: "#fff" },
  subaru: { bg: "#1e3b6f", fg: "#fff" },
  porsche: { bg: "#000", fg: "#fff" },
  landrover: { bg: "#005a3c", fg: "#fff" },
  "land rover": { bg: "#005a3c", fg: "#fff" },
  jaguar: { bg: "#1f1f1f", fg: "#fff" },
  tesla: { bg: "#cc0000", fg: "#fff" },
  mini: { bg: "#000", fg: "#fff" },
  ferrari: { bg: "#ff2800", fg: "#fff" },
  lamborghini: { bg: "#dbb321", fg: "#000" },
  maserati: { bg: "#003893", fg: "#fff" },
  alfaromeo: { bg: "#920037", fg: "#fff" },
  "alfa romeo": { bg: "#920037", fg: "#fff" },
};

const DARK_INVERT_BRANDS = new Set([
  "audi", "bmw", "mercedes", "mercedes-benz", "benz", "porsche",
  "jaguar", "landrover", "land rover", "bentley", "maserati",
  "lexus", "acura", "cadillac", "chrysler", "dodge", "jeep",
  "astonmartin", "aston martin", "mclaren", "alfaromeo", "alfa romeo",
  "genesis", "ferrari", "lamborghini", "smart", "mini", "mb",
]);

function getBrandLogo(brandValue) {
  const raw = (brandValue || "").toLowerCase().trim();
  // direct match
  if (BRAND_LOGOS[raw]) return BRAND_LOGOS[raw];
  // try without hyphens and underscores
  const clean = raw.replace(/[-_\s]+/g, "");
  for (const [k, v] of Object.entries(BRAND_LOGOS)) {
    if (k.replace(/[-_\s]+/g, "") === clean) return v;
  }
  return null;
}

function getBrandFallbackColor(brandValue) {
  const key = (brandValue || "").toLowerCase().trim();
  return BRAND_FALLBACK_COLORS[key] || null;
}

function brandInitials(label) {
  const words = (label || "").trim().split(/\s+/);
  if (words.length >= 2) return (words[0][0] + words[1][0]).toUpperCase();
  return (label || "?").slice(0, 2).toUpperCase();
}

const BASIC_GROUPS = [PARAM_GROUPS[0], PARAM_GROUPS[2]];
const ADVANCED_GROUPS = [PARAM_GROUPS[1], ...PARAM_GROUPS.slice(3)];
const IGNORED_KEYS = new Set([...PARAM_GROUPS.flatMap((g) => g.fields.map((f) => f.key)), "exclude_manufacturer_seo_name", "exclude_model_seo_name", "exclude_body_seo"]);

function fmtVal(val, fmt) {
  const n = parseFloat(val);
  if (isNaN(n)) return val ?? "";
  if (fmt === "price") return n.toLocaleString("cs-CZ") + " Kč";
  if (fmt === "km") return n.toLocaleString("cs-CZ") + " km";
  if (fmt === "ratio") return n.toFixed(2) + "×";
  return n.toLocaleString("cs-CZ");
}

function fmtDate(ts) {
  if (!ts) return "—";
  return new Date(ts * 1000).toLocaleString("cs-CZ");
}

function csvToArray(value) {
  return String(value || "")
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean);
}

function uniq(arr) {
  return Array.from(new Set(arr));
}

// ── Memoized results table – renders only when its data props change ──
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
          <th className="cell-check"><CustomCheckbox checked={allVisibleSelected} onChange={toggleSelectVisible} size="sm" /></th>
          <th className="cell-mark"></th>
          <th className="sortable-th" onClick={() => toggleSort("score")}>Skóre <span>{sortIndicator("score")}</span></th>
          <th className="sortable-th" onClick={() => toggleSort("name")}>Název <span>{sortIndicator("name")}</span></th>
          <th className="sortable-th" onClick={() => toggleSort("price")}>Cena (Kč) <span>{sortIndicator("price")}</span></th>
          <th className="sortable-th" onClick={() => toggleSort("power_kw")}>kW <span>{sortIndicator("power_kw")}</span></th>
          <th className="sortable-th" onClick={() => toggleSort("tachometer")}>Km <span>{sortIndicator("tachometer")}</span></th>
          <th className="sortable-th" onClick={() => toggleSort("drive_type")}>Pohon <span>{sortIndicator("drive_type")}</span></th>
          <th className="sortable-th" onClick={() => toggleSort("gearbox_type")}>Převod. <span>{sortIndicator("gearbox_type")}</span></th>
          <th className="sortable-th" onClick={() => toggleSort("price_per_kw")}>Kč/kW <span>{sortIndicator("price_per_kw")}</span></th>
          <th className="sortable-th" onClick={() => toggleSort("price_per_km")}>Kč/km <span>{sortIndicator("price_per_km")}</span></th>
          <th className="sortable-th" onClick={() => toggleSort("km_per_year")}>Km/rok <span>{sortIndicator("km_per_year")}</span></th>
          <th className="sortable-th" onClick={() => toggleSort("annual_total_cost")}>Náklady/rok <span>{sortIndicator("annual_total_cost")}</span></th>
          <th></th>
        </tr>
      </thead>
      <tbody>
        {visibleItems.map((item, i) => {
          const key = resultKey(item);
          const selected = selectedIds.includes(key);
          const marked = markedIds.includes(String(item.ad_id));
          const cachedScore = getCachedScore(item);
          const scoreClass = cachedScore >= 80 ? "score-hi" : cachedScore >= 50 ? "score-mid" : "score-lo";
          const rowClass = `${selected ? "row-selected" : ""}${marked ? " row-marked" : ""} ${cachedScore >= 80 ? "row-score-hi" : cachedScore >= 50 ? "row-score-mid" : "row-score-lo"}`;
          return (
          <tr key={item.ad_id || i} className={rowClass}>
            <td className="cell-check">
              <CustomCheckbox checked={selected} onChange={() => toggleSelected(key)} size="sm" />
            </td>
            <td className="cell-mark">
              <button className={`mark-chip${marked ? " marked" : ""}`} onClick={() => markSelected(marked ? false : true)} disabled={!selected && !marked} title={marked ? "Odznačit" : "Označit"}>
                <Star className="ui-icon" aria-hidden="true" />
              </button>
            </td>
            <td>
              <span className={`score ${scoreClass}`}>
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
              {item.url
                ? <a href={item.url} target="_blank" rel="noreferrer" className="link">↗</a>
                : "—"}
            </td>
          </tr>
          );
        })}
      </tbody>
    </table>
  );
});

const Field = memo(function Field({ def, value, onChange }) {
  const { key, type, label, min, max, step, options, fmt } = def;
  const raw = value ?? "";

  const formatValue = useMemo(() => {
    if (!fmt) return undefined;
    return (v) => fmtVal(v, fmt);
  }, [fmt]);

  const handleSliderChange = useCallback(
    (val) => onChange(key, String(val)),
    [onChange, key],
  );

  if (type === "slider") {
    const num = parseFloat(raw);
    const safe = isNaN(num) ? min : num;
    return (
      <CustomSlider
        label={label}
        value={safe}
        min={min}
        max={max}
        step={step}
        formatValue={formatValue}
        onChange={handleSliderChange}
      />
    );
  }

  if (type === "boolean") {
    const checked = raw === "true" || raw === true;
    return (
      <div className="field">
        <CustomToggle
          label={label}
          checked={checked}
          onChange={(val) => onChange(key, String(val))}
          id={key}
        />
      </div>
    );
  }

  if (type === "select") {
    return (
      <div className="field">
        <label>{label}</label>
        <select value={raw} onChange={(e) => onChange(key, e.target.value)}>
          {options.map((opt) => (
            <option key={opt} value={opt}>{opt || "— jakýkoliv —"}</option>
          ))}
        </select>
      </div>
    );
  }

  if (type === "number") {
    return (
      <div className="field">
        <label>{label}</label>
        <input type="number" value={raw} onChange={(e) => onChange(key, e.target.value)} />
      </div>
    );
  }

  return (
    <div className="field">
      <label>{label}</label>
      <input type="text" value={raw} onChange={(e) => onChange(key, e.target.value)} />
    </div>
  );
});

export default function App() {
  const [params, setParams] = useState({});
  const [theme, setTheme] = useState(() => {
    const stored = window.localStorage.getItem("sauto_theme");
    return stored === "dark" ? "dark" : "light";
  });
  const [status, setStatus] = useState(null);
  const [items, setItems] = useState([]);
  const [logs, setLogs] = useState([]);
  const [markedIds, setMarkedIds] = useState([]);
  const [selectedIds, setSelectedIds] = useState([]);
  const [resultsPath, setResultsPath] = useState("data/sauto_interesting.json");
  const [showAdvanced, setShowAdvanced] = useState(false);
  const [message, setMessage] = useState("");
  const [loading, setLoading] = useState(false);
  const [initialLoading, setInitialLoading] = useState(true);
  const [runPhase, setRunPhase] = useState("idle");
  const [showLogsModal, setShowLogsModal] = useState(false);
  const [popupLog, setPopupLog] = useState(null);
  const [apiHealth, setApiHealth] = useState(null);
  const [scraperRunningFromResults, setScraperRunningFromResults] = useState(false);
  const [brandOptions, setBrandOptions] = useState([]);
  const [selectedBrands, setSelectedBrands] = useState([]);
  const [selectedModels, setSelectedModels] = useState([]);
  const [excludedBrands, setExcludedBrands] = useState([]);
  const [excludedModels, setExcludedModels] = useState([]);
  const [modelsByBrand, setModelsByBrand] = useState({});
  const [loadingModelsByBrand, setLoadingModelsByBrand] = useState({});
  const [brandFilterText, setBrandFilterText] = useState("");
  const [modelFilterText, setModelFilterText] = useState("");
  const [bodyOptions, setBodyOptions] = useState([]);
  const [selectedBodies, setSelectedBodies] = useState([]);
  const [excludedBodies, setExcludedBodies] = useState([]);
  const [bodyFilterText, setBodyFilterText] = useState("");
  const [tickerStep, setTickerStep] = useState(0);
  const [sortConfig, setSortConfig] = useState({ key: "score", direction: "desc" });
  const [isSidebarHidden, setIsSidebarHidden] = useState(false);
  const [showApiDetails, setShowApiDetails] = useState(false);
  const [showStatusDropdown, setShowStatusDropdown] = useState(false);
  const [scoringPresets, setScoringPresets] = useState(LOCAL_SCORING_PRESETS);
  const [selectedPreset, setSelectedPreset] = useState("balanced");
  const [customPresets, setCustomPresets] = useState({});
  const [showPresetModal, setShowPresetModal] = useState(false);
  const [editingPresetId, setEditingPresetId] = useState(null);
  const [presetForm, setPresetForm] = useState({ name: "", description: "", weights: {}, hard_rejects: [], must_have_equipment: [] });
  const [presetFormEquipInput, setPresetFormEquipInput] = useState("");
  const [presetFormRejectPattern, setPresetFormRejectPattern] = useState("");
  const [presetFormRejectReason, setPresetFormRejectReason] = useState("");
  const [equipmentOptions, setEquipmentOptions] = useState([]);
  const [equipmentFilterText, setEquipmentFilterText] = useState("");
  const [quickEquipMust, setQuickEquipMust] = useState([]);
  const [quickEquipExcl, setQuickEquipExcl] = useState([]);
  const [showQuickEquip, setShowQuickEquip] = useState(false);
  const [quickBodyFilter, setQuickBodyFilter] = useState([]);
  const [quickBodyExcl, setQuickBodyExcl] = useState([]);
  const [showQuickBody, setShowQuickBody] = useState(false);
  const [quickPriceFrom, setQuickPriceFrom] = useState("");
  const [quickPriceTo, setQuickPriceTo] = useState("");
  const [quickYearFrom, setQuickYearFrom] = useState("");
  const [quickYearTo, setQuickYearTo] = useState("");
  const [quickKmFrom, setQuickKmFrom] = useState("");
  const [quickKmTo, setQuickKmTo] = useState("");
  const [quickPowerFrom, setQuickPowerFrom] = useState("");
  const [quickPowerTo, setQuickPowerTo] = useState("");
  const [quickFuel, setQuickFuel] = useState("");
  const [quickGearbox, setQuickGearbox] = useState("");
  const [quickDrive, setQuickDrive] = useState("");
  const [quickBrandSelected, setQuickBrandSelected] = useState([]);
  const [quickBrandExcluded, setQuickBrandExcluded] = useState([]);
  const [quickModelSelected, setQuickModelSelected] = useState([]);
  const [quickModelExcluded, setQuickModelExcluded] = useState([]);
  const [quickBrandFilterText, setQuickBrandFilterText] = useState("");
  const [quickModelFilterText, setQuickModelFilterText] = useState("");
  const [toastMsg, setToastMsg] = useState("");
  const [toastType, setToastType] = useState("");
  const toastTimer = useRef(null);
  const fileInputRef = useRef(null);
  const logsModalBodyRef = useRef(null);
  const prevIsRunningRef = useRef(null);

  const isRunning = Boolean(status?.running);
  const busy = loading || initialLoading || isRunning || runPhase !== "idle";

  useEffect(() => {
    document.documentElement.classList.toggle("theme-dark", theme === "dark");
    window.localStorage.setItem("sauto_theme", theme);
  }, [theme]);

  useEffect(() => {
    if (Object.keys(scoringPresets).length > 0 && !scoringPresets[selectedPreset]) {
      setSelectedPreset(scoringPresets.balanced ? "balanced" : Object.keys(scoringPresets)[0]);
    }
  }, [scoringPresets, selectedPreset]);

  function fmtUptime(s) {
    if (!s && s !== 0) return "—";
    if (s < 60) return `${s}s`;
    if (s < 3600) return `${Math.floor(s / 60)}m ${s % 60}s`;
    return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`;
  }
  const selectedCount = selectedIds.length;

  async function fetchParams() {
    const res = await fetch(`${API_BASE}/api/params`);
    const data = await res.json();
    setParams(data.params || {});
  }

  async function fetchStatus() {
    const res = await fetch(`${API_BASE}/api/status`);
    const data = await res.json();
    setStatus(data);
  }

  async function fetchResults() {
    const res = await fetch(`${API_BASE}/api/results?path=${encodeURIComponent(resultsPath)}`);
    const data = await res.json();
    setItems(data.items || []);
    setMarkedIds(data.marked_ids || []);
    setResultsPath(data.path || resultsPath);
    setScraperRunningFromResults(Boolean(data.scraper_running));
    setSelectedIds((prev) => prev.filter((id) => (data.items || []).some((item) => String(item.ad_id) === String(id))));
  }

  async function fetchLogs() {
    const res = await fetch(`${API_BASE}/api/logs?limit=160`);
    const data = await res.json();
    setLogs(data.lines || []);
  }

  async function fetchApiHealth() {
    try {
      const res = await fetch(`${API_BASE}/api/health`, { signal: AbortSignal.timeout(4000) });
      const data = await res.json();
      setApiHealth(data);
    } catch {
      setApiHealth({ status: "error" });
    }
  }

  async function fetchBrands() {
    try {
      const res = await fetch(`${API_BASE}/api/catalog/brands`, { signal: AbortSignal.timeout(12000) });
      const data = await res.json();
      setBrandOptions(Array.isArray(data.items) ? data.items : []);
    } catch {
      setBrandOptions([]);
    }
  }

  async function fetchEquipment() {
    try {
      const res = await fetch(`${API_BASE}/api/catalog/equipment`, { signal: AbortSignal.timeout(30000) });
      const data = await res.json();
      setEquipmentOptions(Array.isArray(data.items) ? data.items : []);
    } catch {
      setEquipmentOptions([]);
    }
  }

  async function fetchScoringPresets() {
    try {
      const res = await fetch(`${API_BASE}/api/scoring/presets`, { signal: AbortSignal.timeout(4000) });
      const data = await res.json();
      const custom = data.custom || {};
      const merged = { ...LOCAL_SCORING_PRESETS, ...custom };
      setScoringPresets(merged);
      setCustomPresets(custom);
    } catch {
      setScoringPresets(LOCAL_SCORING_PRESETS);
    }
  }

  async function fetchBodies() {
    try {
      const res = await fetch(`${API_BASE}/api/catalog/bodies`, { signal: AbortSignal.timeout(12000) });
      const data = await res.json();
      setBodyOptions(Array.isArray(data.items) ? data.items : []);
    } catch {
      setBodyOptions([]);
    }
  }

  async function fetchModelsForBrand(brand) {
    const b = String(brand || "").trim();
    if (!b || modelsByBrand[b] || loadingModelsByBrand[b]) return;
    setLoadingModelsByBrand((prev) => ({ ...prev, [b]: true }));
    try {
      const res = await fetch(`${API_BASE}/api/catalog/models?brand=${encodeURIComponent(b)}`, { signal: AbortSignal.timeout(12000) });
      const data = await res.json();
      const items = Array.isArray(data.items) ? data.items : [];
      setModelsByBrand((prev) => ({ ...prev, [b]: items }));
    } catch {
      setModelsByBrand((prev) => ({ ...prev, [b]: [] }));
    } finally {
      setLoadingModelsByBrand((prev) => ({ ...prev, [b]: false }));
    }
  }

  function syncFilterParams(brands, models, exclBrands = excludedBrands, exclModels = excludedModels) {
    const brandCsv = brands.join(",");
    const modelCsv = models.join(",");
    const exclBrandCsv = exclBrands.join(",");
    const exclModelCsv = exclModels.join(",");
    setParams((prev) => ({
      ...prev,
      manufacturer_seo_name: brandCsv,
      model_seo_name: modelCsv,
      exclude_manufacturer_seo_name: exclBrandCsv,
      exclude_model_seo_name: exclModelCsv,
    }));
  }

  function toggleBrand(brand) {
    const b = String(brand || "").trim();
    if (!b) return;

    let nextBrands, nextExcluded;
    if (excludedBrands.includes(b)) {
      nextExcluded = excludedBrands.filter((x) => x !== b);
      nextBrands = selectedBrands;
    } else if (selectedBrands.includes(b)) {
      nextBrands = selectedBrands.filter((x) => x !== b);
      nextExcluded = [...excludedBrands, b];
    } else {
      nextBrands = [...selectedBrands, b];
      nextExcluded = excludedBrands;
    }

    const allowedModels = new Set(nextBrands.flatMap((k) => (modelsByBrand[k] || []).map((m) => m.value)));
    const nextModels = selectedModels.filter((m) => allowedModels.has(m));
    const nextExclModels = excludedModels.filter((m) => allowedModels.has(m));

    setSelectedBrands(nextBrands);
    setExcludedBrands(nextExcluded);
    setSelectedModels(nextModels);
    setExcludedModels(nextExclModels);
    syncFilterParams(nextBrands, nextModels, nextExcluded, nextExclModels);

    if (nextBrands.length > 0 && !selectedBrands.includes(b)) {
      fetchModelsForBrand(b).catch(() => null);
    }
  }

  function toggleModel(model) {
    const m = String(model || "").trim();
    if (!m) return;

    let nextModels, nextExcluded;
    if (excludedModels.includes(m)) {
      nextExcluded = excludedModels.filter((x) => x !== m);
      nextModels = selectedModels;
    } else if (selectedModels.includes(m)) {
      nextModels = selectedModels.filter((x) => x !== m);
      nextExcluded = [...excludedModels, m];
    } else {
      nextModels = [...selectedModels, m];
      nextExcluded = excludedModels;
    }

    setSelectedModels(nextModels);
    setExcludedModels(nextExcluded);
    syncFilterParams(selectedBrands, nextModels, excludedBrands, nextExcluded);
  }

  function toggleBody(body) {
    const b = String(body || "").trim();
    if (!b) return;
    let nextSel = [...selectedBodies];
    let nextExcl = [...excludedBodies];
    if (excludedBodies.includes(b)) {
      nextExcl = excludedBodies.filter((x) => x !== b);
    } else if (selectedBodies.includes(b)) {
      nextSel = selectedBodies.filter((x) => x !== b);
      nextExcl = [...excludedBodies, b];
    } else {
      nextSel = [...selectedBodies, b];
    }
    setSelectedBodies(nextSel);
    setExcludedBodies(nextExcl);
    setParams((prev) => ({
      ...prev,
      body_seo: nextSel.join(","),
      exclude_body_seo: nextExcl.join(","),
    }));
  }

  // Quick toggles (side filter without preset)
  function toggleQuickEquip(val) {
    setQuickEquipMust((p) => {
      if (p.includes(val)) {
        const next = p.filter((x) => x !== val);
        setQuickEquipExcl((e) => [...e, val]);
        return next;
      }
      setQuickEquipExcl((e) => e.filter((x) => x !== val));
      return [...p, val];
    });
  }
  function toggleQuickBody(val) {
    setQuickBodyFilter((p) => {
      if (p.includes(val)) {
        const next = p.filter((x) => x !== val);
        setQuickBodyExcl((e) => [...e, val]);
        return next;
      }
      setQuickBodyExcl((e) => e.filter((x) => x !== val));
      return [...p, val];
    });
  }
  function toggleQuickBrand(val) {
    setQuickBrandSelected((p) => {
      if (p.includes(val)) {
        const next = p.filter((x) => x !== val);
        setQuickBrandExcluded((e) => [...e, val]);
        return next;
      }
      setQuickBrandExcluded((e) => e.filter((x) => x !== val));
      return [...p, val];
    });
  }
  function toggleQuickModel(val) {
    setQuickModelSelected((p) => {
      if (p.includes(val)) {
        const next = p.filter((x) => x !== val);
        setQuickModelExcluded((e) => [...e, val]);
        return next;
      }
      setQuickModelExcluded((e) => e.filter((x) => x !== val));
      return [...p, val];
    });
  }

  async function refreshAll() {
    await Promise.all([fetchParams(), fetchStatus(), fetchResults(), fetchLogs(), fetchApiHealth(), fetchScoringPresets()]);
  }

  function resultKey(item) {
    return String(item.ad_id || item.id || item.url || item.name || "");
  }

  function toggleSelected(id) {
    const key = String(id);
    setSelectedIds((prev) =>
      prev.includes(key) ? prev.filter((x) => x !== key) : [...prev, key],
    );
  }

  function toggleSelectVisible() {
    const visibleIds = visibleItems.map((item) => resultKey(item)).filter(Boolean);
    if (visibleIds.length === 0) return;
    const allSelected = visibleIds.every((id) => selectedIds.includes(id));
    setSelectedIds(allSelected ? selectedIds.filter((id) => !visibleIds.includes(id)) : Array.from(new Set([...selectedIds, ...visibleIds])));
  }

  function toggleSort(key) {
    setSortConfig((prev) => {
      if (prev.key !== key) return { key, direction: key === "score" ? "desc" : "asc" };
      return { key, direction: prev.direction === "asc" ? "desc" : "asc" };
    });
  }

  function sortIndicator(key) {
    if (sortConfig.key !== key) return "⇅";
    return sortConfig.direction === "asc" ? "↑" : "↓";
  }

  const DEFAULT_SCORE_WEIGHTS = {
    age: 1,
    mileage: 1,
    price: 1,
    consumption: 1,
    cost: 1,
    price_power: 1,
    power: 0.75,
    equipment: 0.85,
    flags: 1,
    sport: 0.35,
    luxury: 0.35,
    power_weight: 0.4,
    sport_badge: 0.3,
    premium_equipment: 0.5,
    tco: 0.6,
  };

  function num(value, fallback = null) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : fallback;
  }

  function clamp(value, min, max) {
    return Math.max(min, Math.min(max, value));
  }

  function scoreMax(value, bands, fallback = 0) {
    const n = num(value);
    if (n === null || n <= 0) return fallback;
    for (const [max, score] of bands) {
      if (n <= max) return score;
    }
    return bands[bands.length - 1][1];
  }

  function scoreMin(value, bands, fallback = 0) {
    const n = num(value);
    if (n === null || n <= 0) return fallback;
    for (const [min, score] of bands) {
      if (n >= min) return score;
    }
    return bands[bands.length - 1][1];
  }

  function equipmentText(item) {
    return (Array.isArray(item.equipment_list) ? item.equipment_list : [])
      .join(" ")
      .toLowerCase();
  }

  function hasAny(text, patterns) {
    return patterns.some((pattern) => pattern.test(text));
  }

  // ── First pass: raw component scores (0‑100 after normalization) ──
  function calculateScoreComponents(item) {
    const equipment = Array.isArray(item.equipment_list) ? item.equipment_list : [];
    const eqText = equipmentText(item);
    const power = num(item.power_kw, 0);
    const fuel = String(item.fuel_seo || "").toLowerCase();
    const gearbox = String(item.gearbox_type || "").toLowerCase();
    const drive = String(item.drive_type || "").toLowerCase();
    const brandTier = String(item.brand_tier || "").toLowerCase();

    // Raw scores (original scale, before normalization)
    const raw = {
      age: scoreMax(item.age_years, [[2, 78], [5, 62], [8, 43], [12, 24], [16, 6], [20, -18], [999, -35]]),
      mileage: scoreMax(item.tachometer, [[50000, 72], [80000, 58], [140000, 38], [200000, 14], [260000, -12], [9999999, -36]]),
      price: scoreMax(item.price, [[120000, 56], [200000, 40], [350000, 22], [600000, 4], [1000000, -12], [99999999, -30]]),
      price_power: scoreMax(item.price_per_kw, [[1200, 72], [1800, 52], [2600, 32], [3600, 10], [5200, -14], [999999, -36]]),
      power: scoreMin(power, [[220, 72], [170, 56], [130, 36], [100, 16], [75, 2], [55, -12], [0, -28]]),
      cost: scoreMax(item.annual_total_cost, [[35000, 48], [50000, 32], [70000, 14], [95000, -8], [9999999, -28]]),
      consumption: fuel === "elektro"
        ? scoreMax(item.estimated_consumption_per_100km, [[16, 34], [20, 24], [24, 10], [30, -6], [999, -18]])
        : scoreMax(item.estimated_consumption_per_100km, [[5.5, 34], [6.8, 24], [8.0, 10], [9.5, -8], [999, -24]]),
      equipment: 0,
      flags: 0,
      sport: 0,
      luxury: 0,
    };

    // ── equipment (sub‑score, max ~71 → normalized to 100) ──
    if (equipment.length >= 45) raw.equipment += 18;
    else if (equipment.length >= 30) raw.equipment += 11;
    else if (equipment.length >= 18) raw.equipment += 5;

    if (hasAny(eqText, [/adaptivn[ií]\s*tempomat/, /front assist/, /nouzov[eé]\s*brzd/])) raw.equipment += 12;
    if (hasAny(eqText, [/parkovac[ií]\s*kamera/, /360/, /couvac[ií]\s*kamera/])) raw.equipment += 8;
    if (hasAny(eqText, [/parkovac[ií]\s*senzory/])) raw.equipment += 5;
    if (hasAny(eqText, [/apple\s*car\s*play/, /android\s*auto/, /navigace/])) raw.equipment += 8;
    if (hasAny(eqText, [/vyh[rř]ivan[aá]\s*sedadla/, /vyh[rř]ivan[eé]\s*celn[ií]\s*sklo/])) raw.equipment += 6;
    if (hasAny(eqText, [/led\s*sv[eě]tl/, /xenon/, /matrix/])) raw.equipment += 6;
    if (hasAny(eqText, [/mrtv[eé]ho\s*[uú]hlu/, /j[ií]zdn[ií]ho\s*pruhu/, /lane assist/])) raw.equipment += 8;
    raw.equipment = clamp(raw.equipment, 0, 71);

    // ── flags ──
    if (item.service_book) raw.flags += 14;
    if (item.first_owner) raw.flags += 9;
    if (item.tuning) raw.flags -= 28;

    // ── sport ──
    if (power >= 220) raw.sport += 36;
    else if (power >= 170) raw.sport += 28;
    else if (power >= 130) raw.sport += 16;
    else if (power < 75) raw.sport -= 12;
    if (drive === "rwd") raw.sport += 12;
    else if (drive === "awd") raw.sport += 9;
    if (gearbox === "manual") raw.sport += 6;
    if (num(item.price_per_kw, 999999) <= 2200) raw.sport += 10;
    if (item.tuning) raw.sport -= 18;

    // ── luxury ──
    if (brandTier === "premium") raw.luxury += 24;
    else if (brandTier === "budget") raw.luxury -= 6;
    if (gearbox === "automatic") raw.luxury += 10;
    if (hasAny(eqText, [/ko[zž]en[aá]/, /alcantara/, /mas[aá][zž]/])) raw.luxury += 13;
    if (hasAny(eqText, [/panoramatick[aá]\s*st[rř]echa/, /st[rř]e[sš]n[ií]\s*okno/])) raw.luxury += 9;
    if (equipment.length >= 40) raw.luxury += 14;
    else if (equipment.length >= 25) raw.luxury += 8;
    if (num(item.age_years, 99) <= 5) raw.luxury += 12;
    if (power >= 130) raw.luxury += 7;
    raw.luxury = clamp(raw.luxury, -20, 75);

    // ── experimental signals ──
    const listingName = String(item.name || "").toLowerCase();

    const BODY_WEIGHT_KG = {
      hatchback: 1250, liftback: 1400, sedan: 1480, kombi: 1500,
      coupe: 1450, kabriolet: 1550, mpv: 1700, suv: 1850,
      terenni: 2000, "pick-up": 2100, van: 1950,
    };
    const estWeightKg = BODY_WEIGHT_KG[String(item.body_seo || "").toLowerCase()] || 1500;
    const powerPerTonne = power > 0 ? power / (estWeightKg / 1000) : 0;
    raw.power_weight = scoreMin(powerPerTonne, [[180, 40], [145, 30], [115, 18], [88, 6], [62, -4], [0, -12]]);

    const STRONG_BADGE = /(\bamg\b|\bm[1-8]\b|\bm\s?performance\b|\brs\s?\d?\b|\bvrs\b|\bgti\b|\bgtd\b|\bgts\b|\btype[\s-]?r\b|\bsti\b|\bnismo\b|\babarth\b|\bpolestar\b|\bcupra\b|\bgr\b)/;
    const MILD_BADGE = /(m[\s-]?paket|m[\s-]?sport|s[\s-]?line|r[\s-]?line|n[\s-]?line|st[\s-]?line|\bsport\b)/;
    if (STRONG_BADGE.test(listingName)) raw.sport_badge = 24;
    else if (MILD_BADGE.test(listingName) || MILD_BADGE.test(eqText)) raw.sport_badge = 8;
    else raw.sport_badge = 0;

    const PREMIUM_FEATURES = [
      /ko[zž]en|alcantara/,
      /panoramatick|panorama/,
      /matrix/,
      /adaptivn[ií]\s*tempomat/,
      /vzduchov[eé]\s*odpru|pneumatick[eé]\s*odpru|air\s*suspension/,
      /ventilovan|mas[aá][zž]/,
      /head[\s-]?up/,
      /360/,
      /bezkl[ií][cč]|keyless/,
      /pam[eě][tť]\s*sedadel|memory/,
      /ambientn/,
    ];
    const premiumCount = PREMIUM_FEATURES.reduce((c, re) => c + (re.test(eqText) ? 1 : 0), 0);
    raw.premium_equipment =
      premiumCount >= 6 ? 32 : premiumCount >= 4 ? 22 : premiumCount >= 2 ? 12 : premiumCount >= 1 ? 5 : 0;

    const tco5y = num(item.price, 0) + num(item.annual_total_cost, 0) * 5;
    raw.tco = scoreMax(tco5y, [[250000, 40], [400000, 28], [600000, 14], [900000, 0], [1400000, -14], [99999999, -30]]);

    // ── Normalize every component to 0‑100 ──
    // [rawMin, rawMax] known ranges per component
    const RANGES = {
      age: [-35, 78],
      mileage: [-36, 72],
      price: [-30, 56],
      price_power: [-36, 72],
      power: [-28, 72],
      cost: [-28, 48],
      consumption: [-24, 34],  // worst-case for combustion engines
      equipment: [0, 71],
      flags: [-28, 23],
      sport: [-30, 73],
      luxury: [-20, 75],
      power_weight: [-12, 40],
      sport_badge: [0, 24],
      premium_equipment: [0, 32],
      tco: [-30, 40],
    };

    const norm = (val, min, max) => clamp(Math.round(((val - min) / (max - min)) * 100), 0, 100);

    return {
      age: norm(raw.age, ...RANGES.age),
      mileage: norm(raw.mileage, ...RANGES.mileage),
      price: norm(raw.price, ...RANGES.price),
      price_power: norm(raw.price_power, ...RANGES.price_power),
      power: norm(raw.power, ...RANGES.power),
      cost: norm(raw.cost, ...RANGES.cost),
      consumption: norm(raw.consumption, ...RANGES.consumption),
      equipment: norm(raw.equipment, ...RANGES.equipment),
      flags: norm(raw.flags, ...RANGES.flags),
      sport: norm(raw.sport, ...RANGES.sport),
      luxury: norm(raw.luxury, ...RANGES.luxury),
      power_weight: norm(raw.power_weight, ...RANGES.power_weight),
      sport_badge: norm(raw.sport_badge, ...RANGES.sport_badge),
      premium_equipment: norm(raw.premium_equipment, ...RANGES.premium_equipment),
      tco: norm(raw.tco, ...RANGES.tco),
    };
  }

  function getPresetWeights(preset) {
    return preset?.weights || preset?.multipliers || DEFAULT_SCORE_WEIGHTS;
  }

  function getItemScore(item, preset) {
    const components = calculateScoreComponents(item);
    const weights = getPresetWeights(preset);
    const weightedScore = Object.entries(components).reduce((sum, [key, value]) => {
      return sum + value * (weights[key] ?? DEFAULT_SCORE_WEIGHTS[key] ?? 1);
    }, 0);

    return Math.round(weightedScore);
  }

  // ── Suspicious mileage detection ──
  function isSuspiciousMileage(item) {
    const age = num(item.age_years);
    const km = num(item.tachometer);
    if (age === null || km === null || age <= 0) return false;
    // car older than 10 years with less than 80 000 km
    if (age >= 10 && km < 80000) return true;
    // average less than 3 000 km per year
    const kmPerYear = km / age;
    if (kmPerYear < 3000 && km > 0) return true;
    return false;
  }

  function sortValue(item, key) {
    // Special handling for score - apply current preset
    if (key === "score") {
      const preset = scoringPresets[selectedPreset];
      return getItemScore(item, preset);
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
        return String(item?.[key] ?? "").toLowerCase();
      default:
        return item?.[key];
    }
  }

  async function postResultAction(url, body) {
    const res = await fetch(`${API_BASE}${url}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      throw new Error(err.detail || "Akce selhala.");
    }
    return res.json();
  }

  async function deleteSelected() {
    if (selectedCount === 0) return;
    setLoading(true);
    try {
      await postResultAction("/api/results/delete", { ids: selectedIds, path: resultsPath });
      showToast(`Smazáno ${selectedCount} záznamů`, "success");
      setSelectedIds([]);
      await refreshAll();
    } catch (e) {
      showToast(e.message, "error");
    } finally {
      setLoading(false);
    }
  }

  async function clearAllResults() {
    if (!window.confirm("Opravdu vymazat všechna výsledky?")) return;
    setLoading(true);
    try {
      await postResultAction("/api/results/clear", { path: resultsPath });
      showToast("Výsledky smazány", "success");
      setSelectedIds([]);
      await refreshAll();
    } catch (e) {
      showToast(e.message, "error");
    } finally {
      setLoading(false);
    }
  }

  async function markSelected(marked) {
    if (selectedCount === 0) return;
    setLoading(true);
    try {
      await postResultAction("/api/results/mark", { ids: selectedIds, marked });
      showToast(marked ? "Označeno" : "Označení zrušeno", "success");
      await fetchResults();
    } catch (e) {
      showToast(e.message, "error");
    } finally {
      setLoading(false);
    }
  }

  async function exportResults(scope) {
    const visible = visibleItems;
    const exportItems = scope === "selected"
      ? visible.filter((item) => selectedIds.includes(resultKey(item)))
      : visible;

    const blob = new Blob([JSON.stringify(exportItems, null, 2)], { type: "application/json;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = scope === "selected" ? "sauto_selected.json" : "sauto_export.json";
    a.click();
    URL.revokeObjectURL(url);
  }

  async function importResultsFile(file) {
    if (!file) return;
    setLoading(true);
    try {
      const text = await file.text();
      const parsed = JSON.parse(text);
      const itemsToImport = Array.isArray(parsed) ? parsed : parsed.items;
      if (!Array.isArray(itemsToImport)) {
        throw new Error("Soubor musí obsahovat JSON pole nebo objekt s položkou items.");
      }
      await postResultAction("/api/results/import", { items: itemsToImport, path: resultsPath });
      setMessage(`Importováno ${itemsToImport.length} záznamů.`);
      setSelectedIds([]);
      await refreshAll();
    } catch (e) {
      setMessage(e.message);
    } finally {
      setLoading(false);
      if (fileInputRef.current) fileInputRef.current.value = "";
    }
  }

  useEffect(() => {
    setInitialLoading(true);
    refreshAll()
      .catch(() => setMessage("API není dostupné — spusť backend na portu 8000."))
      .finally(() => setInitialLoading(false));
  }, []);

  useEffect(() => {
    fetchBrands().catch(() => null);
    fetchBodies().catch(() => null);
  }, []);

  useEffect(() => {
    const parsedBrands = uniq(csvToArray(params.manufacturer_seo_name));
    const parsedModels = uniq(csvToArray(params.model_seo_name));
    const parsedExclBrands = uniq(csvToArray(params.exclude_manufacturer_seo_name));
    const parsedExclModels = uniq(csvToArray(params.exclude_model_seo_name));
    const parsedBodies = uniq(csvToArray(params.body_seo));
    const parsedExclBodies = uniq(csvToArray(params.exclude_body_seo));
    setSelectedBrands(parsedBrands);
    setSelectedModels(parsedModels);
    setExcludedBrands(parsedExclBrands);
    setExcludedModels(parsedExclModels);
    setSelectedBodies(parsedBodies);
    setExcludedBodies(parsedExclBodies);
  }, [params.manufacturer_seo_name, params.model_seo_name, params.body_seo, params.exclude_body_seo]);

  useEffect(() => {
    selectedBrands.forEach((b) => { fetchModelsForBrand(b).catch(() => null); });
  }, [selectedBrands]);

  useEffect(() => {
    const t = setInterval(() => fetchStatus().catch(() => null), 2000);
    return () => clearInterval(t);
  }, []);

  useEffect(() => {
    const t = setInterval(() => fetchLogs().catch(() => null), 1500);
    return () => clearInterval(t);
  }, []);

  useEffect(() => {
    if (!isRunning) {
      setTickerStep(0);
      return;
    }
    const t = setInterval(() => {
      setTickerStep((prev) => (prev + 1) % 4);
    }, 650);
    return () => clearInterval(t);
  }, [isRunning]);

  useEffect(() => {
    fetchApiHealth().catch(() => null);
    const t = setInterval(() => fetchApiHealth().catch(() => null), 10000);
    return () => clearInterval(t);
  }, []);

  function showToast(msg, type = "") {
    if (toastTimer.current) clearTimeout(toastTimer.current);
    setToastMsg(msg);
    setToastType(type);
    toastTimer.current = setTimeout(() => { setToastMsg(""); setToastType(""); }, 3000);
  }

  const setParam = useCallback((key, val) => {
    setParams((prev) => ({ ...prev, [key]: val }));
  }, []);

  async function save() {
    setLoading(true);
    setMessage("");
    try {
      const res = await fetch(`${API_BASE}/api/params`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ params }),
      });
      if (!res.ok) throw new Error("Uložení selhalo.");
      setMessage("Uloženo.");
    } catch (e) {
      setMessage(e.message);
    } finally {
      setLoading(false);
    }
  }

  // ── Custom preset CRUD ──

  // ── 3-state equipment toggle (same pattern as toggleBrand) ──
  function toggleEquipment(equipValue) {
    const v = String(equipValue || "").trim();
    if (!v) return;
    setPresetForm((prev) => {
      const must = Array.isArray(prev.must_have_equipment) ? prev.must_have_equipment : [];
      const excl = Array.isArray(prev.excluded_equipment) ? prev.excluded_equipment : [];
      let nextMust = must;
      let nextExcl = excl;
      if (excl.includes(v)) {
        // 3rd click: remove from excluded → back to neutral
        nextExcl = excl.filter((x) => x !== v);
      } else if (must.includes(v)) {
        // 2nd click: remove from must, add to excluded
        nextMust = must.filter((x) => x !== v);
        nextExcl = [...excl, v];
      } else {
        // 1st click: add to must
        nextMust = [...must, v];
      }
      return { ...prev, must_have_equipment: nextMust, excluded_equipment: nextExcl };
    });
  }

  function openNewPreset() {
    setEditingPresetId(null);
    setPresetForm({ name: "", description: "", weights: { ...DEFAULT_SCORE_WEIGHTS }, hard_rejects: [], must_have_equipment: [], excluded_equipment: [] });
    setPresetFormEquipInput("");
    setPresetFormRejectPattern("");
    setPresetFormRejectReason("");
    setEquipmentFilterText("");
    fetchEquipment().catch(() => null);
    setShowPresetModal(true);
  }

  function openEditPreset(presetId) {
    const preset = customPresets[presetId];
    if (!preset) return;
    setEditingPresetId(presetId);
    setPresetForm({
      name: preset.name || "",
      description: preset.description || "",
      weights: preset.weights ? { ...DEFAULT_SCORE_WEIGHTS, ...preset.weights } : { ...DEFAULT_SCORE_WEIGHTS },
      hard_rejects: Array.isArray(preset.hard_rejects) ? [...preset.hard_rejects] : [],
      must_have_equipment: Array.isArray(preset.must_have_equipment) ? [...preset.must_have_equipment] : [],
      excluded_equipment: Array.isArray(preset.excluded_equipment) ? [...preset.excluded_equipment] : [],
    });
    setPresetFormEquipInput("");
    setPresetFormRejectPattern("");
    setPresetFormRejectReason("");
    setEquipmentFilterText("");
    fetchEquipment().catch(() => null);
    setShowPresetModal(true);
  }

  async function deleteCustomPreset(presetId) {
    if (!window.confirm(`Opravdu smazat preset "${customPresets[presetId]?.name || presetId}"?`)) return;
    try {
      const res = await fetch(`${API_BASE}/api/scoring/presets/custom/${encodeURIComponent(presetId)}`, { method: "DELETE" });
      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(err.detail || "Smazání selhalo.");
      }
      // If currently selected, switch back to balanced
      if (selectedPreset === presetId) setSelectedPreset("balanced");
      showToast("Preset smazán", "success");
      await fetchScoringPresets();
    } catch (e) {
      showToast(e.message, "error");
    }
  }

  async function savePreset() {
    if (!presetForm.name.trim()) {
      showToast("Zadej název presetu", "error");
      return;
    }
    const payload = {
      name: presetForm.name.trim(),
      description: presetForm.description.trim(),
      weights: presetForm.weights,
      hard_rejects: presetForm.hard_rejects.filter((r) => r.pattern),
      must_have_equipment: (presetForm.must_have_equipment || []).filter(Boolean),
      excluded_equipment: (presetForm.excluded_equipment || []).filter(Boolean),
    };
    try {
      const url = editingPresetId
        ? `/api/scoring/presets/custom/${encodeURIComponent(editingPresetId)}`
        : "/api/scoring/presets/custom";
      const method = editingPresetId ? "PUT" : "POST";
      const res = await fetch(`${API_BASE}${url}`, {
        method,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(err.detail || "Uložení presetu selhalo.");
      }
      const data = await res.json();
      showToast(editingPresetId ? "Preset upraven" : "Preset vytvořen", "success");
      setShowPresetModal(false);
      await fetchScoringPresets();
      // Select the new/edited preset
      setSelectedPreset(data.preset_id);
    } catch (e) {
      showToast(e.message, "error");
    }
  }

  function addPresetEquipTag() {
    const term = presetFormEquipInput.trim().toLowerCase();
    if (!term) return;
    setPresetForm((prev) => ({
      ...prev,
      must_have_equipment: [...prev.must_have_equipment, term],
    }));
    setPresetFormEquipInput("");
  }

  function removePresetEquipTag(index) {
    setPresetForm((prev) => ({
      ...prev,
      must_have_equipment: prev.must_have_equipment.filter((_, i) => i !== index),
    }));
  }

  function addPresetReject() {
    const pattern = presetFormRejectPattern.trim();
    const reason = presetFormRejectReason.trim();
    if (!pattern) return;
    setPresetForm((prev) => ({
      ...prev,
      hard_rejects: [...prev.hard_rejects, { pattern, reason }],
    }));
    setPresetFormRejectPattern("");
    setPresetFormRejectReason("");
  }

  function removePresetReject(index) {
    setPresetForm((prev) => ({
      ...prev,
      hard_rejects: prev.hard_rejects.filter((_, i) => i !== index),
    }));
  }

  function setPresetWeight(key, val) {
    setPresetForm((prev) => ({
      ...prev,
      weights: { ...prev.weights, [key]: parseFloat(val) || 0 },
    }));
  }

  const ALL_WEIGHT_KEYS = [
    { key: "age", label: "Stáří" },
    { key: "mileage", label: "Nájezd" },
    { key: "price", label: "Cena" },
    { key: "price_power", label: "Cena/kW" },
    { key: "power", label: "Výkon" },
    { key: "consumption", label: "Spotřeba" },
    { key: "cost", label: "Provozní náklady" },
    { key: "equipment", label: "Výbava" },
    { key: "flags", label: "Stav/Historie" },
    { key: "sport", label: "Sport" },
    { key: "luxury", label: "Luxus" },
    { key: "power_weight", label: "Výkon/váha" },
    { key: "sport_badge", label: "Sportovní označení" },
    { key: "premium_equipment", label: "Prémiová výbava" },
    { key: "tco", label: "TCO (5 let)" },
  ];

  async function run() {
    setLoading(true);
    setRunPhase("saving");
    setMessage("");
    try {
      const saveRes = await fetch(`${API_BASE}/api/params`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ params }),
      });
      if (!saveRes.ok) throw new Error("Uložení parametrů selhalo.");
      setRunPhase("starting");
      const res = await fetch(`${API_BASE}/api/run`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ output_file: "data/sauto_interesting.json" }),
      });
      if (!res.ok) {
        const err = await res.json();
        throw new Error(err.detail || "Spuštění selhalo.");
      }
      setMessage("Scraper spuštěn.");
      setRunPhase("running");
      await refreshAll();
    } catch (e) {
      setMessage(e.message);
      setRunPhase("error");
    } finally {
      setLoading(false);
    }
  }

  // Advance runPhase UI state when scraper finishes
  useEffect(() => {
    if (runPhase === "idle") return;
    if (isRunning) return;

    if (runPhase === "running") {
      setRunPhase("refreshing");
      fetchResults()
        .catch(() => null)
        .finally(() => setRunPhase("done"));
      return;
    }

    if (runPhase === "done" || runPhase === "error") {
      const t = setTimeout(() => setRunPhase("idle"), 1800);
      return () => clearTimeout(t);
    }
  }, [isRunning, runPhase]);

  // Poll results every 3s while scraper is running (driven by live status, not runPhase)
  useEffect(() => {
    if (!isRunning) return;
    const t = setInterval(() => fetchResults().catch(() => null), 3000);
    return () => clearInterval(t);
  }, [isRunning]);

  // When scraper stops (even externally), fetch final results once
  useEffect(() => {
    if (prevIsRunningRef.current === true && isRunning === false) {
      fetchResults().catch(() => null);
    }
    prevIsRunningRef.current = isRunning;
  }, [isRunning]);

  function statusLabel() {
    if (initialLoading) return "Načítám data…";
    if (runPhase === "saving") return "Ukládám parametry…";
    if (runPhase === "starting") return "Spouštím scraper…";
    if (isRunning) return "Scraper běží…";
    if (runPhase === "running") return "Scraper běží…";
    if (runPhase === "refreshing") return "Aktualizuji výsledky…";
    if (runPhase === "done") return "Hotovo.";
    if (runPhase === "error") return "Spuštění selhalo.";
    return "Připraveno.";
  }

  function tickerPrefix() {
    if (!isRunning) return "Poslední log";
    const phases = ["Crawling", "Načítám", "Zpracovávám", "Kontroluji"];
    const dots = [".", "..", "...", "...."];
    return `${phases[tickerStep]} ${dots[tickerStep]}`;
  }

  // Extra params from params.json that aren't in PARAM_GROUPS (e.g. discord webhook)
  const extraKeys = Object.keys(params).filter((k) => !IGNORED_KEYS.has(k));
  const visibleItems = useMemo(() => {
    // Format numbers once for all items (avoids .toLocaleString() in every cell)
    const fmt = (v, style) => {
      if (v == null || !Number.isFinite(v)) return null;
      if (style === "price") return v.toLocaleString("cs-CZ");
      if (style === "tacho") return v.toLocaleString("cs-CZ");
      if (style === "ppkw") return v.toLocaleString("cs-CZ", { maximumFractionDigits: 2 });
      if (style === "ppkm") return v.toLocaleString("cs-CZ", { maximumFractionDigits: 4 });
      if (style === "kpy") return v.toLocaleString("cs-CZ");
      if (style === "atc") return v.toLocaleString("cs-CZ");
      return v;
    };

    const list = items.map((item) => ({
      ...item,
      _fmt_price: fmt(item.price, "price"),
      _fmt_tacho: fmt(item.tachometer, "tacho"),
      _fmt_ppkw: fmt(item.price_per_kw, "ppkw"),
      _fmt_ppkm: fmt(item.price_per_km, "ppkm"),
      _fmt_kpy: fmt(item.km_per_year, "kpy"),
      _fmt_atc: fmt(item.annual_total_cost, "atc"),
      _suspicious: isSuspiciousMileage(item),
    }));

    if (!sortConfig.key) return list;

    list.sort((a, b) => {
      const av = sortValue(a, sortConfig.key);
      const bv = sortValue(b, sortConfig.key);

      const aMissing = av === "" || av === null || av === undefined || Number.isNaN(av);
      const bMissing = bv === "" || bv === null || bv === undefined || Number.isNaN(bv);
      if (aMissing && bMissing) return 0;
      if (aMissing) return 1;
      if (bMissing) return -1;

      let cmp = 0;
      if (typeof av === "number" && typeof bv === "number") {
        cmp = av - bv;
      } else {
        cmp = String(av).localeCompare(String(bv), "cs");
      }

      return sortConfig.direction === "asc" ? cmp : -cmp;
    });

    return list;
  }, [items, sortConfig, selectedPreset, scoringPresets]);

  // Precompute scores for all visible items once (avoids 2× calculation per row)
  const scoreCache = useMemo(() => {
    const cache = new Map();
    const preset = scoringPresets[selectedPreset];
    if (!preset) return cache;
    for (const item of visibleItems) {
      const key = resultKey(item);
      cache.set(key, getItemScore(item, preset));
    }
    return cache;
  }, [visibleItems, selectedPreset, scoringPresets]);

  // Quick-filtered visible items (applied on top of main visibleItems)
  const quickFilteredItems = useMemo(() => {
    let list = visibleItems;
    // numeric range filters
    const pf = parseFloat(quickPriceFrom);
    const pt = parseFloat(quickPriceTo);
    const yf = parseFloat(quickYearFrom);
    const yt = parseFloat(quickYearTo);
    const kf = parseFloat(quickKmFrom);
    const kt = parseFloat(quickKmTo);
    const pwf = parseFloat(quickPowerFrom);
    const pwt = parseFloat(quickPowerTo);
    const hasRange = !isNaN(pf) || !isNaN(pt) || !isNaN(yf) || !isNaN(yt) || !isNaN(kf) || !isNaN(kt) || !isNaN(pwf) || !isNaN(pwt);
    const hasSelect = quickFuel || quickGearbox || quickDrive;
    const hasEquip = quickEquipMust.length > 0 || quickEquipExcl.length > 0;
    const hasBody = quickBodyFilter.length > 0 || quickBodyExcl.length > 0;
    if (hasRange || hasSelect || hasEquip || hasBody) {
      list = list.filter((item) => {
        const price = item.price ?? 0;
        const year = 2026 - (item.age_years ?? 0);
        const km = item.tachometer ?? 0;
        const power = item.power_kw ?? 0;
        const fuel = String(item.fuel_seo || "").toLowerCase();
        const gearbox = String(item.gearbox_type || "").toLowerCase();
        const drive = String(item.drive_type || "").toLowerCase();
        const eqText = (Array.isArray(item.equipment_list) ? item.equipment_list : []).join(" ").toLowerCase();
        const body = String(item.body_seo || "").toLowerCase();
        if (!isNaN(pf) && price < pf) return false;
        if (!isNaN(pt) && price > pt) return false;
        if (!isNaN(yf) && year < yf) return false;
        if (!isNaN(yt) && year > yt) return false;
        if (!isNaN(kf) && km < kf) return false;
        if (!isNaN(kt) && km > kt) return false;
        if (!isNaN(pwf) && power < pwf) return false;
        if (!isNaN(pwt) && power > pwt) return false;
        if (quickFuel && fuel !== quickFuel) return false;
        if (quickGearbox && gearbox !== quickGearbox) return false;
        if (quickDrive && drive !== quickDrive) return false;
        for (const t of quickEquipMust) { if (!eqText.includes(t.toLowerCase())) return false; }
        for (const t of quickEquipExcl) { if (eqText.includes(t.toLowerCase())) return false; }
        for (const t of quickBodyFilter) { if (body !== t.toLowerCase()) return false; }
        for (const t of quickBodyExcl) { if (body === t.toLowerCase()) return false; }
        return true;
      });
    }
    return list;
  }, [visibleItems, quickEquipMust, quickEquipExcl, quickBodyFilter, quickBodyExcl, quickPriceFrom, quickPriceTo, quickYearFrom, quickYearTo, quickKmFrom, quickKmTo, quickPowerFrom, quickPowerTo, quickFuel, quickGearbox, quickDrive]);

  function getCachedScore(item) {
    return scoreCache.get(resultKey(item)) ?? 0;
  }

  const allVisibleSelected = visibleItems.length > 0 && visibleItems.every((item) => selectedIds.includes(resultKey(item)));

  return (
    <>
    <div className="app">
      <div className="topbar">
        <div className="brand-block">
          <div>
            <h1>Sauto Scraper</h1>
          </div>
        </div>
        <button className="btn-primary" onClick={run} disabled={busy} style={{ marginLeft: 8 }}>
          {busy ? <><LoaderCircle className="ui-icon icon-spin" aria-hidden="true" /> Pracuji…</> : <><Play className="ui-icon" aria-hidden="true" /> Spustit</>}
        </button>

        {/* Status dropdown */}
        <div className="status-chip-wrapper">
          <div
            className={`status-chip${busy ? " running" : ""}${showStatusDropdown ? " open" : ""}`}
            onClick={() => setShowStatusDropdown((v) => !v)}
            title="Klikni pro detail stavu"
          >
            <span className={`mini-dot${busy ? " running" : ""}`} />
            <span className="status-chip-label">{statusLabel()}</span>
            <ChevronDown className="ui-icon" style={{ width: 12, height: 12 }} />
          </div>
          {showStatusDropdown && (
            <div className="status-detail-dropdown">
              <div className="status-detail-row"><span className="status-detail-lbl">Stav</span> <span className="status-detail-val">{statusLabel()}</span></div>
              <div className="status-detail-row"><span className="status-detail-lbl">Start</span> <span className="status-detail-val">{fmtDate(status?.last_started_at)}</span></div>
              <div className="status-detail-row"><span className="status-detail-lbl">Konec</span> <span className="status-detail-val">{fmtDate(status?.last_finished_at)}</span></div>
              <div className="status-detail-row"><span className="status-detail-lbl">Exit</span> <span className="status-detail-val">{status?.last_exit_code ?? "—"}</span></div>
              <div className="status-detail-row"><span className="status-detail-lbl">PID</span> <span className="status-detail-val">{status?.pid || "—"}</span></div>
            </div>
          )}
        </div>

        <div className="topbar-spacer" />
        <button
          type="button"
          className="theme-toggle"
          onClick={() => setIsSidebarHidden((prev) => !prev)}
          title={isSidebarHidden ? "Zobrazit panel filtrů" : "Skrýt panel filtrů"}
        >
          {isSidebarHidden ? <><Menu className="ui-icon" aria-hidden="true" /> Filtry</> : <><ArrowLeft className="ui-icon" aria-hidden="true" /> Skrýt</>}
        </button>
        <button
          type="button"
          className="theme-toggle"
          onClick={() => setTheme((prev) => (prev === "dark" ? "light" : "dark"))}
          title="Přepnout tmavý režim"
        >
          {theme === "dark" ? <><Sun className="ui-icon" aria-hidden="true" /> Světlý</> : <><Moon className="ui-icon" aria-hidden="true" /> Tmavý</>}
        </button>
        {apiHealth && (
          <div className="api-chip-wrapper">
            <div
              className={`api-status-chip${apiHealth.status === "ok" ? " up" : " down"}${showApiDetails ? " open" : ""}`}
              onClick={() => setShowApiDetails((v) => !v)}
              title="Klikni pro dev info"
            >
              <span className="api-status-dot" />
              <span className="api-status-label">API {apiHealth.status === "ok" ? "UP" : "DOWN"}</span>
              <ChevronDown className="ui-icon" style={{ width: 12, height: 12 }} />
            </div>
            {showApiDetails && (
              <div className="api-detail-dropdown">
                <div className="api-detail-row"><span className="api-detail-lbl">Verze</span> <span className="api-detail-val">{apiHealth.version || "—"}</span></div>
                <div className="api-detail-row"><span className="api-detail-lbl">Uptime</span> <span className="api-detail-val">{fmtUptime(apiHealth.uptime)}</span></div>
                <div className="api-detail-row"><span className="api-detail-lbl">PID</span> <span className="api-detail-val">{apiHealth.pid || "—"}</span></div>
                <div className="api-detail-row"><span className="api-detail-lbl">URL</span> <span className="api-detail-val">{API_BASE}</span></div>
                <div className="api-detail-row"><span className="api-detail-lbl">Endpoint</span> <span className="api-detail-val">/api/health</span></div>
              </div>
            )}
          </div>
        )}
      </div>

      <div className={`layout${isSidebarHidden ? " sidebar-hidden" : ""}`}>
        {/* Sidebar */}
        {!isSidebarHidden && <aside className="sidebar">
          <div className="sidebar-head">
            <div>
              <strong>Filtry</strong>
            </div>
            <button
              type="button"
              className="link-btn"
              onClick={() => setIsSidebarHidden(true)}
              title="Skrýt levý panel"
            >
              <X className="ui-icon" aria-hidden="true" />
            </button>
          </div>
          {initialLoading && (
            <div className="loading-panel">
              <div className="loading-ring" />
              <div>
                <div className="loading-title">Načítám parametry a poslední výsledky</div>
                <div className="loading-text">Prosím vyčkej, za okamžik se objeví aktuální stav.</div>
              </div>
            </div>
          )}

          {!initialLoading && (
            null
          )}

          {BASIC_GROUPS.map((group) => (
            <div key={group.label} className="param-group card-section">
              <div className="group-label">{group.label}</div>
              {group.label === "Hledání" && (
                <div className="catalog-multi-wrap">
                  <div className="catalog-block">
                    <div className="catalog-title">Výrobce</div>
                    <input
                      type="text"
                      className="catalog-search"
                      placeholder="Filtrovat značky..."
                      value={brandFilterText}
                      onChange={(e) => setBrandFilterText(e.target.value)}
                    />
                    <div className="catalog-list">
                      {brandOptions
                        .filter((b) => b.label.toLowerCase().includes(brandFilterText.toLowerCase()) || b.value.toLowerCase().includes(brandFilterText.toLowerCase()))
                        .map((b) => {
                          const logoUrl = getBrandLogo(b.value);
                          const fc = getBrandFallbackColor(b.value);
                          return (
                          <div key={b.value} className={`catalog-item${excludedBrands.includes(b.value) ? " excluded" : ""}`}>
                            <span
                              className={`catalog-toggle-btn${selectedBrands.includes(b.value) ? " checked" : excludedBrands.includes(b.value) ? " excluded" : ""}`}
                              onClick={(e) => { e.stopPropagation(); toggleBrand(b.value); }}
                              title={selectedBrands.includes(b.value) ? "✓ Zahrnuto — klikni pro vyloučení" : excludedBrands.includes(b.value) ? "✕ Vyloučeno — klikni pro zrušení" : "Klikni pro zahrnutí"}
                            >
                              {selectedBrands.includes(b.value) ? "✓" : excludedBrands.includes(b.value) ? "✕" : ""}
                            </span>
                            {logoUrl ? (
                              <img
                                className={`catalog-brand-logo${theme === "dark" && DARK_INVERT_BRANDS.has(b.value.toLowerCase().trim()) ? " invert" : ""}`}
                                src={logoUrl}
                                alt={b.label}
                                loading="lazy"
                                onError={(e) => {
                                  e.target.style.display = "none";
                                  const bdg = e.target.parentElement.querySelector(".catalog-brand-badge");
                                  if (bdg) bdg.style.display = "inline-flex";
                                }}
                              />
                            ) : null}
                            <span
                              className="catalog-brand-badge"
                              style={fc ? { background: fc.bg, color: fc.fg, display: logoUrl ? "none" : "inline-flex" } : { display: logoUrl ? "none" : "inline-flex" }}
                            >
                              {brandInitials(b.label)}
                            </span>
                            <span>{b.label}</span>
                          </div>
                          );
                        })}
                    </div>
                  </div>

                  {selectedBrands.length > 0 && (
                    <div className="catalog-block models">
                      <div className="catalog-title">Modely (pro vybrané značky)</div>
                      <input
                        type="text"
                        className="catalog-search"
                        placeholder="Filtrovat modely..."
                        value={modelFilterText}
                        onChange={(e) => setModelFilterText(e.target.value)}
                      />
                      <div className="catalog-list">
                        {selectedBrands.flatMap((brand) => {
                          const models = modelsByBrand[brand] || [];
                          const loadingModels = loadingModelsByBrand[brand];
                          if (loadingModels) {
                            return [
                              <div key={`loading-${brand}`} className="catalog-subhead">{brand} · načítám...</div>,
                            ];
                          }
                          const filtered = models.filter((m) =>
                            m.label.toLowerCase().includes(modelFilterText.toLowerCase()) ||
                            m.value.toLowerCase().includes(modelFilterText.toLowerCase()),
                          );
                          return [
                            <div key={`head-${brand}`} className="catalog-subhead">{brand}</div>,
                            ...filtered.map((m) => (
                              <div key={`${brand}-${m.value}`} className={`catalog-item model${excludedModels.includes(m.value) ? " excluded" : ""}`}>
                                <span
                                  className={`catalog-toggle-btn${selectedModels.includes(m.value) ? " checked" : excludedModels.includes(m.value) ? " excluded" : ""}`}
                                  onClick={(e) => { e.stopPropagation(); toggleModel(m.value); }}
                                  title={selectedModels.includes(m.value) ? "✓ Zahrnuto — klikni pro vyloučení" : excludedModels.includes(m.value) ? "✕ Vyloučeno — klikni pro zrušení" : "Klikni pro zahrnutí"}
                                >
                                  {selectedModels.includes(m.value) ? "✓" : excludedModels.includes(m.value) ? "✕" : ""}
                                </span>
                                <span>{m.label}</span>
                              </div>
                            )),
                          ];
                        })}
                      </div>
                    </div>
                  )}

                  <div className="catalog-selected-note">
                    {selectedBrands.length > 0 && <>{selectedBrands.length} {selectedBrands.length === 1 ? "značka" : selectedBrands.length >= 2 && selectedBrands.length <= 4 ? "značky" : "značek"}</>}{selectedBrands.length === 0 && "0 značek"}{excludedBrands.length > 0 && ` + ${excludedBrands.length} vyloučeno`}{selectedModels.length > 0 && ` · ${selectedModels.length} ${selectedModels.length === 1 ? "model" : selectedModels.length >= 2 && selectedModels.length <= 4 ? "modely" : "modelů"}`}{excludedModels.length > 0 && ` + ${excludedModels.length} vyloučeno`}
                  </div>
                  {/* Karoserie grid */}
                  <div className="catalog-block">
                    <div className="catalog-title">Karoserie</div>
                    <input
                      type="text"
                      className="catalog-search"
                      placeholder="Filtrovat karoserie..."
                      value={bodyFilterText}
                      onChange={(e) => setBodyFilterText(e.target.value)}
                    />
                    <div className="catalog-list">
                      {bodyOptions.length === 0 && (
                        <div className="catalog-subhead">Načítám karoserie…</div>
                      )}
                      {bodyOptions
                        .filter((b) => b.label.toLowerCase().includes(bodyFilterText.toLowerCase()) || b.value.toLowerCase().includes(bodyFilterText.toLowerCase()))
                        .map((b) => {
                          const selected = selectedBodies.includes(b.value);
                          const excluded = excludedBodies.includes(b.value);
                          return (
                            <div key={b.value} className={`catalog-item${excluded ? " excluded" : ""}`}>
                              <span
                                className={`catalog-toggle-btn${selected ? " checked" : excluded ? " excluded" : ""}`}
                                onClick={(e) => { e.stopPropagation(); toggleBody(b.value); }}
                                title={selected ? "✓ Zahrnuto — klikni pro vyloučení" : excluded ? "✕ Vyloučeno — klikni pro zrušení" : "Klikni pro zahrnutí"}
                              >
                                {selected ? "✓" : excluded ? "✕" : ""}
                              </span>
                              <span>{b.label}</span>
                            </div>
                          );
                        })}
                    </div>
                  </div>
                </div>
              )}

              {group.fields
                .filter((def) => !["manufacturer_seo_name", "model_seo_name", "body_seo"].includes(def.key))
                .map((def) => (
                  <Field key={def.key} def={def} value={params[def.key]} onChange={setParam} />
                ))}
            </div>
          ))}

          <div className="advanced-toggle-row">
            <button className="link-btn" onClick={() => setShowAdvanced((v) => !v)}>
              {showAdvanced ? <><ChevronUp className="ui-icon" aria-hidden="true" /> Skrýt pokročilé</> : <><ChevronDown className="ui-icon" aria-hidden="true" /> Pokročilé filtry</>}
            </button>
            <span className="muted">{ADVANCED_GROUPS.length} sekcí</span>
          </div>

          {showAdvanced && (
            <div className="advanced-panel">
              {ADVANCED_GROUPS.map((group) => (
                <div key={group.label} className="param-group card-section advanced-card">
                  <div className="group-label">{group.label}</div>
                  {group.fields.map((def) => (
                    <Field key={def.key} def={def} value={params[def.key]} onChange={setParam} />
                  ))}
                </div>
              ))}

              {extraKeys.length > 0 && (
                <div className="param-group card-section advanced-card">
                  <div className="group-label">Ostatní</div>
                  {extraKeys.map((k) => (
                    <div key={k} className="field">
                      <label>{k}</label>
                      <input type="text" value={params[k] ?? ""}
                        onChange={(e) => setParam(k, e.target.value)} />
                    </div>
                  ))}
                </div>
              )}
            </div>
          )}

          <div className="actions sticky-actions">
            <button onClick={save} disabled={busy}><Save className="ui-icon" aria-hidden="true" /> Uložit</button>
            <button onClick={refreshAll} disabled={busy}><RefreshCw className="ui-icon" aria-hidden="true" /> Obnovit</button>
          </div>
          {message && <p className="msg">{message}</p>}
        </aside>}

        {/* Main */}
        <div className="main">
          {(isRunning || logs.length > 0) && <div className="debug-ticker-wrap">
            <span className={`debug-ticker-dot${isRunning ? " active" : ""}`} title={isRunning ? "Běží" : "Nečinný"} />
            <span className="debug-ticker-text" title={logs.length > 0 ? logs[logs.length - 1] : ""}>
              <span className="debug-prefix">[{tickerPrefix()}]</span>{" "}
              {logs.length > 0 ? logs[logs.length - 1] : <span className="debug-ticker-empty">Žádný log výstup.</span>}
            </span>
            <button
              className="debug-history-btn"
              onClick={() => { setShowLogsModal(true); setTimeout(() => { if (logsModalBodyRef.current) logsModalBodyRef.current.scrollTop = logsModalBodyRef.current.scrollHeight; }, 50); }}
            >
              <History className="ui-icon" aria-hidden="true" /> Historie ({logs.length})
            </button>
          </div>}

          <div className="results-hd results-header">
            <div className="results-title-block">
              <div className="results-title-row">
                <strong>Výsledky</strong>
                <span className="muted">{items.length} záznamů</span>
                {scraperRunningFromResults && <span className="muted"> · scrape běží</span>}
              </div>
              <label className="score-control-inline">
                <span>Bodování</span>
                <select value={selectedPreset} onChange={(e) => {
                  const val = e.target.value;
                  if (val === "__new__") { openNewPreset(); return; }
                  setSelectedPreset(val);
                }}>
                  <optgroup label="Vestavěné">
                    {Object.entries(scoringPresets).map(([key, preset]) => (
                      <option key={key} value={key}>
                        {preset.name || key}
                      </option>
                    ))}
                  </optgroup>
                  {Object.keys(customPresets).length > 0 && (
                    <optgroup label="Moje presety">
                      {Object.entries(customPresets).map(([key, preset]) => (
                        <option key={`custom-${key}`} value={key}>
                          {preset.name || key}
                        </option>
                      ))}
                    </optgroup>
                  )}
                  <option value="__new__" style={{ fontStyle: "italic", color: "var(--accent)" }}>
                    ＋ Vytvořit nový preset...
                  </option>
                </select>
                {customPresets[selectedPreset] && (
                  <span className="preset-actions-inline">
                    <button className="link-btn" onClick={(e) => { e.stopPropagation(); openEditPreset(selectedPreset); }} title="Upravit preset">✎</button>
                    <button className="link-btn danger" onClick={(e) => { e.stopPropagation(); deleteCustomPreset(selectedPreset); }} title="Smazat preset">🗑</button>
                  </span>
                )}
                {(scoringPresets[selectedPreset]?.description || customPresets[selectedPreset]?.description) && (
                  <span className="score-control-description">
                    {scoringPresets[selectedPreset]?.description || customPresets[selectedPreset]?.description}
                  </span>
                )}
                {/* Single quick filter button */}
                <span className="quick-filter-inline">
                  <button className={`quick-filter-btn${showQuickEquip || showQuickBody ? " active" : ""}`} onClick={() => {
                    const opening = !showQuickEquip && !showQuickBody;
                    setShowQuickEquip(opening);
                    setShowQuickBody(opening);
                    if (opening) { fetchEquipment().catch(() => null); fetchBodies().catch(() => null); }
                  }}>
                    Filtr{quickEquipMust.length + quickEquipExcl.length + quickBodyFilter.length + quickBodyExcl.length > 0
                      ? ` (${quickEquipMust.length + quickEquipExcl.length + quickBodyFilter.length + quickBodyExcl.length})`
                      : ""}
                  </button>
                </span>
              </label>
            </div>
            <div className="results-actions">
              <button className="link-btn" onClick={() => exportResults("all")}><Download className="ui-icon" aria-hidden="true" /> Export</button>
              <button className="link-btn" onClick={() => fileInputRef.current?.click()}><Upload className="ui-icon" aria-hidden="true" /> Import</button>
              <button className="link-btn" onClick={refreshAll}><RefreshCw className="ui-icon" aria-hidden="true" /> Obnovit</button>
            </div>
          </div>
          <input
            ref={fileInputRef}
            type="file"
            accept="application/json,.json"
            style={{ display: "none" }}
            onChange={(e) => importResultsFile(e.target.files?.[0])}
          />

          {selectedCount > 0 && <div className="selection-bar">
            <div>
              <strong>{selectedCount}</strong> vybraných
              <span className="muted"> · zdroj {resultsPath}</span>
            </div>
            <div className="selection-actions">
              <button className="link-btn" onClick={() => exportResults("selected")}><Download className="ui-icon" aria-hidden="true" /> Export</button>
              <button className="link-btn" onClick={() => markSelected(true)}><Star className="ui-icon" aria-hidden="true" /> Označit</button>
              <button className="link-btn" onClick={() => markSelected(false)}><Star className="ui-icon icon-muted" aria-hidden="true" /> Odznačit</button>
              <button className="link-btn danger" onClick={deleteSelected}><Trash2 className="ui-icon" aria-hidden="true" /> Smazat</button>
              <button className="link-btn" onClick={toggleSelectVisible}>
                {visibleItems.every((item) => selectedIds.includes(resultKey(item))) ? "Odznačit viditelné" : "Vybrat viditelné"}
              </button>
              <button className="link-btn" onClick={() => setSelectedIds([])} disabled={selectedCount === 0}>Vyčistit výběr</button>
            </div>
          </div>}

          <div className="table-wrap">
            <ResultsTable
              visibleItems={quickFilteredItems}
              scoreCache={scoreCache}
              selectedIds={selectedIds}
              markedIds={markedIds}
              toggleSelected={toggleSelected}
              markSelected={markSelected}
              toggleSelectVisible={toggleSelectVisible}
              allVisibleSelected={allVisibleSelected}
              getCachedScore={getCachedScore}
              toggleSort={toggleSort}
              sortIndicator={sortIndicator}
              resultKey={resultKey}
            />
          </div>
        </div>
      </div>

    {showLogsModal && (
      <div className="debug-modal-overlay" onClick={() => setShowLogsModal(false)}>
        <div className="debug-modal" onClick={(e) => e.stopPropagation()}>
          <div className="debug-modal-head">
            <strong>Debug výpis — Historie</strong>
            <span className="muted">{logs.length} řádků</span>
            <button className="debug-modal-close" onClick={() => setShowLogsModal(false)}><X className="ui-icon" aria-hidden="true" /></button>
          </div>
          <div className="debug-modal-body" ref={logsModalBodyRef}>
            {logs.length === 0 ? (
              <div className="debug-empty">Zatím žádný log výstup.</div>
            ) : (
              logs.map((line, i) => (
                <div key={`log-${i}`} className="debug-modal-line" onClick={() => setPopupLog(line)}>
                  <span className="debug-line-num">{i + 1}</span>
                  <span className="debug-line-text">{line}</span>
                </div>
              ))
            )}
          </div>
        </div>
      </div>
    )}

    {popupLog !== null && (
      <div className="log-popup-overlay" onClick={() => setPopupLog(null)}>
        <div className="log-popup" onClick={(e) => e.stopPropagation()}>
          <div className="log-popup-head">
            <strong>Detail řádku</strong>
            <button className="debug-modal-close" onClick={() => setPopupLog(null)}><X className="ui-icon" aria-hidden="true" /></button>
          </div>
          <pre className="log-popup-body">{popupLog}</pre>
          <div className="log-popup-foot">
            <button className="btn-sm" onClick={() => { navigator.clipboard.writeText(popupLog).catch(() => null); }}>Kopírovat</button>
            <button className="btn-sm secondary" onClick={() => setPopupLog(null)}>Zavřít</button>
          </div>
        </div>
      </div>
    )}

    {toastMsg && (
      <div className={`toast toast-${toastType || "info"}`}>
        <span>{toastMsg}</span>
        <button className="toast-close" onClick={() => { setToastMsg(""); setToastType(""); }}>
          <X className="ui-icon" style={{ width: 13, height: 13 }} />
        </button>
      </div>
    )}

    {/* Quick Filter Popup */}
    {(showQuickEquip || showQuickBody) && (
      <div className="debug-modal-overlay" onClick={() => { setShowQuickEquip(false); setShowQuickBody(false); }}>
        <div className="preset-modal" style={{ maxWidth: 600 }} onClick={(e) => e.stopPropagation()}>
          <div className="preset-modal-head">
            <strong>Rychlý filtr</strong>
            <button className="debug-modal-close" onClick={() => { setShowQuickEquip(false); setShowQuickBody(false); }}><X className="ui-icon" aria-hidden="true" /></button>
          </div>
          <div className="preset-modal-body" style={{ display: "flex", gap: 16, flexWrap: "wrap" }}>
            {/* Numeric filters - one row per pair */}
            <div style={{ flex: "1 1 100%", display: "flex", gap: 6, flexWrap: "wrap", alignItems: "flex-end" }}>
              <div className="field" style={{ flex: "1 1 200px", minWidth: 160 }}>
                <label>Cena (Kč) od/do</label>
                <div style={{ display: "flex", gap: 4 }}>
                  <input type="number" placeholder="Od" style={{ flex: 1 }} value={quickPriceFrom} onChange={(e) => setQuickPriceFrom(e.target.value)} />
                  <input type="number" placeholder="Do" style={{ flex: 1 }} value={quickPriceTo} onChange={(e) => setQuickPriceTo(e.target.value)} />
                </div>
              </div>
              <div className="field" style={{ flex: "1 1 200px", minWidth: 160 }}>
                <label>Rok od/do</label>
                <div style={{ display: "flex", gap: 4 }}>
                  <input type="number" placeholder="Od" style={{ flex: 1 }} value={quickYearFrom} onChange={(e) => setQuickYearFrom(e.target.value)} />
                  <input type="number" placeholder="Do" style={{ flex: 1 }} value={quickYearTo} onChange={(e) => setQuickYearTo(e.target.value)} />
                </div>
              </div>
              <div className="field" style={{ flex: "1 1 200px", minWidth: 160 }}>
                <label>Km od/do</label>
                <div style={{ display: "flex", gap: 4 }}>
                  <input type="number" placeholder="Od" style={{ flex: 1 }} value={quickKmFrom} onChange={(e) => setQuickKmFrom(e.target.value)} />
                  <input type="number" placeholder="Do" style={{ flex: 1 }} value={quickKmTo} onChange={(e) => setQuickKmTo(e.target.value)} />
                </div>
              </div>
              <div className="field" style={{ flex: "1 1 200px", minWidth: 160 }}>
                <label>kW od/do</label>
                <div style={{ display: "flex", gap: 4 }}>
                  <input type="number" placeholder="Od" style={{ flex: 1 }} value={quickPowerFrom} onChange={(e) => setQuickPowerFrom(e.target.value)} />
                  <input type="number" placeholder="Do" style={{ flex: 1 }} value={quickPowerTo} onChange={(e) => setQuickPowerTo(e.target.value)} />
                </div>
              </div>
            </div>
            {/* Select filters */}
            <div style={{ flex: "1 1 100%", display: "flex", gap: 8, flexWrap: "wrap" }}>
              <div className="field" style={{ flex: "1 1 160px" }}>
                <label>Palivo</label>
                <select value={quickFuel} onChange={(e) => setQuickFuel(e.target.value)}>
                  <option value="">— jakékoliv —</option>
                  <option value="benzin">Benzín</option>
                  <option value="nafta">Nafta</option>
                  <option value="lpg-benzin">LPG</option>
                  <option value="hybrid">Hybrid</option>
                  <option value="elektro">Elektro</option>
                </select>
              </div>
              <div className="field" style={{ flex: "1 1 160px" }}>
                <label>Převodovka</label>
                <select value={quickGearbox} onChange={(e) => setQuickGearbox(e.target.value)}>
                  <option value="">— jakákoliv —</option>
                  <option value="manual">Manuál</option>
                  <option value="automatic">Automat</option>
                </select>
              </div>
              <div className="field" style={{ flex: "1 1 160px" }}>
                <label>Pohon</label>
                <select value={quickDrive} onChange={(e) => setQuickDrive(e.target.value)}>
                  <option value="">— jakýkoliv —</option>
                  <option value="fwd">Přední (FWD)</option>
                  <option value="rwd">Zadní (RWD)</option>
                  <option value="awd">4×4 (AWD)</option>
                </select>
              </div>
            </div>
            {/* Equipment + Body */}
            <div style={{ flex: "1 1 260px" }}>
              <div className="preset-section-title">Výbava</div>
              <input className="catalog-search" placeholder="Hledat..." value={equipmentFilterText} onChange={(e) => setEquipmentFilterText(e.target.value)} />
              <div className="catalog-list" style={{ maxHeight: 200 }}>
                {equipmentOptions.filter(e => e.label.toLowerCase().includes(equipmentFilterText.toLowerCase())).map(e => {
                  const m = quickEquipMust.includes(e.value);
                  const x = quickEquipExcl.includes(e.value);
                  return <div key={e.value} className={`catalog-item${x ? " excluded" : ""}`} onClick={() => toggleQuickEquip(e.value)}>
                    <span className={`catalog-toggle-btn${m ? " checked" : x ? " excluded" : ""}`}>{m ? "✓" : x ? "✕" : ""}</span>
                    <span>{e.label}</span>
                  </div>;
                })}
              </div>
            </div>
            <div style={{ flex: "1 1 260px" }}>
              <div className="preset-section-title">Karoserie</div>
              <input className="catalog-search" placeholder="Hledat..." value={bodyFilterText} onChange={(e) => setBodyFilterText(e.target.value)} />
              <div className="catalog-list" style={{ maxHeight: 200 }}>
                {bodyOptions.filter(b => b.label.toLowerCase().includes(bodyFilterText.toLowerCase())).map(b => {
                  const s = quickBodyFilter.includes(b.value);
                  const x = quickBodyExcl.includes(b.value);
                  return <div key={b.value} className={`catalog-item${x ? " excluded" : ""}`} onClick={() => toggleQuickBody(b.value)}>
                    <span className={`catalog-toggle-btn${s ? " checked" : x ? " excluded" : ""}`}>{s ? "✓" : x ? "✕" : ""}</span>
                    <span>{b.label}</span>
                  </div>;
                })}
              </div>
            </div>
          </div>
        </div>
      </div>
    )}

    {/* Custom Preset Modal */}
    {showPresetModal && (
      <div className="debug-modal-overlay" onClick={() => setShowPresetModal(false)}>
        <div className="preset-modal" onClick={(e) => e.stopPropagation()}>
          <div className="preset-modal-head">
            <strong>{editingPresetId ? "Upravit preset" : "Nový preset"}</strong>
            <button className="debug-modal-close" onClick={() => setShowPresetModal(false)}><X className="ui-icon" aria-hidden="true" /></button>
          </div>
          <div className="preset-modal-body">
            {/* Basic info */}
            <div className="preset-section">
              <div className="preset-section-title">Základní info</div>
              <div className="field">
                <label>Název presetu</label>
                <input type="text" value={presetForm.name} onChange={(e) => setPresetForm((prev) => ({ ...prev, name: e.target.value }))} placeholder="např. Můj daily driver" />
              </div>
              <div className="field">
                <label>Popis (nepovinný)</label>
                <input type="text" value={presetForm.description} onChange={(e) => setPresetForm((prev) => ({ ...prev, description: e.target.value }))} placeholder="Stručný popis co preset hledá" />
              </div>
            </div>

            {/* Scoring weights */}
            <div className="preset-section">
              <div className="preset-section-title">Scoring váhy</div>
              <div className="preset-weights-grid">
                {ALL_WEIGHT_KEYS.map(({ key, label }) => (
                  <div key={key} className="preset-weight-row">
                    <label>{label}</label>
                    <CustomSlider
                      value={presetForm.weights[key] ?? 1}
                      min={0}
                      max={2.5}
                      step={0.05}
                      size="sm"
                      formatValue={(v) => v.toFixed(2)}
                      onChange={(val) => setPresetWeight(key, val)}
                    />
                  </div>
                ))}
              </div>
            </div>

            {/* Deal breakers */}
            <div className="preset-section">
              <div className="preset-section-title">Deal breakery (vyloučit)</div>
              <div className="preset-quick-rejects">
                {[
                  // ══ Převodovka ══
                  { label: "Automat", pattern: "automat", reason: "Jen manuál" },
                  { label: "Manuál", pattern: "\\b(manu[aá]l|ru[cč]n[ií]\\s*p[rř]evodovka)\\b", reason: "Jen automat" },
                  // ══ Palivo ══
                  { label: "Nafta", pattern: "\\b(diesel|nafta|tdi|tdci|cdti|dci|hdi|d4d|d5)\\b", reason: "Jen benzín" },
                  { label: "Benzín", pattern: "\\b(benz[ií]n|z[aá][zž]ehový|benzinový|lpg|cng|hybrid|elektro)\\b", reason: "Jen nafta" },
                  { label: "Elektro/Hybrid", pattern: "\\b(elektro|hybrid|plug[\s-]?in|elektrick[ýe])\\b", reason: "Jen spalovací" },
                  { label: "LPG/CNG", pattern: "\\b(lpg|cng|plyn|zemn[ií]\\s*plyn)\\b", reason: "Žádný plyn" },
                  // ══ Pohon ══
                  { label: "FWD (přední)", pattern: "\\b(fwd|p[rř]edn[ií]\\s*n[aá]hon|4x2)\\b", reason: "Jen zadní/4×4" },
                  { label: "RWD (zadní)", pattern: "\\b(rwd|zadn[ií]\\s*n[aá]hon|zadokolka)\\b", reason: "Jen přední/4×4" },
                  { label: "AWD / 4×4", pattern: "\\b(awd|4x4|4\\s*x\\s*4|v[sš]echny\\s*kola|[cč]ty[rř]kolka|quattro|4matic|xdrive|4motion)\\b", reason: "Jen 2WD" },
                  // ══ Původ / dovoz ══
                  { label: "Dovoz", pattern: "\\b(dovoz|import|itali[ei]|n[ěe]mecko|usa|amerik[ay]|[sš]v[aý]carsko|rakousko|francie|japonsko|angli[ei]|belgie|holandsko|polsko)\\b", reason: "Jen tuzemské" },
                  { label: "Zahraniční původ", pattern: "\\b(dovoz|import|zahrani[cč][ií]|prvn[ií]\\s*majitel\\s*v\\s*[cč]r)\\b", reason: "Jen český původ" },
                  // ══ Historie ══
                  { label: "Bouraný / totálka", pattern: "(bouran|havar|totaln[ií]\\s*[sš]kod|oprava\\s*po\\s*nehod)", reason: "Žádný bouraný" },
                  { label: "Tuning / chip", pattern: "(tuning|chip|na[cč]ipov|upraven[eo]|stage\\s*[1-3]|remap)", reason: "Žádný tuning" },
                  { label: "Koroze / rez", pattern: "(koroze|rez|prorezl|prorezav|zkorodov)", reason: "Žádná koroze" },
                  { label: "Povodeň", pattern: "(povod[eě][nň]|zatopen|zaplaven|z[aá]plava)", reason: "Žádná povodeň" },
                  { label: "Nefunkční díly", pattern: "(nefunk[cč]n[ií]|nefunguje|porouch|porucha)", reason: "Vše funkční" },
                  { label: "Díly / na náhradní", pattern: "na\\s*n[aá]hradn[ií]\\s*d[ií]ly", reason: "Pojízdný" },
                  { label: "Taxi", pattern: "\\b(taxi|taxik[aá][rř]|z\\s*taxislu[zž]by|slu[zž]ebn[ií]\\s*v[ůu]z)\\b", reason: "Nebylo taxi" },
                  { label: "Veterán", pattern: "\\b(veter[aá]n|oldtimer|historick[ée]\\s*vozidlo|30\\s*let\\s*star[ée])\\b", reason: "Žádné veterány" },
                  // ══ Servis / STK ══
                  { label: "Bez servisky", pattern: "\\b(bez\\s*servisn[ií]\\s*kn[ií][zž]ky|servisn[ií]\\s*kniha\\s*chyb[ií]|serviska\\s*nen[ií]|servisn[ií]\\s*kniha\\s*nevedena|bez\\s*servisky)\\b", reason: "Musí mít servisku" },
                  { label: "STK propadlá", pattern: "\\b(stk\\s*propadl|bez\\s*stk|neplatn[aá]\\s*stk|stk\\s*chyb[ií]|bez\\s*technick[ée]|technick[aá]\\s*pro[sš]la|technick[aá]\\s*propadla|bez\\s*platn[ée]\\s*technick[ée])\\b", reason: "Platná STK" },
                  // ══ Vlastnictví ══
                  { label: "Víc majitelů (3+)", pattern: "\\b(3\\.\\s*majitel|t[rř]et[ií]\\s*majitel|4\\.\\s*majitel|[cč]tvrt[ýy]\\s*majitel|5\\.\\s*majitel|p[aá]t[ýy]\\s*majitel|v[ií]ce\\s*majitel)\\b", reason: "Max 2 majitelé" },
                  { label: "Leasing / úvěr", pattern: "\\b(leasing|[uú]v[ěe]r|spl[aá]tky|financov[aá]n|na\\s*[uú]v[ěe]r|na\\s*spl[aá]tky)\\b", reason: "Jen za hotové" },
                  // ══ Stáří ══
                  { label: "Mladší 3 let (zánovní)", pattern: "\\b(st[aá]r[ée]\\s*auto|star[sš][ií]\\s*ne[zž]\\s*3|v[ií]ce\\s*ne[zž]\\s*3\\s*roky|p[rř]es\\s*3\\s*roky\\s*star[ée])\\b", reason: "Jen do 3 let" },
                  // ══ Další ══
                  { label: "Fleetové auto", pattern: "\\b(fleet|flotil|firemn[ií]\\s*v[ůu]z|slu[zž]ebn[ií]\\s*auto|poolov[ée])\\b", reason: "Žádné fleetové" },
                  { label: "Bez DPH", pattern: "\\b(bez\\s*dph|nepl[aá]tce\\s*dph|dph\\s*nen[ií]\\s*v\\s*cen[ěe]|cena\\s*bez\\s*dph)\\b", reason: "Cena s DPH" },
                ].map((quick) => {
                  const alreadyAdded = presetForm.hard_rejects.some((r) => r.pattern === quick.pattern);
                  return (
                    <button
                      key={quick.pattern}
                      className={`preset-chip-btn${alreadyAdded ? " active" : ""}`}
                      onClick={() => {
                        if (alreadyAdded) {
                          setPresetForm((prev) => ({ ...prev, hard_rejects: prev.hard_rejects.filter((r) => r.pattern !== quick.pattern) }));
                        } else {
                          setPresetForm((prev) => ({ ...prev, hard_rejects: [...prev.hard_rejects, { pattern: quick.pattern, reason: quick.reason }] }));
                        }
                      }}
                    >
                      {alreadyAdded ? "✓" : "+"} {quick.label}
                    </button>
                  );
                })}
              </div>
              <div className="preset-reject-custom">
                <div className="preset-reject-row">
                  <input type="text" placeholder="Regex pattern" value={presetFormRejectPattern} onChange={(e) => setPresetFormRejectPattern(e.target.value)} onKeyDown={(e) => { if (e.key === "Enter") addPresetReject(); }} />
                  <input type="text" placeholder="Důvod (nepovinný)" value={presetFormRejectReason} onChange={(e) => setPresetFormRejectReason(e.target.value)} onKeyDown={(e) => { if (e.key === "Enter") addPresetReject(); }} />
                  <button className="link-btn" onClick={addPresetReject}>+ Přidat</button>
                </div>
              </div>
              {presetForm.hard_rejects.length > 0 && (
                <div className="preset-tags">
                  {presetForm.hard_rejects.map((r, i) => (
                    <span key={i} className="preset-tag reject" title={r.reason || r.pattern}>
                      ✕ {r.reason || r.pattern}
                      <button onClick={() => removePresetReject(i)}>×</button>
                    </span>
                  ))}
                </div>
              )}
            </div>

            {/* Equipment picker with 3‑state toggle (✓ must → ✕ excluded → neutral) */}
            <div className="preset-section">
              <div className="preset-section-title">Výbava (Sauto seznam)</div>
              <input
                type="text"
                className="catalog-search"
                placeholder="Filtrovat výbavu..."
                value={equipmentFilterText}
                onChange={(e) => setEquipmentFilterText(e.target.value)}
              />
              <div className="catalog-list" style={{ maxHeight: 360 }}>
                {equipmentOptions.length === 0 && (
                  <div className="catalog-subhead">Načítám seznam výbavy…</div>
                )}
                {equipmentOptions
                  .filter((e) => e.label.toLowerCase().includes(equipmentFilterText.toLowerCase()) || e.value.toLowerCase().includes(equipmentFilterText.toLowerCase()))
                  .map((e) => {
                    const must = (presetForm.must_have_equipment || []).includes(e.value);
                    const excl = (presetForm.excluded_equipment || []).includes(e.value);
                    return (
                      <div key={e.value} className={`catalog-item${excl ? " excluded" : ""}`}>
                        <span
                          className={`catalog-toggle-btn${must ? " checked" : excl ? " excluded" : ""}`}
                          onClick={() => toggleEquipment(e.value)}
                          title={must ? "✓ Vyžadováno — klikni pro vyloučení" : excl ? "✕ Vyloučeno — klikni pro zrušení" : "Klikni pro vyžadování"}
                        >
                          {must ? "✓" : excl ? "✕" : ""}
                        </span>
                        <span>{e.label}</span>
                      </div>
                    );
                  })}
              </div>
              <div className="catalog-selected-note">
                {(presetForm.must_have_equipment || []).length > 0 && <>{presetForm.must_have_equipment.length} vyžadováno</>}
                {(presetForm.must_have_equipment || []).length === 0 && "0 vyžadováno"}
                {(presetForm.excluded_equipment || []).length > 0 && ` · ${presetForm.excluded_equipment.length} vyloučeno`}
              </div>
            </div>
          </div>
          <div className="preset-modal-foot">
            <button className="btn-primary" onClick={savePreset}>{editingPresetId ? "Uložit změny" : "Vytvořit preset"}</button>
            <button className="link-btn" onClick={() => setShowPresetModal(false)}>Zrušit</button>
          </div>
        </div>
      </div>
    )}
    </div>
    </>
  );
}
