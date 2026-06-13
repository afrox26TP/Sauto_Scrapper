import { useEffect, useMemo, useRef, useState } from "react";
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
    weights: { age: 0.75, mileage: 1.1, price: 1.4, price_power: 1.85, cost: 1.45, consumption: 1.15, power: 0.85, equipment: 0.45, flags: 1.15, sport: 0.25, luxury: 0.15 },
  },
  balanced: {
    name: "Balanced",
    description: "Univerzální hodnocení: stav, nájezd, cena, výkon, náklady i výbava.",
    weights: { age: 1, mileage: 1, price: 1, price_power: 1, cost: 1, consumption: 1, power: 0.75, equipment: 0.85, flags: 1, sport: 0.35, luxury: 0.35 },
  },
  sport: {
    name: "Sport",
    description: "Priorita: výkon, dynamika, cena za kW, pohon a mladší kusy.",
    weights: { age: 1.05, mileage: 0.75, price: 0.55, price_power: 1.3, cost: 0.55, consumption: 0.35, power: 2.1, equipment: 0.45, flags: 0.8, sport: 1.45, luxury: 0.2 },
  },
  luxury: {
    name: "Luxury",
    description: "Priorita: prémiová značka, výbava, komfort a kultivovaný výkon.",
    weights: { age: 1.35, mileage: 0.9, price: 0.25, price_power: 0.45, cost: 0.35, consumption: 0.25, power: 0.8, equipment: 2.1, flags: 0.9, sport: 0.25, luxury: 1.9 },
  },
};

const PARAM_GROUPS = [
  {
    label: "Hledání",
    fields: [
      { key: "seller_type", type: "select", label: "Prodejce", options: ["", "soukromy", "bazar"] },
      { key: "condition_seo", type: "text", label: "Stav (čárkou)" },
      { key: "operating_lease", type: "boolean", label: "Operativní leasing" },
      { key: "category_id", type: "text", label: "Kategorie ID" },
    ],
  },
  {
    label: "Stránkování",
    fields: [
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
      { key: "body_seo", type: "text", label: "Karoserie (např. suv,kombi,hatchback)" },
      { key: "required_equipment", type: "text", label: "Musí mít výbavu/funkce (čárkou)" },
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

const BASIC_GROUPS = PARAM_GROUPS.slice(0, 3);
const ADVANCED_GROUPS = PARAM_GROUPS.slice(3);
const IGNORED_KEYS = new Set(PARAM_GROUPS.flatMap((g) => g.fields.map((f) => f.key)));

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

function Field({ def, value, onChange }) {
  const { key, type, label, min, max, step, options, fmt } = def;
  const raw = value ?? "";

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
        formatValue={(v) => fmtVal(v, fmt)}
        onChange={(val) => onChange(key, String(val))}
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
}

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
  const [modelsByBrand, setModelsByBrand] = useState({});
  const [loadingModelsByBrand, setLoadingModelsByBrand] = useState({});
  const [brandFilterText, setBrandFilterText] = useState("");
  const [modelFilterText, setModelFilterText] = useState("");
  const [tickerStep, setTickerStep] = useState(0);
  const [sortConfig, setSortConfig] = useState({ key: "score", direction: "desc" });
  const [isSidebarHidden, setIsSidebarHidden] = useState(false);
  const [showApiDetails, setShowApiDetails] = useState(false);
  const [showStatusDropdown, setShowStatusDropdown] = useState(false);
  const [scoringPresets, setScoringPresets] = useState(LOCAL_SCORING_PRESETS);
  const [selectedPreset, setSelectedPreset] = useState("balanced");
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

  async function fetchScoringPresets() {
    try {
      const res = await fetch(`${API_BASE}/api/scoring/presets`, { signal: AbortSignal.timeout(4000) });
      const data = await res.json();
      setScoringPresets(Object.keys(data.presets || {}).length ? data.presets : LOCAL_SCORING_PRESETS);
    } catch {
      setScoringPresets(LOCAL_SCORING_PRESETS);
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

  function syncFilterParams(brands, models) {
    const brandCsv = brands.join(",");
    const modelCsv = models.join(",");
    setParams((prev) => ({
      ...prev,
      manufacturer_seo_name: brandCsv,
      model_seo_name: modelCsv,
    }));
  }

  function toggleBrand(brand) {
    const b = String(brand || "").trim();
    if (!b) return;
    const nextBrands = selectedBrands.includes(b)
      ? selectedBrands.filter((x) => x !== b)
      : [...selectedBrands, b];

    const allowedModels = new Set(nextBrands.flatMap((k) => (modelsByBrand[k] || []).map((m) => m.value)));
    const nextModels = selectedModels.filter((m) => allowedModels.has(m));

    setSelectedBrands(nextBrands);
    setSelectedModels(nextModels);
    syncFilterParams(nextBrands, nextModels);

    if (nextBrands.length > 0) {
      fetchModelsForBrand(b).catch(() => null);
    }
  }

  function toggleModel(model) {
    const m = String(model || "").trim();
    if (!m) return;
    const nextModels = selectedModels.includes(m)
      ? selectedModels.filter((x) => x !== m)
      : [...selectedModels, m];
    setSelectedModels(nextModels);
    syncFilterParams(selectedBrands, nextModels);
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

  function calculateScoreComponents(item) {
    const equipment = Array.isArray(item.equipment_list) ? item.equipment_list : [];
    const eqText = equipmentText(item);
    const power = num(item.power_kw, 0);
    const fuel = String(item.fuel_seo || "").toLowerCase();
    const gearbox = String(item.gearbox_type || "").toLowerCase();
    const drive = String(item.drive_type || "").toLowerCase();
    const brandTier = String(item.brand_tier || "").toLowerCase();

    const components = {
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

    if (equipment.length >= 45) components.equipment += 18;
    else if (equipment.length >= 30) components.equipment += 11;
    else if (equipment.length >= 18) components.equipment += 5;

    if (hasAny(eqText, [/adaptivn[ií]\s*tempomat/, /front assist/, /nouzov[eé]\s*brzd/])) components.equipment += 12;
    if (hasAny(eqText, [/parkovac[ií]\s*kamera/, /360/, /couvac[ií]\s*kamera/])) components.equipment += 8;
    if (hasAny(eqText, [/parkovac[ií]\s*senzory/])) components.equipment += 5;
    if (hasAny(eqText, [/apple\s*car\s*play/, /android\s*auto/, /navigace/])) components.equipment += 8;
    if (hasAny(eqText, [/vyh[rř]ivan[aá]\s*sedadla/, /vyh[rř]ivan[eé]\s*celn[ií]\s*sklo/])) components.equipment += 6;
    if (hasAny(eqText, [/led\s*sv[eě]tl/, /xenon/, /matrix/])) components.equipment += 6;
    if (hasAny(eqText, [/mrtv[eé]ho\s*[uú]hlu/, /j[ií]zdn[ií]ho\s*pruhu/, /lane assist/])) components.equipment += 8;
    components.equipment = clamp(components.equipment, 0, 70);

    if (item.service_book) components.flags += 14;
    if (item.first_owner) components.flags += 9;
    if (item.tuning) components.flags -= 28;

    if (power >= 220) components.sport += 36;
    else if (power >= 170) components.sport += 28;
    else if (power >= 130) components.sport += 16;
    else if (power < 75) components.sport -= 12;
    if (drive === "rwd") components.sport += 12;
    else if (drive === "awd") components.sport += 9;
    if (gearbox === "manual") components.sport += 6;
    if (num(item.price_per_kw, 999999) <= 2200) components.sport += 10;
    if (item.tuning) components.sport -= 18;

    if (brandTier === "premium") components.luxury += 24;
    else if (brandTier === "budget") components.luxury -= 6;
    if (gearbox === "automatic") components.luxury += 10;
    if (hasAny(eqText, [/ko[zž]en[aá]/, /alcantara/, /mas[aá][zž]/])) components.luxury += 13;
    if (hasAny(eqText, [/panoramatick[aá]\s*st[rř]echa/, /st[rř]e[sš]n[ií]\s*okno/])) components.luxury += 9;
    if (equipment.length >= 40) components.luxury += 14;
    else if (equipment.length >= 25) components.luxury += 8;
    if (num(item.age_years, 99) <= 5) components.luxury += 12;
    if (power >= 130) components.luxury += 7;
    components.luxury = clamp(components.luxury, -20, 75);

    return components;
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

    return Math.round(weightedScore * 0.55);
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
  }, []);

  useEffect(() => {
    const parsedBrands = uniq(csvToArray(params.manufacturer_seo_name));
    const parsedModels = uniq(csvToArray(params.model_seo_name));
    setSelectedBrands(parsedBrands);
    setSelectedModels(parsedModels);
  }, [params.manufacturer_seo_name, params.model_seo_name]);

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

  function setParam(key, val) {
    setParams((prev) => ({ ...prev, [key]: val }));
  }

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
    const list = [...items];
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
                          <div key={b.value} className="catalog-item">
                            <CustomCheckbox
                              checked={selectedBrands.includes(b.value)}
                              onChange={() => toggleBrand(b.value)}
                              size="sm"
                            />
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
                              <div key={`${brand}-${m.value}`} className="catalog-item model">
                                <CustomCheckbox
                                  checked={selectedModels.includes(m.value)}
                                  onChange={() => toggleModel(m.value)}
                                  size="sm"
                                />
                                <span>{m.label}</span>
                              </div>
                            )),
                          ];
                        })}
                      </div>
                    </div>
                  )}

                  <div className="catalog-selected-note">
                    {selectedBrands.length} značek, {selectedModels.length} modelů vybráno
                  </div>
                </div>
              )}

              {group.fields
                .filter((def) => !["manufacturer_seo_name", "model_seo_name"].includes(def.key))
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
                <select value={selectedPreset} onChange={(e) => setSelectedPreset(e.target.value)}>
                  {Object.entries(scoringPresets).map(([key, preset]) => (
                    <option key={key} value={key}>
                      {preset.name || key}
                    </option>
                  ))}
                </select>
                {scoringPresets[selectedPreset]?.description && (
                  <span className="score-control-description">
                    {scoringPresets[selectedPreset].description}
                  </span>
                )}
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
            {items.length === 0 ? (
              <p className="empty">Žádné výsledky — spusť scraper a obnov.</p>
            ) : (
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
                    return (
                    <tr key={item.ad_id || i} className={`${selected ? "row-selected" : ""}${marked ? " row-marked" : ""} ${(() => { const s = getItemScore(item, scoringPresets[selectedPreset]); return s >= 80 ? "row-score-hi" : s >= 50 ? "row-score-mid" : "row-score-lo"; })()}`}>
                      <td className="cell-check">
                        <CustomCheckbox checked={selected} onChange={() => toggleSelected(key)} size="sm" />
                      </td>
                      <td className="cell-mark">
                        <button className={`mark-chip${marked ? " marked" : ""}`} onClick={() => markSelected(marked ? false : true)} disabled={!selected && !marked} title={marked ? "Odznačit" : "Označit"}>
                          <Star className="ui-icon" aria-hidden="true" />
                        </button>
                      </td>
                      <td>
                        {(() => {
                          const preset = scoringPresets[selectedPreset];
                          const calculatedScore = getItemScore(item, preset);
                          const scoreClass = calculatedScore >= 80 ? "score-hi" : calculatedScore >= 50 ? "score-mid" : "score-lo";
                          return (
                            <span className={`score ${scoreClass}`}>
                              {calculatedScore}
                            </span>
                          );
                        })()}
                      </td>
                      <td className="name-cell">{item.name || "—"}</td>
                      <td>{item.price ? item.price.toLocaleString("cs-CZ") : "—"}</td>
                      <td>{item.power_kw ?? "—"}</td>
                      <td>{item.tachometer ? item.tachometer.toLocaleString("cs-CZ") : "—"}</td>
                      <td>{item.drive_type || "—"}</td>
                      <td>{item.gearbox_type || "—"}</td>
                      <td>{Number.isFinite(item.price_per_kw) ? item.price_per_kw.toLocaleString("cs-CZ", { maximumFractionDigits: 2 }) : "—"}</td>
                      <td>{Number.isFinite(item.price_per_km) ? item.price_per_km.toLocaleString("cs-CZ", { maximumFractionDigits: 4 }) : "—"}</td>
                      <td>{Number.isFinite(item.km_per_year) ? item.km_per_year.toLocaleString("cs-CZ") : "—"}</td>
                      <td>{item.annual_total_cost ? item.annual_total_cost.toLocaleString("cs-CZ") : "—"}</td>
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
            )}
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
    </div>
    </>
  );
}