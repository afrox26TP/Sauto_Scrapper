import { useEffect, useRef, useState } from "react";

const API_BASE = "http://localhost:8000";

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
      { key: "price_from", type: "slider", label: "Cena od", min: 0, max: 2000000, step: 10000, fmt: "price" },
      { key: "price_to", type: "slider", label: "Cena do", min: 0, max: 2000000, step: 10000, fmt: "price" },
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

const BASIC_GROUPS = PARAM_GROUPS.slice(0, 3);
const ADVANCED_GROUPS = PARAM_GROUPS.slice(3);
const IGNORED_KEYS = new Set(PARAM_GROUPS.flatMap((g) => g.fields.map((f) => f.key)));

const VALUATION_META = {
  deep_undervalued: { icon: "↘", label: "deep_undervalued", title: "Silně podhodnocené" },
  undervalued: { icon: "↓", label: "undervalued", title: "Podhodnocené" },
  fair: { icon: "→", label: "fair", title: "Férová cena" },
  slightly_overpriced: { icon: "↗", label: "slightly_overpriced", title: "Lehce předražené" },
  overpriced: { icon: "↑", label: "overpriced", title: "Předražené" },
  unknown: { icon: "•", label: "—", title: "Neznámé ocenění" },
};

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
      <div className="field">
        <label>{label}</label>
        <div className="slider-row">
          <input type="range" min={min} max={max} step={step} value={safe}
            onChange={(e) => onChange(key, e.target.value)} />
          <span className="slider-val">{fmtVal(safe, fmt)}</span>
        </div>
      </div>
    );
  }

  if (type === "boolean") {
    const checked = raw === "true" || raw === true;
    return (
      <div className="field">
        <div className="bool-row">
          <input type="checkbox" id={key} checked={checked}
            onChange={(e) => onChange(key, String(e.target.checked))} />
          <label htmlFor={key} style={{ marginBottom: 0, cursor: "pointer" }}>{label}</label>
        </div>
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
  const fileInputRef = useRef(null);
  const logsModalBodyRef = useRef(null);
  const prevIsRunningRef = useRef(null);

  const isRunning = Boolean(status?.running);
  const busy = loading || initialLoading || isRunning || runPhase !== "idle";

  useEffect(() => {
    document.documentElement.classList.toggle("theme-dark", theme === "dark");
    window.localStorage.setItem("sauto_theme", theme);
  }, [theme]);

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
    await Promise.all([fetchParams(), fetchStatus(), fetchResults(), fetchLogs(), fetchApiHealth()]);
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
    const visibleIds = items.map((item) => resultKey(item)).filter(Boolean);
    if (visibleIds.length === 0) return;
    const allSelected = visibleIds.every((id) => selectedIds.includes(id));
    setSelectedIds(allSelected ? selectedIds.filter((id) => !visibleIds.includes(id)) : Array.from(new Set([...selectedIds, ...visibleIds])));
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
      setMessage(`Smazáno ${selectedCount} záznamů.`);
      setSelectedIds([]);
      await refreshAll();
    } catch (e) {
      setMessage(e.message);
    } finally {
      setLoading(false);
    }
  }

  async function clearAllResults() {
    if (!window.confirm("Opravdu vymazat všechna výsledky?")) return;
    setLoading(true);
    try {
      await postResultAction("/api/results/clear", { path: resultsPath });
      setMessage("Výsledky byly smazány.");
      setSelectedIds([]);
      await refreshAll();
    } catch (e) {
      setMessage(e.message);
    } finally {
      setLoading(false);
    }
  }

  async function markSelected(marked) {
    if (selectedCount === 0) return;
    setLoading(true);
    try {
      await postResultAction("/api/results/mark", { ids: selectedIds, marked });
      setMessage(marked ? "Výsledky označeny." : "Označení zrušeno.");
      await fetchResults();
    } catch (e) {
      setMessage(e.message);
    } finally {
      setLoading(false);
    }
  }

  async function exportResults(scope) {
    const visible = items;
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
  const visibleItems = items;
  const allVisibleSelected = visibleItems.length > 0 && visibleItems.every((item) => selectedIds.includes(resultKey(item)));

  return (
    <>
    <div className="app">
      <div className="topbar">
        <h1>DobráKára</h1>
        <span className={`status-dot${busy ? " running" : ""}`}>
          {statusLabel()}
        </span>
        <div className="topbar-spacer" />
        <button
          type="button"
          className="theme-toggle"
          onClick={() => setTheme((prev) => (prev === "dark" ? "light" : "dark"))}
          title="Přepnout tmavý režim"
        >
          {theme === "dark" ? "Světlý režim" : "Tmavý režim"}
        </button>
        {apiHealth && (
          <div className={`api-status-chip${apiHealth.status === "ok" ? " up" : " down"}`}>
            <span className="api-status-dot" />
            <span className="api-status-label">API {apiHealth.status === "ok" ? "UP" : "DOWN"}</span>
            {apiHealth.status === "ok" && (
              <>
                <span className="api-status-sep">|</span>
                <span className="api-status-meta">v{apiHealth.version}</span>
                <span className="api-status-sep">·</span>
                <span className="api-status-meta">Python {apiHealth.python}</span>
                <span className="api-status-sep">·</span>
                <span className="api-status-meta">↑ {fmtUptime(apiHealth.uptime_s)}</span>
              </>
            )}
          </div>
        )}
      </div>

      <div className="layout">
        {/* Sidebar */}
        <aside className="sidebar">
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
            <div className="status-panel">
              <div className="status-panel-row">
                <span className={`mini-dot${isRunning ? " running" : ""}`} />
                <strong>{statusLabel()}</strong>
              </div>
              <div className="status-panel-meta">
                <span>PID: {status?.pid || "—"}</span>
                <span>Exit: {status?.last_exit_code ?? "—"}</span>
              </div>
            </div>
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
                        .map((b) => (
                          <label key={b.value} className="catalog-item">
                            <input
                              type="checkbox"
                              checked={selectedBrands.includes(b.value)}
                              onChange={() => toggleBrand(b.value)}
                            />
                            <span>{b.label}</span>
                          </label>
                        ))}
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
                              <label key={`${brand}-${m.value}`} className="catalog-item model">
                                <input
                                  type="checkbox"
                                  checked={selectedModels.includes(m.value)}
                                  onChange={() => toggleModel(m.value)}
                                />
                                <span>{m.label}</span>
                              </label>
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
              {showAdvanced ? "Skrýt advanced nastavení" : "Zobrazit advanced nastavení"}
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
            <button className="btn-primary" onClick={run} disabled={busy}>
              {busy ? "Pracuji…" : "Spustit"}
            </button>
            <button onClick={save} disabled={busy}>Uložit</button>
            <button onClick={refreshAll} disabled={busy}>Obnovit</button>
          </div>
          {message && <p className="msg">{message}</p>}
        </aside>

        {/* Main */}
        <div className="main">
          <div className="debug-ticker-wrap">
            <span className={`debug-ticker-dot${isRunning ? " active" : ""}`} title={isRunning ? "Běží" : "Nečinný"} />
            <span className="debug-ticker-text" title={logs.length > 0 ? logs[logs.length - 1] : ""}>
              <span className="debug-prefix">[{tickerPrefix()}]</span>{" "}
              {logs.length > 0 ? logs[logs.length - 1] : <span className="debug-ticker-empty">Žádný log výstup.</span>}
            </span>
            <button
              className="debug-history-btn"
              onClick={() => { setShowLogsModal(true); setTimeout(() => { if (logsModalBodyRef.current) logsModalBodyRef.current.scrollTop = logsModalBodyRef.current.scrollHeight; }, 50); }}
            >
              Historie ({logs.length})
            </button>
          </div>

          {status && (
            <div className="status-bar">
              <span><span className="lbl">Stav</span> {statusLabel()}</span>
              <span><span className="lbl">Start</span> {fmtDate(status.last_started_at)}</span>
              <span><span className="lbl">Konec</span> {fmtDate(status.last_finished_at)}</span>
              <span><span className="lbl">Exit</span> {status.last_exit_code ?? "—"}</span>
              <span><span className="lbl">PID</span> {status.pid || "—"}</span>
            </div>
          )}

          <div className="results-hd results-header">
            <div>
              <strong>Výsledky</strong>
              <span className="muted">{items.length} záznamů</span>
              {scraperRunningFromResults && <span className="muted"> · běží scrape, data se doplňují</span>}
            </div>
            <div className="results-actions">
              <button className="link-btn" onClick={() => exportResults("all")}>Export všech</button>
              <button className="link-btn" onClick={() => exportResults("selected")} disabled={selectedCount === 0}>Export vybraných</button>
              <button className="link-btn" onClick={() => markSelected(true)} disabled={selectedCount === 0}>Označit</button>
              <button className="link-btn" onClick={() => markSelected(false)} disabled={selectedCount === 0}>Odznačit</button>
              <button className="link-btn danger" onClick={deleteSelected} disabled={selectedCount === 0}>Smazat</button>
              <button className="link-btn danger" onClick={clearAllResults}>Smazat vše</button>
              <button className="link-btn" onClick={() => fileInputRef.current?.click()}>Import</button>
              <button className="link-btn" onClick={refreshAll}>Obnovit</button>
            </div>
          </div>
          <input
            ref={fileInputRef}
            type="file"
            accept="application/json,.json"
            style={{ display: "none" }}
            onChange={(e) => importResultsFile(e.target.files?.[0])}
          />

          <div className="selection-bar">
            <div>
              <strong>{selectedCount}</strong> vybraných
              <span className="muted"> · zdroj {resultsPath}</span>
            </div>
            <div className="selection-actions">
              <button className="link-btn" onClick={toggleSelectVisible}>
                {items.every((item) => selectedIds.includes(resultKey(item))) ? "Odznačit viditelné" : "Vybrat viditelné"}
              </button>
              <button className="link-btn" onClick={() => setSelectedIds([])} disabled={selectedCount === 0}>Vyčistit výběr</button>
            </div>
          </div>

          <div className="table-wrap">
            {items.length === 0 ? (
              <p className="empty">Žádné výsledky — spusť scraper a obnov.</p>
            ) : (
              <table>
                <thead>
                  <tr>
                    <th className="cell-check"><input type="checkbox" checked={allVisibleSelected} onChange={toggleSelectVisible} /></th>
                    <th className="cell-mark"></th>
                    <th>Skóre</th>
                    <th>Název</th>
                    <th>Cena (Kč)</th>
                    <th>kW</th>
                    <th>Km</th>
                    <th>Pohon</th>
                    <th>Převod.</th>
                    <th>Valuation</th>
                    <th>Náklady/rok</th>
                    <th></th>
                  </tr>
                </thead>
                <tbody>
                  {visibleItems.map((item, i) => {
                    const key = resultKey(item);
                    const selected = selectedIds.includes(key);
                    const marked = markedIds.includes(String(item.ad_id));
                    const valuationKey = item.valuation_label || "unknown";
                    const valuation = VALUATION_META[valuationKey] || VALUATION_META.unknown;
                    return (
                    <tr key={item.ad_id || i} className={`${selected ? "row-selected" : ""}${marked ? " row-marked" : ""}`}>
                      <td className="cell-check">
                        <input type="checkbox" checked={selected} onChange={() => toggleSelected(key)} />
                      </td>
                      <td className="cell-mark">
                        <button className={`mark-chip${marked ? " marked" : ""}`} onClick={() => markSelected(marked ? false : true)} disabled={!selected && !marked}>★</button>
                      </td>
                      <td>
                        <span className={`score ${(item.score ?? 0) >= 90 ? "score-hi" : ""}`}>
                          {item.score ?? "—"}
                        </span>
                      </td>
                      <td className="name-cell">{item.name || "—"}</td>
                      <td>{item.price ? item.price.toLocaleString("cs-CZ") : "—"}</td>
                      <td>{item.power_kw ?? "—"}</td>
                      <td>{item.tachometer ? item.tachometer.toLocaleString("cs-CZ") : "—"}</td>
                      <td>{item.drive_type || "—"}</td>
                      <td>{item.gearbox_type || "—"}</td>
                      <td>
                        <span
                          className={`pill valuation-pill pill-${valuationKey}`}
                          title={`${valuation.title} (${valuation.label})`}
                          aria-label={`${valuation.title} (${valuation.label})`}
                        >
                          <span className="valuation-icon" aria-hidden="true">{valuation.icon}</span>
                        </span>
                      </td>
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
            <button className="debug-modal-close" onClick={() => setShowLogsModal(false)}>✕</button>
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
            <button className="debug-modal-close" onClick={() => setPopupLog(null)}>✕</button>
          </div>
          <pre className="log-popup-body">{popupLog}</pre>
          <div className="log-popup-foot">
            <button className="btn-sm" onClick={() => { navigator.clipboard.writeText(popupLog).catch(() => null); }}>Kopírovat</button>
            <button className="btn-sm secondary" onClick={() => setPopupLog(null)}>Zavřít</button>
          </div>
        </div>
      </div>
    )}
    </div>
    </>
  );
}
