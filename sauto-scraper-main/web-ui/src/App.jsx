import { useEffect, useState } from "react";

const API_BASE = "http://localhost:8000";

const PARAM_GROUPS = [
  {
    label: "Hledání",
    fields: [
      { key: "manufacturer_seo_name", type: "text", label: "Výrobce" },
      { key: "model_seo_name", type: "text", label: "Model" },
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
      { key: "interesting_min_score", type: "slider", label: "Min. skóre", min: 0, max: 300, step: 1 },
      { key: "interesting_top_n", type: "slider", label: "Top N", min: 1, max: 100, step: 1 },
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
  const [status, setStatus] = useState(null);
  const [items, setItems] = useState([]);
  const [message, setMessage] = useState("");
  const [loading, setLoading] = useState(false);

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
    const res = await fetch(`${API_BASE}/api/results?path=data/sauto_interesting.json`);
    const data = await res.json();
    setItems(data.items || []);
  }

  useEffect(() => {
    Promise.all([fetchParams(), fetchStatus(), fetchResults()]).catch(() =>
      setMessage("API není dostupné — spusť backend na portu 8000.")
    );
  }, []);

  useEffect(() => {
    const t = setInterval(() => fetchStatus().catch(() => null), 2000);
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
    setMessage("");
    try {
      const saveRes = await fetch(`${API_BASE}/api/params`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ params }),
      });
      if (!saveRes.ok) throw new Error("Uložení parametrů selhalo.");
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
      await fetchStatus();
    } catch (e) {
      setMessage(e.message);
    } finally {
      setLoading(false);
    }
  }

  const isRunning = status?.running;

  // Extra params from params.json that aren't in PARAM_GROUPS (e.g. discord webhook)
  const extraKeys = Object.keys(params).filter((k) => !IGNORED_KEYS.has(k));

  return (
    <div className="app">
      <div className="topbar">
        <h1>Sauto Scraper</h1>
        <span className={`status-dot${isRunning ? " running" : ""}`}>
          {isRunning ? "Běží" : "Nečinný"}
        </span>
      </div>

      <div className="layout">
        {/* Sidebar */}
        <aside className="sidebar">
          {PARAM_GROUPS.map((group) => (
            <div key={group.label} className="param-group">
              <div className="group-label">{group.label}</div>
              {group.fields.map((def) => (
                <Field key={def.key} def={def} value={params[def.key]} onChange={setParam} />
              ))}
            </div>
          ))}

          {extraKeys.length > 0 && (
            <div className="param-group">
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

          <div className="actions">
            <button className="btn-primary" onClick={run} disabled={loading || isRunning}>
              {isRunning ? "Běží…" : "Spustit"}
            </button>
            <button onClick={save} disabled={loading}>Uložit</button>
            <button onClick={fetchResults} disabled={loading}>Obnovit</button>
          </div>
          {message && <p className="msg">{message}</p>}
        </aside>

        {/* Main */}
        <div className="main">
          {status && (
            <div className="status-bar">
              <span><span className="lbl">Start</span> {fmtDate(status.last_started_at)}</span>
              <span><span className="lbl">Konec</span> {fmtDate(status.last_finished_at)}</span>
              <span><span className="lbl">Exit</span> {status.last_exit_code ?? "—"}</span>
              <span><span className="lbl">PID</span> {status.pid || "—"}</span>
            </div>
          )}

          <div className="results-hd">
            <strong>Výsledky</strong> <span className="muted">{items.length} záznamů</span>
          </div>

          <div className="table-wrap">
            {items.length === 0 ? (
              <p className="empty">Žádné výsledky — spusť scraper a obnov.</p>
            ) : (
              <table>
                <thead>
                  <tr>
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
                  {items.slice(0, 100).map((item, i) => (
                    <tr key={item.ad_id || i}>
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
                        <span className={`pill pill-${item.valuation_label}`}>
                          {item.valuation_label || "—"}
                        </span>
                      </td>
                      <td>{item.annual_total_cost ? item.annual_total_cost.toLocaleString("cs-CZ") : "—"}</td>
                      <td>
                        {item.url
                          ? <a href={item.url} target="_blank" rel="noreferrer" className="link">↗</a>
                          : "—"}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
