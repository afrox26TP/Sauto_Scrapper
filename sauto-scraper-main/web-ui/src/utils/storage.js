// ── localStorage persistence for projects ──

const STORAGE_KEY = "sauto_projects";

function generateId() {
  return "proj_" + Date.now().toString(36) + "_" + Math.random().toString(36).slice(2, 8);
}

export function createProject(name = "", config = {}) {
  return {
    id: generateId(),
    name: name || "Nový projekt",
    customName: !!name,
    phase: "config", // config | running | queued | done | error
    queuePosition: 0,
    config: { ...config },
    results: [],
    markedIds: [],
    logs: [],
    resultsPath: `data/${generateId()}_results.json`,
    selectedPreset: "balanced",
    createdAt: Date.now(),
    errorMessage: "",
  };
}

export function loadProjects() {
  try {
    const raw = window.localStorage.getItem(STORAGE_KEY);
    if (!raw) return [];
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    // Reset running/queued projects to config if they were interrupted
    return parsed.map((p) => {
      if (p.phase === "running" || p.phase === "queued") {
        return { ...p, phase: "config", queuePosition: 0, logs: [...(p.logs || []), "[systém] Stav resetován po obnovení stránky."] };
      }
      return p;
    });
  } catch {
    return [];
  }
}

export function saveProjects(projects) {
  try {
    // Don't store too much data in localStorage (limit results to prevent quota)
    const trimmed = projects.map((p) => ({
      ...p,
      results: (p.results || []).slice(0, 500),
      logs: (p.logs || []).slice(-500),
    }));
    window.localStorage.setItem(STORAGE_KEY, JSON.stringify(trimmed));
  } catch {
    // localStorage quota exceeded - try to trim more
    try {
      const minimal = projects.map((p) => ({
        ...p,
        results: [],
        logs: (p.logs || []).slice(-200),
      }));
      window.localStorage.setItem(STORAGE_KEY, JSON.stringify(minimal));
    } catch {
      // Give up
    }
  }
}

export function generateAutoName(config, brandOptions = [], modelsByBrand = {}) {
  const parts = [];

  // Brands
  const brands = String(config.manufacturer_seo_name || "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
  if (brands.length > 0) {
    const brandLabels = brands
      .map((b) => {
        const found = brandOptions.find((opt) => opt.value === b);
        return found ? found.label : b;
      })
      .slice(0, 3);
    parts.push(brandLabels.join(", "));
  }

  // Price range
  const priceFrom = parseInt(config.price_from);
  const priceTo = parseInt(config.price_to);
  if (!isNaN(priceFrom) && priceFrom > 0) {
    parts.push(`od ${priceFrom.toLocaleString("cs-CZ")} Kč`);
  }
  if (!isNaN(priceTo) && priceTo > 0) {
    parts.push(`do ${priceTo.toLocaleString("cs-CZ")} Kč`);
  }

  // Year
  const yearFrom = parseInt(config.year_from);
  const yearTo = parseInt(config.year_to);
  if (!isNaN(yearFrom) && yearFrom > 0) {
    parts.push(`rok ${yearFrom}+`);
  }
  if (!isNaN(yearTo) && yearTo > 0) {
    parts.push(`rok ≤${yearTo}`);
  }

  // Fuel
  const fuel = String(config.fuel_seo || "").trim();
  if (fuel) {
    const fuelMap = { benzin: "benzín", nafta: "nafta", elektro: "elektro", hybrid: "hybrid", "lpg-benzin": "LPG" };
    parts.push(fuelMap[fuel] || fuel);
  }

  // Power
  const powerFrom = parseInt(config.power_from);
  const powerTo = parseInt(config.power_to);
  if (!isNaN(powerFrom) && powerFrom > 0) {
    parts.push(`${powerFrom}+ kW`);
  }
  if (!isNaN(powerTo) && powerTo > 0) {
    parts.push(`≤${powerTo} kW`);
  }

  if (parts.length === 0) return "Nový projekt";
  return parts.join(" · ");
}