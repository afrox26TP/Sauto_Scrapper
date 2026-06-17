// ── API wrapper ──
const API_BASE = "http://localhost:8000";

function apiUrl(path, query = {}) {
  const url = new URL(`${API_BASE}${path}`);
  Object.entries(query).forEach(([k, v]) => {
    if (v !== undefined && v !== null && v !== "") url.searchParams.set(k, v);
  });
  return url.toString();
}

async function request(method, path, body = null, query = {}) {
  const opts = {
    method,
    headers: body ? { "Content-Type": "application/json" } : undefined,
  };
  if (body) opts.body = JSON.stringify(body);
  const res = await fetch(apiUrl(path, query), opts);
  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    const detail = err.detail || `Request failed (${res.status})`;
    throw Object.assign(new Error(detail), { status: res.status, data: err });
  }
  return res.json();
}

export async function fetchParams() {
  const data = await request("GET", "/api/params");
  return data.params || {};
}

export async function saveParams(params) {
  const res = await request("PUT", "/api/params", { params });
  return res;
}

export async function fetchStatus() {
  return request("GET", "/api/status");
}

export async function fetchHealth() {
  return request("GET", "/api/health");
}

export async function fetchResults(path = undefined) {
  const query = path ? { path } : {};
  const data = await request("GET", "/api/results", null, query);
  return data;
}

export async function fetchLogs(limit = 160) {
  const data = await request("GET", "/api/logs", null, { limit });
  return data.lines || [];
}

export async function runScraper(outputFile = "data/sauto_interesting.json") {
  const data = await request("POST", "/api/run", { output_file: outputFile });
  return data;
}

export async function deleteResultItems(ids, path) {
  return request("POST", "/api/results/delete", { ids, path });
}

export async function clearResults(path) {
  return request("POST", "/api/results/clear", { path });
}

export async function markResultItems(ids, marked, path) {
  return request("POST", "/api/results/mark", { ids, marked });
}

export async function importResults(items, path) {
  return request("POST", "/api/results/import", { items, path });
}

export async function fetchBrands() {
  const data = await request("GET", "/api/catalog/brands");
  return Array.isArray(data.items) ? data.items : [];
}

export async function fetchModels(brand) {
  const data = await request("GET", "/api/catalog/models", null, { brand });
  return Array.isArray(data.items) ? data.items : [];
}

export async function fetchEquipment() {
  const data = await request("GET", "/api/catalog/equipment");
  return Array.isArray(data.items) ? data.items : [];
}

export async function fetchBodies() {
  const data = await request("GET", "/api/catalog/bodies");
  return Array.isArray(data.items) ? data.items : [];
}

export async function fetchScoringPresets() {
  const data = await request("GET", "/api/scoring/presets");
  return {
    builtin: data.builtin || {},
    custom: data.custom || {},
  };
}

export async function saveCustomPreset(payload, presetId = null) {
  if (presetId) {
    return request("PUT", `/api/scoring/presets/custom/${encodeURIComponent(presetId)}`, payload);
  }
  return request("POST", "/api/scoring/presets/custom", payload);
}

export async function deleteCustomPreset(presetId) {
  return request("DELETE", `/api/scoring/presets/custom/${encodeURIComponent(presetId)}`);
}

export { API_BASE };