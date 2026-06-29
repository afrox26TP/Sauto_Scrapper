// ── API wrapper ──
const RAW_API_BASE = String(import.meta.env.VITE_API_BASE_URL || "").trim();
const API_BASE = RAW_API_BASE.replace(/\/+$/, "") || (import.meta.env.DEV ? "http://127.0.0.1:8000" : "");
const AUTH_TOKEN_KEY = "sauto_auth_token";

export function getAuthToken() {
  return window.localStorage.getItem(AUTH_TOKEN_KEY) || "";
}

export function setAuthToken(token) {
  const value = String(token || "").trim();
  if (!value) {
    window.localStorage.removeItem(AUTH_TOKEN_KEY);
    return;
  }
  window.localStorage.setItem(AUTH_TOKEN_KEY, value);
}

export function clearAuthToken() {
  window.localStorage.removeItem(AUTH_TOKEN_KEY);
}

function apiUrl(path, query = {}) {
  const normalizedPath = String(path || "").startsWith("/") ? String(path) : `/${String(path || "")}`;
  const url = API_BASE
    ? new URL(normalizedPath, `${API_BASE}/`)
    : new URL(normalizedPath, window.location.origin);
  Object.entries(query).forEach(([k, v]) => {
    if (v !== undefined && v !== null && v !== "") url.searchParams.set(k, v);
  });
  return url.toString();
}

async function request(method, path, body = null, query = {}) {
  const token = getAuthToken();
  const headers = {};
  if (body) headers["Content-Type"] = "application/json";
  if (token) headers.Authorization = `Bearer ${token}`;

  const opts = {
    method,
    headers: Object.keys(headers).length > 0 ? headers : undefined,
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

export async function signup(email, password) {
  const data = await request("POST", "/api/auth/signup", { email, password });
  if (data?.token) setAuthToken(data.token);
  return data;
}

export async function login(email, password) {
  const data = await request("POST", "/api/auth/login", { email, password });
  if (data?.token) setAuthToken(data.token);
  return data;
}

export async function fetchCurrentUser() {
  const data = await request("GET", "/api/auth/me");
  return data?.user || null;
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

export async function runScraper(outputFile = "data/sauto_interesting.json", projectId = "default") {
  const data = await request("POST", "/api/run", { output_file: outputFile, project_id: projectId });
  return data;
}

export async function pauseScraper() {
  return request("POST", "/api/pause", {});
}

export async function resumeScraper() {
  return request("POST", "/api/resume", {});
}

export async function stopScraper() {
  return request("POST", "/api/stop", {});
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

export async function fetchModelCounts(brand, config = null) {
  if (config && typeof config === "object") {
    const data = await request("POST", "/api/catalog/model-counts", { brand, config });
    return Array.isArray(data.items) ? data.items : [];
  }
  const data = await request("GET", "/api/catalog/model-counts", null, { brand });
  return Array.isArray(data.items) ? data.items : [];
}

export async function fetchCatalogEstimate(config) {
  const data = await request("POST", "/api/catalog/estimate", { config: config || {} });
  return {
    count: Number(data?.count || 0),
    note: String(data?.note || ""),
    params: data?.params || {},
  };
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

export async function fetchBillingRates() {
  const data = await request("GET", "/api/billing/rates");
  return data.rates || {};
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