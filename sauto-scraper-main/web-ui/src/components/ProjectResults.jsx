import React, { memo, useState, useRef, useMemo, useCallback, useEffect } from "react";
import { flushSync } from "react-dom";
import {
  Download,
  Upload,
  RefreshCw,
  Star,
  Trash2,
  History,
  X,
} from "lucide-react";
import ResultsTable from "./ResultsTable";
import { CustomCheckbox, CustomSlider } from "./index";
import {
  getItemScore,
  isSuspiciousMileage,
  LOCAL_SCORING_PRESETS,
  DEFAULT_SCORE_WEIGHTS,
  ALL_WEIGHT_KEYS,
} from "../utils/scoring";
import {
  deleteResultItems,
  clearResults,
  markResultItems,
  importResults,
  fetchResults,
} from "../utils/api";

const EMPTY_ITEMS = [];

export default memo(function ProjectResults({
  project,
  onUpdateProject,
  onRefresh,
}) {
  function resultKey(item) {
    return String(item.ad_id || item.id || item.url || item.name || "");
  }

  const [selectedIds, setSelectedIds] = useState([]);
  const [sortConfig, setSortConfig] = useState({ key: "score", direction: "desc" });
  const [selectedPreset, setSelectedPreset] = useState(project.selectedPreset || "balanced");
  const [showLogsModal, setShowLogsModal] = useState(false);
  const [popupLog, setPopupLog] = useState(null);
  const [loading, setLoading] = useState(false);
  const [tableBusy, setTableBusy] = useState(false);
  const [switchLoading, setSwitchLoading] = useState(true);
  const fileInputRef = useRef(null);
  const logsModalBodyRef = useRef(null);
  const workTimerRef = useRef(null);
  const rawResults = project.results ?? EMPTY_ITEMS;
  const rawMarkedIds = project.markedIds ?? EMPTY_ITEMS;
  const tableLoading = switchLoading;
  const effectiveResults = tableLoading ? EMPTY_ITEMS : rawResults;
  const effectiveMarkedIds = tableLoading ? EMPTY_ITEMS : rawMarkedIds;

  useEffect(() => {
    setSwitchLoading(true);
    const timer = setTimeout(() => setSwitchLoading(false), 220);
    setSelectedIds([]);
    setSelectedPreset(project.selectedPreset || "balanced");
    return () => clearTimeout(timer);
  }, [project.id]);

  useEffect(() => {
    return () => {
      if (workTimerRef.current) clearTimeout(workTimerRef.current);
    };
  }, []);

  // Scoring presets merged
  const scoringPresets = useMemo(() => {
    const custom = project.customPresets || {};
    return { ...LOCAL_SCORING_PRESETS, ...custom };
  }, [project.customPresets]);

  // Format items
  const formattedItems = useMemo(() => {
    if (tableLoading) return EMPTY_ITEMS;

    const fmt = (v, style) => {
      if (v == null || !Number.isFinite(v)) return null;
      return v.toLocaleString("cs-CZ", style === "ppkw" ? { maximumFractionDigits: 2 } : style === "ppkm" ? { maximumFractionDigits: 4 } : undefined);
    };

    return effectiveResults.map((item) => ({
      ...item,
      _fmt_price: fmt(item.price),
      _fmt_tacho: fmt(item.tachometer),
      _fmt_ppkw: fmt(item.price_per_kw, "ppkw"),
      _fmt_ppkm: fmt(item.price_per_km, "ppkm"),
      _fmt_kpy: fmt(item.km_per_year),
      _fmt_atc: fmt(item.annual_total_cost),
      _suspicious: isSuspiciousMileage(item),
    }));
  }, [effectiveResults, tableLoading]);

  // Precompute score once per item for the active preset.
  const presetScoreByKey = useMemo(() => {
    const cache = new Map();
    const preset = scoringPresets[selectedPreset];
    if (!preset) return cache;
    for (const item of formattedItems) {
      cache.set(resultKey(item), getItemScore(item, preset));
    }
    return cache;
  }, [formattedItems, selectedPreset, scoringPresets]);

  // Sort
  const visibleItems = useMemo(() => {
    const list = [...formattedItems];
    if (!sortConfig.key) return list;

    list.sort((a, b) => {
      const av = sortValue(a, sortConfig.key, presetScoreByKey);
      const bv = sortValue(b, sortConfig.key, presetScoreByKey);

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
  }, [formattedItems, sortConfig, presetScoreByKey]);

  function sortValue(item, key, scoreByKey) {
    if (key === "score") {
      return scoreByKey.get(resultKey(item)) ?? 0;
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

  // Score cache
  const selectedIdSet = useMemo(() => new Set(selectedIds), [selectedIds]);
  const markedIdSet = useMemo(
    () => new Set(effectiveMarkedIds.map((id) => String(id))),
    [effectiveMarkedIds]
  );

  const getCachedScore = useCallback((item) => {
    return presetScoreByKey.get(resultKey(item)) ?? 0;
  }, [presetScoreByKey]);

  const toggleSelected = useCallback((id) => {
    const key = String(id);
    setSelectedIds((prev) =>
      prev.includes(key) ? prev.filter((x) => x !== key) : [...prev, key]
    );
  }, []);

  const toggleSelectVisible = useCallback(() => {
    const visibleIds = visibleItems.map((item) => resultKey(item)).filter(Boolean);
    if (visibleIds.length === 0) return;
    const allSelected = visibleIds.every((id) => selectedIdSet.has(id));
    setSelectedIds((prev) => {
      if (allSelected) {
        const visibleSet = new Set(visibleIds);
        return prev.filter((id) => !visibleSet.has(id));
      }
      return Array.from(new Set([...prev, ...visibleIds]));
    });
  }, [visibleItems, resultKey, selectedIdSet]);

  const toggleSort = useCallback((key) => {
    flushSync(() => setTableBusy(true));
    if (workTimerRef.current) clearTimeout(workTimerRef.current);
    workTimerRef.current = setTimeout(() => {
      setSortConfig((prev) => {
        if (prev.key !== key) return { key, direction: key === "score" ? "desc" : "asc" };
        return { key, direction: prev.direction === "asc" ? "desc" : "asc" };
      });
      setTimeout(() => setTableBusy(false), 0);
      workTimerRef.current = null;
    }, 0);
  }, []);

  const sortIndicator = useCallback((key) => {
    if (sortConfig.key !== key) return "⇅";
    return sortConfig.direction === "asc" ? "↑" : "↓";
  }, [sortConfig]);

  const selectedCount = selectedIds.length;
  const allVisibleSelected =
    visibleItems.length > 0 && visibleItems.every((item) => selectedIdSet.has(resultKey(item)));
  const softTableLoading = (tableBusy || loading) && !tableLoading;

  async function handleRefresh() {
    flushSync(() => setTableBusy(true));
    if (workTimerRef.current) clearTimeout(workTimerRef.current);
    workTimerRef.current = setTimeout(async () => {
      try {
        await onRefresh();
      } finally {
        setTableBusy(false);
      }
      workTimerRef.current = null;
    }, 0);
  }

  // Actions
  async function handleDeleteSelected() {
    if (selectedCount === 0) return;
    setLoading(true);
    try {
      await deleteResultItems(selectedIds, project.resultsPath);
      setSelectedIds([]);
      onRefresh();
    } catch (e) {
      alert(e.message);
    } finally {
      setLoading(false);
    }
  }

  async function handleClearAll() {
    if (!window.confirm("Opravdu vymazat všechna výsledky?")) return;
    setLoading(true);
    try {
      await clearResults(project.resultsPath);
      setSelectedIds([]);
      onRefresh();
    } catch (e) {
      alert(e.message);
    } finally {
      setLoading(false);
    }
  }

  const handleMark = useCallback(async (marked) => {
    if (selectedCount === 0) return;
    setLoading(true);
    try {
      await markResultItems(selectedIds, marked);
      onRefresh();
    } catch (e) {
      alert(e.message);
    } finally {
      setLoading(false);
    }
  }, [selectedCount, selectedIds, onRefresh]);

  function handleExport(scope) {
    const exportItems = scope === "selected"
      ? visibleItems.filter((item) => selectedIdSet.has(resultKey(item)))
      : visibleItems;

    const blob = new Blob([JSON.stringify(exportItems, null, 2)], { type: "application/json;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = scope === "selected" ? "sauto_selected.json" : "sauto_export.json";
    a.click();
    URL.revokeObjectURL(url);
  }

  async function handleImport(file) {
    if (!file) return;
    setLoading(true);
    try {
      const text = await file.text();
      const parsed = JSON.parse(text);
      const itemsToImport = Array.isArray(parsed) ? parsed : parsed.items;
      if (!Array.isArray(itemsToImport)) {
        throw new Error("Soubor musí obsahovat JSON pole nebo objekt s položkou items.");
      }
      await importResults(itemsToImport, project.resultsPath);
      setSelectedIds([]);
      onRefresh();
    } catch (e) {
      alert(e.message);
    } finally {
      setLoading(false);
      if (fileInputRef.current) fileInputRef.current.value = "";
    }
  }

  function handlePresetChange(val) {
    if (val === "__new__") {
      // Custom preset creation would go here
      return;
    }
    if (val === selectedPreset) return;
    flushSync(() => setTableBusy(true));
    if (workTimerRef.current) clearTimeout(workTimerRef.current);
    workTimerRef.current = setTimeout(() => {
      setSelectedPreset(val);
      onUpdateProject({ selectedPreset: val });
      setTimeout(() => setTableBusy(false), 0);
      workTimerRef.current = null;
    }, 0);
  }

  return (
    <div className="project-results">
      <div className="results-hd results-header">
        <div className="results-title-block">
          <div className="results-title-row">
            <strong>Výsledky</strong>
            <span className="muted">{project.results.length} záznamů</span>
            <label className="score-control-inline">
              <span>Bodování</span>
              <select value={selectedPreset} onChange={(e) => handlePresetChange(e.target.value)}>
                {Object.entries(scoringPresets).map(([key, preset]) => (
                  <option key={key} value={key}>
                    {preset.name || key}
                  </option>
                ))}
              </select>
            </label>
          </div>
        </div>
        <div className="results-actions">
          <button className="link-btn" onClick={() => handleExport("all")}>
            <Download className="ui-icon" aria-hidden="true" /> Export
          </button>
          <button className="link-btn" onClick={() => fileInputRef.current?.click()}>
            <Upload className="ui-icon" aria-hidden="true" /> Import
          </button>
          <button className="link-btn" onClick={handleRefresh}>
            <RefreshCw className="ui-icon" aria-hidden="true" /> Obnovit
          </button>
        </div>
      </div>
      <input
        ref={fileInputRef}
        type="file"
        accept="application/json,.json"
        style={{ display: "none" }}
        onChange={(e) => handleImport(e.target.files?.[0])}
      />

      {selectedCount > 0 && (
        <div className="selection-bar">
          <div>
            <strong>{selectedCount}</strong> vybraných
          </div>
          <div className="selection-actions">
            <button className="link-btn" onClick={() => handleExport("selected")}>
              <Download className="ui-icon" aria-hidden="true" /> Export
            </button>
            <button className="link-btn" onClick={() => handleMark(true)}>
              <Star className="ui-icon" aria-hidden="true" /> Označit
            </button>
            <button className="link-btn" onClick={() => handleMark(false)}>
              <Star className="ui-icon icon-muted" aria-hidden="true" /> Odznačit
            </button>
            <button className="link-btn danger" onClick={handleDeleteSelected}>
              <Trash2 className="ui-icon" aria-hidden="true" /> Smazat
            </button>
            <button className="link-btn" onClick={toggleSelectVisible}>
              {allVisibleSelected
                ? "Odznačit viditelné"
                : "Vybrat viditelné"}
            </button>
            <button className="link-btn" onClick={() => setSelectedIds([])} disabled={selectedCount === 0}>
              Vyčistit výběr
            </button>
          </div>
        </div>
      )}

      <div className="table-wrap">
        {softTableLoading && (
          <div
            className="results-soft-overlay"
            role="status"
            aria-live="polite"
          >
            <div className="results-soft-badge">
              <span className="results-soft-spinner" aria-hidden="true" />
              <span>Načítám tabulku...</span>
            </div>
          </div>
        )}
        {tableLoading ? (
          <div className="table-loading-blank" role="status" aria-live="polite">
            <div className="table-loading-core" aria-hidden="true" />
            <div className="table-loading-text">Načítám tabulku projektu...</div>
            <div className="table-loading-bars" aria-hidden="true">
              <span />
              <span />
              <span />
            </div>
          </div>
        ) : (
          <ResultsTable
            visibleItems={visibleItems}
            selectedIdSet={selectedIdSet}
            markedIdSet={markedIdSet}
            toggleSelected={toggleSelected}
            markSelected={handleMark}
            toggleSelectVisible={toggleSelectVisible}
            allVisibleSelected={allVisibleSelected}
            getCachedScore={getCachedScore}
            toggleSort={toggleSort}
            sortIndicator={sortIndicator}
            resultKey={resultKey}
          />
        )}
      </div>

      {/* Log modal */}
      {showLogsModal && (
        <div className="debug-modal-overlay" onClick={() => setShowLogsModal(false)}>
          <div className="debug-modal" onClick={(e) => e.stopPropagation()}>
            <div className="debug-modal-head">
              <strong>Debug výpis — Historie</strong>
              <span className="muted">{(project.logs || []).length} řádků</span>
              <button className="debug-modal-close" onClick={() => setShowLogsModal(false)}>
                <X className="ui-icon" aria-hidden="true" />
              </button>
            </div>
            <div className="debug-modal-body" ref={logsModalBodyRef}>
              {(project.logs || []).length === 0 ? (
                <div className="debug-empty">Zatím žádný log výstup.</div>
              ) : (
                (project.logs || []).map((line, i) => (
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
              <button className="debug-modal-close" onClick={() => setPopupLog(null)}>
                <X className="ui-icon" aria-hidden="true" />
              </button>
            </div>
            <pre className="log-popup-body">{popupLog}</pre>
            <div className="log-popup-foot">
              <button className="btn-sm" onClick={() => { navigator.clipboard.writeText(popupLog).catch(() => null); }}>
                Kopírovat
              </button>
              <button className="btn-sm secondary" onClick={() => setPopupLog(null)}>
                Zavřít
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
});