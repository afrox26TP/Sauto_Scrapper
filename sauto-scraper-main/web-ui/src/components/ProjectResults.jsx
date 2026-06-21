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
  const [workerRows, setWorkerRows] = useState(EMPTY_ITEMS);
  const [workerMarkedIds, setWorkerMarkedIds] = useState(EMPTY_ITEMS);
  const [workerKick, setWorkerKick] = useState(0);
  const fileInputRef = useRef(null);
  const logsModalBodyRef = useRef(null);
  const workerRef = useRef(null);
  const workerRequestRef = useRef(0);
  const rawResults = project.results ?? EMPTY_ITEMS;
  const rawMarkedIds = project.markedIds ?? EMPTY_ITEMS;
  const tableLoading = switchLoading;

  useEffect(() => {
    setSwitchLoading(true);
    const timer = setTimeout(() => setSwitchLoading(false), 220);
    setSelectedIds([]);
    setSelectedPreset(project.selectedPreset || "balanced");
    return () => clearTimeout(timer);
  }, [project.id]);

  useEffect(() => {
    const worker = new Worker(new URL("../workers/resultsWorker.js", import.meta.url), { type: "module" });
    workerRef.current = worker;

    worker.onmessage = (event) => {
      const { requestId, visibleItems, markedIds } = event.data || {};
      if (requestId !== workerRequestRef.current) return;
      setWorkerRows(Array.isArray(visibleItems) ? visibleItems : EMPTY_ITEMS);
      setWorkerMarkedIds(Array.isArray(markedIds) ? markedIds : EMPTY_ITEMS);
      setTableBusy(false);
    };

    worker.onerror = () => {
      setTableBusy(false);
    };

    return () => {
      worker.terminate();
      workerRef.current = null;
    };
  }, []);

  // Scoring presets merged
  const scoringPresets = useMemo(() => {
    const custom = project.customPresets || {};
    return { ...LOCAL_SCORING_PRESETS, ...custom };
  }, [project.customPresets]);

  useEffect(() => {
    if (tableLoading) {
      setWorkerRows(EMPTY_ITEMS);
      setWorkerMarkedIds(EMPTY_ITEMS);
      return;
    }

    if (!workerRef.current) return;

    workerRequestRef.current += 1;
    const requestId = workerRequestRef.current;

    workerRef.current.postMessage({
      requestId,
      items: rawResults,
      markedIds: rawMarkedIds,
      sortConfig,
      preset: scoringPresets[selectedPreset] || null,
    });
  }, [tableLoading, rawResults, rawMarkedIds, sortConfig, selectedPreset, scoringPresets, workerKick]);

  // Score cache
  const visibleItems = workerRows;
  const selectedIdSet = useMemo(() => new Set(selectedIds), [selectedIds]);
  const markedIdSet = useMemo(
    () => new Set(workerMarkedIds.map((id) => String(id))),
    [workerMarkedIds]
  );

  const getCachedScore = useCallback((item) => {
    return Number(item?._score ?? 0);
  }, []);

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
    setSortConfig((prev) => {
      if (prev.key !== key) return { key, direction: key === "score" ? "desc" : "asc" };
      return { key, direction: prev.direction === "asc" ? "desc" : "asc" };
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
    await onRefresh();
    // Force worker recompute even if upstream data identity did not change.
    setWorkerKick((n) => n + 1);
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
    setSelectedPreset(val);
    onUpdateProject({ selectedPreset: val });
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
              <select
                value={selectedPreset}
                onChange={(e) => handlePresetChange(e.target.value)}
              >
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