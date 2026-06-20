import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Moon, Sun, X, History } from "lucide-react";
import TabBar from "./components/TabBar";
import ProjectSetup from "./components/ProjectSetup";
import ProjectRunning from "./components/ProjectRunning";
import ProjectQueued from "./components/ProjectQueued";
import ProjectResults from "./components/ProjectResults";
import TerminalBar from "./components/TerminalBar";
import { useProjects } from "./hooks/useProjects";
import { fetchBrands, fetchBodies, fetchModels, fetchResults, fetchEquipment } from "./utils/api";
import { csvToArray, uniq } from "./utils/scoring";

export default function App() {
  const [theme, setTheme] = useState(() => {
    const stored = window.localStorage.getItem("sauto_theme");
    return stored === "dark" ? "dark" : "light";
  });
  const [brandOptions, setBrandOptions] = useState([]);
  const [bodyOptions, setBodyOptions] = useState([]);
  const [equipmentOptions, setEquipmentOptions] = useState([]);
  const [modelsByBrand, setModelsByBrand] = useState({});
  const [loadingModelsByBrand, setLoadingModelsByBrand] = useState({});
  const [tickerStep, setTickerStep] = useState(0);
  const [showLogsModal, setShowLogsModal] = useState(false);
  const [popupLog, setPopupLog] = useState(null);
  const [toastMsg, setToastMsg] = useState("");
  const [toastType, setToastType] = useState("");
  const [isSwitchingProject, startSwitchProjectTransition] = React.useTransition();
  const toastTimer = useRef(null);
  const logsModalBodyRef = useRef(null);

  const {
    projects,
    activeProject,
    activeProjectId,
    globalLogs,
    scraperRunning,
    addProject,
    removeProject,
    activateProject,
    updateProject,
    updateProjectConfig,
    runProject,
    setProjects,
  } = useProjects(brandOptions, modelsByBrand);

  // Theme
  useEffect(() => {
    document.documentElement.classList.toggle("theme-dark", theme === "dark");
    window.localStorage.setItem("sauto_theme", theme);
  }, [theme]);

  // Ticker
  useEffect(() => {
    if (!scraperRunning) {
      setTickerStep(0);
      return;
    }
    const t = setInterval(() => {
      setTickerStep((prev) => (prev + 1) % 4);
    }, 650);
    return () => clearInterval(t);
  }, [scraperRunning]);

  function tickerPrefix() {
    if (!scraperRunning) return "Poslední log";
    const phases = ["Crawling", "Načítám", "Zpracovávám", "Kontroluji"];
    const dots = [".", "..", "...", "...."];
    return `${phases[tickerStep]} ${dots[tickerStep]}`;
  }

  // Fetch catalog data on mount
  useEffect(() => {
    fetchBrands().then(setBrandOptions).catch(() => {});
    fetchBodies().then(setBodyOptions).catch(() => {});
    fetchEquipment().then(setEquipmentOptions).catch(() => {});
  }, []);

  // Fetch models for selected brands
  const selectedBrands = useMemo(() => {
    if (!activeProject) return [];
    return uniq(csvToArray(activeProject.config?.manufacturer_seo_name));
  }, [activeProject?.config?.manufacturer_seo_name]);

  useEffect(() => {
    selectedBrands.forEach((brand) => {
      const b = String(brand || "").trim();
      if (!b || modelsByBrand[b] || loadingModelsByBrand[b]) return;
      setLoadingModelsByBrand((prev) => ({ ...prev, [b]: true }));
      fetchModels(b)
        .then((items) => {
          setModelsByBrand((prev) => ({ ...prev, [b]: items }));
        })
        .catch(() => {
          setModelsByBrand((prev) => ({ ...prev, [b]: [] }));
        })
        .finally(() => {
          setLoadingModelsByBrand((prev) => ({ ...prev, [b]: false }));
        });
    });
  }, [selectedBrands]);

  // Refresh project results
  const refreshProjectResults = useCallback(async () => {
    if (!activeProject) return;
    try {
      const data = await fetchResults(activeProject.resultsPath);
      updateProject(activeProject.id, {
        results: data.items || [],
        markedIds: data.marked_ids || [],
      });
    } catch {
      // ignore
    }
  }, [activeProject, updateProject]);

  const updateActiveProject = useCallback(
    (updates) => {
      if (!activeProjectId) return;
      updateProject(activeProjectId, updates);
    },
    [activeProjectId, updateProject]
  );

  const updateActiveProjectConfig = useCallback(
    (updates) => {
      if (!activeProjectId) return;
      updateProjectConfig(activeProjectId, updates);
    },
    [activeProjectId, updateProjectConfig]
  );

  const activateProjectSmooth = useCallback(
    (id) => {
      startSwitchProjectTransition(() => {
        activateProject(id);
      });
    },
    [activateProject]
  );

  // Toast
  function showToast(msg, type = "") {
    if (toastTimer.current) clearTimeout(toastTimer.current);
    setToastMsg(msg);
    setToastType(type);
    toastTimer.current = setTimeout(() => {
      setToastMsg("");
      setToastType("");
    }, 3000);
  }

  // Render active project content based on phase
  function renderProjectContent() {
    if (!activeProject) {
      return (
        <div className="no-project">
          <p>Žádný projekt. Klikni na "+ Nový" pro vytvoření prvního projektu.</p>
        </div>
      );
    }

    switch (activeProject.phase) {
      case "config":
        return (
          <ProjectSetup
            project={activeProject}
            brandOptions={brandOptions}
            bodyOptions={bodyOptions}
            equipmentOptions={equipmentOptions}
            modelsByBrand={modelsByBrand}
            loadingModelsByBrand={loadingModelsByBrand}
            onUpdateConfig={updateActiveProjectConfig}
            onUpdateProject={updateActiveProject}
            onRun={runProject}
            isRunning={scraperRunning}
          />
        );
      case "running":
        return (
          <ProjectRunning
            project={activeProject}
            globalLogs={globalLogs}
          />
        );
      case "queued":
        return (
          <ProjectQueued project={activeProject} />
        );
      case "done":
        return (
          <ProjectResults
            project={activeProject}
            onUpdateProject={updateActiveProject}
            onRefresh={refreshProjectResults}
          />
        );
      case "error":
        return (
          <div className="project-error">
            <h2>Chyba</h2>
            <p>{activeProject.errorMessage || "Neznámá chyba."}</p>
            <button
              className="btn-primary"
              onClick={() => updateProject(activeProject.id, { phase: "config", errorMessage: "" })}
            >
              Zpět na konfiguraci
            </button>
          </div>
        );
      default:
        return null;
    }
  }

  return (
    <>
      <div className="app">
        {/* Top bar */}
        <div className="topbar">
          <div className="brand-block">
            <h1>Sauto Scraper</h1>
          </div>
          <button
            type="button"
            className="theme-toggle"
            onClick={() => setTheme((prev) => (prev === "dark" ? "light" : "dark"))}
            title="Přepnout tmavý režim"
          >
            {theme === "dark" ? (
              <>
                <Sun className="ui-icon" aria-hidden="true" /> Světlý
              </>
            ) : (
              <>
                <Moon className="ui-icon" aria-hidden="true" /> Tmavý
              </>
            )}
          </button>
        </div>

        {/* Tab bar */}
        <TabBar
          projects={projects}
          activeProjectId={activeProjectId}
          onActivate={activateProjectSmooth}
          onRemove={removeProject}
          onAdd={() => addProject()}
          scraperRunning={scraperRunning}
        />

        {/* Main content */}
        <div className="main-content">
          {isSwitchingProject && (
            <div className="project-switch-overlay" role="status" aria-live="polite">
              <span className="project-switch-pill">Prepinam projekt...</span>
            </div>
          )}
          {renderProjectContent()}
        </div>

        {/* Terminal bar */}
        <TerminalBar
          scraperRunning={scraperRunning}
          globalLogs={globalLogs}
          tickerStep={tickerStep}
          tickerPrefix={tickerPrefix()}
          onShowHistory={() => {
            setShowLogsModal(true);
            setTimeout(() => {
              if (logsModalBodyRef.current)
                logsModalBodyRef.current.scrollTop = logsModalBodyRef.current.scrollHeight;
            }, 50);
          }}
        />
      </div>

      {/* Logs Modal */}
      {showLogsModal && (
        <div className="debug-modal-overlay" onClick={() => setShowLogsModal(false)}>
          <div className="debug-modal" onClick={(e) => e.stopPropagation()}>
            <div className="debug-modal-head">
              <strong>Debug výpis — Historie</strong>
              <span className="muted">{globalLogs.length} řádků</span>
              <button className="debug-modal-close" onClick={() => setShowLogsModal(false)}>
                <X className="ui-icon" aria-hidden="true" />
              </button>
            </div>
            <div className="debug-modal-body" ref={logsModalBodyRef}>
              {globalLogs.length === 0 ? (
                <div className="debug-empty">Zatím žádný log výstup.</div>
              ) : (
                globalLogs.map((line, i) => (
                  <div
                    key={`log-${i}`}
                    className="debug-modal-line"
                    onClick={() => setPopupLog(line)}
                  >
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
              <button
                className="btn-sm"
                onClick={() => {
                  navigator.clipboard.writeText(popupLog).catch(() => null);
                }}
              >
                Kopírovat
              </button>
              <button className="btn-sm secondary" onClick={() => setPopupLog(null)}>
                Zavřít
              </button>
            </div>
          </div>
        </div>
      )}

      {toastMsg && (
        <div className={`toast toast-${toastType || "info"}`}>
          <span>{toastMsg}</span>
          <button
            className="toast-close"
            onClick={() => {
              setToastMsg("");
              setToastType("");
            }}
          >
            <X className="ui-icon" style={{ width: 13, height: 13 }} />
          </button>
        </div>
      )}
    </>
  );
}