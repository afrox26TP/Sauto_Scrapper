import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { flushSync } from "react-dom";
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
  const toastTimer = useRef(null);
  const logsModalBodyRef = useRef(null);
  const uiActiveProjectIdRef = useRef(null);
  const displayProjectIdRef = useRef(null);
  const switchRafRef = useRef(null);
  const switchTokenRef = useRef(0);

  const {
    projects,
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

  const [uiActiveProjectId, setUiActiveProjectId] = useState(null);
  const [displayProjectId, setDisplayProjectId] = useState(null);

  useEffect(() => {
    uiActiveProjectIdRef.current = uiActiveProjectId;
  }, [uiActiveProjectId]);

  useEffect(() => {
    displayProjectIdRef.current = displayProjectId;
  }, [displayProjectId]);

  useEffect(() => {
    return () => {
      if (switchRafRef.current) cancelAnimationFrame(switchRafRef.current);
    };
  }, []);

  useEffect(() => {
    if (projects.length === 0) {
      if (uiActiveProjectId !== null) setUiActiveProjectId(null);
      if (displayProjectId !== null) setDisplayProjectId(null);
      return;
    }

    const exists = projects.some((p) => p.id === uiActiveProjectId);
    if (!exists) {
      const nextId = projects[0].id;
      setUiActiveProjectId(nextId);
      setDisplayProjectId(nextId);
      activateProject(nextId);
      return;
    }

    const displayExists = projects.some((p) => p.id === displayProjectId);
    if (!displayExists) {
      setDisplayProjectId(uiActiveProjectId || projects[0].id);
    }
  }, [projects, uiActiveProjectId, displayProjectId, activateProject]);

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
  const currentProject = useMemo(() => {
    const byDisplay = projects.find((p) => p.id === displayProjectId);
    if (byDisplay) return byDisplay;
    const byUi = projects.find((p) => p.id === uiActiveProjectId);
    return byUi || projects[0] || null;
  }, [projects, displayProjectId, uiActiveProjectId]);

  const selectedBrands = useMemo(() => {
    if (!currentProject) return [];
    return uniq(csvToArray(currentProject.config?.manufacturer_seo_name));
  }, [currentProject?.config?.manufacturer_seo_name]);

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
    if (!currentProject) return;
    try {
      const data = await fetchResults(currentProject.resultsPath);
      updateProject(currentProject.id, {
        results: data.items || [],
        markedIds: data.marked_ids || [],
      });
    } catch {
      // ignore
    }
  }, [currentProject, updateProject]);

  const updateActiveProject = useCallback(
    (updates) => {
      if (!uiActiveProjectId) return;
      updateProject(uiActiveProjectId, updates);
    },
    [uiActiveProjectId, updateProject]
  );

  const updateActiveProjectConfig = useCallback(
    (updates) => {
      if (!uiActiveProjectId) return;
      updateProjectConfig(uiActiveProjectId, updates);
    },
    [uiActiveProjectId, updateProjectConfig]
  );

  const activateProjectSmooth = useCallback((id) => {
    if (!id || id === uiActiveProjectIdRef.current) return;
    // Paint active tab highlight immediately, independent from content loading.
    flushSync(() => {
      setUiActiveProjectId(id);
    });

    // Delay heavy content swap by one frame so tab highlight paints instantly.
    if (switchRafRef.current) cancelAnimationFrame(switchRafRef.current);
    const token = ++switchTokenRef.current;
    switchRafRef.current = requestAnimationFrame(() => {
      if (switchTokenRef.current !== token) return;
      setDisplayProjectId(id);
      activateProject(id);
      switchRafRef.current = null;
    });
  }, [activateProject]);

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
    if (!currentProject) {
      return (
        <div className="no-project">
          <p>Žádný projekt. Klikni na "+ Nový" pro vytvoření prvního projektu.</p>
        </div>
      );
    }

    switch (currentProject.phase) {
      case "config":
        return (
          <ProjectSetup
            project={currentProject}
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
            project={currentProject}
            globalLogs={globalLogs}
          />
        );
      case "queued":
        return (
          <ProjectQueued project={currentProject} />
        );
      case "done":
        return (
          <ProjectResults
            key={currentProject.id}
            project={currentProject}
            onUpdateProject={updateActiveProject}
            onRefresh={refreshProjectResults}
          />
        );
      case "error":
        return (
          <div className="project-error">
            <h2>Chyba</h2>
            <p>{currentProject.errorMessage || "Neznámá chyba."}</p>
            <button
              className="btn-primary"
              onClick={() => updateProject(currentProject.id, { phase: "config", errorMessage: "" })}
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
          activeProjectId={uiActiveProjectId}
          onActivate={activateProjectSmooth}
          onRemove={removeProject}
          onAdd={() => addProject()}
          scraperRunning={scraperRunning}
        />

        {/* Main content */}
        <div className="main-content">
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