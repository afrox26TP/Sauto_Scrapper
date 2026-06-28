import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { flushSync } from "react-dom";
import { Moon, Sun, X, History, BadgeEuro, ArrowLeft } from "lucide-react";
import TabBar from "./components/TabBar";
import ProjectSetup from "./components/ProjectSetup";
import ProjectRunning from "./components/ProjectRunning";
import ProjectQueued from "./components/ProjectQueued";
import ProjectResults from "./components/ProjectResults";
import TerminalBar from "./components/TerminalBar";
import { useProjects } from "./hooks/useProjects";
import { clearAuthToken, fetchBillingRates, fetchBrands, fetchBodies, fetchCurrentUser, fetchModels, fetchModelCounts, fetchResults, fetchEquipment, getAuthToken, login, signup } from "./utils/api";
import { csvToArray, uniq } from "./utils/scoring";

export default function App() {
  const [authToken, setAuthToken] = useState(() => getAuthToken());
  const [authUser, setAuthUser] = useState(null);
  const [authBooting, setAuthBooting] = useState(true);
  const [authMode, setAuthMode] = useState("login");
  const [authEmail, setAuthEmail] = useState("");
  const [authPassword, setAuthPassword] = useState("");
  const [authError, setAuthError] = useState("");
  const [authBusy, setAuthBusy] = useState(false);
  const [showAuthModal, setShowAuthModal] = useState(false);
  const [currentPage, setCurrentPage] = useState(() =>
    window.location.pathname.startsWith("/pricing") ? "pricing" : "dashboard"
  );
  const [theme, setTheme] = useState(() => {
    const stored = window.localStorage.getItem("sauto_theme");
    return stored === "dark" ? "dark" : "light";
  });
  const [billingRates, setBillingRates] = useState(null);
  const [billingRatesError, setBillingRatesError] = useState("");
  const [brandOptions, setBrandOptions] = useState([]);
  const [bodyOptions, setBodyOptions] = useState([]);
  const [equipmentOptions, setEquipmentOptions] = useState([]);
  const [modelsByBrand, setModelsByBrand] = useState({});
  const [loadingModelsByBrand, setLoadingModelsByBrand] = useState({});
  const [loadingModelCountsByBrand, setLoadingModelCountsByBrand] = useState({});
  const [modelCountsKeyByBrand, setModelCountsKeyByBrand] = useState({});
  const [modelLoadErrorsByBrand, setModelLoadErrorsByBrand] = useState({});
  const [tickerStep, setTickerStep] = useState(0);
  const [showLogsModal, setShowLogsModal] = useState(false);
  const [popupLog, setPopupLog] = useState(null);
  const [showStopConfirmModal, setShowStopConfirmModal] = useState(false);
  const [toastMsg, setToastMsg] = useState("");
  const [toastType, setToastType] = useState("");
  const toastTimer = useRef(null);
  const logsModalBodyRef = useRef(null);
  const uiActiveProjectIdRef = useRef(null);
  const displayProjectIdRef = useRef(null);
  const switchRafRef = useRef(null);
  const switchTokenRef = useRef(0);

  const isAuthenticated = !!authToken && !!authUser;

  const {
    projects,
    scraperRunning,
    scraperPaused,
    scraperStartedAt,
    billingRates: runtimeBillingRates,
    addProject,
    removeProject,
    activateProject,
    updateProject,
    updateProjectConfig,
    runProject,
    pauseRunningProject,
    resumeRunningProject,
    stopRunningProject,
    setProjects,
  } = useProjects(brandOptions, modelsByBrand, { enabled: true });

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
    const onPopState = () => {
      setCurrentPage(window.location.pathname.startsWith("/pricing") ? "pricing" : "dashboard");
    };
    window.addEventListener("popstate", onPopState);
    return () => window.removeEventListener("popstate", onPopState);
  }, []);

  useEffect(() => {
    const token = getAuthToken();
    setAuthToken(token);
    if (!token) {
      setAuthUser(null);
      setAuthBooting(false);
      return;
    }

    setAuthBooting(true);
    fetchCurrentUser()
      .then((user) => {
        if (!user) {
          clearAuthToken();
          setAuthToken("");
          setAuthUser(null);
          return;
        }
        setAuthUser(user);
      })
      .catch(() => {
        clearAuthToken();
        setAuthToken("");
        setAuthUser(null);
      })
      .finally(() => {
        setAuthBooting(false);
      });
  }, []);

  const handleAuthSubmit = useCallback(async (event) => {
    event.preventDefault();
    if (authBusy) return;
    setAuthBusy(true);
    setAuthError("");
    try {
      const response = authMode === "signup"
        ? await signup(authEmail, authPassword)
        : await login(authEmail, authPassword);
      setAuthToken(String(response?.token || ""));
      setAuthUser(response?.user || null);
      setAuthPassword("");
      setShowAuthModal(false);
    } catch (err) {
      setAuthError(err?.message || "Přihlášení selhalo.");
    } finally {
      setAuthBusy(false);
      setAuthBooting(false);
    }
  }, [authBusy, authEmail, authMode, authPassword]);

  const handleLogout = useCallback(() => {
    clearAuthToken();
    setAuthToken("");
    setAuthUser(null);
    setAuthPassword("");
    setAuthError("");
    setShowAuthModal(false);
  }, []);

  useEffect(() => {
    if (!isAuthenticated) return;
    if (currentPage !== "pricing") return;
    fetchBillingRates()
      .then((rates) => {
        setBillingRates(rates || {});
        setBillingRatesError("");
      })
      .catch(() => {
        setBillingRates({
          run_base_czk: 5.0,
          item_czk: 0.02,
          api_call_czk: 0.05,
          proxy_run_czk: 0.0,
        });
        setBillingRatesError("Sazby z API teď nejsou dostupné, používám výchozí sazby.");
      });
  }, [currentPage, isAuthenticated]);

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

  const modelCountsRequestKey = useMemo(() => {
    if (!currentProject) return "";
    const cfg = currentProject.config || {};
    return `${currentProject.id}|${JSON.stringify(cfg)}`;
  }, [currentProject?.id, currentProject?.config]);

  const currentProjectLogs = useMemo(() => {
    if (!currentProject) return [];
    const projectLogs = currentProject.logs || [];
    const liveLogs = currentProject.phase === "running" ? (currentProject.liveLogs || []) : [];
    return [...projectLogs, ...liveLogs].slice(-200);
  }, [currentProject]);

  useEffect(() => {
    if (isAuthenticated) return;
    if (!currentProject?.id || !currentProject?.resultsPath) return;
    fetchResults(currentProject.resultsPath)
      .then((data) => {
        updateProject(currentProject.id, {
          results: data.items || [],
          markedIds: data.marked_ids || [],
        });
      })
      .catch(() => {});
  }, [isAuthenticated, currentProject?.id, currentProject?.resultsPath, updateProject]);

  const estimatedTotalRunSec = useMemo(() => {
    const projectSpecific = Number(currentProject?.lastRunDurationSec || 0);
    if (Number.isFinite(projectSpecific) && projectSpecific > 0) {
      return Math.max(30, Math.min(600, Math.round(projectSpecific)));
    }

    const finished = (projects || [])
      .map((p) => Number(p.lastRunDurationSec || 0))
      .filter((n) => Number.isFinite(n) && n > 0)
      .sort((a, b) => a - b);

    if (!finished.length) return 90;

    const mid = Math.floor(finished.length / 2);
    const median =
      finished.length % 2 === 0
        ? (finished[mid - 1] + finished[mid]) / 2
        : finished[mid];

    return Math.max(30, Math.min(600, Math.round(median)));
  }, [projects, currentProject?.lastRunDurationSec]);

  const handlePause = useCallback(async () => {
    if (!currentProject) return;
    try {
      await pauseRunningProject(currentProject.id);
    } catch (err) {
      showToast(err.message || "Pause selhal.", "error");
    }
  }, [currentProject, pauseRunningProject]);

  const handleResume = useCallback(async () => {
    if (!currentProject) return;
    try {
      await resumeRunningProject(currentProject.id);
    } catch (err) {
      showToast(err.message || "Resume selhal.", "error");
    }
  }, [currentProject, resumeRunningProject]);

  const handleStop = useCallback(async () => {
    if (!currentProject) return;
    setShowStopConfirmModal(false);
    try {
      await stopRunningProject(currentProject.id);
      showToast("Scraper se ukončuje...", "info");
    } catch (err) {
      showToast(err.message || "Stop selhal.", "error");
    }
  }, [currentProject, stopRunningProject]);

  const requestStopConfirmation = useCallback(() => {
    if (!currentProject) return;
    setShowStopConfirmModal(true);
  }, [currentProject]);

  const handleRunProject = useCallback((projectId) => {
    if (!isAuthenticated) {
      setAuthMode("login");
      setAuthError("Pro spuštění scraperu se nejdřív přihlas.");
      setShowAuthModal(true);
      return;
    }
    runProject(projectId);
  }, [isAuthenticated, runProject]);

  useEffect(() => {
    selectedBrands.forEach((brand) => {
      const b = String(brand || "").trim();
      const hasLoadedModels = Object.prototype.hasOwnProperty.call(modelsByBrand, b);
      if (!b || hasLoadedModels || loadingModelsByBrand[b]) return;

      setLoadingModelsByBrand((prev) => ({ ...prev, [b]: true }));
      fetchModels(b)
        .then((items) => {
          setModelsByBrand((prev) => ({ ...prev, [b]: items }));
          setModelLoadErrorsByBrand((prev) => {
            if (!prev[b]) return prev;
            const next = { ...prev };
            delete next[b];
            return next;
          });
        })
        .catch((err) => {
          setModelLoadErrorsByBrand((prev) => ({
            ...prev,
            [b]: err?.message || "Nepodařilo se načíst modely.",
          }));
        })
        .finally(() => {
          setLoadingModelsByBrand((prev) => ({ ...prev, [b]: false }));
        });
    });
  }, [selectedBrands, modelsByBrand]);

  useEffect(() => {
    selectedBrands.forEach((brand) => {
      const b = String(brand || "").trim();
      const models = modelsByBrand[b];
      if (!b || !Array.isArray(models) || models.length === 0 || loadingModelCountsByBrand[b]) return;

      if (modelCountsKeyByBrand[b] === modelCountsRequestKey) return;

      setLoadingModelCountsByBrand((prev) => ({ ...prev, [b]: true }));
      fetchModelCounts(b, currentProject?.config || {})
        .then((items) => {
          const countMap = new Map((items || []).map((x) => [String(x.value || ""), Number(x.count || 0)]));
          const labelMap = new Map((items || []).map((x) => [String(x.value || ""), String(x.label || x.value || "")]));
          setModelsByBrand((prev) => {
            const current = prev[b] || [];
            const mergedCurrent = current
              .map((m) => {
              const key = String(m?.value || "");
              if (!countMap.has(key)) return m;
              return { ...m, count: countMap.get(key) };
              });

            const currentKeys = new Set(mergedCurrent.map((m) => String(m?.value || "")));
            const addedFromCounts = [];
            for (const [value, count] of countMap.entries()) {
              if (currentKeys.has(value)) continue;
              addedFromCounts.push({ value, label: labelMap.get(value) || value, count });
            }

            const merged = [...mergedCurrent, ...addedFromCounts]
              .filter((m) => Number.isFinite(Number(m?.count)) ? Number(m.count) > 0 : false)
              .sort((a, b) => String(a?.label || a?.value || "").localeCompare(String(b?.label || b?.value || "")));

            return { ...prev, [b]: merged };
          });
          setModelCountsKeyByBrand((prev) => ({ ...prev, [b]: modelCountsRequestKey }));
        })
        .catch(() => {})
        .finally(() => {
          setLoadingModelCountsByBrand((prev) => ({ ...prev, [b]: false }));
        });
    });
  }, [selectedBrands, modelsByBrand, loadingModelCountsByBrand, modelCountsKeyByBrand, modelCountsRequestKey, currentProject?.config]);

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

  const handleAddProject = useCallback(() => {
    const proj = addProject();
    if (!proj?.id) return;
    setUiActiveProjectId(proj.id);
    setDisplayProjectId(proj.id);
    activateProject(proj.id);
  }, [addProject, activateProject]);

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

  function navigateTo(page) {
    const target = page === "pricing" ? "pricing" : "dashboard";
    const path = target === "pricing" ? "/pricing" : "/";
    if (window.location.pathname !== path) {
      window.history.pushState({}, "", path);
    }
    setCurrentPage(target);
  }

  function fmtCzk(value) {
    const num = Number(value || 0);
    return `${num.toFixed(2)} Kč`;
  }

  function renderPricingPage() {
    const runBase = Number(billingRates?.run_base_czk ?? 5.0);
    const item = Number(billingRates?.item_czk ?? 0.02);
    const apiCall = Number(billingRates?.api_call_czk ?? 0.05);
    const proxyRun = Number(billingRates?.proxy_run_czk ?? 0.0);
    const exampleItems = 120;
    const exampleApiCalls = 250;
    const exampleScraperCost = runBase + (exampleItems * item) + proxyRun;
    const exampleIntegrationCost = exampleApiCalls * apiCall;
    const exampleTotalCost = exampleScraperCost + exampleIntegrationCost;

    return (
      <div className="pricing-page">
        <div className="pricing-head">
          <h2>Pricing</h2>
          <p>Platíš jen za skutečné použití. Žádné měsíční plány, žádné minimální commit ceny.</p>
        </div>

        <div className="pricing-grid">
          <article className="pricing-card">
            <h3>Scraper Usage</h3>
            <p className="pricing-line"><strong>{fmtCzk(runBase)}</strong> za každý dokončený run</p>
            <p className="pricing-line"><strong>{fmtCzk(item)}</strong> za každý výsledný inzerát v outputu</p>
            <p className="pricing-line"><strong>{fmtCzk(proxyRun)}</strong> proxy příplatek za run při zapnutých proxy</p>
          </article>

          <article className="pricing-card">
            <h3>API Integrace</h3>
            <p className="pricing-line"><strong>{fmtCzk(apiCall)}</strong> za každý API call s hlavičkou <code>x-api-key</code></p>
            <p className="pricing-note">Bez <code>x-api-key</code> se call nepočítá jako integrační usage.</p>
          </article>

          <article className="pricing-card pricing-card-wide">
            <h3>Jak se počítá cena</h3>
            <p className="pricing-formula">
              Cena runu = <code>run_base</code> + (<code>počet výsledků * item_rate</code>) + <code>proxy_run</code>
            </p>
            <p className="pricing-formula">
              Cena integrace = <code>počet API callů * api_call_rate</code>
            </p>
            <div className="pricing-example">
              <h4>Příklad</h4>
              <p className="pricing-formula">
                1 run s <strong>{exampleItems}</strong> výsledky: <code>{fmtCzk(runBase)}</code> + ({exampleItems} * <code>{fmtCzk(item)}</code>) + <code>{fmtCzk(proxyRun)}</code> = <strong>{fmtCzk(exampleScraperCost)}</strong>
              </p>
              <p className="pricing-formula">
                Integrace <strong>{exampleApiCalls}</strong> API callů: {exampleApiCalls} * <code>{fmtCzk(apiCall)}</code> = <strong>{fmtCzk(exampleIntegrationCost)}</strong>
              </p>
              <p className="pricing-formula pricing-total">
                Celkem v příkladu: <strong>{fmtCzk(exampleTotalCost)}</strong>
              </p>
            </div>
            <p className="pricing-note">
              Aktuální sazby načítáme z backendu přes <code>/api/billing/rates</code>.
            </p>
            {billingRatesError ? <p className="pricing-note">{billingRatesError}</p> : null}
            <div className="pricing-actions">
              <button className="btn-primary" onClick={() => navigateTo("dashboard")}>
                <ArrowLeft className="ui-icon" aria-hidden="true" /> Zpět na dashboard
              </button>
            </div>
          </article>

          <article className="pricing-card pricing-card-wide">
            <h3>Proč se platí</h3>
            <p className="pricing-note">
              Účtujeme jen reálnou spotřebu, protože každé spuštění a API integrace mají přímé provozní náklady.
            </p>
            <ul className="pricing-bullets">
              <li>Proxy infrastruktura a její rotace proti blokacím.</li>
              <li>Výpočetní výkon backendu během scrapingu a zpracování dat.</li>
              <li>Síťový provoz, monitoring, logování a provoz API endpointů.</li>
              <li>Průběžná údržba scraperu při změnách cílového webu.</li>
            </ul>
            <p className="pricing-note">
              Proto nemáme fixní paušál: kdo používá méně, platí méně; kdo používá více, platí férově podle usage.
            </p>
          </article>
        </div>
      </div>
    );
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
            modelLoadErrorsByBrand={modelLoadErrorsByBrand}
            onUpdateConfig={updateActiveProjectConfig}
            onUpdateProject={updateActiveProject}
            onRun={handleRunProject}
            isRunning={scraperRunning}
          />
        );
      case "running":
        return (
          <ProjectRunning
            project={currentProject}
            scraperPaused={scraperPaused}
            scraperStartedAt={scraperStartedAt}
            billingRates={runtimeBillingRates}
            estimatedTotalSec={estimatedTotalRunSec}
            onPause={handlePause}
            onResume={handleResume}
            onStop={requestStopConfirmation}
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
            <button
              type="button"
              className="brand-home-btn"
              onClick={() => navigateTo("dashboard")}
              title="Zpět na domovskou obrazovku"
            >
              <h1>Sauto Scraper</h1>
            </button>
          </div>
          <div className="topbar-spacer" />
          <button
            type="button"
            className={`topbar-link-btn ${currentPage === "pricing" ? "active" : ""}`}
            onClick={() => navigateTo("pricing")}
            title="Otevřít pricing"
          >
            <BadgeEuro className="ui-icon" aria-hidden="true" /> Pricing
          </button>
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
          {authBooting ? <span className="auth-user-chip">Auth...</span> : null}
          {!authBooting && isAuthenticated ? <span className="auth-user-chip">{authUser?.email || "user"}</span> : null}
          {!authBooting && isAuthenticated ? (
            <button type="button" className="theme-toggle" onClick={handleLogout} title="Odhlásit">
              Logout
            </button>
          ) : null}
          {!authBooting && !isAuthenticated ? (
            <button
              type="button"
              className="theme-toggle"
              onClick={() => {
                setAuthMode("login");
                setAuthError("");
                setShowAuthModal(true);
              }}
              title="Přihlášení"
            >
              Login
            </button>
          ) : null}
        </div>

        {/* Tab bar */}
        {currentPage === "dashboard" ? (
          <TabBar
            projects={projects}
            activeProjectId={uiActiveProjectId}
            onActivate={activateProjectSmooth}
            onRemove={removeProject}
            onAdd={handleAddProject}
            scraperRunning={scraperRunning}
          />
        ) : null}

        {/* Main content */}
        <div className="main-content">
          {currentPage === "dashboard" && !isAuthenticated ? (
            <div className="auth-dashboard-cta">
              <h2>Přihlášení je potřeba</h2>
              <p>Nastavení projektu můžeš dělat i bez loginu, ale pro spuštění scraperu se musíš přihlásit.</p>
              <button
                type="button"
                className="btn-primary"
                onClick={() => {
                  setAuthMode("login");
                  setAuthError("");
                  setShowAuthModal(true);
                }}
              >
                Otevřít login
              </button>
            </div>
          ) : null}
          {currentPage === "pricing" ? renderPricingPage() : renderProjectContent()}
        </div>

        {/* Terminal bar */}
        {currentPage === "dashboard" ? (
          <TerminalBar
            projectRunning={currentProject?.phase === "running"}
            projectPaused={currentProject?.phase === "running" && scraperPaused}
            projectLogs={currentProjectLogs}
            tickerPrefix={tickerPrefix()}
            onShowHistory={() => {
              setShowLogsModal(true);
              setTimeout(() => {
                if (logsModalBodyRef.current)
                  logsModalBodyRef.current.scrollTop = logsModalBodyRef.current.scrollHeight;
              }, 50);
            }}
          />
        ) : null}
      </div>

      {showAuthModal && (
        <div className="auth-modal-overlay" onClick={() => setShowAuthModal(false)}>
          <form className="auth-card auth-modal-card" onSubmit={handleAuthSubmit} onClick={(e) => e.stopPropagation()}>
            <div className="auth-modal-head">
              <h2>Sauto Scraper</h2>
              <button type="button" className="debug-modal-close" onClick={() => setShowAuthModal(false)}>
                <X className="ui-icon" aria-hidden="true" />
              </button>
            </div>
            <p>{authMode === "signup" ? "Vytvoř účet" : "Přihlas se"}</p>

            <label className="auth-label">
              Email
              <input
                type="email"
                value={authEmail}
                onChange={(e) => setAuthEmail(e.target.value)}
                required
                autoComplete="email"
              />
            </label>

            <label className="auth-label">
              Heslo
              <input
                type="password"
                value={authPassword}
                onChange={(e) => setAuthPassword(e.target.value)}
                required
                minLength={6}
                autoComplete={authMode === "signup" ? "new-password" : "current-password"}
              />
            </label>

            {authError ? <div className="auth-error">{authError}</div> : null}

            <button className="btn-primary auth-submit" type="submit" disabled={authBusy || authBooting}>
              {authBusy ? "Prosím čekej..." : authMode === "signup" ? "Sign up" : "Login"}
            </button>

            <button
              type="button"
              className="auth-switch"
              onClick={() => {
                setAuthMode((prev) => (prev === "signup" ? "login" : "signup"));
                setAuthError("");
              }}
            >
              {authMode === "signup" ? "Máš účet? Přihlas se" : "Nemáš účet? Vytvoř ho"}
            </button>
          </form>
        </div>
      )}

      {/* Logs Modal */}
      {showLogsModal && (
        <div className="debug-modal-overlay" onClick={() => setShowLogsModal(false)}>
          <div className="debug-modal" onClick={(e) => e.stopPropagation()}>
            <div className="debug-modal-head">
              <strong>Debug výpis — Historie</strong>
              <span className="muted">{currentProjectLogs.length} řádků</span>
              <button className="debug-modal-close" onClick={() => setShowLogsModal(false)}>
                <X className="ui-icon" aria-hidden="true" />
              </button>
            </div>
            <div className="debug-modal-body" ref={logsModalBodyRef}>
              {currentProjectLogs.length === 0 ? (
                <div className="debug-empty">Zatím žádný log výstup.</div>
              ) : (
                currentProjectLogs.map((line, i) => (
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

      {showStopConfirmModal && currentProject?.phase === "running" && (
        <div className="log-popup-overlay" onClick={() => setShowStopConfirmModal(false)}>
          <div className="log-popup stop-confirm-popup" onClick={(e) => e.stopPropagation()}>
            <div className="log-popup-head">
              <strong>Opravdu ukončit scraping?</strong>
              <button className="debug-modal-close" onClick={() => setShowStopConfirmModal(false)}>
                <X className="ui-icon" aria-hidden="true" />
              </button>
            </div>
            <div className="stop-confirm-body">
              <p>
                Po full stopu se ztratí většina průběžně scrapnutých výsledků tohoto běhu.
              </p>
              <p className="stop-confirm-warning">
                Refund získáte jen za hodnotu té části běhu, která se ještě neprotočila přes proxy.
              </p>
            </div>
            <div className="log-popup-foot">
              <button className="btn-sm secondary" onClick={() => setShowStopConfirmModal(false)}>
                Zpět
              </button>
              <button className="btn-sm danger" onClick={handleStop}>
                Full stop
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