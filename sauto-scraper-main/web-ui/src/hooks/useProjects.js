import { useState, useCallback, useEffect, useRef } from "react";
import {
  createProject,
  loadProjects,
  saveProjects,
  generateAutoName,
  ensureUniqueProjectName,
} from "../utils/storage";
import {
  fetchStatus,
  fetchResults,
  fetchLogs,
  runScraper,
  pauseScraper,
  resumeScraper,
  stopScraper,
  fetchBillingRates,
  saveParams,
  fetchParams,
} from "../utils/api";

export function useProjects(brandOptions, modelsByBrand, options = {}) {
  const enabled = options.enabled !== false;
  const [projects, setProjects] = useState(() => loadProjects());
  const [activeProjectId, setActiveProjectId] = useState(() => {
    const loaded = loadProjects();
    return loaded.length > 0 ? loaded[0].id : null;
  });
  const [globalLogs, setGlobalLogs] = useState([]);
  const [scraperRunning, setScraperRunning] = useState(false);
  const [scraperPaused, setScraperPaused] = useState(false);
  const [scraperStartedAt, setScraperStartedAt] = useState(null);
  const [runnerPid, setRunnerPid] = useState(null);
  const [billingRates, setBillingRates] = useState({
    run_base_czk: 5.0,
    item_czk: 0.02,
    api_call_czk: 0.05,
    proxy_run_czk: 0.0,
  });
  const [statusReady, setStatusReady] = useState(false);
  const [migrated, setMigrated] = useState(false);
  const stopRequestedProjectIdRef = useRef(null);

  const projectsRef = useRef(projects);
  projectsRef.current = projects;

  // Refs for intervals
  const statusInterval = useRef(null);
  const logsInterval = useRef(null);

  // ── Migrate: import existing params.json + sauto_interesting.json into first project ──
  useEffect(() => {
    if (!enabled) return;
    if (migrated) return;
    const loaded = loadProjects();
    if (loaded.length > 0) {
      setMigrated(true);
      return;
    }

    // No projects in localStorage – try to import existing API data
    Promise.all([
      fetchParams().catch(() => null),
      fetchResults("data/sauto_interesting.json").catch(() => null),
    ]).then(([params, resultsData]) => {
      const results = resultsData?.items || [];
      const markedIds = resultsData?.marked_ids || [];
      const existingData = params || results.length > 0;

      if (existingData) {
        const proj = createProject("Původní data (migrováno)", params || {});
        proj.phase = results.length > 0 ? "done" : "config";
        proj.results = results;
        proj.markedIds = markedIds;
        proj.resultsPath = "data/sauto_interesting.json";
        proj.logs = ["[systém] Původní data načtena z params.json a sauto_interesting.json."];
        setProjects([proj]);
        setActiveProjectId(proj.id);
      } else {
        const fresh = createProject();
        setProjects([fresh]);
        setActiveProjectId(fresh.id);
      }
      setMigrated(true);
    }).catch(() => {
      const fresh = createProject();
      setProjects([fresh]);
      setActiveProjectId(fresh.id);
      setMigrated(true);
    });
  }, [enabled, migrated]);

  // ── Persist (skip until migration done) ──
  useEffect(() => {
    if (!enabled) return;
    if (!migrated) return;
    saveProjects(projects);
  }, [enabled, projects, migrated]);

  // ── Reload results for done projects from API on page refresh ──
  useEffect(() => {
    if (!enabled) return;
    if (!migrated) return;
    projects.forEach((p) => {
      if (p.phase === "done" && p.resultsPath) {
        fetchResults(p.resultsPath)
          .then((data) => {
            setProjects((current) =>
              current.map((cp) =>
                cp.id === p.id
                  ? { ...cp, results: data.items || [], markedIds: data.marked_ids || [] }
                  : cp
              )
            );
          })
          .catch(() => {});
      }
    });
  }, [enabled, migrated]);

  // ── Fetch global scraper status ──
  useEffect(() => {
    if (!enabled) {
      setStatusReady(true);
      setScraperRunning(false);
      setScraperPaused(false);
      setScraperStartedAt(null);
      setRunnerPid(null);
      return;
    }
    const poll = async () => {
      try {
        const status = await fetchStatus();
        setScraperRunning(status.running || false);
        setScraperPaused(status.paused || false);
        setScraperStartedAt(typeof status.last_started_at === "number" ? status.last_started_at : null);
        setRunnerPid(status.pid || null);
      } catch {
        setScraperRunning(false);
        setScraperPaused(false);
        setScraperStartedAt(null);
        setRunnerPid(null);
      } finally {
        setStatusReady(true);
      }
    };
    poll();
    statusInterval.current = setInterval(poll, 2000);
    return () => clearInterval(statusInterval.current);
  }, [enabled]);

  useEffect(() => {
    if (!enabled) return;
    const loadRates = async () => {
      try {
        const rates = await fetchBillingRates();
        if (rates && typeof rates === "object") {
          setBillingRates((prev) => ({ ...prev, ...rates }));
        }
      } catch {
        // keep defaults
      }
    };
    loadRates();
    const t = setInterval(loadRates, 30000);
    return () => clearInterval(t);
  }, [enabled]);

  // ── Fetch global logs ──
  useEffect(() => {
    if (!enabled) {
      setGlobalLogs([]);
      return;
    }
    const poll = async () => {
      try {
        const lines = await fetchLogs(160);
        setGlobalLogs(lines);
        setProjects((prev) => {
          const runningIdx = prev.findIndex((p) => p.phase === "running");
          if (runningIdx === -1) return prev;
          return prev.map((p, idx) =>
            idx === runningIdx
              ? { ...p, liveLogs: Array.isArray(lines) ? lines : [] }
              : p
          );
        });
      } catch {
        // ignore
      }
    };
    poll();
    logsInterval.current = setInterval(poll, 1500);
    return () => clearInterval(logsInterval.current);
  }, [enabled]);

  // ── Update running project results ──
  useEffect(() => {
    if (!enabled) return;
    if (!scraperRunning) return;
    const poll = async () => {
      setProjects((prev) => {
        const runningProjects = prev.filter((p) => p.phase === "running");
        runningProjects.forEach((p) => {
          fetchResults(p.resultsPath)
            .then((data) => {
              setProjects((current) =>
                current.map((cp) =>
                  cp.id === p.id
                    ? {
                        ...cp,
                        results: data.items || [],
                        markedIds: data.marked_ids || [],
                      }
                    : cp
                )
              );
            })
            .catch(() => {});
        });
        return prev;
      });
    };
    poll();
    const t = setInterval(poll, 3000);
    return () => clearInterval(t);
  }, [enabled, scraperRunning]);

  // ── Auto-transition running → done when scraper stops ──
  useEffect(() => {
    if (!enabled) return;
    if (!statusReady) return;
    if (scraperRunning) return;
    // Scraper stopped – check if any project was running
    setProjects((prev) => {
      let changed = false;
      prev.forEach((p) => {
        if (p.phase === "running") {
          changed = true;
          fetchResults(p.resultsPath)
            .then((data) => {
              setProjects((current) =>
                current.map((cp) =>
                  cp.id === p.id
                    ? {
                        ...cp,
                        phase: "done",
                        results: data.items || [],
                        markedIds: data.marked_ids || [],
                        lastRunDurationSec:
                          typeof scraperStartedAt === "number"
                            ? Math.max(1, Math.round(Date.now() / 1000 - scraperStartedAt))
                            : cp.lastRunDurationSec,
                        logs: [
                          ...(cp.logs || []),
                          ...((cp.liveLogs || []).slice(-160)),
                          stopRequestedProjectIdRef.current === cp.id
                            ? "[systém] Scraping ukončen uživatelem."
                            : "[systém] Scraping dokončen.",
                        ],
                        liveLogs: [],
                      }
                    : cp
                )
              );
              if (stopRequestedProjectIdRef.current === p.id) {
                stopRequestedProjectIdRef.current = null;
              }
              // After transition, check queue
              setTimeout(() => {
                setProjects((curr) => {
                  const queuedIdx = curr.findIndex((pr) => pr.phase === "queued");
                  if (queuedIdx === -1) return curr;
                  const queuedProject = curr[queuedIdx];
                  const isAlreadyRunning = curr.some((pr) => pr.phase === "running");
                  if (isAlreadyRunning) {
                    let pos = 1;
                    return curr.map((pr) => {
                      if (pr.phase === "queued") return { ...pr, queuePosition: pos++ };
                      return pr;
                    });
                  }
                  // Start queued project
                  startQueuedScrape(queuedProject.id);
                  return curr;
                });
              }, 500);
            })
            .catch(() => {
              setProjects((current) =>
                current.map((cp) =>
                  cp.id === p.id
                    ? { ...cp, phase: "error", errorMessage: "Nepodařilo se načíst výsledky." }
                    : cp
                )
              );
            });
        }
      });
      // Recalculate queue positions
      let pos = 1;
      return prev.map((p) => {
        if (p.phase === "queued") return { ...p, queuePosition: pos++ };
        return p;
      });
    });
  }, [enabled, scraperRunning, statusReady, scraperStartedAt]);

  // ── Start queued scrape helper (no deps on closures) ──
  function startQueuedScrape(projectId) {
    const project = projectsRef.current.find((p) => p.id === projectId);
    if (!project) return;

    setProjects((prev) =>
      prev.map((p) =>
        p.id === projectId
          ? {
              ...p,
              phase: "running",
              logs: [...(p.logs || []), "[systém] Spouštím scraper z fronty..."],
              liveLogs: [],
            }
          : p
      )
    );

    saveParams(project.config)
      .then(() => runScraper(project.resultsPath, project.id, project.runMode || "cloud_paid"))
      .then(() => {
        setScraperRunning(true);
        setScraperPaused(false);
        setProjects((prev) =>
          prev.map((p) =>
            p.id === projectId
              ? {
                  ...p,
                  logs: [
                    ...(p.logs || []),
                    project.runMode === "local_free"
                      ? "[systém] Scraper spuštěn v local free režimu (běh z lokální IP)."
                      : "[systém] Scraper úspěšně spuštěn z fronty.",
                  ],
                }
              : p
          )
        );
      })
      .catch((err) => {
        if (err.status === 409) {
          setProjects((prev) =>
            prev.map((p) =>
              p.id === projectId
                ? {
                    ...p,
                    logs: [...(p.logs || []), "[systém] Nelze spustit – scraper stále běží."],
                  }
                : p
            )
          );
        } else {
          setProjects((prev) =>
            prev.map((p) =>
              p.id === projectId
                ? {
                    ...p,
                    phase: "error",
                    errorMessage: err.message,
                    logs: [...(p.logs || []), `[chyba] ${err.message}`],
                  }
                : p
            )
          );
        }
      });
  }

  // ── Start scrape for a specific project ──
  const startScrapeForProject = useCallback(async (projectId) => {
    setProjects((prev) =>
      prev.map((p) =>
        p.id === projectId
          ? {
              ...p,
              phase: "running",
              logs: [...(p.logs || []), "[systém] Spouštím scraper..."],
              liveLogs: [],
            }
          : p
      )
    );

    try {
      const project = projectsRef.current.find((p) => p.id === projectId);
      if (!project) throw new Error("Projekt nenalezen.");

      await saveParams(project.config);
      await runScraper(project.resultsPath, project.id, project.runMode || "cloud_paid");
      setScraperRunning(true);
      setScraperPaused(false);

      setProjects((prev) =>
        prev.map((p) =>
          p.id === projectId
            ? {
                ...p,
                logs: [
                  ...(p.logs || []),
                  project.runMode === "local_free"
                    ? "[systém] Scraper spuštěn v local free režimu (běh z lokální IP)."
                    : "[systém] Scraper úspěšně spuštěn.",
                ],
              }
            : p
        )
      );
    } catch (err) {
      if (err.status === 409) {
        // Scraper already running - add to queue
        setProjects((prev) => {
          const queuePos = prev.filter((p) => p.phase === "queued").length + 1;
          return prev.map((p) =>
            p.id === projectId
              ? {
                  ...p,
                  phase: "queued",
                  queuePosition: queuePos,
                  logs: [...(p.logs || []), `[systém] Zařazeno do fronty (pozice ${queuePos}).`],
                }
              : p
          );
        });
      } else {
        setProjects((prev) =>
          prev.map((p) =>
            p.id === projectId
              ? {
                  ...p,
                  phase: "error",
                  errorMessage: err.message,
                  logs: [...(p.logs || []), `[chyba] ${err.message}`],
                }
              : p
          )
        );
      }
    }
  }, []);

  // ── Actions ──
  const addProject = useCallback(
    (name = "", config = {}) => {
      const proj = createProject(name, config);
      const autoName = generateAutoName(config, brandOptions, modelsByBrand);
      const requestedName = name || autoName;
      proj.name = ensureUniqueProjectName(requestedName, projectsRef.current);
      proj.customName = !!name;
      setProjects((prev) => [...prev, proj]);
      setActiveProjectId(proj.id);
      return proj;
    },
    [brandOptions, modelsByBrand]
  );

  const removeProject = useCallback((id) => {
    setProjects((prev) => {
      const filtered = prev.filter((p) => p.id !== id);
      if (filtered.length === 0) {
        const fresh = createProject();
        setActiveProjectId(fresh.id);
        return [fresh];
      }
      setActiveProjectId((current) => {
        if (current === id) return filtered[0].id;
        return current;
      });
      return filtered;
    });
  }, []);

  const activateProject = useCallback((id) => {
    setActiveProjectId(id);
  }, []);

  const updateProject = useCallback((id, updates) => {
    setProjects((prev) =>
      prev.map((p) => (p.id === id ? { ...p, ...updates } : p))
    );
  }, []);

  const updateProjectConfig = useCallback(
    (id, configUpdates) => {
      setProjects((prev) =>
        prev.map((p) => {
          if (p.id !== id) return p;
          const newConfig = { ...p.config, ...configUpdates };
          const updated = { ...p, config: newConfig };
          // Auto-name if not custom
          if (!p.customName) {
            const autoName = generateAutoName(newConfig, brandOptions, modelsByBrand);
            updated.name = ensureUniqueProjectName(autoName, prev, p.id);
          }
          return updated;
        })
      );
    },
    [brandOptions, modelsByBrand]
  );

  const runProject = useCallback(
    (id) => {
      const project = projectsRef.current.find((p) => p.id === id);
      if (!project || project.phase === "running" || project.phase === "queued") return;
      startScrapeForProject(id);
    },
    [startScrapeForProject]
  );

  const pauseRunningProject = useCallback(async (projectId) => {
    try {
      await pauseScraper();
      setScraperPaused(true);
      setProjects((prev) =>
        prev.map((p) =>
          p.id === projectId
            ? { ...p, logs: [...(p.logs || []), "[systém] Pozastaveno uživatelem."] }
            : p
        )
      );
    } catch (err) {
      setProjects((prev) =>
        prev.map((p) =>
          p.id === projectId
            ? { ...p, logs: [...(p.logs || []), `[chyba] Pause selhal: ${err.message}`] }
            : p
        )
      );
      throw err;
    }
  }, []);

  const resumeRunningProject = useCallback(async (projectId) => {
    try {
      await resumeScraper();
      setScraperPaused(false);
      setProjects((prev) =>
        prev.map((p) =>
          p.id === projectId
            ? { ...p, logs: [...(p.logs || []), "[systém] Pokračuji po pauze."] }
            : p
        )
      );
    } catch (err) {
      setProjects((prev) =>
        prev.map((p) =>
          p.id === projectId
            ? { ...p, logs: [...(p.logs || []), `[chyba] Resume selhal: ${err.message}`] }
            : p
        )
      );
      throw err;
    }
  }, []);

  const stopRunningProject = useCallback(async (projectId) => {
    stopRequestedProjectIdRef.current = projectId;
    setProjects((prev) =>
      prev.map((p) =>
        p.id === projectId
          ? { ...p, logs: [...(p.logs || []), "[systém] Posílám požadavek na předčasné ukončení..."] }
          : p
      )
    );

    try {
      await stopScraper();
      setScraperPaused(false);
      setScraperRunning(false);

      const project = projectsRef.current.find((p) => p.id === projectId);
      let latestItems = project?.results || [];
      let latestMarked = project?.markedIds || [];
      if (project?.resultsPath) {
        try {
          const data = await fetchResults(project.resultsPath);
          latestItems = Array.isArray(data?.items) ? data.items : latestItems;
          latestMarked = Array.isArray(data?.marked_ids) ? data.marked_ids : latestMarked;
        } catch {
          // keep current in-memory results if fetch fails
        }
      }

      setProjects((prev) =>
        prev.map((p) =>
          p.id === projectId
            ? {
                ...p,
                phase: "done",
                results: latestItems,
                markedIds: latestMarked,
                liveLogs: [],
                logs: [...(p.logs || []), "[systém] Běh byl předčasně ukončen uživatelem."],
              }
            : p
        )
      );
    } catch (err) {
      stopRequestedProjectIdRef.current = null;
      setProjects((prev) =>
        prev.map((p) =>
          p.id === projectId
            ? { ...p, logs: [...(p.logs || []), `[chyba] Stop selhal: ${err.message}`] }
            : p
        )
      );
      throw err;
    }
  }, []);

  const activeProject = projects.find((p) => p.id === activeProjectId) || projects[0] || createProject();

  return {
    projects,
    activeProjectId,
    activeProject,
    globalLogs,
    scraperRunning,
    scraperPaused,
    scraperStartedAt,
    runnerPid,
    billingRates,
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
  };
}