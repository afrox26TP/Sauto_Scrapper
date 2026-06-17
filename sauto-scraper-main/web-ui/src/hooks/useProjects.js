import { useState, useCallback, useEffect, useRef } from "react";
import {
  createProject,
  loadProjects,
  saveProjects,
  generateAutoName,
} from "../utils/storage";
import {
  fetchStatus,
  fetchResults,
  fetchLogs,
  runScraper,
  saveParams,
  fetchParams,
} from "../utils/api";

export function useProjects(brandOptions, modelsByBrand) {
  const [projects, setProjects] = useState(() => loadProjects());
  const [activeProjectId, setActiveProjectId] = useState(() => {
    const loaded = loadProjects();
    return loaded.length > 0 ? loaded[0].id : null;
  });
  const [globalLogs, setGlobalLogs] = useState([]);
  const [scraperRunning, setScraperRunning] = useState(false);
  const [migrated, setMigrated] = useState(false);

  const projectsRef = useRef(projects);
  projectsRef.current = projects;

  // Refs for intervals
  const statusInterval = useRef(null);
  const logsInterval = useRef(null);

  // ── Migrate: import existing params.json + sauto_interesting.json into first project ──
  useEffect(() => {
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
  }, [migrated]);

  // ── Persist (skip until migration done) ──
  useEffect(() => {
    if (!migrated) return;
    saveProjects(projects);
  }, [projects, migrated]);

  // ── Fetch global scraper status ──
  useEffect(() => {
    const poll = async () => {
      try {
        const status = await fetchStatus();
        setScraperRunning(status.running || false);
      } catch {
        setScraperRunning(false);
      }
    };
    poll();
    statusInterval.current = setInterval(poll, 2000);
    return () => clearInterval(statusInterval.current);
  }, []);

  // ── Fetch global logs ──
  useEffect(() => {
    const poll = async () => {
      try {
        const lines = await fetchLogs(160);
        setGlobalLogs(lines);
      } catch {
        // ignore
      }
    };
    poll();
    logsInterval.current = setInterval(poll, 1500);
    return () => clearInterval(logsInterval.current);
  }, []);

  // ── Update running project results ──
  useEffect(() => {
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
  }, [scraperRunning]);

  // ── Auto-transition running → done when scraper stops ──
  useEffect(() => {
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
                        logs: [...(cp.logs || []), "[systém] Scraping dokončen."],
                      }
                    : cp
                )
              );
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
  }, [scraperRunning]);

  // ── Start queued scrape helper (no deps on closures) ──
  function startQueuedScrape(projectId) {
    const project = projectsRef.current.find((p) => p.id === projectId);
    if (!project) return;

    setProjects((prev) =>
      prev.map((p) =>
        p.id === projectId
          ? { ...p, phase: "running", logs: [...(p.logs || []), "[systém] Spouštím scraper z fronty..."] }
          : p
      )
    );

    saveParams(project.config)
      .then(() => runScraper(project.resultsPath))
      .then(() => {
        setProjects((prev) =>
          prev.map((p) =>
            p.id === projectId
              ? { ...p, logs: [...(p.logs || []), "[systém] Scraper úspěšně spuštěn z fronty."] }
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
          ? { ...p, phase: "running", logs: [...(p.logs || []), "[systém] Spouštím scraper..."] }
          : p
      )
    );

    try {
      const project = projectsRef.current.find((p) => p.id === projectId);
      if (!project) throw new Error("Projekt nenalezen.");

      await saveParams(project.config);
      await runScraper(project.resultsPath);

      setProjects((prev) =>
        prev.map((p) =>
          p.id === projectId
            ? { ...p, logs: [...(p.logs || []), "[systém] Scraper úspěšně spuštěn."] }
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
      proj.name = name || autoName;
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
            updated.name = generateAutoName(newConfig, brandOptions, modelsByBrand);
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

  const activeProject = projects.find((p) => p.id === activeProjectId) || projects[0] || createProject();

  return {
    projects,
    activeProjectId,
    activeProject,
    globalLogs,
    scraperRunning,
    addProject,
    removeProject,
    activateProject,
    updateProject,
    updateProjectConfig,
    runProject,
    setProjects,
  };
}