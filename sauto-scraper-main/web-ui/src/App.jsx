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
import { clearAuthToken, createCheckoutSession, fetchBillingAccess, fetchBillingRates, fetchBrands, fetchBodies, fetchCurrentUser, fetchModels, fetchModelCounts, fetchResults, fetchEquipment, fetchProxyConfig, saveProxyConfig, testProxyConnection, getAuthToken, login, signup } from "./utils/api";
import { csvToArray, uniq } from "./utils/scoring";

export default function App() {
  const resolvePageFromPathname = useCallback((pathname) => {
    const path = String(pathname || "").toLowerCase();
    if (path.startsWith("/pricing")) return "pricing";
    if (path.startsWith("/proxy")) return "proxy";
    return "dashboard";
  }, []);

  const [authToken, setAuthToken] = useState(() => getAuthToken());
  const [authUser, setAuthUser] = useState(null);
  const [authBooting, setAuthBooting] = useState(true);
  const [authMode, setAuthMode] = useState("login");
  const [authEmail, setAuthEmail] = useState("");
  const [authPassword, setAuthPassword] = useState("");
  const [authError, setAuthError] = useState("");
  const [authBusy, setAuthBusy] = useState(false);
  const [showAuthModal, setShowAuthModal] = useState(false);
  const [currentPage, setCurrentPage] = useState(() => resolvePageFromPathname(window.location.pathname));
  const [theme, setTheme] = useState(() => {
    const stored = window.localStorage.getItem("sauto_theme");
    return stored === "dark" ? "dark" : "light";
  });
  const [billingRates, setBillingRates] = useState(null);
  const [billingRatesError, setBillingRatesError] = useState("");
  const [billingAccess, setBillingAccess] = useState(null);
  const DEFAULT_FREE_PROXY_PROFILE_ID = "free_proxy_default";
  const DEFAULT_PAID_PROXY_PROFILE_ID = "paid_proxy_default";
  const [proxyConfig, setProxyConfig] = useState({
    has_free_proxy_config: false,
    has_paid_proxy_config: false,
    free_proxy_url: "",
    paid_proxy_url: "",
    profiles: [],
  });
  const [proxyEditors, setProxyEditors] = useState({
    // [profileId]: { name, kind, proxy_url, dirty_url }
  });
  const [proxyLoading, setProxyLoading] = useState(false);
  const [proxySaving, setProxySaving] = useState(false);
  const [showProxyHelpModal, setShowProxyHelpModal] = useState(false);
  const [selectedProviderId, setSelectedProviderId] = useState("webshare");
  const [proxySmartInput, setProxySmartInput] = useState({});
  const [proxyTestStatus, setProxyTestStatus] = useState({ state: "idle", message: "" });
  const [helpProviderId, setHelpProviderId] = useState("webshare");
  const [checkoutBusy, setCheckoutBusy] = useState(false);
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
  const providerCards = useMemo(() => ([
    {
      id: "webshare",
      name: "Webshare",
      logo: "WS",
      recommended: true,
      ctaLabel: "Ziskat proxy u Webshare",
      url: "https://www.webshare.io/?referral_code=jixav3l993nd",
      whereToFind: "Dashboard > Proxy list > Download/Copy endpoint",
    },
    {
      id: "custom",
      name: "Vlastni reseni",
      logo: "MY",
      recommended: false,
      ctaLabel: "Pouzivam vlastni providera",
      url: "#proxy-profiles",
      whereToFind: "V administraci tveho providera hledej endpoint proxy (host, port, user, pass)",
    },
  ]), []);

  const normalizeProxyProfiles = useCallback((profiles) => {
    if (!Array.isArray(profiles)) return [];
    const isSystemDefaultProfile = (id) => (
      id === DEFAULT_FREE_PROXY_PROFILE_ID ||
      id === DEFAULT_PAID_PROXY_PROFILE_ID ||
      // Backward compatibility for previously used frontend-only ids.
      id === "profile_free_default" ||
      id === "profile_paid_default"
    );

    return profiles
      .map((item) => ({
        id: String(item?.id || "").trim(),
        name: String(item?.name || "").trim(),
        kind: String(item?.kind || "free_proxy").trim() === "paid_proxy" ? "paid_proxy" : "free_proxy",
        has_proxy_url: Boolean(item?.has_proxy_url),
        proxy_url: String(item?.proxy_url || "").trim(),
        proxy_curl: String(item?.proxy_curl || ""),
        proxy_preview: String(item?.proxy_preview || ""),
      }))
      .filter((item) => {
        if (!item.id) return false;
        // Backend auto-creates default A/B profiles. Hide them in UI when empty,
        // so users only see profiles they explicitly created/configured.
        if (isSystemDefaultProfile(item.id) && !item.has_proxy_url) return false;
        return true;
      });
  }, [DEFAULT_FREE_PROXY_PROFILE_ID, DEFAULT_PAID_PROXY_PROFILE_ID]);

  const createDraftProxyProfile = useCallback(() => ({
    id: `profile_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 7)}`,
    name: "",
    kind: "free_proxy",
    has_proxy_url: false,
    proxy_url: "",
    proxy_curl: "",
    proxy_preview: "",
  }), []);

  const applyProxyConfigResponse = useCallback((data) => {
    const incomingProfiles = normalizeProxyProfiles(data?.profiles);
    const profiles = incomingProfiles.length ? incomingProfiles : [createDraftProxyProfile()];
    const byId = Object.fromEntries(incomingProfiles.map((p) => [p.id, p]));
    const freeProfile = byId[DEFAULT_FREE_PROXY_PROFILE_ID] || null;
    const paidProfile = byId[DEFAULT_PAID_PROXY_PROFILE_ID] || null;

    setProxyConfig({
      has_free_proxy_config: Boolean(data?.has_free_proxy_config ?? freeProfile?.has_proxy_url),
      has_paid_proxy_config: Boolean(data?.has_paid_proxy_config ?? paidProfile?.has_proxy_url),
      free_proxy_url: String(data?.free_proxy_url || freeProfile?.proxy_preview || ""),
      paid_proxy_url: String(data?.paid_proxy_url || paidProfile?.proxy_preview || ""),
      profiles,
    });

    setProxyEditors(
      Object.fromEntries(
        profiles.map((profile) => [
          profile.id,
          {
            name: profile.name || profile.id,
            kind: profile.kind,
            proxy_url: String(profile.proxy_url || profile.proxy_preview || ""),
            dirty_url: false,
          },
        ])
      )
    );
    setProxySmartInput((prev) => Object.fromEntries(
      profiles.map((profile) => [
        profile.id,
        String(profile.proxy_curl || prev?.[profile.id] || profile.proxy_url || ""),
      ])
    ));
  }, [DEFAULT_FREE_PROXY_PROFILE_ID, DEFAULT_PAID_PROXY_PROFILE_ID, createDraftProxyProfile, normalizeProxyProfiles]);

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
      setCurrentPage(resolvePageFromPathname(window.location.pathname));
    };
    window.addEventListener("popstate", onPopState);
    return () => window.removeEventListener("popstate", onPopState);
  }, [resolvePageFromPathname]);

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
    if (!isAuthenticated) {
      setBillingAccess(null);
      return;
    }
    fetchBillingAccess().then(setBillingAccess).catch(() => setBillingAccess(null));
  }, [isAuthenticated]);

  useEffect(() => {
    if (!isAuthenticated) {
      setProxyConfig({
        has_free_proxy_config: false,
        has_paid_proxy_config: false,
        free_proxy_url: "",
        paid_proxy_url: "",
        profiles: [],
      });
      setProxyEditors({});
      setProxySmartInput({});
      setProxyTestStatus({ state: "idle", message: "" });
      setProxyLoading(false);
      setProxySaving(false);
      return;
    }

    setProxyLoading(true);
    fetchProxyConfig()
      .then((data) => {
        applyProxyConfigResponse(data);
        setProxyTestStatus({ state: "idle", message: "" });
      })
      .catch((err) => {
        setProxyConfig({
          has_free_proxy_config: false,
          has_paid_proxy_config: false,
          free_proxy_url: "",
          paid_proxy_url: "",
          profiles: [],
        });
        setProxyEditors({});
        setProxySmartInput({});
        setProxyTestStatus({ state: "idle", message: "" });
        showToast(err?.message || "Nacteni konfigurace proxy selhalo.", "error");
      })
      .finally(() => {
        setProxyLoading(false);
      });
  }, [isAuthenticated, applyProxyConfigResponse]);

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
    runProject(projectId);
  }, [runProject]);

  const handleStartCheckout = useCallback(async () => {
    if (checkoutBusy) return;
    if (!isAuthenticated) {
      setAuthMode("login");
      setAuthError("Nejdřív se přihlas, pak otevři checkout.");
      setShowAuthModal(true);
      return;
    }
    setCheckoutBusy(true);
    try {
      const successUrl = `${window.location.origin}/pricing?checkout=success`;
      const cancelUrl = `${window.location.origin}/pricing?checkout=cancel`;
      const data = await createCheckoutSession({ success_url: successUrl, cancel_url: cancelUrl });
      if (data?.url) {
        window.location.href = data.url;
        return;
      }
      showToast("Checkout URL není dostupná.", "error");
    } catch (err) {
      showToast(err?.message || "Checkout se nepodařilo vytvořit.", "error");
    } finally {
      setCheckoutBusy(false);
    }
  }, [checkoutBusy, isAuthenticated]);

  const handleProxyProfileChange = useCallback((profileId, field, value) => {
    const id = String(profileId || "");
    if (!id) return;

    if (field === "name" || field === "kind") {
      setProxyConfig((prev) => ({
        ...prev,
        profiles: (prev.profiles || []).map((profile) =>
          profile.id === id
            ? {
                ...profile,
                [field]: field === "kind" && value === "paid_proxy" ? "paid_proxy" : field === "kind" ? "free_proxy" : String(value || ""),
              }
            : profile
        ),
      }));
    }

    if (field === "proxy_url") {
      setProxyEditors((prev) => ({
        ...prev,
        [id]: {
          ...(prev[id] || {}),
          proxy_url: String(value || ""),
          dirty_url: true,
        },
      }));
      return;
    }

    setProxyEditors((prev) => ({
      ...prev,
      [id]: {
        ...(prev[id] || {}),
        [field]: field === "kind" && value === "paid_proxy" ? "paid_proxy" : field === "kind" ? "free_proxy" : String(value || ""),
      },
    }));
  }, []);

  const parseProxyString = useCallback((rawValue) => {
    const source = String(rawValue || "").trim();
    if (!source) return null;
    const safeDecode = (value) => {
      try {
        return decodeURIComponent(String(value || ""));
      } catch {
        return String(value || "");
      }
    };

    // Accept full shell commands like: curl --proxy "http://user:pass@host:port/" https://...
    let candidate = source;
    const curlProxyMatch = source.match(/(?:--proxy|-x)\s+["']?([^"'\s]+)["']?/i);
    if (curlProxyMatch?.[1]) {
      candidate = String(curlProxyMatch[1] || "").trim();
    }
    candidate = candidate.replace(/\/+$/, "");

    const urlPattern = /^(?:(https?|socks5h):\/\/)?(?:([^:@\s]+):([^@\s]*)@)?([^:\/\s]+):(\d{2,5})$/i;
    const urlMatch = candidate.match(urlPattern);
    if (urlMatch) {
      return {
        scheme: String(urlMatch[1] || "http").toLowerCase(),
        username: safeDecode(urlMatch[2]),
        password: safeDecode(urlMatch[3]),
        host: String(urlMatch[4] || ""),
        port: String(urlMatch[5] || ""),
      };
    }

    const parts = candidate.split(":");
    if (parts.length === 4 && /^\d{2,5}$/.test(parts[1])) {
      return {
        scheme: "http",
        host: parts[0],
        port: parts[1],
        username: parts[2],
        password: parts[3],
      };
    }

    if (parts.length === 2 && /^\d{2,5}$/.test(parts[1])) {
      return {
        scheme: "http",
        host: parts[0],
        port: parts[1],
        username: "",
        password: "",
      };
    }

    return null;
  }, []);

  const buildProxyUrlFromParts = useCallback((parts) => {
    if (!parts?.host || !parts?.port) return "";
    const scheme = ["http", "https", "socks5h"].includes(String(parts.scheme || "").toLowerCase())
      ? String(parts.scheme).toLowerCase()
      : "http";
    const username = String(parts.username || "").trim();
    const password = String(parts.password || "");
    const authPart = username
      ? `${encodeURIComponent(username)}:${encodeURIComponent(password)}@`
      : "";
    return `${scheme}://${authPart}${String(parts.host || "").trim()}:${String(parts.port || "").trim()}`;
  }, []);

  const isMaskedProxyValue = useCallback((value) => {
    const raw = String(value || "").trim();
    if (!raw) return false;
    if (raw === "<configured>") return true;
    return raw.includes("***@");
  }, []);

  const handleSmartPasteApply = useCallback((profileId) => {
    const id = String(profileId || "");
    if (!id) return;
    const parsed = parseProxyString(proxySmartInput[id]);
    if (!parsed) {
      showToast("Smart paste nepoznal format. Zkus URL nebo IP:PORT:USER:PASS.", "error");
      return;
    }
    const composed = buildProxyUrlFromParts(parsed);
    if (!composed) {
      showToast("Smart paste nenasel host/port.", "error");
      return;
    }
    setProxyEditors((prev) => ({
      ...prev,
      [id]: {
        ...(prev[id] || {}),
        proxy_url: composed,
        dirty_url: true,
      },
    }));
    showToast("Udaje byly rozpoznany a vyplneny automaticky.", "info");
  }, [buildProxyUrlFromParts, parseProxyString, proxySmartInput]);

  const handleSmartInputChange = useCallback((profileId, value) => {
    const id = String(profileId || "");
    if (!id) return;
    const nextValue = String(value || "");
    setProxySmartInput((prev) => ({ ...prev, [id]: nextValue }));

    const parsed = parseProxyString(nextValue);
    if (!parsed) return;
    const composed = buildProxyUrlFromParts(parsed);
    if (!composed) return;

    setProxyEditors((prev) => {
      const currentUrl = String(prev[id]?.proxy_url || "").trim();
      if (currentUrl === composed) return prev;
      return {
        ...prev,
        [id]: {
          ...(prev[id] || {}),
          proxy_url: composed,
          dirty_url: true,
        },
      };
    });
  }, [buildProxyUrlFromParts, parseProxyString]);

  const handleAddProxyProfile = useCallback(() => {
    const profile = createDraftProxyProfile();

    setProxyConfig((prev) => ({
      ...prev,
      profiles: [
        ...(prev.profiles || []),
        profile,
      ],
    }));
    setProxyEditors((prev) => ({
      ...prev,
      [profile.id]: {
        name: "",
        kind: "free_proxy",
        proxy_url: "",
        dirty_url: false,
      },
    }));
  }, [createDraftProxyProfile]);

  const handleRemoveProxyProfile = useCallback((profileId) => {
    const id = String(profileId || "");
    if (!id) return;
    if (id === DEFAULT_FREE_PROXY_PROFILE_ID || id === DEFAULT_PAID_PROXY_PROFILE_ID) {
      showToast("Vychozi profily nelze odstranit, lze jen prepsat nebo vymazat URL.", "info");
      return;
    }
    const fallbackProfile = createDraftProxyProfile();
    setProxyConfig((prev) => ({
      ...prev,
      profiles: (() => {
        const remaining = (prev.profiles || []).filter((profile) => profile.id !== id);
        return remaining.length ? remaining : [fallbackProfile];
      })(),
    }));
    setProxyEditors((prev) => {
      const next = { ...prev };
      delete next[id];
      const hasAnyEditor = Object.keys(next).length > 0;
      if (!hasAnyEditor) {
        next[fallbackProfile.id] = {
          name: "",
          kind: "free_proxy",
          proxy_url: "",
          dirty_url: false,
        };
      }
      return next;
    });
  }, [DEFAULT_FREE_PROXY_PROFILE_ID, DEFAULT_PAID_PROXY_PROFILE_ID, createDraftProxyProfile]);

  const handleClearProxyProfileUrl = useCallback((profileId) => {
    const id = String(profileId || "");
    if (!id) return;
    setProxyEditors((prev) => ({
      ...prev,
      [id]: {
        ...(prev[id] || {}),
        proxy_url: "",
        dirty_url: true,
      },
    }));
  }, []);

  const handleSaveProxyConfig = useCallback(async (event) => {
    event.preventDefault();
    if (!isAuthenticated) {
      setAuthMode("login");
      setAuthError("Nejdřív se přihlas, pak nastav proxy profily.");
      setShowAuthModal(true);
      return;
    }
    if (proxySaving) return;

    const validationErrors = [];
    for (const profile of (proxyConfig.profiles || [])) {
      const id = String(profile?.id || "").trim();
      if (!id) continue;
      const editor = proxyEditors[id] || {};
      const smartRaw = String(proxySmartInput[id] || "").trim();
      const nameRaw = String(editor.name || profile.name || "").trim();
      const urlRaw = String(editor.proxy_url || "").trim();
      const profileLabel = nameRaw || id;

      const hasExplicitUrl = urlRaw.length > 0 && !isMaskedProxyValue(urlRaw);
      const hasAnyInput = smartRaw.length > 0 || nameRaw.length > 0 || hasExplicitUrl;
      if (!hasAnyInput) continue;

      if (smartRaw && !parseProxyString(smartRaw) && !hasExplicitUrl) {
        validationErrors.push(`Profil '${profileLabel}': pole Curl nema validni format.`);
        continue;
      }

      if (nameRaw && !hasExplicitUrl && !profile.has_proxy_url) {
        validationErrors.push(`Profil '${profileLabel}': chybi Proxy URL.`);
      }
    }

    if (validationErrors.length > 0) {
      const message = validationErrors[0];
      setProxyTestStatus({ state: "error", message });
      showToast(message, "error");
      return;
    }

    const profiles = (proxyConfig.profiles || []).map((profile) => {
      const editor = proxyEditors[profile.id] || {};
      const explicitName = String(editor.name || profile.name || "").trim();
      const explicitUrl = String(editor.proxy_url || "").trim();
      const item = {
        id: profile.id,
        name: explicitName || String(profile.id || ""),
        kind: editor.kind === "paid_proxy" ? "paid_proxy" : profile.kind === "paid_proxy" ? "paid_proxy" : "free_proxy",
        proxy_curl: String(proxySmartInput[profile.id] || "").trim(),
      };
      if (editor.dirty_url) {
        item.proxy_url = explicitUrl;
      }
      return item;
    }).filter((item) => {
      const editor = proxyEditors[item.id] || {};
      const rawName = String(editor.name || "").trim();
      const rawUrl = String(editor.proxy_url || "").trim();
      const hasExplicitUrlInput = rawUrl.length > 0 && !isMaskedProxyValue(rawUrl);
      const hasUrlInput = hasExplicitUrlInput || String(item.proxy_url || "").trim().length > 0;
      const hasNameInput = rawName.length > 0;

      // Skip local empty draft rows so "delete all" can be persisted.
      if (!hasNameInput && !hasUrlInput) {
        return false;
      }
      return true;
    });

    setProxySaving(true);
    setProxyTestStatus({ state: "testing", message: "Testuji pripojeni proxy..." });
    try {
      const profilesToTest = profiles.filter((item) => {
        if (!Object.prototype.hasOwnProperty.call(item, "proxy_url")) return false;
        return String(item.proxy_url || "").trim().length > 0;
      });
      const testOutputs = [];
      for (const profile of profilesToTest) {
        const url = String(profile.proxy_url || "").trim();
        if (!url) continue;
        try {
          const result = await testProxyConnection(url);
          const profileLabel = String(profile.name || profile.id || "profil").trim();
          const externalIp = String(result?.external_ip || "").trim();
          if (externalIp) {
            testOutputs.push(`${profileLabel}: OK, vystupni IP ${externalIp}`);
          } else {
            testOutputs.push(`${profileLabel}: OK`);
          }
        } catch (err) {
          const profileLabel = String(profile.name || profile.id || "profil").trim();
          throw new Error(`Test profilu '${profileLabel}' selhal: ${String(err?.message || "neznamy duvod")}`);
        }
      }

      const data = await saveProxyConfig({ profiles });
      applyProxyConfigResponse(data);
      if (profilesToTest.length > 0) {
        setProxyTestStatus({
          state: "success",
          message: testOutputs.length
            ? `Vysledek testu: ${testOutputs.join(" | ")}`
            : "Proxy byla otestovana a ulozena.",
        });
        showToast("Proxy konfigurace byla otestovana a ulozena.", "info");
      } else {
        const hasStoredProxy = profiles.some((item) => {
          const sourceProfile = (proxyConfig.profiles || []).find((p) => String(p.id || "") === String(item.id || ""));
          return Boolean(sourceProfile?.has_proxy_url);
        });

        if (hasStoredProxy) {
          setProxyTestStatus({
            state: "success",
            message: "URL uz je ulozena (maskovana), ale nebyla znovu testovana, protoze neni dostupna v plnem tvaru.",
          });
          showToast("Ulozeno. Existujici URL zustala beze zmen (maskovana).", "info");
        } else {
          setProxyTestStatus({
            state: "success",
            message: "Neni vyplnena zadna Proxy URL, proto nebylo co testovat.",
          });
          showToast("Neni vyplnena zadna Proxy URL, ulozeni probehlo bez testu.", "info");
        }
      }
    } catch (err) {
      setProxyTestStatus({ state: "error", message: String(err?.message || "Proxy test nebo ulozeni selhalo.") });
      showToast(err?.message || "Ulozeni konfigurace proxy selhalo.", "error");
    } finally {
      setProxySaving(false);
    }
  }, [
    isAuthenticated,
    proxySaving,
    proxyConfig.profiles,
    proxyEditors,
    proxySmartInput,
    isMaskedProxyValue,
    parseProxyString,
    applyProxyConfigResponse,
  ]);

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
    const target = page === "pricing" || page === "proxy" ? page : "dashboard";
    const path = target === "pricing" ? "/pricing" : target === "proxy" ? "/proxy" : "/";
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
            <p className="pricing-note">
              Rezimy s vlastni proxy pouzivaji endpointy, ktere si uzivatel doda sam. Prenos a GB se uctuji u tveho proxy providera, ne v teto aplikaci.
            </p>
            {billingRatesError ? <p className="pricing-note">{billingRatesError}</p> : null}
            <div className="pricing-actions">
              {isAuthenticated ? (
                <button className="btn-primary" onClick={handleStartCheckout} disabled={checkoutBusy}>
                  {checkoutBusy ? "Vytvarim checkout..." : "Otevrit billing checkout"}
                </button>
              ) : (
                <button
                  className="btn-primary"
                  onClick={() => {
                    setAuthMode("login");
                    setAuthError("");
                    setShowAuthModal(true);
                  }}
                >
                  Prihlasit se pro billing a API usage
                </button>
              )}
              {billingAccess ? (
                <p className="pricing-note">
                  Stav uctu: billing={billingAccess.can_run_cloud ? "active" : "inactive"}, proxy_access={(billingAccess.can_run_free_proxy ?? billingAccess.can_run_local_free) ? "on" : "off"}
                </p>
              ) : null}
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
              <li>Orchestrace proxy behu (řízení běhů, validace, bezpečnostní kontroly konfigurace).</li>
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

  function renderProxyPage() {
    if (!isAuthenticated) {
      return (
        <div className="auth-dashboard-cta">
          <h2>Nastaveni proxy</h2>
          <p>Pro editaci proxy profilu se prosim prihlas.</p>
          <button
            type="button"
            className="btn-primary"
            onClick={() => {
              setAuthMode("login");
              setAuthError("");
              setShowAuthModal(true);
            }}
          >
            Otevrit login
          </button>
        </div>
      );
    }

    return (
      <div className="pricing-page">
        <div className="pricing-head">
          <h2>Proxy</h2>
          <p>Sprava proxy profilu je oddelena od dashboardu, aby zustal landing cisty.</p>
        </div>
        {renderProxyConfigPanel()}
      </div>
    );
  }

  function renderProxyConfigPanel() {
    const selectedProvider = providerCards.find((p) => p.id === selectedProviderId) || providerCards[0];

    return (
      <section className="byop-config-card" id="proxy-profiles">
        <div className="byop-config-head">
          <button
            type="button"
            className="byop-help-corner"
            onClick={() => {
              setHelpProviderId(selectedProvider.id);
              setShowProxyHelpModal(true);
            }}
            title="Proc je proxy potreba a jak ji nastavit"
          >
            ?
          </button>
          <h3>Proxy Profily</h3>
          <p>
            Transparentne: dáváme ti svobodu a platis jen za data, ktera realne spotrebujes u sveho providera.
          </p>
        </div>

        <div className="proxy-provider-cards" role="radiogroup" aria-label="Vyber providera">
          {providerCards.map((provider) => {
            const active = provider.id === selectedProvider.id;
            return (
              <button
                key={provider.id}
                type="button"
                className={`proxy-provider-card ${active ? "active" : ""}`}
                onClick={() => setSelectedProviderId(provider.id)}
                role="radio"
                aria-checked={active}
              >
                <span className="proxy-provider-logo">{provider.logo}</span>
                <span className="proxy-provider-name">{provider.name}</span>
                {provider.recommended ? <span className="proxy-provider-badge">Doporuceno</span> : null}
              </button>
            );
          })}
        </div>

        <div className="proxy-provider-steps">
          <a
            className="btn-primary proxy-provider-cta"
            href={selectedProvider.url}
            target={selectedProvider.url.startsWith("#") ? "_self" : "_blank"}
            rel={selectedProvider.url.startsWith("#") ? undefined : "noreferrer"}
          >
            {selectedProvider.ctaLabel}
          </a>
          <ol>
            <li>Zaloz si ucet pres toto tlacitko.</li>
            <li>Aktivuj zakladni balicek.</li>
            <li>Zkopiruj vygenerovane udaje o proxy.</li>
          </ol>
          <button
            type="button"
            className="theme-toggle"
            onClick={() => {
              setHelpProviderId(selectedProvider.id);
              setShowProxyHelpModal(true);
            }}
          >
            Kde tyto udaje najdu?
          </button>
        </div>

        <form className="byop-config-form" onSubmit={handleSaveProxyConfig}>
          <div className="byop-config-grid byop-profile-list">
            {(proxyConfig.profiles || []).map((profile) => {
              const editor = proxyEditors[profile.id] || {};
              const profileName = String(editor.name || profile.name || profile.id || "");
              const smartValue = String(proxySmartInput[profile.id] || "");
              const parsedSmart = parseProxyString(smartValue);
              const parsedUrl = parseProxyString(String(editor.proxy_url || ""));
              const parsed = parsedSmart || parsedUrl;
              return (
                <div className="byop-field byop-profile-row" key={profile.id}>
                  <div className="byop-profile-row-meta">
                    <input
                      className="proxy-field proxy-field-name"
                      type="text"
                      value={profileName}
                      onChange={(e) => handleProxyProfileChange(profile.id, "name", e.target.value)}
                      placeholder="Nazev profilu"
                      autoComplete="off"
                    />
                    <div className="proxy-kind-pill">Proxy</div>
                  </div>
                  <div className="proxy-smart-paste-row">
                    <input
                      className="proxy-field proxy-field-smart"
                      type="text"
                      value={smartValue}
                      onChange={(e) => handleSmartInputChange(profile.id, e.target.value)}
                      placeholder="Curl: curl --proxy http://user:pass@host:port https://api.ipify.org"
                      autoComplete="off"
                      spellCheck={false}
                    />
                    <button
                      type="button"
                      className="theme-toggle"
                      disabled={proxySaving || proxyLoading}
                      onClick={() => handleSmartPasteApply(profile.id)}
                    >
                      Automatic
                    </button>
                  </div>
                  <span className="byop-hint">Priklad: curl --proxy http://user:pass@host:port https://api.ipify.org</span>
                  <input
                    className="proxy-field proxy-field-url"
                    type="text"
                    value={String(editor.proxy_url || "")}
                    onChange={(e) => handleProxyProfileChange(profile.id, "proxy_url", e.target.value)}
                    placeholder="http://user:pass@host:port nebo socks5h://user:pass@host:port"
                    autoComplete="off"
                    spellCheck={false}
                  />
                  {parsed ? (
                    <div className="proxy-parts-preview">
                      <span>Host: {parsed.host || "-"}</span>
                      <span>Port: {parsed.port || "-"}</span>
                      <span>Jmeno: {parsed.username || "-"}</span>
                      <span>Heslo: {parsed.password ? "******" : "-"}</span>
                    </div>
                  ) : null}
                  <span className="byop-hint">
                    Stav: {profile.has_proxy_url ? `nastaven (${profile.proxy_preview || "maskovano"})` : "nenastaven"}
                  </span>
                  <div className="byop-inline-actions">
                    <button
                      type="button"
                      className="theme-toggle"
                      disabled={proxySaving || proxyLoading}
                      onClick={() => handleClearProxyProfileUrl(profile.id)}
                    >
                      Vymazat URL
                    </button>
                    <button
                      type="button"
                      className="theme-toggle"
                      disabled={proxySaving || proxyLoading || profile.id === DEFAULT_FREE_PROXY_PROFILE_ID || profile.id === DEFAULT_PAID_PROXY_PROFILE_ID}
                      onClick={() => handleRemoveProxyProfile(profile.id)}
                    >
                      Odebrat profil
                    </button>
                  </div>
                </div>
              );
            })}

          </div>

          <div className="byop-actions">
            <button
              type="button"
              className="theme-toggle"
              disabled={proxySaving || proxyLoading}
              onClick={handleAddProxyProfile}
            >
              + Pridat profil
            </button>
            <button type="submit" className="btn-primary" disabled={proxySaving || proxyLoading}>
              {proxySaving ? "Testuji a ukladam..." : "Otestovat a ulozit"}
            </button>
            {proxyLoading ? <span className="muted">Nacitam konfiguraci...</span> : null}
            {proxyTestStatus.state === "testing" ? <span className="muted">{proxyTestStatus.message}</span> : null}
            {proxyTestStatus.state === "success" ? <span className="proxy-test-success">OK {proxyTestStatus.message}</span> : null}
            {proxyTestStatus.state === "error" ? <span className="proxy-test-error">{proxyTestStatus.message}</span> : null}
          </div>
        </form>
      </section>
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
            proxyProfiles={proxyConfig.profiles || []}
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

  const helpProvider = providerCards.find((p) => p.id === helpProviderId) || providerCards[0];

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
            className={`topbar-link-btn ${currentPage === "proxy" ? "active" : ""}`}
            onClick={() => navigateTo("proxy")}
            title="Otevrit nastaveni proxy"
          >
            Proxy
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
              <h2>Prihlaseni je volitelne</h2>
              <p>Nastaveni i spusteni projektu funguje bez loginu. Prihlaseni je potreba hlavne pro checkout a billing.</p>
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
          {currentPage === "pricing" ? renderPricingPage() : currentPage === "proxy" ? renderProxyPage() : renderProjectContent()}
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

      {showProxyHelpModal && (
        <div className="log-popup-overlay" onClick={() => setShowProxyHelpModal(false)}>
          <div className="log-popup byop-help-modal" onClick={(e) => e.stopPropagation()}>
            <div className="log-popup-head">
              <strong>Proxy navod: {helpProvider.name}</strong>
              <button className="debug-modal-close" onClick={() => setShowProxyHelpModal(false)}>
                <X className="ui-icon" aria-hidden="true" />
              </button>
            </div>
            <div className="byop-help-body">
              <p>
                Nechceme, aby onboarding bolel: tady je presne co na webu poskytovatele hledat a co zkopirovat.
              </p>
              <div className="proxy-help-shot" aria-hidden="true">
                <div className="proxy-help-shot-top">{helpProvider.name} dashboard</div>
                <div className="proxy-help-shot-body">
                  <span className="proxy-help-pill">Proxy list</span>
                  <span className="proxy-help-pill">Endpoint</span>
                  <span className="proxy-help-pill proxy-help-pill-danger">Copy string</span>
                </div>
                <div className="proxy-help-circle">Zkopiruj tento retezec</div>
              </div>
              <h4>Co presne hledat</h4>
              <ol>
                <li>Otevri: {helpProvider.whereToFind}.</li>
                <li>Zkopiruj endpoint ve tvaru `IP:PORT:USER:PASS` nebo `http://user:pass@host:port`.</li>
                <li>Vloz string do Smart paste pole a klikni na `Rozparsovat`.</li>
                <li>Pouzij `Otestovat a ulozit` a pak vyber profil ve Scraping Settings.</li>
              </ol>
              <h4>Dulezite</h4>
              <ul>
                <li>Podporovane schema: `http`, `https`, `socks5h`.</li>
                <li>Localhost a privatni IP nejsou z bezpecnostnich duvodu povolene.</li>
                <li>Aplikace proxy neprodava, jen ji bezpecne orchestruje.</li>
              </ul>
            </div>
            <div className="log-popup-foot">
              <button className="btn-sm secondary" onClick={() => setShowProxyHelpModal(false)}>
                Zavrit
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