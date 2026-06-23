// ── Scoring constants & calculation logic extracted from App.jsx ──

export const DEFAULT_SCORE_WEIGHTS = {
  age: 1,
  mileage: 1,
  price: 1,
  consumption: 1,
  cost: 1,
  price_power: 1,
  power: 0.75,
  equipment: 0.85,
  flags: 1,
  sport: 0.35,
  luxury: 0.35,
  power_weight: 0.4,
  sport_badge: 0.3,
  premium_equipment: 0.5,
  tco: 0.6,
};

export const LOCAL_SCORING_PRESETS = {
  value: {
    name: "Cena / výkon",
    description: "Nejlepší poměr ceny, výkonu a provozních nákladů.",
    weights: { age: 0.75, mileage: 1.1, price: 1.4, price_power: 1.85, cost: 1.45, consumption: 1.15, power: 0.85, equipment: 0.45, flags: 1.15, sport: 0.25, luxury: 0.15, power_weight: 0.2, sport_badge: 0.1, premium_equipment: 0.15, tco: 1.5 },
  },
  balanced: {
    name: "Balanced",
    description: "Univerzální hodnocení: stav, nájezd, cena, výkon, náklady i výbava.",
    weights: { age: 1, mileage: 1, price: 1, price_power: 1, cost: 1, consumption: 1, power: 0.75, equipment: 0.85, flags: 1, sport: 0.35, luxury: 0.35, power_weight: 0.4, sport_badge: 0.3, premium_equipment: 0.5, tco: 0.6 },
  },
  sport: {
    name: "Sport",
    description: "Priorita: výkon, dynamika, cena za kW, pohon a mladší kusy.",
    weights: { age: 1.05, mileage: 0.75, price: 0.55, price_power: 1.3, cost: 0.55, consumption: 0.35, power: 2.1, equipment: 0.45, flags: 0.8, sport: 1.45, luxury: 0.2, power_weight: 1.6, sport_badge: 1.4, premium_equipment: 0.25, tco: 0.3 },
  },
  luxury: {
    name: "Luxury",
    description: "Priorita: prémiová značka, výbava, komfort a kultivovaný výkon.",
    weights: { age: 1.35, mileage: 0.9, price: 0.25, price_power: 0.45, cost: 0.35, consumption: 0.25, power: 0.8, equipment: 2.1, flags: 0.9, sport: 0.25, luxury: 1.9, power_weight: 0.3, sport_badge: 0.2, premium_equipment: 1.7, tco: 0.3 },
  },
  daily: {
    name: "Daily Driver",
    description: "Spolehlivé auto na každý den s nízkými náklady a rozumným nájezdem.",
    weights: { age: 0.9, mileage: 1.3, price: 1.1, price_power: 0.5, cost: 1.5, consumption: 1.4, power: 0.6, equipment: 1.2, flags: 1.3, sport: 0.15, luxury: 0.25, power_weight: 0.15, sport_badge: 0.05, premium_equipment: 0.3, tco: 1.6 },
  },
  weekend: {
    name: "Weekend Toy",
    description: "Víkendová hračka – výkon, dynamika a radost z jízdy nad všechno.",
    weights: { age: 1.0, mileage: 0.4, price: 0.3, price_power: 1.6, cost: 0.2, consumption: 0.2, power: 2.0, equipment: 0.6, flags: 0.7, sport: 1.8, luxury: 0.4, power_weight: 2.0, sport_badge: 1.8, premium_equipment: 0.2, tco: 0.1 },
  },
  family: {
    name: "Family Hauler",
    description: "Rodinné auto – bezpečnost, prostor, výbava a přijatelné náklady.",
    weights: { age: 1.2, mileage: 1.1, price: 1.0, price_power: 0.4, cost: 1.3, consumption: 1.1, power: 0.5, equipment: 1.8, flags: 1.5, sport: 0.1, luxury: 0.6, power_weight: 0.1, sport_badge: 0.0, premium_equipment: 1.5, tco: 1.2 },
  },
  budget: {
    name: "Budget King",
    description: "Nejlepší poměr cena/užitná hodnota – co nejvíc auta za co nejmíň peněz.",
    weights: { age: 0.6, mileage: 0.8, price: 2.2, price_power: 2.0, cost: 1.8, consumption: 1.5, power: 0.4, equipment: 0.6, flags: 1.0, sport: 0.1, luxury: 0.1, power_weight: 0.1, sport_badge: 0.0, premium_equipment: 0.05, tco: 2.0 },
  },
  tech: {
    name: "Tech & Comfort",
    description: "Moderní technologické auto – výbava, asistenty a komfort na prvním místě.",
    weights: { age: 1.4, mileage: 0.8, price: 0.5, price_power: 0.3, cost: 0.5, consumption: 0.7, power: 0.6, equipment: 2.3, flags: 0.9, sport: 0.2, luxury: 1.4, power_weight: 0.2, sport_badge: 0.1, premium_equipment: 2.2, tco: 0.4 },
  },
};

export function csvToArray(value) {
  return String(value || "")
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean);
}

export function uniq(arr) {
  return Array.from(new Set(arr));
}

export function num(value, fallback = null) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

export function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

export function scoreMax(value, bands, fallback = 0) {
  const n = num(value);
  if (n === null || n <= 0) return fallback;
  for (const [max, score] of bands) {
    if (n <= max) return score;
  }
  return bands[bands.length - 1][1];
}

export function scoreMin(value, bands, fallback = 0) {
  const n = num(value);
  if (n === null || n <= 0) return fallback;
  for (const [min, score] of bands) {
    if (n >= min) return score;
  }
  return bands[bands.length - 1][1];
}

export function equipmentText(item) {
  return (Array.isArray(item.equipment_list) ? item.equipment_list : [])
    .join(" ")
    .toLowerCase();
}

export function hasAny(text, patterns) {
  return patterns.some((pattern) => pattern.test(text));
}

// ── First pass: raw component scores (0‑100 after normalization) ──
export function calculateScoreComponents(item) {
  const equipment = Array.isArray(item.equipment_list) ? item.equipment_list : [];
  const eqText = equipmentText(item);
  const power = num(item.power_kw, 0);
  const fuel = String(item.fuel_seo || "").toLowerCase();
  const gearbox = String(item.gearbox_type || "").toLowerCase();
  const drive = String(item.drive_type || "").toLowerCase();
  const brandTier = String(item.brand_tier || "").toLowerCase();

  const raw = {
    age: scoreMax(item.age_years, [[2, 78], [5, 62], [8, 43], [12, 24], [16, 6], [20, -18], [999, -35]]),
    mileage: scoreMax(item.tachometer, [[50000, 72], [80000, 58], [140000, 38], [200000, 14], [260000, -12], [9999999, -36]]),
    price: scoreMax(item.price, [[120000, 56], [200000, 40], [350000, 22], [600000, 4], [1000000, -12], [99999999, -30]]),
    price_power: scoreMax(item.price_per_kw, [[1200, 72], [1800, 52], [2600, 32], [3600, 10], [5200, -14], [999999, -36]]),
    power: scoreMin(power, [[220, 72], [170, 56], [130, 36], [100, 16], [75, 2], [55, -12], [0, -28]]),
    cost: scoreMax(item.annual_total_cost, [[35000, 48], [50000, 32], [70000, 14], [95000, -8], [9999999, -28]]),
    consumption: fuel === "elektro"
      ? scoreMax(item.estimated_consumption_per_100km, [[16, 34], [20, 24], [24, 10], [30, -6], [999, -18]])
      : scoreMax(item.estimated_consumption_per_100km, [[5.5, 34], [6.8, 24], [8.0, 10], [9.5, -8], [999, -24]]),
    equipment: 0,
    flags: 0,
    sport: 0,
    luxury: 0,
  };

  if (equipment.length >= 45) raw.equipment += 18;
  else if (equipment.length >= 30) raw.equipment += 11;
  else if (equipment.length >= 18) raw.equipment += 5;

  if (hasAny(eqText, [/adaptivn[ií]\s*tempomat/, /front assist/, /nouzov[eé]\s*brzd/])) raw.equipment += 12;
  if (hasAny(eqText, [/parkovac[ií]\s*kamera/, /360/, /couvac[ií]\s*kamera/])) raw.equipment += 8;
  if (hasAny(eqText, [/parkovac[ií]\s*senzory/])) raw.equipment += 5;
  if (hasAny(eqText, [/apple\s*car\s*play/, /android\s*auto/, /navigace/])) raw.equipment += 8;
  if (hasAny(eqText, [/vyh[rř]ivan[aá]\s*sedadla/, /vyh[rř]ivan[eé]\s*celn[ií]\s*sklo/])) raw.equipment += 6;
  if (hasAny(eqText, [/led\s*sv[eě]tl/, /xenon/, /matrix/])) raw.equipment += 6;
  if (hasAny(eqText, [/mrtv[eé]ho\s*[uú]hlu/, /j[ií]zdn[ií]ho\s*pruhu/, /lane assist/])) raw.equipment += 8;
  raw.equipment = clamp(raw.equipment, 0, 71);

  if (item.service_book) raw.flags += 14;
  if (item.first_owner) raw.flags += 9;
  if (item.tuning) raw.flags -= 28;

  if (power >= 220) raw.sport += 36;
  else if (power >= 170) raw.sport += 28;
  else if (power >= 130) raw.sport += 16;
  else if (power < 75) raw.sport -= 12;
  if (drive === "rwd") raw.sport += 12;
  else if (drive === "awd") raw.sport += 9;
  if (gearbox === "manual") raw.sport += 6;
  if (num(item.price_per_kw, 999999) <= 2200) raw.sport += 10;
  if (item.tuning) raw.sport -= 18;

  if (brandTier === "premium") raw.luxury += 24;
  else if (brandTier === "budget") raw.luxury -= 6;
  if (gearbox === "automatic") raw.luxury += 10;
  if (hasAny(eqText, [/ko[zž]en[aá]/, /alcantara/, /mas[aá][zž]/])) raw.luxury += 13;
  if (hasAny(eqText, [/panoramatick[aá]\s*st[rř]echa/, /st[rř]e[sš]n[ií]\s*okno/])) raw.luxury += 9;
  if (equipment.length >= 40) raw.luxury += 14;
  else if (equipment.length >= 25) raw.luxury += 8;
  if (num(item.age_years, 99) <= 5) raw.luxury += 12;
  if (power >= 130) raw.luxury += 7;
  raw.luxury = clamp(raw.luxury, -20, 75);

  const BODY_WEIGHT_KG = {
    hatchback: 1250, liftback: 1400, sedan: 1480, kombi: 1500,
    coupe: 1450, kabriolet: 1550, mpv: 1700, suv: 1850,
    terenni: 2000, "pick-up": 2100, van: 1950,
  };
  const estWeightKg = BODY_WEIGHT_KG[String(item.body_seo || "").toLowerCase()] || 1500;
  const powerPerTonne = power > 0 ? power / (estWeightKg / 1000) : 0;
  raw.power_weight = scoreMin(powerPerTonne, [[180, 40], [145, 30], [115, 18], [88, 6], [62, -4], [0, -12]]);

  const listingName = String(item.name || "").toLowerCase();
  const STRONG_BADGE = /(\bamg\b|\bm[1-8]\b|\bm\s?performance\b|\brs\s?\d?\b|\bvrs\b|\bgti\b|\bgtd\b|\bgts\b|\btype[\s-]?r\b|\bsti\b|\bnismo\b|\babarth\b|\bpolestar\b|\bcupra\b|\bgr\b)/;
  const MILD_BADGE = /(m[\s-]?paket|m[\s-]?sport|s[\s-]?line|r[\s-]?line|n[\s-]?line|st[\s-]?line|\bsport\b)/;
  if (STRONG_BADGE.test(listingName)) raw.sport_badge = 24;
  else if (MILD_BADGE.test(listingName) || MILD_BADGE.test(eqText)) raw.sport_badge = 8;
  else raw.sport_badge = 0;

  const PREMIUM_FEATURES = [
    /ko[zž]en|alcantara/,
    /panoramatick|panorama/,
    /matrix/,
    /adaptivn[ií]\s*tempomat/,
    /vzduchov[eé]\s*odpru|pneumatick[eé]\s*odpru|air\s*suspension/,
    /ventilovan|mas[aá][zž]/,
    /head[\s-]?up/,
    /360/,
    /bezkl[ií][cč]|keyless/,
    /pam[eě][tť]\s*sedadel|memory/,
    /ambientn/,
  ];
  const premiumCount = PREMIUM_FEATURES.reduce((c, re) => c + (re.test(eqText) ? 1 : 0), 0);
  raw.premium_equipment =
    premiumCount >= 6 ? 32 : premiumCount >= 4 ? 22 : premiumCount >= 2 ? 12 : premiumCount >= 1 ? 5 : 0;

  const tco5y = num(item.price, 0) + num(item.annual_total_cost, 0) * 5;
  raw.tco = scoreMax(tco5y, [[250000, 40], [400000, 28], [600000, 14], [900000, 0], [1400000, -14], [99999999, -30]]);

  const RANGES = {
    age: [-35, 78],
    mileage: [-36, 72],
    price: [-30, 56],
    price_power: [-36, 72],
    power: [-28, 72],
    cost: [-28, 48],
    consumption: [-24, 34],
    equipment: [0, 71],
    flags: [-28, 23],
    sport: [-30, 73],
    luxury: [-20, 75],
    power_weight: [-12, 40],
    sport_badge: [0, 24],
    premium_equipment: [0, 32],
    tco: [-30, 40],
  };

  const norm = (val, min, max) => clamp(Math.round(((val - min) / (max - min)) * 100), 0, 100);

  return {
    age: norm(raw.age, ...RANGES.age),
    mileage: norm(raw.mileage, ...RANGES.mileage),
    price: norm(raw.price, ...RANGES.price),
    price_power: norm(raw.price_power, ...RANGES.price_power),
    power: norm(raw.power, ...RANGES.power),
    cost: norm(raw.cost, ...RANGES.cost),
    consumption: norm(raw.consumption, ...RANGES.consumption),
    equipment: norm(raw.equipment, ...RANGES.equipment),
    flags: norm(raw.flags, ...RANGES.flags),
    sport: norm(raw.sport, ...RANGES.sport),
    luxury: norm(raw.luxury, ...RANGES.luxury),
    power_weight: norm(raw.power_weight, ...RANGES.power_weight),
    sport_badge: norm(raw.sport_badge, ...RANGES.sport_badge),
    premium_equipment: norm(raw.premium_equipment, ...RANGES.premium_equipment),
    tco: norm(raw.tco, ...RANGES.tco),
  };
}

export function getPresetWeights(preset) {
  return preset?.weights || preset?.multipliers || DEFAULT_SCORE_WEIGHTS;
}

export function getItemScore(item, preset) {
  const components = calculateScoreComponents(item);
  const weights = getPresetWeights(preset);
  const weightedScore = Object.entries(components).reduce((sum, [key, value]) => {
    return sum + value * (weights[key] ?? DEFAULT_SCORE_WEIGHTS[key] ?? 1);
  }, 0);

  return Math.round(weightedScore);
}

function formatScoreNumber(value, decimals = 0) {
  if (!Number.isFinite(value)) return "0";
  if (decimals <= 0) return String(Math.round(value));
  return Number(value.toFixed(decimals)).toLocaleString("cs-CZ", {
    minimumFractionDigits: 0,
    maximumFractionDigits: decimals,
  });
}

const SCORE_COMPONENT_LABELS = {
  age: "Stari",
  mileage: "Najezd",
  price: "Cena",
  price_power: "Cena/kW",
  power: "Vykon",
  consumption: "Spotreba",
  cost: "Provozni naklady",
  equipment: "Vybava",
  flags: "Stav/Historie",
  sport: "Sport",
  luxury: "Luxus",
  power_weight: "Vykon/vaha",
  sport_badge: "Sportovni oznaceni",
  premium_equipment: "Premiova vybava",
  tco: "TCO (5 let)",
};

export function getItemScoreDetails(item, preset, topN = 6) {
  const components = calculateScoreComponents(item);
  const weights = getPresetWeights(preset);

  const contributions = Object.entries(components).map(([key, componentScore]) => {
    const weight = weights[key] ?? DEFAULT_SCORE_WEIGHTS[key] ?? 1;
    const points = componentScore * weight;
    return {
      key,
      label: SCORE_COMPONENT_LABELS[key] || key,
      componentScore,
      weight,
      points,
    };
  });

  const total = contributions.reduce((sum, entry) => sum + entry.points, 0);
  const score = Math.round(total);
  const topFactors = contributions
    .slice()
    .sort((a, b) => Math.abs(b.points) - Math.abs(a.points))
    .slice(0, Math.max(1, topN));

  const tooltipLines = [
    `Skore: ${formatScoreNumber(score)}`,
    `Preset: ${preset?.name || "Custom"}`,
    "Nejvetsi vlivy:",
    ...topFactors.map((entry) => {
      const sign = entry.points >= 0 ? "+" : "-";
      const absPoints = Math.abs(entry.points);
      return `${entry.label}: ${formatScoreNumber(entry.componentScore)} x ${formatScoreNumber(entry.weight, 2)} = ${sign}${formatScoreNumber(absPoints, 1)}`;
    }),
  ];

  return {
    score,
    contributions,
    topFactors,
    tooltip: tooltipLines.join("\n"),
  };
}

export function isSuspiciousMileage(item) {
  const age = num(item.age_years);
  const km = num(item.tachometer);
  if (age === null || km === null || age <= 0) return false;
  if (age >= 10 && km < 80000) return true;
  const kmPerYear = km / age;
  if (kmPerYear < 3000 && km > 0) return true;
  return false;
}

export function fmtVal(val, fmt) {
  const n = parseFloat(val);
  if (isNaN(n)) return val ?? "";
  if (fmt === "price") return n.toLocaleString("cs-CZ") + " Kč";
  if (fmt === "km") return n.toLocaleString("cs-CZ") + " km";
  if (fmt === "ratio") return n.toFixed(2) + "×";
  return n.toLocaleString("cs-CZ");
}

export function fmtDate(ts) {
  if (!ts) return "—";
  return new Date(ts * 1000).toLocaleString("cs-CZ");
}

export const ALL_WEIGHT_KEYS = [
  { key: "age", label: "Stáří" },
  { key: "mileage", label: "Nájezd" },
  { key: "price", label: "Cena" },
  { key: "price_power", label: "Cena/kW" },
  { key: "power", label: "Výkon" },
  { key: "consumption", label: "Spotřeba" },
  { key: "cost", label: "Provozní náklady" },
  { key: "equipment", label: "Výbava" },
  { key: "flags", label: "Stav/Historie" },
  { key: "sport", label: "Sport" },
  { key: "luxury", label: "Luxus" },
  { key: "power_weight", label: "Výkon/váha" },
  { key: "sport_badge", label: "Sportovní označení" },
  { key: "premium_equipment", label: "Prémiová výbava" },
  { key: "tco", label: "TCO (5 let)" },
];

export const PARAM_GROUPS = [
  {
    label: "Hledání",
    fields: [
      { key: "seller_type", type: "select", label: "Prodejce", options: ["", "soukromy", "bazar"] },
      { key: "condition_seo", type: "text", label: "Stav (čárkou)" },
      { key: "operating_lease", type: "boolean", label: "Operativní leasing" },
    ],
  },
  {
    label: "Stránkování",
    fields: [
      { key: "category_id", type: "text", label: "Kategorie ID (838 = osobní)" },
      { key: "limit", type: "slider", label: "Limit na stránku", min: 1, max: 1000, step: 1 },
      { key: "offset", type: "number", label: "Offset" },
    ],
  },
  {
    label: "Hodnocení",
    fields: [
      { key: "interesting_min_score", type: "slider", label: "Min. skóre", min: -1000, max: 300, step: 1 },
      { key: "interesting_top_n", type: "slider", label: "Top N", min: 1, max: 5000, step: 1 },
      { key: "interesting_min_price", type: "slider", label: "Min. cena pro hodnocení", min: 0, max: 500000, step: 5000, fmt: "price" },
    ],
  },
  {
    label: "Tržní analýza",
    fields: [
      { key: "market_min_cohort_size", type: "slider", label: "Min. kohorta", min: 2, max: 50, step: 1 },
      { key: "market_expected_km_per_year", type: "slider", label: "Očekávaných km/rok", min: 5000, max: 40000, step: 1000, fmt: "km" },
      { key: "model_price_min_samples", type: "slider", label: "Min. vzorků modelu", min: 2, max: 30, step: 1 },
      { key: "undervalue_ratio_threshold", type: "slider", label: "Podhodnoceno ≤", min: 0.5, max: 0.99, step: 0.01, fmt: "ratio" },
      { key: "deep_undervalue_ratio_threshold", type: "slider", label: "Velmi podhodnoceno ≤", min: 0.4, max: 0.95, step: 0.01, fmt: "ratio" },
      { key: "overprice_ratio_threshold", type: "slider", label: "Předraženo ≥", min: 1.01, max: 2.0, step: 0.01, fmt: "ratio" },
    ],
  },
  {
    label: "Notifikace",
    fields: [
      { key: "discord_webhook_url", type: "text", label: "Discord webhook URL" },
      { key: "discord_notify_only_new", type: "boolean", label: "Pouze nové" },
    ],
  },
];

export const BASIC_GROUPS = [PARAM_GROUPS[0]];
// Stránkování/kategorie jsou zatím interně zamčené defaulty
// (category_id=838, limit=100, offset=0), takže je v UI nezobrazujeme.
export const ADVANCED_GROUPS = PARAM_GROUPS.slice(2);
export const IGNORED_KEYS = new Set([
  ...PARAM_GROUPS.flatMap((g) => g.fields.map((f) => f.key)),
  "exclude_manufacturer_seo_name",
  "exclude_model_seo_name",
  "exclude_body_seo",
  "exclude_condition_seo",
  "price_from", "price_to",
  "year_from", "year_to",
  "tachometer_from", "tachometer_to",
  "power_from", "power_to",
  "fuel_seo", "gearbox_filter", "drive_filter",
]);
