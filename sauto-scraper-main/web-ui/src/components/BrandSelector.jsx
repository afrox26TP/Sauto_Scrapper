import React from "react";

const BRAND_LOGOS = {
  skoda: "/logos/skoda.png",
  volkswagen: "/logos/volkswagen.png",
  vw: "/logos/volkswagen.png",
  audi: "/logos/audi.png",
  bmw: "/logos/bmw.png",
  mercedes: "/logos/mercedes-benz.png",
  "mercedes-benz": "/logos/mercedes-benz.png",
  benz: "/logos/mercedes-benz.png",
  ford: "/logos/ford.png",
  toyota: "/logos/toyota.png",
  honda: "/logos/honda.png",
  hyundai: "/logos/hyundai.png",
  kia: "/logos/kia.png",
  mazda: "/logos/mazda.png",
  nissan: "/logos/nissan.png",
  opel: "/logos/opel.png",
  peugeot: "/logos/peugeot.png",
  renault: "/logos/renault.png",
  seat: "/logos/seat.png",
  citroen: "/logos/citroen.png",
  "citroën": "/logos/citroen.png",
  fiat: "/logos/fiat.png",
  volvo: "/logos/volvo.png",
  dacia: "/logos/dacia.png",
  suzuki: "/logos/suzuki.png",
  mitsubishi: "/logos/mitsubishi.png",
  subaru: "/logos/subaru.png",
  porsche: "/logos/porsche.png",
  landrover: "/logos/land-rover.png",
  "land rover": "/logos/land-rover.png",
  jaguar: "/logos/jaguar.png",
  tesla: "/logos/tesla.png",
  mini: "/logos/mini.png",
  ferrari: "/logos/ferrari.png",
  lamborghini: "/logos/lamborghini.png",
  maserati: "/logos/maserati.png",
  alfaromeo: "/logos/alfa-romeo.png",
  "alfa romeo": "/logos/alfa-romeo.png",
  chevrolet: "/logos/chevrolet.png",
  lexus: "/logos/lexus.png",
  infiniti: "/logos/infiniti.png",
  acura: "/logos/acura.png",
  cadillac: "/logos/cadillac.png",
  chrysler: "/logos/chrysler.png",
  dodge: "/logos/dodge.png",
  jeep: "/logos/jeep.png",
  bentley: "/logos/bentley.png",
  astonmartin: "/logos/aston-martin.png",
  "aston martin": "/logos/aston-martin.png",
  mclaren: "/logos/mclaren.png",
  saab: "/logos/saab.png",
  genesis: "/logos/genesis.png",
  smart: "/logos/smart.png",
  lada: "/logos/lada.png",
  alpina: "/logos/alpina.png",
  byd: "/logos/byd.png",
  daewoo: "/logos/daewoo.png",
  daihatsu: "/logos/daihatsu.png",
  ds: "/logos/ds.png",
  hummer: "/logos/hummer.png",
  lancia: "/logos/lancia.png",
  mb: "/logos/mercedes-benz.png",
  polestar: "/logos/polestar.png",
  ram: "/logos/ram.png",
  ssangyong: "/logos/ssangyong.png",
};

const BRAND_FALLBACK_COLORS = {
  skoda: { bg: "#4ba82e", fg: "#fff" },
  volkswagen: { bg: "#1e3a5f", fg: "#fff" },
  vw: { bg: "#1e3a5f", fg: "#fff" },
  audi: { bg: "#000", fg: "#fff" },
  bmw: { bg: "#1c69d4", fg: "#fff" },
  mercedes: { bg: "#1a1a1a", fg: "#fff" },
  "mercedes-benz": { bg: "#1a1a1a", fg: "#fff" },
  benz: { bg: "#1a1a1a", fg: "#fff" },
  ford: { bg: "#003399", fg: "#fff" },
  toyota: { bg: "#eb0a1e", fg: "#fff" },
  honda: { bg: "#cc0000", fg: "#fff" },
  hyundai: { bg: "#003469", fg: "#fff" },
  kia: { bg: "#bb162b", fg: "#fff" },
  mazda: { bg: "#910a2d", fg: "#fff" },
  nissan: { bg: "#c3002f", fg: "#fff" },
  opel: { bg: "#ffcc00", fg: "#000" },
  peugeot: { bg: "#003355", fg: "#fff" },
  renault: { bg: "#ffcc00", fg: "#000" },
  seat: { bg: "#cc0000", fg: "#fff" },
  citroen: { bg: "#da291c", fg: "#fff" },
  "citroën": { bg: "#da291c", fg: "#fff" },
  fiat: { bg: "#c41230", fg: "#fff" },
  volvo: { bg: "#003057", fg: "#fff" },
  dacia: { bg: "#646b52", fg: "#fff" },
  suzuki: { bg: "#003366", fg: "#fff" },
  mitsubishi: { bg: "#e60012", fg: "#fff" },
  subaru: { bg: "#1e3b6f", fg: "#fff" },
  porsche: { bg: "#000", fg: "#fff" },
  landrover: { bg: "#005a3c", fg: "#fff" },
  "land rover": { bg: "#005a3c", fg: "#fff" },
  jaguar: { bg: "#1f1f1f", fg: "#fff" },
  tesla: { bg: "#cc0000", fg: "#fff" },
  mini: { bg: "#000", fg: "#fff" },
  ferrari: { bg: "#ff2800", fg: "#fff" },
  lamborghini: { bg: "#dbb321", fg: "#000" },
  maserati: { bg: "#003893", fg: "#fff" },
  alfaromeo: { bg: "#920037", fg: "#fff" },
  "alfa romeo": { bg: "#920037", fg: "#fff" },
};

const DARK_INVERT_BRANDS = new Set([
  "audi", "bmw", "mercedes", "mercedes-benz", "benz", "porsche",
  "jaguar", "landrover", "land rover", "bentley", "maserati",
  "lexus", "acura", "cadillac", "chrysler", "dodge", "jeep",
  "astonmartin", "aston martin", "mclaren", "alfaromeo", "alfa romeo",
  "genesis", "ferrari", "lamborghini", "smart", "mini", "mb",
]);

function getBrandLogo(brandValue) {
  const raw = (brandValue || "").toLowerCase().trim();
  if (BRAND_LOGOS[raw]) return BRAND_LOGOS[raw];
  const clean = raw.replace(/[-_\s]+/g, "");
  for (const [k, v] of Object.entries(BRAND_LOGOS)) {
    if (k.replace(/[-_\s]+/g, "") === clean) return v;
  }
  return null;
}

function getBrandFallbackColor(brandValue) {
  const key = (brandValue || "").toLowerCase().trim();
  return BRAND_FALLBACK_COLORS[key] || null;
}

function brandInitials(label) {
  const words = (label || "").trim().split(/\s+/);
  if (words.length >= 2) return (words[0][0] + words[1][0]).toUpperCase();
  return (label || "?").slice(0, 2).toUpperCase();
}

export default function BrandSelector({
  brands,
  selected,
  excluded,
  onToggle,
  filterText,
  onFilterChange,
  theme = "light",
}) {
  const filtered = brands.filter(
    (b) =>
      b.label.toLowerCase().includes(filterText.toLowerCase()) ||
      b.value.toLowerCase().includes(filterText.toLowerCase())
  );

  return (
    <div className="catalog-block">
      <div className="catalog-title">Výrobce</div>
      <input
        type="text"
        className="catalog-search"
        placeholder="Filtrovat značky..."
        value={filterText}
        onChange={(e) => onFilterChange(e.target.value)}
      />
      <div className="catalog-list">
        {filtered.map((b) => {
          const logoUrl = getBrandLogo(b.value);
          const fc = getBrandFallbackColor(b.value);
          const isExcluded = excluded.includes(b.value);
          return (
            <div key={b.value} className={`catalog-item${isExcluded ? " excluded" : ""}`}>
              <span
                className={`catalog-toggle-btn${selected.includes(b.value) ? " checked" : isExcluded ? " excluded" : ""}`}
                onClick={(e) => { e.stopPropagation(); onToggle(b.value); }}
                title={
                  selected.includes(b.value)
                    ? "✓ Zahrnuto — klikni pro vyloučení"
                    : isExcluded
                    ? "✕ Vyloučeno — klikni pro zrušení"
                    : "Klikni pro zahrnutí"
                }
              >
                {selected.includes(b.value) ? "✓" : isExcluded ? "✕" : ""}
              </span>
              {logoUrl ? (
                <img
                  className={`catalog-brand-logo${theme === "dark" && DARK_INVERT_BRANDS.has(b.value.toLowerCase().trim()) ? " invert" : ""}`}
                  src={logoUrl}
                  alt={b.label}
                  loading="lazy"
                  onError={(e) => {
                    e.target.style.display = "none";
                    const bdg = e.target.parentElement.querySelector(".catalog-brand-badge");
                    if (bdg) bdg.style.display = "inline-flex";
                  }}
                />
              ) : null}
              <span
                className="catalog-brand-badge"
                style={
                  fc
                    ? {
                        background: fc.bg,
                        color: fc.fg,
                        display: logoUrl ? "none" : "inline-flex",
                      }
                    : { display: logoUrl ? "none" : "inline-flex" }
                }
              >
                {brandInitials(b.label)}
              </span>
              <span>{b.label}</span>
            </div>
          );
        })}
      </div>
    </div>
  );
}

export { BRAND_LOGOS, BRAND_FALLBACK_COLORS, DARK_INVERT_BRANDS, getBrandLogo, getBrandFallbackColor };