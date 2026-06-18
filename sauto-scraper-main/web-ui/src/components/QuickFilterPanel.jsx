import React, { useState } from "react";
import { CustomSlider } from "./index";

function PairSlider({ label, enabled, onToggleEnabled, valueA, valueB, onChangeA, onChangeB, min, max, step, fmt }) {
  return (
    <div className={`quick-pair${enabled ? " enabled" : " disabled"}`}>
      <div
        className="quick-pair-label-row"
        onClick={() => onToggleEnabled(!enabled)}
        title={enabled ? "✓ Aktivní — klikni pro deaktivaci" : "Klikni pro aktivaci"}
      >
        <span className={`catalog-toggle-btn${enabled ? " checked" : ""}`}>
          {enabled ? "✓" : ""}
        </span>
        <span className={`quick-pair-label-text${enabled ? " checked" : ""}`}>{label}</span>
      </div>
      {enabled ? (
        <div className="quick-pair-sliders">
          <CustomSlider
            value={valueA || min}
            min={min}
            max={max}
            step={step}
            size="sm"
            formatValue={(v) => fmt ? fmt(v) : v}
            onChange={(val) => onChangeA(String(val))}
          />
          <CustomSlider
            value={valueB || max}
            min={min}
            max={max}
            step={step}
            size="sm"
            formatValue={(v) => fmt ? fmt(v) : v}
            onChange={(val) => onChangeB(String(val))}
          />
        </div>
      ) : (
        <div className="quick-pair-infinite">∞</div>
      )}
    </div>
  );
}

export default function QuickFilterPanel({
  priceFrom, priceTo, onPriceFromChange, onPriceToChange,
  yearFrom, yearTo, onYearFromChange, onYearToChange,
  kmFrom, kmTo, onKmFromChange, onKmToChange,
  powerFrom, powerTo, onPowerFromChange, onPowerToChange,
  fuel, onFuelChange,
  gearbox, onGearboxChange,
  drive, onDriveChange,
}) {
  const [priceEnabled, setPriceEnabled] = useState(false);
  const [yearEnabled, setYearEnabled] = useState(false);
  const [kmEnabled, setKmEnabled] = useState(false);
  const [powerEnabled, setPowerEnabled] = useState(false);

  const fmtPrice = (v) => Number(v).toLocaleString("cs-CZ") + " Kč";
  const fmtKm = (v) => Number(v).toLocaleString("cs-CZ") + " km";
  const fmtKw = (v) => v + " kW";

  function handlePriceFrom(v) { if (priceEnabled) onPriceFromChange(v); }
  function handlePriceTo(v) { if (priceEnabled) onPriceToChange(v); }
  function handleYearFrom(v) { if (yearEnabled) onYearFromChange(v); }
  function handleYearTo(v) { if (yearEnabled) onYearToChange(v); }
  function handleKmFrom(v) { if (kmEnabled) onKmFromChange(v); }
  function handleKmTo(v) { if (kmEnabled) onKmToChange(v); }
  function handlePowerFrom(v) { if (powerEnabled) onPowerFromChange(v); }
  function handlePowerTo(v) { if (powerEnabled) onPowerToChange(v); }

  return (
    <div className="quick-filters-full">
      <div className="quick-pairs-row">
        <PairSlider
          label="Cena (Kč)"
          enabled={priceEnabled}
          onToggleEnabled={(v) => { setPriceEnabled(v); if (!v) { onPriceFromChange(""); onPriceToChange(""); } }}
          valueA={Number(priceFrom) || 0} valueB={Number(priceTo) || 0}
          onChangeA={handlePriceFrom} onChangeB={handlePriceTo}
          min={0} max={5000000} step={50000} fmt={fmtPrice}
        />
        <PairSlider
          label="Rok"
          enabled={yearEnabled}
          onToggleEnabled={(v) => { setYearEnabled(v); if (!v) { onYearFromChange(""); onYearToChange(""); } }}
          valueA={Number(yearFrom) || 1950} valueB={Number(yearTo) || 2026}
          onChangeA={handleYearFrom} onChangeB={handleYearTo}
          min={1950} max={2026} step={1}
        />
        <PairSlider
          label="Nájezd (km)"
          enabled={kmEnabled}
          onToggleEnabled={(v) => { setKmEnabled(v); if (!v) { onKmFromChange(""); onKmToChange(""); } }}
          valueA={Number(kmFrom) || 0} valueB={Number(kmTo) || 0}
          onChangeA={handleKmFrom} onChangeB={handleKmTo}
          min={0} max={500000} step={10000} fmt={fmtKm}
        />
        <PairSlider
          label="Výkon (kW)"
          enabled={powerEnabled}
          onToggleEnabled={(v) => { setPowerEnabled(v); if (!v) { onPowerFromChange(""); onPowerToChange(""); } }}
          valueA={Number(powerFrom) || 0} valueB={Number(powerTo) || 0}
          onChangeA={handlePowerFrom} onChangeB={handlePowerTo}
          min={0} max={500} step={10} fmt={fmtKw}
        />
      </div>
      <div className="quick-selects-row">
        <div className="field quick-select">
          <label>Palivo</label>
          <select value={fuel} onChange={(e) => onFuelChange(e.target.value)}>
            <option value="">— jakékoliv —</option>
            <option value="benzin">Benzín</option>
            <option value="nafta">Nafta</option>
            <option value="lpg-benzin">LPG</option>
            <option value="hybrid">Hybrid</option>
            <option value="elektro">Elektro</option>
          </select>
        </div>
        <div className="field quick-select">
          <label>Převodovka</label>
          <select value={gearbox} onChange={(e) => onGearboxChange(e.target.value)}>
            <option value="">— jakákoliv —</option>
            <option value="manual">Manuál</option>
            <option value="automatic">Automat</option>
          </select>
        </div>
        <div className="field quick-select">
          <label>Pohon</label>
          <select value={drive} onChange={(e) => onDriveChange(e.target.value)}>
            <option value="">— jakýkoliv —</option>
            <option value="fwd">Přední (FWD)</option>
            <option value="rwd">Zadní (RWD)</option>
            <option value="awd">4×4 (AWD)</option>
          </select>
        </div>
      </div>
    </div>
  );
}