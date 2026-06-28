import React, { useState, useCallback, useEffect, useRef } from "react";
import { Play, LoaderCircle, ChevronDown, ChevronUp } from "lucide-react";
import ProjectNameInput from "./ProjectNameInput";
import BrandSelector from "./BrandSelector";
import ModelSelector from "./ModelSelector";
import BodySelector from "./BodySelector";
import EquipmentSelector from "./EquipmentSelector";
import QuickFilterPanel from "./QuickFilterPanel";
import { CustomSlider, CustomToggle } from "./index";
import {
  BASIC_GROUPS,
  ADVANCED_GROUPS,
  IGNORED_KEYS,
  fmtVal,
  csvToArray,
  uniq,
} from "../utils/scoring";

const Field = React.memo(function Field({ def, value, onChange }) {
  const { key, type, label, min, max, step, options, fmt } = def;
  const raw = value ?? "";

  const formatValue = React.useMemo(() => {
    if (!fmt) return undefined;
    return (v) => fmtVal(v, fmt);
  }, [fmt]);

  if (type === "slider") {
    const num = parseFloat(raw);
    const safe = isNaN(num) ? min || 0 : num;
    return (
      <CustomSlider
        label={label}
        value={safe}
        min={min || 0}
        max={max || 100}
        step={step || 1}
        formatValue={formatValue}
        onChange={(val) => onChange(key, String(val))}
      />
    );
  }

  if (type === "boolean") {
    const checked = raw === "true" || raw === true;
    return (
      <div className="field">
        <CustomToggle
          label={label}
          checked={checked}
          onChange={(val) => onChange(key, String(val))}
          id={key}
        />
      </div>
    );
  }

  if (type === "select") {
    return (
      <div className="field">
        <label>{label}</label>
        <select value={raw} onChange={(e) => onChange(key, e.target.value)}>
          {(options || []).map((opt) => (
            <option key={opt} value={opt}>{opt || "— jakýkoliv —"}</option>
          ))}
        </select>
      </div>
    );
  }

  if (type === "number") {
    return (
      <div className="field">
        <label>{label}</label>
        <input type="number" value={raw} onChange={(e) => onChange(key, e.target.value)} />
      </div>
    );
  }

  return (
    <div className="field">
      <label>{label}</label>
      <input type="text" value={raw} onChange={(e) => onChange(key, e.target.value)} />
    </div>
  );
});

export default function ProjectSetup({
  project,
  brandOptions,
  bodyOptions,
  equipmentOptions,
  modelsByBrand,
  loadingModelsByBrand,
  modelLoadErrorsByBrand,
  onUpdateConfig,
  onUpdateProject,
  onRun,
  isRunning,
}) {
  const config = project.config || {};
  const busy = isRunning || project.phase === "running" || project.phase === "queued";
  const [showAdvanced, setShowAdvanced] = useState(false);
  const [isScrolled, setIsScrolled] = useState(false);
  const [brandFilterText, setBrandFilterText] = useState("");
  const [modelFilterText, setModelFilterText] = useState("");
  const [bodyFilterText, setBodyFilterText] = useState("");
  const [equipmentFilterText, setEquipmentFilterText] = useState("");

  const selectedBrands = React.useMemo(() => uniq(csvToArray(config.manufacturer_seo_name)), [config.manufacturer_seo_name]);
  const selectedModels = React.useMemo(() => uniq(csvToArray(config.model_seo_name)), [config.model_seo_name]);
  const excludedBrands = React.useMemo(() => uniq(csvToArray(config.exclude_manufacturer_seo_name)), [config.exclude_manufacturer_seo_name]);
  const excludedModels = React.useMemo(() => uniq(csvToArray(config.exclude_model_seo_name)), [config.exclude_model_seo_name]);
  const selectedBodies = React.useMemo(() => uniq(csvToArray(config.body_seo)), [config.body_seo]);
  const excludedBodies = React.useMemo(() => uniq(csvToArray(config.exclude_body_seo)), [config.exclude_body_seo]);
  const selectedEquipment = React.useMemo(() => uniq(csvToArray(config.equipment_include)), [config.equipment_include]);
  const excludedEquipment = React.useMemo(() => uniq(csvToArray(config.equipment_exclude)), [config.equipment_exclude]);

  const toggleBrand = useCallback((brand) => {
    const b = String(brand || "").trim();
    if (!b) return;
    let nextBrands = [...selectedBrands], nextExcluded = [...excludedBrands];
    if (excludedBrands.includes(b)) { nextExcluded = nextExcluded.filter((x) => x !== b); }
    else if (selectedBrands.includes(b)) { nextBrands = nextBrands.filter((x) => x !== b); nextExcluded = [...nextExcluded, b]; }
    else { nextBrands = [...nextBrands, b]; }
    const allowedModels = new Set(nextBrands.flatMap((k) => (modelsByBrand[k] || []).map((m) => m.value)));
    const nextModels = selectedModels.filter((m) => allowedModels.has(m));
    const nextExclModels = excludedModels.filter((m) => allowedModels.has(m));
    onUpdateConfig({
      manufacturer_seo_name: nextBrands.join(","),
      model_seo_name: nextModels.join(","),
      exclude_manufacturer_seo_name: nextExcluded.join(","),
      exclude_model_seo_name: nextExclModels.join(","),
    });
  }, [selectedBrands, excludedBrands, selectedModels, excludedModels, modelsByBrand, onUpdateConfig]);

  const toggleModel = useCallback((model) => {
    const m = String(model || "").trim();
    if (!m) return;
    let nextModels = [...selectedModels], nextExcluded = [...excludedModels];
    if (excludedModels.includes(m)) { nextExcluded = nextExcluded.filter((x) => x !== m); }
    else if (selectedModels.includes(m)) { nextModels = nextModels.filter((x) => x !== m); nextExcluded = [...nextExcluded, m]; }
    else { nextModels = [...nextModels, m]; }
    onUpdateConfig({ model_seo_name: nextModels.join(","), exclude_model_seo_name: nextExcluded.join(",") });
  }, [selectedModels, excludedModels, onUpdateConfig]);

  const toggleBody = useCallback((body) => {
    const b = String(body || "").trim();
    if (!b) return;
    let nextSel = [...selectedBodies], nextExcl = [...excludedBodies];
    if (excludedBodies.includes(b)) { nextExcl = nextExcl.filter((x) => x !== b); }
    else if (selectedBodies.includes(b)) { nextSel = nextSel.filter((x) => x !== b); nextExcl = [...nextExcl, b]; }
    else { nextSel = [...nextSel, b]; }
    onUpdateConfig({ body_seo: nextSel.join(","), exclude_body_seo: nextExcl.join(",") });
  }, [selectedBodies, excludedBodies, onUpdateConfig]);

  const toggleEquipment = useCallback((equip) => {
    const e = String(equip || "").trim();
    if (!e) return;
    let nextSel = [...selectedEquipment], nextExcl = [...excludedEquipment];
    if (excludedEquipment.includes(e)) { nextExcl = nextExcl.filter((x) => x !== e); }
    else if (selectedEquipment.includes(e)) { nextSel = nextSel.filter((x) => x !== e); nextExcl = [...nextExcl, e]; }
    else { nextSel = [...nextSel, e]; }
    onUpdateConfig({ equipment_include: nextSel.join(","), equipment_exclude: nextExcl.join(",") });
  }, [selectedEquipment, excludedEquipment, onUpdateConfig]);

  const scrollContainerRef = useRef(null);

  useEffect(() => {
    const el = scrollContainerRef.current?.closest('.main-content') || document.querySelector('.main-content');
    if (!el) return;
    const handleScroll = () => {
      setIsScrolled(el.scrollTop > 100);
    };
    el.addEventListener('scroll', handleScroll);
    return () => el.removeEventListener('scroll', handleScroll);
  }, []);

  const setConfigParam = useCallback((key, val) => { onUpdateConfig({ [key]: val }); }, [onUpdateConfig]);
  const extraKeys = Object.keys(config).filter((k) => !IGNORED_KEYS.has(k));

  return (
<div className="project-setup">
      <div className={`project-header-sticky ${isScrolled ? 'scrolled' : ''}`}>
        <button className="btn-primary btn-run" onClick={() => onRun(project.id)} disabled={busy}>
          {busy ? <><LoaderCircle className="ui-icon icon-spin" /> Pracuji…</> : <><Play className="ui-icon" /> Spustit scraper</>}
        </button>
        <ProjectNameInput
          name={project.name}
          customName={project.customName}
          onNameChange={(val) => onUpdateProject({ name: val, customName: true })}
          onToggleCustom={() => onUpdateProject({ customName: !project.customName })}
        />
      </div>

      {/* Brand / Model / Body / Equipment – první řádek vedle sebe */}
      <div className="catalog-multi-wrap catalog-horizontal">
        <BrandSelector brands={brandOptions} selected={selectedBrands} excluded={excludedBrands} onToggle={toggleBrand} filterText={brandFilterText} onFilterChange={setBrandFilterText} />
        <ModelSelector selectedBrands={selectedBrands} modelsByBrand={modelsByBrand} loadingModelsByBrand={loadingModelsByBrand} modelLoadErrorsByBrand={modelLoadErrorsByBrand} selected={selectedModels} excluded={excludedModels} onToggle={toggleModel} filterText={modelFilterText} onFilterChange={setModelFilterText} />
        <BodySelector bodies={bodyOptions} selected={selectedBodies} excluded={excludedBodies} onToggle={toggleBody} filterText={bodyFilterText} onFilterChange={setBodyFilterText} />
        <EquipmentSelector equipment={equipmentOptions} selected={selectedEquipment} excluded={excludedEquipment} onToggle={toggleEquipment} filterText={equipmentFilterText} onFilterChange={setEquipmentFilterText} />
        <div className="catalog-selected-note catalog-selected-full">
          {selectedBrands.length > 0 && <>{selectedBrands.length} {selectedBrands.length === 1 ? "značka" : "značek"}</>}
          {selectedBrands.length === 0 && "0 značek"}
          {excludedBrands.length > 0 && ` + ${excludedBrands.length} vyloučeno`}
          {selectedModels.length > 0 && ` · ${selectedModels.length} modelů`}
          {excludedModels.length > 0 && ` + ${excludedModels.length} vyloučeno`}
        </div>
      </div>

      {/* Quick filter row – párové slidery */}
      <div className="param-group card-section">
        <div className="group-label">Rychlé filtry</div>
        <QuickFilterPanel
          priceFrom={config.price_from || ""} priceTo={config.price_to || ""}
          onPriceFromChange={(v) => setConfigParam("price_from", v)} onPriceToChange={(v) => setConfigParam("price_to", v)}
          yearFrom={config.year_from || ""} yearTo={config.year_to || ""}
          onYearFromChange={(v) => setConfigParam("year_from", v)} onYearToChange={(v) => setConfigParam("year_to", v)}
          kmFrom={config.tachometer_from || ""} kmTo={config.tachometer_to || ""}
          onKmFromChange={(v) => setConfigParam("tachometer_from", v)} onKmToChange={(v) => setConfigParam("tachometer_to", v)}
          powerFrom={config.power_from || ""} powerTo={config.power_to || ""}
          onPowerFromChange={(v) => setConfigParam("power_from", v)} onPowerToChange={(v) => setConfigParam("power_to", v)}
          fuel={config.fuel_seo || ""} onFuelChange={(v) => setConfigParam("fuel_seo", v)}
          gearbox={config.gearbox_filter || ""} onGearboxChange={(v) => setConfigParam("gearbox_filter", v)}
          drive={config.drive_filter || ""} onDriveChange={(v) => setConfigParam("drive_filter", v)}
        />
      </div>

      {/* Hledání – horizontal */}
      <div className="param-group card-section">
        <div className="group-label">{BASIC_GROUPS[0].label}</div>
        <div className="horizontal-fields-row">
          <div className="horizontal-field">
            <Field def={BASIC_GROUPS[0].fields[0]} value={config.seller_type} onChange={setConfigParam} />
          </div>
          <div className="horizontal-field">
            <div className="field">
              <label>Stav</label>
              <div className="condition-chips">
                {["nove:Nové", "ojete:Ojeté", "predvadeci:Předváděcí", "poskozeny:Poškozený"].map((entry) => {
                  const [val, lbl] = entry.split(":");
                  const condArr = csvToArray(config.condition_seo);
                  const isSelected = condArr.includes(val);
                  const isExcluded = csvToArray(config.exclude_condition_seo).includes(val);
                  return (
                    <span key={val} className={`condition-chip${isSelected ? " checked" : isExcluded ? " excluded" : ""}`}
                      onClick={() => {
                        let next = [...condArr];
                        let nextExcl = [...csvToArray(config.exclude_condition_seo)];
                        if (isExcluded) { nextExcl = nextExcl.filter((x) => x !== val); }
                        else if (isSelected) { next = next.filter((x) => x !== val); nextExcl = [...nextExcl, val]; }
                        else { next = [...next, val]; }
                        setConfigParam("condition_seo", next.join(","));
                        setConfigParam("exclude_condition_seo", nextExcl.join(","));
                      }}
                      title={isSelected ? "✓ Zahrnuto — klikni pro vyloučení" : isExcluded ? "✕ Vyloučeno — klikni pro zrušení" : "Klikni pro zahrnutí"}
                    >{isSelected ? "✓" : isExcluded ? "✕" : ""} {lbl}</span>
                  );
                })}
              </div>
            </div>
          </div>
          <div className="horizontal-field">
            <Field def={BASIC_GROUPS[0].fields[2]} value={config.operating_lease} onChange={setConfigParam} />
          </div>
        </div>
      </div>

      {/* Advanced toggle */}
      <div className="advanced-toggle-row">
        <button className="link-btn" onClick={() => setShowAdvanced((v) => !v)}>
          {showAdvanced ? <><ChevronUp className="ui-icon" /> Skrýt pokročilé</> : <><ChevronDown className="ui-icon" /> Pokročilé filtry</>}
        </button>
        <span className="muted">{ADVANCED_GROUPS.length} sekcí</span>
      </div>

      {showAdvanced && (
        <div className="advanced-panel">
          {ADVANCED_GROUPS.map((group) => (
            <div key={group.label} className="param-group card-section advanced-card">
              <div className="group-label">{group.label}</div>
              {group.fields.map((def) => (
                <Field key={def.key} def={def} value={config[def.key]} onChange={setConfigParam} />
              ))}
            </div>
          ))}
          {extraKeys.length > 0 && (
            <div className="param-group card-section advanced-card">
              <div className="group-label">Ostatní</div>
              {extraKeys.map((k) => (
                <div key={k} className="field">
                  <label>{k}</label>
                  <input type="text" value={config[k] ?? ""} onChange={(e) => setConfigParam(k, e.target.value)} />
                </div>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}