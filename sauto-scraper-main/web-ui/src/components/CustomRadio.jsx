import React, { useMemo } from "react";

/**
 * Custom radio button with animated dot indicator.
 * @param {boolean} checked
 * @param {() => void} onChange - called when selected
 * @param {string} [id]
 * @param {string} [label] - optional label text
 * @param {string} name - radio group name
 * @param {string} value - value for this radio
 * @param {boolean} [disabled]
 * @param {string} [className]
 * @param {'md'|'sm'|'lg'} [size]
 */
export default function CustomRadio({
  checked,
  onChange,
  id,
  label,
  name,
  value,
  disabled = false,
  className = "",
  size = "md",
}) {
  const _id = useMemo(() => id || `crd-${Math.random().toString(36).slice(2, 8)}`, [id]);
  return (
    <label
      className={`custom-radio-wrapper custom-radio-${size} ${checked ? "is-checked" : ""} ${disabled ? "is-disabled" : ""} ${className}`}
      htmlFor={_id}
    >
      <input
        id={_id}
        type="radio"
        className="custom-radio-input"
        name={name}
        value={value}
        checked={checked}
        onChange={onChange}
        disabled={disabled}
      />
      <span className="custom-radio-circle" aria-hidden="true">
        <span className="custom-radio-dot" />
      </span>
      {label !== undefined && (
        <span className="custom-radio-label">{label}</span>
      )}
    </label>
  );
}