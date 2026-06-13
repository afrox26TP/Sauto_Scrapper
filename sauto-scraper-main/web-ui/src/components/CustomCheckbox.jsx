import React, { useMemo } from "react";

/**
 * Custom checkbox with animated check mark.
 * @param {boolean} checked
 * @param {(e: React.ChangeEvent<HTMLInputElement>) => void} onChange
 * @param {string} [id]
 * @param {string} [label] - optional label, renders as clickable inline text
 * @param {boolean} [disabled]
 * @param {string} [className]
 * @param {'md'|'sm'|'lg'} [size]
 */
export default function CustomCheckbox({
  checked,
  onChange,
  id,
  label,
  disabled = false,
  className = "",
  size = "md",
}) {
  const _id = useMemo(() => id || `ccb-${Math.random().toString(36).slice(2, 8)}`, [id]);
  return (
    <label
      className={`custom-checkbox-wrapper custom-checkbox-${size} ${disabled ? "is-disabled" : ""} ${className}`}
      htmlFor={_id}
    >
      <input
        id={_id}
        type="checkbox"
        className="custom-checkbox-input"
        checked={checked}
        onChange={onChange}
        disabled={disabled}
      />
      <span className="custom-checkbox-box" aria-hidden="true">
        <span className="custom-checkbox-check" />
      </span>
      {label !== undefined && (
        <span className="custom-checkbox-label">{label}</span>
      )}
    </label>
  );
}