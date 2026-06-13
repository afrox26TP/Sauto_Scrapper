import React, { useCallback, useId, useMemo } from "react";

/**
 * Custom toggle / switch component.
 * Toggle track + optional label side by side, no overlap.
 * @param {boolean} checked
 * @param {(checked: boolean) => void} onChange
 * @param {string} [id]
 * @param {string} [label] - optional label text beside the toggle
 * @param {boolean} [disabled]
 * @param {string} [className]
 * @param {'md'|'sm'|'lg'} [size]
 */
export default function CustomToggle({
  checked,
  onChange,
  id,
  label,
  disabled = false,
  className = "",
  size = "md",
}) {
  const reactId = useId();
  const _id = useMemo(() => id || `ctg-${reactId}`, [id, reactId]);

  const handleChange = useCallback(
    (e) => onChange(e.target.checked),
    [onChange],
  );

  return (
    <div
      className={`custom-toggle-wrapper custom-toggle-${size} ${checked ? "is-checked" : ""} ${disabled ? "is-disabled" : ""} ${className}`}
    >
      <input
        id={_id}
        type="checkbox"
        className="custom-toggle-input-native"
        checked={checked}
        onChange={handleChange}
        disabled={disabled}
      />
      <label htmlFor={_id} className="custom-toggle-visual">
        <span className="custom-toggle-knob" />
      </label>
      {label !== undefined && (
        <label htmlFor={_id} className="custom-toggle-label">
          {label}
        </label>
      )}
    </div>
  );
}