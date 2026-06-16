import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";

/**
 * Custom range slider with animated track fill, value tooltip and pip support.
 * Uses local state during drag to avoid parent re-renders on every mousemove.
 * Commits the final value via onChange only on mouseup / touchend.
 *
 * Uses refs for onChange/formatValue callbacks so input handlers stay stable
 * even when parent creates new function identities on every render.
 *
 * @param {number} value - controlled value (from parent)
 * @param {(val: number) => void} onChange - called with final numeric value
 * @param {number} min
 * @param {number} max
 * @param {number} step
 * @param {string} [label]
 * @param {(val: number) => string} [formatValue]
 * @param {boolean} [disabled]
 * @param {string} [className]
 * @param {'md'|'sm'} [size]
 * @param {number[]} [pips]
 */
export default function CustomSlider({
  value,
  onChange,
  min,
  max,
  step = 1,
  label,
  formatValue,
  disabled = false,
  className = "",
  size = "md",
  pips,
}) {
  const inputRef = useRef(null);
  const isDragging = useRef(false);
  const onChangeRef = useRef(onChange);
  const formatValueRef = useRef(formatValue);

  // Always keep refs in sync with latest props (no re-render needed)
  useEffect(() => {
    onChangeRef.current = onChange;
    formatValueRef.current = formatValue;
  });

  // local value that updates instantly during drag
  const [localValue, setLocalValue] = useState(() => {
    const v = Number(value);
    return Number.isFinite(v) ? v : Number(min);
  });

  const safeMin = Number(min);
  const safeMax = Number(max);

  // sync from parent when not dragging
  useEffect(() => {
    if (isDragging.current) return;
    const v = Number(value);
    setLocalValue(Number.isFinite(v) ? v : safeMin);
  }, [value, safeMin]);

  const pct =
    safeMax !== safeMin
      ? ((localValue - safeMin) / (safeMax - safeMin)) * 100
      : 0;

  // Memoize display text so it doesn't recreate formatValue call on every render
  const displayText = useMemo(() => {
    const fmt = formatValueRef.current;
    return fmt ? fmt(localValue) : localValue.toLocaleString("cs-CZ");
  }, [localValue]);

  // commit reads latest onChange from ref – stable identity forever
  const commit = useCallback((val) => {
    onChangeRef.current(Number(val));
  }, []);

  const handleInput = useCallback((e) => {
    setLocalValue(Number(e.target.value));
  }, []);

  const handlePointerDown = useCallback(() => {
    isDragging.current = true;
    const finish = () => {
      if (!isDragging.current) return;
      isDragging.current = false;
      if (inputRef.current) {
        commit(inputRef.current.value);
      }
      window.removeEventListener("mouseup", finish);
      window.removeEventListener("touchend", finish);
    };
    window.addEventListener("mouseup", finish, { once: true });
    window.addEventListener("touchend", finish, { once: true });
  }, [commit]);

  // Also commit on change event (for keyboard/accessibility)
  const handleChange = useCallback(
    (e) => {
      const val = Number(e.target.value);
      setLocalValue(val);
      // If not dragging (keyboard), commit immediately
      if (!isDragging.current) {
        commit(val);
      }
    },
    [commit],
  );

  return (
    <div className={`custom-slider-field ${className}`}>
      {label && <span className="custom-slider-label">{label}</span>}
      <div className={`custom-slider-wrap custom-slider-${size}`}>
        <div className="custom-slider-track-wrap">
          <div className="custom-slider-rail" />
          <div
            className="custom-slider-fill"
            style={{ width: `${Math.max(0, Math.min(100, pct))}%` }}
          />
          {pips && pips.length > 0 && (
            <div className="custom-slider-pips">
              {pips.map((pip) => {
                const pipPct =
                  safeMax !== safeMin
                    ? ((pip - safeMin) / (safeMax - safeMin)) * 100
                    : 0;
                return (
                  <span
                    key={pip}
                    className="custom-slider-pip"
                    style={{ left: `${pipPct}%` }}
                  />
                );
              })}
            </div>
          )}
          <input
            ref={inputRef}
            type="range"
            className="custom-slider-input"
            min={safeMin}
            max={safeMax}
            step={step}
            value={localValue}
            onInput={handleInput}
            onChange={handleChange}
            onMouseDown={handlePointerDown}
            onTouchStart={handlePointerDown}
            disabled={disabled}
            aria-label={label || undefined}
          />
        </div>
        <span className="custom-slider-value">{displayText}</span>
      </div>
    </div>
  );
}