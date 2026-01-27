from __future__ import annotations

from pathlib import Path
import logging
from typing import Iterable, List, Union, Tuple

import pandas as pd

logger = logging.getLogger(__name__)


def _log_column_warnings(df: pd.DataFrame) -> None:
    """Log potential data-quality issues.

    This is intentionally conservative: we only warn when the column set strongly
    suggests that later calculations could be wrong (e.g. max/min without avg).
    """
    try:
        cols = [str(c).lower() for c in df.columns]
    except Exception:
        return

    def _has_any(subs):
        return any(any(s in c for s in subs) for c in cols)

    # Statistic-triplets without avg are common and can break totals if someone
    # accidentally sums max/min/avg columns.
    checks = [
        ("current", ["max_current", "min_current"], ["avg_current"]),
        ("voltage", ["max_voltage", "min_voltage"], ["avg_voltage"]),
        ("power", ["max_power", "min_power"], ["avg_power", "avg_act_power", "avg_active_power"]),
    ]
    for name, bad, good in checks:
        if _has_any(bad) and not _has_any(good):
            logger.warning(
                "CSV columns contain %s max/min but no avg_* columns. Totals may be misleading if the export is statistical.", name
            )

    # Very common confusion: columns for phases exist but are all zeros for L1/L3.
    # We do not warn here (it can be normal), but we do log when *no* usable power
    # columns are present.
    if not _has_any(["power", "act_power", "active_power", "apower", "watts", "w"]):
        logger.warning("CSV seems to contain no power columns (power/active_power). Plots may be empty.")

# Accept multiple timestamp column names for backwards compatibility.
TS_CANDIDATES = [
    "timestamp",  # preferred
    "ts",         # common legacy
    "time",
    "datetime",
    "date",
]

POWER_CANDIDATES = [
    "a_act_power",
    "b_act_power",
    "c_act_power",
    "act_power_a",
    "act_power_b",
    "act_power_c",
]


def _parse_timestamp_col(series: pd.Series) -> pd.Series:
    """Parse a timestamp column robustly.

    Shelly exports may contain:
      - unix seconds (10 digits)
      - unix milliseconds (13 digits)
      - ISO strings
    """
    ser = series
    try:
        # If it's numeric-ish, detect scale by magnitude.
        nums = pd.to_numeric(ser, errors="coerce")
        med = float(nums.dropna().median()) if nums.notna().any() else 0.0
        if med > 1e15:
            s = pd.to_datetime(nums, errors="coerce", unit="ns")
        elif med > 1e12:
            s = pd.to_datetime(nums, errors="coerce", unit="ms")
        else:
            s = pd.to_datetime(nums, errors="coerce", unit="s")
        # If numeric parse failed badly, fall back to generic parse
        if s.isna().mean() > 0.5:
            s = pd.to_datetime(ser, errors="coerce", utc=False)
        return s
    except Exception:
        s = pd.to_datetime(ser, errors="coerce", utc=False)
        return s


def _read_one_csv(path: Path) -> Tuple[pd.DataFrame, str]:
    """Read one CSV with a few robustness tricks.

    Returns (df, used_sep) or raises.
    """
    # Try comma first (fast path)
    try:
        df = pd.read_csv(path)
        return df, ","
    except Exception:
        pass

    # Fallback: delimiter sniff (handles ';' often used in EU exports)
    df = pd.read_csv(path, sep=None, engine="python")
    return df, "auto"


def read_csv_files(paths: Iterable[Union[str, Path]]) -> pd.DataFrame:
    """Read and concat multiple CSV files.

    Important behavior: one bad/foreign CSV must NOT break the whole dataset.
    We skip unreadable files and only fail if *none* were usable.
    """
    frames: List[pd.DataFrame] = []
    errors: List[str] = []
    for p in paths:
        p = Path(p)
        if not p.exists():
            continue
        try:
            df, _sep = _read_one_csv(p)

            # Backwards compatibility: accept legacy timestamp columns.
            def _find_ts_col(_df: pd.DataFrame) -> str | None:
                for c in TS_CANDIDATES:
                    if c in _df.columns:
                        return c
                return None

            ts_col = _find_ts_col(df)
            if ts_col is None:
                # Common issue: file is ';' separated -> pandas reads 1 big column.
                # Try ';' explicitly before skipping.
                try:
                    df2 = pd.read_csv(p, sep=';')
                    ts_col = _find_ts_col(df2)
                    if ts_col is not None:
                        df = df2
                except Exception:
                    pass

            if ts_col is None:
                # Some stray CSVs (e.g. phases-only) may not have timestamp; skip them.
                raise ValueError(f"missing timestamp (tried {', '.join(TS_CANDIDATES)})")

            df = df.copy()
            if ts_col != "timestamp":
                df = df.rename(columns={ts_col: "timestamp"})
            df["timestamp"] = _parse_timestamp_col(df["timestamp"])
            frames.append(df)
        except Exception as e:
            errors.append(f"{p.name}: {e}")
            continue

    if not frames:
        raise ValueError("No usable CSV files loaded. " + ("; ".join(errors[:5]) if errors else ""))

    out = pd.concat(frames, ignore_index=True)
    out = out.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)

    # Deduplicate: incremental downloads overlap; keep last occurrence
    out = out.drop_duplicates(subset=["timestamp"], keep="last").reset_index(drop=True)
    # Diagnostics for unusual column sets (written to app log)
    try:
        _log_column_warnings(out)
    except Exception:
        pass
    return out


def detect_power_columns(df: pd.DataFrame) -> List[str]:
    cols = [c for c in POWER_CANDIDATES if c in df.columns]
    if cols:
        return cols
    return [
        c
        for c in df.columns
        if "power" in c.lower() and pd.api.types.is_numeric_dtype(df[c])
    ]
