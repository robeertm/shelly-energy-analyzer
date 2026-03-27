"""SQLite-based energy data storage (v6.0.0+).

Replaces CSV chunk files with a single indexed database for fast range queries.
Uses WAL mode for concurrent reads (web dashboard + UI) during writes (sync).
"""
from __future__ import annotations

import csv
import datetime
import io
import logging
import sqlite3
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)

# Maximum plausible gap between samples (seconds).  If the time delta exceeds
# this, we treat the gap as missing data and assign zero energy rather than
# integrating power over an unrealistic span (e.g. device offline for days).
_MAX_DELTA_S = 600  # 10 minutes

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS samples (
    device_key  TEXT    NOT NULL,
    timestamp   INTEGER NOT NULL,
    -- Active power (W) per phase
    a_act_power REAL, b_act_power REAL, c_act_power REAL,
    -- Voltage (V) per phase: instantaneous / single-value columns
    a_voltage   REAL, b_voltage   REAL, c_voltage   REAL,
    -- Current (A) per phase: instantaneous / single-value columns
    a_current   REAL, b_current   REAL, c_current   REAL,
    -- Interval active energy (Wh) per phase
    a_total_act_energy REAL, b_total_act_energy REAL, c_total_act_energy REAL,
    -- Active power min/max per phase
    a_min_act_power REAL, a_max_act_power REAL,
    b_min_act_power REAL, b_max_act_power REAL,
    c_min_act_power REAL, c_max_act_power REAL,
    -- Computed totals
    total_power REAL,
    energy_kwh  REAL,
    PRIMARY KEY (device_key, timestamp)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS hourly_energy (
    device_key   TEXT    NOT NULL,
    hour_ts      INTEGER NOT NULL,
    kwh          REAL    NOT NULL DEFAULT 0,
    avg_power_w  REAL,
    max_power_w  REAL,
    sample_count INTEGER DEFAULT 0,
    PRIMARY KEY (device_key, hour_ts)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS monthly_energy (
    device_key   TEXT    NOT NULL,
    month_ts     INTEGER NOT NULL,   -- Unix seconds: 1st of month 00:00 UTC
    kwh          REAL    NOT NULL DEFAULT 0,
    avg_power_w  REAL,
    max_power_w  REAL,
    min_power_w  REAL,
    -- Per-phase voltage averages
    avg_a_voltage REAL, avg_b_voltage REAL, avg_c_voltage REAL,
    min_a_voltage REAL, min_b_voltage REAL, min_c_voltage REAL,
    max_a_voltage REAL, max_b_voltage REAL, max_c_voltage REAL,
    -- Per-phase current averages
    avg_a_current REAL, avg_b_current REAL, avg_c_current REAL,
    max_a_current REAL, max_b_current REAL, max_c_current REAL,
    -- Neutral current
    avg_n_current REAL, max_n_current REAL,
    -- Grid frequency
    avg_freq_hz  REAL,
    sample_count INTEGER DEFAULT 0,
    PRIMARY KEY (device_key, month_ts)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS device_meta (
    device_key  TEXT PRIMARY KEY,
    last_end_ts INTEGER,
    updated_at  INTEGER
);

CREATE TABLE IF NOT EXISTS co2_intensity (
    hour_ts            INTEGER PRIMARY KEY,  -- Unix seconds: start of hour UTC
    zone               TEXT    NOT NULL,
    intensity_g_per_kwh REAL   NOT NULL,
    source             TEXT,                 -- e.g. "entsoe" or "static"
    fetched_at         INTEGER               -- Unix seconds when this row was written
);
"""

# Additional columns added in v6.0.0.2 for full Shelly EMData CSV support.
# ALTER TABLE ADD COLUMN is used for backward compatibility (existing DBs).
_EXTRA_COLUMNS: Tuple[Tuple[str, str], ...] = (
    # Fundamental / return active energy per phase
    ("a_fund_act_energy", "REAL"), ("b_fund_act_energy", "REAL"), ("c_fund_act_energy", "REAL"),
    ("a_total_act_ret_energy", "REAL"), ("b_total_act_ret_energy", "REAL"), ("c_total_act_ret_energy", "REAL"),
    ("a_fund_act_ret_energy", "REAL"), ("b_fund_act_ret_energy", "REAL"), ("c_fund_act_ret_energy", "REAL"),
    # Reactive energy per phase (needed for VAR sign)
    ("a_lag_react_energy", "REAL"), ("b_lag_react_energy", "REAL"), ("c_lag_react_energy", "REAL"),
    ("a_lead_react_energy", "REAL"), ("b_lead_react_energy", "REAL"), ("c_lead_react_energy", "REAL"),
    # Apparent power min/max per phase (needed for VAR / cos phi)
    ("a_max_aprt_power", "REAL"), ("a_min_aprt_power", "REAL"),
    ("b_max_aprt_power", "REAL"), ("b_min_aprt_power", "REAL"),
    ("c_max_aprt_power", "REAL"), ("c_min_aprt_power", "REAL"),
    # Voltage min/max/avg per phase (Shelly EMData format)
    ("a_max_voltage", "REAL"), ("a_min_voltage", "REAL"), ("a_avg_voltage", "REAL"),
    ("b_max_voltage", "REAL"), ("b_min_voltage", "REAL"), ("b_avg_voltage", "REAL"),
    ("c_max_voltage", "REAL"), ("c_min_voltage", "REAL"), ("c_avg_voltage", "REAL"),
    # Current min/max/avg per phase (Shelly EMData format)
    ("a_max_current", "REAL"), ("a_min_current", "REAL"), ("a_avg_current", "REAL"),
    ("b_max_current", "REAL"), ("b_min_current", "REAL"), ("b_avg_current", "REAL"),
    ("c_max_current", "REAL"), ("c_min_current", "REAL"), ("c_avg_current", "REAL"),
    # Neutral current
    ("n_max_current", "REAL"), ("n_min_current", "REAL"), ("n_avg_current", "REAL"),
    # Grid frequency (Hz) – average of all three phases
    ("freq_hz", "REAL"),
)

# All known sample columns in canonical order (base + extra).
SAMPLE_COLUMNS: Tuple[str, ...] = (
    "device_key", "timestamp",
    "a_act_power", "b_act_power", "c_act_power",
    "a_voltage", "b_voltage", "c_voltage",
    "a_current", "b_current", "c_current",
    "a_total_act_energy", "b_total_act_energy", "c_total_act_energy",
    "a_min_act_power", "a_max_act_power",
    "b_min_act_power", "b_max_act_power",
    "c_min_act_power", "c_max_act_power",
    "total_power", "energy_kwh",
) + tuple(col for col, _ in _EXTRA_COLUMNS)

# Timestamp column name variations (same as csv_read.TS_CANDIDATES).
_TS_NAMES = {"timestamp", "ts", "time", "datetime", "date"}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _safe_float(v) -> Optional[float]:
    """Convert to float or None."""
    if v is None or v == "":
        return None
    try:
        f = float(v)
        return f
    except (ValueError, TypeError):
        return None


def _detect_ts_magnitude(median_val: float) -> str:
    """Detect unix timestamp scale from median value."""
    if median_val > 1e15:
        return "ns"
    if median_val > 1e12:
        return "ms"
    return "s"


def _compute_energy_row(
    total_power: Optional[float],
    delta_s: float,
    a_energy: Optional[float],
    b_energy: Optional[float],
    c_energy: Optional[float],
) -> Tuple[Optional[float], Optional[float]]:
    """Compute (total_power, energy_kwh) for a single row.

    Prefers interval energy (Wh) if available, otherwise integrates power.
    Returns (total_power, energy_kwh).
    """
    # Cap delta to avoid absurd energy values during data gaps.
    if delta_s > _MAX_DELTA_S:
        delta_s = 0.0

    # If interval energy columns are available, use them.
    if a_energy is not None and b_energy is not None and c_energy is not None:
        wh = (a_energy if a_energy else 0.0) + (b_energy if b_energy else 0.0) + (c_energy if c_energy else 0.0)
        kwh = wh / 1000.0
        # Derive average power from energy (avoid div/0)
        if delta_s > 0:
            tp = (kwh * 1000.0) * (3600.0 / delta_s)
        else:
            tp = total_power
        return tp, kwh

    # Otherwise integrate power over time.
    if total_power is not None and delta_s > 0:
        kwh = (total_power * (delta_s / 3600.0)) / 1000.0
        return total_power, kwh

    return total_power, 0.0


# ---------------------------------------------------------------------------
# EnergyDB
# ---------------------------------------------------------------------------


class EnergyDB:
    """Thread-safe SQLite wrapper for energy sample data."""

    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._write_lock = threading.Lock()
        self._local = threading.local()
        # Ensure schema exists (uses the writer connection).
        conn = self._conn()
        with conn:
            conn.executescript(_SCHEMA_SQL)
        # Migrate schema: add any missing columns from _EXTRA_COLUMNS.
        self._ensure_extra_columns(conn)
        # Backfill neutral current from phase currents where missing.
        self._backfill_n_current(conn)
        # Apply retention policy: compress data older than 2 years to monthly.
        try:
            self.apply_retention()
        except Exception:
            logger.debug("Retention policy application skipped", exc_info=True)

    def _ensure_extra_columns(self, conn: sqlite3.Connection) -> None:
        """Add any missing columns to the samples table (idempotent)."""
        try:
            existing = {row[1].lower() for row in conn.execute("PRAGMA table_info(samples)").fetchall()}
        except Exception:
            return
        for col_name, col_type in _EXTRA_COLUMNS:
            if col_name.lower() not in existing:
                try:
                    conn.execute(f"ALTER TABLE samples ADD COLUMN {col_name} {col_type}")
                    logger.info("Added column '%s' to samples table", col_name)
                except Exception:
                    pass  # Column might already exist from a concurrent init.

    def _backfill_n_current(self, conn: sqlite3.Connection) -> None:
        """Compute n_avg_current from phase currents where it is NULL.

        Uses the 120° displacement formula:
            I_N = sqrt(Ia² + Ib² + Ic² - Ia·Ib - Ia·Ic - Ib·Ic)
        """
        try:
            existing = {row[1].lower() for row in conn.execute("PRAGMA table_info(samples)").fetchall()}
            needed = {"n_avg_current", "a_avg_current", "b_avg_current", "c_avg_current"}
            if not needed.issubset(existing):
                if not {"n_avg_current", "a_current", "b_current", "c_current"}.issubset(existing):
                    return
            row = conn.execute(
                "SELECT 1 FROM samples WHERE n_avg_current IS NULL "
                "AND (a_avg_current IS NOT NULL OR a_current IS NOT NULL) LIMIT 1"
            ).fetchone()
            if row is None:
                return
            logger.info("Backfilling n_avg_current from phase currents …")
            ia = "COALESCE(a_avg_current, a_current, 0)"
            ib = "COALESCE(b_avg_current, b_current, 0)"
            ic = "COALESCE(c_avg_current, c_current, 0)"
            # Only process rows that actually have at least one phase current value.
            # Use the raw (non-COALESCE) columns for the NULL check so rows with
            # no current data at all are skipped efficiently.
            has_current = (
                "a_avg_current IS NOT NULL OR a_current IS NOT NULL OR "
                "b_avg_current IS NOT NULL OR b_current IS NOT NULL OR "
                "c_avg_current IS NOT NULL OR c_current IS NOT NULL"
            )
            cursor = conn.execute(
                f"SELECT device_key, timestamp, {ia}, {ib}, {ic} "
                f"FROM samples WHERE n_avg_current IS NULL "
                f"AND ({has_current})"
            )
            import math
            batch = []
            for dk, ts, a, b, c in cursor:
                a, b, c = float(a or 0), float(b or 0), float(c or 0)
                n = math.sqrt(max(a*a + b*b + c*c - a*b - a*c - b*c, 0.0))
                batch.append((n, dk, ts))
                if len(batch) >= 5000:
                    conn.executemany(
                        "UPDATE samples SET n_avg_current = ? WHERE device_key = ? AND timestamp = ?",
                        batch,
                    )
                    batch.clear()
            if batch:
                conn.executemany(
                    "UPDATE samples SET n_avg_current = ? WHERE device_key = ? AND timestamp = ?",
                    batch,
                )
            conn.commit()
            logger.info("Backfilled n_avg_current for historical samples")
        except Exception:
            logger.debug("n_avg_current backfill skipped", exc_info=True)

    # -- connection management (one connection per thread) -------------------

    def _conn(self) -> sqlite3.Connection:
        c = getattr(self._local, "conn", None)
        if c is None:
            c = sqlite3.connect(str(self.db_path), timeout=30.0)
            c.execute("PRAGMA journal_mode=WAL")
            c.execute("PRAGMA synchronous=NORMAL")
            c.execute("PRAGMA cache_size=-8000")  # 8 MB cache
            c.execute("PRAGMA temp_store=MEMORY")
            self._local.conn = c
        return c

    def close(self) -> None:
        c = getattr(self._local, "conn", None)
        if c is not None:
            try:
                c.close()
            except Exception:
                pass
            self._local.conn = None

    # -- insert methods ------------------------------------------------------

    def insert_csv_bytes(self, device_key: str, csv_bytes: bytes) -> int:
        """Parse raw CSV bytes (as returned by Shelly EMData API) and insert.

        Returns number of rows inserted.
        """
        try:
            text = csv_bytes.decode("utf-8", errors="replace")
        except Exception:
            text = csv_bytes.decode("latin-1", errors="replace")

        reader = csv.DictReader(io.StringIO(text))
        if reader.fieldnames is None:
            return 0

        # Normalize header names to lowercase.
        header_map = {h: h.strip().lower() for h in reader.fieldnames}

        # Find timestamp column.
        ts_col = None
        for orig, norm in header_map.items():
            if norm in _TS_NAMES:
                ts_col = orig
                break
        if ts_col is None:
            logger.warning("CSV has no timestamp column (headers: %s)", list(reader.fieldnames))
            return 0

        # Detect timestamp scale from first few rows.
        rows_raw = list(reader)
        if not rows_raw:
            return 0

        ts_vals = [_safe_float(r.get(ts_col)) for r in rows_raw[:20]]
        ts_nums = [v for v in ts_vals if v is not None and v > 0]
        if not ts_nums:
            return 0
        ts_scale = _detect_ts_magnitude(sorted(ts_nums)[len(ts_nums) // 2])
        ts_divisor = {"s": 1, "ms": 1000, "ns": 1_000_000_000}.get(ts_scale, 1)

        # Build a lookup: normalised column name → original header key.
        _norm_to_orig: Dict[str, str] = {}
        for orig, norm in header_map.items():
            if norm not in _norm_to_orig:
                _norm_to_orig[norm] = orig

        # Build insert rows.
        insert_rows: list = []
        prev_ts: Optional[int] = None

        for row in rows_raw:
            raw_ts = _safe_float(row.get(ts_col))
            if raw_ts is None or raw_ts <= 0:
                continue
            ts_int = int(raw_ts / ts_divisor)

            # Map CSV columns → DB columns.
            def _get(csv_name: str) -> Optional[float]:
                orig_key = _norm_to_orig.get(csv_name)
                if orig_key is not None:
                    return _safe_float(row.get(orig_key))
                return None

            def _get_fallback(name1: str, name2: str) -> Optional[float]:
                """Return the first non-None value (0.0 is a valid reading)."""
                v = _get(name1)
                return v if v is not None else _get(name2)

            a_p = _get_fallback("a_act_power", "act_power_a")
            b_p = _get_fallback("b_act_power", "act_power_b")
            c_p = _get_fallback("c_act_power", "act_power_c")
            total_p = (a_p if a_p is not None else 0.0) + (b_p if b_p is not None else 0.0) + (c_p if c_p is not None else 0.0)

            a_e = _get("a_total_act_energy")
            b_e = _get("b_total_act_energy")
            c_e = _get("c_total_act_energy")

            delta_s = float(ts_int - prev_ts) if prev_ts is not None and prev_ts < ts_int else 0.0
            tp, kwh = _compute_energy_row(total_p, delta_s, a_e, b_e, c_e)

            # Frequency: prefer pre-computed column; else average the per-phase columns.
            # Read each key exactly once to avoid redundant dict lookups.
            _fhz = _get("freq_hz")
            if _fhz is None:
                _fphase = [v for v in (_get("a_freq"), _get("b_freq"), _get("c_freq")) if v is not None]
                _fhz = sum(_fphase) / len(_fphase) if _fphase else None

            insert_rows.append((
                device_key, ts_int,
                # Base columns
                a_p, b_p, c_p,
                _get_fallback("a_voltage", "a_avg_voltage"),
                _get_fallback("b_voltage", "b_avg_voltage"),
                _get_fallback("c_voltage", "c_avg_voltage"),
                _get_fallback("a_current", "a_avg_current"),
                _get_fallback("b_current", "b_avg_current"),
                _get_fallback("c_current", "c_avg_current"),
                a_e, b_e, c_e,
                _get("a_min_act_power"), _get("a_max_act_power"),
                _get("b_min_act_power"), _get("b_max_act_power"),
                _get("c_min_act_power"), _get("c_max_act_power"),
                tp, kwh,
                # Extra columns (v6.0.0.2)
                _get("a_fund_act_energy"), _get("b_fund_act_energy"), _get("c_fund_act_energy"),
                _get("a_total_act_ret_energy"), _get("b_total_act_ret_energy"), _get("c_total_act_ret_energy"),
                _get("a_fund_act_ret_energy"), _get("b_fund_act_ret_energy"), _get("c_fund_act_ret_energy"),
                _get("a_lag_react_energy"), _get("b_lag_react_energy"), _get("c_lag_react_energy"),
                _get("a_lead_react_energy"), _get("b_lead_react_energy"), _get("c_lead_react_energy"),
                _get("a_max_aprt_power"), _get("a_min_aprt_power"),
                _get("b_max_aprt_power"), _get("b_min_aprt_power"),
                _get("c_max_aprt_power"), _get("c_min_aprt_power"),
                _get("a_max_voltage"), _get("a_min_voltage"), _get("a_avg_voltage"),
                _get("b_max_voltage"), _get("b_min_voltage"), _get("b_avg_voltage"),
                _get("c_max_voltage"), _get("c_min_voltage"), _get("c_avg_voltage"),
                _get("a_max_current"), _get("a_min_current"), _get("a_avg_current"),
                _get("b_max_current"), _get("b_min_current"), _get("b_avg_current"),
                _get("c_max_current"), _get("c_min_current"), _get("c_avg_current"),
                _get("n_max_current"), _get("n_min_current"), _get("n_avg_current"),
                _fhz,
            ))
            prev_ts = ts_int

        if not insert_rows:
            return 0

        placeholders = ",".join(["?"] * len(SAMPLE_COLUMNS))
        sql = f"INSERT OR REPLACE INTO samples ({','.join(SAMPLE_COLUMNS)}) VALUES ({placeholders})"

        # Compute actual min/max timestamps (data may not be sorted).
        all_ts = [r[1] for r in insert_rows]
        ts_min = min(all_ts)
        ts_max = max(all_ts)

        with self._write_lock:
            conn = self._conn()
            with conn:
                conn.executemany(sql, insert_rows)
                # Update hourly aggregation inside the same transaction.
                self._update_hourly_inner(conn, device_key, ts_min, ts_max)

        return len(insert_rows)

    def insert_dataframe(self, device_key: str, df: pd.DataFrame) -> int:
        """Bulk-insert from a pandas DataFrame (used by CSV migration).

        Expects the DataFrame to already have 'timestamp' (datetime) and
        optionally 'total_power', 'energy_kwh' columns.
        """
        if df is None or df.empty:
            return 0

        rows: list = []
        for _, r in df.iterrows():
            ts_val = r.get("timestamp")
            if pd.isna(ts_val):
                continue
            # Convert datetime → unix seconds.
            if hasattr(ts_val, "timestamp"):
                ts_int = int(ts_val.timestamp())
            else:
                ts_int = int(ts_val)

            def _col(name: str) -> Optional[float]:
                v = r.get(name)
                if v is not None and not (isinstance(v, float) and pd.isna(v)):
                    try:
                        return float(v)
                    except (ValueError, TypeError):
                        pass
                return None

            def _col_fallback(name1: str, name2: str) -> Optional[float]:
                """Return first non-None column value (0.0 is valid)."""
                v = _col(name1)
                return v if v is not None else _col(name2)

            rows.append((
                device_key, ts_int,
                # Base columns
                _col("a_act_power"), _col("b_act_power"), _col("c_act_power"),
                _col_fallback("a_voltage", "a_avg_voltage"),
                _col_fallback("b_voltage", "b_avg_voltage"),
                _col_fallback("c_voltage", "c_avg_voltage"),
                _col_fallback("a_current", "a_avg_current"),
                _col_fallback("b_current", "b_avg_current"),
                _col_fallback("c_current", "c_avg_current"),
                _col("a_total_act_energy"), _col("b_total_act_energy"), _col("c_total_act_energy"),
                _col("a_min_act_power"), _col("a_max_act_power"),
                _col("b_min_act_power"), _col("b_max_act_power"),
                _col("c_min_act_power"), _col("c_max_act_power"),
                _col("total_power"), _col("energy_kwh"),
                # Extra columns (v6.0.0.2)
                _col("a_fund_act_energy"), _col("b_fund_act_energy"), _col("c_fund_act_energy"),
                _col("a_total_act_ret_energy"), _col("b_total_act_ret_energy"), _col("c_total_act_ret_energy"),
                _col("a_fund_act_ret_energy"), _col("b_fund_act_ret_energy"), _col("c_fund_act_ret_energy"),
                _col("a_lag_react_energy"), _col("b_lag_react_energy"), _col("c_lag_react_energy"),
                _col("a_lead_react_energy"), _col("b_lead_react_energy"), _col("c_lead_react_energy"),
                _col("a_max_aprt_power"), _col("a_min_aprt_power"),
                _col("b_max_aprt_power"), _col("b_min_aprt_power"),
                _col("c_max_aprt_power"), _col("c_min_aprt_power"),
                _col("a_max_voltage"), _col("a_min_voltage"), _col("a_avg_voltage"),
                _col("b_max_voltage"), _col("b_min_voltage"), _col("b_avg_voltage"),
                _col("c_max_voltage"), _col("c_min_voltage"), _col("c_avg_voltage"),
                _col("a_max_current"), _col("a_min_current"), _col("a_avg_current"),
                _col("b_max_current"), _col("b_min_current"), _col("b_avg_current"),
                _col("c_max_current"), _col("c_min_current"), _col("c_avg_current"),
                _col("n_max_current"), _col("n_min_current"), _col("n_avg_current"),
                _col("freq_hz"),
            ))

        if not rows:
            return 0

        placeholders = ",".join(["?"] * len(SAMPLE_COLUMNS))
        sql = f"INSERT OR REPLACE INTO samples ({','.join(SAMPLE_COLUMNS)}) VALUES ({placeholders})"

        # Compute actual min/max timestamps (data may not be sorted).
        all_ts = [r[1] for r in rows]
        ts_min = min(all_ts)
        ts_max = max(all_ts)

        with self._write_lock:
            conn = self._conn()
            with conn:
                conn.executemany(sql, rows)
                # Update hourly aggregation inside the same transaction.
                self._update_hourly_inner(conn, device_key, ts_min, ts_max)

        return len(rows)

    # -- query methods -------------------------------------------------------

    def query_samples(
        self,
        device_key: str,
        start_ts: Optional[int] = None,
        end_ts: Optional[int] = None,
    ) -> pd.DataFrame:
        """Read samples as a pandas DataFrame, optionally filtered by time range.

        Transparently merges monthly aggregated data for periods that have been
        compressed by the retention policy.
        """
        conn = self._conn()
        conditions = ["device_key = ?"]
        params: list = [device_key]
        if start_ts is not None:
            conditions.append("timestamp >= ?")
            params.append(int(start_ts))
        if end_ts is not None:
            conditions.append("timestamp <= ?")
            params.append(int(end_ts))

        where = " AND ".join(conditions)
        sql = f"SELECT * FROM samples WHERE {where} ORDER BY timestamp"
        df = pd.read_sql_query(sql, conn, params=params)

        # Convert integer timestamps → pandas datetime (UTC).
        if "timestamp" in df.columns and not df.empty:
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
            # Strip timezone for compatibility with existing code.
            try:
                df["timestamp"] = df["timestamp"].dt.tz_localize(None)
            except Exception:
                pass

        # Drop the device_key column (caller already knows it).
        if "device_key" in df.columns:
            df = df.drop(columns=["device_key"])

        # Merge monthly aggregated data if the query range covers compressed
        # periods.  Monthly data is included when no start_ts is given or when
        # start_ts falls before the oldest raw sample.
        try:
            monthly_df = self.query_monthly(device_key, start_ts=start_ts, end_ts=end_ts)
            if not monthly_df.empty:
                if df.empty:
                    df = monthly_df
                else:
                    # Only include monthly rows older than the oldest raw sample
                    # to avoid overlap.
                    oldest_raw = df["timestamp"].min()
                    monthly_df = monthly_df[monthly_df["timestamp"] < oldest_raw]
                    if not monthly_df.empty:
                        df = pd.concat([monthly_df, df], ignore_index=True)
        except Exception:
            logger.debug("Failed to merge monthly data", exc_info=True)

        # Drop columns where ALL values are NULL.  The DB schema always
        # includes every possible column (a_voltage, a_current, …) but the
        # actual CSV data from Shelly EMData often contains only power/energy
        # columns.  If we keep the all-NULL columns, downstream code (e.g.
        # _df_has_wva_cols) thinks voltage/current data exists and skips the
        # live-data fallback, resulting in empty V/A plots.
        if not df.empty:
            df = df.dropna(axis=1, how="all")

        return df

    def query_hourly(
        self,
        device_key: str,
        start_ts: Optional[int] = None,
        end_ts: Optional[int] = None,
    ) -> pd.DataFrame:
        """Read pre-aggregated hourly stats."""
        conn = self._conn()
        conditions = ["device_key = ?"]
        params: list = [device_key]
        if start_ts is not None:
            conditions.append("hour_ts >= ?")
            params.append(int(start_ts))
        if end_ts is not None:
            conditions.append("hour_ts <= ?")
            params.append(int(end_ts))

        where = " AND ".join(conditions)
        sql = f"SELECT * FROM hourly_energy WHERE {where} ORDER BY hour_ts"
        df = pd.read_sql_query(sql, conn, params=params)
        if "device_key" in df.columns:
            df = df.drop(columns=["device_key"])
        return df

    # -- meta ----------------------------------------------------------------

    def load_meta(self, device_key: str):
        """Load device meta state. Returns (last_end_ts, updated_at) or (None, None)."""
        conn = self._conn()
        row = conn.execute(
            "SELECT last_end_ts, updated_at FROM device_meta WHERE device_key = ?",
            (device_key,),
        ).fetchone()
        if row is None:
            return None, None
        return row[0], row[1]

    def save_meta(self, device_key: str, last_end_ts: Optional[int], updated_at: Optional[int]) -> None:
        with self._write_lock:
            conn = self._conn()
            with conn:
                conn.execute(
                    "INSERT OR REPLACE INTO device_meta (device_key, last_end_ts, updated_at) VALUES (?, ?, ?)",
                    (device_key, last_end_ts, updated_at),
                )

    # -- device info ---------------------------------------------------------

    def device_keys(self) -> List[str]:
        conn = self._conn()
        rows = conn.execute("SELECT DISTINCT device_key FROM samples ORDER BY device_key").fetchall()
        return [r[0] for r in rows]

    def has_data(self, device_key: str) -> bool:
        conn = self._conn()
        row = conn.execute(
            "SELECT 1 FROM samples WHERE device_key = ? LIMIT 1",
            (device_key,),
        ).fetchone()
        return row is not None

    def row_count(self, device_key: str) -> int:
        conn = self._conn()
        row = conn.execute(
            "SELECT COUNT(*) FROM samples WHERE device_key = ?",
            (device_key,),
        ).fetchone()
        return int(row[0]) if row else 0

    def needs_reimport(self) -> bool:
        """Check if the DB was created with an older schema or has unfilled columns.

        Returns True if:
        - Extra columns are missing entirely (schema migration just happened)
        - Extra columns exist but are all NULL (imported before v6.0.0.2)
        - Base V/A columns are all NULL even though data exists (v6.0.0.2
          didn't populate base columns from a_avg_voltage fallback)
        """
        conn = self._conn()
        try:
            existing = {row[1].lower() for row in conn.execute("PRAGMA table_info(samples)").fetchall()}
            # If any extra column is missing entirely, schema migration just happened.
            for col, _ in _EXTRA_COLUMNS:
                if col.lower() not in existing:
                    return True
            # Check if base voltage column has data.  If a_voltage is all NULL
            # but a_avg_voltage has data, the fallback mapping was missing and
            # we need to re-import to fill the base columns.
            row = conn.execute(
                "SELECT 1 FROM samples WHERE a_voltage IS NOT NULL LIMIT 1"
            ).fetchone()
            if row is None:
                # a_voltage is all NULL — check if data exists at all.
                row = conn.execute("SELECT 1 FROM samples LIMIT 1").fetchone()
                if row is not None:
                    return True
            # Also check extra columns (original v6.0.0.2 check).
            row = conn.execute(
                "SELECT 1 FROM samples WHERE a_avg_voltage IS NOT NULL LIMIT 1"
            ).fetchone()
            if row is None:
                # No voltage data → check if there are any rows at all.
                row = conn.execute("SELECT 1 FROM samples LIMIT 1").fetchone()
                return row is not None
        except Exception:
            pass
        return False

    # -- hourly aggregation --------------------------------------------------

    def _update_hourly_inner(self, conn: sqlite3.Connection, device_key: str, ts_min: int, ts_max: int) -> None:
        """Recompute hourly_energy rows for the affected time range.

        MUST be called inside an existing transaction (``with conn:`` block).
        """
        # Align to hour boundaries.
        hour_start = (ts_min // 3600) * 3600
        hour_end = ((ts_max // 3600) + 1) * 3600

        sql = """
            INSERT OR REPLACE INTO hourly_energy (device_key, hour_ts, kwh, avg_power_w, max_power_w, sample_count)
            SELECT
                device_key,
                (timestamp / 3600) * 3600 AS hour_ts,
                COALESCE(SUM(energy_kwh), 0) AS kwh,
                AVG(total_power) AS avg_power_w,
                MAX(total_power) AS max_power_w,
                COUNT(*) AS sample_count
            FROM samples
            WHERE device_key = ? AND timestamp >= ? AND timestamp < ?
            GROUP BY device_key, hour_ts
        """
        conn.execute(sql, (device_key, hour_start, hour_end))

    def rebuild_hourly(self, device_key: str) -> None:
        """Full rebuild of hourly aggregation for a device."""
        with self._write_lock:
            conn = self._conn()
            with conn:
                conn.execute("DELETE FROM hourly_energy WHERE device_key = ?", (device_key,))
                conn.execute("""
                    INSERT INTO hourly_energy (device_key, hour_ts, kwh, avg_power_w, max_power_w, sample_count)
                    SELECT
                        device_key,
                        (timestamp / 3600) * 3600 AS hour_ts,
                        COALESCE(SUM(energy_kwh), 0),
                        AVG(total_power),
                        MAX(total_power),
                        COUNT(*)
                    FROM samples
                    WHERE device_key = ?
                    GROUP BY device_key, (timestamp / 3600) * 3600
                """, (device_key,))

    # -- retention / monthly compression -------------------------------------

    @staticmethod
    def _retention_cutoff_ts() -> Optional[int]:
        """Return Unix timestamp of Jan 1 two years ago, or None if no compression needed.

        Policy: keep full-resolution data for the current year and the previous
        year.  Everything older is compressed to monthly aggregates.

        Example (today = 2026-03-18):
          current year = 2026, previous year = 2025
          cutoff = 2025-01-01 00:00:00 UTC  →  data before this is compressed.
        """
        now = datetime.datetime.now(datetime.timezone.utc)
        cutoff_year = now.year - 1  # keep this year + previous year
        cutoff = datetime.datetime(cutoff_year, 1, 1, tzinfo=datetime.timezone.utc)
        return int(cutoff.timestamp())

    def apply_retention(self) -> Dict[str, int]:
        """Compress old samples to monthly aggregates and delete the raw rows.

        Returns dict mapping device_key → number of raw rows deleted.
        """
        cutoff_ts = self._retention_cutoff_ts()
        if cutoff_ts is None:
            return {}

        conn = self._conn()

        # Find devices that have data older than cutoff.
        rows = conn.execute(
            "SELECT DISTINCT device_key FROM samples WHERE timestamp < ?",
            (cutoff_ts,),
        ).fetchall()
        if not rows:
            return {}

        device_keys = [r[0] for r in rows]
        result: Dict[str, int] = {}

        for dk in device_keys:
            deleted = self._compress_device_before(conn, dk, cutoff_ts)
            if deleted > 0:
                result[dk] = deleted
                logger.info(
                    "Retention: compressed %d old samples to monthly for device '%s'",
                    deleted, dk,
                )

        return result

    def _compress_device_before(
        self, conn: sqlite3.Connection, device_key: str, cutoff_ts: int
    ) -> int:
        """Aggregate samples before *cutoff_ts* into monthly_energy rows, then delete them.

        Returns number of raw sample rows deleted.
        """
        # Build monthly aggregates from raw samples.
        # We group by calendar month using strftime on Unix timestamps.
        # month_ts = Unix timestamp of the 1st of each month (UTC).
        agg_sql = """
            SELECT
                -- First day of month as Unix ts
                CAST(strftime('%%s', strftime('%%Y-%%m-01', timestamp, 'unixepoch')) AS INTEGER) AS month_ts,
                COALESCE(SUM(energy_kwh), 0) AS kwh,
                AVG(total_power) AS avg_power_w,
                MAX(total_power) AS max_power_w,
                MIN(total_power) AS min_power_w,
                AVG(COALESCE(a_voltage, a_avg_voltage)) AS avg_a_voltage,
                AVG(COALESCE(b_voltage, b_avg_voltage)) AS avg_b_voltage,
                AVG(COALESCE(c_voltage, c_avg_voltage)) AS avg_c_voltage,
                MIN(COALESCE(a_min_voltage, a_voltage, a_avg_voltage)) AS min_a_voltage,
                MIN(COALESCE(b_min_voltage, b_voltage, b_avg_voltage)) AS min_b_voltage,
                MIN(COALESCE(c_min_voltage, c_voltage, c_avg_voltage)) AS min_c_voltage,
                MAX(COALESCE(a_max_voltage, a_voltage, a_avg_voltage)) AS max_a_voltage,
                MAX(COALESCE(b_max_voltage, b_voltage, b_avg_voltage)) AS max_b_voltage,
                MAX(COALESCE(c_max_voltage, c_voltage, c_avg_voltage)) AS max_c_voltage,
                AVG(COALESCE(a_current, a_avg_current)) AS avg_a_current,
                AVG(COALESCE(b_current, b_avg_current)) AS avg_b_current,
                AVG(COALESCE(c_current, c_avg_current)) AS avg_c_current,
                MAX(COALESCE(a_max_current, a_current, a_avg_current)) AS max_a_current,
                MAX(COALESCE(b_max_current, b_current, b_avg_current)) AS max_b_current,
                MAX(COALESCE(c_max_current, c_current, c_avg_current)) AS max_c_current,
                AVG(n_avg_current) AS avg_n_current,
                MAX(n_max_current) AS max_n_current,
                AVG(freq_hz) AS avg_freq_hz,
                COUNT(*) AS sample_count
            FROM samples
            WHERE device_key = ? AND timestamp < ?
            GROUP BY month_ts
            ORDER BY month_ts
        """

        agg_rows = conn.execute(agg_sql, (device_key, cutoff_ts)).fetchall()
        if not agg_rows:
            return 0

        # Insert monthly aggregates (merge with any existing rows).
        insert_sql = """
            INSERT OR REPLACE INTO monthly_energy (
                device_key, month_ts, kwh, avg_power_w, max_power_w, min_power_w,
                avg_a_voltage, avg_b_voltage, avg_c_voltage,
                min_a_voltage, min_b_voltage, min_c_voltage,
                max_a_voltage, max_b_voltage, max_c_voltage,
                avg_a_current, avg_b_current, avg_c_current,
                max_a_current, max_b_current, max_c_current,
                avg_n_current, max_n_current,
                avg_freq_hz, sample_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        with self._write_lock:
            with conn:
                for row in agg_rows:
                    conn.execute(insert_sql, (device_key, *row))

                # Count rows to delete.
                count_row = conn.execute(
                    "SELECT COUNT(*) FROM samples WHERE device_key = ? AND timestamp < ?",
                    (device_key, cutoff_ts),
                ).fetchone()
                deleted = int(count_row[0]) if count_row else 0

                # Delete compressed raw samples.
                conn.execute(
                    "DELETE FROM samples WHERE device_key = ? AND timestamp < ?",
                    (device_key, cutoff_ts),
                )

                # Also clean up hourly_energy for the compressed period.
                conn.execute(
                    "DELETE FROM hourly_energy WHERE device_key = ? AND hour_ts < ?",
                    (device_key, cutoff_ts),
                )

        return deleted

    def query_monthly(
        self,
        device_key: str,
        start_ts: Optional[int] = None,
        end_ts: Optional[int] = None,
    ) -> pd.DataFrame:
        """Read monthly aggregated data as a DataFrame compatible with sample queries.

        Returns rows with timestamp, total_power, energy_kwh, voltage, current,
        freq_hz columns — one row per month.
        """
        conn = self._conn()
        conditions = ["device_key = ?"]
        params: list = [device_key]
        if start_ts is not None:
            conditions.append("month_ts >= ?")
            params.append(int(start_ts))
        if end_ts is not None:
            conditions.append("month_ts <= ?")
            params.append(int(end_ts))

        where = " AND ".join(conditions)
        sql = f"""
            SELECT
                month_ts AS timestamp,
                avg_power_w AS total_power,
                kwh AS energy_kwh,
                avg_a_voltage AS a_voltage,
                avg_b_voltage AS b_voltage,
                avg_c_voltage AS c_voltage,
                avg_a_current AS a_current,
                avg_b_current AS b_current,
                avg_c_current AS c_current,
                avg_n_current AS n_avg_current,
                avg_freq_hz AS freq_hz,
                avg_power_w AS a_act_power,
                max_power_w,
                min_power_w,
                sample_count
            FROM monthly_energy
            WHERE {where}
            ORDER BY month_ts
        """
        df = pd.read_sql_query(sql, conn, params=params)

        if "timestamp" in df.columns and not df.empty:
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
            try:
                df["timestamp"] = df["timestamp"].dt.tz_localize(None)
            except Exception:
                pass

        return df

    def oldest_sample_ts(self, device_key: str) -> Optional[int]:
        """Return the oldest sample timestamp for a device, or None."""
        conn = self._conn()
        row = conn.execute(
            "SELECT MIN(timestamp) FROM samples WHERE device_key = ?",
            (device_key,),
        ).fetchone()
        return int(row[0]) if row and row[0] is not None else None

    def oldest_monthly_ts(self, device_key: str) -> Optional[int]:
        """Return the oldest monthly aggregate timestamp for a device, or None."""
        conn = self._conn()
        row = conn.execute(
            "SELECT MIN(month_ts) FROM monthly_energy WHERE device_key = ?",
            (device_key,),
        ).fetchone()
        return int(row[0]) if row and row[0] is not None else None

    # ── CO₂ intensity helpers ─────────────────────────────────────────────

    def upsert_co2_intensity(self, rows: List[Tuple[int, str, float, str, int]]) -> int:
        """Insert or replace CO₂ intensity rows.

        Each row is (hour_ts, zone, intensity_g_per_kwh, source, fetched_at).
        Returns the number of rows written.
        """
        if not rows:
            return 0
        conn = self._conn()
        with self._write_lock:
            with conn:
                conn.executemany(
                    "INSERT OR REPLACE INTO co2_intensity "
                    "(hour_ts, zone, intensity_g_per_kwh, source, fetched_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    rows,
                )
        return len(rows)

    def query_co2_intensity(
        self,
        zone: str,
        start_ts: int,
        end_ts: int,
    ) -> pd.DataFrame:
        """Return CO₂ intensity rows for a zone in [start_ts, end_ts).

        Columns: hour_ts, zone, intensity_g_per_kwh, source, fetched_at.
        """
        conn = self._conn()
        df = pd.read_sql_query(
            "SELECT hour_ts, zone, intensity_g_per_kwh, source, fetched_at "
            "FROM co2_intensity "
            "WHERE zone = ? AND hour_ts >= ? AND hour_ts < ? "
            "ORDER BY hour_ts",
            conn,
            params=(zone, start_ts, end_ts),
        )
        return df

    def latest_co2_ts(self, zone: str) -> Optional[int]:
        """Return the most recent hour_ts for a zone, or None."""
        conn = self._conn()
        row = conn.execute(
            "SELECT MAX(hour_ts) FROM co2_intensity WHERE zone = ?",
            (zone,),
        ).fetchone()
        return int(row[0]) if row and row[0] is not None else None

    def oldest_co2_ts(self, zone: str) -> Optional[int]:
        """Return the oldest hour_ts for a zone, or None."""
        conn = self._conn()
        row = conn.execute(
            "SELECT MIN(hour_ts) FROM co2_intensity WHERE zone = ?",
            (zone,),
        ).fetchone()
        return int(row[0]) if row and row[0] is not None else None

    def delete_all_co2_data(self) -> int:
        """Delete all rows from the co2_intensity table. Returns rows deleted."""
        conn = self._conn()
        with self._write_lock:
            with conn:
                cur = conn.execute("DELETE FROM co2_intensity")
                return cur.rowcount
