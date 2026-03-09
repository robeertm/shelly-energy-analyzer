"""SQLite-based energy data storage (v6.0.0+).

Replaces CSV chunk files with a single indexed database for fast range queries.
Uses WAL mode for concurrent reads (web dashboard + UI) during writes (sync).
"""
from __future__ import annotations

import csv
import io
import logging
import sqlite3
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

from shelly_analyzer.io.config import DeviceConfig

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS samples (
    device_key  TEXT    NOT NULL,
    timestamp   INTEGER NOT NULL,
    a_act_power REAL, b_act_power REAL, c_act_power REAL,
    a_voltage   REAL, b_voltage   REAL, c_voltage   REAL,
    a_current   REAL, b_current   REAL, c_current   REAL,
    a_total_act_energy REAL, b_total_act_energy REAL, c_total_act_energy REAL,
    a_min_act_power REAL, a_max_act_power REAL,
    b_min_act_power REAL, b_max_act_power REAL,
    c_min_act_power REAL, c_max_act_power REAL,
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

CREATE TABLE IF NOT EXISTS device_meta (
    device_key  TEXT PRIMARY KEY,
    last_end_ts INTEGER,
    updated_at  INTEGER
);
"""

# All known sample columns in canonical order (must match CREATE TABLE).
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
)

# Columns that come from the CSV (everything except device_key and computed cols).
_CSV_VALUE_COLS = SAMPLE_COLUMNS[1:-2]  # timestamp … c_max_act_power

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
    # If interval energy columns are available, use them.
    if a_energy is not None and b_energy is not None and c_energy is not None:
        wh = (a_energy or 0.0) + (b_energy or 0.0) + (c_energy or 0.0)
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
                # Try original name, then lowercased form.
                for candidate_key, norm in header_map.items():
                    if norm == csv_name:
                        return _safe_float(row.get(candidate_key))
                return None

            a_p = _get("a_act_power") or _get("act_power_a")
            b_p = _get("b_act_power") or _get("act_power_b")
            c_p = _get("c_act_power") or _get("act_power_c")
            total_p = (a_p or 0.0) + (b_p or 0.0) + (c_p or 0.0)

            a_e = _get("a_total_act_energy")
            b_e = _get("b_total_act_energy")
            c_e = _get("c_total_act_energy")

            delta_s = float(ts_int - prev_ts) if prev_ts is not None and prev_ts < ts_int else 0.0
            tp, kwh = _compute_energy_row(total_p, delta_s, a_e, b_e, c_e)

            insert_rows.append((
                device_key, ts_int,
                a_p, b_p, c_p,
                _get("a_voltage"), _get("b_voltage"), _get("c_voltage"),
                _get("a_current"), _get("b_current"), _get("c_current"),
                a_e, b_e, c_e,
                _get("a_min_act_power"), _get("a_max_act_power"),
                _get("b_min_act_power"), _get("b_max_act_power"),
                _get("c_min_act_power"), _get("c_max_act_power"),
                tp, kwh,
            ))
            prev_ts = ts_int

        if not insert_rows:
            return 0

        placeholders = ",".join(["?"] * len(SAMPLE_COLUMNS))
        sql = f"INSERT OR REPLACE INTO samples ({','.join(SAMPLE_COLUMNS)}) VALUES ({placeholders})"

        with self._write_lock:
            conn = self._conn()
            with conn:
                conn.executemany(sql, insert_rows)
            # Update hourly aggregation for affected range.
            ts_min = insert_rows[0][1]
            ts_max = insert_rows[-1][1]
            self._update_hourly(conn, device_key, ts_min, ts_max)

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

            rows.append((
                device_key, ts_int,
                _col("a_act_power"), _col("b_act_power"), _col("c_act_power"),
                _col("a_voltage"), _col("b_voltage"), _col("c_voltage"),
                _col("a_current"), _col("b_current"), _col("c_current"),
                _col("a_total_act_energy"), _col("b_total_act_energy"), _col("c_total_act_energy"),
                _col("a_min_act_power"), _col("a_max_act_power"),
                _col("b_min_act_power"), _col("b_max_act_power"),
                _col("c_min_act_power"), _col("c_max_act_power"),
                _col("total_power"), _col("energy_kwh"),
            ))

        if not rows:
            return 0

        placeholders = ",".join(["?"] * len(SAMPLE_COLUMNS))
        sql = f"INSERT OR REPLACE INTO samples ({','.join(SAMPLE_COLUMNS)}) VALUES ({placeholders})"

        with self._write_lock:
            conn = self._conn()
            with conn:
                conn.executemany(sql, rows)
            ts_min = rows[0][1]
            ts_max = rows[-1][1]
            self._update_hourly(conn, device_key, ts_min, ts_max)

        return len(rows)

    # -- query methods -------------------------------------------------------

    def query_samples(
        self,
        device_key: str,
        start_ts: Optional[int] = None,
        end_ts: Optional[int] = None,
    ) -> pd.DataFrame:
        """Read samples as a pandas DataFrame, optionally filtered by time range."""
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

    # -- hourly aggregation --------------------------------------------------

    def _update_hourly(self, conn: sqlite3.Connection, device_key: str, ts_min: int, ts_max: int) -> None:
        """Recompute hourly_energy rows for the affected time range."""
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
        with conn:
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
