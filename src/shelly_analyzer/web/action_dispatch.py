"""Action dispatcher for web API requests.

Extracted from ui/mixins/liveweb.py — handles remote actions from the web
dashboard without any tkinter dependency.
"""
from __future__ import annotations

import calendar
import io
import json
import math
import re
import time
import threading
import logging
from collections import defaultdict, deque
from dataclasses import replace, asdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo
from zipfile import ZipFile, ZIP_DEFLATED

import pandas as pd
import numpy as np

from shelly_analyzer import __version__
from shelly_analyzer.io.config import AppConfig, save_config
from shelly_analyzer.io.storage import Storage
from shelly_analyzer.io.http import ShellyHttp, HttpConfig, get_shelly_status, get_switch_status, set_switch_state
from shelly_analyzer.i18n import t as _t, format_date_local, format_number_local
from shelly_analyzer.core.energy import filter_by_time, calculate_energy
from shelly_analyzer.core.stats import daily_kwh, weekly_kwh, monthly_kwh
from shelly_analyzer.services.compute import ComputedDevice, load_device, summarize
from shelly_analyzer.services.sync import sync_all
from shelly_analyzer.services.webdash import LiveStateStore
from shelly_analyzer.services.export import (
    ReportTotals,
    InvoiceLine,
    export_to_excel,
    export_pdf_summary,
    export_pdf_invoice,
    export_figure_png,
)
from shelly_analyzer.web.utils import _parse_date_flexible, _period_bounds

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helper: extract switch on/off from Shelly RPC / REST payloads
# ---------------------------------------------------------------------------

def _extract_switch_on(raw: Dict[str, Any]) -> Optional[bool]:
    """Best-effort extraction of on/off from Switch.GetStatus payload."""
    if not isinstance(raw, dict):
        return None

    def _coerce(v: Any) -> Optional[bool]:
        if v is None:
            return None
        if isinstance(v, bool):
            return v
        if isinstance(v, (int, float)):
            return bool(v)
        if isinstance(v, str):
            s = v.strip().lower()
            if s in ("on", "true", "1", "yes", "ein"):
                return True
            if s in ("off", "false", "0", "no", "aus"):
                return False
            return bool(s)
        try:
            return bool(v)
        except Exception:
            return None

    # 1) Direct keys (typical for RPC responses)
    for k in ("output", "on", "ison", "is_on", "state"):
        if k in raw:
            return _coerce(raw.get(k))

    # 2) Gen1 /status response nests relay state under `relays` (list).
    try:
        relays = raw.get("relays")
        if isinstance(relays, list) and relays:
            vals: List[Optional[bool]] = []
            for item in relays:
                if not isinstance(item, dict):
                    continue
                for k in ("ison", "on", "state"):
                    if k in item:
                        vals.append(_coerce(item.get(k)))
                        break
            if any(v is True for v in vals):
                return True
            if any(v is False for v in vals):
                return False
    except Exception:
        pass

    # 3) Some devices nest under `status` or `result` (or expose arrays).
    for parent in ("status", "result"):
        try:
            d = raw.get(parent)
            if isinstance(d, dict):
                for k in ("output", "on", "ison", "is_on", "state"):
                    if k in d:
                        return _coerce(d.get(k))
                for arr_key in ("switches", "relays"):
                    arr = d.get(arr_key)
                    if isinstance(arr, list) and arr:
                        vals2: List[Optional[bool]] = []
                        for item in arr:
                            if not isinstance(item, dict):
                                continue
                            for kk in ("output", "ison", "on", "is_on", "state"):
                                if kk in item:
                                    vals2.append(_coerce(item.get(kk)))
                                    break
                        if any(v is True for v in vals2):
                            return True
                        if any(v is False for v in vals2):
                            return False
        except Exception:
            pass

    # 4) RPC map-style payloads (e.g. {"switch:0": {"output": true, ...}}).
    try:
        vals3: List[Optional[bool]] = []
        for k, v in raw.items():
            if not (isinstance(k, str) and isinstance(v, dict)):
                continue
            if not (k.startswith("switch:") or k.startswith("relay:")):
                continue
            for kk in ("output", "on", "ison", "is_on", "state"):
                if kk in v:
                    vals3.append(_coerce(v.get(kk)))
                    break
        if any(v is True for v in vals3):
            return True
        if any(v is False for v in vals3):
            return False
    except Exception:
        pass

    return None


class ActionDispatcher:
    """Handles web dashboard actions independently of tkinter.

    This is a direct extraction of LiveWebMixin._web_action_dispatch with
    self.xxx references replaced by explicit attributes.
    """

    def __init__(
        self,
        cfg: AppConfig,
        storage: Storage,
        live_store: Optional[LiveStateStore],
        *,
        out_dir: Path,
        cfg_path: Optional[Path] = None,
        lang: str = "de",
    ) -> None:
        self.cfg = cfg
        self.storage = storage
        self.live_store = live_store
        self.out_dir = out_dir
        self.cfg_path = cfg_path
        self.lang = lang
        self._live_frozen_state = False
        # Anomaly log (populated externally or by on-demand detection)
        self._anomaly_log: list = []
        # Cache of computed devices (loaded on demand)
        self._computed: Dict[str, ComputedDevice] = {}
        self._computed_lock = threading.Lock()
        self._computed_ts: float = 0.0  # time.time() when cache was populated
        self._computed_ttl: float = 120.0  # auto-refresh every 2 min
        # Mapping text from last _wva_series call (debug aid)
        self._last_wva_mapping_text = ""

    # ------------------------------------------------------------------
    # Lazy computed device cache
    # ------------------------------------------------------------------

    @property
    def computed(self) -> Dict[str, ComputedDevice]:
        """Lazy-load computed devices from storage, auto-refresh after TTL."""
        import time as _time
        with self._computed_lock:
            now = _time.time()
            if not self._computed or (now - self._computed_ts) > self._computed_ttl:
                self._computed.clear()
                for d in self.cfg.devices:
                    try:
                        self._computed[d.key] = load_device(self.storage, d)
                    except Exception:
                        pass
                self._computed_ts = now
            return self._computed

    def reload(self, cfg: AppConfig, lang: Optional[str] = None) -> None:
        """Hot-reload configuration."""
        self.cfg = cfg
        if lang is not None:
            self.lang = lang
        with self._computed_lock:
            self._computed.clear()

    # ------------------------------------------------------------------
    # i18n helper
    # ------------------------------------------------------------------

    def t(self, key: str, **kw) -> str:
        return _t(self.lang, key, **kw)

    # ------------------------------------------------------------------
    # Tariff helpers (ported from core.py)
    # ------------------------------------------------------------------

    def _is_dynamic_tariff(self) -> bool:
        spot_cfg = getattr(self.cfg, "spot_price", None)
        if not spot_cfg or not getattr(spot_cfg, "enabled", False):
            return False
        return str(getattr(spot_cfg, "tariff_type", "fixed")) == "dynamic"

    def _get_effective_unit_price(self) -> float:
        """Return the effective unit price in EUR/kWh for the current hour."""
        if self._is_dynamic_tariff():
            now_ts = int(time.time())
            hour_ts = (now_ts // 3600) * 3600
            spot_cfg = getattr(self.cfg, "spot_price", None)
            zone = getattr(spot_cfg, "bidding_zone", "DE-LU") or "DE-LU"
            markup = float(spot_cfg.total_markup_ct()) / 100.0
            pricing = getattr(self.cfg, "pricing", None)
            vat_rate = pricing.vat_rate() if getattr(spot_cfg, "include_vat", True) and pricing else 0.0
            try:
                df = self.storage.db.query_spot_prices(zone, hour_ts, hour_ts + 3600)
                if not df.empty:
                    raw_eur = float(df["price_eur_mwh"].mean()) / 1000.0
                    return (raw_eur + markup) * (1.0 + vat_rate)
            except Exception:
                pass
        pricing = getattr(self.cfg, "pricing", None)
        if pricing:
            return float(pricing.unit_price_gross())
        return 0.30

    # ------------------------------------------------------------------
    # Compare helpers (ported from compare.py)
    # ------------------------------------------------------------------

    def _cmp_load_daily(
        self,
        device_key: str,
        from_date: date,
        to_date: date,
        use_eur: bool,
        price_kwh: float,
    ) -> Dict[str, float]:
        """Load daily kWh/EUR totals for *device_key* between two dates."""
        try:
            start_ts = int(datetime(from_date.year, from_date.month, from_date.day).timestamp())
            to_plus = to_date + timedelta(days=1)
            end_ts = int(datetime(to_plus.year, to_plus.month, to_plus.day).timestamp())

            def _aggregate(ts_series, kwh_series) -> Dict[str, float]:
                local_dates = ts_series.apply(
                    lambda ts: datetime.fromtimestamp(int(ts)).date()
                )
                tmp = pd.DataFrame({"_date": local_dates, "_kwh": kwh_series.fillna(0.0)})
                daily = tmp.groupby("_date")["_kwh"].sum()
                out: Dict[str, float] = {}
                for d, kwh in daily.items():
                    if from_date <= d <= to_date:
                        val = float(kwh) * price_kwh if use_eur else float(kwh)
                        out[d.strftime("%Y-%m-%d")] = max(0.0, val)
                return out

            try:
                hourly_df = self.storage.db.query_hourly(
                    device_key, start_ts=start_ts, end_ts=end_ts
                )
                if hourly_df is not None and not hourly_df.empty and "kwh" in hourly_df.columns:
                    result = _aggregate(hourly_df["hour_ts"], hourly_df["kwh"])
                    if result:
                        return result
            except Exception as e:
                logger.debug("_cmp_load_daily hourly path error for '%s': %s", device_key, e)

            df = self.storage.read_device_df(device_key, start_ts=start_ts, end_ts=end_ts)
            if df is None or df.empty or "energy_kwh" not in df.columns:
                return {}

            ts_col = "timestamp" if "timestamp" in df.columns else "ts"
            df = df.copy()
            for col_name in ("timestamp", "ts"):
                if col_name not in df.columns:
                    continue
                col = df[col_name]
                if hasattr(col.dtype, "tz") or pd.api.types.is_datetime64_any_dtype(col):
                    df[col_name] = col.astype("int64") // 10 ** 9

            return _aggregate(df[ts_col], df["energy_kwh"])

        except Exception as e:
            logger.warning("_cmp_load_daily error for '%s': %s", device_key, e, exc_info=True)
            return {}

    def _cmp_load_daily_spot(
        self,
        device_key: str,
        from_date: date,
        to_date: date,
    ) -> Dict[str, float]:
        """Load daily spot-price costs for a device between two dates."""
        try:
            spot_cfg = getattr(self.cfg, "spot_price", None)
            if not spot_cfg or not getattr(spot_cfg, "enabled", False):
                return {}

            zone = getattr(spot_cfg, "bidding_zone", "DE-LU") or "DE-LU"
            markup = float(spot_cfg.total_markup_ct() if hasattr(spot_cfg, "total_markup_ct") else getattr(spot_cfg, "markup_ct_per_kwh", 16.0)) / 100.0
            pricing = getattr(self.cfg, "pricing", None)
            if getattr(spot_cfg, "include_vat", True) and pricing:
                vat_rate = pricing.vat_rate()
            else:
                vat_rate = 0.0

            db = self.storage.db
            out: Dict[str, float] = {}
            d = from_date
            while d <= to_date:
                start_ts = int(datetime(d.year, d.month, d.day).timestamp())
                end_ts = start_ts + 86400
                cost, kwh, avg = db.calc_spot_cost(
                    device_key, zone, start_ts, end_ts, markup, vat_rate
                )
                if cost > 0:
                    out[d.strftime("%Y-%m-%d")] = cost
                d += timedelta(days=1)
            return out
        except Exception as e:
            logger.warning("_cmp_load_daily_spot error: %s", e, exc_info=True)
            return {}

    def _cmp_align_daily(
        self,
        daily_a: Dict[str, float],
        from_a: date,
        to_a: date,
        daily_b: Dict[str, float],
        from_b: date,
        to_b: date,
    ) -> Tuple[List[float], List[float], List[str]]:
        n_a = (to_a - from_a).days + 1
        n_b = (to_b - from_b).days + 1
        n = max(n_a, n_b)
        vals_a, vals_b, x_labels = [], [], []
        for i in range(n):
            d_a = from_a + timedelta(days=i)
            d_b = from_b + timedelta(days=i)
            va = daily_a.get(d_a.strftime("%Y-%m-%d"), 0.0) if i < n_a else 0.0
            vb = daily_b.get(d_b.strftime("%Y-%m-%d"), 0.0) if i < n_b else 0.0
            vals_a.append(va)
            vals_b.append(vb)
            x_labels.append(f"+{i}d")
        return vals_a, vals_b, x_labels

    def _cmp_align_weekly(
        self,
        daily_a: Dict[str, float],
        from_a: date,
        to_a: date,
        daily_b: Dict[str, float],
        from_b: date,
        to_b: date,
    ) -> Tuple[List[float], List[float], List[str]]:
        def _weekly(daily: Dict[str, float], from_d: date, to_d: date):
            w: Dict[Tuple[int, int], float] = defaultdict(float)
            d = from_d
            while d <= to_d:
                iso = d.isocalendar()
                w[(iso[0], iso[1])] += daily.get(d.strftime("%Y-%m-%d"), 0.0)
                d += timedelta(days=1)
            return dict(w)

        w_a = _weekly(daily_a, from_a, to_a)
        w_b = _weekly(daily_b, from_b, to_b)
        keys_a = sorted(w_a)
        keys_b = sorted(w_b)
        n = max(len(keys_a), len(keys_b), 1)
        vals_a, vals_b, x_labels = [], [], []
        for i in range(n):
            ka = keys_a[i] if i < len(keys_a) else None
            kb = keys_b[i] if i < len(keys_b) else None
            vals_a.append(w_a.get(ka, 0.0) if ka else 0.0)
            vals_b.append(w_b.get(kb, 0.0) if kb else 0.0)
            lbl = f"W{ka[1]:02d}" if ka else (f"W{kb[1]:02d}" if kb else "–")
            x_labels.append(lbl)
        return vals_a, vals_b, x_labels

    def _cmp_align_monthly(
        self,
        daily_a: Dict[str, float],
        from_a: date,
        to_a: date,
        daily_b: Dict[str, float],
        from_b: date,
        to_b: date,
    ) -> Tuple[List[float], List[float], List[str]]:
        def _monthly(daily: Dict[str, float], from_d: date, to_d: date):
            m: Dict[Tuple[int, int], float] = defaultdict(float)
            d = from_d
            while d <= to_d:
                m[(d.year, d.month)] += daily.get(d.strftime("%Y-%m-%d"), 0.0)
                d += timedelta(days=1)
            return dict(m)

        m_a = _monthly(daily_a, from_a, to_a)
        m_b = _monthly(daily_b, from_b, to_b)
        keys_a = sorted(m_a)
        keys_b = sorted(m_b)
        n = max(len(keys_a), len(keys_b))
        vals_a, vals_b, x_labels = [], [], []
        for i in range(n):
            ka = keys_a[i] if i < len(keys_a) else None
            kb = keys_b[i] if i < len(keys_b) else None
            vals_a.append(m_a.get(ka, 0.0) if ka else 0.0)
            vals_b.append(m_b.get(kb, 0.0) if kb else 0.0)
            lbl = f"{ka[0]}-{ka[1]:02d}" if ka else (f"{kb[0]}-{kb[1]:02d}" if kb else "–")
            x_labels.append(lbl)
        return vals_a, vals_b, x_labels

    # ------------------------------------------------------------------
    # _stats_series (ported from core.py)
    # ------------------------------------------------------------------

    def _stats_series(self, df: Optional[pd.DataFrame], mode: str) -> Tuple[List[str], List[float]]:
        """Return (labels, kWh_values) for stats bar charts."""
        mode = str(mode or "days").lower().strip()
        unit = mode
        limit_n = None
        if ':' in mode:
            try:
                unit, n_raw = mode.split(':', 1)
                unit = unit.strip()
                limit_n = int(float(n_raw.strip()))
                if limit_n <= 0:
                    limit_n = None
            except Exception:
                unit = mode
                limit_n = None
        if df is None or df.empty:
            return ([], [])
        try:
            df2 = calculate_energy(df, method="auto")
        except Exception:
            return ([], [])

        s = pd.to_numeric(df2.get("energy_kwh"), errors="coerce").fillna(0.0)
        ts = pd.to_datetime(df2.get("timestamp"), errors="coerce")
        # Stored timestamps are UTC (tz-stripped by query_samples). Convert to
        # local tz (Europe/Berlin) so hour/day/week/month buckets line up with
        # what the user sees on their clock.
        try:
            _tz_local = "Europe/Berlin"
            if getattr(ts, "dt", None) is not None:
                if ts.dt.tz is None:
                    ts = ts.dt.tz_localize("UTC").dt.tz_convert(_tz_local).dt.tz_localize(None)
                else:
                    ts = ts.dt.tz_convert(_tz_local).dt.tz_localize(None)
        except Exception:
            pass
        tmp = pd.DataFrame({"timestamp": ts, "energy_kwh": s}).dropna(subset=["timestamp"]).sort_values("timestamp")
        if tmp.empty:
            return ([], [])
        tmp = tmp.set_index("timestamp")

        if unit == "all":
            total = float(tmp["energy_kwh"].sum())
            return (["Total"], [total])

        if unit == "hours":
            hr = tmp["energy_kwh"].resample("h").sum()
            if limit_n is not None:
                hr = hr.tail(int(limit_n))
            labels = [pd.Timestamp(x).strftime("%Y-%m-%d %H:00") for x in hr.index]
            return (labels, [float(v) for v in hr.values])

        if unit == "weeks":
            wk = tmp["energy_kwh"].resample("W-MON").sum()
            if limit_n is not None:
                wk = wk.tail(int(limit_n))
            labels = [f"{int(x.isocalendar().year)}-W{int(x.isocalendar().week):02d}" for x in wk.index]
            return (labels, [float(v) for v in wk.values])

        if unit == "months":
            mo = tmp["energy_kwh"].resample("MS").sum()
            if limit_n is not None:
                mo = mo.tail(int(limit_n))
            labels = [pd.Timestamp(x).strftime("%Y-%m") for x in mo.index]
            return (labels, [float(v) for v in mo.values])

        # default: days
        day = tmp["energy_kwh"].resample("D").sum()
        if limit_n is not None:
            day = day.tail(int(limit_n))
        labels = [pd.Timestamp(x).strftime("%Y-%m-%d") for x in day.index]
        return (labels, [float(v) for v in day.values])

    # ------------------------------------------------------------------
    # _wva_series (ported from core.py — W/V/A timeseries for web plots)
    # ------------------------------------------------------------------

    def _wva_series(self, df: pd.DataFrame, metric: str) -> Tuple[pd.Series, str]:
        if df is None or df.empty:
            return pd.Series(dtype=float), metric

        metric_u = (metric or '').upper()
        cols_lower = {c.lower(): c for c in df.columns}

        # Timestamp source
        if 'timestamp' in cols_lower:
            ts_raw = df[cols_lower['timestamp']]
            if pd.api.types.is_numeric_dtype(ts_raw):
                ts_num = pd.to_numeric(ts_raw, errors='coerce')
                mx = float(ts_num.dropna().max()) if ts_num.notna().any() else 0.0
                unit = 's'
                if mx > 1e15:
                    unit = 'ns'
                elif mx > 1e12:
                    unit = 'ms'
                ts = pd.to_datetime(ts_num, unit=unit, errors='coerce')
            else:
                ts = pd.to_datetime(ts_raw, errors='coerce')
        elif isinstance(df.index, pd.DatetimeIndex):
            ts = pd.to_datetime(df.index, errors='coerce')
        elif 'ts' in cols_lower:
            ts_num = pd.to_numeric(df[cols_lower['ts']], errors='coerce')
            mx = float(ts_num.dropna().max()) if ts_num.notna().any() else 0.0
            unit = 's'
            if mx > 1e15:
                unit = 'ns'
            elif mx > 1e12:
                unit = 'ms'
            elif mx > 1e9:
                unit = 's'
            ts = pd.to_datetime(ts_num, unit=unit, errors='coerce')
        else:
            return pd.Series(dtype=float), metric

        def first_col(candidates):
            for n in candidates:
                key = n.lower()
                if key in cols_lower:
                    return cols_lower[key]
            return None

        def phase_cols(kind: str, phase_tokens=None):
            out = []
            phase_re = re.compile(r"(^|_)(l1|l2|l3|phase1|phase2|phase3|phase_a|phase_b|phase_c|a|b|c)(_|$)")
            for c in df.columns:
                cl = str(c).lower()
                if not phase_re.search(cl):
                    continue
                if any(bad in cl for bad in ['price', 'kwh', 'energy', 'cost', 'total_kwh']):
                    continue
                if kind == 'power':
                    if ('power' in cl) or ('watt' in cl) or ('active_power' in cl) or ('apower' in cl) or cl.endswith('_p') or cl.startswith('p_'):
                        out.append(c)
                elif kind == 'voltage':
                    if ('voltage' in cl) or ('volt' in cl) or cl == 'u' or cl.startswith('u_') or cl.endswith('_u'):
                        out.append(c)
                elif kind == 'current':
                    if ('current' in cl) or ('amp' in cl) or ('amps' in cl) or cl == 'i' or cl.startswith('i_') or cl.endswith('_i'):
                        if 'angle' not in cl:
                            out.append(c)
            return out

        def _collapse_phase_stat_cols(cols, kind: str):
            def _phase_label(col: str):
                cl = str(col).lower()
                for token, lab in (
                    ('phase_a', 'L1'), ('phase_b', 'L2'), ('phase_c', 'L3'),
                    ('phase1', 'L1'), ('phase2', 'L2'), ('phase3', 'L3'),
                    ('l1', 'L1'), ('l2', 'L2'), ('l3', 'L3'),
                ):
                    if re.search(rf'(^|_)({token})(_|$)', cl):
                        return lab
                if cl.startswith('a_') or re.search(r'(^|_)(a)(_|$)', cl):
                    return 'L1'
                if cl.startswith('b_') or re.search(r'(^|_)(b)(_|$)', cl):
                    return 'L2'
                if cl.startswith('c_') or re.search(r'(^|_)(c)(_|$)', cl):
                    return 'L3'
                return None

            groups = {'L1': [], 'L2': [], 'L3': []}
            for c in cols:
                lab = _phase_label(c)
                if lab in groups:
                    groups[lab].append(c)

            if kind == 'current':
                avg_tag, max_tag, min_tag = 'avg_current', 'max_current', 'min_current'
                plain_re_local = re.compile(r'(^|_)current(_|$)')
            elif kind == 'voltage':
                avg_tag, max_tag, min_tag = 'avg_voltage', 'max_voltage', 'min_voltage'
                plain_re_local = re.compile(r'(^|_)voltage(_|$)|(^|_)volt(_|$)|(^|_)u(_|$)')
            elif kind == 'power':
                avg_tag, max_tag, min_tag = 'avg_power', 'max_power', 'min_power'
                plain_re_local = re.compile(r'(^|_)(act_power|active_power|apower|power|watts|w)(_|$)')
            else:
                avg_tag, max_tag, min_tag = 'avg', 'max', 'min'
                plain_re_local = re.compile(r'.*')

            def _num(col):
                return pd.to_numeric(df[col], errors='coerce')

            out_c = {}
            for lab, cols2 in groups.items():
                if not cols2:
                    continue

                if kind == "power":
                    try:
                        cols_act = [
                            c for c in cols2
                            if (
                                ("act_power" in str(c).lower())
                                or ("active_power" in str(c).lower())
                                or (("apower" in str(c).lower()) and ("aprt" not in str(c).lower()))
                            )
                            and ("aprt" not in str(c).lower())
                            and ("apparent" not in str(c).lower())
                        ]
                        cols_non_apparent = [c for c in cols2 if ("aprt" not in str(c).lower()) and ("apparent" not in str(c).lower())]
                        if cols_act:
                            cols2 = cols_act
                        elif cols_non_apparent:
                            cols2 = cols_non_apparent
                    except Exception:
                        pass

                    def _is_active_power(cl: str) -> bool:
                        cl = cl or ""
                        if ("aprt" in cl) or ("apparent" in cl):
                            return False
                        return ("act_power" in cl) or ("active_power" in cl) or (("apower" in cl) and ("aprt" not in cl)) or bool(re.search(r"(^|_)power(_|$)", cl))

                    try:
                        cols_active = [c for c in cols2 if _is_active_power(str(c).lower())]
                        if cols_active:
                            cols2 = cols_active
                    except Exception:
                        pass

                    cavg = next((c for c in cols2 if ("avg" in str(c).lower() or "_mean" in str(c).lower()) and _is_active_power(str(c).lower())), None)
                    if cavg is not None:
                        out_c[lab] = _num(cavg)
                        continue

                    cmax = next((c for c in cols2 if ("max" in str(c).lower()) and _is_active_power(str(c).lower())), None)
                    cmin = next((c for c in cols2 if ("min" in str(c).lower()) and _is_active_power(str(c).lower())), None)
                    if cmax is not None and cmin is not None:
                        out_c[lab] = (_num(cmax) + _num(cmin)) / 2.0
                        continue

                    cplain = next((c for c in cols2 if _is_active_power(str(c).lower()) and not any(tag in str(c).lower() for tag in ["avg", "max", "min"])), None)
                    if cplain is not None:
                        out_c[lab] = _num(cplain)
                        continue

                cols2_l = [str(c).lower() for c in cols2]
                for c in cols2:
                    if avg_tag in str(c).lower():
                        out_c[lab] = _num(c)
                        break
                if lab in out_c:
                    continue

                plain = []
                for c in cols2:
                    cl = str(c).lower()
                    if (avg_tag in cl) or (max_tag in cl) or (min_tag in cl):
                        continue
                    if plain_re_local.search(cl):
                        plain.append(c)
                if plain:
                    out_c[lab] = _num(plain[0])
                    continue

                cmax_f = None
                cmin_f = None
                for c in cols2:
                    cl = str(c).lower()
                    if (max_tag in cl) and cmax_f is None:
                        cmax_f = c
                    elif (min_tag in cl) and cmin_f is None:
                        cmin_f = c
                if cmax_f is not None and cmin_f is not None:
                    out_c[lab] = (_num(cmax_f) + _num(cmin_f)) / 2.0
                elif cmax_f is not None:
                    out_c[lab] = _num(cmax_f)
                elif cmin_f is not None:
                    out_c[lab] = _num(cmin_f)

            return out_c

        ylab = metric_u
        mapping_text = ""

        if metric_u == 'W':
            ylab = 'W'
            power_col = first_col([
                'power_total_w',
                'total_act_power', 'total_active_power', 'total_apower',
                'total_power', 'total_w', 'total_power_w', 'total_act_power_w',
                'total_act_power_ph', 'total_power_ph',
                'avg_power', 'avg_active_power', 'avg_act_power', 'avg_apower', 'avg_w',
                'active_power', 'act_power', 'apower',
                'power', 'watts', 'w', 'power_w'
            ])
            if power_col:
                y = pd.to_numeric(df[power_col], errors='coerce')
                mapping_text = f"W: total={power_col}"
            else:
                pcs = phase_cols(
                    'power',
                    ['l1', 'l2', 'l3', 'phase1', 'phase2', 'phase3', 'phase_a', 'phase_b', 'phase_c', 'a_', '_a', 'b_', '_b', 'c_', '_c'],
                )
                if not pcs:
                    for c in df.columns:
                        cl = c.lower()
                        if ('power' in cl or cl.startswith('p')) and any(tag in cl for tag in ['1', '2', '3']):
                            if not any(bad in cl for bad in ['price', 'kwh', 'energy']):
                                pcs.append(c)
                if pcs:
                    collapsed = _collapse_phase_stat_cols(pcs, 'power')
                    if collapsed:
                        num = pd.DataFrame(collapsed)
                        y = num.sum(axis=1, min_count=1)
                        try:
                            mapping_text = "W: phases=" + ", ".join(
                                [f"{k}:{v.name if hasattr(v,'name') and v.name else '<calc>'}" for k, v in collapsed.items()]
                            ) + " | sum"
                        except Exception:
                            mapping_text = "W: phases=" + ", ".join(list(collapsed.keys())) + " | sum"
                    else:
                        num = df[pcs].apply(lambda s: pd.to_numeric(s, errors='coerce'))
                        y = num.sum(axis=1, min_count=1)
                        mapping_text = "W: sum(" + ", ".join(map(str, pcs)) + ")"
                else:
                    vcol = first_col(['avg_voltage', 'voltage', 'u', 'v', 'total_voltage'])
                    icol = first_col(['avg_current', 'total_current', 'current', 'amps', 'a', 'i'])
                    if vcol and icol:
                        y = pd.to_numeric(df[vcol], errors='coerce') * pd.to_numeric(df[icol], errors='coerce')
                        mapping_text = f"W: {vcol}*{icol}"
                    else:
                        y = pd.Series([0.0] * len(df), index=df.index)
                        mapping_text = "W: (no columns)"

        elif metric_u in {'VAR', 'Q', 'COSPHI', 'PF', 'POWERFACTOR'}:
            want_pf = metric_u in {'COSPHI', 'PF', 'POWERFACTOR'}
            ylab = 'cos \u03c6' if want_pf else 'VAR'

            def _num_series(colname: str) -> pd.Series:
                return pd.to_numeric(df[colname], errors='coerce')

            def _active_total_series() -> Tuple[pd.Series, str]:
                col = first_col([
                    'power_total_w',
                    'total_act_power', 'total_active_power', 'total_apower',
                    'total_power', 'total_w', 'total_power_w', 'total_act_power_w',
                    'avg_power', 'avg_active_power', 'avg_act_power', 'avg_apower', 'avg_w',
                    'active_power', 'act_power', 'apower',
                    'power', 'watts', 'w', 'power_w',
                ])
                if col:
                    return _num_series(col), f"P(total)={col}"
                pcs_a = phase_cols('power', ['l1','l2','l3','phase1','phase2','phase3','phase_a','phase_b','phase_c','a_','_a','b_','_b','c_','_c'])
                if pcs_a:
                    collapsed = _collapse_phase_stat_cols(pcs_a, 'power')
                    if collapsed:
                        num = pd.DataFrame(collapsed)
                        return num.sum(axis=1, min_count=1), "P(phases)=" + ", ".join(list(collapsed.keys()))
                    num = df[pcs_a].apply(lambda s: pd.to_numeric(s, errors='coerce'))
                    return num.sum(axis=1, min_count=1), "P(sum)=" + ", ".join(map(str, pcs_a))
                return pd.Series([0.0]*len(df), index=df.index, dtype="float64"), "P(no cols)"

            def _apparent_total_series() -> Tuple[pd.Series, str]:
                col = first_col([
                    'total_aprt_power', 'total_apparent_power', 'total_va', 'va_total',
                    'aprt_power', 'apparent_power', 'va',
                ])
                if col:
                    return _num_series(col), f"S(total)={col}"

                pcs_s = [c for c in df.columns if (
                    re.search(r'(^|_)(l1|l2|l3|phase1|phase2|phase3|phase_a|phase_b|phase_c|a|b|c)(_|$)', str(c).lower())
                    and (('aprt_power' in str(c).lower()) or ('apparent' in str(c).lower()) or re.search(r'(^|_)va(_|$)', str(c).lower()))
                )]

                def _collapse_apparent(cols_ap) -> Dict[str, pd.Series]:
                    def _phase_label_ap(c):
                        cl = str(c).lower()
                        for token, lab in (
                            ('phase_a','L1'),('phase_b','L2'),('phase_c','L3'),
                            ('phase1','L1'),('phase2','L2'),('phase3','L3'),
                            ('l1','L1'),('l2','L2'),('l3','L3'),
                        ):
                            if re.search(rf'(^|_)({token})(_|$)', cl):
                                return lab
                        if cl.startswith('a_') or re.search(r'(^|_)(a)(_|$)', cl): return 'L1'
                        if cl.startswith('b_') or re.search(r'(^|_)(b)(_|$)', cl): return 'L2'
                        if cl.startswith('c_') or re.search(r'(^|_)(c)(_|$)', cl): return 'L3'
                        return None

                    groups_ap={'L1':[], 'L2':[], 'L3':[]}
                    for c in cols_ap:
                        lab=_phase_label_ap(c)
                        if lab in groups_ap:
                            groups_ap[lab].append(c)

                    out_ap={}
                    for lab, cols2 in groups_ap.items():
                        if not cols2:
                            continue
                        cavg = next((c for c in cols2 if 'avg' in str(c).lower()), None)
                        if cavg is not None:
                            out_ap[lab] = pd.to_numeric(df[cavg], errors='coerce')
                            continue
                        cmax = next((c for c in cols2 if 'max' in str(c).lower()), None)
                        cmin = next((c for c in cols2 if 'min' in str(c).lower()), None)
                        if cmax is not None and cmin is not None:
                            out_ap[lab] = (pd.to_numeric(df[cmax], errors='coerce') + pd.to_numeric(df[cmin], errors='coerce'))/2.0
                            continue
                        cplain = next((c for c in cols2 if not any(t in str(c).lower() for t in ['avg','max','min'])), None)
                        if cplain is not None:
                            out_ap[lab] = pd.to_numeric(df[cplain], errors='coerce')
                            continue
                        out_ap[lab] = pd.to_numeric(df[cols2[0]], errors='coerce')
                    return out_ap

                collapsed_s = _collapse_apparent(pcs_s) if pcs_s else {}
                if collapsed_s:
                    num = pd.DataFrame(collapsed_s)
                    return num.sum(axis=1, min_count=1), "S(phases)=" + ", ".join(list(collapsed_s.keys()))
                if pcs_s:
                    num = df[pcs_s].apply(lambda s: pd.to_numeric(s, errors='coerce'))
                    return num.sum(axis=1, min_count=1), "S(sum)=" + ", ".join(map(str, pcs_s))
                return pd.Series([0.0]*len(df), index=df.index, dtype="float64"), "S(no cols)"

            def _reactive_sign_series() -> Tuple[pd.Series, str]:
                lag_col = first_col(['total_lag_react_energy','lag_react_energy','lag_reactive_energy'])
                lead_col = first_col(['total_lead_react_energy','lead_react_energy','lead_reactive_energy'])
                if lag_col and lead_col:
                    lag = pd.to_numeric(df[lag_col], errors='coerce')
                    lead = pd.to_numeric(df[lead_col], errors='coerce')
                    src = f"sign: d({lag_col})-d({lead_col})"
                else:
                    lag_cols = [c for c in df.columns if ('lag_react_energy' in str(c).lower())]
                    lead_cols = [c for c in df.columns if ('lead_react_energy' in str(c).lower())]
                    if lag_cols and lead_cols:
                        lag = df[lag_cols].apply(lambda s: pd.to_numeric(s, errors='coerce')).sum(axis=1, min_count=1)
                        lead = df[lead_cols].apply(lambda s: pd.to_numeric(s, errors='coerce')).sum(axis=1, min_count=1)
                        src = "sign: d(lag_sum)-d(lead_sum)"
                    else:
                        return pd.Series([1.0]*len(df), index=df.index, dtype="float64"), "sign: default(+)"
                d = lag.diff().fillna(0.0) - lead.diff().fillna(0.0)
                sgn = d.apply(lambda x: 1.0 if x > 0 else (-1.0 if x < 0 else float('nan')))
                sgn = sgn.ffill().fillna(1.0)
                return sgn, src

            P, mapP = _active_total_series()
            S, mapS = _apparent_total_series()
            sgn, mapSign = _reactive_sign_series()
            Qmag = (S.astype('float64')**2 - P.astype('float64')**2).clip(lower=0.0) ** 0.5
            Q = sgn * Qmag

            if want_pf:
                denom = (P.astype('float64')**2 + Q.astype('float64')**2) ** 0.5
                pf = (P.abs() / denom).replace([np.inf, -np.inf], np.nan).fillna(0.0)
                pf = pf.clip(lower=0.0, upper=1.0)
                y = pf
                mapping_text = f"cos\u03c6: {mapP}; {mapS}; {mapSign}"
            else:
                y = Q
                mapping_text = f"VAR: {mapP}; {mapS}; {mapSign}"

            if "S(no cols)" in mapping_text or mapS == "S(no cols)":
                if want_pf:
                    _pf_col = first_col(["cosphi_total", "pf_total", "power_factor_total", "cos_phi_total", "cosphi", "pf"])
                    if _pf_col:
                        y = pd.to_numeric(df[_pf_col], errors="coerce")
                        mapping_text = f"cos\u03c6: direct={_pf_col}"
                    else:
                        _pfa = first_col(["pfa", "pf_a", "cosphi_a", "a_cosphi", "a_pf"])
                        _pfb = first_col(["pfb", "pf_b", "cosphi_b", "b_cosphi", "b_pf"])
                        _pfc = first_col(["pfc", "pf_c", "cosphi_c", "c_cosphi", "c_pf"])
                        _pf_cols = [c for c in (_pfa, _pfb, _pfc) if c]
                        if _pf_cols:
                            _num_pf = pd.to_numeric(df[_pf_cols[0]], errors="coerce")
                            if len(_pf_cols) > 1:
                                for _c in _pf_cols[1:]:
                                    _num_pf = _num_pf + pd.to_numeric(df[_c], errors="coerce")
                                _num_pf = _num_pf / float(len(_pf_cols))
                            y = _num_pf
                            mapping_text = f"cos\u03c6: mean({','.join(_pf_cols)})"
                else:
                    _q_col = first_col(["q_total_var", "reactive_var_total", "total_reactive_var", "var_total", "q_total"])
                    if _q_col:
                        y = pd.to_numeric(df[_q_col], errors="coerce")
                        mapping_text = f"VAR: direct={_q_col}"
                    else:
                        _qa = first_col(["qa", "q_a", "reactive_var_a", "a_reactive_var", "a_var", "a_q"])
                        _qb = first_col(["qb", "q_b", "reactive_var_b", "b_reactive_var", "b_var", "b_q"])
                        _qc = first_col(["qc", "q_c", "reactive_var_c", "c_reactive_var", "c_var", "c_q"])
                        _q_cols = [c for c in (_qa, _qb, _qc) if c]
                        if _q_cols:
                            _num_q = pd.to_numeric(df[_q_cols[0]], errors="coerce")
                            for _c in _q_cols[1:]:
                                _num_q = _num_q + pd.to_numeric(df[_c], errors="coerce")
                            y = _num_q
                            mapping_text = f"VAR: sum({','.join(_q_cols)})"

        elif metric_u == 'V':
            ylab = 'V'
            vcol = first_col(['avg_voltage', 'voltage', 'u', 'v', 'total_voltage'])
            if vcol:
                y = pd.to_numeric(df[vcol], errors='coerce')
                mapping_text = f"V: total={vcol}"
            else:
                vcols = phase_cols(
                    'voltage',
                    ['l1', 'l2', 'l3', 'phase1', 'phase2', 'phase3', 'phase_a', 'phase_b', 'phase_c', 'a_', '_a', 'b_', '_b', 'c_', '_c'],
                )
                if vcols:
                    collapsed = _collapse_phase_stat_cols(vcols, 'voltage')
                    if collapsed:
                        num = pd.DataFrame(collapsed)
                        try:
                            mapping_text = "V: phases=" + ", ".join([f"{k}:{v.name if hasattr(v,'name') and v.name else '<calc>'}" for k, v in collapsed.items()])
                        except Exception:
                            mapping_text = "V: phases=" + ", ".join([f"{k}" for k in collapsed.keys()])
                    else:
                        num = df[vcols].apply(lambda s: pd.to_numeric(s, errors='coerce'))
                        mapping_text = "V: mean(" + ", ".join(map(str, vcols)) + ")"
                    if num.shape[1] > 1:
                        num = num.mask(num.abs() < 1e-6)
                    y = num.mean(axis=1, skipna=True)
                else:
                    y = pd.Series([0.0] * len(df), index=df.index)
                    mapping_text = "V: (no columns)"

        elif metric_u == 'A':
            ylab = 'A'

            icols = phase_cols(
                'current',
                ['l1', 'l2', 'l3', 'phase1', 'phase2', 'phase3', 'phase_a', 'phase_b', 'phase_c', 'a_', '_a', 'b_', '_b', 'c_', '_c'],
            )
            if not icols:
                for c in df.columns:
                    cl = c.lower()
                    if ('current' in cl or cl.startswith('i')) and any(tag in cl for tag in ['1', '2', '3']):
                        if not any(bad in cl for bad in ['price', 'kwh', 'energy']):
                            icols.append(c)

            if icols:
                collapsed = _collapse_phase_stat_cols(icols, 'current')
                if collapsed:
                    num = pd.DataFrame(collapsed)
                    try:
                        mapping_text = "A: phases=" + ", ".join([f"{k}:{v.name if hasattr(v,'name') and v.name else '<calc>'}" for k, v in collapsed.items()])
                    except Exception:
                        mapping_text = "A: phases=" + ", ".join([f"{k}" for k in collapsed.keys()])
                else:
                    num = df[icols].apply(lambda s: pd.to_numeric(s, errors='coerce'))
                    mapping_text = "A: sum(" + ", ".join(map(str, icols)) + ")"

                dup_phases = False
                true_multiphase = False

                try:
                    p_total_col = first_col(['total_power', 'power', 'watts', 'w', 'active_power', 'apower', 'power_w'])
                    pcs_dup = phase_cols(
                        'power',
                        ['l1', 'l2', 'l3', 'phase1', 'phase2', 'phase3', 'phase_a', 'phase_b', 'phase_c', 'a_', '_a', 'b_', '_b', 'c_', '_c'],
                    )
                    if p_total_col and pcs_dup and len(pcs_dup) >= 2:
                        p_total = pd.to_numeric(df[p_total_col], errors='coerce')
                        p_num = df[pcs_dup].apply(lambda s: pd.to_numeric(s, errors='coerce'))
                        p_sum = p_num.sum(axis=1, min_count=1)
                        p_mean = p_num.mean(axis=1, skipna=True)

                        denom_d = p_total.abs().where(p_total.abs() > 1e-6)
                        rel_sum = (p_total - p_sum).abs() / denom_d
                        rel_mean = (p_total - p_mean).abs() / denom_d

                        m_sum = rel_sum.dropna().median() if rel_sum.notna().any() else float('nan')
                        m_mean = rel_mean.dropna().median() if rel_mean.notna().any() else float('nan')

                        if pd.notna(m_sum) and m_sum < 0.08:
                            true_multiphase = True

                        if (not true_multiphase) and pd.notna(m_mean) and pd.notna(m_sum):
                            if (m_mean < 0.05) and (m_mean < m_sum * 0.5):
                                dup_phases = True

                    if (not dup_phases) and (not true_multiphase) and num.shape[1] >= 2:
                        base = num.iloc[:, 0]
                        diffs = []
                        for j in range(1, num.shape[1]):
                            other = num.iloc[:, j]
                            denom_dd = base.abs().combine(other.abs(), max)
                            denom_dd = denom_dd.where(denom_dd > 1e-6)
                            diffs.append(((base - other).abs() / denom_dd))
                        if diffs:
                            rel_d = pd.concat(diffs, axis=1).median(axis=1, skipna=True)
                            rel_d = rel_d.dropna()
                            if len(rel_d) >= 20 and pd.notna(rel_d.median()) and float(rel_d.median()) < 0.02:
                                dup_phases = True

                        if not dup_phases:
                            row_max = num.max(axis=1, skipna=True)
                            row_min = num.min(axis=1, skipna=True)
                            denom_rr = row_max.abs().where(row_max.abs() > 1e-6)
                            rr = ((row_max - row_min).abs() / denom_rr).dropna()
                            if len(rr) >= 20 and pd.notna(rr.median()) and float(rr.median()) < 0.02:
                                dup_phases = True

                except Exception:
                    dup_phases = False
                    true_multiphase = False

                if dup_phases:
                    y = num.max(axis=1, skipna=True)
                    mapping_text += " | dup->max"
                else:
                    y = num.sum(axis=1, min_count=1)
                    mapping_text += " | sum"

            else:
                icol = first_col(['avg_current', 'total_current', 'current', 'amps', 'a', 'i'])
                if icol:
                    y = pd.to_numeric(df[icol], errors='coerce')
                    mapping_text = f"A: total={icol}"
                else:
                    y = pd.Series([0.0] * len(df), index=df.index)
                    mapping_text = "A: (no columns)"

        elif metric_u in {"HZ", "FREQ", "FREQUENCY"}:
            ylab = "Hz"
            fq_col = first_col(["freq_hz", "avg_freq_hz", "frequency", "freq", "hz"])
            if fq_col:
                y = pd.to_numeric(df[fq_col], errors="coerce")
                # Grid frequency is never 0 – treat 0 as missing (NaN) so old
                # rows without Hz data appear as gaps instead of a 0-line.
                y = y.where(y > 1.0)
                mapping_text = f"Hz: {fq_col}"
            else:
                y = pd.Series([float("nan")] * len(df), index=df.index)
                mapping_text = "Hz(no cols)"

        else:
            y = pd.Series([0.0] * len(df), index=df.index)

        # Ensure y length matches index ts (defensive – early returns may leave
        # y empty while ts has df-length)
        if len(y) != len(ts):
            y = pd.Series([float("nan")] * len(ts), index=range(len(ts)))
        out = pd.Series(y.to_numpy(), index=ts, name=metric_u)
        try:
            out.index = pd.to_datetime(out.index, errors='coerce')
        except Exception:
            pass
        # Convert stored UTC index → local time (Europe/Berlin) so timeseries
        # x-axis shows user-clock hours.
        try:
            idx = out.index
            if isinstance(idx, pd.DatetimeIndex):
                if idx.tz is None:
                    out.index = idx.tz_localize("UTC").tz_convert("Europe/Berlin").tz_localize(None)
                else:
                    out.index = idx.tz_convert("Europe/Berlin").tz_localize(None)
        except Exception:
            pass
        try:
            out = out[~pd.isna(out.index)]
        except Exception:
            pass
        out = out.sort_index()
        if out.index.has_duplicates:
            out = out.groupby(level=0).mean()
        self._last_wva_mapping_text = str(mapping_text)
        return out, ylab

    # ------------------------------------------------------------------
    # _wva_phase_series (ported from core.py)
    # ------------------------------------------------------------------

    def _wva_phase_series(self, df: pd.DataFrame, metric: str) -> Dict[str, pd.Series]:
        """Return per-phase series for W/V/A if available."""
        out: Dict[str, pd.Series] = {}
        if df is None or df.empty:
            return out

        metric_u = (metric or "").upper().strip()
        if metric_u not in {"W", "V", "A", "VAR", "Q", "COSPHI", "PF", "POWERFACTOR"}:
            return out

        # Derived metrics: Reactive power (VAR) and power factor (cos phi)
        if metric_u in {"VAR", "Q", "COSPHI", "PF", "POWERFACTOR"}:
            want_pf = metric_u in {"COSPHI", "PF", "POWERFACTOR"}

            try:
                cols_lut2 = {str(c).lower(): c for c in df.columns}
                direct_map = {
                    "L1": (["pfa", "pf_a", "cosphi_a", "a_cosphi", "a_pf"] if want_pf else ["qa", "q_a", "reactive_var_a", "a_reactive_var", "a_var", "a_q"]),
                    "L2": (["pfb", "pf_b", "cosphi_b", "b_cosphi", "b_pf"] if want_pf else ["qb", "q_b", "reactive_var_b", "b_reactive_var", "b_var", "b_q"]),
                    "L3": (["pfc", "pf_c", "cosphi_c", "c_cosphi", "c_pf"] if want_pf else ["qc", "q_c", "reactive_var_c", "c_reactive_var", "c_var", "c_q"]),
                }
                for lab, cands in direct_map.items():
                    col = None
                    for cc in cands:
                        if str(cc).lower() in cols_lut2:
                            col = cols_lut2[str(cc).lower()]
                            break
                    if col is not None:
                        out[lab] = pd.to_numeric(df[col], errors="coerce")
                if out:
                    return out
            except Exception:
                pass

            cols_all = list(df.columns)
            cols_lut = {str(c).lower(): c for c in cols_all}

            PHASES = [
                ("L1", ["a", "l1", "phase_a", "phase1"]),
                ("L2", ["b", "l2", "phase_b", "phase2"]),
                ("L3", ["c", "l3", "phase_c", "phase3"]),
            ]

            def _first_existing(cands):
                for cc in cands:
                    ccl = str(cc).lower()
                    if ccl in cols_lut:
                        return cols_lut[ccl]
                return None

            def _num(col):
                return pd.to_numeric(df[col], errors="coerce")

            def _series_for_phase(token_list, kind: str) -> Tuple[Optional[pd.Series], str]:
                avg_cands = []
                max_cands = []
                min_cands = []
                plain_cands = []
                for t in token_list:
                    if kind == "act":
                        avg_cands += [f"{t}_avg_act_power", f"{t}_avg_active_power", f"{t}_avg_power", f"{t}_act_power_avg", f"avg_act_power_{t}"]
                        max_cands += [f"{t}_max_act_power", f"{t}_max_active_power", f"{t}_max_power"]
                        min_cands += [f"{t}_min_act_power", f"{t}_min_active_power", f"{t}_min_power"]
                        plain_cands += [f"{t}_act_power", f"{t}_active_power", f"{t}_power", f"act_power_{t}", f"active_power_{t}", f"power_{t}"]
                    else:
                        avg_cands += [f"{t}_avg_aprt_power", f"{t}_avg_apparent_power", f"{t}_aprt_power_avg", f"avg_aprt_power_{t}", f"avg_apparent_power_{t}"]
                        max_cands += [f"{t}_max_aprt_power", f"{t}_max_apparent_power"]
                        min_cands += [f"{t}_min_aprt_power", f"{t}_min_apparent_power"]
                        plain_cands += [f"{t}_aprt_power", f"{t}_apparent_power", f"aprt_power_{t}", f"apparent_power_{t}"]
                    if kind == "act":
                        plain_cands += [f"{t}_act_power", f"{t}_active_power"]
                    else:
                        plain_cands += [f"{t}_aprt_power", f"{t}_apparent_power"]

                c_avg = _first_existing(avg_cands)
                if c_avg:
                    return _num(c_avg), f"avg:{c_avg}"
                c_max = _first_existing(max_cands)
                c_min = _first_existing(min_cands)
                if c_max and c_min:
                    return (_num(c_max) + _num(c_min)) / 2.0, f"(max+min)/2:{c_max},{c_min}"
                c_plain = _first_existing(plain_cands)
                if c_plain:
                    return _num(c_plain), f"plain:{c_plain}"
                return None, "missing"

            def _reactive_sign_for_phase(token_list) -> Tuple[pd.Series, str]:
                lag_cands = []
                lead_cands = []
                for t in token_list:
                    lag_cands += [f"{t}_lag_react_energy", f"{t}_lag_reactive_energy", f"lag_react_energy_{t}", f"lag_reactive_energy_{t}"]
                    lead_cands += [f"{t}_lead_react_energy", f"{t}_lead_reactive_energy", f"lead_react_energy_{t}", f"lead_reactive_energy_{t}"]
                c_lag = _first_existing(lag_cands)
                c_lead = _first_existing(lead_cands)
                if c_lag and c_lead:
                    d = (_num(c_lag).diff() - _num(c_lead).diff())
                    s = d.apply(lambda x: 1.0 if pd.isna(x) or x == 0 else (1.0 if x > 0 else -1.0))
                    s = s.replace(0, np.nan).ffill().fillna(1.0)
                    return s, f"sign:diff({c_lag}-{c_lead})"
                return pd.Series(1.0, index=df.index, dtype=float), "sign:default(+)"

            for lab, tokens in PHASES:
                P_ph, mapP = _series_for_phase(tokens, "act")
                S_ph, mapS = _series_for_phase(tokens, "aprt")
                if P_ph is None or S_ph is None:
                    continue
                S_abs = S_ph.abs()
                pf_ph = (P_ph.abs() / S_abs).replace([np.inf, -np.inf], np.nan)
                pf_ph = pf_ph.clip(lower=0.0, upper=1.0)
                if want_pf:
                    out[lab] = pf_ph
                else:
                    mag = np.sqrt(np.maximum((S_ph.astype(float) ** 2) - (P_ph.astype(float) ** 2), 0.0))
                    sgn, mapSign = _reactive_sign_for_phase(tokens)
                    q = pd.Series(mag, index=P_ph.index, dtype=float) * pd.to_numeric(sgn, errors="coerce").fillna(1.0)
                    out[lab] = q

            return out

        # W / V / A
        cand = []
        kind = None
        avg_tag = max_tag = min_tag = None
        cols_all = list(df.columns)

        if metric_u == "W":
            kind = "power"
            cand = [
                c for c in cols_all
                if (
                    ("power" in str(c).lower())
                    or ("apower" in str(c).lower())
                    or re.search(r"(^|_)w(_|$)", str(c).lower())
                    or ("watt" in str(c).lower())
                )
            ]
        elif metric_u == "V":
            kind = "voltage"
            avg_tag, max_tag, min_tag = "avg_voltage", "max_voltage", "min_voltage"
            cand = [
                c for c in cols_all
                if (
                    ("voltage" in str(c).lower())
                    or re.search(r"(^|_)v(_|$)", str(c).lower())
                    or ("volt" in str(c).lower())
                )
            ]
        else:  # A
            kind = "current"
            avg_tag, max_tag, min_tag = "avg_current", "max_current", "min_current"
            cand = [
                c for c in cols_all
                if (
                    ("current" in str(c).lower())
                    or re.search(r"(^|_)a(_|$)", str(c).lower())
                    or ("amp" in str(c).lower())
                )
            ]

        if not cand:
            return out

        phase_re_p = re.compile(r"(^|_)(l1|l2|l3|phase1|phase2|phase3|phase_a|phase_b|phase_c|a|b|c)(_|$)")

        def _phase_label(col: str):
            cl = str(col).lower()
            for token, lab in (
                ("phase_a", "L1"), ("phase_b", "L2"), ("phase_c", "L3"),
                ("phase1", "L1"), ("phase2", "L2"), ("phase3", "L3"),
                ("l1", "L1"), ("l2", "L2"), ("l3", "L3"),
                ("a", "L1"), ("b", "L2"), ("c", "L3"),
            ):
                if re.search(rf"(^|_)({token})(_|$)", cl):
                    return lab
            return None

        groups: Dict[str, list] = {"L1": [], "L2": [], "L3": []}
        for c in cand:
            cl = str(c).lower()
            if not phase_re_p.search(cl):
                continue
            lab = _phase_label(cl)
            if lab in groups:
                groups[lab].append(c)

        def _num_ph(col):
            return pd.to_numeric(df.get(col), errors="coerce")

        def _is_avg(cl: str) -> bool:
            if kind == "power":
                return ("avg" in cl) and ("power" in cl or "act_power" in cl or "active_power" in cl or "apower" in cl or bool(re.search(r"(^|_)w(_|$)", cl)))
            return avg_tag in cl if avg_tag else False

        def _is_max(cl: str) -> bool:
            if kind == "power":
                return ("max" in cl) and ("power" in cl)
            return max_tag in cl if max_tag else False

        def _is_min(cl: str) -> bool:
            if kind == "power":
                return ("min" in cl) and ("power" in cl)
            return min_tag in cl if min_tag else False

        for lab, cols_g in groups.items():
            if not cols_g:
                continue

            if kind == "power":
                try:
                    cols_act = [
                        c for c in cols_g
                        if (
                            ("act_power" in str(c).lower())
                            or ("active_power" in str(c).lower())
                            or (("apower" in str(c).lower()) and ("aprt" not in str(c).lower()))
                        )
                        and ("aprt" not in str(c).lower())
                        and ("apparent" not in str(c).lower())
                    ]
                    cols_non_apparent = [c for c in cols_g if ("aprt" not in str(c).lower()) and ("apparent" not in str(c).lower())]
                    if cols_act:
                        cols_g = cols_act
                    elif cols_non_apparent:
                        cols_g = cols_non_apparent
                except Exception:
                    pass

            chosen = None
            for c in cols_g:
                if _is_avg(str(c).lower()):
                    chosen = c
                    break
            if chosen is not None:
                out[lab] = _num_ph(chosen)
                continue

            plain = []
            for c in cols_g:
                cl = str(c).lower()
                if _is_avg(cl) or _is_max(cl) or _is_min(cl):
                    continue
                plain.append(c)
            if plain:
                out[lab] = _num_ph(plain[0])
                continue

            cmax = None
            cmin = None
            for c in cols_g:
                cl = str(c).lower()
                if _is_max(cl) and cmax is None:
                    cmax = c
                elif _is_min(cl) and cmin is None:
                    cmin = c
            if cmax is not None and cmin is not None:
                out[lab] = (_num_ph(cmax) + _num_ph(cmin)) / 2.0
            elif cmax is not None:
                out[lab] = _num_ph(cmax)
            elif cmin is not None:
                out[lab] = _num_ph(cmin)

        # Ensure per-phase series are time-indexed
        try:
            if "timestamp" in df.columns:
                ts_idx = pd.to_datetime(df["timestamp"], errors="coerce")
            elif "ts" in df.columns:
                ts_idx = pd.to_datetime(df["ts"], errors="coerce")
            else:
                ts_idx = None
            if ts_idx is not None:
                ts_idx = pd.DatetimeIndex(ts_idx)
                msk = ~pd.isna(ts_idx)
                for kk in list(out.keys()):
                    s = out.get(kk)
                    if s is None:
                        continue
                    try:
                        s2 = pd.Series(pd.to_numeric(s, errors="coerce").to_numpy(), index=ts_idx)
                        s2 = s2[msk]
                        s2 = s2.dropna().sort_index()
                        if s2.index.has_duplicates:
                            s2 = s2.groupby(level=0).mean()
                        out[kk] = s2
                    except Exception:
                        pass
        except Exception:
            pass

        # Add neutral conductor current for "A" metric
        if kind == "current":
            if "L1" in out and "L2" in out and "L3" in out:
                try:
                    ia = pd.to_numeric(out["L1"], errors="coerce").fillna(0.0)
                    ib = pd.to_numeric(out["L2"], errors="coerce").fillna(0.0)
                    ic = pd.to_numeric(out["L3"], errors="coerce").fillna(0.0)
                    n_calc = np.sqrt(np.maximum(ia**2 + ib**2 + ic**2 - ia*ib - ia*ic - ib*ic, 0.0))
                    out["N"] = n_calc
                except Exception:
                    pass

        result = {k: out[k] for k in ("L1", "L2", "L3") if k in out}
        if "N" in out:
            result["N"] = out["N"]
        return result

    # ==================================================================
    # MAIN DISPATCH
    # ==================================================================

    def dispatch(
        self,
        action: str,
        params: Dict[str, Any],
        progress: Optional[Callable[[str, int, int, str], None]] = None,
    ) -> Dict[str, Any]:
        """Handle a web action -- direct copy of _web_action_dispatch logic."""
        action = str(action or "").strip()
        params = params if isinstance(params, dict) else {}
        out_root = (self.out_dir / "exports").resolve()
        out_root.mkdir(parents=True, exist_ok=True)

        def _pdate(x: Any) -> Optional[pd.Timestamp]:
            try:
                return _parse_date_flexible(str(x or "").strip())
            except Exception:
                return None

        # --- Plotly plots data (JSON) ---
        if action == "plots_data":
            return self._web_plots_data(params)

        # --- Live Freeze ---
        if action in {"get_freeze", "set_freeze", "toggle_freeze"}:
            cur = bool(self._live_frozen_state)
            if action == "get_freeze":
                return {"ok": True, "freeze": cur}

            if action == "set_freeze":
                if "freeze" in params:
                    desired = bool(params.get("freeze"))
                elif "on" in params:
                    desired = bool(params.get("on"))
                else:
                    return {"ok": False, "error": "missing freeze"}
                self._live_frozen_state = desired
                return {"ok": True, "freeze": bool(desired)}

            # toggle
            desired = (not cur)
            self._live_frozen_state = desired
            return {"ok": True, "freeze": bool(desired)}

        # --- Switch control (Gen2/Plus/Pro) ---
        if action in {"get_switch", "set_switch", "toggle_switch"}:
            device_key = str(params.get("device_key") or "").strip()
            dev = next((d for d in self.cfg.devices if d.key == device_key), None)
            if dev is None:
                return {"ok": False, "error": "unknown device"}
            if str(getattr(dev, "kind", "")) != "switch":
                return {"ok": False, "error": "not a switch"}

            http = ShellyHttp(
                HttpConfig(
                    timeout_seconds=float(self.cfg.download.timeout_seconds),
                    retries=int(self.cfg.download.retries),
                    backoff_base_seconds=float(self.cfg.download.backoff_base_seconds),
                )
            )

            try:
                st = get_switch_status(http, dev.host, int(dev.em_id))
                cur_on = _extract_switch_on(st)
                if cur_on is not True:
                    try:
                        full = get_shelly_status(http, dev.host)
                        any_on = _extract_switch_on(full)
                        if any_on is True:
                            cur_on = True
                    except Exception:
                        pass
            except Exception as e:
                return {"ok": False, "error": str(e)}
            if action == "get_switch":
                return {"ok": True, "on": bool(cur_on)}

            target_on: Optional[bool] = None
            if action == "set_switch":
                if "on" not in params:
                    return {"ok": False, "error": "missing on"}
                target_on = bool(params.get("on"))
            else:
                target_on = (not bool(cur_on))

            try:
                set_switch_state(http, dev.host, int(dev.em_id), bool(target_on))
                st2 = get_switch_status(http, dev.host, int(dev.em_id))
                on2 = _extract_switch_on(st2)
                return {"ok": True, "on": bool(on2)}
            except Exception as e:
                return {"ok": False, "error": str(e)}

        if action == "sync":
            mode = str(params.get("mode") or "incremental")
            start_date = str(params.get("start_date") or "").strip()
            now = int(time.time())
            range_override: Optional[Tuple[int, int]] = None
            label = mode
            if mode == "custom":
                if not start_date:
                    raise ValueError("custom requires start_date (TT.MM.JJJJ)")
                dt = datetime.strptime(start_date, "%d.%m.%Y").replace(tzinfo=ZoneInfo("Europe/Berlin"))
                a = int(dt.timestamp())
                b = now
                if b <= a:
                    b = a + 1
                range_override = (a, b)
                label = f"ab {start_date}"
            elif mode == "day":
                range_override = (max(0, now - 86400), now)
            elif mode == "week":
                range_override = (max(0, now - 7 * 86400), now)
            elif mode == "month":
                range_override = (max(0, now - 30 * 86400), now)
            else:
                range_override = None

            results = sync_all(
                self.cfg,
                self.storage,
                range_override=range_override,
                fallback_last_days=7,
                progress=progress,
            )
            summary = []
            for r in results:
                ok_chunks = sum(1 for c in r.chunks if c.ok)
                err = next((c for c in r.chunks if not c.ok), None)
                summary.append(
                    {
                        "device": r.device_name,
                        "range": [int(r.requested_range[0]), int(r.requested_range[1])],
                        "ok_chunks": int(ok_chunks),
                        "total_chunks": int(len(r.chunks)),
                        "error": err.error if err else None,
                        "updated_last_end_ts": r.updated_last_end_ts,
                    }
                )
            return {"ok": True, "mode": mode, "label": label, "results": summary}

        if action == "plots":
            mode = str(params.get("mode") or "days")
            start = _pdate(params.get("start"))
            end = _pdate(params.get("end"))
            if start is not None and end is not None and end < start:
                start, end = end, start
            from matplotlib.figure import Figure

            def _series(df_s: pd.DataFrame, m: str) -> Tuple[List[str], List[float]]:
                if df_s is None or df_s.empty:
                    return [], []
                if m == "all":
                    total = float(pd.to_numeric(df_s["energy_kwh"], errors="coerce").fillna(0.0).sum())
                    return ["Total"], [total]
                if m == "days":
                    s = daily_kwh(df_s)
                    return [pd.Timestamp(x).strftime("%Y-%m-%d") for x in s.index], [float(v) for v in s.values]
                if m == "weeks":
                    s = weekly_kwh(df_s)
                    return [str(x) for x in s.index], [float(v) for v in s.values]
                if m == "months":
                    s = monthly_kwh(df_s)
                    return [str(x) for x in s.index], [float(v) for v in s.values]
                return [], []

            def _apply_xticks(ax, labels_t: List[str]) -> None:
                if not labels_t:
                    return
                n = len(labels_t)
                max_labels = 40
                if n <= max_labels:
                    ax.set_xticks(range(n))
                    ax.set_xticklabels(labels_t, rotation=45, ha="right")
                    return
                step = int(math.ceil(n / max_labels))
                ticks = list(range(0, n, step))
                ax.set_xticks(ticks)
                ax.set_xticklabels([labels_t[i] for i in ticks], rotation=45, ha="right")

            ts_str = time.strftime("%Y%m%d_%H%M%S")
            web_dir = out_root / "web"
            web_dir.mkdir(parents=True, exist_ok=True)
            files: List[Dict[str, str]] = []

            devs2 = list(self.cfg.devices[:2])
            total = max(1, len(devs2))
            for idx, d in enumerate(devs2, start=1):
                if progress:
                    try:
                        progress(d.key, idx-1, total, f"Plot {mode} \u2026")
                    except Exception:
                        pass
                cd = load_device(self.storage, d)
                df_use = filter_by_time(cd.df, start=start, end=end)
                labels_p, values = _series(df_use, mode)
                fig = Figure(figsize=(11, 3.6), dpi=170)
                ax = fig.add_subplot(111)
                ax.set_ylabel("kWh")
                bars = ax.bar(range(len(values)), values)
                _apply_xticks(ax, labels_p)
                ax.grid(True, axis="y", alpha=0.3)
                for b in bars:
                    try:
                        h = float(b.get_height())
                    except Exception:
                        continue
                    ax.annotate(
                        f"{h:.2f}",
                        xy=(b.get_x() + b.get_width() / 2, h),
                        xytext=(0, 3),
                        textcoords="offset points",
                        ha="center",
                        va="bottom",
                        fontsize=8,
                        rotation=90,
                    )
                rng = ""
                if start is not None or end is not None:
                    a_s = start.date().isoformat() if start is not None else "\u2026"
                    b_s = end.date().isoformat() if end is not None else "\u2026"
                    rng = f" | {a_s}\u2013{b_s}"
                fig.suptitle(f"{d.name} \u2013 {mode}{rng}", fontsize=12)
                fig.tight_layout()
                safe = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in d.name).strip("_")
                out_p = web_dir / f"plot_{safe or d.key}_{mode}_{ts_str}.png"
                export_figure_png(fig, out_p, dpi=180)
                files.append({"name": out_p.name, "url": f"/files/web/{out_p.name}"})
                if progress:
                    try:
                        progress(d.key, idx, total, "OK")
                    except Exception:
                        pass

            return {"ok": True, "files": files}

        if action == "export_summary":
            start = _pdate(params.get("start"))
            end = _pdate(params.get("end"))
            if start is not None and end is not None and end < start:
                start, end = end, start

            _tz_s = ZoneInfo("Europe/Berlin")

            if start is not None:
                start_d = pd.Timestamp(start).date()
            else:
                start_d = date.today().replace(day=1)
            if end is not None:
                end_d = pd.Timestamp(end).date()
            else:
                end_d = date.today()
            end_excl = end_d + timedelta(days=1)
            span_days = (end_excl - start_d).days

            is_monthly = span_days > 2
            report_type = "monthly" if is_monthly else "daily"

            prev_span = timedelta(days=span_days)
            prev_start_d = start_d - prev_span
            prev_end_d = start_d

            start_dt = datetime.combine(start_d, datetime.min.time(), tzinfo=_tz_s)
            end_dt = datetime.combine(end_excl, datetime.min.time(), tzinfo=_tz_s)
            prev_start_dt = datetime.combine(prev_start_d, datetime.min.time(), tzinfo=_tz_s)
            prev_end_dt = datetime.combine(prev_end_d, datetime.min.time(), tzinfo=_tz_s)

            # Skip _build_email_report_data (tkinter-dependent); use fallback path
            report_data = None

            ts_str = time.strftime("%Y%m%d_%H%M%S")
            out_f = out_root / "web" / f"summary_{ts_str}.pdf"

            if report_data is not None:
                from shelly_analyzer.services.export import export_pdf_email_daily, export_pdf_email_monthly
                if is_monthly:
                    export_pdf_email_monthly(report_data, out_f, lang=self.lang)
                else:
                    export_pdf_email_daily(report_data, out_f, lang=self.lang)
            else:
                unit_gross = float(self.cfg.pricing.unit_price_gross())
                totals: List[ReportTotals] = []
                for d in self.cfg.devices:
                    cd = load_device(self.storage, d)
                    df_s = filter_by_time(cd.df, start=pd.Timestamp(start_d), end=pd.Timestamp(end_excl))
                    kwh, avgp, maxp = summarize(df_s)
                    totals.append(ReportTotals(name=d.name, kwh_total=kwh, cost_eur=kwh * unit_gross, avg_power_w=avgp, max_power_w=maxp))
                export_pdf_summary(
                    title=self.t("pdf.summary.title"),
                    period_label=f"{start_d} \u2013 {end_d}",
                    totals=totals,
                    out_path=out_f,
                    lang=self.lang,
                )
            return {"ok": True, "files": [{"name": out_f.name, "url": f"/files/web/{out_f.name}"}]}

        if action == "export_invoices":
            start = _pdate(params.get("start"))
            end = _pdate(params.get("end"))
            period = str(params.get("period") or "custom")
            anchor = _pdate(params.get("anchor"))
            if period != "custom":
                if anchor is None and start is not None:
                    anchor = start
                if anchor is None:
                    anchor = pd.Timestamp(date.today())
                start, end = _period_bounds(anchor, period)

            inv_dir = out_root / "web" / "invoices"
            inv_dir.mkdir(parents=True, exist_ok=True)
            unit_net = float(self.cfg.pricing.unit_price_net())
            issue = date.today()
            due = issue + timedelta(days=int(self.cfg.billing.payment_terms_days))
            ts_str = time.strftime("%Y%m%d")
            files: List[Dict[str, str]] = []
            for d in self.cfg.devices[:2]:
                cd = load_device(self.storage, d)
                df_inv = filter_by_time(cd.df, start=start, end=end)
                kwh, _avgp, _maxp = summarize(df_inv)
                if start is None and end is None:
                    period_label = self.t("period.all")
                    suffix = "all"
                else:
                    period_label = f"{format_date_local(self.lang, start) if start is not None else '\u2026'} {self.t('common.to')} {format_date_local(self.lang, end) if end is not None else '\u2026'}"
                    if period == "day" and start is not None:
                        suffix = start.strftime("%Y%m%d")
                    elif period == "week" and start is not None:
                        iso = start.isocalendar()
                        suffix = f"W{iso.week:02d}{iso.year}"
                    elif period == "month" and start is not None:
                        suffix = start.strftime("%Y%m")
                    elif period == "year" and start is not None:
                        suffix = start.strftime("%Y")
                    else:
                        suffix = f"{(start.date().isoformat() if start is not None else 'x')}-{(end.date().isoformat() if end is not None else 'y')}"

                invoice_no = f"{self.cfg.billing.invoice_prefix}-{ts_str}-{d.key}-{period}-{suffix}"
                safe = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in d.name).strip("_")
                out_inv = inv_dir / f"invoice_{invoice_no}_{safe or d.key}.pdf"
                line = InvoiceLine(
                    description=self.t("pdf.invoice.line_energy", device=d.name, period=period_label),
                    quantity=float(kwh),
                    unit="kWh",
                    unit_price_net=unit_net,
                )
                lines = [line]
                try:
                    base_year = float(getattr(self.cfg.pricing, 'base_fee_eur_per_year', 0.0))
                except Exception:
                    base_year = 0.0
                if base_year > 0:
                    if start is None and end is None:
                        if not df_inv.empty:
                            s_eff = pd.Timestamp(df_inv['timestamp'].min()).normalize()
                            e_eff = pd.Timestamp(df_inv['timestamp'].max()).normalize()
                        else:
                            s_eff = pd.Timestamp(date.today()).normalize()
                            e_eff = s_eff
                    else:
                        s_eff = pd.Timestamp(start).normalize() if start is not None else pd.Timestamp(df_inv['timestamp'].min()).normalize()
                        e_eff = pd.Timestamp(end).normalize() if end is not None else pd.Timestamp(df_inv['timestamp'].max()).normalize()
                    days = int((e_eff.date() - s_eff.date()).days) + 1
                    days = max(1, days)
                    base_day_net = float(self.cfg.pricing.base_fee_day_net())
                    lines.append(InvoiceLine(description=self.t("pdf.invoice.line_base_fee", days=days), quantity=float(days), unit=self.t("unit.days"), unit_price_net=base_day_net))
                export_pdf_invoice(
                    out_path=out_inv,
                    invoice_no=invoice_no,
                    issue_date=issue,
                    due_date=due,
                    issuer={
                        "name": self.cfg.billing.issuer.name,
                        "address_lines": self.cfg.billing.issuer.address_lines,
                        "vat_id": self.cfg.billing.issuer.vat_id,
                        "email": self.cfg.billing.issuer.email,
                        "phone": self.cfg.billing.issuer.phone,
                        "iban": self.cfg.billing.issuer.iban,
                        "bic": self.cfg.billing.issuer.bic,
                    },
                    customer={
                        "name": self.cfg.billing.customer.name,
                        "address_lines": self.cfg.billing.customer.address_lines,
                    },
                    lines=lines,
                    vat_rate_percent=float(self.cfg.pricing.vat_rate_percent),
                    vat_enabled=bool(self.cfg.pricing.vat_enabled),
                    lang=self.lang,
                    logo_path=getattr(self.cfg.billing, "invoice_logo_path", ""),
                )
                files.append({"name": out_inv.name, "url": f"/files/web/invoices/{out_inv.name}"})

            return {"ok": True, "files": files}

        if action == "export_excel":
            start = _pdate(params.get("start"))
            end = _pdate(params.get("end"))
            if start is not None and end is not None and end < start:
                start, end = end, start

            ts_str = time.strftime("%Y%m%d_%H%M%S")
            web_dir = out_root / "web"
            web_dir.mkdir(parents=True, exist_ok=True)
            sheets: Dict[str, Any] = {}
            for d in self.cfg.devices[:2]:
                cd = load_device(self.storage, d)
                df_ex = filter_by_time(cd.df, start=start, end=end)
                if not df_ex.empty:
                    sheets[d.name[:31]] = df_ex
            out_xl = web_dir / f"export_{ts_str}.xlsx"
            export_to_excel(sheets, out_xl)
            return {"ok": True, "files": [{"name": out_xl.name, "url": f"/files/web/{out_xl.name}"}]}

        # --- Widget data (compact JSON for iOS Scriptable) ---
        if action == "widget":
            try:
                _tz = ZoneInfo("Europe/Berlin")
                _now = datetime.now(_tz)
                _today_start = _now.replace(hour=0, minute=0, second=0, microsecond=0)
                _month_start = _today_start.replace(day=1)

                try:
                    _unit = float(self.cfg.pricing.effective_pricing_for_date(date.today()).unit_price_gross())
                except Exception:
                    _unit = float(getattr(getattr(self.cfg, "pricing", None), "electricity_price_eur_per_kwh", 0.30) or 0.30)
                _fixed_ct = round(_unit * 100, 2)

                _all_devs = [d for d in (self.cfg.devices or [])
                             if str(getattr(d, "kind", "em")) != "switch"]
                _widget_keys = str(getattr(self.cfg.ui, "widget_devices", "") or "").strip()
                if _widget_keys:
                    _allowed = {k.strip() for k in _widget_keys.split(",") if k.strip()}
                    _three_phase = [d for d in _all_devs if d.key in _allowed]
                else:
                    _three_phase = _all_devs
                _today_kwh = 0.0
                _month_kwh = 0.0
                _per_device = []
                for d in _three_phase:
                    try:
                        cd = self.computed.get(d.key)
                        if cd is None:
                            continue
                        df_w = cd.df.copy()
                        if "timestamp" not in df_w.columns:
                            continue
                        df_w["timestamp"] = pd.to_datetime(df_w["timestamp"], errors="coerce")
                        df_w = df_w.dropna(subset=["timestamp"])
                        try:
                            if df_w["timestamp"].dt.tz is None:
                                df_w["timestamp"] = df_w["timestamp"].dt.tz_localize("UTC")
                            df_w["timestamp"] = df_w["timestamp"].dt.tz_convert(_tz)
                        except Exception:
                            pass
                        _e = pd.to_numeric(df_w["energy_kwh"], errors="coerce").fillna(0.0)
                        m_today = (df_w["timestamp"] >= _today_start) & (df_w["timestamp"] < _now)
                        _d_today = float(_e.loc[m_today].sum())
                        _today_kwh += _d_today
                        m_month = (df_w["timestamp"] >= _month_start) & (df_w["timestamp"] < _now)
                        _d_month = float(_e.loc[m_month].sum())
                        _month_kwh += _d_month
                        _d_power = 0.0
                        try:
                            if "total_power" in df_w.columns:
                                _lp = df_w["total_power"].dropna()
                                if not _lp.empty:
                                    _d_power = float(_lp.iloc[-1])
                        except Exception:
                            pass
                        _per_device.append({
                            "key": d.key, "name": d.name,
                            "power_w": round(_d_power, 0),
                            "today_kwh": round(_d_today, 3),
                            "today_eur": round(_d_today * _unit, 2),
                            "month_kwh": round(_d_month, 3),
                            "month_eur": round(_d_month * _unit, 2),
                        })
                    except Exception:
                        pass

                _today_kwh = round(_today_kwh, 3)
                _month_kwh = round(_month_kwh, 3)
                _today_eur = round(_today_kwh * _unit, 2)
                _month_eur = round(_month_kwh * _unit, 2)

                _dim = calendar.monthrange(_now.year, _now.month)[1]
                _elapsed = max(1, (_now - _month_start).total_seconds() / 86400.0)
                _proj_kwh = round(_month_kwh / _elapsed * _dim, 1)
                _proj_eur = round(_proj_kwh * _unit, 2)

                _widget_dev_keys = {d.key for d in _three_phase}
                _power_w = 0.0
                try:
                    snap = self.live_store.snapshot()
                    for _dk, _pts in snap.items():
                        if _dk in _widget_dev_keys and isinstance(_pts, list) and _pts:
                            _power_w += float(_pts[-1].get("power_total_w", 0) or 0)
                except Exception:
                    pass
                if _power_w == 0:
                    try:
                        for d in _three_phase:
                            cd = self.computed.get(d.key)
                            if cd and hasattr(cd, "df") and "total_power" in cd.df.columns:
                                _last_p = cd.df["total_power"].dropna()
                                if not _last_p.empty:
                                    _power_w += float(_last_p.iloc[-1])
                    except Exception:
                        pass

                _spot_cfg = getattr(self.cfg, "spot_price", None)
                _spot_enabled = getattr(_spot_cfg, "enabled", False) if _spot_cfg else False
                _current_spot_ct = None
                _spot_today_eur = None
                _spot_chart_mini = []
                if _spot_enabled:
                    try:
                        _sp_zone = getattr(_spot_cfg, "bidding_zone", "DE-LU") or "DE-LU"
                        _sp_markup = float(_spot_cfg.total_markup_ct() if hasattr(_spot_cfg, "total_markup_ct") else 16.0)
                        _sp_vat = self.cfg.pricing.vat_rate() if getattr(_spot_cfg, "include_vat", True) else 0.0

                        _now_ts_w = int(time.time())
                        _cur_h = (_now_ts_w // 3600) * 3600
                        _df_cur = self.storage.db.query_spot_prices(_sp_zone, _cur_h, _cur_h + 3600)
                        if not _df_cur.empty:
                            _raw = float(_df_cur["price_eur_mwh"].mean()) / 10.0
                            _current_spot_ct = round((_raw + _sp_markup) * (1.0 + _sp_vat), 1)

                        _sp_mk_eur = _sp_markup / 100.0
                        try:
                            _sc, _, _ = self.storage.db.calc_spot_cost(
                                "", _sp_zone,
                                int(_today_start.timestamp()), int(_now.timestamp()),
                                _sp_mk_eur, _sp_vat
                            )
                            if _sc > 0:
                                _spot_today_eur = round(_sc, 2)
                        except Exception:
                            pass

                        _chart_s = int((_now - timedelta(hours=12)).timestamp())
                        _chart_e = int((_now + timedelta(hours=12)).timestamp())
                        _df_sp = self.storage.db.query_spot_prices(_sp_zone, _chart_s, _chart_e)
                        if not _df_sp.empty:
                            _df_sp["hour_ts"] = (_df_sp["slot_ts"] // 3600) * 3600
                            _df_h = _df_sp.groupby("hour_ts").agg(price=("price_eur_mwh", "mean")).reset_index()
                            _df_h = _df_h.sort_values("hour_ts")
                            for _, _r in _df_h.iterrows():
                                _raw_ct = float(_r["price"]) / 10.0
                                _total_ct = (_raw_ct + _sp_markup) * (1.0 + _sp_vat)
                                _spot_chart_mini.append([int(_r["hour_ts"]), round(_total_ct, 1)])
                    except Exception:
                        pass

                _co2_enabled = False
                _co2_current = None
                _co2_chart_mini = []
                _co2_green_thr = 150.0
                _co2_dirty_thr = 400.0
                try:
                    _co2_cfg_w = getattr(self.cfg, "co2", None)
                    if _co2_cfg_w and getattr(_co2_cfg_w, "enabled", False):
                        _co2_enabled = True
                        _co2_zone_w = str(getattr(_co2_cfg_w, "bidding_zone", "DE_LU") or "DE_LU")
                        _co2_green_thr = float(getattr(_co2_cfg_w, "green_threshold_g_per_kwh", 150.0))
                        _co2_dirty_thr = float(getattr(_co2_cfg_w, "dirty_threshold_g_per_kwh", 400.0))
                        _now_ts_co2 = int(time.time())
                        # Query last 24h of CO2 data (historical only – no forecasts)
                        _co2_chart_s = int((_now - timedelta(hours=24)).timestamp())
                        _co2_chart_e = _now_ts_co2 + 3600
                        _df_co2_ch = self.storage.db.query_co2_intensity(_co2_zone_w, _co2_chart_s, _co2_chart_e)
                        if _df_co2_ch is not None and not _df_co2_ch.empty:
                            _df_co2_ch = _df_co2_ch.sort_values("hour_ts")
                            for _, _r_co2 in _df_co2_ch.iterrows():
                                _co2_chart_mini.append([int(_r_co2["hour_ts"]), round(float(_r_co2["intensity_g_per_kwh"]), 0)])
                            # Current = most recent available value in last 24h (handles missed fetches)
                            _co2_current = _co2_chart_mini[-1][1]
                except Exception:
                    pass

                return {
                    "ok": True,
                    "ts": int(_now.timestamp()),
                    "power_w": round(_power_w, 0),
                    "today_kwh": _today_kwh,
                    "today_eur": _today_eur,
                    "month_kwh": _month_kwh,
                    "month_eur": _month_eur,
                    "proj_kwh": _proj_kwh,
                    "proj_eur": _proj_eur,
                    "fixed_ct": _fixed_ct,
                    "spot_enabled": _spot_enabled,
                    "spot_ct": _current_spot_ct,
                    "spot_today_eur": _spot_today_eur,
                    "spot_chart": _spot_chart_mini,
                    "devices": _per_device,
                    "co2_enabled": _co2_enabled,
                    "co2_current": _co2_current,
                    "co2_chart": _co2_chart_mini,
                    "co2_green_thr": _co2_green_thr,
                    "co2_dirty_thr": _co2_dirty_thr,
                }
            except Exception as e:
                return {"ok": False, "error": str(e)}

        # --- Cost data for web dashboard ---
        if action == "costs":
            try:
                _tz = ZoneInfo("Europe/Berlin")
                _now = datetime.now(_tz)
                _today_start = _now.replace(hour=0, minute=0, second=0, microsecond=0)
                _week_start = _today_start - timedelta(days=_now.weekday())
                _month_start = _today_start.replace(day=1)
                _year_start = _today_start.replace(month=1, day=1)
                _last_month_start = (_month_start - timedelta(days=1)).replace(day=1)

                try:
                    _unit = float(self.cfg.pricing.effective_pricing_for_date(date.today()).unit_price_gross())
                except Exception:
                    _unit = float(getattr(getattr(self.cfg, "pricing", None), "electricity_price_eur_per_kwh", 0.30) or 0.30)
                _co2_g = float(getattr(getattr(self.cfg, "pricing", None), "co2_intensity_g_per_kwh", 380.0) or 0.0)

                _co2_cfg = getattr(self.cfg, "co2", None)
                _co2_zone = getattr(_co2_cfg, "bidding_zone", "DE_LU") or "DE_LU"
                _use_entsoe = False
                try:
                    _entsoe_token = getattr(_co2_cfg, "entsoe_token", "") or ""
                    if _entsoe_token and hasattr(self.storage, "db"):
                        _use_entsoe = True
                except Exception:
                    pass

                def _calc_co2(dev_key: str, rng_s, rng_e, kwh_fb: float) -> float:
                    if _use_entsoe:
                        try:
                            s_ts = int(rng_s.timestamp())
                            e_ts = int(rng_e.timestamp())
                            db = self.storage.db
                            df_co2 = db.query_co2_intensity(_co2_zone, s_ts, e_ts + 3600)
                            if not df_co2.empty:
                                df_h = db.query_hourly(dev_key, start_ts=s_ts, end_ts=e_ts + 3600)
                                if df_h is not None and not df_h.empty:
                                    merged = pd.merge(
                                        df_h[["hour_ts", "kwh"]],
                                        df_co2[["hour_ts", "intensity_g_per_kwh"]],
                                        on="hour_ts", how="inner",
                                    )
                                    if not merged.empty:
                                        return float((merged["kwh"] * merged["intensity_g_per_kwh"]).sum()) / 1000.0
                        except Exception:
                            pass
                    if _co2_g > 0:
                        return kwh_fb * _co2_g / 1000.0
                    return 0.0

                _ranges = {
                    "today": (_today_start, _now),
                    "week": (_week_start, _now),
                    "month": (_month_start, _now),
                    "year": (_year_start, _now),
                    "last_month": (_last_month_start, _month_start),
                }

                _pricing = self.cfg.pricing
                _has_schedule = bool(getattr(_pricing, "tariff_schedule", None))

                def _calc_cost_with_schedule(df_slice, rng_start, rng_end):
                    """Calculate cost using per-day effective pricing from tariff schedule."""
                    if df_slice.empty or "energy_kwh" not in df_slice.columns:
                        return 0.0
                    if not _has_schedule:
                        return float(pd.to_numeric(df_slice["energy_kwh"], errors="coerce").fillna(0).sum()) * _unit
                    # Group by date and apply effective price per day
                    df_tmp = df_slice.copy()
                    df_tmp["_date"] = df_tmp["timestamp"].dt.date
                    total_cost = 0.0
                    for day, grp in df_tmp.groupby("_date"):
                        day_kwh = float(pd.to_numeric(grp["energy_kwh"], errors="coerce").fillna(0).sum())
                        try:
                            day_price = float(_pricing.effective_pricing_for_date(day).unit_price_gross())
                        except Exception:
                            day_price = _unit
                        total_cost += day_kwh * day_price
                    return total_cost

                _cost_devices = [d for d in (self.cfg.devices or [])
                                if str(getattr(d, "kind", "em")) != "switch"]

                _spot_cfg = getattr(self.cfg, "spot_price", None)
                _use_dynamic_web = (
                    getattr(_spot_cfg, "enabled", False) if _spot_cfg else False
                ) and str(getattr(_spot_cfg, "tariff_type", "fixed") or "fixed") == "dynamic"

                devices_out = []
                for d in _cost_devices:
                    dev_data: Dict[str, Any] = {"key": d.key, "name": d.name, "host": d.host}
                    # Pre-load device DataFrame once
                    _dev_df = None
                    try:
                        cd = self.computed.get(d.key)
                        if cd is not None and cd.df is not None and not cd.df.empty:
                            _dev_df = cd.df.copy()
                            if "timestamp" in _dev_df.columns:
                                _dev_df["timestamp"] = pd.to_datetime(_dev_df["timestamp"], errors="coerce")
                                _dev_df = _dev_df.dropna(subset=["timestamp"])
                                try:
                                    if _dev_df["timestamp"].dt.tz is None:
                                        _dev_df["timestamp"] = _dev_df["timestamp"].dt.tz_localize("UTC")
                                    _dev_df["timestamp"] = _dev_df["timestamp"].dt.tz_convert(_tz)
                                except Exception:
                                    pass
                    except Exception:
                        pass

                    for rng_key, (rng_start, rng_end) in _ranges.items():
                        kwh = 0.0
                        cost = 0.0
                        _df_slice = None
                        try:
                            if _dev_df is not None:
                                m = (_dev_df["timestamp"] >= rng_start) & (_dev_df["timestamp"] < rng_end)
                                _df_slice = _dev_df.loc[m]
                                kwh = float(pd.to_numeric(_df_slice["energy_kwh"], errors="coerce").fillna(0.0).sum())
                        except Exception:
                            pass
                        dev_data[rng_key + "_kwh"] = round(kwh, 3)
                        if _use_dynamic_web:
                            try:
                                _sp_zone_d = getattr(_spot_cfg, "bidding_zone", "DE-LU") or "DE-LU"
                                _sp_mk_d = float(_spot_cfg.total_markup_ct() if hasattr(_spot_cfg, "total_markup_ct") else 16.0) / 100.0
                                _sp_vat_d = self.cfg.pricing.vat_rate() if getattr(_spot_cfg, "include_vat", True) else 0.0
                                _dyn_c, _, _ = self.storage.db.calc_spot_cost(
                                    d.key, _sp_zone_d, int(rng_start.timestamp()), int(rng_end.timestamp()),
                                    _sp_mk_d, _sp_vat_d
                                )
                                dev_data[rng_key + "_eur"] = round(_dyn_c if _dyn_c > 0 else kwh * _unit, 2)
                            except Exception:
                                dev_data[rng_key + "_eur"] = round(kwh * _unit, 2)
                        else:
                            # Use tariff-schedule-aware cost calculation
                            if _has_schedule and _df_slice is not None and not _df_slice.empty:
                                cost = _calc_cost_with_schedule(_df_slice, rng_start, rng_end)
                                dev_data[rng_key + "_eur"] = round(cost, 2)
                            else:
                                dev_data[rng_key + "_eur"] = round(kwh * _unit, 2)
                        dev_data[rng_key + "_co2_kg"] = round(_calc_co2(d.key, rng_start, rng_end, kwh), 3)

                    try:
                        _dim = calendar.monthrange(_now.year, _now.month)[1]
                        _elapsed = max(1, (_now - _month_start).total_seconds() / 86400.0)
                        _mk = dev_data.get("month_kwh", 0.0)
                        dev_data["proj_kwh"] = round(_mk / _elapsed * _dim, 1)
                        dev_data["proj_eur"] = round(dev_data["proj_kwh"] * _unit, 2)
                        _month_co2 = _calc_co2(d.key, _month_start, _now, _mk)
                        dev_data["proj_co2_kg"] = round(_month_co2 / _elapsed * _dim, 2) if _month_co2 > 0 else 0.0
                    except Exception:
                        dev_data["proj_kwh"] = 0.0
                        dev_data["proj_eur"] = 0.0
                        dev_data["proj_co2_kg"] = 0.0

                    _lm = dev_data.get("last_month_kwh", 0.0)
                    _cm = dev_data.get("month_kwh", 0.0)
                    if _lm > 0:
                        dev_data["vs_last_pct"] = round((_cm - _lm) / _lm * 100, 1)
                    else:
                        dev_data["vs_last_pct"] = None
                    dev_data["last_month_kwh"] = round(_lm, 3)
                    dev_data["last_month_eur"] = round(_lm * _unit, 2)

                    devices_out.append(dev_data)

                _solar_co2_saved_month_kg = 0.0
                try:
                    _solar_cfg_c = getattr(self.cfg, "solar", None)
                    _pv_key_c = str(getattr(_solar_cfg_c, "pv_meter_device_key", "") or "") if _solar_cfg_c else ""
                    if _pv_key_c and getattr(_solar_cfg_c, "enabled", False):
                        _pv_df_c = self.storage.db.query_hourly(_pv_key_c, start_ts=int(_month_start.timestamp()), end_ts=int(_now.timestamp()))
                        if _pv_df_c is not None and not _pv_df_c.empty and "kwh" in _pv_df_c.columns:
                            _kwh_col_c = pd.to_numeric(_pv_df_c["kwh"], errors="coerce").fillna(0.0)
                            _feed_in_c = float(_kwh_col_c[_kwh_col_c < 0].abs().sum())
                            _self_kwh_c = 0.0
                            _grid_c = float(_kwh_col_c[_kwh_col_c >= 0].sum())
                            _hh_c = sum(d_o.get("month_kwh", 0.0) for d_o in devices_out)
                            _self_kwh_c = max(0.0, _hh_c - _grid_c)
                            _pv_kwh_c = _self_kwh_c + _feed_in_c
                            _solar_co2_saved_month_kg = _pv_kwh_c * _co2_g / 1000.0
                except Exception:
                    pass

                _spot_enabled = getattr(_spot_cfg, "enabled", False) if _spot_cfg else False
                if _spot_enabled:
                    _sp_zone = getattr(_spot_cfg, "bidding_zone", "DE-LU") or "DE-LU"
                    _sp_markup = float(_spot_cfg.total_markup_ct() if hasattr(_spot_cfg, "total_markup_ct") else getattr(_spot_cfg, "markup_ct_per_kwh", 16.0)) / 100.0
                    _sp_vat = self.cfg.pricing.vat_rate() if getattr(_spot_cfg, "include_vat", True) else 0.0
                    for dev_data in devices_out:
                        for rng_key, (rng_start, rng_end) in _ranges.items():
                            try:
                                _sc, _sk, _sa = self.storage.db.calc_spot_cost(
                                    dev_data["key"], _sp_zone,
                                    int(rng_start.timestamp()), int(rng_end.timestamp()),
                                    _sp_markup, _sp_vat
                                )
                                dev_data[rng_key + "_spot_eur"] = round(_sc, 2)
                            except Exception:
                                dev_data[rng_key + "_spot_eur"] = 0.0
                        try:
                            _sp_month = dev_data.get("month_spot_eur", 0.0)
                            if _sp_month > 0:
                                _dim2 = calendar.monthrange(_now.year, _now.month)[1]
                                _el2 = max(1, (_now - _month_start).total_seconds() / 86400.0)
                                dev_data["proj_spot_eur"] = round(_sp_month / _el2 * _dim2, 2)
                            else:
                                dev_data["proj_spot_eur"] = 0.0
                        except Exception:
                            dev_data["proj_spot_eur"] = 0.0

                _tariff_sched = [
                    {"start_date": tp.start_date, "price": tp.electricity_price_eur_per_kwh, "base_fee": tp.base_fee_eur_per_year}
                    for tp in getattr(self.cfg.pricing, "tariff_schedule", []) or []
                ]

                _spot_chart = []
                if _spot_enabled:
                    try:
                        _sp_zone2 = getattr(_spot_cfg, "bidding_zone", "DE-LU") or "DE-LU"
                        _sp_markup2 = float(_spot_cfg.total_markup_ct() if hasattr(_spot_cfg, "total_markup_ct") else getattr(_spot_cfg, "markup_ct_per_kwh", 16.0))
                        _sp_vat2 = (self.cfg.pricing.vat_rate() if getattr(_spot_cfg, "include_vat", True) else 0.0)
                        _chart_start = int((_now - timedelta(hours=24)).timestamp())
                        _chart_end = int((_now + timedelta(hours=24)).timestamp())
                        _df_sp = self.storage.db.query_spot_prices(_sp_zone2, _chart_start, _chart_end)
                        if not _df_sp.empty:
                            _df_sp_h = _df_sp.copy()
                            _df_sp_h["hour_ts"] = (_df_sp_h["slot_ts"] // 3600) * 3600
                            _df_sp_h = _df_sp_h.groupby("hour_ts").agg(price=("price_eur_mwh", "mean")).reset_index()
                            _df_sp_h = _df_sp_h.sort_values("hour_ts")
                            for _, _r in _df_sp_h.iterrows():
                                _raw_ct = float(_r["price"]) / 10.0
                                _total_ct = (_raw_ct + _sp_markup2) * (1.0 + _sp_vat2)
                                _spot_chart.append({
                                    "ts": int(_r["hour_ts"]),
                                    "raw_ct": round(_raw_ct, 2),
                                    "total_ct": round(_total_ct, 2),
                                })
                    except Exception:
                        pass

                _current_spot_ct = None
                if _spot_chart:
                    _cur_hour_ts = int(_now.timestamp()) // 3600 * 3600
                    for _sp_entry in _spot_chart:
                        if int(_sp_entry["ts"]) == _cur_hour_ts:
                            _current_spot_ct = _sp_entry["total_ct"]
                            break

                # Build summary from device totals
                _s = {}
                for rk in ["today", "week", "month", "year", "last_month"]:
                    _s[rk + "_kwh"] = round(sum(d.get(rk + "_kwh", 0) for d in devices_out), 3)
                    _s[rk + "_eur"] = round(sum(d.get(rk + "_eur", 0) for d in devices_out), 2)
                try:
                    _dim_s = calendar.monthrange(_now.year, _now.month)[1]
                    _el_s = max(1, (_now - _month_start).total_seconds() / 86400.0)
                    _s["proj_kwh"] = round(_s["month_kwh"] / _el_s * _dim_s, 1)
                    _s["proj_eur"] = round(_s["month_eur"] / _el_s * _dim_s, 2)
                except Exception:
                    _s["proj_kwh"] = 0
                    _s["proj_eur"] = 0

                return {
                    "ok": True, "devices": devices_out, "unit_eur": _unit,
                    "summary": _s,
                    "co2_g_per_kwh": _co2_g,
                    "solar_co2_saved_month_kg": round(_solar_co2_saved_month_kg, 3),
                    "tariff_schedule": _tariff_sched,
                    "spot_enabled": _spot_enabled,
                    "spot_chart": _spot_chart,
                    "fixed_ct_per_kwh": round(_unit * 100, 2),
                    "current_spot_ct": _current_spot_ct,
                }
            except Exception as e:
                return {"ok": False, "error": str(e)}

        # --- Report Button ---
        if action == "report":
            period = str(params.get("period") or params.get("kind") or "day").strip().lower()
            anchor = _pdate(params.get("anchor"))
            if anchor is None:
                anchor = pd.Timestamp(date.today())
            anchor = pd.Timestamp(anchor)

            _tz_r = ZoneInfo("Europe/Berlin")

            is_monthly = period in {"month", "mon", "m"}

            if is_monthly:
                start_d = anchor.replace(day=1).date()
                if start_d.month == 12:
                    end_d = date(start_d.year + 1, 1, 1)
                else:
                    end_d = date(start_d.year, start_d.month + 1, 1)
                fname = f"energy_report_month_{start_d.strftime('%Y%m')}_{time.strftime('%H%M%S')}.pdf"
                prev_end_d = start_d
                prev_start_d = (start_d - timedelta(days=1)).replace(day=1)
                report_type = "monthly"
            else:
                start_d = anchor.date()
                end_d = start_d + timedelta(days=1)
                fname = f"energy_report_day_{start_d.strftime('%Y%m%d')}_{time.strftime('%H%M%S')}.pdf"
                prev_start_d = start_d - timedelta(days=1)
                prev_end_d = start_d
                report_type = "daily"

            start_dt = datetime.combine(start_d, datetime.min.time(), tzinfo=_tz_r)
            end_dt = datetime.combine(end_d, datetime.min.time(), tzinfo=_tz_r)
            prev_start_dt = datetime.combine(prev_start_d, datetime.min.time(), tzinfo=_tz_r)
            prev_end_dt = datetime.combine(prev_end_d, datetime.min.time(), tzinfo=_tz_r)

            if progress:
                try:
                    progress("report", 0, 3, "Daten sammeln \u2026")
                except Exception:
                    pass

            # Skip _build_email_report_data; use fallback path
            report_data = None

            if progress:
                try:
                    progress("report", 1, 3, "PDF erzeugen \u2026")
                except Exception:
                    pass

            rep_dir = out_root / "web" / "reports"
            rep_dir.mkdir(parents=True, exist_ok=True)
            out_path_r = rep_dir / fname

            if report_data is not None:
                from shelly_analyzer.services.export import export_pdf_email_daily, export_pdf_email_monthly
                if is_monthly:
                    export_pdf_email_monthly(report_data, out_path_r, lang=self.lang)
                else:
                    export_pdf_email_daily(report_data, out_path_r, lang=self.lang)
            else:
                from shelly_analyzer.services.export import export_pdf_energy_report_variant1
                devices_payload: List[Tuple[str, str, pd.DataFrame]] = []
                for d in self.cfg.devices:
                    cd = load_device(self.storage, d)
                    df_use = filter_by_time(cd.df, start=pd.Timestamp(start_d), end=pd.Timestamp(end_d))
                    devices_payload.append((d.key, d.name, df_use))
                try:
                    unit_gross = float(self.cfg.pricing.unit_price_gross())
                except Exception:
                    unit_gross = 0.30
                export_pdf_energy_report_variant1(
                    out_path=out_path_r,
                    title=self.t("pdf.report.title.month") if is_monthly else self.t("pdf.report.title.day"),
                    period_label=f"{start_d} \u2013 {end_d}",
                    pricing_note="",
                    unit_price_gross=unit_gross,
                    devices=devices_payload,
                    lang=self.lang,
                )

            if progress:
                try:
                    progress("report", 3, 3, "OK")
                except Exception:
                    pass

            period_label = f"{format_date_local(self.lang, pd.Timestamp(start_d))} \u2013 {format_date_local(self.lang, pd.Timestamp(end_d - timedelta(days=0 if is_monthly else 0)))}"
            return {
                "ok": True,
                "period": period,
                "period_label": period_label,
                "files": [{"name": out_path_r.name, "url": f"/files/web/reports/{out_path_r.name}"}],
            }

        if action == "bundle":
            try:
                hours = int(params.get("hours") or 48)
            except Exception:
                hours = 48
            hours = max(1, min(24 * 365, hours))
            since = time.time() - (hours * 3600)

            web_dir = out_root / "web"
            web_dir.mkdir(parents=True, exist_ok=True)
            ts_str = time.strftime("%Y%m%d_%H%M%S")
            zpath = web_dir / f"bundle_{ts_str}.zip"

            exp_root = out_root
            wanted_ext = {".pdf", ".png", ".jpg", ".jpeg", ".xlsx", ".csv", ".json", ".txt", ".log"}
            paths: List[Path] = []
            for p in exp_root.rglob("*"):
                try:
                    if not p.is_file():
                        continue
                    if p.suffix.lower() not in wanted_ext:
                        continue
                    if p.name.startswith("bundle_") and p.suffix.lower() == ".zip":
                        continue
                    if p.stat().st_mtime < since:
                        continue
                    paths.append(p)
                except Exception:
                    continue
            paths.sort(key=lambda x: x.stat().st_mtime if x.exists() else 0.0, reverse=True)

            total = max(1, len(paths))
            done = 0
            if progress:
                try:
                    progress("bundle", 0, total, f"ZIP (letzte {hours}h) \u2026")
                except Exception:
                    pass

            with ZipFile(zpath, "w", compression=ZIP_DEFLATED) as zf:
                try:
                    snap = {
                        "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                        "hours": hours,
                        "version": __version__,
                        "pricing": {
                            "unit_price_gross": float(self.cfg.pricing.unit_price_gross()),
                            "unit_price_net": float(self.cfg.pricing.unit_price_net()),
                            "vat_enabled": bool(self.cfg.pricing.vat_enabled),
                            "vat_rate_percent": float(self.cfg.pricing.vat_rate_percent),
                            "price_includes_vat": bool(self.cfg.pricing.price_includes_vat),
                        },
                        "devices": [{"key": d.key, "name": d.name, "host": d.host} for d in self.cfg.devices[:2]],
                    }
                    zf.writestr("config_snapshot.json", json.dumps(snap, indent=2, ensure_ascii=False))
                except Exception:
                    pass

                for p in paths:
                    rel = p.relative_to(exp_root)
                    try:
                        zf.write(p, arcname=str(rel))
                    except Exception:
                        pass
                    done += 1
                    if progress and (done % 5 == 0 or done == total):
                        try:
                            progress("bundle", done, total, f"{done}/{total} Dateien")
                        except Exception:
                            pass

            if progress:
                try:
                    progress("bundle", total, total, "OK")
                except Exception:
                    pass

            return {"ok": True, "files": [{"name": zpath.name, "url": f"/files/web/{zpath.name}"}], "count": len(paths), "hours": hours}

        # --- Heatmap data ---
        if action == "heatmap":
            try:
                device_key = str(params.get("device") or "").strip()
                try:
                    year = int(params.get("year") or datetime.now().year)
                except Exception:
                    year = datetime.now().year
                unit_h = str(params.get("unit") or "kWh").strip()
                use_eur = (unit_h.lower() in ("eur", "\u20ac", "euro"))
                use_co2 = (unit_h.lower() in ("co2", "g co\u2082", "gco2"))
                try:
                    _unit_price = float(self.cfg.pricing.unit_price_gross())
                except Exception:
                    _unit_price = 0.30

                if not device_key and self.cfg.devices:
                    device_key = self.cfg.devices[0].key
                if not device_key:
                    return {"ok": False, "error": "no device"}

                start_ts = int(datetime(year, 1, 1).timestamp())
                end_ts = int(datetime(year, 12, 31, 23, 59, 59).timestamp())

                try:
                    hourly_df = self.storage.db.query_hourly(device_key, start_ts=start_ts, end_ts=end_ts)
                except Exception:
                    hourly_df = None

                co2_intensity_map: Dict[int, float] = {}
                _co2_fallback_g = 0.0
                if use_co2:
                    try:
                        _co2_fallback_g = float(getattr(getattr(self.cfg, "pricing", None), "co2_intensity_g_per_kwh", 380.0) or 380.0)
                    except Exception:
                        _co2_fallback_g = 380.0
                    try:
                        _co2_cfg_hm = getattr(self.cfg, "co2", None)
                        _zone_hm = getattr(_co2_cfg_hm, "bidding_zone", "DE_LU") or "DE_LU"
                        _token_hm = getattr(_co2_cfg_hm, "entsoe_token", "") or ""
                        if _token_hm and hasattr(self.storage, "db"):
                            df_co2_hm = self.storage.db.query_co2_intensity(_zone_hm, start_ts, end_ts + 3600)
                            if not df_co2_hm.empty:
                                for _, r in df_co2_hm.iterrows():
                                    co2_intensity_map[int(r["hour_ts"])] = float(r["intensity_g_per_kwh"])
                    except Exception:
                        pass

                calendar_data: List[Dict[str, Any]] = []
                hourly_matrix: Dict[int, Dict[int, float]] = {wd: {h: 0.0 for h in range(24)} for wd in range(7)}
                hourly_counts: Dict[int, Dict[int, int]] = {wd: {h: 0 for h in range(24)} for wd in range(7)}

                if hourly_df is not None and not hourly_df.empty and "hour_ts" in hourly_df.columns and "kwh" in hourly_df.columns:
                    daily_totals: Dict[str, float] = {}
                    for _, row in hourly_df.iterrows():
                        try:
                            ts_val = int(row["hour_ts"])
                            kwh_val = float(row["kwh"] or 0.0)
                            dt_local = datetime.fromtimestamp(ts_val)
                            date_str = dt_local.strftime("%Y-%m-%d")

                            if use_co2:
                                intensity = co2_intensity_map.get(ts_val, _co2_fallback_g)
                                val_h = kwh_val * intensity
                            else:
                                val_h = kwh_val

                            daily_totals[date_str] = daily_totals.get(date_str, 0.0) + val_h
                            wd = dt_local.weekday()
                            h = dt_local.hour
                            hourly_matrix[wd][h] += val_h
                            hourly_counts[wd][h] += 1
                        except Exception:
                            continue

                    for date_str, total_val in daily_totals.items():
                        if use_eur:
                            val = total_val * _unit_price
                        elif use_co2:
                            val = total_val
                        else:
                            val = total_val
                        calendar_data.append({"date": date_str, "value": round(val, 3)})

                hourly_out: Dict[str, Dict[str, float]] = {}
                for wd in range(7):
                    hourly_out[str(wd)] = {}
                    for h in range(24):
                        cnt = hourly_counts[wd][h]
                        avg_val = (hourly_matrix[wd][h] / cnt) if cnt > 0 else 0.0
                        if use_eur and not use_co2:
                            val = avg_val * _unit_price
                        else:
                            val = avg_val
                        hourly_out[str(wd)][str(h)] = round(val, 4)

                devices_list = [{"key": d.key, "name": d.name} for d in self.cfg.devices]
                return {
                    "ok": True,
                    "device_key": device_key,
                    "year": year,
                    "unit": unit_h,
                    "calendar": calendar_data,
                    "hourly": hourly_out,
                    "devices": devices_list,
                }
            except Exception as e:
                return {"ok": False, "error": str(e)}

        # --- Save solar config from web ---
        if action == "save_solar_config":
            try:
                from shelly_analyzer.io.config import SolarConfig as _SC
                import dataclasses
                _old = getattr(self.cfg, "solar", _SC())
                _new = _SC(
                    enabled=bool(params.get("enabled", getattr(_old, "enabled", False))),
                    pv_meter_device_key=str(params.get("pv_meter_device_key", getattr(_old, "pv_meter_device_key", "")) or ""),
                    feed_in_tariff_eur_per_kwh=float(params.get("feed_in_tariff", getattr(_old, "feed_in_tariff_eur_per_kwh", 0.082))),
                    kw_peak=float(params.get("kw_peak", getattr(_old, "kw_peak", 0.0))),
                    battery_kwh=float(params.get("battery_kwh", getattr(_old, "battery_kwh", 0.0))),
                    co2_production_kg_per_kwp=float(params.get("co2_production_kg_per_kwp", getattr(_old, "co2_production_kg_per_kwp", 1000.0))),
                )
                self.cfg = dataclasses.replace(self.cfg, solar=_new)
                save_config(self.cfg, self.cfg_path)
                return {"ok": True}
            except Exception as e:
                return {"ok": False, "error": str(e)}

        # --- Weather correlation ---
        if action == "weather_correlation":
            try:
                weather_cfg = getattr(self.cfg, "weather", None)
                api_key = getattr(weather_cfg, "api_key", "") if weather_cfg else ""
                lat = getattr(weather_cfg, "lat", 0.0) if weather_cfg else 0.0
                lon = getattr(weather_cfg, "lon", 0.0) if weather_cfg else 0.0

                if not api_key or (lat == 0 and lon == 0):
                    return {"ok": False, "error": _t(self.lang, "web.weather.no_data")}

                current_data = None
                try:
                    from shelly_analyzer.services.weather import fetch_current_weather
                    snap_w = fetch_current_weather(api_key, lat, lon)
                    if snap_w:
                        current_data = {
                            "temp_c": snap_w.temp_c,
                            "humidity_pct": snap_w.humidity_pct,
                            "wind_speed_ms": snap_w.wind_speed_ms,
                            "clouds_pct": snap_w.clouds_pct,
                        }
                        hour_ts = (int(snap_w.timestamp) // 3600) * 3600
                        self.storage.db.upsert_weather([(
                            hour_ts, snap_w.temp_c, snap_w.humidity_pct,
                            snap_w.wind_speed_ms, snap_w.clouds_pct,
                            snap_w.pressure_hpa, snap_w.description,
                            int(time.time()),
                        )])
                except Exception:
                    pass

                dev_key = str(params.get("device_key", "")).strip()
                dev = None
                if dev_key:
                    dev = next((d for d in self.cfg.devices if d.key == dev_key), None)
                if dev is None and self.cfg.devices:
                    dev = self.cfg.devices[0]
                if dev is None:
                    return {"ok": True, "current": current_data, "correlation": None, "paired": []}

                now_wc = datetime.now(ZoneInfo("UTC"))
                start_ts_wc = int((now_wc - timedelta(days=30)).timestamp())
                end_ts_wc = int(now_wc.timestamp())

                weather_df = self.storage.db.query_weather(start_ts_wc, end_ts_wc)
                hourly_wc = self.storage.db.query_hourly(dev.key, start_ts=start_ts_wc, end_ts=end_ts_wc)

                if weather_df.empty or hourly_wc.empty:
                    return {"ok": True, "current": current_data, "correlation": None, "paired": []}

                w_by_hour = {}
                for _, row in weather_df.iterrows():
                    h = int(row["hour_ts"])
                    w_by_hour[h] = float(row["temp_c"]) if row["temp_c"] is not None else None

                paired = []
                temps_list = []
                kwh_list = []
                for _, row in hourly_wc.iterrows():
                    h = int(row["hour_ts"])
                    if h in w_by_hour and w_by_hour[h] is not None:
                        t_val = w_by_hour[h]
                        k_val = float(row["kwh"])
                        dt_wc = datetime.fromtimestamp(h, tz=ZoneInfo("UTC"))
                        paired.append({
                            "ts": h,
                            "temp": round(t_val, 2),
                            "kwh": round(k_val, 4),
                            "hour_of_day": dt_wc.hour,
                        })
                        temps_list.append(t_val)
                        kwh_list.append(k_val)

                correlation = None
                if len(temps_list) >= 3:
                    temps_arr = np.array(temps_list)
                    kwh_arr = np.array(kwh_list)
                    r_val = float(np.corrcoef(temps_arr, kwh_arr)[0, 1]) if np.std(temps_arr) > 0 and np.std(kwh_arr) > 0 else 0.0
                    hdd = sum(max(0, 18.0 - t) / 24.0 for t in temps_list)
                    cdd = sum(max(0, t - 22.0) / 24.0 for t in temps_list)
                    total_kwh = float(kwh_arr.sum())
                    slope = None
                    intercept = None
                    if len(temps_arr) > 2:
                        z = np.polyfit(temps_arr, kwh_arr, 1)
                        slope = round(float(z[0]), 6)
                        intercept = round(float(z[1]), 6)
                    correlation = {
                        "r_value": round(r_val, 4),
                        "hdd": round(hdd, 1),
                        "cdd": round(cdd, 1),
                        "kwh_per_hdd": round(total_kwh / hdd, 2) if hdd > 1 else None,
                        "kwh_per_cdd": round(total_kwh / cdd, 2) if cdd > 1 else None,
                        "slope": slope,
                        "intercept": intercept,
                    }

                return {"ok": True, "current": current_data, "correlation": correlation, "paired": paired}
            except Exception as e:
                return {"ok": False, "error": str(e)}

        # --- Solar data ---
        if action == "solar":
            try:
                solar_cfg = getattr(self.cfg, "solar", None)
                _all_devs_s = [{"key": d.key, "name": d.name} for d in self.cfg.devices]
                _scfg_resp = {
                    "enabled": bool(getattr(solar_cfg, "enabled", False)) if solar_cfg else False,
                    "pv_meter_device_key": str(getattr(solar_cfg, "pv_meter_device_key", "") or "") if solar_cfg else "",
                    "feed_in_tariff": float(getattr(solar_cfg, "feed_in_tariff_eur_per_kwh", 0.082)) if solar_cfg else 0.082,
                    "kw_peak": float(getattr(solar_cfg, "kw_peak", 0.0) or 0.0) if solar_cfg else 0.0,
                    "battery_kwh": float(getattr(solar_cfg, "battery_kwh", 0.0) or 0.0) if solar_cfg else 0.0,
                    "co2_production_kg_per_kwp": float(getattr(solar_cfg, "co2_production_kg_per_kwp", 1000.0) or 1000.0) if solar_cfg else 1000.0,
                }
                if solar_cfg is None or not getattr(solar_cfg, "enabled", False):
                    return {"ok": True, "configured": False, "devices": _all_devs_s, "config": _scfg_resp}
                pv_key = str(getattr(solar_cfg, "pv_meter_device_key", "") or "")
                if not pv_key:
                    return {"ok": True, "configured": False, "devices": _all_devs_s, "config": _scfg_resp}

                period_s = str(params.get("period") or "today").strip()
                _tz3 = ZoneInfo("Europe/Berlin")
                _now3 = datetime.now(_tz3)
                _today3 = _now3.replace(hour=0, minute=0, second=0, microsecond=0)
                if period_s == "week":
                    _start3 = _today3 - timedelta(days=_now3.weekday())
                    _end3 = _now3
                elif period_s == "month":
                    _start3 = _today3.replace(day=1)
                    _end3 = _now3
                elif period_s == "year":
                    _start3 = _today3.replace(month=1, day=1)
                    _end3 = _now3
                else:
                    _start3 = _today3
                    _end3 = _now3

                start_ts3 = int(_start3.timestamp())
                end_ts3 = int(_end3.timestamp())

                def _load_hourly_kwh(dev_key_s: str) -> float:
                    try:
                        df_h = self.storage.db.query_hourly(dev_key_s, start_ts=start_ts3, end_ts=end_ts3)
                        if df_h is not None and not df_h.empty and "kwh" in df_h.columns:
                            return float(pd.to_numeric(df_h["kwh"], errors="coerce").fillna(0.0).sum())
                    except Exception:
                        pass
                    return 0.0

                feed_in_kwh = 0.0
                grid_kwh = 0.0
                try:
                    pv_df = self.storage.db.query_hourly(pv_key, start_ts=start_ts3, end_ts=end_ts3)
                    if pv_df is not None and not pv_df.empty and "kwh" in pv_df.columns:
                        kwh_col = pd.to_numeric(pv_df["kwh"], errors="coerce").fillna(0.0)
                        feed_in_kwh = float(kwh_col[kwh_col < 0].abs().sum())
                        grid_kwh = float(kwh_col[kwh_col >= 0].sum())
                except Exception:
                    pass

                household_kwh = 0.0
                for d in self.cfg.devices:
                    if d.key == pv_key:
                        continue
                    household_kwh += _load_hourly_kwh(d.key)

                self_kwh = max(0.0, household_kwh - grid_kwh) if household_kwh > 0 else 0.0
                pv_kwh = self_kwh + feed_in_kwh
                autarky_pct = (min(100.0, self_kwh / household_kwh * 100.0) if household_kwh > 0 else 0.0)

                try:
                    feed_in_tariff = float(getattr(solar_cfg, "feed_in_tariff_eur_per_kwh", 0.082))
                    unit_price = float(self.cfg.pricing.unit_price_gross())
                except Exception:
                    feed_in_tariff = 0.082
                    unit_price = 0.30

                co2_g_per_kwh = float(getattr(getattr(self.cfg, "pricing", None), "co2_intensity_g_per_kwh", 380.0) or 380.0)
                _co2_source = "static"
                try:
                    _co2_cfg_s = getattr(self.cfg, "co2", None)
                    _co2_zone_s = getattr(_co2_cfg_s, "bidding_zone", "DE_LU") or "DE_LU"
                    _co2_token_s = getattr(_co2_cfg_s, "entsoe_token", "") or ""
                    if _co2_token_s and hasattr(self.storage, "db"):
                        df_co2_s = self.storage.db.query_co2_intensity(_co2_zone_s, start_ts3, end_ts3 + 3600)
                        if df_co2_s is not None and not df_co2_s.empty and "intensity_g_per_kwh" in df_co2_s.columns:
                            avg_int = float(pd.to_numeric(df_co2_s["intensity_g_per_kwh"], errors="coerce").mean())
                            if avg_int > 0:
                                co2_g_per_kwh = avg_int
                                _co2_source = "entsoe"
                except Exception:
                    pass

                co2_saved_kg = pv_kwh * co2_g_per_kwh / 1000.0
                co2_grid_kg = grid_kwh * co2_g_per_kwh / 1000.0

                kw_peak = float(getattr(solar_cfg, "kw_peak", 0.0) or 0.0)
                battery_kwh_cfg = float(getattr(solar_cfg, "battery_kwh", 0.0) or 0.0)
                co2_prod_per_kwp = float(getattr(solar_cfg, "co2_production_kg_per_kwp", 1000.0) or 1000.0)
                co2_embodied_kg = kw_peak * co2_prod_per_kwp if kw_peak > 0 else 0.0

                _all_devices = [{"key": d.key, "name": d.name} for d in self.cfg.devices]
                return {
                    "ok": True,
                    "configured": True,
                    "period": period_s,
                    "feed_in_kwh": round(feed_in_kwh, 3),
                    "grid_kwh": round(grid_kwh, 3),
                    "self_kwh": round(self_kwh, 3),
                    "pv_kwh": round(pv_kwh, 3),
                    "autarky_pct": round(autarky_pct, 1),
                    "household_kwh": round(household_kwh, 3),
                    "revenue_eur": round(feed_in_kwh * feed_in_tariff, 2),
                    "savings_eur": round(self_kwh * unit_price, 2),
                    "co2_saved_kg": round(co2_saved_kg, 3),
                    "co2_grid_kg": round(co2_grid_kg, 3),
                    "co2_intensity_g_per_kwh": round(co2_g_per_kwh, 1),
                    "co2_source": _co2_source,
                    "kw_peak": round(kw_peak, 2),
                    "battery_kwh": round(battery_kwh_cfg, 1),
                    "co2_embodied_kg": round(co2_embodied_kg, 1),
                    "co2_production_kg_per_kwp": round(co2_prod_per_kwp, 0),
                    "feed_in_tariff": round(feed_in_tariff, 4),
                    "pv_meter_device_key": pv_key,
                    "devices": _all_devices,
                }
            except Exception as e:
                return {"ok": False, "error": str(e)}

        # --- Compare data ---
        if action == "compare":
            try:
                device_a = str(params.get("device_a") or "").strip()
                device_b = str(params.get("device_b") or "").strip()
                if not device_a and self.cfg.devices:
                    device_a = self.cfg.devices[0].key
                if not device_b and self.cfg.devices:
                    device_b = self.cfg.devices[0].key

                def _pdate4(s: Any) -> Optional[date]:
                    try:
                        return _parse_date_flexible(str(s or "").strip()).date()
                    except Exception:
                        return None

                today4 = date.today()
                jan1 = date(today4.year, 1, 1)
                jan1_last = date(today4.year - 1, 1, 1)
                dec31_last = date(today4.year - 1, 12, 31)

                from_a = _pdate4(params.get("from_a")) or jan1
                to_a = _pdate4(params.get("to_a")) or (today4 - timedelta(days=1))
                from_b = _pdate4(params.get("from_b")) or jan1_last
                to_b = _pdate4(params.get("to_b")) or dec31_last

                if to_a < from_a:
                    from_a, to_a = to_a, from_a
                if to_b < from_b:
                    from_b, to_b = to_b, from_b

                unit_c = str(params.get("unit") or "kWh").strip()
                use_eur = unit_c.lower() in ("eur", "\u20ac", "euro")
                try:
                    _price4 = float(self.cfg.pricing.unit_price_gross())
                except Exception:
                    _price4 = 0.30

                gran = str(params.get("gran") or "total").strip()

                preset = str(params.get("preset") or "").strip()
                if preset:
                    _now4 = today4
                    if preset == "month":
                        _ms = date(_now4.year, _now4.month, 1)
                        _lms = (_ms - timedelta(days=1)).replace(day=1)
                        _lme = _ms - timedelta(days=1)
                        from_a, to_a = _ms, _now4
                        from_b, to_b = _lms, _lme
                    elif preset == "quarter":
                        _q = (_now4.month - 1) // 3
                        _qs = date(_now4.year, _q * 3 + 1, 1)
                        _lqs_y = _now4.year if _q > 0 else _now4.year - 1
                        _lqs_m = (_q - 1) * 3 + 1 if _q > 0 else 10
                        _lqs = date(_lqs_y, _lqs_m, 1)
                        _lqe = _qs - timedelta(days=1)
                        from_a, to_a = _qs, _now4
                        from_b, to_b = _lqs, _lqe
                    elif preset == "halfyear":
                        _hs = date(_now4.year, 1, 1) if _now4.month <= 6 else date(_now4.year, 7, 1)
                        _lhs = (_hs - timedelta(days=1)).replace(day=1)
                        if _lhs.month > 6:
                            _lhs = date(_lhs.year, 7, 1)
                        else:
                            _lhs = date(_lhs.year, 1, 1)
                        _lhe = _hs - timedelta(days=1)
                        from_a, to_a = _hs, _now4
                        from_b, to_b = _lhs, _lhe
                    elif preset == "year":
                        from_a = date(_now4.year, 1, 1)
                        to_a = _now4
                        from_b = date(_now4.year - 1, 1, 1)
                        to_b = date(_now4.year - 1, 12, 31)

                _spot_mode4 = str(params.get("mode") or "").strip() == "spot"

                if _spot_mode4:
                    daily_a = self._cmp_load_daily(device_a, from_a, to_a, True, _price4)
                    daily_b = self._cmp_load_daily_spot(device_a, from_a, to_a)
                    from_b, to_b = from_a, to_a
                    device_b = device_a
                    use_eur = True
                else:
                    daily_a = self._cmp_load_daily(device_a, from_a, to_a, use_eur, _price4)
                    daily_b = self._cmp_load_daily(device_b, from_b, to_b, use_eur, _price4)

                total_a = sum(daily_a.values())
                total_b = sum(daily_b.values())
                delta = total_a - total_b
                delta_pct = ((delta / total_b * 100.0) if total_b > 0 else 0.0)

                if gran == "monthly":
                    vals_a, vals_b, labels_c = self._cmp_align_monthly(daily_a, from_a, to_a, daily_b, from_b, to_b)
                elif gran == "weekly":
                    vals_a, vals_b, labels_c = self._cmp_align_weekly(daily_a, from_a, to_a, daily_b, from_b, to_b)
                elif gran == "daily":
                    vals_a, vals_b, labels_c = self._cmp_align_daily(daily_a, from_a, to_a, daily_b, from_b, to_b)
                else:
                    vals_a, vals_b, labels_c = [total_a], [total_b], ["A vs B"]

                return {
                    "ok": True,
                    "device_a": device_a,
                    "device_b": device_b,
                    "from_a": from_a.isoformat(),
                    "to_a": to_a.isoformat(),
                    "from_b": from_b.isoformat(),
                    "to_b": to_b.isoformat(),
                    "unit": unit_c,
                    "gran": gran,
                    "labels": labels_c,
                    "values_a": [round(v, 3) for v in vals_a],
                    "values_b": [round(v, 3) for v in vals_b],
                    "total_a": round(total_a, 3),
                    "total_b": round(total_b, 3),
                    "delta": round(delta, 3),
                    "delta_pct": round(delta_pct, 1),
                }
            except Exception as e:
                return {"ok": False, "error": str(e)}

        # --- Anomaly data ---
        if action == "anomalies":
            try:
                anom_cfg = getattr(self.cfg, "anomaly", None)
                enabled = bool(getattr(anom_cfg, "enabled", False)) if anom_cfg else False

                log = self._anomaly_log
                all_events = []
                for ev in log:
                    ts_str_a = ""
                    try:
                        ts_str_a = ev.timestamp.isoformat() if hasattr(ev.timestamp, "isoformat") else str(ev.timestamp)
                    except Exception:
                        ts_str_a = str(getattr(ev, "timestamp", ""))
                    all_events.append({
                        "event_id": ev.event_id,
                        "timestamp": ts_str_a,
                        "device_key": ev.device_key,
                        "device_name": ev.device_name,
                        "anomaly_type": ev.anomaly_type,
                        "type": ev.anomaly_type,
                        "value": round(ev.value, 3),
                        "sigma_count": round(ev.sigma_count, 2),
                        "sigma": round(ev.sigma_count, 2),
                        "description": ev.description,
                    })

                if not all_events and enabled:
                    try:
                        from shelly_analyzer.services.anomaly import detect_anomalies as _detect
                        for d in self.cfg.devices:
                            try:
                                cd = load_device(self.storage, d)
                                if cd is None or cd.df is None or cd.df.empty:
                                    continue
                                events = _detect(
                                    cd.df, d.key, d.name,
                                    sigma=float(getattr(anom_cfg, "sigma_threshold", 2.0)),
                                    min_deviation_kwh=float(getattr(anom_cfg, "min_deviation_kwh", 0.1)),
                                    window_days=int(getattr(anom_cfg, "window_days", 30)),
                                )
                                for ev in events:
                                    all_events.append({
                                        "event_id": ev.event_id,
                                        "timestamp": ev.timestamp.isoformat() if hasattr(ev.timestamp, "isoformat") else str(ev.timestamp),
                                        "device_key": ev.device_key,
                                        "device_name": ev.device_name,
                                        "anomaly_type": ev.anomaly_type,
                                        "type": ev.anomaly_type,
                                        "value": round(ev.value, 3),
                                        "sigma_count": round(ev.sigma_count, 2),
                                        "sigma": round(ev.sigma_count, 2),
                                        "description": ev.description,
                                    })
                            except Exception:
                                continue
                    except Exception:
                        pass

                all_events.sort(key=lambda x: x.get("timestamp", ""), reverse=True)

                # Compute statistics for rich UI
                type_counts: Dict[str, int] = {}
                device_counts: Dict[str, Dict[str, Any]] = {}
                sigma_values: list = []
                for ev in all_events:
                    at = ev.get("anomaly_type", "unknown")
                    type_counts[at] = type_counts.get(at, 0) + 1
                    dk = ev.get("device_key", "")
                    dn = ev.get("device_name", dk)
                    if dk not in device_counts:
                        device_counts[dk] = {"name": dn, "count": 0, "types": {}}
                    device_counts[dk]["count"] += 1
                    device_counts[dk]["types"][at] = device_counts[dk]["types"].get(at, 0) + 1
                    sc = ev.get("sigma_count", 0)
                    if sc:
                        sigma_values.append(round(sc, 1))
                max_sigma = max(sigma_values) if sigma_values else 0
                avg_sigma = round(sum(sigma_values) / len(sigma_values), 1) if sigma_values else 0

                return {
                    "ok": True,
                    "enabled": enabled,
                    "model": str(getattr(anom_cfg, "model", "rolling_zscore") or "rolling_zscore") if anom_cfg else "rolling_zscore",
                    "sigma_threshold": float(getattr(anom_cfg, "sigma_threshold", 2.0)) if anom_cfg else 2.0,
                    "events": all_events[:200],
                    "total_count": len(all_events),
                    "type_counts": type_counts,
                    "device_counts": device_counts,
                    "max_sigma": max_sigma,
                    "avg_sigma": avg_sigma,
                }
            except Exception as e:
                return {"ok": False, "error": str(e)}

        if action == "co2_live":
            try:
                co2_cfg = getattr(self.cfg, "co2", None)
                if not co2_cfg or not getattr(co2_cfg, "enabled", False):
                    return {"ok": True, "enabled": False}
                zone = str(getattr(co2_cfg, "bidding_zone", "DE_LU") or "DE_LU")
                green_thr = float(getattr(co2_cfg, "green_threshold_g_per_kwh", 150.0))
                dirty_thr = float(getattr(co2_cfg, "dirty_threshold_g_per_kwh", 400.0))

                now_ts = int(time.time())
                range_start = ((now_ts // 3600) - 2) * 3600
                df_now = self.storage.db.query_co2_intensity(zone, range_start, now_ts + 3600)
                ci = 0.0
                ci_hour_ts = 0
                if df_now is not None and not df_now.empty:
                    ci = float(df_now.iloc[-1].get("intensity_g_per_kwh", 0))
                    ci_hour_ts = int(df_now.iloc[-1].get("hour_ts", 0))

                device_rates = []
                if ci > 0:
                    live_snap = {}
                    try:
                        snap = self.live_store.snapshot()
                        for dk, points in snap.items():
                            if points:
                                live_snap[dk] = points[-1].get("power_total_w", 0.0)
                    except Exception:
                        pass
                    for d in self.cfg.devices:
                        watts = abs(live_snap.get(d.key, 0.0))
                        co2_g_h = watts * ci / 1000.0
                        device_rates.append({
                            "key": d.key,
                            "name": d.name,
                            "watts": round(watts, 0),
                            "co2_g_h": round(co2_g_h, 1),
                        })

                return {
                    "ok": True,
                    "current_intensity": round(ci, 1),
                    "intensity_hour_ts": ci_hour_ts,
                    "green_threshold": green_thr,
                    "dirty_threshold": dirty_thr,
                    "device_rates": device_rates,
                }
            except Exception as e:
                return {"ok": False, "error": str(e)}

        if action == "forecast":
            try:
                from shelly_analyzer.services.forecast import compute_forecast
                dk = str(params.get("device_key", "") or "")
                if not dk and self.cfg.devices:
                    dk = self.cfg.devices[0].key
                dev_name = dk
                for d in self.cfg.devices:
                    if d.key == dk:
                        dev_name = d.name
                        break
                fc_cfg = getattr(self.cfg, "forecast", None)
                price = self._get_effective_unit_price()
                r = compute_forecast(
                    self.storage.db, dk, dev_name,
                    horizon_days=int(getattr(fc_cfg, "horizon_days", 30)) if fc_cfg else 30,
                    price_eur_per_kwh=price,
                    history_days=int(getattr(fc_cfg, "history_days", 90)) if fc_cfg else 90,
                )
                if r is None:
                    return {"ok": True, "no_data": True}
                return {
                    "ok": True,
                    "device_key": r.device_key,
                    "device_name": r.device_name,
                    "avg_daily_kwh": r.avg_daily_kwh,
                    "trend_pct_per_month": r.trend_pct_per_month,
                    "forecast_next_month_kwh": r.forecast_next_month_kwh,
                    "forecast_next_month_cost": r.forecast_next_month_cost,
                    "forecast_year_kwh": r.forecast_year_kwh,
                    "forecast_year_cost": r.forecast_year_cost,
                    "history_dates": [str(d) for d in r.history_dates],
                    "history_kwh": r.history_kwh,
                    "forecast_dates": [str(d) for d in r.forecast_dates],
                    "forecast_kwh": r.forecast_kwh,
                    "forecast_upper": r.forecast_upper,
                    "forecast_lower": r.forecast_lower,
                    "weekday_profile": {str(k): v for k, v in r.weekday_profile.items()},
                    "hourly_profile": {str(k): v for k, v in r.hourly_profile.items()},
                }
            except Exception as e:
                return {"ok": False, "error": str(e)}

        if action == "standby":
            try:
                from shelly_analyzer.services.standby import generate_standby_report, analyze_standby_from_df
                price = self._get_effective_unit_price()
                report = generate_standby_report(self.storage.db, self.cfg.devices, price)

                # Fallback: if DB-based analysis found nothing, use computed
                # DataFrames (CSV-based, same source as Costs tab).
                if not report.devices:
                    from shelly_analyzer.services.standby import StandbyReport
                    results = []
                    for dev in self.cfg.devices:
                        if str(getattr(dev, "kind", "em")) == "switch":
                            continue
                        try:
                            cd = self.computed.get(dev.key)
                            if cd is None or cd.df is None or cd.df.empty:
                                continue
                            r = analyze_standby_from_df(cd.df, dev.key, dev.name, price)
                            if r is not None:
                                results.append(r)
                        except Exception:
                            pass
                    if results:
                        results.sort(key=lambda x: x.annual_standby_cost, reverse=True)
                        report = StandbyReport(
                            devices=results,
                            total_annual_standby_kwh=round(sum(r.annual_standby_kwh for r in results), 1),
                            total_annual_standby_cost=round(sum(r.annual_standby_cost for r in results), 2),
                            analysis_days=30,
                            generated_at=datetime.now().strftime("%Y-%m-%d %H:%M"),
                        )

                return {
                    "ok": True,
                    "total_annual_standby_kwh": report.total_annual_standby_kwh,
                    "total_annual_standby_cost": report.total_annual_standby_cost,
                    "analysis_days": report.analysis_days,
                    "devices": [
                        {
                            "device_key": d.device_key,
                            "device_name": d.device_name,
                            "base_load_w": d.base_load_w,
                            "night_median_w": getattr(d, "night_median_w", 0.0),
                            "standby_pct": getattr(d, "standby_pct", 0.0),
                            "total_kwh": getattr(d, "total_kwh", 0.0),
                            "annual_standby_kwh": d.annual_standby_kwh,
                            "annual_standby_cost": d.annual_standby_cost,
                            "standby_share_pct": d.standby_share_pct,
                            "risk": d.risk,
                            "hourly_profile": d.hourly_profile,
                        }
                        for d in report.devices
                    ],
                }
            except Exception as e:
                return {"ok": False, "error": str(e)}

        if action == "sankey":
            try:
                from shelly_analyzer.services.sankey import compute_sankey, sankey_to_plotly_dict
                period_sk = str(params.get("period", "today") or "today")
                data = compute_sankey(self.storage.db, self.cfg.devices, self.cfg.solar, period_sk)
                plotly_data = sankey_to_plotly_dict(data)
                return {
                    "ok": True,
                    "grid_import_kwh": data.grid_import_kwh,
                    "pv_production_kwh": data.pv_production_kwh,
                    "self_consumption_kwh": data.self_consumption_kwh,
                    "feed_in_kwh": data.feed_in_kwh,
                    "total_consumption_kwh": data.total_consumption_kwh,
                    "sankey": plotly_data,
                }
            except Exception as e:
                return {"ok": False, "error": str(e)}

        if action == "co2":
            try:
                co2_cfg = getattr(self.cfg, "co2", None)
                enabled = bool(getattr(co2_cfg, "enabled", False)) if co2_cfg else False
                if not enabled:
                    return {"ok": True, "enabled": False}

                zone = str(getattr(co2_cfg, "bidding_zone", "DE_LU") or "DE_LU")
                green_thr = float(getattr(co2_cfg, "green_threshold_g_per_kwh", 150.0))
                dirty_thr = float(getattr(co2_cfg, "dirty_threshold_g_per_kwh", 400.0))
                cross_border = bool(getattr(co2_cfg, "cross_border_flows", False))

                _tzc = ZoneInfo("Europe/Berlin")
                _nowc = datetime.now(_tzc)
                _todayc = _nowc.replace(hour=0, minute=0, second=0, microsecond=0)
                _week_start = _todayc - timedelta(days=_nowc.weekday())
                _month_start = _todayc.replace(day=1)
                _year_start = _todayc.replace(month=1, day=1)

                today_start_ts = int(_todayc.timestamp())
                week_start_ts = int(_week_start.timestamp())
                month_start_ts = int(_month_start.timestamp())
                year_start_ts = int(_year_start.timestamp())
                now_ts = int(_nowc.timestamp())

                co2_range = str(params.get("range", "24h")) if params else "24h"
                if co2_range == "7d":
                    h24_start = now_ts - 7 * 86400
                elif co2_range == "30d":
                    h24_start = now_ts - 30 * 86400
                elif co2_range == "all":
                    oldest = self.storage.db.oldest_co2_ts(zone)
                    h24_start = oldest if oldest else now_ts - 24 * 3600
                else:
                    h24_start = now_ts - 24 * 3600
                h24_start = (h24_start // 3600) * 3600
                df_24h = self.storage.db.query_co2_intensity(zone, h24_start, now_ts + 3600)

                hourly_data = []
                current_intensity = 0.0
                current_source = "unknown"
                current_hour_ts = 0
                if df_24h is not None and not df_24h.empty:
                    for _, row in df_24h.iterrows():
                        ts_v = int(row.get("hour_ts", 0))
                        intensity = float(row.get("intensity_g_per_kwh", 0))
                        source = str(row.get("source", ""))
                        if co2_range in ("7d", "30d", "all"):
                            hour_str = datetime.fromtimestamp(ts_v, tz=_tzc).strftime("%d.%m %H:%M")
                        else:
                            hour_str = datetime.fromtimestamp(ts_v, tz=_tzc).strftime("%H:%M")
                        hourly_data.append({
                            "hour": hour_str,
                            "ts": ts_v,
                            "intensity": round(intensity, 1),
                            "source": source,
                        })
                    last_row = df_24h.iloc[-1]
                    current_intensity = float(last_row.get("intensity_g_per_kwh", 0))
                    current_source = str(last_row.get("source", ""))
                    current_hour_ts = int(last_row.get("hour_ts", 0))

                _solar_cfg = getattr(self.cfg, "solar", None)
                _pv_key = getattr(_solar_cfg, "pv_meter_device_key", "") if _solar_cfg else ""
                _pv_on = bool(getattr(_solar_cfg, "enabled", False)) if _solar_cfg else False

                def _device_co2(start_ts_d, end_ts_d):
                    grid_kwh_d = 0.0
                    pv_saved_kwh = 0.0
                    df_co2 = self.storage.db.query_co2_intensity(zone, start_ts_d, end_ts_d + 3600)
                    if df_co2 is None or df_co2.empty:
                        return 0.0
                    avg_int = float(pd.to_numeric(df_co2["intensity_g_per_kwh"], errors="coerce").mean())
                    if avg_int <= 0:
                        return 0.0
                    for d in self.cfg.devices:
                        try:
                            df_h = self.storage.db.query_hourly(d.key, start_ts=start_ts_d, end_ts=end_ts_d)
                            if df_h is None or df_h.empty or "kwh" not in df_h.columns:
                                continue
                            if _pv_on and d.key == _pv_key:
                                kwh_vals = pd.to_numeric(df_h["kwh"], errors="coerce").fillna(0.0)
                                grid_kwh_d += float(kwh_vals.clip(lower=0).sum())
                                pv_saved_kwh += float(kwh_vals.clip(upper=0).abs().sum())
                            else:
                                grid_kwh_d += float(pd.to_numeric(df_h["kwh"], errors="coerce").fillna(0.0).clip(lower=0).sum())
                        except Exception:
                            pass
                    net_kg = max(0.0, (grid_kwh_d - pv_saved_kwh) * avg_int / 1000.0)
                    return net_kg

                co2_today = _device_co2(today_start_ts, now_ts)
                co2_week = _device_co2(week_start_ts, now_ts)
                co2_month = _device_co2(month_start_ts, now_ts)
                co2_year = _device_co2(year_start_ts, now_ts)

                device_rates = []
                if current_intensity > 0:
                    live_snap = {}
                    try:
                        snap = self.live_store.snapshot()
                        for dk, points in snap.items():
                            if points:
                                live_snap[dk] = points[-1].get("power_total_w", 0.0)
                    except Exception:
                        pass
                    for d in self.cfg.devices:
                        watts = abs(live_snap.get(d.key, 0.0))
                        co2_g_h = watts * current_intensity / 1000.0
                        device_rates.append({
                            "key": d.key,
                            "name": d.name,
                            "watts": round(watts, 0),
                            "co2_g_h": round(co2_g_h, 1),
                        })

                fuel_mix = {}
                fuel_mix_hour = None
                try:
                    mix_hour, mix_data = None, {}
                    mix_hour, mix_data = self.storage.db.query_latest_fuel_mix(zone)
                    if mix_hour and mix_data:
                        fuel_mix_hour = datetime.fromtimestamp(mix_hour, tz=_tzc).strftime("%H:%M")
                        from shelly_analyzer.services.entsoe import _CO2_FACTORS, FUEL_DISPLAY_NAMES
                        total_mw = sum(mix_data.values())
                        for fuel, mw in sorted(mix_data.items(), key=lambda x: -x[1]):
                            if mw > 0:
                                fuel_mix[fuel] = {
                                    "name": FUEL_DISPLAY_NAMES.get(fuel, fuel),
                                    "mw": round(mw, 0),
                                    "share_pct": round(mw / total_mw * 100, 1) if total_mw > 0 else 0,
                                    "factor": _CO2_FACTORS.get(fuel, 400.0),
                                }
                except Exception:
                    pass

                device_hourly_co2 = []
                df_co2_24h = self.storage.db.query_co2_intensity(zone, h24_start, now_ts + 3600)
                co2_by_hour = {}
                if df_co2_24h is not None and not df_co2_24h.empty:
                    for _, row in df_co2_24h.iterrows():
                        co2_by_hour[int(row["hour_ts"])] = float(row["intensity_g_per_kwh"])
                for d in self.cfg.devices:
                    if int(getattr(d, "phases", 3) or 3) < 3:
                        continue
                    if str(getattr(d, "kind", "em")) == "switch":
                        continue
                    try:
                        df_h = self.storage.db.query_hourly(d.key, start_ts=h24_start, end_ts=now_ts + 3600)
                        if df_h is None or df_h.empty or "kwh" not in df_h.columns:
                            continue
                        bars = []
                        for _, hrow in df_h.iterrows():
                            hts = int(hrow.get("hour_ts", 0))
                            kwh_v = float(pd.to_numeric(hrow.get("kwh", 0), errors="coerce") or 0)
                            if kwh_v < 0:
                                kwh_v = 0.0
                            ci_h = co2_by_hour.get(hts, current_intensity)
                            co2_g = kwh_v * ci_h
                            hour_str = datetime.fromtimestamp(hts, tz=_tzc).strftime("%H:%M")
                            bars.append({
                                "hour": hour_str,
                                "ts": hts,
                                "kwh": round(kwh_v, 4),
                                "co2_g": round(co2_g, 1),
                                "intensity": round(ci_h, 1),
                            })
                        if bars:
                            total_co2_g = sum(b["co2_g"] for b in bars)
                            device_hourly_co2.append({
                                "key": d.key,
                                "name": d.name,
                                "total_co2_g": round(total_co2_g, 1),
                                "bars": bars,
                            })
                    except Exception:
                        continue

                tree_days = co2_month / 22.0 * 365 if co2_month > 0 else 0
                car_km = co2_month / 0.170 if co2_month > 0 else 0

                return {
                    "ok": True,
                    "enabled": True,
                    "zone": zone,
                    "cross_border": cross_border,
                    "green_threshold": green_thr,
                    "dirty_threshold": dirty_thr,
                    "current_intensity": round(current_intensity, 1),
                    "intensity_hour_ts": current_hour_ts,
                    "current_source": current_source,
                    "co2_today_kg": round(co2_today, 3),
                    "co2_week_kg": round(co2_week, 3),
                    "co2_month_kg": round(co2_month, 3),
                    "co2_year_kg": round(co2_year, 3),
                    "tree_days": round(tree_days, 0),
                    "car_km": round(car_km, 0),
                    "hourly": hourly_data,
                    "device_rates": device_rates,
                    "fuel_mix": fuel_mix,
                    "fuel_mix_hour": fuel_mix_hour,
                    "device_hourly_co2": device_hourly_co2,
                }
            except Exception as e:
                return {"ok": False, "error": str(e)}

        # -- New feature action handlers --
        if action == "smart_schedule":
            try:
                from shelly_analyzer.services.smart_schedule import find_cheapest_block
                zone = str(getattr(self.cfg.spot_price, "bidding_zone", "DE-LU") or "DE-LU")
                duration = float(params.get("duration", getattr(self.cfg.smart_schedule, "default_duration_hours", 3.0)))
                # Surcharges + VAT → real price the user will pay
                _sp_cfg = getattr(self.cfg, "spot_price", None)
                _markup_ct = 0.0
                _vat_f = 1.0
                try:
                    if _sp_cfg is not None:
                        _markup_ct = float(_sp_cfg.total_markup_ct() if hasattr(_sp_cfg, "total_markup_ct") else getattr(_sp_cfg, "markup_ct_per_kwh", 0.0))
                        if bool(getattr(_sp_cfg, "include_vat", True)):
                            _vat_f = 1.0 + float(self.cfg.pricing.vat_rate())
                except Exception:
                    pass
                _fixed_ct = round(float(self.cfg.pricing.electricity_price_eur_per_kwh) * 100.0, 2)

                _now_ts = int(time.time())
                _win_start = (_now_ts // 3600) * 3600
                _win_end = _win_start + 24 * 3600
                _df_sp = self.storage.db.query_spot_prices(zone, _win_start, _win_end)
                if _df_sp is None or _df_sp.empty:
                    return {"ok": True, "data": {"blocks": [], "duration_h": duration, "fixed_ct": _fixed_ct}}
                _prices_raw = list(zip(_df_sp["slot_ts"].astype(int), _df_sp["price_eur_mwh"].astype(float)))
                _prices_raw.sort(key=lambda x: x[0])

                def _to_real_ct(eur_mwh: float) -> float:
                    return ((eur_mwh / 10.0) + _markup_ct) * _vat_f

                # Find top-3 non-overlapping cheapest blocks
                blocks_out: List[Dict[str, Any]] = []
                remaining = list(_prices_raw)
                _all_avg_raw = sum(p for _, p in _prices_raw) / max(1, len(_prices_raw))
                _all_avg_ct = round(_to_real_ct(_all_avg_raw), 2)
                _min_p = min(p for _, p in _prices_raw)
                _max_p = max(p for _, p in _prices_raw)
                _cheapest_ct = round(_to_real_ct(_min_p), 2)
                _mostexp_ct = round(_to_real_ct(_max_p), 2)

                for _ in range(3):
                    rec = find_cheapest_block(remaining, duration, earliest_ts=_win_start, latest_ts=_win_end)
                    if rec is None:
                        break
                    _real_avg_ct = round(_to_real_ct(rec.avg_price_ct * 10.0), 2)  # rec.avg_price_ct is ct (raw), convert back to eur/MWh for helper
                    _save_vs_fixed = round((_fixed_ct - _real_avg_ct) * float(duration), 1)  # ct/kWh diff × hours (= ct saved for a 1 kW load over the block)
                    blocks_out.append({
                        "start_ts": rec.start_ts,
                        "end_ts": rec.end_ts,
                        "avg_price_ct": _real_avg_ct,
                        "savings_vs_avg_ct": round(_all_avg_ct - _real_avg_ct, 2),
                        "savings_vs_fixed_ct_per_kwh": round(_fixed_ct - _real_avg_ct, 2),
                        "block_hours": rec.block_hours,
                    })
                    # Remove block from remaining for next iteration (non-overlapping top-N)
                    remaining = [(ts, p) for (ts, p) in remaining if not (rec.start_ts <= ts < rec.end_ts)]

                return {"ok": True, "data": {
                    "blocks": blocks_out,
                    "duration_h": duration,
                    "fixed_ct": _fixed_ct,
                    "avg_24h_ct": _all_avg_ct,
                    "cheapest_hour_ct": _cheapest_ct,
                    "most_expensive_hour_ct": _mostexp_ct,
                    "window_start_ts": _win_start,
                    "window_end_ts": _win_end,
                    "zone": zone,
                }}
            except Exception as e:
                return {"ok": False, "error": str(e)}

        if action == "ev_sessions":
            try:
                from shelly_analyzer.services.ev_charging_log import detect_charging_sessions, get_monthly_summary
                dev_key = str(getattr(self.cfg.ev_charging, "wallbox_device_key", "") or "")
                if not dev_key:
                    return {"ok": True, "data": {"total_sessions": 0, "total_kwh": 0, "total_cost": 0, "sessions": []}}
                df_ev = self.storage.read_device_df(dev_key)
                sessions = detect_charging_sessions(
                    df_ev, dev_key,
                    threshold_w=float(getattr(self.cfg.ev_charging, "detection_threshold_w", 1500)),
                    min_duration_s=int(getattr(self.cfg.ev_charging, "min_session_minutes", 5)) * 60,
                    price_eur_per_kwh=float(self.cfg.pricing.electricity_price_eur_per_kwh),
                )
                summary_ev = get_monthly_summary(sessions)
                return {"ok": True, "data": {
                    "total_sessions": summary_ev.total_sessions,
                    "total_kwh": summary_ev.total_kwh,
                    "total_cost": summary_ev.total_cost,
                    "avg_kwh_per_session": summary_ev.avg_kwh_per_session,
                    "avg_duration_min": summary_ev.avg_duration_min,
                    "sessions": [
                        {"session_id": s.session_id, "start_ts": s.start_ts, "end_ts": s.end_ts,
                         "energy_kwh": s.energy_kwh, "peak_power_w": s.peak_power_w,
                         "avg_power_w": s.avg_power_w, "cost_eur": s.cost_eur}
                        for s in sessions
                    ],
                }}
            except Exception as e:
                return {"ok": False, "error": str(e)}

        if action == "tariff_compare":
            try:
                from shelly_analyzer.services.tariff_compare import compare_tariffs
                results = compare_tariffs(
                    self.storage.db, self.cfg,
                    current_price_eur_per_kwh=float(self.cfg.pricing.electricity_price_eur_per_kwh),
                    current_base_fee_eur_per_year=float(self.cfg.pricing.base_fee_eur_per_year),
                )
                return {"ok": True, "data": {"results": [
                    {"name": r.name, "provider": r.provider, "tariff_type": r.tariff_type,
                     "annual_cost_eur": r.annual_cost_eur, "monthly_avg_eur": r.monthly_avg_eur,
                     "effective_price_ct": r.effective_price_ct,
                     "savings_vs_current_eur": r.savings_vs_current_eur, "is_current": r.is_current}
                    for r in results
                ]}}
            except Exception as e:
                return {"ok": False, "error": str(e)}

        if action == "battery":
            try:
                from shelly_analyzer.services.battery import get_battery_status
                status = get_battery_status(self.storage.db, self.cfg.battery)
                return {"ok": True, "data": {
                    "soc_pct": status.soc_pct, "power_w": status.power_w,
                    "mode": status.mode, "cycle_count": status.cycle_count,
                    "total_charged_kwh": status.total_charged_kwh,
                    "total_discharged_kwh": status.total_discharged_kwh,
                    "avg_efficiency_pct": status.avg_efficiency_pct,
                }}
            except Exception as e:
                return {"ok": False, "error": str(e)}

        if action == "advisor":
            try:
                from shelly_analyzer.services.ai_advisor import get_advisor_tips
                result = get_advisor_tips(self.storage.db, self.cfg, self.storage)
                return {"ok": True, "data": result}
            except Exception as e:
                return {"ok": False, "error": str(e)}

        if action == "goals":
            try:
                from shelly_analyzer.services.gamification import get_gamification_status
                result = get_gamification_status(self.storage.db, self.cfg)
                return {"ok": True, "data": result}
            except Exception as e:
                return {"ok": False, "error": str(e)}

        raise ValueError(f"Unknown action: {action}")

    # ==================================================================
    # _web_plots_data (Plotly JSON builder)
    # ==================================================================

    def _web_plots_data(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Build JSON payload for the Plotly /plots page."""
        try:
            view = str(params.get("view") or "timeseries")
            devices_raw = str(params.get("devices") or "")
            dev_keys_in = [k.strip() for k in devices_raw.split(",") if k and str(k).strip()]
            lang = str(params.get("lang") or self.lang or "de")

            dev_cfgs = {d.key: d for d in self.cfg.devices}

            def _norm(s: str) -> str:
                s = (s or "").strip().lower()
                s = s.replace("\u00e4", "ae").replace("\u00f6", "oe").replace("\u00fc", "ue").replace("\u00df", "ss")
                for ch in [" ", "-", "_", "."]:
                    s = s.replace(ch, "")
                return s

            alias: Dict[str, str] = {}
            for d in self.cfg.devices:
                alias[_norm(getattr(d, "key", ""))] = d.key
                alias[_norm(getattr(d, "name", ""))] = d.key
            dev_keys: List[str] = []
            for raw in dev_keys_in:
                k = alias.get(_norm(raw), raw)
                if k and k not in dev_keys:
                    dev_keys.append(k)
            dev_keys = dev_keys[:2]

            try:
                self.storage.ensure_data_for_devices(
                    [{"key": d.key, "host": getattr(d, "host", ""), "name": getattr(d, "name", d.key)} for d in self.cfg.devices]
                )
            except Exception:
                pass

            if not dev_keys:
                dev_keys = [d.key for d in self.cfg.devices[:2] if getattr(d, "key", None)]

            start = None
            end = None
            try:
                if str(params.get("start") or "").strip():
                    start = _parse_date_flexible(str(params.get("start")))
                if str(params.get("end") or "").strip():
                    end = _parse_date_flexible(str(params.get("end")))
            except Exception:
                start = None
                end = None

            def _df_for(key: str) -> pd.DataFrame:
                try:
                    return self.storage.read_device_df(key)
                except Exception:
                    return pd.DataFrame()

            if view == "kwh":
                mode = str(params.get("mode") or "days")

                try:
                    if start is None and end is None and str(params.get("len") or "").strip():
                        try:
                            ln_kwh = float(params.get("len") or 24.0)
                        except Exception:
                            ln_kwh = 24.0
                        unit_kwh = str(params.get("unit") or "hours")
                        delta_kwh = pd.Timedelta(hours=ln_kwh)
                        if unit_kwh.startswith("min"):
                            delta_kwh = pd.Timedelta(minutes=ln_kwh)
                        elif unit_kwh.startswith("day"):
                            delta_kwh = pd.Timedelta(days=ln_kwh)
                        _kwh_preset = {"delta": delta_kwh}
                    else:
                        _kwh_preset = None
                except Exception:
                    _kwh_preset = None
                labels: List[str] = []
                traces: List[Dict[str, Any]] = []
                diag: Dict[str, Any] = {
                    "requested_devices": dev_keys,
                    "counts": {},
                    "data": getattr(self.storage, "last_data_diag", {}),
                    "base_dir": str(getattr(self.storage, "base_dir", "")),
                }
                for k in dev_keys:
                    df = _df_for(k)
                    if df is None or len(df) == 0:
                        diag["counts"][k] = 0
                        continue
                    diag["counts"][k] = int(len(df))
                    if start is not None or end is not None:
                        df = filter_by_time(df, start, end)
                    elif _kwh_preset is not None:
                        try:
                            col = "timestamp" if "timestamp" in df.columns else ("ts" if "ts" in df.columns else None)
                            if col:
                                end_i = pd.to_datetime(df[col], errors="coerce").max()
                                if end_i is not pd.NaT and end_i == end_i:
                                    start_i = end_i - _kwh_preset["delta"]
                                    df = filter_by_time(df, start_i, end_i)
                        except Exception:
                            pass
                    lbls, vals = self._stats_series(df, mode)
                    s = pd.Series(vals, index=[str(x) for x in lbls], dtype="float64")

                    idx = [str(x) for x in s.index.tolist()]
                    if not labels:
                        labels = idx
                    else:
                        all_lab = sorted(set(labels).union(idx))
                        labels = all_lab
                    name = dev_cfgs.get(k).name if k in dev_cfgs else k
                    traces.append({"key": k, "name": name, "series": s})

                out_traces: List[Dict[str, Any]] = []
                for tr in traces:
                    s = tr["series"]
                    y = []
                    for lab in labels:
                        try:
                            y.append(float(s.get(lab, 0.0)))
                        except Exception:
                            y.append(0.0)
                    out_traces.append({"key": tr["key"], "name": tr["name"], "y": y})

                # Parse bucket unit from mode ("hours" / "days" / "weeks" / "months" / "hours:24" …)
                _mode_s = str(mode or "days").lower().strip()
                unit = _mode_s.split(":", 1)[0].strip() if ":" in _mode_s else _mode_s

                # Total kWh per label bucket (sum across all devices)
                total_per_label: List[float] = []
                for i, _ in enumerate(labels):
                    s_tot = 0.0
                    for tr in out_traces:
                        try:
                            s_tot += float(tr["y"][i])
                        except Exception:
                            pass
                    total_per_label.append(s_tot)

                # CO2 (g) and price (EUR) aggregated per bucket, using db tables
                def _label_to_ts_range(lab: str, u: str):
                    """Return (start_ts, end_ts) seconds for a bucket label.
                    Labels are local time (Europe/Berlin) – localize before
                    getting the UTC POSIX timestamp."""
                    try:
                        if u == "hours":
                            t0 = pd.Timestamp(lab + ":00")
                            t1 = t0 + pd.Timedelta(hours=1)
                        elif u == "weeks":
                            # "YYYY-Wxx" → Monday of ISO week
                            yr, wk = lab.split("-W")
                            t0 = pd.Timestamp.fromisocalendar(int(yr), int(wk), 1)
                            t1 = t0 + pd.Timedelta(days=7)
                        elif u == "months":
                            t0 = pd.Timestamp(lab + "-01")
                            t1 = (t0 + pd.offsets.MonthBegin(1)).normalize() if False else (t0 + pd.DateOffset(months=1))
                        else:  # days
                            t0 = pd.Timestamp(lab).normalize()
                            t1 = t0 + pd.Timedelta(days=1)
                        try:
                            t0 = t0.tz_localize("Europe/Berlin", nonexistent="shift_forward", ambiguous="NaT")
                            t1 = t1.tz_localize("Europe/Berlin", nonexistent="shift_forward", ambiguous="NaT")
                        except Exception:
                            pass
                        return int(t0.timestamp()), int(t1.timestamp())
                    except Exception:
                        return None, None

                co2_intensity: List[Optional[float]] = [None] * len(labels)
                price_ct_kwh: List[Optional[float]] = [None] * len(labels)
                co2_zone = None
                price_zone = None
                co2_green_thr = 150.0
                co2_dirty_thr = 400.0
                # Include spot-price surcharges (grid fee, tax, VAT) – default on
                include_surcharges = str(params.get("include_surcharges", "1")).lower() not in {"0", "false", "no"}
                surcharge_markup_ct = 0.0
                vat_factor = 1.0
                try:
                    co2_cfg = getattr(self.cfg, "co2", None)
                    if co2_cfg and getattr(co2_cfg, "enabled", False):
                        co2_zone = str(getattr(co2_cfg, "bidding_zone", "DE_LU") or "DE_LU")
                        co2_green_thr = float(getattr(co2_cfg, "green_threshold_g_per_kwh", 150.0))
                        co2_dirty_thr = float(getattr(co2_cfg, "dirty_threshold_g_per_kwh", 400.0))
                except Exception:
                    co2_zone = None
                try:
                    spot_cfg = getattr(self.cfg, "spot_price", None)
                    if spot_cfg and getattr(spot_cfg, "enabled", False):
                        price_zone = str(getattr(spot_cfg, "bidding_zone", "DE-LU") or "DE-LU")
                        if include_surcharges:
                            try:
                                surcharge_markup_ct = float(spot_cfg.total_markup_ct())
                            except Exception:
                                surcharge_markup_ct = 0.0
                            if bool(getattr(spot_cfg, "include_vat", True)):
                                vat_factor = 1.19
                except Exception:
                    price_zone = None

                try:
                    ranges = [_label_to_ts_range(lab, unit) for lab in labels]
                    valid = [r for r in ranges if r[0] is not None]
                    if valid and (co2_zone or price_zone):
                        rng_start = min(r[0] for r in valid)
                        rng_end = max(r[1] for r in valid)
                        co2_df = None
                        price_df = None
                        try:
                            if co2_zone:
                                co2_df = self.storage.db.query_co2_intensity(co2_zone, rng_start, rng_end)
                        except Exception:
                            co2_df = None
                        try:
                            if price_zone:
                                price_df = self.storage.db.query_spot_prices(price_zone, rng_start, rng_end)
                        except Exception:
                            price_df = None

                        def _avg_in_range(df: "pd.DataFrame", col: str, ts_col: str, a: int, b: int):
                            if df is None or df.empty:
                                return None
                            try:
                                m = (df[ts_col] >= a) & (df[ts_col] < b)
                                sub = df.loc[m, col]
                                if sub.empty:
                                    return None
                                v = float(sub.mean())
                                return v if v == v else None
                            except Exception:
                                return None

                        for i, (a, b) in enumerate(ranges):
                            if a is None:
                                continue
                            if co2_df is not None:
                                avg_g = _avg_in_range(co2_df, "intensity_g_per_kwh", "hour_ts", a, b)
                                if avg_g is not None:
                                    co2_intensity[i] = round(avg_g, 1)
                            if price_df is not None:
                                avg_eur_mwh = _avg_in_range(price_df, "price_eur_mwh", "slot_ts", a, b)
                                if avg_eur_mwh is not None:
                                    # Spot wholesale ct/kWh (gross) incl. surcharges + VAT if configured
                                    base_ct = avg_eur_mwh * 0.1  # €/MWh → ct/kWh
                                    final_ct = (base_ct + surcharge_markup_ct) * vat_factor
                                    price_ct_kwh[i] = round(final_ct, 2)
                except Exception:
                    pass

                title = f"kWh \u2022 {mode}"
                # Fixed tariff price (€/kWh, gross) from PricingConfig
                fixed_ct_kwh: Optional[float] = None
                try:
                    pricing_cfg = getattr(self.cfg, "pricing", None)
                    if pricing_cfg is not None:
                        p_eur = float(getattr(pricing_cfg, "electricity_price_eur_per_kwh", 0.0) or 0.0)
                        incl_vat = bool(getattr(pricing_cfg, "price_includes_vat", True))
                        vat_enabled = bool(getattr(pricing_cfg, "vat_enabled", True))
                        vat_pct = float(getattr(pricing_cfg, "vat_rate_percent", 19.0) or 19.0)
                        # Normalize to gross ct/kWh
                        if not incl_vat and vat_enabled:
                            p_eur = p_eur * (1.0 + vat_pct / 100.0)
                        fixed_ct_kwh = round(p_eur * 100.0, 3) if p_eur > 0 else None
                except Exception:
                    fixed_ct_kwh = None

                # Per-device CO2 (g) and price (EUR) aggregations
                co2_per_device: List[Dict[str, Any]] = []
                price_per_device: List[Dict[str, Any]] = []
                for tr in out_traces:
                    y_dev = tr.get("y") or []
                    g_arr: List[Optional[float]] = []
                    eur_arr: List[Optional[float]] = []
                    for i in range(len(labels)):
                        try:
                            kwh_dev = float(y_dev[i]) if i < len(y_dev) else 0.0
                        except Exception:
                            kwh_dev = 0.0
                        gi = co2_intensity[i] if i < len(co2_intensity) else None
                        ci = price_ct_kwh[i] if i < len(price_ct_kwh) else None
                        g_arr.append(round(gi * kwh_dev, 1) if gi is not None else None)
                        # ct/kWh × kWh / 100 = €
                        eur_arr.append(round(ci * kwh_dev / 100.0, 2) if ci is not None else None)
                    # Fixed-tariff EUR per bucket for this device (ct_fix × kWh / 100)
                    fixed_eur_arr: List[Optional[float]] = []
                    if fixed_ct_kwh is not None:
                        for i in range(len(labels)):
                            try:
                                kwh_dev = float(y_dev[i]) if i < len(y_dev) else 0.0
                            except Exception:
                                kwh_dev = 0.0
                            fixed_eur_arr.append(round(fixed_ct_kwh * kwh_dev / 100.0, 2))
                    else:
                        fixed_eur_arr = [None] * len(labels)
                    co2_per_device.append({"key": tr["key"], "name": tr["name"], "g": g_arr})
                    price_per_device.append({"key": tr["key"], "name": tr["name"], "eur": eur_arr, "eur_fixed": fixed_eur_arr})

                return {
                    "ok": True, "view": "kwh",
                    "labels": labels, "traces": out_traces,
                    "total_kwh": total_per_label,
                    "co2_intensity_g_per_kwh": co2_intensity,
                    "co2_green_thr": co2_green_thr, "co2_dirty_thr": co2_dirty_thr,
                    "co2_per_device": co2_per_device,
                    "price_ct_kwh": price_ct_kwh,
                    "price_fixed_ct_kwh": fixed_ct_kwh,
                    "price_per_device": price_per_device,
                    "price_surcharges_included": include_surcharges and (surcharge_markup_ct > 0 or vat_factor != 1.0),
                    "price_surcharge_ct": round(surcharge_markup_ct, 3),
                    "price_vat_pct": round((vat_factor - 1.0) * 100, 0),
                    "co2_zone": co2_zone, "price_zone": price_zone,
                    "unit": unit, "title": title, "diag": diag,
                }

            # timeseries
            metric = str(params.get("metric") or "W").upper().strip()
            metric_norm = metric
            metric_label = {'W':'W','V':'V','A':'A','VAR':'VAR','Q':'VAR','COSPHI':'cos \u03c6','PF':'cos \u03c6','POWERFACTOR':'cos \u03c6'}.get(metric_norm, metric_norm)

            series = str(params.get("series") or "total").lower().strip()
            series_mode = 'phases' if series.startswith('phase') else 'total'

            try:
                ln = float(params.get("len") or 24.0)
            except Exception:
                ln = 24.0
            unit_ts = str(params.get("unit") or "hours")
            delta = pd.Timedelta(hours=ln)
            if unit_ts.startswith("min"):
                delta = pd.Timedelta(minutes=ln)
            elif unit_ts.startswith("day"):
                delta = pd.Timedelta(days=ln)

            out_devs: List[Dict[str, Any]] = []
            diag_ts: Dict[str, Any] = {
                "requested_devices": dev_keys,
                "counts": {},
                "data": getattr(self.storage, "last_data_diag", {}),
                "base_dir": str(getattr(self.storage, "base_dir", "")),
            }
            for k in dev_keys:
                df = _df_for(k)
                if df is None or len(df) == 0:
                    diag_ts["counts"][k] = 0
                    continue
                diag_ts["counts"][k] = int(len(df))
                if start is None and end is None:
                    try:
                        _tcol = "timestamp" if "timestamp" in df.columns else ("ts" if "ts" in df.columns else None)
                        end_i = pd.to_datetime(df[_tcol]).max() if _tcol else None
                        start_i = end_i - delta
                    except Exception:
                        start_i, end_i = None, None
                    dff = filter_by_time(df, start_i, end_i)
                else:
                    dff = filter_by_time(df, start, end)

                s_total, ylab = self._wva_series(dff, metric)
                phases = self._wva_phase_series(dff, metric)
                try:
                    if not isinstance(s_total.index, pd.DatetimeIndex) and 'timestamp' in dff.columns:
                        ts_idx = pd.to_datetime(dff['timestamp'], errors='coerce')
                        s_total = pd.Series(pd.to_numeric(s_total, errors='coerce').to_numpy(), index=pd.DatetimeIndex(ts_idx)).dropna().sort_index()
                        if s_total.index.has_duplicates:
                            s_total = s_total.groupby(level=0).mean()
                    if isinstance(phases, dict) and phases and 'timestamp' in dff.columns:
                        ts_idx = pd.DatetimeIndex(pd.to_datetime(dff['timestamp'], errors='coerce'))
                        msk = ~pd.isna(ts_idx)
                        for kk in list(phases.keys()):
                            try:
                                ps = phases.get(kk)
                                if ps is None:
                                    continue
                                if not isinstance(ps.index, pd.DatetimeIndex):
                                    ps = pd.Series(pd.to_numeric(ps, errors='coerce').to_numpy(), index=ts_idx)
                                ps = ps[msk].dropna().sort_index()
                                if ps.index.has_duplicates:
                                    ps = ps.groupby(level=0).mean()
                                phases[kk] = ps
                            except Exception:
                                continue
                except Exception:
                    phases = {}

                try:
                    if int(getattr(dev_cfgs.get(k), "phases", 3) or 3) <= 1:
                        phases = {"L1": phases.get("L1")} if ("L1" in phases) else {}
                except Exception:
                    pass

                def _downsample(s: pd.Series) -> pd.Series:
                    try:
                        if len(s) <= 2500:
                            return s
                        span = (s.index.max() - s.index.min()) if len(s) else pd.Timedelta(hours=0)
                        rule = "1min"
                        if span > pd.Timedelta(days=14):
                            rule = "30min"
                        elif span > pd.Timedelta(days=3):
                            rule = "10min"
                        elif span > pd.Timedelta(hours=12):
                            rule = "2min"
                        return s.resample(rule).mean().dropna()
                    except Exception:
                        return s

                s_total = _downsample(s_total)
                _idx = pd.to_datetime(getattr(s_total, "index", []), errors="coerce")

                def _iso_ts(x: Any) -> str:
                    try:
                        if x is pd.NaT or x != x:
                            return ""
                        ts_v = pd.Timestamp(x)
                        try:
                            return ts_v.to_pydatetime(warn=False).isoformat()
                        except TypeError:
                            try:
                                ts_v = ts_v.floor("us")
                            except Exception:
                                pass
                            return ts_v.isoformat()
                    except Exception:
                        return ""

                xs = [_iso_ts(x) for x in _idx]
                ys = [float(v) if v == v else 0.0 for v in s_total.values.tolist()]
                dev_name = dev_cfgs.get(k).name if k in dev_cfgs else k
                out_d: Dict[str, Any] = {"key": k, "name": dev_name, "x": xs, "y": ys}
                if series_mode == "phases":
                    try:
                        out_d.pop("y", None)
                    except Exception:
                        pass
                try:
                    out_d["mapping"] = str(self._last_wva_mapping_text)
                except Exception:
                    out_d["mapping"] = ""
                if phases:
                    ph_out: Dict[str, Any] = {}
                    for pk, ps in phases.items():
                        ps = _downsample(ps)
                        ph_out[pk] = {
                            "x": [_iso_ts(x) for x in pd.to_datetime(getattr(ps, "index", []), errors="coerce")],
                            "y": [float(v) if v == v else 0.0 for v in ps.values.tolist()],
                        }
                    if series_mode == "phases":
                        out_d["phases"] = ph_out
                out_devs.append(out_d)

            title = f"{metric_label} \u2022 {ln:g} {unit_ts}"
            return {"ok": True, "view": "timeseries", "metric": metric, "metric_label": metric_label, "series": series_mode, "devices": out_devs, "title": title, "diag": diag_ts}
        except Exception as e:
            return {"ok": False, "error": str(e)}
