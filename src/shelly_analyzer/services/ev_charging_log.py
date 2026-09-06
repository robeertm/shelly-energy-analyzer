from __future__ import annotations
import hashlib
import logging
import threading
import time
from dataclasses import dataclass, field, replace
from typing import List, Optional

import numpy as np
import pandas as pd

_log = logging.getLogger(__name__)


@dataclass
class ChargingSession:
    session_id: str
    device_key: str
    start_ts: int
    end_ts: int
    energy_kwh: float
    peak_power_w: float
    avg_power_w: float
    cost_eur: float = 0.0
    cost_model: str = "fixed"
    # Where the energy came from. Zero and cost_model "fixed" while the session
    # is priced at the flat tariff; filled in by :func:`price_by_source` once a
    # grid/PV/battery measurement covers the charge. The three always add up to
    # ``energy_kwh`` so the log and the totals cannot drift apart.
    solar_kwh: float = 0.0
    battery_kwh: float = 0.0
    grid_kwh: float = 0.0
    source_coverage: float = 0.0   # 0..1, how much of the window was measured


@dataclass
class ChargingSummary:
    total_sessions: int = 0
    total_kwh: float = 0.0
    total_cost: float = 0.0
    avg_kwh_per_session: float = 0.0
    avg_duration_min: float = 0.0
    total_solar_kwh: float = 0.0
    total_battery_kwh: float = 0.0
    total_grid_kwh: float = 0.0
    cost_if_all_grid: float = 0.0   # what the same energy would have cost bought
    sessions: List[ChargingSession] = field(default_factory=list)


# In-process detection cache: keyed by (device_key, threshold, min_duration,
# price, max_gap). Hit while the underlying samples haven't grown.
_DETECT_CACHE: dict = {}
_DETECT_LOCK = threading.Lock()


def _fresh(sessions: List["ChargingSession"]) -> List["ChargingSession"]:
    """Hand every caller its own session objects.

    The cache used to return the very objects it stored, and copying only the
    *list* was not enough: :func:`price_by_source` writes the source split onto
    each session, so the second caller found the first one's prices baked into
    the cache — switching the pricing mode back to flat then changed nothing.
    """
    return [replace(s) for s in sessions]


def detect_charging_sessions(
    df: pd.DataFrame,
    device_key: str,
    threshold_w: float = 1500.0,
    min_duration_s: int = 300,
    price_eur_per_kwh: float = 0.30,
    max_gap_s: int = 900,
) -> List[ChargingSession]:
    """Detect EV charging sessions from a power time series.

    Vectorized via numpy — replaces the previous per-row iterrows loop, which
    was O(n) Python overhead per sample and dominated /api/ev_sessions latency
    on multi-month wallbox history (tens of thousands of rows).

    Semantics preserved from the iterative implementation:
      * A session starts when power first crosses ``threshold_w`` upward.
      * Hysteresis: it continues while power stays above ``threshold_w * 0.5``.
      * Brief drops below the hysteresis floor — up to ``max_gap_s`` seconds —
        are *bridged* so a single charge is not split by short data dips or
        one-off zero-power artifacts. The session ends after low has persisted
        longer than ``max_gap_s``; its end is the last active sample.
      * Bridged low-power samples are excluded from peak/avg/energy.
      * Sessions shorter than ``min_duration_s`` are dropped (brief spikes).
    """
    if df is None or df.empty:
        return []

    power_col = "total_power" if "total_power" in df.columns else "energy_kwh"
    if power_col not in df.columns or "timestamp" not in df.columns:
        return []

    df = df.sort_values("timestamp").reset_index(drop=True)

    _ts = df["timestamp"]
    if pd.api.types.is_datetime64_any_dtype(_ts):
        if getattr(_ts.dt, "tz", None) is not None:
            _ts = _ts.dt.tz_convert("UTC").dt.tz_localize(None)
        ts_arr = _ts.astype("datetime64[s]").astype("int64").to_numpy()
    else:
        ts_arr = pd.to_numeric(_ts, errors="coerce").fillna(0).astype("int64").to_numpy()

    power = pd.to_numeric(df[power_col], errors="coerce").fillna(0.0).to_numpy(dtype=np.float64)
    energy_arr = (
        pd.to_numeric(df["energy_kwh"], errors="coerce").fillna(0.0).to_numpy(dtype=np.float64)
        if "energy_kwh" in df.columns
        else None
    )

    n = len(power)
    if n == 0:
        return []

    cache_key = (
        device_key,
        float(threshold_w),
        int(min_duration_s),
        float(price_eur_per_kwh),
        int(max_gap_s),
    )
    max_ts = int(ts_arr[-1])
    with _DETECT_LOCK:
        cached = _DETECT_CACHE.get(cache_key)
        if cached and cached[0] == max_ts and cached[1] == n:
            return _fresh(cached[2])

    above_low = power >= threshold_w * 0.5
    above_trigger = power >= threshold_w

    active_idx = np.where(above_low)[0]
    if active_idx.size == 0:
        with _DETECT_LOCK:
            _DETECT_CACHE[cache_key] = (max_ts, n, [])
        return []

    if active_idx.size == 1:
        cluster_bounds = [(int(active_idx[0]), int(active_idx[0]))]
    else:
        gaps_ts = ts_arr[active_idx[1:]] - ts_arr[active_idx[:-1]]
        break_positions = np.where(gaps_ts > max_gap_s)[0]
        if break_positions.size:
            starts = np.concatenate([[active_idx[0]], active_idx[break_positions + 1]])
            ends = np.concatenate([active_idx[break_positions], [active_idx[-1]]])
        else:
            starts = active_idx[:1]
            ends = active_idx[-1:]
        cluster_bounds = list(zip(starts.tolist(), ends.tolist()))

    sessions: List[ChargingSession] = []
    for s_idx, e_idx in cluster_bounds:
        seg_trigger = above_trigger[s_idx:e_idx + 1]
        if not seg_trigger.any():
            continue
        first_trigger = s_idx + int(np.argmax(seg_trigger))
        start_ts = int(ts_arr[first_trigger])
        end_ts = int(ts_arr[e_idx])
        duration_s = end_ts - start_ts
        if duration_s < min_duration_s:
            continue

        # Only above_low samples in [first_trigger, e_idx] contribute to
        # energy / peak / avg. Bridged low-power dips are excluded so they
        # don't pull the averages and energy totals down.
        active_mask = above_low[first_trigger:e_idx + 1]
        seg_power = power[first_trigger:e_idx + 1][active_mask]
        if seg_power.size == 0:
            continue
        if energy_arr is not None:
            seg_energy = energy_arr[first_trigger:e_idx + 1][active_mask]
            energy = float(seg_energy.sum())
            if energy <= 0:
                energy = float(seg_power.mean()) * duration_s / 3600.0 / 1000.0
        else:
            energy = float(seg_power.mean()) * duration_s / 3600.0 / 1000.0

        sid = hashlib.md5(f"{device_key}:{start_ts}:{end_ts}".encode()).hexdigest()[:12]
        sessions.append(ChargingSession(
            session_id=sid,
            device_key=device_key,
            start_ts=start_ts,
            end_ts=end_ts,
            energy_kwh=round(energy, 3),
            peak_power_w=round(float(seg_power.max()), 1),
            avg_power_w=round(float(seg_power.mean()), 1),
            cost_eur=round(energy * price_eur_per_kwh, 2),
            cost_model="fixed",
        ))

    with _DETECT_LOCK:
        _DETECT_CACHE[cache_key] = (max_ts, n, sessions)
    return _fresh(sessions)


def get_monthly_summary(sessions: List[ChargingSession]) -> ChargingSummary:
    """Aggregate charging sessions into a summary."""
    if not sessions:
        return ChargingSummary()

    total_kwh = sum(s.energy_kwh for s in sessions)
    total_cost = sum(s.cost_eur for s in sessions)
    durations = [(s.end_ts - s.start_ts) / 60 for s in sessions]

    return ChargingSummary(
        total_sessions=len(sessions),
        total_kwh=round(total_kwh, 2),
        total_cost=round(total_cost, 2),
        avg_kwh_per_session=round(total_kwh / len(sessions), 2),
        avg_duration_min=round(sum(durations) / len(durations), 1),
        total_solar_kwh=round(sum(s.solar_kwh for s in sessions), 2),
        total_battery_kwh=round(sum(s.battery_kwh for s in sessions), 2),
        total_grid_kwh=round(sum(s.grid_kwh for s in sessions), 2),
        sessions=sessions,
    )


@dataclass
class ChargingGroup:
    """One physical charge = one or more detected sessions merged together."""
    group_id: str
    device_key: str
    start_ts: int
    end_ts: int
    energy_kwh: float
    peak_power_w: float
    avg_power_w: float
    cost_eur: float
    session_count: int
    solar_kwh: float = 0.0
    battery_kwh: float = 0.0
    grid_kwh: float = 0.0
    cost_model: str = "fixed"
    sessions: List[ChargingSession] = field(default_factory=list)


def group_sessions_into_charges(
    sessions: List[ChargingSession],
    max_gap_s: int = 14400,
    surplus_ts: Optional["np.ndarray"] = None,
    surplus_export_w: Optional["np.ndarray"] = None,
    min_charge_w: float = 1500.0,
) -> List[ChargingGroup]:
    """Merge consecutive sessions that belong to ONE physical charge.

    Surplus (PV) charging pauses whenever available solar drops below the car's
    minimum charge power, fragmenting one plugged-in charge into many short
    sessions. Two consecutive sessions are merged when:

      * the gap between them is <= ``max_gap_s`` (an overnight gap stays
        separate), AND
      * if a grid-export (surplus) series is supplied: the *median* available
        export during the gap stayed below ``min_charge_w`` — i.e. the car could
        not have charged then (still plugged in, waiting for sun) rather than
        being unplugged while surplus went spare. Without a series the gap alone
        decides (best effort).

    Each returned group keeps its member sessions so the UI can expand detail.
    """
    if not sessions:
        return []
    ordered = sorted(sessions, key=lambda s: s.start_ts)
    buckets: List[List[ChargingSession]] = [[ordered[0]]]
    have_surplus = (
        surplus_ts is not None and surplus_export_w is not None
        and getattr(surplus_ts, "size", 0)
    )
    for s in ordered[1:]:
        prev = buckets[-1][-1]
        gap = s.start_ts - prev.end_ts
        merge = 0 <= gap <= max_gap_s
        if merge and have_surplus:
            m = (surplus_ts >= prev.end_ts) & (surplus_ts <= s.start_ts)
            if bool(np.any(m)):
                # Merge only if surplus was too low to charge (a genuine pause).
                merge = float(np.median(surplus_export_w[m])) < min_charge_w
            # no samples inside the gap → keep the gap-only decision
        if merge:
            buckets[-1].append(s)
        else:
            buckets.append([s])

    groups: List[ChargingGroup] = []
    for grp in buckets:
        start = grp[0].start_ts
        end = grp[-1].end_ts
        energy = round(sum(x.energy_kwh for x in grp), 3)
        cost = round(sum(x.cost_eur for x in grp), 2)
        peak = max((x.peak_power_w for x in grp), default=0.0)
        active_s = sum(max(0, x.end_ts - x.start_ts) for x in grp)
        avg = round(energy / (active_s / 3600.0) * 1000.0, 0) if active_s > 0 else 0.0
        groups.append(ChargingGroup(
            group_id=grp[0].session_id,   # stable id = first session's id
            device_key=grp[0].device_key,
            start_ts=start, end_ts=end,
            energy_kwh=energy, peak_power_w=round(peak, 0),
            avg_power_w=avg, cost_eur=cost,
            session_count=len(grp),
            # The merged charge is the sum of its parts — never a second
            # attribution run over the whole span, which would also swallow the
            # pauses in between (when the car drew nothing but the house did).
            solar_kwh=round(sum(x.solar_kwh for x in grp), 3),
            battery_kwh=round(sum(x.battery_kwh for x in grp), 3),
            grid_kwh=round(sum(x.grid_kwh for x in grp), 3),
            cost_model=("source" if any(x.cost_model == "source" for x in grp)
                        else grp[0].cost_model),
            sessions=list(grp),
        ))
    return groups


# Below this share of the charge window covered by the supply meters the split
# is a guess, not a measurement — the session keeps the flat tariff and says so.
# Half the window is already generous: the meters poll every 15 s … 1 min, so a
# genuinely measured charge comes back at ~1.0 and only real outages fall short.
MIN_SOURCE_COVERAGE = 0.5


def price_by_source(sessions: List[ChargingSession], splits,
                    price_eur_per_kwh: float,
                    solar_price_eur_per_kwh=0.0,
                    min_coverage: float = MIN_SOURCE_COVERAGE) -> int:
    """Re-price sessions from where their energy actually came from. In place.

    ``splits`` is aligned with ``sessions`` (one
    :class:`~shelly_analyzer.services.energy_balance.SourceSplit` each).
    ``solar_price_eur_per_kwh`` may be a number or a callable taking the session,
    so a feed-in tariff that changes over the years prices each charge with the
    tariff that was effective on its own day.

    A session is re-priced only when its window was really measured: the split
    must exist and cover at least ``min_coverage`` of the window. Everything
    else keeps the flat tariff and ``cost_model == "fixed"`` — an unmeasured
    charge must never be *presented* as free solar.

    Returns the number of sessions actually re-priced.
    """
    n = 0
    for se, sp in zip(sessions, splits):
        fr = sp.fractions() if sp is not None else None
        if fr is None or sp.coverage < min_coverage:
            continue
        fs, fb, fg = fr
        # The shares come from the supply meters, the energy from the wallbox
        # meter: applying one to the other keeps the three parts adding up to
        # exactly the kWh shown for the charge, whatever drift is between the
        # two meters.
        se.solar_kwh = round(se.energy_kwh * fs, 3)
        se.battery_kwh = round(se.energy_kwh * fb, 3)
        # The remainder rather than energy·fg, so rounding can never make the
        # three parts miss the total by a watt-hour.
        se.grid_kwh = round(se.energy_kwh - se.solar_kwh - se.battery_kwh, 3)
        sp_price = (float(solar_price_eur_per_kwh(se))
                    if callable(solar_price_eur_per_kwh)
                    else float(solar_price_eur_per_kwh))
        se.cost_eur = round(se.grid_kwh * float(price_eur_per_kwh)
                            + (se.solar_kwh + se.battery_kwh) * sp_price, 2)
        se.cost_model = "source"
        se.source_coverage = round(float(sp.coverage), 3)
        n += 1
    return n
