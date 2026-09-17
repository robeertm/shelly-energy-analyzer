"""NILM appliance detector with ML-enhanced power transition clustering.

Matches a live power reading (in Watts) against a built-in database of
household appliance power signatures and returns a ranked list of candidate
devices with a confidence score.

Additionally provides a learning engine that clusters power transitions
(step changes) to discover recurring appliance patterns automatically.
"""
from __future__ import annotations

import json
import logging
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ApplianceSignature:
    """Power signature for a household appliance.

    Power alone cannot tell a fridge from a television — a 90 W step is
    either. What separates them is how long the step lasts, how often it
    comes per day, and when: a fridge runs 15–30 minutes, 20–40 times a day,
    day and night alike; a television runs hours, once or twice, in the
    evening. The fields below carry those expectations; the classifier scores
    every candidate on all of them and never on the wattage alone.
    """

    id: str                      # i18n key: appliance.{id}.name
    category: str                # appliance category
    icon: str                    # display emoji
    power_min: float             # minimum typical power (W)
    power_max: float             # maximum typical power (W)
    pattern_type: str            # "constant" | "cyclic" | "variable" | "short_peak"
    typical_duration_min: float  # typical operating duration (minutes)
    duration_min: float = 1.0    # plausible run length, minutes (min)
    duration_max: float = 1440.0  # plausible run length, minutes (max)
    runs_min: float = 0.0        # plausible starts per day (min)
    runs_max: float = 100.0      # plausible starts per day (max)
    hours: str = "any"           # "any" | "day" | "evening" | "morning" | "meals" | "night" | "allday"
    prior: float = 1.0           # how common the appliance is (0..1)


#: Built-in appliance signature database
APPLIANCES: List[ApplianceSignature] = [
    ApplianceSignature("fridge",           "cooling",       "❄️",  60,    200,   "cyclic",      20,   8,   45,  10,  60, "allday", 1.0),
    ApplianceSignature("freezer",          "cooling",       "🧊",  80,    300,   "cyclic",      25,  10,   60,   8,  50, "allday", 0.8),
    ApplianceSignature("washing_machine",  "laundry",       "🫧",  1500,  2500,  "variable",    90,   5,   40,   0,   3, "day", 0.9),
    ApplianceSignature("dryer",            "laundry",       "🌀",  1500,  3000,  "constant",    60,  20,  180,   0,   2, "day", 0.7),
    ApplianceSignature("dishwasher",       "kitchen",       "🍽️", 1200,  2400,  "variable",    60,  10,   60,   0,   2, "evening", 0.9),
    ApplianceSignature("oven",             "kitchen",       "🔥",  1500,  3500,  "cyclic",      60,   3,   30,   0,   6, "meals", 0.8),
    ApplianceSignature("hob",              "kitchen",       "🍳",  1000,  3500,  "constant",    30,   2,   40,   0,   8, "meals", 0.8),
    ApplianceSignature("microwave",        "kitchen",       "📡",  600,   1500,  "short_peak",   5,   0.5,  12,   0,  10, "meals", 0.8),
    ApplianceSignature("kettle",           "kitchen",       "☕",  1500,  3000,  "short_peak",   3,   0.5,   6,   0,  12, "day", 0.9),
    ApplianceSignature("coffee_machine",   "kitchen",       "☕",  800,   1500,  "short_peak",   5,   0.5,  10,   0,  12, "morning", 0.8),
    ApplianceSignature("toaster",          "kitchen",       "🍞",  700,   1500,  "short_peak",   4,   1,    6,   0,   4, "morning", 0.5),
    ApplianceSignature("iron",             "laundry",       "👔",  1000,  2500,  "cyclic",      30,   2,   20,   0,   6, "day", 0.4),
    ApplianceSignature("hair_dryer",       "personal_care", "💨",  1000,  2200,  "constant",    10,   1,   12,   0,   4, "morning", 0.6),
    ApplianceSignature("vacuum",           "cleaning",      "🌪️", 500,   2000,  "variable",    20,   3,   45,   0,   2, "day", 0.6),
    ApplianceSignature("ev_charger",       "transport",     "⚡",  2000,  11000, "constant",   300,  20,  900,   0,   3, "any", 0.7),
    ApplianceSignature("heat_pump",        "heating",       "🌡️", 800,   5000,  "cyclic",      30,  10,  180,   2,  40, "allday", 0.6),
    ApplianceSignature("boiler_instant",   "heating",       "🚿",  18000, 27000, "constant",     5,   1,   20,   0,  10, "morning", 0.3),
    ApplianceSignature("boiler_storage",   "heating",       "🛁",  1500,  4000,  "cyclic",      60,  15,  180,   0,   4, "night", 0.5),
    ApplianceSignature("circulation_pump", "heating",       "♨️",  20,    90,    "cyclic",      60,  10, 1440,   0,  40, "allday", 0.6),
    ApplianceSignature("dehumidifier",     "heating",       "💧",  150,   450,   "cyclic",      60,  10,  240,   0,  20, "any", 0.3),
    ApplianceSignature("tv",               "entertainment", "📺",  50,    250,   "constant",   120,  30,  420,   0,   4, "evening", 0.9),
    ApplianceSignature("pc",               "entertainment", "🖥️", 100,   600,   "variable",   120,  30,  720,   0,   4, "day", 0.8),
    ApplianceSignature("laptop",           "entertainment", "💻",  30,    90,    "constant",   120,  20,  600,   0,   6, "day", 0.5),
    ApplianceSignature("router",           "network",       "📡",  5,     20,    "constant",  1440, 600, 1440,   0,   1, "any", 0.3),
    ApplianceSignature("led_light",        "lighting",      "💡",  5,     120,   "constant",   240,  10,  600,   0,  30, "evening", 0.9),
    ApplianceSignature("air_conditioner",  "heating",       "🌬️", 800,   3000,  "cyclic",      60,  10,  180,   0,  20, "day", 0.2),
    ApplianceSignature("fan",              "heating",       "🌀",  30,    100,   "constant",    60,  20,  480,   0,   6, "day", 0.15),
    ApplianceSignature("well_pump",        "garden",        "🚰",  500,   1500,  "constant",    10,   1,   30,   0,  20, "day", 0.3),
]

_CATEGORY_FOR_SIZE = [
    # (max delta W, generic id) — the honest label when no candidate is convincing
    (150.0, "small_load"),
    (600.0, "medium_load"),
    (2500.0, "large_load"),
    (1e9, "very_large_load"),
]

_HOUR_PRIORS = {
    # weight per hour of day, 0..23 — shape only, normalised in scoring
    "any":     [1.0] * 24,
    "allday":  [1.0] * 24,
    "day":     [0.2, 0.15, 0.1, 0.1, 0.1, 0.2, 0.5, 0.9, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 0.9, 0.8, 0.6, 0.4, 0.3],
    "evening": [0.3, 0.15, 0.1, 0.1, 0.1, 0.15, 0.3, 0.4, 0.4, 0.4, 0.4, 0.4, 0.5, 0.5, 0.5, 0.6, 0.7, 0.9, 1.0, 1.0, 1.0, 1.0, 0.9, 0.6],
    "morning": [0.1, 0.05, 0.05, 0.05, 0.1, 0.4, 0.9, 1.0, 1.0, 0.9, 0.6, 0.4, 0.4, 0.3, 0.3, 0.3, 0.3, 0.4, 0.4, 0.4, 0.3, 0.2, 0.2, 0.1],
    "meals":   [0.1, 0.05, 0.05, 0.05, 0.1, 0.3, 0.7, 0.9, 0.8, 0.5, 0.5, 0.8, 1.0, 0.9, 0.5, 0.4, 0.5, 0.8, 1.0, 1.0, 0.8, 0.5, 0.3, 0.2],
    "night":   [1.0, 1.0, 1.0, 1.0, 1.0, 0.9, 0.6, 0.3, 0.2, 0.2, 0.2, 0.2, 0.2, 0.2, 0.2, 0.2, 0.2, 0.2, 0.3, 0.4, 0.6, 0.8, 1.0, 1.0],
}


def _range_score(v: float, lo: float, hi: float, tol: float = 0.25) -> float:
    """1 inside [lo, hi], fading to 0 at ``tol`` (fraction of the bound) outside."""
    if v is None:
        return 0.6
    if lo <= v <= hi:
        return 1.0
    if v < lo:
        d = (lo - v) / max(lo * tol, 1e-9)
    else:
        d = (v - hi) / max(hi * tol, 1e-9)
    return max(0.0, 1.0 - d)


def classify_cluster(delta_w: float, duration_min: Optional[float], runs_per_day: Optional[float],
                     hour_hist: Optional[List[int]] = None, device_hint: str = "") -> List[Tuple[ApplianceSignature, float]]:
    """Score every signature against what was observed and rank them.

    ``hour_hist`` is a 24-bin histogram of the cluster's start hours.
    ``device_hint`` may name the circuit ("wallbox", "tenant", …) — a step on a
    wallbox circuit is a car, not a dryer. Returns ``(signature, score)``
    sorted by score, the score being 0..1 with the prior folded in.
    """
    p = abs(float(delta_w or 0.0))
    if p <= 0:
        return []
    hist = list(hour_hist or [])
    tot = float(sum(hist)) if hist else 0.0
    out: List[Tuple[ApplianceSignature, float]] = []
    hint = (device_hint or "").lower()
    for sig in APPLIANCES:
        ps = _range_score(p, sig.power_min, sig.power_max, 0.2)
        if ps <= 0:
            continue
        ds = _range_score(duration_min, sig.duration_min, sig.duration_max, 0.6) if duration_min is not None else 0.6
        fs = _range_score(runs_per_day, sig.runs_min, sig.runs_max, 0.8) if runs_per_day is not None else 0.7
        if tot > 0:
            pr = _HOUR_PRIORS.get(sig.hours, _HOUR_PRIORS["any"])
            mean_pr = sum(pr) / 24.0
            hs = sum(hist[h] * pr[h] for h in range(24)) / (tot * max(mean_pr, 1e-9))
            hs = max(0.2, min(1.5, hs)) / 1.5
        else:
            hs = 0.6
        score = sig.prior * ps * ds * fs * hs
        if hint:
            is_wb = ("wallbox" in hint or "ladesäule" in hint or "charger" in hint or " ev " in (" " + hint + " "))
            if sig.id == "ev_charger" and is_wb:
                # Single-phase surplus charging starts at ~1.4 kW (6 A).
                score = min(1.0, max(score, _range_score(p, 1200, 11000, 0.2) * 0.9))
            if sig.id != "ev_charger" and is_wb:
                # Whatever else steps on a wallbox circuit is the car's charge
                # electronics or a ramp — never a dishwasher.
                score *= 0.25 if p >= 800 else 0.6
        out.append((sig, round(min(1.0, score), 3)))
    out.sort(key=lambda x: x[1], reverse=True)
    return out[:6]


def generic_label_for(delta_w: float) -> str:
    p = abs(float(delta_w or 0.0))
    for lim, gid in _CATEGORY_FOR_SIZE:
        if p <= lim:
            return gid
    return "very_large_load"


def identify_appliance(power_watts: float) -> List[Tuple[ApplianceSignature, float]]:
    """Match a live power reading against the appliance database.

    Returns a list of ``(ApplianceSignature, confidence)`` tuples sorted by
    confidence descending.  Only appliances whose power range contains the
    measured value (with ±5 % boundary tolerance) are included.

    Confidence is 1.0 when the reading is at the centre of the appliance's
    power range and falls to 0.0 toward the boundaries.  Readings that only
    match within the tolerance zone receive a fixed low confidence of 0.25.
    """
    if power_watts <= 0:
        return []

    results: List[Tuple[ApplianceSignature, float]] = []
    tolerance = 0.05  # ±5 % beyond declared range boundaries

    for sig in APPLIANCES:
        lo = sig.power_min * (1.0 - tolerance)
        hi = sig.power_max * (1.0 + tolerance)
        if not (lo <= power_watts <= hi):
            continue

        center = (sig.power_min + sig.power_max) / 2.0
        half_range = (sig.power_max - sig.power_min) / 2.0

        if half_range == 0.0:
            confidence = 1.0
        elif sig.power_min <= power_watts <= sig.power_max:
            # Inside the declared range: linear falloff from centre to boundary
            dist = abs(power_watts - center)
            confidence = max(0.0, 1.0 - dist / (half_range * 1.1))
        else:
            # In tolerance zone only: lower fixed confidence
            confidence = 0.25

        results.append((sig, round(confidence, 3)))

    results.sort(key=lambda x: x[1], reverse=True)
    return results[:10]


# ---------------------------------------------------------------------------
# ML-enhanced transition clustering
# ---------------------------------------------------------------------------

@dataclass
class PowerTransition:
    """A detected step change in power consumption."""
    timestamp: float
    device_key: str
    delta_w: float       # positive = turn on, negative = turn off
    power_before: float
    power_after: float


@dataclass
class LearnedCluster:
    """A cluster of similar power transitions (learned appliance pattern)."""
    cluster_id: int
    centroid_w: float         # Average delta watts
    std_w: float              # Standard deviation
    count: int                # Number of observations
    avg_duration_min: float   # Average duration between on/off pairs
    typical_hour: int         # Most common hour of day
    label: str = ""           # User-assigned or auto-matched label
    icon: str = "🔌"
    matched_appliance: str = ""  # Matched built-in appliance ID
    confidence: float = 0.0   # score of the best candidate, 0..1
    candidates: List[Dict[str, Any]] = field(default_factory=list)  # [{id, icon, score}]
    median_duration_min: Optional[float] = None
    paired: int = 0           # how many starts found their stop
    runs_per_day: float = 0.0
    night_share: float = 0.0  # share of starts between 23:00 and 06:00
    hour_hist: List[int] = field(default_factory=list)
    first_ts: float = 0.0
    last_ts: float = 0.0


class TransitionLearner:
    """Learns recurring power transitions using k-means clustering.

    Observes live power readings, detects step changes (transitions),
    and clusters them to discover recurring appliance patterns.
    """

    def __init__(
        self,
        min_step_w: float = 50.0,
        max_clusters: int = 20,
        persist_path: Optional[Path] = None,
    ) -> None:
        self.min_step_w = min_step_w
        self.max_clusters = max_clusters
        self.persist_path = persist_path
        self._lock = threading.Lock()

        # Recent power readings per device (for transition detection)
        self._history: Dict[str, deque] = {}  # device_key → deque of (ts, watts)
        self._transitions: List[PowerTransition] = []
        self._clusters: List[LearnedCluster] = []
        self._max_transitions = 5000

        # Load persisted clusters
        if persist_path and persist_path.exists():
            self._load(persist_path)

    #: A step must hold for this many readings before it counts. At a 1 s
    #: poll the old detector fired on every flicker — a modulating inverter
    #: or a heat-pump ramp produced thousands of "appliances" a day.
    _CONFIRM_READINGS = 3

    def observe(self, device_key: str, timestamp: float, power_w: float) -> Optional[PowerTransition]:
        """Feed a new power reading. Returns a PowerTransition once a step is
        confirmed: the new level has to hold for ``_CONFIRM_READINGS`` readings,
        and a step of the opposite sign within that window cancels it (that
        was a spike, not a switch-on)."""
        with self._lock:
            if device_key not in self._history:
                self._history[device_key] = deque(maxlen=10)
            hist = self._history[device_key]
            pend = getattr(self, "_pending", None)
            if pend is None:
                self._pending = {}
                pend = self._pending
            cand = pend.get(device_key)
            if cand is not None:
                # Waiting for confirmation of a candidate step.
                cand["seen"].append(power_w)
                level = float(np.median(cand["seen"]))
                delta_now = level - cand["baseline"]
                same_dir = (delta_now > 0) == (cand["delta"] > 0)
                if not same_dir or abs(delta_now) < self.min_step_w * 0.6:
                    pend.pop(device_key, None)            # spike — forget it
                    hist.append((timestamp, power_w))
                    return None
                if len(cand["seen"]) >= self._CONFIRM_READINGS:
                    pend.pop(device_key, None)
                    transition = PowerTransition(
                        timestamp=cand["ts"], device_key=device_key,
                        delta_w=delta_now, power_before=cand["baseline"], power_after=level)
                    self._transitions.append(transition)
                    if len(self._transitions) > self._max_transitions:
                        self._transitions = self._transitions[-self._max_transitions:]
                    if self.persist_path and len(self._transitions) % 10 == 0:
                        self._save(self.persist_path)
                    hist.clear()
                    hist.append((timestamp, level))
                    return transition
                return None

            if len(hist) < 3:
                hist.append((timestamp, power_w))
                return None
            recent = [w for _, w in hist]
            baseline = float(np.median(recent))
            delta = power_w - baseline
            if abs(delta) >= self.min_step_w:
                pend[device_key] = {"ts": timestamp, "baseline": baseline, "delta": delta, "seen": [power_w]}
                return None
            hist.append((timestamp, power_w))
            return None

    def _pair_runs(self) -> Dict[int, float]:
        """Match every start (+Δ) with the next stop (−Δ') of about the same
        size on the same circuit; returns {start_index: duration_min}.

        A stop is accepted when it is within 25 % (or 30 W) of the start and
        no later than 24 h after it; each stop is used once. Unpaired starts
        keep ``None`` — the classifier treats an unknown duration as neutral,
        never as zero.
        """
        out: Dict[int, float] = {}
        by_dev: Dict[str, List[int]] = {}
        for i, tr in enumerate(self._transitions):
            by_dev.setdefault(tr.device_key, []).append(i)
        for dev, idxs in by_dev.items():
            idxs.sort(key=lambda i: self._transitions[i].timestamp)
            used: set = set()
            for a_pos, i in enumerate(idxs):
                tr = self._transitions[i]
                if tr.delta_w <= 0:
                    continue
                tol = max(30.0, 0.25 * tr.delta_w)
                for j in idxs[a_pos + 1:]:
                    if j in used:
                        continue
                    tj = self._transitions[j]
                    if tj.timestamp - tr.timestamp > 86400:
                        break
                    if tj.delta_w < 0 and abs(-tj.delta_w - tr.delta_w) <= tol:
                        used.add(j)
                        out[i] = max(0.0, (tj.timestamp - tr.timestamp) / 60.0)
                        break
        return out

    def cluster(self, device_hint: str = "") -> List[LearnedCluster]:
        """Group the confirmed starts by size, measure each group (how long,
        how often, when) and name it — or say honestly that it is a "small
        load" when no candidate is convincing."""
        with self._lock:
            if len(self._transitions) < 10:
                return self._clusters
            import datetime as _dt
            on_idx = [i for i, t in enumerate(self._transitions) if t.delta_w > 0]
            if len(on_idx) < 5:
                return self._clusters
            durations = self._pair_runs()
            on_deltas = np.array([self._transitions[i].delta_w for i in on_idx])
            # Sizes are clustered on a log scale: 60 W and 90 W are different
            # appliances, 2 400 W and 2 430 W are the same kettle.
            k = min(self.max_clusters, max(2, len(on_deltas) // 8))
            raw = self._kmeans_1d(np.log(on_deltas), k)
            # Merge groups whose centres sit within 12 % of each other.
            groups: List[List[int]] = []
            centres: List[float] = []
            for c, members in sorted(raw, key=lambda x: x[0]):
                if not members:
                    continue
                if centres and abs(c - centres[-1]) < np.log(1.12):
                    groups[-1].extend(members)
                    centres[-1] = float(np.mean(np.log(on_deltas[groups[-1]])))
                else:
                    groups.append(list(members))
                    centres.append(float(c))
            span_days = max(1.0, (self._transitions[-1].timestamp - self._transitions[0].timestamp) / 86400.0)
            self._clusters = []
            for cid, members in enumerate(groups):
                if len(members) < 3:
                    continue
                vals = on_deltas[members]
                idxs = [on_idx[m] for m in members]
                hours = [0] * 24
                durs = []
                ts_list = []
                for i in idxs:
                    tr = self._transitions[i]
                    hours[_dt.datetime.fromtimestamp(tr.timestamp).hour] += 1
                    ts_list.append(tr.timestamp)
                    if i in durations:
                        durs.append(durations[i])
                med_dur = float(np.median(durs)) if durs else None
                # Runs that stop within a minute are flicker, not use — only a
                # cluster whose runs mostly last count as an appliance.
                if durs and len(durs) >= max(3, len(idxs) // 2) and med_dur is not None and med_dur < 0.75:
                    continue
                runs_per_day = len(idxs) / span_days
                night = sum(hours[h] for h in (23, 0, 1, 2, 3, 4, 5)) / max(1, len(idxs))
                typical_hour = int(np.argmax(hours)) if any(hours) else 12
                cluster = LearnedCluster(
                    cluster_id=cid,
                    centroid_w=round(float(np.mean(vals)), 1),
                    std_w=round(float(np.std(vals)), 1),
                    count=len(members),
                    avg_duration_min=round(float(np.mean(durs)), 1) if durs else 0.0,
                    typical_hour=typical_hour,
                    median_duration_min=round(med_dur, 1) if med_dur is not None else None,
                    paired=len(durs),
                    runs_per_day=round(runs_per_day, 2),
                    night_share=round(night, 3),
                    hour_hist=hours,
                    first_ts=min(ts_list) if ts_list else 0.0,
                    last_ts=max(ts_list) if ts_list else 0.0,
                )
                cands = classify_cluster(cluster.centroid_w, med_dur, runs_per_day, hours, device_hint)
                cluster.candidates = [{"id": c[0].id, "icon": c[0].icon, "score": c[1]} for c in cands[:4]]
                if cands and cands[0][1] >= 0.45 and (len(cands) < 2 or cands[0][1] >= cands[1][1] * 1.15):
                    cluster.matched_appliance = cands[0][0].id
                    cluster.icon = cands[0][0].icon
                    cluster.label = cands[0][0].id
                    cluster.confidence = cands[0][1]
                else:
                    gid = generic_label_for(cluster.centroid_w)
                    cluster.matched_appliance = ""
                    cluster.label = gid
                    cluster.icon = "🔌"
                    cluster.confidence = cands[0][1] if cands else 0.0
                self._clusters.append(cluster)
            self._clusters.sort(key=lambda c: c.count, reverse=True)
            if self.persist_path:
                self._save(self.persist_path)
            return self._clusters

    @staticmethod
    def _kmeans_1d(data: np.ndarray, k: int, max_iter: int = 50) -> List[Tuple[float, List[int]]]:
        """Simple 1D k-means clustering."""
        # Initialize centroids using quantiles
        centroids = np.percentile(data, np.linspace(0, 100, k + 2)[1:-1])

        for _ in range(max_iter):
            # Assign points to nearest centroid
            dists = np.abs(data[:, np.newaxis] - centroids[np.newaxis, :])
            labels = np.argmin(dists, axis=1)

            # Update centroids
            new_centroids = np.array([
                data[labels == i].mean() if (labels == i).any() else centroids[i]
                for i in range(k)
            ])

            if np.allclose(centroids, new_centroids, atol=1.0):
                break
            centroids = new_centroids

        # Collect results
        result = []
        for i in range(k):
            members = list(np.where(labels == i)[0])
            result.append((float(centroids[i]), members))
        return result

    def get_clusters(self) -> List[LearnedCluster]:
        """Return current clusters without re-computing."""
        with self._lock:
            return list(self._clusters)

    def get_transition_count(self) -> int:
        with self._lock:
            return len(self._transitions)

    def flush(self) -> None:
        """Persist current state (clusters + transitions) to disk immediately."""
        if self.persist_path:
            with self._lock:
                self._save(self.persist_path)

    def _save(self, path: Path) -> None:
        try:
            data = {
                "version": 2,
                "clusters": [
                    {
                        "cluster_id": c.cluster_id,
                        "centroid_w": c.centroid_w,
                        "std_w": c.std_w,
                        "count": c.count,
                        "avg_duration_min": c.avg_duration_min,
                        "typical_hour": c.typical_hour,
                        "label": c.label,
                        "icon": c.icon,
                        "matched_appliance": c.matched_appliance,
                        "confidence": c.confidence,
                        "candidates": c.candidates,
                        "median_duration_min": c.median_duration_min,
                        "paired": c.paired,
                        "runs_per_day": c.runs_per_day,
                        "night_share": c.night_share,
                        "hour_hist": c.hour_hist,
                        "first_ts": c.first_ts,
                        "last_ts": c.last_ts,
                    }
                    for c in self._clusters
                ],
                "transitions": [
                    {
                        "timestamp": t.timestamp,
                        "device_key": t.device_key,
                        "delta_w": t.delta_w,
                        "power_before": t.power_before,
                        "power_after": t.power_after,
                    }
                    for t in self._transitions
                ],
            }
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        except Exception:
            logger.debug("Failed to save NILM clusters", exc_info=True)

    def _load(self, path: Path) -> None:
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if int(data.get("version", 1) or 1) < 2:
                # Steps recorded before v17 were unconfirmed flicker (a
                # modulating inverter fired thousands a day); learning them
                # again would only bring the phantom appliances back.
                logger.info("NILM: discarding pre-v17 transitions in %s, learning afresh", path.name)
                return
            self._clusters = []
            for c in data.get("clusters", []):
                self._clusters.append(LearnedCluster(
                    cluster_id=int(c.get("cluster_id", 0)),
                    centroid_w=float(c.get("centroid_w", 0)),
                    std_w=float(c.get("std_w", 0)),
                    count=int(c.get("count", 0)),
                    avg_duration_min=float(c.get("avg_duration_min", 0)),
                    typical_hour=int(c.get("typical_hour", 12)),
                    label=str(c.get("label", "")),
                    icon=str(c.get("icon", "🔌")),
                    matched_appliance=str(c.get("matched_appliance", "")),
                    confidence=float(c.get("confidence", 0.0) or 0.0),
                    candidates=list(c.get("candidates", []) or []),
                    median_duration_min=(float(c["median_duration_min"]) if c.get("median_duration_min") is not None else None),
                    paired=int(c.get("paired", 0) or 0),
                    runs_per_day=float(c.get("runs_per_day", 0.0) or 0.0),
                    night_share=float(c.get("night_share", 0.0) or 0.0),
                    hour_hist=list(c.get("hour_hist", []) or []),
                    first_ts=float(c.get("first_ts", 0.0) or 0.0),
                    last_ts=float(c.get("last_ts", 0.0) or 0.0),
                ))
            self._transitions = []
            for t in data.get("transitions", []):
                self._transitions.append(PowerTransition(
                    timestamp=float(t.get("timestamp", 0)),
                    device_key=str(t.get("device_key", "")),
                    delta_w=float(t.get("delta_w", 0)),
                    power_before=float(t.get("power_before", 0)),
                    power_after=float(t.get("power_after", 0)),
                ))
        except Exception:
            logger.debug("Failed to load NILM clusters", exc_info=True)
