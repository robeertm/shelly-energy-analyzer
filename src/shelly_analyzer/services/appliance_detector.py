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
    """Power signature for a household appliance."""

    id: str                      # i18n key: appliance.{id}.name
    category: str                # appliance category
    icon: str                    # display emoji
    power_min: float             # minimum typical power (W)
    power_max: float             # maximum typical power (W)
    pattern_type: str            # "constant" | "cyclic" | "variable" | "short_peak"
    typical_duration_min: float  # typical operating duration (minutes)


#: Built-in appliance signature database
APPLIANCES: List[ApplianceSignature] = [
    ApplianceSignature("fridge",           "cooling",       "❄️",  80,    200,   "cyclic",      20),
    ApplianceSignature("freezer",          "cooling",       "🧊",  100,   300,   "cyclic",      25),
    ApplianceSignature("washing_machine",  "laundry",       "🫧",  300,   2200,  "variable",    90),
    ApplianceSignature("dryer",            "laundry",       "🌀",  1800,  5000,  "constant",    60),
    ApplianceSignature("dishwasher",       "kitchen",       "🍽️", 1200,  2400,  "variable",    60),
    ApplianceSignature("oven",             "kitchen",       "🔥",  2000,  3500,  "cyclic",      60),
    ApplianceSignature("hob",              "kitchen",       "🍳",  1000,  3500,  "constant",    30),
    ApplianceSignature("microwave",        "kitchen",       "📡",  600,   1500,  "short_peak",   5),
    ApplianceSignature("kettle",           "kitchen",       "☕",  1500,  3000,  "short_peak",   3),
    ApplianceSignature("coffee_machine",   "kitchen",       "☕",  800,   1500,  "short_peak",   5),
    ApplianceSignature("toaster",          "kitchen",       "🍞",  800,   1500,  "short_peak",   4),
    ApplianceSignature("iron",             "laundry",       "👔",  1000,  2500,  "cyclic",      30),
    ApplianceSignature("hair_dryer",       "personal_care", "💨",  1000,  2200,  "constant",    10),
    ApplianceSignature("vacuum",           "cleaning",      "🌪️", 500,   2000,  "variable",    20),
    ApplianceSignature("ev_charger",       "transport",     "⚡",  2300,  11000, "constant",   300),
    ApplianceSignature("heat_pump",        "heating",       "🌡️", 1000,  5000,  "cyclic",      30),
    ApplianceSignature("boiler_instant",   "heating",       "🚿",  18000, 27000, "constant",     5),
    ApplianceSignature("boiler_storage",   "heating",       "🛁",  2000,  4000,  "cyclic",      60),
    ApplianceSignature("tv",               "entertainment", "📺",  50,    400,   "constant",   120),
    ApplianceSignature("pc",               "entertainment", "🖥️", 200,   800,   "variable",   120),
    ApplianceSignature("laptop",           "entertainment", "💻",  30,    90,    "constant",   120),
    ApplianceSignature("router",           "network",       "📡",  5,     20,    "constant",  1440),
    ApplianceSignature("led_light",        "lighting",      "💡",  5,     100,   "constant",   240),
    ApplianceSignature("air_conditioner",  "heating",       "🌬️", 1000,  4000,  "cyclic",      60),
    ApplianceSignature("fan",              "heating",       "🌀",  30,    100,   "constant",    60),
]


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

    def observe(self, device_key: str, timestamp: float, power_w: float) -> Optional[PowerTransition]:
        """Feed a new power reading. Returns a PowerTransition if a step was detected."""
        with self._lock:
            if device_key not in self._history:
                self._history[device_key] = deque(maxlen=10)

            hist = self._history[device_key]
            if len(hist) < 3:
                hist.append((timestamp, power_w))
                return None

            # Use median of last 3 readings as baseline
            recent = [w for _, w in hist]
            baseline = float(np.median(recent))

            delta = power_w - baseline
            if abs(delta) >= self.min_step_w:
                transition = PowerTransition(
                    timestamp=timestamp,
                    device_key=device_key,
                    delta_w=delta,
                    power_before=baseline,
                    power_after=power_w,
                )
                self._transitions.append(transition)
                if len(self._transitions) > self._max_transitions:
                    self._transitions = self._transitions[-self._max_transitions:]
                # Auto-save every 10 new transitions to avoid data loss
                if self.persist_path and len(self._transitions) % 10 == 0:
                    self._save(self.persist_path)
                hist.clear()
                hist.append((timestamp, power_w))
                return transition

            hist.append((timestamp, power_w))
            return None

    def cluster(self) -> List[LearnedCluster]:
        """Run k-means clustering on accumulated transitions.

        Returns list of discovered clusters.
        """
        with self._lock:
            if len(self._transitions) < 10:
                return self._clusters

            # Extract positive transitions (device turn-on events)
            on_deltas = np.array([
                t.delta_w for t in self._transitions if t.delta_w > 0
            ])
            on_times = [
                t for t in self._transitions if t.delta_w > 0
            ]

            if len(on_deltas) < 5:
                return self._clusters

            # Simple 1D k-means clustering
            k = min(self.max_clusters, max(2, len(on_deltas) // 5))
            clusters = self._kmeans_1d(on_deltas, k)

            self._clusters = []
            for cid, (centroid, members) in enumerate(clusters):
                if len(members) < 2:
                    continue

                vals = on_deltas[members]
                # Find typical hour
                hours = []
                for idx in members:
                    if idx < len(on_times):
                        import datetime
                        dt = datetime.datetime.fromtimestamp(on_times[idx].timestamp)
                        hours.append(dt.hour)
                typical_hour = max(set(hours), key=hours.count) if hours else 12

                cluster = LearnedCluster(
                    cluster_id=cid,
                    centroid_w=round(float(centroid), 1),
                    std_w=round(float(np.std(vals)), 1),
                    count=len(members),
                    avg_duration_min=0,  # Would need on/off pairing
                    typical_hour=typical_hour,
                )

                # Try to match against built-in appliances
                matches = identify_appliance(abs(cluster.centroid_w))
                if matches:
                    best = matches[0]
                    cluster.matched_appliance = best[0].id
                    cluster.icon = best[0].icon
                    cluster.label = best[0].id

                self._clusters.append(cluster)

            # Sort by frequency
            self._clusters.sort(key=lambda c: c.count, reverse=True)

            # Persist
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
