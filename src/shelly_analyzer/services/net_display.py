"""Net "meter behind meter" display subtraction.

Generic, config-driven transform for the common wiring where one sub-meter sits
physically *behind* another (e.g. a wallbox on a circuit fed through the house
meter). The upstream meter then measures its own load *plus* the downstream
meter, so its Live tile and Plots series double-count the downstream device.

A device opts in with ``DeviceConfig.subtract_from_parent_display = True`` and a
``parent`` pointing at the upstream device's ``key``. The upstream device's
*displayed* power/energy is then reduced by this device's values. It is a pure
display transform — stored samples are never modified, and callers expose a
raw/gross bypass (``?raw=1``) that shows everything again.

This lives in one place so the Live builder and the Plots builder subtract
identically. Fully generic: any user can configure any parent/child pair.
"""
from __future__ import annotations

import bisect
from typing import Any, Dict, Iterable, List, Optional, Tuple


def net_display_children(devices: Iterable[Any]) -> Dict[str, List[str]]:
    """Map ``parent_device_key -> [child_key, ...]`` for the net-display cascade.

    A child qualifies when it is flagged ``subtract_from_parent_display`` and its
    ``parent`` is another *device* key present in ``devices`` (a main-meter id or
    a dangling/empty parent yields no display subtraction — that is a calibration
    concept, handled elsewhere). Self-references are ignored.

    Returns ``{}`` when nothing is configured, so callers can cheaply skip.
    """
    dev_list = list(devices or [])
    keys = {str(getattr(d, "key", "") or "") for d in dev_list}
    keys.discard("")
    out: Dict[str, List[str]] = {}
    for d in dev_list:
        if not bool(getattr(d, "subtract_from_parent_display", False)):
            continue
        child = str(getattr(d, "key", "") or "").strip()
        parent = str(getattr(d, "parent", "") or "").strip()
        if not child or not parent or parent == child:
            continue
        if parent not in keys:
            continue
        out.setdefault(parent, []).append(child)
    return out


def apply_live_subtraction(tiles: List[Dict[str, Any]],
                           submap: Dict[str, List[str]]) -> None:
    """Mutate live ``tiles`` in place so each parent shows net of its children.

    ``tiles`` is the list of per-device dicts built for ``/api/state`` (each with
    ``key``, ``power_w``, ``today_kwh``). Children keep their own tiles. The child
    values used are the *raw* pre-subtraction values captured up front, so a chain
    (A behind B behind C) removes each level's full raw load from its parent. The
    parent gains ``net_of_children`` (the applied child keys) for the UI to badge.
    """
    if not submap or not tiles:
        return
    by_key = {str(t.get("key", "")): t for t in tiles if t.get("key")}
    raw_p = {k: float(t.get("power_w") or 0.0) for k, t in by_key.items()}
    raw_k = {k: float(t.get("today_kwh") or 0.0) for k, t in by_key.items()}
    for parent, kids in submap.items():
        pt = by_key.get(parent)
        if not pt:
            continue
        applied = [c for c in kids if c in by_key]
        if not applied:
            continue
        pt["power_w"] = raw_p.get(parent, float(pt.get("power_w") or 0.0)) \
            - sum(raw_p.get(c, 0.0) for c in applied)
        pt["today_kwh"] = raw_k.get(parent, float(pt.get("today_kwh") or 0.0)) \
            - sum(raw_k.get(c, 0.0) for c in applied)
        pt["net_of_children"] = applied


def _nearest_w(pairs: List[Tuple[int, float]], ts: int, tol_ms: int = 10000) -> float:
    """Child power at ``ts`` from a time-sorted ``[(ts_ms, w), ...]`` list, taking
    the nearest sample within ``tol_ms``; 0.0 if none (a gap → leave gross there)."""
    if not pairs:
        return 0.0
    keys = [p[0] for p in pairs]
    i = bisect.bisect_left(keys, ts)
    best: Optional[Tuple[int, float]] = None
    for j in (i - 1, i):
        if 0 <= j < len(pairs):
            d = abs(pairs[j][0] - ts)
            if best is None or d < best[0]:
                best = (d, pairs[j][1])
    if best is None or best[0] > tol_ms:
        return 0.0
    return float(best[1])


def apply_history_subtraction(hist: Dict[str, List[Dict[str, Any]]],
                              submap: Dict[str, List[str]]) -> None:
    """Mutate the ``/api/history`` payload so each parent's power series (``w``) is
    net of its children, aligned by nearest timestamp. Same transform as the live
    tiles and Plots, so the Live view's sparklines/history chart stay consistent
    when the net/raw toggle flips. Only ``w`` is netted (voltages/currents aren't).
    """
    if not submap or not hist:
        return
    for parent, kids in submap.items():
        ppts = hist.get(parent)
        if not ppts:
            continue
        cmaps: List[List[Tuple[int, float]]] = []
        for c in kids:
            cp = hist.get(c)
            if not cp:
                continue
            cmaps.append(sorted(
                ((int(p.get("ts") or 0), float(p.get("w") or 0.0)) for p in cp),
                key=lambda x: x[0]))
        if not cmaps:
            continue
        for p in ppts:
            tsp = int(p.get("ts") or 0)
            sub = 0.0
            for cm in cmaps:
                sub += _nearest_w(cm, tsp)
            p["w"] = float(p.get("w") or 0.0) - sub
