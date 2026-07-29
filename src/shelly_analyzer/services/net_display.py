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

from typing import Any, Dict, Iterable, List


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
