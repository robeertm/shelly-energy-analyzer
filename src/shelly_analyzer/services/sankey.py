"""Sankey/energy flow diagram data computation.

Computes energy flows: Grid → House → Devices and PV → Self-consumption / Feed-in.
Returns structured data for rendering Sankey diagrams in Plotly (desktop + web).
"""
from __future__ import annotations

import datetime
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


@dataclass
class EnergyFlow:
    """A single flow edge in the Sankey diagram."""
    source: str
    target: str
    value_kwh: float
    color: str = ""


@dataclass
class SankeyData:
    """Complete data for rendering a Sankey diagram."""
    nodes: List[str] = field(default_factory=list)
    node_colors: List[str] = field(default_factory=list)
    flows: List[EnergyFlow] = field(default_factory=list)
    # Summary values for display
    grid_import_kwh: float = 0.0
    pv_production_kwh: float = 0.0
    self_consumption_kwh: float = 0.0
    feed_in_kwh: float = 0.0
    total_consumption_kwh: float = 0.0
    # Per-device breakdown
    device_kwh: Dict[str, float] = field(default_factory=dict)
    device_names: Dict[str, str] = field(default_factory=dict)


def compute_sankey(
    db,
    devices: list,
    solar_config,
    period: str = "today",
    net_children: "Optional[Dict[str, list]]" = None,
) -> SankeyData:
    """Compute energy flow data for a Sankey diagram.

    Args:
        db: EnergyDB instance
        devices: list of DeviceConfig
        solar_config: SolarConfig with pv_meter_device_key
        period: "today" | "week" | "month" | "year"
        net_children: optional ``{parent_key: [child_key, ...]}`` map (from
            ``net_display.net_display_children``). When given, each child wired
            *behind* its parent has its energy subtracted from the parent's node,
            so a meter that physically contains a sub-meter (e.g. the house meter
            carrying the wallbox) isn't double-counted in the flow. ``None`` /
            empty = gross (raw) diagram.
    """
    now = datetime.datetime.now(datetime.timezone.utc)

    # Determine time range
    if period == "week":
        start = now - datetime.timedelta(days=now.weekday())
        start = start.replace(hour=0, minute=0, second=0, microsecond=0)
    elif period == "month":
        start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    elif period == "year":
        start = now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    else:  # today
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)

    start_ts = int(start.timestamp())
    end_ts = int(now.timestamp())

    # Signed net/grid meter: a dedicated PV meter if set, else the grid meter.
    pv_key = getattr(solar_config, "pv_meter_device_key", "") if solar_config else ""
    if not pv_key and solar_config is not None:
        pv_key = getattr(solar_config, "grid_meter_device_key", "") or ""
    has_solar = bool(pv_key) and getattr(solar_config, "enabled", False)

    # Collect per-device energy
    device_kwh: Dict[str, float] = {}
    device_names: Dict[str, str] = {}
    pv_feed_in_kwh = 0.0
    pv_grid_import_kwh = 0.0

    for dev in devices:
        hourly = db.query_hourly(dev.key, start_ts=start_ts, end_ts=end_ts)
        if hourly.empty:
            continue

        total = float(hourly["kwh"].sum())

        if dev.key == pv_key and has_solar:
            # PV meter: negative = export, positive = import
            # Individual hour analysis
            for _, row in hourly.iterrows():
                kwh = float(row["kwh"])
                if kwh < 0:
                    pv_feed_in_kwh += abs(kwh)
                else:
                    pv_grid_import_kwh += kwh
        else:
            if total > 0:
                device_kwh[dev.key] = total
                device_names[dev.key] = dev.name

    # Net "meter behind meter": subtract each child's energy from the parent it
    # is wired behind, so a meter that contains a sub-meter (e.g. the house meter
    # carrying the wallbox) is not double-counted here. The child keeps its own
    # node; only the parent shrinks. Pure display transform (raw diagram when
    # net_children is None/empty).
    if net_children:
        for _parent, _kids in net_children.items():
            if _parent in device_kwh:
                _sub = sum(device_kwh.get(_c, 0.0) for _c in _kids if _c in device_kwh)
                if _sub:
                    device_kwh[_parent] = max(0.0, device_kwh[_parent] - _sub)

    # Calculate flows
    total_consumption = sum(device_kwh.values())
    grid_import = pv_grid_import_kwh if has_solar else total_consumption

    # If no PV, all consumption comes from grid
    if has_solar:
        # PV production = self-consumption + feed-in
        # Self-consumption = total_consumption - grid_import
        self_consumption = max(0.0, total_consumption - pv_grid_import_kwh)
        pv_production = self_consumption + pv_feed_in_kwh
    else:
        self_consumption = 0.0
        pv_production = 0.0

    # Prefer a measured PV-production series when available (external PV source
    # ingested as the synthetic "pv" device).
    pv_prod_key = getattr(solar_config, "pv_production_device_key", "") if solar_config else ""
    if not pv_prod_key and getattr(solar_config, "enabled", False):
        pv_prod_key = "pv"
    if pv_prod_key:
        try:
            _pvh = db.query_hourly(pv_prod_key, start_ts=start_ts, end_ts=end_ts)
            if _pvh is not None and not _pvh.empty and "kwh" in _pvh.columns:
                _pvp = float(_pvh["kwh"].clip(lower=0).sum())
                if _pvp > 0:
                    pv_production = _pvp
                    has_solar = True
                    if pv_feed_in_kwh > 0 or pv_grid_import_kwh > 0:
                        # A signed grid meter is present → trust its metered
                        # import/export for the split.
                        self_consumption = max(0.0, pv_production - pv_feed_in_kwh)
                    else:
                        # Measured PV only (no signed grid meter, e.g. the
                        # external HA/EMMA source): derive the split from the
                        # measured load so the diagram balances.
                        self_consumption = max(0.0, min(pv_production, total_consumption))
                        pv_feed_in_kwh = max(0.0, pv_production - self_consumption)
                    # Recompute grid import so Grid→House + PV→House == load
                    # (was left at total_consumption → House over-supplied).
                    grid_import = max(0.0, total_consumption - self_consumption)
        except Exception:
            pass

    # Battery flows: the external "battery" series is signed net energy per hour
    # (+ = charging, − = discharging). PV that charged the battery must NOT be
    # counted as house self-consumption; the battery is its own node so the
    # diagram balances (PV = direct-to-house + to-battery + feed-in).
    battery_charge = 0.0
    battery_discharge = 0.0
    batt_key = getattr(solar_config, "battery_production_device_key", "") if solar_config else ""
    if not batt_key and getattr(solar_config, "enabled", False):
        batt_key = "battery"
    if batt_key:
        try:
            _bh = db.query_hourly(batt_key, start_ts=start_ts, end_ts=end_ts)
            if _bh is not None and not _bh.empty and "kwh" in _bh.columns:
                battery_charge = float(_bh["kwh"].clip(lower=0).sum())
                battery_discharge = float(-_bh["kwh"].clip(upper=0).sum())
        except Exception:
            pass

    has_battery = (battery_charge > 0.01 or battery_discharge > 0.01)

    # Re-derive the split so BOTH the House and the PV node balance, prioritising
    # House-load conservation (its inflows must equal what the devices consumed).
    # External PV/grid/battery/house meters rarely reconcile to the last kWh, so
    # any PV that is neither direct-to-house, stored, nor already metered as
    # export is attributed to feed-in (keeps PV = direct + charge + feed-in and
    # House = direct + discharge + grid_import, never over-supplying the house).
    #   House   = pv_direct + battery_to_house + grid_import
    #   PV      = pv_direct + battery_charge + feed_in
    battery_to_house = min(battery_discharge, total_consumption)
    if has_solar and pv_production > 0.01:
        pv_direct = max(0.0, min(pv_production - pv_feed_in_kwh - battery_charge,
                                 total_consumption - battery_to_house))
        grid_import = max(0.0, total_consumption - pv_direct - battery_to_house)
        _pv_left = max(0.0, pv_production - pv_feed_in_kwh - battery_charge - pv_direct)
        pv_feed_in_kwh += _pv_left  # unaccounted surplus → export
        self_consumption = pv_direct + battery_charge  # PV used on-site (house + stored)
    else:
        pv_direct = 0.0
        grid_import = max(0.0, total_consumption - battery_to_house)
    # The Battery→House flow uses the load-clamped value so it can't over-supply.
    battery_discharge = battery_to_house

    # Build Sankey nodes and flows
    nodes: List[str] = []
    node_colors: List[str] = []
    flows: List[EnergyFlow] = []

    # Node indices
    def add_node(name: str, color: str) -> int:
        if name not in nodes:
            nodes.append(name)
            node_colors.append(color)
        return nodes.index(name)

    # House is always present; Grid is added only if there is real grid import
    # (avoids an orphan zero Grid node on a fully self-supplied day).
    house_idx = add_node("House", "#3498db")

    if has_solar and pv_production > 0.01:
        pv_idx = add_node("PV", "#f39c12")

        # PV → House (direct self-consumption, excluding battery charging)
        if pv_direct > 0.01:
            flows.append(EnergyFlow("PV", "House", pv_direct, "#f1c40f"))

        # PV → Battery (charging)
        if battery_charge > 0.01:
            add_node("Battery", "#8e44ad")
            flows.append(EnergyFlow("PV", "Battery", battery_charge, "#a569bd"))

        # PV → Feed-in (export)
        if pv_feed_in_kwh > 0.01:
            add_node("Feed-in", "#27ae60")
            flows.append(EnergyFlow("PV", "Feed-in", pv_feed_in_kwh, "#2ecc71"))

    # Battery → House (discharging)
    if battery_discharge > 0.01:
        add_node("Battery", "#8e44ad")
        flows.append(EnergyFlow("Battery", "House", battery_discharge, "#a569bd"))

    # Grid → House
    if grid_import > 0.01:
        add_node("Grid", "#e74c3c")
        flows.append(EnergyFlow("Grid", "House", grid_import, "#e74c3c"))

    # House → Devices
    # Color palette for devices
    dev_colors = [
        "#3498db", "#9b59b6", "#1abc9c", "#e67e22", "#2ecc71",
        "#e91e63", "#00bcd4", "#ff9800", "#795548", "#607d8b",
    ]
    sorted_devices = sorted(device_kwh.items(), key=lambda x: x[1], reverse=True)
    for i, (dk, kwh) in enumerate(sorted_devices):
        if kwh > 0.01:
            name = device_names.get(dk, dk)
            color = dev_colors[i % len(dev_colors)]
            add_node(name, color)
            flows.append(EnergyFlow("House", name, kwh, color))

    return SankeyData(
        nodes=nodes,
        node_colors=node_colors,
        flows=flows,
        grid_import_kwh=round(grid_import, 3),
        pv_production_kwh=round(pv_production, 3),
        self_consumption_kwh=round(self_consumption, 3),
        feed_in_kwh=round(pv_feed_in_kwh, 3),
        total_consumption_kwh=round(total_consumption, 3),
        device_kwh={k: round(v, 3) for k, v in device_kwh.items()},
        device_names=device_names,
    )


def sankey_to_plotly_dict(data: SankeyData) -> Dict:
    """Convert SankeyData to a Plotly-compatible figure dict."""
    sources = []
    targets = []
    values = []
    link_colors = []

    for flow in data.flows:
        if flow.source in data.nodes and flow.target in data.nodes:
            sources.append(data.nodes.index(flow.source))
            targets.append(data.nodes.index(flow.target))
            values.append(flow.value_kwh)
            # Semi-transparent link color
            c = flow.color or "#cccccc"
            link_colors.append(c + "80")  # Add alpha

    return {
        "node": {
            "label": data.nodes,
            "color": data.node_colors,
            "pad": 20,
            "thickness": 25,
        },
        "link": {
            "source": sources,
            "target": targets,
            "value": values,
            "color": link_colors,
        },
    }
