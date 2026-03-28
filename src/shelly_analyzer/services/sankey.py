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
) -> SankeyData:
    """Compute energy flow data for a Sankey diagram.

    Args:
        db: EnergyDB instance
        devices: list of DeviceConfig
        solar_config: SolarConfig with pv_meter_device_key
        period: "today" | "week" | "month" | "year"
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

    pv_key = getattr(solar_config, "pv_meter_device_key", "") if solar_config else ""
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

    # Source nodes
    grid_idx = add_node("Grid", "#e74c3c")
    house_idx = add_node("House", "#3498db")

    if has_solar and pv_production > 0.01:
        pv_idx = add_node("PV", "#f39c12")

        # PV → House (self-consumption)
        if self_consumption > 0.01:
            flows.append(EnergyFlow("PV", "House", self_consumption, "#f1c40f"))

        # PV → Grid (feed-in)
        if pv_feed_in_kwh > 0.01:
            feed_idx = add_node("Feed-in", "#27ae60")
            flows.append(EnergyFlow("PV", "Feed-in", pv_feed_in_kwh, "#2ecc71"))

    # Grid → House
    if grid_import > 0.01:
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
