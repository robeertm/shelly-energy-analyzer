from __future__ import annotations
import logging
import time
from typing import Dict, List, Optional

_log = logging.getLogger(__name__)


def generate_metrics(live_state_store, devices, cfg) -> str:
    """Generate Prometheus text exposition format metrics.

    Returns a string in Prometheus text format.
    """
    lines: List[str] = []
    now = int(time.time())

    lines.append("# HELP shelly_power_watts Current power consumption in watts")
    lines.append("# TYPE shelly_power_watts gauge")
    lines.append("# HELP shelly_voltage_volts Current voltage in volts")
    lines.append("# TYPE shelly_voltage_volts gauge")
    lines.append("# HELP shelly_current_amps Current in amperes")
    lines.append("# TYPE shelly_current_amps gauge")
    lines.append("# HELP shelly_energy_kwh_total Total energy consumption in kWh")
    lines.append("# TYPE shelly_energy_kwh_total counter")
    lines.append("# HELP shelly_frequency_hz Grid frequency in Hz")
    lines.append("# TYPE shelly_frequency_hz gauge")

    if live_state_store is None:
        return "\n".join(lines) + "\n"

    try:
        snapshot = live_state_store.snapshot()
    except Exception:
        return "\n".join(lines) + "\n"

    for dev in (devices or []):
        key = dev.key if hasattr(dev, 'key') else str(dev)
        name = dev.name if hasattr(dev, 'name') else key
        pts = snapshot.get(key)
        if not pts or not isinstance(pts, list) or not pts:
            continue

        last = pts[-1] if isinstance(pts[-1], dict) else {}

        # Total power
        power = last.get("power_total_w")
        if power is not None:
            lines.append(f'shelly_power_watts{{device="{key}",name="{name}"}} {float(power)}')

        # Per-phase metrics
        phases = last.get("phases", [])
        for i, ph in enumerate(phases):
            if not isinstance(ph, dict):
                continue
            phase_label = chr(ord('a') + i)

            p = ph.get("power_w")
            if p is not None:
                lines.append(f'shelly_power_watts{{device="{key}",name="{name}",phase="{phase_label}"}} {float(p)}')

            v = ph.get("voltage_v")
            if v is not None:
                lines.append(f'shelly_voltage_volts{{device="{key}",name="{name}",phase="{phase_label}"}} {float(v)}')

            a = ph.get("current_a")
            if a is not None:
                lines.append(f'shelly_current_amps{{device="{key}",name="{name}",phase="{phase_label}"}} {float(a)}')

        # Frequency
        freq = last.get("freq_hz")
        if freq is not None:
            lines.append(f'shelly_frequency_hz{{device="{key}",name="{name}"}} {float(freq)}')

    # Spot price if available
    if hasattr(cfg, 'spot_price') and cfg.spot_price.enabled:
        lines.append("# HELP shelly_spot_price_ct_per_kwh Current spot price in ct/kWh")
        lines.append("# TYPE shelly_spot_price_ct_per_kwh gauge")

    # CO2 intensity if available
    if hasattr(cfg, 'co2') and cfg.co2.enabled:
        lines.append("# HELP shelly_co2_intensity_g_per_kwh Current grid CO2 intensity")
        lines.append("# TYPE shelly_co2_intensity_g_per_kwh gauge")

    return "\n".join(lines) + "\n"
