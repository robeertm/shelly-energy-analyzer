from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple

import pandas as pd

from shelly_analyzer.core.energy import calculate_energy
from shelly_analyzer.io.config import DeviceConfig
from shelly_analyzer.io.storage import Storage


@dataclass(frozen=True)
class ComputedDevice:
    device_key: str
    device_name: str
    df: pd.DataFrame  # includes timestamp, energy_kwh, total_power


def load_device(storage: Storage, device: DeviceConfig) -> ComputedDevice:
    # Devices without EMData CSV support (e.g. switch/plug devices) should still
    # be usable in Live mode. For these, CSV-based stats/plots are simply empty.
    try:
        df_raw = storage.read_device_df(device.key)
    except Exception:
        if not bool(getattr(device, "supports_emdata", True)):
            # Live-only device (no CSV import). Return an empty frame with the
            # expected columns so plots/summaries can still render as 0.
            empty = pd.DataFrame(
                {
                    "timestamp": pd.to_datetime([], unit="s"),
                    "total_power": pd.Series([], dtype="float64"),
                    "energy_kwh": pd.Series([], dtype="float64"),
                }
            )
            return ComputedDevice(device_key=device.key, device_name=device.name, df=empty)
        raise

    df = calculate_energy(df_raw)
    # Ensure numeric
    df["energy_kwh"] = pd.to_numeric(df["energy_kwh"], errors="coerce").fillna(0.0)
    # df.get(...) returns a scalar when the column is missing; make sure we always
    # operate on a Series.
    if "total_power" in df.columns:
        df["total_power"] = pd.to_numeric(df["total_power"], errors="coerce").fillna(0.0)
    else:
        df["total_power"] = pd.Series(0.0, index=df.index, dtype="float64")
    return ComputedDevice(device_key=device.key, device_name=device.name, df=df)


def combine_devices(devices: List[ComputedDevice], freq: str = "1min") -> pd.DataFrame:
    """Combine devices by resampling to a common frequency.

    - total_power: mean over the bin
    - energy_kwh: sum over the bin
    """
    if not devices:
        raise ValueError("No devices to combine")

    frames: List[pd.DataFrame] = []
    for d in devices:
        x = d.df[["timestamp", "total_power", "energy_kwh"]].copy()
        x = x.set_index("timestamp").sort_index()
        # resample
        p = x["total_power"].resample(freq).mean()
        e = x["energy_kwh"].resample(freq).sum()
        frames.append(pd.DataFrame({f"{d.device_key}_power": p, f"{d.device_key}_kwh": e}))

    merged = pd.concat(frames, axis=1).fillna(0.0)
    merged["total_power"] = merged[[c for c in merged.columns if c.endswith("_power")]].sum(axis=1)
    merged["energy_kwh"] = merged[[c for c in merged.columns if c.endswith("_kwh")]].sum(axis=1)
    out = merged.reset_index().rename(columns={"index": "timestamp"})
    return out


def summarize(df: pd.DataFrame) -> Tuple[float, float, float]:
    """Return (kwh_total, avg_power_w, max_power_w)."""
    kwh_total = float(pd.to_numeric(df["energy_kwh"], errors="coerce").fillna(0.0).sum())
    p = pd.to_numeric(df["total_power"], errors="coerce").fillna(0.0)
    avg_power = float(p.mean()) if len(p) else 0.0
    max_power = float(p.max()) if len(p) else 0.0
    return kwh_total, avg_power, max_power
