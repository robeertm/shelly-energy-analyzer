from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Literal, Optional

import pandas as pd

from .csv_read import detect_power_columns


@dataclass(frozen=True)
class TimeRange:
    start: pd.Timestamp
    end: pd.Timestamp


def _find_interval_energy_cols(df: pd.DataFrame) -> List[str]:
    candidates = [
        ("a_total_act_energy", "b_total_act_energy", "c_total_act_energy"),
        ("a_fund_act_energy", "b_fund_act_energy", "c_fund_act_energy"),
    ]
    for triplet in candidates:
        if all(c in df.columns for c in triplet):
            return list(triplet)
    return []


def calculate_energy(
    df: pd.DataFrame,
    power_columns: Optional[Iterable[str]] = None,
    method: Literal["auto", "interval", "avg", "max", "min"] = "auto",
) -> pd.DataFrame:
    """Compute per-row interval energy (kWh) and total_power (W).

    Notes:
    - If the CSV provides per-interval energy in Wh columns, we prefer them.
    - Otherwise we integrate power over the timestamp delta.
    """
    out = df.copy().sort_values("timestamp")
    out["delta_s"] = out["timestamp"].diff().dt.total_seconds().fillna(0).clip(lower=0)

    energy_cols = _find_interval_energy_cols(out)
    has_minmax = all(
        c in out.columns
        for c in [
            "a_min_act_power",
            "b_min_act_power",
            "c_min_act_power",
            "a_max_act_power",
            "b_max_act_power",
            "c_max_act_power",
        ]
    )

    eff = method
    if eff == "auto":
        if energy_cols:
            eff = "interval"
        elif has_minmax:
            eff = "avg"
        else:
            cols = list(power_columns) if power_columns else detect_power_columns(out)
            power_columns = cols
            eff = "avg" if cols else "max"

    if eff == "interval" and energy_cols:
        wh = out[energy_cols].fillna(0).sum(axis=1)
        out["energy_kwh"] = wh / 1000.0
        # Derive average W over the delta (avoid div by 0)
        out["total_power"] = 0.0
        mask = out["delta_s"] > 0
        out.loc[mask, "total_power"] = (out.loc[mask, "energy_kwh"] * 1000.0) * (3600.0 / out.loc[mask, "delta_s"])
        out["calc_method"] = "interval(Wh)"
        return out

    if eff in {"avg", "max", "min"} and has_minmax:
        a_min = out["a_min_act_power"].fillna(0)
        b_min = out["b_min_act_power"].fillna(0)
        c_min = out["c_min_act_power"].fillna(0)
        a_max = out["a_max_act_power"].fillna(0)
        b_max = out["b_max_act_power"].fillna(0)
        c_max = out["c_max_act_power"].fillna(0)

        if eff == "avg":
            p = (a_min + a_max + b_min + b_max + c_min + c_max) / 2.0
        elif eff == "max":
            p = a_max + b_max + c_max
        else:
            p = a_min + b_min + c_min

        out["total_power"] = p
        out["energy_kwh"] = (out["total_power"] * (out["delta_s"] / 3600.0)) / 1000.0
        out["calc_method"] = f"power-{eff}"
        return out

    cols = list(power_columns) if power_columns else detect_power_columns(out)
    if not cols:
        raise ValueError("No usable columns to compute energy.")
    out["total_power"] = out[cols].fillna(0).sum(axis=1)
    out["energy_kwh"] = (out["total_power"] * (out["delta_s"] / 3600.0)) / 1000.0
    out["calc_method"] = "power-sum"
    return out


def suggest_time_range(df: pd.DataFrame) -> TimeRange:
    return TimeRange(start=df["timestamp"].min(), end=df["timestamp"].max())


def filter_by_time(df: pd.DataFrame, start: Optional[pd.Timestamp] = None, end: Optional[pd.Timestamp] = None) -> pd.DataFrame:
    if start is None and end is None:
        return df.copy()
    if start is None:
        return df.loc[df["timestamp"] <= end].copy()
    if end is None:
        return df.loc[df["timestamp"] >= start].copy()
    return df.loc[(df["timestamp"] >= start) & (df["timestamp"] <= end)].copy()
