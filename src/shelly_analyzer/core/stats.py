from __future__ import annotations

import pandas as pd


def daily_kwh(df: pd.DataFrame) -> pd.Series:
    return df.set_index("timestamp")["energy_kwh"].resample("D").sum()


def weekly_kwh(df: pd.DataFrame) -> pd.Series:
    g = df.set_index("timestamp")["energy_kwh"].resample("W-MON").sum()
    g.index = g.index.date
    return g


def monthly_kwh(df: pd.DataFrame) -> pd.Series:
    g = df.set_index("timestamp")["energy_kwh"].resample("MS").sum()
    g.index = g.index.strftime("%Y-%m")
    return g
