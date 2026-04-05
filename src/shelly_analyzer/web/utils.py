"""Utility functions extracted from ui/_shared.py for Flask web app."""
from __future__ import annotations

from typing import Optional, Tuple

import pandas as pd


def _parse_date_flexible(s: str) -> Optional[pd.Timestamp]:
    """Parse a date string in common formats."""
    s = (s or "").strip()
    if not s:
        return None
    for fmt in ("%d.%m.%Y", "%d.%m.%y", "%Y-%m-%d"):
        try:
            from datetime import datetime
            return pd.Timestamp(datetime.strptime(s, fmt))
        except Exception:
            pass
    try:
        return pd.to_datetime(s, errors="raise")
    except Exception:
        return None


def _period_bounds(anchor: pd.Timestamp, period: str) -> Tuple[pd.Timestamp, pd.Timestamp]:
    """Return inclusive [start, end] bounds for a given period containing anchor."""
    a = pd.Timestamp(anchor).to_pydatetime()
    t = pd.Timestamp(a).normalize()
    if period == "day":
        start = t
        end = t + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
        return start, end
    if period == "week":
        start = t - pd.Timedelta(days=int(t.weekday()))
        end = start + pd.Timedelta(days=7) - pd.Timedelta(seconds=1)
        return start, end
    if period == "month":
        start = t.replace(day=1)
        end = (start + pd.offsets.MonthBegin(1)) - pd.Timedelta(seconds=1)
        return start, end
    if period == "year":
        start = pd.Timestamp(year=int(t.year), month=1, day=1)
        end = pd.Timestamp(year=int(t.year) + 1, month=1, day=1) - pd.Timedelta(seconds=1)
        return start, end
    return t, t + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
