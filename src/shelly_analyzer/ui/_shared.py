from __future__ import annotations
from typing import Any, Tuple, Optional

def _fmt_eur(x: float) -> str:
    return f"{x:,.2f} €".replace(",", "X").replace(".", ",").replace("X", ".")

def _fmt_kwh(x: float) -> str:
    return f"{x:,.3f} kWh".replace(",", "X").replace(".", ",").replace("X", ".")

def _parse_date_flexible(s: str) -> Optional[pd.Timestamp]:
    """Parse a date string in common formats.
    Accepts:
    - YYYY-MM-DD
    - DD.MM.YYYY
    - DD.MM.YY
    - Any pandas-parseable date/time
    """
    s = (s or "").strip()
    if not s:
        return None
    # Fast-path: common German format
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
        # ISO week: Monday..Sunday
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
    # Fallback
    return t, t + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)

PLOTS_MODES = [
    ("all", "All"),
    ("days", "Days"),
    ("weeks", "Weeks"),
    ("months", "Months"),
]

AUTOSYNC_INTERVAL_OPTIONS = [1, 2, 3, 6, 12, 24]

AUTOSYNC_MODE_OPTIONS = [
    ("incremental", "Inkrementell"),
    ("day", "Day"),
    ("week", "Week"),
    ("month", "Month"),
]

INVOICE_PERIOD_OPTIONS = [
    ("custom", "Custom"),
    ("day", "Tag"),
    ("week", "Woche"),
    ("month", "Monat"),
    ("year", "Jahr"),
]

# End of shared helpers/constants
