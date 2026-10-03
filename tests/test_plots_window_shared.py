"""One chart, one period — even when a device stopped reporting months ago.

Run: python3 tests/test_plots_window_shared.py  (no pytest dependency)

Covers the "my whole history is gone" report, which was not a data loss at
all. A preset range ("last 30 days") used to be anchored on each device's
OWN newest row, so a switch that last reported in January got the 30 days
before *its* last row. The chart's x-axis then spanned the union of all
those windows: nine months wide, with the real 30 days squeezed into the
right-hand quarter and a lone block of ancient bars at the far left. On the
install this was found on, a 30-day request returned 784 hourly buckets
from 2026-01-13 to 2026-10-03 — every value correct, and the picture
useless.

The anchor is still the newest row rather than the wall clock, on purpose:
a system that has been off for a day should show its last 30 days of data,
not 30 days of emptiness. It is now the newest row across the *selected*
devices, so one chart covers one period. A device with nothing in that
period simply has no bars.
"""
import json
import os
import sqlite3
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from shelly_analyzer.io.config import load_config  # noqa: E402
from shelly_analyzer.io.storage import Storage  # noqa: E402
from shelly_analyzer.web.action_dispatch import ActionDispatcher  # noqa: E402

_failures = []


def check(cond, msg):
    print(("  ok: " if cond else "  FAIL: ") + msg)
    if not cond:
        _failures.append(msg)


class _LiveStub:
    def snapshot(self, *a, **k):
        return {}


def _fill(db_path, key, erste_stunde, stunden, watt):
    """Hourly samples straight into the table — the test owns this DB."""
    con = sqlite3.connect(db_path)
    with con:
        con.executemany(
            "INSERT OR REPLACE INTO samples "
            "(device_key, timestamp, a_act_power, total_power, energy_kwh) "
            "VALUES (?,?,?,?,?)",
            [(key, erste_stunde + i * 3600, watt, watt, watt / 1000.0)
             for i in range(stunden)])
    con.close()


JETZT = (int(time.time()) // 3600) * 3600
TAG = 86400

with tempfile.TemporaryDirectory() as tmp:
    tmp = Path(tmp)
    st = Storage(tmp / "data")
    db_datei = str(st.db.db_path)

    # "aktiv" meldet bis jetzt, "still" hat vor 200 Tagen aufgehört.
    _fill(db_datei, "aktiv", JETZT - 5 * TAG, 5 * 24, 1000.0)
    _fill(db_datei, "still", JETZT - 200 * TAG, 48, 500.0)

    cfg_pfad = tmp / "config.json"
    cfg_pfad.write_text(json.dumps({
        "devices": [
            {"key": "aktiv", "name": "Aktiv", "host": "127.0.0.1", "em_id": 0, "kind": "em"},
            {"key": "still", "name": "Still", "host": "127.0.0.1", "em_id": 0, "kind": "em"},
        ],
    }))
    d = ActionDispatcher(load_config(cfg_pfad), st, _LiveStub(),
                         out_dir=tmp, cfg_path=cfg_pfad, lang="en")

    print("\n== a 2-day window stays two days wide ==")
    r = d.dispatch("plots_data", {
        "view": "kwh", "mode": "hours", "len": "2", "unit": "days",
        "devices": "aktiv,still",
    })
    check(r.get("ok"), f"the request succeeds ({r.get('error')})")
    lbls = r.get("labels") or []
    check(len(lbls) > 0, "there are buckets at all")
    spanne_h = len(lbls)
    check(spanne_h <= 2 * 24 + 2,
          f"the axis is ~48 buckets wide, not months: {spanne_h}")

    # 🔑 The decisive property: the FIRST bucket must lie inside the
    # requested period. Counting buckets alone could pass on a chart that
    # happens to be short; this pins where the axis starts.
    check(lbls[0] >= "2", "labels are dated strings")
    import datetime as _dt
    def _ts(lbl):
        for form in ("%Y-%m-%d %H:%M", "%Y-%m-%d"):
            try:
                return _dt.datetime.strptime(lbl, form).timestamp()
            except ValueError:
                continue
        return None
    erster = _ts(lbls[0])
    check(erster is not None and erster >= JETZT - 3 * TAG,
          f"the first bucket lies inside the period, not months back: {lbls[0]}")

    reihen = {t.get("key"): t for t in (r.get("traces") or [])}
    check("aktiv" in reihen, f"the active device is on the chart: {sorted(reihen)}")
    check(any(reihen.get("aktiv", {}).get("y") or []), "the active device has bars")
    # The silent device has nothing to show in this period. Whether the
    # app draws an empty series or leaves it out, what must NOT happen is
    # that it drags its own window onto the axis.
    still_y = reihen.get("still", {}).get("y") or []
    check(not any(still_y),
          f"the silent device adds no bars "
          f"({sum(1 for v in still_y if v)} non-zero)")

    print("\n== the silent device alone still shows its own last days ==")
    # Anchoring on data rather than on the clock is deliberate, so asking
    # for that device by itself must still find something.
    r2 = d.dispatch("plots_data", {
        "view": "kwh", "mode": "hours", "len": "2", "unit": "days",
        "devices": "still",
    })
    check(r2.get("ok"), "the request succeeds")
    y2 = [t.get("y") or [] for t in (r2.get("traces") or [])]
    check(any(any(y) for y in y2),
          "on its own, the silent device shows its last recorded days")

print()
if _failures:
    print(f"🔴 {len(_failures)} check(s) failed")
    sys.exit(1)
print("✅ all checks passed")
