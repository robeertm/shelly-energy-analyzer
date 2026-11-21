"""The running app stays under its disk-write budget (issue #20).

Starts the real server against eight simulated Plug Gen3 devices polled every
second (``write_load.py``), seeds a full 2 h Live window, keeps an open Live
tab asking ``/api/state`` once a second, and measures what the process writes
over a window that includes at least one Live-tab snapshot (every 5 min) and
the minute history of all plugs. The projection to a day must stay under
``BUDGET_GB_PER_DAY`` — 17.10.4 wrote ~44 GB a day in this setup.

``SEA_WRITE_LOAD_MINUTES`` sets the window (default 6); the full hour is
``python -m tests.e2e.write_load --minutes 60``.
"""
import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))


@pytest.mark.skipif(os.environ.get("SEA_E2E") != "1", reason="set SEA_E2E=1 to run the process checks")
@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="reads /proc/<pid>/io")
def test_eight_plugs_at_one_second_stay_under_the_daily_write_budget():
    from tests.e2e import write_load

    minutes = float(os.environ.get("SEA_WRITE_LOAD_MINUTES", "6"))
    r = write_load.run(minutes)
    report = write_load.format_result(r)
    print(report)
    assert r["live_devices_seen"] or r["files"], report
    # The window covered a Live-tab snapshot, so the biggest periodic writer was measured.
    if r["strace"] and minutes >= 5.5:
        assert any("live_history" in k for k in r["files"]), report
    assert r["gb_per_day"] < write_load.BUDGET_GB_PER_DAY, report
    # The console log of one open Live tab: no line per request any more.
    assert r["stdout_mb_per_day"] < 2.0, report
