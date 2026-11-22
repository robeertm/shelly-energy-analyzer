"""Start-up, memory and CPU of the running app stay inside their limits.

Starts the real server against eight simulated Plug Gen3 devices polled every
second (``perf_bench.py``, the installation of the write-load bench), at
``nice -n 19`` on one CPU core, with an open Live tab asking ``/api/state`` once
a second. The limits are CPU *time*, not wall time, so a busy machine does not
turn the test red; the memory limit is the peak resident size.

``SEA_PERF_MINUTES`` sets the window (default 3); the longer run is
``python -m tests.e2e.perf_bench --minutes 10``.
"""
import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))


@pytest.mark.skipif(os.environ.get("SEA_E2E") != "1", reason="set SEA_E2E=1 to run the process checks")
@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="reads /proc/<pid>/stat and status")
def test_eight_plugs_at_one_second_stay_inside_the_resource_limits():
    from tests.e2e import perf_bench

    minutes = float(os.environ.get("SEA_PERF_MINUTES", "3"))
    r = perf_bench.run(minutes)
    report = perf_bench.format_result(r)
    print(report)
    # The bench saw the plugs live before it measured anything.
    assert r["start_live_cpu_s"] is not None, report
    assert not perf_bench.over_limits(r), report
    # One feed thread per start: the thread count does not grow during the window.
    assert r["threads_last"] <= r["threads_first"] + 2, report
