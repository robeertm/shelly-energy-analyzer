"""Pytest entry for the customer walkthrough and the API tour (quick mode).

The browser walkthrough is skipped unless ``SEA_E2E=1`` (a run takes minutes) and when
playwright or a browser is not available; the API tour always runs.  The report is written to ``$SEA_E2E_REPORT`` (default: a
temp file) so CI can upload it.
"""
import json
import os
import sys
import tempfile
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

# The browser walkthrough takes minutes and needs a browser: opt in with SEA_E2E=1.
# The API tour needs neither and runs in every normal test run.
needs_e2e = pytest.mark.skipif(os.environ.get("SEA_E2E") != "1", reason="set SEA_E2E=1 to run the browser walkthrough")

SEVERE = ("high", "medium")


def _report_path(name: str) -> Path:
    base = os.environ.get("SEA_E2E_REPORT")
    if base:
        p = Path(base)
        return p.with_name(p.stem + "-" + name + p.suffix) if name != "walkthrough" else p
    return Path(tempfile.gettempdir()) / f"sea-e2e-{name}.json"


@needs_e2e
def test_walkthrough_quick():
    pytest.importorskip("playwright")
    import common
    why = common.browser_available("chromium")
    if why:
        pytest.skip("no usable browser: " + why)
    import walkthrough
    rep = walkthrough.run(quick=True, browsers=["chromium"], jobs=int(os.environ.get("SEA_E2E_JOBS", "2")))
    _report_path("walkthrough").write_text(json.dumps(rep, indent=1, default=list), encoding="utf-8")
    bad = [f for f in rep["findings"] if f["severity"] in SEVERE]
    assert not bad, "walkthrough findings:\n" + "\n".join(
        f"[{f['severity']}] {f['installation']}/{f['lang']}/{f['viewport']} {f['page']} {f['kind']}: {f['detail'][:160]}" for f in bad[:30])


def test_api_tour_quick():
    import api_tour
    rep = api_tour.run(quick=True, installations=["a", "e", "f"])
    _report_path("api").write_text(json.dumps(rep, indent=1), encoding="utf-8")
    bad = [f for f in rep["findings"] if f["severity"] in SEVERE]
    assert not bad, "api findings:\n" + "\n".join(
        f"[{f['severity']}] {f['installation']} {f['endpoint'][:80]} {f['kind']}: {f['detail'][:140]}" for f in bad[:30])
