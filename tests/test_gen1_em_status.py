"""Gen1 Shelly EM/3EM live status: /status → flat EM.GetStatus-style fields.

Run: python3 tests/test_gen1_em_status.py  (no pytest dependency)

Covers the "Gen1 3EM shows Offline / no data despite /status responding" bug:
the live poller only spoke the Gen2 ``EM.GetStatus`` RPC, which 404s on a Gen1
device (SHEM-3, no /rpc). No live sample → device permanently offline. Gen1 must
instead be read from ``/status`` and its ``emeters`` array normalized.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from shelly_analyzer.io.http import (  # noqa: E402
    _gen1_status_to_em_fields, get_em_status,
)
from shelly_analyzer.services.live import parse_live_fields  # noqa: E402


# A realistic Gen1 Shelly 3EM /status payload (three emeters = three phases).
GEN1_3EM_STATUS = {
    "emeters": [
        {"power": 884.01, "pf": 0.99, "current": 3.77, "voltage": 234.5,
         "is_valid": True, "total": 12345.6, "total_returned": 0.0},
        {"power": 120.0, "pf": 0.90, "current": 0.55, "voltage": 233.1,
         "is_valid": True, "total": 6789.0, "total_returned": 0.0},
        {"power": 0.0, "pf": 0.00, "current": 0.00, "voltage": 232.8,
         "is_valid": True, "total": 42.0, "total_returned": 0.0},
    ],
    "total_power": 1004.01,
}


class _FakeResp:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


class _FakeHttp:
    """Minimal ShellyHttp stand-in that only knows GET /status, and blows up if
    anything tries the Gen2 RPC (POST) — proving Gen1 never hits /rpc."""

    def __init__(self, status):
        self._status = status
        self.get_urls = []

    def get(self, url, params=None, headers=None):
        self.get_urls.append(url)
        assert url.endswith("/status"), f"Gen1 must read /status, got {url}"
        return _FakeResp(self._status)

    def post(self, *a, **k):  # pragma: no cover - must never be called for Gen1
        raise AssertionError("Gen1 must not call the Gen2 RPC endpoint")


def test_gen1_3em_flat_fields():
    flat = _gen1_status_to_em_fields(GEN1_3EM_STATUS, em_id=0)
    assert flat["a_act_power"] == 884.01
    assert flat["b_act_power"] == 120.0
    assert flat["c_act_power"] == 0.0
    assert flat["a_voltage"] == 234.5
    assert flat["a_current"] == 3.77
    assert flat["a_pf"] == 0.99
    print("OK  Gen1 3EM /status maps three emeters to phases a/b/c")


def test_gen1_flat_fields_parse_into_live():
    flat = _gen1_status_to_em_fields(GEN1_3EM_STATUS, em_id=0)
    fields = parse_live_fields(flat)
    # Total power is the sum across phases.
    assert abs(fields["power_w"]["total"] - 1004.01) < 1e-6
    assert fields["power_w"]["a"] == 884.01
    assert fields["voltage_v"]["a"] == 234.5
    assert fields["current_a"]["a"] == 3.77
    print("OK  Gen1 flat fields feed parse_live_fields → correct per-phase live values")


def test_get_em_status_gen1_uses_status_not_rpc():
    http = _FakeHttp(GEN1_3EM_STATUS)
    data = get_em_status(http, "10.0.0.9", em_id=0, gen=1)
    assert http.get_urls == ["http://10.0.0.9/status"]
    assert data["a_act_power"] == 884.01
    print("OK  get_em_status(gen=1) reads /status and never touches /rpc")


def test_gen1_shelly_em_two_channels_selects_em_id():
    # Two-channel Shelly EM (single phase): em_id selects the channel → phase a.
    status = {"emeters": [
        {"power": 50.0, "pf": 0.8, "current": 0.25, "voltage": 230.0},
        {"power": 300.0, "pf": 0.95, "current": 1.4, "voltage": 231.0},
    ]}
    flat0 = _gen1_status_to_em_fields(status, em_id=0)
    flat1 = _gen1_status_to_em_fields(status, em_id=1)
    assert flat0["a_act_power"] == 50.0
    assert flat1["a_act_power"] == 300.0
    # Only phase a is populated for a single channel.
    assert "b_act_power" not in flat1
    print("OK  Gen1 two-channel Shelly EM honors em_id and exposes it as phase a")


def test_empty_status_is_safe():
    assert _gen1_status_to_em_fields({}, em_id=0) == {}
    assert _gen1_status_to_em_fields({"emeters": []}, em_id=0) == {}
    print("OK  Missing/empty emeters yields empty fields (no crash)")


if __name__ == "__main__":
    test_gen1_3em_flat_fields()
    test_gen1_flat_fields_parse_into_live()
    test_get_em_status_gen1_uses_status_not_rpc()
    test_gen1_shelly_em_two_channels_selects_em_id()
    test_empty_status_is_safe()
    print("\nALL GEN1 EM-STATUS TESTS PASSED")
