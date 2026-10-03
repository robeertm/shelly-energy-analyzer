"""The live poller must survive a network that swallows packets.

Run: python3 tests/test_live_poller_outage.py  (no pytest dependency)

Covers the "live view frozen on old values, nothing in the log" bug.

``MultiLivePoller`` had two completely different behaviours, and only one
of them was healthy:

* A device that **refuses** the connection answers fast. Its future
  completes inside the ``as_completed`` window, so the error is counted,
  logged and backed off. Fine.
* A device whose packets simply **vanish** — a wrong route, a VPN down, a
  switch unplugged — never completes inside that window. ``as_completed``
  raised ``TimeoutError``, the futures were abandoned without ever being
  examined, and because nothing was counted the device was due again on
  the very next tick. Measured on the released code: four submissions per
  second indefinitely, a thread-pool backlog growing ~2.4/s, **not one
  line in the log**, and no backoff at all. After a twelve-hour outage
  the pool had a six-figure backlog of stale requests, so the workers
  were still chewing through those when the network came back and no
  fresh sample ever arrived. The dashboard sat on the values from the
  moment the network broke, which to a person looking at it reads as
  "live".

So this does not test "an unreachable device yields no data" — that much
is obvious. It tests the three things that were actually wrong:

  1. it says something,
  2. it backs off instead of hammering,
  3. it keeps at most one request per device outstanding — which is what
     makes the recovery immediate.

The mock Shelly accepts the connection and then stays silent, because
that is what a black-holed route looks like from the client's side. A
mock that refuses the connection would exercise the half that was never
broken.
"""
import concurrent.futures
import json
import logging
import os
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from shelly_analyzer.io.config import DeviceConfig, DownloadConfig  # noqa: E402
from shelly_analyzer.services.live import MultiLivePoller  # noqa: E402

BLACKHOLE = {"on": False}
_lines = []


class _Collect(logging.Handler):
    def emit(self, record):
        _lines.append(f"{record.levelname}: {record.getMessage()}")


logging.basicConfig(level=logging.DEBUG)
logging.getLogger().handlers = [_Collect()]

_STATUS = {
    "id": 0,
    "a_voltage": 233.0, "a_current": 0.5, "a_act_power": 110.0,
    "b_voltage": 233.0, "b_current": 0.5, "b_act_power": 110.0,
    "c_voltage": 233.0, "c_current": 0.5, "c_act_power": 110.0,
    "total_act_power": 330.0, "total_current": 1.5,
}


class _MockShelly(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.0"

    def do_POST(self):
        if BLACKHOLE["on"]:
            time.sleep(60)  # accepted, then silent — a black hole
            return
        n = int(self.headers.get("Content-Length") or 0)
        if n:
            self.rfile.read(n)
        body = json.dumps({"result": _STATUS}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        pass


# Count what the poller submits. The released code submitted four per
# second for as long as the outage lasted; the fix must submit far fewer.
_SUBMITS = {"n": 0}
_orig_submit = concurrent.futures.ThreadPoolExecutor.submit


def _counting_submit(self, fn, *args, **kwargs):
    _SUBMITS["n"] += 1
    return _orig_submit(self, fn, *args, **kwargs)


concurrent.futures.ThreadPoolExecutor.submit = _counting_submit

_failures = []


def check(cond, msg):
    print(("  ok: " if cond else "  FAIL: ") + msg)
    if not cond:
        _failures.append(msg)


srv = ThreadingHTTPServer(("127.0.0.1", 0), _MockShelly)
PORT = srv.server_address[1]
threading.Thread(target=srv.serve_forever, daemon=True).start()

DEVICES = [
    DeviceConfig(key=f"s{i}", name=f"Device {i}", host=f"127.0.0.1:{PORT}",
                 em_id=0, kind="em", gen=2)
    for i in range(1, 5)
]
POLL = 1.0
poller = MultiLivePoller(
    DEVICES,
    DownloadConfig(timeout_seconds=3.0, retries=1, backoff_base_seconds=0.1),
    poll_seconds=POLL,
)


def drain():
    n = 0
    while True:
        try:
            poller.samples.get_nowait()
            n += 1
        except Exception:
            return n


poller.start()

print("\n== healthy: samples arrive ==")
time.sleep(8)
check(drain() >= 10, "samples arrive while the device answers")
check(not [x for x in _lines if "Live poll failed" in x],
      "and nothing is reported as a failure")

print("\n== black hole: it must speak up, back off, and stay bounded ==")
_lines.clear()
BLACKHOLE["on"] = True
windows = []
peak_outstanding = 0
peak_backlog = 0
for _ in range(3):
    before = _SUBMITS["n"]
    for _ in range(16):
        time.sleep(0.5)
        # Sampled, not read once at the end: at any single instant every
        # device may happen to be inside its backoff with nothing in
        # flight, and "0 <= 4" would be a check that cannot fail.
        peak_outstanding = max(peak_outstanding,
                               len(getattr(poller, "_inflight", {}) or {}))
        peak_backlog = max(peak_backlog,
                           poller._executor._work_queue.qsize())
    windows.append(_SUBMITS["n"] - before)

reported = [x for x in _lines if "Live poll failed" in x]
worst = max(poller._err_count.values())
print(f"     submissions per 8 s: {windows} | peak outstanding "
      f"{peak_outstanding} | peak backlog {peak_backlog} | "
      f"error count {worst} | log lines {len(reported)}")

check(len(reported) >= 1, f"it says something: {len(reported)} log line(s)")
check(worst >= 2, f"failures are counted: highest counter {worst}")
# A brake is measured over time: the attempts must get rarer.
# "fewer than at the start" alone let the released code through on
# noise (28 < 30). Exponential backoff must at least halve the rate.
check(windows[-1] * 2 <= windows[0],
      f"it backs off: last window {windows[-1]} is at most half of the "
      f"first {windows[0]}")
check(1 <= peak_outstanding <= len(DEVICES),
      f"between one and one-per-device outstanding at any time: "
      f"{peak_outstanding} (upper bound {len(DEVICES)})")
check(peak_backlog == 0,
      f"no thread-pool backlog at any point: {peak_backlog} "
      f"(released code: grew ~2.4/s, ~70 after 30 s)")
check(sum(windows) <= 40,
      f"submissions stay bounded: {sum(windows)} in 24 s "
      f"(released code: ~96)")

print("\n== recovery: back within one backoff window ==")
BLACKHOLE["on"] = False
drain()
_lines.clear()
back_after = None
for sec in range(1, 41):
    time.sleep(1)
    if drain() > 0:
        back_after = sec
        break
check(back_after is not None and back_after <= 35,
      f"fresh samples again after {back_after} s (backoff caps at 30 s)")
time.sleep(3)
check(drain() >= 3, "and it keeps running")
check(any("recovered" in x for x in _lines), "the recovery is reported")

poller.stop()
print()
if _failures:
    print(f"🔴 {len(_failures)} check(s) failed")
    sys.exit(1)
print("✅ all checks passed")
