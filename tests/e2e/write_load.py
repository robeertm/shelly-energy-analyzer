"""Write-load bench: how many bytes a day does the running app write to disk?

Issue #20 reported 30–48 GB a day from an installation with eight plugs at the
default 1 s poll. This bench reproduces that installation and measures the
real server process:

* eight simulated Plug Gen3 devices answer ``Switch.GetStatus`` on localhost
  (power changes every few seconds, a running ``aenergy.total``);
* the installation is built like the walkthrough's "plugs only" one (two days
  of minute history in the database) and the Live-tab restart cache is seeded
  with a full 2 h window, so the buffer is at its steady-state size from the
  first second instead of filling up during the run;
* the installation is started and stopped once before the measurement, like
  one that has run before, and the WAL is folded into the database: one-off
  work of a very first start is not counted (``--first-start`` measures it);
  what the measured start writes before the window is reported separately;
* the server runs with its defaults (1 s poll, 120 min live window) and no
  internet (outbound calls fail fast); an open Live tab asks ``/api/state``
  once a second;
* every ``write``/``pwrite``/``writev`` the process makes to a regular file is
  counted per file (``strace``), plus ``write_bytes`` from ``/proc/<pid>/io``
  when strace is not available.

Run it from the repository root::

    python -m tests.e2e.write_load --minutes 60          # the full hour
    python -m tests.e2e.write_load --minutes 10 --json out.json

The result lists every file with its bytes and the projection to GB a day.
``tests/e2e/test_write_load_budget.py`` runs a shorter window and holds the
total under the budget below.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import random
import re
import shutil
import subprocess
import sys
import threading
import time
import urllib.request
from collections import defaultdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Dict, List, Optional

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))

from tests.e2e.common import OFFLINE_ENV, free_port, make_tmp, rm_tree  # noqa: E402
from tests.e2e import installations as inst  # noqa: E402

# The whole process, every file together, may write at most this much a day.
BUDGET_GB_PER_DAY = 0.5

DEVICES = 8
DAY_S = 86400.0


# --- simulated plugs ----------------------------------------------------------------------

class _PlugState:
    """One Plug Gen3: a load that switches every few seconds to minutes."""

    def __init__(self, key: str, seed: int):
        self.key = key
        self.rnd = random.Random(seed)
        self.wh = 1000.0 + seed * 37.0
        self.load = 0.0
        self.next_change = 0.0
        self.last = time.time()
        self.lock = threading.Lock()

    def status(self) -> dict:
        with self.lock:
            now = time.time()
            if now >= self.next_change:
                self.load = self.rnd.choice([0.0, 0.0, 1.2, 4.8, 62.0, 105.0, 1180.0, 1850.0])
                self.next_change = now + self.rnd.uniform(5.0, 240.0)
            w = round(self.load + self.rnd.uniform(-0.4, 0.4), 1) if self.load else 0.0
            self.wh += max(0.0, w) * (now - self.last) / 3600.0
            self.last = now
            v = round(self.rnd.uniform(229.0, 233.0), 1)
            return {
                "id": 0, "source": "init", "output": True, "apower": w, "voltage": v,
                "freq": round(self.rnd.uniform(49.97, 50.03), 2), "current": round(w / v, 3),
                "aenergy": {"total": round(self.wh, 3), "by_minute": [0.0, 0.0, 0.0],
                            "minute_ts": int(now) // 60 * 60},
                "ret_aenergy": {"total": 0.0, "by_minute": [0.0, 0.0, 0.0],
                                "minute_ts": int(now) // 60 * 60},
                "temperature": {"tC": 31.2, "tF": 88.2},
            }


def _handler_for(plug: _PlugState):
    class _H(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def _answer(self):
            n = int(self.headers.get("Content-Length") or 0)
            if n:
                self.rfile.read(n)
            path = self.path.split("?")[0]
            if path.endswith("/Switch.GetStatus"):
                body = plug.status()
            elif path.endswith("/Shelly.GetStatus"):
                body = {"switch:0": plug.status(), "sys": {"uptime": 1000}}
            elif path.endswith("/Shelly.GetDeviceInfo"):
                body = {"id": f"shellyplugsg3-{plug.key}", "model": inst.PLUG_MODEL, "gen": 3,
                        "fw_id": "20250101-000000/1.5.0", "ver": "1.5.0", "app": "PlugSG3"}
            else:
                body = {"code": 404, "message": "No handler for " + path}
            raw = json.dumps(body).encode()
            self.send_response(200 if "code" not in body else 404)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        do_GET = _answer
        do_POST = _answer

        def log_message(self, *a):
            pass

    return _H


class SimulatedPlugs:
    """Eight HTTP servers on localhost, one per plug."""

    def __init__(self, n: int = DEVICES):
        self.servers: List[ThreadingHTTPServer] = []
        self.hosts: List[str] = []
        for i in range(n):
            plug = _PlugState(f"plug{i}", seed=20 + i)
            srv = ThreadingHTTPServer(("127.0.0.1", free_port()), _handler_for(plug))
            srv.daemon_threads = True
            threading.Thread(target=srv.serve_forever, daemon=True).start()
            self.servers.append(srv)
            self.hosts.append(f"127.0.0.1:{srv.server_address[1]}")

    def close(self) -> None:
        for s in self.servers:
            s.shutdown()
            s.server_close()


# --- installation --------------------------------------------------------------------------

def build_installation(target: Path, hosts: List[str]) -> Path:
    """The walkthrough's 'plugs only' installation, pointed at the simulated plugs,
    with a full Live-tab restart cache."""
    devices = []
    for i, ((key, name), host) in enumerate(zip(inst.PLUG_NAMES, hosts)):
        d = inst._plug(key, name, i)
        d["host"] = host
        devices.append(d)
    cfg = inst._base_cfg(devices, "en")
    inst._write(target, cfg)
    inst._fill_plugs(target, [k for k, _ in inst.PLUG_NAMES])
    _seed_live_cache(target, [k for k, _ in inst.PLUG_NAMES])
    return target / "config.json"


def _checkpoint(target: Path) -> None:
    """Fold the WAL into the database, so a checkpoint during the run does not
    count pages written before it as writes of the measured window."""
    import sqlite3
    con = sqlite3.connect(str(target / "data" / "energy.db"))
    try:
        con.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    finally:
        con.close()


def _prestart(cfg_path: Path, cwd: Path, env: dict) -> None:
    """Start the app once and stop it cleanly, like an installation that has run
    before: one-off work of a first start (migrations, heals over the history
    the builder wrote) is then not part of the measurement."""
    port = free_port()
    p = subprocess.Popen([sys.executable, "-m", "shelly_analyzer", "--config", str(cfg_path),
                          "--port", str(port), "--no-ssl", "--host", "127.0.0.1"],
                         env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, cwd=str(cwd))
    try:
        deadline = time.time() + 180
        while time.time() < deadline and p.poll() is None:
            try:
                urllib.request.urlopen(f"http://127.0.0.1:{port}/api/health", timeout=3).read()
                break
            except Exception:
                time.sleep(0.5)
        time.sleep(15)
    finally:
        if p.poll() is None:
            p.terminate()
            try:
                p.wait(timeout=60)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait(timeout=10)


def _seed_live_cache(target: Path, keys: List[str]) -> None:
    from shelly_analyzer.services.webdash import LivePoint, LiveStateStore
    from shelly_analyzer.web.background import live_history_path

    points = 120 * 60 + 50
    st = LiveStateStore(max_points=points)
    rnd = random.Random(7)
    t0 = int(time.time()) - 120 * 60 + 5
    for d, key in enumerate(keys):
        kwh, load = 1.25, 0.0
        for i in range(120 * 60 - 5):
            if i % 120 == 0:
                load = rnd.choice([0.0, 4.8, 62.0, 1180.0])
            w = round(load + rnd.uniform(-0.4, 0.4), 1) if load else 0.0
            kwh += w / 3_600_000.0
            st.update(key, LivePoint(
                ts=t0 + i, power_total_w=w, va=round(rnd.uniform(229.0, 233.0), 1), vb=0.0, vc=0.0,
                ia=round(w / 230.0, 3), ib=0.0, ic=0.0, pa=w, kwh_today=round(kwh, 5),
                cost_today=kwh * 0.32, freq_hz=50.0))
    st.dump_to_path(live_history_path(target), skip_unchanged=False)


# --- measurement ---------------------------------------------------------------------------

_WRITE_RE = re.compile(
    r"^(?:\[pid\s+)?(\d+)\]?\s*(?:\d+\s+)?(write|pwrite64|writev|pwritev|pwritev2)\((\d+)<([^>]*)>.*\)\s+=\s+(\d+)")


def parse_strace(path: Path, root: Path) -> Dict[str, int]:
    """Bytes written per regular file (path relative to ``root`` when inside it)."""
    per_file: Dict[str, int] = defaultdict(int)
    root_s = str(root.resolve())
    with open(path, "r", errors="replace") as fh:
        for line in fh:
            m = _WRITE_RE.search(line)
            if not m:
                # strace prints "<pid> write(...)" with -f into one file
                m2 = re.search(r"(write|pwrite64|writev|pwritev|pwritev2)\((\d+)<([^>]*)>.*\)\s+=\s+(\d+)", line)
                if not m2:
                    continue
                target, n = m2.group(3), int(m2.group(4))
            else:
                target, n = m.group(4), int(m.group(5))
            if not target.startswith("/") or target.startswith("/dev/"):
                continue  # sockets, pipes, ttys
            if target.startswith(root_s + "/"):
                target = target[len(root_s) + 1:]
            target = _group(target)
            per_file[target] += n
    return dict(per_file)


def _group(rel: str) -> str:
    """Fold temporary names of atomic writes onto the file they replace."""
    rel = re.sub(r"\.live_history\.[^/]*\.tmp$", "live_history.json.gz (tmp+rename)", rel)
    rel = re.sub(r"/tmp[a-z0-9_]{6,}(\.tmp)?$", "/(mkstemp)", rel)
    return rel


def _proc_io(pid: int) -> Dict[str, int]:
    out: Dict[str, int] = {}
    try:
        for line in Path(f"/proc/{pid}/io").read_text().splitlines():
            k, _, v = line.partition(":")
            out[k.strip()] = int(v.strip())
    except OSError:
        pass
    return out


def _find_app_pid(root_pid: int) -> int:
    """strace forks the app: the python child is the one to read /proc of."""
    try:
        kids = Path(f"/proc/{root_pid}/task/{root_pid}/children").read_text().split()
        return int(kids[0]) if kids else root_pid
    except (OSError, ValueError, IndexError):
        return root_pid


def _stop_pid(pid: int, parent: int) -> None:
    if pid == parent:
        return
    import signal
    for sig, wait_s in ((signal.SIGTERM, 30.0), (signal.SIGKILL, 10.0)):
        try:
            os.kill(pid, sig)
        except ProcessLookupError:
            return
        deadline = time.time() + wait_s
        while time.time() < deadline:
            if not Path(f"/proc/{pid}").exists():
                return
            try:
                if Path(f"/proc/{pid}/stat").read_text().split()[2] == "Z":
                    return
            except (OSError, IndexError):
                return
            time.sleep(0.2)


def run(minutes: float, keep: bool = False, use_strace: Optional[bool] = None,
        workdir: Optional[Path] = None, warmup_s: float = 20.0, prestart: bool = True) -> dict:
    """Start the simulated installation, measure ``minutes`` of steady state,
    return the per-file result. ``warmup_s`` of start-up (loading the cache,
    first sync of the minute history) is not part of the window."""
    if use_strace is None:
        use_strace = shutil.which("strace") is not None and sys.platform.startswith("linux")
    tmp = Path(workdir) if workdir else make_tmp("sea-writeload-")
    tmp.mkdir(parents=True, exist_ok=True)
    plugs = SimulatedPlugs()
    proc = None
    log = None
    try:
        cfg_path = build_installation(tmp, plugs.hosts)
        env = {**os.environ, **OFFLINE_ENV, "PYTHONPATH": str(ROOT / "src"), "PYTHONUNBUFFERED": "1"}
        if prestart:
            _prestart(cfg_path, tmp, env)
        _checkpoint(tmp)
        port = free_port()
        cmd = [sys.executable, "-m", "shelly_analyzer", "--config", str(cfg_path),
               "--port", str(port), "--no-ssl", "--host", "127.0.0.1"]
        trace = tmp / "strace.log"
        if use_strace:
            cmd = ["strace", "-f", "-qq", "-y", "-s", "0", "-e", "signal=none",
                   "-e", "trace=write,pwrite64,writev,pwritev,pwritev2", "-o", str(trace)] + cmd
        # stdout goes through a pipe, as under systemd (journal) or Docker (container
        # log): those lines are written by another process, not by the app. Their
        # volume is reported separately.
        log = open(tmp / "server.log", "wb")
        proc = subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                cwd=str(tmp))
        stdout_bytes = [0]
        stdout_mark = [0]

        def _drain():
            for chunk in iter(lambda: proc.stdout.read1(65536), b""):
                stdout_bytes[0] += len(chunk)
                log.write(chunk)
                log.flush()

        drainer = threading.Thread(target=_drain, daemon=True)
        drainer.start()
        deadline = time.time() + 180
        while True:
            if proc.poll() is not None:
                raise RuntimeError("server exited early:\n" + (tmp / "server.log").read_text(errors="replace")[-2000:])
            try:
                urllib.request.urlopen(f"http://127.0.0.1:{port}/api/health", timeout=3).read()
                break
            except Exception:
                if time.time() > deadline:
                    raise RuntimeError("server did not come up in 180 s")
                time.sleep(0.5)
        app_pid = _find_app_pid(proc.pid) if use_strace else proc.pid
        time.sleep(warmup_s)
        trace_mark = trace.stat().st_size if use_strace and trace.exists() else 0
        io0 = _proc_io(app_pid)
        stdout_mark[0] = stdout_bytes[0]
        t0 = time.time()
        # Ask the Live endpoint like an open Live tab does: once a second.
        end = t0 + minutes * 60.0
        samples = 0
        while time.time() < end:
            if proc.poll() is not None:
                raise RuntimeError("server died during the run:\n" + (tmp / "server.log").read_text(errors="replace")[-2000:])
            try:
                st = json.loads(urllib.request.urlopen(f"http://127.0.0.1:{port}/api/state", timeout=10).read())
                samples = max(samples, sum(1 for _ in (st.get("devices") or [])) if isinstance(st, dict) else 0)
            except Exception:
                pass
            time.sleep(min(1.0, max(0.0, end - time.time())))
        elapsed = time.time() - t0
        io1 = _proc_io(app_pid)
        out_b = stdout_bytes[0] - stdout_mark[0]
        per_file: Dict[str, int] = {}
        startup: Dict[str, int] = {}
        if use_strace:
            head = tmp / "strace.startup.log"
            with open(trace, "rb") as src, open(head, "wb") as dst:
                dst.write(src.read(trace_mark))
            startup = parse_strace(head, tmp)
            # Only the part of the trace that belongs to the window.
            window = tmp / "strace.window.log"
            with open(trace, "rb") as src, open(window, "wb") as dst:
                src.seek(trace_mark)
                src.readline()  # a possibly cut first line
                shutil.copyfileobj(src, dst)
            per_file = parse_strace(window, tmp)
        files_total = sum(per_file.values())
        wb = io1.get("write_bytes", 0) - io0.get("write_bytes", 0)
        scale = DAY_S / elapsed
        result = {
            "minutes": round(elapsed / 60.0, 2),
            "devices": DEVICES,
            "poll_seconds": 1.0,
            "strace": bool(use_strace),
            "files": {k: {"bytes": v, "gb_per_day": round(v * scale / 1e9, 4)}
                      for k, v in sorted(per_file.items(), key=lambda kv: -kv[1])},
            "startup_bytes": sum(startup.values()),
            "startup_files": dict(sorted(startup.items(), key=lambda kv: -kv[1])[:5]),
            "files_total_bytes": files_total,
            "files_gb_per_day": round(files_total * scale / 1e9, 4),
            "proc_write_bytes": wb,
            "proc_gb_per_day": round(wb * scale / 1e9, 4),
            "stdout_bytes": out_b,
            "stdout_mb_per_day": round(out_b * scale / 1e6, 2),
            "budget_gb_per_day": BUDGET_GB_PER_DAY,
            "live_devices_seen": samples,
            "workdir": str(tmp),
        }
        # The budget is checked on the larger of both views.
        result["gb_per_day"] = max(result["files_gb_per_day"], result["proc_gb_per_day"])
        return result
    finally:
        if proc is not None and proc.poll() is None and use_strace:
            # Stop the app itself first: terminating strace only detaches it and
            # the server would go on running.
            _stop_pid(_find_app_pid(proc.pid), proc.pid)
        if proc is not None and proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=10)
        if log is not None:
            log.close()
        plugs.close()
        if not keep and not workdir:
            rm_tree(tmp)


def format_result(r: dict) -> str:
    lines = [f"{r['devices']} plugs at {r['poll_seconds']:g} s, {r['minutes']:.1f} min measured"
             f" ({'strace per file' if r['strace'] else '/proc io only'})", ""]
    if r["files"]:
        w = max(len(k) for k in r["files"])
        lines.append(f"{'file':<{w}}  {'bytes':>12}  {'GB/day':>8}")
        for k, v in r["files"].items():
            lines.append(f"{k:<{w}}  {v['bytes']:>12,}  {v['gb_per_day']:>8.4f}")
        lines.append(f"{'all files':<{w}}  {r['files_total_bytes']:>12,}  {r['files_gb_per_day']:>8.4f}")
        lines.append("")
    if r.get("startup_files"):
        lines.append("start-up (not in the window): " + ", ".join(
            f"{k} {v:,} B" for k, v in r["startup_files"].items()))
    lines.append(f"/proc/<pid>/io write_bytes: {r['proc_write_bytes']:,} B -> {r['proc_gb_per_day']:.4f} GB/day")
    lines.append(f"stdout (journal / container log, written by another process): "
                 f"{r['stdout_bytes']:,} B -> {r['stdout_mb_per_day']:.2f} MB/day")
    lines.append(f"budget {r['budget_gb_per_day']} GB/day -> {'OK' if r['gb_per_day'] < r['budget_gb_per_day'] else 'OVER'}")
    return "\n".join(lines)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--minutes", type=float, default=60.0)
    ap.add_argument("--json", type=Path, default=None, help="also write the result as JSON")
    ap.add_argument("--keep", action="store_true", help="keep the work directory")
    ap.add_argument("--no-strace", action="store_true", help="only /proc/<pid>/io")
    ap.add_argument("--first-start", action="store_true",
                    help="measure the very first start (no earlier run of the installation)")
    a = ap.parse_args(argv)
    r = run(a.minutes, keep=a.keep, use_strace=False if a.no_strace else None,
            prestart=not a.first_start)
    print(format_result(r))
    if a.json:
        a.json.write_text(json.dumps(r, indent=2))
    return 0 if r["gb_per_day"] < BUDGET_GB_PER_DAY else 1


if __name__ == "__main__":
    sys.exit(main())
