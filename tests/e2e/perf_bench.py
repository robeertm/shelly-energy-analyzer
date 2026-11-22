"""Resource bench: start-up time, memory and CPU of the running app with eight plugs.

The installation is the one of the write-load bench (``write_load.py``): eight
simulated Plug Gen3 devices on localhost, polled every second (the default), two
days of minute history and a full 2 h Live-tab restart cache. The server runs at
the lowest priority on one CPU core (``nice -n 19``, ``taskset``), like on a
small single-board computer that has other work to do, and an open Live tab
asks ``/api/state`` once a second.

Measured:

* **start-up**: wall time and CPU time from the process start to the first
  ``/api/health`` answer, and to the moment all eight plugs show a fresh live
  reading in ``/api/state``;
* **memory**: resident set size (``VmRSS``) sampled during the window, and the
  peak (``VmHWM``);
* **CPU**: user + system time of the process over the window, as a share of one
  core; also the number of threads.

CPU *time* (not wall time) is the number that is compared against the limits:
on a busy machine the wall time grows, the work the process does does not.

Run it from the repository root::

    python -m tests.e2e.perf_bench --minutes 5
    python -m tests.e2e.perf_bench --minutes 5 --profile prof.txt   # py-spy, collapsed stacks

``tests/e2e/test_perf_budget.py`` runs a short window and holds the numbers
under the limits below.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import threading
import time
import urllib.request
from pathlib import Path
from typing import Dict, List, Optional

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT))

from tests.e2e.common import OFFLINE_ENV, free_port, make_tmp, rm_tree  # noqa: E402
from tests.e2e import write_load as wl  # noqa: E402

# Limits (one core, eight plugs at 1 s, one open Live tab).
# Measured on a 4-core arm64 board (17.11.9): 2 % CPU, 215 MB peak, 1.9 CPU s to
# /api/health, all plugs live 0.1 CPU s later. The limits leave room for a
# slower runner, not for a regression of several times.
CPU_LIMIT_PCT = 15.0        # mean CPU of the steady state, % of one core
RSS_LIMIT_MB = 320.0        # peak resident memory
START_CPU_LIMIT_S = 10.0    # CPU seconds until /api/health answers
LIVE_CPU_LIMIT_S = 15.0     # CPU seconds until all plugs have a fresh reading

_CLK = os.sysconf("SC_CLK_TCK") if hasattr(os, "sysconf") else 100


def _cpu_s(pid: int) -> float:
    """User + system CPU seconds of the whole process (all threads)."""
    try:
        f = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        return (int(f[11]) + int(f[12])) / _CLK
    except (OSError, IndexError, ValueError):
        return 0.0


def _status(pid: int) -> Dict[str, int]:
    out: Dict[str, int] = {}
    try:
        for line in Path(f"/proc/{pid}/status").read_text().splitlines():
            k, _, v = line.partition(":")
            v = v.strip().split()
            if k in ("VmRSS", "VmHWM") and v:
                out[k] = int(v[0])  # kB
            elif k == "Threads" and v:
                out[k] = int(v[0])
    except OSError:
        pass
    return out


def _app_pid(root_pid: int) -> int:
    """Follow wrappers (nice, taskset exec in place; py-spy forks a child)."""
    pid = root_pid
    for _ in range(4):
        try:
            cmd = Path(f"/proc/{pid}/cmdline").read_bytes()
        except OSError:
            return pid
        if b"shelly_analyzer" in cmd and b"py-spy" not in cmd:
            return pid
        try:
            kids = Path(f"/proc/{pid}/task/{pid}/children").read_text().split()
        except OSError:
            return pid
        if not kids:
            return pid
        pid = int(kids[0])
    return pid


def _get(url: str, timeout: float = 10.0) -> Optional[dict]:
    try:
        return json.loads(urllib.request.urlopen(url, timeout=timeout).read())
    except Exception:
        return None


def _all_fresh(state: Optional[dict], keys: List[str], max_age: float = 5.0) -> bool:
    if not isinstance(state, dict):
        return False
    devs = {d.get("key"): d for d in (state.get("devices") or []) if isinstance(d, dict)}
    for k in keys:
        d = devs.get(k)
        if not d or d.get("offline"):
            return False
        age = d.get("sample_age_s")
        if age is None or float(age) > max_age:
            return False
    return True


def build_installation(target: Path, hosts: List[str], history_days: float = 2.0,
                       db_from: Optional[Path] = None) -> Path:
    """``write_load.build_installation`` with a chosen length of minute history.
    ``db_from`` copies a database built before (filling 30 days takes minutes)."""
    if history_days == 2.0 and db_from is None:
        return wl.build_installation(target, hosts)
    devices = []
    for i, ((key, name), host) in enumerate(zip(wl.inst.PLUG_NAMES, hosts)):
        d = wl.inst._plug(key, name, i)
        d["host"] = host
        devices.append(d)
    wl.inst._write(target, wl.inst._base_cfg(devices, "en"))
    keys = [k for k, _ in wl.inst.PLUG_NAMES]
    if db_from is not None:
        (target / "data").mkdir(parents=True, exist_ok=True)
        shutil.copyfile(db_from, target / "data" / "energy.db")
    else:
        wl.inst._fill_plugs(target, keys, days=history_days)
    wl._seed_live_cache(target, keys)
    return target / "config.json"


def run(minutes: float, core: Optional[int] = None, profile: Optional[Path] = None,
        keep: bool = False, prestart: bool = True, warmup_s: float = 20.0,
        history_days: float = 2.0, db_from: Optional[Path] = None) -> dict:
    tmp = make_tmp("sea-perf-")
    plugs = wl.SimulatedPlugs()
    keys = [k for k, _ in wl.inst.PLUG_NAMES][: wl.DEVICES]
    proc = None
    log = None
    try:
        cfg_path = build_installation(tmp, plugs.hosts, history_days, db_from)
        env = {**os.environ, **OFFLINE_ENV, "PYTHONPATH": str(ROOT / "src"), "PYTHONUNBUFFERED": "1"}
        if prestart:
            wl._prestart(cfg_path, tmp, env)
        port = free_port()
        cmd = [sys.executable, "-m", "shelly_analyzer", "--config", str(cfg_path),
               "--port", str(port), "--no-ssl", "--host", "127.0.0.1"]
        if profile is not None:
            spy = shutil.which("py-spy") or str(Path(sys.executable).parent / "py-spy")
            cmd = [spy, "record", "--format", "raw", "--rate", "50", "--idle", "-o", str(profile),
                   "--subprocesses", "--"] + cmd
        if core is None and hasattr(os, "sched_getaffinity"):
            core = max(os.sched_getaffinity(0))
        if core is not None and shutil.which("taskset"):
            cmd = ["taskset", "-c", str(core)] + cmd
        if shutil.which("nice"):
            cmd = ["nice", "-n", "19"] + cmd
        log = open(tmp / "server.log", "wb")
        t_start = time.time()
        proc = subprocess.Popen(cmd, env=env, stdout=log, stderr=subprocess.STDOUT, cwd=str(tmp))
        pid = proc.pid
        timeline: List[List[float]] = []   # [s since start, RSS MB, CPU s] until the window
        health_wall = health_cpu = None
        live_wall = live_cpu = None
        deadline = t_start + 300
        while time.time() < deadline:
            if proc.poll() is not None:
                raise RuntimeError("server exited early:\n" +
                                   (tmp / "server.log").read_text(errors="replace")[-2000:])
            pid = _app_pid(proc.pid)
            _st = _status(pid)
            timeline.append([round(time.time() - t_start, 2), round(_st.get("VmRSS", 0) / 1024.0, 1),
                             round(_cpu_s(pid), 2)])
            if health_wall is None:
                if _get(f"http://127.0.0.1:{port}/api/health", timeout=3) is not None:
                    health_wall, health_cpu = time.time() - t_start, _cpu_s(pid)
            elif _all_fresh(_get(f"http://127.0.0.1:{port}/api/state"), keys):
                live_wall, live_cpu = time.time() - t_start, _cpu_s(pid)
                break
            time.sleep(0.25)
        if health_wall is None:
            raise RuntimeError("server did not answer /api/health in 300 s")
        wend = time.time() + warmup_s
        while time.time() < wend:
            _st = _status(pid)
            timeline.append([round(time.time() - t_start, 2), round(_st.get("VmRSS", 0) / 1024.0, 1),
                             round(_cpu_s(pid), 2)])
            time.sleep(0.5)
        cpu0, t0 = _cpu_s(pid), time.time()
        end = t0 + minutes * 60.0
        rss: List[int] = []
        threads: List[int] = []
        state_ms: List[float] = []
        next_sample = t0
        while time.time() < end:
            if proc.poll() is not None:
                raise RuntimeError("server died during the run:\n" +
                                   (tmp / "server.log").read_text(errors="replace")[-2000:])
            q0 = time.time()
            _get(f"http://127.0.0.1:{port}/api/state")
            state_ms.append((time.time() - q0) * 1000.0)
            if time.time() >= next_sample:
                st = _status(pid)
                timeline.append([round(time.time() - t_start, 2), round(st.get("VmRSS", 0) / 1024.0, 1),
                                 round(_cpu_s(pid), 2)])
                if "VmRSS" in st:
                    rss.append(st["VmRSS"])
                if "Threads" in st:
                    threads.append(st["Threads"])
                next_sample += 5.0
            time.sleep(min(1.0, max(0.0, end - time.time())))
        elapsed = time.time() - t0
        cpu = _cpu_s(pid) - cpu0
        st = _status(pid)
        state_ms.sort()
        result = {
            "devices": wl.DEVICES,
            "history_days": history_days,
            "poll_seconds": 1.0,
            "core": core,
            "nice": 19,
            "profiled": profile is not None,
            "minutes": round(elapsed / 60.0, 2),
            "start_health_wall_s": round(health_wall, 2),
            "start_health_cpu_s": round(health_cpu or 0.0, 2),
            "start_live_wall_s": round(live_wall, 2) if live_wall else None,
            "start_live_cpu_s": round(live_cpu, 2) if live_cpu else None,
            "cpu_pct_one_core": round(100.0 * cpu / elapsed, 2) if elapsed else None,
            "cpu_s_window": round(cpu, 2),
            "rss_mb_mean": round(sum(rss) / len(rss) / 1024.0, 1) if rss else None,
            "rss_mb_last": round(rss[-1] / 1024.0, 1) if rss else None,
            "rss_mb_peak": round(st.get("VmHWM", 0) / 1024.0, 1),
            "threads_first": threads[0] if threads else None,
            "threads_max": max(threads) if threads else None,
            "threads_last": threads[-1] if threads else None,
            "api_state_ms_median": round(state_ms[len(state_ms) // 2], 1) if state_ms else None,
            "api_state_ms_p95": round(state_ms[int(len(state_ms) * 0.95)], 1) if state_ms else None,
            "limits": {"cpu_pct": CPU_LIMIT_PCT, "rss_mb": RSS_LIMIT_MB,
                       "start_cpu_s": START_CPU_LIMIT_S, "live_cpu_s": LIVE_CPU_LIMIT_S},
            "workdir": str(tmp),
            "timeline": timeline,
        }
        return result
    finally:
        if proc is not None and proc.poll() is None:
            if profile is not None:
                wl._stop_pid(_app_pid(proc.pid), proc.pid)  # py-spy writes its file when the child ends
            proc.terminate()
            try:
                proc.wait(timeout=60)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=10)
        if log is not None:
            log.close()
        plugs.close()
        if not keep:
            rm_tree(tmp)


def over_limits(r: dict) -> List[str]:
    bad = []
    if r["cpu_pct_one_core"] is not None and r["cpu_pct_one_core"] > CPU_LIMIT_PCT:
        bad.append(f"CPU {r['cpu_pct_one_core']} % > {CPU_LIMIT_PCT} %")
    if r["rss_mb_peak"] > RSS_LIMIT_MB:
        bad.append(f"peak RSS {r['rss_mb_peak']} MB > {RSS_LIMIT_MB} MB")
    if r["start_health_cpu_s"] > START_CPU_LIMIT_S:
        bad.append(f"start-up {r['start_health_cpu_s']} CPU s > {START_CPU_LIMIT_S}")
    if r["start_live_cpu_s"] is None or r["start_live_cpu_s"] > LIVE_CPU_LIMIT_S:
        bad.append(f"live after {r['start_live_cpu_s']} CPU s (limit {LIVE_CPU_LIMIT_S})")
    return bad


def format_result(r: dict) -> str:
    bad = over_limits(r)
    return "\n".join([
        f"{r['devices']} plugs at {r['poll_seconds']:g} s, {r['history_days']:g} days of history, nice 19 on core {r['core']}, "
        f"{r['minutes']:.1f} min measured{' (under py-spy)' if r['profiled'] else ''}",
        f"start-up: /api/health after {r['start_health_wall_s']} s wall / {r['start_health_cpu_s']} s CPU; "
        f"all plugs live after {r['start_live_wall_s']} s wall / {r['start_live_cpu_s']} s CPU",
        f"CPU: {r['cpu_pct_one_core']} % of one core ({r['cpu_s_window']} s)",
        f"memory: RSS mean {r['rss_mb_mean']} MB, last {r['rss_mb_last']} MB, peak {r['rss_mb_peak']} MB",
        f"threads: max {r['threads_max']}, last {r['threads_last']}",
        f"/api/state: median {r['api_state_ms_median']} ms, p95 {r['api_state_ms_p95']} ms",
        "limits: " + ("OK" if not bad else "OVER — " + "; ".join(bad)),
    ])


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--minutes", type=float, default=5.0)
    ap.add_argument("--core", type=int, default=None, help="CPU core to pin to (default: the last one)")
    ap.add_argument("--profile", type=Path, default=None, help="py-spy collapsed stacks to this file")
    ap.add_argument("--json", type=Path, default=None)
    ap.add_argument("--keep", action="store_true")
    ap.add_argument("--history-days", type=float, default=2.0, help="days of minute history per plug")
    ap.add_argument("--db-from", type=Path, default=None, help="copy this energy.db instead of filling one")
    ap.add_argument("--warmup", type=float, default=20.0, help="seconds between live and the window")
    ap.add_argument("--first-start", action="store_true", help="no earlier run of the installation")
    a = ap.parse_args(argv)
    r = run(a.minutes, core=a.core, profile=a.profile, keep=a.keep, prestart=not a.first_start,
            warmup_s=a.warmup, history_days=a.history_days, db_from=a.db_from)
    print(format_result(r))
    if a.json:
        a.json.write_text(json.dumps(r, indent=2))
    return 0 if not over_limits(r) else 1


if __name__ == "__main__":
    sys.exit(main())
