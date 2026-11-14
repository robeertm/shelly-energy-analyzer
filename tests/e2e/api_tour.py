#!/usr/bin/env python3
"""API tour: call every GET route of the app with every installation and parameter variants.

    python tests/e2e/api_tour.py --report out.json [--quick] [--installations a,b]

The route list comes from the Flask url_map (read in-process); the calls go to a real
server started per installation (and per time zone), so background services and the
action dispatcher are wired exactly as in production.  Findings: HTTP >= 500, an /api/
route that answers with something other than JSON, JSON containing NaN / Infinity, Python
tracebacks in a body, and a 200 whose JSON says ``ok: false`` with an exception-looking
error string.  State-changing GET routes (delete / set / clear ...) are not called.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Dict, List

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from common import Server, make_tmp, rm_tree, server_tracebacks  # noqa: E402
from installations import INSTALLATIONS, materialize  # noqa: E402

SKIP_RULE = re.compile(r"delete|clear|reset|/set\b|_set|remove|/files/|/static/|/logs|/job\b")
EXPECT_TEXT = ("/metrics", "/widget.js", "/w", "/widget-manifest.json", "/widget-icon.svg", "/api/widget")
DST_DAYS = ("2026-10-25", "2026-03-29")
EXC_LOOK = re.compile(r"(Error|Exception)\b|object has no attribute|not subscriptable|unsupported operand|"
                      r"invalid literal|KeyError|IndexError|referenced before assignment|is not defined")


def _param_variants(keys: List[str], quick: bool) -> List[Dict[str, str]]:
    k0 = keys[0] if keys else "house"
    k1 = keys[1] if len(keys) > 1 else k0
    v: List[Dict[str, str]] = [{}, {"device": k0}, {"device_key": k0}, {"device": "no-such-device"},
                               {"device_key": "no-such-device"}, {"device": ""},
                               {"range": "24h"}, {"preset": "7d", "device": k0}]
    for d in DST_DAYS:
        nxt = "2026-10-26" if d.endswith("25") else "2026-03-30"
        v.append({"start": d, "end": nxt, "device": k0})
        v.append({"start": d, "end": nxt, "device_key": k0, "mode": "hours"})
        v.append({"date": d})
    if quick:
        return v
    v += [{"range": r} for r in ("1h", "6h", "7d", "30d", "all", "bogus")]
    v += [{"days": d} for d in ("0", "1", "7", "400", "-3", "abc")]
    v += [{"period": p, "device": k0} for p in ("day", "week", "month", "year")]
    v += [{"view": "timeseries", "metric": m, "series": s, "preset": p, "device": k0}
          for m in ("W", "V", "A", "HZ", "VAR", "COSPHI") for s in ("total", "phases") for p in ("1h", "7d")]
    v += [{"view": "kwh", "mode": m, "preset": p, "device": k0} for m in ("hours", "days", "weeks", "months") for p in ("24h", "30d")]
    v += [{"view": "kwh", "mode": "days", "preset": "custom", "start": "2026-03-28", "end": "2026-03-31"},
          {"view": "kwh", "mode": "hours", "preset": "custom", "start": "2026-10-25", "end": "2026-10-26"},
          {"view": "kwh", "mode": "days", "preset": "custom", "start": "2026-01-02", "end": "2026-01-01"}]
    v += [{"start": "01.01.2026", "end": "02.01.2026"}, {"start": "", "end": ""}, {"start": "x", "end": "y"}]
    v += [{"device_a": k0, "device_b": k1, "from_a": "2026-10-01", "to_a": "2026-10-10", "from_b": "2026-10-11", "to_b": "2026-10-20",
           "unit": u, "gran": g} for u in ("kWh", "eur") for g in ("total", "daily", "weekly", "monthly")]
    v += [{"year": "2026", "unit": "kwh", "device": k0}, {"year": "1999", "device": k0}, {"hours": "0"}, {"hours": "48"}]
    v += [{"raw": "1", "device": k0}, {"since": "0"}, {"limit": "0"}, {"limit": "5"}, {"profile": "x"}]
    return v


def _fill_rule(rule: str, keys: List[str]):
    """Yield concrete paths for a rule with converters."""
    k0 = keys[0] if keys else "house"
    cands = {"key": [k0, "no-such-device"], "device_key": [k0, "no-such-device"], "kind": ["daily", "monthly"],
             "subpath": ["", "x"], "filename": ["x"], "rel_path": ["x"]}
    names = re.findall(r"<(?:[a-z]+:)?([a-z_]+)>", rule)
    if not names:
        yield rule
        return
    first = {n: (cands.get(n) or ["x"]) for n in names}
    for i in range(max(len(v) for v in first.values())):
        p = rule
        for n in names:
            vals = first[n]
            p = re.sub(r"<(?:[a-z]+:)?" + n + ">", vals[min(i, len(vals) - 1)], p)
        yield p.replace("//", "/")


def _reject_const(c):
    raise ValueError("JSON constant " + c)


def check_response(path: str, status: int, ctype: str, body: bytes):
    """Return a list of (kind, severity, detail)."""
    out = []
    text = body.decode("utf-8", "replace")
    if status >= 500:
        out.append(("http-5xx", "high", f"{status}: {text[:200]}"))
    if "Traceback (most recent call last)" in text:
        out.append(("traceback-in-body", "high", text[text.index("Traceback"):][:300]))
    is_api = path.startswith("/api/") and not path.endswith(".pdf") and not any(path.startswith(p) for p in EXPECT_TEXT)
    if is_api and status < 500:
        if "json" not in ctype.lower():
            if status < 400 or status == 500:
                out.append(("not-json", "medium", f"content-type '{ctype}': {text[:120]}"))
        else:
            try:
                data = json.loads(text, parse_constant=_reject_const)
            except ValueError as e:
                msg = str(e)
                out.append(("json-nan" if "constant" in msg else "invalid-json", "high" if "constant" in msg else "medium", msg[:200]))
            else:
                if isinstance(data, dict) and data.get("ok") is False:
                    err = str(data.get("error", ""))
                    offline = re.search(r"Max retries exceeded|ProxyError|Connection refused|timed out", err)
                    if EXC_LOOK.search(err) and "no-such-device" not in path and not offline:
                        out.append(("error-in-200", "medium", f"ok:false with exception text: {err[:200]}"))
    return out


def discover_routes() -> List[str]:
    """GET rules from the app factory (in-process, throw-away installation)."""
    tmp = make_tmp("sea-e2e-routes-")
    try:
        materialize("e", tmp)
        from shelly_analyzer.web import create_app
        app = create_app(str(tmp / "config.json"))
        rules = sorted({r.rule for r in app.url_map.iter_rules() if "GET" in r.methods})
        return [r for r in rules if not SKIP_RULE.search(r)]
    finally:
        rm_tree(tmp)


def tour_installation(inst: str, routes: List[str], quick: bool, env_extra: Dict[str, str], tz_label: str, findings: list,
                      stats: dict, log=print):
    label, builder, env = INSTALLATIONS[inst]
    tmp = make_tmp(f"sea-e2e-api-{inst}-")
    try:
        materialize(inst, tmp)
        cfg = json.loads((tmp / "config.json").read_text())
        keys = [d["key"] for d in cfg.get("devices", [])]
        # a renamed / deleted device: the key is still in the data but not in the config any more
        variants = _param_variants(keys, quick)
        with Server(tmp, {**env, **env_extra}) as srv:
            seen = set()
            t0 = time.time()
            for rule in routes:
                is_api = rule.startswith("/api/")
                vs = variants if (is_api and not rule.startswith("/api/v1") and "<" not in rule) else variants[:1]
                for path in _fill_rule(rule, keys):
                    for q in vs:
                        url = path + (("?" + urllib.parse.urlencode(q)) if q else "")
                        try:
                            req = urllib.request.Request(srv.url + url, headers={"Accept-Encoding": "identity"})
                            try:
                                with urllib.request.urlopen(req, timeout=90) as r:
                                    status, ct, body = r.status, r.headers.get("Content-Type", ""), r.read()
                            except urllib.error.HTTPError as e:
                                status, ct, body = e.code, e.headers.get("Content-Type", ""), e.read()
                        except Exception as e:
                            findings.append({"installation": inst, "tz": tz_label, "endpoint": url, "kind": "request-error",
                                             "severity": "high", "detail": f"{type(e).__name__}: {e}"})
                            continue
                        stats["calls"] += 1
                        for kind, sev, detail in check_response(path, status, ct, body):
                            key = (inst, tz_label, rule, kind, re.sub(r"\d+", "#", detail)[:100])
                            if key in seen:
                                continue
                            seen.add(key)
                            findings.append({"installation": inst, "tz": tz_label, "endpoint": url, "route": rule,
                                             "kind": kind, "severity": sev, "detail": detail, "status": status})
            # delete / rename a device on the throw-away installation, then read again
            if not quick and len(keys) > 1 and inst in ("a", "f"):
                _rename_delete(srv, keys, findings, inst, tz_label)
            log(f"[api {inst}/{tz_label}] {stats['calls']} calls so far, {time.time() - t0:.0f}s")
        tb_seen = set()
        for exc, frame in server_tracebacks(srv.log_text()):
            if re.search(r"Connection refused|ProxyError|Max retries|timed out|Network is unreachable", exc):
                continue
            if (exc, frame) in tb_seen:
                continue
            tb_seen.add((exc, frame))
            findings.append({"installation": inst, "tz": tz_label, "endpoint": "(server log)", "kind": "server-traceback",
                             "severity": "high", "detail": f"{exc}  @ {frame}"})
    finally:
        rm_tree(tmp)


def _rename_delete(srv, keys, findings, inst, tz_label):
    """Harmless on the temp installation: rename, then delete a device, then re-read the data routes."""
    def call(method, path, body=None):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(srv.url + path, data=data, method=method, headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                return r.status, r.read()
        except urllib.error.HTTPError as e:
            return e.code, e.read()
        except Exception as e:  # noqa: BLE001
            return 0, str(e).encode()
    victim = keys[-1]
    call("PUT", f"/api/devices/{victim}", {"name": "Renamed device"})
    call("DELETE", f"/api/devices/{victim}")
    for path in ("/api/state", "/api/costs", "/api/standby", "/api/energy_flow", "/api/co2",
                 f"/api/plots_data?view=timeseries&metric=W&preset=24h&device={victim}",
                 f"/api/plots_data?view=kwh&mode=days&preset=7d&device={victim}", "/api/heatmap", "/api/sankey",
                 "/api/tenants", "/api/devices", "/", "/settings"):
        status, body = call("GET", path)
        for kind, sev, detail in check_response(path.split("?")[0], status, "application/json" if path.startswith("/api/") else "text/html", body):
            findings.append({"installation": inst, "tz": tz_label, "endpoint": path + " (after delete)", "kind": kind,
                             "severity": sev, "detail": detail, "status": status})


def run(quick=False, installations=None, log=print) -> dict:
    t0 = time.time()
    routes = discover_routes()
    findings: List[dict] = []
    stats = {"calls": 0}
    insts = installations or list(INSTALLATIONS)
    for inst in insts:
        # every parameter variant on three representative installations, the short list on the rest
        tour_installation(inst, routes, quick or inst not in ("a", "e", "f"), {}, "default", findings, stats, log)
    if not quick:
        for inst, tz in (("b", "America/New_York"), ("f", "Pacific/Auckland")):
            if inst in insts:
                tour_installation(inst, routes, True, {"TZ": tz}, tz, findings, stats, log)
    return {"mode": "quick" if quick else "full", "seconds": round(time.time() - t0, 1), "routes": len(routes),
            "calls": stats["calls"], "findings": findings}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--report", default="api-tour-report.json")
    ap.add_argument("--quick", action="store_true")
    ap.add_argument("--installations", default="")
    a = ap.parse_args(argv)
    rep = run(a.quick, [x for x in a.installations.split(",") if x] or None, log=lambda s: print(s, flush=True))
    Path(a.report).write_text(json.dumps(rep, indent=1), encoding="utf-8")
    print(f"{rep['routes']} routes, {rep['calls']} calls, {len(rep['findings'])} findings, {rep['seconds']} s -> {a.report}")
    for f in rep["findings"]:
        print(f"  [{f['severity']}] {f['kind']} {f['installation']} {f['endpoint'][:90]}: {f['detail'][:120]}")
    return 1 if any(f["severity"] in ("high", "medium") for f in rep["findings"]) else 0


if __name__ == "__main__":
    raise SystemExit(main())
