# -*- coding: utf-8 -*-
"""The app must work behind a path prefix (Home Assistant ingress).

Behind ``/api/hassio_ingress/<token>/`` an absolute URL such as
``fetch('/api/state')`` or ``<a href="/settings">`` goes to Home Assistant
and 404s.  Every same-origin URL in page code therefore goes through
``seaUrl()`` (see web/base_path.py).  This file

* scans the page sources for absolute same-origin URL literals that are not
  wrapped (the literal guard),
* runs the client script in node and checks the prefix it derives,
* drives the real Flask app with an ``X-Ingress-Path`` header.
"""
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys

import pytest

ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, os.path.join(ROOT, "src"))

from shelly_analyzer.web import base_path  # noqa: E402

PKG = os.path.join(ROOT, "src", "shelly_analyzer")
SOURCES = [
    "services/webdash.py",
    "web/templates/settings.html",
    "web/templates/setup.html",
    "web/templates/widget_page.html",
    "web/static/aurora.js",
    "web/static/units.js",
]

# Paths of the app itself.  A string literal starting with one of these is a
# same-origin URL unless it sits in the allow-list below.
APP_PATH = r"/(?:api|static|files|plots|control|settings|setup|widget(?:\.js|-icon\.svg|-manifest\.json)?|w|metrics|login|index\.html)"
LITERAL = re.compile(r"""(?<!seaUrl\()(?<!seaUrl\(\\)(?P<q>['"`])""" + APP_PATH + r"""(?=[/?#.'"`])""")
# `href="/"`, `src="/x"`, `action="/x"` written as HTML (also inside JS strings)
HTML_ATTR = re.compile(r"""\b(?:href|src|action)=\\?["']/(?!/)""")
# `.href = '/'`, `location.href = '/'`, `setAttribute('href', '/')`, `fetch('/')`
SINK_ROOT = re.compile(
    r"""(?:\.(?:href|src)\s*=\s*|setAttribute\(\s*['"](?:href|src)['"]\s*,\s*|"""
    r"""\b(?:fetch|api|_insightFetch|fetchJsonWithTimeout|open|EventSource)\(\s*)['"`]/(?!/)"""
)

DATA_SEA = re.compile(r"""data-sea-(?:href|src|action)=["'][^"']*["']""")

# Lines that are legitimately not same-origin page URLs.  (file suffix, snippet, reason)
ALLOW = [
    ("services/webdash.py", "path_only", "legacy HTTP server routing on the server side, not a page URL"),
    ("services/webdash.py", "parsed.path[len(", "legacy HTTP server routing on the server side"),
    ("services/webdash.py", "/* ", "CSS/JS comment"),
    ("services/webdash.py", "// ", "JS comment"),
    ("services/webdash.py", '").encode("utf-8")', "route argument of _with_base_path, not a URL"),
    ("web/templates/settings.html", "/etc/letsencrypt", "a filesystem path shown to the user, not a URL"),
]

# The generated widget.js (Scriptable script, runs on the phone) builds absolute
# URLs from the host the user typed; it never runs inside the page, so it
# ends before the first page template.
WIDGET_JS_END = "_HTML_TEMPLATE = "


def scan(name: str, text: str) -> list[str]:
    """Return ``file:line: text`` for every unwrapped same-origin URL literal."""
    lines = text.split("\n")
    if name.endswith("webdash.py"):
        for i, ln in enumerate(lines):
            if ln.startswith(WIDGET_JS_END):
                lines = [""] * i + lines[i:]
                break
    found = []
    for i, ln in enumerate(lines, 1):
        # attributes written for the DOMContentLoaded pass are fine
        ln = DATA_SEA.sub("", ln)
        if not (LITERAL.search(ln) or HTML_ATTR.search(ln) or SINK_ROOT.search(ln)):
            continue
        if any(name.endswith(f) and s in ln for f, s, _ in ALLOW):
            continue
        if ln.lstrip().startswith(("#", "@bp.route", "@app.route")):
            continue
        found.append("%s:%d: %s" % (name, i, ln.strip()[:140]))
    return found


def _read(rel: str) -> str:
    with open(os.path.join(PKG, rel), encoding="utf-8") as fh:
        return fh.read()


def test_no_unwrapped_absolute_urls_in_pages():
    bad: list[str] = []
    for rel in SOURCES:
        bad += scan(rel, _read(rel))
    assert not bad, "absolute same-origin URL without seaUrl():\n" + "\n".join(bad)


def test_literal_guard_catches_old_code():
    """The guard must fire on the shapes that broke the ingress setup."""
    old = "\n".join([
        "const r = await fetch('/api/state');",
        "window.location.href = '/settings#sec-updates';",
        '<a href="/settings">x</a>',
        "fr.src = '/plots?raw=1';",
        '<script defer src="/static/plotly.min.js"></script>',
        "window.open('/widget.js','_blank')",
        "nl.setAttribute('href', '/' + suf)",
        "new EventSource(`/api/stream?x=${y}`)",
    ])
    assert len(scan("x.html", old)) == 8
    new = "\n".join([
        "const r = await fetch(seaUrl('/api/state'));",
        "window.location.href = seaUrl('/settings#sec-updates');",
        '<a data-sea-href="/settings">x</a>',
    ])
    assert scan("x.html", new) == []


def _git_head(rel: str):
    if not shutil.which("git"):
        return None
    try:
        out = subprocess.run(
            ["git", "show", "HEAD:src/shelly_analyzer/" + rel],
            cwd=ROOT, capture_output=True, timeout=20)
    except Exception:
        return None
    return out.stdout.decode("utf-8") if out.returncode == 0 else None


def test_literal_guard_fails_on_pre_ingress_sources():
    """Run the scan over the sources as committed before the ingress work
    (skipped once HEAD itself carries seaUrl)."""
    head = {rel: _git_head(rel) for rel in SOURCES[:4]}
    if any(v is None for v in head.values()):
        pytest.skip("git history not available")
    if any("seaUrl(" in v for v in head.values()):
        pytest.skip("HEAD already carries the ingress fix")
    found = []
    for rel, txt in head.items():
        found += scan(rel, txt)
    assert len(found) > 100, len(found)


# ── the client script ─────────────────────────────────────────────────────

NODE = shutil.which("node")


def _sea_base(route: str, pathname: str) -> dict:
    js = (
        "global.location={pathname:%s};global.document={readyState:'complete',"
        "querySelectorAll:function(){return[]},addEventListener:function(){}};global.window=global;"
        % json.dumps(pathname)
        + re.sub(r"^<script>|</script>$", "", base_path.base_path_script(route))
        + ";console.log(JSON.stringify({b:SEA_BASE,a:seaUrl('/api/x'),c:seaUrl('//h/x'),"
        "d:seaUrl('http://h/x'),e:seaUrl('rel'),f:String(seaUrl(5)),g:seaUrl('/')}))"
    )
    out = subprocess.run([NODE, "-e", js], capture_output=True, text=True, timeout=30)
    assert out.returncode == 0, out.stderr
    return json.loads(out.stdout)


@pytest.mark.skipif(not NODE, reason="node not available")
@pytest.mark.parametrize("route,pathname,expected", [
    ("/plots", "/api/hassio_ingress/tok/plots", "/api/hassio_ingress/tok"),
    ("/plots", "/api/hassio_ingress/tok/plots/sub", "/api/hassio_ingress/tok"),
    ("/", "/api/hassio_ingress/tok/", "/api/hassio_ingress/tok"),
    ("/", "/api/hassio_ingress/tok", "/api/hassio_ingress/tok"),
    ("/plots", "/plots", ""),
    ("/plots", "/plots/", ""),
    ("/plots", "/plots/x/y", ""),
    ("/", "/", ""),
    ("/", "/index.html", ""),
    ("/settings", "/foo/settings", "/foo"),
    ("/settings", "/foo/bar/settings", "/foo/bar"),
    ("/", "/foo/", "/foo"),
    ("/", "/foo/index.html", "/foo"),
    ("/control", "/foo/control/x", "/foo"),
    ("/plots", "/foo/plotsx/plots", "/foo/plotsx"),
])
def test_client_sea_base(route, pathname, expected):
    r = _sea_base(route, pathname)
    assert r["b"] == expected
    assert r["a"] == expected + "/api/x"
    assert r["g"] == expected + "/"
    # full URLs, protocol-relative, relative and non-strings stay untouched
    assert (r["c"], r["d"], r["e"], r["f"]) == ("//h/x", "http://h/x", "rel", "5")


def test_base_script_is_first_in_head_and_idempotent():
    html = "<!doctype html><html><head><meta charset=utf-8><title>x</title></head><body></body></html>"
    out = base_path.inject_base_path(html, "/plots")
    assert out.index("window.SEA_BASE") < out.index("<meta")
    assert base_path.inject_base_path(out, "/plots") == out


def test_ingress_prefix_validation():
    assert base_path.ingress_prefix("/api/hassio_ingress/abc123token") == "/api/hassio_ingress/abc123token"
    assert base_path.ingress_prefix("/api/hassio_ingress/abc/") == "/api/hassio_ingress/abc"
    for bad in ("", "//evil.example", "http://evil.example", "/a/../b", "evil", "/a b", "/x//y", None):
        assert base_path.ingress_prefix(bad) == ""


# ── the server side ───────────────────────────────────────────────────────

@pytest.fixture()
def flask_app(tmp_path):
    from shelly_analyzer.web import create_app

    # no config file yet: first run writes a minimal one, without devices
    return create_app(str(tmp_path / "config.json"))


def test_first_run_redirect_carries_ingress_prefix(flask_app):
    c = flask_app.test_client()
    plain = c.get("/")
    assert plain.status_code == 302
    assert plain.headers["Location"].endswith("/setup")
    assert not plain.headers["Location"].startswith("/api/hassio_ingress")

    pre = "/api/hassio_ingress/abc123token"
    r = c.get("/", headers={"X-Ingress-Path": pre})
    assert r.status_code == 302
    assert r.headers["Location"].endswith(pre + "/setup")


def test_pages_carry_base_script_and_no_absolute_page_urls(flask_app):
    c = flask_app.test_client()
    for path in ("/plots", "/control", "/setup", "/settings", "/w"):
        r = c.get(path, headers={"X-Ingress-Path": "/api/hassio_ingress/abc123token"})
        assert r.status_code == 200, path
        body = r.get_data(as_text=True)
        assert "window.SEA_BASE" in body, path
        assert body.index("window.SEA_BASE") < body.index("<style") if "<style" in body else True
    # the manifest points at the prefixed start page and icon
    m = c.get("/widget-manifest.json", headers={"X-Ingress-Path": "/api/hassio_ingress/t"}).get_json()
    assert m["start_url"] == "/api/hassio_ingress/t/w"
    assert m["icons"][0]["src"] == "/api/hassio_ingress/t/widget-icon.svg"


def test_login_redirect_and_next_are_prefixed(tmp_path):
    from shelly_analyzer.web import create_app

    cfg = tmp_path / "config.json"
    cfg.write_text(json.dumps({"devices": [], "ui": {"live_web_token": "s3cret"}}), encoding="utf-8")
    c = create_app(str(cfg)).test_client()
    pre = "/api/hassio_ingress/abc"
    r = c.get("/plots", headers={"X-Ingress-Path": pre})
    assert r.status_code == 302
    assert r.headers["Location"].endswith(pre + "/login?next=/plots")
    r = c.post("/login?next=/plots", data={"token": "s3cret"}, headers={"X-Ingress-Path": pre})
    assert r.status_code == 302
    assert r.headers["Location"].endswith(pre + "/plots")
    # an off-site "next" never leaves the app
    r = c.post("/login?next=//evil.example/x", data={"token": "s3cret"}, headers={"X-Ingress-Path": pre})
    assert r.headers["Location"].endswith(pre + "/")
    page = c.get("/login", headers={"X-Ingress-Path": pre}).get_data(as_text=True)
    assert "window.SEA_BASE" in page and "data-sea-href" in page


def test_header_ignored_when_not_a_safe_path(flask_app):
    c = flask_app.test_client()
    r = c.get("/", headers={"X-Ingress-Path": "//evil.example"})
    assert r.status_code == 302
    assert "evil.example" not in r.headers["Location"]
