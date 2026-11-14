"""Running behind a path prefix (Home Assistant ingress, reverse proxies).

Behind Home Assistant ingress the browser URL is e.g.
``http://ha/api/hassio_ingress/<token>/plots`` while the app only ever sees
``/plots`` (the proxy strips the prefix and passes it in ``X-Ingress-Path``).
Every absolute URL in a page (``fetch('/api/..')``, ``<a href="/settings">``,
``location.href='/'``) would therefore hit Home Assistant instead of the app.

One mechanism, used everywhere:

* Client: :func:`base_path_script` emits a tiny inline script at the very top
  of ``<head>`` of every page.  It defines ``window.SEA_BASE`` and
  ``seaUrl(p)``.  All page JS wraps its same-origin URLs in ``seaUrl(...)``.
  Static HTML attributes carry ``data-sea-href`` / ``data-sea-src`` /
  ``data-sea-action`` instead, which the same script rewrites on
  DOMContentLoaded.  Scripts/styles that must load synchronously are written
  by :func:`script_tag` / :func:`stylesheet_tag` (``document.write`` through
  ``seaUrl``).
* Server: :class:`IngressPathMiddleware` turns ``X-Ingress-Path`` into
  ``SCRIPT_NAME`` so Flask's own redirects and ``url_for`` carry the prefix;
  hard-coded redirects use :func:`prefixed`.
"""
from __future__ import annotations

import json
import re
from typing import Any, Callable, Iterable

# A prefix we are willing to trust from the header: one or more path
# segments, no scheme/host, no "//" or ".." (no open redirect, no traversal).
_PREFIX_RE = re.compile(r"^/(?:[A-Za-z0-9._~%!$&'()*+,;=:@-]+/?)+$")

_JS = (
    "(function(){var P=location.pathname,R=%(route)s,B='',"
    "m=P.match(/^(.*\\/api\\/hassio_ingress\\/[^\\/]+)/);"
    "if(m){B=m[1];}else{"
    "if(R==='/'){B=P.replace(/\\/index\\.html$/,'').replace(/\\/+$/,'');}"
    "else{var i=0,k;while((k=P.indexOf(R,i))>=0){"
    "var e=k+R.length;if(e===P.length||P.charAt(e)==='/'){B=P.slice(0,k);break;}"
    "i=k+1;}}}"
    "window.SEA_BASE=B;"
    "window.seaUrl=seaUrl;"
    "function seaUrl(p){return(typeof p==='string'&&p.charAt(0)==='/'"
    "&&p.charAt(1)!=='/')?B+p:p;}"
    "function fix(){var a=['href','src','action'],j,n,l,x;"
    "for(j=0;j<3;j++){l=document.querySelectorAll('[data-sea-'+a[j]+']');"
    "for(n=0;n<l.length;n++){x=l[n];x.setAttribute(a[j],"
    "seaUrl(x.getAttribute('data-sea-'+a[j])));"
    "x.removeAttribute('data-sea-'+a[j]);}}}"
    "if(document.readyState==='loading')"
    "document.addEventListener('DOMContentLoaded',fix);else fix();"
    "})();"
)


def base_path_script(route: str) -> str:
    """Inline ``<script>`` defining ``window.SEA_BASE`` and ``seaUrl()``.

    *route* is the page's own route (``'/plots'``, ``'/'``, ``'/settings'``)
    used to derive the prefix when it is not a Home Assistant ingress path.
    """
    r = "/" + str(route or "/").strip("/")
    if r == "/index.html":
        r = "/"
    return "<script>" + _JS % {"route": json.dumps(r)} + "</script>"


def inject_base_path(html: str, route: str) -> str:
    """Put :func:`base_path_script` first in ``<head>`` (idempotent)."""
    if "window.SEA_BASE" in html:
        return html
    tag = base_path_script(route)
    m = re.search(r"<head[^>]*>", html, re.I)
    if m:
        return html[: m.end()] + tag + html[m.end():]
    m = re.search(r"<html[^>]*>", html, re.I)
    if m:
        return html[: m.end()] + tag + html[m.end():]
    return tag + html


def script_tag(path: str, defer: bool = False, onerror: str = "") -> str:
    """A ``<script src>`` that resolves *path* through ``seaUrl`` at parse time.

    Written with ``document.write`` so the script still loads synchronously
    (or deferred, like the attribute) and in document order.  *onerror* is
    JavaScript for the element's ``onerror``; it must not contain ``"``.
    """
    attrs = (" defer" if defer else "") + (
        ' onerror="%s"' % onerror.replace("'", "\\'") if onerror else "")
    return (
        "<script>document.write('<script%s src=\"'+seaUrl(%s)+'\"><\\/script>');</script>"
        % (attrs, json.dumps(path))
    )


def stylesheet_tag(path: str) -> str:
    """A ``<link rel=stylesheet>`` that resolves *path* through ``seaUrl``."""
    return (
        "<script>document.write('<link rel=\"stylesheet\" href=\"'+seaUrl(%s)+'\">');</script>"
        % json.dumps(path)
    )


def ingress_prefix(value: Any) -> str:
    """Normalise an ``X-Ingress-Path`` value; ``''`` when it is not a safe prefix."""
    v = str(value or "").strip()
    if not v or "//" in v or ".." in v or not _PREFIX_RE.match(v):
        return ""
    return v.rstrip("/")


class IngressPathMiddleware:
    """WSGI middleware: ``X-Ingress-Path`` becomes ``SCRIPT_NAME``."""

    def __init__(self, app: Callable[..., Iterable[bytes]]):
        self.app = app

    def __call__(self, environ, start_response):
        prefix = ingress_prefix(environ.get("HTTP_X_INGRESS_PATH"))
        if prefix:
            environ["SCRIPT_NAME"] = prefix
            path = environ.get("PATH_INFO", "")
            if path.startswith(prefix + "/") or path == prefix:
                # Defensive: a proxy that did not strip the prefix.
                environ["PATH_INFO"] = path[len(prefix):] or "/"
        return self.app(environ, start_response)


def prefixed(path: str) -> str:
    """*path* with the current request's script root (for server redirects)."""
    try:
        from flask import request

        root = request.script_root or ""
    except Exception:
        root = ""
    return root + path
