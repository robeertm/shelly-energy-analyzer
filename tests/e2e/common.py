"""Shared helpers: start/stop the app server, switch its language, locate browsers."""
from __future__ import annotations

import json
import os
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path
from typing import Dict, Optional

ROOT = Path(__file__).resolve().parents[2]
LANGS = ("de", "en", "es", "fr", "pt", "it", "pl", "cs", "ru")

# Some tests and the walkthrough share one server startup contract:
# a dead proxy makes every outbound call of the app (weather, spot prices, URDB, update
# check) fail fast instead of reaching the internet.
OFFLINE_ENV = {
    "HTTP_PROXY": "http://127.0.0.1:9", "HTTPS_PROXY": "http://127.0.0.1:9",
    "http_proxy": "http://127.0.0.1:9", "https_proxy": "http://127.0.0.1:9",
    "NO_PROXY": "127.0.0.1,localhost", "no_proxy": "127.0.0.1,localhost",
}


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class Server:
    """The app in a subprocess; always use as a context manager so it is terminated."""

    def __init__(self, cfg_dir: Path, env: Optional[Dict[str, str]] = None):
        self.dir = Path(cfg_dir)
        self.env = dict(env or {})
        self.port = free_port()
        self.proc: Optional[subprocess.Popen] = None
        self.log_path = self.dir / "server.log"
        self._log = None

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def __enter__(self) -> "Server":
        env = {**os.environ, **OFFLINE_ENV, **self.env, "PYTHONPATH": str(ROOT / "src"), "PYTHONUNBUFFERED": "1"}
        self._log = open(self.log_path, "wb")
        try:
            self.proc = subprocess.Popen(
                [sys.executable, "-m", "shelly_analyzer", "--config", str(self.dir / "config.json"),
                 "--port", str(self.port), "--no-ssl", "--host", "127.0.0.1"],
                env=env, stdout=self._log, stderr=subprocess.STDOUT, cwd=str(self.dir))
            deadline = time.time() + 120
            while time.time() < deadline:
                if self.proc.poll() is not None:
                    raise RuntimeError("server exited early:\n" + self.log_text()[-2000:])
                try:
                    urllib.request.urlopen(self.url + "/api/health", timeout=3).read()
                    return self
                except Exception:
                    time.sleep(0.5)
            raise RuntimeError("server did not come up in 120 s")
        except BaseException:
            self.__exit__(None, None, None)
            raise

    def __exit__(self, *exc) -> None:
        p = self.proc
        if p is not None and p.poll() is None:
            p.terminate()
            try:
                p.wait(timeout=15)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait(timeout=10)
        if self._log is not None:
            self._log.close()

    def log_text(self) -> str:
        try:
            return self.log_path.read_text(errors="replace")
        except OSError:
            return ""

    def request(self, method: str, path: str, body=None, timeout: int = 60):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(self.url + path, data=data, method=method, headers={"Content-Type": "application/json"})
        return urllib.request.urlopen(req, timeout=timeout).read().decode("utf-8", "replace")

    def set_language(self, lang: str) -> None:
        """Switch the UI language the way the settings page does (PUT /api/settings)."""
        self.request("PUT", "/api/settings", {"ui": {"language": lang}})
        got = json.loads(self.request("GET", "/api/i18n")).get("lang")
        if got != lang:
            raise RuntimeError(f"language did not switch: wanted {lang}, server says {got}")

    def probe_dashboard_language_switch(self, lang: str = "fr"):
        """The dashboard's own language dropdown posts /api/run set_language.  Returns a problem text or None."""
        self.request("POST", "/api/run", {"action": "set_language", "params": {"language": lang}})
        got = json.loads(self.request("GET", "/api/i18n")).get("lang")
        html = self.request("GET", "/")
        m = re.search(r"<html[^>]*lang=\"([a-z]+)\"", html)
        if got != lang or (m and m.group(1) != lang):
            return (f"POST /api/run set_language={lang} succeeds but the server keeps lang='{got}' "
                    f"(<html lang='{m.group(1) if m else '?'}'>); the dashboard dropdown has no effect until restart")
        return None


def server_tracebacks(log: str):
    """Yield (exception line, last frame) for every traceback in a server log."""
    lines = log.splitlines()
    i = 0
    while i < len(lines):
        if lines[i].startswith("Traceback (most recent call last)"):
            j = i + 1
            frames = []
            while j < len(lines) and (lines[j].startswith(" ") or lines[j].startswith("\t")):
                if lines[j].strip().startswith("File ") and "shelly_analyzer" in lines[j]:
                    frames.append(lines[j].strip().replace(str(ROOT), "").replace("File ", "", 1))
                j += 1
            exc = lines[j].strip() if j < len(lines) else ""
            yield exc, " <- ".join(reversed(frames[-3:]))
            i = j
        i += 1


def make_tmp(prefix: str = "sea-e2e-") -> Path:
    return Path(tempfile.mkdtemp(prefix=prefix))


def rm_tree(p: Path) -> None:
    shutil.rmtree(p, ignore_errors=True)


def launch_browser(pw, name: str):
    """Return a launched browser or raise; chromium uses SEA_CHROMIUM / system chromium when present."""
    if name == "chromium":
        exe = os.environ.get("SEA_CHROMIUM") or shutil.which("chromium") or shutil.which("chromium-browser")
        args = ["--no-sandbox", "--disable-dev-shm-usage"]
        if exe:
            return pw.chromium.launch(executable_path=exe, args=args)
        return pw.chromium.launch(args=args)
    if name == "webkit":
        return pw.webkit.launch()
    raise ValueError(name)


def browser_available(name: str) -> Optional[str]:
    """None when the browser starts, else the reason it does not."""
    try:
        from playwright.sync_api import sync_playwright
    except Exception as e:  # pragma: no cover
        return f"playwright missing: {e}"
    try:
        with sync_playwright() as pw:
            b = launch_browser(pw, name)
            b.close()
        return None
    except Exception as e:
        return str(e).splitlines()[0][:200]
