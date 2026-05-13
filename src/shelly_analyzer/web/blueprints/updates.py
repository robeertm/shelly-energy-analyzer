"""Updates API: check GitHub releases + install/rollback to any of the last 10 versions."""
from __future__ import annotations

import logging
import os
import subprocess
import sys
import tempfile
import threading
import urllib.request
import zipfile
from dataclasses import asdict
from pathlib import Path

from flask import Blueprint, current_app, jsonify, request

from shelly_analyzer import __version__
from shelly_analyzer.services.updater import (
    check_latest_release,
    fetch_releases,
    is_newer,
    parse_version,
)

logger = logging.getLogger(__name__)

bp = Blueprint("updates", __name__)

# In-memory TTL cache for /api/updates/releases to avoid hitting GitHub
# on every Settings → Updates page load (rate limit 60/h unauthenticated).
_releases_cache: dict = {"ts": 0.0, "limit": 0, "data": None}
_RELEASES_CACHE_TTL_S = 600  # 10 minutes


def _get_state():
    return current_app.extensions["state"]


def _repo() -> str:
    state = _get_state()
    return str(getattr(getattr(state.cfg, "updates", None), "repo", "") or "").strip()


@bp.route("/api/updates/cached", methods=["GET"])
def api_updates_cached():
    """Return the cached update-check result written by the background
    UpdateChecker thread. Lightweight (no network call) — used by the Live
    tab to poll for a new-version banner without hitting GitHub on every
    page load."""
    state = _get_state()
    bg = getattr(state, "_bg", None)
    cached = getattr(bg, "_update_check_state", None) if bg is not None else None
    if cached is None:
        return jsonify({
            "ok": True,
            "checked": False,
            "current": __version__,
            "has_update": False,
        })
    out = dict(cached)
    out["checked"] = True
    return jsonify(out)


@bp.route("/api/updates/status", methods=["GET"])
def api_updates_status():
    """Return current version + latest release info.

    Prefers the cached result from the background update checker to avoid
    hitting GitHub on every call (rate limit: 60/h unauthenticated).
    Only falls back to a live fetch if the cache is missing AND the caller
    explicitly passes ?force=1.
    """
    import time
    state = _get_state()
    bg = getattr(state, "_bg", None)
    cached = getattr(bg, "_update_check_state", None) if bg is not None else None
    force = request.args.get("force") == "1"

    if cached and cached.get("ok") and not force:
        age = int(time.time()) - int(cached.get("checked_at", 0) or 0)
        out = dict(cached)
        out["cache_age_seconds"] = age
        return jsonify(out)

    repo = _repo()
    if not repo:
        return jsonify({"ok": False, "error": "updates.repo not configured",
                        "current": __version__})
    try:
        info = check_latest_release(repo)
        result = {
            "ok": True,
            "current": __version__,
            "repo": repo,
            "reachable": info.reachable,
            "status": info.status,
            "latest_tag": info.latest_tag,
            "has_update": bool(info.latest_tag and is_newer(info.latest_tag, __version__)),
            "asset_url": info.asset_url,
            "asset_name": info.asset_name,
        }
        # Promote a force-refresh into the background cache too, so the next
        # caller (without force=1) sees the fresh result instead of the stale
        # 30-min cache that a just-released version would otherwise be hidden by.
        if force and bg is not None and info.reachable:
            bg._update_check_state = {**result, "checked_at": int(time.time()),
                                      "rate_limited": False}
        return jsonify(result)
    except Exception as e:
        logger.exception("updates status failed")
        return jsonify({"ok": False, "error": str(e), "current": __version__})


@bp.route("/api/updates/releases", methods=["GET"])
def api_updates_releases():
    """Return the last N GitHub releases (default 10) for rollback/install selection.

    Uses a 10-minute in-memory TTL cache to avoid hitting GitHub on every
    Settings → Updates page load. Pass ?force=1 to bypass the cache.
    """
    import time
    repo = _repo()
    if not repo:
        return jsonify({"ok": False, "error": "updates.repo not configured",
                        "current": __version__, "releases": []})
    try:
        limit = int(request.args.get("limit", "10"))
    except Exception:
        limit = 10
    limit = max(1, min(limit, 30))
    force = request.args.get("force") == "1"

    now = time.time()
    cached = _releases_cache.get("data")
    if (not force) and cached is not None and _releases_cache.get("limit", 0) >= limit:
        age = now - _releases_cache.get("ts", 0)
        if age < _RELEASES_CACHE_TTL_S:
            out = [r for r in cached[:limit]]
            return jsonify({"ok": True, "current": __version__, "repo": repo,
                            "releases": out, "cache_age_seconds": int(age)})

    try:
        releases = fetch_releases(repo, limit=limit)
        out = []
        for r in releases:
            tag = r.tag or ""
            out.append({
                "tag": tag,
                "asset_url": r.asset_url,
                "asset_name": r.asset_name,
                "is_current": tag.lstrip("v") == __version__.lstrip("v"),
                "is_newer": bool(tag and is_newer(tag, __version__)),
                "is_older": bool(tag and is_newer(__version__, tag)),
            })
        _releases_cache["ts"] = now
        _releases_cache["limit"] = limit
        _releases_cache["data"] = out
        return jsonify({"ok": True, "current": __version__, "repo": repo,
                        "releases": out, "cache_age_seconds": 0})
    except Exception as e:
        logger.exception("updates releases failed")
        # Serve stale cache on failure rather than empty list
        if cached is not None:
            return jsonify({"ok": True, "current": __version__, "repo": repo,
                            "releases": cached[:limit], "stale": True,
                            "error": str(e)})
        return jsonify({"ok": False, "error": str(e), "current": __version__, "releases": []})


def _download_to_temp(url: str) -> Path:
    req = urllib.request.Request(url, headers={"User-Agent": "shelly-energy-analyzer-updater"})
    tmp_zip = Path(tempfile.mkstemp(suffix=".zip")[1])
    with urllib.request.urlopen(req, timeout=60) as resp, open(tmp_zip, "wb") as f:
        while True:
            chunk = resp.read(65536)
            if not chunk:
                break
            f.write(chunk)
    return tmp_zip


def _unzip_to_staging(zip_path: Path) -> Path:
    staging = Path(tempfile.mkdtemp(prefix="sea_update_"))
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(staging)
    # If the zip has a single top-level folder, descend into it.
    entries = [p for p in staging.iterdir() if not p.name.startswith(".")]
    if len(entries) == 1 and entries[0].is_dir():
        return entries[0]
    return staging


def _app_dir() -> Path:
    """Runtime install root (parent of the running package)."""
    env = os.environ.get("SEA_APP_DIR", "").strip()
    if env:
        return Path(env).resolve()
    # src/shelly_analyzer/web/blueprints/updates.py → app root = 4 parents up from this file
    return Path(__file__).resolve().parents[4]


def _restart_script(app_dir: Path) -> Path:
    if os.name == "nt":
        for name in ("start.bat", "start.cmd"):
            p = app_dir / name
            if p.exists():
                return p
        return app_dir / "start.bat"
    for name in ("start.command", "start.sh"):
        p = app_dir / name
        if p.exists():
            return p
    return app_dir / "start.command"


@bp.route("/api/updates/install", methods=["POST"])
def api_updates_install():
    """Install a specific release tag. Body: {tag: 'v16.13.51'}. Spawns updater_helper
    in a detached process, then the current app exits so the helper can replace files
    and restart via start.{command,sh,bat}."""
    body = request.get_json(silent=True) or {}
    tag = str(body.get("tag", "")).strip()
    asset_url = str(body.get("asset_url", "")).strip() or None
    if not tag:
        return jsonify({"ok": False, "error": "tag is required"}), 400

    repo = _repo()
    if not repo:
        return jsonify({"ok": False, "error": "updates.repo not configured"}), 400

    # Resolve asset_url from /releases if caller didn't supply one
    if not asset_url:
        try:
            releases = fetch_releases(repo, limit=30)
        except Exception as e:
            return jsonify({"ok": False, "error": f"fetch releases failed: {e}"}), 502
        match = next((r for r in releases if r.tag.lstrip("v") == tag.lstrip("v")), None)
        if not match or not match.asset_url:
            return jsonify({"ok": False,
                            "error": f"no downloadable asset found for tag {tag}"}), 404
        asset_url = match.asset_url

    try:
        logger.info("[updates] downloading %s from %s", tag, asset_url)
        zip_path = _download_to_temp(asset_url)
        logger.info("[updates] extracting %s", zip_path)
        staging = _unzip_to_staging(zip_path)
    except Exception as e:
        logger.exception("download/extract failed")
        return jsonify({"ok": False, "error": f"download/extract failed: {e}"}), 502

    app_dir = _app_dir()
    restart = _restart_script(app_dir)
    # Use the helper that lives alongside the running package to avoid depending
    # on the staged copy which would be overwritten mid-operation.
    helper = Path(__file__).resolve().parents[2] / "updater_helper.py"
    if not helper.exists():
        return jsonify({"ok": False, "error": f"updater_helper.py not found at {helper}"}), 500

    # Common helper arguments (the --wait-pid value differs per platform below).
    helper_args = [
        "--app-dir", str(app_dir),
        "--staging-dir", str(staging),
        "--restart", str(restart),
        "--update-deps", "1",
    ]
    logger.info("[updates] preparing updater handoff (app_dir=%s, staging=%s)", app_dir, staging)

    def _handoff_to_updater():
        """Hand control to updater_helper.

        POSIX (Linux/macOS): ``os.execv`` replaces the current process image
        in-place with the helper, **keeping the same PID**. Under systemd this
        means the service-MainPID never exits, so ``KillMode=mixed`` doesn't
        tear down the cgroup and the helper survives to finish the job. Under
        launchd the same logic applies (job stays ``alive``). Under Docker
        (PID 1) the container keeps running throughout.

        Windows: keep the historical detached-spawn + exit path — Windows has
        no cgroup-style cleanup on SCM/NSSM service exit that would reach the
        child, and ``os.execv`` on Windows doesn't replace in-place (it spawns
        + terminates the current process), so detached spawn is strictly
        safer there.
        """
        import time as _time
        # Give Flask/werkzeug a beat to finish flushing the HTTP response
        # before we yank the process away.
        _time.sleep(1.2)

        # Snapshot the live-history buffer to disk so the Live tab comes
        # back fully populated after the helper execvs the new app image.
        # execv doesn't run atexit / stop_all, so without this the user
        # would see an empty chart for ~2 h after every update.
        try:
            state = _get_state()
            bg = getattr(state, "_bg", None)
            if bg is not None:
                bg._save_live_history()
        except Exception:
            logger.debug("Pre-update live-history save failed", exc_info=True)

        if os.name == "nt":
            win_cmd = [sys.executable, str(helper), "--wait-pid", str(os.getpid())] + helper_args
            try:
                subprocess.Popen(
                    win_cmd,
                    cwd=str(app_dir),
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    creationflags=subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP,
                    close_fds=False,
                )
            except Exception:
                logger.exception("[updates] failed to spawn Windows updater")
                return
            logger.info("[updates] (Windows) exiting so detached helper can take over")
            os._exit(0)

        # POSIX: execv in-place. --wait-pid=0 because we ARE the process the
        # helper would otherwise be waiting for — we're about to become it.
        posix_cmd = [sys.executable, str(helper), "--wait-pid", "0"] + helper_args
        logger.info("[updates] execv into updater_helper (in-place, same PID %d)", os.getpid())
        # Close any fds above std* to avoid leaking the listening HTTP socket
        # / sqlite file handles into the helper image. Python-created sockets
        # already carry FD_CLOEXEC on Linux, but be explicit.
        try:
            os.closerange(3, 256)
        except Exception:
            pass
        try:
            os.execv(sys.executable, posix_cmd)
        except Exception:
            logger.exception("[updates] execv failed; falling back to detached spawn")
            # Last-resort fallback: detached spawn + exit. On systemd this may
            # still die with the cgroup, but it's better than hanging the UI.
            try:
                subprocess.Popen(
                    [sys.executable, str(helper), "--wait-pid", str(os.getpid())] + helper_args,
                    cwd=str(app_dir),
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    start_new_session=True,
                    close_fds=True,
                )
            except Exception:
                logger.exception("[updates] fallback spawn also failed")
            os._exit(1)

    threading.Thread(target=_handoff_to_updater, daemon=True).start()
    return jsonify({
        "ok": True,
        "tag": tag,
        "message": f"Installing {tag}… the app will restart automatically.",
        "staging": str(staging),
        "app_dir": str(app_dir),
    })
