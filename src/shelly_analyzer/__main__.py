from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import tempfile
from pathlib import Path


def _parse_version_parts(v: str) -> list[int]:
    """Extract dotted numeric parts from a version string. Missing = [0]."""
    parts = [int(x) for x in re.findall(r"\d+", v)]
    return parts or [0]


def _is_strictly_newer(staged: str, current: str) -> bool:
    """Return True iff ``staged`` is strictly newer than ``current`` by
    dotted-numeric comparison. Tolerant of 'v' prefixes and suffixes like
    '-dev'/'rc1' (non-numeric tail is ignored)."""
    try:
        return _parse_version_parts(staged) > _parse_version_parts(current)
    except Exception:
        return False


def _autoheal_pending_update(logger: logging.Logger) -> None:
    """Finish any interrupted in-app update before we boot the current code.

    Scans ``tempfile.gettempdir()`` for ``sea_update_*`` directories left
    behind by a failed install. If one contains a newer ``shelly_analyzer``
    package, chain-handoff into ``updater_helper.py`` via ``os.execv`` so
    the helper applies the copy, reinstalls deps and execs the new app —
    all on the same PID so systemd / launchd / Docker stay happy.

    Intentionally swallows every exception: auto-heal must NEVER block
    normal startup. We'd rather boot the old (safe) code than crash here.
    """
    try:
        from shelly_analyzer import __version__ as _current_version
        tmp_root = Path(tempfile.gettempdir())
        candidates = sorted(tmp_root.glob("sea_update_*"))
        if not candidates:
            return

        app_dir = Path(__file__).resolve().parent.parent.parent
        if not (app_dir / "src" / "shelly_analyzer").is_dir():
            # Running from an unusual layout — don't try to patch.
            return

        for cand in candidates:
            if not cand.is_dir():
                continue
            # Probe two layouts: (a) staging root IS the release root (the
            # common case — /tmp/sea_update_XXX/src/...); (b) release is
            # wrapped in a single top-level folder (/tmp/sea_update_XXX/
            # shelly-energy-analyzer-X.Y.Z/src/...). Pick whichever has the
            # package __init__.py at src/shelly_analyzer/__init__.py.
            scan_root = None
            for probe in (cand, *[p for p in cand.iterdir() if p.is_dir() and not p.name.startswith(".")]):
                if (probe / "src" / "shelly_analyzer" / "__init__.py").is_file():
                    scan_root = probe
                    break
            if scan_root is None:
                continue  # not a release layout, skip

            staged_init = scan_root / "src" / "shelly_analyzer" / "__init__.py"

            try:
                text = staged_init.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                continue
            m = re.search(r'__version__\s*=\s*"([^"]+)"', text)
            if not m:
                continue
            staged_v = m.group(1)

            if not _is_strictly_newer(staged_v, _current_version):
                logger.info(
                    "Auto-heal: staged %s in %s is not newer than running %s, skipping",
                    staged_v, cand, _current_version,
                )
                continue

            # Newer release found — hand off to the helper.
            helper = Path(__file__).resolve().parent / "updater_helper.py"
            if not helper.is_file():
                logger.warning("Auto-heal: updater_helper.py not found at %s — aborting heal", helper)
                return

            restart_name = "start.bat" if os.name == "nt" else "start.command"
            restart = app_dir / restart_name

            cmd = [
                sys.executable, str(helper),
                "--app-dir", str(app_dir),
                "--staging-dir", str(scan_root),
                "--restart", str(restart),
                "--wait-pid", "0",
                "--update-deps", "1",
            ]
            logger.warning(
                "Auto-heal: staged update %s (current %s) detected in %s — applying now",
                staged_v, _current_version, cand,
            )

            if os.name == "nt":
                # Windows can't execv in-place reliably. Spawn detached +
                # exit — on Windows services this works since there's no
                # cgroup teardown of children.
                try:
                    import subprocess
                    subprocess.Popen(
                        cmd,
                        cwd=str(app_dir),
                        stdin=subprocess.DEVNULL,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                        creationflags=subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP,
                        close_fds=False,
                    )
                    logger.info("Auto-heal: spawned detached helper on Windows, exiting")
                    os._exit(0)
                except Exception as e:
                    logger.warning("Auto-heal: Windows spawn failed: %s — continuing normal startup", e)
                    return

            # POSIX — execv in place so we keep the MainPID.
            try:
                os.closerange(3, 256)
            except Exception:
                pass
            try:
                os.execv(sys.executable, cmd)
                # execv does not return on success
            except Exception as e:
                logger.warning("Auto-heal: execv failed: %s — continuing normal startup", e)
                return
    except Exception as e:
        # Must never block startup — log and return.
        try:
            logger.warning("Auto-heal scan failed (%s) — continuing normal startup", e)
        except Exception:
            pass


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="shelly-analyzer", description="Shelly Energy Analyzer")
    parser.add_argument("--version", action="store_true", help="print version and exit")
    parser.add_argument("--config", default="config.json", help="path to config.json")
    parser.add_argument("--host", default="0.0.0.0", help="bind address (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=None, help="port (default: from config, or 8765)")
    parser.add_argument("--no-ssl", action="store_true", help="disable HTTPS")
    parser.add_argument("--debug", action="store_true", help="enable Flask debug mode")
    args = parser.parse_args(argv)

    if args.version:
        from shelly_analyzer import __version__
        print(__version__)
        return 0

    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logger = logging.getLogger("shelly_analyzer")

    from pathlib import Path
    from shelly_analyzer.io.config import load_config

    # Load config
    cfg_path = Path(args.config).resolve()
    cfg = load_config(str(cfg_path))
    out_dir = cfg_path.parent

    port = args.port or int(cfg.ui.live_web_port) or 8765

    # ── Auto-heal: finish an interrupted in-app update ─────────────────
    # If a previous "Install update" click crashed partway (e.g. the
    # pre-16.26.1 updater was killed by systemd's cgroup cleanup), the
    # staged ZIP is still sitting in /tmp/sea_update_*. Pick it up and
    # finish the job before we continue booting the old code.
    #
    # NOTE: this only kicks in for v16.26.3+. Users still stuck on 16.25.x
    # / 16.26.0 need scripts/rescue.sh (or rescue.ps1) to reach 16.26.3 —
    # after that they're self-healing against any future updater bug.
    import os
    _autoheal_pending_update(logger)

    # ── Single-instance lock ────────────────────────────────────────────
    # Prevent multiple parallel instances on the same config directory.
    # Multiple instances cause duplicate Telegram/email/webhook deliveries
    # because each runs its own background scheduler with its own in-memory
    # "already sent today" guard.
    lock_path = out_dir / ".shelly_analyzer.lock"
    try:
        if lock_path.exists():
            try:
                old_pid = int(lock_path.read_text().strip())
                # If the lock's PID is our own PID, the previous process image
                # already exited (we just took its place via execv during an
                # in-app update, or the OS recycled the PID). Treat as stale.
                if old_pid == os.getpid():
                    logger.info(
                        "Lock file PID %d matches our own — previous image replaced via execv, taking over",
                        old_pid,
                    )
                else:
                    # Probe if old PID is still alive
                    try:
                        os.kill(old_pid, 0)
                        logger.error(
                            "Another shelly_analyzer instance (PID %d) is already running with this config. "
                            "Refusing to start to avoid duplicate notifications. "
                            "Stop it first or remove %s if it's stale.",
                            old_pid, lock_path,
                        )
                        return 1
                    except ProcessLookupError:
                        logger.info("Stale lock file from PID %d, replacing", old_pid)
                    except PermissionError:
                        # Process exists but we can't signal it — assume alive
                        logger.error(
                            "Another shelly_analyzer instance (PID %d) appears to be running. "
                            "Refusing to start. Remove %s manually if you're sure it's gone.",
                            old_pid, lock_path,
                        )
                        return 1
            except (ValueError, OSError):
                logger.info("Lock file unreadable, replacing")
        lock_path.write_text(str(os.getpid()))
    except OSError as e:
        logger.warning("Could not write lock file %s: %s (continuing without lock)", lock_path, e)
        lock_path = None  # type: ignore[assignment]

    # Create Flask app
    from shelly_analyzer.web import create_app
    app = create_app(config_path=str(cfg_path))
    state = app.extensions["state"]

    # Start background services
    from shelly_analyzer.web.background import BackgroundServiceManager
    from shelly_analyzer.web.action_dispatch import ActionDispatcher

    dispatcher = ActionDispatcher(
        cfg=cfg,
        storage=state.storage,
        live_store=state.live_store,
        out_dir=out_dir,
        cfg_path=cfg_path,
    )
    state.on_action = dispatcher.dispatch
    state._dispatcher = dispatcher  # type: ignore[attr-defined]

    bg = BackgroundServiceManager(
        cfg=cfg,
        storage=state.storage,
        live_store=state.live_store,
        out_dir=out_dir,
        on_action=dispatcher.dispatch,
    )
    bg._dispatcher = dispatcher  # type: ignore[attr-defined]
    dispatcher._bg = bg  # type: ignore[attr-defined]
    bg.start_all()
    # Expose for API access (status / trigger)
    state._bg = bg  # type: ignore[attr-defined]

    # Persist NILM state on normal exit (Ctrl-C / kill -TERM)
    import atexit
    import signal
    def _graceful_stop(*_args):
        try:
            bg.stop_all()
        except Exception:
            pass
        # Remove single-instance lock file so a fresh start can take over.
        try:
            if lock_path is not None and lock_path.exists():
                lock_path.unlink()
        except Exception:
            pass
    atexit.register(_graceful_stop)
    try:
        signal.signal(signal.SIGTERM, lambda *a: (_graceful_stop(), sys.exit(0)))
    except Exception:
        pass

    # SSL setup
    ssl_context = None
    if not args.no_ssl:
        ssl_mode = str(getattr(cfg.ui, "live_web_ssl_mode", "auto") or "auto").lower()
        if ssl_mode != "off":
            try:
                import ssl
                from shelly_analyzer.web.ssl_utils import (
                    ensure_ssl_cert,
                    inspect_cert,
                )
                from pathlib import Path as _P

                ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
                if ssl_mode == "custom" and getattr(cfg.ui, "live_web_ssl_cert", "") and getattr(cfg.ui, "live_web_ssl_key", ""):
                    custom_cert = _P(cfg.ui.live_web_ssl_cert)
                    ctx.load_cert_chain(str(custom_cert), cfg.ui.live_web_ssl_key)
                    info = inspect_cert(custom_cert)
                    if info.days_remaining is not None:
                        if info.days_remaining < 0:
                            logger.error(
                                "Custom TLS certificate EXPIRED %d days ago — browsers will reject it",
                                -info.days_remaining,
                            )
                        elif info.days_remaining < int(getattr(cfg.ui, "live_web_ssl_renew_days", 30) or 30):
                            logger.warning(
                                "Custom TLS certificate expires in %d days — renew it soon",
                                info.days_remaining,
                            )
                    logger.info(
                        "HTTPS enabled (custom certificate: %s, %s days remaining)",
                        custom_cert,
                        info.days_remaining if info.days_remaining is not None else "?",
                    )
                    state._ssl_cert_info = info
                else:
                    cert_dir = out_dir / "data" / "runtime" / "ssl"
                    cert, key, info = ensure_ssl_cert(
                        cert_dir,
                        auto_renew=bool(getattr(cfg.ui, "live_web_ssl_auto_renew", True)),
                        renew_days=int(getattr(cfg.ui, "live_web_ssl_renew_days", 30) or 30),
                    )
                    ctx.load_cert_chain(str(cert), str(key))
                    logger.info(
                        "HTTPS enabled (self-signed certificate, %s days remaining)",
                        info.days_remaining if info.days_remaining is not None else "?",
                    )
                    state._ssl_cert_info = info
                ssl_context = ctx
                state._is_https = True
                state._ssl_mode = ssl_mode
            except Exception as e:
                logger.warning("HTTPS not available: %s — falling back to HTTP", e)
                state._ssl_cert_info = None
                state._ssl_mode = "off"

    scheme = "https" if ssl_context else "http"
    from shelly_analyzer.web.ssl_utils import _local_ip_guess
    ip = _local_ip_guess()

    # Try the configured port, fall back to next 19 ports if busy (same as webdash.py)
    import socket as _socket
    actual_port = None
    for try_port in range(port, port + 20):
        try:
            s = _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM)
            s.setsockopt(_socket.SOL_SOCKET, _socket.SO_REUSEADDR, 1)
            s.bind((args.host, try_port))
            s.close()
            actual_port = try_port
            break
        except OSError:
            continue
    if actual_port is None:
        logger.error("Could not find a free port in range %d-%d", port, port + 19)
        bg.stop_all()
        return 1
    if actual_port != port:
        logger.warning("Port %d is in use, using port %d instead", port, actual_port)
    port = actual_port
    state.port = port

    logger.info("Starting Shelly Energy Analyzer on %s://%s:%d/", scheme, ip, port)

    try:
        app.run(
            host=args.host,
            port=port,
            ssl_context=ssl_context,
            debug=args.debug,
            use_reloader=False,  # We manage our own background threads
            threaded=True,
        )
    except KeyboardInterrupt:
        pass
    finally:
        bg.stop_all()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
