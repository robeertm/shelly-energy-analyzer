from __future__ import annotations

import argparse
import logging
import sys


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
                from shelly_analyzer.web.ssl_utils import _ensure_ssl_cert

                ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
                if ssl_mode == "custom" and getattr(cfg.ui, "live_web_ssl_cert", "") and getattr(cfg.ui, "live_web_ssl_key", ""):
                    ctx.load_cert_chain(cfg.ui.live_web_ssl_cert, cfg.ui.live_web_ssl_key)
                    logger.info("HTTPS enabled (custom certificate)")
                else:
                    cert_dir = out_dir / "data" / "runtime" / "ssl"
                    cert, key = _ensure_ssl_cert(cert_dir)
                    ctx.load_cert_chain(str(cert), str(key))
                    logger.info("HTTPS enabled (self-signed certificate)")
                ssl_context = ctx
                state._is_https = True
            except Exception as e:
                logger.warning("HTTPS not available: %s — falling back to HTTP", e)

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
