"""Standalone pywebview runner.

We start this in a *separate process* from the Tk GUI to avoid macOS
main-thread constraints (Tk already owns the main thread).

Usage:
  python -m shelly_analyzer.webview_runner <url> [title]
"""

from __future__ import annotations

import sys


def main() -> int:
    url = sys.argv[1] if len(sys.argv) > 1 else ""
    title = sys.argv[2] if len(sys.argv) > 2 else "Shelly Energy Analyzer"
    if not url:
        print("No URL provided")
        return 2

    try:
        import webview  # type: ignore
    except Exception as e:
        print(f"pywebview import failed: {e}")
        return 3

    try:
        # On macOS this uses WebKit (Safari engine).
        webview.create_window(title, url, width=1200, height=800)
        webview.start()
        return 0
    except KeyboardInterrupt:
        return 0
    except Exception as e:
        print(f"webview start failed: {e}")
        return 4


if __name__ == "__main__":
    raise SystemExit(main())
