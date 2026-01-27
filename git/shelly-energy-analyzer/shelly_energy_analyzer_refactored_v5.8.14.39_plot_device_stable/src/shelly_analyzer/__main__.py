from __future__ import annotations

import argparse
import sys


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="shelly-analyzer", description="Shelly Energy Analyzer")
    parser.add_argument("--version", action="store_true", help="print version and exit")
    args = parser.parse_args(argv)

    if args.version:
        from shelly_analyzer import __version__
        print(__version__)
        return 0

    # Lazy-import GUI so that `--version` (and headless environments)
    # don't require tkinter.
    from shelly_analyzer.ui.app import run_gui
    run_gui()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
