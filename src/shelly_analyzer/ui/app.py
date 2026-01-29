from __future__ import annotations

"""UI entrypoint.

This module intentionally stays small.
The main Tkinter application is implemented in `ui/app_main.py` and `ui/mixins/*`.
"""

from .app_main import App


def run_gui() -> None:
    """Start the Tkinter GUI application.

    This entrypoint is imported by `shelly_analyzer.__main__`.
    It also configures logging so users can find crash logs under ./logs.
    """
    try:
        from pathlib import Path
        import logging
        from shelly_analyzer.io.logging_setup import setup_logging

        # Ensure logs are created next to the app folder (where config.json lives).
        setup_logging(base_dir=Path.cwd(), level=logging.INFO)
    except Exception:
        # Never fail startup because of logging.
        pass

    app = App()
    app.mainloop()


if __name__ == "__main__":
    run_gui()
