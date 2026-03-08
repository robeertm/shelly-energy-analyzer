from __future__ import annotations

import logging
import logging.handlers
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional


_CONFIGURED = False
_LOG_PATH: Optional[Path] = None

_MAX_LOG_DAYS = 30  # keep logs for 30 days


def _cleanup_old_logs(log_dir: Path, keep_days: int = _MAX_LOG_DAYS) -> None:
    """Delete log files older than *keep_days*."""
    try:
        from datetime import timedelta
        cutoff = datetime.now() - timedelta(days=keep_days)
        for f in log_dir.glob("app_*.log*"):
            try:
                if f.stat().st_mtime < cutoff.timestamp():
                    f.unlink()
            except Exception:
                pass
    except Exception:
        pass


def setup_logging(base_dir: Optional[Path] = None, level: int = logging.INFO) -> Path:
    """Configure file + console logging.

    Creates ./logs/app_YYYY-MM-DD.log (relative to *base_dir* or CWD).
    Uses TimedRotatingFileHandler to rotate daily and keep up to 30 days.
    Also installs a sys.excepthook so uncaught exceptions land in the log.

    Returns the log file path.
    """
    global _CONFIGURED, _LOG_PATH

    root = Path(base_dir) if base_dir else Path.cwd()
    log_dir = root / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"app_{datetime.now().strftime('%Y-%m-%d')}.log"
    _LOG_PATH = log_path

    # Clean up old log files on every startup
    _cleanup_old_logs(log_dir, _MAX_LOG_DAYS)

    if _CONFIGURED:
        return log_path

    logger = logging.getLogger()
    logger.setLevel(level)

    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")

    # Use TimedRotatingFileHandler: rotate at midnight, keep 30 backups
    fh = logging.handlers.TimedRotatingFileHandler(
        log_path, when="midnight", interval=1,
        backupCount=_MAX_LOG_DAYS, encoding="utf-8",
    )
    fh.setLevel(level)
    fh.setFormatter(fmt)

    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(level)
    sh.setFormatter(fmt)

    # Avoid duplicate handlers if the user restarts the GUI from the same process.
    if not any(isinstance(h, logging.FileHandler) for h in logger.handlers):
        logger.addHandler(fh)
    if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
        logger.addHandler(sh)

    def _excepthook(exc_type, exc, tb):
        logging.getLogger("unhandled").error("Unhandled exception", exc_info=(exc_type, exc, tb))
        try:
            sys.__excepthook__(exc_type, exc, tb)
        except Exception:
            pass

    try:
        sys.excepthook = _excepthook
    except Exception:
        pass

    _CONFIGURED = True
    logging.getLogger(__name__).info("Logging initialized: %s", str(log_path))
    return log_path


def get_log_path() -> Optional[Path]:
    return _LOG_PATH
