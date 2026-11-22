"""Hand freed heap memory back to the operating system.

CPython frees the objects of a large temporary result (a pandas frame built
from a database read, the encoded Live-tab cache) into glibc's heap, and glibc
keeps that heap mapped: the resident size of the process stays near its last
peak for a long time, although nothing uses the memory any more. On a small
board that is memory the rest of the machine does not get. ``malloc_trim(0)``
returns the free pages. It only exists in glibc; everywhere else (macOS,
Windows, musl) this is a no-op.
"""
from __future__ import annotations

import ctypes
import ctypes.util
import logging
import sys
import threading

logger = logging.getLogger(__name__)

_lock = threading.Lock()
_trim = None
_looked_up = False


def _lookup():
    global _trim, _looked_up
    _looked_up = True
    if not sys.platform.startswith("linux"):
        return None
    try:
        libc = ctypes.CDLL(ctypes.util.find_library("c") or "libc.so.6")
        fn = libc.malloc_trim
        fn.argtypes = [ctypes.c_size_t]
        fn.restype = ctypes.c_int
        _trim = fn
    except (OSError, AttributeError):
        _trim = None
    return _trim


def release_free_memory() -> bool:
    """Return free heap pages to the OS. True when glibc's ``malloc_trim`` ran."""
    with _lock:
        fn = _trim if _looked_up else _lookup()
        if fn is None:
            return False
        try:
            fn(0)
            return True
        except Exception:  # pragma: no cover - defensive
            logger.debug("malloc_trim failed", exc_info=True)
            return False
