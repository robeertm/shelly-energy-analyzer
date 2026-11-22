"""Seed a test database once per distinct content, then copy the file.

Building the minute history of a simulated house (4 days x 4 devices) through
``insert_dataframe`` costs ~15 s; the EV tests did that for every single test.
The content is identical, so the first request builds it and closes the
database (which checkpoints the WAL into one file); later requests copy it.
The copied database is a private file per test, so nothing is shared.
"""
import atexit
import shutil
import tempfile
from pathlib import Path

_TEMPLATES = {}
_ROOT = None


def _root() -> Path:
    global _ROOT
    if _ROOT is None:
        _ROOT = Path(tempfile.mkdtemp(prefix="dbcache-"))
        atexit.register(shutil.rmtree, str(_ROOT), True)
    return _ROOT


def seed(storage, key, fill):
    """Put the database for ``key`` into ``storage``.

    ``fill(storage)`` writes the rows; it runs only on the first call per key.
    """
    from shelly_analyzer.io.storage import Storage

    target = Path(storage.base_dir) / "energy.db"
    tpl = _TEMPLATES.get(key)
    if tpl is None:
        d = _root() / ("t%d" % len(_TEMPLATES))
        st = Storage(d)
        fill(st)
        st.db.close()
        for ext in ("-wal", "-shm"):
            p = d / ("energy.db" + ext)
            if p.exists() and p.stat().st_size == 0:
                p.unlink()
        _TEMPLATES[key] = tpl = d / "energy.db"
    if storage._db is not None:
        storage.db.close()
        storage._db = None
    shutil.copyfile(tpl, target)
