#!/usr/bin/env python3
"""Mark PNG files as produced from the simulated installation.

The README promises that every picture comes from simulated data
(``scripts/simulate_dataset.py``).  A promise nobody checks drifts, so the
capture script stamps each image it writes with a PNG ``tEXt`` chunk and the
test ``tests/test_screenshots_simulated.py`` refuses any image without it.
A screenshot taken by hand from a real installation has no stamp and fails
the test instead of slipping into the repository.

    python scripts/screenshot_marker.py stamp docs/screenshots/*.png
    python scripts/screenshot_marker.py check docs/screenshots/*.png

``stamp`` only rewrites metadata; the pixels are untouched.  Stamp a file
only after looking at it: the stamp states a fact, it does not create one.
"""
from __future__ import annotations

import struct
import sys
import zlib
from pathlib import Path
from typing import Optional

KEY = "Source"
VALUE = "shelly-analyzer simulated installation (scripts/simulate_dataset.py)"
_SIG = b"\x89PNG\r\n\x1a\n"


def _chunks(data: bytes):
    pos = len(_SIG)
    while pos + 8 <= len(data):
        (length,) = struct.unpack(">I", data[pos:pos + 4])
        ctype = data[pos + 4:pos + 8]
        yield ctype, data[pos + 8:pos + 8 + length], pos, pos + 12 + length
        pos += 12 + length


def read_source(path: Path) -> Optional[str]:
    """Return the ``Source`` text of a PNG, or None when absent / not a PNG."""
    data = Path(path).read_bytes()
    if not data.startswith(_SIG):
        return None
    for ctype, body, _, _ in _chunks(data):
        if ctype == b"tEXt":
            key, _, val = body.partition(b"\x00")
            if key.decode("latin-1") == KEY:
                return val.decode("latin-1")
    return None


def stamp(path: Path) -> bool:
    """Write the marker into *path* (idempotent). Returns True if the file changed."""
    p = Path(path)
    data = p.read_bytes()
    if not data.startswith(_SIG):
        raise ValueError(f"{p}: not a PNG")
    if read_source(p) == VALUE:
        return False
    body = KEY.encode("latin-1") + b"\x00" + VALUE.encode("latin-1")
    chunk = struct.pack(">I", len(body)) + b"tEXt" + body + struct.pack(">I", zlib.crc32(b"tEXt" + body) & 0xFFFFFFFF)
    out, inserted = bytearray(_SIG), False
    for ctype, cbody, start, end in _chunks(data):
        if ctype == b"tEXt" and cbody.partition(b"\x00")[0].decode("latin-1") == KEY:
            continue  # replace an old marker
        if ctype == b"IEND" and not inserted:
            out += chunk
            inserted = True
        out += data[start:end]
    p.write_bytes(bytes(out))
    return True


def main(argv: list[str]) -> int:
    if len(argv) < 3 or argv[1] not in ("stamp", "check"):
        print(__doc__)
        return 2
    bad = 0
    for name in argv[2:]:
        if argv[1] == "stamp":
            print(("stamped " if stamp(Path(name)) else "already  ") + name)
        elif read_source(Path(name)) != VALUE:
            print("MISSING  " + name)
            bad += 1
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
