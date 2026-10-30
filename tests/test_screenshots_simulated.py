"""The README says every screenshot comes from simulated data.  Keep it true.

1. Every PNG under ``docs/screenshots`` must carry the stamp that
   ``scripts/capture_screenshots.py`` writes (see ``scripts/screenshot_marker.py``).
   An image taken by hand from a real installation has none and fails here.
2. Every image the README links must exist.
3. Optional (``SCREENSHOT_OCR=1``, needs ``tesseract``): read the text out of each image
   and fail on anything that looks like a private network address or a local hostname.
   Generic patterns only; names of real devices or people are deliberately not listed in
   a public repository, so the stamp is the primary guard and this is a second net.
"""
from __future__ import annotations

import importlib.util
import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SHOTS = ROOT / "docs" / "screenshots"


def _marker():
    spec = importlib.util.spec_from_file_location("screenshot_marker", ROOT / "scripts" / "screenshot_marker.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _pngs():
    return sorted(SHOTS.glob("*.png"))


def test_every_screenshot_is_stamped_as_simulated():
    m = _marker()
    unstamped = [p.name for p in _pngs() if m.read_source(p) != m.VALUE]
    assert not unstamped, (
        "Screenshots without the 'simulated installation' stamp (re-shoot them with "
        "scripts/capture_screenshots.py, never add pictures of a real installation): " + ", ".join(unstamped)
    )


def test_readme_image_links_exist():
    text = (ROOT / "README.md").read_text(encoding="utf-8")
    missing = [rel for rel in re.findall(r"\]\((docs/screenshots/[^)\s]+)\)", text) if not (ROOT / rel).exists()]
    assert not missing, f"README links to missing images: {missing}"


_PRIVATE_IP = re.compile(r"\b(?:192\.168|10\.\d{1,3}|172\.(?:1[6-9]|2\d|3[01])|100\.(?:6[4-9]|[7-9]\d|1[01]\d|12[0-7]))\.\d{1,3}\.\d{1,3}\b")
_LOCAL_HOST = re.compile(r"\b[\w-]+\.(?:local|lan|home|internal|fritz\.box)\b", re.I)


@pytest.mark.skipif(not (os.environ.get("SCREENSHOT_OCR") and shutil.which("tesseract")),
                    reason="set SCREENSHOT_OCR=1 and install tesseract to run the OCR check")
def test_screenshots_show_no_network_identity():
    hits = []
    with tempfile.TemporaryDirectory() as tmp:
        for p in _pngs():
            base = str(Path(tmp) / p.stem)
            subprocess.run(["tesseract", str(p), base, "-l", "eng"], capture_output=True, check=False)
            txt = Path(base + ".txt").read_text(errors="ignore") if Path(base + ".txt").exists() else ""
            for rx in (_PRIVATE_IP, _LOCAL_HOST):
                for mt in rx.findall(txt):
                    hits.append(f"{p.name}: {mt}")
    assert not hits, "Network identity visible in screenshots: " + "; ".join(hits)
