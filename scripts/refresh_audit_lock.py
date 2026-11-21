#!/usr/bin/env python3
"""Regenerate requirements-audit.txt: the fully resolved versions a fresh
install of requirements.txt would get today. The file is only read by the
dependency audit (the release gate and the security workflow); nothing
installs it, so the loose ranges in requirements.txt stay as they are."""
import json
import os
import subprocess
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HEAD = ("# Resolved snapshot of requirements.txt, scanned by the security gate (dependency audit).\n"
        "# Not installed by anything. Refresh: python3 scripts/refresh_audit_lock.py\n")

with tempfile.TemporaryDirectory() as d:
    rep = os.path.join(d, "report.json")
    subprocess.run([sys.executable, "-m", "pip", "install", "-q", "--dry-run", "--ignore-installed",
                    "--report", rep, "-r", os.path.join(ROOT, "requirements.txt")], check=True)
    with open(rep, encoding="utf-8") as f:
        items = json.load(f)["install"]
pins = sorted((i["metadata"]["name"].lower(), i["metadata"]["version"]) for i in items)
with open(os.path.join(ROOT, "requirements-audit.txt"), "w", encoding="utf-8") as f:
    f.write(HEAD + "".join("%s==%s\n" % p for p in pins))
print("requirements-audit.txt: %d pins" % len(pins))
