"""requirements.txt must cover every runtime dependency of pyproject.toml.

The in-app updater of a local installation runs ``pip install -r
requirements.txt``; a dependency only listed in pyproject.toml would never
reach such an installation (flask, waitress and paho-mqtt were missing).
"""
import re
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _name(spec: str) -> str:
    return re.split(r"[<>=!~;\[ ]", spec.strip(), 1)[0].lower().replace("_", "-")


def test_requirements_cover_pyproject_dependencies():
    deps = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["dependencies"]
    reqs = {_name(l) for l in (ROOT / "requirements.txt").read_text().splitlines()
            if l.strip() and not l.lstrip().startswith("#")}
    missing = sorted(_name(d) for d in deps if _name(d) not in reqs)
    assert not missing, f"requirements.txt lacks {missing}"
