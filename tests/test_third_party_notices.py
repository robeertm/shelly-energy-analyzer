"""THIRD-PARTY-NOTICES.md must name every dependency of the application.

Offline: compares the dependency names in requirements.txt and pyproject.toml
with the generated file. When this fails, regenerate the file in a throwaway
environment (see the docstring of scripts/drittlizenzen.py) and commit it.
"""
import re
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
NOTICES = ROOT / "THIRD-PARTY-NOTICES.md"


def _norm(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def _name(spec: str) -> str:
    return _norm(re.split(r"[<>=!~;\[ ]", spec.strip(), maxsplit=1)[0])


def _listed() -> set[str]:
    text = NOTICES.read_text(encoding="utf-8")
    # Only the component list; the licence texts below it contain bullet lists too.
    section = text.split("## Python packages", 1)[1].split("## Licence texts", 1)[0]
    return {_norm(m) for m in re.findall(r"^- `([^`]+)` ", section, re.M)}


def _dependencies() -> set[str]:
    reqs = {
        _name(line.split("#", 1)[0])
        for line in (ROOT / "requirements.txt").read_text().splitlines()
        if line.split("#", 1)[0].strip()
    }
    proj = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]
    specs = list(proj.get("dependencies", []))
    for extra in (proj.get("optional-dependencies") or {}).values():
        specs += extra
    return reqs | {_name(s) for s in specs}


def test_every_dependency_is_listed():
    missing = sorted(_dependencies() - _listed())
    assert not missing, (
        f"THIRD-PARTY-NOTICES.md lacks {missing}; regenerate it with scripts/drittlizenzen.py"
    )


def test_notices_carry_licence_texts():
    text = NOTICES.read_text(encoding="utf-8")
    assert "## Licence texts" in text
    assert "Permission is hereby granted" in text  # MIT texts are kept with their notices


def test_notice_and_licence_ship_in_the_packages():
    manifest = (ROOT / "MANIFEST.in").read_text()
    assert "THIRD-PARTY-NOTICES.md" in manifest and "LICENSE" in manifest
    docker_ignore = (ROOT / ".dockerignore").read_text().splitlines()
    assert "!THIRD-PARTY-NOTICES.md" in docker_ignore  # *.md is excluded above it
    assert "LICENSE" not in [l.strip() for l in docker_ignore]
    assert "THIRD-PARTY-NOTICES.md" in (ROOT / "Dockerfile").read_text()
