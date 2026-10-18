"""Three ways a new version arrives — and each keeps its own (17.7.0).

* ZIP / start script / service  -> the in-app updater (unchanged)
* Home-Assistant add-on         -> the add-on is rebuilt (unchanged)
* the published Docker image    -> a new image; the in-app updater is OFF

The Docker image sets ``SEA_CONTAINER=1``. Before 17.7.0 nothing told the app it
was running there, so the in-app updater ran inside the container: it swapped
files that the next ``docker compose up`` threw away again, and it competed with
Watchtower. These tests pin all three paths down, including the one that must
NOT change: a normal install still reaches GitHub through the old updater.
"""
from __future__ import annotations

import os
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from flask import Flask  # noqa: E402

from shelly_analyzer.io import config as cfgmod  # noqa: E402
from shelly_analyzer.web.blueprints import updates as updmod  # noqa: E402

KANAL_VARS = ("SUPERVISOR_TOKEN", "HASSIO_TOKEN", "SEA_CONTAINER")


@pytest.fixture
def umgebung(monkeypatch):
    for k in KANAL_VARS:
        monkeypatch.delenv(k, raising=False)

    def setzen(**werte):
        for k, v in werte.items():
            monkeypatch.setenv(k, v)
    return setzen


@pytest.fixture
def client(monkeypatch):
    """The real blueprint, a fake state, and GitHub calls that are recorded."""
    rufe = []

    def fetch_releases(repo, limit=10):
        rufe.append(("fetch_releases", repo))
        return []

    def check_latest_release(*a, **k):
        rufe.append(("check_latest_release",))
        raise AssertionError("must not be reached in these tests")

    monkeypatch.setattr(updmod, "fetch_releases", fetch_releases)
    monkeypatch.setattr(updmod, "check_latest_release", check_latest_release)
    updmod._releases_cache.clear()
    app = Flask(__name__)
    app.extensions["state"] = SimpleNamespace(
        cfg=SimpleNamespace(updates=SimpleNamespace(repo="owner/repo")), _bg=None)
    app.register_blueprint(updmod.bp)
    with app.test_client() as c:
        c.rufe = rufe
        yield c


# ── which channel ─────────────────────────────────────────────────────
@pytest.mark.parametrize("werte, erwartet", [
    ({}, "app"),
    ({"SEA_CONTAINER": "1"}, "docker"),
    ({"SEA_CONTAINER": "true"}, "docker"),
    ({"SEA_CONTAINER": "0"}, "app"),
    ({"SEA_CONTAINER": ""}, "app"),
    ({"SUPERVISOR_TOKEN": "x"}, "addon"),
    ({"HASSIO_TOKEN": "x"}, "addon"),
    # An add-on is a container too: its own path (rebuild) must win.
    ({"SUPERVISOR_TOKEN": "x", "SEA_CONTAINER": "1"}, "addon"),
])
def test_update_channel(umgebung, werte, erwartet):
    umgebung(**werte)
    assert cfgmod.update_channel() == erwartet
    assert cfgmod.is_container() == (erwartet == "docker" or
                                      werte.get("SEA_CONTAINER") in ("1", "true"))


# ── managed channels: no updater, a clear message, never GitHub ───────
@pytest.mark.parametrize("werte, kanal", [
    ({"SEA_CONTAINER": "1"}, "docker"),
    ({"SUPERVISOR_TOKEN": "x"}, "addon"),
])
def test_managed_channels_switch_the_updater_off(umgebung, client, werte, kanal):
    umgebung(**werte)
    for pfad in ("/api/updates/cached", "/api/updates/status", "/api/updates/releases"):
        d = client.get(pfad).get_json()
        assert d["channel"] == kanal, pfad
        assert d["docker"] is (kanal == "docker"), pfad
        assert d["addon"] is (kanal == "addon"), pfad
        assert d.get("has_update", False) is False, pfad
        assert d.get("releases", []) == [], pfad
        wort = "container image" if kanal == "docker" else "add-on"
        assert wort in d["message"], (pfad, d["message"])
    r = client.post("/api/updates/install", json={"tag": "v99.0.0"})
    assert r.status_code == 409
    assert r.get_json()["channel"] == kanal
    assert client.rufe == [], "a managed channel must not ask GitHub"


# ── the normal install keeps the old path ─────────────────────────────
def test_normal_install_keeps_the_in_app_updater(umgebung, client):
    umgebung()
    d = client.get("/api/updates/cached").get_json()
    assert "channel" not in d and "docker" not in d and "addon" not in d
    assert d["checked"] is False          # no checker state yet — the old answer
    d = client.get("/api/updates/releases").get_json()
    assert d["ok"] is True and "channel" not in d
    assert client.rufe == [("fetch_releases", "owner/repo")], \
        "a normal install must still reach GitHub through the old updater"


def test_settings_page_hears_which_channel(umgebung):
    """The settings page hides the updater for `_addon` and `_docker`."""
    import re
    src = open(os.path.join(os.path.dirname(__file__), "..", "src", "shelly_analyzer",
                            "web", "templates", "settings.html"), encoding="utf-8").read()
    assert "cfg._docker" in src and "settings.updates.docker_managed" in src
    assert re.search(r'!\(cfg && \(cfg\._addon \|\| cfg\._docker\)\)', src), \
        "the status check must be skipped in Docker too"


def test_every_language_has_the_docker_text():
    from shelly_analyzer import i18n
    en = i18n.t("en", "settings.updates.docker_managed")
    for lang in i18n.LANGS:
        for key in ("settings.updates.docker_managed", "settings.updates.docker_watchtower",
                    "settings.updates.addon_managed"):
            txt = i18n.t(lang, key)
            assert txt and txt != key, (lang, key)
            if lang != "en" and key == "settings.updates.docker_managed":
                assert txt != en, ("still English", lang)
