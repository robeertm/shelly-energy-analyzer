"""Secrets in the configuration never leave the app in clear text (17.8.3).

``GET /api/settings`` used to mask a fixed list of nine fields; the MQTT
password, the link token, the InfluxDB token, the Home-Assistant token, device
passwords and the web token itself went out as they were. Masking is now by
field *name*, nested and in lists, and ``"***"`` on save never overwrites a
stored value. Only placeholders are used here - no real values.
"""
from __future__ import annotations

import json
import os
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from flask import Flask  # noqa: E402

from shelly_analyzer.io.config import load_config, save_config  # noqa: E402
from shelly_analyzer.io.secret_mask import MASK, is_secret_key, mask_secrets, restore_masked  # noqa: E402
from shelly_analyzer.web.blueprints import devices as devmod  # noqa: E402
from shelly_analyzer.web.blueprints import settings as setmod  # noqa: E402

# The fields that must be masked when set (every secret-named string in AppConfig).
EXPECTED = {
    "ui.live_web_token", "ui.telegram_bot_token", "ui.email_smtp_password",
    "ui.webhook_url", "ui.webhook_custom_headers",
    "pv_source.ha_token", "pv_source.http_api_key", "tou.urdb_api_key",
    "co2.entso_e_api_token", "co2.electricity_maps_api_key",
    "spot_price.eia_api_key", "weather.api_key", "mqtt.password",
    "ev_charging.link_token", "influxdb.token", "api.api_key",
    "advisor.openai_api_key", "advisor.anthropic_api_key",
    "devices[].password",
}
# Names that look like secrets but are identifiers / paths.
NOT_SECRETS = {
    "device_key", "wallbox_device_key", "pv_meter_device_key", "grid_meter_device_key",
    "grid_display_device_key", "pv_production_device_key", "battery_device_key",
    "key", "em_id", "live_web_ssl_key", "live_web_ssl_cert", "mqtt_json_path",
}


def _strings(o, path=""):
    """(path, key, value) for every string leaf; list items are written ``[]``."""
    if isinstance(o, dict):
        for k, v in o.items():
            yield from _strings(v, f"{path}.{k}" if path else k)
    elif isinstance(o, list):
        for v in o:
            yield from _strings(v, path + "[]")
    elif isinstance(o, str):
        yield path, path.rsplit(".", 1)[-1], o


def _fill(o, path=""):
    """Put a placeholder into every secret-named string field (in place)."""
    if isinstance(o, dict):
        for k, v in o.items():
            p = f"{path}.{k}" if path else k
            if isinstance(v, str) and is_secret_key(k):
                o[k] = f"placeholder-{p}"
            else:
                _fill(v, p)
    elif isinstance(o, list):
        for v in o:
            _fill(v, path + "[]")


@pytest.fixture
def app_env(tmp_path):
    base = tmp_path / "base.json"
    save_config(load_config(str(tmp_path / "none.json")), base)
    raw = json.loads(base.read_text(encoding="utf-8"))
    raw["devices"] = [
        {"key": "dev_a", "name": "A", "host": "10.0.0.1", "em_id": 0, "password": ""},
        {"key": "dev_b", "name": "B", "host": "10.0.0.2", "em_id": 0, "password": ""},
    ]
    _fill(raw)
    cfg_path = tmp_path / "config.json"
    cfg_path.write_text(json.dumps(raw), encoding="utf-8")
    cfg = load_config(str(cfg_path))

    state = SimpleNamespace(cfg=cfg, _cfg_path=cfg_path, on_action=None, lang="en",
                            live_store=None)

    def reload_config(c):
        state.cfg = c
    state.reload_config = reload_config
    state.set_language = lambda lang: None
    app = Flask(__name__)
    app.extensions["state"] = state
    app.register_blueprint(setmod.bp)
    app.register_blueprint(devmod.bp)
    return SimpleNamespace(client=app.test_client(), state=state, path=cfg_path)


def _stored(env):
    return json.loads(env.path.read_text(encoding="utf-8"))


def _get(env, path):
    o = _stored(env)
    for part in path.split("."):
        if part.endswith("[]"):
            o = o[part[:-2]][0]
        else:
            o = o[part]
    return o


# ── the rule ──────────────────────────────────────────────────────────
@pytest.mark.parametrize("name", [
    "password", "mqtt_passwd", "telegram_bot_token", "live_web_token", "link_token",
    "ha_token", "api_key", "openai_api_key", "http_api_key", "client_secret",
    "private_key", "some_new_key", "credentials", "webhook_url", "webhook_custom_headers",
])
def test_secret_names(name):
    assert is_secret_key(name)


@pytest.mark.parametrize("name", sorted(NOT_SECRETS) + ["host", "name", "broker", "bucket"])
def test_identifier_names_are_not_secrets(name):
    assert not is_secret_key(name)


# ── GET ───────────────────────────────────────────────────────────────
def test_get_masks_every_secret_named_field(app_env):
    body = app_env.client.get("/api/settings").get_json()
    raw = json.dumps(body)
    assert "placeholder-" not in raw, "a secret went out in clear text"
    masked = set()
    for path, key, val in _strings(body):
        if is_secret_key(key):
            assert val in ("", MASK), (path, val)
            if val == MASK:
                masked.add(path)
    assert EXPECTED <= masked, sorted(EXPECTED - masked)


def test_every_secret_named_config_string_is_covered(app_env):
    """A new config field named like a secret must show up here masked - the
    stored config has a placeholder in every one of them, so none can slip by."""
    stored = _stored(app_env)
    secret_paths = {p for p, k, v in _strings(stored) if is_secret_key(k)}
    assert EXPECTED <= secret_paths
    body = app_env.client.get("/api/settings").get_json()
    for path, key, val in _strings(body):
        if path in secret_paths:
            assert val == MASK, f"{path} not masked"
    # and no non-secret name is swallowed by accident
    for path, key, val in _strings(body):
        if key in NOT_SECRETS:
            assert val != MASK, f"{path} masked but is an identifier"


def test_empty_secret_stays_empty(app_env):
    app_env.client.put("/api/settings", json={"mqtt": {"password": ""}})
    body = app_env.client.get("/api/settings").get_json()
    assert body["mqtt"]["password"] == ""


# ── PUT ───────────────────────────────────────────────────────────────
@pytest.mark.parametrize("path", sorted(p for p in EXPECTED if "[]" not in p))
def test_put_mask_keeps_stored_value(app_env, path):
    before = _get(app_env, path)
    assert before.startswith("placeholder-")
    sec, field = path.split(".")
    r = app_env.client.put("/api/settings", json={sec: {field: MASK}})
    assert r.get_json()["ok"] is True
    assert _get(app_env, path) == before


def test_put_mask_keeps_device_passwords_even_reordered(app_env):
    body = app_env.client.get("/api/settings").get_json()
    devs = list(reversed(body["devices"]))          # the page may reorder
    assert all(d["password"] == MASK for d in devs)
    assert app_env.client.put("/api/settings", json={"devices": devs}).get_json()["ok"]
    stored = {d["key"]: d["password"] for d in _stored(app_env)["devices"]}
    assert stored == {"dev_a": "placeholder-devices[].password",
                      "dev_b": "placeholder-devices[].password"}


def test_device_password_matched_by_key_not_position(app_env):
    raw = _stored(app_env)
    raw["devices"][0]["password"] = "placeholder-a"
    raw["devices"][1]["password"] = "placeholder-b"
    app_env.path.write_text(json.dumps(raw), encoding="utf-8")
    app_env.state.cfg = load_config(str(app_env.path))
    devs = app_env.client.get("/api/settings").get_json()["devices"]
    devs.reverse()
    app_env.client.put("/api/settings", json={"devices": devs})
    assert {d["key"]: d["password"] for d in _stored(app_env)["devices"]} == \
        {"dev_a": "placeholder-a", "dev_b": "placeholder-b"}


def test_full_round_trip_changes_no_secret(app_env):
    before = _stored(app_env)
    body = app_env.client.get("/api/settings").get_json()
    body = {k: v for k, v in body.items() if not k.startswith("_")}
    assert app_env.client.put("/api/settings", json=body).get_json()["ok"]
    after = _stored(app_env)
    for path, key, val in _strings(before):
        if is_secret_key(key):
            assert val.startswith("placeholder-")
    assert {p: v for p, k, v in _strings(after) if is_secret_key(k)} == \
        {p: v for p, k, v in _strings(before) if is_secret_key(k)}
    assert MASK not in app_env.path.read_text(encoding="utf-8")


def test_put_empty_string_deletes_on_purpose(app_env):
    app_env.client.put("/api/settings", json={"mqtt": {"password": ""}})
    assert _get(app_env, "mqtt.password") == ""


def test_put_new_value_replaces(app_env):
    app_env.client.put("/api/settings", json={"mqtt": {"password": "placeholder-new"}})
    assert _get(app_env, "mqtt.password") == "placeholder-new"


def test_mask_without_stored_value_is_never_written(app_env):
    app_env.client.put("/api/settings", json={"mqtt": {"password": ""}})
    app_env.client.put("/api/settings", json={"mqtt": {"password": MASK}})
    assert _get(app_env, "mqtt.password") == ""
    devs = [{"key": "dev_new", "name": "N", "host": "10.0.0.9", "password": MASK}]
    app_env.client.put("/api/settings", json={"devices": devs})
    assert _stored(app_env)["devices"][0]["password"] == ""
    assert MASK not in app_env.path.read_text(encoding="utf-8")


def test_restore_masked_unit():
    merged = {"a": {"token": MASK, "name": "x"}, "items": [{"id": "1", "secret": MASK}, {"id": "9", "secret": MASK}]}
    stored = {"a": {"token": "t"}, "items": [{"id": "1", "secret": "s"}]}
    restore_masked(merged, stored)
    assert merged["a"]["token"] == "t"
    assert merged["items"][0]["secret"] == "s" and merged["items"][1]["secret"] == ""


def test_mask_secrets_unit_does_not_touch_input():
    src = {"x": {"password": "p"}, "l": [{"api_key": "k", "device_key": "d"}], "token": ""}
    out = mask_secrets(src)
    assert out == {"x": {"password": MASK}, "l": [{"api_key": MASK, "device_key": "d"}], "token": ""}
    assert src["x"]["password"] == "p"


# ── the link token is shown once, when the app makes it up ────────────
def test_generated_link_token_is_shown_once(app_env):
    app_env.client.put("/api/settings", json={"ev_charging": {"link_token": ""}})
    r = app_env.client.put("/api/settings", json={"ev_charging": {"link_enabled": True, "link_token": ""}})
    j = r.get_json()
    tok = j["generated"]["ev_charging.link_token"]
    assert tok and _get(app_env, "ev_charging.link_token") == tok
    assert app_env.client.get("/api/settings").get_json()["ev_charging"]["link_token"] == MASK
    j2 = app_env.client.put("/api/settings", json={"ev_charging": {"link_token": MASK}}).get_json()
    assert "generated" not in j2 and _get(app_env, "ev_charging.link_token") == tok


# ── other endpoints that print configuration ──────────────────────────
def test_devices_endpoint_has_no_password(app_env):
    body = app_env.client.get("/api/devices").get_json()
    assert "placeholder-" not in json.dumps(body)
    assert all(d["has_password"] is True for d in body["devices"])


def test_api_v1_and_config_responses_carry_no_secret(app_env):
    from shelly_analyzer.services import api_v1
    from shelly_analyzer.web.app_context import AppState
    cfg = app_env.state.cfg
    for path in ("/api/v1/devices", "/api/v1/status"):
        out = api_v1.handle_v1_request(path, {}, None, cfg)
        assert "placeholder-" not in json.dumps(out), path
    resp = AppState.get_config_response(SimpleNamespace(
        cfg=cfg, window_minutes=10, refresh_seconds=1, available_windows=[10],
        devices_meta=[], lang="en"))
    assert "placeholder-" not in json.dumps(resp)


# ── the page and the texts ────────────────────────────────────────────
def test_settings_page_keeps_masked_fields_working():
    src = open(os.path.join(os.path.dirname(__file__), "..", "src", "shelly_analyzer",
                            "web", "templates", "settings.html"), encoding="utf-8").read()
    assert 'el.value === "***"' in src and "j.generated" in src
    assert "settings.secret.shown_once" in src


def test_every_language_has_the_shown_once_text():
    from shelly_analyzer import i18n
    en = i18n.t("en", "settings.secret.shown_once")
    for lang in i18n.LANGS:
        txt = i18n.t(lang, "settings.secret.shown_once")
        assert txt and txt != "settings.secret.shown_once", lang
        if lang != "en":
            assert txt != en, ("still English", lang)
