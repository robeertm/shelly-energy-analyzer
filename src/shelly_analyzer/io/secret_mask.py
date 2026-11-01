"""Masking of secrets in configuration that leaves the app over HTTP.

``GET /api/settings`` used to mask a fixed list of fields; every field that was
not on the list (MQTT password, link token, web token, device passwords, ...)
went out in clear text. The rule is turned around here: a field is a secret
when its *name* says so, unless it is on a short list of known non-secrets.
A new setting named ``..._token`` or ``..._password`` is therefore masked from
the day it exists, without anybody having to remember a list.

Masked values are the string ``"***"``. On save a ``"***"`` never replaces a
stored value (``restore_masked``); an empty string stays a deliberate delete.
"""
from __future__ import annotations

from typing import Any, Optional

MASK = "***"

# Name fragments that make a field a secret.
_SECRET_PARTS = (
    "password", "passwd", "passphrase", "secret", "token", "credential",
    "api_key", "apikey", "private_key", "authorization",
)

# Fields whose name looks like a secret but is not one: identifiers of devices
# and meters, and a *path* to a key file (the key itself is never in the config).
_NOT_SECRET = frozenset({
    "key", "device_key", "em_id", "live_web_ssl_key", "ssl_key",
})

# Secrets whose name does not say so: a webhook URL is a bearer secret for
# Slack/Discord-style endpoints, and custom headers carry Authorization.
_SECRET_EXTRA = frozenset({"webhook_url", "webhook_custom_headers", "custom_headers"})

# Keys that identify an entry in a list, to pair it with the stored one on save.
_IDENTITY_KEYS = ("key", "tenant_id", "id", "profile_id", "name")


def is_secret_key(name: Any) -> bool:
    """True when a config field of this name holds a secret."""
    n = str(name or "").strip().lower()
    if not n or n in _NOT_SECRET or n.endswith("device_key"):
        return False
    if n in _SECRET_EXTRA:
        return True
    if any(p in n for p in _SECRET_PARTS):
        return True
    return n.endswith("_key")


def mask_secrets(data: Any) -> Any:
    """A copy of ``data`` (nested dicts/lists) with every set secret as ``"***"``.

    Empty values stay empty, so the page can tell "not set" from "set"."""
    if isinstance(data, dict):
        out = {}
        for k, v in data.items():
            if is_secret_key(k) and isinstance(v, str):
                out[k] = MASK if v else v
            else:
                out[k] = mask_secrets(v)
        return out
    if isinstance(data, list):
        return [mask_secrets(v) for v in data]
    return data


def _match(item: dict, index: int, stored: Any) -> Optional[dict]:
    """The stored list entry that ``item`` is an update of, or None."""
    if not isinstance(stored, list):
        return None
    for idk in _IDENTITY_KEYS:
        if idk in item and item[idk] not in (None, ""):
            for s in stored:
                if isinstance(s, dict) and s.get(idk) == item[idk]:
                    return s
            return None
    if 0 <= index < len(stored) and isinstance(stored[index], dict):
        return stored[index]
    return None


def restore_masked(merged: Any, stored: Any) -> None:
    """In place: wherever ``merged`` carries ``"***"`` in a secret field, put the
    stored value back. ``"***"`` with nothing stored becomes empty - the literal
    placeholder is never written to disk. Walks nested dicts and lists
    (``devices[i].password``); list entries are paired by their identity key."""
    if isinstance(merged, dict):
        stored_d = stored if isinstance(stored, dict) else {}
        for k, v in list(merged.items()):
            if is_secret_key(k) and isinstance(v, str) and v == MASK:
                old = stored_d.get(k)
                merged[k] = old if isinstance(old, str) else ""
            elif isinstance(v, (dict, list)):
                restore_masked(v, stored_d.get(k))
    elif isinstance(merged, list):
        for i, item in enumerate(merged):
            if isinstance(item, dict):
                restore_masked(item, _match(item, i, stored))
            elif isinstance(item, list):
                restore_masked(item, None)

