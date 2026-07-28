"""Manufacturer presets for the external PV / battery / grid data source.

A new user with a PV system (Huawei, SolarEdge, Fronius, SMA, Kostal, Sungrow,
GoodWe, …) should be able to pick their system from a dropdown and get every
field pre-filled, rather than hunting for entity names, register maps, ports and
sign conventions.

Each preset lists the connection methods that work for that system:

    "modbus"        – read the inverter/dongle/energy-manager directly over
                      Modbus TCP.  **No Home Assistant required.**
    "http"          – read a vendor local HTTP API (e.g. the Fronius Solar API).
                      **No Home Assistant required.**
    "homeassistant" – read the system's HA entities (works with any inverter
                      already integrated in HA).
    "mqtt"          – subscribe to broker topics (e.g. a Victron Cerbo GX, or a
                      user-built bridge).

``default_method`` is the recommended no-HA path where one exists, so the very
common case (someone who does NOT run Home Assistant) works out of the box.

The catalog is intentionally data-only so it can be served to the settings UI
(:func:`public_catalog`) for client-side auto-fill, while the Modbus register
maps referenced by ``modbus.register_map`` live in :mod:`pv_modbus` and the HTTP
dialects in :mod:`pv_http`.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# The catalog. Entity ids are the DEFAULTS of the respective official HA
# integration; they are hints the user can adjust. Modbus ports/units and signs
# are the documented defaults for each system. Everything can be verified live
# with the "Test connection" button before the source is trusted.
# ---------------------------------------------------------------------------
PRESETS: List[Dict[str, Any]] = [
    {
        "id": "huawei_sun2000",
        "manufacturer": "Huawei",
        "model": "SUN2000 + LUNA2000 (SDongle / EMMA)",
        "methods": ["modbus", "homeassistant"],
        "default_method": "modbus",
        "battery_power_sign": "charge_positive",
        "grid_power_sign": "import_positive",
        "modbus": {"port": 502, "unit_id": 1, "register_map": "huawei"},
        "ha": {
            "pv_power_entity": "sensor.inverter_input_power",
            "battery_power_entity": "sensor.batteries_charge_discharge_power",
            "battery_soc_entity": "sensor.batteries_state_of_capacity",
            "grid_power_entity": "sensor.power_meter_active_power",
        },
        "note": "Enable 'Modbus TCP' in the FusionSolar app (SDongle) or on the "
                "EMMA, and whitelist the analyzer's IP. Unit id is usually 1 for "
                "the SDongle; an EMMA may use a different slave id.",
    },
    {
        "id": "solaredge",
        "manufacturer": "SolarEdge",
        "model": "HD-Wave / Energy Hub (+ StorEdge battery)",
        "methods": ["modbus", "homeassistant"],
        "default_method": "modbus",
        "battery_power_sign": "charge_positive",
        "grid_power_sign": "import_positive",
        "modbus": {"port": 1502, "unit_id": 1, "register_map": "sunspec"},
        "ha": {
            "pv_power_entity": "sensor.solaredge_ac_power",
            "battery_power_entity": "sensor.solaredge_b1_dc_power",
            "battery_soc_entity": "sensor.solaredge_b1_state_of_energy",
            "grid_power_entity": "sensor.solaredge_m1_ac_power",
        },
        "note": "Enable 'Modbus TCP' in the inverter's SetApp/LCD (port 1502). "
                "SunSpec map. The grid meter is meter block M1.",
    },
    {
        "id": "fronius",
        "manufacturer": "Fronius",
        "model": "Symo / Gen24 / Primo (+ BYD HV battery)",
        "methods": ["http", "modbus", "homeassistant"],
        "default_method": "http",
        # Fronius Solar API: P_Akku is + = discharging, − = charging.
        "battery_power_sign": "discharge_positive",
        "grid_power_sign": "import_positive",
        "http": {"http_kind": "fronius_solar_api", "http_base_url_hint": "http://192.168.1.50"},
        "modbus": {"port": 502, "unit_id": 1, "register_map": "sunspec"},
        "ha": {
            "pv_power_entity": "sensor.solarnet_power_photovoltaics",
            "battery_power_entity": "sensor.solarnet_power_battery",
            "battery_soc_entity": "sensor.solarnet_state_of_charge",
            "grid_power_entity": "sensor.solarnet_power_grid",
        },
        "note": "The local Solar API needs no key — just the inverter's IP. "
                "Gen24 also speaks SunSpec Modbus TCP (enable it in the web UI).",
    },
    {
        "id": "sma",
        "manufacturer": "SMA",
        "model": "Sunny Boy / Tripower (+ Sunny Boy Storage)",
        "methods": ["modbus", "homeassistant"],
        "default_method": "modbus",
        "battery_power_sign": "charge_positive",
        "grid_power_sign": "import_positive",
        "modbus": {"port": 502, "unit_id": 3, "register_map": "sunspec"},
        "ha": {
            "pv_power_entity": "sensor.sma_grid_power",
            "battery_power_entity": "sensor.sma_battery_power",
            "battery_soc_entity": "sensor.sma_battery_soc_total",
            "grid_power_entity": "sensor.sma_metering_power_absorbed",
        },
        "note": "Enable 'Modbus Server (TCP)' in the SMA web UI (Speedwire). "
                "The inverter's unit id is commonly 3. SunSpec map.",
    },
    {
        "id": "kostal",
        "manufacturer": "Kostal",
        "model": "Plenticore plus / BYD battery",
        "methods": ["modbus", "homeassistant"],
        "default_method": "modbus",
        "battery_power_sign": "charge_positive",
        "grid_power_sign": "import_positive",
        "modbus": {"port": 1502, "unit_id": 71, "register_map": "sunspec"},
        "ha": {
            "pv_power_entity": "sensor.scb_pv_power_total",
            "battery_power_entity": "sensor.scb_battery_power",
            "battery_soc_entity": "sensor.scb_battery_soc",
            "grid_power_entity": "sensor.scb_grid_power",
        },
        "note": "Enable Modbus/SunSpec (TCP) in the Plenticore web UI (port 1502, "
                "unit id 71). Grid sign may need flipping — verify with Test.",
    },
    {
        "id": "sungrow",
        "manufacturer": "Sungrow",
        "model": "SH-RS / SH-RT hybrid",
        "methods": ["modbus", "homeassistant"],
        "default_method": "modbus",
        "battery_power_sign": "charge_positive",
        "grid_power_sign": "import_positive",
        "modbus": {"port": 502, "unit_id": 1, "register_map": "sunspec"},
        "ha": {
            "pv_power_entity": "sensor.total_dc_power",
            "battery_power_entity": "sensor.battery_power",
            "battery_soc_entity": "sensor.battery_level",
            "grid_power_entity": "sensor.export_power_raw",
        },
        "note": "SH hybrids expose SunSpec over Modbus TCP (port 502). If your "
                "model uses the Sungrow-native map instead, use Home Assistant.",
    },
    {
        "id": "goodwe",
        "manufacturer": "GoodWe",
        "model": "ET / EH / BH hybrid (+ Lynx battery)",
        "methods": ["modbus", "homeassistant"],
        "default_method": "modbus",
        "battery_power_sign": "charge_positive",
        "grid_power_sign": "import_positive",
        "modbus": {"port": 502, "unit_id": 247, "register_map": "sunspec"},
        "ha": {
            "pv_power_entity": "sensor.pv_power",
            "battery_power_entity": "sensor.battery_power",
            "battery_soc_entity": "sensor.battery_state_of_charge",
            "grid_power_entity": "sensor.active_power",
        },
        "note": "Hybrid GoodWe inverters answer Modbus TCP on port 502 "
                "(unit id 247). Verify sign/scale with Test.",
    },
    {
        "id": "victron",
        "manufacturer": "Victron",
        "model": "Cerbo GX / MultiPlus-II / GX device",
        "methods": ["mqtt", "homeassistant"],
        "default_method": "mqtt",
        "battery_power_sign": "charge_positive",
        "grid_power_sign": "import_positive",
        "mqtt": {
            "mqtt_pv_power_topic": "N/<vrm-id>/system/0/Dc/Pv/Power",
            "mqtt_battery_power_topic": "N/<vrm-id>/system/0/Dc/Battery/Power",
            "mqtt_battery_soc_topic": "N/<vrm-id>/system/0/Dc/Battery/Soc",
            "mqtt_grid_power_topic": "N/<vrm-id>/system/0/Ac/Grid/L1/Power",
            "mqtt_json_path": "value",
        },
        "ha": {
            "pv_power_entity": "sensor.victron_system_pv_power",
            "battery_power_entity": "sensor.victron_system_battery_power",
            "battery_soc_entity": "sensor.victron_system_battery_soc",
            "grid_power_entity": "sensor.victron_system_grid_power",
        },
        "note": "The Cerbo GX has a built-in MQTT broker — point the analyzer's "
                "MQTT settings at it and replace <vrm-id> with your portal id. "
                "Victron battery power is + = charge.",
    },
    {
        "id": "solax",
        "manufacturer": "SolaX",
        "model": "X1 / X3 hybrid (+ Triple Power battery)",
        "methods": ["modbus", "homeassistant"],
        "default_method": "modbus",
        "battery_power_sign": "charge_positive",
        "grid_power_sign": "import_positive",
        "modbus": {"port": 502, "unit_id": 1, "register_map": "sunspec"},
        "ha": {
            "pv_power_entity": "sensor.solax_pv_power_total",
            "battery_power_entity": "sensor.solax_battery_power_charge",
            "battery_soc_entity": "sensor.solax_battery_capacity",
            "grid_power_entity": "sensor.solax_grid_power",
        },
        "note": "SolaX hybrids answer Modbus TCP on the Pocket LAN/WiFi module "
                "(port 502). If unreliable, prefer Home Assistant.",
    },
    {
        "id": "enphase",
        "manufacturer": "Enphase",
        "model": "IQ Envoy / IQ Battery (microinverters)",
        "methods": ["homeassistant"],
        "default_method": "homeassistant",
        "battery_power_sign": "charge_positive",
        "grid_power_sign": "import_positive",
        "ha": {
            "pv_power_entity": "sensor.envoy_current_power_production",
            "battery_power_entity": "sensor.envoy_battery_power",
            "battery_soc_entity": "sensor.envoy_battery",
            "grid_power_entity": "sensor.envoy_current_net_power_consumption",
        },
        "note": "Recent Envoy firmware gates the local API behind a token, so "
                "Home Assistant's Enphase integration is the reliable path.",
    },
    {
        "id": "generic_sunspec",
        "manufacturer": "Generic",
        "model": "Any SunSpec-certified inverter (Modbus TCP)",
        "methods": ["modbus", "homeassistant"],
        "default_method": "modbus",
        "battery_power_sign": "charge_positive",
        "grid_power_sign": "import_positive",
        "modbus": {"port": 502, "unit_id": 1, "register_map": "sunspec"},
        "ha": {},
        "note": "Auto-discovers the SunSpec model blocks. Set host, port and "
                "unit id, then use Test connection to confirm the readings.",
    },
    {
        "id": "manual",
        "manufacturer": "Other / Manual",
        "model": "Configure fields manually",
        "methods": ["homeassistant", "mqtt", "modbus", "http"],
        "default_method": "homeassistant",
        "battery_power_sign": "charge_positive",
        "grid_power_sign": "import_positive",
        "ha": {},
        "note": "No preset — fill the fields for your chosen method yourself.",
    },
]

# Fields that a preset may pre-fill, grouped by method, so applying a preset can
# also CLEAR the fields that belong to the other methods (avoids stale values).
_HA_FIELDS = ("pv_power_entity", "battery_power_entity", "battery_soc_entity",
              "grid_power_entity", "house_power_entity", "pv_energy_today_entity")
_MQTT_FIELDS = ("mqtt_pv_power_topic", "mqtt_battery_power_topic",
                "mqtt_battery_soc_topic", "mqtt_grid_power_topic",
                "mqtt_house_power_topic", "mqtt_json_path")
_MODBUS_FIELDS = ("modbus_host", "modbus_port", "modbus_unit_id", "modbus_register_map")
_HTTP_FIELDS = ("http_kind", "http_base_url", "http_api_key", "http_site_id")


def get_preset(preset_id: str) -> Optional[Dict[str, Any]]:
    for p in PRESETS:
        if p.get("id") == preset_id:
            return p
    return None


def public_catalog() -> List[Dict[str, Any]]:
    """The catalog as served to the settings UI (a plain, JSON-safe copy)."""
    return [dict(p) for p in PRESETS]


def apply_preset(cfg_dict: Dict[str, Any], preset_id: str,
                 method: Optional[str] = None) -> Dict[str, Any]:
    """Return the ``pv_source`` field updates for selecting *preset_id* with the
    given connection *method* (default: the preset's ``default_method``).

    Fills the chosen method's fields from the preset and blanks the other
    methods' fields, plus sets the sign conventions and ``preset_id``. Does not
    touch ``enabled`` / ``poll_interval_seconds`` / the exact-energy counters.
    Pure function — the caller merges the result into the live config.
    """
    preset = get_preset(preset_id)
    out: Dict[str, Any] = dict(cfg_dict or {})
    if preset is None:
        out["preset_id"] = preset_id
        return out
    method = method or preset.get("default_method") or "homeassistant"
    if method not in preset.get("methods", []):
        method = preset.get("default_method") or "homeassistant"

    out["preset_id"] = preset_id
    out["source_type"] = method
    out["battery_power_sign"] = preset.get("battery_power_sign", "charge_positive")
    out["grid_power_sign"] = preset.get("grid_power_sign", "import_positive")

    # Clear every method's fields, then fill the selected one.
    for f in (_HA_FIELDS + _MQTT_FIELDS + _MODBUS_FIELDS + _HTTP_FIELDS):
        if f in ("modbus_port", "modbus_unit_id"):
            continue  # numeric — leave existing unless the preset overrides
        out[f] = ""
    out["modbus_register_map"] = "sunspec"
    out["http_kind"] = "fronius_solar_api"

    if method == "homeassistant":
        for f, v in (preset.get("ha") or {}).items():
            out[f] = v
    elif method == "mqtt":
        for f, v in (preset.get("mqtt") or {}).items():
            out[f] = v
    elif method == "modbus":
        mb = preset.get("modbus") or {}
        out["modbus_port"] = int(mb.get("port", 502))
        out["modbus_unit_id"] = int(mb.get("unit_id", 1))
        out["modbus_register_map"] = str(mb.get("register_map", "sunspec"))
    elif method == "http":
        ht = preset.get("http") or {}
        out["http_kind"] = str(ht.get("http_kind", "fronius_solar_api"))
        # base URL is site-specific — leave for the user (hint shown in UI)
    return out
