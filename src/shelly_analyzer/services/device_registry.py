"""Comprehensive Shelly device registry.

Maps every known Shelly hardware model to its capabilities so discovery,
settings UI, and sync logic can identify devices correctly.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple


@dataclass(frozen=True)
class ShellyModel:
    model_id: str
    name: str
    gen: int  # 1, 2, 3, 4
    series: str  # "classic", "plus", "pro", "gen3", "gen4", "blu", "wave"
    category: str  # "energy_meter", "switch", "plug", "dimmer", "rgbw", "cover", "sensor", "display", "other"
    has_power_metering: bool
    phases: int  # 1 or 3 (only >1 for dedicated energy meters)
    channels: int  # number of outputs / metering channels
    has_emdata: bool  # supports historical CSV / EMData RPC
    mdns_prefix: str  # mDNS hostname prefix (before -XXXXXXXXXXXX)


# ---------------------------------------------------------------------------
# Complete Shelly device catalog
# Sources: aioshelly/const.py, Shelly KB, Shelly API docs
# ---------------------------------------------------------------------------

_REGISTRY: List[ShellyModel] = [
    # ======================================================================
    # GEN 1 — Classic
    # ======================================================================
    # Switches / Relays
    ShellyModel("SHSW-1", "Shelly 1", 1, "classic", "switch", False, 1, 1, False, "shelly1"),
    ShellyModel("SHSW-L", "Shelly 1L", 1, "classic", "switch", True, 1, 1, False, "shelly1l"),
    ShellyModel("SHSW-PM", "Shelly 1PM", 1, "classic", "switch", True, 1, 1, False, "shelly1pm"),
    ShellyModel("SHSW-21", "Shelly 2", 1, "classic", "switch", True, 1, 2, False, "shelly2"),
    ShellyModel("SHSW-25", "Shelly 2.5", 1, "classic", "switch", True, 1, 2, False, "shelly25"),
    ShellyModel("SHSW-44", "Shelly 4Pro", 1, "classic", "switch", True, 1, 4, False, "shelly4pro"),
    ShellyModel("SHUNI-1", "Shelly UNI", 1, "classic", "switch", False, 1, 2, False, "shellyuni"),
    ShellyModel("SHIX3-1", "Shelly i3", 1, "classic", "switch", False, 1, 3, False, "shellyix3"),
    # Energy Meters
    ShellyModel("SHEM", "Shelly EM", 1, "classic", "energy_meter", True, 1, 2, True, "shellyem"),
    ShellyModel("SHEM-3", "Shelly 3EM", 1, "classic", "energy_meter", True, 3, 3, True, "shellyem3"),
    # Plugs
    ShellyModel("SHPLG-1", "Shelly Plug", 1, "classic", "plug", True, 1, 1, False, "shellyplug"),
    ShellyModel("SHPLG2-1", "Shelly Plug E", 1, "classic", "plug", True, 1, 1, False, "shellypluge"),
    ShellyModel("SHPLG-S", "Shelly Plug S", 1, "classic", "plug", True, 1, 1, False, "shellyplug-s"),
    ShellyModel("SHPLG-U1", "Shelly Plug US", 1, "classic", "plug", True, 1, 1, False, "shellyplug-u1"),
    # Dimmers
    ShellyModel("SHDM-1", "Shelly Dimmer", 1, "classic", "dimmer", True, 1, 1, False, "shellydimmer"),
    ShellyModel("SHDM-2", "Shelly Dimmer 2", 1, "classic", "dimmer", True, 1, 1, False, "shellydimmer2"),
    ShellyModel("SHDIMW-1", "Shelly Dimmer W1", 1, "classic", "dimmer", False, 1, 1, False, "shellydimmerw1"),
    # RGBW / Lighting
    ShellyModel("SHRGBW2", "Shelly RGBW2", 1, "classic", "rgbw", True, 1, 4, False, "shellyrgbw2"),
    ShellyModel("SHBLB-1", "Shelly Bulb", 1, "classic", "rgbw", True, 1, 1, False, "shellybulb"),
    ShellyModel("SHCB-1", "Shelly Bulb RGBW", 1, "classic", "rgbw", True, 1, 1, False, "shellycolorbulb"),
    ShellyModel("SHBDUO-1", "Shelly DUO", 1, "classic", "rgbw", True, 1, 1, False, "shellybulbduo"),
    ShellyModel("SHVIN-1", "Shelly Vintage", 1, "classic", "rgbw", True, 1, 1, False, "shellyvintage"),
    # Sensors
    ShellyModel("SHHT-1", "Shelly H&T", 1, "classic", "sensor", False, 1, 0, False, "shellyht"),
    ShellyModel("SHWT-1", "Shelly Flood", 1, "classic", "sensor", False, 1, 0, False, "shellyflood"),
    ShellyModel("SHSM-01", "Shelly Smoke", 1, "classic", "sensor", False, 1, 0, False, "shellysmoke"),
    ShellyModel("SHSM-02", "Shelly Smoke 2", 1, "classic", "sensor", False, 1, 0, False, "shellysmoke2"),
    ShellyModel("SHMOS-01", "Shelly Motion", 1, "classic", "sensor", False, 1, 0, False, "shellymotion"),
    ShellyModel("SHMOS-02", "Shelly Motion 2", 1, "classic", "sensor", False, 1, 0, False, "shellymotion2"),
    ShellyModel("SHGS-1", "Shelly Gas", 1, "classic", "sensor", False, 1, 0, False, "shellygas"),
    ShellyModel("SHDW-1", "Shelly Door/Window", 1, "classic", "sensor", False, 1, 0, False, "shellydw"),
    ShellyModel("SHDW-2", "Shelly Door/Window 2", 1, "classic", "sensor", False, 1, 0, False, "shellydw2"),
    # Other Gen1
    ShellyModel("SHBTN-1", "Shelly Button1", 1, "classic", "other", False, 1, 1, False, "shellybutton1"),
    ShellyModel("SHBTN-2", "Shelly Button1 v2", 1, "classic", "other", False, 1, 1, False, "shellybutton1"),
    ShellyModel("SHTRV-01", "Shelly TRV", 1, "classic", "other", False, 1, 1, False, "shellytrv"),

    # ======================================================================
    # GEN 2 — Plus series
    # ======================================================================
    # Switches / Relays
    ShellyModel("SNSW-001X16EU", "Shelly Plus 1", 2, "plus", "switch", False, 1, 1, False, "shellyplus1"),
    ShellyModel("SNSW-001X8EU", "Shelly Plus 1 Mini", 2, "plus", "switch", False, 1, 1, False, "shellyplus1mini"),
    ShellyModel("SNSW-001P16EU", "Shelly Plus 1PM", 2, "plus", "switch", True, 1, 1, False, "shellyplus1pm"),
    ShellyModel("SNSW-001P8EU", "Shelly Plus 1PM Mini", 2, "plus", "switch", True, 1, 1, False, "shellyplus1pmmini"),
    ShellyModel("SNSW-002P16EU", "Shelly Plus 2PM", 2, "plus", "switch", True, 1, 2, False, "shellyplus2pm"),
    ShellyModel("SNSW-102P16EU", "Shelly Plus 2PM v2", 2, "plus", "switch", True, 1, 2, False, "shellyplus2pm"),
    ShellyModel("SNSN-0024X", "Shelly Plus I4", 2, "plus", "switch", False, 1, 4, False, "shellyplusi4"),
    ShellyModel("SNSN-0D24X", "Shelly Plus I4 DC", 2, "plus", "switch", False, 1, 4, False, "shellyplusi4dc"),
    ShellyModel("SNSN-0043X", "Shelly Plus Uni", 2, "plus", "switch", False, 1, 2, False, "shellyplusuni"),
    # Energy Meters
    ShellyModel("SNPM-001PCEU16", "Shelly Plus PM Mini", 2, "plus", "energy_meter", True, 1, 1, False, "shellyplusPMMini"),
    # Plugs
    ShellyModel("SNPL-00112EU", "Shelly Plus Plug S", 2, "plus", "plug", True, 1, 1, False, "shellyplusPLUGS"),
    ShellyModel("SNPL-10112EU", "Shelly Plus Plug S v2", 2, "plus", "plug", True, 1, 1, False, "shellyplusPLUGS"),
    ShellyModel("SNPL-00110IT", "Shelly Plus Plug IT", 2, "plus", "plug", True, 1, 1, False, "shellyplusplugit"),
    ShellyModel("SNPL-00112UK", "Shelly Plus Plug UK", 2, "plus", "plug", True, 1, 1, False, "shellypluspluguk"),
    ShellyModel("SNPL-00116US", "Shelly Plus Plug US", 2, "plus", "plug", True, 1, 1, False, "shellyplusplugus"),
    # Dimmers
    ShellyModel("SNDM-00100WW", "Shelly Plus 0-10V Dimmer", 2, "plus", "dimmer", False, 1, 1, False, "shellyplus010v"),
    ShellyModel("SNDM-0013US", "Shelly Plus Wall Dimmer", 2, "plus", "dimmer", False, 1, 1, False, "shellypluswalldimmer"),
    # RGBW
    ShellyModel("SNDC-0D4P10WW", "Shelly Plus RGBW PM", 2, "plus", "rgbw", True, 1, 4, False, "shellyplusrgbwpm"),
    # Sensors
    ShellyModel("SNSN-0013A", "Shelly Plus H&T", 2, "plus", "sensor", False, 1, 0, False, "shellyplusht"),
    ShellyModel("SNSN-0031Z", "Shelly Plus Smoke", 2, "plus", "sensor", False, 1, 0, False, "shellyplussmoke"),

    # ======================================================================
    # GEN 2 — Pro series (DIN rail)
    # ======================================================================
    # Switches / Relays
    ShellyModel("SPSW-001XE16EU", "Shelly Pro 1", 2, "pro", "switch", False, 1, 1, False, "shellypro1"),
    ShellyModel("SPSW-101XE16EU", "Shelly Pro 1 v2", 2, "pro", "switch", False, 1, 1, False, "shellypro1"),
    ShellyModel("SPSW-201XE16EU", "Shelly Pro 1 v3", 2, "pro", "switch", False, 1, 1, False, "shellypro1"),
    ShellyModel("SPSW-001PE16EU", "Shelly Pro 1PM", 2, "pro", "switch", True, 1, 1, False, "shellypro1pm"),
    ShellyModel("SPSW-101PE16EU", "Shelly Pro 1PM v2", 2, "pro", "switch", True, 1, 1, False, "shellypro1pm"),
    ShellyModel("SPSW-201PE16EU", "Shelly Pro 1PM v3", 2, "pro", "switch", True, 1, 1, False, "shellypro1pm"),
    ShellyModel("SPSW-002XE16EU", "Shelly Pro 2", 2, "pro", "switch", False, 1, 2, False, "shellypro2"),
    ShellyModel("SPSW-102XE16EU", "Shelly Pro 2 v2", 2, "pro", "switch", False, 1, 2, False, "shellypro2"),
    ShellyModel("SPSW-202XE16EU", "Shelly Pro 2 v3", 2, "pro", "switch", False, 1, 2, False, "shellypro2"),
    ShellyModel("SPSW-002PE16EU", "Shelly Pro 2PM", 2, "pro", "switch", True, 1, 2, False, "shellypro2pm"),
    ShellyModel("SPSW-102PE16EU", "Shelly Pro 2PM v2", 2, "pro", "switch", True, 1, 2, False, "shellypro2pm"),
    ShellyModel("SPSW-202PE16EU", "Shelly Pro 2PM v3", 2, "pro", "switch", True, 1, 2, False, "shellypro2pm"),
    ShellyModel("SPSW-003XE16EU", "Shelly Pro 3", 2, "pro", "switch", False, 1, 3, False, "shellypro3"),
    ShellyModel("SPSW-004PE16EU", "Shelly Pro 4PM", 2, "pro", "switch", True, 1, 4, False, "shellypro4pm"),
    ShellyModel("SPSW-104PE16EU", "Shelly Pro 4PM v2", 2, "pro", "switch", True, 1, 4, False, "shellypro4pm"),
    ShellyModel("SPSW-204PE16EU", "Shelly Pro 4PM v3", 2, "pro", "switch", True, 1, 4, False, "shellypro4pm"),
    # Energy Meters
    ShellyModel("SPEM-002CEBEU50", "Shelly Pro EM-50", 2, "pro", "energy_meter", True, 1, 2, True, "shellyproem"),
    ShellyModel("SPEM-003CEBEU", "Shelly Pro 3EM", 2, "pro", "energy_meter", True, 3, 3, True, "shellypro3em"),
    ShellyModel("SPEM-003CEBEU120", "Shelly Pro 3EM (120A)", 2, "pro", "energy_meter", True, 3, 3, True, "shellypro3em"),
    ShellyModel("SPEM-003CEBEU63", "Shelly Pro 3EM 3CT63", 2, "pro", "energy_meter", True, 3, 3, True, "shellypro3em"),
    ShellyModel("SPEM-003CEBEU400", "Shelly Pro 3EM-400", 2, "pro", "energy_meter", True, 3, 3, True, "shellypro3em400"),
    # Cover
    ShellyModel("SPSH-002PE16EU", "Shelly Pro Dual Cover PM", 2, "pro", "cover", True, 1, 2, False, "shellyprodualcoverpm"),
    # Dimmers
    ShellyModel("SPDM-001PE01EU", "Shelly Pro Dimmer 1PM", 2, "pro", "dimmer", True, 1, 1, False, "shellyprodimmer1pm"),
    ShellyModel("SPDM-002PE01EU", "Shelly Pro Dimmer 2PM", 2, "pro", "dimmer", True, 1, 2, False, "shellyprodimmer2pm"),
    # RGBW
    ShellyModel("SPDC-0D5PE16EU", "Shelly Pro RGBWW PM", 2, "pro", "rgbw", True, 1, 5, False, "shellyprorgbwwpm"),
    # Displays
    ShellyModel("SAWD-0A1XX10EU1", "Shelly Wall Display", 2, "pro", "display", False, 1, 1, False, "shellywalldisplay"),

    # ======================================================================
    # GEN 3
    # ======================================================================
    # Switches / Relays
    ShellyModel("S3SW-001X16EU", "Shelly 1 Gen3", 3, "gen3", "switch", False, 1, 1, False, "shelly1g3"),
    ShellyModel("S3SW-001X8EU", "Shelly 1 Mini Gen3", 3, "gen3", "switch", False, 1, 1, False, "shelly1minig3"),
    ShellyModel("S3SW-001P16EU", "Shelly 1PM Gen3", 3, "gen3", "switch", True, 1, 1, False, "shelly1pmg3"),
    ShellyModel("S3SW-001P8EU", "Shelly 1PM Mini Gen3", 3, "gen3", "switch", True, 1, 1, False, "shelly1pmminig3"),
    ShellyModel("S3SW-002P16EU", "Shelly 2PM Gen3", 3, "gen3", "switch", True, 1, 2, False, "shelly2pmg3"),
    ShellyModel("S3SW-0A1X1EUL", "Shelly 1L Gen3", 3, "gen3", "switch", False, 1, 1, False, "shelly1lg3"),
    ShellyModel("S3SW-0A2X4EUL", "Shelly 2L Gen3", 3, "gen3", "switch", False, 1, 2, False, "shelly2lg3"),
    ShellyModel("S3SN-0024X", "Shelly I4 Gen3", 3, "gen3", "switch", False, 1, 4, False, "shellyi4g3"),
    # Energy Meters
    ShellyModel("S3EM-002CXCEU", "Shelly EM Gen3", 3, "gen3", "energy_meter", True, 1, 2, True, "shellyemg3"),
    ShellyModel("S3EM-003CXCEU63", "Shelly 3EM Gen3", 3, "gen3", "energy_meter", True, 3, 3, True, "shelly3em63g3"),
    ShellyModel("S3PM-001PCEU16", "Shelly PM Mini Gen3", 3, "gen3", "energy_meter", True, 1, 1, False, "shellypmminig3"),
    # Plugs
    ShellyModel("S3PL-00112EU", "Shelly Plug S Gen3", 3, "gen3", "plug", True, 1, 1, False, "shellyplusg3"),
    ShellyModel("S3PL-20112EU", "Shelly Outdoor Plug S Gen3", 3, "gen3", "plug", True, 1, 1, False, "shellyoutplugsg3"),
    ShellyModel("S3PL-30110EU", "Shelly Plug M Gen3", 3, "gen3", "plug", True, 1, 1, False, "shellyplugg3"),
    ShellyModel("S3PL-30116EU", "Shelly Plug PM Gen3", 3, "gen3", "plug", True, 1, 1, False, "shellyplugg3"),
    # Dimmers
    ShellyModel("S3DM-0A1WW", "Shelly DALI Dimmer Gen3", 3, "gen3", "dimmer", False, 1, 1, False, "shellydimmerg3"),
    ShellyModel("S3DM-0010WW", "Shelly Dimmer 0/1-10V PM Gen3", 3, "gen3", "dimmer", True, 1, 1, False, "shellydimmer010vg3"),
    # Lighting
    ShellyModel("S3BL-D010009AEU", "Shelly Duo Bulb Gen3", 3, "gen3", "rgbw", True, 1, 1, False, "shellyduobulbg3"),
    ShellyModel("S3BL-C010007AEU", "Shelly Multicolor Bulb Gen3", 3, "gen3", "rgbw", True, 1, 1, False, "shellymcbulbg3"),
    # Sensors
    ShellyModel("S3SN-0U12A", "Shelly H&T Gen3", 3, "gen3", "sensor", False, 1, 0, False, "shellyhtg3"),
    # Cover
    ShellyModel("S3SH-0A2P4EU", "Shelly Shutter Gen3", 3, "gen3", "cover", True, 1, 1, False, "shellyshutterg3"),
    # Other
    ShellyModel("S3GW-1DBT001", "Shelly BLU Gateway Gen3", 3, "gen3", "other", False, 1, 0, False, "shellyblugatewayg3"),

    # ======================================================================
    # GEN 4
    # ======================================================================
    # Switches / Relays
    ShellyModel("S4SW-001X16EU", "Shelly 1 Gen4", 4, "gen4", "switch", False, 1, 1, False, "shelly1g4"),
    ShellyModel("S4SW-001X8EU", "Shelly 1 Mini Gen4", 4, "gen4", "switch", False, 1, 1, False, "shelly1minig4"),
    ShellyModel("S4SW-001P16EU", "Shelly 1PM Gen4", 4, "gen4", "switch", True, 1, 1, False, "shelly1pmg4"),
    ShellyModel("S4SW-001P8EU", "Shelly 1PM Mini Gen4", 4, "gen4", "switch", True, 1, 1, False, "shelly1pmminig4"),
    ShellyModel("S4SW-002P16EU", "Shelly 2PM Gen4", 4, "gen4", "switch", True, 1, 2, False, "shelly2pmg4"),
    ShellyModel("S4SN-0A24X", "Shelly I4 Gen4", 4, "gen4", "switch", False, 1, 4, False, "shellyi4g4"),
    # Energy Meters
    ShellyModel("S4EM-001PXCEU16", "Shelly EM Mini Gen4", 4, "gen4", "energy_meter", True, 1, 1, False, "shellyemminig4"),
    # Plugs
    ShellyModel("S4PL-00116US", "Shelly Plug US Gen4", 4, "gen4", "plug", True, 1, 1, False, "shellyplususg4"),
    ShellyModel("S4PL-00416EU", "Shelly Power Strip 4 Gen4", 4, "gen4", "plug", True, 1, 4, False, "shellypstripg4"),
    ShellyModel("S4PL-10416EU", "Shelly Power Strip 4 Gen4 v2", 4, "gen4", "plug", True, 1, 4, False, "shellypstripg4"),
    # Dimmers
    ShellyModel("S4DM-0A101WWL", "Shelly Dimmer Gen4", 4, "gen4", "dimmer", False, 1, 1, False, "shellydimmerg4"),
    # Sensors
    ShellyModel("S4SN-0071A", "Shelly Flood Gen4", 4, "gen4", "sensor", False, 1, 0, False, "shellyfloodg4"),
    ShellyModel("S4SN-0U61X", "Shelly Presence Gen4", 4, "gen4", "sensor", False, 1, 0, False, "shellypresenceg4"),
]

# ---------------------------------------------------------------------------
# Lookup indices (built once at import time)
# ---------------------------------------------------------------------------

_BY_MODEL_ID: Dict[str, ShellyModel] = {}
_BY_MDNS_PREFIX: Dict[str, ShellyModel] = {}

for _m in _REGISTRY:
    _BY_MODEL_ID[_m.model_id] = _m
    upper = _m.model_id.upper()
    if upper != _m.model_id:
        _BY_MODEL_ID[upper] = _m
    if _m.mdns_prefix and _m.mdns_prefix not in _BY_MDNS_PREFIX:
        _BY_MDNS_PREFIX[_m.mdns_prefix.lower()] = _m


def lookup_by_model_id(model_id: str) -> Optional[ShellyModel]:
    """Look up a device by its hardware model ID (e.g. 'SPEM-003CEBEU')."""
    if not model_id:
        return None
    model_id = model_id.strip()
    hit = _BY_MODEL_ID.get(model_id) or _BY_MODEL_ID.get(model_id.upper())
    if hit:
        return hit
    # Fuzzy: some models have regional suffixes not in our list — try prefix match
    upper = model_id.upper()
    for known_id, m in _BY_MODEL_ID.items():
        if upper.startswith(known_id.upper()) or known_id.upper().startswith(upper):
            return m
    return None


def lookup_by_mdns(hostname: str) -> Optional[ShellyModel]:
    """Look up a device by its mDNS hostname (e.g. 'shellypro3em-AABBCCDDEEFF')."""
    if not hostname:
        return None
    h = hostname.lower().strip()
    # Strip the MAC suffix (after the last hyphen, if it's a hex string)
    parts = h.rsplit("-", 1)
    if len(parts) == 2 and len(parts[1]) >= 6:
        try:
            int(parts[1], 16)
            h = parts[0]
        except ValueError:
            pass
    return _BY_MDNS_PREFIX.get(h)


def get_all_models() -> List[ShellyModel]:
    """Return the full registry sorted by generation then name."""
    return sorted(_REGISTRY, key=lambda m: (m.gen, m.name))


def get_supported_summary() -> List[Dict]:
    """Return a JSON-serializable summary grouped by generation/series."""
    result = []
    for m in get_all_models():
        result.append({
            "model_id": m.model_id,
            "name": m.name,
            "gen": m.gen,
            "series": m.series,
            "category": m.category,
            "has_power_metering": m.has_power_metering,
            "phases": m.phases,
            "channels": m.channels,
            "has_emdata": m.has_emdata,
        })
    return result


# Category labels for UI display
CATEGORY_LABELS = {
    "energy_meter": "Energy Meter",
    "switch": "Switch / Relay",
    "plug": "Plug",
    "dimmer": "Dimmer",
    "rgbw": "RGBW / Lighting",
    "cover": "Cover / Shutter",
    "sensor": "Sensor",
    "display": "Display",
    "other": "Other",
}

SERIES_LABELS = {
    "classic": "Gen 1 (Classic)",
    "plus": "Gen 2 (Plus)",
    "pro": "Gen 2 (Pro)",
    "gen3": "Gen 3",
    "gen4": "Gen 4",
}
