"""Curated lists of bidding zones for the spot-price and CO₂ settings.

The app talks to several different APIs, each with its own zone naming
convention:

- **Energy-Charts** (Fraunhofer ISE, EU spot prices): hyphen form, e.g.
  ``DE-LU``, ``SE-4``, ``IT-NORD``.
- **ENTSO-E Transparency Platform** (EU CO₂ intensity): underscore form
  matching the EIC-code dict in ``services/entsoe.py``: ``DE_LU``,
  ``SE_4``, ``IT_NORD``.
- **AEMO** (Australian NEM, free): ``AU-NSW``, ``AU-VIC``, ``AU-QLD``,
  ``AU-SA``, ``AU-TAS``.
- **EIA** (US wholesale LMP, API key): ``US-CAL``, ``US-MIDA``, ``US-NE``,
  ``US-NY``, ``US-NW``, ``US-SE``, ``US-SW``, ``US-TEN``, ``US-TEX``,
  ``US-FLA``, ``US-CAR``, ``US-MIDW``, ``US-CENT``.
- **Electricity Maps** (global CO₂ intensity, free API key): ISO-3166
  country codes (``DE``, ``FR``, ``US-CAL-CISO``, ``JP-TK``, ...).

Spot-price zones are namespaced by prefix so the fetch service can
dispatch to the right provider automatically — anything starting with
``US-`` goes to EIA, ``AU-`` to AEMO, the rest to Energy-Charts /
aWATTar. CO₂ zones fall back to Electricity Maps when an API key is
present, otherwise ENTSO-E.
"""
from __future__ import annotations

from typing import Dict, List, Optional, Tuple

# ── Spot-price zones ─────────────────────────────────────────────────

# EU + UK via Energy-Charts (hyphen form, no auth)
SPOT_ZONES_ENERGY_CHARTS: List[Tuple[str, str]] = [
    ("DE-LU",    "Germany + Luxembourg (DE-LU)"),
    ("DE-AT-LU", "DE + AT + LU (hist.)"),
    ("AT",       "Austria (AT)"),
    ("BE",       "Belgium (BE)"),
    ("BG",       "Bulgaria (BG)"),
    ("CH",       "Switzerland (CH)"),
    ("CZ",       "Czech Republic (CZ)"),
    ("DK1",      "Denmark West (DK1)"),
    ("DK2",      "Denmark East (DK2)"),
    ("EE",       "Estonia (EE)"),
    ("ES",       "Spain (ES)"),
    ("FI",       "Finland (FI)"),
    ("FR",       "France (FR)"),
    ("GB",       "Great Britain (GB)"),
    ("GR",       "Greece (GR)"),
    ("HR",       "Croatia (HR)"),
    ("HU",       "Hungary (HU)"),
    ("IE",       "Ireland (IE)"),
    ("IT-CALA",  "Italy – Calabria"),
    ("IT-CNOR",  "Italy – Centre/North"),
    ("IT-CSUD",  "Italy – Centre/South"),
    ("IT-NORD",  "Italy – North"),
    ("IT-SARD",  "Italy – Sardinia"),
    ("IT-SICI",  "Italy – Sicily"),
    ("IT-SUD",   "Italy – South"),
    ("LT",       "Lithuania (LT)"),
    ("LV",       "Latvia (LV)"),
    ("ME",       "Montenegro (ME)"),
    ("MK",       "North Macedonia (MK)"),
    ("NL",       "Netherlands (NL)"),
    ("NO1",      "Norway – Oslo (NO1)"),
    ("NO2",      "Norway – Kristiansand (NO2)"),
    ("NO3",      "Norway – Trondheim (NO3)"),
    ("NO4",      "Norway – Tromso (NO4)"),
    ("NO5",      "Norway – Bergen (NO5)"),
    ("PL",       "Poland (PL)"),
    ("PT",       "Portugal (PT)"),
    ("RO",       "Romania (RO)"),
    ("RS",       "Serbia (RS)"),
    ("SE1",      "Sweden – Lulea (SE1)"),
    ("SE2",      "Sweden – Sundsvall (SE2)"),
    ("SE3",      "Sweden – Stockholm (SE3)"),
    ("SE4",      "Sweden – Malmo (SE4)"),
    ("SI",       "Slovenia (SI)"),
    ("SK",       "Slovakia (SK)"),
]

# aWATTar reseller endpoints (same upstream data as Energy-Charts but with
# a simpler API). api.awattar.de only delivers DE; api.awattar.at delivers
# AT. spot_price.py will pick the right endpoint when this zone is selected.
SPOT_ZONES_AWATTAR: List[Tuple[str, str]] = [
    ("DE", "Germany (DE)"),
    ("AT", "Austria (AT)"),
]

# Australia (AEMO NEM dispatch prices, AUD/MWh, public CSV — no key)
SPOT_ZONES_AEMO: List[Tuple[str, str]] = [
    ("AU-NSW", "Australia – New South Wales (NSW1)"),
    ("AU-QLD", "Australia – Queensland (QLD1)"),
    ("AU-SA",  "Australia – South Australia (SA1)"),
    ("AU-TAS", "Australia – Tasmania (TAS1)"),
    ("AU-VIC", "Australia – Victoria (VIC1)"),
]

# USA wholesale via EIA (USD/MWh, requires free API key from
# https://www.eia.gov/opendata/). The US-SUBREGION codes map to EIA's
# "Electricity · Wholesale daily market" dataset BA/regions.
SPOT_ZONES_EIA: List[Tuple[str, str]] = [
    ("US-CAL",  "USA – California (CAL)"),
    ("US-CAR",  "USA – Carolinas (CAR)"),
    ("US-CENT", "USA – Central (CENT)"),
    ("US-FLA",  "USA – Florida (FLA)"),
    ("US-MIDA", "USA – Mid-Atlantic (PJM-MIDA)"),
    ("US-MIDW", "USA – Midwest (MISO, MIDW)"),
    ("US-NE",   "USA – New England (ISO-NE)"),
    ("US-NW",   "USA – Northwest (NW)"),
    ("US-NY",   "USA – New York (NYISO)"),
    ("US-SE",   "USA – Southeast (SE)"),
    ("US-SW",   "USA – Southwest (SW)"),
    ("US-TEN",  "USA – Tennessee (TEN)"),
    ("US-TEX",  "USA – Texas (ERCOT, TEX)"),
]

# Group labels for the UI (used as <optgroup> labels in the settings page)
SPOT_ZONE_GROUPS: List[Tuple[str, List[Tuple[str, str]]]] = [
    ("Europe (Energy-Charts, no key)", SPOT_ZONES_ENERGY_CHARTS),
    ("USA (EIA, API key required)",    SPOT_ZONES_EIA),
    ("Australia (AEMO, no key)",       SPOT_ZONES_AEMO),
]


def spot_provider_for_zone(zone: str) -> str:
    """Return the provider name the spot-price fetch service should use
    for the given bidding zone. Unknown zones default to Energy-Charts."""
    if not zone:
        return "energy_charts"
    z = zone.upper()
    if z.startswith("US-") or z in {"US", "USA"}:
        return "eia"
    if z.startswith("AU-"):
        return "aemo"
    return "energy_charts"


def spot_zone_currency(zone: str) -> str:
    """Return the native currency for a spot-price zone. Used when storing
    raw prices — the app displays everything as €/kWh after FX conversion."""
    if not zone:
        return "EUR"
    z = zone.upper()
    if z.startswith("US-"):
        return "USD"
    if z.startswith("AU-"):
        return "AUD"
    if z in {"GB", "UK"}:
        return "GBP"
    if z.startswith("NO") or z.startswith("SE") or z.startswith("DK"):
        return "EUR"  # EPEX already quotes these in EUR via Energy-Charts
    return "EUR"


# ── CO₂ zones ──────────────────────────────────────────────────────────

def get_co2_zones_entsoe() -> List[Tuple[str, str]]:
    """EU CO₂ zones sourced from entsoe._EIC_CODES so the two lists never
    drift apart."""
    from shelly_analyzer.services.entsoe import _EIC_CODES  # avoid import cycles
    labels = {
        "DE_LU":    "Germany + Luxembourg (DE_LU)",
        "AT":       "Austria (AT)",
        "BE":       "Belgium (BE)",
        "BG":       "Bulgaria (BG)",
        "CH":       "Switzerland (CH)",
        "CZ":       "Czech Republic (CZ)",
        "DE_AT_LU": "DE + AT + LU (hist.)",
        "DK_1":     "Denmark West (DK_1)",
        "DK_2":     "Denmark East (DK_2)",
        "EE":       "Estonia (EE)",
        "ES":       "Spain (ES)",
        "FI":       "Finland (FI)",
        "FR":       "France (FR)",
        "GB":       "Great Britain (GB)",
        "GR":       "Greece (GR)",
        "HR":       "Croatia (HR)",
        "HU":       "Hungary (HU)",
        "IE_SEM":   "Ireland – SEM (IE_SEM)",
        "IT_NORD":  "Italy – North (IT_NORD)",
        "LT":       "Lithuania (LT)",
        "LU":       "Luxembourg (LU)",
        "LV":       "Latvia (LV)",
        "NL":       "Netherlands (NL)",
        "NO_1":     "Norway – Oslo (NO_1)",
        "NO_2":     "Norway – Kristiansand (NO_2)",
        "NO_3":     "Norway – Trondheim (NO_3)",
        "NO_4":     "Norway – Tromso (NO_4)",
        "NO_5":     "Norway – Bergen (NO_5)",
        "PL":       "Poland (PL)",
        "PT":       "Portugal (PT)",
        "RO":       "Romania (RO)",
        "RS":       "Serbia (RS)",
        "SE_1":     "Sweden – Lulea (SE_1)",
        "SE_2":     "Sweden – Sundsvall (SE_2)",
        "SE_3":     "Sweden – Stockholm (SE_3)",
        "SE_4":     "Sweden – Malmo (SE_4)",
        "SI":       "Slovenia (SI)",
        "SK":       "Slovakia (SK)",
    }
    return [(zone, labels.get(zone, zone)) for zone in _EIC_CODES.keys()]


# Electricity Maps zones (global, free API key). Only the most populous
# / commonly-requested ones are listed; the full list is 200+ zones and
# would overwhelm the dropdown. Keys use the Electricity Maps "zone" id
# format (ISO country code, optionally with a subregion suffix).
CO2_ZONES_ELECTRICITY_MAPS: List[Tuple[str, str]] = [
    # North America
    ("US",         "USA – total"),
    ("US-CAL-CISO", "USA – California (CAISO)"),
    ("US-NE-ISNE", "USA – New England (ISO-NE)"),
    ("US-NY-NYIS", "USA – New York (NYISO)"),
    ("US-MIDA-PJM", "USA – Mid-Atlantic (PJM)"),
    ("US-TEX-ERCO", "USA – Texas (ERCOT)"),
    ("US-MIDW-MISO", "USA – Midwest (MISO)"),
    ("US-NW-BPAT", "USA – Pacific Northwest (BPA)"),
    ("US-SW-SRP", "USA – Southwest (SRP)"),
    ("US-CAR-DUK", "USA – Carolinas (Duke)"),
    ("US-FLA-FPL", "USA – Florida (FPL)"),
    ("CA-ON",      "Canada – Ontario"),
    ("CA-QC",      "Canada – Quebec"),
    ("CA-BC",      "Canada – British Columbia"),
    ("CA-AB",      "Canada – Alberta"),
    ("MX",         "Mexico"),
    # South America
    ("BR-CS",      "Brazil – South/Centre"),
    ("BR-N",       "Brazil – North"),
    ("BR-NE",      "Brazil – Northeast"),
    ("BR-S",       "Brazil – South"),
    ("AR",         "Argentina"),
    ("CL-SEN",     "Chile"),
    ("CO",         "Colombia"),
    ("PE",         "Peru"),
    ("UY",         "Uruguay"),
    # Asia
    ("JP-TK",      "Japan – Tokyo (TEPCO)"),
    ("JP-KN",      "Japan – Kansai"),
    ("JP-CB",      "Japan – Chubu"),
    ("JP-KY",      "Japan – Kyushu"),
    ("JP-HR",      "Japan – Hokuriku"),
    ("JP-HKD",     "Japan – Hokkaido"),
    ("JP-ON",      "Japan – Okinawa"),
    ("KR",         "South Korea"),
    ("TW",         "Taiwan"),
    ("CN",         "China – Mainland"),
    ("HK",         "Hong Kong"),
    ("SG",         "Singapore"),
    ("IN-NO",      "India – North"),
    ("IN-SO",      "India – South"),
    ("IN-WE",      "India – West"),
    ("IN-EA",      "India – East"),
    ("ID",         "Indonesia"),
    ("MY-WM",      "Malaysia – Peninsula"),
    ("PH",         "Philippines"),
    ("TH",         "Thailand"),
    ("VN",         "Vietnam"),
    # Oceania
    ("AU-NSW",     "Australia – New South Wales"),
    ("AU-QLD",     "Australia – Queensland"),
    ("AU-SA",      "Australia – South Australia"),
    ("AU-TAS",     "Australia – Tasmania"),
    ("AU-VIC",     "Australia – Victoria"),
    ("AU-WA",      "Australia – Western Australia"),
    ("NZ",         "New Zealand"),
    # Africa
    ("ZA",         "South Africa"),
    ("EG",         "Egypt"),
    ("MA",         "Morocco"),
    ("NG",         "Nigeria"),
    ("KE",         "Kenya"),
    # Middle East
    ("IL",         "Israel"),
    ("TR",         "Turkey"),
    ("AE",         "United Arab Emirates"),
    ("SA",         "Saudi Arabia"),
    # All EU zones (also exposed via ENTSO-E, but listed here for users
    # who only want one integration — Electricity Maps covers everything).
    ("DE",         "Germany"),
    ("FR",         "France"),
    ("IT",         "Italy"),
    ("ES",         "Spain"),
    ("PT",         "Portugal"),
    ("GB",         "Great Britain"),
    ("IE",         "Ireland"),
    ("NL",         "Netherlands"),
    ("BE",         "Belgium"),
    ("LU",         "Luxembourg"),
    ("AT",         "Austria"),
    ("CH",         "Switzerland"),
    ("PL",         "Poland"),
    ("CZ",         "Czech Republic"),
    ("SK",         "Slovakia"),
    ("HU",         "Hungary"),
    ("RO",         "Romania"),
    ("BG",         "Bulgaria"),
    ("GR",         "Greece"),
    ("HR",         "Croatia"),
    ("SI",         "Slovenia"),
    ("DK-DK1",     "Denmark – West (DK1)"),
    ("DK-DK2",     "Denmark – East (DK2)"),
    ("SE",         "Sweden"),
    ("NO",         "Norway"),
    ("FI",         "Finland"),
    ("EE",         "Estonia"),
    ("LV",         "Latvia"),
    ("LT",         "Lithuania"),
    ("IS",         "Iceland"),
]


CO2_ZONE_GROUPS: List[Tuple[str, List[Tuple[str, str]]]] = [
    ("ENTSO-E (Europe, own API token)",        []),  # filled lazily
    ("Electricity Maps (global, free API key)", CO2_ZONES_ELECTRICITY_MAPS),
]


def get_co2_zones() -> List[Tuple[str, str]]:
    """Backwards-compatible alias: returns the ENTSO-E (EU-only) zones.

    The Electricity Maps global list lives in
    ``CO2_ZONES_ELECTRICITY_MAPS`` and is exposed separately through the
    /api/zones endpoint."""
    return get_co2_zones_entsoe()


def co2_provider_for_zone(zone: str, has_em_api_key: bool) -> str:
    """Decide whether a CO₂ zone should be fetched via ENTSO-E or
    Electricity Maps. ENTSO-E only covers EU and uses underscore_codes
    (``DE_LU``, ``SE_4``); Electricity Maps uses hyphen codes. If the
    zone contains an underscore we assume ENTSO-E; otherwise we fall
    back to Electricity Maps (if an API key is configured) so global
    zones can resolve."""
    if not zone:
        return "entsoe"
    if "_" in zone:
        return "entsoe"
    return "electricity_maps" if has_em_api_key else "entsoe"
