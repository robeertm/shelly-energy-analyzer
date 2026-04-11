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
    ("DE-LU",    "Deutschland + Luxemburg (DE-LU)"),
    ("DE-AT-LU", "DE + AT + LU (hist.)"),
    ("AT",       "Österreich (AT)"),
    ("BE",       "Belgien (BE)"),
    ("BG",       "Bulgarien (BG)"),
    ("CH",       "Schweiz (CH)"),
    ("CZ",       "Tschechien (CZ)"),
    ("DK1",      "Dänemark West (DK1)"),
    ("DK2",      "Dänemark Ost (DK2)"),
    ("EE",       "Estland (EE)"),
    ("ES",       "Spanien (ES)"),
    ("FI",       "Finnland (FI)"),
    ("FR",       "Frankreich (FR)"),
    ("GB",       "Großbritannien (GB)"),
    ("GR",       "Griechenland (GR)"),
    ("HR",       "Kroatien (HR)"),
    ("HU",       "Ungarn (HU)"),
    ("IE",       "Irland (IE)"),
    ("IT-CALA",  "Italien – Kalabrien"),
    ("IT-CNOR",  "Italien – Mitte/Nord"),
    ("IT-CSUD",  "Italien – Mitte/Süd"),
    ("IT-NORD",  "Italien – Nord"),
    ("IT-SARD",  "Italien – Sardinien"),
    ("IT-SICI",  "Italien – Sizilien"),
    ("IT-SUD",   "Italien – Süd"),
    ("LT",       "Litauen (LT)"),
    ("LV",       "Lettland (LV)"),
    ("ME",       "Montenegro (ME)"),
    ("MK",       "Nordmazedonien (MK)"),
    ("NL",       "Niederlande (NL)"),
    ("NO1",      "Norwegen – Oslo (NO1)"),
    ("NO2",      "Norwegen – Kristiansand (NO2)"),
    ("NO3",      "Norwegen – Trondheim (NO3)"),
    ("NO4",      "Norwegen – Tromsø (NO4)"),
    ("NO5",      "Norwegen – Bergen (NO5)"),
    ("PL",       "Polen (PL)"),
    ("PT",       "Portugal (PT)"),
    ("RO",       "Rumänien (RO)"),
    ("RS",       "Serbien (RS)"),
    ("SE1",      "Schweden – Luleå (SE1)"),
    ("SE2",      "Schweden – Sundsvall (SE2)"),
    ("SE3",      "Schweden – Stockholm (SE3)"),
    ("SE4",      "Schweden – Malmö (SE4)"),
    ("SI",       "Slowenien (SI)"),
    ("SK",       "Slowakei (SK)"),
]

# aWATTar reseller endpoints (same upstream data as Energy-Charts but with
# a simpler API). api.awattar.de only delivers DE; api.awattar.at delivers
# AT. spot_price.py will pick the right endpoint when this zone is selected.
SPOT_ZONES_AWATTAR: List[Tuple[str, str]] = [
    ("DE", "Deutschland (DE)"),
    ("AT", "Österreich (AT)"),
]

# Australia (AEMO NEM dispatch prices, AUD/MWh, public CSV — no key)
SPOT_ZONES_AEMO: List[Tuple[str, str]] = [
    ("AU-NSW", "Australien – New South Wales (NSW1)"),
    ("AU-QLD", "Australien – Queensland (QLD1)"),
    ("AU-SA",  "Australien – South Australia (SA1)"),
    ("AU-TAS", "Australien – Tasmanien (TAS1)"),
    ("AU-VIC", "Australien – Victoria (VIC1)"),
]

# USA wholesale via EIA (USD/MWh, requires free API key from
# https://www.eia.gov/opendata/). The US-SUBREGION codes map to EIA's
# "Electricity · Wholesale daily market" dataset BA/regions.
SPOT_ZONES_EIA: List[Tuple[str, str]] = [
    ("US-CAL",  "USA – Kalifornien (CAL)"),
    ("US-CAR",  "USA – Carolinas (CAR)"),
    ("US-CENT", "USA – Central (CENT)"),
    ("US-FLA",  "USA – Florida (FLA)"),
    ("US-MIDA", "USA – Mid-Atlantic (PJM-MIDA)"),
    ("US-MIDW", "USA – Mittlerer Westen (MISO, MIDW)"),
    ("US-NE",   "USA – Neuengland (ISO-NE)"),
    ("US-NW",   "USA – Nordwest (NW)"),
    ("US-NY",   "USA – New York (NYISO)"),
    ("US-SE",   "USA – Südost (SE)"),
    ("US-SW",   "USA – Südwest (SW)"),
    ("US-TEN",  "USA – Tennessee (TEN)"),
    ("US-TEX",  "USA – Texas (ERCOT, TEX)"),
]

# Group labels for the UI (used as <optgroup> labels in the settings page)
SPOT_ZONE_GROUPS: List[Tuple[str, List[Tuple[str, str]]]] = [
    ("Europa (Energy-Charts, ohne Anmeldung)", SPOT_ZONES_ENERGY_CHARTS),
    ("USA (EIA, API-Key erforderlich)",         SPOT_ZONES_EIA),
    ("Australien (AEMO, ohne Anmeldung)",       SPOT_ZONES_AEMO),
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
        "DE_LU":    "Deutschland + Luxemburg (DE_LU)",
        "AT":       "Österreich (AT)",
        "BE":       "Belgien (BE)",
        "BG":       "Bulgarien (BG)",
        "CH":       "Schweiz (CH)",
        "CZ":       "Tschechien (CZ)",
        "DE_AT_LU": "DE + AT + LU (hist.)",
        "DK_1":     "Dänemark West (DK_1)",
        "DK_2":     "Dänemark Ost (DK_2)",
        "EE":       "Estland (EE)",
        "ES":       "Spanien (ES)",
        "FI":       "Finnland (FI)",
        "FR":       "Frankreich (FR)",
        "GB":       "Großbritannien (GB)",
        "GR":       "Griechenland (GR)",
        "HR":       "Kroatien (HR)",
        "HU":       "Ungarn (HU)",
        "IE_SEM":   "Irland – SEM (IE_SEM)",
        "IT_NORD":  "Italien – Nord (IT_NORD)",
        "LT":       "Litauen (LT)",
        "LU":       "Luxemburg (LU)",
        "LV":       "Lettland (LV)",
        "NL":       "Niederlande (NL)",
        "NO_1":     "Norwegen – Oslo (NO_1)",
        "NO_2":     "Norwegen – Kristiansand (NO_2)",
        "NO_3":     "Norwegen – Trondheim (NO_3)",
        "NO_4":     "Norwegen – Tromsø (NO_4)",
        "NO_5":     "Norwegen – Bergen (NO_5)",
        "PL":       "Polen (PL)",
        "PT":       "Portugal (PT)",
        "RO":       "Rumänien (RO)",
        "RS":       "Serbien (RS)",
        "SE_1":     "Schweden – Luleå (SE_1)",
        "SE_2":     "Schweden – Sundsvall (SE_2)",
        "SE_3":     "Schweden – Stockholm (SE_3)",
        "SE_4":     "Schweden – Malmö (SE_4)",
        "SI":       "Slowenien (SI)",
        "SK":       "Slowakei (SK)",
    }
    return [(zone, labels.get(zone, zone)) for zone in _EIC_CODES.keys()]


# Electricity Maps zones (global, free API key). Only the most populous
# / commonly-requested ones are listed; the full list is 200+ zones and
# would overwhelm the dropdown. Keys use the Electricity Maps "zone" id
# format (ISO country code, optionally with a subregion suffix).
CO2_ZONES_ELECTRICITY_MAPS: List[Tuple[str, str]] = [
    # North America
    ("US",         "USA – Gesamt"),
    ("US-CAL-CISO", "USA – Kalifornien (CAISO)"),
    ("US-NE-ISNE", "USA – Neuengland (ISO-NE)"),
    ("US-NY-NYIS", "USA – New York (NYISO)"),
    ("US-MIDA-PJM", "USA – Mid-Atlantic (PJM)"),
    ("US-TEX-ERCO", "USA – Texas (ERCOT)"),
    ("US-MIDW-MISO", "USA – Mittlerer Westen (MISO)"),
    ("US-NW-BPAT", "USA – Pazifischer Nordwesten (BPA)"),
    ("US-SW-SRP", "USA – Südwest (SRP)"),
    ("US-CAR-DUK", "USA – Carolinas (Duke)"),
    ("US-FLA-FPL", "USA – Florida (FPL)"),
    ("CA-ON",      "Kanada – Ontario"),
    ("CA-QC",      "Kanada – Québec"),
    ("CA-BC",      "Kanada – British Columbia"),
    ("CA-AB",      "Kanada – Alberta"),
    ("MX",         "Mexiko"),
    # South America
    ("BR-CS",      "Brasilien – Süd/Mitte"),
    ("BR-N",       "Brasilien – Nord"),
    ("BR-NE",      "Brasilien – Nordost"),
    ("BR-S",       "Brasilien – Süd"),
    ("AR",         "Argentinien"),
    ("CL-SEN",     "Chile"),
    ("CO",         "Kolumbien"),
    ("PE",         "Peru"),
    ("UY",         "Uruguay"),
    # Asia
    ("JP-TK",      "Japan – Tokio (TEPCO)"),
    ("JP-KN",      "Japan – Kansai"),
    ("JP-CB",      "Japan – Chūbu"),
    ("JP-KY",      "Japan – Kyūshū"),
    ("JP-HR",      "Japan – Hokuriku"),
    ("JP-HKD",     "Japan – Hokkaidō"),
    ("JP-ON",      "Japan – Okinawa"),
    ("KR",         "Südkorea"),
    ("TW",         "Taiwan"),
    ("CN",         "China – Festland"),
    ("HK",         "Hongkong"),
    ("SG",         "Singapur"),
    ("IN-NO",      "Indien – Nord"),
    ("IN-SO",      "Indien – Süd"),
    ("IN-WE",      "Indien – West"),
    ("IN-EA",      "Indien – Ost"),
    ("ID",         "Indonesien"),
    ("MY-WM",      "Malaysia – Halbinsel"),
    ("PH",         "Philippinen"),
    ("TH",         "Thailand"),
    ("VN",         "Vietnam"),
    # Oceania
    ("AU-NSW",     "Australien – New South Wales"),
    ("AU-QLD",     "Australien – Queensland"),
    ("AU-SA",      "Australien – South Australia"),
    ("AU-TAS",     "Australien – Tasmanien"),
    ("AU-VIC",     "Australien – Victoria"),
    ("AU-WA",      "Australien – Western Australia"),
    ("NZ",         "Neuseeland"),
    # Africa
    ("ZA",         "Südafrika"),
    ("EG",         "Ägypten"),
    ("MA",         "Marokko"),
    ("NG",         "Nigeria"),
    ("KE",         "Kenia"),
    # Middle East
    ("IL",         "Israel"),
    ("TR",         "Türkei"),
    ("AE",         "Vereinigte Arabische Emirate"),
    ("SA",         "Saudi-Arabien"),
    # All EU zones (also exposed via ENTSO-E, but listed here for users
    # who only want one integration — Electricity Maps covers everything).
    ("DE",         "Deutschland"),
    ("FR",         "Frankreich"),
    ("IT",         "Italien"),
    ("ES",         "Spanien"),
    ("PT",         "Portugal"),
    ("GB",         "Großbritannien"),
    ("IE",         "Irland"),
    ("NL",         "Niederlande"),
    ("BE",         "Belgien"),
    ("LU",         "Luxemburg"),
    ("AT",         "Österreich"),
    ("CH",         "Schweiz"),
    ("PL",         "Polen"),
    ("CZ",         "Tschechien"),
    ("SK",         "Slowakei"),
    ("HU",         "Ungarn"),
    ("RO",         "Rumänien"),
    ("BG",         "Bulgarien"),
    ("GR",         "Griechenland"),
    ("HR",         "Kroatien"),
    ("SI",         "Slowenien"),
    ("DK-DK1",     "Dänemark – West (DK1)"),
    ("DK-DK2",     "Dänemark – Ost (DK2)"),
    ("SE",         "Schweden"),
    ("NO",         "Norwegen"),
    ("FI",         "Finnland"),
    ("EE",         "Estland"),
    ("LV",         "Lettland"),
    ("LT",         "Litauen"),
    ("IS",         "Island"),
]


CO2_ZONE_GROUPS: List[Tuple[str, List[Tuple[str, str]]]] = [
    ("ENTSO-E (Europa, eigener API-Token)",       []),  # filled lazily
    ("Electricity Maps (global, freier API-Key)", CO2_ZONES_ELECTRICITY_MAPS),
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
