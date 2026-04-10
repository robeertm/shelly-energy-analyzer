"""Curated lists of bidding zones for the spot-price and CO₂ settings.

Two different naming conventions are in play:
- **Energy-Charts** (Fraunhofer ISE, used for spot prices) expects zones in
  hyphen form, e.g. ``DE-LU``, ``SE-4``, ``IT-NORD``.
- **ENTSO-E Transparency Platform** (used for CO₂ intensity) expects the
  zones in underscore form that matches the EIC-code dictionary in
  ``services/entsoe.py``: ``DE_LU``, ``SE_4``, ``IT_NORD``.

This module centralises both lists with human-readable labels so the web
settings page can render proper ``<select>`` dropdowns instead of a
free-text input where typos break the integrations silently.
"""
from __future__ import annotations

from typing import List, Tuple


# ── Spot-price zones (Energy-Charts / ENTSO-E market data, hyphen form) ──
# Covers every bidding zone that Energy-Charts' /price endpoint accepts.
# Non-EU / overseas zones omitted. The label is what users see in the UI.
SPOT_ZONES_ENERGY_CHARTS: List[Tuple[str, str]] = [
    ("DE-LU",    "Deutschland + Luxemburg (DE-LU)"),
    ("DE-AT-LU", "Deutschland + Österreich + Luxemburg (DE-AT-LU, hist.)"),
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
    ("IT-CALA",  "Italien – Kalabrien (IT-CALA)"),
    ("IT-CNOR",  "Italien – Mitte/Nord (IT-CNOR)"),
    ("IT-CSUD",  "Italien – Mitte/Süd (IT-CSUD)"),
    ("IT-NORD",  "Italien – Nord (IT-NORD)"),
    ("IT-SARD",  "Italien – Sardinien (IT-SARD)"),
    ("IT-SICI",  "Italien – Sizilien (IT-SICI)"),
    ("IT-SUD",   "Italien – Süd (IT-SUD)"),
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

# aWATTar is a reseller that only delivers DE + AT spot prices (same source
# data as Energy-Charts). Since the aWATTar client in spot_price.py uses the
# fixed ``api.awattar.de`` endpoint, only DE is actually functional; AT is
# listed for completeness in case future code switches endpoints.
SPOT_ZONES_AWATTAR: List[Tuple[str, str]] = [
    ("DE", "Deutschland (DE)"),
    ("AT", "Österreich (AT)"),
]


def get_co2_zones() -> List[Tuple[str, str]]:
    """Return the CO₂ zone list sourced from entsoe._EIC_CODES so the two
    never drift apart."""
    from shelly_analyzer.services.entsoe import _EIC_CODES  # avoid import cycles
    labels = {
        "DE_LU":    "Deutschland + Luxemburg (DE_LU)",
        "AT":       "Österreich (AT)",
        "BE":       "Belgien (BE)",
        "BG":       "Bulgarien (BG)",
        "CH":       "Schweiz (CH)",
        "CZ":       "Tschechien (CZ)",
        "DE_AT_LU": "DE + AT + LU (DE_AT_LU, hist.)",
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
    # Preserve the entsoe dict order, fall back to the key itself when a
    # new zone is added there without a label here.
    return [(zone, labels.get(zone, zone)) for zone in _EIC_CODES.keys()]
