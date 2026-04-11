from __future__ import annotations
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional

_log = logging.getLogger(__name__)


@dataclass
class TariffResult:
    name: str
    provider: str
    tariff_type: str
    annual_cost_eur: float
    monthly_avg_eur: float
    effective_price_ct: float  # ct/kWh all-in
    savings_vs_current_eur: float = 0.0
    is_current: bool = False


# Country-specific tariff templates.
#
# Each template's price field is labelled ``price_eur_per_kwh`` /
# ``base_fee_eur_per_year`` for historical reasons, but the numbers are
# simply "price per kWh" and "base fee per year" in the *local currency*
# of the country the user lives in. The comparison is internally
# consistent as long as the current tariff and the templates are in the
# same currency — which they are, because the app reads both from the
# same ``cfg.pricing`` block.
#
# Values are representative 2025 residential retail tariffs for each
# market, sourced from public rate cards. They're meant as
# order-of-magnitude comparison points, not an exact quote.
TARIFF_TEMPLATES_BY_COUNTRY: Dict[str, List[Dict]] = {
    # ── Germany (EUR) ────────────────────────────────────────────────
    "DE": [
        {"name": "Municipal standard", "provider": "Stadtwerke", "tariff_type": "fixed",
         "price_eur_per_kwh": 0.3265, "base_fee_eur_per_year": 127.51},
        {"name": "Tibber Pulse", "provider": "Tibber", "tariff_type": "spot",
         "price_eur_per_kwh": 0.0, "base_fee_eur_per_year": 71.40, "spot_markup_ct": 15.3},
        {"name": "1Komma5° Dynamic", "provider": "1Komma5°", "tariff_type": "spot",
         "price_eur_per_kwh": 0.0, "base_fee_eur_per_year": 0.0, "spot_markup_ct": 14.5},
        {"name": "Ostrom Flex", "provider": "Ostrom", "tariff_type": "spot",
         "price_eur_per_kwh": 0.0, "base_fee_eur_per_year": 107.88, "spot_markup_ct": 16.0},
        {"name": "E.ON Strom Basis", "provider": "E.ON", "tariff_type": "fixed",
         "price_eur_per_kwh": 0.3399, "base_fee_eur_per_year": 167.88},
        {"name": "Vattenfall Easy", "provider": "Vattenfall", "tariff_type": "fixed",
         "price_eur_per_kwh": 0.3199, "base_fee_eur_per_year": 143.88},
        {"name": "EnBW Comfort", "provider": "EnBW", "tariff_type": "fixed",
         "price_eur_per_kwh": 0.3485, "base_fee_eur_per_year": 155.88},
        {"name": "Day/Night TOU", "provider": "Stadtwerke", "tariff_type": "tou",
         "price_eur_per_kwh": 0.0, "base_fee_eur_per_year": 140.0,
         "ht_price": 0.35, "nt_price": 0.22, "ht_start": 6, "ht_end": 22},
    ],
    # ── Austria (EUR) ────────────────────────────────────────────────
    "AT": [
        {"name": "Verbund Optima", "provider": "Verbund", "tariff_type": "fixed",
         "price_eur_per_kwh": 0.2799, "base_fee_eur_per_year": 60.0},
        {"name": "Wien Energie Optima Entspannt", "provider": "Wien Energie", "tariff_type": "fixed",
         "price_eur_per_kwh": 0.2595, "base_fee_eur_per_year": 48.0},
        {"name": "EVN Optima Standard", "provider": "EVN", "tariff_type": "fixed",
         "price_eur_per_kwh": 0.2890, "base_fee_eur_per_year": 72.0},
        {"name": "aWATTar HOURLY", "provider": "aWATTar", "tariff_type": "spot",
         "price_eur_per_kwh": 0.0, "base_fee_eur_per_year": 71.88, "spot_markup_ct": 1.5},
        {"name": "Day/Night TOU", "provider": "Municipal", "tariff_type": "tou",
         "price_eur_per_kwh": 0.0, "base_fee_eur_per_year": 80.0,
         "ht_price": 0.29, "nt_price": 0.19, "ht_start": 6, "ht_end": 22},
    ],
    # ── Switzerland (CHF; stored as "eur" fields) ────────────────────
    "CH": [
        {"name": "EWZ Basisprodukt", "provider": "EWZ", "tariff_type": "fixed",
         "price_eur_per_kwh": 0.2770, "base_fee_eur_per_year": 90.0},
        {"name": "BKW Blue", "provider": "BKW", "tariff_type": "fixed",
         "price_eur_per_kwh": 0.3140, "base_fee_eur_per_year": 108.0},
        {"name": "IWB Standard", "provider": "IWB", "tariff_type": "fixed",
         "price_eur_per_kwh": 0.2930, "base_fee_eur_per_year": 84.0},
        {"name": "Day/Night TOU", "provider": "Municipal", "tariff_type": "tou",
         "price_eur_per_kwh": 0.0, "base_fee_eur_per_year": 108.0,
         "ht_price": 0.32, "nt_price": 0.22, "ht_start": 7, "ht_end": 20},
    ],
    # ── France (EUR) ─────────────────────────────────────────────────
    "FR": [
        {"name": "EDF Tarif Bleu (base)", "provider": "EDF", "tariff_type": "fixed",
         "price_eur_per_kwh": 0.2516, "base_fee_eur_per_year": 151.68},
        {"name": "EDF Heures Creuses", "provider": "EDF", "tariff_type": "tou",
         "price_eur_per_kwh": 0.0, "base_fee_eur_per_year": 157.32,
         "ht_price": 0.2700, "nt_price": 0.2068, "ht_start": 6, "ht_end": 22},
        {"name": "Engie Référence", "provider": "Engie", "tariff_type": "fixed",
         "price_eur_per_kwh": 0.2622, "base_fee_eur_per_year": 156.0},
        {"name": "TotalEnergies Verte Fixe", "provider": "TotalEnergies", "tariff_type": "fixed",
         "price_eur_per_kwh": 0.2466, "base_fee_eur_per_year": 144.0},
        {"name": "Octopus Go", "provider": "Octopus Energy", "tariff_type": "spot",
         "price_eur_per_kwh": 0.0, "base_fee_eur_per_year": 150.0, "spot_markup_ct": 8.0},
    ],
    # ── United Kingdom (GBP; stored as "eur" fields) ─────────────────
    "GB": [
        {"name": "British Gas Standard Variable", "provider": "British Gas", "tariff_type": "fixed",
         "price_eur_per_kwh": 0.2499, "base_fee_eur_per_year": 196.35},
        {"name": "Octopus Flexible", "provider": "Octopus", "tariff_type": "fixed",
         "price_eur_per_kwh": 0.2403, "base_fee_eur_per_year": 193.45},
        {"name": "Octopus Agile", "provider": "Octopus", "tariff_type": "spot",
         "price_eur_per_kwh": 0.0, "base_fee_eur_per_year": 193.45, "spot_markup_ct": 8.5},
        {"name": "OVO Simpler Energy", "provider": "OVO", "tariff_type": "fixed",
         "price_eur_per_kwh": 0.2435, "base_fee_eur_per_year": 200.0},
        {"name": "Economy 7 (TOU)", "provider": "—", "tariff_type": "tou",
         "price_eur_per_kwh": 0.0, "base_fee_eur_per_year": 196.00,
         "ht_price": 0.2820, "nt_price": 0.1360, "ht_start": 7, "ht_end": 24},
    ],
    # ── Netherlands (EUR) ────────────────────────────────────────────
    "NL": [
        {"name": "Eneco Vast 1 jaar", "provider": "Eneco", "tariff_type": "fixed",
         "price_eur_per_kwh": 0.2849, "base_fee_eur_per_year": 78.0},
        {"name": "Vattenfall Vast 1 jaar", "provider": "Vattenfall", "tariff_type": "fixed",
         "price_eur_per_kwh": 0.2795, "base_fee_eur_per_year": 72.0},
        {"name": "Essent Vast", "provider": "Essent", "tariff_type": "fixed",
         "price_eur_per_kwh": 0.2910, "base_fee_eur_per_year": 84.0},
        {"name": "Frank Energie Dynamisch", "provider": "Frank Energie", "tariff_type": "spot",
         "price_eur_per_kwh": 0.0, "base_fee_eur_per_year": 71.88, "spot_markup_ct": 1.8},
        {"name": "Tibber (NL)", "provider": "Tibber", "tariff_type": "spot",
         "price_eur_per_kwh": 0.0, "base_fee_eur_per_year": 71.88, "spot_markup_ct": 2.5},
    ],
    # ── Belgium (EUR) ────────────────────────────────────────────────
    "BE": [
        {"name": "Engie Easy Fixed", "provider": "Engie", "tariff_type": "fixed",
         "price_eur_per_kwh": 0.3150, "base_fee_eur_per_year": 90.0},
        {"name": "Luminus Comfy Fixed", "provider": "Luminus", "tariff_type": "fixed",
         "price_eur_per_kwh": 0.3080, "base_fee_eur_per_year": 84.0},
        {"name": "TotalEnergies Pixel", "provider": "TotalEnergies", "tariff_type": "fixed",
         "price_eur_per_kwh": 0.3245, "base_fee_eur_per_year": 96.0},
        {"name": "Day/Night TOU", "provider": "—", "tariff_type": "tou",
         "price_eur_per_kwh": 0.0, "base_fee_eur_per_year": 96.0,
         "ht_price": 0.33, "nt_price": 0.24, "ht_start": 7, "ht_end": 22},
    ],
    # ── Spain (EUR) ──────────────────────────────────────────────────
    "ES": [
        {"name": "Iberdrola Plan Estable", "provider": "Iberdrola", "tariff_type": "fixed",
         "price_eur_per_kwh": 0.1490, "base_fee_eur_per_year": 120.0},
        {"name": "Endesa One Luz", "provider": "Endesa", "tariff_type": "fixed",
         "price_eur_per_kwh": 0.1560, "base_fee_eur_per_year": 132.0},
        {"name": "PVPC (regulated)", "provider": "—", "tariff_type": "spot",
         "price_eur_per_kwh": 0.0, "base_fee_eur_per_year": 80.0, "spot_markup_ct": 2.0},
        {"name": "Holaluz Sin Sorpresas", "provider": "Holaluz", "tariff_type": "fixed",
         "price_eur_per_kwh": 0.1430, "base_fee_eur_per_year": 108.0},
        {"name": "Tramo TOU", "provider": "—", "tariff_type": "tou",
         "price_eur_per_kwh": 0.0, "base_fee_eur_per_year": 96.0,
         "ht_price": 0.1850, "nt_price": 0.1050, "ht_start": 10, "ht_end": 22},
    ],
    # ── Italy (EUR) ──────────────────────────────────────────────────
    "IT": [
        {"name": "Enel Energia Prezzo Fisso", "provider": "Enel", "tariff_type": "fixed",
         "price_eur_per_kwh": 0.2390, "base_fee_eur_per_year": 96.0},
        {"name": "A2A Click Luce", "provider": "A2A", "tariff_type": "fixed",
         "price_eur_per_kwh": 0.2450, "base_fee_eur_per_year": 88.0},
        {"name": "Edison Sweet", "provider": "Edison", "tariff_type": "fixed",
         "price_eur_per_kwh": 0.2310, "base_fee_eur_per_year": 100.0},
        {"name": "Illumia Pun Indexed", "provider": "Illumia", "tariff_type": "spot",
         "price_eur_per_kwh": 0.0, "base_fee_eur_per_year": 96.0, "spot_markup_ct": 3.5},
    ],
    # ── Sweden (SEK; stored as "eur" fields) ─────────────────────────
    "SE": [
        {"name": "Vattenfall Rörligt", "provider": "Vattenfall", "tariff_type": "spot",
         "price_eur_per_kwh": 0.0, "base_fee_eur_per_year": 468.0, "spot_markup_ct": 4.5},
        {"name": "Tibber Rörligt", "provider": "Tibber", "tariff_type": "spot",
         "price_eur_per_kwh": 0.0, "base_fee_eur_per_year": 588.0, "spot_markup_ct": 4.0},
        {"name": "Fortum Fastpris 1 år", "provider": "Fortum", "tariff_type": "fixed",
         "price_eur_per_kwh": 1.2500, "base_fee_eur_per_year": 468.0},
        {"name": "E.ON Fastpris", "provider": "E.ON", "tariff_type": "fixed",
         "price_eur_per_kwh": 1.3100, "base_fee_eur_per_year": 492.0},
    ],
    # ── Norway (NOK; stored as "eur" fields) ─────────────────────────
    "NO": [
        {"name": "Tibber Spot", "provider": "Tibber", "tariff_type": "spot",
         "price_eur_per_kwh": 0.0, "base_fee_eur_per_year": 468.0, "spot_markup_ct": 3.0},
        {"name": "Fjordkraft Fastpris", "provider": "Fjordkraft", "tariff_type": "fixed",
         "price_eur_per_kwh": 1.1500, "base_fee_eur_per_year": 468.0},
        {"name": "Fortum Variabel", "provider": "Fortum", "tariff_type": "spot",
         "price_eur_per_kwh": 0.0, "base_fee_eur_per_year": 468.0, "spot_markup_ct": 4.9},
        {"name": "LOS Standard", "provider": "LOS", "tariff_type": "fixed",
         "price_eur_per_kwh": 1.1900, "base_fee_eur_per_year": 480.0},
    ],
    # ── Denmark (DKK; stored as "eur" fields) ────────────────────────
    "DK": [
        {"name": "Ørsted Flex", "provider": "Ørsted", "tariff_type": "spot",
         "price_eur_per_kwh": 0.0, "base_fee_eur_per_year": 468.0, "spot_markup_ct": 2.5},
        {"name": "Norlys Fastpris", "provider": "Norlys", "tariff_type": "fixed",
         "price_eur_per_kwh": 2.4500, "base_fee_eur_per_year": 600.0},
        {"name": "OK Spotpris", "provider": "OK", "tariff_type": "spot",
         "price_eur_per_kwh": 0.0, "base_fee_eur_per_year": 468.0, "spot_markup_ct": 3.0},
        {"name": "SEAS-NVE Fast", "provider": "SEAS-NVE", "tariff_type": "fixed",
         "price_eur_per_kwh": 2.3900, "base_fee_eur_per_year": 588.0},
    ],
    # ── Finland (EUR) ────────────────────────────────────────────────
    "FI": [
        {"name": "Fortum Tarkka", "provider": "Fortum", "tariff_type": "spot",
         "price_eur_per_kwh": 0.0, "base_fee_eur_per_year": 72.0, "spot_markup_ct": 0.4},
        {"name": "Helen Perussähkö", "provider": "Helen", "tariff_type": "fixed",
         "price_eur_per_kwh": 0.0895, "base_fee_eur_per_year": 66.0},
        {"name": "Oomi Vakio", "provider": "Oomi", "tariff_type": "fixed",
         "price_eur_per_kwh": 0.0920, "base_fee_eur_per_year": 72.0},
        {"name": "Väre Pörssisähkö", "provider": "Väre", "tariff_type": "spot",
         "price_eur_per_kwh": 0.0, "base_fee_eur_per_year": 60.0, "spot_markup_ct": 0.5},
    ],
    # ── Poland (PLN; stored as "eur" fields) ─────────────────────────
    "PL": [
        {"name": "PGE Gwarancja", "provider": "PGE", "tariff_type": "fixed",
         "price_eur_per_kwh": 0.7420, "base_fee_eur_per_year": 144.0},
        {"name": "Tauron Basic", "provider": "Tauron", "tariff_type": "fixed",
         "price_eur_per_kwh": 0.7550, "base_fee_eur_per_year": 150.0},
        {"name": "Enea Optimum", "provider": "Enea", "tariff_type": "fixed",
         "price_eur_per_kwh": 0.7380, "base_fee_eur_per_year": 138.0},
        {"name": "G12w Day/Night", "provider": "—", "tariff_type": "tou",
         "price_eur_per_kwh": 0.0, "base_fee_eur_per_year": 156.0,
         "ht_price": 0.79, "nt_price": 0.38, "ht_start": 6, "ht_end": 22},
    ],
    # ── Czech Republic (CZK; stored as "eur" fields) ─────────────────
    "CZ": [
        {"name": "ČEZ Standard", "provider": "ČEZ", "tariff_type": "fixed",
         "price_eur_per_kwh": 4.8500, "base_fee_eur_per_year": 1800.0},
        {"name": "E.ON Comfort", "provider": "E.ON", "tariff_type": "fixed",
         "price_eur_per_kwh": 4.9200, "base_fee_eur_per_year": 1920.0},
        {"name": "PRE Klasik", "provider": "PRE", "tariff_type": "fixed",
         "price_eur_per_kwh": 4.7800, "base_fee_eur_per_year": 1680.0},
        {"name": "D25d Low/High tariff", "provider": "—", "tariff_type": "tou",
         "price_eur_per_kwh": 0.0, "base_fee_eur_per_year": 1920.0,
         "ht_price": 5.10, "nt_price": 2.60, "ht_start": 7, "ht_end": 20},
    ],
    # ── Portugal (EUR) ───────────────────────────────────────────────
    "PT": [
        {"name": "EDP Comercial Simples", "provider": "EDP", "tariff_type": "fixed",
         "price_eur_per_kwh": 0.1745, "base_fee_eur_per_year": 120.0},
        {"name": "Galp Plano Casa", "provider": "Galp", "tariff_type": "fixed",
         "price_eur_per_kwh": 0.1680, "base_fee_eur_per_year": 108.0},
        {"name": "Endesa Luz Fija", "provider": "Endesa", "tariff_type": "fixed",
         "price_eur_per_kwh": 0.1790, "base_fee_eur_per_year": 132.0},
        {"name": "Bi-horário (TOU)", "provider": "—", "tariff_type": "tou",
         "price_eur_per_kwh": 0.0, "base_fee_eur_per_year": 120.0,
         "ht_price": 0.2100, "nt_price": 0.1100, "ht_start": 8, "ht_end": 22},
    ],
    # ── Ireland (EUR) ────────────────────────────────────────────────
    "IE": [
        {"name": "Electric Ireland Standard", "provider": "Electric Ireland", "tariff_type": "fixed",
         "price_eur_per_kwh": 0.3689, "base_fee_eur_per_year": 266.28},
        {"name": "Energia Standard", "provider": "Energia", "tariff_type": "fixed",
         "price_eur_per_kwh": 0.3560, "base_fee_eur_per_year": 258.0},
        {"name": "Bord Gáis 1-year fix", "provider": "Bord Gáis", "tariff_type": "fixed",
         "price_eur_per_kwh": 0.3475, "base_fee_eur_per_year": 252.0},
        {"name": "Night Saver", "provider": "—", "tariff_type": "tou",
         "price_eur_per_kwh": 0.0, "base_fee_eur_per_year": 276.0,
         "ht_price": 0.3750, "nt_price": 0.1920, "ht_start": 9, "ht_end": 24},
    ],
    # ── United States (USD; stored as "eur" fields) ──────────────────
    "US": [
        {"name": "PG&E E-1 Residential", "provider": "PG&E", "tariff_type": "fixed",
         "price_eur_per_kwh": 0.3550, "base_fee_eur_per_year": 120.0},
        {"name": "ConEd Standard", "provider": "ConEd", "tariff_type": "fixed",
         "price_eur_per_kwh": 0.2200, "base_fee_eur_per_year": 228.0},
        {"name": "Duke Energy Carolinas", "provider": "Duke Energy", "tariff_type": "fixed",
         "price_eur_per_kwh": 0.1450, "base_fee_eur_per_year": 168.0},
        {"name": "Griddy-style wholesale", "provider": "—", "tariff_type": "spot",
         "price_eur_per_kwh": 0.0, "base_fee_eur_per_year": 120.0, "spot_markup_ct": 1.5},
        {"name": "Time-of-Use (Peak/Off)", "provider": "—", "tariff_type": "tou",
         "price_eur_per_kwh": 0.0, "base_fee_eur_per_year": 144.0,
         "ht_price": 0.32, "nt_price": 0.12, "ht_start": 16, "ht_end": 21},
    ],
    # ── Australia (AUD; stored as "eur" fields) ──────────────────────
    "AU": [
        {"name": "AGL Standing Offer", "provider": "AGL", "tariff_type": "fixed",
         "price_eur_per_kwh": 0.3250, "base_fee_eur_per_year": 365.0},
        {"name": "Origin Basic", "provider": "Origin", "tariff_type": "fixed",
         "price_eur_per_kwh": 0.3180, "base_fee_eur_per_year": 358.0},
        {"name": "EnergyAustralia Total Plan", "provider": "EnergyAustralia", "tariff_type": "fixed",
         "price_eur_per_kwh": 0.3340, "base_fee_eur_per_year": 372.0},
        {"name": "Amber Wholesale", "provider": "Amber", "tariff_type": "spot",
         "price_eur_per_kwh": 0.0, "base_fee_eur_per_year": 228.0, "spot_markup_ct": 3.0},
        {"name": "Time-of-Use (Peak/Shoulder)", "provider": "—", "tariff_type": "tou",
         "price_eur_per_kwh": 0.0, "base_fee_eur_per_year": 365.0,
         "ht_price": 0.4800, "nt_price": 0.2450, "ht_start": 14, "ht_end": 20},
    ],
}


# Aliases: alternate country codes that share a template set with the canonical code.
_COUNTRY_ALIASES = {
    "DE-LU": "DE", "DE_LU": "DE", "DE-AT-LU": "DE", "DE_AT_LU": "DE",
    "UK": "GB",
    "LU": "DE",  # Luxembourg uses the German bidding zone
}


# Generic EU fallback — used when we can't identify the country.
GENERIC_EU_TARIFF_TEMPLATES: List[Dict] = [
    {"name": "Fixed (low)", "provider": "Generic", "tariff_type": "fixed",
     "price_eur_per_kwh": 0.2300, "base_fee_eur_per_year": 100.0},
    {"name": "Fixed (avg)", "provider": "Generic", "tariff_type": "fixed",
     "price_eur_per_kwh": 0.2900, "base_fee_eur_per_year": 120.0},
    {"name": "Fixed (high)", "provider": "Generic", "tariff_type": "fixed",
     "price_eur_per_kwh": 0.3400, "base_fee_eur_per_year": 150.0},
    {"name": "Dynamic (spot + 2 ct)", "provider": "Generic", "tariff_type": "spot",
     "price_eur_per_kwh": 0.0, "base_fee_eur_per_year": 80.0, "spot_markup_ct": 2.0},
    {"name": "Dynamic (spot + 5 ct)", "provider": "Generic", "tariff_type": "spot",
     "price_eur_per_kwh": 0.0, "base_fee_eur_per_year": 100.0, "spot_markup_ct": 5.0},
    {"name": "Day/Night TOU", "provider": "Generic", "tariff_type": "tou",
     "price_eur_per_kwh": 0.0, "base_fee_eur_per_year": 120.0,
     "ht_price": 0.32, "nt_price": 0.20, "ht_start": 6, "ht_end": 22},
]


def _country_from_zone(zone: str) -> str:
    """Derive an ISO country code from a Shelly spot-price bidding zone.

    Handles every zone id used in :mod:`shelly_analyzer.services.zones`
    — hyphen forms (``DE-LU``, ``SE-4``, ``IT-NORD``), underscore forms
    (``DE_LU``, ``SE_4``), EIA US subregions (``US-CAL``, ``US-TEX``)
    and Electricity Maps subzones (``US-CAL-CISO``, ``DK-DK1``,
    ``JP-TK``). Returns an upper-case two-letter code, or an empty
    string if the zone is unrecognised.
    """
    if not zone:
        return ""
    z = str(zone).strip().upper()
    if not z:
        return ""
    # Check the alias table first (catches DE-LU, DE_LU, UK, etc.)
    if z in _COUNTRY_ALIASES:
        return _COUNTRY_ALIASES[z]
    # Split on hyphen/underscore and take the first segment.
    for sep in ("-", "_"):
        if sep in z:
            head, _, _ = z.partition(sep)
            # Two-letter country code → use it directly.
            if len(head) == 2 and head.isalpha():
                return head
            # Italy / Czech / Romania / Poland style single-segment heads
            if head in {"IT", "DE", "SE", "NO", "DK", "FR", "US", "AU", "CA", "BR",
                        "ES", "PT", "GB", "NL", "BE", "AT", "CH", "CZ", "SK", "PL",
                        "HU", "RO", "SI", "HR", "GR", "BG", "IE", "FI", "EE", "LV",
                        "LT", "LU", "JP", "KR", "CN", "HK", "SG", "IN", "ID", "MY",
                        "PH", "TH", "VN", "NZ", "ZA", "EG", "MA", "NG", "KE", "IL",
                        "TR", "AE", "SA", "MX", "AR", "CL", "CO", "PE", "UY", "IS"}:
                return head
            break
    # Single-token zone id — Energy-Charts / ENTSO-E style such as "SE4",
    # "NO2", "DK1" where the trailing digit(s) are a bidding-zone suffix.
    # Strip digits and check whether the two-letter head is a country.
    import re as _re
    head = _re.sub(r"\d+$", "", z)
    if len(head) == 2 and head.isalpha():
        return head
    if len(z) == 2 and z.isalpha():
        return z
    return ""


def get_tariff_templates_for_country(country: str) -> List[Dict]:
    """Return the tariff template list for the given country code.

    Falls back to the generic EU templates when the country is unknown
    or has no curated template set.
    """
    if not country:
        return list(GENERIC_EU_TARIFF_TEMPLATES)
    c = country.strip().upper()
    if c in _COUNTRY_ALIASES:
        c = _COUNTRY_ALIASES[c]
    return list(TARIFF_TEMPLATES_BY_COUNTRY.get(c) or GENERIC_EU_TARIFF_TEMPLATES)


def simulate_fixed_tariff(
    hourly_kwh: List[float],
    price_eur_per_kwh: float,
    base_fee_eur_per_year: float,
) -> float:
    """Simulate annual cost under a fixed tariff."""
    total_kwh = sum(hourly_kwh)
    hours_in_data = len(hourly_kwh)
    if hours_in_data <= 0:
        return 0.0
    annual_kwh = total_kwh * (8760 / hours_in_data)
    return annual_kwh * price_eur_per_kwh + base_fee_eur_per_year


def simulate_tou_tariff(
    hourly_kwh: List[float],
    hourly_hours: List[int],
    ht_price: float, nt_price: float,
    ht_start: int, ht_end: int,
    base_fee_eur_per_year: float,
) -> float:
    """Simulate annual cost under a time-of-use tariff."""
    total_cost = 0.0
    for kwh, hour in zip(hourly_kwh, hourly_hours):
        if ht_start <= hour < ht_end:
            total_cost += kwh * ht_price
        else:
            total_cost += kwh * nt_price
    hours_in_data = len(hourly_kwh)
    if hours_in_data <= 0:
        return 0.0
    annual_factor = 8760 / hours_in_data
    return total_cost * annual_factor + base_fee_eur_per_year


def simulate_spot_tariff(
    hourly_kwh: List[float],
    hourly_spot_ct: List[float],
    markup_ct: float,
    base_fee_eur_per_year: float,
    vat_rate: float = 0.19,
) -> float:
    """Simulate annual cost under a dynamic spot tariff."""
    total_cost = 0.0
    for kwh, spot_ct in zip(hourly_kwh, hourly_spot_ct):
        price_ct = (spot_ct + markup_ct) * (1.0 + vat_rate)
        total_cost += kwh * price_ct / 100.0
    hours_in_data = len(hourly_kwh)
    if hours_in_data <= 0:
        return 0.0
    annual_factor = 8760 / hours_in_data
    return total_cost * annual_factor + base_fee_eur_per_year


def _get_consumption_stats(db, cfg) -> Optional[Dict]:
    """Return consumption summary stats used for tariff comparison."""
    import time
    now = int(time.time())
    start_ts = now - 90 * 86400
    try:
        total_kwh = 0.0
        hours = 0
        devices = cfg.devices if hasattr(cfg, 'devices') else []
        for dev in devices:
            if getattr(dev, 'kind', 'em') != 'em':
                continue
            try:
                df = db.query_hourly(dev.key, start_ts, now)
                if df is not None and not df.empty:
                    total_kwh += float(df["kwh"].sum())
                    hours += len(df)
            except Exception:
                pass
        if hours <= 0:
            return None
        days = hours / 24
        annual_kwh = total_kwh * (8760 / hours)
        return {"total_kwh": total_kwh, "hours": hours, "days": days, "annual_kwh": annual_kwh}
    except Exception:
        return None


def compare_tariffs(
    db, cfg,
    current_price_eur_per_kwh: float = 0.3265,
    current_base_fee_eur_per_year: float = 127.51,
    vat_rate: float = 0.19,
) -> List[TariffResult]:
    """Compare current tariff against all templates using actual consumption data."""
    import time
    now = int(time.time())
    # Use last 90 days of hourly data
    start_ts = now - 90 * 86400

    results: List[TariffResult] = []

    try:
        # Collect hourly consumption across all devices
        hourly_kwh: List[float] = []
        hourly_hours: List[int] = []
        hourly_spot_ct: List[float] = []

        devices = cfg.devices if hasattr(cfg, 'devices') else []
        for dev in devices:
            if getattr(dev, 'kind', 'em') != 'em':
                continue
            try:
                df = db.query_hourly(dev.key, start_ts, now)
                if df is not None and not df.empty:
                    for _, row in df.iterrows():
                        h_ts = int(row.get("hour_ts", 0))
                        kwh = float(row.get("kwh", 0) or 0)
                        hourly_kwh.append(kwh)
                        hourly_hours.append((h_ts % 86400) // 3600)
            except Exception:
                pass

        if not hourly_kwh:
            return []

        # Get spot prices
        try:
            zone = getattr(cfg.spot_price, 'bidding_zone', 'DE-LU')
            spot_df = db.query_spot_prices(zone, start_ts, now)
            if spot_df is not None and not spot_df.empty:
                spot_map: Dict[int, float] = {}
                for _, r in spot_df.iterrows():
                    spot_map[int(r["slot_ts"])] = float(r["price_eur_mwh"]) / 10.0  # to ct/kWh
                # Fill spot prices per hour
                hourly_spot_ct = [spot_map.get(h, 5.0) for h in range(len(hourly_kwh))]
            else:
                hourly_spot_ct = [5.0] * len(hourly_kwh)  # Default 5 ct/kWh wholesale
        except Exception:
            hourly_spot_ct = [5.0] * len(hourly_kwh)

        total_kwh = sum(hourly_kwh)
        hours = len(hourly_kwh)
        annual_kwh = total_kwh * (8760 / hours) if hours > 0 else 0

        # Current tariff
        current_annual = annual_kwh * current_price_eur_per_kwh + current_base_fee_eur_per_year

        # Add current tariff
        results.append(TariffResult(
            name="Current tariff",
            provider="Current",
            tariff_type="fixed",
            annual_cost_eur=round(current_annual, 2),
            monthly_avg_eur=round(current_annual / 12, 2),
            effective_price_ct=round(current_price_eur_per_kwh * 100, 2),
            savings_vs_current_eur=0.0,
            is_current=True,
        ))

        # Pick the template list that matches the user's country. Derived
        # from the spot-price bidding zone they already configured under
        # Settings → Spot prices (no extra setup required).
        country = _country_from_zone(getattr(cfg.spot_price, "bidding_zone", "") or "")
        templates = get_tariff_templates_for_country(country)

        # Simulate each template
        for tmpl in templates:
            tt = tmpl.get("tariff_type", "fixed")
            if tt == "fixed":
                annual = simulate_fixed_tariff(
                    hourly_kwh,
                    tmpl["price_eur_per_kwh"],
                    tmpl["base_fee_eur_per_year"],
                )
                eff_ct = tmpl["price_eur_per_kwh"] * 100
            elif tt == "tou":
                annual = simulate_tou_tariff(
                    hourly_kwh, hourly_hours,
                    tmpl.get("ht_price", 0.35), tmpl.get("nt_price", 0.22),
                    tmpl.get("ht_start", 6), tmpl.get("ht_end", 22),
                    tmpl["base_fee_eur_per_year"],
                )
                eff_ct = round((annual - tmpl["base_fee_eur_per_year"]) / max(annual_kwh, 1) * 100, 2) if annual_kwh > 0 else 0
            elif tt == "spot":
                annual = simulate_spot_tariff(
                    hourly_kwh, hourly_spot_ct,
                    tmpl.get("spot_markup_ct", 15.0),
                    tmpl["base_fee_eur_per_year"],
                    vat_rate,
                )
                eff_ct = round((annual - tmpl["base_fee_eur_per_year"]) / max(annual_kwh, 1) * 100, 2) if annual_kwh > 0 else 0
            else:
                continue

            results.append(TariffResult(
                name=tmpl["name"],
                provider=tmpl.get("provider", ""),
                tariff_type=tt,
                annual_cost_eur=round(annual, 2),
                monthly_avg_eur=round(annual / 12, 2),
                effective_price_ct=round(eff_ct, 2),
                savings_vs_current_eur=round(current_annual - annual, 2),
            ))

        # Sort by annual cost
        results.sort(key=lambda r: r.annual_cost_eur)

    except Exception as e:
        _log.error("Tariff comparison error: %s", e)

    return results
