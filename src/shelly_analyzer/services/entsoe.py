"""ENTSO-E Transparency Platform API client for grid CO₂ intensity.

Fetches actual generation per production type (DocumentType=A75) for a given
bidding zone and calculates the grid CO₂ intensity in g/kWh using standard
emission factors.

Optionally fetches cross-border physical flows (DocumentType=A11) and total
load (DocumentType=A65) to adjust the CO₂ intensity for electricity
imports/exports between bidding zones.

Rate limiting: ENTSO-E enforces 400 requests/min per token.  We are
conservative and cap at 1 request/min by default.

References:
  https://transparency.entsoe.eu/content/static_content/Static%20content/
  web%20api/Guide.html
"""
from __future__ import annotations

import logging
import math
import threading
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ENTSO-E REST API base URL
_API_BASE = "https://web-api.tp.entsoe.eu/api"

# EIC codes for ENTSO-E bidding zones
_EIC_CODES: Dict[str, str] = {
    "DE_LU":    "10Y1001A1001A83F",
    "AT":       "10YAT-APG------L",
    "BE":       "10YBE----------2",
    "BG":       "10YCA-BULGARIA-R",
    "CH":       "10YCH-SWISSGRIDZ",
    "CZ":       "10YCZ-CEPS-----N",
    "DE_AT_LU": "10Y1001A1001A63L",
    "DK_1":     "10YDK-1--------W",
    "DK_2":     "10YDK-2--------M",
    "EE":       "10Y1001A1001A39I",
    "ES":       "10YES-REE------0",
    "FI":       "10YFI-1--------U",
    "FR":       "10YFR-RTE------C",
    "GB":       "10YGB----------A",
    "GR":       "10YGR-HTSO-----Y",
    "HR":       "10YHR-HEP------M",
    "HU":       "10YHU-MAVIR----U",
    "IE_SEM":   "10Y1001A1001A59C",
    "IT_NORD":  "10Y1001A1001A73I",
    "LT":       "10YLT-1001A0008Q",
    "LU":       "10YLU-CEGEDEL-NQ",
    "LV":       "10YLV-1001A00074",
    "NL":       "10YNL----------L",
    "NO_1":     "10YNO-1--------2",
    "NO_2":     "10YNO-2--------T",
    "NO_3":     "10YNO-3--------J",
    "NO_4":     "10YNO-4--------9",
    "NO_5":     "10Y1001A1001A48H",
    "PL":       "10YPL-AREA-----S",
    "PT":       "10YPT-REN------W",
    "RO":       "10YRO-TEL------P",
    "RS":       "10YCS-SERBIATSOV",
    "SE_1":     "10Y1001A1001A44P",
    "SE_2":     "10Y1001A1001A45N",
    "SE_3":     "10Y1001A1001A46L",
    "SE_4":     "10Y1001A1001A47J",
    "SI":       "10YSI-ELES-----O",
    "SK":       "10YSK-SEPS-----K",
}

# ENTSO-E psrType codes → internal fuel names
# Source: ENTSO-E Transparency Platform API Guide, Table 8 (psrType)
# https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html
_PSR_NAMES: Dict[str, str] = {
    "B01": "biomass",           # Biomass
    "B02": "lignite",           # Fossil Brown coal/Lignite
    "B03": "coal_gas",          # Fossil Coal-derived gas
    "B04": "gas",               # Fossil Gas
    "B05": "hard_coal",         # Fossil Hard coal
    "B06": "oil",               # Fossil Oil
    "B07": "oil_shale",         # Fossil Oil shale
    "B08": "peat",              # Fossil Peat
    "B09": "geothermal",        # Geothermal
    "B10": "hydro_pumped",      # Hydro Pumped Storage
    "B11": "hydro_run",         # Hydro Run-of-river and poundage
    "B12": "hydro_reservoir",   # Hydro Water Reservoir
    "B13": "marine",            # Marine
    "B14": "nuclear",           # Nuclear
    "B15": "other_renewable",   # Other renewable
    "B16": "solar",             # Solar
    "B17": "waste",             # Waste
    "B18": "wind_offshore",     # Wind Offshore
    "B19": "wind_onshore",      # Wind Onshore
    "B20": "other",             # Other
}

# Human-readable display names for the CO₂ tab fuel-mix table
FUEL_DISPLAY_NAMES: Dict[str, str] = {
    "biomass":          "Biomass (B01)",
    "lignite":          "Lignite / Brown coal (B02)",
    "coal_gas":         "Coal-derived gas (B03)",
    "gas":              "Natural gas (B04)",
    "hard_coal":        "Hard coal (B05)",
    "oil":              "Oil (B06)",
    "oil_shale":        "Oil shale (B07)",
    "peat":             "Peat (B08)",
    "geothermal":       "Geothermal (B09)",
    "hydro_pumped":     "Hydro – Pumped storage (B10)",
    "hydro_run":        "Hydro – Run-of-river (B11)",
    "hydro_reservoir":  "Hydro – Reservoir (B12)",
    "marine":           "Marine (B13)",
    "nuclear":          "Nuclear (B14)",
    "other_renewable":  "Other renewable (B15)",
    "solar":            "Solar (B16)",
    "waste":            "Waste (B17)",
    "wind_offshore":    "Wind offshore (B18)",
    "wind_onshore":     "Wind onshore (B19)",
    "other":            "Other (B20)",
}

# CO₂ emission factors in g CO₂eq/kWh (lifecycle, IPCC AR5 median values)
# References: IPCC AR5 WG3 Annex II Table A.III.2; EEA 2023
_CO2_FACTORS: Dict[str, float] = {
    "biomass":          230.0,   # Biomass (B01)
    "lignite":         1100.0,   # Fossil Brown coal/Lignite (B02)
    "coal_gas":         700.0,   # Fossil Coal-derived gas (B03)
    "gas":              490.0,   # Fossil Gas (B04)
    "hard_coal":        820.0,   # Fossil Hard coal (B05)
    "oil":              650.0,   # Fossil Oil (B06)
    "oil_shale":        800.0,   # Fossil Oil shale (B07)
    "peat":            1150.0,   # Fossil Peat (B08)
    "geothermal":        38.0,   # Geothermal (B09)
    "hydro_pumped":      24.0,   # Hydro Pumped Storage (B10) – lifecycle incl. pumping losses
    "hydro_run":          4.0,   # Hydro Run-of-river (B11)
    "hydro_reservoir":    4.0,   # Hydro Water Reservoir (B12)
    "marine":             8.0,   # Marine (B13)
    "nuclear":           12.0,   # Nuclear (B14)
    "other_renewable":   30.0,   # Other renewable (B15)
    "solar":             45.0,   # Solar (B16)
    "waste":            330.0,   # Waste (B17)
    "wind_offshore":     12.0,   # Wind Offshore (B18)
    "wind_onshore":      11.0,   # Wind Onshore (B19)
    "other":            400.0,   # Other (B20) – conservative estimate
}

# Neighboring bidding zones for cross-border flow adjustment.
# Each entry maps a zone to its list of physical interconnection partners.
_ZONE_NEIGHBORS: Dict[str, List[str]] = {
    "DE_LU":    ["AT", "BE", "CH", "CZ", "DK_1", "DK_2", "FR", "NL", "NO_2", "PL", "SE_4"],
    "AT":       ["CH", "CZ", "DE_LU", "HU", "IT_NORD", "SI", "SK"],
    "BE":       ["DE_LU", "FR", "GB", "NL"],
    "BG":       ["GR", "RO", "RS"],
    "CH":       ["AT", "DE_LU", "FR", "IT_NORD"],
    "CZ":       ["AT", "DE_LU", "PL", "SK"],
    "DK_1":     ["DE_LU", "DK_2", "NL", "NO_2", "SE_3"],
    "DK_2":     ["DE_LU", "DK_1", "SE_4"],
    "EE":       ["FI", "LT", "LV"],
    "ES":       ["FR", "PT"],
    "FI":       ["EE", "NO_4", "SE_1", "SE_3"],
    "FR":       ["BE", "CH", "DE_LU", "ES", "GB", "IT_NORD"],
    "GB":       ["BE", "FR", "IE_SEM", "NL", "NO_2"],
    "GR":       ["BG", "IT_NORD"],
    "HR":       ["HU", "RS", "SI"],
    "HU":       ["AT", "HR", "RO", "RS", "SK"],
    "IE_SEM":   ["GB"],
    "IT_NORD":  ["AT", "CH", "FR", "GR", "SI"],
    "LT":       ["EE", "LV", "PL", "SE_4"],
    "LV":       ["EE", "LT"],
    "NL":       ["BE", "DE_LU", "DK_1", "GB", "NO_2"],
    "NO_1":     ["NO_2", "NO_3", "NO_5", "SE_3"],
    "NO_2":     ["DE_LU", "DK_1", "GB", "NL", "NO_1", "NO_5"],
    "NO_3":     ["NO_1", "NO_4", "NO_5", "SE_2"],
    "NO_4":     ["FI", "NO_3", "SE_1", "SE_2"],
    "NO_5":     ["NO_1", "NO_2", "NO_3"],
    "PL":       ["CZ", "DE_LU", "LT", "SE_4", "SK"],
    "PT":       ["ES"],
    "RO":       ["BG", "HU", "RS"],
    "RS":       ["BG", "HR", "HU", "RO"],
    "SE_1":     ["FI", "NO_4", "SE_2"],
    "SE_2":     ["NO_3", "NO_4", "SE_1", "SE_3"],
    "SE_3":     ["DK_1", "FI", "NO_1", "SE_2", "SE_4"],
    "SE_4":     ["DE_LU", "DK_2", "LT", "PL", "SE_3"],
    "SI":       ["AT", "HR", "IT_NORD"],
    "SK":       ["CZ", "HU", "PL"],
}

# Static annual average CO₂ intensity per zone (g/kWh).
# Used as fallback for neighbor zones instead of fetching their full generation
# mix.  Based on EEA / ENTSO-E historical data (2022-2024 averages).
_STATIC_ZONE_INTENSITY: Dict[str, float] = {
    "AT":       130.0,   # High hydro share
    "BE":       170.0,   # Nuclear + gas
    "BG":       450.0,   # Lignite heavy
    "CH":        30.0,   # Hydro + nuclear dominated
    "CZ":       450.0,   # Lignite + nuclear
    "DE_LU":    380.0,   # Mixed: coal, gas, renewables
    "DE_AT_LU": 380.0,   # Same as DE_LU
    "DK_1":     150.0,   # High wind share
    "DK_2":     180.0,   # Wind + imports
    "EE":       600.0,   # Oil shale
    "ES":       170.0,   # Solar + wind + nuclear
    "FI":       100.0,   # Nuclear + hydro + biomass
    "FR":        60.0,   # Nuclear dominated
    "GB":       200.0,   # Gas + wind + nuclear
    "GR":       350.0,   # Lignite + gas + renewables
    "HR":       200.0,   # Hydro + gas
    "HU":       250.0,   # Nuclear + gas
    "IE_SEM":   300.0,   # Gas heavy + wind
    "IT_NORD":  330.0,   # Gas + hydro
    "LT":       200.0,   # Varies, imports
    "LU":       300.0,   # Mostly imports
    "LV":       150.0,   # Hydro + gas
    "NL":       350.0,   # Gas heavy
    "NO_1":      15.0,   # Hydro
    "NO_2":      15.0,   # Hydro
    "NO_3":      15.0,   # Hydro
    "NO_4":      15.0,   # Hydro
    "NO_5":      15.0,   # Hydro
    "PL":       700.0,   # Coal dominated
    "PT":       200.0,   # Renewables + gas
    "RO":       300.0,   # Hydro + nuclear + coal
    "RS":       700.0,   # Lignite heavy
    "SE_1":      25.0,   # Hydro
    "SE_2":      25.0,   # Hydro + wind
    "SE_3":      25.0,   # Nuclear + hydro + wind
    "SE_4":      25.0,   # Nuclear + wind
    "SI":       250.0,   # Nuclear + hydro + coal
    "SK":       150.0,   # Nuclear + hydro
}

# XML namespace used in ENTSO-E responses (A75 generation, A65 load)
_NS = "urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0"

# XML namespace for A11 cross-border transmission documents
_NS_TRANSMISSION = "urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3"


def _ts_to_entsoe_fmt(ts: int) -> str:
    """Convert Unix timestamp to ENTSO-E datetime string YYYYMMDDHHММ."""
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    return dt.strftime("%Y%m%d%H%M")


def _parse_generation_xml(xml_text: str) -> Dict[str, Dict[int, float]]:
    """Parse ENTSO-E A75 XML.  Returns {fuel: {hour_ts: MW}}.

    Each time series contains multiple Period blocks.  Each Period has a
    resolution (PT60M or PT15M) and quantity points.  We aggregate all
    points into hourly buckets.
    """
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        logger.error("XML parse error: %s", exc)
        return {}

    results: Dict[str, Dict[int, float]] = {}

    for ts in root.iter(f"{{{_NS}}}TimeSeries"):
        # Get fuel type
        psr_el = ts.find(f".//{{{_NS}}}psrType")
        if psr_el is None:
            continue
        psr_code = (psr_el.text or "").strip()
        fuel = _PSR_NAMES.get(psr_code, "other")

        for period in ts.iter(f"{{{_NS}}}Period"):
            start_el = period.find(f"{{{_NS}}}timeInterval/{{{_NS}}}start")
            res_el = period.find(f"{{{_NS}}}resolution")
            if start_el is None or res_el is None:
                continue

            try:
                start_dt = datetime.strptime(start_el.text.strip(), "%Y-%m-%dT%H:%MZ").replace(tzinfo=timezone.utc)
            except ValueError:
                continue

            resolution_text = (res_el.text or "PT60M").strip()
            if resolution_text == "PT15M":
                step_minutes = 15
            elif resolution_text == "PT30M":
                step_minutes = 30
            else:
                step_minutes = 60

            if fuel not in results:
                results[fuel] = {}

            for point in period.iter(f"{{{_NS}}}Point"):
                pos_el = point.find(f"{{{_NS}}}position")
                qty_el = point.find(f"{{{_NS}}}quantity")
                if pos_el is None or qty_el is None:
                    continue
                try:
                    pos = int(pos_el.text.strip()) - 1  # 1-based → 0-based
                    qty = float(qty_el.text.strip())
                except (ValueError, TypeError):
                    continue

                pt_dt = start_dt + timedelta(minutes=pos * step_minutes)
                # Snap to hour boundary
                hour_dt = pt_dt.replace(minute=0, second=0, microsecond=0)
                hour_ts = int(hour_dt.timestamp())

                results[fuel][hour_ts] = results[fuel].get(hour_ts, 0.0) + qty

    return results


# Installed solar capacity (MW) per zone (approximate, 2024 data)
_SOLAR_CAPACITY_MW: Dict[str, float] = {
    "DE_LU": 82000, "ES": 27000, "IT_NORD": 14000, "IT_CSUD": 10000,
    "IT_SUD": 8000, "FR": 20000, "NL": 22000, "BE": 8000, "AT": 5000,
    "PL": 17000, "CZ": 2500, "GR": 6000, "PT": 3000, "HU": 5000,
    "RO": 2000, "BG": 2500, "SK": 600, "DK_1": 2500, "DK_2": 1500,
    "SE_3": 3000, "SE_4": 2000, "FI": 700, "CH": 6000, "GB": 16000,
}

# Typical solar capacity factor by hour (0-23) and month (1-12) for Central Europe
# Values are fraction of installed capacity (0.0-0.7)
_SOLAR_PROFILE_HOUR = [
    0, 0, 0, 0, 0, 0.02, 0.08, 0.18, 0.30, 0.42, 0.52, 0.58,
    0.60, 0.58, 0.52, 0.42, 0.30, 0.18, 0.08, 0.02, 0, 0, 0, 0,
]
_SOLAR_PROFILE_MONTH = [
    0.3, 0.4, 0.55, 0.65, 0.75, 0.80,  # Jan-Jun
    0.80, 0.75, 0.65, 0.50, 0.35, 0.25, # Jul-Dec
]


def _estimate_solar_if_missing(
    mix: Dict[str, Dict[int, float]],
    zone: str,
) -> Dict[str, Dict[int, float]]:
    """Add estimated solar generation if 'solar' key is missing from the mix.

    Uses installed capacity × typical capacity factor for hour/month.
    This is a rough estimate since ENTSO-E often delays solar data by 1-2 days.
    """
    capacity = _SOLAR_CAPACITY_MW.get(zone, 0)
    if capacity <= 0:
        return mix

    # Get all hour timestamps from any existing fuel
    all_hours: set = set()
    for fuel_hours in mix.values():
        all_hours.update(fuel_hours.keys())
    if not all_hours:
        return mix

    solar_data: Dict[int, float] = {}
    for h_ts in all_hours:
        dt = datetime.fromtimestamp(h_ts, tz=timezone.utc)
        hour = dt.hour
        month = dt.month
        cf_hour = _SOLAR_PROFILE_HOUR[hour] if 0 <= hour < 24 else 0
        cf_month = _SOLAR_PROFILE_MONTH[month - 1] if 1 <= month <= 12 else 0.5
        estimated_mw = capacity * cf_hour * cf_month
        if estimated_mw > 0:
            solar_data[h_ts] = estimated_mw

    if solar_data:
        mix = dict(mix)
        mix["solar"] = solar_data
        logger.info("Solar estimated for %s: added %d hours (capacity %.0f MW)", zone, len(solar_data), capacity)

    return mix


def _parse_crossborder_xml(xml_text: str) -> Dict[int, float]:
    """Parse ENTSO-E A11 cross-border physical flow XML.

    Returns {hour_ts: MW} where positive values indicate flow in the
    direction specified by in_Domain ← out_Domain (i.e. imports into
    in_Domain).

    The A11 document may use different XML namespaces depending on ENTSO-E
    version; we try multiple known namespaces.
    """
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        logger.error("Cross-border XML parse error: %s", exc)
        return {}

    # Detect namespace from root tag
    ns = ""
    root_tag = root.tag
    if root_tag.startswith("{"):
        ns = root_tag[1:root_tag.index("}")]

    result: Dict[int, float] = {}

    for ts in root.iter(f"{{{ns}}}TimeSeries" if ns else "TimeSeries"):
        for period in ts.iter(f"{{{ns}}}Period" if ns else "Period"):
            start_el = period.find(
                f"{{{ns}}}timeInterval/{{{ns}}}start" if ns else "timeInterval/start"
            )
            res_el = period.find(f"{{{ns}}}resolution" if ns else "resolution")
            if start_el is None or res_el is None:
                continue

            try:
                start_text = start_el.text.strip()
                # Handle both Z and +00:00 suffixes
                start_text = start_text.replace("+00:00", "Z")
                if start_text.endswith("Z"):
                    start_text = start_text[:-1]
                start_dt = datetime.strptime(start_text, "%Y-%m-%dT%H:%M").replace(
                    tzinfo=timezone.utc
                )
            except ValueError:
                continue

            resolution_text = (res_el.text or "PT60M").strip()
            if resolution_text == "PT15M":
                step_minutes = 15
            elif resolution_text == "PT30M":
                step_minutes = 30
            else:
                step_minutes = 60

            for point in period.iter(f"{{{ns}}}Point" if ns else "Point"):
                pos_el = point.find(f"{{{ns}}}position" if ns else "position")
                qty_el = point.find(f"{{{ns}}}quantity" if ns else "quantity")
                if pos_el is None or qty_el is None:
                    continue
                try:
                    pos = int(pos_el.text.strip()) - 1
                    qty = float(qty_el.text.strip())
                except (ValueError, TypeError):
                    continue

                pt_dt = start_dt + timedelta(minutes=pos * step_minutes)
                hour_dt = pt_dt.replace(minute=0, second=0, microsecond=0)
                hour_ts = int(hour_dt.timestamp())
                result[hour_ts] = result.get(hour_ts, 0.0) + qty

    return result


def _parse_load_xml(xml_text: str) -> Dict[int, float]:
    """Parse ENTSO-E A65 system total load XML.

    Returns {hour_ts: MW}.  Uses the same GL_MarketDocument namespace as A75.
    """
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        logger.error("Load XML parse error: %s", exc)
        return {}

    result: Dict[int, float] = {}

    for ts in root.iter(f"{{{_NS}}}TimeSeries"):
        for period in ts.iter(f"{{{_NS}}}Period"):
            start_el = period.find(f"{{{_NS}}}timeInterval/{{{_NS}}}start")
            res_el = period.find(f"{{{_NS}}}resolution")
            if start_el is None or res_el is None:
                continue

            try:
                start_dt = datetime.strptime(
                    start_el.text.strip(), "%Y-%m-%dT%H:%MZ"
                ).replace(tzinfo=timezone.utc)
            except ValueError:
                continue

            resolution_text = (res_el.text or "PT60M").strip()
            if resolution_text == "PT15M":
                step_minutes = 15
            elif resolution_text == "PT30M":
                step_minutes = 30
            else:
                step_minutes = 60

            for point in period.iter(f"{{{_NS}}}Point"):
                pos_el = point.find(f"{{{_NS}}}position")
                qty_el = point.find(f"{{{_NS}}}quantity")
                if pos_el is None or qty_el is None:
                    continue
                try:
                    pos = int(pos_el.text.strip()) - 1
                    qty = float(qty_el.text.strip())
                except (ValueError, TypeError):
                    continue

                pt_dt = start_dt + timedelta(minutes=pos * step_minutes)
                hour_dt = pt_dt.replace(minute=0, second=0, microsecond=0)
                hour_ts = int(hour_dt.timestamp())
                result[hour_ts] = result.get(hour_ts, 0.0) + qty

    return result


def calculate_intensity(generation_mix: Dict[str, Dict[int, float]]) -> Dict[int, float]:
    """Compute weighted-average CO₂ intensity per hour from a generation mix.

    Returns {hour_ts: g_per_kwh}.
    """
    all_hours = set()
    for fuel_hours in generation_mix.values():
        all_hours.update(fuel_hours.keys())

    result: Dict[int, float] = {}
    for hour_ts in sorted(all_hours):
        total_mw = 0.0
        weighted_co2 = 0.0
        for fuel, fuel_hours in generation_mix.items():
            mw = fuel_hours.get(hour_ts, 0.0)
            if mw <= 0:
                continue
            factor = _CO2_FACTORS.get(fuel, 300.0)
            total_mw += mw
            weighted_co2 += mw * factor
        if total_mw > 0:
            result[hour_ts] = weighted_co2 / total_mw
        else:
            result[hour_ts] = 0.0
    return result


def calculate_intensity_with_flows(
    generation_mix: Dict[str, Dict[int, float]],
    local_intensity: Dict[int, float],
    load_mw: Dict[int, float],
    import_flows: Dict[str, Dict[int, float]],
    neighbor_intensities: Optional[Dict[str, float]] = None,
) -> Dict[int, float]:
    """Compute flow-adjusted CO₂ intensity per hour.

    Formula per hour:
        local_gen_co2 = local_gen_MW × local_intensity
        import_co2    = Σ(import_MW_from_zone_i × zone_i_intensity)
        total_supply  = total_load_MW  (from A65)
        export_MW     = local_gen_MW + total_imports - total_load
        export_co2    = export_MW × local_intensity  (assumes local mix)
        adjusted      = (local_gen_co2 + import_co2 - export_co2) / total_load

    Falls back to local-only intensity when load data is missing.
    """
    if neighbor_intensities is None:
        neighbor_intensities = {}

    all_hours = set(local_intensity.keys())
    result: Dict[int, float] = {}

    for hour_ts in sorted(all_hours):
        li = local_intensity.get(hour_ts)
        if li is None:
            continue

        # Total local generation for this hour
        local_gen_mw = sum(
            fh.get(hour_ts, 0.0)
            for fh in generation_mix.values()
            if fh.get(hour_ts, 0.0) > 0
        )

        total_load = load_mw.get(hour_ts, 0.0)

        # If no load data, fall back to local-only intensity
        if total_load <= 0:
            result[hour_ts] = li
            continue

        # Sum all imports and their CO₂ contribution
        total_import_mw = 0.0
        import_co2 = 0.0
        for zone, flows in import_flows.items():
            mw = flows.get(hour_ts, 0.0)
            if mw > 0:
                total_import_mw += mw
                zone_intensity = neighbor_intensities.get(
                    zone, _STATIC_ZONE_INTENSITY.get(zone, 400.0)
                )
                import_co2 += mw * zone_intensity

        # Exports = local generation + imports - load (energy balance)
        export_mw = max(0.0, local_gen_mw + total_import_mw - total_load)
        export_co2 = export_mw * li

        # Adjusted intensity
        local_gen_co2 = local_gen_mw * li
        numerator = local_gen_co2 + import_co2 - export_co2
        if numerator < 0:
            numerator = 0.0

        adjusted = numerator / total_load
        result[hour_ts] = adjusted

    return result


class EntsoeClient:
    """ENTSO-E Transparency Platform API client.

    Usage::

        client = EntsoeClient(api_token="...", bidding_zone="DE_LU")
        rows = client.fetch_intensity(start_ts=..., end_ts=...)
        # rows: list of (hour_ts, zone, intensity_g_per_kwh, source, fetched_at)

    The client enforces a per-instance rate limit of 1 request per 62 seconds
    to stay well within ENTSO-E's quota.
    """

    def __init__(
        self,
        api_token: str,
        bidding_zone: str = "DE_LU",
        min_request_interval: float = 62.0,
    ) -> None:
        self.api_token = api_token
        self.bidding_zone = bidding_zone
        self._min_interval = min_request_interval
        self._last_request_ts: float = 0.0
        self._lock = threading.Lock()
        # Raw generation mix from the most recent fetch: {fuel: {hour_ts: MW}}
        self.last_mix: Dict[str, Dict[int, float]] = {}

    # ── Public ───────────────────────────────────────────────────────────────

    def fetch_intensity(
        self,
        start_ts: int,
        end_ts: int,
    ) -> List[Tuple[int, str, float, str, int]]:
        """Fetch CO₂ intensity for [start_ts, end_ts).

        Returns a list of (hour_ts, zone, intensity_g_per_kwh, source, fetched_at)
        ready for EnergyDB.upsert_co2_intensity().

        Raises RuntimeError on API errors.
        """
        if not self.api_token:
            raise RuntimeError("No ENTSO-E API token configured.")

        xml_text = self._fetch_generation_xml(start_ts, end_ts)
        if not xml_text or not xml_text.strip():
            raise RuntimeError("ENTSO-E API returned an empty response.")
        if "GL_MarketDocument" not in xml_text[:600]:
            logger.warning(
                "EntsoeClient: unexpected response (no GL_MarketDocument). "
                "Response prefix: %s", xml_text[:300]
            )
        mix = _parse_generation_xml(xml_text)
        if not mix:
            logger.warning(
                "EntsoeClient: no generation time series found in XML "
                "(zone=%s, start=%s, end=%s). Response prefix: %s",
                self.bidding_zone,
                _ts_to_entsoe_fmt(start_ts),
                _ts_to_entsoe_fmt(end_ts),
                xml_text[:200],
            )
        self.last_mix = mix  # cache for caller inspection

        # Estimate solar if missing from ENTSO-E data (common for DE_LU real-time)
        if "solar" not in mix and mix:
            mix = _estimate_solar_if_missing(mix, self.bidding_zone)

        intensity = calculate_intensity(mix)

        now_ts = int(time.time())
        rows = [
            (hour_ts, self.bidding_zone, g_per_kwh, "entsoe", now_ts)
            for hour_ts, g_per_kwh in sorted(intensity.items())
            if start_ts <= hour_ts < end_ts
        ]
        logger.info(
            "EntsoeClient: fetched %d intensity points for zone %s",
            len(rows),
            self.bidding_zone,
        )
        return rows

    def fetch_intensity_with_flows(
        self,
        start_ts: int,
        end_ts: int,
        progress_cb=None,
    ) -> List[Tuple[int, str, float, str, int]]:
        """Fetch CO₂ intensity with cross-border flow adjustment.

        1. Fetches local generation (A75) and computes base intensity
        2. Fetches total load (A65) for the zone
        3. Fetches import flows (A11) from each neighbor zone
        4. Adjusts intensity using flow-weighted neighbor CO₂ values

        Returns rows ready for EnergyDB.upsert_co2_intensity().
        The progress_cb(step_description: str) is called for each API request.
        """
        if not self.api_token:
            raise RuntimeError("No ENTSO-E API token configured.")

        neighbors = _ZONE_NEIGHBORS.get(self.bidding_zone, [])
        if not neighbors:
            logger.warning(
                "EntsoeClient: no neighbor mapping for zone %s, "
                "falling back to local-only intensity",
                self.bidding_zone,
            )
            return self.fetch_intensity(start_ts, end_ts)

        # Step 1: Local generation (A75) – reuse existing method
        if progress_cb:
            progress_cb(f"Erzeugungsmix {self.bidding_zone} (A75)")
        xml_text = self._fetch_generation_xml(start_ts, end_ts)
        if not xml_text or not xml_text.strip():
            raise RuntimeError("ENTSO-E API returned empty A75 response.")
        mix = _parse_generation_xml(xml_text)
        self.last_mix = mix
        local_intensity = calculate_intensity(mix)

        if not local_intensity:
            logger.warning("EntsoeClient: no generation data, cannot adjust")
            return []

        # Step 2: Total load (A65)
        if progress_cb:
            progress_cb(f"Gesamtlast {self.bidding_zone} (A65)")
        try:
            load_xml = self._fetch_load_xml(start_ts, end_ts)
            load_mw = _parse_load_xml(load_xml) if load_xml else {}
        except Exception as exc:
            logger.warning("EntsoeClient: A65 load fetch failed: %s", exc)
            load_mw = {}

        # Step 3: Cross-border import flows (A11) per neighbor
        import_flows: Dict[str, Dict[int, float]] = {}
        for neighbor in neighbors:
            if progress_cb:
                progress_cb(f"Import {neighbor} → {self.bidding_zone} (A11)")
            try:
                flow_xml = self._fetch_crossborder_xml(
                    from_zone=neighbor, to_zone=self.bidding_zone,
                    start_ts=start_ts, end_ts=end_ts,
                )
                flows = _parse_crossborder_xml(flow_xml) if flow_xml else {}
                if flows:
                    import_flows[neighbor] = flows
                    logger.info(
                        "EntsoeClient: %s → %s: %d flow points",
                        neighbor, self.bidding_zone, len(flows),
                    )
            except Exception as exc:
                logger.warning(
                    "EntsoeClient: A11 %s→%s failed: %s",
                    neighbor, self.bidding_zone, exc,
                )

        # Step 4: Calculate adjusted intensity
        if import_flows and load_mw:
            adjusted = calculate_intensity_with_flows(
                generation_mix=mix,
                local_intensity=local_intensity,
                load_mw=load_mw,
                import_flows=import_flows,
            )
            source = "entsoe_cbf"
            logger.info(
                "EntsoeClient: cross-border adjusted %d hours using %d/%d neighbors",
                len(adjusted), len(import_flows), len(neighbors),
            )
        else:
            adjusted = local_intensity
            source = "entsoe"
            if not load_mw:
                logger.warning("EntsoeClient: no load data, using local-only intensity")
            if not import_flows:
                logger.warning("EntsoeClient: no import flows, using local-only intensity")

        now_ts = int(time.time())
        rows = [
            (hour_ts, self.bidding_zone, g_per_kwh, source, now_ts)
            for hour_ts, g_per_kwh in sorted(adjusted.items())
            if start_ts <= hour_ts < end_ts
        ]
        logger.info(
            "EntsoeClient: %d intensity points (source=%s) for zone %s",
            len(rows), source, self.bidding_zone,
        )
        return rows

    # ── Internal ─────────────────────────────────────────────────────────────

    def _wait_rate_limit(self) -> None:
        with self._lock:
            elapsed = time.monotonic() - self._last_request_ts
            if elapsed < self._min_interval:
                wait = self._min_interval - elapsed
                logger.debug("EntsoeClient: rate-limit wait %.1fs", wait)
                time.sleep(wait)
            self._last_request_ts = time.monotonic()

    def _fetch_generation_xml(self, start_ts: int, end_ts: int) -> str:
        """HTTP GET to ENTSO-E API and return raw XML text."""
        import urllib.request
        import urllib.parse
        import urllib.error

        self._wait_rate_limit()

        eic = _EIC_CODES.get(self.bidding_zone, self.bidding_zone)
        params = {
            "securityToken": self.api_token,
            "documentType": "A75",
            "processType": "A16",
            "in_Domain": eic,
            "periodStart": _ts_to_entsoe_fmt(start_ts),
            "periodEnd": _ts_to_entsoe_fmt(end_ts),
        }
        url = _API_BASE + "?" + urllib.parse.urlencode(params)
        logger.debug("EntsoeClient: GET %s", url[:120])

        try:
            req = urllib.request.Request(url)
            req.add_header("Accept", "application/xml")
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = resp.read()
                return raw.decode("utf-8", errors="replace")
        except urllib.error.HTTPError as exc:
            raise RuntimeError(
                f"ENTSO-E API HTTP {exc.code}: {exc.reason}"
            ) from exc
        except Exception as exc:
            raise RuntimeError(f"ENTSO-E API request failed: {exc}") from exc

    def _fetch_load_xml(self, start_ts: int, end_ts: int) -> str:
        """Fetch A65 (system total load) XML for the configured zone."""
        import urllib.request
        import urllib.parse
        import urllib.error

        self._wait_rate_limit()

        eic = _EIC_CODES.get(self.bidding_zone, self.bidding_zone)
        params = {
            "securityToken": self.api_token,
            "documentType": "A65",
            "processType": "A16",
            "outBiddingZone_Domain": eic,
            "periodStart": _ts_to_entsoe_fmt(start_ts),
            "periodEnd": _ts_to_entsoe_fmt(end_ts),
        }
        url = _API_BASE + "?" + urllib.parse.urlencode(params)
        logger.debug("EntsoeClient: GET A65 %s", url[:120])

        try:
            req = urllib.request.Request(url)
            req.add_header("Accept", "application/xml")
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = resp.read()
                return raw.decode("utf-8", errors="replace")
        except urllib.error.HTTPError as exc:
            raise RuntimeError(
                f"ENTSO-E A65 HTTP {exc.code}: {exc.reason}"
            ) from exc
        except Exception as exc:
            raise RuntimeError(f"ENTSO-E A65 request failed: {exc}") from exc

    def _fetch_crossborder_xml(
        self, from_zone: str, to_zone: str,
        start_ts: int, end_ts: int,
    ) -> str:
        """Fetch A11 (physical cross-border flow) XML.

        Returns flow data for electricity flowing from from_zone into to_zone.
        """
        import urllib.request
        import urllib.parse
        import urllib.error

        self._wait_rate_limit()

        out_eic = _EIC_CODES.get(from_zone, from_zone)
        in_eic = _EIC_CODES.get(to_zone, to_zone)
        params = {
            "securityToken": self.api_token,
            "documentType": "A11",
            "in_Domain": in_eic,
            "out_Domain": out_eic,
            "periodStart": _ts_to_entsoe_fmt(start_ts),
            "periodEnd": _ts_to_entsoe_fmt(end_ts),
        }
        url = _API_BASE + "?" + urllib.parse.urlencode(params)
        logger.debug("EntsoeClient: GET A11 %s→%s %s", from_zone, to_zone, url[:120])

        try:
            req = urllib.request.Request(url)
            req.add_header("Accept", "application/xml")
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = resp.read()
                return raw.decode("utf-8", errors="replace")
        except urllib.error.HTTPError as exc:
            if exc.code == 400:
                # No data for this border pair – not an error
                logger.debug(
                    "EntsoeClient: A11 %s→%s returned 400 (no data)",
                    from_zone, to_zone,
                )
                return ""
            raise RuntimeError(
                f"ENTSO-E A11 HTTP {exc.code}: {exc.reason}"
            ) from exc
        except Exception as exc:
            raise RuntimeError(f"ENTSO-E A11 request failed: {exc}") from exc


class Co2FetchService:
    """Background service that periodically fetches CO₂ intensity from ENTSO-E
    and stores results in the EnergyDB.

    Usage::

        svc = Co2FetchService(db=db, get_config=lambda: app.cfg)
        svc.start()
        svc.trigger_now()   # optional: fetch immediately
        svc.stop()
    """

    def __init__(self, db, get_config) -> None:
        self._db = db
        self._get_config = get_config
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._trigger_event = threading.Event()
        self._last_fetch_ts: float = 0.0
        self._last_error: Optional[str] = None
        self._force_backfill: bool = False
        self._progress_callback = None  # callable(day_fetched: int, total_days: int) | None
        self._log_callback = None       # callable(msg: str) | None – for Sync tab
        # Latest fuel mix for UI display: {fuel: mw_for_most_recent_hour}
        # Try loading persisted mix from DB on init
        self._latest_mix_hour: Optional[int] = None
        self._latest_mix: Dict[str, float] = {}
        try:
            cfg = self._get_config()
            zone = str(getattr(cfg, "bidding_zone", "DE_LU") or "DE_LU")
            h, m = self._db.query_latest_fuel_mix(zone)
            if h is not None and m:
                self._latest_mix_hour = h
                self._latest_mix = m
        except Exception:
            pass

    def set_progress_callback(self, cb) -> None:
        """Set a callback invoked during chunk fetching: cb(day_fetched, total_days).

        Called from the background thread – must be thread-safe (use a queue).
        Pass None to remove.
        """
        self._progress_callback = cb

    def set_log_callback(self, cb) -> None:
        """Set a callback for log messages: cb(msg: str).

        Called from the background thread – the callback must schedule any UI
        updates on the main thread (e.g. via widget.after(0, ...)).
        Pass None to remove.
        """
        self._log_callback = cb

    def get_latest_mix(self) -> Tuple[Optional[int], Dict[str, float]]:
        """Return (hour_ts, {fuel: mw}) for the most recent fetched hour.

        Returns (None, {}) if no fetch has occurred in this session.
        """
        return self._latest_mix_hour, dict(self._latest_mix)

    def _svc_log(self, msg: str) -> None:
        logger.info("Co2FetchService: %s", msg)
        cb = self._log_callback
        if cb is not None:
            try:
                cb(msg)
            except Exception:
                pass

    @property
    def last_error(self) -> Optional[str]:
        return self._last_error

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="Co2FetchService",
            daemon=True,
        )
        self._thread.start()
        logger.info("Co2FetchService started")

    def stop(self) -> None:
        self._stop_event.set()
        self._trigger_event.set()
        if self._thread:
            self._thread.join(timeout=10)
        logger.info("Co2FetchService stopped")

    def trigger_now(self, force: bool = False) -> None:
        """Force an immediate fetch.

        force=True re-fetches from backfill_days ago, ignoring any already-stored
        data.
        """
        self._last_fetch_ts = 0.0
        if force:
            self._force_backfill = True
        self._trigger_event.set()

    def _run(self) -> None:
        # Small initial delay, but wake early if trigger_now() is called
        self._trigger_event.wait(5.0)
        self._trigger_event.clear()
        while not self._stop_event.is_set():
            try:
                self._tick()
            except Exception:
                logger.exception("Co2FetchService tick error")
            # Wait up to fetch_interval_hours, but wake early on trigger
            try:
                cfg = self._get_config()
                interval_h = getattr(getattr(cfg, "co2", None), "fetch_interval_hours", 1) or 1
            except Exception:
                interval_h = 1
            self._trigger_event.wait(interval_h * 3600)
            self._trigger_event.clear()

    def _tick(self) -> None:
        try:
            cfg = self._get_config()
        except Exception:
            return

        co2_cfg = getattr(cfg, "co2", None)
        if co2_cfg is None or not getattr(co2_cfg, "enabled", False):
            return
        token = getattr(co2_cfg, "entso_e_api_token", "") or ""
        if not token:
            return

        zone = getattr(co2_cfg, "bidding_zone", "DE_LU") or "DE_LU"
        cross_border = getattr(co2_cfg, "cross_border_flows", False)

        now_ts = int(time.time())
        force = self._force_backfill
        self._force_backfill = False

        # Find oldest energy measurement to know how far back CO₂ data is needed
        oldest_measurement = self._db.oldest_measurement_ts()
        if oldest_measurement is None:
            # No measurements yet – just fetch the last 2 days
            oldest_measurement = now_ts - 2 * 86400

        # Always check the FULL range from oldest measurement to now for gaps
        range_start = (oldest_measurement // 3600) * 3600
        range_end = ((now_ts // 3600) + 1) * 3600  # next full hour

        if range_start >= range_end:
            return

        # Find all missing hours (real gaps, no data at all)
        gaps = self._db.find_co2_gaps(zone, range_start, range_end, include_estimated=False)

        if not gaps and not force:
            logger.debug("Co2FetchService: data is complete for zone %s", zone)
            return

        if force:
            # Forced: re-fetch entire range
            fetch_ranges = [(range_start, range_end)]
        elif gaps:
            # Merge nearby gaps into larger fetch ranges (API works in day chunks
            # anyway, so fetching a few extra hours is cheaper than many small calls)
            fetch_ranges = []
            for gap_start, gap_end in gaps:
                # Align to day boundaries for efficient API usage
                aligned_start = (gap_start // 86400) * 86400
                aligned_end = min(((gap_end + 86399) // 86400) * 86400, range_end)
                if fetch_ranges and aligned_start <= fetch_ranges[-1][1]:
                    # Merge with previous range
                    fetch_ranges[-1] = (fetch_ranges[-1][0], max(fetch_ranges[-1][1], aligned_end))
                else:
                    fetch_ranges.append((aligned_start, aligned_end))
        else:
            return

        total_hours_missing = sum((e - s) // 3600 for s, e in gaps) if gaps else (range_end - range_start) // 3600
        total_days_fetch = max(1, math.ceil(sum(e - s for s, e in fetch_ranges) / 86400))
        d_from = datetime.fromtimestamp(fetch_ranges[0][0], tz=timezone.utc).strftime("%Y-%m-%d")
        d_to = datetime.fromtimestamp(fetch_ranges[-1][1], tz=timezone.utc).strftime("%Y-%m-%d")
        self._svc_log(
            f"CO₂ Import: {total_hours_missing} fehlende Stunden, "
            f"{len(fetch_ranges)} Bereich(e), Zone {zone} ({d_from} → {d_to})"
        )

        # Split each fetch range into chunks of at most 7 days
        client = EntsoeClient(api_token=token, bidding_zone=zone)
        chunk_s = 7 * 86400
        total_written = 0
        failed_ranges: list = []
        days_fetched = 0
        cb = self._progress_callback
        max_retries = 3

        for fetch_start, fetch_end in fetch_ranges:
            cursor = fetch_start
            while cursor < fetch_end and not self._stop_event.is_set():
                chunk_end = min(cursor + chunk_s, fetch_end)
                if cb is not None:
                    try:
                        cb(days_fetched, total_days_fetch)
                    except Exception:
                        pass
                c_from = datetime.fromtimestamp(cursor, tz=timezone.utc).strftime("%Y-%m-%d")
                c_to = datetime.fromtimestamp(chunk_end, tz=timezone.utc).strftime("%Y-%m-%d")
                self._svc_log(f"  ENTSO-E Abfrage: {c_from} bis {c_to}...")

                chunk_ok = False
                for attempt in range(1, max_retries + 1):
                    if self._stop_event.is_set():
                        break
                    try:
                        if cross_border:
                            rows = client.fetch_intensity_with_flows(
                                cursor, chunk_end,
                                progress_cb=lambda msg: self._svc_log(f"    {msg}"),
                            )
                        else:
                            rows = client.fetch_intensity(cursor, chunk_end)
                        # Write intensity data immediately per chunk (crash-safe)
                        if rows:
                            written = self._db.upsert_co2_intensity(rows)
                            total_written += written
                        self._last_error = None
                        self._svc_log(f"    Empfangen: {len(rows)} Datenpunkte")
                        # Store ALL hours' fuel mix for historical navigation
                        raw_mix = client.last_mix
                        if raw_mix:
                            all_hours = sorted({h for fh in raw_mix.values() for h in fh})
                            if all_hours:
                                # Include solar estimate if missing
                                _enriched = _estimate_solar_if_missing(raw_mix, self.bidding_zone) if "solar" not in raw_mix else raw_mix
                                for h_ts in all_hours:
                                    h_mix = {
                                        fuel: fh[h_ts]
                                        for fuel, fh in _enriched.items()
                                        if fh.get(h_ts, 0.0) > 0
                                    }
                                    if h_mix:
                                        try:
                                            self._db.upsert_fuel_mix(h_ts, zone, h_mix)
                                        except Exception:
                                            pass
                                # Cache latest hour for UI
                                latest_h = max(all_hours)
                                hour_mix = {
                                    fuel: fh[latest_h]
                                    for fuel, fh in _enriched.items()
                                    if fh.get(latest_h, 0.0) > 0
                                }
                                if latest_h >= (self._latest_mix_hour or 0):
                                    self._latest_mix_hour = latest_h
                                    self._latest_mix = hour_mix
                                self._svc_log(f"    Kraftwerksmix: {len(all_hours)} Stunden gespeichert")
                                # Log latest hour breakdown
                                total_mw = sum(hour_mix.values())
                                lh_str = datetime.fromtimestamp(latest_h, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
                                self._svc_log(f"    Aktuellste Stunde ({lh_str}, {total_mw:.0f} MW gesamt):")
                                for fuel, mw in sorted(hour_mix.items(), key=lambda x: -x[1]):
                                    share = mw / total_mw * 100 if total_mw else 0
                                    factor = _CO2_FACTORS.get(fuel, 400.0)
                                    name = FUEL_DISPLAY_NAMES.get(fuel, fuel)
                                    self._svc_log(f"      {name}: {mw:.0f} MW ({share:.1f}%) – {factor:.0f} g/kWh")
                        chunk_ok = True
                        break
                    except Exception as exc:
                        self._last_error = str(exc)
                        if attempt < max_retries:
                            wait_s = attempt * 30  # 30s, 60s backoff
                            self._svc_log(
                                f"    Fehler (Versuch {attempt}/{max_retries}): {exc} – "
                                f"Wiederholung in {wait_s}s..."
                            )
                            logger.warning(
                                "Co2FetchService: chunk %s→%s attempt %d/%d failed: %s",
                                c_from, c_to, attempt, max_retries, exc,
                            )
                            self._stop_event.wait(wait_s)
                        else:
                            self._svc_log(
                                f"    Fehler (Versuch {attempt}/{max_retries}): {exc} – "
                                f"Chunk übersprungen, fahre mit nächstem fort."
                            )
                            logger.warning(
                                "Co2FetchService: chunk %s→%s failed after %d retries, skipping",
                                c_from, c_to, max_retries,
                            )

                if not chunk_ok:
                    failed_ranges.append((cursor, chunk_end))

                days_fetched += max(1, round((chunk_end - cursor) / 86400))
                cursor = chunk_end

        if total_written:
            self._svc_log(f"CO₂ Import: {total_written} Werte gespeichert")
            logger.info("Co2FetchService: stored %d intensity points", total_written)

        # ── Gap detection & estimated-value fill ──────────────────────────
        # Find hours in the full range that are still missing after fetch
        # and fill them with estimated values so every hour is covered.
        if not self._stop_event.is_set():
            remaining_gaps = self._db.find_co2_gaps(zone, range_start, range_end)
            if remaining_gaps:
                total_missing = sum((e - s) // 3600 for s, e in remaining_gaps)
                self._svc_log(
                    f"CO₂ Lückenerkennung: {total_missing} fehlende Stunden gefunden"
                )

                # Try to compute a fallback intensity from the data we do have
                df = self._db.query_co2_intensity(zone, range_start, range_end)
                if not df.empty:
                    avg_intensity = float(df["intensity_g_per_kwh"].mean())
                else:
                    # No data at all – use a conservative default for DE grid
                    avg_intensity = 400.0

                now_ts_fill = int(time.time())
                estimated_rows = []
                for gap_start, gap_end in remaining_gaps:
                    ts = gap_start
                    while ts < gap_end:
                        estimated_rows.append(
                            (ts, zone, round(avg_intensity, 1), "estimated", now_ts_fill)
                        )
                        ts += 3600

                if estimated_rows:
                    est_written = self._db.upsert_co2_intensity(estimated_rows)
                    self._svc_log(
                        f"CO₂ Lücken gefüllt: {est_written} geschätzte Werte "
                        f"({avg_intensity:.0f} g/kWh Durchschnitt) eingefügt"
                    )

        if cb is not None:
            try:
                cb(total_days_fetch, total_days_fetch)
            except Exception:
                pass

        if failed_ranges:
            n = len(failed_ranges)
            self._svc_log(
                f"CO₂ Import abgeschlossen mit {n} fehlgeschlagenen Chunk(s) – "
                f"Lücken wurden mit Schätzwerten aufgefüllt"
            )
        elif total_written == 0 and not failed_ranges:
            self._svc_log("CO₂ Import abgeschlossen: 0 Werte – keine Daten empfangen")
        else:
            self._svc_log("CO₂ Import abgeschlossen")
        self._last_fetch_ts = time.time()

        # ── Fuel mix recovery ────────────────────────────────────────────
        # If we still have no fuel mix (e.g. first run after upgrade, or
        # today's data not yet available), try fetching generation data
        # for yesterday where data is reliably available.
        if not self._latest_mix and not self._stop_event.is_set():
            try:
                self._svc_log("Kraftwerksmix: Lade letzte verfügbare Daten...")
                # Wait for ENTSO-E rate limit (62s between requests)
                self._svc_log("  Warte 65s (ENTSO-E Rate-Limit)...")
                self._stop_event.wait(65)
                if self._stop_event.is_set():
                    return
                # Fetch yesterday's full day – data is always available
                recovery_end = ((now_ts // 3600)) * 3600
                recovery_start = recovery_end - 48 * 3600
                recovery_rows = client.fetch_intensity(recovery_start, recovery_end)
                raw_mix = client.last_mix
                if raw_mix:
                    _enriched2 = _estimate_solar_if_missing(raw_mix, self.bidding_zone) if "solar" not in raw_mix else raw_mix
                    all_hours = sorted({h for fh in _enriched2.values() for h in fh})
                    if all_hours:
                        # Store all hours' mix
                        for h_ts in all_hours:
                            h_mix = {
                                fuel: fh[h_ts]
                                for fuel, fh in _enriched2.items()
                                if fh.get(h_ts, 0.0) > 0
                            }
                            if h_mix:
                                self._db.upsert_fuel_mix(h_ts, zone, h_mix)
                        latest_h = max(all_hours)
                        hour_mix = {
                            fuel: fh[latest_h]
                            for fuel, fh in _enriched2.items()
                            if fh.get(latest_h, 0.0) > 0
                        }
                        if hour_mix:
                            self._latest_mix_hour = latest_h
                            self._latest_mix = hour_mix
                            total_mw = sum(hour_mix.values())
                            lh_str = datetime.fromtimestamp(latest_h, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
                            self._svc_log(f"Kraftwerksmix geladen: {len(all_hours)} Stunden, aktuellste: {lh_str}, {total_mw:.0f} MW")
                            if recovery_rows:
                                self._db.upsert_co2_intensity(recovery_rows)
                if not self._latest_mix:
                    self._svc_log("Kraftwerksmix: Keine Daten verfügbar")
            except Exception as exc:
                self._svc_log(f"Kraftwerksmix Recovery fehlgeschlagen: {exc}")
                logger.warning("Fuel mix recovery failed: %s", exc)
