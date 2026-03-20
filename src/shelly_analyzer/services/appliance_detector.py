"""NILM-Light appliance detector.

Matches a live power reading (in Watts) against a built-in database of
household appliance power signatures and returns a ranked list of candidate
devices with a confidence score.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Tuple


@dataclass(frozen=True)
class ApplianceSignature:
    """Power signature for a household appliance."""

    id: str                      # i18n key: appliance.{id}.name
    category: str                # appliance category
    icon: str                    # display emoji
    power_min: float             # minimum typical power (W)
    power_max: float             # maximum typical power (W)
    pattern_type: str            # "constant" | "cyclic" | "variable" | "short_peak"
    typical_duration_min: float  # typical operating duration (minutes)


#: Built-in appliance signature database
APPLIANCES: List[ApplianceSignature] = [
    ApplianceSignature("fridge",           "cooling",       "❄️",  80,    200,   "cyclic",      20),
    ApplianceSignature("freezer",          "cooling",       "🧊",  100,   300,   "cyclic",      25),
    ApplianceSignature("washing_machine",  "laundry",       "🫧",  300,   2200,  "variable",    90),
    ApplianceSignature("dryer",            "laundry",       "🌀",  1800,  5000,  "constant",    60),
    ApplianceSignature("dishwasher",       "kitchen",       "🍽️", 1200,  2400,  "variable",    60),
    ApplianceSignature("oven",             "kitchen",       "🔥",  2000,  3500,  "cyclic",      60),
    ApplianceSignature("hob",              "kitchen",       "🍳",  1000,  3500,  "constant",    30),
    ApplianceSignature("microwave",        "kitchen",       "📡",  600,   1500,  "short_peak",   5),
    ApplianceSignature("kettle",           "kitchen",       "☕",  1500,  3000,  "short_peak",   3),
    ApplianceSignature("coffee_machine",   "kitchen",       "☕",  800,   1500,  "short_peak",   5),
    ApplianceSignature("toaster",          "kitchen",       "🍞",  800,   1500,  "short_peak",   4),
    ApplianceSignature("iron",             "laundry",       "👔",  1000,  2500,  "cyclic",      30),
    ApplianceSignature("hair_dryer",       "personal_care", "💨",  1000,  2200,  "constant",    10),
    ApplianceSignature("vacuum",           "cleaning",      "🌪️", 500,   2000,  "variable",    20),
    ApplianceSignature("ev_charger",       "transport",     "⚡",  2300,  11000, "constant",   300),
    ApplianceSignature("heat_pump",        "heating",       "🌡️", 1000,  5000,  "cyclic",      30),
    ApplianceSignature("boiler_instant",   "heating",       "🚿",  18000, 27000, "constant",     5),
    ApplianceSignature("boiler_storage",   "heating",       "🛁",  2000,  4000,  "cyclic",      60),
    ApplianceSignature("tv",               "entertainment", "📺",  50,    400,   "constant",   120),
    ApplianceSignature("pc",               "entertainment", "🖥️", 200,   800,   "variable",   120),
    ApplianceSignature("laptop",           "entertainment", "💻",  30,    90,    "constant",   120),
    ApplianceSignature("router",           "network",       "📡",  5,     20,    "constant",  1440),
    ApplianceSignature("led_light",        "lighting",      "💡",  5,     100,   "constant",   240),
    ApplianceSignature("air_conditioner",  "heating",       "🌬️", 1000,  4000,  "cyclic",      60),
    ApplianceSignature("fan",              "heating",       "🌀",  30,    100,   "constant",    60),
]


def identify_appliance(power_watts: float) -> List[Tuple[ApplianceSignature, float]]:
    """Match a live power reading against the appliance database.

    Returns a list of ``(ApplianceSignature, confidence)`` tuples sorted by
    confidence descending.  Only appliances whose power range contains the
    measured value (with ±5 % boundary tolerance) are included.

    Confidence is 1.0 when the reading is at the centre of the appliance's
    power range and falls to 0.0 toward the boundaries.  Readings that only
    match within the tolerance zone receive a fixed low confidence of 0.25.
    """
    if power_watts <= 0:
        return []

    results: List[Tuple[ApplianceSignature, float]] = []
    tolerance = 0.05  # ±5 % beyond declared range boundaries

    for sig in APPLIANCES:
        lo = sig.power_min * (1.0 - tolerance)
        hi = sig.power_max * (1.0 + tolerance)
        if not (lo <= power_watts <= hi):
            continue

        center = (sig.power_min + sig.power_max) / 2.0
        half_range = (sig.power_max - sig.power_min) / 2.0

        if half_range == 0.0:
            confidence = 1.0
        elif sig.power_min <= power_watts <= sig.power_max:
            # Inside the declared range: linear falloff from centre to boundary
            dist = abs(power_watts - center)
            confidence = max(0.0, 1.0 - dist / (half_range * 1.1))
        else:
            # In tolerance zone only: lower fixed confidence
            confidence = 0.25

        results.append((sig, round(confidence, 3)))

    results.sort(key=lambda x: x[1], reverse=True)
    return results[:10]
