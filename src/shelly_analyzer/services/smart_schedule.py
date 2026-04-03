from __future__ import annotations
from dataclasses import dataclass
from typing import List, Optional, Tuple
import logging

_log = logging.getLogger(__name__)

@dataclass
class ScheduleRecommendation:
    start_ts: int
    end_ts: int
    avg_price_ct: float
    savings_vs_avg_ct: float
    block_hours: float

def find_cheapest_block(
    spot_prices: List[Tuple[int, float]],  # [(slot_ts, price_eur_mwh), ...]
    duration_hours: float = 3.0,
    earliest_ts: int = 0,
    latest_ts: int = 0,
) -> Optional[ScheduleRecommendation]:
    """Find the cheapest consecutive block of `duration_hours` within the spot price window.

    spot_prices: sorted list of (slot_ts, price_eur_mwh) tuples with uniform resolution.
    Returns ScheduleRecommendation or None if not enough data.
    """
    if not spot_prices or duration_hours <= 0:
        return None

    # Filter to time window
    prices = sorted(spot_prices, key=lambda x: x[0])
    if earliest_ts:
        prices = [(ts, p) for ts, p in prices if ts >= earliest_ts]
    if latest_ts:
        prices = [(ts, p) for ts, p in prices if ts <= latest_ts]

    if len(prices) < 2:
        return None

    # Detect resolution
    resolution_s = prices[1][0] - prices[0][0]
    if resolution_s <= 0:
        resolution_s = 3600

    slots_needed = max(1, int(duration_hours * 3600 / resolution_s))
    if len(prices) < slots_needed:
        return None

    # Sliding window
    best_sum = float('inf')
    best_start_idx = 0

    current_sum = sum(p for _, p in prices[:slots_needed])
    if current_sum < best_sum:
        best_sum = current_sum
        best_start_idx = 0

    for i in range(1, len(prices) - slots_needed + 1):
        current_sum -= prices[i - 1][1]
        current_sum += prices[i + slots_needed - 1][1]
        if current_sum < best_sum:
            best_sum = current_sum
            best_start_idx = i

    avg_all = sum(p for _, p in prices) / len(prices) if prices else 0
    avg_block = best_sum / slots_needed if slots_needed > 0 else 0

    # Convert EUR/MWh to ct/kWh
    avg_block_ct = avg_block / 10.0
    avg_all_ct = avg_all / 10.0

    return ScheduleRecommendation(
        start_ts=prices[best_start_idx][0],
        end_ts=prices[best_start_idx + slots_needed - 1][0] + resolution_s,
        avg_price_ct=round(avg_block_ct, 2),
        savings_vs_avg_ct=round(avg_all_ct - avg_block_ct, 2),
        block_hours=duration_hours,
    )


def get_schedule_recommendations(
    db, zone: str, duration_hours: float = 3.0,
) -> Optional[ScheduleRecommendation]:
    """Get a scheduling recommendation using spot prices from the database."""
    import time
    now = int(time.time())
    # Look 24h ahead from now
    start = (now // 3600) * 3600
    end = start + 24 * 3600

    try:
        df = db.query_spot_prices(zone, start, end)
        if df is None or df.empty:
            return None
        prices = list(zip(df["slot_ts"].astype(int), df["price_eur_mwh"].astype(float)))
        return find_cheapest_block(prices, duration_hours, earliest_ts=start, latest_ts=end)
    except Exception as e:
        _log.warning("Smart schedule: %s", e)
        return None
