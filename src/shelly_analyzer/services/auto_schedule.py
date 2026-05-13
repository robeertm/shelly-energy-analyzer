"""Spot-price-driven auto-switching scheduler.

For each rule the user configures (one per device-relay), this scheduler:

  1. Pulls today's spot prices for the rule's local-time window.
  2. Finds the cheapest contiguous `duration_hours` block in that window.
  3. If `max_price_ct` is set, skips rules whose cheapest block is above it.
  4. While `NOW` is inside that block, asks the relay to be ON.
     While outside, asks the relay to be OFF — but only if we ourselves
     turned it on. Manual user toggles win.
  5. In `dry_run` mode, every decision is logged and exposed via the
     status API; no relay is actually switched.

Safety:
- `dry_run=True` by default per rule. User must explicitly opt in.
- `max_runs_per_day` caps OFF→ON transitions per UTC day.
- We never toggle a relay we didn't put in its current state — i.e. if
  the user flipped the device on manually, the scheduler keeps its
  hands off until the next planned block.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

_TZ = ZoneInfo("Europe/Berlin")


@dataclass
class RuleDecision:
    """Latest decision the scheduler took for one rule."""
    rule_id: str
    rule_name: str
    enabled: bool
    dry_run: bool
    decision: str = "idle"          # "idle" | "on" | "off" | "skipped" | "no_data"
    reason: str = ""                # human-readable explanation
    block_start_ts: Optional[int] = None
    block_end_ts: Optional[int] = None
    block_avg_ct: Optional[float] = None
    last_evaluated_ts: int = 0
    runs_today: int = 0
    last_run_day: str = ""
    last_set_on: Optional[bool] = None   # what we asked the relay to be last time (None = never)
    last_action_ts: int = 0
    last_error: str = ""


class AutoSchedulerController:
    """Background controller that evaluates AutoScheduleRule list every tick."""

    DEFAULT_TICK_SECONDS = 60

    def __init__(
        self,
        get_config: Callable[[], Any],
        get_spot_prices: Callable[[str, int, int], "Any"],
        switch_callback: Optional[Callable[[str, int, bool], None]] = None,
        tick_seconds: int = DEFAULT_TICK_SECONDS,
    ):
        """
        Args:
            get_config: callable returning the current AppConfig (live, since
                config can change at runtime via Settings save).
            get_spot_prices: callable (zone, start_ts, end_ts) -> DataFrame
                with columns slot_ts (UTC seconds) and price_eur_mwh.
            switch_callback: callable (device_key, switch_id, on_bool) that
                actually toggles the relay. None disables live switching.
            tick_seconds: evaluation cadence. 60 s is plenty since blocks
                are hour-resolution anyway.
        """
        self._get_config = get_config
        self._get_spot_prices = get_spot_prices
        self._switch_callback = switch_callback
        self._tick_seconds = tick_seconds
        self._decisions: Dict[str, RuleDecision] = {}
        self._lock = threading.Lock()
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

    # ── Lifecycle ──────────────────────────────────────────────────────

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._loop, name="AutoSchedule", daemon=True)
        self._thread.start()
        logger.info("AutoScheduler started (tick=%ds)", self._tick_seconds)

    def stop(self) -> None:
        self._running = False
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)
            self._thread = None
        logger.info("AutoScheduler stopped")

    # ── State access ───────────────────────────────────────────────────

    def get_decisions(self) -> List[RuleDecision]:
        with self._lock:
            return list(self._decisions.values())

    # ── Core loop ──────────────────────────────────────────────────────

    def _loop(self) -> None:
        while self._running and not self._stop_event.is_set():
            try:
                self._tick()
            except Exception as exc:
                logger.exception("AutoScheduler tick failed: %s", exc)
            if self._stop_event.wait(self._tick_seconds):
                return

    def _tick(self) -> None:
        cfg = self._get_config()
        ss = getattr(cfg, "smart_schedule", None)
        if ss is None:
            return
        # Global kill switch first
        if not bool(getattr(ss, "auto_schedule_enabled", False)):
            with self._lock:
                self._decisions.clear()
            return
        rules = list(getattr(ss, "auto_rules", []) or [])
        if not rules:
            with self._lock:
                self._decisions.clear()
            return

        spot_cfg = getattr(cfg, "spot_price", None)
        zone = str(getattr(spot_cfg, "bidding_zone", "DE-LU") or "DE-LU") if spot_cfg else "DE-LU"

        now_utc = int(time.time())
        now_local = datetime.now(_TZ)
        today_local = now_local.date()
        today_str = today_local.isoformat()
        weekday = now_local.weekday()  # 0=Mon..6=Sun

        # Keep only decisions for rules that still exist
        existing_ids = {r.rule_id for r in rules if r.rule_id}
        with self._lock:
            self._decisions = {rid: d for rid, d in self._decisions.items() if rid in existing_ids}

        for rule in rules:
            try:
                self._eval_rule(rule, zone, now_utc, now_local, weekday, today_str)
            except Exception as exc:
                logger.warning("Rule %s eval failed: %s", getattr(rule, "rule_id", "?"), exc)

    # ── Per-rule evaluation ────────────────────────────────────────────

    def _eval_rule(self, rule, zone, now_utc, now_local, weekday, today_str) -> None:
        rid = rule.rule_id or f"_anon_{id(rule)}"
        dec = self._decisions.get(rid) or RuleDecision(
            rule_id=rid, rule_name=rule.name or rid,
            enabled=bool(rule.enabled), dry_run=bool(rule.dry_run),
        )
        dec.rule_name = rule.name or rid
        dec.enabled = bool(rule.enabled)
        dec.dry_run = bool(rule.dry_run)
        dec.last_evaluated_ts = now_utc

        # New day: reset daily counter
        if dec.last_run_day != today_str:
            dec.runs_today = 0
            dec.last_run_day = today_str

        if not rule.enabled:
            dec.decision = "idle"
            dec.reason = "rule disabled"
            self._commit(rid, dec)
            return
        if weekday not in (rule.weekdays or []):
            dec.decision = "skipped"
            dec.reason = f"weekday {weekday} not in {rule.weekdays}"
            self._commit(rid, dec)
            return
        if not rule.device_key:
            dec.decision = "skipped"
            dec.reason = "no device_key configured"
            self._commit(rid, dec)
            return

        # Build today's window in local time → UTC
        start_local = now_local.replace(hour=int(rule.earliest_hour), minute=0,
                                        second=0, microsecond=0)
        end_local_hour = int(rule.latest_hour)
        if end_local_hour >= 24:
            end_local = (start_local + timedelta(days=1)).replace(hour=0)
        else:
            end_local = start_local.replace(hour=end_local_hour)
        win_start_ts = int(start_local.timestamp())
        win_end_ts = int(end_local.timestamp())
        if win_end_ts <= win_start_ts:
            dec.decision = "skipped"
            dec.reason = f"invalid window {rule.earliest_hour}..{rule.latest_hour}"
            self._commit(rid, dec)
            return

        # Fetch prices, fall back to "no_data" if scarce
        try:
            df = self._get_spot_prices(zone, win_start_ts, win_end_ts)
        except Exception as exc:
            dec.decision = "no_data"
            dec.reason = f"spot price fetch failed: {exc}"
            self._commit(rid, dec)
            return
        if df is None or len(df) == 0:
            dec.decision = "no_data"
            dec.reason = f"no spot prices for zone={zone} window={rule.earliest_hour}..{rule.latest_hour}"
            self._commit(rid, dec)
            return

        # Aggregate to hourly
        prices_by_hour: Dict[int, float] = {}
        try:
            for _, row in df.iterrows():
                ts = int(row["slot_ts"])
                hour_ts = (ts // 3600) * 3600
                prices_by_hour.setdefault(hour_ts, []).append(float(row["price_eur_mwh"]))  # type: ignore
        except Exception:
            pass
        hourly: List[Tuple[int, float]] = sorted(
            (h, sum(v) / len(v) if isinstance(v, list) else float(v))
            for h, v in prices_by_hour.items()
        )
        if not hourly:
            dec.decision = "no_data"
            dec.reason = "could not aggregate prices to hourly"
            self._commit(rid, dec)
            return

        # Sliding window: find cheapest block of `duration_hours` length.
        duration_h = max(1, int(round(rule.duration_hours)))
        if len(hourly) < duration_h:
            dec.decision = "no_data"
            dec.reason = (f"only {len(hourly)} hourly slots, need {duration_h}")
            self._commit(rid, dec)
            return

        best_start_idx = 0
        best_avg_eur_mwh = float("inf")
        for i in range(0, len(hourly) - duration_h + 1):
            avg = sum(p for _, p in hourly[i:i + duration_h]) / duration_h
            if avg < best_avg_eur_mwh:
                best_avg_eur_mwh = avg
                best_start_idx = i
        block_start_ts = hourly[best_start_idx][0]
        block_end_ts = hourly[best_start_idx + duration_h - 1][0] + 3600
        # Wholesale ct/kWh (gross of surcharges; user sees same number as smart_schedule UI)
        block_avg_ct = best_avg_eur_mwh / 10.0
        dec.block_start_ts = block_start_ts
        dec.block_end_ts = block_end_ts
        dec.block_avg_ct = round(block_avg_ct, 2)

        # max_price filter (sentinel < -100 means "no limit")
        if rule.max_price_ct >= -100.0 and block_avg_ct > rule.max_price_ct:
            self._desire_off(rule, dec, now_utc,
                             reason=f"cheapest block @ {block_avg_ct:.2f} ct/kWh > limit {rule.max_price_ct:.2f}")
            self._commit(rid, dec)
            return

        # Inside block?
        in_block = block_start_ts <= now_utc < block_end_ts
        if in_block:
            self._desire_on(rule, dec, now_utc,
                            reason=f"in cheap block ({block_avg_ct:.2f} ct/kWh, ends {self._fmt_local(block_end_ts)})")
        else:
            self._desire_off(rule, dec, now_utc,
                             reason=f"outside cheap block (next at {self._fmt_local(block_start_ts)})")
        self._commit(rid, dec)

    # ── Desire-vs-action helpers ───────────────────────────────────────

    def _desire_on(self, rule, dec: RuleDecision, now_utc: int, reason: str) -> None:
        dec.decision = "on"
        dec.reason = reason
        # Already on (we last set it on) → nothing to do
        if dec.last_set_on is True:
            return
        # Daily runs cap
        if dec.runs_today >= max(1, int(rule.max_runs_per_day)):
            dec.decision = "skipped"
            dec.reason = f"max_runs_per_day reached ({dec.runs_today})"
            return
        # Trigger
        self._trigger(rule, dec, on=True, now_utc=now_utc)
        if dec.last_error == "":
            dec.runs_today += 1

    def _desire_off(self, rule, dec: RuleDecision, now_utc: int, reason: str) -> None:
        dec.decision = "off"
        dec.reason = reason
        # Only turn off if we previously turned on. Never fight manual user toggles.
        if dec.last_set_on is not True:
            return
        self._trigger(rule, dec, on=False, now_utc=now_utc)

    def _trigger(self, rule, dec: RuleDecision, on: bool, now_utc: int) -> None:
        state_str = "ON" if on else "OFF"
        if rule.dry_run or self._switch_callback is None:
            logger.info("[auto-schedule:dry-run] %s → %s:%d %s (%s)",
                        rule.name or rule.rule_id, rule.device_key,
                        int(rule.switch_id), state_str, dec.reason)
            dec.last_set_on = on
            dec.last_action_ts = now_utc
            dec.last_error = ""
            return
        try:
            self._switch_callback(rule.device_key, int(rule.switch_id), bool(on))
            logger.info("[auto-schedule:live] %s → %s:%d %s (%s)",
                        rule.name or rule.rule_id, rule.device_key,
                        int(rule.switch_id), state_str, dec.reason)
            dec.last_set_on = on
            dec.last_action_ts = now_utc
            dec.last_error = ""
        except Exception as exc:
            dec.last_error = str(exc)
            logger.warning("[auto-schedule:live] %s → %s:%d %s FAILED: %s",
                           rule.name or rule.rule_id, rule.device_key,
                           int(rule.switch_id), state_str, exc)

    def _commit(self, rid: str, dec: RuleDecision) -> None:
        with self._lock:
            self._decisions[rid] = dec

    @staticmethod
    def _fmt_local(ts: int) -> str:
        try:
            return datetime.fromtimestamp(ts, _TZ).strftime("%H:%M")
        except Exception:
            return "?"
