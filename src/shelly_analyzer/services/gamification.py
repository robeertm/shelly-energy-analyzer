from __future__ import annotations
import logging
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

_log = logging.getLogger(__name__)


@dataclass
class Badge:
    badge_id: str
    name: str
    description: str
    icon: str
    unlocked: bool = False
    unlocked_at: int = 0
    value: float = 0.0
    progress_pct: float = 0.0


@dataclass
class GoalStatus:
    goal_id: str
    period: str  # "weekly" | "monthly"
    target_kwh: float
    actual_kwh: float
    progress_pct: float
    achieved: bool
    period_start: int
    period_end: int
    remaining_kwh: float = 0.0


@dataclass
class StreakInfo:
    current_streak_days: int = 0
    best_streak_days: int = 0
    streak_start: int = 0


# Badge definitions
BADGE_DEFS = [
    {"id": "saver_10", "name": "Energy saver", "desc": "10% less than last week", "icon": "\U0001f331", "threshold": 10},
    {"id": "saver_20", "name": "Savings champion", "desc": "20% less than last week", "icon": "\U0001f3c6", "threshold": 20},
    {"id": "low_standby", "name": "Standby killer", "desc": "Standby below 50 W", "icon": "\U0001f50c", "threshold": 50},
    {"id": "green_hour", "name": "Green hour", "desc": "Shifted consumption to low-CO\u2082 hours", "icon": "\U0001f30d", "threshold": 0},
    {"id": "solar_champ", "name": "Solar champion", "desc": ">80% self-consumption in one day", "icon": "\u2600\ufe0f", "threshold": 80},
    {"id": "streak_7", "name": "7-day streak", "desc": "7 days below daily target", "icon": "\U0001f525", "threshold": 7},
    {"id": "streak_30", "name": "30-day streak", "desc": "30 days below daily target", "icon": "\U0001f48e", "threshold": 30},
    {"id": "night_saver", "name": "Night saver", "desc": "Night consumption below 1 kWh", "icon": "\U0001f319", "threshold": 1},
    {"id": "peak_avoider", "name": "Peak avoider", "desc": "No peak load >3 kW per day", "icon": "\U0001f4c9", "threshold": 3000},
    {"id": "consistent", "name": "Consistently frugal", "desc": "4 weeks stable below average", "icon": "\u2b50", "threshold": 4},
]


class GoalsEngine:
    """Manage consumption goals, badges, and streaks."""

    def check_weekly_goal(self, db, device_keys: List[str], target_kwh: float = 0) -> GoalStatus:
        """Check progress toward weekly consumption goal."""
        now = int(time.time())
        # Current week: Monday 00:00 to next Monday 00:00
        import datetime
        today = datetime.date.today()
        monday = today - datetime.timedelta(days=today.weekday())
        week_start = int(datetime.datetime.combine(monday, datetime.time.min).timestamp())
        week_end = week_start + 7 * 86400

        actual_kwh = self._sum_kwh(db, device_keys, week_start, min(now, week_end))

        # Auto-calculate target from last 4 weeks average * 0.9
        if target_kwh <= 0:
            prev_kwh = []
            for w in range(1, 5):
                ws = week_start - w * 7 * 86400
                we = ws + 7 * 86400
                wk = self._sum_kwh(db, device_keys, ws, we)
                if wk > 0:
                    prev_kwh.append(wk)
            target_kwh = (sum(prev_kwh) / len(prev_kwh) * 0.9) if prev_kwh else 100.0

        progress = (actual_kwh / target_kwh * 100) if target_kwh > 0 else 0

        return GoalStatus(
            goal_id=f"weekly_{monday.isoformat()}",
            period="weekly",
            target_kwh=round(target_kwh, 1),
            actual_kwh=round(actual_kwh, 1),
            progress_pct=round(min(progress, 200), 1),
            achieved=actual_kwh <= target_kwh,
            period_start=week_start,
            period_end=week_end,
            remaining_kwh=round(max(0, target_kwh - actual_kwh), 1),
        )

    def check_monthly_goal(self, db, device_keys: List[str], target_kwh: float = 0) -> GoalStatus:
        """Check progress toward monthly consumption goal."""
        import datetime
        now = int(time.time())
        today = datetime.date.today()
        month_start_dt = today.replace(day=1)
        month_start = int(datetime.datetime.combine(month_start_dt, datetime.time.min).timestamp())

        # Next month start
        if today.month == 12:
            next_month = datetime.date(today.year + 1, 1, 1)
        else:
            next_month = datetime.date(today.year, today.month + 1, 1)
        month_end = int(datetime.datetime.combine(next_month, datetime.time.min).timestamp())

        actual_kwh = self._sum_kwh(db, device_keys, month_start, min(now, month_end))

        if target_kwh <= 0:
            # Use previous month
            if today.month == 1:
                prev_start = datetime.date(today.year - 1, 12, 1)
            else:
                prev_start = datetime.date(today.year, today.month - 1, 1)
            prev_ts = int(datetime.datetime.combine(prev_start, datetime.time.min).timestamp())
            prev_kwh = self._sum_kwh(db, device_keys, prev_ts, month_start)
            target_kwh = prev_kwh * 0.9 if prev_kwh > 0 else 300.0

        progress = (actual_kwh / target_kwh * 100) if target_kwh > 0 else 0

        return GoalStatus(
            goal_id=f"monthly_{month_start_dt.isoformat()}",
            period="monthly",
            target_kwh=round(target_kwh, 1),
            actual_kwh=round(actual_kwh, 1),
            progress_pct=round(min(progress, 200), 1),
            achieved=actual_kwh <= target_kwh,
            period_start=month_start,
            period_end=month_end,
            remaining_kwh=round(max(0, target_kwh - actual_kwh), 1),
        )

    def check_badges(self, db, device_keys: List[str], cfg=None) -> List[Badge]:
        """Check all badge conditions and return status."""
        badges = []
        now = int(time.time())

        for bdef in BADGE_DEFS:
            badge = Badge(
                badge_id=bdef["id"],
                name=bdef["name"],
                description=bdef["desc"],
                icon=bdef["icon"],
            )

            try:
                if bdef["id"] == "saver_10":
                    badge = self._check_saving_badge(db, device_keys, badge, 10)
                elif bdef["id"] == "saver_20":
                    badge = self._check_saving_badge(db, device_keys, badge, 20)
                elif bdef["id"] == "streak_7":
                    streak = self.get_streak(db, device_keys)
                    badge.progress_pct = min(100, streak.current_streak_days / 7 * 100)
                    badge.unlocked = streak.current_streak_days >= 7
                elif bdef["id"] == "streak_30":
                    streak = self.get_streak(db, device_keys)
                    badge.progress_pct = min(100, streak.current_streak_days / 30 * 100)
                    badge.unlocked = streak.current_streak_days >= 30
                elif bdef["id"] == "night_saver":
                    badge = self._check_night_badge(db, device_keys, badge)
                elif bdef["id"] == "peak_avoider":
                    badge = self._check_peak_badge(db, device_keys, badge)
            except Exception as e:
                _log.debug("Badge %s check: %s", bdef["id"], e)

            badges.append(badge)

        return badges

    def get_streak(self, db, device_keys: List[str]) -> StreakInfo:
        """Calculate current streak of days under average daily consumption."""
        import datetime
        now = int(time.time())

        # Get last 60 days of daily totals
        start = now - 60 * 86400
        daily_kwh: Dict[str, float] = {}

        for key in device_keys:
            try:
                df = db.query_hourly(key, start, now)
                if df is None or df.empty:
                    continue
                for _, row in df.iterrows():
                    h_ts = int(row.get("hour_ts", 0))
                    day_key = datetime.date.fromtimestamp(h_ts).isoformat()
                    daily_kwh[day_key] = daily_kwh.get(day_key, 0) + float(row.get("kwh", 0) or 0)
            except Exception:
                pass

        if not daily_kwh:
            return StreakInfo()

        avg = sum(daily_kwh.values()) / len(daily_kwh)

        # Count streak from today backwards
        streak = 0
        best = 0
        today = datetime.date.today()

        for i in range(60):
            day = (today - datetime.timedelta(days=i)).isoformat()
            kwh = daily_kwh.get(day, None)
            if kwh is not None and kwh <= avg:
                streak += 1
                best = max(best, streak)
            else:
                if i == 0:
                    continue  # Today might not be complete
                break

        return StreakInfo(
            current_streak_days=streak,
            best_streak_days=best,
            streak_start=int((today - datetime.timedelta(days=streak)).strftime("%s")) if streak > 0 else 0,
        )

    def _sum_kwh(self, db, device_keys: List[str], start_ts: int, end_ts: int) -> float:
        total = 0.0
        for key in device_keys:
            try:
                df = db.query_hourly(key, start_ts, end_ts)
                if df is not None and not df.empty:
                    total += float(df["kwh"].sum())
            except Exception:
                pass
        return total

    def _check_saving_badge(self, db, device_keys, badge, pct_threshold) -> Badge:
        import datetime
        now = int(time.time())
        today = datetime.date.today()
        this_week_start = int(datetime.datetime.combine(
            today - datetime.timedelta(days=today.weekday()), datetime.time.min
        ).timestamp())
        last_week_start = this_week_start - 7 * 86400

        this_kwh = self._sum_kwh(db, device_keys, this_week_start, now)
        last_kwh = self._sum_kwh(db, device_keys, last_week_start, this_week_start)

        if last_kwh > 0:
            reduction = ((last_kwh - this_kwh) / last_kwh) * 100
            badge.progress_pct = min(100, max(0, reduction / pct_threshold * 100))
            badge.unlocked = reduction >= pct_threshold
            badge.value = round(reduction, 1)

        return badge

    def _check_night_badge(self, db, device_keys, badge) -> Badge:
        import datetime
        now = int(time.time())
        today = datetime.date.today()
        yesterday = today - datetime.timedelta(days=1)
        night_start = int(datetime.datetime.combine(yesterday, datetime.time(22, 0)).timestamp())
        night_end = int(datetime.datetime.combine(today, datetime.time(6, 0)).timestamp())

        night_kwh = self._sum_kwh(db, device_keys, night_start, night_end)
        badge.value = round(night_kwh, 2)
        badge.progress_pct = min(100, max(0, (1.0 - night_kwh) / 1.0 * 100)) if night_kwh < 1 else 0
        badge.unlocked = night_kwh < 1.0
        return badge

    def _check_peak_badge(self, db, device_keys, badge) -> Badge:
        # Check if yesterday had no peak above 3kW
        import datetime
        today = datetime.date.today()
        yesterday = today - datetime.timedelta(days=1)
        day_start = int(datetime.datetime.combine(yesterday, datetime.time.min).timestamp())
        day_end = int(datetime.datetime.combine(today, datetime.time.min).timestamp())

        max_power = 0
        for key in device_keys:
            try:
                df = db.query_hourly(key, day_start, day_end)
                if df is not None and not df.empty:
                    mp = float(df.get("max_power_w", 0).max()) if "max_power_w" in df.columns else 0
                    max_power = max(max_power, mp)
            except Exception:
                pass

        badge.value = round(max_power, 0)
        badge.unlocked = max_power < 3000
        badge.progress_pct = min(100, max(0, (3000 - max_power) / 3000 * 100))
        return badge


def get_gamification_status(db, cfg) -> dict:
    """Main entry point for gamification data."""
    import datetime as _dt

    engine = GoalsEngine()
    device_keys = [d.key for d in cfg.devices if getattr(d, 'kind', 'em') == 'em']

    weekly = engine.check_weekly_goal(db, device_keys, cfg.gamification.weekly_goal_kwh)
    monthly = engine.check_monthly_goal(db, device_keys, cfg.gamification.monthly_goal_kwh)
    badges = engine.check_badges(db, device_keys, cfg)
    streak = engine.get_streak(db, device_keys)

    # ── Daily history (last 30 days) ──────────────────────────────────
    now = int(time.time())
    today = _dt.date.today()
    daily_history = []
    daily_kwh_map = {}
    start_30d = now - 30 * 86400
    for key in device_keys:
        try:
            df = db.query_hourly(key, start_30d, now)
            if df is not None and not df.empty:
                for _, row in df.iterrows():
                    h_ts = int(row.get("hour_ts", 0))
                    day_key = _dt.date.fromtimestamp(h_ts).isoformat()
                    daily_kwh_map[day_key] = daily_kwh_map.get(day_key, 0) + float(row.get("kwh", 0) or 0)
        except Exception:
            pass

    avg_daily = sum(daily_kwh_map.values()) / len(daily_kwh_map) if daily_kwh_map else 0
    daily_target = avg_daily * 0.9 if avg_daily > 0 else 10.0
    best_day = None
    worst_day = None
    for i in range(30):
        d = (today - _dt.timedelta(days=29 - i)).isoformat()
        kwh = round(daily_kwh_map.get(d, 0), 2)
        under = kwh <= daily_target and kwh > 0
        daily_history.append({"date": d, "kwh": kwh, "target": round(daily_target, 2), "under": under})
        if kwh > 0:
            if best_day is None or kwh < best_day["kwh"]:
                best_day = {"date": d, "kwh": kwh}
            if worst_day is None or kwh > worst_day["kwh"]:
                worst_day = {"date": d, "kwh": kwh}

    # ── Weekly history (last 8 weeks) ─────────────────────────────────
    weekly_history = []
    monday = today - _dt.timedelta(days=today.weekday())
    for w in range(7, -1, -1):
        ws = monday - _dt.timedelta(weeks=w)
        we = ws + _dt.timedelta(days=7)
        ws_ts = int(_dt.datetime.combine(ws, _dt.time.min).timestamp())
        we_ts = int(_dt.datetime.combine(we, _dt.time.min).timestamp())
        wk_kwh = engine._sum_kwh(db, device_keys, ws_ts, we_ts)
        weekly_history.append({
            "week_start": ws.isoformat(),
            "kwh": round(wk_kwh, 1),
            "target": round(weekly.target_kwh, 1),
        })

    # ── Level / XP system ─────────────────────────────────────────────
    xp = 0
    xp += streak.current_streak_days * 10  # 10 XP per streak day
    xp += sum(50 for b in badges if b.unlocked)  # 50 XP per badge
    if weekly.achieved:
        xp += 100
    if monthly.achieved:
        xp += 200
    # Days under target
    days_under = sum(1 for dh in daily_history if dh["under"])
    xp += days_under * 5
    level = 1
    xp_for_next = 100
    remaining_xp = xp
    while remaining_xp >= xp_for_next:
        remaining_xp -= xp_for_next
        level += 1
        xp_for_next = int(xp_for_next * 1.3)
    level_progress_pct = round(remaining_xp / xp_for_next * 100, 1) if xp_for_next > 0 else 0

    # ── Savings estimate (EUR) ────────────────────────────────────────
    price_kwh = 0.30  # default
    try:
        price_kwh = float(getattr(cfg.pricing, "electricity_price_eur_per_kwh", 0.30) or 0.30)
    except Exception:
        pass
    weekly_saved_kwh = max(0, weekly.target_kwh - weekly.actual_kwh) if weekly.achieved else 0
    monthly_saved_kwh = max(0, monthly.target_kwh - monthly.actual_kwh) if monthly.achieved else 0

    # ── Day ranking (best 10 and worst 5) ─────────────────────────────
    ranked_days = sorted(
        [dh for dh in daily_history if dh["kwh"] > 0],
        key=lambda x: x["kwh"],
    )
    best_days = ranked_days[:10]
    worst_days = ranked_days[-5:][::-1] if len(ranked_days) >= 5 else []

    return {
        "weekly_goal": {
            "target_kwh": weekly.target_kwh,
            "actual_kwh": weekly.actual_kwh,
            "progress_pct": weekly.progress_pct,
            "achieved": weekly.achieved,
            "remaining_kwh": weekly.remaining_kwh,
        },
        "monthly_goal": {
            "target_kwh": monthly.target_kwh,
            "actual_kwh": monthly.actual_kwh,
            "progress_pct": monthly.progress_pct,
            "achieved": monthly.achieved,
            "remaining_kwh": monthly.remaining_kwh,
        },
        "badges": [
            {
                "badge_id": b.badge_id,
                "name": b.name,
                "description": b.description,
                "icon": b.icon,
                "unlocked": b.unlocked,
                "progress_pct": b.progress_pct,
                "value": b.value,
            }
            for b in badges
        ],
        "streak": {
            "current_days": streak.current_streak_days,
            "best_days": streak.best_streak_days,
        },
        "unlocked_count": sum(1 for b in badges if b.unlocked),
        "total_badges": len(badges),
        "daily_history": daily_history,
        "weekly_history": weekly_history,
        "level": level,
        "xp": xp,
        "xp_for_next": xp_for_next,
        "xp_in_level": remaining_xp,
        "level_progress_pct": level_progress_pct,
        "avg_daily_kwh": round(avg_daily, 2),
        "daily_target_kwh": round(daily_target, 2),
        "days_under_target": days_under,
        "days_total": len([dh for dh in daily_history if dh["kwh"] > 0]),
        "best_day": best_day,
        "worst_day": worst_day,
        "best_days": best_days,
        "worst_days": worst_days,
        "savings_eur": {
            "weekly": round(weekly_saved_kwh * price_kwh, 2),
            "monthly": round(monthly_saved_kwh * price_kwh, 2),
            "price_kwh": price_kwh,
        },
    }
