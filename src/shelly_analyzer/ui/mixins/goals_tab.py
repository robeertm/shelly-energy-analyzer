from __future__ import annotations
import logging
import threading
import tkinter as tk
from tkinter import ttk

_log = logging.getLogger(__name__)


class GoalsMixin:
    """Gamification tab: goals, badges, streaks."""

    def _build_goals_tab(self) -> None:
        frm = self.tab_goals
        for w in frm.winfo_children():
            w.destroy()

        canvas = tk.Canvas(frm, highlightthickness=0)
        scrollbar = ttk.Scrollbar(frm, orient="vertical", command=canvas.yview)
        self._goals_inner = ttk.Frame(canvas)
        self._goals_inner.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.create_window((0, 0), window=self._goals_inner, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side="right", fill="y")
        canvas.pack(side="left", fill="both", expand=True)

        inner = self._goals_inner
        ttk.Label(inner, text=self.t("goals.title"), font=("", 14, "bold")).pack(anchor="w", padx=12, pady=(12, 4))

        # Streak display
        streak_frm = ttk.LabelFrame(inner, text=self.t("goals.streak"))
        streak_frm.pack(fill="x", padx=12, pady=8)
        self._goals_streak_text = tk.StringVar(value="\u2014")
        ttk.Label(streak_frm, textvariable=self._goals_streak_text, font=("", 16, "bold")).pack(padx=12, pady=8)

        # Weekly goal
        weekly_frm = ttk.LabelFrame(inner, text=self.t("goals.weekly"))
        weekly_frm.pack(fill="x", padx=12, pady=4)
        self._goals_weekly_text = tk.StringVar(value="\u2014")
        ttk.Label(weekly_frm, textvariable=self._goals_weekly_text, font=("", 12)).pack(padx=12, pady=4, anchor="w")
        self._goals_weekly_bar = ttk.Progressbar(weekly_frm, mode="determinate", maximum=100)
        self._goals_weekly_bar.pack(fill="x", padx=12, pady=(0, 8))

        # Monthly goal
        monthly_frm = ttk.LabelFrame(inner, text=self.t("goals.monthly"))
        monthly_frm.pack(fill="x", padx=12, pady=4)
        self._goals_monthly_text = tk.StringVar(value="\u2014")
        ttk.Label(monthly_frm, textvariable=self._goals_monthly_text, font=("", 12)).pack(padx=12, pady=4, anchor="w")
        self._goals_monthly_bar = ttk.Progressbar(monthly_frm, mode="determinate", maximum=100)
        self._goals_monthly_bar.pack(fill="x", padx=12, pady=(0, 8))

        # Badges
        badges_frm = ttk.LabelFrame(inner, text=self.t("goals.badges"))
        badges_frm.pack(fill="x", padx=12, pady=8)
        self._goals_badges_frame = ttk.Frame(badges_frm)
        self._goals_badges_frame.pack(fill="x", padx=8, pady=8)

        ttk.Button(inner, text=self.t("goals.refresh_btn"), command=self._goals_refresh).pack(padx=12, pady=8)
        self._goals_refresh()

    def _goals_refresh(self) -> None:
        def _worker():
            try:
                from shelly_analyzer.services.gamification import get_gamification_status
                data = get_gamification_status(self.storage.db, self.cfg)
                def _update():
                    # Streak
                    streak = data.get("streak", {})
                    days = streak.get("current_days", 0)
                    fire = "\U0001f525" * min(days, 5)
                    self._goals_streak_text.set(f"{fire} {days} {self.t('goals.days_streak')}")

                    # Weekly
                    wg = data.get("weekly_goal", {})
                    self._goals_weekly_text.set(
                        f"{wg.get('actual_kwh', 0):.1f} / {wg.get('target_kwh', 0):.1f} kWh"
                        f"  ({self.t('goals.remaining')}: {wg.get('remaining_kwh', 0):.1f} kWh)"
                    )
                    self._goals_weekly_bar["value"] = min(100, wg.get("progress_pct", 0))

                    # Monthly
                    mg = data.get("monthly_goal", {})
                    self._goals_monthly_text.set(
                        f"{mg.get('actual_kwh', 0):.1f} / {mg.get('target_kwh', 0):.1f} kWh"
                        f"  ({self.t('goals.remaining')}: {mg.get('remaining_kwh', 0):.1f} kWh)"
                    )
                    self._goals_monthly_bar["value"] = min(100, mg.get("progress_pct", 0))

                    # Badges
                    for w in self._goals_badges_frame.winfo_children():
                        w.destroy()
                    badges = data.get("badges", [])
                    cols_per_row = 5
                    for i, b in enumerate(badges):
                        r, c = divmod(i, cols_per_row)
                        opacity = "" if b["unlocked"] else " (\U0001f512)"
                        badge_frm = ttk.Frame(self._goals_badges_frame)
                        badge_frm.grid(row=r, column=c, padx=6, pady=4)
                        ttk.Label(badge_frm, text=b["icon"], font=("", 20)).pack()
                        ttk.Label(badge_frm, text=b["name"] + opacity, font=("", 8)).pack()
                        pb = ttk.Progressbar(badge_frm, length=60, mode="determinate", maximum=100)
                        pb["value"] = b.get("progress_pct", 0)
                        pb.pack(pady=2)

                self.after(0, _update)
            except Exception as e:
                _log.error("Goals refresh: %s", e)
        threading.Thread(target=_worker, daemon=True).start()
