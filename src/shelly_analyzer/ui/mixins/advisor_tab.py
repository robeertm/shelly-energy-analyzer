from __future__ import annotations
import logging
import threading
import tkinter as tk
from tkinter import ttk

_log = logging.getLogger(__name__)


class AdvisorMixin:
    """AI Energy Advisor tab."""

    def _build_advisor_tab(self) -> None:
        frm = self.tab_advisor
        for w in frm.winfo_children():
            w.destroy()

        canvas = tk.Canvas(frm, highlightthickness=0)
        scrollbar = ttk.Scrollbar(frm, orient="vertical", command=canvas.yview)
        self._advisor_inner = ttk.Frame(canvas)
        self._advisor_inner.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        _win_id = canvas.create_window((0, 0), window=self._advisor_inner, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        canvas.bind("<Configure>", lambda e: canvas.itemconfigure(_win_id, width=e.width))
        scrollbar.pack(side="right", fill="y")
        canvas.pack(side="left", fill="both", expand=True)

        inner = self._advisor_inner
        ttk.Label(inner, text=self.t("advisor.title"), font=("", 14, "bold")).pack(anchor="w", padx=12, pady=(12, 4))
        ttk.Label(inner, text=self.t("advisor.hint"), foreground="gray").pack(anchor="w", padx=12, pady=(0, 8))

        # Savings potential
        self._advisor_savings = tk.StringVar(value="\u2014")
        card = ttk.LabelFrame(inner, text=self.t("advisor.total_savings"))
        card.pack(fill="x", padx=12, pady=8)
        ttk.Label(card, textvariable=self._advisor_savings, font=("", 18, "bold"), foreground="#4caf50").pack(padx=12, pady=8)

        # LLM summary
        self._advisor_llm_frame = ttk.LabelFrame(inner, text=self.t("advisor.llm_summary"))
        self._advisor_llm_frame.pack(fill="x", padx=12, pady=4)
        self._advisor_llm_text = tk.StringVar(value="")
        ttk.Label(self._advisor_llm_frame, textvariable=self._advisor_llm_text, wraplength=700).pack(padx=12, pady=8)

        # Tips container
        self._advisor_tips_frame = ttk.Frame(inner)
        self._advisor_tips_frame.pack(fill="both", expand=True, padx=12, pady=8)

        self._advisor_refresh()

    def _advisor_refresh(self) -> None:
        def _worker():
            try:
                from shelly_analyzer.services.ai_advisor import get_advisor_tips
                result = get_advisor_tips(self.storage.db, self.cfg, self.storage)
                def _update():
                    self._advisor_savings.set(f"{result['total_savings_potential_eur']:.0f} \u20ac/Jahr")
                    self._advisor_llm_text.set(result.get("llm_summary", "") or self.t("advisor.no_llm"))
                    if not result.get("llm_summary"):
                        self._advisor_llm_frame.pack_forget()
                    else:
                        self._advisor_llm_frame.pack(fill="x", padx=12, pady=4, before=self._advisor_tips_frame)

                    for w in self._advisor_tips_frame.winfo_children():
                        w.destroy()

                    for tip in result.get("tips", []):
                        tip_card = ttk.LabelFrame(self._advisor_tips_frame, text=f"{tip['icon']} {tip['title']}")
                        tip_card.pack(fill="x", pady=4)
                        ttk.Label(tip_card, text=tip["description"], wraplength=650).pack(padx=12, pady=(4, 2), anchor="w")
                        if tip["potential_savings_eur"] > 0:
                            ttk.Label(tip_card, text=f"\U0001f4b0 {self.t('advisor.savings_potential')}: {tip['potential_savings_eur']:.0f} \u20ac/Jahr",
                                      foreground="#4caf50").pack(padx=12, pady=(0, 4), anchor="w")
                self.after(0, _update)
            except Exception as e:
                _log.error("Advisor refresh: %s", e)
        threading.Thread(target=_worker, daemon=True).start()
