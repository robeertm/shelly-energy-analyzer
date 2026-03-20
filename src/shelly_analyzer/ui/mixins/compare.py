"""Comparison tab mixin for Shelly Energy Analyzer.

Provides side-by-side comparison of two time periods (optionally two different
devices) with delta display and a matplotlib-based grouped bar chart.
"""
from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
import tkinter as tk
from tkinter import ttk
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

logger = logging.getLogger(__name__)


class CompareMixin:
    """Comparison tab: overlay two time periods (or two devices) with delta."""

    # ── Build ─────────────────────────────────────────────────────────────────

    def _build_compare_tab(self) -> None:
        """Build the comparison tab UI."""
        frm = self.tab_compare

        # ── Top bar ───────────────────────────────────────────────────────────
        top = ttk.Frame(frm)
        top.pack(fill="x", padx=14, pady=(12, 4))
        ttk.Label(top, text=self.t("compare.title"), font=("", 14, "bold")).pack(side="left")
        ttk.Button(top, text=self.t("compare.refresh"), command=self._refresh_compare).pack(side="right")

        # ── Period controls ───────────────────────────────────────────────────
        ctrl = ttk.Frame(frm)
        ctrl.pack(fill="x", padx=14, pady=(0, 4))

        dev_names = [d.name for d in (getattr(self.cfg, "devices", []) or [])]
        dev_keys = [d.key for d in (getattr(self.cfg, "devices", []) or [])]
        self._cmp_dev_names = dev_names
        self._cmp_dev_keys = dev_keys

        today = date.today()
        jan1_this = date(today.year, 1, 1)
        jan1_last = date(today.year - 1, 1, 1)
        dec31_last = date(today.year - 1, 12, 31)

        # Period A
        row_a = ttk.LabelFrame(ctrl, text=self.t("compare.period_a"))
        row_a.pack(fill="x", pady=(0, 4))

        ttk.Label(row_a, text=self.t("compare.device") + ":").pack(side="left", padx=(8, 4), pady=6)
        self._cmp_dev_a_var = tk.StringVar()
        dev_cb_a = ttk.Combobox(
            row_a, textvariable=self._cmp_dev_a_var, values=dev_names, width=22, state="readonly"
        )
        if dev_names:
            dev_cb_a.current(0)
        dev_cb_a.pack(side="left", padx=(0, 14), pady=6)

        ttk.Label(row_a, text=self.t("compare.from") + ":").pack(side="left", padx=(0, 4))
        self._cmp_from_a_var = tk.StringVar(value=jan1_this.strftime("%Y-%m-%d"))
        ttk.Entry(row_a, textvariable=self._cmp_from_a_var, width=12).pack(side="left", padx=(0, 8))

        ttk.Label(row_a, text=self.t("compare.to") + ":").pack(side="left", padx=(0, 4))
        self._cmp_to_a_var = tk.StringVar(value=(today - timedelta(days=1)).strftime("%Y-%m-%d"))
        ttk.Entry(row_a, textvariable=self._cmp_to_a_var, width=12).pack(side="left", padx=(0, 8))

        # Period B
        row_b = ttk.LabelFrame(ctrl, text=self.t("compare.period_b"))
        row_b.pack(fill="x", pady=(0, 4))

        ttk.Label(row_b, text=self.t("compare.device") + ":").pack(side="left", padx=(8, 4), pady=6)
        self._cmp_dev_b_var = tk.StringVar()
        dev_cb_b = ttk.Combobox(
            row_b, textvariable=self._cmp_dev_b_var, values=dev_names, width=22, state="readonly"
        )
        if dev_names:
            dev_cb_b.current(0)
        dev_cb_b.pack(side="left", padx=(0, 14), pady=6)

        ttk.Label(row_b, text=self.t("compare.from") + ":").pack(side="left", padx=(0, 4))
        self._cmp_from_b_var = tk.StringVar(value=jan1_last.strftime("%Y-%m-%d"))
        ttk.Entry(row_b, textvariable=self._cmp_from_b_var, width=12).pack(side="left", padx=(0, 8))

        ttk.Label(row_b, text=self.t("compare.to") + ":").pack(side="left", padx=(0, 4))
        self._cmp_to_b_var = tk.StringVar(value=dec31_last.strftime("%Y-%m-%d"))
        ttk.Entry(row_b, textvariable=self._cmp_to_b_var, width=12).pack(side="left", padx=(0, 8))

        # ── Options row ───────────────────────────────────────────────────────
        opts = ttk.Frame(frm)
        opts.pack(fill="x", padx=14, pady=(0, 4))

        ttk.Label(opts, text=self.t("compare.unit") + ":").pack(side="left", padx=(0, 4))
        self._cmp_unit_var = tk.StringVar(value="kWh")
        unit_cb = ttk.Combobox(
            opts, textvariable=self._cmp_unit_var, values=["kWh", "€"], width=7, state="readonly"
        )
        unit_cb.current(0)
        unit_cb.pack(side="left", padx=(0, 16))

        ttk.Label(opts, text=self.t("compare.granularity") + ":").pack(side="left", padx=(0, 4))
        self._cmp_gran_keys = ["total", "daily", "monthly"]
        gran_display = [
            self.t("compare.gran.total"),
            self.t("compare.gran.daily"),
            self.t("compare.gran.monthly"),
        ]
        self._cmp_gran_var = tk.StringVar(value=gran_display[1])
        self._cmp_gran_cb = ttk.Combobox(
            opts, textvariable=self._cmp_gran_var, values=gran_display, width=14, state="readonly"
        )
        self._cmp_gran_cb.current(1)
        self._cmp_gran_cb.pack(side="left", padx=(0, 16))

        # ── Delta summary strip ───────────────────────────────────────────────
        delta_frm = ttk.Frame(frm)
        delta_frm.pack(fill="x", padx=14, pady=(2, 6))
        self._cmp_delta_var = tk.StringVar(value="")
        ttk.Label(delta_frm, textvariable=self._cmp_delta_var, font=("", 11, "bold")).pack(side="left")

        # ── Plot area ─────────────────────────────────────────────────────────
        try:
            _init_bg = "#111111" if self._resolve_plot_theme() == "night" else "#FFFFFF"
        except Exception:
            _init_bg = "#FFFFFF"

        plot_frm = ttk.Frame(frm)
        plot_frm.pack(fill="both", expand=True, padx=14, pady=(0, 12))

        self._cmp_fig = Figure(figsize=(14, 5), dpi=100)
        self._cmp_fig.patch.set_facecolor(_init_bg)
        self._cmp_ax = self._cmp_fig.add_subplot(111)
        self._cmp_canvas = FigureCanvasTkAgg(self._cmp_fig, master=plot_frm)
        self._cmp_canvas.get_tk_widget().configure(bg=_init_bg)
        self._cmp_canvas.get_tk_widget().pack(fill="both", expand=True)

        # Initial render
        self.after(700, self._refresh_compare)

    # ── Data loading ──────────────────────────────────────────────────────────

    def _cmp_load_daily(
        self,
        device_key: str,
        from_date: date,
        to_date: date,
        use_eur: bool,
        price_kwh: float,
    ) -> Dict[str, float]:
        """Load daily kWh/€ totals for *device_key* between two dates.

        Returns ``{"%Y-%m-%d": value}`` for each day in [from_date, to_date].

        Strategy:
        1. Primary: ``hourly_energy`` table — always populated during sync, stores
           integer ``hour_ts`` (no datetime-conversion issues), and has a ``kwh``
           column guaranteed by COALESCE.
        2. Fallback: raw ``samples`` via ``read_device_df`` — kept for devices
           whose hourly table was never rebuilt (very old DBs).
        """
        try:
            import pandas as pd

            start_ts = int(datetime(from_date.year, from_date.month, from_date.day).timestamp())
            to_plus = to_date + timedelta(days=1)
            end_ts = int(datetime(to_plus.year, to_plus.month, to_plus.day).timestamp())

            def _aggregate(ts_series, kwh_series) -> Dict[str, float]:
                """Group (timestamp_seconds, kwh) pairs into daily totals."""
                local_dates = ts_series.apply(
                    lambda ts: datetime.fromtimestamp(int(ts)).date()
                )
                tmp = pd.DataFrame({"_date": local_dates, "_kwh": kwh_series.fillna(0.0)})
                daily = tmp.groupby("_date")["_kwh"].sum()
                out: Dict[str, float] = {}
                for d, kwh in daily.items():
                    if from_date <= d <= to_date:
                        val = float(kwh) * price_kwh if use_eur else float(kwh)
                        out[d.strftime("%Y-%m-%d")] = max(0.0, val)
                return out

            # ── 1. Hourly pre-aggregated table (primary) ──────────────────────
            try:
                hourly_df = self.storage.db.query_hourly(
                    device_key, start_ts=start_ts, end_ts=end_ts
                )
                if hourly_df is not None and not hourly_df.empty and "kwh" in hourly_df.columns:
                    result = _aggregate(hourly_df["hour_ts"], hourly_df["kwh"])
                    if result:
                        return result
            except Exception as e:
                logger.debug("_cmp_load_daily hourly path error for '%s': %s", device_key, e)

            # ── 2. Raw samples fallback ────────────────────────────────────────
            df = self.storage.read_device_df(device_key, start_ts=start_ts, end_ts=end_ts)
            if df is None or df.empty or "energy_kwh" not in df.columns:
                return {}

            # Normalize timestamp column: query_samples() returns datetime64
            # (may be tz-aware or tz-naive depending on pandas version).
            # Convert back to Unix integer seconds for consistent handling.
            ts_col = "timestamp" if "timestamp" in df.columns else "ts"
            df = df.copy()
            for col_name in ("timestamp", "ts"):
                if col_name not in df.columns:
                    continue
                col = df[col_name]
                if hasattr(col.dtype, "tz") or pd.api.types.is_datetime64_any_dtype(col):
                    df[col_name] = col.astype("int64") // 10 ** 9

            return _aggregate(df[ts_col], df["energy_kwh"])

        except Exception as e:
            logger.warning("_cmp_load_daily error for '%s': %s", device_key, e, exc_info=True)
            return {}

    # ── Aggregation helpers ───────────────────────────────────────────────────

    def _cmp_align_daily(
        self,
        daily_a: Dict[str, float],
        from_a: date,
        to_a: date,
        daily_b: Dict[str, float],
        from_b: date,
        to_b: date,
    ) -> Tuple[List[float], List[float], List[str]]:
        """Align daily data by relative day index (day 0, 1, 2 …)."""
        n_a = (to_a - from_a).days + 1
        n_b = (to_b - from_b).days + 1
        n = max(n_a, n_b)
        vals_a, vals_b, x_labels = [], [], []
        for i in range(n):
            d_a = from_a + timedelta(days=i)
            d_b = from_b + timedelta(days=i)
            va = daily_a.get(d_a.strftime("%Y-%m-%d"), 0.0) if i < n_a else 0.0
            vb = daily_b.get(d_b.strftime("%Y-%m-%d"), 0.0) if i < n_b else 0.0
            vals_a.append(va)
            vals_b.append(vb)
            x_labels.append(f"+{i}d")
        return vals_a, vals_b, x_labels

    def _cmp_align_monthly(
        self,
        daily_a: Dict[str, float],
        from_a: date,
        to_a: date,
        daily_b: Dict[str, float],
        from_b: date,
        to_b: date,
    ) -> Tuple[List[float], List[float], List[str]]:
        """Group daily data into calendar months, aligned by relative month index."""

        def _monthly(daily: Dict[str, float], from_d: date, to_d: date):
            m: Dict[Tuple[int, int], float] = defaultdict(float)
            d = from_d
            while d <= to_d:
                m[(d.year, d.month)] += daily.get(d.strftime("%Y-%m-%d"), 0.0)
                d += timedelta(days=1)
            return dict(m)

        m_a = _monthly(daily_a, from_a, to_a)
        m_b = _monthly(daily_b, from_b, to_b)
        keys_a = sorted(m_a)
        keys_b = sorted(m_b)
        n = max(len(keys_a), len(keys_b))
        vals_a, vals_b, x_labels = [], [], []
        for i in range(n):
            ka = keys_a[i] if i < len(keys_a) else None
            kb = keys_b[i] if i < len(keys_b) else None
            vals_a.append(m_a.get(ka, 0.0) if ka else 0.0)
            vals_b.append(m_b.get(kb, 0.0) if kb else 0.0)
            lbl = f"{ka[0]}-{ka[1]:02d}" if ka else (f"{kb[0]}-{kb[1]:02d}" if kb else "–")
            x_labels.append(lbl)
        return vals_a, vals_b, x_labels

    # ── Public refresh ────────────────────────────────────────────────────────

    def _refresh_compare(self) -> None:
        """Read controls, load data, and redraw the comparison chart."""
        if not hasattr(self, "_cmp_fig"):
            return
        try:
            dev_names = getattr(self, "_cmp_dev_names", [])
            dev_keys = getattr(self, "_cmp_dev_keys", [])
            if not dev_keys:
                self._cmp_show_message(self.t("plots.no_data"))
                return

            use_eur = self._cmp_unit_var.get() == "€"
            try:
                price_kwh = float(
                    getattr(getattr(self.cfg, "pricing", None), "price_per_kwh", 0.0) or 0.0
                )
            except Exception:
                price_kwh = 0.0

            unit_label = "€" if use_eur else "kWh"

            def _resolve_key(var: tk.StringVar) -> str:
                n = var.get()
                try:
                    return dev_keys[dev_names.index(n)]
                except (ValueError, IndexError):
                    return dev_keys[0] if dev_keys else ""

            key_a = _resolve_key(self._cmp_dev_a_var)
            key_b = _resolve_key(self._cmp_dev_b_var)

            def _parse(s: str) -> Optional[date]:
                for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%Y/%m/%d"):
                    try:
                        return datetime.strptime(s.strip(), fmt).date()
                    except Exception:
                        pass
                return None

            from_a = _parse(self._cmp_from_a_var.get())
            to_a = _parse(self._cmp_to_a_var.get())
            from_b = _parse(self._cmp_from_b_var.get())
            to_b = _parse(self._cmp_to_b_var.get())

            if not all([from_a, to_a, from_b, to_b]) or from_a > to_a or from_b > to_b:
                self._cmp_show_message(self.t("compare.err.invalid_dates"))
                return

            gran_idx = self._cmp_gran_cb.current()
            gran = self._cmp_gran_keys[gran_idx] if 0 <= gran_idx < len(self._cmp_gran_keys) else "daily"

            daily_a = self._cmp_load_daily(key_a, from_a, to_a, use_eur, price_kwh)
            daily_b = self._cmp_load_daily(key_b, from_b, to_b, use_eur, price_kwh)

            # If both periods returned no data, show a clear message rather than
            # rendering an empty chart with invisible zero-height bars.
            if not daily_a and not daily_b:
                self._cmp_show_message(self.t("plots.no_data"))
                self._cmp_delta_var.set("")
                return

            total_a = sum(daily_a.values())
            total_b = sum(daily_b.values())

            # Delta summary
            if total_b > 0:
                pct = (total_a - total_b) / total_b * 100.0
                sign = "+" if pct >= 0 else ""
                delta_str = (
                    f"A: {total_a:.2f} {unit_label}  |  "
                    f"B: {total_b:.2f} {unit_label}  |  "
                    f"Δ: {total_a - total_b:+.2f} {unit_label} ({sign}{pct:.1f}%)"
                )
            else:
                delta_str = (
                    f"A: {total_a:.2f} {unit_label}  |  "
                    f"B: {total_b:.2f} {unit_label}  |  "
                    f"Δ: {total_a - total_b:+.2f} {unit_label}"
                )
            self._cmp_delta_var.set(delta_str)

            if gran == "total":
                vals_a = [total_a]
                vals_b = [total_b]
                x_labels: List[str] = [""]
            elif gran == "monthly":
                vals_a, vals_b, x_labels = self._cmp_align_monthly(
                    daily_a, from_a, to_a, daily_b, from_b, to_b
                )
            else:
                vals_a, vals_b, x_labels = self._cmp_align_daily(
                    daily_a, from_a, to_a, daily_b, from_b, to_b
                )

            self._draw_compare_chart(
                vals_a, vals_b, x_labels, gran, unit_label,
                from_a, to_a, from_b, to_b,
                key_a, key_b, dev_names, dev_keys,
            )

        except Exception as e:
            logger.warning("_refresh_compare error: %s", e, exc_info=True)

    # ── Drawing ───────────────────────────────────────────────────────────────

    def _draw_compare_chart(
        self,
        vals_a: List[float],
        vals_b: List[float],
        x_labels: List[str],
        gran: str,
        unit_label: str,
        from_a: date,
        to_a: date,
        from_b: date,
        to_b: date,
        key_a: str,
        key_b: str,
        dev_names: List[str],
        dev_keys: List[str],
    ) -> None:
        """Render the side-by-side grouped bar chart."""
        try:
            fig = self._cmp_fig
            ax = self._cmp_ax
            canvas = self._cmp_canvas

            ax.clear()

            try:
                theme = self._resolve_plot_theme()
            except Exception:
                theme = "day"
            bg = "#111111" if theme == "night" else "#FFFFFF"
            fg = "#E6E6E6" if theme == "night" else "#000000"

            col_a = "#2196F3"   # blue for period A
            col_b = "#FF7043"   # orange for period B

            fig.patch.set_facecolor(bg)
            ax.set_facecolor(bg)

            def _dev_name(k: str) -> str:
                try:
                    return dev_names[dev_keys.index(k)]
                except Exception:
                    return k

            label_a = f"A: {from_a} – {to_a}"
            label_b = f"B: {from_b} – {to_b}"
            if key_a != key_b:
                label_a = f"A: {_dev_name(key_a)} ({from_a}–{to_a})"
                label_b = f"B: {_dev_name(key_b)} ({from_b}–{to_b})"

            if gran == "total":
                x = np.array([0.0, 0.6])
                bars = ax.bar(
                    x, [vals_a[0] if vals_a else 0.0, vals_b[0] if vals_b else 0.0],
                    width=0.4, color=[col_a, col_b],
                )
                ax.set_xticks(x)
                ax.set_xticklabels([label_a, label_b], color=fg, fontsize=9)
                for bar, val in zip(bars, [vals_a[0] if vals_a else 0.0, vals_b[0] if vals_b else 0.0]):
                    ax.text(
                        bar.get_x() + bar.get_width() / 2,
                        bar.get_height() + bar.get_height() * 0.01,
                        f"{val:.2f}",
                        ha="center", va="bottom", color=fg, fontsize=10, fontweight="bold",
                    )
            else:
                n = max(len(vals_a), len(vals_b))
                if n == 0:
                    ax.text(
                        0.5, 0.5, self.t("plots.no_data"),
                        ha="center", va="center", color=fg,
                        transform=ax.transAxes, fontsize=13,
                    )
                    ax.set_xticks([])
                    ax.set_yticks([])
                else:
                    x = np.arange(n)
                    w = 0.38
                    ax.bar(x - w / 2, vals_a, width=w, label=label_a, color=col_a, alpha=0.88)
                    ax.bar(x + w / 2, vals_b, width=w, label=label_b, color=col_b, alpha=0.88)

                    # Show at most ~20 x-axis labels to avoid crowding
                    step = max(1, n // 20)
                    tick_pos = list(range(0, n, step))
                    ax.set_xticks([x[i] for i in tick_pos])
                    ax.set_xticklabels(
                        [x_labels[i] for i in tick_pos if i < len(x_labels)],
                        color=fg, fontsize=8, rotation=45, ha="right",
                    )

                    leg = ax.legend(fontsize=9)
                    if leg:
                        leg.get_frame().set_facecolor(bg)
                        for txt in leg.get_texts():
                            txt.set_color(fg)

            ax.set_ylabel(unit_label, color=fg, fontsize=9)
            ax.tick_params(axis="both", colors=fg)
            for spine in ax.spines.values():
                spine.set_color(fg)

            if key_a == key_b:
                title = (
                    f"{_dev_name(key_a)}: "
                    f"{self.t('compare.period_a')} vs {self.t('compare.period_b')}"
                )
            else:
                title = f"{_dev_name(key_a)} vs {_dev_name(key_b)}"
            ax.set_title(title, color=fg, fontsize=10, pad=6)

            try:
                canvas.get_tk_widget().configure(bg=bg)
            except Exception:
                pass

            fig.tight_layout(pad=0.5)
            canvas.draw()

        except Exception as e:
            logger.warning("_draw_compare_chart error: %s", e, exc_info=True)

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _cmp_show_message(self, msg: str) -> None:
        """Display a centered message in the plot area."""
        try:
            theme = self._resolve_plot_theme()
        except Exception:
            theme = "day"
        bg = "#111111" if theme == "night" else "#FFFFFF"
        fg = "#E6E6E6" if theme == "night" else "#000000"
        try:
            ax = self._cmp_ax
            fig = self._cmp_fig
            canvas = self._cmp_canvas
            ax.clear()
            fig.patch.set_facecolor(bg)
            ax.set_facecolor(bg)
            ax.text(0.5, 0.5, msg, ha="center", va="center", color=fg,
                    transform=ax.transAxes, fontsize=13)
            ax.set_xticks([])
            ax.set_yticks([])
            canvas.get_tk_widget().configure(bg=bg)
            canvas.draw()
        except Exception:
            pass
        try:
            self._cmp_delta_var.set("")
        except Exception:
            pass
