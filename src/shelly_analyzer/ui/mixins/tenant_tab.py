"""Tenant utility billing (Nebenkostenabrechnung) tab mixin."""
from __future__ import annotations

import tkinter as tk
from tkinter import ttk, messagebox, filedialog

from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure


class TenantMixin:
    """Adds the Tenant Billing tab."""

    def _build_tenant_tab(self) -> None:
        frm = self.tab_tenant

        # ── Top bar ──────────────────────────────────────────────────────
        top = ttk.Frame(frm)
        top.pack(fill="x", padx=14, pady=(12, 4))
        ttk.Label(top, text=self.t("tenant.title"), font=("", 14, "bold")).pack(side="left")
        ttk.Button(top, text=self.t("tenant.export_pdf"), command=self._tenant_export_pdf).pack(side="right", padx=5)

        # ── Content area (fills both directions) ─────────────────────────
        content = ttk.Frame(frm)
        content.pack(fill="both", expand=True)
        content.rowconfigure(0, weight=0)  # cards
        content.rowconfigure(1, weight=0)  # table
        content.rowconfigure(2, weight=1)  # charts
        content.columnconfigure(0, weight=1)

        # ── Summary cards ────────────────────────────────────────────────
        cards = ttk.Frame(content)
        cards.grid(row=0, column=0, sticky="ew", padx=14, pady=(4, 4))
        cards.columnconfigure((0, 1, 2), weight=1)

        self._tenant_total_kwh_var = tk.StringVar(value="–")
        self._tenant_total_cost_var = tk.StringVar(value="–")
        self._tenant_common_var = tk.StringVar(value="–")

        for col, (var, label_key, icon) in enumerate([
            (self._tenant_total_kwh_var, "tenant.total_kwh", "⚡"),
            (self._tenant_total_cost_var, "tenant.total_cost", "💰"),
            (self._tenant_common_var, "tenant.common_area", "🏢"),
        ]):
            card = ttk.LabelFrame(cards, text=f"{icon} {self.t(label_key)}")
            card.grid(row=0, column=col, sticky="nsew", padx=3, pady=3)
            ttk.Label(card, textvariable=var, font=("", 13, "bold")).pack(anchor="center", padx=8, pady=8)

        # ── Table ────────────────────────────────────────────────────────
        table_lf = ttk.LabelFrame(content, text=self.t("tenant.col.tenant"))
        table_lf.grid(row=1, column=0, sticky="ew", padx=14, pady=(4, 4))

        cols = ("tenant", "unit", "kwh", "cost_net", "vat", "cost_gross", "persons")
        self._tenant_tree = ttk.Treeview(table_lf, columns=cols, show="headings", height=6)
        for col_id, hdr_key, w in [
            ("tenant",     "tenant.col.tenant",     180),
            ("unit",       "tenant.col.unit",        100),
            ("kwh",        "tenant.col.kwh",         100),
            ("cost_net",   "tenant.col.cost_net",    100),
            ("vat",        "tenant.col.vat",          80),
            ("cost_gross", "tenant.col.cost_gross",  100),
            ("persons",    "tenant.col.persons",      80),
        ]:
            self._tenant_tree.heading(col_id, text=self.t(hdr_key))
            self._tenant_tree.column(col_id, width=w, anchor="center" if col_id != "tenant" else "w")

        sb = ttk.Scrollbar(table_lf, orient="vertical", command=self._tenant_tree.yview)
        self._tenant_tree.configure(yscrollcommand=sb.set)
        self._tenant_tree.pack(side="left", fill="both", expand=True)
        sb.pack(side="right", fill="y")

        # ── Charts (fill remaining space) ────────────────────────────────
        chart_lf = ttk.LabelFrame(content, text=self.t("tenant.total_cost"))
        chart_lf.grid(row=2, column=0, sticky="nsew", padx=14, pady=(4, 12))

        self._tenant_fig = Figure(figsize=(10, 3.5), dpi=96)
        self._tenant_bar_ax = self._tenant_fig.add_subplot(121)
        self._tenant_pie_ax = self._tenant_fig.add_subplot(122)
        self._tenant_canvas = FigureCanvasTkAgg(self._tenant_fig, master=chart_lf)
        self._tenant_canvas.get_tk_widget().pack(fill="both", expand=True)

        self.after(600, self._refresh_tenant_tab)

    def _refresh_tenant_tab(self) -> None:
        import datetime
        from shelly_analyzer.services.tenant import generate_tenant_bills, TenantDef

        tenant_cfg = getattr(self.cfg, "tenant", None)
        if not tenant_cfg or not getattr(tenant_cfg, "enabled", False):
            self._tenant_total_kwh_var.set(self.t("tenant.no_data"))
            self._tenant_total_cost_var.set("–")
            self._tenant_common_var.set("–")
            self._draw_empty_tenant_charts()
            return

        tenants = []
        for t in getattr(tenant_cfg, "tenants", []):
            tenants.append(TenantDef(
                tenant_id=t.tenant_id,
                name=t.name,
                device_keys=list(t.device_keys),
                unit=t.unit,
                persons=t.persons,
                move_in=getattr(t, "move_in", ""),
                move_out=getattr(t, "move_out", ""),
            ))

        if not tenants:
            self._tenant_total_kwh_var.set(self.t("tenant.no_data"))
            self._draw_empty_tenant_charts()
            return

        price = self._get_effective_unit_price()
        base_fee = getattr(self.cfg.pricing, "base_fee_eur_per_year", 127.51)
        vat = self.cfg.pricing.vat_rate()
        common_keys = list(getattr(tenant_cfg, "common_device_keys", []))

        # Use move_in as period_start for each tenant (earliest)
        earliest_move_in = None
        for t in tenants:
            if t.move_in:
                try:
                    mi = datetime.datetime.strptime(t.move_in, "%Y-%m-%d")
                    if earliest_move_in is None or mi < earliest_move_in:
                        earliest_move_in = mi
                except ValueError:
                    pass

        period_start = earliest_move_in.strftime("%Y-%m-%d") if earliest_move_in else None

        report = generate_tenant_bills(
            self.storage.db, tenants, self.cfg.devices,
            price_eur_per_kwh=price,
            base_fee_eur_per_year=base_fee,
            vat_rate=vat,
            common_device_keys=common_keys,
            period_start=period_start,
        )
        self._tenant_report = report

        self._tenant_total_kwh_var.set(f"{report.total_kwh:.1f} kWh")
        self._tenant_total_cost_var.set(f"{report.total_cost:.2f} €")
        self._tenant_common_var.set(f"{report.common_area_kwh:.1f} kWh")

        # Populate table
        self._tenant_tree.delete(*self._tenant_tree.get_children())
        for bill in report.bills:
            self._tenant_tree.insert("", "end", values=(
                bill.tenant.name,
                bill.tenant.unit,
                f"{bill.total_kwh:.1f}",
                f"{bill.subtotal_net:.2f}",
                f"{bill.vat_amount:.2f}",
                f"{bill.total_gross:.2f}",
                str(bill.tenant.persons),
            ))

        # Charts
        self._draw_tenant_charts(report)

    def _draw_tenant_charts(self, report) -> None:
        colors = ["#3498db", "#e74c3c", "#2ecc71", "#f39c12", "#9b59b6",
                 "#1abc9c", "#e67e22", "#34495e", "#16a085", "#c0392b"]

        ax1 = self._tenant_bar_ax
        ax1.clear()

        if report.bills:
            names = [b.tenant.name for b in report.bills]
            costs = [b.total_gross for b in report.bills]
            kwh_vals = [b.total_kwh for b in report.bills]
            c = colors[:len(names)]

            # Grouped bar chart: kWh + costs
            import numpy as np
            x = np.arange(len(names))
            w = 0.35

            bars1 = ax1.bar(x - w/2, kwh_vals, w, color=c, alpha=0.75, label="kWh",
                          edgecolor="white", linewidth=0.5)
            ax1_twin = ax1.twinx()
            bars2 = ax1_twin.bar(x + w/2, costs, w, color=c, alpha=0.45, label="€",
                               edgecolor="white", linewidth=0.5, hatch="//")

            ax1.set_xticks(x)
            ax1.set_xticklabels(names, rotation=30, ha="right", fontsize=8)
            ax1.set_ylabel("kWh", fontsize=9, color="#3498db")
            ax1_twin.set_ylabel("€", fontsize=9, color="#e74c3c")
            ax1.set_title(self.t("tenant.total_cost"), fontsize=10)
            ax1.grid(axis="y", alpha=0.3)
            ax1.set_axisbelow(True)

            # Value labels
            for bar, val in zip(bars1, kwh_vals):
                if val > 0:
                    ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(kwh_vals) * 0.02,
                            f"{val:.0f}", ha="center", fontsize=7, fontweight="bold")
            for bar, val in zip(bars2, costs):
                if val > 0:
                    ax1_twin.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(costs) * 0.02,
                                 f"{val:.0f}€", ha="center", fontsize=7)

        ax2 = self._tenant_pie_ax
        ax2.clear()

        if report.bills:
            kwh_vals = [b.total_kwh for b in report.bills if b.total_kwh > 0]
            pie_names = [b.tenant.name for b in report.bills if b.total_kwh > 0]
            c = colors[:len(pie_names)]

            if len(kwh_vals) > 1:
                wedges, texts, autotexts = ax2.pie(
                    kwh_vals, labels=pie_names, colors=c, autopct="%1.0f%%",
                    startangle=90, textprops={"fontsize": 8},
                    wedgeprops={"edgecolor": "white", "linewidth": 1.5},
                    pctdistance=0.75,
                )
                for t in autotexts:
                    t.set_fontweight("bold")
            elif len(kwh_vals) == 1:
                # Single tenant - show donut with label
                ax2.pie(kwh_vals, colors=c, startangle=90,
                       wedgeprops={"edgecolor": "white", "linewidth": 1.5, "width": 0.4})
                ax2.text(0, 0, f"{pie_names[0]}\n{kwh_vals[0]:.0f} kWh",
                        ha="center", va="center", fontsize=10, fontweight="bold")

            ax2.set_title(self.t("tenant.total_kwh"), fontsize=10)

        self._apply_plot_theme(self._tenant_fig, ax1, self._tenant_canvas)
        self._apply_plot_theme(self._tenant_fig, ax2)
        self._tenant_fig.tight_layout()
        self._tenant_canvas.draw_idle()

    def _draw_empty_tenant_charts(self) -> None:
        tc = self._get_theme_colors()
        for ax in (self._tenant_bar_ax, self._tenant_pie_ax):
            ax.clear()
            ax.text(0.5, 0.5, self.t("tenant.no_data"), ha="center", va="center",
                   fontsize=11, color=tc["muted"])
            ax.axis("off")
            self._apply_plot_theme(self._tenant_fig, ax, self._tenant_canvas)
        self._tenant_fig.tight_layout()
        self._tenant_canvas.draw_idle()

    def _tenant_export_pdf(self) -> None:
        if not hasattr(self, "_tenant_report") or not self._tenant_report.bills:
            messagebox.showinfo(self.t("msg.info"), self.t("tenant.no_data"))
            return

        path = filedialog.asksaveasfilename(
            defaultextension=".pdf",
            filetypes=[("PDF", "*.pdf")],
            title=self.t("tenant.export_pdf"),
        )
        if not path:
            return

        try:
            from shelly_analyzer.services.export import export_pdf_invoice

            for i, bill in enumerate(self._tenant_report.bills):
                bill_path = path if len(self._tenant_report.bills) == 1 else path.replace(".pdf", f"_{i+1}.pdf")
                lines = []
                for li in bill.line_items:
                    lines.append({
                        "description": li.description,
                        "quantity": f"{li.kwh:.2f} kWh" if li.kwh > 0 else "1",
                        "unit_price": f"{li.unit_price:.4f}" if li.unit_price > 0 else "",
                        "amount": f"{li.amount:.2f}",
                    })

                export_pdf_invoice(
                    out_path=bill_path,
                    invoice_no=f"NK-{self._tenant_report.period_start[:4]}-{i+1:03d}",
                    issuer=self.cfg.billing.issuer,
                    customer=type(self.cfg.billing.customer)(
                        name=bill.tenant.name,
                        address_lines=[bill.tenant.unit] if bill.tenant.unit else [],
                    ),
                    lines=lines,
                )
            messagebox.showinfo(self.t("msg.ok"), self.t("tenant.pdf_exported", path=path))
        except Exception as e:
            messagebox.showerror(self.t("msg.error"), str(e))
