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

        # Top bar
        top = ttk.Frame(frm)
        top.pack(fill="x", padx=10, pady=(10, 5))
        ttk.Label(top, text=self.t("tenant.title"), font=("", 14, "bold")).pack(side="left")
        ttk.Button(top, text=self.t("tenant.refresh"), command=self._refresh_tenant_tab).pack(side="right", padx=5)
        ttk.Button(top, text=self.t("tenant.export_pdf"), command=self._tenant_export_pdf).pack(side="right", padx=5)

        # Summary cards
        summary = ttk.Frame(frm)
        summary.pack(fill="x", padx=10, pady=5)

        self._tenant_total_kwh_var = tk.StringVar(value="–")
        self._tenant_total_cost_var = tk.StringVar(value="–")
        self._tenant_common_var = tk.StringVar(value="–")

        for i, (var, label_key, icon) in enumerate([
            (self._tenant_total_kwh_var, "tenant.total_kwh", "⚡"),
            (self._tenant_total_cost_var, "tenant.total_cost", "💰"),
            (self._tenant_common_var, "tenant.common_area", "🏢"),
        ]):
            card = ttk.LabelFrame(summary, text=f"{icon} {self.t(label_key)}")
            card.grid(row=0, column=i, padx=5, pady=5, sticky="nsew")
            summary.columnconfigure(i, weight=1)
            ttk.Label(card, textvariable=var, font=("", 13, "bold")).pack(padx=10, pady=8)

        # Table
        table_frame = ttk.Frame(frm)
        table_frame.pack(fill="both", expand=True, padx=10, pady=5)

        cols = ("tenant", "unit", "kwh", "cost_net", "vat", "cost_gross", "persons")
        self._tenant_tree = ttk.Treeview(table_frame, columns=cols, show="headings", height=8)
        for col, hdr_key, w in [
            ("tenant",     "tenant.col.tenant",     180),
            ("unit",       "tenant.col.unit",        100),
            ("kwh",        "tenant.col.kwh",         100),
            ("cost_net",   "tenant.col.cost_net",    100),
            ("vat",        "tenant.col.vat",          80),
            ("cost_gross", "tenant.col.cost_gross",  100),
            ("persons",    "tenant.col.persons",      80),
        ]:
            self._tenant_tree.heading(col, text=self.t(hdr_key))
            self._tenant_tree.column(col, width=w, anchor="center" if col != "tenant" else "w")

        sb = ttk.Scrollbar(table_frame, orient="vertical", command=self._tenant_tree.yview)
        self._tenant_tree.configure(yscrollcommand=sb.set)
        self._tenant_tree.pack(side="left", fill="both", expand=True)
        sb.pack(side="right", fill="y")

        # Chart: cost comparison
        chart_frame = ttk.Frame(frm)
        chart_frame.pack(fill="both", expand=True, padx=10, pady=(5, 10))

        self._tenant_fig = Figure(figsize=(10, 3.5), dpi=96)
        self._tenant_bar_ax = self._tenant_fig.add_subplot(121)
        self._tenant_pie_ax = self._tenant_fig.add_subplot(122)
        self._tenant_canvas = FigureCanvasTkAgg(self._tenant_fig, master=chart_frame)
        self._tenant_canvas.get_tk_widget().pack(fill="both", expand=True)

        self.after(600, self._refresh_tenant_tab)

    def _refresh_tenant_tab(self) -> None:
        from shelly_analyzer.services.tenant import generate_tenant_bills, TenantDef

        tenant_cfg = getattr(self.cfg, "tenant", None)
        if not tenant_cfg or not getattr(tenant_cfg, "enabled", False):
            self._tenant_total_kwh_var.set(self.t("tenant.no_data"))
            self._tenant_total_cost_var.set("–")
            self._tenant_common_var.set("–")
            return

        # Convert config tenants to service TenantDefs
        tenants = []
        for t in getattr(tenant_cfg, "tenants", []):
            tenants.append(TenantDef(
                tenant_id=t.tenant_id,
                name=t.name,
                device_keys=list(t.device_keys),
                unit=t.unit,
                persons=t.persons,
                move_in=t.move_in,
                move_out=t.move_out,
            ))

        if not tenants:
            self._tenant_total_kwh_var.set(self.t("tenant.no_data"))
            return

        price = self.cfg.pricing.unit_price_gross()
        base_fee = getattr(self.cfg.pricing, "base_fee_eur_per_year", 127.51)
        vat = self.cfg.pricing.vat_rate()
        common_keys = list(getattr(tenant_cfg, "common_device_keys", []))

        report = generate_tenant_bills(
            self.storage.db, tenants, self.cfg.devices,
            price_eur_per_kwh=price,
            base_fee_eur_per_year=base_fee,
            vat_rate=vat,
            common_device_keys=common_keys,
        )
        self._tenant_report = report

        # Update summary
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
        self._tenant_bar_ax.clear()
        self._tenant_pie_ax.clear()

        if report.bills:
            names = [b.tenant.name[:15] for b in report.bills]
            costs = [b.total_gross for b in report.bills]
            colors = ["#3498db", "#e74c3c", "#2ecc71", "#f39c12", "#9b59b6",
                     "#1abc9c", "#e67e22", "#34495e"][:len(names)]

            # Bar chart
            bars = self._tenant_bar_ax.bar(names, costs, color=colors, alpha=0.8)
            self._tenant_bar_ax.set_ylabel("€")
            self._tenant_bar_ax.set_title(self.t("tenant.total_cost"), fontsize=10)
            self._tenant_bar_ax.tick_params(axis="x", rotation=30)
            for bar, cost in zip(bars, costs):
                self._tenant_bar_ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5,
                                        f"{cost:.0f}€", ha="center", fontsize=8)

            # Pie chart (kWh share)
            kwh_vals = [b.total_kwh for b in report.bills if b.total_kwh > 0]
            pie_names = [b.tenant.name[:12] for b in report.bills if b.total_kwh > 0]
            if kwh_vals:
                self._tenant_pie_ax.pie(kwh_vals, labels=pie_names, colors=colors[:len(kwh_vals)],
                                       autopct="%1.0f%%", startangle=90, textprops={"fontsize": 8})
                self._tenant_pie_ax.set_title(self.t("tenant.total_kwh"), fontsize=10)

        self._tenant_fig.tight_layout()
        self._tenant_canvas.draw_idle()

    def _tenant_export_pdf(self) -> None:
        if not hasattr(self, "_tenant_report") or not self._tenant_report.bills:
            messagebox.showinfo("Info", self.t("tenant.no_data"))
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
            messagebox.showinfo("✅", f"PDF(s) exportiert: {path}")
        except Exception as e:
            messagebox.showerror("Fehler", str(e))
