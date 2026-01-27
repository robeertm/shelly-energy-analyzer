from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, Iterable, Optional, Sequence, Tuple, List

import pandas as pd

from shelly_analyzer.i18n import t, normalize_lang, format_date_local, format_datetime_local, format_hour_local, format_number_local

from reportlab.lib.pagesizes import A4
from reportlab.lib.utils import ImageReader
from reportlab.lib.units import cm
from reportlab.pdfgen import canvas


def _fmt_money(x: float, lang: str = "de") -> str:
    lang = normalize_lang(lang)
    return format_number_local(lang, x, decimals=2)


def _fmt_kwh(x: float, lang: str = "de") -> str:
    lang = normalize_lang(lang)
    return format_number_local(lang, x, decimals=3)


def _fmt_int(x: float, lang: str = "de") -> str:
    lang = normalize_lang(lang)
    return format_number_local(lang, x, decimals=0)



@dataclass(frozen=True)
class ReportTotals:
    name: str
    kwh_total: float
    avg_power_w: float
    max_power_w: float
    cost_eur: float = 0.0


def export_to_excel(sheets: Dict[str, pd.DataFrame], out_path: Path) -> Path:
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        for name, df in sheets.items():
            safe = name[:31]
            df.to_excel(writer, sheet_name=safe, index=False)
    return out_path


def export_dataframe_csv(df: pd.DataFrame, out_path: Path) -> Path:
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    return out_path


def export_pdf_summary(
    title: str,
    period_label: str,
    totals: list[ReportTotals],
    out_path: Path,
    note: Optional[str] = None,
    plot_pages: Optional[Sequence[Tuple[str, Path]]] = None,
    lang: str = "de",
) -> Path:
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    c = canvas.Canvas(str(out_path), pagesize=A4)
    w, h = A4

    y = h - 2.0 * cm
    c.setFont("Helvetica-Bold", 16)
    c.drawString(2.0 * cm, y, title)

    y -= 1.0 * cm
    c.setFont("Helvetica", 11)
    c.drawString(2.0 * cm, y, f"{t(lang, 'pdf.period')}: {period_label}")

    y -= 1.2 * cm
    c.setFont("Helvetica-Bold", 12)
    c.drawString(2.0 * cm, y, t(lang, 'pdf.summary'))

    y -= 0.8 * cm
    c.setFont("Helvetica", 10)
    c.drawString(2.0 * cm, y, t(lang, 'pdf.col.name'))
    c.drawString(8.8 * cm, y, "kWh")
    c.drawString(11.2 * cm, y, t(lang, 'pdf.col.cost'))
    c.drawString(13.8 * cm, y, t(lang, 'pdf.col.avg_w'))
    c.drawString(16.2 * cm, y, t(lang, 'pdf.col.max_w'))

    y -= 0.4 * cm
    c.line(2.0 * cm, y, w - 2.0 * cm, y)

    y -= 0.6 * cm
    for t in totals:
        c.drawString(2.0 * cm, y, t.name)
        c.drawRightString(11.0 * cm, y, _fmt_kwh(t.kwh_total, lang))
        c.drawRightString(13.5 * cm, y, _fmt_money(t.cost_eur, lang))
        c.drawRightString(16.0 * cm, y, _fmt_int(t.avg_power_w, lang))
        c.drawRightString(18.5 * cm, y, _fmt_int(t.max_power_w, lang))
        y -= 0.6 * cm
        if y < 3.0 * cm:
            c.showPage()
            y = h - 2.0 * cm

    if note:
        y -= 0.4 * cm
        c.setFont("Helvetica-Oblique", 9)
        c.drawString(2.0 * cm, y, note[:250])

    # Add plot pages (each on its own page)
    if plot_pages:
        for title2, img_path in plot_pages:
            c.showPage()
            w, h = A4
            c.setFont("Helvetica-Bold", 12)
            c.drawString(2.0 * cm, h - 2.0 * cm, title2)

            # Fit image into page box
            box_x = 2.0 * cm
            box_y = 2.0 * cm
            box_w = w - 4.0 * cm
            box_h = h - 4.5 * cm
            try:
                img = ImageReader(str(img_path))
                iw, ih = img.getSize()
                if iw <= 0 or ih <= 0:
                    raise ValueError("invalid image")
                scale = min(box_w / iw, box_h / ih)
                draw_w = iw * scale
                draw_h = ih * scale
                x = box_x + (box_w - draw_w) / 2
                y = box_y + (box_h - draw_h) / 2
                c.drawImage(img, x, y, width=draw_w, height=draw_h, preserveAspectRatio=True, mask='auto')
            except Exception:
                c.setFont("Helvetica", 10)
                c.drawString(2.0 * cm, h - 3.0 * cm, t(lang, "pdf.plot_load_error", path=str(img_path)))

    # Do not call showPage() here; it would append an extra blank page.
    c.save()
    return out_path


@dataclass(frozen=True)
class InvoiceLine:
    description: str
    quantity: float
    unit: str
    unit_price_net: float


def export_pdf_invoice(
    *,
    out_path: Path,
    invoice_no: str,
    issue_date: date,
    due_date: date,
    issuer: Dict[str, object],
    customer: Dict[str, object],
    vat_rate_percent: float,
    vat_enabled: bool,
    currency: str = "EUR",
    lines: Sequence[InvoiceLine],
    footer_note: Optional[str] = None,
    lang: str = "de",
) -> Path:
    """Create a simple but professional A4 invoice PDF.

    All monetary values are treated as NET; VAT will be added if enabled.
    """
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    vat_rate = max(0.0, float(vat_rate_percent)) / 100.0 if vat_enabled else 0.0

    c = canvas.Canvas(str(out_path), pagesize=A4)
    w, h = A4

    # Header
    y = h - 2.0 * cm
    c.setFont("Helvetica-Bold", 16)
    c.drawString(2.0 * cm, y, t(lang, 'pdf.invoice'))
    c.setFont("Helvetica", 10)
    c.drawRightString(w - 2.0 * cm, y, f"{t(lang, 'pdf.invoice_no')}: {invoice_no}")

    y -= 0.9 * cm
    c.drawRightString(w - 2.0 * cm, y, f"{t(lang, 'pdf.date')}: {format_date_local(lang, issue_date)}")
    y -= 0.5 * cm
    c.drawRightString(w - 2.0 * cm, y, f"{t(lang, 'pdf.due')}: {format_date_local(lang, due_date)}")

    # Issuer block
    y -= 1.1 * cm
    c.setFont("Helvetica-Bold", 10)
    c.drawString(2.0 * cm, y, str(issuer.get("name", "")))
    c.setFont("Helvetica", 10)
    for ln in (issuer.get("address_lines") or []):
        y -= 0.45 * cm
        c.drawString(2.0 * cm, y, str(ln))
    if issuer.get("vat_id"):
        y -= 0.6 * cm
        c.drawString(2.0 * cm, y, f"{t(lang, 'pdf.vat_id')}: {issuer.get('vat_id')}")
    if issuer.get("email"):
        y -= 0.45 * cm
        c.drawString(2.0 * cm, y, f"{t(lang, 'pdf.email')}: {issuer.get('email')}")
    if issuer.get("phone"):
        y -= 0.45 * cm
        c.drawString(2.0 * cm, y, f"{t(lang, 'pdf.phone')}: {issuer.get('phone')}")

    # Customer block
    y_c = h - 4.2 * cm
    c.setFont("Helvetica-Bold", 10)
    c.drawString(11.0 * cm, y_c, t(lang, 'pdf.bill_to'))
    y_c -= 0.55 * cm
    c.drawString(11.0 * cm, y_c, str(customer.get("name", "")))
    c.setFont("Helvetica", 10)
    for ln in (customer.get("address_lines") or []):
        y_c -= 0.45 * cm
        c.drawString(11.0 * cm, y_c, str(ln))
    if customer.get("vat_id"):
        y_c -= 0.55 * cm
        c.drawString(11.0 * cm, y_c, f"{t(lang, 'pdf.vat_id')}: {customer.get('vat_id')}")

    # Table header
    y = h - 9.0 * cm
    c.setFont("Helvetica-Bold", 10)
    c.drawString(2.0 * cm, y, t(lang, 'pdf.col.description'))
    c.drawRightString(12.5 * cm, y, t(lang, 'pdf.col.quantity'))
    c.drawRightString(15.5 * cm, y, f"{t(lang, 'pdf.col.unit_price')} ({currency})")
    c.drawRightString(w - 2.0 * cm, y, f"{t(lang, 'pdf.col.amount')} ({currency})")
    y -= 0.25 * cm
    c.line(2.0 * cm, y, w - 2.0 * cm, y)
    y -= 0.6 * cm
    c.setFont("Helvetica", 10)

    net_total = 0.0
    for ln in lines:
        amount = float(ln.quantity) * float(ln.unit_price_net)
        net_total += amount
        c.drawString(2.0 * cm, y, ln.description)
        q = float(ln.quantity) if ln.quantity is not None else 0.0
        unit_l = str(ln.unit or "").strip().lower()
        if unit_l in ("day", "days", "tag", "tage", "jour", "jours", "día", "días", "dia", "dias", "giorno", "giorni", "dzień", "dni", "den", "dny", "день", "дни"):
            qtxt = _fmt_int(q, lang)
        else:
            qtxt = _fmt_kwh(q, lang)
        c.drawRightString(12.5 * cm, y, f"{qtxt} {ln.unit}")
        c.drawRightString(15.5 * cm, y, _fmt_money(ln.unit_price_net, lang))
        c.drawRightString(w - 2.0 * cm, y, _fmt_money(amount, lang))
        y -= 0.6 * cm
        if y < 5.0 * cm:
            c.showPage()
            y = h - 2.0 * cm

    vat_amount = net_total * vat_rate
    gross_total = net_total + vat_amount

    # Totals block
    y -= 0.2 * cm
    c.line(11.0 * cm, y, w - 2.0 * cm, y)
    y -= 0.65 * cm
    c.setFont("Helvetica-Bold", 10)
    c.drawRightString(15.5 * cm, y, t(lang, 'pdf.subtotal'))
    c.drawRightString(w - 2.0 * cm, y, _fmt_money(net_total, lang))
    y -= 0.55 * cm
    c.setFont("Helvetica", 10)
    if vat_enabled and vat_rate > 0:
        c.drawRightString(15.5 * cm, y, f"{t(lang, 'pdf.vat')} ({vat_rate_percent:.1f}%)")
        c.drawRightString(w - 2.0 * cm, y, _fmt_money(vat_amount, lang))
        y -= 0.55 * cm
        c.setFont("Helvetica-Bold", 10)
        c.drawRightString(15.5 * cm, y, t(lang, 'pdf.total'))
        c.drawRightString(w - 2.0 * cm, y, _fmt_money(gross_total, lang))
    else:
        c.setFont("Helvetica-Bold", 10)
        c.drawRightString(15.5 * cm, y, t(lang, 'pdf.total'))
        c.drawRightString(w - 2.0 * cm, y, _fmt_money(net_total, lang))

    # Payment details
    y -= 1.2 * cm
    c.setFont("Helvetica", 9)
    if issuer.get("iban"):
        c.drawString(2.0 * cm, y, t(lang, "pdf.pay_bank", iban=str(issuer.get("iban")), bic=str(issuer.get("bic",""))))
        y -= 0.45 * cm
    c.drawString(2.0 * cm, y, t(lang, "pdf.pay_by", due=format_date_local(lang, due_date)))

    if footer_note:
        y -= 0.8 * cm
        c.setFont("Helvetica-Oblique", 9)
        c.drawString(2.0 * cm, y, footer_note[:300])

    c.save()
    return out_path


def export_figure_png(fig, out_path: Path, dpi: int = 150) -> Path:
    """Save a matplotlib Figure as PNG.

    This is kept in services/export.py so the UI can reuse it and we have one place
    that ensures the output directory exists.
    """
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=int(dpi), bbox_inches="tight")
    return out_path

# ---------------- Etappe 6: Energy Report (Variante 1) ----------------

@dataclass(frozen=True)
class TopHour:
    hour_start: pd.Timestamp
    kwh: float
    cost_eur: float


@dataclass(frozen=True)
class DeviceReport:
    device_key: str
    device_name: str
    kwh_total: float
    cost_eur: float
    peak_w: float
    peak_ts: Optional[pd.Timestamp]
    v_min: Optional[float]
    v_max: Optional[float]
    top_hours: Sequence[TopHour]


@dataclass(frozen=True)
class OverallReport:
    kwh_total: float
    cost_eur: float
    peak_w: float
    peak_ts: Optional[pd.Timestamp]
    top_hours: Sequence[TopHour]
    per_device: Sequence[DeviceReport]


def _voltage_columns(df: pd.DataFrame) -> List[str]:
    """Try to locate voltage columns robustly across Shelly exports."""
    cols = []
    for c in df.columns:
        cl = str(c).lower()
        if "volt" in cl or "voltage" in cl:
            cols.append(c)
            continue
        # Some legacy exports: a_u/b_u/c_u or plain *_u
        if cl in {"a_u", "b_u", "c_u", "u_a", "u_b", "u_c"}:
            cols.append(c)
            continue
        if cl.endswith("_u") and pd.api.types.is_numeric_dtype(df[c]):
            cols.append(c)
            continue
    # Prefer phase + avg columns first if present (stable ordering)
    preferred = []
    for name in ("a_voltage","b_voltage","c_voltage","avg_voltage","a_u","b_u","c_u"):
        if name in df.columns:
            preferred.append(name)
    out = preferred + [c for c in cols if c not in preferred]
    # De-dup while preserving order
    seen=set()
    res=[]
    for c in out:
        if c in seen: 
            continue
        seen.add(c)
        res.append(c)
    return res


def _hourly_top_hours(df: pd.DataFrame, *, unit_price_gross: float, top_n: int = 10) -> List[TopHour]:
    if df is None or df.empty or "timestamp" not in df.columns:
        return []
    if "energy_kwh" not in df.columns:
        return []
    x = df[["timestamp","energy_kwh"]].copy()
    x = x.dropna(subset=["timestamp"])
    if x.empty:
        return []
    x = x.set_index("timestamp").sort_index()
    h = x["energy_kwh"].resample("h").sum().fillna(0.0)
    if h.empty:
        return []
    h = h.sort_values(ascending=False).head(top_n)
    out: List[TopHour] = []
    for ts, kwh in h.items():
        k = float(kwh or 0.0)
        out.append(TopHour(hour_start=pd.Timestamp(ts), kwh=k, cost_eur=k*unit_price_gross))
    return out


def _compute_device_report(
    *,
    device_key: str,
    device_name: str,
    df: pd.DataFrame,
    unit_price_gross: float,
) -> DeviceReport:
    kwh = float(df["energy_kwh"].sum()) if (df is not None and not df.empty and "energy_kwh" in df.columns) else 0.0
    cost = kwh * unit_price_gross

    peak_w = 0.0
    peak_ts: Optional[pd.Timestamp] = None
    if df is not None and not df.empty and "total_power" in df.columns and "timestamp" in df.columns:
        try:
            idx = df["total_power"].astype(float).idxmax()
            peak_w = float(df.loc[idx, "total_power"])
            peak_ts = pd.Timestamp(df.loc[idx, "timestamp"])
        except Exception:
            peak_w = float(df["total_power"].max() or 0.0)
            peak_ts = None

    v_min: Optional[float] = None
    v_max: Optional[float] = None
    vcols = _voltage_columns(df) if (df is not None and not df.empty) else []
    if vcols:
        try:
            vv = df[vcols].apply(pd.to_numeric, errors="coerce")
            v_min = float(vv.min(axis=1, skipna=True).min(skipna=True))
            v_max = float(vv.max(axis=1, skipna=True).max(skipna=True))
        except Exception:
            v_min = None
            v_max = None

    top_hours = _hourly_top_hours(df, unit_price_gross=unit_price_gross, top_n=10)
    return DeviceReport(
        device_key=device_key,
        device_name=device_name,
        kwh_total=kwh,
        cost_eur=cost,
        peak_w=peak_w,
        peak_ts=peak_ts,
        v_min=v_min,
        v_max=v_max,
        top_hours=top_hours,
    )


def _compute_overall_peak_and_top_hours(
    devices: Sequence[Tuple[str, str, pd.DataFrame]],
    *,
    unit_price_gross: float,
) -> Tuple[float, Optional[pd.Timestamp], List[TopHour]]:
    # Peak: align to 1-minute bins and sum mean power
    frames = []
    for _k, _n, df in devices:
        if df is None or df.empty or "timestamp" not in df.columns:
            continue
        if "total_power" not in df.columns:
            continue
        x = df[["timestamp","total_power"]].copy()
        x = x.dropna(subset=["timestamp"])
        if x.empty:
            continue
        x = x.set_index("timestamp").sort_index()
        s = pd.to_numeric(x["total_power"], errors="coerce").resample("1min").mean().fillna(0.0)
        frames.append(s.rename(_k))
    if frames:
        merged = pd.concat(frames, axis=1).fillna(0.0)
        total = merged.sum(axis=1)
        peak_w = float(total.max() or 0.0)
        peak_ts = pd.Timestamp(total.idxmax()) if len(total) else None
    else:
        peak_w, peak_ts = 0.0, None

    # Top hours (kWh): hourly sums per device then sum across
    h_frames = []
    for _k, _n, df in devices:
        if df is None or df.empty or "timestamp" not in df.columns or "energy_kwh" not in df.columns:
            continue
        x = df[["timestamp","energy_kwh"]].copy().dropna(subset=["timestamp"])
        if x.empty:
            continue
        x = x.set_index("timestamp").sort_index()
        h = pd.to_numeric(x["energy_kwh"], errors="coerce").resample("h").sum().fillna(0.0)
        h_frames.append(h.rename(_k))
    top_hours: List[TopHour] = []
    if h_frames:
        hm = pd.concat(h_frames, axis=1).fillna(0.0).sum(axis=1)
        hm = hm.sort_values(ascending=False).head(10)
        for ts, kwh in hm.items():
            k = float(kwh or 0.0)
            top_hours.append(TopHour(hour_start=pd.Timestamp(ts), kwh=k, cost_eur=k*unit_price_gross))
    return peak_w, peak_ts, top_hours


def export_pdf_energy_report_variant1(
    *,
    out_path: Path,
    title: str,
    period_label: str,
    pricing_note: str,
    unit_price_gross: float,
    devices: Sequence[Tuple[str, str, pd.DataFrame]],
    lang: str = "de",
) -> Path:
    """Create an A4 PDF report (Variante 1): overview + one page per device."""
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    per_device: List[DeviceReport] = []
    total_kwh = 0.0
    total_cost = 0.0

    for k, name, df in devices:
        rep = _compute_device_report(device_key=k, device_name=name, df=df, unit_price_gross=unit_price_gross)
        per_device.append(rep)
        total_kwh += rep.kwh_total
        total_cost += rep.cost_eur

    overall_peak_w, overall_peak_ts, overall_top_hours = _compute_overall_peak_and_top_hours(
        devices, unit_price_gross=unit_price_gross
    )

    c = canvas.Canvas(str(out_path), pagesize=A4)
    w, h = A4

    def _header(page_title: str) -> float:
        y = h - 2.0 * cm
        c.setFont("Helvetica-Bold", 16)
        c.drawString(2.0 * cm, y, page_title)
        c.setFont("Helvetica", 10)
        c.drawRightString(w - 2.0 * cm, y, f"{t(lang, 'pdf.created')}: {format_date_local(lang, date.today())}")
        y -= 0.9 * cm
        c.setFont("Helvetica", 11)
        c.drawString(2.0 * cm, y, f"{t(lang, 'pdf.period')}: {period_label}")
        y -= 0.6 * cm
        c.setFont("Helvetica", 10)
        c.drawString(2.0 * cm, y, pricing_note)
        y -= 0.5 * cm
        c.line(2.0 * cm, y, w - 2.0 * cm, y)
        return y - 0.8 * cm

    # -------- Overview page --------
    y = _header(title)
    c.setFont("Helvetica-Bold", 12)
    c.drawString(2.0 * cm, y, t(lang, 'pdf.report.overall'))
    y -= 0.7 * cm

    c.setFont("Helvetica", 11)
    c.drawString(2.0 * cm, y, f"{t(lang, 'pdf.report.total_energy')}: {_fmt_kwh(total_kwh, lang)} kWh")
    c.drawString(8.5 * cm, y, f"{t(lang, 'pdf.report.total_cost')}: {_fmt_money(total_cost, lang)} €")
    y -= 0.6 * cm
    c.drawString(2.0 * cm, y, f"{t(lang, 'pdf.report.peak_total')}: {_fmt_int(overall_peak_w, lang)} W" + (f" ({overall_peak_ts})" if overall_peak_ts is not None else ""))
    y -= 0.8 * cm

    # Per-device table header
    c.setFont("Helvetica-Bold", 10)
    c.drawString(2.0 * cm, y, t(lang, 'pdf.col.device'))
    c.drawRightString(11.0 * cm, y, "kWh")
    c.drawRightString(13.6 * cm, y, "€")
    c.drawRightString(16.2 * cm, y, t(lang, 'pdf.col.peak_w'))
    c.drawRightString(19.0 * cm, y, t(lang, 'pdf.col.v_minmax'))
    y -= 0.4 * cm
    c.line(2.0 * cm, y, w - 2.0 * cm, y)
    y -= 0.6 * cm
    c.setFont("Helvetica", 10)

    for rep in per_device:
        c.drawString(2.0 * cm, y, rep.device_name)
        c.drawRightString(11.0 * cm, y, _fmt_kwh(rep.kwh_total, lang))
        c.drawRightString(13.6 * cm, y, _fmt_money(rep.cost_eur, lang))
        c.drawRightString(16.2 * cm, y, _fmt_int(rep.peak_w, lang))
        vtxt = "—"
        if rep.v_min is not None and rep.v_max is not None:
            vtxt = f"{format_number_local(lang, rep.v_min, 1)}/{format_number_local(lang, rep.v_max, 1)}"
        c.drawRightString(19.0 * cm, y, vtxt)
        y -= 0.55 * cm
        if y < 4.0 * cm:
            c.showPage()
            y = _header(title)
            c.setFont("Helvetica-Bold", 12)
            c.drawString(2.0 * cm, y, t(lang, 'pdf.report.overall_continue'))
            y -= 0.8 * cm
            c.setFont("Helvetica", 10)

    # Top hours overall
    y -= 0.2 * cm
    c.setFont("Helvetica-Bold", 11)
    c.drawString(2.0 * cm, y, t(lang, 'pdf.report.top_hours_total'))
    y -= 0.6 * cm
    c.setFont("Helvetica-Bold", 10)
    c.drawString(2.0 * cm, y, t(lang, 'pdf.col.hour'))
    c.drawRightString(11.0 * cm, y, "kWh")
    c.drawRightString(13.6 * cm, y, "€")
    y -= 0.4 * cm
    c.line(2.0 * cm, y, 14.0 * cm, y)
    y -= 0.6 * cm
    c.setFont("Helvetica", 10)
    for th in overall_top_hours:
        c.drawString(2.0 * cm, y, format_hour_local(lang, th.hour_start))
        c.drawRightString(11.0 * cm, y, _fmt_kwh(th.kwh, lang))
        c.drawRightString(13.6 * cm, y, _fmt_money(th.cost_eur, lang))
        y -= 0.55 * cm
        if y < 3.5 * cm:
            c.showPage()
            y = _header(title)
            c.setFont("Helvetica-Bold", 12)
            c.drawString(2.0 * cm, y, t(lang, 'pdf.report.top_hours_continue'))
            y -= 0.8 * cm
            c.setFont("Helvetica", 10)

    # -------- Device pages --------
    for rep in per_device:
        c.showPage()
        y = _header(f"{title} – {rep.device_name}")
        c.setFont("Helvetica-Bold", 12)
        c.drawString(2.0 * cm, y, t(lang, 'pdf.report.device_overview'))
        y -= 0.7 * cm
        c.setFont("Helvetica", 11)
        c.drawString(2.0 * cm, y, f"{t(lang, 'pdf.report.energy')}: {_fmt_kwh(rep.kwh_total, lang)} kWh")
        c.drawString(8.5 * cm, y, f"{t(lang, 'pdf.report.cost')}: {_fmt_money(rep.cost_eur, lang)} €")
        y -= 0.6 * cm
        peak_txt = f"{rep.peak_w:,.0f} W"
        if rep.peak_ts is not None:
            try:
                peak_txt += " (" + format_datetime_local(lang, pd.Timestamp(rep.peak_ts)) + ")"
            except Exception:
                peak_txt += f" ({rep.peak_ts})"
        c.drawString(2.0 * cm, y, f"{t(lang, 'pdf.report.peak')}: {peak_txt}")
        y -= 0.6 * cm
        vtxt = "—"
        if rep.v_min is not None and rep.v_max is not None:
            vtxt = f"{rep.v_min:,.1f} V / {rep.v_max:,.1f} V"
        c.drawString(2.0 * cm, y, f"{t(lang, 'pdf.report.v_minmax')}: {vtxt}")
        y -= 0.9 * cm

        c.setFont("Helvetica-Bold", 11)
        c.drawString(2.0 * cm, y, t(lang, 'pdf.report.top_hours'))
        y -= 0.6 * cm
        c.setFont("Helvetica-Bold", 10)
        c.drawString(2.0 * cm, y, t(lang, 'pdf.col.hour'))
        c.drawRightString(11.0 * cm, y, "kWh")
        c.drawRightString(13.6 * cm, y, "€")
        y -= 0.4 * cm
        c.line(2.0 * cm, y, 14.0 * cm, y)
        y -= 0.6 * cm
        c.setFont("Helvetica", 10)
        if rep.top_hours:
            for th in rep.top_hours:
                c.drawString(2.0 * cm, y, format_hour_local(lang, th.hour_start))
                c.drawRightString(11.0 * cm, y, _fmt_kwh(th.kwh, lang))
                c.drawRightString(13.6 * cm, y, _fmt_money(th.cost_eur, lang))
                y -= 0.55 * cm
                if y < 3.5 * cm:
                    break
        else:
            c.drawString(2.0 * cm, y, "—")
    c.save()
    return out_path
