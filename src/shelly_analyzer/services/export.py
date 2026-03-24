from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta, datetime
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


def _fmt_qty(x: float, lang: str = "de") -> str:
    """Format quantity column (kWh) for invoice tables."""
    return _fmt_kwh(x, lang)


def _fmt_int(x: float, lang: str = "de") -> str:
    lang = normalize_lang(lang)
    return format_number_local(lang, x, decimals=0)




def _wrap_text(c: canvas.Canvas, text: str, max_width: float, max_lines: int = 2) -> List[str]:
    """Wrap text for canvas drawing based on current font.

    Returns up to `max_lines` lines; last line is truncated with ellipsis if needed.
    """
    text = (text or "").strip()
    if not text:
        return [""]
    words = text.split()
    lines: List[str] = []
    cur: List[str] = []
    for w in words:
        trial = (" ".join(cur + [w])).strip()
        if c.stringWidth(trial) <= max_width or not cur:
            cur.append(w)
        else:
            lines.append(" ".join(cur))
            cur = [w]
            if len(lines) >= max_lines:
                break
    if len(lines) < max_lines and cur:
        lines.append(" ".join(cur))

    # Truncate last line if still too long
    if lines:
        last = lines[-1]
        if c.stringWidth(last) > max_width:
            ell = "…"
            while last and c.stringWidth(last + ell) > max_width:
                last = last[:-1]
            lines[-1] = (last + ell) if last else ell
    return lines[:max_lines]


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
    for row in totals:
        c.drawString(2.0 * cm, y, row.name)
        c.drawRightString(11.0 * cm, y, _fmt_kwh(row.kwh_total, lang))
        c.drawRightString(13.5 * cm, y, _fmt_money(row.cost_eur, lang))
        c.drawRightString(16.0 * cm, y, _fmt_int(row.avg_power_w, lang))
        c.drawRightString(18.5 * cm, y, _fmt_int(row.max_power_w, lang))
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
    period_label: Optional[str] = None,
    device_label: Optional[str] = None,
    footer_note: Optional[str] = None,
    lang: str = "de",
    logo_path: Optional[str] = None,
) -> Path:
    """Create a simple but professional A4 invoice PDF.

    All monetary values are treated as NET; VAT will be added if enabled.
    """
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    vat_rate = max(0.0, float(vat_rate_percent)) / 100.0 if vat_enabled else 0.0

    c = canvas.Canvas(str(out_path), pagesize=A4)
    w, h = A4

    # Logo (top right corner, if provided)
    try:
        if logo_path and str(logo_path).strip():
            from pathlib import Path as _P
            lp = _P(str(logo_path).strip())
            if lp.exists() and lp.suffix.lower() in (".png", ".jpg", ".jpeg", ".gif", ".bmp"):
                from reportlab.lib.utils import ImageReader
                img = ImageReader(str(lp))
                iw, ih = img.getSize()
                # Scale to max 3cm height, keeping aspect ratio
                max_h = 3.0 * cm
                max_w = 5.0 * cm
                scale = min(max_w / iw, max_h / ih, 1.0)
                dw = iw * scale
                dh = ih * scale
                c.drawImage(str(lp), w - 2.0 * cm - dw, h - 1.5 * cm - dh, width=dw, height=dh, preserveAspectRatio=True, mask="auto")
    except Exception:
        pass  # Logo is optional; don't fail the invoice

    # Header / title
    y = h - 2.0 * cm
    c.setFont("Helvetica-Bold", 18)
    c.drawString(2.0 * cm, y, t(lang, "pdf.invoice"))
    # Right header info block
    c.setFont("Helvetica", 10)
    c.drawRightString(w - 2.0 * cm, y, f"{t(lang, 'pdf.invoice_no')}: {invoice_no}")
    y -= 0.55 * cm
    c.drawRightString(w - 2.0 * cm, y, f"{t(lang, 'pdf.date')}: {format_date_local(lang, issue_date)}")
    y -= 0.45 * cm
    c.drawRightString(w - 2.0 * cm, y, f"{t(lang, 'pdf.due')}: {format_date_local(lang, due_date)}")

    # Horizontal rule
    y -= 0.55 * cm
    c.setLineWidth(0.6)
    c.line(2.0 * cm, y, w - 2.0 * cm, y)

    # Address blocks (issuer left, customer right)
    y -= 0.9 * cm
    box_h = 4.1 * cm
    left_x = 2.0 * cm
    right_x = w / 2.0 + 0.3 * cm
    box_w = w / 2.0 - 2.3 * cm

    # Issuer (left)
    c.setLineWidth(0.4)
    c.rect(left_x, y - box_h, box_w, box_h, stroke=1, fill=0)
    c.setFont("Helvetica-Bold", 9)
    c.drawString(left_x + 0.25 * cm, y - 0.55 * cm, t(lang, "pdf.block.issuer"))
    c.setFont("Helvetica", 10)
    yy = y - 1.1 * cm
    if issuer.get("name"):
        c.drawString(left_x + 0.25 * cm, yy, str(issuer.get("name")))
        yy -= 0.45 * cm
    for ln in (issuer.get("address_lines") or []):
        c.drawString(left_x + 0.25 * cm, yy, str(ln))
        yy -= 0.45 * cm
    if issuer.get("email"):
        c.drawString(left_x + 0.25 * cm, yy, f"{t(lang,'pdf.email')}: {issuer.get('email')}")
        yy -= 0.45 * cm
    if issuer.get("vat_id"):
        c.drawString(left_x + 0.25 * cm, yy, f"{t(lang,'pdf.vat_id')}: {issuer.get('vat_id')}")
        yy -= 0.45 * cm

    # Customer (right)
    c.rect(right_x, y - box_h, box_w, box_h, stroke=1, fill=0)
    c.setFont("Helvetica-Bold", 9)
    c.drawString(right_x + 0.25 * cm, y - 0.55 * cm, t(lang, "pdf.block.customer"))
    c.setFont("Helvetica", 10)
    yy = y - 1.1 * cm
    if customer.get("name"):
        c.drawString(right_x + 0.25 * cm, yy, str(customer.get("name")))
        yy -= 0.45 * cm
    for ln in (customer.get("address_lines") or []):
        c.drawString(right_x + 0.25 * cm, yy, str(ln))
        yy -= 0.45 * cm

    # Device / period line
    y = y - box_h - 0.8 * cm
    c.setFont("Helvetica-Bold", 11)
    if device_label:
        c.drawString(2.0 * cm, y, str(device_label))
        y -= 0.55 * cm
    c.setFont("Helvetica", 10)
    if period_label:
        c.drawString(2.0 * cm, y, str(period_label))
        y -= 0.5 * cm

    # Table header
    y -= 0.2 * cm
    c.setLineWidth(0.6)
    c.line(2.0 * cm, y, w - 2.0 * cm, y)
    y -= 0.5 * cm

    col_desc_x = 2.0 * cm
    col_qty_x = 11.2 * cm
    col_unit_x = 13.0 * cm
    col_price_x = 14.5 * cm
    col_amt_x = w - 2.0 * cm

    c.setFont("Helvetica-Bold", 10)
    c.drawString(col_desc_x, y, t(lang, "pdf.col.description"))
    c.drawRightString(col_qty_x, y, t(lang, "pdf.col.quantity"))
    c.drawString(col_unit_x, y, t(lang, "pdf.col.unit"))
    c.drawRightString(col_price_x, y, t(lang, "pdf.col.unit_price"))
    c.drawRightString(col_amt_x, y, t(lang, "pdf.col.amount"))
    y -= 0.35 * cm
    c.setLineWidth(0.4)
    c.line(2.0 * cm, y, w - 2.0 * cm, y)

    # Lines
    net_total = 0.0
    c.setFont("Helvetica", 10)
    y -= 0.55 * cm
    for ln in lines:
        # Wrap long descriptions gently
        desc = str(getattr(ln, "description", ""))
        qty = float(getattr(ln, "quantity", 0.0) or 0.0)
        unit = str(getattr(ln, "unit", "") or "")
        unit_price = float(getattr(ln, "unit_price_net", 0.0) or 0.0)
        amount = qty * unit_price
        net_total += amount

        # Description wrapping (max 2 lines)
        maxw = (col_qty_x - col_desc_x - 0.3 * cm)
        parts = _wrap_text(c, desc, maxw, max_lines=2)

        c.drawString(col_desc_x, y, parts[0])
        if len(parts) > 1:
            c.drawString(col_desc_x, y - 0.45 * cm, parts[1])

        c.drawRightString(col_qty_x, y, _fmt_qty(qty, lang))
        c.drawString(col_unit_x, y, unit)
        c.drawRightString(col_price_x, y, _fmt_money(unit_price, lang))
        c.drawRightString(col_amt_x, y, _fmt_money(amount, lang))

        y -= 0.95 * cm if len(parts) > 1 else 0.55 * cm
        if y < 5.0 * cm:
            c.showPage()
            y = h - 2.0 * cm
            c.setFont("Helvetica", 10)

    # Totals
    vat_amount = net_total * vat_rate
    gross_total = net_total + vat_amount

    # Totals box
    y -= 0.15 * cm
    c.setLineWidth(0.6)
    c.line(2.0 * cm, y, w - 2.0 * cm, y)
    y -= 0.65 * cm
    c.setFont("Helvetica", 10)
    c.drawRightString(15.5 * cm, y, t(lang, "pdf.subtotal"))
    c.drawRightString(col_amt_x, y, _fmt_money(net_total, lang))
    y -= 0.55 * cm
    if vat_enabled and vat_rate > 0:
        c.drawRightString(15.5 * cm, y, f"{t(lang, 'pdf.vat')} ({vat_rate_percent:.1f}%)")
        c.drawRightString(col_amt_x, y, _fmt_money(vat_amount, lang))
        y -= 0.55 * cm
        c.setFont("Helvetica-Bold", 11)
        c.drawRightString(15.5 * cm, y, t(lang, "pdf.total"))
        c.drawRightString(col_amt_x, y, _fmt_money(gross_total, lang))
    else:
        c.setFont("Helvetica-Bold", 11)
        c.drawRightString(15.5 * cm, y, t(lang, "pdf.total"))
        c.drawRightString(col_amt_x, y, _fmt_money(net_total, lang))

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


# ============================================================
# Rich Email Report PDFs  (v10.0.0)
# ============================================================

# Brand colours (RGB 0-1)
_C_HEADER_BG   = (0.118, 0.420, 0.549)   # #1E6B8C  deep blue
_C_HEADER_TEXT = (1.0,   1.0,   1.0)      # white
_C_KPI_BG      = (0.922, 0.961, 0.976)   # #EBF4F9  pale blue
_C_KPI_BORDER  = (0.392, 0.639, 0.773)   # #64A3C5
_C_TH_BG       = (0.173, 0.243, 0.314)   # #2C3E50  dark slate
_C_TH_TEXT     = (1.0,   1.0,   1.0)
_C_ROW_ALT     = (0.957, 0.980, 0.996)   # very light blue
_C_POSITIVE    = (0.153, 0.682, 0.376)   # #27AE60
_C_NEGATIVE    = (0.906, 0.298, 0.235)   # #E74C3C
_C_NEUTRAL     = (0.502, 0.502, 0.502)
_C_LINE        = (0.800, 0.824, 0.843)
_C_TEXT        = (0.173, 0.243, 0.314)


@dataclass
class EmailReportData:
    """Rich data payload for email report PDF generation (v10.1)."""
    report_type: str                            # "daily" | "monthly"
    period_start: datetime
    period_end: datetime
    totals: List[ReportTotals]                  # per-device totals
    hourly_kwh: List[float] = field(default_factory=list)   # len 24 for daily (aggregate)
    daily_kwh: List[Tuple[date, float]] = field(default_factory=list)   # for monthly (aggregate)
    co2_kg: float = 0.0
    co2_intensity_g_per_kwh: float = 380.0
    prev_kwh: float = 0.0                       # previous period total kWh
    prev_cost_eur: float = 0.0
    price_per_kwh: float = 0.0
    vat_rate: float = 0.0                       # 0.0 if disabled
    version: str = ""
    # v10.1 additions – all optional (default_factory keeps backward compat)
    per_device_hourly: Dict[str, List[float]] = field(default_factory=dict)   # name -> 24 vals
    per_device_daily: Dict[str, List[Tuple[date, float]]] = field(default_factory=dict)  # name -> days
    peak_hour: int = -1                         # hour index 0-23 with highest kWh
    avg_power_w: float = 0.0                    # overall weighted average W
    peak_power_w: float = 0.0                   # overall peak W across all devices
    prev_same_weekday_kwh: float = 0.0          # daily: same weekday last week
    weekday_avg_kwh: float = 0.0               # monthly: avg weekday day-kWh
    weekend_avg_kwh: float = 0.0               # monthly: avg weekend day-kWh
    best_day_date: Optional[date] = None       # monthly: day with lowest consumption
    best_day_kwh: float = 0.0
    worst_day_date: Optional[date] = None      # monthly: day with highest consumption
    worst_day_kwh: float = 0.0


# ---------- Matplotlib chart helpers ----------

def _make_hourly_chart(hourly_kwh: List[float], lang: str, tmp_dir: Path) -> Optional[Path]:
    """Render a 24-hour bar chart to a temp PNG and return its path."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.ticker as mticker

        hours = list(range(24))
        vals  = [hourly_kwh[h] if h < len(hourly_kwh) else 0.0 for h in hours]
        max_v = max(vals) if vals else 1.0
        colors = ["#F0A500" if v == max_v and max_v > 0 else "#1E6B8C" for v in vals]

        fig, ax = plt.subplots(figsize=(10, 3.4))
        fig.patch.set_facecolor("#F8FBFD")
        ax.set_facecolor("#F8FBFD")
        ax.bar(hours, vals, color=colors, width=0.7, zorder=3)
        ax.set_xlabel("Hour" if normalize_lang(lang) == "en" else "Stunde", fontsize=9)
        ax.set_ylabel("kWh", fontsize=9)
        ax.set_xticks(hours)
        ax.set_xticklabels([f"{h:02d}" for h in hours], fontsize=7)
        ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.2f"))
        ax.tick_params(axis="y", labelsize=8)
        ax.grid(axis="y", color="#D0DDE8", linewidth=0.6, zorder=0)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        fig.tight_layout(pad=0.6)

        out = tmp_dir / "_chart_hourly.png"
        fig.savefig(str(out), dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
        plt.close(fig)
        return out
    except Exception:
        return None


def _make_daily_chart(daily_kwh: List[Tuple[date, float]], lang: str, tmp_dir: Path) -> Optional[Path]:
    """Render a per-day bar chart to a temp PNG and return its path."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.ticker as mticker

        days  = [d for d, _ in daily_kwh]
        vals  = [v for _, v in daily_kwh]
        if not vals:
            return None
        max_v = max(vals)
        colors = ["#F0A500" if v == max_v and max_v > 0 else "#1E6B8C" for v in vals]
        labels = [str(d.day) for d in days]

        fig_w = max(8.0, len(days) * 0.38)
        fig, ax = plt.subplots(figsize=(fig_w, 3.4))
        fig.patch.set_facecolor("#F8FBFD")
        ax.set_facecolor("#F8FBFD")
        ax.bar(range(len(days)), vals, color=colors, width=0.7, zorder=3)
        ax.set_xlabel("Day" if normalize_lang(lang) == "en" else "Tag", fontsize=9)
        ax.set_ylabel("kWh", fontsize=9)
        ax.set_xticks(range(len(days)))
        ax.set_xticklabels(labels, fontsize=7)
        ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.2f"))
        ax.tick_params(axis="y", labelsize=8)
        ax.grid(axis="y", color="#D0DDE8", linewidth=0.6, zorder=0)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        fig.tight_layout(pad=0.6)

        out = tmp_dir / "_chart_daily.png"
        fig.savefig(str(out), dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
        plt.close(fig)
        return out
    except Exception:
        return None


# Consistent per-device colour palette (up to 10 devices)
_DEVICE_PALETTE = [
    "#1E6B8C",  # blue  (matches brand header)
    "#E67E22",  # orange
    "#27AE60",  # green
    "#8E44AD",  # purple
    "#E74C3C",  # red
    "#16A085",  # teal
    "#F39C12",  # amber
    "#2980B9",  # light blue
    "#C0392B",  # dark red
    "#1ABC9C",  # mint
]


def _device_color(idx: int) -> str:
    return _DEVICE_PALETTE[idx % len(_DEVICE_PALETTE)]


def _make_stacked_hourly_chart(
    per_device_hourly: Dict[str, List[float]],
    lang: str,
    tmp_dir: Path,
) -> Optional[Path]:
    """Render a stacked 24-hour bar chart (one colour per device) to a temp PNG."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.ticker as mticker
        import numpy as np

        names = list(per_device_hourly.keys())
        if not names:
            return None
        hours = list(range(24))
        fig, ax = plt.subplots(figsize=(10, 3.8))
        fig.patch.set_facecolor("#F8FBFD")
        ax.set_facecolor("#F8FBFD")
        bottoms = np.zeros(24)
        for idx, name in enumerate(names):
            vals = np.array([per_device_hourly[name][h] if h < len(per_device_hourly[name]) else 0.0 for h in hours])
            ax.bar(hours, vals, bottom=bottoms, color=_device_color(idx), width=0.7, label=name[:25], zorder=3)
            bottoms += vals
        ax.set_xlabel("Hour" if normalize_lang(lang) == "en" else "Stunde", fontsize=9)
        ax.set_ylabel("kWh", fontsize=9)
        ax.set_xticks(hours)
        ax.set_xticklabels([f"{h:02d}" for h in hours], fontsize=7)
        ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.2f"))
        ax.tick_params(axis="y", labelsize=8)
        ax.grid(axis="y", color="#D0DDE8", linewidth=0.6, zorder=0)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        if len(names) > 1:
            ax.legend(fontsize=7, loc="upper right", framealpha=0.7)
        fig.tight_layout(pad=0.6)
        out = tmp_dir / "_chart_stacked_hourly.png"
        fig.savefig(str(out), dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
        plt.close(fig)
        return out
    except Exception:
        return None


def _make_stacked_daily_chart(
    per_device_daily: Dict[str, List[Tuple[date, float]]],
    lang: str,
    tmp_dir: Path,
) -> Optional[Path]:
    """Render a stacked per-day bar chart (one colour per device) to a temp PNG."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.ticker as mticker
        import numpy as np
        from datetime import date as _date

        names = list(per_device_daily.keys())
        if not names:
            return None
        # Collect all unique dates in order
        all_dates: list = sorted(set(d for nm in names for d, _ in per_device_daily[nm]))
        if not all_dates:
            return None
        date_idx = {d: i for i, d in enumerate(all_dates)}
        n = len(all_dates)
        labels = [str(d.day) for d in all_dates]
        fig_w = max(8.0, n * 0.38)
        fig, ax = plt.subplots(figsize=(fig_w, 3.8))
        fig.patch.set_facecolor("#F8FBFD")
        ax.set_facecolor("#F8FBFD")
        bottoms = np.zeros(n)
        for idx, name in enumerate(names):
            vals = np.zeros(n)
            for d, v in per_device_daily[name]:
                if d in date_idx:
                    vals[date_idx[d]] = float(v or 0.0)
            ax.bar(range(n), vals, bottom=bottoms, color=_device_color(idx), width=0.7, label=name[:25], zorder=3)
            bottoms += vals
        ax.set_xlabel("Day" if normalize_lang(lang) == "en" else "Tag", fontsize=9)
        ax.set_ylabel("kWh", fontsize=9)
        ax.set_xticks(range(n))
        ax.set_xticklabels(labels, fontsize=7)
        ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.2f"))
        ax.tick_params(axis="y", labelsize=8)
        ax.grid(axis="y", color="#D0DDE8", linewidth=0.6, zorder=0)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        if len(names) > 1:
            ax.legend(fontsize=7, loc="upper right", framealpha=0.7)
        fig.tight_layout(pad=0.6)
        out = tmp_dir / "_chart_stacked_daily.png"
        fig.savefig(str(out), dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
        plt.close(fig)
        return out
    except Exception:
        return None


def _make_device_mini_chart_hourly(
    hourly_vals: List[float],
    color: str,
    lang: str,
    tmp_dir: Path,
    suffix: str,
) -> Optional[Path]:
    """Render a small 24h bar chart for one device."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.ticker as mticker

        hours = list(range(24))
        vals = [hourly_vals[h] if h < len(hourly_vals) else 0.0 for h in hours]
        fig, ax = plt.subplots(figsize=(5.5, 1.8))
        fig.patch.set_facecolor("#F8FBFD")
        ax.set_facecolor("#F8FBFD")
        ax.bar(hours, vals, color=color, width=0.7, zorder=3)
        ax.set_xticks(range(0, 24, 3))
        ax.set_xticklabels([f"{h:02d}" for h in range(0, 24, 3)], fontsize=6)
        ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.2f"))
        ax.tick_params(axis="y", labelsize=6)
        ax.grid(axis="y", color="#D0DDE8", linewidth=0.5, zorder=0)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        fig.tight_layout(pad=0.3)
        out = tmp_dir / f"_mini_h_{suffix}.png"
        fig.savefig(str(out), dpi=130, bbox_inches="tight", facecolor=fig.get_facecolor())
        plt.close(fig)
        return out
    except Exception:
        return None


def _make_device_mini_chart_daily(
    daily_vals: List[Tuple[date, float]],
    color: str,
    lang: str,
    tmp_dir: Path,
    suffix: str,
) -> Optional[Path]:
    """Render a small daily bar chart for one device."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.ticker as mticker

        if not daily_vals:
            return None
        labels = [str(d.day) for d, _ in daily_vals]
        vals = [float(v or 0.0) for _, v in daily_vals]
        n = len(vals)
        fig_w = max(5.5, n * 0.22)
        fig, ax = plt.subplots(figsize=(fig_w, 1.8))
        fig.patch.set_facecolor("#F8FBFD")
        ax.set_facecolor("#F8FBFD")
        ax.bar(range(n), vals, color=color, width=0.7, zorder=3)
        step = max(1, n // 10)
        ax.set_xticks(range(0, n, step))
        ax.set_xticklabels(labels[::step], fontsize=6)
        ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.2f"))
        ax.tick_params(axis="y", labelsize=6)
        ax.grid(axis="y", color="#D0DDE8", linewidth=0.5, zorder=0)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        fig.tight_layout(pad=0.3)
        out = tmp_dir / f"_mini_d_{suffix}.png"
        fig.savefig(str(out), dpi=130, bbox_inches="tight", facecolor=fig.get_facecolor())
        plt.close(fig)
        return out
    except Exception:
        return None


def _make_top5_bar_chart(
    totals: List[ReportTotals],
    lang: str,
    tmp_dir: Path,
) -> Optional[Path]:
    """Render a horizontal bar chart of top-5 consumers."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.ticker as mticker

        sorted_t = sorted(totals, key=lambda r: r.kwh_total, reverse=True)[:5]
        if not sorted_t:
            return None
        names = [r.name[:30] for r in reversed(sorted_t)]
        vals = [r.kwh_total for r in reversed(sorted_t)]
        colors = [_device_color(totals.index(r) if r in totals else 0) for r in reversed(sorted_t)]

        fig, ax = plt.subplots(figsize=(7, max(2.0, len(names) * 0.55)))
        fig.patch.set_facecolor("#F8FBFD")
        ax.set_facecolor("#F8FBFD")
        bars = ax.barh(range(len(names)), vals, color=colors, zorder=3)
        ax.set_yticks(range(len(names)))
        ax.set_yticklabels(names, fontsize=8)
        ax.set_xlabel("kWh", fontsize=9)
        ax.xaxis.set_major_formatter(mticker.FormatStrFormatter("%.2f"))
        ax.tick_params(axis="x", labelsize=8)
        ax.grid(axis="x", color="#D0DDE8", linewidth=0.6, zorder=0)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        # Value labels on bars
        for bar, v in zip(bars, vals):
            ax.text(bar.get_width() * 1.01, bar.get_y() + bar.get_height() / 2,
                    f"{v:.2f}", va="center", fontsize=7)
        fig.tight_layout(pad=0.5)
        out = tmp_dir / "_chart_top5.png"
        fig.savefig(str(out), dpi=130, bbox_inches="tight", facecolor=fig.get_facecolor())
        plt.close(fig)
        return out
    except Exception:
        return None


# ---------- Layout helpers ----------

def _rl_set_fill(c: canvas.Canvas, rgb: Tuple) -> None:
    c.setFillColorRGB(*rgb)


def _rl_set_stroke(c: canvas.Canvas, rgb: Tuple) -> None:
    c.setStrokeColorRGB(*rgb)


def _draw_header_band(c: canvas.Canvas, w: float, h: float,
                      title: str, subtitle: str) -> float:
    """Draw top header band; returns y coordinate just below the band."""
    band_h = 1.8 * cm
    y_top  = h
    _rl_set_fill(c, _C_HEADER_BG)
    c.rect(0, y_top - band_h, w, band_h, stroke=0, fill=1)
    _rl_set_fill(c, _C_HEADER_TEXT)
    c.setFont("Helvetica-Bold", 14)
    c.drawString(1.8 * cm, y_top - 1.15 * cm, title)
    c.setFont("Helvetica", 9)
    c.drawRightString(w - 1.8 * cm, y_top - 1.15 * cm, subtitle)
    _rl_set_fill(c, _C_TEXT)
    return y_top - band_h - 0.35 * cm


def _draw_kpi_box(c: canvas.Canvas, x: float, y_top: float,
                  bw: float, bh: float,
                  label: str, value: str, unit: str = "") -> None:
    """Draw a single KPI tile."""
    _rl_set_fill(c, _C_KPI_BG)
    _rl_set_stroke(c, _C_KPI_BORDER)
    c.setLineWidth(0.8)
    c.roundRect(x, y_top - bh, bw, bh, 4, stroke=1, fill=1)
    _rl_set_fill(c, _C_NEUTRAL)
    c.setFont("Helvetica", 8)
    c.drawCentredString(x + bw / 2, y_top - 0.65 * cm, label)
    _rl_set_fill(c, _C_TEXT)
    c.setFont("Helvetica-Bold", 16)
    c.drawCentredString(x + bw / 2, y_top - 1.45 * cm, value)
    if unit:
        _rl_set_fill(c, _C_NEUTRAL)
        c.setFont("Helvetica", 8)
        c.drawCentredString(x + bw / 2, y_top - 1.90 * cm, unit)
    _rl_set_fill(c, _C_TEXT)


def _draw_comparison_line(c: canvas.Canvas, x: float, y: float,
                          label: str, current_kwh: float,
                          prev_kwh: float, lang: str) -> float:
    """Draw a +/-% comparison row; returns y after the row."""
    if prev_kwh and prev_kwh > 0:
        diff_pct = (current_kwh - prev_kwh) / prev_kwh * 100.0
        arrow    = "+" if diff_pct >= 0 else ""
        color    = _C_NEGATIVE if diff_pct > 5 else (_C_POSITIVE if diff_pct < -5 else _C_NEUTRAL)
        diff_str = f"{arrow}{diff_pct:.1f}%"
    else:
        diff_str = "—"
        color    = _C_NEUTRAL
    c.setFont("Helvetica", 9)
    _rl_set_fill(c, _C_NEUTRAL)
    c.drawString(x, y, label)
    _rl_set_fill(c, color)
    c.setFont("Helvetica-Bold", 9)
    c.drawString(x + 5.5 * cm, y, diff_str)
    _rl_set_fill(c, _C_TEXT)
    return y - 0.55 * cm


def _draw_device_table(c: canvas.Canvas, w: float, y: float,
                       totals: List[ReportTotals], lang: str) -> float:
    """Draw device breakdown table; returns y after table."""
    margin = 1.8 * cm
    tw     = w - 2 * margin
    col_x  = [margin, margin + tw * 0.36, margin + tw * 0.54,
               margin + tw * 0.72, margin + tw * 0.88, margin + tw]

    # Header row
    row_h = 0.55 * cm
    _rl_set_fill(c, _C_TH_BG)
    c.rect(margin, y - row_h, tw, row_h, stroke=0, fill=1)
    _rl_set_fill(c, _C_TH_TEXT)
    c.setFont("Helvetica-Bold", 8)
    headers = [
        t(lang, "pdf.col.name"),
        "kWh",
        t(lang, "pdf.col.cost") + " (EUR)",
        t(lang, "pdf.col.avg_w"),
        t(lang, "pdf.col.max_w"),
        "CO\u2082 (kg)",
    ]
    for i, hdr in enumerate(headers):
        if i == 0:
            c.drawString(col_x[i] + 0.1 * cm, y - 0.38 * cm, hdr)
        else:
            c.drawRightString(col_x[i + 1] - 0.1 * cm if i < 5 else col_x[i] + (col_x[i] - col_x[i - 1]) - 0.1 * cm, y - 0.38 * cm, hdr)
    y -= row_h
    _rl_set_fill(c, _C_TEXT)

    # Data rows
    for idx, row in enumerate(totals):
        if y < 3.5 * cm:
            break
        bg = _C_ROW_ALT if idx % 2 == 0 else (1.0, 1.0, 1.0)
        _rl_set_fill(c, bg)
        c.rect(margin, y - row_h, tw, row_h, stroke=0, fill=1)
        _rl_set_fill(c, _C_TEXT)
        co2 = row.kwh_total * 380.0 / 1000.0  # fallback estimate
        c.setFont("Helvetica", 8)
        c.drawString(col_x[0] + 0.1 * cm, y - 0.38 * cm, (row.name or "")[:35])
        c.drawRightString(col_x[2] - 0.1 * cm, y - 0.38 * cm, _fmt_kwh(row.kwh_total, lang))
        c.drawRightString(col_x[3] - 0.1 * cm, y - 0.38 * cm, _fmt_money(row.cost_eur, lang))
        c.drawRightString(col_x[4] - 0.1 * cm, y - 0.38 * cm, _fmt_int(row.avg_power_w, lang))
        c.drawRightString(col_x[5] - 0.1 * cm, y - 0.38 * cm, _fmt_int(row.max_power_w, lang))
        c.drawRightString(col_x[5] + (tw - (col_x[5] - margin)) - 0.1 * cm, y - 0.38 * cm, f"{co2:.2f}")
        y -= row_h

    # Bottom line
    _rl_set_stroke(c, _C_LINE)
    c.setLineWidth(0.5)
    c.line(margin, y, margin + tw, y)
    _rl_set_stroke(c, _C_TEXT)
    return y - 0.3 * cm


def _draw_device_table_enhanced(
    c: canvas.Canvas, pw: float, y: float,
    totals: List[ReportTotals], co2_intensity: float, lang: str,
) -> float:
    """Device table with % share column: name | kWh | % | EUR | avg W | max W."""
    margin = 1.8 * cm
    tw = pw - 2 * margin
    # Column right-edges (relative to margin): name=38%, kWh=52%, %=60%, EUR=72%, avgW=84%, maxW=100%
    col_x = [margin,
              margin + tw * 0.38,
              margin + tw * 0.52,
              margin + tw * 0.62,
              margin + tw * 0.74,
              margin + tw * 0.87,
              margin + tw]
    total_kwh = sum(r.kwh_total for r in totals) or 1.0

    row_h = 0.55 * cm
    _rl_set_fill(c, _C_TH_BG)
    c.rect(margin, y - row_h, tw, row_h, stroke=0, fill=1)
    _rl_set_fill(c, _C_TH_TEXT)
    c.setFont("Helvetica-Bold", 8)
    hdrs = [
        t(lang, "pdf.col.name"),
        "kWh", "%",
        t(lang, "pdf.col.cost") + " (EUR)",
        t(lang, "pdf.col.avg_w"),
        t(lang, "pdf.col.max_w"),
        "CO\u2082 (kg)",
    ]
    c.drawString(col_x[0] + 0.1 * cm, y - 0.38 * cm, hdrs[0])
    for i in range(1, 7):
        c.drawRightString(col_x[i] - 0.08 * cm, y - 0.38 * cm, hdrs[i])
    y -= row_h
    _rl_set_fill(c, _C_TEXT)

    for idx, row in enumerate(totals):
        if y < 3.5 * cm:
            break
        bg = _C_ROW_ALT if idx % 2 == 0 else (1.0, 1.0, 1.0)
        _rl_set_fill(c, bg)
        c.rect(margin, y - row_h, tw, row_h, stroke=0, fill=1)
        _rl_set_fill(c, _C_TEXT)
        pct = row.kwh_total / total_kwh * 100.0
        co2 = row.kwh_total * co2_intensity / 1000.0
        c.setFont("Helvetica", 8)
        c.drawString(col_x[0] + 0.1 * cm, y - 0.38 * cm, (row.name or "")[:32])
        c.drawRightString(col_x[1] - 0.08 * cm, y - 0.38 * cm, _fmt_kwh(row.kwh_total, lang))
        c.drawRightString(col_x[2] - 0.08 * cm, y - 0.38 * cm, f"{pct:.1f}%")
        c.drawRightString(col_x[3] - 0.08 * cm, y - 0.38 * cm, _fmt_money(row.cost_eur, lang))
        c.drawRightString(col_x[4] - 0.08 * cm, y - 0.38 * cm, _fmt_int(row.avg_power_w, lang))
        c.drawRightString(col_x[5] - 0.08 * cm, y - 0.38 * cm, _fmt_int(row.max_power_w, lang))
        c.drawRightString(col_x[6] - 0.08 * cm, y - 0.38 * cm, f"{co2:.2f}")
        y -= row_h

    _rl_set_stroke(c, _C_LINE)
    c.setLineWidth(0.5)
    c.line(margin, y, margin + tw, y)
    _rl_set_stroke(c, _C_TEXT)
    return y - 0.3 * cm


def _draw_totals_row(c: canvas.Canvas, w: float, y: float,
                     totals: List[ReportTotals], co2_kg: float, lang: str) -> float:
    """Draw a bold totals row below the device table."""
    margin    = 1.8 * cm
    tw        = w - 2 * margin
    total_kwh = sum(r.kwh_total for r in totals)
    total_eur = sum(r.cost_eur  for r in totals)
    co2_disp  = co2_kg if co2_kg > 0 else total_kwh * 380.0 / 1000.0
    row_h     = 0.55 * cm
    _rl_set_fill(c, (0.941, 0.961, 0.980))
    c.rect(margin, y - row_h, tw, row_h, stroke=0, fill=1)
    _rl_set_fill(c, _C_TEXT)
    c.setFont("Helvetica-Bold", 8)
    lbl = t(lang, "pdf.total") if t(lang, "pdf.total") else "Total"
    c.drawString(margin + 0.1 * cm, y - 0.38 * cm, lbl)
    col2 = margin + tw * 0.36
    col3 = margin + tw * 0.54
    col4 = margin + tw * 0.72
    col6_right = margin + tw
    c.drawRightString(col2 - 0.1 * cm, y - 0.38 * cm, _fmt_kwh(total_kwh, lang))
    c.drawRightString(col3 - 0.1 * cm, y - 0.38 * cm, _fmt_money(total_eur, lang))
    c.drawRightString(col6_right - 0.1 * cm, y - 0.38 * cm, f"{co2_disp:.2f}")
    return y - row_h - 0.3 * cm


def _embed_chart(c: canvas.Canvas, w: float, y: float,
                 chart_path: Path, title: str, avail_h: float) -> float:
    """Embed a chart PNG into current page; returns y below chart."""
    if not chart_path or not chart_path.exists():
        return y
    try:
        margin = 1.8 * cm
        img    = ImageReader(str(chart_path))
        iw, ih = img.getSize()
        avail_w = w - 2 * margin
        scale   = min(avail_w / iw, avail_h / ih)
        dw, dh  = iw * scale, ih * scale
        ix = margin + (avail_w - dw) / 2
        iy = y - dh
        c.drawImage(img, ix, iy, width=dw, height=dh,
                    preserveAspectRatio=True, mask="auto")
        return iy - 0.3 * cm
    except Exception:
        return y


def _draw_footer(c: canvas.Canvas, w: float, page_n: int, version: str, lang: str) -> None:
    """Draw page footer with page number and app name."""
    _rl_set_fill(c, _C_NEUTRAL)
    c.setFont("Helvetica", 7)
    app_str = f"Shelly Energy Analyzer {version}" if version else "Shelly Energy Analyzer"
    c.drawString(1.8 * cm, 0.7 * cm, app_str)
    c.drawRightString(w - 1.8 * cm, 0.7 * cm, str(page_n))
    _rl_set_fill(c, _C_TEXT)


# ---------- Public export functions ----------

def export_pdf_email_daily(
    data: EmailReportData,
    out_path: Path,
    lang: str = "de",
) -> Path:
    """Generate a detailed daily-report PDF:
    overview KPIs, enhanced device table, comparisons, stacked hourly chart,
    per-device sections with mini charts and operating stats.
    """
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    lang = normalize_lang(lang)
    is_en = lang == "en"
    tmp_dir = out_path.parent
    margin = 1.8 * cm

    total_kwh = sum(r.kwh_total for r in data.totals)
    total_eur = sum(r.cost_eur  for r in data.totals)
    co2_disp  = data.co2_kg if data.co2_kg > 0 else total_kwh * data.co2_intensity_g_per_kwh / 1000.0

    # Derive extra KPI values
    avg_w   = data.avg_power_w
    peak_w  = data.peak_power_w
    if not avg_w and data.totals:
        valid_avgs = [r.avg_power_w for r in data.totals if r.avg_power_w > 0]
        avg_w = sum(valid_avgs) / len(valid_avgs) if valid_avgs else 0.0
    if not peak_w and data.totals:
        peak_w = max((r.max_power_w for r in data.totals), default=0.0)
    peak_hour_lbl = f"{data.peak_hour:02d}:00" if data.peak_hour >= 0 else "—"

    # Build charts
    stacked_chart = _make_stacked_hourly_chart(data.per_device_hourly, lang, tmp_dir) \
        if data.per_device_hourly else _make_hourly_chart(data.hourly_kwh, lang, tmp_dir)

    c = canvas.Canvas(str(out_path), pagesize=A4)
    pw, ph = A4
    page_n = 1

    date_str = data.period_start.strftime("%Y-%m-%d")
    title_str = "Daily Energy Report" if is_en else "Tagesreport Energie"

    # ------------------------------------------------------------------ Page 1: Overview
    y = _draw_header_band(c, pw, ph, title_str, date_str)

    # Row 1 of KPIs: kWh | EUR | CO2
    kpi_gap = 0.4 * cm
    kpi_w   = (pw - 2 * margin - 2 * kpi_gap) / 3
    kpi_h   = 2.0 * cm
    _draw_kpi_box(c, margin,                         y, kpi_w, kpi_h,
                  t(lang, "pdf.report.total_energy") or "Gesamt kWh",
                  _fmt_kwh(total_kwh, lang), "kWh")
    _draw_kpi_box(c, margin + kpi_w + kpi_gap,       y, kpi_w, kpi_h,
                  t(lang, "pdf.report.total_cost") or "Kosten",
                  _fmt_money(total_eur, lang), "EUR")
    _draw_kpi_box(c, margin + 2 * (kpi_w + kpi_gap), y, kpi_w, kpi_h,
                  "CO\u2082",
                  f"{co2_disp:.3f}", "kg")
    y -= kpi_h + 0.3 * cm

    # Row 2 of KPIs: avg W | peak W | peak hour
    avg_w_lbl  = "Mittl. Leistung" if not is_en else "Avg Power"
    peak_w_lbl = "Spitzenleistung" if not is_en else "Peak Power"
    peak_h_lbl = "Spitzenstunde"   if not is_en else "Peak Hour"
    _draw_kpi_box(c, margin,                         y, kpi_w, kpi_h,
                  avg_w_lbl, _fmt_int(avg_w, lang), "W")
    _draw_kpi_box(c, margin + kpi_w + kpi_gap,       y, kpi_w, kpi_h,
                  peak_w_lbl, _fmt_int(peak_w, lang), "W")
    _draw_kpi_box(c, margin + 2 * (kpi_w + kpi_gap), y, kpi_w, kpi_h,
                  peak_h_lbl, peak_hour_lbl, "")
    y -= kpi_h + 0.5 * cm

    # Comparisons
    prev_day_lbl = "vs. Vortag:" if not is_en else "vs. previous day:"
    prev_wk_lbl  = "vs. gleicher Wochentag (Vorwoche):" if not is_en else "vs. same weekday last week:"
    y = _draw_comparison_line(c, margin, y, prev_day_lbl, total_kwh, data.prev_kwh, lang)
    y = _draw_comparison_line(c, margin, y, prev_wk_lbl,  total_kwh, data.prev_same_weekday_kwh, lang)
    y -= 0.3 * cm

    # Separator
    _rl_set_stroke(c, _C_LINE)
    c.setLineWidth(0.5)
    c.line(margin, y, pw - margin, y)
    y -= 0.5 * cm

    # Device table
    _rl_set_fill(c, _C_TEXT)
    c.setFont("Helvetica-Bold", 10)
    sec_lbl = "Verbrauch je Gerat" if not is_en else "Consumption by Device"
    c.drawString(margin, y, sec_lbl)
    y -= 0.6 * cm

    if data.totals:
        y = _draw_device_table_enhanced(c, pw, y, data.totals, data.co2_intensity_g_per_kwh, lang)
        y = _draw_totals_row(c, pw, y, data.totals, co2_disp, lang)

    # Highlights / Lowlights
    if data.totals and y > 4.0 * cm:
        y -= 0.5 * cm
        _rl_set_stroke(c, _C_LINE)
        c.setLineWidth(0.4)
        c.line(margin, y, pw - margin, y)
        y -= 0.45 * cm
        hl_lbl = "Highlights / Lowlights"
        _rl_set_fill(c, _C_TEXT)
        c.setFont("Helvetica-Bold", 9)
        c.drawString(margin, y, hl_lbl)
        y -= 0.45 * cm
        sorted_t = sorted(data.totals, key=lambda r: r.kwh_total, reverse=True)
        c.setFont("Helvetica", 8)
        hl_high_lbl = "Spitzenverbraucher:" if not is_en else "Top consumer:"
        hl_low_lbl  = "Sparsamster Verbraucher:" if not is_en else "Lowest consumer:"
        ph_high_lbl = "Stunde mit h. Verbrauch:" if not is_en else "Highest-consumption hour:"
        ph_low_lbl  = "Stunde mit n. Verbrauch:" if not is_en else "Lowest-consumption hour:"
        _rl_set_fill(c, _C_TEXT)
        if sorted_t:
            best = sorted_t[0]
            worst = sorted_t[-1]
            pct_best  = best.kwh_total  / total_kwh * 100 if total_kwh > 0 else 0.0
            pct_worst = worst.kwh_total / total_kwh * 100 if total_kwh > 0 else 0.0
            if y > 3.5 * cm:
                c.drawString(margin, y, f"{hl_high_lbl}  {best.name}  ({_fmt_kwh(best.kwh_total, lang)} kWh, {pct_best:.1f}%)")
                y -= 0.40 * cm
            if y > 3.5 * cm:
                c.drawString(margin, y, f"{hl_low_lbl}  {worst.name}  ({_fmt_kwh(worst.kwh_total, lang)} kWh, {pct_worst:.1f}%)")
                y -= 0.40 * cm
        if data.hourly_kwh and y > 3.5 * cm:
            nonzero = [(h, v) for h, v in enumerate(data.hourly_kwh) if v > 0]
            if nonzero:
                h_max = max(nonzero, key=lambda x: x[1])
                h_min = min(nonzero, key=lambda x: x[1])
                c.drawString(margin, y, f"{ph_high_lbl}  {h_max[0]:02d}:00  ({_fmt_kwh(h_max[1], lang)} kWh)")
                y -= 0.40 * cm
                if y > 3.5 * cm:
                    c.drawString(margin, y, f"{ph_low_lbl}  {h_min[0]:02d}:00  ({_fmt_kwh(h_min[1], lang)} kWh)")

    _draw_footer(c, pw, page_n, data.version, lang)

    # ------------------------------------------------------------------ Page 2: Stacked hourly chart
    if stacked_chart and stacked_chart.exists():
        c.showPage()
        page_n += 1
        ch_title = "Stundenverbrauch (gestapelt)" if not is_en else "Hourly Consumption (stacked)"
        y2 = _draw_header_band(c, pw, ph, ch_title, date_str)
        y2 -= 0.4 * cm
        _embed_chart(c, pw, y2, stacked_chart, ch_title, ph - 5.0 * cm)
        _draw_footer(c, pw, page_n, data.version, lang)
        try:
            stacked_chart.unlink(missing_ok=True)
        except Exception:
            pass

    # ------------------------------------------------------------------ Pages 3+: Per-device detail
    dev_names = list(data.per_device_hourly.keys()) if data.per_device_hourly else []
    for dev_idx, dev_name in enumerate(dev_names):
        hourly_dev = data.per_device_hourly.get(dev_name, [0.0] * 24)
        # Find matching totals row
        row = next((r for r in data.totals if r.name == dev_name), None)
        mini_chart = _make_device_mini_chart_hourly(
            hourly_dev, _device_color(dev_idx), lang, tmp_dir, f"d{dev_idx}"
        )
        c.showPage()
        page_n += 1
        dev_date = data.period_start.strftime("%Y-%m-%d")
        sub_title = f"{(title_str)} – {dev_name}"
        y = _draw_header_band(c, pw, ph, sub_title, dev_date)

        # Device stat boxes
        if row:
            pct = row.kwh_total / total_kwh * 100 if total_kwh > 0 else 0.0
            kpi_w3 = (pw - 2 * margin - 2 * kpi_gap) / 3
            _draw_kpi_box(c, margin,                          y, kpi_w3, kpi_h,
                          "kWh", _fmt_kwh(row.kwh_total, lang), "kWh")
            _draw_kpi_box(c, margin + kpi_w3 + kpi_gap,      y, kpi_w3, kpi_h,
                          "EUR", _fmt_money(row.cost_eur, lang), "EUR")
            _draw_kpi_box(c, margin + 2 * (kpi_w3 + kpi_gap), y, kpi_w3, kpi_h,
                          "Anteil" if not is_en else "Share", f"{pct:.1f}", "%")
            y -= kpi_h + 0.3 * cm

        # Operating hours + min/avg/max from hourly data
        nonzero_h = [v for v in hourly_dev if v and v > 0]
        op_hours   = len(nonzero_h)
        min_h      = min(nonzero_h) if nonzero_h else 0.0
        max_h      = max(nonzero_h) if nonzero_h else 0.0
        avg_h      = sum(nonzero_h) / len(nonzero_h) if nonzero_h else 0.0
        if row:
            c.setFont("Helvetica", 9)
            _rl_set_fill(c, _C_TEXT)
            lbl_op   = "Betriebsstunden:" if not is_en else "Operating hours:"
            lbl_minH = "Min kWh/h:" if not is_en else "Min kWh/h:"
            lbl_avgH = "Avg kWh/h:" if not is_en else "Avg kWh/h:"
            lbl_maxH = "Max kWh/h:" if not is_en else "Max kWh/h:"
            lbl_avgW = "Mittl. Leistung:" if not is_en else "Avg power:"
            lbl_pkW  = "Spitzenleistung:" if not is_en else "Peak power:"
            stats = [
                (lbl_op,   f"{op_hours} h"),
                (lbl_minH, _fmt_kwh(min_h, lang)),
                (lbl_avgH, _fmt_kwh(avg_h, lang)),
                (lbl_maxH, _fmt_kwh(max_h, lang)),
                (lbl_avgW, f"{_fmt_int(row.avg_power_w, lang)} W"),
                (lbl_pkW,  f"{_fmt_int(row.max_power_w, lang)} W"),
            ]
            col2_x = margin + (pw - 2 * margin) / 2
            for si, (lbl, val) in enumerate(stats):
                xx = margin if si % 2 == 0 else col2_x
                if si % 2 == 0 and si > 0:
                    y -= 0.45 * cm
                c.setFont("Helvetica", 8)
                _rl_set_fill(c, _C_NEUTRAL)
                c.drawString(xx, y, lbl)
                _rl_set_fill(c, _C_TEXT)
                c.setFont("Helvetica-Bold", 8)
                c.drawString(xx + 4.2 * cm, y, val)
            _rl_set_fill(c, _C_TEXT)
            y -= 0.55 * cm

        # Mini chart
        if mini_chart and mini_chart.exists():
            ch_lbl = "Stundenverbrauch" if not is_en else "Hourly Consumption"
            y -= 0.2 * cm
            _rl_set_fill(c, _C_TEXT)
            c.setFont("Helvetica-Bold", 9)
            c.drawString(margin, y, ch_lbl)
            y -= 0.3 * cm
            _embed_chart(c, pw, y, mini_chart, ch_lbl, min(8.0 * cm, y - 2.5 * cm))
            try:
                mini_chart.unlink(missing_ok=True)
            except Exception:
                pass

        _draw_footer(c, pw, page_n, data.version, lang)

    c.save()
    return out_path


def export_pdf_email_monthly(
    data: EmailReportData,
    out_path: Path,
    lang: str = "de",
) -> Path:
    """Generate a detailed monthly-report PDF:
    overview KPIs, weekday/weekend split, stacked daily chart,
    per-device sections with mini charts, top-5 ranking chart.
    """
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    lang = normalize_lang(lang)
    is_en  = lang == "en"
    tmp_dir = out_path.parent
    margin  = 1.8 * cm

    total_kwh = sum(r.kwh_total for r in data.totals)
    total_eur = sum(r.cost_eur  for r in data.totals)
    co2_disp  = data.co2_kg if data.co2_kg > 0 else total_kwh * data.co2_intensity_g_per_kwh / 1000.0

    # Derive day-level KPIs
    n_days = max(1, len(data.daily_kwh))
    day_avg = total_kwh / n_days
    worst_day_str = (
        f"{data.worst_day_date.strftime('%d.%m')} ({_fmt_kwh(data.worst_day_kwh, lang)} kWh)"
        if data.worst_day_date else "—"
    )
    best_day_str = (
        f"{data.best_day_date.strftime('%d.%m')} ({_fmt_kwh(data.best_day_kwh, lang)} kWh)"
        if data.best_day_date else "—"
    )

    # Build charts
    stacked_chart = _make_stacked_daily_chart(data.per_device_daily, lang, tmp_dir) \
        if data.per_device_daily else _make_daily_chart(data.daily_kwh, lang, tmp_dir)
    top5_chart = _make_top5_bar_chart(data.totals, lang, tmp_dir) if len(data.totals) > 1 else None

    c = canvas.Canvas(str(out_path), pagesize=A4)
    pw, ph = A4
    page_n  = 1

    period_str = f"{data.period_start.strftime('%Y-%m-%d')} \u2013 {data.period_end.strftime('%Y-%m-%d')}"
    title_str  = "Monthly Energy Report" if is_en else "Monatsreport Energie"

    # ------------------------------------------------------------------ Page 1: Overview
    y = _draw_header_band(c, pw, ph, title_str, period_str)

    kpi_gap = 0.4 * cm
    kpi_w   = (pw - 2 * margin - 2 * kpi_gap) / 3
    kpi_h   = 2.0 * cm

    # Row 1: kWh | EUR | CO2
    _draw_kpi_box(c, margin,                         y, kpi_w, kpi_h,
                  "Gesamt kWh" if not is_en else "Total kWh",
                  _fmt_kwh(total_kwh, lang), "kWh")
    _draw_kpi_box(c, margin + kpi_w + kpi_gap,       y, kpi_w, kpi_h,
                  "Gesamt EUR" if not is_en else "Total Cost",
                  _fmt_money(total_eur, lang), "EUR")
    _draw_kpi_box(c, margin + 2 * (kpi_w + kpi_gap), y, kpi_w, kpi_h,
                  "CO\u2082",
                  f"{co2_disp:.3f}", "kg")
    y -= kpi_h + 0.3 * cm

    # Row 2: day avg | worst day | best day
    _draw_kpi_box(c, margin,                         y, kpi_w, kpi_h,
                  "Tagesdurchschnitt" if not is_en else "Daily avg",
                  _fmt_kwh(day_avg, lang), "kWh/d")
    _draw_kpi_box(c, margin + kpi_w + kpi_gap,       y, kpi_w, kpi_h,
                  "Teuerster Tag" if not is_en else "Peak day",
                  worst_day_str, "")
    _draw_kpi_box(c, margin + 2 * (kpi_w + kpi_gap), y, kpi_w, kpi_h,
                  "Gunstigster Tag" if not is_en else "Best day",
                  best_day_str, "")
    y -= kpi_h + 0.5 * cm

    # Comparisons
    prev_mo_lbl = "vs. Vormonat:" if not is_en else "vs. previous month:"
    y = _draw_comparison_line(c, margin, y, prev_mo_lbl, total_kwh, data.prev_kwh, lang)
    # Absolute difference line
    if data.prev_kwh and data.prev_kwh > 0:
        diff_abs = total_kwh - data.prev_kwh
        diff_eur = total_eur - data.prev_cost_eur
        sign_k = "+" if diff_abs >= 0 else ""
        sign_e = "+" if diff_eur >= 0 else ""
        _rl_set_fill(c, _C_NEUTRAL)
        c.setFont("Helvetica", 8)
        abs_str = f"({sign_k}{_fmt_kwh(diff_abs, lang)} kWh, {sign_e}{_fmt_money(diff_eur, lang)} EUR)"
        c.drawString(margin + 5.5 * cm, y, abs_str)
        _rl_set_fill(c, _C_TEXT)
    y -= 0.4 * cm

    # Weekday / weekend split
    if data.weekday_avg_kwh > 0 or data.weekend_avg_kwh > 0:
        _rl_set_fill(c, _C_NEUTRAL)
        c.setFont("Helvetica", 8)
        wk_lbl = "Wochentag Ø:" if not is_en else "Weekday avg:"
        we_lbl = "Wochenende Ø:" if not is_en else "Weekend avg:"
        c.drawString(margin, y, f"{wk_lbl}  {_fmt_kwh(data.weekday_avg_kwh, lang)} kWh/d")
        c.drawString(margin + (pw - 2 * margin) / 2, y, f"{we_lbl}  {_fmt_kwh(data.weekend_avg_kwh, lang)} kWh/d")
        _rl_set_fill(c, _C_TEXT)
        y -= 0.45 * cm
    y -= 0.15 * cm

    # Separator
    _rl_set_stroke(c, _C_LINE)
    c.setLineWidth(0.5)
    c.line(margin, y, pw - margin, y)
    y -= 0.5 * cm

    # Enhanced device table
    _rl_set_fill(c, _C_TEXT)
    c.setFont("Helvetica-Bold", 10)
    sec_lbl = "Verbrauch je Gerat" if not is_en else "Consumption by Device"
    c.drawString(margin, y, sec_lbl)
    y -= 0.6 * cm

    if data.totals:
        y = _draw_device_table_enhanced(c, pw, y, data.totals, data.co2_intensity_g_per_kwh, lang)
        y = _draw_totals_row(c, pw, y, data.totals, co2_disp, lang)

    _draw_footer(c, pw, page_n, data.version, lang)

    # ------------------------------------------------------------------ Page 2: Stacked daily chart
    if stacked_chart and stacked_chart.exists():
        c.showPage()
        page_n += 1
        ch_title = "Tagesverbrauch (gestapelt)" if not is_en else "Daily Consumption (stacked)"
        y2 = _draw_header_band(c, pw, ph, ch_title, period_str)
        y2 -= 0.4 * cm
        _embed_chart(c, pw, y2, stacked_chart, ch_title, ph - 5.0 * cm)
        _draw_footer(c, pw, page_n, data.version, lang)
        try:
            stacked_chart.unlink(missing_ok=True)
        except Exception:
            pass

    # ------------------------------------------------------------------ Pages 3+: Per-device detail
    dev_names = list(data.per_device_daily.keys()) if data.per_device_daily else []
    for dev_idx, dev_name in enumerate(dev_names):
        daily_dev = data.per_device_daily.get(dev_name, [])
        row = next((r for r in data.totals if r.name == dev_name), None)
        mini_chart = _make_device_mini_chart_daily(
            daily_dev, _device_color(dev_idx), lang, tmp_dir, f"m{dev_idx}"
        )
        c.showPage()
        page_n += 1
        sub_title = f"{title_str} – {dev_name}"
        y = _draw_header_band(c, pw, ph, sub_title, period_str)

        if row:
            pct = row.kwh_total / total_kwh * 100 if total_kwh > 0 else 0.0
            kpi_w3 = (pw - 2 * margin - 2 * kpi_gap) / 3
            _draw_kpi_box(c, margin,                          y, kpi_w3, kpi_h,
                          "kWh", _fmt_kwh(row.kwh_total, lang), "kWh")
            _draw_kpi_box(c, margin + kpi_w3 + kpi_gap,      y, kpi_w3, kpi_h,
                          "EUR", _fmt_money(row.cost_eur, lang), "EUR")
            _draw_kpi_box(c, margin + 2 * (kpi_w3 + kpi_gap), y, kpi_w3, kpi_h,
                          "Anteil" if not is_en else "Share", f"{pct:.1f}", "%")
            y -= kpi_h + 0.4 * cm

        # Day-level stats from per_device_daily
        day_vals = [v for _, v in daily_dev if v > 0]
        if day_vals:
            min_d = min(day_vals)
            max_d = max(day_vals)
            avg_d = sum(day_vals) / len(day_vals)
            # Trend: compare first half vs second half
            half = max(1, len(day_vals) // 2)
            first_half_avg  = sum(day_vals[:half]) / half
            second_half_avg = sum(day_vals[half:]) / max(1, len(day_vals) - half)
            if second_half_avg > first_half_avg * 1.05:
                trend = "Steigend" if not is_en else "Rising"
            elif second_half_avg < first_half_avg * 0.95:
                trend = "Fallend" if not is_en else "Falling"
            else:
                trend = "Stabil" if not is_en else "Stable"

            c.setFont("Helvetica", 8)
            _rl_set_fill(c, _C_TEXT)
            stats = [
                ("Min kWh/d:", _fmt_kwh(min_d, lang)),
                ("Avg kWh/d:", _fmt_kwh(avg_d, lang)),
                ("Max kWh/d:", _fmt_kwh(max_d, lang)),
                (("Trend:" if is_en else "Trend:"), trend),
            ]
            col2_x = margin + (pw - 2 * margin) / 2
            for si, (lbl, val) in enumerate(stats):
                xx = margin if si % 2 == 0 else col2_x
                if si % 2 == 0 and si > 0:
                    y -= 0.45 * cm
                _rl_set_fill(c, _C_NEUTRAL)
                c.setFont("Helvetica", 8)
                c.drawString(xx, y, lbl)
                _rl_set_fill(c, _C_TEXT)
                c.setFont("Helvetica-Bold", 8)
                c.drawString(xx + 3.5 * cm, y, val)
            _rl_set_fill(c, _C_TEXT)
            y -= 0.55 * cm

        if mini_chart and mini_chart.exists():
            ch_lbl = "Tagesverbrauch" if not is_en else "Daily Consumption"
            y -= 0.2 * cm
            _rl_set_fill(c, _C_TEXT)
            c.setFont("Helvetica-Bold", 9)
            c.drawString(margin, y, ch_lbl)
            y -= 0.3 * cm
            _embed_chart(c, pw, y, mini_chart, ch_lbl, min(8.0 * cm, y - 2.5 * cm))
            try:
                mini_chart.unlink(missing_ok=True)
            except Exception:
                pass

        _draw_footer(c, pw, page_n, data.version, lang)

    # ------------------------------------------------------------------ Last page: Top-5 ranking chart
    if top5_chart and top5_chart.exists():
        c.showPage()
        page_n += 1
        top_title = "Top-5 Verbraucher" if not is_en else "Top-5 Consumers"
        y3 = _draw_header_band(c, pw, ph, top_title, period_str)
        y3 -= 0.4 * cm
        _embed_chart(c, pw, y3, top5_chart, top_title, ph - 5.0 * cm)
        # Ranking list below chart
        sorted_t = sorted(data.totals, key=lambda r: r.kwh_total, reverse=True)[:5]
        y3 -= min(12.0 * cm, ph - 6.0 * cm)
        if y3 > 5.0 * cm:
            _rl_set_fill(c, _C_TEXT)
            c.setFont("Helvetica-Bold", 9)
            c.drawString(margin, y3, top_title)
            y3 -= 0.5 * cm
            for rank, tr in enumerate(sorted_t, 1):
                if y3 < 3.0 * cm:
                    break
                pct = tr.kwh_total / total_kwh * 100 if total_kwh > 0 else 0.0
                c.setFont("Helvetica", 8)
                _rl_set_fill(c, _C_TEXT)
                c.drawString(margin + 0.2 * cm, y3,
                             f"{rank}. {tr.name[:35]}")
                c.drawRightString(pw - margin, y3,
                                  f"{_fmt_kwh(tr.kwh_total, lang)} kWh  "
                                  f"({pct:.1f}%)  {_fmt_money(tr.cost_eur, lang)} EUR")
                y3 -= 0.48 * cm
        _draw_footer(c, pw, page_n, data.version, lang)
        try:
            top5_chart.unlink(missing_ok=True)
        except Exception:
            pass

    c.save()
    return out_path
