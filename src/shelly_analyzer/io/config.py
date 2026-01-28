from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from shelly_analyzer import __version__


@dataclass(frozen=True)
class DeviceConfig:
    key: str
    name: str
    host: str
    em_id: int = 0
    # Auto-detected metadata (optional, but enables auto-add by IP)
    kind: str = "em"  # em | switch | unknown
    gen: int = 0
    model: str = ""
    phases: int = 3
    supports_emdata: bool = True


@dataclass(frozen=True)
class DownloadConfig:
    chunk_seconds: int = 12 * 3600
    overlap_seconds: int = 60
    timeout_seconds: float = 8.0
    retries: int = 3
    backoff_base_seconds: float = 1.5


@dataclass(frozen=True)
class CsvPackConfig:
    threshold_count: int = 120
    max_megabytes: int = 20
    remove_merged: bool = False


@dataclass(frozen=True)
class UiConfig:
    live_poll_seconds: float = 1.0
    # Several plots can be drawn in Live; keep redraw slightly throttled.
    plot_redraw_seconds: float = 1.0

    # UI language: de|en|es
    language: str = "de"

    # How much history is SHOWN in Live plots (minutes).
    live_window_minutes: int = 10

    # How much history is KEPT in memory (minutes).
    # Requirement: always keep at least 120 minutes so switching the window back
    # shows the full history again.
    live_retention_minutes: int = 120

    # Optional web dashboard for Live (served locally, viewable on phone/desktop).
    # Default True so the phone dashboard works out-of-the-box.
    live_web_enabled: bool = True
    live_web_port: int = 8765
    live_web_refresh_seconds: float = 1.0

    # Optional token to protect the local web dashboard and remote actions.
    # If empty, the app may generate a random token on first start.
    live_web_token: str = ""

    # Optional smoothing for Live plots (rolling mean over a time window).
    live_smoothing_enabled: bool = False
    live_smoothing_seconds: int = 10

    # Live plot filter by time-of-day: all|day|night
    live_daynight_mode: str = "all"
    # Day/Night split times (local time, HH:MM). Default: day 06:00-22:00.
    live_day_start: str = "06:00"
    live_night_start: str = "22:00"

    # Optional Telegram notifications for alerts
    telegram_enabled: bool = False
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    telegram_verify_ssl: bool = True
    telegram_detail_level: str = "detailed"  # simple|detailed

    # Scheduled Telegram summaries
    telegram_daily_summary_enabled: bool = False
    telegram_daily_summary_time: str = "00:00"  # HH:MM, local time
    telegram_monthly_summary_enabled: bool = False
    telegram_monthly_summary_time: str = "00:00"  # HH:MM, local time
    telegram_summary_load_w: float = 200.0  # W threshold for VAR/cosφ stats
    telegram_daily_summary_last_sent: str = ""  # YYYY-MM-DD (optional)
    telegram_monthly_summary_last_sent: str = ""  # YYYY-MM (optional)



    # Auto-sync in the background (triggered from the GUI).

    # Auto-sync in the background (triggered from the GUI).
    autosync_enabled: bool = False
    autosync_interval_hours: int = 12
    # One of: incremental|all|day|week|month
    autosync_mode: str = "incremental"

    # Which device page is selected in the desktop UI (0-based). Each page shows up to 2 devices.
    device_page_index: int = 0



@dataclass(frozen=True)
class AlertRule:
    """Simple local alert rule evaluated on live samples."""
    rule_id: str = "rule1"
    enabled: bool = True
    device_key: str = "*"  # device key or '*' for all
    metric: str = "W"  # W|V|A|VAR|COSPHI (+ _L1/L2/L3)
    op: str = ">"  # >|<|>=|<=|=
    threshold: float = 0.0
    duration_seconds: int = 10
    cooldown_seconds: int = 120
    action_popup: bool = True
    action_beep: bool = True
    action_telegram: bool = False
    message: str = ""

@dataclass(frozen=True)
class PricingConfig:
    electricity_price_eur_per_kwh: float = 0.3265

    # Base fee (Grundpreis)
    # Interpreted as per-year price. Like electricity price, this can be entered as gross (incl. VAT)
    # if base_fee_includes_vat is True.
    base_fee_eur_per_year: float = 127.51
    base_fee_includes_vat: bool = True

    # VAT handling
    # - electricity_price_eur_per_kwh is interpreted as *gross* (incl. VAT) if price_includes_vat is True
    # - VAT can be disabled entirely (vat_enabled=False)
    price_includes_vat: bool = True
    vat_enabled: bool = True
    vat_rate_percent: float = 19.0

    def vat_rate(self) -> float:
        if not self.vat_enabled:
            return 0.0
        return max(0.0, float(self.vat_rate_percent)) / 100.0

    def unit_price_net(self) -> float:
        p = float(self.electricity_price_eur_per_kwh)
        r = self.vat_rate()
        if r <= 0:
            return p
        return p / (1.0 + r) if self.price_includes_vat else p

    def unit_price_gross(self) -> float:
        p = float(self.electricity_price_eur_per_kwh)
        r = self.vat_rate()
        if r <= 0:
            return p
        return p if self.price_includes_vat else p * (1.0 + r)

    def base_fee_year_net(self) -> float:
        """Return base fee per year as NET price."""
        p = float(self.base_fee_eur_per_year)
        r = self.vat_rate()
        if r <= 0:
            return p
        return p / (1.0 + r) if self.base_fee_includes_vat else p

    def base_fee_year_gross(self) -> float:
        """Return base fee per year as GROSS price."""
        p = float(self.base_fee_eur_per_year)
        r = self.vat_rate()
        if r <= 0:
            return p
        return p if self.base_fee_includes_vat else p * (1.0 + r)

    def base_fee_day_net(self, days_in_year: float = 365.0) -> float:
        """Net base fee per day (pro-rated from yearly)."""
        return self.base_fee_year_net() / float(days_in_year)



@dataclass(frozen=True)
class BillingParty:
    name: str = ""
    address_lines: List[str] = field(default_factory=list)
    vat_id: str = ""
    email: str = ""
    phone: str = ""
    iban: str = ""
    bic: str = ""


@dataclass(frozen=True)
class BillingConfig:
    issuer: BillingParty = BillingParty(name="Firma Muster GmbH", address_lines=["Musterstraße 1", "12345 Musterstadt"], vat_id="DE000000000")
    customer: BillingParty = BillingParty(name="Kunde", address_lines=["Kundenstraße 1", "12345 Kundenstadt"]) 
    invoice_prefix: str = "INV"
    payment_terms_days: int = 14


@dataclass(frozen=True)
class AppConfig:
    version: str = __version__
    devices: List[DeviceConfig] = field(
        default_factory=lambda: [
            DeviceConfig(key="shelly1", name="Haus", host="192.168.3.175", em_id=0),
            DeviceConfig(key="shelly2", name="Server", host="192.168.3.189", em_id=0),
        ]
    )
    download: DownloadConfig = DownloadConfig()
    csv_pack: CsvPackConfig = CsvPackConfig()
    ui: UiConfig = UiConfig()
    pricing: PricingConfig = PricingConfig()
    billing: BillingConfig = BillingConfig()
    alerts: List[AlertRule] = field(default_factory=list)


def default_config_path(project_root: Optional[Path] = None) -> Path:
    if project_root is None:
        project_root = Path.cwd()
    return project_root / "config.json"


def _coerce_int(x: Any, default: int) -> int:
    try:
        return int(x)
    except Exception:
        return default


def _coerce_float(x: Any, default: float) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _load_json(path: Path) -> Dict[str, Any]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("config.json must contain a JSON object")
    return raw


def _migrate_legacy(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Migrate legacy config formats to the current schema.

    Supported legacy shapes:
    - {"names": {"left": "Haus", ...}, "ips": {"left": "1.2.3.4", ...}}
    """
    if "devices" in raw and isinstance(raw.get("devices"), list):
        return raw

    names = raw.get("names")
    ips = raw.get("ips")
    if isinstance(names, dict) and isinstance(ips, dict) and ("left" in names or "right" in names):
        left_name = str(names.get("left", "Shelly 1"))
        right_name = str(names.get("right", "Shelly 2"))
        left_host = str(ips.get("left", "127.0.0.1"))
        right_host = str(ips.get("right", "127.0.0.1"))
        raw = dict(raw)
        raw["devices"] = [
            {"key": "shelly1", "name": left_name, "host": left_host, "em_id": 0},
            {"key": "shelly2", "name": right_name, "host": right_host, "em_id": 0},
        ]
        raw.pop("names", None)
        raw.pop("ips", None)
        return raw

    return raw


def load_config(path: Optional[Path] = None) -> AppConfig:
    path = Path(path) if path else default_config_path()
    if not path.exists():
        # First run: create a minimal config without sample devices.
        # This avoids confusing 'Haus/Server' defaults and prevents noisy
        # startup logs about missing CSVs.
        cfg = AppConfig(devices=[])
        save_config(cfg, path)
        return cfg

    raw_original = _load_json(path)
    raw = _migrate_legacy(raw_original)

    devices_raw = raw.get("devices")
    if not isinstance(devices_raw, list):
        raise ValueError("config.json: 'devices' must be a list")
    # Allow empty device lists so the app can start in a 'first run' setup mode.

    devices: List[DeviceConfig] = []
    for i, d in enumerate(devices_raw):
        if not isinstance(d, dict):
            raise ValueError(f"config.json: devices[{i}] must be an object")
        key = str(d.get("key", f"shelly{i+1}"))
        name = str(d.get("name", key))
        host = str(d.get("host", "127.0.0.1"))
        em_id = _coerce_int(d.get("em_id", 0), 0)
        kind = str(d.get("kind", "em") or "em").strip().lower()
        if kind not in {"em", "switch", "unknown"}:
            kind = "em"
        gen = _coerce_int(d.get("gen", 0), 0)
        model = str(d.get("model", "") or "")
        phases = _coerce_int(d.get("phases", 3 if kind == "em" else 1), 3 if kind == "em" else 1)
        phases = 3 if kind == "em" else 1 if phases <= 1 else phases
        supports_emdata = bool(d.get("supports_emdata", True if kind == "em" else False))
        devices.append(
            DeviceConfig(
                key=key,
                name=name,
                host=host,
                em_id=em_id,
                kind=kind,
                gen=gen,
                model=model,
                phases=phases,
                supports_emdata=supports_emdata,
            )
        )

    dwn = raw.get("download", {}) if isinstance(raw.get("download"), dict) else {}
    download = DownloadConfig(
        chunk_seconds=_coerce_int(dwn.get("chunk_seconds", DownloadConfig.chunk_seconds), DownloadConfig.chunk_seconds),
        overlap_seconds=_coerce_int(dwn.get("overlap_seconds", DownloadConfig.overlap_seconds), DownloadConfig.overlap_seconds),
        timeout_seconds=_coerce_float(dwn.get("timeout_seconds", DownloadConfig.timeout_seconds), DownloadConfig.timeout_seconds),
        retries=_coerce_int(dwn.get("retries", DownloadConfig.retries), DownloadConfig.retries),
        backoff_base_seconds=_coerce_float(dwn.get("backoff_base_seconds", DownloadConfig.backoff_base_seconds), DownloadConfig.backoff_base_seconds),
    )

    pack = raw.get("csv_pack", {}) if isinstance(raw.get("csv_pack"), dict) else {}
    csv_pack = CsvPackConfig(
        threshold_count=_coerce_int(pack.get("threshold_count", CsvPackConfig.threshold_count), CsvPackConfig.threshold_count),
        max_megabytes=_coerce_int(pack.get("max_megabytes", CsvPackConfig.max_megabytes), CsvPackConfig.max_megabytes),
        remove_merged=bool(pack.get("remove_merged", CsvPackConfig.remove_merged)),
    )

    ui_raw = raw.get("ui", {}) if isinstance(raw.get("ui"), dict) else {}
    ui = UiConfig(
        live_poll_seconds=_coerce_float(ui_raw.get("live_poll_seconds", UiConfig.live_poll_seconds), UiConfig.live_poll_seconds),
        plot_redraw_seconds=_coerce_float(ui_raw.get("plot_redraw_seconds", UiConfig.plot_redraw_seconds), UiConfig.plot_redraw_seconds),
        language=str(ui_raw.get("language", UiConfig.language)),
        live_retention_minutes=_coerce_int(ui_raw.get("live_retention_minutes", UiConfig.live_retention_minutes), UiConfig.live_retention_minutes),
        live_window_minutes=_coerce_int(ui_raw.get("live_window_minutes", UiConfig.live_window_minutes), UiConfig.live_window_minutes),
        live_web_enabled=bool(ui_raw.get("live_web_enabled", UiConfig.live_web_enabled)),
        live_web_port=_coerce_int(ui_raw.get("live_web_port", UiConfig.live_web_port), UiConfig.live_web_port),
        live_web_refresh_seconds=_coerce_float(ui_raw.get("live_web_refresh_seconds", UiConfig.live_web_refresh_seconds), UiConfig.live_web_refresh_seconds),
        live_web_token=str(ui_raw.get("live_web_token", UiConfig.live_web_token) or ""),
        live_smoothing_enabled=bool(ui_raw.get("live_smoothing_enabled", UiConfig.live_smoothing_enabled)),
        live_smoothing_seconds=_coerce_int(ui_raw.get("live_smoothing_seconds", UiConfig.live_smoothing_seconds), UiConfig.live_smoothing_seconds),
        device_page_index=_coerce_int(ui_raw.get("device_page_index", UiConfig.device_page_index), UiConfig.device_page_index),
        autosync_enabled=bool(ui_raw.get("autosync_enabled", UiConfig.autosync_enabled)),
        autosync_interval_hours=_coerce_int(ui_raw.get("autosync_interval_hours", UiConfig.autosync_interval_hours), UiConfig.autosync_interval_hours),
        autosync_mode=str(ui_raw.get("autosync_mode", UiConfig.autosync_mode)),
        telegram_enabled=bool(ui_raw.get("telegram_enabled", UiConfig.telegram_enabled)),
        telegram_bot_token=str(ui_raw.get("telegram_bot_token", UiConfig.telegram_bot_token) or ""),
        telegram_chat_id=str(ui_raw.get("telegram_chat_id", UiConfig.telegram_chat_id) or ""),
        telegram_verify_ssl=bool(ui_raw.get("telegram_verify_ssl", UiConfig.telegram_verify_ssl)),
        telegram_detail_level=str(ui_raw.get("telegram_detail_level", UiConfig.telegram_detail_level) or UiConfig.telegram_detail_level),
        telegram_daily_summary_enabled=bool(ui_raw.get("telegram_daily_summary_enabled", UiConfig.telegram_daily_summary_enabled)),
        telegram_daily_summary_time=str(ui_raw.get("telegram_daily_summary_time", UiConfig.telegram_daily_summary_time) or UiConfig.telegram_daily_summary_time),
        telegram_monthly_summary_enabled=bool(ui_raw.get("telegram_monthly_summary_enabled", UiConfig.telegram_monthly_summary_enabled)),
        telegram_monthly_summary_time=str(ui_raw.get("telegram_monthly_summary_time", UiConfig.telegram_monthly_summary_time) or UiConfig.telegram_monthly_summary_time),
        telegram_summary_load_w=_coerce_float(ui_raw.get("telegram_summary_load_w", UiConfig.telegram_summary_load_w), UiConfig.telegram_summary_load_w),
        telegram_daily_summary_last_sent=str(ui_raw.get("telegram_daily_summary_last_sent", UiConfig.telegram_daily_summary_last_sent) or ""),
        telegram_monthly_summary_last_sent=str(ui_raw.get("telegram_monthly_summary_last_sent", UiConfig.telegram_monthly_summary_last_sent) or ""),
    )


    pricing_raw = raw.get("pricing", {}) if isinstance(raw.get("pricing"), dict) else {}
    pricing = PricingConfig(
        base_fee_eur_per_year=_coerce_float(pricing_raw.get("base_fee_eur_per_year", PricingConfig.base_fee_eur_per_year), PricingConfig.base_fee_eur_per_year),
        base_fee_includes_vat=bool(pricing_raw.get("base_fee_includes_vat", PricingConfig.base_fee_includes_vat)),
        electricity_price_eur_per_kwh=_coerce_float(
            pricing_raw.get("electricity_price_eur_per_kwh", PricingConfig.electricity_price_eur_per_kwh),
            PricingConfig.electricity_price_eur_per_kwh,
        ),
        price_includes_vat=bool(pricing_raw.get("price_includes_vat", PricingConfig.price_includes_vat)),
        vat_enabled=bool(pricing_raw.get("vat_enabled", PricingConfig.vat_enabled)),
        vat_rate_percent=_coerce_float(pricing_raw.get("vat_rate_percent", PricingConfig.vat_rate_percent), PricingConfig.vat_rate_percent),
    )

    def _party_from_raw(obj: Any, defaults: BillingParty) -> BillingParty:
        if not isinstance(obj, dict):
            return defaults
        lines = obj.get("address_lines")
        if not isinstance(lines, list):
            lines = defaults.address_lines
        return BillingParty(
            name=str(obj.get("name", defaults.name)),
            address_lines=[str(x) for x in lines],
            vat_id=str(obj.get("vat_id", defaults.vat_id)),
            email=str(obj.get("email", defaults.email)),
            phone=str(obj.get("phone", defaults.phone)),
            iban=str(obj.get("iban", defaults.iban)),
            bic=str(obj.get("bic", defaults.bic)),
        )

    billing_raw = raw.get("billing", {}) if isinstance(raw.get("billing"), dict) else {}
    billing = BillingConfig(
        issuer=_party_from_raw(billing_raw.get("issuer"), BillingConfig().issuer),
        customer=_party_from_raw(billing_raw.get("customer"), BillingConfig().customer),
        invoice_prefix=str(billing_raw.get("invoice_prefix", BillingConfig.invoice_prefix)),
        payment_terms_days=_coerce_int(billing_raw.get("payment_terms_days", BillingConfig.payment_terms_days), BillingConfig.payment_terms_days),
    )

    alerts: List[AlertRule] = []
    alerts_raw = raw.get("alerts", [])
    if isinstance(alerts_raw, list):
        for i, a in enumerate(alerts_raw):
            if not isinstance(a, dict):
                continue
            rid = str(a.get("rule_id", a.get("id", f"rule{i+1}")) or f"rule{i+1}")
            metric = str(a.get("metric", "W") or "W").strip().upper()
            if metric in {"COSPHI", "COSΦ", "COS PHI", "COS_PHI"}:
                metric = "COSPHI"
            op = str(a.get("op", ">") or ">").strip()
            if op not in {">", "<", ">=", "<=", "=", "==", "=>", "=<"}:
                op = ">"
            alerts.append(
                AlertRule(
                    rule_id=rid,
                    enabled=bool(a.get("enabled", True)),
                    device_key=str(a.get("device_key", a.get("device", "*")) or "*").strip() or "*",
                    metric=metric,
                    op=op,
                    threshold=_coerce_float(a.get("threshold", 0.0), 0.0),
                    duration_seconds=_coerce_int(a.get("duration_seconds", a.get("duration", 10)), 10),
                    cooldown_seconds=_coerce_int(a.get("cooldown_seconds", a.get("cooldown", 120)), 120),
                    action_popup=bool(a.get("action_popup", a.get("popup", True))),
                    action_beep=bool(a.get("action_beep", a.get("beep", True))),
                    action_telegram=bool(a.get("action_telegram", a.get("telegram", False))),
                    message=str(a.get("message", "") or ""),
                )
            )

    cfg = AppConfig(
        version=str(raw.get("version", __version__)),
        devices=devices,
        download=download,
        csv_pack=csv_pack,
        ui=ui,
        pricing=pricing,
        billing=billing,
        alerts=alerts,
    )

    # Write back migrated schema (and missing blocks) if needed
    needs_writeback = False
    if "devices" not in raw_original:
        needs_writeback = True
    if "pricing" not in raw_original:
        needs_writeback = True
    if "billing" not in raw_original:
        needs_writeback = True
    if "ui" in raw_original and isinstance(raw_original.get("ui"), dict):
        # Might miss new autosync fields
        if "autosync_enabled" not in raw_original["ui"]:
            needs_writeback = True
        if "live_smoothing_enabled" not in raw_original["ui"]:
            needs_writeback = True
        if "live_smoothing_seconds" not in raw_original["ui"]:
            needs_writeback = True
    if "pricing" in raw_original and isinstance(raw_original.get("pricing"), dict):
        if "vat_rate_percent" not in raw_original["pricing"]:
            needs_writeback = True
        if "base_fee_eur_per_year" not in raw_original["pricing"]:
            needs_writeback = True

    if needs_writeback:
        save_config(cfg, path)

    return cfg


def save_config(cfg: AppConfig, path: Optional[Path] = None) -> Path:
    path = Path(path) if path else default_config_path()
    obj = {
        "version": cfg.version,
        "devices": [
            {
                "key": d.key,
                "name": d.name,
                "host": d.host,
                "em_id": d.em_id,
                "kind": getattr(d, "kind", "em"),
                "gen": getattr(d, "gen", 0),
                "model": getattr(d, "model", ""),
                "phases": getattr(d, "phases", 3),
                "supports_emdata": getattr(d, "supports_emdata", True),
            }
            for d in cfg.devices
        ],
        "download": {
            "chunk_seconds": cfg.download.chunk_seconds,
            "overlap_seconds": cfg.download.overlap_seconds,
            "timeout_seconds": cfg.download.timeout_seconds,
            "retries": cfg.download.retries,
            "backoff_base_seconds": cfg.download.backoff_base_seconds,
        },
        "csv_pack": {
            "threshold_count": cfg.csv_pack.threshold_count,
            "max_megabytes": cfg.csv_pack.max_megabytes,
            "remove_merged": cfg.csv_pack.remove_merged,
        },
        "ui": {
            "live_poll_seconds": cfg.ui.live_poll_seconds,
            "plot_redraw_seconds": cfg.ui.plot_redraw_seconds,
            "language": getattr(cfg.ui, "language", "de"),
            "live_window_minutes": cfg.ui.live_window_minutes,
            "live_retention_minutes": getattr(cfg.ui, "live_retention_minutes", 120),
            "device_page_index": getattr(cfg.ui, "device_page_index", 0),
            "autosync_enabled": cfg.ui.autosync_enabled,
            "autosync_interval_hours": cfg.ui.autosync_interval_hours,
            "autosync_mode": cfg.ui.autosync_mode,
            "live_web_enabled": cfg.ui.live_web_enabled,
            "live_web_port": cfg.ui.live_web_port,
            "live_web_refresh_seconds": cfg.ui.live_web_refresh_seconds,
            "live_web_token": cfg.ui.live_web_token,
            "live_smoothing_enabled": getattr(cfg.ui, "live_smoothing_enabled", False),
            "live_smoothing_seconds": getattr(cfg.ui, "live_smoothing_seconds", 10),
            "live_daynight_mode": getattr(cfg.ui, "live_daynight_mode", "all"),
            "live_day_start": getattr(cfg.ui, "live_day_start", "06:00"),
            "live_night_start": getattr(cfg.ui, "live_night_start", "22:00"),
            "telegram_enabled": getattr(cfg.ui, "telegram_enabled", False),
            "telegram_bot_token": getattr(cfg.ui, "telegram_bot_token", ""),
            "telegram_chat_id": getattr(cfg.ui, "telegram_chat_id", ""),
            "telegram_verify_ssl": bool(getattr(cfg.ui, "telegram_verify_ssl", True)),
            "telegram_detail_level": getattr(cfg.ui, "telegram_detail_level", "detailed"),
            "telegram_daily_summary_enabled": getattr(cfg.ui, "telegram_daily_summary_enabled", False),
            "telegram_daily_summary_time": getattr(cfg.ui, "telegram_daily_summary_time", "00:00"),
            "telegram_monthly_summary_enabled": getattr(cfg.ui, "telegram_monthly_summary_enabled", False),
            "telegram_monthly_summary_time": getattr(cfg.ui, "telegram_monthly_summary_time", "00:00"),
            "telegram_daily_summary_last_sent": getattr(cfg.ui, "telegram_daily_summary_last_sent", ""),
            "telegram_monthly_summary_last_sent": getattr(cfg.ui, "telegram_monthly_summary_last_sent", ""),
        },

        "alerts": [
            {
                "rule_id": r.rule_id,
                "enabled": r.enabled,
                "device_key": r.device_key,
                "metric": r.metric,
                "op": r.op,
                "threshold": r.threshold,
                "duration_seconds": r.duration_seconds,
                "cooldown_seconds": r.cooldown_seconds,
                "action_popup": r.action_popup,
                "action_beep": r.action_beep,
                "action_telegram": getattr(r, "action_telegram", False),
                "message": r.message,
            }
            for r in (getattr(cfg, "alerts", []) or [])
        ],
        "pricing": {
            "electricity_price_eur_per_kwh": cfg.pricing.electricity_price_eur_per_kwh,
            "base_fee_eur_per_year": cfg.pricing.base_fee_eur_per_year,
            "base_fee_includes_vat": cfg.pricing.base_fee_includes_vat,
            "price_includes_vat": cfg.pricing.price_includes_vat,
            "vat_enabled": cfg.pricing.vat_enabled,
            "vat_rate_percent": cfg.pricing.vat_rate_percent,
        },
        "billing": {
            "issuer": {
                "name": cfg.billing.issuer.name,
                "address_lines": cfg.billing.issuer.address_lines,
                "vat_id": cfg.billing.issuer.vat_id,
                "email": cfg.billing.issuer.email,
                "phone": cfg.billing.issuer.phone,
                "iban": cfg.billing.issuer.iban,
                "bic": cfg.billing.issuer.bic,
            },
            "customer": {
                "name": cfg.billing.customer.name,
                "address_lines": cfg.billing.customer.address_lines,
                "vat_id": cfg.billing.customer.vat_id,
                "email": cfg.billing.customer.email,
                "phone": cfg.billing.customer.phone,
                "iban": cfg.billing.customer.iban,
                "bic": cfg.billing.customer.bic,
            },
            "invoice_prefix": cfg.billing.invoice_prefix,
            "payment_terms_days": cfg.billing.payment_terms_days,
        },
    }
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return path