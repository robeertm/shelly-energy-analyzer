from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from shelly_analyzer import __version__


@dataclass(frozen=True)
class TouRate:
    """A single Time-of-Use tariff window."""
    name: str = "HT"
    price_eur_per_kwh: float = 0.35
    # Start hour (inclusive, 0-23 local time)
    start_hour: int = 6
    # End hour (exclusive, 0-23 local time; if end_hour < start_hour the window wraps overnight)
    end_hour: int = 22
    # If True, this rate only applies Monday–Friday (weekdays 0–4)
    weekdays_only: bool = False


@dataclass(frozen=True)
class TouConfig:
    """Time-of-Use multi-tariff configuration."""
    enabled: bool = False
    rates: List[TouRate] = field(default_factory=lambda: [
        TouRate(name="HT", price_eur_per_kwh=0.35, start_hour=6, end_hour=22, weekdays_only=False),
        TouRate(name="NT", price_eur_per_kwh=0.22, start_hour=22, end_hour=6, weekdays_only=False),
    ])


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

    # Live plot filter by time-of-day: all|day|night  (legacy, migrated to plot_theme_mode)
    live_daynight_mode: str = "all"
    # Day/Night split times (local time, HH:MM). Default: day 06:00-22:00.
    live_day_start: str = "06:00"
    live_night_start: str = "22:00"

    # Global plot theme: auto|day|night  (applies to all plots: Live + History)
    plot_theme_mode: str = "auto"

    # Optional Telegram notifications for alerts
    telegram_enabled: bool = False
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    telegram_verify_ssl: bool = True
    telegram_detail_level: str = "detailed"  # simple|detailed

    # Include last-10-min W/V/A plots as images in Telegram alarm messages
    telegram_alarm_plots_enabled: bool = True

    # Scheduled Telegram summaries
    telegram_daily_summary_enabled: bool = False
    telegram_daily_summary_time: str = "00:00"  # HH:MM, local time
    telegram_monthly_summary_enabled: bool = False
    telegram_monthly_summary_time: str = "00:00"  # HH:MM, local time
    telegram_summary_load_w: float = 200.0  # W threshold for VAR/cosφ stats
    telegram_daily_summary_last_sent: str = ""  # YYYY-MM-DD (optional)
    telegram_monthly_summary_last_sent: str = ""  # YYYY-MM (optional)



    # Webhook integration (generic HTTP POST, compatible with Home Assistant, ntfy.sh, Node-RED, Zapier, …)
    webhook_enabled: bool = False
    webhook_url: str = ""
    # Custom HTTP headers as a JSON object string, e.g. '{"Authorization": "Bearer mytoken"}'
    webhook_custom_headers: str = ""
    # Which event types trigger a webhook POST
    webhook_alarm_enabled: bool = True
    webhook_daily_summary_enabled: bool = False
    webhook_monthly_summary_enabled: bool = False

    # Auto-sync in the background (triggered from the GUI).

    # Auto-sync in the background (triggered from the GUI).
    autosync_enabled: bool = False
    autosync_interval_hours: int = 12
    # One of: incremental|all|day|week|month
    autosync_mode: str = "incremental"

    # E-Mail report integration (SMTP)
    email_enabled: bool = False
    email_smtp_server: str = ""
    email_smtp_port: int = 587
    email_smtp_user: str = ""
    email_smtp_password: str = ""
    email_from_address: str = ""
    email_use_tls: bool = True
    # Comma-separated list of recipient addresses
    email_recipients: str = ""
    # Which event types trigger an e-mail
    email_alarm_enabled: bool = True
    email_daily_summary_enabled: bool = False
    email_monthly_summary_enabled: bool = False
    email_daily_summary_time: str = "00:00"    # HH:MM, local time
    email_monthly_summary_time: str = "00:00"  # HH:MM, local time
    # Attach an invoice PDF to the monthly report email
    email_monthly_invoice_enabled: bool = False

    # Which device page is selected in the desktop UI (0-based). Each page shows up to 2 devices.
    device_page_index: int = 0

    # Selected view type: "page" | "group" | "all"
    selected_view_type: str = "page"
    # Group name when selected_view_type == "group"
    selected_view_group: str = ""




@dataclass(frozen=True)
class DemoConfig:
    enabled: bool = False
    # Deterministic seed so demo behaviour is repeatable across restarts
    seed: int = 1234
    # One of: household|pv-home|heatpump|tenant-metering
    scenario: str = "household"

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
    action_webhook: bool = False
    action_email: bool = False
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

    # CO₂ intensity of the electricity mix (grams CO₂ per kWh).
    # Country presets: DE ~380, AT ~120, CH ~30, green energy ~0.
    co2_intensity_g_per_kwh: float = 380.0

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
    invoice_logo_path: str = ""


@dataclass(frozen=False)
class UpdatesConfig:
    # GitHub repository in the form "owner/repo"
    repo: str = "robertm/shelly-energy-analyzer"
    # Check for updates on startup (non-blocking)
    check_on_start: bool = True
    # Auto-install updates on startup (only if user enabled it)
    auto_install: bool = False


@dataclass(frozen=True)
class SolarConfig:
    """PV/Solar integration settings."""
    enabled: bool = False
    # Key of the device at the grid connection point (negative power = export to grid)
    pv_meter_device_key: str = ""
    # Feed-in tariff (Einspeisevergütung) in €/kWh
    feed_in_tariff_eur_per_kwh: float = 0.082
    # Installed PV capacity in kWp (0 = unknown/not set)
    kw_peak: float = 0.0
    # Battery storage capacity in kWh (0 = no battery)
    battery_kwh: float = 0.0
    # Embodied CO₂ of PV production in kg per kWp (lifecycle, default ~1000 kg/kWp for typical Si panels)
    co2_production_kg_per_kwp: float = 1000.0


@dataclass(frozen=True)
class AnomalyConfig:
    """Anomaly detection settings."""
    enabled: bool = False
    # Number of standard deviations required to trigger an anomaly
    sigma_threshold: float = 2.0
    # Minimum absolute deviation (kWh) to suppress noise for daily-consumption check
    min_deviation_kwh: float = 0.1
    # Rolling baseline window in days
    window_days: int = 30
    # Which checks to run
    check_unusual_daily: bool = True
    check_night_consumption: bool = True
    check_power_peak_time: bool = True
    # Notification channels
    action_telegram: bool = False
    action_webhook: bool = False
    action_email: bool = False
    # Maximum entries kept in the in-memory + persisted history
    max_history: int = 200


@dataclass(frozen=True)
class Co2Config:
    """ENTSO-E CO₂ intensity integration settings."""
    enabled: bool = False
    entso_e_api_token: str = ""
    # ENTSO-E bidding zone (e.g. "DE_LU", "AT", "CH", "FR", "PL", ...)
    bidding_zone: str = "DE_LU"
    # How often to fetch new data from the ENTSO-E API (hours)
    fetch_interval_hours: int = 1
    # How many days of historical data to backfill on first run
    backfill_days: int = 7
    # Whether to highlight green / dirty hours in the 24h heatmap
    show_green_dirty_hours: bool = True
    # g CO₂/kWh threshold for "green" grid hours
    green_threshold_g_per_kwh: float = 150.0
    # g CO₂/kWh threshold for "dirty" grid hours
    dirty_threshold_g_per_kwh: float = 400.0


@dataclass(frozen=True)
class DeviceGroup:
    """A logical group of devices (e.g. 'Apartment 1', 'Workshop')."""
    name: str
    device_keys: List[str] = field(default_factory=list)


@dataclass(frozen=True)
class DeviceSchedule:
    """A timed on/off schedule for a Shelly switch/plug device.

    weekdays: list of integers 0–6 where 0 = Monday … 6 = Sunday.
    shelly_id_on / shelly_id_off: Shelly Schedule IDs returned by Schedule.Create
    (-1 means the schedule lives only locally in the app, not pushed to the device).
    """
    schedule_id: str
    device_key: str
    name: str
    time_on: str               # "HH:MM"
    time_off: str              # "HH:MM"
    weekdays: List[int] = field(default_factory=lambda: [0, 1, 2, 3, 4, 5, 6])
    enabled: bool = True
    switch_id: int = 0
    shelly_id_on: int = -1
    shelly_id_off: int = -1


@dataclass(frozen=True)
class AppConfig:
    version: str = __version__
    devices: List[DeviceConfig] = field(default_factory=list)
    groups: List[DeviceGroup] = field(default_factory=list)

    download: DownloadConfig = field(default_factory=DownloadConfig)
    csv_pack: CsvPackConfig = field(default_factory=CsvPackConfig)
    ui: UiConfig = field(default_factory=UiConfig)
    updates: UpdatesConfig = field(default_factory=UpdatesConfig)
    demo: DemoConfig = field(default_factory=DemoConfig)
    pricing: PricingConfig = field(default_factory=PricingConfig)
    billing: BillingConfig = field(default_factory=BillingConfig)
    alerts: List[AlertRule] = field(default_factory=list)
    solar: SolarConfig = field(default_factory=SolarConfig)
    tou: TouConfig = field(default_factory=TouConfig)
    anomaly: AnomalyConfig = field(default_factory=AnomalyConfig)
    co2: Co2Config = field(default_factory=Co2Config)
    schedules: List[DeviceSchedule] = field(default_factory=list)


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
        _phases_default = 3 if kind == "em" else 1
        phases = _coerce_int(d.get("phases", _phases_default), _phases_default)
        if kind != "em" and phases <= 1:
            phases = 1
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
        selected_view_type=str(ui_raw.get("selected_view_type", UiConfig.selected_view_type) or "page"),
        selected_view_group=str(ui_raw.get("selected_view_group", UiConfig.selected_view_group) or ""),
        autosync_enabled=bool(ui_raw.get("autosync_enabled", UiConfig.autosync_enabled)),
        autosync_interval_hours=_coerce_int(ui_raw.get("autosync_interval_hours", UiConfig.autosync_interval_hours), UiConfig.autosync_interval_hours),
        autosync_mode=str(ui_raw.get("autosync_mode", UiConfig.autosync_mode)),
        telegram_enabled=bool(ui_raw.get("telegram_enabled", UiConfig.telegram_enabled)),
        telegram_bot_token=str(ui_raw.get("telegram_bot_token", UiConfig.telegram_bot_token) or ""),
        telegram_chat_id=str(ui_raw.get("telegram_chat_id", UiConfig.telegram_chat_id) or ""),
        telegram_verify_ssl=bool(ui_raw.get("telegram_verify_ssl", UiConfig.telegram_verify_ssl)),
        telegram_detail_level=str(ui_raw.get("telegram_detail_level", UiConfig.telegram_detail_level) or UiConfig.telegram_detail_level),
        telegram_alarm_plots_enabled=bool(ui_raw.get("telegram_alarm_plots_enabled", UiConfig.telegram_alarm_plots_enabled)),
        telegram_daily_summary_enabled=bool(ui_raw.get("telegram_daily_summary_enabled", UiConfig.telegram_daily_summary_enabled)),
        telegram_daily_summary_time=str(ui_raw.get("telegram_daily_summary_time", UiConfig.telegram_daily_summary_time) or UiConfig.telegram_daily_summary_time),
        telegram_monthly_summary_enabled=bool(ui_raw.get("telegram_monthly_summary_enabled", UiConfig.telegram_monthly_summary_enabled)),
        telegram_monthly_summary_time=str(ui_raw.get("telegram_monthly_summary_time", UiConfig.telegram_monthly_summary_time) or UiConfig.telegram_monthly_summary_time),
        telegram_summary_load_w=_coerce_float(ui_raw.get("telegram_summary_load_w", UiConfig.telegram_summary_load_w), UiConfig.telegram_summary_load_w),
        telegram_daily_summary_last_sent=str(ui_raw.get("telegram_daily_summary_last_sent", UiConfig.telegram_daily_summary_last_sent) or ""),
        telegram_monthly_summary_last_sent=str(ui_raw.get("telegram_monthly_summary_last_sent", UiConfig.telegram_monthly_summary_last_sent) or ""),
        webhook_enabled=bool(ui_raw.get("webhook_enabled", UiConfig.webhook_enabled)),
        webhook_url=str(ui_raw.get("webhook_url", UiConfig.webhook_url) or ""),
        webhook_custom_headers=str(ui_raw.get("webhook_custom_headers", UiConfig.webhook_custom_headers) or ""),
        webhook_alarm_enabled=bool(ui_raw.get("webhook_alarm_enabled", UiConfig.webhook_alarm_enabled)),
        webhook_daily_summary_enabled=bool(ui_raw.get("webhook_daily_summary_enabled", UiConfig.webhook_daily_summary_enabled)),
        webhook_monthly_summary_enabled=bool(ui_raw.get("webhook_monthly_summary_enabled", UiConfig.webhook_monthly_summary_enabled)),
        email_enabled=bool(ui_raw.get("email_enabled", UiConfig.email_enabled)),
        email_smtp_server=str(ui_raw.get("email_smtp_server", UiConfig.email_smtp_server) or ""),
        email_smtp_port=_coerce_int(ui_raw.get("email_smtp_port", UiConfig.email_smtp_port), UiConfig.email_smtp_port),
        email_smtp_user=str(ui_raw.get("email_smtp_user", UiConfig.email_smtp_user) or ""),
        email_smtp_password=str(ui_raw.get("email_smtp_password", UiConfig.email_smtp_password) or ""),
        email_from_address=str(ui_raw.get("email_from_address", UiConfig.email_from_address) or ""),
        email_use_tls=bool(ui_raw.get("email_use_tls", UiConfig.email_use_tls)),
        email_recipients=str(ui_raw.get("email_recipients", UiConfig.email_recipients) or ""),
        email_alarm_enabled=bool(ui_raw.get("email_alarm_enabled", UiConfig.email_alarm_enabled)),
        email_daily_summary_enabled=bool(ui_raw.get("email_daily_summary_enabled", UiConfig.email_daily_summary_enabled)),
        email_monthly_summary_enabled=bool(ui_raw.get("email_monthly_summary_enabled", UiConfig.email_monthly_summary_enabled)),
        email_daily_summary_time=str(ui_raw.get("email_daily_summary_time", UiConfig.email_daily_summary_time) or UiConfig.email_daily_summary_time),
        email_monthly_summary_time=str(ui_raw.get("email_monthly_summary_time", UiConfig.email_monthly_summary_time) or UiConfig.email_monthly_summary_time),
        email_monthly_invoice_enabled=bool(ui_raw.get("email_monthly_invoice_enabled", UiConfig.email_monthly_invoice_enabled)),
        live_daynight_mode=str(ui_raw.get("live_daynight_mode", UiConfig.live_daynight_mode) or UiConfig.live_daynight_mode),
        live_day_start=str(ui_raw.get("live_day_start", UiConfig.live_day_start) or UiConfig.live_day_start),
        live_night_start=str(ui_raw.get("live_night_start", UiConfig.live_night_start) or UiConfig.live_night_start),
        plot_theme_mode=str(ui_raw.get("plot_theme_mode", UiConfig.plot_theme_mode) or UiConfig.plot_theme_mode),
    )

    updates_raw = raw.get("updates", {}) if isinstance(raw.get("updates"), dict) else {}
    updates = UpdatesConfig(
        repo=str(updates_raw.get("repo", UpdatesConfig.repo)),
        check_on_start=bool(updates_raw.get("check_on_start", UpdatesConfig.check_on_start)),
        auto_install=bool(updates_raw.get("auto_install", UpdatesConfig.auto_install)),
    )

    demo_raw = raw.get('demo', {}) if isinstance(raw.get('demo'), dict) else {}
    demo = DemoConfig(
        enabled=bool(demo_raw.get('enabled', False)),
        seed=_coerce_int(demo_raw.get('seed', DemoConfig.seed), DemoConfig.seed),
        scenario=str(demo_raw.get('scenario', DemoConfig.scenario) or DemoConfig.scenario),
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
        co2_intensity_g_per_kwh=_coerce_float(pricing_raw.get("co2_intensity_g_per_kwh", PricingConfig.co2_intensity_g_per_kwh), PricingConfig.co2_intensity_g_per_kwh),
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
        invoice_logo_path=str(billing_raw.get("invoice_logo_path", "") or ""),
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
                    action_webhook=bool(a.get("action_webhook", False)),
                    action_email=bool(a.get("action_email", False)),
                    message=str(a.get("message", "") or ""),
                )
            )

    solar_raw = raw.get("solar", {}) if isinstance(raw.get("solar"), dict) else {}
    solar = SolarConfig(
        enabled=bool(solar_raw.get("enabled", SolarConfig.enabled)),
        pv_meter_device_key=str(solar_raw.get("pv_meter_device_key", SolarConfig.pv_meter_device_key) or ""),
        feed_in_tariff_eur_per_kwh=_coerce_float(
            solar_raw.get("feed_in_tariff_eur_per_kwh", SolarConfig.feed_in_tariff_eur_per_kwh),
            SolarConfig.feed_in_tariff_eur_per_kwh,
        ),
        kw_peak=_coerce_float(solar_raw.get("kw_peak", SolarConfig.kw_peak), SolarConfig.kw_peak),
        battery_kwh=_coerce_float(solar_raw.get("battery_kwh", SolarConfig.battery_kwh), SolarConfig.battery_kwh),
        co2_production_kg_per_kwp=_coerce_float(
            solar_raw.get("co2_production_kg_per_kwp", SolarConfig.co2_production_kg_per_kwp),
            SolarConfig.co2_production_kg_per_kwp,
        ),
    )

    tou_raw = raw.get("tou", {}) if isinstance(raw.get("tou"), dict) else {}
    _default_tou_rates = TouConfig().rates
    tou_rates_raw = tou_raw.get("rates", None)
    if isinstance(tou_rates_raw, list) and tou_rates_raw:
        tou_rates: List[TouRate] = []
        for r in tou_rates_raw:
            if not isinstance(r, dict):
                continue
            tou_rates.append(TouRate(
                name=str(r.get("name", "HT") or "HT"),
                price_eur_per_kwh=_coerce_float(r.get("price_eur_per_kwh", 0.35), 0.35),
                start_hour=_coerce_int(r.get("start_hour", 6), 6),
                end_hour=_coerce_int(r.get("end_hour", 22), 22),
                weekdays_only=bool(r.get("weekdays_only", False)),
            ))
        if not tou_rates:
            tou_rates = list(_default_tou_rates)
    else:
        tou_rates = list(_default_tou_rates)
    tou = TouConfig(
        enabled=bool(tou_raw.get("enabled", False)),
        rates=tou_rates,
    )

    anomaly_raw = raw.get("anomaly", {}) if isinstance(raw.get("anomaly"), dict) else {}
    anomaly = AnomalyConfig(
        enabled=bool(anomaly_raw.get("enabled", AnomalyConfig.enabled)),
        sigma_threshold=_coerce_float(anomaly_raw.get("sigma_threshold", AnomalyConfig.sigma_threshold), AnomalyConfig.sigma_threshold),
        min_deviation_kwh=_coerce_float(anomaly_raw.get("min_deviation_kwh", AnomalyConfig.min_deviation_kwh), AnomalyConfig.min_deviation_kwh),
        window_days=_coerce_int(anomaly_raw.get("window_days", AnomalyConfig.window_days), AnomalyConfig.window_days),
        check_unusual_daily=bool(anomaly_raw.get("check_unusual_daily", AnomalyConfig.check_unusual_daily)),
        check_night_consumption=bool(anomaly_raw.get("check_night_consumption", AnomalyConfig.check_night_consumption)),
        check_power_peak_time=bool(anomaly_raw.get("check_power_peak_time", AnomalyConfig.check_power_peak_time)),
        action_telegram=bool(anomaly_raw.get("action_telegram", AnomalyConfig.action_telegram)),
        action_webhook=bool(anomaly_raw.get("action_webhook", AnomalyConfig.action_webhook)),
        action_email=bool(anomaly_raw.get("action_email", AnomalyConfig.action_email)),
        max_history=_coerce_int(anomaly_raw.get("max_history", AnomalyConfig.max_history), AnomalyConfig.max_history),
    )

    co2_raw = raw.get("co2", {}) if isinstance(raw.get("co2"), dict) else {}
    co2 = Co2Config(
        enabled=bool(co2_raw.get("enabled", Co2Config.enabled)),
        entso_e_api_token=str(co2_raw.get("entso_e_api_token", Co2Config.entso_e_api_token) or ""),
        bidding_zone=str(co2_raw.get("bidding_zone", Co2Config.bidding_zone) or "DE_LU"),
        fetch_interval_hours=_coerce_int(co2_raw.get("fetch_interval_hours", Co2Config.fetch_interval_hours), Co2Config.fetch_interval_hours),
        backfill_days=_coerce_int(co2_raw.get("backfill_days", Co2Config.backfill_days), Co2Config.backfill_days),
        show_green_dirty_hours=bool(co2_raw.get("show_green_dirty_hours", Co2Config.show_green_dirty_hours)),
        green_threshold_g_per_kwh=_coerce_float(co2_raw.get("green_threshold_g_per_kwh", Co2Config.green_threshold_g_per_kwh), Co2Config.green_threshold_g_per_kwh),
        dirty_threshold_g_per_kwh=_coerce_float(co2_raw.get("dirty_threshold_g_per_kwh", Co2Config.dirty_threshold_g_per_kwh), Co2Config.dirty_threshold_g_per_kwh),
    )

    groups: List[DeviceGroup] = []
    groups_raw = raw.get("groups", [])
    if isinstance(groups_raw, list):
        for g in groups_raw:
            if not isinstance(g, dict):
                continue
            gname = str(g.get("name", "") or "").strip()
            if not gname:
                continue
            gkeys = g.get("device_keys", [])
            if not isinstance(gkeys, list):
                gkeys = []
            groups.append(DeviceGroup(
                name=gname,
                device_keys=[str(k) for k in gkeys if k],
            ))

    schedules: List[DeviceSchedule] = []
    schedules_raw = raw.get("schedules", [])
    if isinstance(schedules_raw, list):
        for s in schedules_raw:
            if not isinstance(s, dict):
                continue
            sid = str(s.get("schedule_id", "") or "").strip()
            if not sid:
                continue
            wdays_raw = s.get("weekdays", [0, 1, 2, 3, 4, 5, 6])
            if not isinstance(wdays_raw, list):
                wdays_raw = [0, 1, 2, 3, 4, 5, 6]
            schedules.append(DeviceSchedule(
                schedule_id=sid,
                device_key=str(s.get("device_key", "") or "").strip(),
                name=str(s.get("name", "") or "").strip(),
                time_on=str(s.get("time_on", "06:00") or "06:00"),
                time_off=str(s.get("time_off", "07:00") or "07:00"),
                weekdays=[int(x) for x in wdays_raw if isinstance(x, (int, float))],
                enabled=bool(s.get("enabled", True)),
                switch_id=_coerce_int(s.get("switch_id", 0), 0),
                shelly_id_on=_coerce_int(s.get("shelly_id_on", -1), -1),
                shelly_id_off=_coerce_int(s.get("shelly_id_off", -1), -1),
            ))

    cfg = AppConfig(
        version=str(raw.get("version", __version__)),
        devices=devices,
        download=download,
        csv_pack=csv_pack,
        ui=ui,
        updates=updates,
        demo=demo,
        pricing=pricing,
        billing=billing,
        alerts=alerts,
        solar=solar,
        tou=tou,
        anomaly=anomaly,
        co2=co2,
        groups=groups,
        schedules=schedules,
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

    if "anomaly" not in raw_original:
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
            "selected_view_type": getattr(cfg.ui, "selected_view_type", "page"),
            "selected_view_group": getattr(cfg.ui, "selected_view_group", ""),
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
            "telegram_alarm_plots_enabled": getattr(cfg.ui, "telegram_alarm_plots_enabled", True),
            "telegram_summary_load_w": getattr(cfg.ui, "telegram_summary_load_w", UiConfig.telegram_summary_load_w),
            "telegram_daily_summary_last_sent": getattr(cfg.ui, "telegram_daily_summary_last_sent", ""),
            "telegram_monthly_summary_last_sent": getattr(cfg.ui, "telegram_monthly_summary_last_sent", ""),
            "webhook_enabled": bool(getattr(cfg.ui, "webhook_enabled", False)),
            "webhook_url": str(getattr(cfg.ui, "webhook_url", "") or ""),
            "webhook_custom_headers": str(getattr(cfg.ui, "webhook_custom_headers", "") or ""),
            "webhook_alarm_enabled": bool(getattr(cfg.ui, "webhook_alarm_enabled", True)),
            "webhook_daily_summary_enabled": bool(getattr(cfg.ui, "webhook_daily_summary_enabled", False)),
            "webhook_monthly_summary_enabled": bool(getattr(cfg.ui, "webhook_monthly_summary_enabled", False)),
            "email_enabled": bool(getattr(cfg.ui, "email_enabled", False)),
            "email_smtp_server": str(getattr(cfg.ui, "email_smtp_server", "") or ""),
            "email_smtp_port": int(getattr(cfg.ui, "email_smtp_port", 587)),
            "email_smtp_user": str(getattr(cfg.ui, "email_smtp_user", "") or ""),
            "email_smtp_password": str(getattr(cfg.ui, "email_smtp_password", "") or ""),
            "email_from_address": str(getattr(cfg.ui, "email_from_address", "") or ""),
            "email_use_tls": bool(getattr(cfg.ui, "email_use_tls", True)),
            "email_recipients": str(getattr(cfg.ui, "email_recipients", "") or ""),
            "email_alarm_enabled": bool(getattr(cfg.ui, "email_alarm_enabled", True)),
            "email_daily_summary_enabled": bool(getattr(cfg.ui, "email_daily_summary_enabled", False)),
            "email_monthly_summary_enabled": bool(getattr(cfg.ui, "email_monthly_summary_enabled", False)),
            "email_daily_summary_time": str(getattr(cfg.ui, "email_daily_summary_time", "00:00") or "00:00"),
            "email_monthly_summary_time": str(getattr(cfg.ui, "email_monthly_summary_time", "00:00") or "00:00"),
            "email_monthly_invoice_enabled": bool(getattr(cfg.ui, "email_monthly_invoice_enabled", False)),
            "plot_theme_mode": getattr(cfg.ui, "plot_theme_mode", UiConfig.plot_theme_mode),
        },
        "updates": {
            "repo": getattr(getattr(cfg, "updates", UpdatesConfig()), "repo", UpdatesConfig.repo),
            "check_on_start": bool(getattr(getattr(cfg, "updates", UpdatesConfig()), "check_on_start", True)),
            "auto_install": bool(getattr(getattr(cfg, "updates", UpdatesConfig()), "auto_install", False)),
        },

        "demo": {
            "enabled": bool(getattr(cfg.demo, "enabled", False)),
            "seed": int(getattr(cfg.demo, "seed", 1234)),
            "scenario": str(getattr(cfg.demo, "scenario", "household") or "household"),
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
                "action_webhook": getattr(r, "action_webhook", False),
                "action_email": getattr(r, "action_email", False),
                "message": r.message,
            }
            for r in (getattr(cfg, "alerts", []) or [])
        ],
        "solar": {
            "enabled": bool(getattr(cfg.solar, "enabled", False)),
            "pv_meter_device_key": str(getattr(cfg.solar, "pv_meter_device_key", "") or ""),
            "feed_in_tariff_eur_per_kwh": float(getattr(cfg.solar, "feed_in_tariff_eur_per_kwh", 0.082)),
            "kw_peak": float(getattr(cfg.solar, "kw_peak", 0.0)),
            "battery_kwh": float(getattr(cfg.solar, "battery_kwh", 0.0)),
            "co2_production_kg_per_kwp": float(getattr(cfg.solar, "co2_production_kg_per_kwp", 1000.0)),
        },
        "pricing": {
            "electricity_price_eur_per_kwh": cfg.pricing.electricity_price_eur_per_kwh,
            "base_fee_eur_per_year": cfg.pricing.base_fee_eur_per_year,
            "base_fee_includes_vat": cfg.pricing.base_fee_includes_vat,
            "price_includes_vat": cfg.pricing.price_includes_vat,
            "vat_enabled": cfg.pricing.vat_enabled,
            "vat_rate_percent": cfg.pricing.vat_rate_percent,
            "co2_intensity_g_per_kwh": getattr(cfg.pricing, "co2_intensity_g_per_kwh", 380.0),
        },
        "tou": {
            "enabled": bool(getattr(cfg.tou, "enabled", False)),
            "rates": [
                {
                    "name": r.name,
                    "price_eur_per_kwh": r.price_eur_per_kwh,
                    "start_hour": r.start_hour,
                    "end_hour": r.end_hour,
                    "weekdays_only": r.weekdays_only,
                }
                for r in (getattr(cfg.tou, "rates", []) or [])
            ],
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
            "invoice_logo_path": cfg.billing.invoice_logo_path,
        },
        "groups": [
            {
                "name": g.name,
                "device_keys": list(g.device_keys),
            }
            for g in (getattr(cfg, "groups", []) or [])
        ],
        "schedules": [
            {
                "schedule_id": s.schedule_id,
                "device_key": s.device_key,
                "name": s.name,
                "time_on": s.time_on,
                "time_off": s.time_off,
                "weekdays": list(s.weekdays),
                "enabled": s.enabled,
                "switch_id": s.switch_id,
                "shelly_id_on": s.shelly_id_on,
                "shelly_id_off": s.shelly_id_off,
            }
            for s in (getattr(cfg, "schedules", []) or [])
        ],
        "anomaly": {
            "enabled": bool(getattr(cfg.anomaly, "enabled", False)),
            "sigma_threshold": float(getattr(cfg.anomaly, "sigma_threshold", 2.0)),
            "min_deviation_kwh": float(getattr(cfg.anomaly, "min_deviation_kwh", 0.1)),
            "window_days": int(getattr(cfg.anomaly, "window_days", 30)),
            "check_unusual_daily": bool(getattr(cfg.anomaly, "check_unusual_daily", True)),
            "check_night_consumption": bool(getattr(cfg.anomaly, "check_night_consumption", True)),
            "check_power_peak_time": bool(getattr(cfg.anomaly, "check_power_peak_time", True)),
            "action_telegram": bool(getattr(cfg.anomaly, "action_telegram", False)),
            "action_webhook": bool(getattr(cfg.anomaly, "action_webhook", False)),
            "action_email": bool(getattr(cfg.anomaly, "action_email", False)),
            "max_history": int(getattr(cfg.anomaly, "max_history", 200)),
        },
        "co2": {
            "enabled": bool(getattr(cfg.co2, "enabled", False)),
            "entso_e_api_token": str(getattr(cfg.co2, "entso_e_api_token", "") or ""),
            "bidding_zone": str(getattr(cfg.co2, "bidding_zone", "DE_LU") or "DE_LU"),
            "fetch_interval_hours": int(getattr(cfg.co2, "fetch_interval_hours", 1)),
            "backfill_days": int(getattr(cfg.co2, "backfill_days", 7)),
            "show_green_dirty_hours": bool(getattr(cfg.co2, "show_green_dirty_hours", True)),
            "green_threshold_g_per_kwh": float(getattr(cfg.co2, "green_threshold_g_per_kwh", 150.0)),
            "dirty_threshold_g_per_kwh": float(getattr(cfg.co2, "dirty_threshold_g_per_kwh", 400.0)),
        },
    }
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return path