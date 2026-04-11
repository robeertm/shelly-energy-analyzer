from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from shelly_analyzer import __version__


@dataclass(frozen=True)
class TariffPeriod:
    """A future tariff period with a start date.

    When the current date >= start_date, the prices in this period override
    the base PricingConfig values.
    """
    start_date: str = ""  # ISO "YYYY-MM-DD"
    electricity_price_eur_per_kwh: float = 0.3265
    base_fee_eur_per_year: float = 127.51


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
    # HTTP auth for password-protected devices.
    # Gen2+ uses Digest auth (admin user), Gen1 uses Basic. Empty password = no auth.
    username: str = "admin"
    password: str = ""


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

    # Desktop theme: "auto" | "light" | "dark"
    theme: str = "auto"

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

    # SSL/HTTPS mode for the web dashboard:
    #   "auto"   = self-signed certificate (default, may cause browser warnings)
    #   "custom" = use user-provided cert/key files (e.g. Let's Encrypt)
    #   "off"    = plain HTTP (no encryption)
    live_web_ssl_mode: str = "auto"
    live_web_ssl_cert: str = ""   # path to PEM certificate file (for mode "custom")
    live_web_ssl_key: str = ""    # path to PEM private key file (for mode "custom")
    live_web_ssl_auto_renew: bool = True   # auto-renew Let's Encrypt certs via certbot
    live_web_ssl_renew_days: int = 30      # renew when fewer than N days remaining

    # iOS Widget settings
    widget_domain: str = ""       # auto-detected from SSL cert CN, e.g. "energie.example.de"
    widget_devices: str = ""      # comma-separated device keys, empty = all 3-phase devices

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
    # Optional: sub-hour granularity. When > 0, overrides autosync_interval_hours.
    autosync_interval_minutes: int = 0
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

    # Future tariff periods. Each overrides price/base_fee from its start_date onward.
    tariff_schedule: List[TariffPeriod] = field(default_factory=list)

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

    # ── Tariff schedule helpers ──────────────────────────────────────────

    def effective_price_for_date(self, dt) -> float:
        """Return electricity_price_eur_per_kwh effective on *dt* (date or datetime)."""
        from datetime import date as _date, datetime as _dt
        d = dt.date() if isinstance(dt, _dt) else dt
        for period in sorted(self.tariff_schedule, key=lambda p: p.start_date, reverse=True):
            if period.start_date:
                try:
                    if d >= _date.fromisoformat(period.start_date):
                        return period.electricity_price_eur_per_kwh
                except ValueError:
                    continue
        return self.electricity_price_eur_per_kwh

    def effective_base_fee_for_date(self, dt) -> float:
        """Return base_fee_eur_per_year effective on *dt*."""
        from datetime import date as _date, datetime as _dt
        d = dt.date() if isinstance(dt, _dt) else dt
        for period in sorted(self.tariff_schedule, key=lambda p: p.start_date, reverse=True):
            if period.start_date:
                try:
                    if d >= _date.fromisoformat(period.start_date):
                        return period.base_fee_eur_per_year
                except ValueError:
                    continue
        return self.base_fee_eur_per_year

    def effective_pricing_for_date(self, dt) -> "PricingConfig":
        """Return a PricingConfig with price/base_fee overridden for the given date.

        VAT settings and CO₂ intensity are inherited. Consumers can call
        .unit_price_gross() etc. on the result without API changes.
        """
        return PricingConfig(
            electricity_price_eur_per_kwh=self.effective_price_for_date(dt),
            base_fee_eur_per_year=self.effective_base_fee_for_date(dt),
            base_fee_includes_vat=self.base_fee_includes_vat,
            price_includes_vat=self.price_includes_vat,
            vat_enabled=self.vat_enabled,
            vat_rate_percent=self.vat_rate_percent,
            co2_intensity_g_per_kwh=self.co2_intensity_g_per_kwh,
        )



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
    issuer: BillingParty = BillingParty(name="Example Company Ltd", address_lines=["Example Street 1", "12345 Example City"], vat_id="GB000000000")
    customer: BillingParty = BillingParty(name="Customer", address_lines=["Customer Street 1", "12345 Customer City"])
    invoice_prefix: str = "INV"
    payment_terms_days: int = 14
    invoice_logo_path: str = ""


@dataclass(frozen=False)
class UpdatesConfig:
    # GitHub repository in the form "owner/repo"
    repo: str = "robeertm/shelly-energy-analyzer"
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
    # Feed-in tariff in €/kWh
    feed_in_tariff_eur_per_kwh: float = 0.082
    # Installed PV capacity in kWp (0 = unknown/not set)
    kw_peak: float = 0.0
    # Battery storage capacity in kWh (0 = no battery)
    battery_kwh: float = 0.0
    # Embodied CO₂ of PV production in kg per kWp (lifecycle, default ~1000 kg/kWp for typical Si panels)
    co2_production_kg_per_kwp: float = 1000.0
    # PV amortization: total investment cost in EUR
    investment_eur: float = 0.0
    # Year of PV installation (for amortization timeline)
    installation_year: int = 0
    # Annual degradation rate in % (typical: 0.5%)
    degradation_pct: float = 0.5


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
    # Automatic detection interval in minutes (0 = disabled)
    auto_interval_minutes: int = 15
    # Maximum entries kept in the in-memory + persisted history
    max_history: int = 200


@dataclass(frozen=True)
class Co2Config:
    """Grid CO₂ intensity integration settings.

    Two providers are supported:

    - **ENTSO-E** (EU only, zones use underscore form like ``DE_LU``).
      Requires a free API token from transparency.entsoe.eu.
    - **Electricity Maps** (global, zones use hyphen form like ``US-CAL-CISO``,
      ``JP-TK``). Requires a free API key from electricitymap.org.

    The fetch service dispatches to Electricity Maps automatically when
    the selected zone does not contain an underscore AND an Electricity
    Maps API key is configured; otherwise it uses ENTSO-E.
    """
    enabled: bool = False
    entso_e_api_token: str = ""
    # Electricity Maps API key (free tier at https://api.electricitymap.org)
    electricity_maps_api_key: str = ""
    # Bidding / Electricity-Maps zone. EU default uses the ENTSO-E format.
    bidding_zone: str = "DE_LU"
    # How often to fetch new data from the ENTSO-E API (hours)
    fetch_interval_hours: int = 1
    # Deprecated: kept for backward compat with existing config.json files.
    # The service now auto-detects the range from oldest measurement data.
    backfill_days: int = 7
    # Whether to highlight green / dirty hours in the 24h heatmap
    show_green_dirty_hours: bool = True
    # g CO₂/kWh threshold for "green" grid hours
    green_threshold_g_per_kwh: float = 150.0
    # g CO₂/kWh threshold for "dirty" grid hours
    dirty_threshold_g_per_kwh: float = 400.0
    # Enable cross-border flow adjustment (uses A11 physical flows + A65 load)
    cross_border_flows: bool = False


@dataclass(frozen=True)
class SpotPriceConfig:
    """Dynamic spot market electricity price settings."""
    enabled: bool = False
    # Primary API: "energy_charts" (EU, 15-min from Oct 2025), "awattar" (DE/AT,
    # hourly from 2015), "eia" (US wholesale daily, needs API key), or "aemo"
    # (Australian NEM, 30-min rolling window, no key).
    primary_api: str = "energy_charts"
    # Bidding zone. Format depends on provider: "DE-LU", "AT", ... for EU,
    # "US-CAL" / "US-TEX" / ... for the US, "AU-NSW" / "AU-VIC" / ... for AU.
    bidding_zone: str = "DE-LU"
    # Free API key registered at https://www.eia.gov/opendata/ — required
    # only when a US- prefixed zone is selected.
    eia_api_key: str = ""
    # How often to fetch new prices (hours)
    fetch_interval_hours: int = 1
    # Detailed markup breakdown (all in ct/kWh, net, excl. VAT)
    # These sum up to the total surcharge added on top of the EPEX Spot wholesale price.
    grid_fee_ct: float = 8.50          # Netzentgelte (varies by region, 6-13 ct/kWh)
    electricity_tax_ct: float = 2.05   # Stromsteuer (§3 StromStG, fixed by law)
    concession_fee_ct: float = 1.66    # Konzessionsabgabe (varies by municipality size)
    kwk_surcharge_ct: float = 0.277    # KWK-Aufschlag (annual, set by TSOs)
    sec19_surcharge_ct: float = 0.643  # §19 StromNEV-Umlage (annual)
    offshore_surcharge_ct: float = 0.656  # Offshore-Netzumlage (2025, annual)
    supplier_margin_ct: float = 1.50   # Anbieter-Marge (Tibber ~1, Ostrom ~2, 1Komma5° ~1)
    # Legacy single-value field (ignored if any breakdown field is explicitly set; kept for backward compat)
    markup_ct_per_kwh: float = 0.0
    # Whether to apply VAT on top of (spot price + markup)
    include_vat: bool = True
    # Show dynamic price comparison even if user has a fixed tariff
    show_as_comparison: bool = True
    # Tariff type: "fixed" = fixed tariff (dynamic as comparison only),
    # "dynamic" = dynamic spot tariff is the PRIMARY billing method
    tariff_type: str = "fixed"

    def total_markup_ct(self) -> float:
        """Sum of all surcharge components in ct/kWh (net)."""
        return (self.grid_fee_ct + self.electricity_tax_ct + self.concession_fee_ct
                + self.kwk_surcharge_ct + self.sec19_surcharge_ct
                + self.offshore_surcharge_ct + self.supplier_margin_ct)


@dataclass(frozen=True)
class ForecastConfig:
    """Consumption forecasting settings."""
    enabled: bool = False
    horizon_days: int = 30
    history_days: int = 90


@dataclass(frozen=True)
class WeatherConfig:
    """OpenWeatherMap integration for weather–energy correlation."""
    enabled: bool = False
    api_key: str = ""
    city: str = ""
    lat: float = 0.0
    lon: float = 0.0
    fetch_interval_minutes: int = 30


@dataclass(frozen=True)
class MqttConfig:
    """MQTT publisher for Home Assistant integration."""
    enabled: bool = False
    broker: str = "127.0.0.1"
    port: int = 1883
    username: str = ""
    password: str = ""
    topic_prefix: str = "shelly_analyzer"
    ha_discovery: bool = True
    ha_discovery_prefix: str = "homeassistant"
    publish_interval_seconds: float = 10.0
    use_tls: bool = False


@dataclass(frozen=True)
class TenantDef:
    """A single tenant definition for utility billing."""
    tenant_id: str = ""
    name: str = ""
    device_keys: List[str] = field(default_factory=list)
    unit: str = ""
    persons: int = 1
    move_in: str = ""
    move_out: str = ""


@dataclass(frozen=True)
class TenantConfig:
    """Tenant utility billing (Nebenkostenabrechnung) settings."""
    enabled: bool = False
    tenants: List[TenantDef] = field(default_factory=list)
    common_device_keys: List[str] = field(default_factory=list)
    billing_period_months: int = 12


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
class SmartScheduleConfig:
    """Smart scheduling based on spot prices."""
    enabled: bool = False
    default_duration_hours: float = 3.0
    auto_schedule_enabled: bool = False


@dataclass(frozen=True)
class PvSurplusConsumer:
    """A relay consumer for PV surplus control."""
    device_key: str = ""
    switch_id: int = 0
    priority: int = 1
    min_power_w: float = 500.0
    name: str = ""


@dataclass(frozen=True)
class PvSurplusConfig:
    """PV surplus control: auto-switch relays on solar excess."""
    enabled: bool = False
    on_threshold_w: float = 500.0
    off_threshold_w: float = 200.0
    debounce_seconds: int = 30
    consumers: List[PvSurplusConsumer] = field(default_factory=list)


@dataclass(frozen=True)
class EvChargingConfig:
    """EV charging session detection and logging."""
    enabled: bool = False
    wallbox_device_key: str = ""
    detection_threshold_w: float = 1500.0
    min_session_minutes: int = 5


@dataclass(frozen=True)
class TariffTemplate:
    """A tariff template for comparison."""
    name: str = ""
    provider: str = ""
    tariff_type: str = "fixed"  # fixed | tou | spot
    price_eur_per_kwh: float = 0.30
    base_fee_eur_per_year: float = 100.0
    # For TOU tariffs
    ht_price: float = 0.35
    nt_price: float = 0.22
    ht_start: int = 6
    ht_end: int = 22
    # For spot tariffs
    spot_markup_ct: float = 15.0


@dataclass(frozen=True)
class TariffCompareConfig:
    """Tariff comparison settings."""
    enabled: bool = False
    custom_tariffs: List[TariffTemplate] = field(default_factory=list)


@dataclass(frozen=True)
class BatteryConfig:
    """Battery storage monitoring settings."""
    enabled: bool = False
    device_key: str = ""
    capacity_kwh: float = 10.0
    max_charge_rate_kw: float = 5.0
    max_discharge_rate_kw: float = 5.0
    efficiency_pct: float = 95.0


@dataclass(frozen=True)
class InfluxDBConfig:
    """InfluxDB time-series export."""
    enabled: bool = False
    url: str = "http://127.0.0.1:8086"
    token: str = ""
    org: str = ""
    bucket: str = "shelly"
    measurement: str = "energy"
    push_interval_seconds: int = 60
    version: int = 2  # 1 or 2


@dataclass(frozen=True)
class PrometheusConfig:
    """Prometheus metrics endpoint."""
    enabled: bool = False
    port: int = 9090
    path: str = "/metrics"


@dataclass(frozen=True)
class ApiConfig:
    """REST API v1 settings."""
    enabled: bool = False
    api_key: str = ""
    cors_allowed_origins: str = "*"
    rate_limit_per_minute: int = 60


@dataclass(frozen=True)
class AdvisorConfig:
    """AI Energy Advisor settings."""
    enabled: bool = False
    use_llm: bool = False
    llm_provider: str = "ollama"  # ollama | openai | anthropic
    llm_model: str = "llama3"
    ollama_url: str = "http://127.0.0.1:11434"
    openai_api_key: str = ""
    anthropic_api_key: str = ""


@dataclass(frozen=True)
class GamificationConfig:
    """Gamification / goals and badges settings."""
    enabled: bool = False
    weekly_goal_kwh: float = 0.0  # 0 = auto from last 4 weeks avg * 0.9
    monthly_goal_kwh: float = 0.0


@dataclass(frozen=True)
class LocationDef:
    """A single location (site) definition for multi-location support."""
    location_id: str = ""
    name: str = ""
    device_keys: List[str] = field(default_factory=list)
    db_file: str = ""  # empty = default DB


@dataclass(frozen=True)
class MultiLocationConfig:
    """Multi-location support settings."""
    enabled: bool = False
    locations: List[LocationDef] = field(default_factory=list)
    active_location_id: str = ""  # empty = all / single-site mode


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
    spot_price: SpotPriceConfig = field(default_factory=SpotPriceConfig)
    forecast: ForecastConfig = field(default_factory=ForecastConfig)
    weather: WeatherConfig = field(default_factory=WeatherConfig)
    mqtt: MqttConfig = field(default_factory=MqttConfig)
    tenant: TenantConfig = field(default_factory=TenantConfig)
    schedules: List[DeviceSchedule] = field(default_factory=list)

    # New feature configs
    smart_schedule: SmartScheduleConfig = field(default_factory=SmartScheduleConfig)
    pv_surplus: PvSurplusConfig = field(default_factory=PvSurplusConfig)
    ev_charging: EvChargingConfig = field(default_factory=EvChargingConfig)
    tariff_compare: TariffCompareConfig = field(default_factory=TariffCompareConfig)
    battery: BatteryConfig = field(default_factory=BatteryConfig)
    influxdb: InfluxDBConfig = field(default_factory=InfluxDBConfig)
    prometheus: PrometheusConfig = field(default_factory=PrometheusConfig)
    api: ApiConfig = field(default_factory=ApiConfig)
    advisor: AdvisorConfig = field(default_factory=AdvisorConfig)
    gamification: GamificationConfig = field(default_factory=GamificationConfig)
    multi_location: MultiLocationConfig = field(default_factory=MultiLocationConfig)


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
        username = str(d.get("username", "admin") or "admin")
        password = str(d.get("password", "") or "")
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
                username=username,
                password=password,
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
        theme=str(ui_raw.get("theme", UiConfig.theme) or "auto"),
        live_retention_minutes=_coerce_int(ui_raw.get("live_retention_minutes", UiConfig.live_retention_minutes), UiConfig.live_retention_minutes),
        live_window_minutes=_coerce_int(ui_raw.get("live_window_minutes", UiConfig.live_window_minutes), UiConfig.live_window_minutes),
        live_web_enabled=bool(ui_raw.get("live_web_enabled", UiConfig.live_web_enabled)),
        live_web_port=_coerce_int(ui_raw.get("live_web_port", UiConfig.live_web_port), UiConfig.live_web_port),
        live_web_refresh_seconds=_coerce_float(ui_raw.get("live_web_refresh_seconds", UiConfig.live_web_refresh_seconds), UiConfig.live_web_refresh_seconds),
        live_web_ssl_mode=str(ui_raw.get("live_web_ssl_mode", UiConfig.live_web_ssl_mode) or "auto"),
        live_web_ssl_cert=str(ui_raw.get("live_web_ssl_cert", UiConfig.live_web_ssl_cert) or ""),
        live_web_ssl_key=str(ui_raw.get("live_web_ssl_key", UiConfig.live_web_ssl_key) or ""),
        live_web_ssl_auto_renew=bool(ui_raw.get("live_web_ssl_auto_renew", UiConfig.live_web_ssl_auto_renew)),
        live_web_ssl_renew_days=_coerce_int(ui_raw.get("live_web_ssl_renew_days", UiConfig.live_web_ssl_renew_days), UiConfig.live_web_ssl_renew_days),
        widget_domain=str(ui_raw.get("widget_domain", UiConfig.widget_domain) or ""),
        widget_devices=str(ui_raw.get("widget_devices", UiConfig.widget_devices) or ""),
        live_web_token=str(ui_raw.get("live_web_token", UiConfig.live_web_token) or ""),
        live_smoothing_enabled=bool(ui_raw.get("live_smoothing_enabled", UiConfig.live_smoothing_enabled)),
        live_smoothing_seconds=_coerce_int(ui_raw.get("live_smoothing_seconds", UiConfig.live_smoothing_seconds), UiConfig.live_smoothing_seconds),
        device_page_index=_coerce_int(ui_raw.get("device_page_index", UiConfig.device_page_index), UiConfig.device_page_index),
        selected_view_type=str(ui_raw.get("selected_view_type", UiConfig.selected_view_type) or "page"),
        selected_view_group=str(ui_raw.get("selected_view_group", UiConfig.selected_view_group) or ""),
        autosync_enabled=bool(ui_raw.get("autosync_enabled", UiConfig.autosync_enabled)),
        autosync_interval_hours=_coerce_int(ui_raw.get("autosync_interval_hours", UiConfig.autosync_interval_hours), UiConfig.autosync_interval_hours),
        autosync_interval_minutes=_coerce_int(ui_raw.get("autosync_interval_minutes", UiConfig.autosync_interval_minutes), UiConfig.autosync_interval_minutes),
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
        tariff_schedule=[
            TariffPeriod(
                start_date=str(tp.get("start_date", "") or ""),
                electricity_price_eur_per_kwh=_coerce_float(
                    tp.get("electricity_price_eur_per_kwh", PricingConfig.electricity_price_eur_per_kwh),
                    PricingConfig.electricity_price_eur_per_kwh),
                base_fee_eur_per_year=_coerce_float(
                    tp.get("base_fee_eur_per_year", PricingConfig.base_fee_eur_per_year),
                    PricingConfig.base_fee_eur_per_year),
            )
            for tp in (pricing_raw.get("tariff_schedule") or [])
            if isinstance(tp, dict)
        ],
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
        investment_eur=_coerce_float(solar_raw.get("investment_eur", SolarConfig.investment_eur), SolarConfig.investment_eur),
        installation_year=_coerce_int(solar_raw.get("installation_year", SolarConfig.installation_year), SolarConfig.installation_year),
        degradation_pct=_coerce_float(solar_raw.get("degradation_pct", SolarConfig.degradation_pct), SolarConfig.degradation_pct),
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
        auto_interval_minutes=_coerce_int(anomaly_raw.get("auto_interval_minutes", AnomalyConfig.auto_interval_minutes), AnomalyConfig.auto_interval_minutes),
        max_history=_coerce_int(anomaly_raw.get("max_history", AnomalyConfig.max_history), AnomalyConfig.max_history),
    )

    co2_raw = raw.get("co2", {}) if isinstance(raw.get("co2"), dict) else {}
    co2 = Co2Config(
        enabled=bool(co2_raw.get("enabled", Co2Config.enabled)),
        entso_e_api_token=str(co2_raw.get("entso_e_api_token", Co2Config.entso_e_api_token) or ""),
        electricity_maps_api_key=str(co2_raw.get("electricity_maps_api_key", Co2Config.electricity_maps_api_key) or ""),
        bidding_zone=str(co2_raw.get("bidding_zone", Co2Config.bidding_zone) or "DE_LU"),
        fetch_interval_hours=_coerce_int(co2_raw.get("fetch_interval_hours", Co2Config.fetch_interval_hours), Co2Config.fetch_interval_hours),
        backfill_days=_coerce_int(co2_raw.get("backfill_days", Co2Config.backfill_days), Co2Config.backfill_days),
        show_green_dirty_hours=bool(co2_raw.get("show_green_dirty_hours", Co2Config.show_green_dirty_hours)),
        green_threshold_g_per_kwh=_coerce_float(co2_raw.get("green_threshold_g_per_kwh", Co2Config.green_threshold_g_per_kwh), Co2Config.green_threshold_g_per_kwh),
        dirty_threshold_g_per_kwh=_coerce_float(co2_raw.get("dirty_threshold_g_per_kwh", Co2Config.dirty_threshold_g_per_kwh), Co2Config.dirty_threshold_g_per_kwh),
        cross_border_flows=bool(co2_raw.get("cross_border_flows", Co2Config.cross_border_flows)),
    )

    spot_raw = raw.get("spot_price", {}) if isinstance(raw.get("spot_price"), dict) else {}
    # Migrate legacy single markup_ct_per_kwh: if breakdown fields are absent but markup exists, distribute
    _legacy_markup = _coerce_float(spot_raw.get("markup_ct_per_kwh", 0.0), 0.0)
    _has_breakdown = any(k in spot_raw for k in ("grid_fee_ct", "electricity_tax_ct", "supplier_margin_ct"))
    spot_price = SpotPriceConfig(
        enabled=bool(spot_raw.get("enabled", SpotPriceConfig.enabled)),
        primary_api=str(spot_raw.get("primary_api", SpotPriceConfig.primary_api) or "energy_charts"),
        bidding_zone=str(spot_raw.get("bidding_zone", SpotPriceConfig.bidding_zone) or "DE-LU"),
        eia_api_key=str(spot_raw.get("eia_api_key", SpotPriceConfig.eia_api_key) or ""),
        fetch_interval_hours=_coerce_int(spot_raw.get("fetch_interval_hours", SpotPriceConfig.fetch_interval_hours), SpotPriceConfig.fetch_interval_hours),
        grid_fee_ct=_coerce_float(spot_raw.get("grid_fee_ct", SpotPriceConfig.grid_fee_ct if _has_breakdown else (max(0, _legacy_markup - 7.5) if _legacy_markup > 0 else SpotPriceConfig.grid_fee_ct)), SpotPriceConfig.grid_fee_ct),
        electricity_tax_ct=_coerce_float(spot_raw.get("electricity_tax_ct", SpotPriceConfig.electricity_tax_ct), SpotPriceConfig.electricity_tax_ct),
        concession_fee_ct=_coerce_float(spot_raw.get("concession_fee_ct", SpotPriceConfig.concession_fee_ct), SpotPriceConfig.concession_fee_ct),
        kwk_surcharge_ct=_coerce_float(spot_raw.get("kwk_surcharge_ct", SpotPriceConfig.kwk_surcharge_ct), SpotPriceConfig.kwk_surcharge_ct),
        sec19_surcharge_ct=_coerce_float(spot_raw.get("sec19_surcharge_ct", SpotPriceConfig.sec19_surcharge_ct), SpotPriceConfig.sec19_surcharge_ct),
        offshore_surcharge_ct=_coerce_float(spot_raw.get("offshore_surcharge_ct", SpotPriceConfig.offshore_surcharge_ct), SpotPriceConfig.offshore_surcharge_ct),
        supplier_margin_ct=_coerce_float(spot_raw.get("supplier_margin_ct", SpotPriceConfig.supplier_margin_ct), SpotPriceConfig.supplier_margin_ct),
        markup_ct_per_kwh=_legacy_markup,
        include_vat=bool(spot_raw.get("include_vat", SpotPriceConfig.include_vat)),
        show_as_comparison=bool(spot_raw.get("show_as_comparison", SpotPriceConfig.show_as_comparison)),
        tariff_type=str(spot_raw.get("tariff_type", SpotPriceConfig.tariff_type) or "fixed"),
    )

    forecast_raw = raw.get("forecast", {}) if isinstance(raw.get("forecast"), dict) else {}
    forecast = ForecastConfig(
        enabled=bool(forecast_raw.get("enabled", ForecastConfig.enabled)),
        horizon_days=_coerce_int(forecast_raw.get("horizon_days", ForecastConfig.horizon_days), ForecastConfig.horizon_days),
        history_days=_coerce_int(forecast_raw.get("history_days", ForecastConfig.history_days), ForecastConfig.history_days),
    )

    weather_raw = raw.get("weather", {}) if isinstance(raw.get("weather"), dict) else {}
    weather = WeatherConfig(
        enabled=bool(weather_raw.get("enabled", WeatherConfig.enabled)),
        api_key=str(weather_raw.get("api_key", WeatherConfig.api_key) or ""),
        city=str(weather_raw.get("city", WeatherConfig.city) or ""),
        lat=_coerce_float(weather_raw.get("lat", WeatherConfig.lat), WeatherConfig.lat),
        lon=_coerce_float(weather_raw.get("lon", WeatherConfig.lon), WeatherConfig.lon),
        fetch_interval_minutes=_coerce_int(weather_raw.get("fetch_interval_minutes", WeatherConfig.fetch_interval_minutes), WeatherConfig.fetch_interval_minutes),
    )

    mqtt_raw = raw.get("mqtt", {}) if isinstance(raw.get("mqtt"), dict) else {}
    mqtt_cfg = MqttConfig(
        enabled=bool(mqtt_raw.get("enabled", MqttConfig.enabled)),
        broker=str(mqtt_raw.get("broker", MqttConfig.broker) or "127.0.0.1"),
        port=_coerce_int(mqtt_raw.get("port", MqttConfig.port), MqttConfig.port),
        username=str(mqtt_raw.get("username", MqttConfig.username) or ""),
        password=str(mqtt_raw.get("password", MqttConfig.password) or ""),
        topic_prefix=str(mqtt_raw.get("topic_prefix", MqttConfig.topic_prefix) or "shelly_analyzer"),
        ha_discovery=bool(mqtt_raw.get("ha_discovery", MqttConfig.ha_discovery)),
        ha_discovery_prefix=str(mqtt_raw.get("ha_discovery_prefix", MqttConfig.ha_discovery_prefix) or "homeassistant"),
        publish_interval_seconds=_coerce_float(mqtt_raw.get("publish_interval_seconds", MqttConfig.publish_interval_seconds), MqttConfig.publish_interval_seconds),
        use_tls=bool(mqtt_raw.get("use_tls", MqttConfig.use_tls)),
    )

    tenant_raw = raw.get("tenant", {}) if isinstance(raw.get("tenant"), dict) else {}
    tenant_tenants: List[TenantDef] = []
    for t in (tenant_raw.get("tenants", []) if isinstance(tenant_raw.get("tenants"), list) else []):
        if not isinstance(t, dict):
            continue
        dkeys = t.get("device_keys", [])
        if not isinstance(dkeys, list):
            dkeys = []
        tenant_tenants.append(TenantDef(
            tenant_id=str(t.get("tenant_id", "") or ""),
            name=str(t.get("name", "") or ""),
            device_keys=[str(k) for k in dkeys],
            unit=str(t.get("unit", "") or ""),
            persons=_coerce_int(t.get("persons", 1), 1),
            move_in=str(t.get("move_in", "") or ""),
            move_out=str(t.get("move_out", "") or ""),
        ))
    common_keys = tenant_raw.get("common_device_keys", [])
    if not isinstance(common_keys, list):
        common_keys = []
    tenant = TenantConfig(
        enabled=bool(tenant_raw.get("enabled", TenantConfig.enabled)),
        tenants=tenant_tenants,
        common_device_keys=[str(k) for k in common_keys],
        billing_period_months=_coerce_int(tenant_raw.get("billing_period_months", TenantConfig.billing_period_months), TenantConfig.billing_period_months),
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

    # ── New feature configs ──────────────────────────────────────────────
    ss_raw = raw.get("smart_schedule", {}) if isinstance(raw.get("smart_schedule"), dict) else {}
    smart_schedule = SmartScheduleConfig(
        enabled=bool(ss_raw.get("enabled", False)),
        default_duration_hours=_coerce_float(ss_raw.get("default_duration_hours", 3.0), 3.0),
        auto_schedule_enabled=bool(ss_raw.get("auto_schedule_enabled", False)),
    )

    pvs_raw = raw.get("pv_surplus", {}) if isinstance(raw.get("pv_surplus"), dict) else {}
    pvs_consumers: List[PvSurplusConsumer] = []
    for c in (pvs_raw.get("consumers", []) if isinstance(pvs_raw.get("consumers"), list) else []):
        if isinstance(c, dict):
            pvs_consumers.append(PvSurplusConsumer(
                device_key=str(c.get("device_key", "") or ""),
                switch_id=_coerce_int(c.get("switch_id", 0), 0),
                priority=_coerce_int(c.get("priority", 1), 1),
                min_power_w=_coerce_float(c.get("min_power_w", 500.0), 500.0),
                name=str(c.get("name", "") or ""),
            ))
    pv_surplus = PvSurplusConfig(
        enabled=bool(pvs_raw.get("enabled", False)),
        on_threshold_w=_coerce_float(pvs_raw.get("on_threshold_w", 500.0), 500.0),
        off_threshold_w=_coerce_float(pvs_raw.get("off_threshold_w", 200.0), 200.0),
        debounce_seconds=_coerce_int(pvs_raw.get("debounce_seconds", 30), 30),
        consumers=pvs_consumers,
    )

    evc_raw = raw.get("ev_charging", {}) if isinstance(raw.get("ev_charging"), dict) else {}
    ev_charging = EvChargingConfig(
        enabled=bool(evc_raw.get("enabled", False)),
        wallbox_device_key=str(evc_raw.get("wallbox_device_key", "") or ""),
        detection_threshold_w=_coerce_float(evc_raw.get("detection_threshold_w", 1500.0), 1500.0),
        min_session_minutes=_coerce_int(evc_raw.get("min_session_minutes", 5), 5),
    )

    tc_raw = raw.get("tariff_compare", {}) if isinstance(raw.get("tariff_compare"), dict) else {}
    tc_tariffs: List[TariffTemplate] = []
    for tt in (tc_raw.get("custom_tariffs", []) if isinstance(tc_raw.get("custom_tariffs"), list) else []):
        if isinstance(tt, dict):
            tc_tariffs.append(TariffTemplate(
                name=str(tt.get("name", "") or ""),
                provider=str(tt.get("provider", "") or ""),
                tariff_type=str(tt.get("tariff_type", "fixed") or "fixed"),
                price_eur_per_kwh=_coerce_float(tt.get("price_eur_per_kwh", 0.30), 0.30),
                base_fee_eur_per_year=_coerce_float(tt.get("base_fee_eur_per_year", 100.0), 100.0),
                ht_price=_coerce_float(tt.get("ht_price", 0.35), 0.35),
                nt_price=_coerce_float(tt.get("nt_price", 0.22), 0.22),
                ht_start=_coerce_int(tt.get("ht_start", 6), 6),
                ht_end=_coerce_int(tt.get("ht_end", 22), 22),
                spot_markup_ct=_coerce_float(tt.get("spot_markup_ct", 15.0), 15.0),
            ))
    tariff_compare = TariffCompareConfig(
        enabled=bool(tc_raw.get("enabled", False)),
        custom_tariffs=tc_tariffs,
    )

    bat_raw = raw.get("battery", {}) if isinstance(raw.get("battery"), dict) else {}
    battery_cfg = BatteryConfig(
        enabled=bool(bat_raw.get("enabled", False)),
        device_key=str(bat_raw.get("device_key", "") or ""),
        capacity_kwh=_coerce_float(bat_raw.get("capacity_kwh", 10.0), 10.0),
        max_charge_rate_kw=_coerce_float(bat_raw.get("max_charge_rate_kw", 5.0), 5.0),
        max_discharge_rate_kw=_coerce_float(bat_raw.get("max_discharge_rate_kw", 5.0), 5.0),
        efficiency_pct=_coerce_float(bat_raw.get("efficiency_pct", 95.0), 95.0),
    )

    idb_raw = raw.get("influxdb", {}) if isinstance(raw.get("influxdb"), dict) else {}
    influxdb_cfg = InfluxDBConfig(
        enabled=bool(idb_raw.get("enabled", False)),
        url=str(idb_raw.get("url", "http://127.0.0.1:8086") or "http://127.0.0.1:8086"),
        token=str(idb_raw.get("token", "") or ""),
        org=str(idb_raw.get("org", "") or ""),
        bucket=str(idb_raw.get("bucket", "shelly") or "shelly"),
        measurement=str(idb_raw.get("measurement", "energy") or "energy"),
        push_interval_seconds=_coerce_int(idb_raw.get("push_interval_seconds", 60), 60),
        version=_coerce_int(idb_raw.get("version", 2), 2),
    )

    prom_raw = raw.get("prometheus", {}) if isinstance(raw.get("prometheus"), dict) else {}
    prometheus_cfg = PrometheusConfig(
        enabled=bool(prom_raw.get("enabled", False)),
        port=_coerce_int(prom_raw.get("port", 9090), 9090),
        path=str(prom_raw.get("path", "/metrics") or "/metrics"),
    )

    api_raw = raw.get("api", {}) if isinstance(raw.get("api"), dict) else {}
    api_cfg = ApiConfig(
        enabled=bool(api_raw.get("enabled", False)),
        api_key=str(api_raw.get("api_key", "") or ""),
        cors_allowed_origins=str(api_raw.get("cors_allowed_origins", "*") or "*"),
        rate_limit_per_minute=_coerce_int(api_raw.get("rate_limit_per_minute", 60), 60),
    )

    adv_raw = raw.get("advisor", {}) if isinstance(raw.get("advisor"), dict) else {}
    advisor_cfg = AdvisorConfig(
        enabled=bool(adv_raw.get("enabled", False)),
        use_llm=bool(adv_raw.get("use_llm", False)),
        llm_provider=str(adv_raw.get("llm_provider", "ollama") or "ollama"),
        llm_model=str(adv_raw.get("llm_model", "llama3") or "llama3"),
        ollama_url=str(adv_raw.get("ollama_url", "http://127.0.0.1:11434") or "http://127.0.0.1:11434"),
        openai_api_key=str(adv_raw.get("openai_api_key", "") or ""),
        anthropic_api_key=str(adv_raw.get("anthropic_api_key", "") or ""),
    )

    gam_raw = raw.get("gamification", {}) if isinstance(raw.get("gamification"), dict) else {}
    gamification_cfg = GamificationConfig(
        enabled=bool(gam_raw.get("enabled", False)),
        weekly_goal_kwh=_coerce_float(gam_raw.get("weekly_goal_kwh", 0.0), 0.0),
        monthly_goal_kwh=_coerce_float(gam_raw.get("monthly_goal_kwh", 0.0), 0.0),
    )

    ml_raw = raw.get("multi_location", {}) if isinstance(raw.get("multi_location"), dict) else {}
    ml_locations: List[LocationDef] = []
    for loc in (ml_raw.get("locations", []) if isinstance(ml_raw.get("locations"), list) else []):
        if isinstance(loc, dict):
            lkeys = loc.get("device_keys", [])
            if not isinstance(lkeys, list):
                lkeys = []
            ml_locations.append(LocationDef(
                location_id=str(loc.get("location_id", "") or ""),
                name=str(loc.get("name", "") or ""),
                device_keys=[str(k) for k in lkeys],
                db_file=str(loc.get("db_file", "") or ""),
            ))
    multi_location_cfg = MultiLocationConfig(
        enabled=bool(ml_raw.get("enabled", False)),
        locations=ml_locations,
        active_location_id=str(ml_raw.get("active_location_id", "") or ""),
    )

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
        spot_price=spot_price,
        forecast=forecast,
        weather=weather,
        mqtt=mqtt_cfg,
        tenant=tenant,
        groups=groups,
        schedules=schedules,
        smart_schedule=smart_schedule,
        pv_surplus=pv_surplus,
        ev_charging=ev_charging,
        tariff_compare=tariff_compare,
        battery=battery_cfg,
        influxdb=influxdb_cfg,
        prometheus=prometheus_cfg,
        api=api_cfg,
        advisor=advisor_cfg,
        gamification=gamification_cfg,
        multi_location=multi_location_cfg,
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
                "username": getattr(d, "username", "admin") or "admin",
                "password": getattr(d, "password", "") or "",
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
            "theme": getattr(cfg.ui, "theme", "auto"),
            "live_window_minutes": cfg.ui.live_window_minutes,
            "live_retention_minutes": getattr(cfg.ui, "live_retention_minutes", 120),
            "device_page_index": getattr(cfg.ui, "device_page_index", 0),
            "selected_view_type": getattr(cfg.ui, "selected_view_type", "page"),
            "selected_view_group": getattr(cfg.ui, "selected_view_group", ""),
            "autosync_enabled": cfg.ui.autosync_enabled,
            "autosync_interval_hours": cfg.ui.autosync_interval_hours,
            "autosync_interval_minutes": cfg.ui.autosync_interval_minutes,
            "autosync_mode": cfg.ui.autosync_mode,
            "live_web_enabled": cfg.ui.live_web_enabled,
            "live_web_port": cfg.ui.live_web_port,
            "live_web_refresh_seconds": cfg.ui.live_web_refresh_seconds,
            "live_web_ssl_mode": getattr(cfg.ui, "live_web_ssl_mode", "auto"),
            "live_web_ssl_cert": getattr(cfg.ui, "live_web_ssl_cert", ""),
            "live_web_ssl_key": getattr(cfg.ui, "live_web_ssl_key", ""),
            "live_web_ssl_auto_renew": getattr(cfg.ui, "live_web_ssl_auto_renew", True),
            "live_web_ssl_renew_days": getattr(cfg.ui, "live_web_ssl_renew_days", 30),
            "widget_domain": getattr(cfg.ui, "widget_domain", ""),
            "widget_devices": getattr(cfg.ui, "widget_devices", ""),
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
            "investment_eur": float(getattr(cfg.solar, "investment_eur", 0.0)),
            "installation_year": int(getattr(cfg.solar, "installation_year", 0)),
            "degradation_pct": float(getattr(cfg.solar, "degradation_pct", 0.5)),
        },
        "pricing": {
            "electricity_price_eur_per_kwh": cfg.pricing.electricity_price_eur_per_kwh,
            "base_fee_eur_per_year": cfg.pricing.base_fee_eur_per_year,
            "base_fee_includes_vat": cfg.pricing.base_fee_includes_vat,
            "price_includes_vat": cfg.pricing.price_includes_vat,
            "vat_enabled": cfg.pricing.vat_enabled,
            "vat_rate_percent": cfg.pricing.vat_rate_percent,
            "co2_intensity_g_per_kwh": getattr(cfg.pricing, "co2_intensity_g_per_kwh", 380.0),
            "tariff_schedule": [
                {
                    "start_date": tp.start_date,
                    "electricity_price_eur_per_kwh": tp.electricity_price_eur_per_kwh,
                    "base_fee_eur_per_year": tp.base_fee_eur_per_year,
                }
                for tp in (cfg.pricing.tariff_schedule or [])
            ],
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
            "auto_interval_minutes": int(getattr(cfg.anomaly, "auto_interval_minutes", 15)),
            "max_history": int(getattr(cfg.anomaly, "max_history", 200)),
        },
        "co2": {
            "enabled": bool(getattr(cfg.co2, "enabled", False)),
            "entso_e_api_token": str(getattr(cfg.co2, "entso_e_api_token", "") or ""),
            "electricity_maps_api_key": str(getattr(cfg.co2, "electricity_maps_api_key", "") or ""),
            "bidding_zone": str(getattr(cfg.co2, "bidding_zone", "DE_LU") or "DE_LU"),
            "fetch_interval_hours": int(getattr(cfg.co2, "fetch_interval_hours", 1)),
            "backfill_days": int(getattr(cfg.co2, "backfill_days", 7)),
            "show_green_dirty_hours": bool(getattr(cfg.co2, "show_green_dirty_hours", True)),
            "green_threshold_g_per_kwh": float(getattr(cfg.co2, "green_threshold_g_per_kwh", 150.0)),
            "dirty_threshold_g_per_kwh": float(getattr(cfg.co2, "dirty_threshold_g_per_kwh", 400.0)),
            "cross_border_flows": bool(getattr(cfg.co2, "cross_border_flows", False)),
        },
        "spot_price": {
            "enabled": bool(getattr(cfg.spot_price, "enabled", False)),
            "primary_api": str(getattr(cfg.spot_price, "primary_api", "energy_charts") or "energy_charts"),
            "bidding_zone": str(getattr(cfg.spot_price, "bidding_zone", "DE-LU") or "DE-LU"),
            "eia_api_key": str(getattr(cfg.spot_price, "eia_api_key", "") or ""),
            "fetch_interval_hours": int(getattr(cfg.spot_price, "fetch_interval_hours", 1)),
            "grid_fee_ct": float(getattr(cfg.spot_price, "grid_fee_ct", 8.50)),
            "electricity_tax_ct": float(getattr(cfg.spot_price, "electricity_tax_ct", 2.05)),
            "concession_fee_ct": float(getattr(cfg.spot_price, "concession_fee_ct", 1.66)),
            "kwk_surcharge_ct": float(getattr(cfg.spot_price, "kwk_surcharge_ct", 0.277)),
            "sec19_surcharge_ct": float(getattr(cfg.spot_price, "sec19_surcharge_ct", 0.643)),
            "offshore_surcharge_ct": float(getattr(cfg.spot_price, "offshore_surcharge_ct", 0.816)),
            "supplier_margin_ct": float(getattr(cfg.spot_price, "supplier_margin_ct", 2.50)),
            "include_vat": bool(getattr(cfg.spot_price, "include_vat", True)),
            "show_as_comparison": bool(getattr(cfg.spot_price, "show_as_comparison", True)),
            "tariff_type": str(getattr(cfg.spot_price, "tariff_type", "fixed") or "fixed"),
        },
        "forecast": {
            "enabled": bool(getattr(cfg.forecast, "enabled", False)),
            "horizon_days": int(getattr(cfg.forecast, "horizon_days", 30)),
            "history_days": int(getattr(cfg.forecast, "history_days", 90)),
        },
        "weather": {
            "enabled": bool(getattr(cfg.weather, "enabled", False)),
            "api_key": str(getattr(cfg.weather, "api_key", "") or ""),
            "city": str(getattr(cfg.weather, "city", "") or ""),
            "lat": float(getattr(cfg.weather, "lat", 0.0)),
            "lon": float(getattr(cfg.weather, "lon", 0.0)),
            "fetch_interval_minutes": int(getattr(cfg.weather, "fetch_interval_minutes", 30)),
        },
        "mqtt": {
            "enabled": bool(getattr(cfg.mqtt, "enabled", False)),
            "broker": str(getattr(cfg.mqtt, "broker", "localhost") or "127.0.0.1"),
            "port": int(getattr(cfg.mqtt, "port", 1883)),
            "username": str(getattr(cfg.mqtt, "username", "") or ""),
            "password": str(getattr(cfg.mqtt, "password", "") or ""),
            "topic_prefix": str(getattr(cfg.mqtt, "topic_prefix", "shelly_analyzer") or "shelly_analyzer"),
            "ha_discovery": bool(getattr(cfg.mqtt, "ha_discovery", True)),
            "ha_discovery_prefix": str(getattr(cfg.mqtt, "ha_discovery_prefix", "homeassistant") or "homeassistant"),
            "publish_interval_seconds": float(getattr(cfg.mqtt, "publish_interval_seconds", 10.0)),
            "use_tls": bool(getattr(cfg.mqtt, "use_tls", False)),
        },
        "tenant": {
            "enabled": bool(getattr(cfg.tenant, "enabled", False)),
            "tenants": [
                {
                    "tenant_id": t.tenant_id,
                    "name": t.name,
                    "device_keys": list(t.device_keys),
                    "unit": t.unit,
                    "persons": t.persons,
                    "move_in": t.move_in,
                    "move_out": t.move_out,
                }
                for t in (getattr(cfg.tenant, "tenants", []) or [])
            ],
            "common_device_keys": list(getattr(cfg.tenant, "common_device_keys", []) or []),
            "billing_period_months": int(getattr(cfg.tenant, "billing_period_months", 12)),
        },
        "smart_schedule": {
            "enabled": bool(getattr(cfg.smart_schedule, "enabled", False)),
            "default_duration_hours": float(getattr(cfg.smart_schedule, "default_duration_hours", 3.0)),
            "auto_schedule_enabled": bool(getattr(cfg.smart_schedule, "auto_schedule_enabled", False)),
        },
        "pv_surplus": {
            "enabled": bool(getattr(cfg.pv_surplus, "enabled", False)),
            "on_threshold_w": float(getattr(cfg.pv_surplus, "on_threshold_w", 500.0)),
            "off_threshold_w": float(getattr(cfg.pv_surplus, "off_threshold_w", 200.0)),
            "debounce_seconds": int(getattr(cfg.pv_surplus, "debounce_seconds", 30)),
            "consumers": [
                {
                    "device_key": c.device_key,
                    "switch_id": c.switch_id,
                    "priority": c.priority,
                    "min_power_w": c.min_power_w,
                    "name": c.name,
                }
                for c in (getattr(cfg.pv_surplus, "consumers", []) or [])
            ],
        },
        "ev_charging": {
            "enabled": bool(getattr(cfg.ev_charging, "enabled", False)),
            "wallbox_device_key": str(getattr(cfg.ev_charging, "wallbox_device_key", "") or ""),
            "detection_threshold_w": float(getattr(cfg.ev_charging, "detection_threshold_w", 1500.0)),
            "min_session_minutes": int(getattr(cfg.ev_charging, "min_session_minutes", 5)),
        },
        "tariff_compare": {
            "enabled": bool(getattr(cfg.tariff_compare, "enabled", False)),
            "custom_tariffs": [
                {
                    "name": tt.name, "provider": tt.provider, "tariff_type": tt.tariff_type,
                    "price_eur_per_kwh": tt.price_eur_per_kwh, "base_fee_eur_per_year": tt.base_fee_eur_per_year,
                    "ht_price": tt.ht_price, "nt_price": tt.nt_price,
                    "ht_start": tt.ht_start, "ht_end": tt.ht_end,
                    "spot_markup_ct": tt.spot_markup_ct,
                }
                for tt in (getattr(cfg.tariff_compare, "custom_tariffs", []) or [])
            ],
        },
        "battery": {
            "enabled": bool(getattr(cfg.battery, "enabled", False)),
            "device_key": str(getattr(cfg.battery, "device_key", "") or ""),
            "capacity_kwh": float(getattr(cfg.battery, "capacity_kwh", 10.0)),
            "max_charge_rate_kw": float(getattr(cfg.battery, "max_charge_rate_kw", 5.0)),
            "max_discharge_rate_kw": float(getattr(cfg.battery, "max_discharge_rate_kw", 5.0)),
            "efficiency_pct": float(getattr(cfg.battery, "efficiency_pct", 95.0)),
        },
        "influxdb": {
            "enabled": bool(getattr(cfg.influxdb, "enabled", False)),
            "url": str(getattr(cfg.influxdb, "url", "http://127.0.0.1:8086") or "http://127.0.0.1:8086"),
            "token": str(getattr(cfg.influxdb, "token", "") or ""),
            "org": str(getattr(cfg.influxdb, "org", "") or ""),
            "bucket": str(getattr(cfg.influxdb, "bucket", "shelly") or "shelly"),
            "measurement": str(getattr(cfg.influxdb, "measurement", "energy") or "energy"),
            "push_interval_seconds": int(getattr(cfg.influxdb, "push_interval_seconds", 60)),
            "version": int(getattr(cfg.influxdb, "version", 2)),
        },
        "prometheus": {
            "enabled": bool(getattr(cfg.prometheus, "enabled", False)),
            "port": int(getattr(cfg.prometheus, "port", 9090)),
            "path": str(getattr(cfg.prometheus, "path", "/metrics") or "/metrics"),
        },
        "api": {
            "enabled": bool(getattr(cfg.api, "enabled", False)),
            "api_key": str(getattr(cfg.api, "api_key", "") or ""),
            "cors_allowed_origins": str(getattr(cfg.api, "cors_allowed_origins", "*") or "*"),
            "rate_limit_per_minute": int(getattr(cfg.api, "rate_limit_per_minute", 60)),
        },
        "advisor": {
            "enabled": bool(getattr(cfg.advisor, "enabled", False)),
            "use_llm": bool(getattr(cfg.advisor, "use_llm", False)),
            "llm_provider": str(getattr(cfg.advisor, "llm_provider", "ollama") or "ollama"),
            "llm_model": str(getattr(cfg.advisor, "llm_model", "llama3") or "llama3"),
            "ollama_url": str(getattr(cfg.advisor, "ollama_url", "http://127.0.0.1:11434") or "http://127.0.0.1:11434"),
            "openai_api_key": str(getattr(cfg.advisor, "openai_api_key", "") or ""),
            "anthropic_api_key": str(getattr(cfg.advisor, "anthropic_api_key", "") or ""),
        },
        "gamification": {
            "enabled": bool(getattr(cfg.gamification, "enabled", False)),
            "weekly_goal_kwh": float(getattr(cfg.gamification, "weekly_goal_kwh", 0.0)),
            "monthly_goal_kwh": float(getattr(cfg.gamification, "monthly_goal_kwh", 0.0)),
        },
        "multi_location": {
            "enabled": bool(getattr(cfg.multi_location, "enabled", False)),
            "locations": [
                {
                    "location_id": loc.location_id,
                    "name": loc.name,
                    "device_keys": list(loc.device_keys),
                    "db_file": loc.db_file,
                }
                for loc in (getattr(cfg.multi_location, "locations", []) or [])
            ],
            "active_location_id": str(getattr(cfg.multi_location, "active_location_id", "") or ""),
        },
    }
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return path