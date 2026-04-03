from __future__ import annotations

# Thin composition layer: the former mega ui/app.py was split into mixins.
import tkinter as tk

from .mixins.core import CoreMixin
from .mixins.scaling import ScalingMixin
from .mixins.plots import PlotsMixin
from .mixins.liveweb import LiveWebMixin
from .mixins.updates import UpdatesMixin
from .mixins.heatmap import HeatmapMixin
from .mixins.solar import SolarMixin
from .mixins.compare import CompareMixin
from .mixins.anomaly import AnomalyMixin
from .mixins.schedule import ScheduleMixin
from .mixins.co2 import Co2Mixin
from .mixins.forecast import ForecastMixin
from .mixins.standby_tab import StandbyMixin
from .mixins.weather_tab import WeatherMixin
from .mixins.sankey_tab import SankeyMixin
from .mixins.tenant_tab import TenantMixin
from .mixins.smart_schedule_tab import SmartScheduleMixin
from .mixins.ev_log_tab import EvLogMixin
from .mixins.tariff_tab import TariffMixin
from .mixins.battery_tab import BatteryMixin
from .mixins.advisor_tab import AdvisorMixin
from .mixins.goals_tab import GoalsMixin


class App(CoreMixin, tk.Tk, ScalingMixin, PlotsMixin, LiveWebMixin, UpdatesMixin, HeatmapMixin, SolarMixin, CompareMixin, AnomalyMixin, ScheduleMixin, Co2Mixin, ForecastMixin, StandbyMixin, WeatherMixin, SankeyMixin, TenantMixin, SmartScheduleMixin, EvLogMixin, TariffMixin, BatteryMixin, AdvisorMixin, GoalsMixin):
    """Main GUI application class (composed from mixins)."""
    pass
