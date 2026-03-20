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


class App(CoreMixin, tk.Tk, ScalingMixin, PlotsMixin, LiveWebMixin, UpdatesMixin, HeatmapMixin, SolarMixin):
    """Main GUI application class (composed from mixins)."""
    pass
