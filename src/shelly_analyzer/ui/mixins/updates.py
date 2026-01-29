from __future__ import annotations
import shutil
from dataclasses import asdict
import json
import io
import math
import re
import queue
import sys
import os
import subprocess
import socket
import logging
import threading
import urllib.parse
import urllib.request
import urllib.error
import ssl
import time
from collections import deque
from datetime import datetime
from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple
import tkinter as tk
import tkinter.font as tkfont
from tkinter import ttk, filedialog, messagebox
import pandas as pd
import numpy as np
import requests
import qrcode
from PIL import Image, ImageTk
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk
from matplotlib.figure import Figure
import matplotlib.dates as mdates
from shelly_analyzer import __version__
from shelly_analyzer.i18n import t as _t, normalize_lang, LANGS, format_date_local, format_number_local
from shelly_analyzer.core.energy import filter_by_time, calculate_energy
from shelly_analyzer.core.stats import daily_kwh, weekly_kwh, monthly_kwh
from shelly_analyzer.io.config import (
    AppConfig,
    BillingConfig,
    BillingParty,
    CsvPackConfig,
    DeviceConfig,
    DownloadConfig,
    PricingConfig,
    UiConfig,
    UpdatesConfig,
    AlertRule,
    load_config,
    save_config,
)
from shelly_analyzer.io.storage import Storage
from shelly_analyzer.io.http import ShellyHttp, HttpConfig, get_shelly_status, get_switch_status, set_switch_state
from shelly_analyzer.io.logging_setup import get_log_path
from shelly_analyzer.services.compute import ComputedDevice, load_device, summarize
from shelly_analyzer.services.export import (
    ReportTotals,
    export_figure_png,
    export_pdf_invoice,
    export_pdf_summary,
    export_pdf_energy_report_variant1,
    export_to_excel,
    InvoiceLine,
)
from shelly_analyzer.services.live import LivePoller, MultiLivePoller, DemoMultiLivePoller, LiveSample
from shelly_analyzer.services.sync import sync_all
from shelly_analyzer.services.webdash import LivePoint, LiveStateStore, LiveWebDashboard
from shelly_analyzer.services.discovery import probe_device
from shelly_analyzer.services.demo import default_demo_devices, ensure_demo_csv
from shelly_analyzer.services.mdns import discover_shelly_mdns


from .._shared import _fmt_eur, _fmt_kwh, _parse_date_flexible, _period_bounds, PLOTS_MODES, AUTOSYNC_INTERVAL_OPTIONS, AUTOSYNC_MODE_OPTIONS, INVOICE_PERIOD_OPTIONS

class UpdatesMixin:
    """Auto-generated mixin extracted from the former ui/app.py to keep files smaller."""

    def _updates_repo(self) -> str:
            try:
                u = getattr(self.cfg, "updates", None)
                if u and getattr(u, "repo", None):
                    return str(getattr(u, "repo"))
            except Exception:
                pass
            return "robeertm/shelly-energy-analyzer"

    def _updates_check_on_startup(self) -> None:
            # Always check when the app opens (non-blocking). Auto-install only if user enabled it.
            try:
                self._updates_check_async(auto_install=bool(self.upd_auto.get()))
            except Exception:
                pass

    def _updates_check_async(self, auto_install: bool = False) -> None:
            """Non-blocking GitHub release check with immediate UI feedback.

            Uses a sequence id so slow/failed older requests cannot overwrite newer results.
            """
            import threading
            from types import SimpleNamespace

            # Bump sequence id for race-free UI updates
            try:
                self._upd_check_seq = int(getattr(self, "_upd_check_seq", 0)) + 1
            except Exception:
                self._upd_check_seq = 1
            seq = self._upd_check_seq

            # Immediate feedback on click (runs on UI thread)
            try:
                self._updates_set_status(self.t("updates.searching"))
            except Exception:
                pass
            try:
                if hasattr(self, "btn_upd_install"):
                    self.btn_upd_install.configure(state="disabled")
            except Exception:
                pass

            def worker() -> None:
                repo = self._updates_repo()
                try:
                    from shelly_analyzer.services.updater import check_latest_release, is_newer
                    info = check_latest_release(repo, timeout_s=10.0)
                except Exception as e:
                    info = None
                    err = str(e)

                    def apply_err():
                        if getattr(self, "_upd_check_seq", 0) != seq:
                            return
                        try:
                            self._upd_latest = None
                        except Exception:
                            pass
                        try:
                            self._updates_set_status(f"{self.t('updates.status.unreachable')} ({err})")
                        except Exception:
                            pass
                        try:
                            if hasattr(self, "btn_upd_install"):
                                self.btn_upd_install.configure(state="disabled")
                        except Exception:
                            pass

                    try:
                        self.after(0, apply_err)
                    except Exception:
                        apply_err()
                    return

                def apply_ok():
                    if getattr(self, "_upd_check_seq", 0) != seq:
                        return

                    if not info or not getattr(info, "reachable", False):
                        try:
                            self._upd_latest = None
                        except Exception:
                            pass
                        try:
                            msg = getattr(info, "status", None) or self.t("updates.status.unreachable")
                            self._updates_set_status(msg)
                        except Exception:
                            pass
                        try:
                            if hasattr(self, "btn_upd_install"):
                                self.btn_upd_install.configure(state="disabled")
                        except Exception:
                            pass
                        return

                    tag = getattr(info, "latest_tag", None) or ""
                    asset_url = getattr(info, "asset_url", None)
                    html_url = f"https://github.com/{repo}/releases/tag/{tag}" if tag else f"https://github.com/{repo}/releases/latest"

                    # Only offer install if tag is newer than current version
                    try:
                        if tag and is_newer(tag, f"v{__version__}"):
                            self._upd_latest = SimpleNamespace(tag=tag, zip_url=asset_url, html_url=html_url)
                            self._updates_set_status(self.t("updates.status.available", tag=tag))
                            try:
                                if hasattr(self, "btn_upd_install"):
                                    self.btn_upd_install.configure(state="normal" if asset_url else "disabled")
                            except Exception:
                                pass

                            if auto_install and asset_url:
                                try:
                                    self.after(0, self._updates_install_latest)
                                except Exception:
                                    self._updates_install_latest()
                        else:
                            # Not newer → disable install
                            self._upd_latest = SimpleNamespace(tag=tag, zip_url=None, html_url=html_url)
                            self._updates_set_status(self.t("updates.status.none"))
                            try:
                                if hasattr(self, "btn_upd_install"):
                                    self.btn_upd_install.configure(state="disabled")
                            except Exception:
                                pass
                    except Exception:
                        try:
                            self._upd_latest = SimpleNamespace(tag=tag, zip_url=None, html_url=html_url)
                        except Exception:
                            pass
                        try:
                            self._updates_set_status(self.t("updates.status.none"))
                        except Exception:
                            pass
                        try:
                            if hasattr(self, "btn_upd_install"):
                                self.btn_upd_install.configure(state="disabled")
                        except Exception:
                            pass

                try:
                    self.after(0, apply_ok)
                except Exception:
                    apply_ok()

            threading.Thread(target=worker, daemon=True).start()

    def _updates_set_status(self, msg: str) -> None:
            try:
                print(f"[updates] status: {msg}")
            except Exception:
                pass
            try:
                self.after(0, lambda m=msg: self.upd_status.set(m))
            except Exception:
                try:
                    self.upd_status.set(msg)
                except Exception:
                    pass

    def _updates_open_release(self) -> None:
            """Open GitHub release page (works even if check failed)."""
            try:
                import webbrowser
                repo = self._updates_repo()
                rel = getattr(self, "_upd_latest", None)
                url = None
                if rel and getattr(rel, "html_url", None):
                    url = rel.html_url
                else:
                    url = f"https://github.com/{repo}/releases/latest"
                webbrowser.open(url)
                self._updates_set_status(self.t("updates.opened_browser"))
            except Exception as e:
                try:
                    self._updates_set_status(f"{self.t('updates.open_failed')}: {e}")
                except Exception:
                    pass

            try:
                req = urllib.request.Request(rel.zip_url, headers={"User-Agent": "shelly-energy-analyzer"})
                with urllib.request.urlopen(req, timeout=10.0) as resp:
                    data = resp.read()
                zip_path.write_bytes(data)
            except Exception:
                self._updates_set_status(self.t("updates.status.unreachable"))
                return

    def _updates_install_latest(self) -> None:
            """Download & install the latest ZIP via updater_helper (cross-platform)."""
            rel = getattr(self, "_upd_latest", None)
            if not rel or not getattr(rel, "zip_url", None) or not getattr(rel, "tag", None):
                try:
                    self._updates_set_status(self.t("updates.not_checked"))
                except Exception:
                    pass
                try:
                    from tkinter import messagebox
                    messagebox.showinfo(self.t("updates.title"), self.t("updates.not_checked"))
                except Exception:
                    pass
                return

            # Download to updates/ and stage unpack
            import urllib.request
            import zipfile
            from pathlib import Path

            app_dir = Path(self.project_root).resolve()
            upd_dir = app_dir / "updates"
            upd_dir.mkdir(parents=True, exist_ok=True)
            zip_path = upd_dir / f"{rel.tag}.zip"
            staging = upd_dir / "staging"

            # clean staging
            try:
                import shutil
                if staging.exists():
                    shutil.rmtree(staging)
            except Exception:
                pass
            staging.mkdir(parents=True, exist_ok=True)

            try:
                self._updates_set_status(f"{self.t('updates.download_install')}…")
            except Exception:
                pass

            try:
                req = urllib.request.Request(str(rel.zip_url), headers={"User-Agent": "shelly-energy-analyzer"})
                with urllib.request.urlopen(req, timeout=20.0) as resp:
                    data = resp.read()
                zip_path.write_bytes(data)
            except Exception as e:
                try:
                    self._updates_set_status(f"{self.t('updates.status.unreachable')} ({e})")
                except Exception:
                    pass
                return

            try:
                with zipfile.ZipFile(zip_path, "r") as z:
                    z.extractall(staging)
            except Exception as e:
                try:
                    self._updates_set_status(f"ZIP extract failed: {e}")
                except Exception:
                    pass
                return

            # If zip contains a single top folder, use it as staging root
            staging_root = staging
            try:
                entries = [p for p in staging.iterdir() if p.name not in (".DS_Store",)]
                if len(entries) == 1 and entries[0].is_dir():
                    staging_root = entries[0]
            except Exception:
                staging_root = staging

            # Launch helper and exit app
            try:
                import subprocess
                helper = [
                    sys.executable, "-m", "shelly_analyzer.updater_helper",
                    "--app-dir", str(app_dir),
                    "--staging-dir", str(staging_root),
                    "--restart", str(app_dir / ("start.bat" if os.name=="nt" else "start.command" if sys.platform=="darwin" else "start.sh")),
                    "--wait-pid", str(os.getpid()),
                    "--update-deps", "1",
                ]
                subprocess.Popen(helper, cwd=str(app_dir))
            except Exception as e:
                try:
                    self._updates_set_status(str(e))
                except Exception:
                    pass
                return

            try:
                self.destroy()
            except Exception:
                pass
            raise SystemExit(0)

            pass


    # Backwards-compatible handlers for older button wiring.
    def _updates_on_check_clicked(self) -> None:
        try:
            self._updates_set_status(self.t("updates.searching"))
        except Exception:
            pass
        self._updates_check_async(auto_install=False)

    def _updates_on_download_clicked(self) -> None:
        self._updates_install_latest()

    def _updates_on_open_clicked(self) -> None:
        self._updates_open_release()
