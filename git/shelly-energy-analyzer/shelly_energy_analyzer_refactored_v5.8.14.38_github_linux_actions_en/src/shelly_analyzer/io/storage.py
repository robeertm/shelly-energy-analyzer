from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
import shutil
import time
from typing import Dict, List, Optional, Tuple

from shelly_analyzer.core.csv_read import read_csv_files


@dataclass
class MetaState:
    last_end_ts: Optional[int] = None
    updated_at: Optional[int] = None


class Storage:
    def __init__(self, base_dir: Optional[Path] = None) -> None:
        self.base_dir = Path(base_dir) if base_dir else Path.cwd() / "data"
        self.base_dir.mkdir(parents=True, exist_ok=True)
        # Diagnostics for auto-discovery/import (useful for /api/plots_data diag)
        self.last_data_diag: Dict[str, object] = {}

    def device_dir(self, device_key: str) -> Path:
        d = self.base_dir / device_key
        d.mkdir(parents=True, exist_ok=True)
        return d

    def archive_device_data(self, device_key: str) -> Optional[Path]:
        """Move a device's data directory into data/deleted/ instead of deleting it.

        Returns the new archived path, or None if nothing was moved.
        """
        src = self.base_dir / device_key
        if not src.exists() or not src.is_dir():
            return None

        deleted_root = self.base_dir / "deleted"
        deleted_root.mkdir(parents=True, exist_ok=True)

        ts = time.strftime("%Y%m%d_%H%M%S")
        base_name = f"{device_key}_{ts}"
        dst = deleted_root / base_name
        # Ensure uniqueness
        i = 2
        while dst.exists():
            dst = deleted_root / f"{base_name}_{i}"
            i += 1

        try:
            shutil.move(str(src), str(dst))
            return dst
        except Exception:
            return None

    def meta_path(self, device_key: str) -> Path:
        return self.device_dir(device_key) / "meta.json"

    def load_meta(self, device_key: str) -> MetaState:
        p = self.meta_path(device_key)
        if not p.exists():
            return MetaState(last_end_ts=None, updated_at=None)
        try:
            raw = json.loads(p.read_text(encoding="utf-8"))
            if not isinstance(raw, dict):
                return MetaState()
            last_end_ts = raw.get("last_end_ts")
            return MetaState(
                last_end_ts=int(last_end_ts) if last_end_ts is not None else None,
                updated_at=int(raw.get("updated_at")) if raw.get("updated_at") is not None else None,
            )
        except Exception:
            return MetaState()

    def save_meta(self, device_key: str, state: MetaState) -> None:
        p = self.meta_path(device_key)
        obj = {"last_end_ts": state.last_end_ts, "updated_at": state.updated_at}
        p.write_text(json.dumps(obj, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    def save_chunk(self, device_key: str, ts: int, end_ts: int, content: bytes) -> Path:
        d = self.device_dir(device_key)
        fn = f"emdata_{device_key}_{int(ts)}-{int(end_ts)}.csv"
        p = d / fn
        p.write_bytes(content)
        return p

    def list_csv_files(self, device_key: str) -> List[Path]:
        """Return CSV files for a device.

        Newer versions store CSV chunks under:
            data/<device_key>/*.csv

        Older versions (and some deployments) used a flat layout like:
            data/<device_key>.csv
            data/<device_key>_phases.csv

        To keep backwards compatibility, we fall back to the legacy layout
        if the per-device directory is empty.
        """
        # Preferred (new) location
        d = self.device_dir(device_key)
        # IMPORTANT: avoid mixing in phases/foreign CSVs placed in the device dir.
        # A single foreign CSV without timestamps would previously break Plotly plots
        # (read_csv_files would raise), resulting in empty devices.
        files = []
        for p in d.glob("*.csv"):
            if not p.is_file():
                continue
            n = p.name.lower()
            if "phases" in n or n.endswith("_phases.csv"):
                continue
            files.append(p)

        # Legacy fallbacks (flat data directory)
        if not files:
            legacy: List[Path] = []
            # Single merged file
            p0 = self.base_dir / f"{device_key}.csv"
            if p0.exists() and p0.is_file():
                legacy.append(p0)
            # Historical chunk naming in base_dir (if any)
            legacy.extend([p for p in self.base_dir.glob(f"emdata_{device_key}_*.csv") if p.is_file()])
            legacy.extend([p for p in self.base_dir.glob(f"emdata_{device_key.lower()}_*.csv") if p.is_file()])
            legacy.extend([p for p in self.base_dir.glob(f"emdata_{device_key.upper()}_*.csv") if p.is_file()])
            files = legacy

        files.sort(key=lambda x: x.name)
        return files

    def has_usable_data(self, device_key: str) -> bool:
        """Return True if the device has CSVs that actually contain usable rows.

        We consider a device "missing" if there are no CSV files OR if the
        existing CSV files cannot be parsed (e.g. wrong timestamp column) OR
        they contain only headers.
        """
        files = self.list_csv_files(device_key)
        if not files:
            return False
        # Quick size check (header-only files are often < 80 bytes)
        try:
            if max((p.stat().st_size for p in files), default=0) < 80:
                return False
        except Exception:
            pass
        # Parse just enough to know if we have rows
        try:
            df = read_csv_files(files[:3])
            return df is not None and len(df) > 0
        except Exception:
            return False

    def auto_import_from_previous_installs(self, device_keys: List[str], search_root: Optional[Path] = None) -> Dict[str, int]:
        """Try to auto-import CSV data from previous installations.

        Many users keep multiple version folders side-by-side, e.g.:
            ~/shelly_energy_analyzer_refactored_v5.5.2/
            ~/shelly_energy_analyzer_refactored_v5.6.6/

        In that case, they typically expect the new version to immediately show
        existing data without manually copying the data/ folder.

        This routine searches sibling folders under *search_root* (default:
        project root's parent) and copies any matching device CSVs into the
        current data/<device_key>/ folder (and phases CSV into data/).

        Returns a dict mapping device_key -> number of CSV files imported.
        """

        imported: Dict[str, int] = {k: 0 for k in device_keys}

        # Only attempt when we currently have no *usable* data for a device.
        missing = [k for k in device_keys if not self.has_usable_data(k)]
        if not missing:
            return imported

        try:
            root = Path(search_root) if search_root else self.base_dir.parent.parent
        except Exception:
            root = None
        if root is None or not root.exists() or not root.is_dir():
            return imported

        # Candidate folders: sibling directories that look like app versions.
        # We keep this cheap: only 1 level deep.
        candidates: List[Path] = []
        for p in root.iterdir():
            if not p.is_dir():
                continue
            name = p.name.lower()
            if "shelly_energy_analyzer" in name and "refactored" in name and "_v" in name:
                candidates.append(p)

        # Sort descending by version-ish suffix if present
        def _ver_key(path: Path) -> Tuple[int, int, int]:
            import re
            m = re.search(r"_v(\d+)\.(\d+)\.(\d+)", path.name)
            if not m:
                return (0, 0, 0)
            return (int(m.group(1)), int(m.group(2)), int(m.group(3)))

        candidates.sort(key=_ver_key, reverse=True)

        # Skip our current project root if it appears in candidates
        try:
            cur_root = self.base_dir.parent.resolve()
        except Exception:
            cur_root = None

        for cand in candidates:
            try:
                if cur_root is not None and cand.resolve() == cur_root:
                    continue
            except Exception:
                pass

            data_dir = cand / "data"
            if not data_dir.exists() or not data_dir.is_dir():
                continue

            for key in list(missing):
                # If we already imported for this key, skip
                if imported.get(key, 0) > 0:
                    continue

                # Search possible legacy layouts
                src_csvs: List[Path] = []

                # New layout in older folder
                dev_dir = data_dir / key
                if dev_dir.exists() and dev_dir.is_dir():
                    src_csvs.extend([p for p in dev_dir.glob("*.csv") if p.is_file()])

                # Legacy flat layout
                p0 = data_dir / f"{key}.csv"
                if p0.exists() and p0.is_file():
                    src_csvs.append(p0)
                src_csvs.extend([p for p in data_dir.glob(f"emdata_{key}_*.csv") if p.is_file()])
                src_csvs.extend([p for p in data_dir.glob(f"emdata_{key.lower()}_*.csv") if p.is_file()])
                src_csvs.extend([p for p in data_dir.glob(f"emdata_{key.upper()}_*.csv") if p.is_file()])

                if not src_csvs:
                    continue

                # Copy CSVs into our per-device directory
                dst_dir = self.device_dir(key)
                copied = 0
                for src in sorted(set(src_csvs), key=lambda x: x.name):
                    try:
                        dst = dst_dir / src.name
                        if not dst.exists():
                            shutil.copy2(src, dst)
                            copied += 1
                    except Exception:
                        continue

                # Copy optional phases file into our base_dir
                try:
                    ph = data_dir / f"{key}_phases.csv"
                    if ph.exists() and ph.is_file():
                        dst_ph = self.base_dir / f"{key}_phases.csv"
                        if not dst_ph.exists():
                            shutil.copy2(ph, dst_ph)
                except Exception:
                    pass

                imported[key] = copied

        return imported

    def auto_import_from_previous_installs_mapped(
        self,
        devices: List[Dict[str, str]],
        search_root: Optional[Path] = None,
    ) -> Dict[str, int]:
        """Auto-import using host/name mapping from previous config.json.

        This fixes a common real-world migration: earlier versions used keys like
        "shelly1"/"shelly2" (and filenames emdata_shelly1_*.csv), while the user
        later renamed keys to "haus"/"server" in config.

        devices: list of dicts with at least {"key":..., "host":..., "name":...}
        """

        cur = [
            {
                "key": str(d.get("key") or "").strip(),
                "host": str(d.get("host") or "").strip(),
                "name": str(d.get("name") or "").strip(),
            }
            for d in (devices or [])
            if str(d.get("key") or "").strip()
        ]

        imported: Dict[str, int] = {d["key"]: 0 for d in cur}
        missing = [d for d in cur if not self.has_usable_data(d["key"])]
        if not missing:
            return imported

        try:
            root = Path(search_root) if search_root else self.base_dir.parent.parent
        except Exception:
            root = None
        if root is None or not root.exists() or not root.is_dir():
            return imported

        # Candidate folders: sibling directories that look like app versions.
        candidates: List[Path] = []
        for p in root.iterdir():
            if not p.is_dir():
                continue
            name = p.name.lower()
            if "shelly_energy_analyzer" in name and "refactored" in name and "_v" in name:
                candidates.append(p)

        def _ver_key(path: Path) -> Tuple[int, int, int]:
            import re
            m = re.search(r"_v(\d+)\.(\d+)\.(\d+)", path.name)
            if not m:
                return (0, 0, 0)
            return (int(m.group(1)), int(m.group(2)), int(m.group(3)))

        candidates.sort(key=_ver_key, reverse=True)

        try:
            cur_root = self.base_dir.parent.resolve()
        except Exception:
            cur_root = None

        def _load_old_devices(cfg_path: Path) -> List[Dict[str, str]]:
            try:
                raw = json.loads(cfg_path.read_text(encoding="utf-8"))
                if not isinstance(raw, dict):
                    return []
                devs = raw.get("devices")
                if not isinstance(devs, list):
                    return []
                out: List[Dict[str, str]] = []
                for x in devs:
                    if not isinstance(x, dict):
                        continue
                    k = str(x.get("key") or "").strip()
                    if not k:
                        continue
                    out.append({
                        "key": k,
                        "host": str(x.get("host") or "").strip(),
                        "name": str(x.get("name") or k).strip(),
                    })
                return out
            except Exception:
                return []

        def _match_src_key(cur_dev: Dict[str, str], old_devs: List[Dict[str, str]]) -> Optional[str]:
            ch = (cur_dev.get("host") or "").strip()
            cn = (cur_dev.get("name") or "").strip().lower()
            # First: host match
            if ch:
                for od in old_devs:
                    if (od.get("host") or "").strip() == ch:
                        return str(od.get("key") or "").strip() or None
            # Second: name match (case-insensitive contains)
            if cn:
                for od in old_devs:
                    on = (od.get("name") or "").strip().lower()
                    if on and (on == cn or on in cn or cn in on):
                        return str(od.get("key") or "").strip() or None
            return None

        for cand in candidates:
            try:
                if cur_root is not None and cand.resolve() == cur_root:
                    continue
            except Exception:
                pass

            data_dir = cand / "data"
            if not data_dir.exists() or not data_dir.is_dir():
                continue

            old_devs = _load_old_devices(cand / "config.json")

            for cur_dev in missing:
                ck = cur_dev["key"]
                if imported.get(ck, 0) > 0:
                    continue
                src_key = _match_src_key(cur_dev, old_devs) or ck

                # Gather source CSVs for src_key
                src_csvs: List[Path] = []
                dev_dir = data_dir / src_key
                if dev_dir.exists() and dev_dir.is_dir():
                    src_csvs.extend([p for p in dev_dir.glob("*.csv") if p.is_file()])

                p0 = data_dir / f"{src_key}.csv"
                if p0.exists() and p0.is_file():
                    src_csvs.append(p0)
                src_csvs.extend([p for p in data_dir.glob(f"emdata_{src_key}_*.csv") if p.is_file()])
                src_csvs.extend([p for p in data_dir.glob(f"emdata_{src_key.lower()}_*.csv") if p.is_file()])
                src_csvs.extend([p for p in data_dir.glob(f"emdata_{src_key.upper()}_*.csv") if p.is_file()])

                if not src_csvs:
                    continue

                dst_dir = self.device_dir(ck)
                copied = 0
                for src in sorted(set(src_csvs), key=lambda x: x.name):
                    try:
                        dst = dst_dir / src.name
                        if not dst.exists():
                            shutil.copy2(src, dst)
                            copied += 1
                    except Exception:
                        continue

                # Copy phases if present (by src key or current key)
                try:
                    for ph_name in (f"{src_key}_phases.csv", f"{ck}_phases.csv"):
                        ph = data_dir / ph_name
                        if ph.exists() and ph.is_file():
                            dst_ph = self.base_dir / f"{ck}_phases.csv"
                            if not dst_ph.exists():
                                shutil.copy2(ph, dst_ph)
                            break
                except Exception:
                    pass

                imported[ck] = copied

        return imported

    def ensure_data_for_devices(self, devices: List[Dict[str, str]]) -> Dict[str, object]:
        """Best-effort: make existing data visible without manual copying.

        Strategy:
        1) Try mapped auto-import from previous installs (by host/name).
        2) Try common user locations (power_website/data) as alternate base_dir.

        Returns a diagnostic dict.
        """
        diag: Dict[str, object] = {
            "base_dir": str(self.base_dir),
            "attempts": [],
            "imported": {},
            "files": {},
        }
        keys = [str(d.get("key") or "").strip() for d in (devices or []) if str(d.get("key") or "").strip()]
        # Always report what we currently see for each key
        try:
            diag["files"] = {k: [p.name for p in self.list_csv_files(k)] for k in keys}
        except Exception:
            pass

        missing = [k for k in keys if not self.has_usable_data(k)]
        if not missing:
            self.last_data_diag = diag
            return diag

        # 1) mapped import
        try:
            imp = self.auto_import_from_previous_installs_mapped(devices)
            diag["attempts"].append("import_previous_mapped")
            diag["imported"] = imp
        except Exception as e:
            diag["attempts"].append(f"import_previous_mapped_error:{e}")

        missing = [k for k in keys if not self.has_usable_data(k)]
        if not missing:
            self.last_data_diag = diag
            return diag

        # 2) common location: ~/power_website/data (many setups)
        try:
            home = Path.home()
            common = [
                home / "power_website" / "data",
                home / "powerweb" / "data",
                home / "PowerWebsite" / "data",
            ]
            for cand in common:
                if not cand.exists() or not cand.is_dir():
                    continue
                # Temporarily switch base_dir and test
                old = self.base_dir
                self.base_dir = cand
                try:
                    cand_missing = [k for k in keys if not self.has_usable_data(k)]
                    if len(cand_missing) < len(missing):
                        diag["attempts"].append(f"use_base_dir:{cand}")
                        diag["base_dir"] = str(self.base_dir)
                        missing = cand_missing
                        if not missing:
                            break
                    else:
                        # revert if it doesn't help
                        self.base_dir = old
                except Exception:
                    self.base_dir = old
                    continue
        except Exception as e:
            diag["attempts"].append(f"common_location_error:{e}")

        self.last_data_diag = diag
        return diag

    def read_device_df(self, device_key: str):
        """Read a device dataframe.

        If a separate legacy "<device_key>_phases.csv" exists, it is merged
        onto the main dataframe by timestamp to expose per-phase columns.

        Some installs store phases data inside the per-device directory
        (data/<device_key>/*phases*.csv). Older versions stored it as a flat
        file (data/<device_key>_phases.csv). We merge either location.
        """
        files = self.list_csv_files(device_key)
        if not files:
            raise ValueError(f"No CSV data found for device '{device_key}'.")

        df = read_csv_files(files)

        # Compatibility: parts of the code (and some older JSON endpoints) still
        # expect a datetime column named 'ts'. Provide it unconditionally.
        try:
            if "ts" not in df.columns and "timestamp" in df.columns:
                df["ts"] = df["timestamp"]
        except Exception:
            pass

        # Optional phases file(s) (flat layout and/or per-device directory)
        try:
            phase_candidates = []
            # Legacy flat layout
            phase_candidates.append(self.base_dir / f"{device_key}_phases.csv")
            # Some installs keep phases next to chunks
            dev_dir = self.device_dir(device_key)
            phase_candidates.append(dev_dir / f"{device_key}_phases.csv")
            # Best-effort: any phases file in device directory
            phase_candidates.extend(sorted([p for p in dev_dir.glob("*phases*.csv") if p.is_file()]))

            # Keep unique existing paths
            seen = set()
            uniq = []
            for p in phase_candidates:
                try:
                    rp = str(p.resolve())
                except Exception:
                    rp = str(p)
                if rp in seen:
                    continue
                seen.add(rp)
                if p.exists() and p.is_file():
                    uniq.append(p)

            for phases_path in uniq:
                try:
                    ph = read_csv_files([phases_path])
                except Exception:
                    continue
                if "timestamp" in df.columns and "timestamp" in ph.columns and len(ph) > 0:
                    # Merge, prefer main df columns on conflicts
                    df = df.merge(ph, on="timestamp", how="left", suffixes=("", "_ph"))
        except Exception:
            pass

        return df

    def pack_csvs(
        self,
        device_key: str,
        threshold_count: int = 120,
        max_megabytes: int = 20,
        remove_merged: bool = False,
    ) -> Tuple[bool, Optional[Path]]:
        """Merge many chunks into a single packed CSV (deduped by timestamp).

        Returns (did_pack, packed_path).
        """
        files = self.list_csv_files(device_key)
        if len(files) < 2:
            return False, None

        total_bytes = sum(p.stat().st_size for p in files)
        total_mb = total_bytes / (1024 * 1024)
        if len(files) < threshold_count and total_mb < max_megabytes:
            return False, None

        df = read_csv_files(files)
        packed_path = self.device_dir(device_key) / "packed.csv"
        df.to_csv(packed_path, index=False)

        if remove_merged:
            for p in files:
                if p.name != "packed.csv":
                    try:
                        p.unlink()
                    except Exception:
                        pass
        return True, packed_path
