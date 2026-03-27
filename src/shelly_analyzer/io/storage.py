from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
import shutil
import time
from typing import Callable, Dict, List, Optional, Tuple

import pandas as pd

from shelly_analyzer.core.csv_read import read_csv_files

logger = logging.getLogger(__name__)


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
        self._db = None

    # -- SQLite DB (v6.0.0+) ------------------------------------------------

    @property
    def db(self):
        """Lazy-init EnergyDB instance."""
        if self._db is None:
            from shelly_analyzer.io.database import EnergyDB
            self._db = EnergyDB(self.base_dir / "energy.db")
        return self._db

    @property
    def db_exists(self) -> bool:
        """True if the energy.db file exists on disk."""
        return (self.base_dir / "energy.db").exists()

    # -- device directory ----------------------------------------------------

    def device_dir(self, device_key: str) -> Path:
        d = self.base_dir / device_key
        d.mkdir(parents=True, exist_ok=True)
        return d

    def archive_device_data(self, device_key: str) -> Optional[Path]:
        """Move a device's data directory into data/deleted/ instead of deleting it."""
        src = self.base_dir / device_key
        if not src.exists() or not src.is_dir():
            return None

        deleted_root = self.base_dir / "deleted"
        deleted_root.mkdir(parents=True, exist_ok=True)

        ts = time.strftime("%Y%m%d_%H%M%S")
        base_name = f"{device_key}_{ts}"
        dst = deleted_root / base_name
        i = 2
        while dst.exists():
            dst = deleted_root / f"{base_name}_{i}"
            i += 1

        try:
            shutil.move(str(src), str(dst))
            return dst
        except Exception:
            return None

    # -- meta (DB-backed, JSON fallback for migration) -----------------------

    def meta_path(self, device_key: str) -> Path:
        return self.device_dir(device_key) / "meta.json"

    def load_meta(self, device_key: str) -> MetaState:
        # Try DB first.
        try:
            last_end_ts, updated_at = self.db.load_meta(device_key)
            if last_end_ts is not None:
                return MetaState(last_end_ts=last_end_ts, updated_at=updated_at)
        except Exception:
            pass
        # Fallback: legacy JSON file (pre-v6).
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
        try:
            self.db.save_meta(device_key, state.last_end_ts, state.updated_at)
        except Exception:
            # Fallback: persist to JSON so data is not lost.
            try:
                p = self.meta_path(device_key)
                p.write_text(json.dumps({
                    "last_end_ts": state.last_end_ts,
                    "updated_at": state.updated_at,
                }), encoding="utf-8")
            except Exception:
                pass

    # -- save chunk (DB-backed) ----------------------------------------------

    def save_chunk(self, device_key: str, ts: int, end_ts: int, content: bytes) -> int:
        """Insert CSV chunk data into the database.

        Returns the number of rows inserted (changed from returning a Path in v5).
        """
        return self.db.insert_csv_bytes(device_key, content)

    # -- read device data (DB-backed) ----------------------------------------

    def read_device_df(self, device_key: str, start_ts: Optional[int] = None, end_ts: Optional[int] = None):
        """Read device data as a pandas DataFrame.

        v6.0.0+: reads from SQLite (fast indexed range query).
        Fallback: reads from CSV files if DB has no data (pre-migration).
        """
        # Try DB first (guarded so a corrupt/locked DB falls back to CSV).
        try:
            if self.db.has_data(device_key):
                df = self.db.query_samples(device_key, start_ts=start_ts, end_ts=end_ts)
                # Compatibility: provide 'ts' alias.
                if "ts" not in df.columns and "timestamp" in df.columns:
                    df["ts"] = df["timestamp"]
                # Always return from DB when the device is registered there — even if
                # this specific date range is empty.  Falling through to the CSV path
                # causes a ValueError (no legacy CSV files) that callers such as
                # _cmp_load_daily silently swallow, producing a blank compare chart.
                return df
        except Exception:
            logger.debug("DB read failed for '%s', falling back to CSV", device_key, exc_info=True)

        # Fallback: CSV files (pre-migration or empty DB).
        files = self.list_csv_files(device_key)
        if not files:
            raise ValueError(f"No data found for device '{device_key}'.")

        df = read_csv_files(files)

        # Compatibility: 'ts' alias.
        try:
            if "ts" not in df.columns and "timestamp" in df.columns:
                df["ts"] = df["timestamp"]
        except Exception:
            pass

        # Optional phases file(s) merge (legacy).
        try:
            phase_candidates = []
            phase_candidates.append(self.base_dir / f"{device_key}_phases.csv")
            dev_dir = self.device_dir(device_key)
            phase_candidates.append(dev_dir / f"{device_key}_phases.csv")
            phase_candidates.extend(sorted([p for p in dev_dir.glob("*phases*.csv") if p.is_file()]))

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
                    df = df.merge(ph, on="timestamp", how="left", suffixes=("", "_ph"))
        except Exception:
            pass

        return df

    # -- has_usable_data (DB + CSV fallback) ---------------------------------

    def has_usable_data(self, device_key: str) -> bool:
        """Return True if the device has usable data in DB or CSV files."""
        # DB check (fast).
        try:
            if self.db.has_data(device_key):
                return True
        except Exception:
            pass
        # CSV fallback.
        files = self.list_csv_files(device_key)
        if not files:
            return False
        try:
            if max((p.stat().st_size for p in files), default=0) < 80:
                return False
        except Exception:
            pass
        try:
            df = read_csv_files(files[:3])
            return df is not None and len(df) > 0
        except Exception:
            return False

    # -- CSV file listing (kept for migration + legacy import) ---------------

    def list_csv_files(self, device_key: str) -> List[Path]:
        """Return CSV files for a device (used for migration, not for normal reads)."""
        d = self.device_dir(device_key)
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
            seen: set = set()
            legacy: List[Path] = []
            p0 = self.base_dir / f"{device_key}.csv"
            if p0.exists() and p0.is_file():
                legacy.append(p0)
                seen.add(p0)
            for variant in {device_key, device_key.lower(), device_key.upper()}:
                for p in self.base_dir.glob(f"emdata_{variant}_*.csv"):
                    if p.is_file() and p not in seen:
                        legacy.append(p)
                        seen.add(p)
            files = legacy

        files.sort(key=lambda x: x.name)
        return files

    # -- CSV → DB migration --------------------------------------------------

    def needs_migration(self, device_keys: List[str]) -> bool:
        """Check if any device has CSV data but no DB data."""
        for key in device_keys:
            csv_files = self.list_csv_files(key)
            if csv_files and not self.db.has_data(key):
                return True
        return False

    def migrate_csvs_to_db(
        self,
        device_keys: List[str],
        progress: Optional[Callable] = None,
    ) -> Dict[str, int]:
        """One-time migration: read all CSV files → insert into SQLite DB.

        Returns dict mapping device_key → number of rows migrated.
        """
        from shelly_analyzer.core.energy import calculate_energy

        result: Dict[str, int] = {}
        total = len(device_keys)

        for i, key in enumerate(device_keys):
            if progress:
                try:
                    progress(key, i, total, "Loading CSVs...")
                except Exception:
                    pass

            csv_files = self.list_csv_files(key)
            if not csv_files:
                result[key] = 0
                continue

            # Skip if DB already has data for this device.
            if self.db.has_data(key):
                result[key] = self.db.row_count(key)
                continue

            try:
                df = read_csv_files(csv_files)
                if df.empty:
                    result[key] = 0
                    continue

                # Calculate energy (total_power + energy_kwh) before inserting.
                try:
                    df = calculate_energy(df)
                except Exception:
                    # If energy calc fails, insert raw data anyway.
                    pass

                rows = self.db.insert_dataframe(key, df)
                result[key] = rows

                if progress:
                    try:
                        progress(key, i, total, f"{rows} rows migrated")
                    except Exception:
                        pass

                logger.info("Migrated %d rows for device '%s' from CSV to DB", rows, key)

            except Exception as e:
                logger.warning("Migration failed for device '%s': %s", key, e)
                result[key] = 0

        # Migrate meta.json files to DB.
        for key in device_keys:
            try:
                p = self.meta_path(key)
                if p.exists():
                    raw = json.loads(p.read_text(encoding="utf-8"))
                    if isinstance(raw, dict):
                        last_end_ts = raw.get("last_end_ts")
                        updated_at = raw.get("updated_at")
                        self.db.save_meta(
                            key,
                            int(last_end_ts) if last_end_ts is not None else None,
                            int(updated_at) if updated_at is not None else None,
                        )
            except Exception:
                pass

        return result

    def archive_csv_files(self, device_keys: List[str]) -> Dict[str, int]:
        """Move CSV files to data/csv_archive/<device_key>/ after migration.

        Returns dict mapping device_key → number of files archived.
        """
        archive_root = self.base_dir / "csv_archive"
        result: Dict[str, int] = {}

        for key in device_keys:
            csv_files = self.list_csv_files(key)
            if not csv_files:
                result[key] = 0
                continue

            dst_dir = archive_root / key
            dst_dir.mkdir(parents=True, exist_ok=True)

            archived = 0
            for src in csv_files:
                try:
                    dst = dst_dir / src.name
                    shutil.move(str(src), str(dst))
                    archived += 1
                except Exception:
                    pass

            # Also archive phases files.
            for ph_name in (f"{key}_phases.csv",):
                ph = self.base_dir / ph_name
                if ph.exists() and ph.is_file():
                    try:
                        shutil.move(str(ph), str(dst_dir / ph.name))
                    except Exception:
                        pass

            # Archive meta.json.
            try:
                meta_p = self.device_dir(key) / "meta.json"
                if meta_p.exists():
                    shutil.move(str(meta_p), str(dst_dir / "meta.json"))
            except Exception:
                pass

            result[key] = archived

        return result

    # -- re-import from csv_archive (v6.0.0.2: fill expanded schema) ---------

    def needs_reimport(self, device_keys: List[str]) -> bool:
        """Check if existing DB data needs re-import to fill new columns."""
        try:
            return self.db.needs_reimport()
        except Exception:
            return False

    def reimport_from_archive(
        self,
        device_keys: List[str],
        progress: Optional[Callable] = None,
    ) -> Dict[str, int]:
        """Re-import CSV data from csv_archive/ to fill expanded DB columns.

        v6.0.0.2: The DB schema was expanded with ~42 new columns (voltage
        min/max/avg, current min/max/avg, apparent power, reactive energy).
        Existing data was imported before those columns existed, so all new
        columns are NULL.  This method re-reads the archived CSVs and uses
        INSERT OR REPLACE to fill the missing columns.

        Returns dict mapping device_key → number of rows re-imported.
        """
        from shelly_analyzer.core.energy import calculate_energy

        archive_root = self.base_dir / "csv_archive"
        result: Dict[str, int] = {}
        total = len(device_keys)

        for i, key in enumerate(device_keys):
            if progress:
                try:
                    progress(key, i, total, "Re-importing from archive...")
                except Exception:
                    pass

            archive_dir = archive_root / key
            if not archive_dir.exists() or not archive_dir.is_dir():
                result[key] = 0
                continue

            csv_files = sorted([
                p for p in archive_dir.glob("*.csv")
                if p.is_file() and "phases" not in p.name.lower()
            ])
            if not csv_files:
                result[key] = 0
                continue

            try:
                df = read_csv_files(csv_files)
                if df is None or df.empty:
                    result[key] = 0
                    continue

                # Calculate energy (total_power + energy_kwh) before inserting.
                try:
                    df = calculate_energy(df)
                except Exception:
                    pass

                rows = self.db.insert_dataframe(key, df)
                result[key] = rows

                if progress:
                    try:
                        progress(key, i, total, f"{rows} rows re-imported")
                    except Exception:
                        pass

                logger.info(
                    "Re-imported %d rows for device '%s' from csv_archive "
                    "(expanded schema columns now filled)",
                    rows, key,
                )

            except Exception as e:
                logger.warning("Re-import from archive failed for device '%s': %s", key, e)
                result[key] = 0

        return result

    # -- auto-import from previous installs (unchanged) ----------------------

    def auto_import_from_previous_installs(self, device_keys: List[str], search_root: Optional[Path] = None) -> Dict[str, int]:
        """Try to auto-import CSV data from previous installations.

        Many users keep multiple version folders side-by-side, e.g.:
            ~/shelly_energy_analyzer_refactored_v5.5.2/
            ~/shelly_energy_analyzer_refactored_v5.6.6/

        This routine searches sibling folders under *search_root* (default:
        project root's parent) and imports matching device CSVs into the DB.

        Returns a dict mapping device_key -> number of rows imported.
        """
        imported: Dict[str, int] = {k: 0 for k in device_keys}

        missing = [k for k in device_keys if not self.has_usable_data(k)]
        if not missing:
            return imported

        try:
            root = Path(search_root) if search_root else self.base_dir.parent.parent
        except Exception:
            root = None
        if root is None or not root.exists() or not root.is_dir():
            return imported

        candidates: List[Path] = []
        for p in root.iterdir():
            if not p.is_dir():
                continue
            name = p.name.lower()
            if "shelly_energy_analyzer" in name and "refactored" in name and "_v" in name:
                candidates.append(p)

        import re

        def _ver_key(path: Path) -> Tuple[int, int, int]:
            m = re.search(r"_v(\d+)\.(\d+)\.(\d+)", path.name)
            if not m:
                return (0, 0, 0)
            return (int(m.group(1)), int(m.group(2)), int(m.group(3)))

        candidates.sort(key=_ver_key, reverse=True)

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
                if imported.get(key, 0) > 0:
                    continue

                src_csvs: List[Path] = []
                dev_dir = data_dir / key
                if dev_dir.exists() and dev_dir.is_dir():
                    src_csvs.extend([p for p in dev_dir.glob("*.csv") if p.is_file()])

                p0 = data_dir / f"{key}.csv"
                if p0.exists() and p0.is_file():
                    src_csvs.append(p0)
                src_csvs.extend([p for p in data_dir.glob(f"emdata_{key}_*.csv") if p.is_file()])
                src_csvs.extend([p for p in data_dir.glob(f"emdata_{key.lower()}_*.csv") if p.is_file()])
                src_csvs.extend([p for p in data_dir.glob(f"emdata_{key.upper()}_*.csv") if p.is_file()])

                if not src_csvs:
                    continue

                # Import directly into DB instead of copying CSV files.
                try:
                    unique_csvs = sorted(set(src_csvs), key=lambda x: x.name)
                    df = read_csv_files(unique_csvs)
                    if df is not None and len(df) > 0:
                        from shelly_analyzer.core.energy import calculate_energy
                        try:
                            df = calculate_energy(df)
                        except Exception:
                            pass
                        rows = self.db.insert_dataframe(key, df)
                        imported[key] = rows
                except Exception:
                    continue

        return imported

    def auto_import_from_previous_installs_mapped(
        self,
        devices: List[Dict[str, str]],
        search_root: Optional[Path] = None,
    ) -> Dict[str, int]:
        """Auto-import using host/name mapping from previous config.json.

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

        candidates: List[Path] = []
        for p in root.iterdir():
            if not p.is_dir():
                continue
            name = p.name.lower()
            if "shelly_energy_analyzer" in name and "refactored" in name and "_v" in name:
                candidates.append(p)

        import re

        def _ver_key(path: Path) -> Tuple[int, int, int]:
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
            if ch:
                for od in old_devs:
                    if (od.get("host") or "").strip() == ch:
                        return str(od.get("key") or "").strip() or None
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

                # Import directly into DB.
                try:
                    unique_csvs = sorted(set(src_csvs), key=lambda x: x.name)
                    df = read_csv_files(unique_csvs)
                    if df is not None and len(df) > 0:
                        from shelly_analyzer.core.energy import calculate_energy
                        try:
                            df = calculate_energy(df)
                        except Exception:
                            pass
                        rows = self.db.insert_dataframe(ck, df)
                        imported[ck] = rows
                except Exception:
                    continue

        return imported

    def ensure_data_for_devices(self, devices: List[Dict[str, str]]) -> Dict[str, object]:
        """Best-effort: make existing data visible without manual copying."""
        diag: Dict[str, object] = {
            "base_dir": str(self.base_dir),
            "attempts": [],
            "imported": {},
            "files": {},
        }
        keys = [str(d.get("key") or "").strip() for d in (devices or []) if str(d.get("key") or "").strip()]
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
                old = self.base_dir
                old_db = self._db
                self.base_dir = cand
                self._db = None  # Reset so db property re-inits for new base_dir
                try:
                    cand_missing = [k for k in keys if not self.has_usable_data(k)]
                    if len(cand_missing) < len(missing):
                        diag["attempts"].append(f"use_base_dir:{cand}")
                        diag["base_dir"] = str(self.base_dir)
                        missing = cand_missing
                        if not missing:
                            break
                    else:
                        self.base_dir = old
                        self._db = old_db
                except Exception:
                    self.base_dir = old
                    self._db = old_db
                    continue
        except Exception as e:
            diag["attempts"].append(f"common_location_error:{e}")

        self.last_data_diag = diag
        return diag
